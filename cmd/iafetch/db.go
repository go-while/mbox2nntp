package main

import (
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"time"

	_ "modernc.org/sqlite"
)

// Item states. An item is "done" when every file it should have was
// downloaded and verified; "dark", "missing" and "empty" items have nothing
// to download and are skipped unless -retry-unavailable is given;
// "incomplete" items had a file fail and are tried again on the next run.
const (
	stDone       = "done"
	stIncomplete = "incomplete"
	stDark       = "dark"
	stMissing    = "missing"
	stEmpty      = "empty"
)

const schema = `
CREATE TABLE IF NOT EXISTS items (
	identifier TEXT PRIMARY KEY,
	ord        INTEGER,
	tier       TEXT,
	list       TEXT,
	status     TEXT NOT NULL,
	files      INTEGER NOT NULL DEFAULT 0,
	bytes      INTEGER NOT NULL DEFAULT 0,
	note       TEXT NOT NULL DEFAULT '',
	updated    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
	identifier TEXT NOT NULL,
	name       TEXT NOT NULL,
	size       INTEGER NOT NULL,
	md5        TEXT NOT NULL,
	path       TEXT NOT NULL,
	done_at    TEXT NOT NULL,
	PRIMARY KEY (identifier, name)
);
`

type db struct{ *sql.DB }

func openDB(path string) (*db, error) {
	dsn := "file:" + (&url.URL{Path: path}).EscapedPath() +
		"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	d, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: the writes are few and small, and SQLite allows one
	// writer at a time anyway.
	d.SetMaxOpenConns(1)
	if _, err := d.Exec(schema); err != nil {
		d.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &db{d}, nil
}

func now() string { return time.Now().UTC().Format(time.RFC3339) }

// itemStatus returns the recorded state of an item, "" if it has none.
func (d *db) itemStatus(id string) (string, error) {
	var s string
	err := d.QueryRow(`SELECT status FROM items WHERE identifier = ?`, id).Scan(&s)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return s, err
}

func (d *db) setItem(it item, status string, files int, bytes int64, note string) error {
	_, err := d.Exec(`INSERT INTO items (identifier, ord, tier, list, status, files, bytes, note, updated)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (identifier) DO UPDATE SET ord = excluded.ord, tier = excluded.tier,
			list = excluded.list, status = excluded.status, files = excluded.files,
			bytes = excluded.bytes, note = excluded.note, updated = excluded.updated`,
		it.ID, it.Order, it.Tier, it.List, status, files, bytes, note, now())
	return err
}

// doneFiles returns the names of an item's files already downloaded.
func (d *db) doneFiles(id string) (map[string]bool, error) {
	rows, err := d.Query(`SELECT name FROM files WHERE identifier = ?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		m[n] = true
	}
	return m, rows.Err()
}

func (d *db) fileDone(id string, f iaFile, path string) error {
	_, err := d.Exec(`INSERT OR REPLACE INTO files (identifier, name, size, md5, path, done_at)
		VALUES (?, ?, ?, ?, ?, ?)`, id, f.Name, f.Size, f.MD5, path, now())
	return err
}

// printStatus writes how many items are in each state, by tier.
func (d *db) printStatus(w io.Writer) error {
	rows, err := d.Query(`SELECT tier, status, COUNT(*), SUM(files), SUM(bytes)
		FROM items GROUP BY tier, status ORDER BY tier, status`)
	if err != nil {
		return err
	}
	defer rows.Close()
	fmt.Fprintf(w, "%-5s %-11s %8s %9s %12s\n", "tier", "status", "items", "files", "GB")
	for rows.Next() {
		var tier, status string
		var n, files, bytes int64
		if err := rows.Scan(&tier, &status, &n, &files, &bytes); err != nil {
			return err
		}
		fmt.Fprintf(w, "%-5s %-11s %8d %9d %12.2f\n", tier, status, n, files, float64(bytes)/1e9)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var files, bytes int64
	if err := d.QueryRow(`SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files`).Scan(&files, &bytes); err != nil {
		return err
	}
	fmt.Fprintf(w, "files downloaded: %d, %.2f GB\n", files, float64(bytes)/1e9)
	return nil
}
