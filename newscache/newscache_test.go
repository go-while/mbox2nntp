package newscache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/go-while/mbox2nntp/newscache/newscachetest"
)

const (
	big   = "Path: a!not-for-mail\r\nNewsgroups: x.y\r\nSubject: big\r\nMessage-ID: <big@x>\r\n\r\nbody\r\n..leading dot\r\n...\r\n"
	small = "Path: a!not-for-mail\r\nNewsgroups: x.y\r\nSubject: small\r\nMessage-ID: <small@x>\r\n\r\nFrom here\r\n"
)

var dbRecs = map[uint64]newscachetest.Record{
	2: {Type: newscachetest.Overview, Data: "2\tbig\ta@b\tdate\t<big@x>\t\t100\t3\t"},
	3: {Type: newscachetest.Article, Data: small},
	5: {Type: newscachetest.BigArticle, Data: "5\tgone\ta@b\tdate\t<gone@x>\t\t100\t3\t"},
}

func TestParseDB(t *testing.T) {
	for _, bo := range []binary.ByteOrder{binary.LittleEndian, binary.BigEndian} {
		for _, w := range []int{4, 8} {
			db, err := ParseDB(newscachetest.DB(bo, w, 2, 6, dbRecs))
			if err != nil {
				t.Fatalf("%v/%d: %v", bo, w, err)
			}
			want := []Record{
				{2, Overview, []byte(dbRecs[2].Data)},
				{3, Article, []byte(small)},
				{5, BigArticle, []byte(dbRecs[5].Data)},
			}
			if db.First != 2 || db.Last != 6 || !reflect.DeepEqual(db.Records, want) || db.Mtime.Year() != 2020 {
				t.Errorf("%v/%d: %d-%d %+v, mtime %v", bo, w, db.First, db.Last, db.Records, db.Mtime)
			}
		}
	}
	if db, err := ParseDB(newscachetest.DB(binary.LittleEndian, 8, 1, 0, nil)); err != nil || len(db.Records) != 0 {
		t.Errorf("empty: %+v, %v", db, err)
	}

	good := newscachetest.DB(binary.LittleEndian, 8, 2, 6, dbRecs)
	for name, b := range map[string][]byte{
		"empty":       nil,
		"text":        []byte("Path: x\r\n"),
		"truncated":   good[:40],
		"version 1":   append([]byte{48, 0, 0, 0, 1, 0, 0, 0}, good[8:]...),
		"short range": good[:0x40],
	} {
		if _, err := ParseDB(b); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	// article 4, which has no record, pointing past the end: the records
	// before it are returned with the error
	cut := slices.Clone(good)
	binary.LittleEndian.PutUint64(cut[0x38+(2+4-2)*8:], uint64(len(cut)+64))
	db, err := ParseDB(cut)
	if err == nil || db == nil || len(db.Records) != 2 {
		t.Errorf("bad offset: %v, %+v", err, db)
	}
}

func TestArtNum(t *testing.T) {
	for name, want := range map[string]uint64{".art1": 1, ".art42": 42, ".art007": 7} {
		if n, ok := ArtNum(name); !ok || n != want {
			t.Errorf("%s: %d, %t", name, n, ok)
		}
	}
	for _, name := range []string{".art", ".art-1", ".art+1", ".art1x", "art1", ".db", ".art 1"} {
		if _, ok := ArtNum(name); ok {
			t.Errorf("%s: taken", name)
		}
	}
}

func TestSplit(t *testing.T) {
	for _, tc := range []struct {
		in         string
		head, body []string
		odd        bool
	}{
		{big, []string{"Path: a!not-for-mail", "Newsgroups: x.y", "Subject: big", "Message-ID: <big@x>"},
			[]string{"body", ".leading dot", ".."}, false},
		// a single dot is kept, the doubled ones are still undone
		{"A: 1\r\n\r\n.single\r\n..two\r\n", []string{"A: 1"}, []string{".single", ".two"}, true},
		{"A: 1\n\nlf only\n.\n", []string{"A: 1"}, []string{"lf only"}, false},
		{"A: 1\r\n\r\nend line\r\n.\r\n", []string{"A: 1"}, []string{"end line"}, false},
		{"A: 1\r\n\r\nno end", []string{"A: 1"}, []string{"no end"}, false},
		{"A: 1\r\n\r\ntrailing blank\r\n\r\n", []string{"A: 1"}, []string{"trailing blank", ""}, false},
		{"A: 1\r\nB: 2\r\n", []string{"A: 1", "B: 2"}, nil, false},
	} {
		head, body, odd := Split([]byte(tc.in))
		if !reflect.DeepEqual(head, tc.head) || !reflect.DeepEqual(body, tc.body) || odd != tc.odd {
			t.Errorf("%q:\n%q %q %t\nwant %q %q %t", tc.in, head, body, odd, tc.head, tc.body, tc.odd)
		}
	}
}

// spool is a two-group spool as path: content, the way NewsCache lays it
// out: articles 2 (.art2) and 3 (inside .db) of x.y, article 5 listed but
// not in the backup, and x.y.z with only an .art file.
func spool() map[string][]byte {
	return map[string][]byte{
		"x/y/.db":     newscachetest.DB(binary.LittleEndian, 8, 2, 6, dbRecs),
		"x/y/.art2":   []byte(big),
		"x/y/z/.art1": []byte(small),
		"x/.active":   []byte("other"),
	}
}

func wantStats() Stats {
	return Stats{Groups: 2, DBs: 1, InDB: 1, Files: 2, Listed: 2, Missing: 1, Other: 1}
}

func writeTar(t *testing.T, files map[string][]byte, order []string, gz bool) []byte {
	var buf bytes.Buffer
	var zw *gzip.Writer
	tw := tar.NewWriter(&buf)
	if gz {
		zw = gzip.NewWriter(&buf)
		tw = tar.NewWriter(zw)
	}
	tw.WriteHeader(&tar.Header{Name: "x/", Typeflag: tar.TypeDir, Mode: 0755})
	for _, name := range order {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(files[name]))}); err != nil {
			t.Fatal(err)
		}
		tw.Write(files[name])
	}
	tw.Close()
	if zw != nil {
		zw.Close()
	}
	return buf.Bytes()
}

type got struct {
	group string
	num   uint64
	inDB  bool
	subj  string
}

func collect(items *[]got) func(Item) error {
	return func(it Item) error {
		head, _, _ := Split(it.Text)
		subj := ""
		for _, l := range head {
			if s, ok := strings.CutPrefix(l, "Subject: "); ok {
				subj = s
			}
		}
		*items = append(*items, got{it.Group, it.Num, it.InDB, subj})
		return nil
	}
}

func TestReadTar(t *testing.T) {
	files := spool()
	for _, gz := range []bool{false, true} {
		// the .db after an .art file of its group, as tar may store them
		data := writeTar(t, files, []string{"x/y/.art2", "x/y/z/.art1", "x/.active", "x/y/.db"}, gz)
		var items []got
		st, err := ReadTar(bytes.NewReader(data), collect(&items))
		if err != nil {
			t.Fatal(err)
		}
		want := []got{{"x.y", 2, false, "big"}, {"x.y.z", 1, false, "small"}, {"x.y", 3, true, "small"}}
		if !reflect.DeepEqual(items, want) {
			t.Errorf("gz=%t: %+v\nwant %+v", gz, items, want)
		}
		if !reflect.DeepEqual(st, wantStats()) {
			t.Errorf("gz=%t: stats %+v\nwant %+v", gz, st, wantStats())
		}
	}

	stop := errors.New("stop")
	data := writeTar(t, files, []string{"x/y/.art2", "x/y/.db"}, false)
	if _, err := ReadTar(bytes.NewReader(data), func(Item) error { return stop }); err != stop {
		t.Errorf("fn's error: %v", err)
	}
	if _, err := ReadTar(bytes.NewReader(data[:700]), func(Item) error { return nil }); err == nil {
		t.Error("truncated tar: no error")
	}
}

func TestReadDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "x")
	for name, data := range spool() {
		p := filepath.Join(filepath.Dir(root), filepath.FromSlash(name))
		os.MkdirAll(filepath.Dir(p), 0755)
		if err := os.WriteFile(p, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	// a leading zero: read by its own name
	os.Rename(filepath.Join(root, "y", ".art2"), filepath.Join(root, "y", ".art02"))
	var items []got
	st, err := ReadDir(root, collect(&items))
	if err != nil {
		t.Fatal(err)
	}
	// by directory, then by number: the inline article 3 after .art2
	want := []got{{"x.y", 2, false, "big"}, {"x.y", 3, true, "small"}, {"x.y.z", 1, false, "small"}}
	if !reflect.DeepEqual(items, want) {
		t.Errorf("%+v\nwant %+v", items, want)
	}
	if !reflect.DeepEqual(st, wantStats()) {
		t.Errorf("stats %+v\nwant %+v", st, wantStats())
	}

	// a group directory as root
	items = nil
	if _, err := ReadDir(filepath.Join(root, "y", "z"), collect(&items)); err != nil || len(items) != 1 || items[0].group != "z" {
		t.Errorf("group root: %+v, %v", items, err)
	}
}

func TestBadDB(t *testing.T) {
	files := map[string][]byte{"g/.db": []byte("garbage"), "g/.art1": []byte(small)}
	var items []got
	st, err := ReadTar(bytes.NewReader(writeTar(t, files, []string{"g/.db", "g/.art1"}, false)), collect(&items))
	if err != nil || len(items) != 1 || st.BadDBs != 1 || len(st.Problems) != 1 || !strings.Contains(st.Problems[0], "g/.db") {
		t.Errorf("%+v %+v %v", items, st, err)
	}
}

func TestNewscachetestTypes(t *testing.T) {
	if newscachetest.Overview != Overview || newscachetest.Article != Article || newscachetest.BigArticle != BigArticle {
		t.Error("newscachetest's record types differ")
	}
}

// A gzipped tarball whose gzip trailer does not match is an error: tar
// stops before the trailer, so it was never checked (review #8).
func TestReadTarGzipChecksum(t *testing.T) {
	data := writeTar(t, spool(), []string{"x/y/.art2", "x/y/.db"}, true)
	if _, err := ReadTar(bytes.NewReader(data), func(Item) error { return nil }); err != nil {
		t.Fatalf("intact: %v", err)
	}
	bad := slices.Clone(data)
	bad[len(bad)-8] ^= 0xff // the CRC-32 in the trailer
	if _, err := ReadTar(bytes.NewReader(bad), func(Item) error { return nil }); err == nil {
		t.Error("corrupt CRC: no error")
	}
	short := data[:len(data)-4] // no ISIZE
	if _, err := ReadTar(bytes.NewReader(short), func(Item) error { return nil }); err == nil {
		t.Error("truncated trailer: no error")
	}
}
