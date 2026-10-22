// Command ncexport writes the articles of a NewsCache spool, such as the
// tarballs of the textusenet-backup items on archive.org, out as one mbox
// file, plain or gzipped.
//
//	ncexport [flags] HIERARCHY.tar.gz
//	curl -sL URL | ncexport -o HIERARCHY.mbox.gz -gzip -
//	ncexport [flags] SPOOLDIR
//
// A tarball is read as a stream, in one pass, so it needs no space to be
// unpacked into and can come from a pipe.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/go-while/mbox2nntp/internal/mboxout"
	"github.com/go-while/mbox2nntp/internal/version"
	"github.com/go-while/mbox2nntp/newscache"
)

const usage = `Usage: %[1]s [flags] INPUT

INPUT is a tar archive of a NewsCache spool (.tar, .tar.gz or .tgz, or "-"
for standard input) or a spool directory. The articles go to NAME.mbox, with
-gzip to NAME.mbox.gz, NAME being INPUT without its extension; -o names the
output, and is needed for "-". An existing output is never replaced. The
output is written under a temporary name next to it, ".NAME.*.tmp", and
renamed when complete.

NewsCache keeps one directory per newsgroup with a .db file and .artN files;
an article is in its .artN file or, in an unmodified NewsCache, inside the
.db when it is 15 KB or less. Both are read. Articles come out as the server
sent them, with the CRLF line ends and the doubled leading dots undone. A
tarball is written in its own order, a directory group by group and by
article number.

A .db is read one record at a time, never whole; a .db over -max-db is
skipped. From a tarball, a .db over 1 MiB is copied to a temporary file in
-tmpdir while it is read, as its records are not in file order.

An article crossposted to several groups is in each group's directory. It
is written once, the first time its Message-ID is seen (-dedup=false writes
every copy).

The mbox is mboxrd: each article starts with "From nobody DATE", DATE taken
from its Date header, and body lines that start with "From " or ">From "
get one more ">", as do lines starting "#! rnews ", which mbox2nntp's
parser takes for the start of a message.

Flags:
`

type options struct {
	out     string
	gzip    int
	dedup   bool
	tmpdir  string
	missing string
}

// sizeFlag is a size in bytes, given as a number with an optional K, M, G
// or T (powers of 1024, also written KiB, MiB, ... or KB, MB, ...). 0 means
// no limit.
type sizeFlag struct{ p *int64 }

func (f sizeFlag) String() string {
	if f.p == nil {
		return ""
	}
	return formatSize(*f.p)
}

func (f sizeFlag) Set(s string) error {
	n, err := parseSize(s)
	if err != nil {
		return err
	}
	*f.p = n
	return nil
}

func parseSize(s string) (int64, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	t = strings.TrimSuffix(strings.TrimSuffix(t, "B"), "I")
	shift := 0
	if t != "" {
		if i := strings.IndexByte("KMGT", t[len(t)-1]); i >= 0 {
			shift = 10 * (i + 1)
			t = t[:len(t)-1]
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n < 0 || n > math.MaxInt64>>shift {
		return 0, fmt.Errorf("not a size: %q (such as 4G, 16GiB or 1048576)", s)
	}
	if n == 0 {
		return math.MaxInt64, nil
	}
	return n << shift, nil
}

func formatSize(n int64) string {
	if n == math.MaxInt64 {
		return "0"
	}
	for _, u := range []struct {
		s     string
		shift int
	}{{"T", 40}, {"G", 30}, {"M", 20}, {"K", 10}} {
		if n >= 1<<u.shift && n%(1<<u.shift) == 0 {
			return strconv.FormatInt(n>>u.shift, 10) + u.s
		}
	}
	return strconv.FormatInt(n, 10)
}

func main() {
	log.SetFlags(0)
	o := options{}
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&o.out, "o", "", "output `PATH` (default: NAME.mbox or NAME.mbox.gz)")
	fs.Var(mboxout.GzipFlag{Level: &o.gzip}, "gzip", "gzip the mbox, to NAME.mbox.gz; -gzip=N sets the level, from 1 (fastest) to 9 (smallest), default 6")
	fs.BoolVar(&o.dedup, "dedup", true, "write an article seen in several groups once, by Message-ID")
	fs.Var(sizeFlag{&newscache.MaxDBSize}, "max-db", "skip a .db over `SIZE`, such as 16G; 0 for no limit. 4G is the most a 32-bit NewsCache can write; one from a 64-bit build can be bigger")
	fs.StringVar(&o.missing, "missing", "", "write the articles a .db lists that are in neither the spool nor the export to `FILE`, tab-separated: group, number, date, subject, from, Message-ID. One crossposted to another group is found there, by Message-ID, and not written")
	fs.StringVar(&o.tmpdir, "tmpdir", "", "`DIR` for temporary files: the copy of a .db over 1 MiB from a tarball, and the listed articles for -missing (default: the output's directory)")
	showVersion := fs.Bool("version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), usage, filepath.Base(os.Args[0]))
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])
	if *showVersion {
		fmt.Println(version.String("ncexport"))
		return
	}
	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(2)
	}
	if fs.Arg(0) == "-" && o.out == "" {
		log.Print(`-o is needed for "-"`)
		os.Exit(2)
	}
	mboxout.RemoveTempsOnSignal()
	st, err := export(fs.Arg(0), os.Stdin, o)
	for _, p := range st.Problems {
		log.Printf("WARNING: %s", p)
	}
	if err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
	log.Print(st.summary(fs.Arg(0)))
}

type stats struct {
	newscache.Stats
	out                    string
	n, dups, noID, oddDots int
	missingFile            string // -missing
	listedFound, lost      int    // of Missing: exported from another group, or not
}

func (st stats) missingWhat() string {
	if st.missingFile == "" {
		return "listed in .db without an .art file (mostly crossposts stored under another group; -missing FILE lists the others)"
	}
	return fmt.Sprintf("listed in .db without an .art file: %d of them exported from another group, %d lost, listed in %s", st.listedFound, st.lost, st.missingFile)
}

func (st stats) summary(input string) string {
	s := fmt.Sprintf("%s: %d articles to %s, from %d groups: %d .art files, %d inside .db files",
		input, st.n, st.out, st.Groups, st.Files, st.InDB)
	for _, c := range []struct {
		n    int
		what string
	}{
		{st.dups, "crossposted copies skipped"},
		{st.noID, "without Message-ID"},
		{st.oddDots, `with a line starting with a single "." (kept)`},
		{st.Missing, st.missingWhat()},
		{st.BadDBs, ".db files unreadable"},
		{st.TooBig, "articles over 1 MiB skipped"},
		{st.BigDBs, ".db files over -max-db (" + formatSize(newscache.MaxDBSize) + ") skipped: their articles are missing, raise -max-db"},
		{st.Other, "other files skipped"},
	} {
		if c.n != 0 {
			s += fmt.Sprintf("; %d %s", c.n, c.what)
		}
	}
	return s
}

// outName is INPUT without its directory and extension, plus .mbox or
// .mbox.gz.
func outName(input string, gz bool) string {
	name := filepath.Base(filepath.Clean(input))
	for _, ext := range []string{".tar.gz", ".tgz", ".tar"} {
		if strings.HasSuffix(name, ext) {
			name = strings.TrimSuffix(name, ext)
			break
		}
	}
	name += ".mbox"
	if gz {
		name += ".gz"
	}
	return name
}

// export reads input, or stdin for "-", and writes the mbox.
func export(input string, stdin io.Reader, o options) (st stats, err error) {
	st.out = o.out
	if st.out == "" {
		st.out = outName(input, o.gzip != 0)
	}
	read := func(fn func(newscache.Item) error) (newscache.Stats, error) {
		return newscache.ReadTar(stdin, fn)
	}
	if input != "-" {
		fi, err := os.Stat(input)
		if err != nil {
			return st, err
		}
		if fi.IsDir() {
			read = func(fn func(newscache.Item) error) (newscache.Stats, error) {
				return newscache.ReadDir(input, fn)
			}
		} else {
			f, err := os.Open(input)
			if err != nil {
				return st, err
			}
			defer f.Close()
			read = func(fn func(newscache.Item) error) (newscache.Stats, error) {
				return newscache.ReadTar(f, fn)
			}
		}
	}

	newscache.TempDir = o.tmpdir
	if newscache.TempDir == "" {
		newscache.TempDir = filepath.Dir(st.out)
	} else if err := writableDir(o.tmpdir); err != nil {
		// now, not at the first big .db, minutes into the run
		return st, fmt.Errorf("-tmpdir: %w", err)
	}
	seen := map[string]bool{}
	var mf *missingFile
	if o.missing != "" {
		if samePath(o.missing, st.out) {
			return st, fmt.Errorf("-missing %s is the output", o.missing)
		}
		if mf, err = createMissing(o.missing); err != nil {
			return st, err
		}
		st.missingFile = o.missing
		// called after the last article: seen holds every Message-ID written
		newscache.Missing = func(l newscache.Listed) error {
			if len(l.Overview) > 4 && seen[strings.TrimSpace(l.Overview[4])] {
				st.listedFound++
				return nil
			}
			st.lost++
			return mf.write(l)
		}
		defer func() { newscache.Missing = nil }()
	}
	w, err := mboxout.Create(st.out, o.gzip)
	if err != nil {
		mf.abort()
		return st, err
	}
	st.Stats, err = read(func(it newscache.Item) error {
		head, body, odd := newscache.Split(it.Text)
		if odd {
			st.oddDots++
		}
		switch id := messageID(head); {
		case id == "":
			st.noID++
		case o.dedup || mf != nil:
			// -missing looks crossposts up in seen too
			if !firstSeen(seen, id) && o.dedup {
				st.dups++
				return nil
			}
		}
		st.n++
		return w.Write(head, body)
	})
	if err != nil {
		w.Abort()
		mf.abort()
		return st, err
	}
	// the mbox first: a missing list left without its mbox would make the
	// rerun refuse to start
	if err := w.Close(); err != nil {
		mf.abort()
		return st, err
	}
	return st, mf.close()
}

// writableDir checks that a file can be made in dir.
func writableDir(dir string) error {
	f, err := os.CreateTemp(dir, ".ncexport-check-*")
	if err != nil {
		return err
	}
	f.Close()
	return os.Remove(f.Name())
}

// samePath reports whether a and b name the same file, as far as their
// absolute paths tell; ignoring case on Windows and macOS, whose file
// systems mostly do.
func samePath(a, b string) bool {
	aa, err1 := filepath.Abs(a)
	bb, err2 := filepath.Abs(b)
	if err1 != nil || err2 != nil {
		return false
	}
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		return strings.EqualFold(aa, bb)
	}
	return aa == bb
}

// missingFile is the file -missing writes. As the mbox, it is written
// under a temporary name, removed on a signal, and renamed when complete;
// an existing one is not replaced.
type missingFile struct {
	path string
	f    *os.File
	w    *bufio.Writer
}

func createMissing(path string) (*missingFile, error) {
	path = filepath.Clean(path)
	if err := mboxout.MustNotExist(path); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(mboxout.TempName(path))
	if err != nil {
		return nil, err
	}
	mboxout.AddTempFile(f)
	m := &missingFile{path, f, bufio.NewWriter(f)}
	m.w.WriteString("group\tnumber\tdate\tsubject\tfrom\tmessage-id\n")
	return m, nil
}

func (m *missingFile) write(l newscache.Listed) error {
	field := func(i int) string {
		if i < len(l.Overview) {
			return oneField(l.Overview[i])
		}
		return ""
	}
	_, err := fmt.Fprintf(m.w, "%s\t%d\t%s\t%s\t%s\t%s\n", oneField(l.Group), l.Num, field(3), field(1), field(2), field(4))
	return err
}

// oneField turns tabs and line ends into spaces. An overview field holds
// none; a broken one, or a crafted group name, might.
func oneField(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\t' || r == '\n' || r == '\r' {
			return ' '
		}
		return r
	}, s)
}

func (m *missingFile) close() error {
	if m == nil {
		return nil
	}
	err := m.w.Flush()
	if cerr := m.f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(m.f.Name(), 0644) // os.CreateTemp makes it 0600
	}
	if err != nil {
		mboxout.RemoveTemp(m.f.Name())
		return err
	}
	return mboxout.Commit(m.f.Name(), m.path)
}

// abort removes the unfinished file after a failed export.
func (m *missingFile) abort() {
	if m != nil {
		m.f.Close()
		mboxout.RemoveTemp(m.f.Name())
	}
}

// messageID is the Message-ID header's value, unfolded, or "".
// firstSeen reports whether id is not in seen yet, and adds it. It stores
// a copy: id is a slice of the whole article's text, which as a map key
// stayed in memory to the end, every article of the export.
func firstSeen(seen map[string]bool, id string) bool {
	if seen[id] {
		return false
	}
	seen[strings.Clone(id)] = true
	return true
}

func messageID(head []string) string {
	for i, l := range head {
		if len(l) >= 11 && strings.EqualFold(l[:11], "message-id:") {
			v := l[11:]
			for _, c := range head[i+1:] {
				if c == "" || (c[0] != ' ' && c[0] != '\t') {
					break
				}
				v += c
			}
			return strings.TrimSpace(v)
		}
	}
	return ""
}
