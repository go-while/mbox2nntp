// Command uatexport writes the articles of a UAT archive (Usenet Archive
// Toolkit, https://github.com/wolfpld/usenetarchive) out as an mbox file,
// plain or gzipped, or as a directory with one article per file.
//
//	uatexport [flags] GROUP.usenet
//	uatexport [flags] GROUP.usenet.xz
//	uatexport [flags] GROUP.usenet.7z
//
// Articles go through uat.Fix, as mbox2nntp sends them: a declared UTF-8
// body and RFC 2047 encoded header text. -raw writes the messages exactly
// as the toolkit stores them instead. It needs the zstd program, and xz or
// 7-Zip for the compressed forms.
package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/mail"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-while/mbox2nntp/internal/unpack"
	"github.com/go-while/mbox2nntp/internal/version"
	"github.com/go-while/mbox2nntp/uat"
)

const usage = `Usage: %[1]s [flags] GROUP.usenet[.xz|.7z]

Writes the articles of a UAT archive to GROUP.mbox, with -gzip to
GROUP.mbox.gz, or with -format dir to GROUP/1, GROUP/2, ... (one article
per file, in the archive's order). An existing output is never replaced.
The output is written under a temporary name next to it, ".NAME.*.tmp",
and renamed when complete.

The mbox is mboxrd: each article starts with "From nobody DATE", DATE taken
from its Date header, and body lines that start with "From " or ">From "
get one more ">". To feed an archive to a server, give the .usenet file to
mbox2nntp itself: it does not un-escape ">From " lines.

Flags:
`

type options struct {
	format, out, tmpdir string
	raw                 bool
	gzip                int // the gzip level of the mbox, 1 to 9; 0 for none
}

// gzipFlag is -gzip (level 6, gzip's default), -gzip=LEVEL or -gzip=false.
type gzipFlag struct{ level *int }

func (g gzipFlag) IsBoolFlag() bool { return true }

func (g gzipFlag) String() string {
	if g.level == nil || *g.level == 0 {
		return "false"
	}
	return strconv.Itoa(*g.level)
}

func (g gzipFlag) Set(s string) error {
	switch s {
	case "true":
		*g.level = 6
	case "false":
		*g.level = 0
	default:
		n, err := strconv.Atoi(s)
		if err != nil || n < gzip.BestSpeed || n > gzip.BestCompression {
			return errors.New("a level from 1 to 9, true or false")
		}
		*g.level = n
	}
	return nil
}

func main() {
	log.SetFlags(0)
	o := options{}
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&o.format, "format", "mbox", "`mbox` or dir")
	fs.StringVar(&o.out, "o", "", "output `PATH` (default: GROUP.mbox, GROUP.mbox.gz, or the directory GROUP)")
	fs.Var(gzipFlag{&o.gzip}, "gzip", "gzip the mbox, to GROUP.mbox.gz; -gzip=N sets the level, from 1 (fastest) to 9 (smallest), default 6")
	fs.BoolVar(&o.raw, "raw", false, "write the messages as the archive stores them: raw UTF-8 headers, no Content-Type")
	fs.StringVar(&o.tmpdir, "tmpdir", "", "`DIR` to unpack .xz and .7z inputs into (default: the system's)")
	fs.StringVar(&unpack.Zstd, "zstd", "", "the zstd program at `PATH` (default: zstd in PATH)")
	fs.StringVar(&unpack.Xz, "xz", "", "the xz program at `PATH` for .usenet.xz (default: xz in PATH, else 7-Zip)")
	fs.StringVar(&unpack.SevenZip, "7z", "", "the 7-Zip program at `PATH` for .usenet.7z (default: 7z, 7zz or 7za in PATH)")
	showVersion := fs.Bool("version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), usage, filepath.Base(os.Args[0]))
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])
	if *showVersion {
		fmt.Println(version.String("uatexport"))
		return
	}
	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(2)
	}
	if o.format != "mbox" && o.format != "dir" {
		log.Printf("-format %q: mbox or dir", o.format)
		os.Exit(2)
	}
	if o.format == "dir" && o.gzip != 0 {
		log.Printf("-gzip is for -format mbox")
		os.Exit(2)
	}
	// deferred calls do not run when a signal ends the program
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sig
		removeTemps()
		log.Printf("%v: stopped, temporary files removed", s)
		os.Exit(1)
	}()
	st, err := export(fs.Arg(0), o)
	if err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
	log.Printf("%s: %d articles to %s, %d header fields re-encoded, %d left with non-ASCII", st.group, st.n, st.out, st.encoded, st.raw)
}

type stats struct {
	group, out      string
	n, encoded, raw int
}

func export(input string, o options) (st stats, err error) {
	zstd, err := unpack.FindZstd()
	if err != nil {
		return st, err
	}
	path := input
	ext := filepath.Ext(input)
	unpacked := ext == ".xz" || ext == ".7z"
	if unpacked && !strings.HasSuffix(strings.TrimSuffix(input, ext), ".usenet") {
		return st, fmt.Errorf("%s: not a .usenet%s file", input, ext)
	}
	// for the unpacked archive and the zstd dictionary, so that one
	// removal, on a signal too, takes all of them
	tmp, err := os.MkdirTemp(o.tmpdir, "uatexport-")
	if err != nil {
		return st, err
	}
	addTemp(tmp)
	defer removeTemp(tmp)
	if unpacked {
		path = filepath.Join(tmp, strings.TrimSuffix(filepath.Base(input), ext))
		fn := unpack.UnXzTo
		if ext == ".7z" {
			fn = unpack.Un7zTo
		}
		if err := fn(input, path); err != nil {
			return st, fmt.Errorf("unpacking %s: %v", input, err)
		}
	}
	p, err := uat.Open(path)
	if err != nil {
		return st, err
	}
	defer p.Close()
	st.group = p.Name
	if st.group == "" {
		st.group = strings.TrimSuffix(filepath.Base(path), ".usenet")
	}
	st.out = o.out
	if st.out == "" {
		st.out = st.group
		if o.format == "mbox" {
			st.out += ".mbox"
			if o.gzip != 0 {
				st.out += ".gz"
			}
		}
	}

	var w writer
	if o.format == "mbox" {
		w, err = newMboxWriter(st.out, o.gzip)
	} else {
		w, err = newDirWriter(st.out)
	}
	if err != nil {
		return st, err
	}
	err = p.Each(zstd, tmp, func(i int, msg []byte) error {
		var head, body []string
		if o.raw {
			h, b, _ := strings.Cut(string(msg), "\n\n")
			head, body = uat.Lines(h), uat.Lines(b)
		} else {
			var info uat.Info
			head, body, info = uat.Fix(msg)
			st.encoded += info.Encoded
			st.raw += len(info.Raw)
		}
		st.n++
		return w.write(i, head, body)
	})
	if err != nil {
		w.abort()
		return st, err
	}
	return st, w.close()
}

// A writer writes under a temporary name. close renames the output to its
// path, abort removes it.
type writer interface {
	write(i int, head, body []string) error
	close() error
	abort()
}

// Temporary files and directories, removed by removeTemps when a signal
// ends the program.
var (
	tempMu sync.Mutex
	temps  = map[string]bool{}
)

func addTemp(path string) {
	tempMu.Lock()
	defer tempMu.Unlock()
	temps[path] = true
}

// removeTemp removes path, a file or a directory tree.
func removeTemp(path string) {
	tempMu.Lock()
	defer tempMu.Unlock()
	delete(temps, path)
	os.RemoveAll(path)
}

// removeTemps removes all of them, and keeps the lock: the program is about
// to exit.
func removeTemps() {
	tempMu.Lock()
	for path := range temps {
		os.RemoveAll(path)
	}
}

// tempName is the pattern for os.CreateTemp and os.MkdirTemp: the output's
// name in its directory, hidden and with a suffix no reader takes for an
// mbox or an article.
func tempName(path string) (dir, pattern string) {
	return filepath.Dir(path), "." + filepath.Base(path) + ".*.tmp"
}

// mustNotExist fails when path exists, even as a dangling symlink.
func mustNotExist(path string) error {
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("%s exists", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// commit renames the finished tmp to path, unless path has appeared since
// the start.
func commit(tmp, path string) error {
	if err := mustNotExist(path); err != nil {
		removeTemp(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		removeTemp(tmp)
		return err
	}
	tempMu.Lock()
	defer tempMu.Unlock()
	delete(temps, tmp)
	return nil
}

// mboxWriter writes mboxrd to a new file, gzipped when gz is set.
type mboxWriter struct {
	path string
	f    *os.File
	gz   *gzip.Writer
	w    *bufio.Writer
}

func newMboxWriter(path string, level int) (*mboxWriter, error) {
	path = filepath.Clean(path)
	if err := mustNotExist(path); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(tempName(path))
	if err != nil {
		return nil, err
	}
	addTemp(f.Name())
	m := &mboxWriter{path: path, f: f}
	var w io.Writer = f
	if level != 0 {
		if m.gz, err = gzip.NewWriterLevel(f, level); err != nil {
			m.abort()
			return nil, err
		}
		// what gunzip -N restores
		m.gz.Name = strings.TrimSuffix(filepath.Base(path), ".gz")
		w = m.gz
	}
	m.w = bufio.NewWriterSize(w, 1<<20)
	return m, nil
}

func (m *mboxWriter) write(i int, head, body []string) error {
	fmt.Fprintf(m.w, "From nobody %s\n", fromDate(head))
	for _, l := range head {
		m.w.WriteString(l)
		m.w.WriteByte('\n')
	}
	m.w.WriteByte('\n')
	for _, l := range body {
		if isFromLine(l) {
			m.w.WriteByte('>')
		}
		m.w.WriteString(l)
		m.w.WriteByte('\n')
	}
	_, err := m.w.WriteString("\n") // the blank line before the next "From "
	return err
}

func (m *mboxWriter) close() error {
	err := m.w.Flush()
	if m.gz != nil && err == nil {
		err = m.gz.Close() // writes the gzip trailer
	}
	if cerr := m.f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(m.f.Name(), 0644) // os.CreateTemp makes it 0600
	}
	if err != nil {
		removeTemp(m.f.Name())
		return err
	}
	return commit(m.f.Name(), m.path)
}

func (m *mboxWriter) abort() {
	m.f.Close()
	removeTemp(m.f.Name())
}

// isFromLine reports a line mboxrd quotes: "From " after any number of ">".
func isFromLine(l string) bool {
	return strings.HasPrefix(strings.TrimLeft(l, ">"), "From ")
}

// fromDate is the date on the "From " line: the article's Date in UTC, in
// the asctime form the mbox parser in go-lib-mbox2nntp knows
// ("Thu Dec  8 12:55:04 2016"), or the epoch when Date is missing or
// does not parse.
func fromDate(head []string) string {
	t := time.Unix(0, 0)
	for i, l := range head {
		if len(l) > 5 && strings.EqualFold(l[:5], "date:") {
			v := l[5:]
			for _, c := range head[i+1:] {
				if c == "" || (c[0] != ' ' && c[0] != '\t') {
					break
				}
				v += c
			}
			if d, ok := parseDate(strings.TrimSpace(v)); ok {
				t = d
			}
			break
		}
	}
	return t.UTC().Format("Mon Jan _2 15:04:05 2006")
}

// Date shapes from early Usenet that net/mail does not take.
var oldDates = []string{
	"Mon, _2-Jan-06 15:04:05 MST", // "Mon, 12-Jan-87 19:53:54 EST"
	"Monday, _2 Jan 2006 15:04:05 MST",
	"_2 Jan 06 15:04:05",
	"2006/01/02",
}

func parseDate(v string) (time.Time, bool) {
	if d, err := mail.ParseDate(v); err == nil {
		return d, true
	}
	for _, layout := range oldDates {
		// in UTC, so that "EST" means the same on every machine: a zone
		// abbreviation the location does not know gets offset 0
		if d, err := time.ParseInLocation(layout, v, time.UTC); err == nil {
			return d, true
		}
	}
	return time.Time{}, false
}

// dirWriter writes one article per file, named 1, 2, ... like the article
// numbers of a news spool, into a new directory.
type dirWriter struct{ path, tmp string }

func newDirWriter(path string) (*dirWriter, error) {
	path = filepath.Clean(path)
	if err := mustNotExist(path); err != nil {
		return nil, err
	}
	tmp, err := os.MkdirTemp(tempName(path))
	if err != nil {
		return nil, err
	}
	addTemp(tmp)
	return &dirWriter{path, tmp}, nil
}

func (d *dirWriter) write(i int, head, body []string) error {
	f, err := os.Create(filepath.Join(d.tmp, strconv.Itoa(i+1)))
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	writeLines(w, head)
	w.WriteByte('\n')
	writeLines(w, body)
	err = w.Flush()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (d *dirWriter) close() error {
	if err := os.Chmod(d.tmp, 0755); err != nil { // os.MkdirTemp makes it 0700
		removeTemp(d.tmp)
		return err
	}
	return commit(d.tmp, d.path)
}

func (d *dirWriter) abort() { removeTemp(d.tmp) }

func writeLines(w io.StringWriter, lines []string) {
	for _, l := range lines {
		w.WriteString(l)
		w.WriteString("\n")
	}
}
