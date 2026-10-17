// Command uatexport writes the articles of a UAT archive (Usenet Archive
// Toolkit, https://github.com/wolfpld/usenetarchive) out as an mbox file
// or as a directory with one article per file.
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
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/mail"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-while/mbox2nntp/internal/unpack"
	"github.com/go-while/mbox2nntp/uat"
)

const usage = `Usage: %[1]s [flags] GROUP.usenet[.xz|.7z]

Writes the articles of a UAT archive to GROUP.mbox, or with -format dir to
GROUP/1, GROUP/2, ... (one article per file, in the archive's order).

The mbox is mboxrd: each article starts with "From nobody DATE", DATE taken
from its Date header, and body lines that start with "From " or ">From "
get one more ">". To feed an archive to a server, give the .usenet file to
mbox2nntp itself: it does not un-escape ">From " lines.

Flags:
`

type options struct {
	format, out, tmpdir string
	raw                 bool
}

func main() {
	log.SetFlags(0)
	o := options{}
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&o.format, "format", "mbox", "`mbox` or dir")
	fs.StringVar(&o.out, "o", "", "output `PATH` (default: GROUP.mbox, or the directory GROUP)")
	fs.BoolVar(&o.raw, "raw", false, "write the messages as the archive stores them: raw UTF-8 headers, no Content-Type")
	fs.StringVar(&o.tmpdir, "tmpdir", "", "`DIR` to unpack .xz and .7z inputs into (default: the system's)")
	fs.StringVar(&unpack.Zstd, "zstd", "", "the zstd program at `PATH` (default: zstd in PATH)")
	fs.StringVar(&unpack.Xz, "xz", "", "the xz program at `PATH` for .usenet.xz (default: xz in PATH, else 7-Zip)")
	fs.StringVar(&unpack.SevenZip, "7z", "", "the 7-Zip program at `PATH` for .usenet.7z (default: 7z, 7zz or 7za in PATH)")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), usage, filepath.Base(os.Args[0]))
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])
	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(2)
	}
	if o.format != "mbox" && o.format != "dir" {
		log.Printf("-format %q: mbox or dir", o.format)
		os.Exit(2)
	}
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
	if ext := filepath.Ext(input); ext == ".xz" || ext == ".7z" {
		if !strings.HasSuffix(strings.TrimSuffix(input, ext), ".usenet") {
			return st, fmt.Errorf("%s: not a .usenet%s file", input, ext)
		}
		dir, err := os.MkdirTemp(o.tmpdir, "uatexport-")
		if err != nil {
			return st, err
		}
		defer os.RemoveAll(dir)
		path = filepath.Join(dir, strings.TrimSuffix(filepath.Base(input), ext))
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
		}
	}

	var w writer
	if o.format == "mbox" {
		w, err = newMboxWriter(st.out)
	} else {
		w, err = newDirWriter(st.out)
	}
	if err != nil {
		return st, err
	}
	err = p.Each(zstd, o.tmpdir, func(i int, msg []byte) error {
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
	if cerr := w.close(); err == nil {
		err = cerr
	}
	return st, err
}

type writer interface {
	write(i int, head, body []string) error
	close() error
}

// mboxWriter writes mboxrd to a new file.
type mboxWriter struct {
	f *os.File
	w *bufio.Writer
}

func newMboxWriter(path string) (*mboxWriter, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	return &mboxWriter{f: f, w: bufio.NewWriterSize(f, 1<<20)}, nil
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
	if cerr := m.f.Close(); err == nil {
		err = cerr
	}
	return err
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
type dirWriter struct{ dir string }

func newDirWriter(dir string) (*dirWriter, error) {
	if err := os.Mkdir(dir, 0755); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("%s exists", dir)
		}
		return nil, err
	}
	return &dirWriter{dir}, nil
}

func (d *dirWriter) write(i int, head, body []string) error {
	f, err := os.Create(filepath.Join(d.dir, strconv.Itoa(i+1)))
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

func (d *dirWriter) close() error { return nil }

func writeLines(w io.StringWriter, lines []string) {
	for _, l := range lines {
		w.WriteString(l)
		w.WriteString("\n")
	}
}
