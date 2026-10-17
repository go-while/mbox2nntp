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
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-while/mbox2nntp/internal/mboxout"
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

func main() {
	log.SetFlags(0)
	o := options{}
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&o.format, "format", "mbox", "`mbox` or dir")
	fs.StringVar(&o.out, "o", "", "output `PATH` (default: GROUP.mbox, GROUP.mbox.gz, or the directory GROUP)")
	fs.Var(mboxout.GzipFlag{Level: &o.gzip}, "gzip", "gzip the mbox, to GROUP.mbox.gz; -gzip=N sets the level, from 1 (fastest) to 9 (smallest), default 6")
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
	mboxout.RemoveTempsOnSignal()
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
	mboxout.AddTemp(tmp)
	defer mboxout.RemoveTemp(tmp)
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
		var m *mboxout.Writer
		if m, err = mboxout.Create(st.out, o.gzip); err == nil {
			w = mboxWriter{m}
		}
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

// mboxWriter is an mboxout.Writer as a writer.
type mboxWriter struct{ *mboxout.Writer }

func (m mboxWriter) write(i int, head, body []string) error { return m.Write(head, body) }
func (m mboxWriter) close() error                           { return m.Close() }
func (m mboxWriter) abort()                                 { m.Abort() }

// dirWriter writes one article per file, named 1, 2, ... like the article
// numbers of a news spool, into a new directory.
type dirWriter struct{ path, tmp string }

func newDirWriter(path string) (*dirWriter, error) {
	path = filepath.Clean(path)
	if err := mboxout.MustNotExist(path); err != nil {
		return nil, err
	}
	tmp, err := os.MkdirTemp(mboxout.TempName(path))
	if err != nil {
		return nil, err
	}
	mboxout.AddTemp(tmp)
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
		mboxout.RemoveTemp(d.tmp)
		return err
	}
	return mboxout.Commit(d.tmp, d.path)
}

func (d *dirWriter) abort() { mboxout.RemoveTemp(d.tmp) }

func writeLines(w io.StringWriter, lines []string) {
	for _, l := range lines {
		w.WriteString(l)
		w.WriteString("\n")
	}
}
