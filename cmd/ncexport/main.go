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
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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

An article crossposted to several groups is in each group's directory. It
is written once, the first time its Message-ID is seen (-dedup=false writes
every copy).

The mbox is mboxrd: each article starts with "From nobody DATE", DATE taken
from its Date header, and body lines that start with "From " or ">From "
get one more ">".

Flags:
`

type options struct {
	out   string
	gzip  int
	dedup bool
}

func main() {
	log.SetFlags(0)
	o := options{}
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&o.out, "o", "", "output `PATH` (default: NAME.mbox or NAME.mbox.gz)")
	fs.Var(mboxout.GzipFlag{Level: &o.gzip}, "gzip", "gzip the mbox, to NAME.mbox.gz; -gzip=N sets the level, from 1 (fastest) to 9 (smallest), default 6")
	fs.BoolVar(&o.dedup, "dedup", true, "write an article seen in several groups once, by Message-ID")
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
		{st.Missing, "listed in .db but not in the spool"},
		{st.BadDBs, ".db files unreadable"},
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

	w, err := mboxout.Create(st.out, o.gzip)
	if err != nil {
		return st, err
	}
	seen := map[string]bool{}
	st.Stats, err = read(func(it newscache.Item) error {
		head, body, odd := newscache.Split(it.Text)
		if odd {
			st.oddDots++
		}
		if id := messageID(head); id == "" {
			st.noID++
		} else if o.dedup {
			if seen[id] {
				st.dups++
				return nil
			}
			seen[id] = true
		}
		st.n++
		return w.Write(head, body)
	})
	if err != nil {
		w.Abort()
		return st, err
	}
	return st, w.Close()
}

// messageID is the Message-ID header's value, unfolded, or "".
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
