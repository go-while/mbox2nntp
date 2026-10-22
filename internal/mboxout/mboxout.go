// Package mboxout writes mboxrd files, plain or gzipped, for the export
// commands. An output is written under a temporary name next to it,
// ".NAME.*.tmp", and renamed when complete, so a reader never sees half an
// mbox; an existing output is never replaced.
package mboxout

import (
	"bufio"
	"compress/gzip"
	"errors"
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
)

// GzipFlag is -gzip (level 6, gzip's default), -gzip=LEVEL or -gzip=false.
type GzipFlag struct{ Level *int }

func (g GzipFlag) IsBoolFlag() bool { return true }

func (g GzipFlag) String() string {
	if g.Level == nil || *g.Level == 0 {
		return "false"
	}
	return strconv.Itoa(*g.Level)
}

func (g GzipFlag) Set(s string) error {
	switch s {
	case "true":
		*g.Level = 6
	case "false":
		*g.Level = 0
	default:
		n, err := strconv.Atoi(s)
		if err != nil || n < gzip.BestSpeed || n > gzip.BestCompression {
			return errors.New("a level from 1 to 9, true or false")
		}
		*g.Level = n
	}
	return nil
}

// Temporary files and directories, removed by RemoveTemps when a signal
// ends the program; an open file is closed first.
var (
	tempMu sync.Mutex
	temps  = map[string]*os.File{}
)

// AddTemp registers path for removal on a signal.
func AddTemp(path string) {
	tempMu.Lock()
	defer tempMu.Unlock()
	temps[path] = nil
}

// AddTempFile registers an open file for removal on a signal. It is closed
// first: Windows does not remove an open file.
func AddTempFile(f *os.File) {
	tempMu.Lock()
	defer tempMu.Unlock()
	temps[f.Name()] = f
}

// RemoveTemp removes path, a file or a directory tree.
func RemoveTemp(path string) {
	tempMu.Lock()
	defer tempMu.Unlock()
	delete(temps, path)
	os.RemoveAll(path)
}

// RemoveTemps removes all of them, and keeps the lock: the program is about
// to exit.
func RemoveTemps() {
	tempMu.Lock()
	for path, f := range temps {
		if f != nil {
			f.Close()
		}
		os.RemoveAll(path)
	}
}

// RemoveTempsOnSignal makes SIGINT and SIGTERM remove the temporary files
// and exit 1: deferred calls do not run when a signal ends the program.
func RemoveTempsOnSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sig
		RemoveTemps()
		log.Printf("%v: stopped, temporary files removed", s)
		os.Exit(1)
	}()
}

// TempName is the pattern for os.CreateTemp and os.MkdirTemp: the output's
// name in its directory, hidden and with a suffix no reader takes for an
// mbox or an article.
func TempName(path string) (dir, pattern string) {
	return filepath.Dir(path), "." + filepath.Base(path) + ".*.tmp"
}

// MustNotExist fails when path exists, even as a dangling symlink.
func MustNotExist(path string) error {
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("%s exists", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// Commit renames the finished tmp to path, unless path has appeared since
// the start.
func Commit(tmp, path string) error {
	if err := MustNotExist(path); err != nil {
		RemoveTemp(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		RemoveTemp(tmp)
		return err
	}
	tempMu.Lock()
	defer tempMu.Unlock()
	delete(temps, tmp)
	return nil
}

// Writer writes mboxrd to a new file, gzipped when the level is not 0.
type Writer struct {
	path      string
	f         *os.File
	gz        *gzip.Writer
	w         *bufio.Writer
	appending bool // Append: no temporary name, nothing to commit
}

// Append adds to the mbox at path, creating it when missing, without a
// temporary name: for files inside a directory that is itself temporary
// until complete. Gzipped, each Append adds a gzip member, which gzip
// readers (Go's, zcat) read on as one stream.
func Append(path string, level int) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	AddTempFile(f) // closed before its directory is removed on a signal
	m := &Writer{path: path, f: f, appending: true}
	var w io.Writer = f
	if level != 0 {
		if m.gz, err = gzip.NewWriterLevel(f, level); err != nil {
			m.Abort()
			return nil, err
		}
		m.gz.Name = strings.TrimSuffix(filepath.Base(path), ".gz")
		w = m.gz
	}
	m.w = bufio.NewWriterSize(w, 1<<20)
	return m, nil
}

// Create starts the mbox at path, under a temporary name until Close.
func Create(path string, level int) (*Writer, error) {
	path = filepath.Clean(path)
	if err := MustNotExist(path); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(TempName(path))
	if err != nil {
		return nil, err
	}
	AddTempFile(f)
	m := &Writer{path: path, f: f}
	var w io.Writer = f
	if level != 0 {
		if m.gz, err = gzip.NewWriterLevel(f, level); err != nil {
			m.Abort()
			return nil, err
		}
		// what gunzip -N restores
		m.gz.Name = strings.TrimSuffix(filepath.Base(path), ".gz")
		w = m.gz
	}
	m.w = bufio.NewWriterSize(w, 1<<20)
	return m, nil
}

// FormatField ends the header of every article Writer writes. It tells
// mbox2nntp that the body is quoted (see NeedsQuote), so that it unquotes
// it, and drops the field before sending. Old archive mboxes are not
// quoted: in them a ">From " line is part of the article as posted.
const FormatField = "X-Mbox2nntp-Format: mboxrd"

// IsQuoted reports whether head holds FormatField.
func IsQuoted(head []string) bool {
	for _, l := range head {
		if name, value, ok := strings.Cut(l, ":"); ok &&
			strings.EqualFold(strings.TrimSpace(name), "X-Mbox2nntp-Format") &&
			strings.EqualFold(strings.TrimSpace(value), "mboxrd") {
			return true
		}
	}
	return false
}

// Write adds one article: a "From nobody DATE" line, the header lines and
// FormatField, a blank line, the body lines with NeedsQuote lines quoted,
// and a blank line.
func (m *Writer) Write(head, body []string) error {
	fmt.Fprintf(m.w, "From nobody %s\n", FromDate(head))
	for _, l := range head {
		m.w.WriteString(l)
		m.w.WriteByte('\n')
	}
	m.w.WriteString(FormatField + "\n\n")
	for _, l := range body {
		if NeedsQuote(l) {
			m.w.WriteByte('>')
		}
		m.w.WriteString(l)
		m.w.WriteByte('\n')
	}
	_, err := m.w.WriteString("\n") // the blank line before the next "From "
	return err
}

// Close finishes the file and renames it to its path.
func (m *Writer) Close() error {
	err := m.w.Flush()
	if m.gz != nil && err == nil {
		err = m.gz.Close() // writes the gzip trailer
	}
	if cerr := m.f.Close(); err == nil {
		err = cerr
	}
	if m.appending {
		tempMu.Lock()
		delete(temps, m.f.Name())
		tempMu.Unlock()
		return err
	}
	if err == nil {
		err = os.Chmod(m.f.Name(), 0644) // os.CreateTemp makes it 0600
	}
	if err != nil {
		RemoveTemp(m.f.Name())
		return err
	}
	return Commit(m.f.Name(), m.path)
}

// Abort removes the unfinished file; an Append file is only closed, as it
// goes with its directory.
func (m *Writer) Abort() {
	m.f.Close()
	if m.appending {
		tempMu.Lock()
		delete(temps, m.f.Name())
		tempMu.Unlock()
		return
	}
	RemoveTemp(m.f.Name())
}

// NeedsQuote reports a body line that gets one more ">": the lines mboxrd
// quotes, "From " after any number of ">", and lines starting "#! rnews ".
// go-lib-mbox2nntp, mbox2nntp's mbox parser, also takes "#! rnews N" for
// the start of a message (rnews batches), so such a line in a body would
// split the article there; ">#! rnews N" is body text to it.
//
// "#! rnews " is quoted after any number of ">" too, so that Unquote can
// undo every quote: ">#! rnews N" used to be written as it was, and
// would have lost its ">".
func NeedsQuote(l string) bool {
	t := strings.TrimLeft(l, ">")
	return strings.HasPrefix(t, "From ") || strings.HasPrefix(t, "#! rnews ")
}

// Unquote undoes the quote Write adds to a body line.
func Unquote(l string) string {
	if strings.HasPrefix(l, ">") && NeedsQuote(l[1:]) {
		return l[1:]
	}
	return l
}

// FromDate is the date on the "From " line: the article's Date in UTC, in
// the asctime form the mbox parser in go-lib-mbox2nntp knows
// ("Thu Dec  8 12:55:04 2016"), or the epoch when Date is missing or
// does not parse.
func FromDate(head []string) string {
	t := time.Unix(0, 0)
	for i, l := range head {
		if len(l) >= 5 && strings.EqualFold(l[:5], "date:") {
			v := l[5:]
			for _, c := range head[i+1:] {
				if c == "" || (c[0] != ' ' && c[0] != '\t') {
					break
				}
				v += c
			}
			// unfolded, with each run of spaces and tabs one space
			if d, ok := ParseDate(strings.Join(strings.Fields(v), " ")); ok {
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

// ParseDate reads a Date header value.
func ParseDate(v string) (time.Time, bool) {
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
