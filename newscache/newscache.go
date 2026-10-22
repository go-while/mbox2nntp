// Package newscache reads the news spool of NewsCache
// (https://github.com/hstraub/NewsCache), the format of the
// textusenet-backup items on archive.org: one directory per newsgroup
// (babbel-net/mailboxwerbung for babbel-net.mailboxwerbung) holding a .db
// file and .artN files, N being the article number.
//
// The .db is an NVArray, a file NewsCache maps into memory: a header, the
// first and last article number, and per number the offset of a record. A
// record is a type byte and a NUL-terminated string:
//
//   - Overview (2): the overview line; the article is not cached
//   - Article (3): the whole article, as NewsCache stores those of 15 KB or less
//   - BigArticle (4): the overview line; the article is in .artN
//
// The textusenet backup ran a modified NewsCache: every article is in an
// .artN file and its record is of type Overview.
//
// NewsCache stores an article as it came from the server: CRLF line ends and
// lines starting with "." doubled, without the final "." line. Split undoes
// both. (Checked on the backup's yEnc articles: their data lines start with
// ".." where a dot was stuffed.)
package newscache

import (
	"archive/tar"
	"bufio"
	"bytes"
	"cmp"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Record types.
const (
	Overview   = 2
	Article    = 3
	BigArticle = 4
)

// A Record is one article number's entry in a .db.
type Record struct {
	Num  uint64
	Type byte
	Data []byte // the overview line or the article, without the final NUL
	// Size is the length of the record after its type byte, as stored.
	// Data is nil when that is over MaxArticleSize.
	Size int64
}

// A DB is a parsed .db file.
type DB struct {
	Mtime       time.Time
	First, Last uint64
	Records     []Record // in number order, numbers without a record left out
}

// ParseDB parses a .db held in memory. When a record points outside the
// file it returns the records read so far with the error. ReadDB reads one
// without holding it whole.
func ParseDB(b []byte) (*DB, error) {
	var recs []Record
	db, err := ReadDB(bytes.NewReader(b), int64(len(b)), func(r Record) error {
		recs = append(recs, r)
		return nil
	})
	if db != nil {
		db.Records = recs
	}
	return db, err
}

// ReadDB reads a .db of size bytes from r and calls fn for each record in
// number order; the DB it returns has no Records. It takes the layouts of
// 32-bit and 64-bit builds in either byte order.
//
// The file is never held whole. NewsCache allocates the records and the
// table of their offsets anywhere in the file, first fit, and moves the
// table when the article range grows, so the file is not in any order.
// ReadDB reads the table through a buffered reader and each record, one at
// a time, where the table points. When a record points outside the file it
// returns the error after the records before it went to fn. An error from
// fn stops the reading and is returned as it is.
func ReadDB(r io.ReaderAt, size int64, fn func(Record) error) (*DB, error) {
	var hdr [8 + 5*8]byte
	n, _ := r.ReadAt(hdr[:], 0)
	b := hdr[:n]
	var (
		bo binary.ByteOrder
		w  int // the size of an unsigned long in the build that wrote it
	)
	for _, o := range []binary.ByteOrder{binary.LittleEndian, binary.BigEndian} {
		if len(b) < 8 || o.Uint32(b[4:]) != 2 { // the format version
			continue
		}
		switch o.Uint32(b) { // the header's length
		case 8 + 5*8:
			bo, w = o, 8
		case 8 + 5*4:
			bo, w = o, 4
		}
	}
	if bo == nil {
		return nil, errors.New("not a NewsCache .db (NVcontainer version 2)")
	}
	fsize := uint64(size)
	get := func(p []byte) uint64 {
		if w == 8 {
			return bo.Uint64(p)
		}
		return uint64(bo.Uint32(p))
	}
	var wbuf [8]byte
	word := func(off uint64) (uint64, bool) {
		if off > fsize || fsize-off < uint64(w) {
			return 0, false
		}
		if _, err := r.ReadAt(wbuf[:w], int64(off)); err != nil {
			return 0, false
		}
		return get(wbuf[:w]), true
	}
	db := &DB{}
	mtime, ok1 := word(8 + uint64(w))
	userdata, ok2 := word(8 + 4*uint64(w))
	if !ok1 || !ok2 {
		return nil, errors.New("short header")
	}
	db.Mtime = time.Unix(int64(mtime), 0).UTC()
	if userdata == 0 {
		return db, nil // no article yet
	}
	first, ok1 := word(userdata)
	last, ok2 := word(userdata + uint64(w))
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("article range at %#x is outside the file", userdata)
	}
	if first > last {
		return db, nil
	}
	count := last - first + 1
	tab := userdata + 2*uint64(w)
	if count > fsize/uint64(w) || tab+count*uint64(w) > fsize {
		return nil, fmt.Errorf("article range %d-%d does not fit the file", first, last)
	}
	db.First, db.Last = first, last
	table := bufio.NewReaderSize(io.NewSectionReader(r, int64(tab), int64(count*uint64(w))), 1<<16)
	// most records are an overview line or a small article: one read gets
	// the size word and the record together
	buf := make([]byte, 1024)
	for i := range count {
		if _, err := io.ReadFull(table, wbuf[:w]); err != nil {
			return db, fmt.Errorf("offset table: %w", err)
		}
		off := get(wbuf[:w])
		if off == 0 {
			continue
		}
		bad := fmt.Errorf("record of article %d at %#x is outside the file", first+i, off)
		if off > fsize || fsize-off < uint64(w) {
			return db, bad
		}
		k, err := r.ReadAt(buf, int64(off))
		if err != nil && err != io.EOF {
			return db, err
		}
		if k < w {
			return db, bad
		}
		rsize := get(buf[:w])
		start := off + uint64(w)
		if rsize == 0 || rsize > fsize || start+rsize > fsize {
			return db, bad
		}
		rec := Record{Num: first + i, Size: int64(rsize) - 1}
		p := buf[w:k]
		if uint64(len(p)) >= rsize {
			p = bytes.Clone(p[:rsize])
		} else {
			// longer than buf: read it on its own, or only its type
			// byte when it is over the limit
			p = make([]byte, 1)
			if rec.Size <= MaxArticleSize {
				p = make([]byte, rsize)
			}
			if _, err := r.ReadAt(p, int64(start)); err != nil {
				return db, fmt.Errorf("record of article %d at %#x: %w", first+i, off, err)
			}
		}
		rec.Type = p[0]
		if rec.Size <= MaxArticleSize {
			rec.Data = p[1:]
		}
		if k := len(rec.Data); k > 0 && rec.Data[k-1] == 0 {
			rec.Data = rec.Data[:k-1]
		}
		if err := fn(rec); err != nil {
			return db, err
		}
	}
	return db, nil
}

// ArtNum reports the number in the file name ".artN".
func ArtNum(name string) (uint64, bool) {
	digits, ok := strings.CutPrefix(name, ".art")
	if !ok || digits == "" || digits[0] < '0' || digits[0] > '9' {
		return 0, false
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	return n, err == nil
}

// Split turns a stored article into header and body lines, without line
// ends and with the doubled leading dots made single again. A line that
// starts with a single "." cannot come from dot-stuffing; it is kept as it
// is, and odd is true. (In the textusenet backup such lines turn up only in
// damaged yEnc articles whose other dot lines are doubled.)
func Split(text []byte) (head, body []string, odd bool) {
	lines := strings.Split(string(text), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1] // after the last line end
	}
	if k := len(lines); k > 0 && strings.TrimSuffix(lines[k-1], "\r") == "." {
		lines = lines[:k-1] // the end-of-article line, should one be there
	}
	for i, l := range lines {
		l = strings.TrimSuffix(l, "\r")
		if strings.HasPrefix(l, "..") {
			l = l[1:]
		} else if strings.HasPrefix(l, ".") {
			odd = true
		}
		lines[i] = l
	}
	for i, l := range lines {
		if l == "" {
			return lines[:i], lines[i+1:], odd
		}
	}
	return lines, nil, odd
}

// An Item is one article of a spool.
type Item struct {
	Group string // from the directory: babbel-net/mailboxwerbung is babbel-net.mailboxwerbung
	Num   uint64
	Text  []byte // as stored; see Split
	InDB  bool   // a record in .db, not an .artN file
}

// Stats counts what a spool held.
type Stats struct {
	Groups   int // directories with a .db or an .artN file
	DBs      int // .db files
	BadDBs   int // .db files that did not parse, wholly or in part
	InDB     int // articles inside .db files
	Files    int // .artN files
	Listed   int // .db records of an article that is not inside the .db
	Missing  int // of those, how many have no .artN either: not in the backup
	Other    int // other files, skipped
	TooBig   int // articles over MaxArticleSize, .artN files or .db records, skipped
	BigDBs   int // .db files over MaxDBSize, skipped
	Problems []string
}

// The largest article, as an .artN file or a .db record, and the largest
// .db file read; a bigger one is skipped, and counted in Stats.TooBig.
// Text articles are mostly under 16 KB, and servers rarely take one over
// 256 KB. A .db is never held whole (see ReadDB): it holds an overview
// line per article, and NewsCache keeps the articles of 15 KB or less in
// it too, so a big group's .db can be large. MaxDBSize stops a broken or
// crafted file: in a tarball a few KB of gzip can stand for gigabytes,
// and a .db from a tarball goes to a temporary file. 4 GiB is the most a
// 32-bit NewsCache can address; a 64-bit one has no such bound, so a big
// group's .db from one may need more.
var (
	MaxArticleSize int64 = 1 << 20
	MaxDBSize      int64 = 4 << 30
)

// TempDir is where a .db from a tarball over 1 MiB is copied while it is
// read; empty means os.TempDir().
var TempDir string

// A .db from a tarball up to this size is read into memory; a bigger one
// is copied to a temporary file, as ReadDB needs to read it out of order.
var dbInMemory int64 = 1 << 20 // a var for the tests

// tooBig reports, and counts, a file over its limit.
func (s *scan) tooBig(name string, size int64, isArt bool) bool {
	limit := MaxDBSize
	if isArt {
		limit = MaxArticleSize
	}
	if size <= limit {
		return false
	}
	if isArt {
		s.st.TooBig++
	} else {
		s.st.BigDBs++
	}
	s.problem("%s: %d bytes, over the limit of %d, skipped", name, size, limit)
	return true
}

const maxProblems = 100

type scan struct {
	st     Stats
	fn     func(Item) error
	groups map[string]*nums
}

// The article numbers of a group: listed in .db without the article, and
// found as .artN files.
type nums struct{ listed, files []uint64 }

func newScan(fn func(Item) error) *scan {
	return &scan{fn: fn, groups: map[string]*nums{}}
}

func (s *scan) group(name string) *nums {
	g := s.groups[name]
	if g == nil {
		g = &nums{}
		s.groups[name] = g
		s.st.Groups++
	}
	return g
}

func (s *scan) problem(format string, a ...any) {
	if len(s.st.Problems) < maxProblems {
		s.st.Problems = append(s.st.Problems, fmt.Sprintf(format, a...))
	}
}

// db reads a .db file of size bytes from r. Its inline articles go to
// emit, in number order; an error from emit stops it and is returned.
// A .db that does not parse is counted and reported, not an error.
func (s *scan) db(group, name string, r io.ReaderAt, size int64, emit func(Item) error) error {
	g := s.group(group)
	s.st.DBs++
	var emitErr error
	_, err := ReadDB(r, size, func(rec Record) error {
		switch {
		case rec.Data == nil && rec.Size > MaxArticleSize:
			s.st.TooBig++
			s.problem("%s: article %d: %d bytes, over the limit of %d, skipped", name, rec.Num, rec.Size, MaxArticleSize)
		case rec.Type == Article:
			s.st.InDB++
			emitErr = emit(Item{Group: group, Num: rec.Num, Text: rec.Data, InDB: true})
			return emitErr
		case rec.Type == Overview || rec.Type == BigArticle:
			g.listed = append(g.listed, rec.Num)
		default:
			s.problem("%s: article %d: record type %d", name, rec.Num, rec.Type)
		}
		return nil
	})
	if emitErr != nil {
		return emitErr
	}
	if err != nil {
		s.st.BadDBs++
		s.problem("%s: %v", name, err)
	}
	return nil
}

// tarDB reads a .db from a tar entry: in memory when it is small, else
// through a temporary file, removed afterwards.
func (s *scan) tarDB(group, name string, tr io.Reader, size int64) error {
	if size <= dbInMemory {
		data, err := io.ReadAll(tr)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		return s.db(group, name, bytes.NewReader(data), int64(len(data)), s.fn)
	}
	f, err := os.CreateTemp(TempDir, "newscache-*.db")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	n, err := io.Copy(f, tr)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return s.db(group, name, f, n, s.fn)
}

func (s *scan) file(group string, num uint64, text []byte) error {
	g := s.group(group)
	g.files = append(g.files, num)
	s.st.Files++
	return s.fn(Item{Group: group, Num: num, Text: text})
}

func (s *scan) finish() Stats {
	for _, g := range s.groups {
		s.st.Listed += len(g.listed)
		slices.Sort(g.files)
		for _, n := range g.listed {
			if _, found := slices.BinarySearch(g.files, n); !found {
				s.st.Missing++
			}
		}
	}
	return s.st
}

// ReadTar reads a tar archive of a spool, gzipped or not, and calls fn for
// each article in the order of the archive: an .artN file where it is, the
// articles inside a .db where the .db is. The group is the directory path
// in the archive. An error from fn stops the reading and is returned.
func ReadTar(r io.Reader, fn func(Item) error) (Stats, error) {
	s := newScan(fn)
	br := bufio.NewReaderSize(r, 1<<16)
	var in io.Reader = br
	var zr *gzip.Reader
	if magic, _ := br.Peek(2); bytes.Equal(magic, []byte{0x1f, 0x8b}) {
		var err error
		if zr, err = gzip.NewReader(br); err != nil {
			return s.st, err
		}
		defer zr.Close()
		in = zr
	}
	tr := tar.NewReader(in)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			if zr != nil {
				// tar stops at its end-of-archive blocks, before the gzip
				// trailer: read to the end, which checks the CRC and size
				if _, err := io.Copy(io.Discard, zr); err != nil {
					return s.st, fmt.Errorf("after the last tar entry: %w", err)
				}
			}
			return s.finish(), nil
		}
		if err != nil {
			return s.st, err
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		name := strings.TrimPrefix(path.Clean("/"+h.Name), "/")
		dir, base := path.Split(name)
		group := strings.ReplaceAll(strings.Trim(dir, "/"), "/", ".")
		num, isArt := ArtNum(base)
		if base != ".db" && !isArt {
			s.st.Other++
			continue
		}
		if s.tooBig(h.Name, h.Size, isArt) {
			continue
		}
		if !isArt {
			if err := s.tarDB(group, h.Name, tr, h.Size); err != nil {
				return s.st, err
			}
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return s.st, fmt.Errorf("%s: %w", h.Name, err)
		}
		if err := s.file(group, num, data); err != nil {
			return s.st, err
		}
	}
}

// ReadDir reads a spool directory and calls fn for each article, directory
// by directory in lexical order and, within one, by article number. The
// group is the directory path from root's parent, so root itself may be the
// top of a hierarchy (babbel-net) or a group directory.
func ReadDir(root string, fn func(Item) error) (Stats, error) {
	s := newScan(fn)
	parent := filepath.Dir(filepath.Clean(root))
	err := filepath.WalkDir(root, func(dir string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(parent, dir)
		if err != nil {
			return err
		}
		group := strings.ReplaceAll(filepath.ToSlash(rel), "/", ".")
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		// the .artN files by number, merged with the .db's articles as
		// ReadDB returns them, also in number order
		type art struct {
			num  uint64
			file string
		}
		var arts []art
		var dbName string
		var dbSize int64
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			num, isArt := ArtNum(e.Name())
			if !isArt && e.Name() != ".db" {
				s.st.Other++
				continue
			}
			fi, err := e.Info()
			if err != nil {
				return err
			}
			name := filepath.Join(dir, e.Name())
			if s.tooBig(name, fi.Size(), isArt) {
				continue
			}
			if isArt {
				arts = append(arts, art{num, e.Name()})
			} else {
				dbName, dbSize = name, fi.Size()
			}
		}
		slices.SortStableFunc(arts, func(a, b art) int { return cmp.Compare(a.num, b.num) })
		next := 0
		// artsUpTo passes on the .artN files up to number n
		artsUpTo := func(n uint64) error {
			for ; next < len(arts) && arts[next].num <= n; next++ {
				text, err := os.ReadFile(filepath.Join(dir, arts[next].file))
				if err != nil {
					return err
				}
				if err := s.file(group, arts[next].num, text); err != nil {
					return err
				}
			}
			return nil
		}
		if dbName != "" {
			f, err := os.Open(dbName)
			if err != nil {
				return err
			}
			err = s.db(group, dbName, f, dbSize, func(it Item) error {
				if err := artsUpTo(it.Num); err != nil {
					return err
				}
				return s.fn(it)
			})
			f.Close()
			if err != nil {
				return err
			}
		}
		if err := artsUpTo(math.MaxUint64); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return s.st, err
	}
	return s.finish(), nil
}
