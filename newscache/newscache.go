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
}

// A DB is a parsed .db file.
type DB struct {
	Mtime       time.Time
	First, Last uint64
	Records     []Record // in number order, numbers without a record left out
}

// ParseDB reads a .db file. It takes the layouts of 32-bit and 64-bit
// builds in either byte order. When a record points outside the file it
// returns the records read so far with the error.
func ParseDB(b []byte) (*DB, error) {
	var (
		bo binary.ByteOrder
		w  int // the size of an unsigned long in the build that wrote b
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
	word := func(off uint64) (uint64, bool) {
		if off > uint64(len(b)) || uint64(len(b))-off < uint64(w) {
			return 0, false
		}
		if w == 8 {
			return bo.Uint64(b[off:]), true
		}
		return uint64(bo.Uint32(b[off:])), true
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
	n := last - first + 1
	tab := userdata + 2*uint64(w)
	if n > uint64(len(b))/uint64(w) || tab+n*uint64(w) > uint64(len(b)) {
		return nil, fmt.Errorf("article range %d-%d does not fit the file", first, last)
	}
	db.First, db.Last = first, last
	for i := range n {
		off, _ := word(tab + i*uint64(w))
		if off == 0 {
			continue
		}
		size, ok := word(off)
		start := off + uint64(w)
		if !ok || size == 0 || size > uint64(len(b)) || start+size > uint64(len(b)) {
			return db, fmt.Errorf("record of article %d at %#x is outside the file", first+i, off)
		}
		data := b[start : start+size]
		r := Record{Num: first + i, Type: data[0], Data: data[1:]}
		if k := len(r.Data); k > 0 && r.Data[k-1] == 0 {
			r.Data = r.Data[:k-1]
		}
		db.Records = append(db.Records, r)
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
	TooBig   int // .artN and .db files over MaxArticleSize or MaxDBSize, skipped
	Problems []string
}

// The largest .artN and .db files read. A bigger one is a broken or crafted
// file, and would be held in memory whole: in a tarball a few KB of gzip can
// stand for gigabytes. It is skipped, and counted in Stats.TooBig.
var (
	MaxArticleSize int64 = 256 << 20
	MaxDBSize      int64 = 1 << 30
)

// tooBig reports, and counts, a file over its limit.
func (s *scan) tooBig(name string, size int64, isArt bool) bool {
	limit := MaxDBSize
	if isArt {
		limit = MaxArticleSize
	}
	if size <= limit {
		return false
	}
	s.st.TooBig++
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

// db takes a .db file. Its inline articles are returned, not passed to fn,
// so that ReadDir can merge them with the .artN files in number order.
func (s *scan) db(group, name string, data []byte) []Item {
	g := s.group(group)
	s.st.DBs++
	db, err := ParseDB(data)
	if err != nil {
		s.st.BadDBs++
		s.problem("%s: %v", name, err)
		if db == nil {
			return nil
		}
	}
	var items []Item
	for _, r := range db.Records {
		switch r.Type {
		case Article:
			items = append(items, Item{Group: group, Num: r.Num, Text: r.Data, InDB: true})
		case Overview, BigArticle:
			g.listed = append(g.listed, r.Num)
		default:
			s.problem("%s: article %d: record type %d", name, r.Num, r.Type)
		}
	}
	s.st.InDB += len(items)
	return items
}

func (s *scan) file(group string, num uint64, text []byte) error {
	g := s.group(group)
	g.files = append(g.files, num)
	s.st.Files++
	return s.fn(Item{Group: group, Num: num, Text: text})
}

func (s *scan) emit(items []Item) error {
	for _, it := range items {
		if err := s.fn(it); err != nil {
			return err
		}
	}
	return nil
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
		data, err := io.ReadAll(tr)
		if err != nil {
			return s.st, fmt.Errorf("%s: %w", h.Name, err)
		}
		if isArt {
			err = s.file(group, num, data)
		} else {
			err = s.emit(s.db(group, h.Name, data))
		}
		if err != nil {
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
		// the .db's articles, and the .artN files by their names
		type entry struct {
			Item
			file string
		}
		var items []entry
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			num, isArt := ArtNum(e.Name())
			if isArt || e.Name() == ".db" {
				fi, err := e.Info()
				if err != nil {
					return err
				}
				if s.tooBig(filepath.Join(dir, e.Name()), fi.Size(), isArt) {
					continue
				}
			}
			switch {
			case isArt:
				items = append(items, entry{Item{Group: group, Num: num}, e.Name()})
			case e.Name() == ".db":
				name := filepath.Join(dir, ".db")
				data, err := os.ReadFile(name)
				if err != nil {
					return err
				}
				for _, it := range s.db(group, name, data) {
					items = append(items, entry{Item: it})
				}
			default:
				s.st.Other++
			}
		}
		slices.SortStableFunc(items, func(a, b entry) int { return cmp.Compare(a.Num, b.Num) })
		for _, it := range items {
			if it.InDB {
				err = s.fn(it.Item)
			} else {
				var text []byte
				if text, err = os.ReadFile(filepath.Join(dir, it.file)); err == nil {
					err = s.file(group, it.Num, text)
				}
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return s.st, err
	}
	return s.finish(), nil
}
