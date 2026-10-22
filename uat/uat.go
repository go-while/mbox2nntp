// Package uat reads the archive files of the Usenet Archive Toolkit
// (https://github.com/wolfpld/usenetarchive), such as the Polish Usenet
// archive at https://usenet.nereid.pl/ and the archive.org item
// usenet-uat-pc-games: one newsgroup per GROUP.usenet file.
//
// A .usenet file is a package of the toolkit's databases. This package
// reads only the messages: each is compressed on its own with zstd and a
// dictionary shared by the archive. Go's standard library cannot decode
// that, so Each runs the zstd program over the whole message store in one
// pass and splits its output by the stored sizes.
//
// The messages are not the articles as posted: see Fix.
package uat

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
)

// The files in a package, in order (common/Package.hpp in the toolkit).
// Versions before 3 lack the last one, before 2 the last two, and before 1
// also "lexdist" and "lexdistmeta"; every version has the ones read here.
var packageFiles = []string{
	"name", "desc_short", "desc_long", "conndata", "connmeta", "lexdata", "lexmeta",
	"lexhash", "lexhashdata", "lexhit", "lexstr", "middata", "midmeta", "midhash",
	"midhashdata", "strings", "strmeta", "toplevel", "zdata", "zmeta", "zdict",
	"lexdist", "lexdistmeta", "prefix", "msgid.codebook",
}

const (
	magic       = "\x00Usenet"
	maxVersion  = 3
	metaRecSize = 16 // RawImportMeta: uint64 offset, uint32 size, uint32 compressed size
)

// MaxMessageSize is the largest message Each passes on. Text articles are
// mostly under 16 KB, and servers rarely take one over 256 KB. A bigger
// message is read past and counted in Package.TooBig. Memory is taken as
// the data arrives, so a size in the package that is a lie costs no more
// than the data that is really there.
var MaxMessageSize = 1 << 20

type section struct{ off, size int64 }

// Package is an open .usenet file.
type Package struct {
	Name      string // the newsgroup, from the package's "name" file
	DescShort string // its short description, often empty
	Version   int
	TooBig    int // messages over MaxMessageSize, skipped by Each
	f         *os.File
	files     map[string]section
	meta      []byte
	dict      []byte
}

// Open reads the package's table of contents, the message sizes and the
// zstd dictionary. The messages stay on disk until Each.
func Open(path string) (*Package, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	p, err := open(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return p, nil
}

func open(f *os.File) (*Package, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	head := make([]byte, len(magic)+1)
	if _, err := io.ReadFull(f, head); err != nil || string(head[:len(magic)]) != magic {
		return nil, errors.New("not a UAT .usenet archive")
	}
	version := int(head[len(magic)])
	if version > maxVersion {
		return nil, fmt.Errorf("UAT archive version %d, only up to %d is known", version, maxVersion)
	}
	n := len(packageFiles)
	if version < 3 {
		n--
	}
	if version < 2 {
		n--
	}
	if version < 1 {
		n -= 2
	}
	sizes := make([]uint64, n)
	if err := binary.Read(f, binary.LittleEndian, sizes); err != nil {
		return nil, fmt.Errorf("table of contents: %v", err)
	}
	p := &Package{Version: version, f: f, files: make(map[string]section)}
	off := int64(len(head) + 8*n)
	for i, size := range sizes {
		if size > uint64(fi.Size()) || off+int64(size) > fi.Size() {
			return nil, fmt.Errorf("%s runs past the end of the file", packageFiles[i])
		}
		p.files[packageFiles[i]] = section{off, int64(size)}
		off = (off + int64(size) + 7) / 8 * 8 // each file starts on 8 bytes
	}
	name, err := p.read("name")
	if err != nil {
		return nil, err
	}
	desc, err := p.read("desc_short")
	if err != nil {
		return nil, err
	}
	p.Name = strings.TrimSpace(string(name))
	p.DescShort = strings.TrimSpace(string(desc))
	if p.meta, err = p.read("zmeta"); err != nil {
		return nil, err
	}
	if len(p.meta)%metaRecSize != 0 {
		return nil, fmt.Errorf("zmeta is %d bytes, not a multiple of %d", len(p.meta), metaRecSize)
	}
	if p.dict, err = p.read("zdict"); err != nil {
		return nil, err
	}
	if err := p.checkLayout(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Package) read(name string) ([]byte, error) {
	s := p.files[name]
	b := make([]byte, s.size)
	if _, err := p.f.ReadAt(b, s.off); err != nil {
		return nil, fmt.Errorf("%s: %v", name, err)
	}
	return b, nil
}

func (p *Package) entry(i int) (off uint64, size, csize uint32) {
	r := p.meta[i*metaRecSize:]
	return binary.LittleEndian.Uint64(r), binary.LittleEndian.Uint32(r[8:]), binary.LittleEndian.Uint32(r[12:])
}

// checkLayout makes sure the compressed messages lie back to back in index
// order and fill zdata, which is what lets Each decode them in one pass.
// The toolkit writes them that way (repack-zstd, update-zstd).
func (p *Package) checkLayout() error {
	var next uint64
	for i := 0; i < p.Count(); i++ {
		off, _, csize := p.entry(i)
		if off != next {
			return fmt.Errorf("message %d is at offset %d of zdata, expected %d: messages not stored in order", i, off, next)
		}
		next += uint64(csize)
	}
	if zdata := p.files["zdata"].size; next != uint64(zdata) {
		return fmt.Errorf("messages take %d bytes, zdata has %d", next, zdata)
	}
	return nil
}

// Count is the number of messages.
func (p *Package) Count() int { return len(p.meta) / metaRecSize }

// Close closes the file.
func (p *Package) Close() error { return p.f.Close() }

// Each calls fn with every message, in the archive's order, as the
// toolkit stores it: header lines and body, LF line endings, UTF-8. msg is
// only valid during the call. zstd is the program to run; the dictionary
// is written to a temporary file in tmpdir ("" for the system default) and
// removed afterwards. An error from fn stops the run and is returned.
func (p *Package) Each(zstd, tmpdir string, fn func(i int, msg []byte) error) error {
	dict, err := os.CreateTemp(tmpdir, "uat-zdict-*")
	if err != nil {
		return err
	}
	defer os.Remove(dict.Name())
	if _, err := dict.Write(p.dict); err != nil {
		dict.Close()
		return err
	}
	if err := dict.Close(); err != nil {
		return err
	}

	zdata := p.files["zdata"]
	var stderr strings.Builder
	cmd := exec.Command(zstd, "-d", "-q", "-c", "-D", dict.Name())
	cmd.Stdin = io.NewSectionReader(p.f, zdata.off, zdata.size)
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	ferr := p.split(bufio.NewReaderSize(stdout, 1<<20), fn)
	if ferr != nil {
		cmd.Process.Kill() // it would block on a full pipe
	}
	werr := cmd.Wait()
	if ferr != nil {
		return ferr
	}
	if werr != nil {
		return fmt.Errorf("zstd: %v: %s", werr, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func (p *Package) split(r io.Reader, fn func(i int, msg []byte) error) error {
	var buf []byte
	for i := 0; i < p.Count(); i++ {
		_, size, _ := p.entry(i)
		if int64(size) > int64(MaxMessageSize) {
			if n, err := io.CopyN(io.Discard, r, int64(size)); err != nil {
				return fmt.Errorf("message %d of %d: zstd output ended early, after %d of %d bytes: %v", i, p.Count(), n, size, err)
			}
			p.TooBig++
			continue
		}
		buf = buf[:0]
		for len(buf) < int(size) {
			n := min(int(size)-len(buf), 1<<20)
			buf = slices.Grow(buf, n)
			m, err := io.ReadFull(r, buf[len(buf):len(buf)+n])
			buf = buf[:len(buf)+m]
			if err != nil {
				return fmt.Errorf("message %d of %d: zstd output ended early: %v", i, p.Count(), err)
			}
		}
		if err := fn(i, buf); err != nil {
			return err
		}
	}
	if n, _ := io.Copy(io.Discard, r); n != 0 {
		return fmt.Errorf("zstd output is %d bytes longer than the messages", n)
	}
	return nil
}
