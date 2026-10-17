// Package newscachetest builds NewsCache spool files for tests of code that
// reads them, such as package newscache.
package newscachetest

import "encoding/binary"

// Record types, as in package newscache.
const (
	Overview   = 2
	Article    = 3
	BigArticle = 4
)

// A Record is the entry of one article number.
type Record struct {
	Type byte
	Data string // the overview line or the whole article
}

// DB returns a .db as NewsCache's NVArray writes it, for articles first to
// last: a header, the article range with one offset per number (0 for a
// number without a record), and per record its size as an unsigned long,
// the type byte and the NUL-terminated data, on 16-byte boundaries. w is
// the size of an unsigned long, 4 or 8, and bo the byte order, of the build
// that wrote it; NewsCache on amd64 is binary.LittleEndian and 8.
func DB(bo binary.ByteOrder, w int, first, last uint64, recs map[uint64]Record) []byte {
	put := func(b []byte, off int, v uint64) {
		if w == 8 {
			bo.PutUint64(b[off:], v)
		} else {
			bo.PutUint32(b[off:], uint32(v))
		}
	}
	align := func(n int) int { return (n + 15) &^ 15 }
	hlen := 8 + 5*w
	userdata := align(hlen) + w // the block after the header, past its size
	n := 0
	if first <= last {
		n = int(last - first + 1)
	}
	b := make([]byte, align(userdata+(2+n)*w))
	for num := first; num <= last && n > 0; num++ {
		r, ok := recs[num]
		if !ok {
			continue
		}
		data := append([]byte{r.Type}, r.Data...)
		data = append(data, 0)
		off := len(b) + w // past the block's own size, as nvalloc returns it
		b = append(b, make([]byte, align(w+w+len(data)))...)
		put(b, off, uint64(len(data)))
		copy(b[off+w:], data)
		put(b, userdata+(2+int(num-first))*w, uint64(off))
	}
	bo.PutUint32(b, uint32(hlen))
	bo.PutUint32(b[4:], 2) // the format version
	put(b, 8, uint64(len(b)))
	put(b, 8+w, 1602759658) // mtime, 2020-10-15
	put(b, 8+4*w, uint64(userdata))
	put(b, userdata, first)
	put(b, userdata+w, last)
	return b
}
