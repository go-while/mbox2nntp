package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unsafe"

	mbox "github.com/go-while/go-lib-mbox2nntp"
	"github.com/go-while/mbox2nntp/internal/mboxout"
	"github.com/go-while/mbox2nntp/newscache"
	"github.com/go-while/mbox2nntp/newscache/newscachetest"
)

const (
	// crossposted: in both groups' directories
	cross = "Path: a!not-for-mail\r\nNewsgroups: h.one,h.two\r\nSubject: cross\r\nDate: Sat, 3 Jan 2004 02:40:01 +0000 (UTC)\r\nMessage-ID: <cross@x>\r\n\r\nFrom the start\r\n..dot\r\n"
	// inside h.one's .db
	inline = "Path: a!not-for-mail\r\nNewsgroups: h.one\r\nSubject: inline\r\nMessage-ID:\r\n <inline@x>\r\n\r\nsmall\r\n"
	noID   = "Path: a!not-for-mail\r\nNewsgroups: h.two\r\nSubject: no id\r\n\r\n.single dot\r\n"
)

// spool is h.one with articles 1 (.art1) and 2 (inside .db), and 3 listed
// but missing, and h.two with articles 7 and 8.
func spool() map[string][]byte {
	return map[string][]byte{
		"h/one/.db": newscachetest.DB(binary.LittleEndian, 8, 1, 3, map[uint64]newscachetest.Record{
			1: {Type: newscachetest.Overview, Data: "1\tcross"},
			2: {Type: newscachetest.Article, Data: inline},
			3: {Type: newscachetest.Overview, Data: "3\tgone"},
		}),
		"h/one/.art1": []byte(cross),
		"h/two/.art7": []byte(cross),
		"h/two/.art8": []byte(noID),
	}
}

// .art1 before the .db of its group, as tar may store them
var order = []string{"h/one/.art1", "h/one/.db", "h/two/.art7", "h/two/.art8"}

func tarball(t *testing.T, gz bool) []byte {
	var buf bytes.Buffer
	var zw *gzip.Writer
	tw := tar.NewWriter(&buf)
	if gz {
		zw = gzip.NewWriter(&buf)
		tw = tar.NewWriter(zw)
	}
	files := spool()
	for _, name := range order {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(files[name]))}); err != nil {
			t.Fatal(err)
		}
		tw.Write(files[name])
	}
	tw.Close()
	if zw != nil {
		zw.Close()
	}
	return buf.Bytes()
}

// readBack parses an mbox with go-lib-mbox2nntp, mbox2nntp's parser, and
// returns each article's headers and body.
func readBack(t *testing.T, path string) (heads [][]string, bodies []string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		zr, err := gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		r = zr
	}
	mr, err := mbox.CreateMboxStream(bufio.NewReader(r))
	if err != nil {
		t.Fatal(err)
	}
	for {
		m, err := mr.ReadMessage()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(m.BodyReader())
		heads = append(heads, m.Headers())
		bodies = append(bodies, string(b))
	}
}

func head(text string) []string {
	h, _, _ := newscache.Split([]byte(text))
	return h
}

// subjects returns the Subject of every article in an mbox, in order.
func subjects(t *testing.T, path string) []string {
	t.Helper()
	heads, _ := readBack(t, path)
	var out []string
	for _, h := range heads {
		for _, l := range h {
			if v, ok := strings.CutPrefix(l, "Subject: "); ok {
				out = append(out, v)
			}
		}
	}
	return out
}

func TestExportTar(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "h.tar.gz")
	os.WriteFile(in, tarball(t, true), 0644)
	t.Chdir(dir)
	st, err := export(in, nil, options{dedup: true})
	if err != nil {
		t.Fatal(err)
	}
	// the crossposted article goes to both groups
	if st.out != "h" || st.n != 4 || st.dups != 0 || st.noID != 1 || st.oddDots != 1 || st.groupFiles != 2 ||
		st.Groups != 2 || st.Files != 3 || st.InDB != 1 || st.Missing != 1 {
		t.Errorf("stats %+v", st)
	}
	f := mboxout.FormatField
	for name, want := range map[string][][]string{
		"h/h.one.mbox": {append(head(cross), f), append(head(inline), f)},
		"h/h.two.mbox": {append(head(cross), f), append(head(noID), f)},
	} {
		if heads, _ := readBack(t, name); !reflect.DeepEqual(heads, want) {
			t.Errorf("%s: headers %q\nwant %q", name, heads, want)
		}
	}
	// the doubled dot undone, the single one kept, "From " quoted
	one, _ := os.ReadFile("h/h.one.mbox")
	two, _ := os.ReadFile("h/h.two.mbox")
	for _, c := range []struct {
		data []byte
		want string
	}{
		{one, "From nobody Sat Jan  3 02:40:01 2004\n"},
		{one, "\n>From the start\n.dot\n"},
		{two, "\n.single dot\n"},
	} {
		if !strings.Contains(string(c.data), c.want) {
			t.Errorf("mbox lacks %q", c.want)
		}
	}
	if strings.Contains(string(one)+string(two), "\r") {
		t.Error("CR left in the mbox")
	}
	if _, err := export(in, nil, options{dedup: true}); err == nil {
		t.Error("an existing directory was replaced")
	}

	// gzipped
	st, err = export(in, nil, options{out: "gz", dedup: true, gzip: 6})
	if err != nil || st.n != 4 {
		t.Errorf("-gzip: %+v, %v", st, err)
	}
	if got := subjects(t, "gz/h.one.mbox.gz"); !slices.Equal(got, []string{"cross", "inline"}) {
		t.Errorf("-gzip: h.one %q", got)
	}
}

// Dedup works within one group's mbox: the same Message-ID twice in a group
// is written once, and every copy with -dedup=false.
func TestDedupWithinGroup(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range []string{"g/.art1", "g/.art2", "k/.art1"} {
		tw.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(cross))})
		tw.Write([]byte(cross))
	}
	tw.Close()
	for _, c := range []struct {
		dedup   bool
		n, dups int
		g       []string
	}{{true, 2, 1, []string{"cross"}}, {false, 3, 0, []string{"cross", "cross"}}} {
		out := fmt.Sprintf("out-%t", c.dedup)
		st, err := export("-", bytes.NewReader(buf.Bytes()), options{out: out, dedup: c.dedup})
		if err != nil || st.n != c.n || st.dups != c.dups {
			t.Errorf("dedup=%t: %+v, %v", c.dedup, st, err)
		}
		if got := subjects(t, out+"/g.mbox"); !slices.Equal(got, c.g) {
			t.Errorf("dedup=%t: g %q, want %q", c.dedup, got, c.g)
		}
		if got := subjects(t, out+"/k.mbox"); !slices.Equal(got, []string{"cross"}) {
			t.Errorf("dedup=%t: k %q", c.dedup, got)
		}
	}
}

// "-" reads a plain tar from stdin; a directory gives the same mboxes, its
// groups in number order.
func TestExportStdinAndDir(t *testing.T) {
	dir := t.TempDir()
	st, err := export("-", bytes.NewReader(tarball(t, false)), options{out: filepath.Join(dir, "stdin"), dedup: true})
	if err != nil || st.n != 4 {
		t.Fatalf("stdin: %+v, %v", st, err)
	}
	root := filepath.Join(dir, "spool", "h")
	for name, data := range spool() {
		p := filepath.Join(dir, "spool", filepath.FromSlash(name))
		os.MkdirAll(filepath.Dir(p), 0755)
		os.WriteFile(p, data, 0644)
	}
	st, err = export(root, nil, options{out: filepath.Join(dir, "dir"), dedup: true})
	if err != nil || st.n != 4 || st.Missing != 1 {
		t.Fatalf("dir: %+v, %v", st, err)
	}
	for _, name := range []string{"h.one.mbox", "h.two.mbox"} {
		a, _ := os.ReadFile(filepath.Join(dir, "stdin", name))
		b, _ := os.ReadFile(filepath.Join(dir, "dir", name))
		if len(a) == 0 || !bytes.Equal(a, b) {
			t.Errorf("%s: stdin and directory differ:\n%s\n----\n%s", name, a, b)
		}
	}
}

// A tarball that breaks off leaves neither the output nor its temporary
// directory.
func TestExportTruncated(t *testing.T) {
	dir := t.TempDir()
	data := tarball(t, false)
	_, err := export("-", bytes.NewReader(data[:len(data)/2]), options{out: filepath.Join(dir, "x")})
	if err == nil {
		t.Error("no error")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("left %v", entries)
	}
}

func TestBaseName(t *testing.T) {
	for in, want := range map[string]string{
		"babbel-net.tar.gz":                 "babbel-net",
		"/x/rainbow.tgz":                    "rainbow",
		"a.b.tar":                           "a.b",
		"/spool/comp/":                      "comp",
		"textusenet-backup_9/97comp.tar.gz": "97comp",
	} {
		if got := baseName(in); got != want {
			t.Errorf("baseName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{
		"1048576": 1 << 20, "4G": 4 << 30, "4g": 4 << 30, "16GiB": 16 << 30, "16GB": 16 << 30,
		"512M": 512 << 20, "2T": 2 << 40, "8k": 8 << 10, " 3G ": 3 << 30, "0": math.MaxInt64,
	} {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "G", "-1G", "1.5G", "4X", "9999999T"} {
		if got, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) = %d, want an error", in, got)
		}
	}
	for n, want := range map[int64]string{4 << 30: "4G", 1536 << 20: "1536M", 1000: "1000", math.MaxInt64: "0"} {
		if got := formatSize(n); got != want {
			t.Errorf("formatSize(%d) = %q, want %q", n, got, want)
		}
	}
}

// A .db over -max-db is skipped and counted on its own, and the summary
// says its articles are missing.
func TestMaxDB(t *testing.T) {
	saved := newscache.MaxDBSize
	t.Cleanup(func() { newscache.MaxDBSize = saved })
	fs := flag.NewFlagSet("x", flag.ContinueOnError)
	fs.Var(sizeFlag{&newscache.MaxDBSize}, "max-db", "")
	if err := fs.Parse([]string{"-max-db", "100"}); err != nil || newscache.MaxDBSize != 100 {
		t.Fatalf("-max-db 100: %d, %v", newscache.MaxDBSize, err)
	}
	dir := t.TempDir()
	in := filepath.Join(dir, "h.tar")
	if err := os.WriteFile(in, tarball(t, false), 0644); err != nil {
		t.Fatal(err)
	}
	st, err := export(in, nil, options{out: filepath.Join(dir, "h"), dedup: true})
	if err != nil || st.BigDBs != 1 || st.TooBig != 0 || st.InDB != 0 || st.n != 3 {
		t.Fatalf("%+v, %v", st, err)
	}
	if s := st.summary(in); !strings.Contains(s, "1 .db files over -max-db (100) skipped") {
		t.Errorf("summary: %s", s)
	}
}

// The temporary copy of a big .db goes to -tmpdir, by default the
// output's directory, not to os.TempDir(), which may be small or in RAM.
func TestTmpdir(t *testing.T) {
	saved := newscache.TempDir
	t.Cleanup(func() { newscache.TempDir = saved })
	recs := map[uint64]newscachetest.Record{}
	for i := uint64(1); i <= 5000; i++ {
		recs[i] = newscachetest.Record{Type: newscachetest.Overview, Data: strings.Repeat("o", 300)}
	}
	db := newscachetest.DB(binary.LittleEndian, 8, 1, 5000, recs) // over 1 MiB
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{Name: "g/.db", Mode: 0644, Size: int64(len(db))})
	tw.Write(db)
	tw.Close()
	dir := t.TempDir()
	in := filepath.Join(dir, "g.tar")
	if err := os.WriteFile(in, buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out", "g")
	os.Mkdir(filepath.Dir(out), 0755)
	st, err := export(in, nil, options{out: out})
	if err != nil || st.Listed != 5000 || newscache.TempDir != filepath.Dir(out) {
		t.Fatalf("default: %+v, %v, TempDir %q", st, err, newscache.TempDir)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "out", "newscache-*")); len(left) != 0 {
		t.Errorf("left behind: %v", left)
	}
	missing := filepath.Join(dir, "no-such-dir")
	if _, err := export(in, nil, options{out: filepath.Join(dir, "g2"), tmpdir: missing}); err == nil || !strings.Contains(err.Error(), missing) {
		t.Errorf("-tmpdir %s: %v", missing, err)
	}
}

// The dedup map keeps a copy of the Message-ID, not a slice of the
// article: that kept every article in memory (950 MB for rss.tar.gz).
func TestFirstSeenCopies(t *testing.T) {
	head, _, _ := newscache.Split([]byte(cross + strings.Repeat("body line\r\n", 1000)))
	id := messageID(head)
	seen := map[string]bool{}
	if !firstSeen(seen, id) || firstSeen(seen, id) {
		t.Fatal("firstSeen: wrong answers")
	}
	for k := range seen {
		if k != "<cross@x>" || unsafe.StringData(k) == unsafe.StringData(id) {
			t.Errorf("key %q shares the article's memory", k)
		}
	}
}

// -missing writes the listed articles that are in neither the spool nor
// the export; one exported from another group, a crosspost, is not.
func TestMissingFile(t *testing.T) {
	dir := t.TempDir()
	files := spool()
	files["h/one/.db"] = newscachetest.DB(binary.LittleEndian, 8, 1, 4, map[uint64]newscachetest.Record{
		1: {Type: newscachetest.Overview, Data: "1\tcross"},
		2: {Type: newscachetest.Article, Data: inline},
		3: {Type: newscachetest.Overview, Data: "3\tgone\ta@b\tSat, 3 Jan 2004 02:40:01 +0000\t<gone@x>\t\t100\t3\t"},
		// stored as h/two/.art7
		4: {Type: newscachetest.Overview, Data: "4\tcross\ta@b\tSat, 3 Jan 2004 02:40:01 +0000\t <cross@x>\t\t100\t3\t"},
	})
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range order {
		tw.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(files[name]))})
		tw.Write(files[name])
	}
	tw.Close()
	in := filepath.Join(dir, "h.tar")
	if err := os.WriteFile(in, buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	for _, dedup := range []bool{true, false} {
		missing := filepath.Join(dir, fmt.Sprintf("missing-%t.tsv", dedup))
		st, err := export(in, nil, options{out: filepath.Join(dir, fmt.Sprintf("h-%t", dedup)), dedup: dedup, missing: missing})
		if err != nil || st.Missing != 2 || st.listedFound != 1 || st.lost != 1 {
			t.Fatalf("dedup=%t: %+v, %v", dedup, st, err)
		}
		got, _ := os.ReadFile(missing)
		want := "group\tnumber\tdate\tsubject\tfrom\tmessage-id\nh.one\t3\tSat, 3 Jan 2004 02:40:01 +0000\tgone\ta@b\t<gone@x>\n"
		if string(got) != want {
			t.Errorf("dedup=%t: %q\nwant %q", dedup, got, want)
		}
		if s := st.summary(in); !strings.Contains(s, "2 listed in .db without an .art file: 1 of them exported from another group, 1 lost, listed in "+missing) {
			t.Errorf("summary: %s", s)
		}
	}
	// an existing file is not replaced, and nothing is written
	missing := filepath.Join(dir, "missing-true.tsv")
	if _, err := export(in, nil, options{out: filepath.Join(dir, "again"), missing: missing}); err == nil {
		t.Error("existing -missing file: no error")
	}
	if _, err := os.Stat(filepath.Join(dir, "again")); err == nil {
		t.Error("existing -missing file: the mbox was written")
	}
}

// eofHook runs fn once when r reaches its end.
type eofHook struct {
	r    io.Reader
	fn   func()
	done bool
}

func (h *eofHook) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if err == io.EOF && !h.done {
		h.done = true
		h.fn()
	}
	return n, err
}

// The mbox is committed before the -missing file: when the mbox fails at
// the end, here because its name turned up meanwhile, no missing list is
// left to block the rerun.
func TestMissingAfterMbox(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "h")
	missing := filepath.Join(dir, "missing.tsv")
	in := &eofHook{r: bytes.NewReader(tarball(t, true)), fn: func() { os.WriteFile(out, []byte("someone else"), 0644) }}
	if _, err := export("-", in, options{out: out, dedup: true, missing: missing}); err == nil {
		t.Fatal("the mbox's name taken: no error")
	}
	if _, err := os.Stat(missing); err == nil {
		t.Errorf("no mbox, but %s was written", missing)
	}
	os.Remove(out)
	if _, err := export("-", bytes.NewReader(tarball(t, false)), options{out: out, dedup: true, missing: missing}); err != nil {
		t.Errorf("rerun: %v", err)
	}
	left, _ := filepath.Glob(filepath.Join(dir, ".*.tmp"))
	if len(left) != 0 {
		t.Errorf("temporary files left: %v", left)
	}
}

// -missing naming the output is refused before anything is read.
func TestMissingSameAsOut(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "h")
	_, err := export("-", bytes.NewReader(tarball(t, false)), options{out: out, missing: filepath.Join(dir, ".", "h")})
	if err == nil || !strings.Contains(err.Error(), "is the output") {
		t.Errorf("err = %v", err)
	}
	if _, err := os.Stat(out); err == nil {
		t.Error("the mbox was written")
	}
}

// A -tmpdir that cannot be used is an error at the start, not at the
// first big .db.
func TestTmpdirCheckedFirst(t *testing.T) {
	dir := t.TempDir()
	read := false
	in := &eofHook{r: bytes.NewReader(tarball(t, false)), fn: func() { read = true }}
	_, err := export("-", in, options{out: filepath.Join(dir, "h"), tmpdir: filepath.Join(dir, "no-such-dir")})
	if err == nil || !strings.Contains(err.Error(), "-tmpdir") || read {
		t.Errorf("err = %v, input read: %t", err, read)
	}
}

// A group met again later in the tarball is appended to: here both groups
// are interleaved, so each file holds two runs (two gzip members with
// -gzip).
func TestGroupMboxes(t *testing.T) {
	files := spool()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range []string{"h/one/.art1", "h/two/.art7", "h/one/.db", "h/two/.art8"} {
		tw.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(files[name]))})
		tw.Write(files[name])
	}
	tw.Close()
	for _, level := range []int{0, 6} {
		dir := t.TempDir()
		in := filepath.Join(dir, "h.tar")
		if err := os.WriteFile(in, buf.Bytes(), 0644); err != nil {
			t.Fatal(err)
		}
		t.Chdir(dir)
		st, err := export(in, nil, options{dedup: true, gzip: level})
		if err != nil || st.out != "h" || st.n != 4 || st.dups != 0 || st.groupFiles != 2 {
			t.Fatalf("level %d: %+v, %v", level, st, err)
		}
		if s := st.summary(in); !strings.Contains(s, "4 articles to h/ (2 group files)") {
			t.Errorf("summary: %s", s)
		}
		ext := ".mbox"
		if level != 0 {
			ext += ".gz"
		}
		entries, _ := os.ReadDir("h")
		if len(entries) != 2 || entries[0].Name() != "h.one"+ext || entries[1].Name() != "h.two"+ext {
			t.Fatalf("level %d: %v", level, entries)
		}
		// h.one: .art1 (cross), then the .db's inline article; h.two:
		// .art7 (cross again), then .art8
		for name, want := range map[string][]string{"h.one": {"cross", "inline"}, "h.two": {"cross", "no id"}} {
			if got := subjects(t, filepath.Join("h", name+ext)); !slices.Equal(got, want) {
				t.Errorf("level %d: %s: %q, want %q", level, name, got, want)
			}
		}
		if left, _ := filepath.Glob(".h.*.tmp"); len(left) != 0 {
			t.Errorf("temporary directory left: %v", left)
		}
		// the directory exists now: not replaced
		if _, err := export(in, nil, options{gzip: level}); err == nil {
			t.Error("existing directory: no error")
		}
	}
}

// A failed export leaves neither the directory nor its temporary.
func TestGroupMboxesFailed(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	data := tarball(t, false)
	if _, err := export("-", bytes.NewReader(data[:len(data)/2]), options{out: "h"}); err == nil {
		t.Fatal("truncated tarball: no error")
	}
	if left, _ := os.ReadDir("."); len(left) != 0 {
		t.Errorf("left behind: %v", left)
	}
}

func TestGroupFile(t *testing.T) {
	for group, want := range map[string]string{
		"rec.arts.movies":   "rec.arts.movies",
		"alt.fan.c++":       "alt.fan.c++",
		"de.comp.os.unix-x": "de.comp.os.unix-x",
		"a b/c\\d:e":        "a_b_c_d_e",
		"":                  "no-group",
		"con.foo":           "_con.foo",
		"aux":               "_aux",
		"contest.x":         "contest.x",
		"grüße":             "gr____e",
	} {
		if got := groupFile(group); got != want {
			t.Errorf("groupFile(%q) = %q, want %q", group, got, want)
		}
	}
}
