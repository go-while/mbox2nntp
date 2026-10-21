package uat

import (
	"bytes"
	"encoding/binary"
	"errors"
	"mime"
	"net/mail"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/go-while/mbox2nntp/uat/uattest"
)

var testMsgs = []string{
	"Path: a!b\nNewsgroups: pl.test\nSubject: one\nMessage-ID: <1@x>\n\nfirst\n",
	"Path: a!b\nNewsgroups: pl.test\nSubject: Zażółć\nMessage-ID: <2@x>\n\nsecond, gęślą\n",
	"Path: a!b\nNewsgroups: pl.test\nSubject: three, no final newline\nMessage-ID: <3@x>\n\nthird",
	"", // the toolkit can store an empty message
}

func TestOpenAndEach(t *testing.T) {
	zstd := uattest.Zstd(t)
	path := uattest.Write(t, zstd, t.TempDir(), "pl.test\n", "a test group\r\n", testMsgs)
	p, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.Name != "pl.test" || p.DescShort != "a test group" || p.Version != 3 || p.Count() != len(testMsgs) {
		t.Errorf("Name %q, DescShort %q, Version %d, Count %d", p.Name, p.DescShort, p.Version, p.Count())
	}
	var got []string
	if err := p.Each(zstd, t.TempDir(), func(i int, msg []byte) error {
		if i != len(got) {
			t.Errorf("index %d, want %d", i, len(got))
		}
		got = append(got, string(msg))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, testMsgs) {
		t.Errorf("got %q\nwant %q", got, testMsgs)
	}

	stop := errors.New("stop")
	n := 0
	if err := p.Each(zstd, "", func(i int, msg []byte) error { n++; return stop }); err != stop || n != 1 {
		t.Errorf("an error from fn: Each = %v after %d calls, want stop after 1", err, n)
	}
	if err := p.Each(filepath.Join(t.TempDir(), "no-such-zstd"), "", func(int, []byte) error { return nil }); err == nil {
		t.Error("missing zstd program: err = nil")
	}
}

func TestUattestMatchesFormat(t *testing.T) {
	if !reflect.DeepEqual(uattest.Files, packageFiles) {
		t.Error("uattest.Files and packageFiles differ")
	}
}

func TestOpenRejects(t *testing.T) {
	dir := t.TempDir()
	meta := func(recs ...[3]uint64) []byte {
		var b bytes.Buffer
		for _, r := range recs {
			binary.Write(&b, binary.LittleEndian, r[0])
			binary.Write(&b, binary.LittleEndian, uint32(r[1]))
			binary.Write(&b, binary.LittleEndian, uint32(r[2]))
		}
		return b.Bytes()
	}
	notUAT := filepath.Join(dir, "plain")
	os.WriteFile(notUAT, []byte("From nobody Thu Dec  8 12:55:04 2016\n"), 0644)
	truncated := filepath.Join(dir, "truncated")
	os.WriteFile(truncated, append([]byte(magic), 3, 1, 2, 3), 0644)
	cases := map[string]string{
		"not an archive":   notUAT,
		"truncated":        truncated,
		"newer version":    uattest.WriteRaw(t, filepath.Join(t.TempDir(), "x.usenet"), 4, nil),
		"zmeta not 16s":    uattest.WriteRaw(t, filepath.Join(t.TempDir(), "x.usenet"), 3, map[string][]byte{"zmeta": make([]byte, 15)}),
		"out of order":     uattest.WriteRaw(t, filepath.Join(t.TempDir(), "x.usenet"), 3, map[string][]byte{"zdata": make([]byte, 20), "zmeta": meta([3]uint64{10, 5, 10}, [3]uint64{0, 5, 10})}),
		"zdata too long":   uattest.WriteRaw(t, filepath.Join(t.TempDir(), "x.usenet"), 3, map[string][]byte{"zdata": make([]byte, 21), "zmeta": meta([3]uint64{0, 5, 10}, [3]uint64{10, 5, 10})}),
		"zdata too short":  uattest.WriteRaw(t, filepath.Join(t.TempDir(), "x.usenet"), 3, map[string][]byte{"zdata": make([]byte, 19), "zmeta": meta([3]uint64{0, 5, 10}, [3]uint64{10, 5, 10})}),
		"missing the file": filepath.Join(dir, "no-such.usenet"),
	}
	for name, path := range cases {
		if p, err := Open(path); err == nil {
			p.Close()
			t.Errorf("%s: Open succeeded", name)
		}
	}
	// an empty archive is valid
	p, err := Open(uattest.WriteRaw(t, filepath.Join(t.TempDir(), "x.usenet"), 3, map[string][]byte{"name": []byte("pl.empty")}))
	if err != nil {
		t.Fatal(err)
	}
	if p.Count() != 0 || p.Name != "pl.empty" {
		t.Errorf("empty archive: Count %d, Name %q", p.Count(), p.Name)
	}
	p.Close()
}

// setFirstSize writes size into the first zmeta record of the package at path.
func setFirstSize(t *testing.T, path string, size uint32) {
	t.Helper()
	b, _ := os.ReadFile(path)
	// zmeta is the 20th file; its first record's size field is at +8
	off := int64(len(magic) + 1 + 8*len(packageFiles))
	sizes := make([]uint64, len(packageFiles))
	binary.Read(bytes.NewReader(b[len(magic)+1:]), binary.LittleEndian, sizes)
	for i, f := range packageFiles {
		if f == "zmeta" {
			binary.LittleEndian.PutUint32(b[off+8:], size)
			break
		}
		off = (off + int64(sizes[i]) + 7) / 8 * 8
	}
	if err := os.WriteFile(path, b, 0644); err != nil {
		t.Fatal(err)
	}
}

// A message whose stored size disagrees with what zstd puts out is an
// error, not a shifted split.
func TestEachSizeMismatch(t *testing.T) {
	zstd := uattest.Zstd(t)
	path := uattest.Write(t, zstd, t.TempDir(), "pl.test", "", testMsgs[:2])
	setFirstSize(t, path, uint32(len(testMsgs[0])+1))
	p, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Each(zstd, "", func(int, []byte) error { return nil }); err == nil {
		t.Error("size mismatch: err = nil")
	}
}

func TestFix(t *testing.T) {
	cases := []struct {
		name, in string
		head     []string
		body     []string
		encoded  int
		raw      []string
	}{
		{"ASCII stays byte for byte, MIME-Version added, 7bit",
			"Path: a!b\nSubject: plain\n\tcontinued\nX-Odd:  two  spaces \n\nbody\n",
			[]string{"Path: a!b", "Subject: plain", "\tcontinued", "X-Odd:  two  spaces ", "MIME-Version: 1.0",
				"Content-Type: text/plain; charset=UTF-8", "Content-Transfer-Encoding: 7bit"},
			[]string{"body"}, 0, nil},
		{"stale content headers go, MIME-Version stays, 8bit body",
			"Mime-Version: 1.0\nContent-Type: text/plain; charset=iso-8859-2\nContent-Transfer-Encoding: quoted-printable\nSubject: x\n\nżółw\r\n\n",
			[]string{"Mime-Version: 1.0", "Subject: x", "Content-Type: text/plain; charset=UTF-8", "Content-Transfer-Encoding: 8bit"},
			[]string{"żółw", ""}, 0, nil},
		{"a run of non-ASCII words is one encoded-word, ASCII words stay",
			"Subject: Re: Zażółć gęślą jaźń - test ąę\n\nx\n",
			[]string{"Subject: Re:", " =?UTF-8?q?Za=C5=BC=C3=B3=C5=82=C4=87_g=C4=99=C5=9Bl=C4=85_ja=C5=BA=C5=84?= -", " test =?UTF-8?q?=C4=85=C4=99?="},
			[]string{"x"}, 1, nil},
		{"a quoted display name",
			"From: \"Paweł\" <qwak111@go2.pl>\n\nx\n",
			[]string{"From: =?utf-8?q?Pawe=C5=82?= <qwak111@go2.pl>"}, []string{"x"}, 1, nil},
		{"an old-style comment name",
			"From: whitley@usfvax2.UUCP (Jöhn Whitley)\n\nx\n",
			[]string{"From: =?utf-8?q?J=C3=B6hn_Whitley?= <whitley@usfvax2.UUCP>"}, []string{"x"}, 1, nil},
		{"an address net/mail refuses keeps its address",
			"From: RëîÑ <ca...@voyageronline.net>\n\nx\n",
			[]string{"From: =?UTF-8?q?R=C3=AB=C3=AE=C3=91?= <ca...@voyageronline.net>"}, []string{"x"}, 1, nil},
		{"structured fields are never touched",
			"Message-ID: <zażółć@x>\nNewsgroups: pl.test\nReferences: <ą@x>\n\nx\n",
			[]string{"Message-ID: <zażółć@x>", "Newsgroups: pl.test", "References: <ą@x>"}, []string{"x"}, 0,
			[]string{"Message-ID", "References"}},
		{"no body",
			"Subject: only a header\n",
			[]string{"Subject: only a header"}, nil, 0, nil},
	}
	for _, c := range cases {
		head, body, info := Fix([]byte(c.in))
		// the content headers come last in every case; compare the rest
		want := c.head
		if !strings.Contains(c.name, "MIME-Version") && !strings.Contains(c.name, "stale") {
			want = append(append([]string{}, c.head...), "MIME-Version: 1.0", "Content-Type: text/plain; charset=UTF-8")
			cte := "Content-Transfer-Encoding: 7bit"
			if strings.ContainsFunc(strings.Join(c.body, ""), func(r rune) bool { return r > 127 }) {
				cte = "Content-Transfer-Encoding: 8bit"
			}
			want = append(want, cte)
		}
		if !reflect.DeepEqual(head, want) {
			t.Errorf("%s:\n head %q\n want %q", c.name, head, want)
		}
		if !reflect.DeepEqual(body, c.body) {
			t.Errorf("%s: body %q, want %q", c.name, body, c.body)
		}
		if info.Encoded != c.encoded || !reflect.DeepEqual(info.Raw, c.raw) {
			t.Errorf("%s: Encoded %d Raw %q, want %d %q", c.name, info.Encoded, info.Raw, c.encoded, c.raw)
		}
	}
}

// Every encoded field decodes back to the stored text, long ones folded
// into lines of at most 78 bytes.
func TestFixRoundTrip(t *testing.T) {
	long := strings.Repeat("Zażółć gęślą jaźń, ", 12)
	in := "Subject: " + long + "\nOrganization: Klub fanów\nFrom: \"Łukasz Ćwik\" <l@x.pl>, Ola <o@x.pl>\nX-Info: " + strings.Repeat("żółw ", 30) + "\n\nbody\n"
	head, _, info := Fix([]byte(in))
	if info.Encoded != 4 {
		t.Errorf("Encoded = %d, want 4", info.Encoded)
	}
	fields := parseFields(strings.Join(head, "\n"))
	dec := new(mime.WordDecoder)
	want := map[string]string{"Subject": strings.TrimSpace(long), "Organization": "Klub fanów", "X-Info": strings.TrimSpace(strings.Repeat("żółw ", 30))}
	for _, f := range fields {
		for _, l := range f.lines {
			// longer only when the line holds one word: an encoded-word
			// is up to 75 bytes and cannot be split
			if words := strings.Fields(strings.TrimPrefix(l, f.name+":")); len(l) > 78 && len(words) > 1 {
				t.Errorf("line not folded (%d bytes): %q", len(l), l)
			}
			for i := 0; i < len(l); i++ {
				if l[i] >= 0x80 {
					t.Errorf("non-ASCII left in %q", l)
					break
				}
			}
		}
		_, v, _ := strings.Cut(strings.Join(f.lines, ""), ":")
		v = strings.TrimSpace(v)
		if w, ok := want[f.name]; ok {
			if d, err := dec.DecodeHeader(v); err != nil || d != w {
				t.Errorf("%s decodes to %q, %v; want %q", f.name, d, err, w)
			}
		}
		if f.name == "From" {
			list, err := mail.ParseAddressList(v)
			if err != nil || len(list) != 2 || list[0].Name != "Łukasz Ćwik" || list[0].Address != "l@x.pl" || list[1].Name != "Ola" {
				t.Errorf("From %q parses to %v, %v", v, list, err)
			}
		}
	}
}

func TestFold(t *testing.T) {
	for _, c := range []struct {
		in   string
		want []string
	}{
		{"Subject: short", []string{"Subject: short"}},
		{"Subject: " + strings.Repeat("x", 90), []string{"Subject: " + strings.Repeat("x", 90)}},
		{"Subject: " + strings.Repeat("x", 90) + " tail", []string{"Subject: " + strings.Repeat("x", 90), " tail"}},
		{"Subject: " + strings.Repeat("ab ", 30), []string{"Subject: " + strings.TrimSuffix(strings.Repeat("ab ", 23), " "), strings.TrimSuffix(" "+strings.Repeat("ab ", 7), "")}},
	} {
		if got := fold(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("fold(%q)\n = %q\nwant %q", c.in, got, c.want)
		}
	}
}

// A size over MaxMessageSize is an error. A size under it that the data
// does not back costs memory only for the data there is: a 99 KB file
// used to make Each allocate 3 GiB.
func TestEachSizeLimit(t *testing.T) {
	zstd := uattest.Zstd(t)
	path := uattest.Write(t, zstd, t.TempDir(), "pl.test", "", testMsgs[:2])
	saved := MaxMessageSize
	t.Cleanup(func() { MaxMessageSize = saved })

	MaxMessageSize = len(testMsgs[0]) - 1
	p, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	err = p.Each(zstd, "", func(int, []byte) error { return nil })
	p.Close()
	if err == nil || !strings.Contains(err.Error(), "over the limit") {
		t.Errorf("over MaxMessageSize: err = %v", err)
	}

	MaxMessageSize = saved
	setFirstSize(t, path, 200<<20)
	p, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = p.Each(zstd, "", func(int, []byte) error { return nil })
	runtime.ReadMemStats(&after)
	if err == nil || !strings.Contains(err.Error(), "ended early") {
		t.Errorf("size claimed 200 MiB: err = %v", err)
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 32<<20 {
		t.Errorf("size claimed 200 MiB: allocated %d MiB", got>>20)
	}
}
