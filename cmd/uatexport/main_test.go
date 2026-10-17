package main

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	mbox "github.com/go-while/go-lib-mbox2nntp"
	"github.com/go-while/mbox2nntp/internal/uattest"
	"github.com/go-while/mbox2nntp/uat"
)

var msgs = []string{
	"Path: a!b\nNewsgroups: pl.test\nSubject: Zażółć\nDate: Thu, 8 Dec 2016 12:55:04 +0100\nMessage-ID: <1@x>\n\nfirst\nFrom here on, quoted:\n>From the archive\n",
	"Path: a!b\nNewsgroups: pl.test\nSubject: two\nDate: not a date\nMessage-ID: <2@x>\n\nFrom nobody Thu Dec  8 12:55:04 2016\nlooks like a separator\n",
	"Path: a!b\nNewsgroups: pl.test\nSubject: three\nMessage-ID: <3@x>\n\nno date, no final newline",
}

func TestExportMbox(t *testing.T) {
	zstd := uattest.Zstd(t)
	dir := t.TempDir()
	in := uattest.Write(t, zstd, dir, "pl.test", "", msgs)
	out := filepath.Join(dir, "out.mbox")
	st, err := export(in, options{format: "mbox", out: out})
	if err != nil {
		t.Fatal(err)
	}
	if st.n != 3 || st.group != "pl.test" || st.encoded != 1 || st.raw != 0 {
		t.Errorf("stats %+v", st)
	}
	data, _ := os.ReadFile(out)
	text := string(data)
	for _, want := range []string{
		"From nobody Thu Dec  8 11:55:04 2016\n", // Date in UTC
		"From nobody Thu Jan  1 00:00:00 1970\n", // no usable Date
		"\n>From here on, quoted:\n>>From the archive\n",
		"\n>From nobody Thu Dec  8 12:55:04 2016\nlooks like a separator\n",
		"Subject: =?UTF-8?q?Za=C5=BC=C3=B3=C5=82=C4=87?=\n",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("mbox lacks %q", want)
		}
	}

	// go-lib-mbox2nntp, mbox2nntp's parser, reads the same three articles
	// back, each header as uat.Fix makes it
	f, _ := os.Open(out)
	defer f.Close()
	mr, err := mbox.CreateMboxStream(bufio.NewReader(f))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; ; i++ {
		m, err := mr.ReadMessage()
		if err == io.EOF {
			if i != len(msgs) {
				t.Errorf("read back %d articles, want %d", i, len(msgs))
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, m.BodyReader())
		if i >= len(msgs) {
			continue
		}
		want, _, _ := uat.Fix([]byte(msgs[i]))
		if got := m.Headers(); !reflect.DeepEqual(got, want) {
			t.Errorf("article %d: headers %q\nwant %q", i, got, want)
		}
	}

	if _, err := export(in, options{format: "mbox", out: out}); err == nil {
		t.Error("an existing mbox was overwritten")
	}
}

func TestExportDir(t *testing.T) {
	zstd := uattest.Zstd(t)
	dir := t.TempDir()
	in := uattest.Write(t, zstd, dir, "pl.test", "", msgs)
	for _, raw := range []bool{false, true} {
		out := filepath.Join(dir, "spool")
		if raw {
			out += "-raw"
		}
		if _, err := export(in, options{format: "dir", out: out, raw: raw}); err != nil {
			t.Fatal(err)
		}
		entries, _ := os.ReadDir(out)
		if len(entries) != len(msgs) {
			t.Fatalf("%d files, want %d", len(entries), len(msgs))
		}
		for i, m := range msgs {
			got, err := os.ReadFile(filepath.Join(out, []string{"1", "2", "3"}[i]))
			if err != nil {
				t.Fatal(err)
			}
			want := m
			if !strings.HasSuffix(want, "\n") {
				want += "\n"
			}
			if !raw {
				head, body, _ := uat.Fix([]byte(m))
				want = strings.Join(head, "\n") + "\n\n" + strings.Join(body, "\n") + "\n"
			}
			if string(got) != want {
				t.Errorf("raw=%t article %d:\n%q\nwant\n%q", raw, i+1, got, want)
			}
		}
		if _, err := export(in, options{format: "dir", out: out}); err == nil {
			t.Error("an existing directory was written into")
		}
	}
}

// A .usenet.xz goes through xz (or 7-Zip) into a temporary directory that
// is removed afterwards.
func TestExportXz(t *testing.T) {
	zstd := uattest.Zstd(t)
	xz, err := exec.LookPath("xz")
	if err != nil {
		t.Skip("xz not found")
	}
	dir := t.TempDir()
	in := uattest.Write(t, zstd, dir, "pl.test", "", msgs)
	if err := exec.Command(xz, in).Run(); err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	st, err := export(in+".xz", options{format: "mbox", out: filepath.Join(dir, "x.mbox"), tmpdir: tmp})
	if err != nil || st.n != len(msgs) {
		t.Fatalf("export = %+v, %v", st, err)
	}
	if left, _ := os.ReadDir(tmp); len(left) != 0 {
		t.Errorf("tmpdir not cleaned: %v", left)
	}
	if _, err := export(filepath.Join(dir, "x.mbox.xz"), options{format: "mbox"}); err == nil {
		t.Error("a .xz that is not .usenet.xz: err = nil")
	}
}

func TestParseDate(t *testing.T) {
	for in, want := range map[string]string{
		"Thu, 8 Dec 2016 12:55:04 +0100":     "Thu Dec  8 11:55:04 2016",
		"Mon, 12-Jan-87 19:53:54 EST":        "Mon Jan 12 19:53:54 1987",
		"Wednesday, 9 Jul 1986 10:11:12 EST": "Wed Jul  9 10:11:12 1986",
		"9 Jun 88 01:02:03":                  "Thu Jun  9 01:02:03 1988",
		"1989/03/31":                         "Fri Mar 31 00:00:00 1989",
	} {
		d, ok := parseDate(in)
		if got := d.UTC().Format("Mon Jan _2 15:04:05 2006"); !ok || got != want {
			t.Errorf("parseDate(%q) = %q, %t; want %q", in, got, ok, want)
		}
	}
	if _, ok := parseDate("not a date"); ok {
		t.Error("parseDate(not a date) succeeded")
	}
}
