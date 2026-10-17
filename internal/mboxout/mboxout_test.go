package mboxout

import (
	"compress/gzip"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// An mbox appears only on Close, gzipped when asked, and is never replaced.
func TestWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "g.mbox.gz")
	w, err := Create(path, 6)
	if err != nil {
		t.Fatal(err)
	}
	head := []string{"Subject: x", "Date: Thu, 8 Dec 2016 12:55:04 +0100"}
	if err := w.Write(head, []string{"From me", ">From you", "ok"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err == nil {
		t.Error("output visible before Close")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(zr)
	want := "From nobody Thu Dec  8 11:55:04 2016\nSubject: x\nDate: Thu, 8 Dec 2016 12:55:04 +0100\n\n>From me\n>>From you\nok\n\n"
	if string(got) != want || zr.Name != "g.mbox" {
		t.Errorf("got %q (name %q)\nwant %q", got, zr.Name, want)
	}
	if _, err := Create(path, 0); err == nil {
		t.Error("an existing mbox was replaced")
	}

	w, err = Create(filepath.Join(dir, "aborted.mbox"), 0)
	if err != nil {
		t.Fatal(err)
	}
	w.Abort()
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("left %d entries, want only g.mbox.gz", len(entries))
	}
}

func TestGzipFlag(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want int
	}{
		{nil, 0},
		{[]string{"-gzip"}, 6},
		{[]string{"-gzip=1"}, 1},
		{[]string{"-gzip=9"}, 9},
		{[]string{"-gzip=true"}, 6},
		{[]string{"-gzip=9", "-gzip=false"}, 0},
	} {
		level := 0
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		fs.Var(GzipFlag{&level}, "gzip", "")
		if err := fs.Parse(tc.args); err != nil || level != tc.want {
			t.Errorf("%q: level %d, %v; want %d", tc.args, level, err, tc.want)
		}
	}
	for _, bad := range []string{"-gzip=0", "-gzip=10", "-gzip=fast"} {
		level := 0
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		fs.Var(GzipFlag{&level}, "gzip", "")
		if err := fs.Parse([]string{bad}); err == nil {
			t.Errorf("%s: no error, level %d", bad, level)
		}
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
		d, ok := ParseDate(in)
		if got := d.UTC().Format("Mon Jan _2 15:04:05 2006"); !ok || got != want {
			t.Errorf("ParseDate(%q) = %q, %t; want %q", in, got, ok, want)
		}
	}
	if _, ok := ParseDate("not a date"); ok {
		t.Error("ParseDate(not a date) succeeded")
	}
}

func TestFromDate(t *testing.T) {
	for _, tc := range []struct {
		head []string
		want string
	}{
		{[]string{"Subject: x", "Date: Thu, 8 Dec 2016 12:55:04 +0100"}, "Thu Dec  8 11:55:04 2016"},
		{[]string{"date:Thu, 8 Dec 2016 12:55:04 +0100"}, "Thu Dec  8 11:55:04 2016"},
		{[]string{"Date:", " Thu, 8 Dec 2016", "\t12:55:04 +0100"}, "Thu Dec  8 11:55:04 2016"}, // folded
		{[]string{"Date: soon"}, "Thu Jan  1 00:00:00 1970"},
		{[]string{"Subject: no date"}, "Thu Jan  1 00:00:00 1970"},
	} {
		if got := FromDate(tc.head); got != tc.want {
			t.Errorf("FromDate(%q) = %q, want %q", tc.head, got, tc.want)
		}
	}
}
