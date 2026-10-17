package unpack

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHas7zEntry(t *testing.T) {
	listing := "\n7-Zip 25.01\n\nListing archive: x.mbox.7z\n\n--\nPath = x.mbox.7z\nType = 7z\n\n" +
		"----------\nPath = x.mbox\nSize = 5\n\nPath = sub/y.mbox\r\nSize = 5\r\n"
	for name, want := range map[string]bool{"x.mbox": true, "x.mbox.7z": false, "y.mbox": false, "sub/y.mbox": true, "": false} {
		if got := Has7zEntry(listing, name); got != want {
			t.Errorf("Has7zEntry(%q) = %t, want %t", name, got, want)
		}
	}
}

const xzContent = "From nobody Thu Dec  8 12:55:04 2016\nSubject: xz\n\nbody\n"

// UnXzTo uses xz, or 7-Zip where there is no xz, as on Windows; both ways
// must give the same file. testdata/x.xz was made by xz -9.
func TestUnXzTo(t *testing.T) {
	dir := t.TempDir()
	ran := 0
	if _, err := FindXz(); err == nil {
		ran++
		dst := filepath.Join(dir, "a")
		if err := UnXzTo("testdata/x.xz", dst); err != nil {
			t.Fatal(err)
		}
		if got, _ := os.ReadFile(dst); string(got) != xzContent {
			t.Errorf("xz: got %q", got)
		}
	}
	if prog, err := Find7z(); err == nil {
		ran++
		dst := filepath.Join(dir, "b")
		// the arguments UnXzTo gives 7-Zip when there is no xz
		if err := RunTo(dst, prog, "e", "-so", "-bd", "-txz", "--", "testdata/x.xz"); err != nil {
			t.Fatal(err)
		}
		if got, _ := os.ReadFile(dst); string(got) != xzContent {
			t.Errorf("7z: got %q", got)
		}
	}
	if ran == 0 {
		if os.Getenv("CI") != "" {
			t.Fatal("neither xz nor 7-Zip found") // the workflow installs them
		}
		t.Skip("neither xz nor 7-Zip found")
	}
	if err := UnXzTo("testdata/no-such.xz", filepath.Join(dir, "c")); err == nil {
		t.Error("missing input: err = nil")
	}
	saved := Xz
	t.Cleanup(func() { Xz = saved })
	Xz = "no-such-xz"
	if err := UnXzTo("testdata/x.xz", filepath.Join(dir, "d")); err == nil {
		t.Error("-xz no-such-xz: err = nil")
	}
}
