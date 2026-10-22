package unpack

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

// The test binary run as a program that writes to its standard output
// until the pipe breaks, for TestRunToWriteError.
func init() {
	if os.Getenv("UNPACK_TEST_WRITE_FOREVER") == "1" {
		buf := make([]byte, 64*1024)
		for {
			if _, err := os.Stdout.Write(buf); err != nil {
				os.Exit(1)
			}
		}
	}
}

// When dst cannot be written, RunTo kills the program; the error must be
// the write error, not the program's "signal: killed".
func TestRunToWriteError(t *testing.T) {
	t.Setenv("UNPACK_TEST_WRITE_FOREVER", "1")
	prog, err := os.Executable()
	if err != nil {
		t.Skip(err)
	}
	cases := map[string]func(error) bool{
		filepath.Join(t.TempDir(), "no-such-dir", "x"): func(err error) bool { return errors.Is(err, fs.ErrNotExist) },
	}
	if runtime.GOOS == "linux" {
		// writes fail with ENOSPC, as on a full disk
		cases["/dev/full"] = func(err error) bool { return errors.Is(err, syscall.ENOSPC) }
	}
	for dst, want := range cases {
		err := RunTo(dst, prog, "-test.run=^$")
		if err == nil || !want(err) || strings.Contains(err.Error(), "killed") {
			t.Errorf("RunTo(%s) = %v, want the write error", dst, err)
		}
	}
}

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
