//go:build unix

package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// A new database gets 0666 less the umask, its -wal and -shm files too,
// so that readers in the group can open it.
func TestDBMode(t *testing.T) {
	old := syscall.Umask(0o002)
	defer syscall.Umask(old)
	path := filepath.Join(t.TempDir(), "x.sqlite")
	d, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.setItem(item{ID: "it"}, stDone, 0, 0, ""); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if m := fi.Mode().Perm(); m != 0o664 {
			t.Errorf("%s: mode %o, want 664", filepath.Base(p), m)
		}
	}
}
