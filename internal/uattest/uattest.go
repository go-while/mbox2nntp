// Package uattest writes small UAT .usenet files for tests: the messages
// and the files package uat reads, the other databases left empty. The
// toolkit's own programs cannot use them.
package uattest

import (
	"bytes"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Files are the files of a version 3 package, in order.
var Files = []string{
	"name", "desc_short", "desc_long", "conndata", "connmeta", "lexdata", "lexmeta",
	"lexhash", "lexhashdata", "lexhit", "lexstr", "middata", "midmeta", "midhash",
	"midhashdata", "strings", "strmeta", "toplevel", "zdata", "zmeta", "zdict",
	"lexdist", "lexdistmeta", "prefix", "msgid.codebook",
}

// Zstd returns the zstd program, skipping the test without one, or failing
// it under CI, whose workflow installs zstd.
func Zstd(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("zstd")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("zstd not found")
		}
		t.Skip("zstd not found")
	}
	return path
}

// Write writes a package holding msgs to a file in dir and returns its
// path. Each message is compressed on its own against one dictionary, as
// the toolkit stores them; a plain file serves as a zstd dictionary as
// well as a trained one.
func Write(t *testing.T, zstd, dir, name, desc string, msgs []string) string {
	t.Helper()
	dict := filepath.Join(dir, "dict")
	if err := os.WriteFile(dict, []byte("Path: Newsgroups: Subject: Message-ID: Date: From: References: "), 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(dict)
	var zdata, zmeta bytes.Buffer
	for _, m := range msgs {
		cmd := exec.Command(zstd, "-q", "-c", "-D", dict)
		cmd.Stdin = strings.NewReader(m)
		c, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		binary.Write(&zmeta, binary.LittleEndian, uint64(zdata.Len()))
		binary.Write(&zmeta, binary.LittleEndian, uint32(len(m)))
		binary.Write(&zmeta, binary.LittleEndian, uint32(len(c)))
		zdata.Write(c)
	}
	d, _ := os.ReadFile(dict)
	return WriteRaw(t, filepath.Join(dir, strings.TrimSpace(name)+".usenet"), 3, map[string][]byte{
		"name": []byte(name), "desc_short": []byte(desc), "zdata": zdata.Bytes(), "zmeta": zmeta.Bytes(), "zdict": d,
	})
}

// WriteRaw writes a package with the given version and file contents to
// path; files not in contents are empty.
func WriteRaw(t *testing.T, path string, version byte, contents map[string][]byte) string {
	t.Helper()
	var b bytes.Buffer
	b.WriteString("\x00Usenet")
	b.WriteByte(version)
	for _, f := range Files {
		binary.Write(&b, binary.LittleEndian, uint64(len(contents[f])))
	}
	for _, f := range Files {
		b.Write(contents[f])
		for b.Len()%8 != 0 {
			b.WriteByte(0)
		}
	}
	if err := os.WriteFile(path, b.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
