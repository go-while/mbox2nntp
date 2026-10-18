package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIsMboxText(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		body string
		want bool
	}{
		{"From -1622751026628752420\nMessage-ID: <a@b>\n\nhi\n", true},
		{"From x", true},
		{"Fro", false},
		{"", false},
		{"Index of comp.sources.unix\nFrom here on\n", false},
		{"\ufeffFrom x\n", false},
		{"from x\n", false},
	} {
		p := filepath.Join(dir, "in.txt")
		if err := os.WriteFile(p, []byte(tc.body), 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := isMboxText(p)
		if err != nil || got != tc.want {
			t.Errorf("%q: got %v, %v; want %v", tc.body, got, err, tc.want)
		}
	}
	if _, err := isMboxText(filepath.Join(dir, "missing.txt")); err == nil {
		t.Error("missing file: no error")
	}
}
