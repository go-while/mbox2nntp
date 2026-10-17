package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseArgsFlags(t *testing.T) {
	o, err := parseArgs("m", []string{"-server", "news:119", "-workers", "4", "-tmpdir", "/t",
		"-dry-run", "-keep-binary", "-no-dos2unix", "-dos2unix", "/opt/d2u", "-7z", "/opt/7zz", "-xz", "/opt/xz", "-zstd", "/opt/zstd", "-timeout", "30s",
		"-max-tries", "3", "-prehash=false", "-lax-msgid=false", "a.mbox.gz"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	want := options{input: "a.mbox.gz", server: "news:119", workers: 4, tmpdir: "/t", dryRun: true,
		keepBinary: true, noDos2unix: true, dos2unix: "/opt/d2u", sevenZip: "/opt/7zz", xz: "/opt/xz", zstd: "/opt/zstd", timeout: 30 * time.Second,
		maxTries: 3, prehash: false, laxMsgid: false}
	if *o != want {
		t.Errorf("got %+v\nwant %+v", *o, want)
	}
}

func TestParseArgsDefaults(t *testing.T) {
	o, err := parseArgs("m", []string{"-server", "news:119", "a.mbox"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	want := *defaultOptions()
	want.input, want.server = "a.mbox", "news:119"
	if *o != want {
		t.Errorf("got %+v\nwant %+v", *o, want)
	}
	// the defaults are what the importer did before there were flags
	if !o.prehash || !o.laxMsgid || o.keepBinary || o.workers != 1 || o.tmpdir != "." ||
		o.timeout != 2*time.Minute || o.maxTries != 10 || o.dos2unix != "" {
		t.Errorf("defaults changed: %+v", *o)
	}
}

// The positional form keeps working, so existing scripts do not break.
func TestParseArgsLegacy(t *testing.T) {
	nodrop := filepath.Join(t.TempDir(), ".noDROPBINARY")
	if err := os.WriteFile(nodrop, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	o, err := parseArgs("m", []string{"a.mbox.zip", "news:119", "8", "/t", "DRYRUN", "-", nodrop}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if !o.legacy || o.input != "a.mbox.zip" || o.server != "news:119" || o.workers != 8 ||
		o.tmpdir != "/t" || !o.dryRun || !o.keepBinary {
		t.Errorf("got %+v", *o)
	}
	o, err = parseArgs("m", []string{"a.mbox.zip", "news:119", "2", ".", "send", "-", "/does/not/exist"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if o.dryRun || o.keepBinary || o.tmpdir != "." {
		t.Errorf("got %+v", *o)
	}
}

func TestParseArgsErrors(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"a.mbox"},
		{"-server", "news:119"},
		{"-dry-run"},
		{"-server", "news:119", "a.mbox", "b.mbox"},
		{"a.mbox", "-server", "news:119"}, // flags after INPUT
		{"-server", "news:119", "-workers", "0", "a.mbox"},
		{"-server", "news:119", "-timeout", "0s", "a.mbox"},
		{"-server", "news:119", "-max-tries", "0", "a.mbox"},
		{"-server", "news:119", "-bogus", "a.mbox"},
		{"a.mbox", "news:119", "many"},
		{"a.mbox", "news:119", "1", ".", "DRYRUN", "-", "x", "extra"},
	} {
		if o, err := parseArgs("m", args, &bytes.Buffer{}); err == nil {
			t.Errorf("parseArgs(%q) = %+v, want an error", args, *o)
		}
	}
}

func TestParseArgsHelp(t *testing.T) {
	var out bytes.Buffer
	if _, err := parseArgs("mbox2nntp", []string{"-h"}, &out); err != flag.ErrHelp {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
	for _, s := range []string{"Usage: mbox2nntp [flags] -server HOST:PORT INPUT", "-keep-binary", "-no-dos2unix", "NAME.mbox.7z", "NAME.usenet"} {
		if !strings.Contains(out.String(), s) {
			t.Errorf("help lacks %q:\n%s", s, out.String())
		}
	}
}

func TestOptionsApplyDirectory(t *testing.T) {
	defer func(w int, s, d string, b bool) { MaxWorkers, nntpserv, DOS2UNIX, DROPBINARY = w, s, d, b }(MaxWorkers, nntpserv, DOS2UNIX, DROPBINARY)
	dir := t.TempDir()
	o := defaultOptions()
	o.input, o.server, o.keepBinary = dir, "news:119", true
	o.apply()
	if o.input != dir+"/" {
		t.Errorf("directory input = %q, want a trailing /", o.input)
	}
	if nntpserv != "news:119" || DROPBINARY {
		t.Errorf("globals not applied: server=%q DROPBINARY=%t", nntpserv, DROPBINARY)
	}
}

func TestParseArgsDryRunNeedsNoServer(t *testing.T) {
	o, err := parseArgs("m", []string{"-dry-run", "a.mbox"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if !o.dryRun || o.server != "" {
		t.Errorf("got %+v", *o)
	}
}
