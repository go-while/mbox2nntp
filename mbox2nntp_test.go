package main

import (
	"bufio"
	"net"
	"os"
	"strings"
	"testing"
)

/*
 * Regression tests for the article-corruption and crash bugs.
 *
 * Each of these was a live defect that silently damaged or dropped articles
 * during the archive.org imports. Before changing any of the code they cover,
 * check that the test still fails against the old behaviour -- that is the
 * whole point of them.
 */

// startFakeNNTP speaks just enough of IHAVE to capture what the client sends,
// so a full article can be checked byte-for-byte without touching a real INN.
func startFakeNNTP(t *testing.T, captured chan<- string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("200 fake ready\r\n"))

		reader := bufio.NewReader(conn)
		var article strings.Builder
		sending := false
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if !sending {
				if strings.HasPrefix(line, "IHAVE ") {
					conn.Write([]byte("335 send it\r\n"))
					sending = true
				}
				continue
			}
			if line == DOT+CRLF {
				conn.Write([]byte("235 thanks\r\n"))
				captured <- article.String()
				return
			}
			article.WriteString(line)
		}
	}()
	return ln.Addr().String()
}

// TestIhaveBodyRoundTrip covers A1 (dot-stuffing) and A3 (trailing CR):
// what IHAVE puts on the wire must un-stuff back to exactly what went in.
func TestIhaveBodyRoundTrip(t *testing.T) {
	globalBools.V = make(map[string]bool)

	captured := make(chan string, 1)
	srvtp, cliwriter, err := connectBackend(startFakeNNTP(t, captured))
	if err != nil {
		t.Fatalf("connectBackend: %v", err)
	}

	head := []string{
		"From: someone@example.invalid",
		"Subject: dot stuffing",
		"Date: Mon, 30 Jun 2003 23:33:23 GMT",
		"Message-ID: <roundtrip@test>",
	}
	body := []string{
		"normal line",
		".",      // lone dot
		".foo",   // one leading dot
		"..bar",  // A1: used to arrive as ".bar"
		"...baz", // A1: used to arrive as "..baz"
		"end",
	}

	if code := ihave(1, "<roundtrip@test>", head, body, srvtp, cliwriter); code != 235 {
		t.Fatalf("ihave returned %d, want 235", code)
	}

	wire := <-captured
	var unstuffed []string
	for _, line := range strings.Split(strings.TrimSuffix(wire, CRLF), CRLF) {
		unstuffed = append(unstuffed, strings.TrimPrefix(line, DOT))
	}

	sep := -1
	for i, line := range unstuffed {
		if line == "" {
			sep = i
			break
		}
	}
	if sep < 0 {
		t.Fatalf("no header/body separator on the wire:\n%q", wire)
	}

	got := unstuffed[sep+1:]
	if len(got) != len(body) {
		t.Fatalf("body has %d lines, sent %d\n got=%q\nwant=%q", len(got), len(body), got, body)
	}
	for i := range body {
		if got[i] != body[i] {
			t.Errorf("body[%d] corrupted: got %q want %q", i, got[i], body[i])
		}
	}
}

// TestReadFileTrailingPeriod covers B1: an article whose last line merely ends
// in a period got no dot terminator, so ReadDotLines failed and it was dropped.
// It also covers A2: the blank header/body separator must not become body[0].
func TestReadFileTrailingPeriod(t *testing.T) {
	globalBools.V = make(map[string]bool)
	worker_channel = make(chan *ArticleItem, 8)

	const article = "From: someone@example.invalid\n" +
		"Subject: trailing period\n" +
		"Date: Mon, 30 Jun 2003 23:33:23 GMT\n" +
		"Message-ID: <trailing@test>\n" +
		"\n" +
		"first body line\n" +
		"ends with a period." // no trailing newline on purpose

	fh, err := os.CreateTemp(t.TempDir(), "article")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := fh.WriteString(article); err != nil {
		t.Fatalf("write: %v", err)
	}
	fh.Close()

	if msgid := read_file(fh.Name()); msgid == "" {
		t.Fatal("B1: article was dropped, read_file returned an empty msgid")
	}

	item := <-worker_channel
	want := []string{"first body line", "ends with a period."}
	if len(item.body) != len(want) {
		t.Fatalf("A2: body=%q want %q", item.body, want)
	}
	for i := range want {
		if item.body[i] != want[i] {
			t.Errorf("A2: body[%d]=%q want %q", i, item.body[i], want[i])
		}
	}
}

// TestStatHelpersOnBadPath covers C1: os.Stat leaves info==nil for any error,
// not just ENOENT, so the old os.IsNotExist(err) check dereferenced nil.
func TestStatHelpersOnBadPath(t *testing.T) {
	fh, err := os.CreateTemp(t.TempDir(), "regular")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	fh.Close()

	// traversing through a regular file yields ENOTDIR, not ENOENT
	notdir := fh.Name() + "/below"
	if fileExists(notdir) {
		t.Errorf("fileExists(%q) = true", notdir)
	}
	if dirExists(notdir) {
		t.Errorf("dirExists(%q) = true", notdir)
	}
}
