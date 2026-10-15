package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mbox "github.com/go-while/go-lib-mbox2nntp"
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

// writeMbox writes n small messages, about 9 KiB for n=100: more than the
// library's 4 KiB read buffer, so a parser that stops early leaves bytes
// unread at the source.
func writeMbox(t *testing.T, n int) string {
	t.Helper()
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "From nobody Thu Dec  8 12:55:04 2016\nSubject: %d\nMessage-ID: <%d@example>\n\nbody %d\n", i, i, i)
	}
	path := filepath.Join(t.TempDir(), "test.mbox")
	if err := os.WriteFile(path, []byte(b.String()), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

// ReadMessage returns io.EOF both at the end of the file and when it finds no
// separator where a message should start. iterateMbox used to take the second
// for the first, and mark a file done with most of it never imported.
func TestUnreadBytesSeesEarlyStop(t *testing.T) {
	path := writeMbox(t, 100)

	mr, src := openMboxFile(path)
	n := 0
	for {
		msg, err := mr.ReadMessage()
		if err != nil {
			break
		}
		n++
		io.Copy(io.Discard, msg.BodyReader())
	}
	if n != 100 {
		t.Fatalf("read %d messages, want 100", n)
	}
	if u := unreadBytes(src); u != 0 {
		t.Errorf("full read: unreadBytes = %d, want 0", u)
	}

	// Skipping a body leaves the stream inside it: the next ReadMessage
	// finds no separator and returns io.EOF after one message.
	mr, src = openMboxFile(path)
	if _, err := mr.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	if _, err := mr.ReadMessage(); err != io.EOF {
		t.Fatalf("ReadMessage after a skipped body: err = %v, want io.EOF", err)
	}
	if u := unreadBytes(src); u == 0 {
		t.Error("early stop: unreadBytes = 0, want > 0")
	}
}

// An empty mbox used to reach os.Exit(1) and abort the run.
func TestOpenMboxFileEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.mbox")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatal(err)
	}
	mr, src := openMboxFile(path)
	if mr != nil || src != nil {
		t.Errorf("openMboxFile(empty) = %v, %v; want nil, nil", mr, src)
	}
}

func TestCheckExecutable(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "exe")
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plain, nil, 0644); err != nil {
		t.Fatal(err)
	}
	if err := checkExecutable(exe); err != nil {
		t.Errorf("executable file: %v", err)
	}
	for _, p := range []string{plain, dir, filepath.Join(dir, "missing")} {
		if checkExecutable(p) == nil {
			t.Errorf("checkExecutable(%s) = nil, want an error", p)
		}
	}
}

// A CRLF article read straight from an mbox, as on a .nodos2unix run, must
// go out with plain CRLF line endings: no "\r\r\n" in the header or body.
func TestCRLFArticleOnTheWire(t *testing.T) {
	globalBools.V = make(map[string]bool)
	in := "From -7666732389843091160\n" +
		"From: C-ap@clari.net (AP)\r\n" +
		"Newsgroups: clari.world.mideast.iraq\r\n" +
		"Subject: a folded\r\n" +
		"\tsubject line\r\n" +
		"Date: Thu, 5 Aug 2004 21:50:04 EDT\r\n" +
		"Message-ID: <crlf@test>\r\n" +
		"\r\n" +
		"body one\r\n" +
		"body two\r\n"
	mr, err := mbox.CreateMboxStream(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := mr.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	head := msg.Headers()
	raw, err := io.ReadAll(msg.BodyReader())
	if err != nil {
		t.Fatal(err)
	}
	// as iterateMbox does
	var body []string
	for _, l := range strings.SplitAfter(string(raw), "\n") {
		if l != "" {
			body = append(body, strings.TrimRight(l, "\r\n"))
		}
	}

	captured := make(chan string, 1)
	srvtp, cliwriter, err := connectBackend(startFakeNNTP(t, captured))
	if err != nil {
		t.Fatalf("connectBackend: %v", err)
	}
	if code := ihave(1, "<crlf@test>", head, body, srvtp, cliwriter); code != 235 {
		t.Fatalf("ihave returned %d, want 235", code)
	}
	wire := <-captured
	if strings.Contains(strings.ReplaceAll(wire, CRLF, ""), "\r") {
		t.Errorf("stray CR on the wire:\n%q", wire)
	}
	for _, want := range []string{"From: C-ap@clari.net (AP)\r\n", "Subject: a folded\r\n", "\r\n\r\nbody one\r\nbody two\r\n"} {
		if !strings.Contains(wire, want) {
			t.Errorf("wire is missing %q:\n%q", want, wire)
		}
	}
}
