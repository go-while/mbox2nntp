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
	"time"

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

// nntpHandler scripts one backend connection. startScriptedNNTP hands the
// i-th accepted connection to handlers[i]; the last handler repeats.
type nntpHandler func(c net.Conn, r *bufio.Reader)

func startScriptedNNTP(t *testing.T, handlers ...nntpHandler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for i := 0; ; i++ {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			h := handlers[len(handlers)-1]
			if i < len(handlers) {
				h = handlers[i]
			}
			go func() {
				defer c.Close()
				h(c, bufio.NewReader(c))
			}()
		}
	}()
	return ln.Addr().String()
}

func say(c net.Conn, line string) { c.Write([]byte(line + CRLF)) }

// expectIHAVE reads one command line and returns the message-id, or "" if
// the command was not IHAVE (QUIT) or the client went away.
func expectIHAVE(r *bufio.Reader) string {
	line, err := r.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "IHAVE ") {
		return ""
	}
	return strings.Fields(line)[1]
}

func readArticle(r *bufio.Reader) {
	for {
		line, err := r.ReadString('\n')
		if err != nil || line == DOT+CRLF {
			return
		}
	}
}

// waitForClose reports whether the client closed the connection within d.
func waitForClose(c net.Conn, r *bufio.Reader, d time.Duration) bool {
	c.SetReadDeadline(time.Now().Add(d))
	for {
		if _, err := r.ReadString('\n'); err != nil {
			return err == io.EOF
		}
	}
}

// accepting takes every article and reports its message-id on got.
func accepting(got chan<- string) nntpHandler {
	return func(c net.Conn, r *bufio.Reader) {
		say(c, "200 fake ready")
		for {
			msgid := expectIHAVE(r)
			if msgid == "" {
				return
			}
			say(c, "335 send it")
			readArticle(r)
			say(c, "235 thanks")
			got <- msgid
		}
	}
}

// shortTimeouts makes failures fast for one test.
func shortTimeouts(t *testing.T) {
	timeout, delay, tries := NNTP_TIMEOUT, retryDelay, MAX_TRIES
	NNTP_TIMEOUT, retryDelay, MAX_TRIES = 300*time.Millisecond, 10*time.Millisecond, 3
	t.Cleanup(func() { NNTP_TIMEOUT, retryDelay, MAX_TRIES = timeout, delay, tries })
}

var testArticleHead = []string{
	"From: someone@example.invalid",
	"Subject: session test",
	"Newsgroups: test.group",
	"Date: Mon, 30 Jun 2003 23:33:23 GMT",
	"Message-ID: <session@test>",
}

// D3: ReadCodeLine(20) accepts 200-209, so a greeting such as "205 closing
// connection" passed it, and connectBackend returned (nil, nil, nil); the
// worker then used the nil conn. Other codes already came back as errors.
func TestConnectBackendUnexpectedGreeting(t *testing.T) {
	for _, greeting := range []string{"205 closing connection", "202 odd", "250 surprise", "400 go away"} {
		addr := startScriptedNNTP(t, func(c net.Conn, r *bufio.Reader) { say(c, greeting) })
		srvtp, w, err := connectBackend(addr)
		if err == nil || srvtp != nil || w != nil {
			t.Errorf("%q: connectBackend = %v, %v, %v; want nil, nil, an error", greeting, srvtp, w, err)
		}
	}
}

// B3, D2: a connection that fails while ihave waits for a reply must return
// -999 (requeue, reconnect), not 0, which dropped the article and kept the
// dead connection. A server refusing the article keeps its own code.
func TestIhaveResponseCodes(t *testing.T) {
	shortTimeouts(t)
	cases := []struct {
		name string
		h    nntpHandler
		want int
	}{
		{"closed after IHAVE", func(c net.Conn, r *bufio.Reader) {
			say(c, "200 ok")
			expectIHAVE(r)
		}, -999},
		{"closed after the article", func(c net.Conn, r *bufio.Reader) {
			say(c, "200 ok")
			expectIHAVE(r)
			say(c, "335 send it")
			readArticle(r)
		}, -999},
		{"silent after the article", func(c net.Conn, r *bufio.Reader) {
			say(c, "200 ok")
			expectIHAVE(r)
			say(c, "335 send it")
			readArticle(r)
			waitForClose(c, r, 5*time.Second)
		}, -999},
		{"garbled reply", func(c net.Conn, r *bufio.Reader) {
			say(c, "200 ok")
			expectIHAVE(r)
			say(c, "what?")
		}, -999},
		{"435 duplicate", func(c net.Conn, r *bufio.Reader) {
			say(c, "200 ok")
			expectIHAVE(r)
			say(c, "435 have it")
		}, 435},
		{"436 later", func(c net.Conn, r *bufio.Reader) {
			say(c, "200 ok")
			expectIHAVE(r)
			say(c, "436 throttled")
		}, 436},
		{"437 rejected", func(c net.Conn, r *bufio.Reader) {
			say(c, "200 ok")
			expectIHAVE(r)
			say(c, "335 send it")
			readArticle(r)
			say(c, "437 no thanks")
		}, 437},
	}
	for _, tc := range cases {
		srvtp, w, err := connectBackend(startScriptedNNTP(t, tc.h))
		if err != nil {
			t.Fatalf("%s: connectBackend: %v", tc.name, err)
		}
		done := make(chan int, 1)
		go func() { done <- ihave(1, "<session@test>", testArticleHead, []string{"body"}, srvtp, w) }()
		select {
		case got := <-done:
			if got != tc.want {
				t.Errorf("%s: ihave = %d, want %d", tc.name, got, tc.want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: ihave still blocked after 5s", tc.name)
		}
		srvtp.Close()
	}
}

// setupWorker gives one worker a fresh queue holding one article.
func setupWorker(t *testing.T, addr string) {
	t.Helper()
	shortTimeouts(t)
	globalBools.V = make(map[string]bool)
	globalCounter = GlobalCounter{v: make(map[string]int)}
	worker_channel = make(chan *ArticleItem, 10)
	requeue_channel = make(chan *ArticleItem, 10)
	saved := nntpserv
	nntpserv = addr
	t.Cleanup(func() { nntpserv = saved })
	worker_channel <- &ArticleItem{msgid: "<session@test>", head: testArticleHead, body: []string{"body"}}
	globalCounter.setCounter("msgnums", 1)
	globalBools.SetGB("process", true)
	globalBools.SetGB("feeder_done", true)
}

func runWorker(t *testing.T) {
	t.Helper()
	go worker(1)
	deadline := time.Now().Add(10 * time.Second)
	for globalCounter.getCounter("worker_done") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("worker did not finish within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// B2, B3, D1, D2 together: the server answers 436, then drops the
// connection, then goes silent, then takes the article. It must arrive
// exactly once, each dead connection must be closed by the client, and
// nothing may count as an error.
func TestWorkerSurvivesASessionGoingWrong(t *testing.T) {
	got := make(chan string, 10)
	closed := make(chan bool, 1)
	addr := startScriptedNNTP(t,
		func(c net.Conn, r *bufio.Reader) {
			say(c, "200 ok")
			expectIHAVE(r)
			say(c, "436 throttled")
			expectIHAVE(r)
			say(c, "335 send it")
			readArticle(r)
		},
		func(c net.Conn, r *bufio.Reader) {
			say(c, "200 ok")
			expectIHAVE(r)
			say(c, "335 send it")
			readArticle(r)
			closed <- waitForClose(c, r, 5*time.Second)
		},
		accepting(got),
	)
	setupWorker(t, addr)
	runWorker(t)

	select {
	case ok := <-closed:
		if !ok {
			t.Error("the client never closed the connection that went silent")
		}
	case <-time.After(10 * time.Second):
		t.Error("the worker never opened a second connection")
	}
	if n := len(got); n != 1 {
		t.Errorf("article arrived %d times, want 1", n)
	}
	for name, want := range map[string]int{"sent": 1, "wnntperrcnt": 0, "msgnums": 0} {
		if v := globalCounter.getCounter(name); v != want {
			t.Errorf("counter %s = %d, want %d", name, v, want)
		}
	}
}

// B2: a server that never stops answering 436 must cost an error after
// MAX_TRIES, so the file is not marked done, and the worker must still finish.
func TestWorkerGivesUpAfterMaxTries(t *testing.T) {
	ihaves := make(chan string, 20)
	addr := startScriptedNNTP(t, func(c net.Conn, r *bufio.Reader) {
		say(c, "200 ok")
		for {
			msgid := expectIHAVE(r)
			if msgid == "" {
				return
			}
			ihaves <- msgid
			say(c, "436 throttled")
		}
	})
	setupWorker(t, addr)
	runWorker(t)

	if n := len(ihaves); n != MAX_TRIES+1 {
		t.Errorf("IHAVE sent %d times, want %d", n, MAX_TRIES+1)
	}
	for name, want := range map[string]int{"sent": 0, "wnntperrcnt": 1, "msgnums": 0} {
		if v := globalCounter.getCounter(name); v != want {
			t.Errorf("counter %s = %d, want %d", name, v, want)
		}
	}
}
