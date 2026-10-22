package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	mbox "github.com/go-while/go-lib-mbox2nntp"
	"github.com/go-while/mbox2nntp/internal/unpack"
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
// what IHAVE puts on the wire must un-stuff back to exactly what went in,
// header lines included (RFC 3977 3.1.1 stuffs every line).
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
		".X-Dot: a header line starting with a dot",
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

	if !slices.Contains(unstuffed[:sep], head[4]) {
		t.Errorf("header line %q lost its dot on the wire:\n%q", head[4], unstuffed[:sep])
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
	globalCounter = GlobalCounter{v: make(map[string]int)}
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

// Directory mode read every file as dot-stuffed wire format, so in a plain
// article a body line holding only "." ended the article and "..x" lost a
// dot. A plain file is now read as it is; a file in wire format (INN's
// wireformat spool, timehash), which ends in a lone "." line, is still
// dot-decoded.
func TestReadFileDotLines(t *testing.T) {
	const head = "From: someone@example.invalid\n" +
		"Subject: dots\n" +
		"Date: Mon, 30 Jun 2003 23:33:23 GMT\n" +
		"Message-ID: <dots@test>\n" +
		"\n"
	plainBody := []string{"", ".", "..two dots", ".one dot", "last"}
	cases := []struct {
		name, file string
		want       []string
	}{
		{"plain, LF", head + strings.Join(plainBody, "\n") + "\n", plainBody},
		{"plain, CRLF", strings.ReplaceAll(head+strings.Join(plainBody, "\n")+"\n", "\n", "\r\n"), plainBody},
		{"plain, no final newline", head + strings.Join(plainBody, "\n"), plainBody},
		{"wire format, CRLF", strings.ReplaceAll(head+"\n..\n...two dots\n..one dot\nlast\n.\n", "\n", "\r\n"), plainBody},
		{"wire format, LF", head + "\n..\n...two dots\n..one dot\nlast\n.\n", plainBody},
		{"wire format, no newline after the dot", head + "\n..\n...two dots\n..one dot\nlast\n.", plainBody},
	}
	for _, c := range cases {
		globalBools.V = make(map[string]bool)
		globalCounter = GlobalCounter{v: make(map[string]int)}
		worker_channel = make(chan *ArticleItem, 8)
		fp := filepath.Join(t.TempDir(), "1")
		if err := os.WriteFile(fp, []byte(c.file), 0o644); err != nil {
			t.Fatal(err)
		}
		if msgid := read_file(fp); msgid != "<dots@test>" {
			t.Errorf("%s: read_file = %q", c.name, msgid)
			continue
		}
		item := <-worker_channel
		if !reflect.DeepEqual(item.body, c.want) {
			t.Errorf("%s: body %q\nwant %q", c.name, item.body, c.want)
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
	defer src.Close()
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
	defer src.Close()
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

// proc closes the mbox before main renames it to .done or removes it:
// Windows refuses both on an open file.
func TestEOFReaderCloseReleasesFile(t *testing.T) {
	path := writeMbox(t, 3)
	mr, src := openMboxFile(path)
	for {
		msg, err := mr.ReadMessage()
		if err != nil {
			break
		}
		io.Copy(io.Discard, msg.BodyReader())
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".done"); err != nil {
		t.Fatalf("rename after Close: %v", err)
	}
	if err := os.Remove(path + ".done"); err != nil {
		t.Fatalf("remove after Close: %v", err)
	}
}

// dos2unix only runs when -dos2unix names a program. By default the input,
// a plain .mbox included, is read as it is and never rewritten.
func TestDos2unixOffByDefault(t *testing.T) {
	defer func(d string, n bool) { DOS2UNIX, NODOS2UNIX = d, n }(DOS2UNIX, NODOS2UNIX)
	DOS2UNIX, NODOS2UNIX = defaultOptions().dos2unix, false
	path := filepath.Join(t.TempDir(), "crlf.mbox")
	data := "From nobody Thu Dec  8 12:55:04 2016\r\nSubject: s\r\n\r\nbody\r\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
	dos2unix(path)
	if got, _ := os.ReadFile(path); string(got) != data {
		t.Errorf("input changed without -dos2unix: %q", got)
	}
}

// A CRLF article read straight from an mbox, as on any run without -dos2unix, must
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

// shortTimeouts makes failures fast for one test. Only a server that goes
// silent waits out NNTP_TIMEOUT; every other exchange must fit in it on a
// slow CI runner under -race, so it is not much shorter than 2s. The
// tests' own limits (waitForClose, "still blocked", runWorker) are well
// above it, and are only reached when a test fails.
func shortTimeouts(t *testing.T) {
	timeout, delay, tries := NNTP_TIMEOUT, retryDelay, MAX_TRIES
	NNTP_TIMEOUT, retryDelay, MAX_TRIES = 2*time.Second, 10*time.Millisecond, 3
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
			waitForClose(c, r, 15*time.Second)
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
		case <-time.After(15 * time.Second):
			t.Fatalf("%s: ihave still blocked after 15s", tc.name)
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
	saved := nntpserv
	nntpserv = addr
	t.Cleanup(func() { nntpserv = saved })
	worker_channel <- &ArticleItem{msgid: "<session@test>", head: testArticleHead, body: []string{"body"}}
	globalCounter.setCounter("msgnums", 1)
	globalBools.SetGB("feeder_done", true)
}

func runWorker(t *testing.T) {
	t.Helper()
	go worker(1)
	deadline := time.Now().Add(30 * time.Second)
	for globalCounter.getCounter("worker_done") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("worker did not finish within 30s")
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
			closed <- waitForClose(c, r, 15*time.Second)
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
	case <-time.After(30 * time.Second):
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

// The totals are final once worker_done counts the worker: finishFeeding
// and main read them then. The worker used to count itself done first, and
// a run could read 0/0, or miss its errors and mark the input done.
func TestWorkerCountsBeforeDone(t *testing.T) {
	saved := DRYRUN
	t.Cleanup(func() { DRYRUN = saved })
	DRYRUN = true             // ihave answers 235 without a server
	for i := 0; i < 50; i++ { // each takes a tick of the worker, 100 ms
		setupWorker(t, "")
		returned := make(chan bool)
		go func() { worker(1); close(returned) }()
		for globalCounter.getCounter("worker_done") != 1 {
			runtime.Gosched()
		}
		sent, total := globalCounter.getCounter("sent"), globalCounter.getCounter("total")
		<-returned // before the next round replaces the counters
		if sent != 1 || total != 1 {
			t.Fatalf("run %d: worker done with sent=%d total=%d, want 1/1", i, sent, total)
		}
	}
}

// needGNUFindSort skips the comparisons with find.sh unless there is a
// find and a GNU sort to compare with: Windows has no sh and a find.exe of
// its own, and macOS sort -V is BSD's.
func needGNUFindSort(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("needs sh, find and GNU sort -V")
	}
	if _, err := exec.LookPath("find"); err != nil {
		t.Skip("find not installed")
	}
	if out, err := exec.Command("sort", "--version").Output(); err != nil || !strings.Contains(string(out), "GNU coreutils") {
		t.Skip("sort is not GNU coreutils sort")
	}
}

// walkFiles replaces /bin/find.sh, which was
// `find "$1" -mindepth 1 -type f | sort -V`. Same files, same order.
func TestWalkFilesMatchesFindSh(t *testing.T) {
	needGNUFindSort(t)
	root := t.TempDir()
	files := []string{
		"comp/lang/go/1", "comp/lang/go/2", "comp/lang/go/10", "comp/lang/go/100",
		"comp/lang/go/9", "comp/lang/go/.overview", "comp/lang/c/3", "comp/lang/c2/1",
		"comp/lang/10", "comp/lang/2", "alt/a9", "alt/a10", "alt/a2b", "alt/b",
		"alt/007", "alt/7", "alt/x.tar.gz", "alt/x", "alt/x~1", "alt/a.b", "alt/a/1",
		"comp/lang/c++/5", "comp/lang/c.d/2", "comp/lang/c-x/1", "comp/lang/Z/1",
		"comp/lang/go/1.1", "comp/lang/go/1.10", "comp/lang/go/1.2",
	}
	for _, f := range files {
		p := filepath.Join(root, f)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	// find -type f skips symlinks, and does not descend into linked dirs
	os.Symlink(filepath.Join(root, "comp/lang/go/1"), filepath.Join(root, "alt/link"))
	os.Symlink(filepath.Join(root, "comp"), filepath.Join(root, "alt/linkdir"))

	out, err := exec.Command("sh", "-c", `find "$1" -mindepth 1 -type f | sort -V`, "sh", root).Output()
	if err != nil {
		t.Skipf("find | sort -V: %v", err)
	}
	want := strings.Split(strings.TrimSpace(string(out)), "\n")

	var got []string
	if err := walkFiles(root, func(p string) { got = append(got, p) }); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("walkFiles order differs from find.sh\ngot:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// The usenet-* .mbox.gz archives hold one gzip member per article.
func TestGunzipToConcatenatedMembers(t *testing.T) {
	dir := t.TempDir()
	var gz bytes.Buffer
	for _, part := range []string{"From a\nfirst\n", "From b\nsecond\n"} {
		w := gzip.NewWriter(&gz)
		w.Write([]byte(part))
		w.Close()
	}
	src := filepath.Join(dir, "x.mbox.gz")
	if err := os.WriteFile(src, gz.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "x.mbox")
	if err := gunzipTo(src, dst); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(dst)
	if want := "From a\nfirst\nFrom b\nsecond\n"; string(got) != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// unzipTo and unpack.Un7zTo extract only the entry named like dst, overwriting a
// file left by an earlier run, and fail when the archive lacks it.
func TestUnzipTo(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "x.mbox.zip")
	fh, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(fh)
	for name, body := range map[string]string{"x.mbox": "From nobody\nmbox\n", "other.txt": "other\n"} {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		io.WriteString(w, body)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	fh.Close()
	testExtractor(t, unzipTo, archive, "From nobody\nmbox\n")
}

func TestUn7zTo(t *testing.T) {
	if _, err := unpack.Find7z(); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal(err) // the workflow installs 7-Zip
		}
		t.Skip(err)
	}
	dir := t.TempDir()
	data, err := os.ReadFile("testdata/x.mbox.7z") // made by 7z a -m0=LZMA2, with other.txt
	if err != nil {
		t.Fatal(err)
	}
	// A name 7z must not read as a switch.
	archive := filepath.Join(dir, "-x.mbox.7z")
	if err := os.WriteFile(archive, data, 0644); err != nil {
		t.Fatal(err)
	}
	testExtractor(t, unpack.Un7zTo, archive, "From nobody Thu Dec  8 12:55:04 2016\nSubject: 7z\nMessage-ID: <7z@test>\n\nbody\n")

	// A damaged archive is an error, not a short mbox.
	bad := append([]byte(nil), data...)
	bad[40] ^= 0xff // in the packed data, which starts after the 32-byte signature header
	if err := os.WriteFile(archive, bad, 0644); err != nil {
		t.Fatal(err)
	}
	if err := unpack.Un7zTo(archive, filepath.Join(dir, "x.mbox")); err == nil {
		t.Error("damaged archive: err = nil")
	}

	saved := unpack.SevenZip
	t.Cleanup(func() { unpack.SevenZip = saved })
	unpack.SevenZip = "no-such-7z"
	if err := unpack.Un7zTo(archive, filepath.Join(dir, "x.mbox")); err == nil {
		t.Error("-7z no-such-7z: err = nil")
	}
}

func testExtractor(t *testing.T, fn func(archive, dst string) error, archive, want string) {
	t.Helper()
	out := t.TempDir()
	dst := filepath.Join(out, "x.mbox")
	if err := os.WriteFile(dst, []byte("left by an interrupted run, longer than the real file"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := fn(archive, dst); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(dst); string(got) != want {
		t.Errorf("extracted %q, want %q", got, want)
	}
	if entries, _ := os.ReadDir(out); len(entries) != 1 {
		t.Errorf("%d files in tmpdir, want only x.mbox", len(entries))
	}
	if err := fn(archive, filepath.Join(out, "y.mbox")); err == nil {
		t.Error("missing entry: err = nil")
	}
	if err := fn(filepath.Join(out, "x.mbox"), filepath.Join(out, "z.mbox")); err == nil {
		t.Error("not an archive: err = nil")
	}
}

func TestExtractedPathAndArgPath(t *testing.T) {
	savedTmp, savedDir := tmpdir, unpackDir
	t.Cleanup(func() { tmpdir, unpackDir = savedTmp, savedDir })
	tmpdir, unpackDir = t.TempDir(), ""

	// the user's own x.mbox next to x.mbox.gz, in tmpdir
	mine := filepath.Join(tmpdir, "x.mbox")
	if err := os.WriteFile(mine, []byte("not from the archive"), 0644); err != nil {
		t.Fatal(err)
	}
	var dirs []string
	for _, tc := range []struct{ archive, ext, want string }{
		{"x.mbox.gz", ".gz", "x.mbox"},
		{"/abs/dir/x.mbox.zip", ".zip", "x.mbox"},
		{"rel/dir/a.(1).mbox.7z", ".7z", "a.(1).mbox"},
		{"x.gz.mbox.gz", ".gz", "x.gz.mbox"},
	} {
		got, err := extractedPath(tc.archive, tc.ext)
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.Dir(got)
		dirs = append(dirs, dir)
		if filepath.Base(got) != tc.want || filepath.Dir(dir) != tmpdir || !strings.HasPrefix(filepath.Base(dir), ".mbox2nntp-") {
			t.Errorf("extractedPath(%q, %q) = %q, want %s in a new directory in tmpdir", tc.archive, tc.ext, got, tc.want)
		}
	}
	if dirs[0] != dirs[3] {
		t.Errorf("one run, two directories: %q, %q", dirs[0], dirs[3])
	}
	// an extractor writes into the run's directory, and the cleanup removes
	// that directory and nothing else
	dst, _ := extractedPath("x.mbox.gz", ".gz")
	if err := unpack.WriteTo(dst, strings.NewReader("from the archive")); err != nil {
		t.Fatal(err)
	}
	removeUnpacked()
	if got, err := os.ReadFile(mine); err != nil || string(got) != "not from the archive" {
		t.Errorf("the user's x.mbox: %q, %v", got, err)
	}
	if entries, _ := os.ReadDir(tmpdir); len(entries) != 1 {
		t.Errorf("tmpdir holds %d entries after the cleanup, want only x.mbox", len(entries))
	}
	if unpackDir != "" {
		t.Errorf("unpackDir = %q after the cleanup", unpackDir)
	}
	removeUnpacked() // nothing to remove: no-op

	tmpdir = filepath.Join(tmpdir, "missing")
	if _, err := extractedPath("x.mbox.gz", ".gz"); err == nil {
		t.Error("tmpdir that does not exist: err = nil")
	}
	unpackDir = ""

	for in, want := range map[string]string{"-rf.mbox": "./-rf.mbox", "a.mbox": "a.mbox", "/x/-y": "/x/-y"} {
		if got := argPath(in); got != want {
			t.Errorf("argPath(%q) = %q, want %q", in, got, want)
		}
	}
}

// Generated names against the real find | sort -V, fixed seeds.
func TestWalkFilesMatchesFindShGenerated(t *testing.T) {
	needGNUFindSort(t)
	const alphabet = "0123456789ab.~-+Z_"
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		name := func() string {
			b := make([]byte, 1+rng.Intn(6))
			for i := range b {
				b[i] = alphabet[rng.Intn(len(alphabet))]
			}
			if b[0] == '-' || string(b) == "." || string(b) == ".." {
				b[0] = 'a'
			}
			return string(b)
		}
		root := t.TempDir()
		dirs := []string{root}
		for i := 0; i < 60; i++ {
			parent := dirs[rng.Intn(len(dirs))]
			p := filepath.Join(parent, name())
			if rng.Intn(4) == 0 {
				if os.Mkdir(p, 0755) == nil {
					dirs = append(dirs, p)
				}
				continue
			}
			if _, err := os.Stat(p); err == nil {
				continue
			}
			os.WriteFile(p, nil, 0644)
		}
		out, err := exec.Command("sh", "-c", `find "$1" -mindepth 1 -type f | sort -V`, "sh", root).Output()
		if err != nil {
			t.Skipf("find | sort -V: %v", err)
		}
		want := strings.TrimSpace(string(out))
		var got []string
		walkFiles(root, func(p string) { got = append(got, p) })
		if strings.Join(got, "\n") != want {
			t.Errorf("seed %d: walkFiles order differs from find.sh\ngot:\n%s\nwant:\n%s", seed, strings.Join(got, "\n"), want)
		}
	}
}

// rewriteHead, review section E. Each case lists lines that must and must
// not be in the result.
func TestRewriteHead(t *testing.T) {
	base := []string{
		"From: someone@example.invalid",
		"Newsgroups: test.group",
		"Date: Mon, 30 Jun 2003 23:33:23 GMT",
		"Message-ID: <rh@test>",
	}
	with := func(lines ...string) []string { return append(append([]string{}, base...), lines...) }
	cases := []struct {
		name      string
		head      []string
		want, not []string
		code      int
	}{
		// E1: the last line was never looked at as a continuation, so a
		// dropped Xref folded onto it leaked its tail into the header
		{"Xref folded onto the last line", with("Subject: s", "Xref: host a.b:1", " c.d:2"),
			nil, []string{" c.d:2", "Xref: host a.b:1"}, 0},
		// E1: an empty Subject folded onto the last line was dropped as
		// "too short", its continuation was kept, and the article got 437
		{"Subject folded onto the last line", with("Subject:", " the subject"),
			[]string{"Subject: ", " the subject"}, nil, 0},
		// E2: head[0] was never anyone's previous line
		{"broken date line after a first continuation", append([]string{" folded", "Thu, 5 Aug 2004 21:50:04 EDT", "Subject: s"}, base...),
			nil, []string{"Thu, 5 Aug 2004 21: 50:04 EDT", "Thu, 5 Aug 2004 21:50:04 EDT"}, 0},
		// E4: one-character values without a space were dropped
		{"one-character values", with("Subject: s", "Lines:7", "X-No-Archive:1"),
			[]string{"Lines: 7", "X-No-Archive: 1"}, nil, 0},
		{"empty values are still dropped", with("Subject: s", "Keywords:", "Summary: "),
			nil, []string{"Keywords:", "Summary: ", "Summary:"}, 0},
		// E5: header content passes through; only control characters go
		{"spaces in values are kept", with("Subject: a  b    c", "X-Trace: x  y"),
			[]string{"Subject: a  b    c", "X-Trace: x  y"}, nil, 0},
		{"tab and NUL become spaces", with("Subject: a\tb\x00c"),
			[]string{"Subject: a b c"}, nil, 0},
		{"Path gets the prefix", with("Subject: s", "Path: a!b"),
			[]string{"Path: mbox2nntp" + my_mboxfile + "!a!b"}, nil, 0},
		// review #3: a dropped field went without its continuation lines,
		// which were then joined to the field kept before it
		{"folded Message-ID", []string{"From: a@b.invalid", "Date: Mon, 30 Jun 2003 23:33:23 GMT", "Subject: hello", "Message-ID:", " <rh@test>"},
			[]string{"Subject: hello", "Message-ID: <rh@test>"}, []string{" <rh@test>", "Message-ID:"}, 0},
		{"second Message-ID, folded", with("Subject: s", "Message-ID:", " <other@test>"),
			[]string{"Message-ID: <rh@test>"}, []string{" <other@test>"}, 0},
		{"folded Expires", with("Subject: s", "Expires: Mon, 30 Jun", " 2003 23:33:23 GMT"),
			nil, []string{" 2003 23:33:23 GMT", "Expires: Mon, 30 Jun"}, 0},
		{"no colon, folded", with("Subject: s", "garbage line", " its continuation"),
			nil, []string{"garbage line", " its continuation"}, 0},
		{"no Date", []string{"Subject: s", "Message-ID: <rh@test>"}, nil, nil, 437},
		{"no Subject", base, nil, nil, 437},
	}
	// the reviewer's example, whole: nothing joined to Subject
	got, _, _ := rewriteHead("<rh@test>", []string{"From: a@b.invalid", "Subject: hello", "Message-ID:", " <rh@test>", "Date: Mon, 30 Jun 2003 23:33:23 GMT"})
	if want := []string{"From: a@b.invalid", "Subject: hello", "Date: Mon, 30 Jun 2003 23:33:23 GMT", "Message-ID: <rh@test>"}; !reflect.DeepEqual(got, want) {
		t.Errorf("folded Message-ID: %q\nwant %q", got, want)
	}
	for _, tc := range cases {
		got, _, code := rewriteHead("<rh@test>", tc.head)
		if code != tc.code {
			t.Errorf("%s: code = %d, want %d (head %q)", tc.name, code, tc.code, got)
			continue
		}
		has := map[string]bool{}
		for _, l := range got {
			has[l] = true
		}
		for _, l := range tc.want {
			if !has[l] {
				t.Errorf("%s: missing %q in %q", tc.name, l, got)
			}
		}
		for _, l := range tc.not {
			if has[l] {
				t.Errorf("%s: unexpected %q in %q", tc.name, l, got)
			}
		}
	}
}

func TestSkipDirFile(t *testing.T) {
	for fp, want := range map[string]bool{
		// B5: a parent directory's name must not decide for its files
		"spool/alt.foo (part 2)/123":     false,
		"spool/comp.sources.patches/123": false,
		"spool/patch/123":                false,
		// the file's own name still does
		"spool/alt.foo/123 (copy)":   true,
		"spool/comp.sources/patch01": true,
		"spool/comp.sources/Patch02": true,
		"spool/alt.foo/index":        true,
		"spool/alt.foo/INDEX":        true,
		"spool/alt.foo/.#index":      true,
		"spool/alt.foo/x.overview":   true,
		"spool/alt.foo/x.tar.Z":      true,
		"spool/alt.foo/main.c":       true,
		"spool/alt.foo/123":          false,
		"spool/alt.foo/reindex":      false,
		"spool/alt.foo/index.txt":    false,
	} {
		if got := skipDirFile(fp); got != want {
			t.Errorf("skipDirFile(%q) = %v, want %v", fp, got, want)
		}
	}
}

// TestReadFileHonoursDropBinary: the directory path dropped yEnc articles
// even with .noDROPBINARY, which the mbox path honoured.
func TestReadFileHonoursDropBinary(t *testing.T) {
	globalBools.V = make(map[string]bool)
	globalCounter = GlobalCounter{v: make(map[string]int)}
	worker_channel = make(chan *ArticleItem, 8)
	defer func(v bool) { DROPBINARY = v }(DROPBINARY)

	const article = "From: someone@example.invalid\n" +
		"Subject: a binary\n" +
		"Date: Mon, 30 Jun 2003 23:33:23 GMT\n" +
		"Message-ID: <ybegin@test>\n" +
		"\n" +
		"=ybegin line=128 size=3 name=x.bin\n" +
		"abc\n" +
		"=yend size=3\n"
	fp := filepath.Join(t.TempDir(), "123")
	if err := os.WriteFile(fp, []byte(article), 0o644); err != nil {
		t.Fatal(err)
	}

	DROPBINARY = true
	if msgid := read_file(fp); msgid != "" || len(worker_channel) != 0 {
		t.Fatalf("DROPBINARY: read_file = %q, queued %d; want dropped", msgid, len(worker_channel))
	}
	DROPBINARY = false
	if msgid := read_file(fp); msgid != "<ybegin@test>" || len(worker_channel) != 1 {
		t.Fatalf("noDROPBINARY: read_file = %q, queued %d; want sent", msgid, len(worker_channel))
	}
}

// chunkReader hands out its data at most max bytes per Read, a line at a
// time, the way the library's bodyReader does.
type chunkReader struct {
	lines []string
	where int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.lines) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.lines[0][c.where:])
	c.where += n
	if c.where == len(c.lines[0]) {
		c.lines, c.where = c.lines[1:], 0
	}
	return n, nil
}

// I3: a body line longer than readBody's 64 KiB buffer comes back over
// several Reads; each piece used to become a line of its own, with CRLF
// inserted between them on the wire.
func TestReadBodyLongLine(t *testing.T) {
	long := strings.Repeat("x", 200*1024)
	exact := strings.Repeat("y", 64*1024-1) // with its "\n", fills the buffer exactly
	body, isbinary, err := readBody(&chunkReader{lines: []string{
		"short\n", long + "\r\n", exact + "\n", "", "no newline at the end",
	}})
	want := []string{"short", long, exact, "no newline at the end"}
	if err != nil {
		t.Fatal(err)
	}
	if isbinary || len(body) != len(want) {
		t.Fatalf("got %d lines (isbinary=%t), want %d", len(body), isbinary, len(want))
	}
	for i := range want {
		if body[i] != want[i] {
			t.Errorf("line %d: got %d bytes, want %d", i, len(body[i]), len(want[i]))
		}
	}
}

// isBadDateLine replaced a regexp2 pattern; it must give the same verdict.
// The pattern has no \s or other class where regexp2 and Go's regexp differ.
func TestIsBadDateLineMatchesRegex(t *testing.T) {
	re := regexp.MustCompile(`^[A-Z][a-z][a-z],\ `)
	lines := []string{"", "Thu, 1 Jan 1970 00:00:00 +0000", "Thu,", "Thu, ", "thu, 1", "THu, 1",
		"Thu,1", "Th\u00fc, 1", " Thu, 1", "Date: Thu, 1 Jan 1970", "Mon, ", "Zzz, x", "Aaa,  "}
	r := rand.New(rand.NewSource(1))
	alphabet := []byte("AZaz@[`{, \t\xc3")
	for i := 0; i < 20000; i++ {
		b := make([]byte, r.Intn(7))
		for j := range b {
			b[j] = alphabet[r.Intn(len(alphabet))]
		}
		lines = append(lines, string(b))
	}
	for _, l := range lines {
		if got, want := isBadDateLine(l), re.MatchString(l); got != want {
			t.Errorf("isBadDateLine(%q) = %v, regex says %v", l, got, want)
		}
	}
}

// I3, through the library: the last body line of an mbox with no final
// newline reaches the article (go-lib-mbox2nntp 605980f).
func TestFinalLineWithoutNewlineReachesBody(t *testing.T) {
	mr, err := mbox.CreateMboxStream(strings.NewReader("From nobody Thu Dec  8 12:55:04 2016\n" +
		"Subject: s\nMessage-ID: <last@test>\n\nfirst\nlast, no newline"))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := mr.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	body, _, err := readBody(msg.BodyReader())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"first", "last, no newline"}; strings.Join(body, "|") != strings.Join(want, "|") {
		t.Errorf("body = %q, want %q", body, want)
	}
}

func TestUATSuffix(t *testing.T) {
	for in, want := range map[string]string{
		"pl.test.usenet": ".usenet", "pl.test.usenet.xz": ".usenet.xz", "a/b/rec.games.rogue.usenet.7z": ".usenet.7z",
		"x.mbox.7z": "", "x.usenet.gz": "", "x.usenet.done": "", "usenet": "",
	} {
		if got := uatSuffix(in); got != want {
			t.Errorf("uatSuffix(%q) = %q, want %q", in, got, want)
		}
	}
}

func setMaxArticleSize(t *testing.T, n int) {
	saved := maxArticleSize
	maxArticleSize = n
	t.Cleanup(func() { maxArticleSize = saved })
}

type failingReader struct {
	r   io.Reader
	err error
}

func (f *failingReader) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if err == io.EOF {
		return 0, f.err
	}
	return n, err
}

// A body over maxArticleSize is read to its end and dropped, and a read
// error is returned instead of the lines read so far: the article would be
// sent cut short.
func TestReadBodyLimits(t *testing.T) {
	setMaxArticleSize(t, 100)
	line := strings.Repeat("x", 39) + "\n"
	lines := []string{line, line, line}
	cr := &chunkReader{lines: append(append([]string{}, lines...), "after\n")}
	body, _, err := readBody(cr)
	if err != errArticleTooBig || body != nil {
		t.Errorf("over the limit: %d lines, %v; want none, errArticleTooBig", len(body), err)
	}
	if len(cr.lines) != 0 {
		t.Errorf("%d chunks left unread", len(cr.lines))
	}

	if body, _, err := readBody(&chunkReader{lines: lines[:2]}); err != nil || len(body) != 2 {
		t.Errorf("under the limit: %d lines, %v", len(body), err)
	}

	// dropped anyway: not an error
	saved := DROPBINARY
	t.Cleanup(func() { DROPBINARY = saved })
	DROPBINARY = true
	if _, isbinary, err := readBody(&chunkReader{lines: append([]string{"=ybegin line=128\n"}, lines...)}); !isbinary || err != nil {
		t.Errorf("binary over the limit: isbinary=%t err=%v; want true, nil", isbinary, err)
	}

	broken := errors.New("disk on fire")
	body, _, err = readBody(&failingReader{r: strings.NewReader("one\ntwo\n"), err: broken})
	if err != broken || body != nil {
		t.Errorf("read error: %q, %v; want no lines, the error", body, err)
	}
}

// iterateMbox with a dry-run worker: an article over the limit is not
// sent and counts as a 437, the others are sent, and the input is done. A
// line over mbox.MaxLineLength stops the reading there, not done.
func TestIterateMboxTooBig(t *testing.T) {
	setMaxArticleSize(t, 1000)
	savedLine := mbox.MaxLineLength
	t.Cleanup(func() { mbox.MaxLineLength = savedLine })
	mbox.MaxLineLength = 500
	savedDry, savedMax := DRYRUN, MaxWorkers
	t.Cleanup(func() { DRYRUN, MaxWorkers = savedDry, savedMax })
	DRYRUN, MaxWorkers = true, 1
	const sep = "From nobody Thu Dec  8 12:55:04 2016\n"
	article := func(id, body string) string {
		return sep + "Subject: s\nFrom: a@b.c\nDate: Thu, 8 Dec 2016 12:55:04 +0000\nNewsgroups: x.test\nMessage-ID: " + id + "\n\n" + body
	}
	for _, c := range []struct {
		name string
		big  string
		sent int
		e437 int
		done bool
	}{
		{"fits", strings.Repeat("y\n", 400), 3, 0, true},
		{"too big", strings.Repeat("y\n", 600), 2, 1, true},
		// over mbox.MaxLineLength, set to 500 above: the parser stops
		{"long body line", strings.Repeat("y", 600) + "\n", 1, 0, false},
		{"long header line", "", 1, 0, false},
	} {
		if c.big == "" {
			c.big = "body\n"
		}
		in := article("<a@test>", "one\n") + article("<b@test>", c.big) + article("<c@test>", "three\n")
		if c.name == "long header line" {
			in = strings.Replace(in, "Message-ID: <b@test>", "X-Long: "+strings.Repeat("h", 600)+"\nMessage-ID: <b@test>", 1)
		}
		mr, err := mbox.CreateMboxStream(strings.NewReader(in))
		if err != nil {
			t.Fatal(err)
		}
		shortTimeouts(t)
		globalBools.V = make(map[string]bool)
		globalCounter = GlobalCounter{v: make(map[string]int)}
		worker_channel = make(chan *ArticleItem, 10)
		returned := make(chan bool)
		go func() { worker(1); close(returned) }()
		iterateMbox(mr, &eofReader{r: strings.NewReader(""), sawEOF: true}, c.name)
		<-returned
		if sent := globalCounter.getCounter("sent"); sent != c.sent {
			t.Errorf("%s: sent %d, want %d", c.name, sent, c.sent)
		}
		if e437, total := globalCounter.getCounter("wnntperr437"), globalCounter.getCounter("total"); e437 != c.e437 || total != c.sent+c.e437 {
			t.Errorf("%s: e437 %d, total %d; want %d, %d", c.name, e437, total, c.e437, c.sent+c.e437)
		}
		if done := globalBools.GetGB("done"); done != c.done {
			t.Errorf("%s: done = %t, want %t", c.name, done, c.done)
		}
	}
}

// Directory mode: an article file over maxArticleSize is not read.
func TestReadFileTooBig(t *testing.T) {
	globalBools.V = make(map[string]bool)
	globalCounter = GlobalCounter{v: make(map[string]int)}
	worker_channel = make(chan *ArticleItem, 8)
	fp := filepath.Join(t.TempDir(), "1")
	art := "From: a@b.c\nSubject: s\nDate: Mon, 30 Jun 2003 23:33:23 GMT\nMessage-ID: <big@test>\n\n" + strings.Repeat("y\n", 100)
	if err := os.WriteFile(fp, []byte(art), 0644); err != nil {
		t.Fatal(err)
	}
	setMaxArticleSize(t, len(art))
	if msgid := read_file(fp); msgid != "<big@test>" {
		t.Fatalf("at the limit: read_file = %q", msgid)
	}
	setMaxArticleSize(t, len(art)-1)
	if msgid := read_file(fp); msgid != "" || len(worker_channel) != 1 {
		t.Errorf("over the limit: read_file = %q, queued %d; want dropped", msgid, len(worker_channel))
	}
	// counted as a 437, as in mbox mode, and not as dropped
	if total, e437, dropped := globalCounter.getCounter("total"), globalCounter.getCounter("wnntperr437"), globalCounter.getCounter("dropped"); total != 1 || e437 != 1 || dropped != 0 {
		t.Errorf("over the limit: total %d, e437 %d, dropped %d; want 1, 1, 0", total, e437, dropped)
	}
}

// Messages read but not offered are counted: "dropped" for no Message-ID,
// no body or a parse error, "binary" for yEnc. Neither is in "total", and
// neither keeps the input from being done. They used to go uncounted.
func TestIterateMboxDropped(t *testing.T) {
	savedDry, savedMax := DRYRUN, MaxWorkers
	t.Cleanup(func() { DRYRUN, MaxWorkers = savedDry, savedMax })
	DRYRUN, MaxWorkers = true, 1
	const sep = "From nobody Thu Dec  8 12:55:04 2016\n"
	const head = "Subject: s\nFrom: a@b.c\nDate: Thu, 8 Dec 2016 12:55:04 +0000\nNewsgroups: x.test\n"
	in := sep + head + "Message-ID: <a@test>\n\nbody\n" +
		sep + head + "\nno Message-ID\n" +
		sep + head + "Message-ID: <empty@test>\n\n\n" +
		sep + head + "Message-ID: <yenc@test>\n\n=ybegin line=128 size=3 name=x\nabc\n=yend size=3\n" +
		sep + "not a header\n\nbody\n" +
		sep + head + "Message-ID: <b@test>\n\nbody\n"
	mr, err := mbox.CreateMboxStream(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	shortTimeouts(t)
	globalBools.V = make(map[string]bool)
	globalCounter = GlobalCounter{v: make(map[string]int)}
	worker_channel = make(chan *ArticleItem, 10)
	returned := make(chan bool)
	go func() { worker(1); close(returned) }()
	iterateMbox(mr, &eofReader{r: strings.NewReader(""), sawEOF: true}, "dropped")
	<-returned
	for name, want := range map[string]int{"sent": 2, "total": 2, "dropped": 3, "binary": 1} {
		if got := globalCounter.getCounter(name); got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
	if !globalBools.GetGB("done") {
		t.Error("done = false, want true")
	}
}

// Directory mode counts its drops the same way.
func TestReadFileDropped(t *testing.T) {
	globalBools.V = make(map[string]bool)
	globalCounter = GlobalCounter{v: make(map[string]int)}
	worker_channel = make(chan *ArticleItem, 8)
	dir := t.TempDir()
	const head = "From: a@b.c\nSubject: s\nDate: Mon, 30 Jun 2003 23:33:23 GMT\n"
	for name, art := range map[string]string{
		"nomsgid": head + "\nbody\n",
		"empty":   head + "Message-ID: <empty@test>\n\n",
		"yenc":    head + "Message-ID: <yenc@test>\n\n=ybegin line=128 size=3 name=x\nabc\n=yend size=3\n",
		"ok":      head + "Message-ID: <ok@test>\n\nbody\n",
	} {
		fp := filepath.Join(dir, name)
		if err := os.WriteFile(fp, []byte(art), 0644); err != nil {
			t.Fatal(err)
		}
		read_file(fp)
	}
	if len(worker_channel) != 1 {
		t.Errorf("queued %d, want 1", len(worker_channel))
	}
	for name, want := range map[string]int{"dropped": 2, "binary": 1, "total": 0} {
		if got := globalCounter.getCounter(name); got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
}

// The input name goes into our Path element: only what RFC 5536 allows in
// a path-identity may stay.
func TestPathSuffix(t *testing.T) {
	for name, want := range map[string]string{
		"x.mbox.gz":                  "-x.mbox.gz",
		"0.akita-inu.(1825).mbox.7z": "-0.akita-inu._1825_.mbox.7z",
		"my news!x.mbox":             "-my_news_x.mbox",
		"a\tb\r\nc":                  "-a_b__c",
		"grüße.mbox":                 "-gr____e.mbox",
		"c:x_":                       "-c:x",
		"x.mbox~":                    "-x.mbox",
		"_-.":                        "",
		"!!":                         "",
		"":                           "",
	} {
		if got := pathSuffix(name); got != want {
			t.Errorf("pathSuffix(%q) = %q, want %q", name, got, want)
		}
	}
}
