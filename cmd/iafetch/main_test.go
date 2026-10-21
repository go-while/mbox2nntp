package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeIA serves /metadata/ID and /download/ID/NAME like archive.org.
type fakeIA struct {
	mu       sync.Mutex
	items    map[string]map[string][]byte // id -> name -> content
	md5s     map[string]string            // "id/name" -> md5 to advertise instead of the real one
	dark     map[string]bool
	fail500  map[string]int // "id/name" -> 500s to send before serving
	stall    map[string]int // "id/name" -> bytes to send, once, before going silent
	requests []string
	ranges   int
}

func newFake() *fakeIA {
	return &fakeIA{items: map[string]map[string][]byte{}, md5s: map[string]string{}, dark: map[string]bool{}, fail500: map[string]int{}, stall: map[string]int{}}
}

func (f *fakeIA) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, _ := url.PathUnescape(r.URL.EscapedPath())
	f.requests = append(f.requests, p)
	switch {
	case strings.HasPrefix(p, "/metadata/"):
		id := strings.TrimPrefix(p, "/metadata/")
		if f.dark[id] {
			fmt.Fprint(w, `{"created":1,"is_dark":true,"dir":"/x"}`)
			return
		}
		files, ok := f.items[id]
		if !ok {
			fmt.Fprint(w, `{}`)
			return
		}
		type jf struct {
			Name   string `json:"name"`
			Source string `json:"source"`
			Size   string `json:"size,omitempty"`
			MD5    string `json:"md5,omitempty"`
		}
		var out struct {
			Created  int               `json:"created"`
			Metadata map[string]string `json:"metadata"`
			Files    []jf              `json:"files"`
		}
		out.Created, out.Metadata = 1, map[string]string{"identifier": id}
		for name, b := range files {
			sum := md5.Sum(b)
			m := hex.EncodeToString(sum[:])
			if alt, ok := f.md5s[id+"/"+name]; ok {
				m = alt
			}
			out.Files = append(out.Files, jf{name, "original", fmt.Sprint(len(b)), m})
		}
		// What archive.org adds to every item: not to be downloaded.
		out.Files = append(out.Files, jf{id + "_meta.xml", "original", "10", ""}, jf{id + "_files.xml", "original", "", ""},
			jf{id + ".torrent", "metadata", "5", ""}, jf{id + "_meta.sqlite", "original", "8", ""}, jf{"thumb.jpg", "derivative", "3", ""},
			jf{"history/files/old.mbox.7z.~1~", "original", "4", ""})
		json.NewEncoder(w).Encode(out)
	case strings.HasPrefix(p, "/download/"):
		id, name, _ := strings.Cut(strings.TrimPrefix(p, "/download/"), "/")
		b, ok := f.items[id][name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if f.fail500[id+"/"+name] > 0 {
			f.fail500[id+"/"+name]--
			http.Error(w, "busy", http.StatusInternalServerError)
			return
		}
		if n, ok := f.stall[id+"/"+name]; ok {
			delete(f.stall, id+"/"+name)
			w.Header().Set("Content-Length", fmt.Sprint(len(b)))
			w.Write(b[:n])
			w.(http.Flusher).Flush()
			f.mu.Unlock() // others may go on meanwhile
			<-r.Context().Done()
			f.mu.Lock()
			return
		}
		if r.Header.Get("Range") != "" {
			f.ranges++
		}
		http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(b))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeIA) reset() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.requests
	f.requests = nil
	return r
}

func setup(t *testing.T, csv string) (*fakeIA, options, string) {
	t.Helper()
	logf = t.Logf
	t.Cleanup(func() { logf = func(string, ...any) {} })
	fake := newFake()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	list := filepath.Join(dir, "list.csv")
	if err := os.WriteFile(list, []byte(csv), 0o644); err != nil {
		t.Fatal(err)
	}
	o := options{
		out: filepath.Join(dir, "dl"), dbPath: filepath.Join(dir, "state.sqlite"),
		jobs: 3, retries: 3, backoff: time.Millisecond, base: srv.URL,
	}
	return fake, o, list
}

func statuses(t *testing.T, o options) map[string]string {
	t.Helper()
	d, err := openDB(o.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	rows, err := d.Query(`SELECT identifier, status FROM items`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	m := map[string]string{}
	for rows.Next() {
		var id, s string
		rows.Scan(&id, &s)
		m[id] = s
	}
	return m
}

const list3 = "order,tier,list,identifier,item_size\n" +
	"2,B1,usenet-.txt,usenet-b,100\n" +
	"1,A,other.txt,item-a,50\n" +
	"3,X,gna.txt,gna-junk,1\n" +
	"4,B1,usenet-.txt,usenet-b,100\n"

func TestDownloadThenSkipWhenMoved(t *testing.T) {
	fake, o, list := setup(t, list3)
	fake.items["item-a"] = map[string][]byte{"a.mbox.gz": []byte("aaaa"), "sub/a2.7z": []byte("a2")}
	fake.items["usenet-b"] = map[string][]byte{"b (1).mbox.gz": bytes.Repeat([]byte("b"), 5000)}
	fake.items["gna-junk"] = map[string][]byte{"x": []byte("x")}
	o.tiers = set("A,B1")

	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	for p, want := range map[string]string{
		"item-a/a.mbox.gz": "aaaa", "item-a/sub/a2.7z": "a2", "usenet-b/b (1).mbox.gz": strings.Repeat("b", 5000),
	} {
		b, err := os.ReadFile(filepath.Join(o.out, p))
		if err != nil || string(b) != want {
			t.Errorf("%s: %q, %v", p, b, err)
		}
	}
	for _, p := range []string{"item-a/item-a_meta.xml", "item-a/history"} {
		if _, err := os.Stat(filepath.Join(o.out, p)); err == nil {
			t.Errorf("%s was downloaded", p)
		}
	}
	if _, err := os.Stat(filepath.Join(o.out, "gna-junk")); err == nil {
		t.Error("tier X item downloaded")
	}
	got := fake.reset()
	if got[0] != "/metadata/item-a" {
		t.Errorf("first request %q, want the item with order 1", got[0])
	}
	if s := statuses(t, o); s["item-a"] != stDone || s["usenet-b"] != stDone || len(s) != 2 {
		t.Errorf("statuses %v", s)
	}

	// Move the files away, as after an import: nothing is fetched again.
	if err := os.RemoveAll(o.out); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	if got := fake.reset(); len(got) != 0 {
		t.Errorf("second run made requests: %v", got)
	}
	if _, err := os.Stat(o.out); err == nil {
		t.Error("second run created the download directory again")
	}
}

func TestUnavailableItemsRecorded(t *testing.T) {
	fake, o, list := setup(t, "identifier\ngone\nnothere\nonlymeta\n")
	fake.dark["gone"] = true
	fake.items["onlymeta"] = map[string][]byte{}
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"gone": stDark, "nothere": stMissing, "onlymeta": stEmpty}
	if s := statuses(t, o); fmt.Sprint(s) != fmt.Sprint(want) {
		t.Errorf("statuses %v, want %v", s, want)
	}
	fake.reset()
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	if got := fake.reset(); len(got) != 0 {
		t.Errorf("unavailable items asked for again: %v", got)
	}
	o.retryUnavailable = true
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	if got := fake.reset(); len(got) != 3 {
		t.Errorf("-retry-unavailable: %v", got)
	}
}

func TestBadMD5ThenFixed(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	fake.items["it"] = map[string][]byte{"good": []byte("good"), "bad": []byte("data")}
	fake.md5s["it/bad"] = strings.Repeat("0", 32)
	err := run(context.Background(), o, list, os.Stdout)
	if err == nil || !strings.Contains(err.Error(), "1 files failed") {
		t.Fatalf("err %v, want 1 file failed", err)
	}
	if s := statuses(t, o); s["it"] != stIncomplete {
		t.Errorf("status %q, want incomplete", s["it"])
	}
	if _, err := os.Stat(filepath.Join(o.out, "it", "bad")); err == nil {
		t.Error("file with the wrong md5 was kept under its name")
	}
	delete(fake.md5s, "it/bad")
	fake.reset()
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	for _, r := range fake.reset() {
		if r == "/download/it/good" {
			t.Error("the good file was downloaded again")
		}
	}
	if s := statuses(t, o); s["it"] != stDone {
		t.Errorf("status %q, want done", s["it"])
	}
}

func TestResumeAndRetry(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	content := bytes.Repeat([]byte("0123456789"), 1000)
	fake.items["it"] = map[string][]byte{"big": content}
	fake.fail500["it/big"] = 2
	if err := os.MkdirAll(filepath.Join(o.out, "it"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(o.out, "it", "big.part"), content[:4000], 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(o.out, "it", "big"))
	if err != nil || !bytes.Equal(b, content) {
		t.Fatalf("resumed file differs (%d bytes, %v)", len(b), err)
	}
	if fake.ranges != 1 {
		t.Errorf("%d range requests, want 1", fake.ranges)
	}
	if _, err := os.Stat(filepath.Join(o.out, "it", "big.part")); err == nil {
		t.Error(".part left behind")
	}
}

func TestExistingFileVerified(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	fake.items["it"] = map[string][]byte{"f": []byte("right")}
	os.MkdirAll(filepath.Join(o.out, "it"), 0o755)
	os.WriteFile(filepath.Join(o.out, "it", "f"), []byte("right"), 0o644)
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	for _, r := range fake.reset() {
		if strings.HasPrefix(r, "/download/") {
			t.Errorf("downloaded although the right file was there: %s", r)
		}
	}
	// A different file under the name is not replaced.
	_, o2, list2 := setup(t, "identifier\nit\n")
	o2.base = o.base
	os.MkdirAll(filepath.Join(o2.out, "it"), 0o755)
	os.WriteFile(filepath.Join(o2.out, "it", "f"), []byte("wrong"), 0o644)
	if err := run(context.Background(), o2, list2, os.Stdout); err == nil {
		t.Error("no error for a different file in the way")
	}
	if b, _ := os.ReadFile(filepath.Join(o2.out, "it", "f")); string(b) != "wrong" {
		t.Error("the file in the way was replaced")
	}
}

func TestUnsafeNames(t *testing.T) {
	for _, c := range []struct{ id, name string }{{"it", "../evil"}, {"it", "/etc/passwd"}, {"..", "f"}, {"a/b", "f"}} {
		if _, err := localPath("/dl", c.id, iaFile{Name: c.name}); err == nil {
			t.Errorf("%s/%s accepted", c.id, c.name)
		}
	}
	if p, err := localPath("/dl", "it", iaFile{Name: "sub/f.gz"}); err != nil || p != filepath.Join("/dl", "it", "sub", "f.gz") {
		t.Errorf("sub/f.gz: %q %v", p, err)
	}
}

func TestReadListAndFilters(t *testing.T) {
	items, err := readList(strings.NewReader(list3))
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, it := range items {
		ids = append(ids, fmt.Sprintf("%d %s", it.Order, it.ID))
	}
	if got := strings.Join(ids, " "); got != "1 item-a 2 usenet-b 3 gna-junk" {
		t.Errorf("items %s", got)
	}
	o := options{tiers: set("A,B1"), from: 2}
	if o.selected(items[0]) || !o.selected(items[1]) || o.selected(items[2]) {
		t.Error("tier/from filter")
	}
	if _, err := readList(strings.NewReader("name\nx\n")); err == nil {
		t.Error("list without identifier column accepted")
	}
}

func TestStatusAndDryRun(t *testing.T) {
	fake, o, list := setup(t, list3)
	fake.items["item-a"] = map[string][]byte{"a": []byte("a")}
	o.match = regexp.MustCompile(`^item-`)
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	o.match, o.dryRun = nil, true
	fake.reset()
	if err := run(context.Background(), o, list, &out); err != nil {
		t.Fatal(err)
	}
	if got := fake.reset(); len(got) != 0 {
		t.Errorf("dry run made requests: %v", got)
	}
	if s := out.String(); strings.Contains(s, "item-a") || !strings.Contains(s, "usenet-b") || !strings.Contains(s, "2 items") {
		t.Errorf("dry run output:\n%s", s)
	}
	out.Reset()
	o.dryRun, o.status = false, true
	if err := run(context.Background(), o, "", &out); err != nil {
		t.Fatal(err)
	}
	if s := out.String(); !strings.Contains(s, "done") || !strings.Contains(s, "files downloaded: 1") {
		t.Errorf("status output:\n%s", s)
	}
}

// Review #9: an item with files "x" and "x.part". x was downloaded through
// x.part, overwriting the finished x.part and renaming it away while the
// database had it done.
func TestPartNamedLikeAnotherFile(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	files := map[string][]byte{
		"x":        bytes.Repeat([]byte("x"), 3000),
		"x.part":   bytes.Repeat([]byte("p"), 2000),
		"x.part.1": bytes.Repeat([]byte("1"), 1000),
	}
	fake.items["it"] = files
	o.jobs = 3 // all at once
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	for name, want := range files {
		if got, err := os.ReadFile(filepath.Join(o.out, "it", name)); err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s: %d bytes, %v; want its own %d bytes", name, len(got), err, len(want))
		}
	}
	if left, _ := filepath.Glob(filepath.Join(o.out, "it", "*.part.*")); len(left) != 1 { // x.part.1 itself
		t.Errorf("left %q", left)
	}
}

func TestPartName(t *testing.T) {
	taken := map[string]bool{"/d/it/x": true, "/d/it/x.part": true, "/d/it/x.part.1": true, "/d/it/y": true, "/d/it/z.part": true}
	for path, want := range map[string]string{
		"/d/it/x": "/d/it/x.part.2",
		"/d/it/y": "/d/it/y.part",
		"/d/it/Z": "/d/it/Z.part.1", // case-insensitive file systems
	} {
		if got := partName(path, taken); got != want {
			t.Errorf("partName(%q) = %q, want %q", path, got, want)
		}
	}
}

// Review #10: a .part longer than the file was started over without
// resetting the md5, so the first try always failed its check.
func TestOversizedPart(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	content := bytes.Repeat([]byte("0123456789"), 1000)
	fake.items["it"] = map[string][]byte{"big": content}
	o.retries = 0 // the first try has to be right
	os.MkdirAll(filepath.Join(o.out, "it"), 0o755)
	if err := os.WriteFile(filepath.Join(o.out, "it", "big.part"), append(content, "and more"...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(o.out, "it", "big")); err != nil || !bytes.Equal(b, content) {
		t.Fatalf("big: %d bytes, %v", len(b), err)
	}
	n := 0
	for _, r := range fake.reset() {
		if r == "/download/it/big" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%d downloads, want 1", n)
	}
}

// A server that stops sending in the middle of a file: the request is
// given up after o.stall, and the next try resumes the part. It used to
// hang the worker, and the run, for good.
func TestStalledDownload(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	content := bytes.Repeat([]byte("0123456789"), 1000)
	fake.items["it"] = map[string][]byte{"big": content}
	fake.stall["it/big"] = 3000
	o.stall = 200 * time.Millisecond
	done := make(chan error, 1)
	go func() { done <- run(context.Background(), o, list, os.Stdout) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run still waiting after 20s")
	}
	if b, err := os.ReadFile(filepath.Join(o.out, "it", "big")); err != nil || !bytes.Equal(b, content) {
		t.Fatalf("big: %d bytes, %v", len(b), err)
	}
	if fake.ranges != 1 {
		t.Errorf("%d range requests, want 1: the second try resumes", fake.ranges)
	}
}

// The watchdog leaves a slow server alone as long as data keeps coming.
func TestWatchdogSlowServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 5; i++ {
			fmt.Fprint(w, "x")
			w.(http.Flusher).Flush()
			time.Sleep(100 * time.Millisecond)
		}
	}))
	t.Cleanup(srv.Close)
	c := &client{http: srv.Client(), stall: 300 * time.Millisecond}
	resp, err := c.get(context.Background(), srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil || string(b) != "xxxxx" {
		t.Errorf("read %q, %v", b, err)
	}

	c.stall = 50 * time.Millisecond
	resp, err = c.get(context.Background(), srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err == nil || !strings.Contains(err.Error(), "no data for 50ms") {
		t.Errorf("stalled: err = %v", err)
	}
}

// firstErr takes errors of any type, from any goroutine, and keeps the
// first. atomic.Value panicked on the second type.
func TestFirstErr(t *testing.T) {
	var f firstErr
	if f.get() != nil {
		t.Fatal("not nil at first")
	}
	first := errors.New("first")
	var wg sync.WaitGroup
	f.set(first)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.set(&os.PathError{Op: "open", Path: "x", Err: os.ErrNotExist})
			f.set(fmt.Errorf("wrapped: %w", first))
		}()
	}
	wg.Wait()
	if f.get() != first {
		t.Errorf("get = %v, want the first", f.get())
	}
}
