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
	// nodes makes the metadata name data nodes: this server, under
	// /0/items/ID/, and the hosts in extraHosts.
	nodes       bool
	extraHosts  []string
	nodeMissing map[string]bool          // "id/name" -> 404 on the node, served by /download/
	noRange     bool                     // ignore Range headers
	metaDelay   map[string]time.Duration // id -> wait before answering /metadata/id
	// slow holds every file request this long, outside mu, and counts the
	// requests in flight, in all and per item.
	slow      time.Duration
	cmu       sync.Mutex
	inflight  map[string]int
	maxItem   map[string]int
	maxAll    int
	inflightN int
}

func newFake() *fakeIA {
	return &fakeIA{items: map[string]map[string][]byte{}, md5s: map[string]string{}, dark: map[string]bool{},
		fail500: map[string]int{}, stall: map[string]int{}, nodeMissing: map[string]bool{}}
}

func (f *fakeIA) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if d := f.metaDelay[strings.TrimPrefix(r.URL.Path, "/metadata/")]; d > 0 {
		time.Sleep(d)
	}
	if f.slow > 0 && !strings.HasPrefix(r.URL.Path, "/metadata/") {
		id, _, _ := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/download/"), "/0/items/"), "/")
		f.cmu.Lock()
		if f.inflight == nil {
			f.inflight, f.maxItem = map[string]int{}, map[string]int{}
		}
		f.inflight[id]++
		f.inflightN++
		f.maxItem[id] = max(f.maxItem[id], f.inflight[id])
		f.maxAll = max(f.maxAll, f.inflightN)
		f.cmu.Unlock()
		time.Sleep(f.slow)
		defer func() {
			f.cmu.Lock()
			f.inflight[id]--
			f.inflightN--
			f.cmu.Unlock()
		}()
	}
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
			Dir      string            `json:"dir,omitempty"`
			Servers  []string          `json:"workable_servers,omitempty"`
		}
		out.Created, out.Metadata = 1, map[string]string{"identifier": id}
		if f.nodes {
			out.Dir, out.Servers = "/0/items/"+id, append([]string{r.Host}, f.extraHosts...)
		}
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
	case strings.HasPrefix(p, "/download/"), strings.HasPrefix(p, "/0/items/"):
		node := strings.HasPrefix(p, "/0/items/")
		id, name, _ := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(p, "/download/"), "/0/items/"), "/")
		b, ok := f.items[id][name]
		if node && (!f.nodes || f.nodeMissing[id+"/"+name]) {
			ok = false
		}
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
		if f.noRange {
			r.Header.Del("Range")
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
	// far above the 100ms gaps: a loaded machine under -race once took
	// over 300ms for one
	c := &client{http: srv.Client(), stall: 2 * time.Second}
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

func count(reqs []string, prefix string) int {
	n := 0
	for _, r := range reqs {
		if strings.HasPrefix(r, prefix) {
			n++
		}
	}
	return n
}

// With -nodes the files come from the data nodes the metadata names, and
// from /download/ only when a node does not have one.
func TestNodesDirect(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	fake.items["it"] = map[string][]byte{"f": []byte("from the node"), "sub/g h": []byte("only via the front door")}
	fake.nodes, o.nodes = true, true
	fake.extraHosts = []string{"bad/host", "user@host", ""} // left out
	fake.nodeMissing["it/sub/g h"] = true
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	for name, want := range fake.items["it"] {
		if b, err := os.ReadFile(filepath.Join(o.out, "it", name)); err != nil || !bytes.Equal(b, fake.items["it"][name]) {
			t.Errorf("%s: %q, %v; want %q", name, b, err, want)
		}
	}
	reqs := fake.reset()
	for p, want := range map[string]int{
		"/0/items/it/f": 1, "/download/it/f": 0, "/0/items/it/sub/g h": 1, "/download/it/sub/g h": 1,
	} {
		if n := count(reqs, p); n != want {
			t.Errorf("%d requests for %s, want %d (all: %q)", n, p, want, reqs)
		}
	}
}

func TestNodeURLs(t *testing.T) {
	got := nodeURLs("https://archive.org", "/12/items/a b", []string{"ia1.us.archive.org", "ia1.us.archive.org", "x/y", "a@b", "", "dn2:8443"})
	want := []string{"https://ia1.us.archive.org/12/items/a%20b", "https://dn2:8443/12/items/a%20b"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("nodeURLs = %q, want %q", got, want)
	}
	for _, dir := range []string{"", "12/items/x", "/12/../etc", "/12//x"} {
		if got := nodeURLs("https://archive.org", dir, []string{"h"}); got != nil {
			t.Errorf("dir %q: %q", dir, got)
		}
	}
}

// The front door joins the round only after each node failed once.
func TestMirrorsPick(t *testing.T) {
	m := newMirrors("https://archive.org", "it", "f", []string{"https://n1/d", "https://n2/d"})
	seen := map[string]int{}
	for range 4 {
		seen[m.pick()[0]]++
	}
	if seen["https://n1/d/f"] != 2 || seen["https://n2/d/f"] != 2 {
		t.Errorf("before failures: %v", seen)
	}
	m.fail()
	m.fail()
	seen = map[string]int{}
	for range 6 {
		seen[m.pick()[0]]++
	}
	if seen["https://archive.org/download/it/f"] != 2 {
		t.Errorf("after failures: %v", seen)
	}
	if !m.drop("https://n1/d/f") || !m.drop("https://n2/d/f") || m.drop("https://archive.org/download/it/f") {
		t.Error("drop")
	}
	if u := m.pick(); len(u) != 1 || u[0] != "https://archive.org/download/it/f" {
		t.Errorf("no nodes left: %q", u)
	}
}

func TestHostSlots(t *testing.T) {
	h := newHostSlots(2)
	ctx := context.Background()
	urls := []string{"https://a/x", "https://b/x"}
	got := map[string]int{}
	var rels []func()
	for range 4 {
		u, rel, err := h.take(ctx, urls)
		if err != nil {
			t.Fatal(err)
		}
		got[hostOf(u)]++
		rels = append(rels, rel)
	}
	if got["a"] != 2 || got["b"] != 2 {
		t.Errorf("not spread: %v", got)
	}
	// both full: the next waits for a slot
	tctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	if _, _, err := h.take(tctx, urls); err == nil {
		t.Error("a third request to a full server")
	}
	cancel()
	done := make(chan string)
	go func() {
		u, _, _ := h.take(ctx, urls)
		done <- u
	}()
	time.Sleep(10 * time.Millisecond)
	rels[3]() // the 4th went to b
	rels[3]() // twice is harmless
	select {
	case u := <-done:
		if hostOf(u) != "b" {
			t.Errorf("got %s, want the freed b", u)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no slot after a release")
	}
	if n := h.busy["b"]; n != 2 {
		t.Errorf("b busy %d, want 2", n)
	}
	if newHostSlots(0) != nil {
		t.Error("0 is no limit")
	}
	if u, rel, err := (*hostSlots)(nil).take(ctx, urls); err != nil || u != urls[0] {
		t.Errorf("no limit: %s %v", u, err)
	} else {
		rel()
	}
}

// By default an item has one connection: its files come one after another
// and a big one in one go, while the items run side by side.
func TestOneConnPerItem(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit1\nit2\nit3\n")
	big := bytes.Repeat([]byte("0123456789abcdefghij"), 525)
	for _, id := range []string{"it1", "it2", "it3"} {
		fake.items[id] = map[string][]byte{"big": big, "a": []byte("a"), "b": []byte("b"), "c": []byte("c")}
	}
	fake.nodes, o.nodes = true, true
	fake.slow = 20 * time.Millisecond
	o.piece, o.itemConns = 1000, 1
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	for id, n := range fake.maxItem {
		if n != 1 {
			t.Errorf("%s: %d connections at once", id, n)
		}
	}
	if fake.maxAll < 2 {
		t.Errorf("items one at a time: at most %d connections", fake.maxAll)
	}
	if fake.ranges != 0 {
		t.Errorf("%d range requests: a file was split", fake.ranges)
	}
}

// -host-conns caps the connections to one server, whatever -j says.
func TestHostConns(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit1\nit2\nit3\n")
	for _, id := range []string{"it1", "it2", "it3"} {
		fake.items[id] = map[string][]byte{"a": []byte("a"), "b": []byte("b")}
	}
	fake.nodes, o.nodes = true, true
	fake.slow = 10 * time.Millisecond
	o.jobs, o.hostConns = 3, 1
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	if fake.maxAll != 1 {
		t.Errorf("%d connections at once to the one server", fake.maxAll)
	}
}

func pieceCount(t *testing.T, o options) int {
	t.Helper()
	d, err := openDB(o.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM pieces`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A file larger than -piece is fetched in pieces, in parallel.
func TestPieces(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	content := bytes.Repeat([]byte("0123456789abcdefghij"), 525) // 10500 bytes: 11 pieces
	fake.items["it"] = map[string][]byte{"big": content, "small": []byte("small")}
	fake.nodes, o.nodes = true, true
	o.piece, o.conns = 1000, 4
	o.itemConns = 4
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(o.out, "it", "big")); err != nil || !bytes.Equal(b, content) {
		t.Fatalf("big: %d bytes, %v", len(b), err)
	}
	if fake.ranges != 11 {
		t.Errorf("%d range requests, want 11", fake.ranges)
	}
	if reqs := fake.reset(); count(reqs, "/download/") != 0 {
		t.Errorf("went through the front door: %q", reqs)
	}
	if n := pieceCount(t, o); n != 0 {
		t.Errorf("%d pieces left in the database", n)
	}
	if left, _ := filepath.Glob(filepath.Join(o.out, "it", "*.part*")); len(left) != 0 {
		t.Errorf("left %q", left)
	}
}

// The finished pieces are in the database: the next run fetches the rest.
func TestPiecesResume(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	content := bytes.Repeat([]byte("0123456789abcdefghij"), 525)
	fake.items["it"] = map[string][]byte{"big": content}
	fake.fail500["it/big"] = 3
	o.piece, o.conns, o.retries = 1000, 1, 0
	o.itemConns = 4
	if err := run(context.Background(), o, list, os.Stdout); err == nil {
		t.Fatal("no error with 3 pieces failing")
	}
	if n := pieceCount(t, o); n != 8 {
		t.Errorf("%d pieces recorded, want 8", n)
	}
	fake.ranges = 0
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	if fake.ranges != 3 {
		t.Errorf("second run: %d range requests, want 3", fake.ranges)
	}
	if b, err := os.ReadFile(filepath.Join(o.out, "it", "big")); err != nil || !bytes.Equal(b, content) {
		t.Fatalf("big: %d bytes, %v", len(b), err)
	}
	if s := statuses(t, o); s["it"] != stDone {
		t.Errorf("status %q", s["it"])
	}
}

// A .part from a download from the start (an older iafetch, or -piece 0):
// its whole pieces are kept.
func TestPiecesFromSequentialPart(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	content := bytes.Repeat([]byte("0123456789abcdefghij"), 525)
	fake.items["it"] = map[string][]byte{"big": content}
	o.piece, o.conns = 1000, 3
	o.itemConns = 4
	os.MkdirAll(filepath.Join(o.out, "it"), 0o755)
	if err := os.WriteFile(filepath.Join(o.out, "it", "big.part"), content[:3500], 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	if fake.ranges != 8 {
		t.Errorf("%d range requests, want 8 (pieces 3 to 10)", fake.ranges)
	}
	if b, err := os.ReadFile(filepath.Join(o.out, "it", "big")); err != nil || !bytes.Equal(b, content) {
		t.Fatalf("big: %d bytes, %v", len(b), err)
	}
}

// A server that answers a piece with the whole file: downloaded in one go.
func TestPiecesNoRange(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	content := bytes.Repeat([]byte("0123456789abcdefghij"), 525)
	fake.items["it"] = map[string][]byte{"big": content}
	fake.noRange = true
	o.piece, o.conns = 1000, 4
	o.itemConns = 4
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(o.out, "it", "big")); err != nil || !bytes.Equal(b, content) {
		t.Fatalf("big: %d bytes, %v", len(b), err)
	}
	if n := pieceCount(t, o); n != 0 {
		t.Errorf("%d pieces left in the database", n)
	}
}

// A piece that arrives wrong fails the md5 of the whole: started over.
func TestPiecesBadMD5(t *testing.T) {
	fake, o, list := setup(t, "identifier\nit\n")
	content := bytes.Repeat([]byte("0123456789abcdefghij"), 525)
	fake.items["it"] = map[string][]byte{"big": content}
	fake.md5s["it/big"] = strings.Repeat("0", 32)
	o.piece, o.conns, o.retries = 1000, 4, 1
	o.itemConns = 4
	if err := run(context.Background(), o, list, os.Stdout); err == nil {
		t.Fatal("no error for a wrong md5")
	}
	if fake.ranges != 22 {
		t.Errorf("%d range requests, want 22: every piece again on the second try", fake.ranges)
	}
	if _, err := os.Stat(filepath.Join(o.out, "it", "big")); err == nil {
		t.Error("kept under its name")
	}
	if n := pieceCount(t, o); n != 0 {
		t.Errorf("%d pieces left in the database", n)
	}
}

// An item whose metadata is slow does not hold up the items after it.
func TestSlowMetadata(t *testing.T) {
	fake, o, list := setup(t, "identifier\nslow\nit2\nit3\n")
	for _, id := range []string{"slow", "it2", "it3"} {
		fake.items[id] = map[string][]byte{"f": []byte(id)}
	}
	fake.metaDelay = map[string]time.Duration{"slow": 300 * time.Millisecond}
	o.jobs, o.meta = 2, 2
	if err := run(context.Background(), o, list, os.Stdout); err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, r := range fake.reset() {
		if strings.HasPrefix(r, "/download/") {
			order = append(order, r)
		}
	}
	if len(order) != 3 || order[2] != "/download/slow/f" {
		t.Errorf("downloads in order %q, want slow last", order)
	}
	if s := statuses(t, o); s["slow"] != stDone || s["it2"] != stDone || s["it3"] != stDone {
		t.Errorf("statuses %v", s)
	}
}
