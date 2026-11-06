package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// iaFile is one file of an archive.org item, as its metadata lists it.
type iaFile struct {
	Name string
	Size int64 // -1 when the metadata gives none
	MD5  string
}

type iaMeta struct {
	Files  []iaFile
	Dark   bool
	Exists bool
	Nodes  []string // the item's directory on each data node, as URLs; see mirrors
}

// generated are suffixes of the files archive.org makes itself for every
// item; they are no part of the upload and are not downloaded. Neither are
// the files under history/: archive.org keeps there the earlier versions of
// files that were replaced, as "history/files/NAME.~1~".
var generated = []string{
	"_meta.xml", "_files.xml", "_meta.sqlite", "_reviews.xml",
	".torrent", "_torrent.txt", "__ia_thumb.jpg",
}

func isGenerated(name string) bool {
	if strings.HasPrefix(name, "history/") {
		return true
	}
	for _, s := range generated {
		if strings.HasSuffix(name, s) {
			return true
		}
	}
	return false
}

// permanentError is a failure that retrying will not fix, such as a 404.
type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

// statusError is an HTTP response with a status get does not accept.
type statusError struct {
	url    string
	status string
	code   int
}

func (e statusError) Error() string { return e.url + ": " + e.status }

// errNoRange: the server sent the whole file when asked for a piece.
var errNoRange = errors.New("the server ignores Range requests")

type client struct {
	http    *http.Client
	base    string // https://archive.org
	agent   string
	retries int
	backoff time.Duration // first pause between tries; doubles, up to 64x
	stall   time.Duration // see get; 0 for none
	read    atomic.Int64  // bytes downloaded, for the progress line
	nodes   bool          // download from the data nodes (iaMeta.Nodes)
	piece   int64         // fetch files larger than this in pieces of this size; 0 never
	split   int           // pieces of one file at a time; below 2 files are never split
	conns   chan struct{} // one token per request in flight, files and pieces alike
	hosts   *hostSlots    // requests per server; nil for no limit
	db      *db           // records the finished pieces
}

// hostSlots caps the requests in flight to each server, so that many
// items at once do not pile onto the few nodes that hold several of them.
type hostSlots struct {
	max   int
	mu    sync.Mutex
	busy  map[string]int
	freed chan struct{} // closed, and replaced, when a slot frees
}

func newHostSlots(max int) *hostSlots {
	if max < 1 {
		return nil
	}
	return &hostSlots{max: max, busy: map[string]int{}, freed: make(chan struct{})}
}

func hostOf(u string) string {
	if p, err := url.Parse(u); err == nil {
		return p.Host
	}
	return u
}

// take picks the least busy server of urls that has a slot free, the first
// in urls on a tie, and waits while all are full. release gives the slot
// back; it may be called more than once.
func (h *hostSlots) take(ctx context.Context, urls []string) (u string, release func(), err error) {
	if h == nil {
		return urls[0], func() {}, nil
	}
	for {
		h.mu.Lock()
		best, bestN := "", 0
		for _, c := range urls {
			if n := h.busy[hostOf(c)]; n < h.max && (best == "" || n < bestN) {
				best, bestN = c, n
			}
		}
		if best != "" {
			host := hostOf(best)
			h.busy[host]++
			h.mu.Unlock()
			var once sync.Once
			return best, func() {
				once.Do(func() {
					h.mu.Lock()
					if h.busy[host]--; h.busy[host] <= 0 {
						delete(h.busy, host)
					}
					close(h.freed)
					h.freed = make(chan struct{})
					h.mu.Unlock()
				})
			}, nil
		}
		freed := h.freed
		h.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	}
}

// slotBody gives the server slot back when the body is closed.
type slotBody struct {
	io.ReadCloser
	release func()
}

func (b slotBody) Close() error {
	err := b.ReadCloser.Close()
	b.release()
	return err
}

// retry runs f until it succeeds, fails permanently, the context ends or
// the tries run out.
func (c *client) retry(ctx context.Context, what string, f func() error) error {
	pause := c.backoff
	for try := 0; ; try++ {
		err := f()
		if err == nil || ctx.Err() != nil {
			return err
		}
		var pe permanentError
		if errors.As(err, &pe) || try >= c.retries {
			return err
		}
		logf("retry %d/%d in %s: %s: %v", try+1, c.retries, pause, what, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pause):
		}
		if pause < 64*c.backoff {
			pause *= 2
		}
	}
}

// get sends a GET for u with the extra headers hdr. A 200, 206 or 416
// response is returned with its body to read and close; any other status
// is an error, permanent unless it is a 429 or a 5xx.
//
// The request is cancelled when no data arrives for c.stall, from the start
// to the end of the body: a server that stops sending would otherwise hold
// a worker for good. A read then fails with an error saying so, and the
// download is retried from where it stopped.
func (c *client) get(ctx context.Context, u string, hdr map[string]string) (*http.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	w := &watchdog{stall: c.stall, cancel: cancel}
	if c.stall > 0 {
		w.timer = time.AfterFunc(c.stall, func() { w.stalled.Store(true); cancel() })
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		w.stop()
		return nil, permanentError{err}
	}
	req.Header.Set("User-Agent", c.agent)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		w.stop()
		return nil, w.wrap(err)
	}
	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent,
		resp.StatusCode == http.StatusRequestedRangeNotSatisfiable: // the caller decides
		w.body = resp.Body
		resp.Body = w
		return resp, nil
	}
	w.stop()
	resp.Body.Close()
	err = statusError{u, resp.Status, resp.StatusCode}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, err
	}
	return nil, permanentError{err}
}

// mirrors are the URLs one file can be had from: the item's directory on
// each of its data nodes, in random order, and archive.org's /download/
// address, which only redirects to one of them. With many downloads at
// once the redirects are what limits the rate, so the nodes are asked
// directly, and /download/ only once each node has failed once, or when
// none is left. A node that answers 4xx is dropped.
type mirrors struct {
	mu       sync.Mutex
	nodes    []string
	front    string
	next     int
	failures int
}

func newMirrors(base, id, name string, nodes []string) *mirrors {
	p := escapePath(name)
	m := &mirrors{front: base + "/download/" + url.PathEscape(id) + "/" + p}
	for _, n := range nodes {
		m.nodes = append(m.nodes, n+"/"+p)
	}
	rand.Shuffle(len(m.nodes), func(i, j int) { m.nodes[i], m.nodes[j] = m.nodes[j], m.nodes[i] })
	return m
}

// pick returns the URLs to ask, best first; the first goes round the
// nodes from call to call.
func (m *mirrors) pick() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	all := slices.Clone(m.nodes)
	if len(all) == 0 || m.failures >= len(all) {
		all = append(all, m.front) // the front door joins the round
	}
	i := m.next % len(all)
	m.next++
	return append(all[i:], all[:i]...)
}

func (m *mirrors) fail() {
	m.mu.Lock()
	m.failures++
	m.mu.Unlock()
}

// drop takes a node out after a 4xx; it reports false for the front door.
func (m *mirrors) drop(u string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, n := range m.nodes {
		if n == u {
			m.nodes = slices.Delete(m.nodes, i, i+1)
			return true
		}
	}
	return false
}

// fetch is get from the next of m's URLs, the least busy one when there
// is a limit per server (c.hosts). A node that answers 4xx is dropped, and
// the error made one to retry: the next try asks another.
func (c *client) fetch(ctx context.Context, m *mirrors, hdr map[string]string) (*http.Response, string, error) {
	u, release, err := c.hosts.take(ctx, m.pick())
	if err != nil {
		return nil, "", err
	}
	resp, err := c.get(ctx, u, hdr)
	if err == nil {
		resp.Body = slotBody{resp.Body, release}
		return resp, u, nil
	}
	release()
	var se statusError
	if errors.As(err, &se) && se.code < 500 && se.code != http.StatusTooManyRequests && m.drop(u) {
		return nil, u, fmt.Errorf("%v; trying another server", se)
	}
	if ctx.Err() == nil {
		m.fail()
	}
	return nil, u, err
}

// nodeURLs makes the URLs of an item's directory on its data nodes, with
// the scheme of base. Odd host names or directories are left out.
func nodeURLs(base, dir string, hosts []string) []string {
	b, err := url.Parse(base)
	if err != nil || dir == "" || !strings.HasPrefix(dir, "/") || path.Clean(dir) != dir {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	for _, h := range hosts {
		if h == "" || seen[h] || strings.ContainsAny(h, "/\\@?#") {
			continue
		}
		seen[h] = true
		if u, err := url.Parse("//" + h); err != nil || u.Host != h {
			continue
		}
		out = append(out, b.Scheme+"://"+h+escapePath(dir))
	}
	return out
}

// watchdog is the body of a response from get, and cancels its request
// when no data arrives for stall.
type watchdog struct {
	body    io.ReadCloser
	stall   time.Duration
	timer   *time.Timer // nil without a stall limit
	cancel  context.CancelFunc
	stalled atomic.Bool
}

func (w *watchdog) Read(p []byte) (int, error) {
	n, err := w.body.Read(p)
	if n > 0 && w.timer != nil {
		w.timer.Reset(w.stall)
	}
	return n, w.wrap(err)
}

func (w *watchdog) Close() error {
	w.stop()
	return w.body.Close()
}

func (w *watchdog) stop() {
	if w.timer != nil {
		w.timer.Stop()
	}
	w.cancel()
}

func (w *watchdog) wrap(err error) error {
	if err != nil && err != io.EOF && w.stalled.Load() {
		return fmt.Errorf("no data for %s: %w", w.stall, err)
	}
	return err
}

// metadata fetches the file list of an item.
func (c *client) metadata(ctx context.Context, id string) (iaMeta, error) {
	var m iaMeta
	err := c.retry(ctx, "metadata "+id, func() error {
		resp, err := c.get(ctx, c.base+"/metadata/"+url.PathEscape(id), nil)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		var raw struct {
			Files []struct {
				Name   string `json:"name"`
				Source string `json:"source"`
				Size   string `json:"size"`
				MD5    string `json:"md5"`
			} `json:"files"`
			Dark     bool            `json:"is_dark"`
			Metadata json.RawMessage `json:"metadata"`
			Created  int64           `json:"created"`
			Dir      string          `json:"dir"`
			Servers  []string        `json:"workable_servers"`
			D1       string          `json:"d1"`
			D2       string          `json:"d2"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			return fmt.Errorf("metadata %s: %w", id, err)
		}
		m = iaMeta{Dark: raw.Dark, Exists: raw.Created != 0 || len(raw.Metadata) > 0 || len(raw.Files) > 0}
		if c.nodes {
			hosts := raw.Servers
			if len(hosts) == 0 {
				hosts = []string{raw.D1, raw.D2}
			}
			m.Nodes = nodeURLs(c.base, raw.Dir, hosts)
		}
		for _, f := range raw.Files {
			if f.Source != "original" || isGenerated(f.Name) {
				continue
			}
			size := int64(-1)
			if n, err := strconv.ParseInt(f.Size, 10, 64); err == nil {
				size = n
			}
			m.Files = append(m.Files, iaFile{Name: f.Name, Size: size, MD5: strings.ToLower(f.MD5)})
		}
		return nil
	})
	return m, err
}

// localPath is where a file of an item goes under dir. Names that would
// leave the item's directory are refused.
func localPath(dir, id string, f iaFile) (string, error) {
	if !filepath.IsLocal(id) || strings.ContainsAny(id, `/\`) {
		return "", fmt.Errorf("unsafe identifier %q", id)
	}
	name := filepath.FromSlash(f.Name)
	if !filepath.IsLocal(name) {
		return "", fmt.Errorf("%s: unsafe file name %q", id, f.Name)
	}
	return filepath.Join(dir, id, name), nil
}

// verify checks a complete file against the size and md5 of the metadata.
func verify(path string, f iaFile) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	h := md5.New()
	n, err := io.Copy(h, fh)
	if err != nil {
		return err
	}
	return check(f, n, h)
}

func check(f iaFile, n int64, h hash.Hash) error {
	if f.Size >= 0 && n != f.Size {
		return fmt.Errorf("size %d, expected %d", n, f.Size)
	}
	if f.MD5 != "" {
		if got := hex.EncodeToString(h.Sum(nil)); got != f.MD5 {
			return fmt.Errorf("md5 %s, expected %s", got, f.MD5)
		}
	}
	return nil
}

// download fetches a file of an item to path, through part (see partName),
// which a later run resumes. The file appears under its name only when its
// size and md5 match the metadata. nodes are the item's directories on its
// data nodes (iaMeta.Nodes).
func (c *client) download(ctx context.Context, id string, f iaFile, nodes []string, path, part string) error {
	if _, err := os.Stat(path); err == nil {
		// Left by an earlier run that stopped before recording it, or put
		// there by hand: keep it if it is the right file.
		if err := verify(path, f); err != nil {
			return permanentError{fmt.Errorf("%s exists and is not the file of the item (%v); move it away", path, err)}
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return permanentError{err}
	}
	m := newMirrors(c.base, id, f.Name, nodes)
	split := c.piece > 0 && c.split > 1 && f.Size > c.piece && c.db != nil
	err := c.retry(ctx, id+"/"+f.Name, func() error {
		if split {
			err := c.fetchSplit(ctx, id, f, m, part)
			if errors.Is(err, errNoRange) {
				split = false
				return fmt.Errorf("%w; downloading it in one go", err)
			}
			return err
		}
		if !c.acquire(ctx) {
			return ctx.Err()
		}
		defer c.release()
		return c.fetchPart(ctx, m, part, f)
	})
	if err != nil {
		return err
	}
	return os.Rename(part, path)
}

// acquire takes a connection token; false when the context ended first.
func (c *client) acquire(ctx context.Context) bool {
	if c.conns == nil {
		return true
	}
	select {
	case c.conns <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *client) release() {
	if c.conns != nil {
		<-c.conns
	}
}

// fetchSplit downloads a file of more than c.piece bytes in pieces of that
// size, c.split at a time while connection tokens last, each written to
// its place in part. A finished piece is recorded in the database, so the
// next try, or the next run, fetches only the pieces still missing. When
// all are there the whole part is checked against the size and md5 of the
// metadata.
//
// A part with no pieces recorded comes from a download from the start (an
// earlier version, or -piece 0): its whole pieces count as done.
func (c *client) fetchSplit(ctx context.Context, id string, f iaFile, m *mirrors, part string) error {
	fh, err := os.OpenFile(part, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return permanentError{err}
	}
	defer fh.Close()
	n := int((f.Size + c.piece - 1) / c.piece)
	end := func(i int) int64 { return min(int64(i+1)*c.piece, f.Size) }
	done, err := c.db.pieces(id, f.Name, c.piece)
	if err != nil {
		return permanentError{err}
	}
	if len(done) == 0 {
		fi, err := fh.Stat()
		if err != nil {
			return permanentError{err}
		}
		have := fi.Size()
		if have > f.Size {
			have = 0
		}
		var whole []int
		for i := 0; i < n && end(i) <= have; i++ {
			whole = append(whole, i)
			done[i] = true
		}
		if err := c.db.piecesDone(id, f.Name, c.piece, whole); err != nil {
			return permanentError{err}
		}
	}
	if err := fh.Truncate(f.Size); err != nil {
		return permanentError{err}
	}

	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var first firstErr
	mine := make(chan struct{}, max(c.split, 1)) // this file's share
	for i := 0; i < n && pctx.Err() == nil; i++ {
		if done[i] {
			continue
		}
		select {
		case mine <- struct{}{}:
		case <-pctx.Done():
		}
		if pctx.Err() != nil || !c.acquire(pctx) {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-mine }()
			defer c.release()
			start := int64(i) * c.piece
			err := c.fetchPiece(pctx, m, fh, start, end(i)-1)
			if err == nil {
				if err = c.db.piecesDone(id, f.Name, c.piece, []int{i}); err != nil {
					err = permanentError{err}
				}
			}
			if err != nil && pctx.Err() == nil {
				first.set(err)
				// The other pieces go on, so the next try has less to do,
				// unless trying again is useless.
				var pe permanentError
				if errors.Is(err, errNoRange) || errors.As(err, &pe) {
					cancel()
				}
			}
		}()
	}
	wg.Wait()
	if err := first.get(); err != nil {
		if errors.Is(err, errNoRange) {
			c.db.clearPieces(id, f.Name)
			fh.Truncate(0)
		}
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		return permanentError{err}
	}
	h := md5.New()
	got, err := io.Copy(h, fh)
	if err != nil {
		return err
	}
	if err := check(f, got, h); err != nil {
		c.db.clearPieces(id, f.Name)
		fh.Truncate(0)
		return fmt.Errorf("%s: %v; starting over", part, err)
	}
	if err := fh.Sync(); err != nil {
		return err
	}
	if err := c.db.clearPieces(id, f.Name); err != nil {
		return permanentError{err}
	}
	return nil
}

// fetchPiece downloads bytes start to end (inclusive) of a file into fh.
func (c *client) fetchPiece(ctx context.Context, m *mirrors, fh *os.File, start, end int64) error {
	resp, u, err := c.fetch(ctx, m, map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", start, end)})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusOK:
		return fmt.Errorf("%s: %w", u, errNoRange)
	default:
		return fmt.Errorf("%s: %s for bytes %d-%d", u, resp.Status, start, end)
	}
	if cr := resp.Header.Get("Content-Range"); !strings.HasPrefix(cr, fmt.Sprintf("bytes %d-%d/", start, end)) {
		return fmt.Errorf("%s: Content-Range %q for bytes %d-%d", u, cr, start, end)
	}
	n, err := io.Copy(io.NewOffsetWriter(fh, start), &counter{r: resp.Body, n: &c.read})
	if err != nil {
		return err
	}
	if want := end - start + 1; n != want {
		return fmt.Errorf("%s: %d bytes for bytes %d-%d", u, n, start, end)
	}
	return nil
}

// partName is the file path is downloaded through: path.part, unless that
// is the path of another file of the item (taken), which it would have
// overwritten, and then renamed away, after that file was recorded done;
// then path.part.1, .2, ... taken holds the item's paths in lower case,
// for file systems that ignore case.
func partName(path string, taken map[string]bool) string {
	part := path + ".part"
	for i := 1; taken[strings.ToLower(part)]; i++ {
		part = fmt.Sprintf("%s.part.%d", path, i)
	}
	return part
}

func escapePath(name string) string {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// fetchPart downloads into part, continuing what is there, and checks the
// result. A part that turns out wrong is removed, so the next try starts
// over.
func (c *client) fetchPart(ctx context.Context, m *mirrors, part string, f iaFile) error {
	fh, err := os.OpenFile(part, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return permanentError{err}
	}
	defer fh.Close()
	h := md5.New()
	have, err := io.Copy(h, fh)
	if err != nil {
		return err
	}
	if f.Size >= 0 && have > f.Size {
		// longer than the file: start over, md5 and all
		have = 0
		h.Reset()
	}
	if f.Size >= 0 && have == f.Size {
		if err := check(f, have, h); err != nil {
			fh.Truncate(0)
			return fmt.Errorf("%s: %v; starting over", part, err)
		}
		return nil
	}
	var hdr map[string]string
	if have > 0 {
		hdr = map[string]string{"Range": fmt.Sprintf("bytes=%d-", have)}
	}
	resp, u, err := c.fetch(ctx, m, hdr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if !strings.HasPrefix(resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-", have)) {
			return fmt.Errorf("%s: unexpected Content-Range %q", u, resp.Header.Get("Content-Range"))
		}
	case http.StatusOK:
		if have > 0 { // the server ignored the Range: start over
			have = 0
			h.Reset()
		}
	default:
		fh.Truncate(0)
		return fmt.Errorf("%s: %s; starting over", u, resp.Status)
	}
	if err := fh.Truncate(have); err != nil {
		return permanentError{err}
	}
	if _, err := fh.Seek(have, io.SeekStart); err != nil {
		return permanentError{err}
	}
	n, err := io.Copy(io.MultiWriter(fh, h), &counter{r: resp.Body, n: &c.read})
	if err != nil {
		return err // keep the part, the next try resumes it
	}
	if err := fh.Sync(); err != nil {
		return err
	}
	if err := check(f, have+n, h); err != nil {
		fh.Truncate(0)
		return fmt.Errorf("%s: %v; starting over", u, err)
	}
	return nil
}

type counter struct {
	r io.Reader
	n *atomic.Int64
}

func (c *counter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}
