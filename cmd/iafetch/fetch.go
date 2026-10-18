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
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
}

// generated are suffixes of the files archive.org makes itself for every
// item; they are no part of the upload and are not downloaded.
var generated = []string{
	"_meta.xml", "_files.xml", "_meta.sqlite", "_reviews.xml",
	".torrent", "_torrent.txt", "__ia_thumb.jpg",
}

func isGenerated(name string) bool {
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

type client struct {
	http    *http.Client
	base    string // https://archive.org
	agent   string
	retries int
	backoff time.Duration // first pause between tries; doubles, up to 64x
	read    atomic.Int64  // bytes downloaded, for the progress line
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

func (c *client) get(ctx context.Context, u string, hdr map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, permanentError{err}
	}
	req.Header.Set("User-Agent", c.agent)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent:
		return resp, nil
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
		return resp, nil // the caller decides
	}
	resp.Body.Close()
	err = fmt.Errorf("%s: %s", u, resp.Status)
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, err
	}
	return nil, permanentError{err}
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
		}
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			return fmt.Errorf("metadata %s: %w", id, err)
		}
		m = iaMeta{Dark: raw.Dark, Exists: raw.Created != 0 || len(raw.Metadata) > 0 || len(raw.Files) > 0}
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

// download fetches a file of an item to path, through path.part, which a
// later run resumes. The file appears under its name only when its size
// and md5 match the metadata.
func (c *client) download(ctx context.Context, id string, f iaFile, path string) error {
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
	part := path + ".part"
	u := c.base + "/download/" + url.PathEscape(id) + "/" + escapePath(f.Name)
	err := c.retry(ctx, id+"/"+f.Name, func() error { return c.fetchPart(ctx, u, part, f) })
	if err != nil {
		return err
	}
	return os.Rename(part, path)
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
func (c *client) fetchPart(ctx context.Context, u, part string, f iaFile) error {
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
		have = 0
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
	resp, err := c.get(ctx, u, hdr)
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
