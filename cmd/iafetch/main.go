// Command iafetch downloads archive.org items in the order of a list such as
// archive.org/import-order.csv, several items at a time, and records in a
// SQLite database what it got. An item recorded as done is skipped on later
// runs without asking archive.org, so the files can be moved away (after
// they are imported, say) and the next run goes on with the items after
// them.
//
//	iafetch [flags] import-order.csv
package main

import (
	"cmp"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const usage = `Usage: %[1]s [flags] LIST.csv

Downloads the archive.org items of LIST.csv, in its order, to DIR/ITEM/FILE
(-o sets DIR), -j items at a time, the files of an item one after another.

LIST.csv needs a header line with an "identifier" column. The columns
"order", "tier", "list" and "item_size" are used when present: items are
taken by "order" (else in file order), -tier and -list select by the others.
archive.org/import-order.csv in the mbox2nntp repository is such a list. An
identifier listed twice is taken once.

For each item the file list comes from the archive.org metadata. The files
of the upload are downloaded, not the ones archive.org makes itself
(_meta.xml, _files.xml, _meta.sqlite, .torrent and the like) nor the old
versions archive.org keeps under history/; -include and
-exclude choose among them by name. A file is written to FILE.part, resumed
from there after an interruption, and renamed to FILE when its size and md5
match the metadata. Failed requests are retried with a growing pause.

Files come straight from the data nodes the metadata names, archive.org's
/download/ only when a node lacks a file or each has failed (-nodes).
Each item has two nodes; a request goes to the less busy one, and waits
while both have -host-conns requests out, so that the items running at
once spread over the servers instead of piling onto one.

By default an item has one connection: archive.org gives one connection
well under 1 MB/s, so a fast line needs many items at once (-j 384 fills
1 Gbit/s). With -item-conns above 1, files larger than -piece MB are
fetched in pieces of that size, that many side by side; the database
records the finished pieces, so a stopped download resumes with the
missing ones. -conns caps the requests in flight in all.

The database (-db) records each downloaded file and the state of each item:
  done        every file downloaded and verified; skipped from then on,
              whether or not its files are still in DIR
  incomplete  a file failed; the next run downloads what is missing
  dark        taken down on archive.org ("is_dark")
  missing     archive.org does not know the identifier
  empty       none of its files to download
dark, missing and empty items are skipped too, until -retry-unavailable.
Only the database decides what is skipped: keep it when moving files.

Stop with Ctrl-C: the files being downloaded stay as .part and are resumed.

Flags:
`

type options struct {
	out, dbPath      string
	jobs, retries    int
	tiers, lists     map[string]bool
	match            *regexp.Regexp
	include, exclude *regexp.Regexp
	from, to, limit  int
	retryUnavailable bool
	dryRun, status   bool
	progress         time.Duration
	base             string
	backoff          time.Duration
	stall            time.Duration
	conns, meta      int   // requests at once (0: jobs x itemConns); metadata requests ahead
	itemConns        int   // requests at once for one item (pieces of a big file)
	hostConns        int   // requests at once to one server; 0 for no limit
	piece            int64 // bytes; 0 never splits
	nodes            bool  // ask the data nodes directly
}

func main() {
	log.SetFlags(log.LstdFlags)
	o, csvPath := parseFlags(os.Args[1:])
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, o, csvPath, os.Stdout); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (options, string) {
	o := options{base: "https://archive.org", backoff: 5 * time.Second, stall: 2 * time.Minute}
	var tiers, lists, match, include, exclude string
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&o.out, "o", "ia-download", "download to `DIR`/ITEM/FILE")
	fs.StringVar(&o.dbPath, "db", "iafetch.sqlite", "SQLite database `PATH` that records what was downloaded")
	fs.IntVar(&o.jobs, "j", 64, "download `N` items at a time, the files of each one after another")
	fs.IntVar(&o.itemConns, "item-conns", 1, "at most `N` connections per item: files larger than -piece come in N pieces side by side; 1 never splits")
	fs.IntVar(&o.hostConns, "host-conns", 4, "at most `N` connections to one archive.org server; 0 for no limit")
	fs.IntVar(&o.conns, "conns", 0, "at most `N` connections in all; 0 for -j times -item-conns")
	pieceMB := fs.Int("piece", 32, "with -item-conns above 1, fetch files larger than `MB` megabytes in pieces of that size; 0 for never")
	fs.IntVar(&o.meta, "meta", 8, "send up to `N` metadata requests at once, for up to 4N items ahead")
	fs.BoolVar(&o.nodes, "nodes", true, "download from the item's data nodes, as the metadata names them, instead of through archive.org/download/")
	fs.IntVar(&o.retries, "retries", 8, "retry a failed request `N` times, waiting 5s, 10s, 20s ... up to 5m20s")
	fs.StringVar(&tiers, "tier", "A,B0,B1,B2,B3,B4,C", "only items of these `TIERS`, comma-separated; empty for all")
	fs.StringVar(&lists, "list", "", "only items of these `LISTS` (the \"list\" column), comma-separated")
	fs.StringVar(&match, "match", "", "only items whose identifier matches this `REGEXP`")
	fs.StringVar(&include, "include", "", "only files whose name matches this `REGEXP`")
	fs.StringVar(&exclude, "exclude", "", "no files whose name matches this `REGEXP`")
	fs.IntVar(&o.from, "from", 0, "start at order number `N`")
	fs.IntVar(&o.to, "to", 0, "stop after order number `N`")
	fs.IntVar(&o.limit, "limit", 0, "take at most `N` items that are not yet done")
	fs.BoolVar(&o.retryUnavailable, "retry-unavailable", false, "try dark, missing and empty items again")
	fs.BoolVar(&o.dryRun, "n", false, "dry run: list the items that would be downloaded, with their size, and stop")
	fs.BoolVar(&o.status, "status", false, "print what the database records, by tier and state, and stop (LIST.csv not needed)")
	fs.DurationVar(&o.progress, "progress", time.Minute, "log a progress line this often; 0 for none")
	fs.DurationVar(&o.stall, "stall", o.stall, "give up on a request, and retry it, when no data arrives for this long; 0 to wait for ever")
	showVersion := fs.Bool("version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), usage, "iafetch")
		fs.PrintDefaults()
	}
	fs.Parse(args)
	if *showVersion {
		fmt.Println(versionString("iafetch"))
		os.Exit(0)
	}
	if (fs.NArg() != 1 && !o.status) || fs.NArg() > 1 || o.jobs < 1 || o.conns < 0 || o.itemConns < 1 || o.hostConns < 0 || o.meta < 1 || *pieceMB < 0 {
		fs.Usage()
		os.Exit(2)
	}
	o.piece = int64(*pieceMB) << 20
	o.tiers, o.lists = set(tiers), set(lists)
	for _, r := range []struct {
		s string
		p **regexp.Regexp
	}{{match, &o.match}, {include, &o.include}, {exclude, &o.exclude}} {
		if r.s != "" {
			re, err := regexp.Compile(r.s)
			if err != nil {
				log.Fatalf("ERROR: %v", err)
			}
			*r.p = re
		}
	}
	return o, fs.Arg(0)
}

func set(s string) map[string]bool {
	if s == "" {
		return nil
	}
	m := map[string]bool{}
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			m[v] = true
		}
	}
	return m
}

var logf = log.Printf

// item is one line of the list.
type item struct {
	Order      int
	Tier, List string
	ID         string
	Size       int64
}

func readList(r io.Reader) ([]item, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	head, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("reading the header: %w", err)
	}
	col := map[string]int{}
	for i, h := range head {
		col[strings.TrimPrefix(strings.TrimSpace(h), "\ufeff")] = i
	}
	idCol, ok := col["identifier"]
	if !ok {
		return nil, errors.New(`no "identifier" column`)
	}
	field := func(rec []string, name string) string {
		if i, ok := col[name]; ok && i < len(rec) {
			return strings.TrimSpace(rec[i])
		}
		return ""
	}
	var items []item
	seen := map[string]bool{}
	for line := 2; ; line++ {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if idCol >= len(rec) || strings.TrimSpace(rec[idCol]) == "" {
			continue
		}
		it := item{ID: strings.TrimSpace(rec[idCol]), Tier: field(rec, "tier"), List: field(rec, "list"), Order: line - 1}
		if s := field(rec, "order"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil {
				return nil, fmt.Errorf("line %d: order %q: %w", line, s, err)
			}
			it.Order = n
		}
		it.Size, _ = strconv.ParseInt(field(rec, "item_size"), 10, 64)
		if seen[it.ID] {
			continue
		}
		seen[it.ID] = true
		items = append(items, it)
	}
	// The list is normally in order already; a stable sort keeps file
	// order for equal numbers.
	slices.SortStableFunc(items, func(a, b item) int { return cmp.Compare(a.Order, b.Order) })
	return items, nil
}

func (o options) selected(it item) bool {
	switch {
	case o.tiers != nil && !o.tiers[it.Tier],
		o.lists != nil && !o.lists[it.List],
		o.match != nil && !o.match.MatchString(it.ID),
		o.from > 0 && it.Order < o.from,
		o.to > 0 && it.Order > o.to:
		return false
	}
	return true
}

func (o options) wantFile(name string) bool {
	return (o.include == nil || o.include.MatchString(name)) && (o.exclude == nil || !o.exclude.MatchString(name))
}

// job is one file to download; itemRun tracks the files of its item.
type job struct {
	run        *itemRun
	file       iaFile
	nodes      []string
	path, part string
}

type itemRun struct {
	item    item
	files   int   // files the item should have
	bytes   int64 // their size
	pending atomic.Int32
	failed  atomic.Int32
}

// firstErr keeps the first error set, for any number of goroutines. It
// was an atomic.Value, whose Store panics when two workers store errors of
// different types.
type firstErr struct{ p atomic.Pointer[error] }

func (f *firstErr) set(err error) { f.p.CompareAndSwap(nil, &err) }

func (f *firstErr) get() error {
	if p := f.p.Load(); p != nil {
		return *p
	}
	return nil
}

type stats struct {
	items, skipped, unavailable, filesDone, filesFailed atomic.Int64
	bytesDone                                           atomic.Int64
}

func run(ctx context.Context, o options, csvPath string, stdout io.Writer) error {
	d, err := openDB(o.dbPath)
	if err != nil {
		return err
	}
	defer d.Close()
	if o.status {
		return d.printStatus(stdout)
	}
	f, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	items, err := readList(f)
	f.Close()
	if err != nil {
		return fmt.Errorf("%s: %w", csvPath, err)
	}

	conns := o.conns
	if conns < 1 {
		conns = o.jobs * max(o.itemConns, 1)
	}
	// DefaultTransport's dial, TLS handshake and idle timeouts; the stall
	// watchdog (client.get) covers the rest
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 0 // no limit; per host as below
	tr.MaxIdleConnsPerHost = conns + o.meta + 1
	tr.ResponseHeaderTimeout = 2 * time.Minute
	c := &client{
		http:    &http.Client{Transport: tr},
		base:    strings.TrimRight(o.base, "/"),
		agent:   versionString("iafetch") + " (https://github.com/go-while/mbox2nntp)",
		retries: o.retries,
		backoff: o.backoff,
		stall:   o.stall,
		nodes:   o.nodes,
		piece:   o.piece,
		split:   o.itemConns,
		conns:   make(chan struct{}, conns),
		hosts:   newHostSlots(o.hostConns),
		db:      d,
	}
	var st stats
	jobs := make(chan []job) // the files of one item
	var wg sync.WaitGroup
	var dbErr firstErr
	for range o.jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for files := range jobs {
				for _, j := range files {
					if err := work(ctx, c, d, j, &st); err != nil {
						dbErr.set(err)
					}
				}
			}
		}()
	}
	if o.progress > 0 && !o.dryRun {
		go progress(ctx, o.progress, c, &st, o.jobs, conns)
	}

	// The metadata of the next items is fetched while the files of the
	// earlier ones download, o.meta requests at once, and up to 4 x o.meta
	// items ahead of the workers. An item goes to a worker as soon as its
	// metadata is in, so the order is kept only roughly: in strict order, one
	// metadata request that hung (archive.org's take minutes now and then)
	// held up every item behind it, and the workers ran dry.
	type ahead struct {
		it  item
		m   iaMeta
		err error
	}
	metaN := max(o.meta, 1)
	window := make(chan struct{}, 4*metaN) // items fetched ahead, not yet with a worker
	metaReq := make(chan struct{}, metaN)  // metadata requests at once
	ready := make(chan *ahead, 4*metaN)
	var fetching sync.WaitGroup
	scheduled := make(chan error, 1)
	go func() {
		var err error
		for a := range ready {
			if err == nil && ctx.Err() == nil {
				e := a.err
				if e == nil {
					e = schedule(ctx, o, d, a.it, a.m, jobs, &st)
				}
				if e != nil && ctx.Err() == nil {
					// The metadata could not be had even with retries, or the
					// item could not be queued: note it and go on with the next.
					logf("FAILED %s: %v", a.it.ID, e)
					err = d.setItem(a.it, stIncomplete, 0, 0, e.Error())
				}
				if err != nil {
					dbErr.set(err)
				}
			}
			<-window
		}
		close(jobs)
		scheduled <- err
	}()

	start := time.Now()
	var queuedBytes int64
	taken := 0
	for _, it := range items {
		if ctx.Err() != nil || dbErr.get() != nil {
			break
		}
		if !o.selected(it) {
			continue
		}
		status, err := d.itemStatus(it.ID)
		if err != nil {
			return err
		}
		switch status {
		case stDone:
			st.skipped.Add(1)
			continue
		case stDark, stMissing, stEmpty:
			if !o.retryUnavailable {
				st.skipped.Add(1)
				continue
			}
		}
		if o.limit > 0 && taken >= o.limit {
			break
		}
		taken++
		if o.dryRun {
			fmt.Fprintf(stdout, "%6d %-3s %-18s %10.2f GB  %s\n", it.Order, it.Tier, it.List, float64(it.Size)/1e9, it.ID)
			queuedBytes += it.Size
			continue
		}
		select {
		case window <- struct{}{}:
		case <-ctx.Done():
			continue // the loop ends at the top
		}
		a := &ahead{it: it}
		fetching.Add(1)
		go func() {
			defer fetching.Done()
			select {
			case metaReq <- struct{}{}:
				a.m, a.err = c.metadata(ctx, a.it.ID)
				<-metaReq
			case <-ctx.Done():
				a.err = ctx.Err()
			}
			ready <- a // never blocks: ready holds a whole window
		}()
	}
	fetching.Wait()
	close(ready)
	schedErr := <-scheduled
	wg.Wait()
	if err := dbErr.get(); err != nil {
		return err
	}
	if schedErr != nil {
		return schedErr
	}
	if o.dryRun {
		fmt.Fprintf(stdout, "%d items, %.2f GB (item sizes from the list); %d skipped as done or unavailable\n",
			taken, float64(queuedBytes)/1e9, st.skipped.Load())
		return nil
	}
	el := time.Since(start)
	logf("finished in %s: %d items done, %d skipped (recorded before), %d unavailable; %d files, %.2f GB downloaded, %d files failed",
		el.Round(time.Second), st.items.Load(), st.skipped.Load(), st.unavailable.Load(),
		st.filesDone.Load(), float64(st.bytesDone.Load())/1e9, st.filesFailed.Load())
	if ctx.Err() != nil {
		return errors.New("interrupted; run again to go on")
	}
	if n := st.filesFailed.Load(); n > 0 {
		return fmt.Errorf("%d files failed; run again to retry them", n)
	}
	return nil
}

// schedule queues the files an item still needs, from its metadata m.
func schedule(ctx context.Context, o options, d *db, it item, m iaMeta, jobs chan<- []job, st *stats) error {
	switch {
	case m.Dark:
		logf("UNAVAILABLE %s: taken down on archive.org (dark)", it.ID)
		st.unavailable.Add(1)
		return d.setItem(it, stDark, 0, 0, "is_dark")
	case !m.Exists:
		logf("UNAVAILABLE %s: no such item on archive.org", it.ID)
		st.unavailable.Add(1)
		return d.setItem(it, stMissing, 0, 0, "no metadata")
	}
	var files []iaFile
	var bytes int64
	for _, f := range m.Files {
		if o.wantFile(f.Name) {
			files = append(files, f)
			bytes += max(f.Size, 0)
		}
	}
	if len(files) == 0 {
		logf("UNAVAILABLE %s: no files to download", it.ID)
		st.unavailable.Add(1)
		return d.setItem(it, stEmpty, 0, 0, fmt.Sprintf("%d original files, none selected", len(m.Files)))
	}
	done, err := d.doneFiles(it.ID)
	if err != nil {
		return err
	}
	r := &itemRun{item: it, files: len(files), bytes: bytes}
	// every file of the item, so that no .part is named like one of them
	taken := map[string]bool{}
	for _, f := range m.Files {
		if p, err := localPath(o.out, it.ID, f); err == nil {
			taken[strings.ToLower(p)] = true
		}
	}
	var todo []job
	for _, f := range files {
		if done[f.Name] {
			continue
		}
		p, err := localPath(o.out, it.ID, f)
		if err != nil {
			return err
		}
		todo = append(todo, job{run: r, file: f, nodes: m.Nodes, path: p, part: partName(p, taken)})
	}
	if len(todo) == 0 {
		st.items.Add(1)
		return d.setItem(it, stDone, r.files, r.bytes, "")
	}
	logf("item %d %s: %d files, %.2f GB (%d done before)", it.Order, it.ID, len(todo), float64(bytes)/1e9, len(files)-len(todo))
	r.pending.Store(int32(len(todo)))
	select {
	case jobs <- todo:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// work downloads one file and, when it was the last of its item, records
// the item. Only database errors are returned; they stop the run.
func work(ctx context.Context, c *client, d *db, j job, st *stats) error {
	r := j.run
	err := c.download(ctx, r.item.ID, j.file, j.nodes, j.path, j.part)
	if err == nil {
		if err := d.fileDone(r.item.ID, j.file, j.path); err != nil {
			return err
		}
		st.filesDone.Add(1)
		st.bytesDone.Add(max(j.file.Size, 0))
	} else if ctx.Err() == nil {
		logf("FAILED %s/%s: %v", r.item.ID, j.file.Name, err)
		st.filesFailed.Add(1)
		r.failed.Add(1)
	}
	if r.pending.Add(-1) != 0 || ctx.Err() != nil {
		return nil
	}
	if n := r.failed.Load(); n > 0 {
		return d.setItem(r.item, stIncomplete, r.files, r.bytes, fmt.Sprintf("%d files failed", n))
	}
	st.items.Add(1)
	logf("done %d %s", r.item.Order, r.item.ID)
	return d.setItem(r.item, stDone, r.files, r.bytes, "")
}

func progress(ctx context.Context, every time.Duration, c *client, st *stats, jobs, conns int) {
	t := time.NewTicker(every)
	defer t.Stop()
	last, lastT := c.read.Load(), time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			n := c.read.Load()
			rate := float64(n-last) / now.Sub(lastT).Seconds()
			last, lastT = n, now
			logf("progress: %d items done, %d files, %.2f GB read, %.1f MB/s over the last %s (%d items at a time, %d of %d connections busy)",
				st.items.Load(), st.filesDone.Load(), float64(n)/1e9, rate/1e6, every, jobs, len(c.conns), conns)
		}
	}
}
