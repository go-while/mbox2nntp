package main

import (
	//"config"
	"crypto/sha256"
	//"encoding/binary"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/textproto"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	//"regexp"
	"sort"
	"sync"
	"time"
	//"runtime"
	//"math/rand"
	//"github.com/sam-falvo/mbox-giganews-archive.org-mod"
	//"github.com/edsrzf/mmap-go"
	"github.com/go-while/go-lib-mbox2nntp"
	"github.com/go-while/mbox2nntp/internal/unpack"
	"github.com/go-while/mbox2nntp/internal/version"
	tsm_GlobalBools "github.com/go-while/mbox2nntp/tsm/GlobalBools" // package is tsm_globalbools; alias keeps existing call sites
	//"nntp/data/overview"
)

const (
	DEBUG bool = false
	// queue this many articles. Workers are network-bound and the parser is
	// not, so a big queue only fills up: at 1,000,000 a single 73k-article
	// archive reached 2.7 GB resident. A few thousand covers any jitter.
	WORKER_CHAN_SIZE int    = 4096
	CRLF             string = "\r\n"
	DOT              string = "."
)

type ArticleItem struct {
	msgid string
	head  []string
	body  []string
	tries int // times requeued after 436 or a failed connection
} // end ArticleItem struct

var (
	DROPBINARY          = true
	LAXMID       bool   = true
	my_mboxfile  string = ""
	PREHASH_MODE bool   = true
	//MaxCPUs = 2
	MaxWorkers = 1
	tmpdir     = "."
	nntpserv   = ""
	// NNTP_TIMEOUT bounds every read and write on a backend connection, so
	// a peer that stops responding fails the call instead of hanging the
	// worker. Generous: it detects a dead peer, it is not a latency limit.
	NNTP_TIMEOUT = 2 * time.Minute
	// MAX_TRIES caps how often one article is requeued after a 436 or a
	// failed connection; after that it counts as an error.
	MAX_TRIES      = 10
	retryDelay     = time.Second // backoff unit between tries
	DRYRUN         bool
	NODOS2UNIX     bool // skip -dos2unix: -no-dos2unix, or a .nodos2unix file in the working directory
	globalBools    tsm_GlobalBools.GlobalBools
	globalCounter  GlobalCounter
	worker_channel chan *ArticleItem
)

// GlobalCounter is safe to use concurrently.
type GlobalCounter struct {
	v   map[string]int
	mux sync.Mutex
}

func (c *GlobalCounter) incCounter(wid int, countername string) {
	//log.Printf("incCounter Worker %d", wid)
	c.mux.Lock()
	c.v[countername]++
	c.mux.Unlock()
} // end func globalCounter.incCounter

func (c *GlobalCounter) decCounter(wid int, countername string) {
	//log.Printf("decCounter Worker %d", wid)
	c.mux.Lock()
	c.v[countername]--
	c.mux.Unlock()
} // end func globalCounter.decCounter

func (c *GlobalCounter) addCounter(wid int, countername string, value int) {
	//log.Printf("addCounter Worker %d", wid)
	c.mux.Lock()
	c.v[countername] += value
	c.mux.Unlock()
} // end func globalCounter.addCounter

func (c *GlobalCounter) subCounter(wid int, countername string, value int) {
	//log.Printf("subCounter Worker %d", wid)
	c.mux.Lock()
	c.v[countername] -= value
	c.mux.Unlock()
} // end func globalCounter.subCounter

func (c *GlobalCounter) getCounter(countername string) int {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.v[countername]
} // end func globalCounter.getCounter

func (c *GlobalCounter) setCounter(countername string, value int) int {
	c.mux.Lock()
	c.v[countername] = value
	defer c.mux.Unlock()
	return c.v[countername]
} // end func globalCounter.getCounter

func (c *GlobalCounter) initCounter(countername string) {
	c.mux.Lock()
	c.v[countername] = 0
	c.mux.Unlock()
} // end func globalCounter.setCounter

/*
// GlobalBools is safe to use concurrently.
type GlobalBools struct {
	v   map[string]bool
	mux sync.RWMutex
}

// get all global bools for users session
func (c *GlobalBools) getAllGB() (map[string]bool){
	retval := make(map[string]bool)
	c.mux.Lock()
	for name, boolv := range c.v {
		aname := string(name)
		var setv bool
		if boolv {
			setv = true
		} else {
			setv = false
		}
		retval[aname] = setv
	}
	c.mux.Unlock()
	return retval
}

// set a global bool by name
func (c *GlobalBools) setGB(name string, value bool) {
	//log.Printf("setGB name=%s value=%t", name, value)
	c.mux.Lock()
	c.v[name] = value
	c.mux.Unlock()
}

// switch a global bool by name
func (c *GlobalBools) switchGB(name string) {
	c.mux.Lock()
	switch c.v[name] {
		case true:
			c.v[name] = false
		case false:
			c.v[name] = true
	}
	boolvalue := c.v[name]
	log.Printf("OK: switchGB name=%s value=%t", name, boolvalue)
	c.mux.Unlock()

} // end func globalBools.SwitchGB

// set a global bool by name and return a true or false on success change or fail
func (c *GlobalBools) setGBR(name string, value bool) bool {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.v[name] != value {
		c.v[name] = value
		return true
	}
	return false
} // end func globalBools.SetGBR


// get a global bool by name
func (c *GlobalBools) getGB(name string) bool {
	//log.Printf("GlobalBools: getGB=%s read", name)
	c.mux.RLock()
	retval := c.v[name]
	c.mux.RUnlock()
	//log.Printf("GlobalBools: getGB=%s retval=%t", name, retval)
	return retval
} // end func globalBools.GetGB
*/

// deadlineConn sets a fresh NNTP_TIMEOUT deadline before every Read and
// Write: net.DialTimeout covers only the dial.
type deadlineConn struct {
	net.Conn
	timeout time.Duration
}

func (c *deadlineConn) Read(p []byte) (int, error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.timeout))
	return c.Conn.Read(p)
}

func (c *deadlineConn) Write(p []byte) (int, error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.timeout))
	return c.Conn.Write(p)
}

func connectBackend(rserver string) (*textproto.Conn, *bufio.Writer, error) {
	//conn, err := net.DialTimeout("tcp4", rserver, 5 * time.Second)
	//conn, err := net.DialTimeout("tcp6", rserver, 5 * time.Second)
	conn, err := net.DialTimeout("tcp", rserver, 9*time.Second)

	if err != nil {
		log.Printf("error net.Dial err=%v", err)
		return nil, nil, err
	}

	dconn := &deadlineConn{Conn: conn, timeout: NNTP_TIMEOUT}
	srvtp := textproto.NewConn(dconn)
	CliWriter := bufio.NewWriterSize(dconn, 64*1024) // an article in one or two writes
	code, msg, err := srvtp.ReadCodeLine(20)

	if code != 200 && code != 201 {
		log.Printf("connectBackend code=%d msg='%s' err='%v'", code, msg, err)
		conn.Close()
		if err == nil {
			// any other 2xx passes ReadCodeLine(20): without this the
			// caller got (nil, nil, nil) and crashed on the nil conn
			err = fmt.Errorf("connectBackend: unexpected greeting %d %s", code, msg)
		}
		return nil, nil, err
	}

	return srvtp, CliWriter, nil
}

// closeBackend says QUIT, without waiting for the answer, and closes.
func closeBackend(srvtp *textproto.Conn, cliwriter *bufio.Writer) {
	if srvtp == nil {
		return
	}
	io.WriteString(cliwriter, "QUIT"+CRLF)
	cliwriter.Flush()
	srvtp.Close()
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if err != nil {
		// not only ENOENT: EACCES/ENOTDIR/ELOOP/ENAMETOOLONG also leave info==nil
		return false
	}
	return !info.IsDir()
}

// argPath keeps a file name from being read as an option by an external
// tool: a relative name starting with '-' gets a "./" prefix.
func argPath(p string) string {
	if strings.HasPrefix(p, "-") {
		return "./" + p
	}
	return p
}

// walkFiles calls fn for every regular file under dir, in the order
// find.sh gave: `find "$1" -mindepth 1 -type f | sort -V`. Each directory
// is read and sorted on its own (versionLess), then walked depth-first,
// which reproduces sort -V over the full paths without holding them all.
// Symlinks are not followed and unreadable directories are logged and
// skipped, as find does.
func walkFiles(dir string, fn func(path string)) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	// In the full paths sort -V compares, a directory's name is followed
	// by its "/".
	key := func(e os.DirEntry) string {
		if e.IsDir() {
			return e.Name() + "/"
		}
		return e.Name()
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return versionLess(key(entries[i]), key(entries[j]))
	})
	for _, e := range entries {
		p := filepath.Join(dir, e.Name())
		switch {
		case e.IsDir():
			if err := walkFiles(p, fn); err != nil {
				log.Printf("ERROR dir walk '%s': %v", p, err)
			}
		case e.Type().IsRegular():
			fn(p)
		}
	}
	return nil
}

// versionLess orders two names of one directory as GNU sort -V orders the
// full paths: gnulib filevercmp, i.e. verrevcmp without the file suffixes
// first, then with them, then plain bytes as sort's last resort. Digit
// runs compare by value, so article 2 comes before article 10.
func versionLess(a, b string) bool {
	pa, pb := suffixStart(a), suffixStart(b)
	r := verrevcmp(a[:pa], b[:pb])
	if r == 0 && (pa != len(a) || pb != len(b)) {
		r = verrevcmp(a, b)
	}
	if r == 0 {
		return a < b
	}
	return r < 0
}

// suffixStart is gnulib's file_prefixlen: where a trailing run of
// suffixes like ".tar.gz" (dot, letter or '~', then letters, digits, '~')
// starts. A suffix may start at s[0] (".overview"): in the full path that
// gnulib sees, a '/' precedes the name.
func suffixStart(s string) int {
	isAlpha := func(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
	prefix := 0
	for i := 0; i < len(s); {
		if s[i] == '.' && i+1 < len(s) && (isAlpha(s[i+1]) || s[i+1] == '~') {
			for i += 2; i < len(s) && (isAlpha(s[i]) || isDigit(s[i]) || s[i] == '~'); i++ {
			}
			continue
		}
		i++
		prefix = i
	}
	return prefix
}

// verrevcmp is gnulib's: non-digit runs compare by verOrder, digit runs
// by value with leading zeros skipped.
func verrevcmp(a, b string) int {
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		for (i < len(a) && !isDigit(a[i])) || (j < len(b) && !isDigit(b[j])) {
			ca, cb := 0, 0
			if i < len(a) {
				ca = verOrder(a[i])
			}
			if j < len(b) {
				cb = verOrder(b[j])
			}
			if ca != cb {
				return ca - cb
			}
			i++
			j++
		}
		for i < len(a) && a[i] == '0' {
			i++
		}
		for j < len(b) && b[j] == '0' {
			j++
		}
		firstDiff := 0
		for i < len(a) && j < len(b) && isDigit(a[i]) && isDigit(b[j]) {
			if firstDiff == 0 {
				firstDiff = int(a[i]) - int(b[j])
			}
			i++
			j++
		}
		if i < len(a) && isDigit(a[i]) {
			return 1
		}
		if j < len(b) && isDigit(b[j]) {
			return -1
		}
		if firstDiff != 0 {
			return firstDiff
		}
	}
	return 0
}

func verOrder(c byte) int {
	switch {
	case isDigit(c):
		return 0
	case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
		return int(c)
	case c == '~':
		return -1
	}
	return int(c) + 256
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// extractedPath is where an archive's mbox is unpacked to: tmpdir, under
// the archive's base name without ext. Every extractor writes there.
func extractedPath(archive, ext string) string {
	return filepath.Join(tmpdir, strings.TrimSuffix(filepath.Base(archive), ext))
}

// unpackOrExit runs fn to extract archive to dst, and exits on failure. The
// extractors overwrite dst: a file left by an interrupted run may be
// truncated, and must not be imported in place of the real one.
func unpackOrExit(kind string, fn func(archive, dst string) error, archive, dst string) {
	log.Printf("%s %s to %s", kind, archive, dst)
	if err := fn(archive, dst); err != nil {
		log.Printf("%s failed mboxfile='%s' err='%v' =exit(1)", kind, archive, err)
		os.Remove(dst)
		os.Exit(1)
	}
}

// gunzipTo decompresses archive to dst in-process. It used to run gzip, or
// /scripts/gunzip.sh when a tmpdir was set, a script that is not in the
// repository. compress/gzip reads concatenated members as one stream, as
// the usenet-* archives need: they hold one gzip member per article.
func gunzipTo(archive, dst string) error {
	in, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer in.Close()
	zr, err := gzip.NewReader(bufio.NewReaderSize(in, 1024*1024))
	if err != nil {
		return err
	}
	return unpack.WriteTo(dst, zr)
}

// unzipTo extracts the entry named like dst from a zip archive. It replaces
// /usr/bin/unzip -o -d tmpdir, which also unpacked every other entry and
// left them in tmpdir. Every usenet-clari zip holds just NAME.mbox.
func unzipTo(archive, dst string) error {
	z, err := zip.OpenReader(archive)
	if err != nil {
		return err
	}
	defer z.Close()
	name := filepath.Base(dst)
	for _, f := range z.File {
		if f.Name != name {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer rc.Close()
		return unpack.WriteTo(dst, rc) // the zip reader checks the CRC at the end
	}
	return fmt.Errorf("no entry '%s' in the archive", name)
}

func rmmbox(mboxfile string) {
	err := os.Remove(mboxfile)
	if err != nil {
		log.Printf("rmmbox failed mboxfile='%s' err='%v'", mboxfile, err)
		os.Exit(1)
	}
	log.Printf("cleanup mboxfile='%s' err='%v'", mboxfile, err)
} // end func rmmbox

func mboxhasZ(mboxfile string) bool {
	has_gz := false
	has_zip := false
	has_zip7 := false

	gz := mboxfile + ".gz"
	zip := mboxfile + ".zip"
	zip7 := mboxfile + ".7z"

	if fileExists(gz) {
		has_gz = true
	}

	if fileExists(zip) {
		has_zip = true
	}

	if fileExists(zip7) {
		has_zip7 = true
	}

	if has_gz || has_zip || has_zip7 {

		if has_gz {
			mvmbox(gz)
		}

		if has_zip {
			mvmbox(zip)
		}

		if has_zip7 {
			mvmbox(zip7)
		}

		//rmmbox(mboxfile)
		return true
	}

	return false
} // end func mboxhasZ

// DOS2UNIX, when set with -dos2unix, is run on every mbox before parsing,
// unless NODOS2UNIX. It used to be /usr/bin/dos2unix and always run; the
// parser handles CRLF itself, and on the whole archive.org corpus every
// article comes out byte-identical without it.
var DOS2UNIX = ""

func dos2unix(mboxfile string) {
	if DOS2UNIX == "" || NODOS2UNIX {
		return
	}
	log.Printf("proc dos2unix %s", mboxfile)
	cmd := exec.Command(DOS2UNIX, "-f", argPath(mboxfile))
	err := cmd.Run()
	if err != nil {
		log.Printf("dos2unix failed mboxfile='%s' err='%v'", mboxfile, err)
		os.Exit(1)
	}
} // end func dos2unix

func mvmbox(mboxfile string) {
	if !globalBools.GetGB("done") {
		log.Printf("ERROR MBOX='%s'", mboxfile)
		return
	}
	mmboxfile := fmt.Sprintf("%s.done", mboxfile)
	err := os.Rename(mboxfile, mmboxfile)
	if err != nil {
		log.Printf("mvmbox failed mboxfile='%s' err='%v'", mboxfile, err)
		os.Exit(1)
	} else {
		log.Printf("OK mvmbox: '%s'", mmboxfile)
	}
} // end func mvmbox

func read_file(file_path string) string {

	lines, err0 := ioutil.ReadFile(file_path)
	if err0 != nil {
		log.Printf("read_file(%s) ReadFile err0='%v'", file_path, err0)
		return ""
	}

	if len(lines) <= 1 {
		log.Printf(" error empty file='%s'", file_path)
		return ""
	}

	//log.Printf(" -> read %d bytes fp='%s'", len(lines), filepath)

	//var headb []byte
	var header []string
	var body []string

	msglines, err1 := articleLines(lines)
	if err1 != nil {
		log.Printf("ERROR read_file(%s) ReadDotLines err1='%v'", file_path, err1)
		return ""
	}
	if len(msglines) <= 0 {
		log.Printf("ERROR read_file(%s) msglines=%d", file_path, len(msglines))
		return ""
	}

	read_head := true
	has_dist_local := false
	has_hdr_subj := false
	has_hdr_from := false
	has_hdr_msgid := false

	continue_nextline := false
	rxbh, rxbb := 0, 0

	for i, line := range msglines {
		if continue_nextline {
			continue_nextline = false
			continue
		}
		if read_head && (line == "\r\n" || line == "") {
			//log.Printf("break reading head")
			//header = append(header, line)
			read_head = false
			continue // the blank separator belongs to neither head nor body
		}
		if read_head {

			line = cleanHeadline(line)
			if !has_hdr_msgid && line == "Message-ID:" {
				if i+1 < len(msglines) && strings.HasPrefix(msglines[i+1], " <") {
					line = "Message-ID: " + msglines[i+1]
					continue_nextline = true
				}
				has_hdr_msgid = true

			}
			if !has_hdr_subj && strings.HasPrefix(line, "Subject: ") && len(line) > 9 {
				has_hdr_subj = true
			}
			if !has_hdr_subj && line == "Subject: " {
				line = "Subject: -"
				has_hdr_subj = true
			}
			if !has_hdr_from && strings.HasPrefix(line, "From: ") && len(line) > 6 {
				has_hdr_from = true
			}
			if !has_hdr_from && line == "From: " {
				line = "From: anonymous"
				has_hdr_from = true
			}
			if !has_dist_local && line == "Distribution: local" {
				has_dist_local = true
				continue
			}
			rxbh += len(line)
			/*
				if !regex_validhead.MatchString(line) && !strings.HasPrefix(line, " ") {
				//if !strings.HasPrefix(line, " ") && !strings.Contains(line, ":") {
					// missing colon in this header line and not prefixed with a space ... dnews does this?
					log.Printf(":: missing colon line='%s'", line)
					/,*
					 * |X-User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/53
					 * |7.36
					 *
					 *,/
					prev_ind := len(header)-1
					if prev_ind > 0 {
						//log.Printf(":: len(header)=%d prev_ind=%d", len(header), prev_ind)

						prev_line := header[prev_ind]
						//if bytes.IndexByte(header[prev_ind], ':') < 0 {
						if strings.Contains(prev_line, ":") {
							// add this line to previous line
							//prev_line := header[prev_ind]
							header[prev_ind] = prev_line + line
							continue
						}
					}
				}
			*/
			header = append(header, line)
		} else {
			if DROPBINARY && strings.HasPrefix(line, "=ybegin") {
				log.Printf("DROP BINARY file='%s'", file_path)
				return ""
			}
			/*
				if strings.Contains(line, "\r") {
					log.Printf("ERROR BODY LINE CONTAINS CR")
				}
				if strings.Contains(line, "\n") {
					log.Printf("ERROR BODY LINE CONTAINS LF")
				}
			*/
			rxbb += len(line)
			body = append(body, line)
		}
	}
	if !has_hdr_subj {
		header = append(header, "Subject: --")
	}
	if !has_hdr_from {
		header = append(header, "From: anonymous")
	}

	/*
		for _, headline := range header {
			headb = append(headb, []byte(headline+"\r\n")...)
		}
	*/

	/*
		r2 := textproto.NewReader(bufio.NewReader(bytes.NewReader(headb)))
		//r2 := textproto.NewReader(header)
		head, err2 := r2.ReadMIMEHeader()
		r2 = nil
		if err2 != nil {
			log.Printf("ERROR read_file(%s) ReadMIMEHeader err2='%v'", filepath, err2)
			return ""
		}
	*/

	if rxbh == 0 || rxbb == 0 {
		log.Printf("ERROR rxbh=%d rxbb=%d fp='%s'", rxbh, rxbb, file_path)
		return ""
	}

	//msgid := extract_msgid(header)
	//headermap, keysorder, msgid, err := overview.ParseHeaderKeys(&head)
	_, _, msgid, err := ParseHeaderKeys(header, LAXMID)
	if msgid == "" || err != nil {
		log.Printf("Error read_file(fp='%s') msgid not found err='%v'", file_path, err)
	}

	body = body_remove_finalemptylines(body)

	//head = nil
	if msgid == "" || len(header) < 4 || len(body) == 0 {
		log.Printf("ERROR read_file(fp='%s') extract_msgid msgid='%s' <- empty?? head=%d body=%d", file_path, msgid, len(header), len(body))
		return ""
	}
	/*
		debug := false
		if debug {
			log.Printf(":: len(head)=%d", len(header))

			var retlines []string
			for _, line := range header {
				retlines = append(retlines, line)
			}

			for _, line := range body {
				retlines = append(retlines, line)
			}

			log.Printf(":: retlines=%d", len(retlines))
		}
	*/
	globalCounter.incCounter(0, "msgnums")
	worker_channel <- &ArticleItem{msgid: msgid, head: header, body: body}
	return msgid
} // end read_file

// articleLines splits an article file into lines without line endings.
// A file whose last line is a lone "." is in wire format, as INN stores
// articles with wireformat or in timehash: dot-stuffed and terminated, so
// it is dot-decoded. Any other file is a plain article and is taken as it
// is. All files used to be dot-decoded, which ended a plain article at a
// body line holding only "." and took a dot off every line starting "..".
func articleLines(data []byte) ([]string, error) {
	last := bytes.TrimSuffix(bytes.TrimSuffix(data, []byte("\n")), []byte("\r"))
	if bytes.Equal(last, []byte(".")) || bytes.HasSuffix(last, []byte("\n.")) {
		return textproto.NewReader(bufio.NewReader(bytes.NewReader(data))).ReadDotLines()
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimSuffix(l, "\r")
	}
	return lines, nil
}

func body_remove_finalemptylines(body []string) []string {
	//oldlen := len(body)
	d := 0
	for {
		if len(body) == 0 {
			break
		}
		if body[len(body)-1] != "" && body[len(body)-1] != "\n" && body[len(body)-1] != "\r\n" {
			break
		}
		body = body[:len(body)-1]
		d++
	}
	if d > 0 {
		//log.Printf("removed %d empty lines body=%d", d, len(body))
	}
	return body
} // end func body_remove_finalemptylines

func main() {
	started := UnixTimeSec()

	//runtime.GOMAXPROCS(MaxCPUs)
	var err error

	globalBools = tsm_GlobalBools.GlobalBools{V: make(map[string]bool)}
	globalCounter = GlobalCounter{v: make(map[string]int)}
	worker_channel = make(chan *ArticleItem, WORKER_CHAN_SIZE)
	globalCounter.initCounter("worker_done")

	opts, err := parseArgs(os.Args[0], os.Args[1:], os.Stderr)
	if err == flag.ErrHelp {
		os.Exit(0)
	}
	if err != nil {
		log.Printf("ERROR: %v (see -h)", err)
		os.Exit(2)
	}
	if opts.version {
		fmt.Println(version.String("mbox2nntp"))
		os.Exit(0)
	}
	if opts.legacy {
		log.Printf("NOTE: positional arguments are deprecated, use flags (see -h)")
	}
	log.Printf("%s", version.String("mbox2nntp"))
	opts.apply()
	mboxfile := opts.input
	if !fileExists(mboxfile) && !dirExists(mboxfile) {
		log.Printf("ERROR: input not found '%s'", mboxfile)
		os.Exit(1)
	}
	log.Printf("server=%s DRYRUN=%t DROPBINARY=%t dos2unix=%q nodos2unix=%t prehash=%t laxmid=%t timeout=%v max-tries=%d",
		nntpserv, DRYRUN, DROPBINARY, DOS2UNIX, NODOS2UNIX, PREHASH_MODE, LAXMID, NNTP_TIMEOUT, MAX_TRIES)
	log.Printf("MaxWorkers=%d", MaxWorkers)
	log.Printf("TMPDIR=%s", tmpdir)

	is_zip7 := false
	is_zip := false
	is_gzp := false
	is_mbx := false
	is_dir := false
	is_bag := false
	uatsuffix := uatSuffix(mboxfile) // a UAT archive: .usenet, .usenet.xz, .usenet.7z
	group := ""

	if strings.HasSuffix(mboxfile, ".mbox.zip") {
		is_zip = true
		is_mbx = true
		group = strings.Replace(mboxfile, ".mbox.zip", "", -1)

	} else if strings.HasSuffix(mboxfile, ".mbox.gz") {
		is_gzp = true
		is_mbx = true
		group = strings.Replace(mboxfile, ".mbox.gz", "", -1)

	} else if strings.HasSuffix(mboxfile, ".mbox.7z") {
		is_zip7 = true
		is_mbx = true
		group = strings.Replace(mboxfile, ".mbox.7z", "", -1)

	} else if strings.HasSuffix(mboxfile, ".mbox") {
		is_mbx = true
		group = strings.Replace(mboxfile, ".mbox", "", -1)

	} else if strings.HasSuffix(mboxfile, ".bag") {
		is_bag = true

	} else if uatsuffix != "" {
		group = strings.TrimSuffix(mboxfile, uatsuffix)

	} else if strings.HasSuffix(mboxfile, "/") {
		if !dirExists(mboxfile) {
			log.Printf("ERROR: dir not found '%s'", mboxfile)
			os.Exit(1)
		}
		is_dir = true
	}

	if !is_mbx && !is_gzp && !is_zip && !is_zip7 && !is_dir && !is_bag && uatsuffix == "" {
		log.Printf("file %s is not an mboxfile?", mboxfile)
		os.Exit(1)
	}

	// -dos2unix runs on every mbox (not on a directory). Look it up now,
	// before anything is unpacked, instead of failing after the unzip.
	// LookPath also finds dos2unix.exe on Windows.
	if !is_dir && DOS2UNIX != "" && !NODOS2UNIX {
		path, err := exec.LookPath(DOS2UNIX)
		if err != nil {
			log.Printf("ERROR: -dos2unix: %v", err)
			os.Exit(1)
		}
		DOS2UNIX = path
	} else if NODOS2UNIX && DOS2UNIX == "" {
		log.Printf("NOTE: -no-dos2unix and .nodos2unix are no longer needed: dos2unix only runs with -dos2unix")
	}

	log.Printf("process mboxfile='%s' group='%s'", mboxfile, group)
	//os.Exit(1)

	mboxfileZ := ""
	if is_zip || is_zip7 || is_gzp || is_mbx || is_bag || uatsuffix != "" {
		mboxfileZ = mboxfile
		my_mboxfile = "-" + filepath.Base(mboxfile)
		log.Printf("mboxfileZ = %s", mboxfileZ)
	}

	for i := 1; i <= MaxWorkers; i++ {
		go worker(i)
	}

	log.Printf("START: mboxfile = %s", mboxfile)

	if uatsuffix != "" {
		mboxfile = procUAT(mboxfile, uatsuffix)
	} else {
		mboxfile = proc(mboxfile, is_zip, is_zip7, is_gzp, is_mbx, is_dir, is_bag)
	}
	// an archive was unpacked into tmpdir: mboxfile is that copy
	unpacked := is_zip || is_zip7 || is_gzp || uatsuffix == ".usenet.xz" || uatsuffix == ".usenet.7z"
	if is_dir {
		globalBools.SetGB("feeder_done", true)
		for {
			workers := globalCounter.getCounter("worker_done")
			if workers != MaxWorkers {
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				break
			}
		}
	}
	took := UnixTimeSec() - started

	ttot := globalCounter.getCounter("total")
	tsentcnt := globalCounter.getCounter("sent")
	tnntpdup435 := globalCounter.getCounter("wnntpdup435")
	tnntp437 := globalCounter.getCounter("wnntperr437")
	tnntp999 := globalCounter.getCounter("tnntp999")
	tnntperr := globalCounter.getCounter("wnntperrcnt")

	log.Printf("Done sending. %d/%d dup=%d e437=%d err=%d filter=%d", tsentcnt, ttot, tnntpdup435, tnntp437, tnntperr, tnntp999)
	log.Printf("END: mboxfile='%s' took=%d remaining sent=%d", mboxfile, took, tsentcnt)

	if DRYRUN {
		// nothing was sent, so nothing is done: only remove what was unpacked
		if unpacked && fileExists(mboxfile) {
			log.Printf("cleanup: rm '%s'", mboxfile)
			rmmbox(mboxfile)
		}
		log.Printf("dry run: '%s' not marked done", mboxfileZ)
	} else if is_zip || is_zip7 || is_gzp || is_bag || uatsuffix != "" {
		if tnntperr == 0 {
			mvmbox(mboxfileZ)
			// proc returned the extracted file's path. Only an archive has
			// one: for a .bag it is the source itself, which the old
			// tmpdir+"/"+name check removed whenever the run was not done.
			if unpacked && fileExists(mboxfile) {
				log.Printf("cleanup: rm '%s'", mboxfile)
				rmmbox(mboxfile)
			}
		}
	} else if is_mbx {
		if !mboxhasZ(mboxfile) {
			if tnntperr == 0 {
				mvmbox(mboxfile)
			}
		}
	}

} // end func main

func UnixTimeSec() int64 {
	return time.Now().UnixNano() / 1e9
}

// func proc(mboxfile string, is_zip bool, is_gzp bool, is_mbx bool, srvtp *textproto.Conn) string {
func proc(mboxfile string, is_zip bool, is_zip7 bool, is_gzp bool, is_mbx bool, is_dir bool, is_bag bool) string {
	var reader *mbox.MboxStream
	var src *eofReader
	mboxfiletrim := ""

	if !is_zip && !is_zip7 && !is_gzp && !is_dir && (is_mbx || is_bag) {
		dos2unix(mboxfile)
		//time.Sleep(5 * time.Second)
		reader, src = openMboxFile(mboxfile)

	} else if is_zip {
		// mbox is zip
		mboxfiletrim = extractedPath(mboxfile, ".zip")
		unpackOrExit("unzip", unzipTo, mboxfile, mboxfiletrim)
		dos2unix(mboxfiletrim)
		if !fileExists(mboxfiletrim) {
			log.Printf("unzipped '%s' but not found '%s'", mboxfile, mboxfiletrim)
			os.Exit(1)
		}
		log.Printf("unzipped '%s' to '%s'", mboxfile, mboxfiletrim)
		reader, src = openMboxFile(mboxfiletrim)

	} else if is_gzp {
		// mbox is gzip
		mboxfiletrim = extractedPath(mboxfile, ".gz")
		unpackOrExit("gunzip", gunzipTo, mboxfile, mboxfiletrim)
		dos2unix(mboxfiletrim)
		if !fileExists(mboxfiletrim) {
			log.Printf("gunzipped '%s' but not found '%s'", mboxfile, mboxfiletrim)
			os.Exit(1)
		}
		log.Printf("gunzipped '%s' to '%s'", mboxfile, mboxfiletrim)
		reader, src = openMboxFile(mboxfiletrim)
	} else if is_zip7 {
		// mbox is 7z
		mboxfiletrim = extractedPath(mboxfile, ".7z")
		unpackOrExit("un7z", unpack.Un7zTo, mboxfile, mboxfiletrim)
		dos2unix(mboxfiletrim)
		if !fileExists(mboxfiletrim) {
			log.Printf("unzipped 7z '%s' but not found '%s'", mboxfile, mboxfiletrim)
			os.Exit(1)
		}
		log.Printf("unzipped 7z '%s' to '%s'", mboxfile, mboxfiletrim)
		reader, src = openMboxFile(mboxfiletrim)
	} else if is_dir {
		// was /bin/find.sh (find -type f | sort -V), whose output was
		// buffered whole and split on "\n" before any work started
		log.Printf("dir=%s", mboxfile)
		t, totf, badfiles := 0, 0, 0
		werr := walkFiles(mboxfile, func(fp string) {
			totf++
			if skipDirFile(fp) {
				badfiles++
				return
			}
			msgid := read_file(fp)
			if msgid == "" {
				badfiles++
			}
			if t >= 1000 {
				log.Printf("read %d dir='%s' queue=%d", totf, mboxfile, len(worker_channel))
				t = 0
			}
			t++
		})
		if werr != nil {
			log.Printf("ERROR dir walk '%s': %v", mboxfile, werr)
		}
		globalBools.SetGB("feeder_done", true)
		log.Printf("mbox2nntp done is_dir reading totalfiles=%d badfiles=%d", totf, badfiles)
		//close(worker_channel)
		return mboxfile
	} else {
		log.Printf("proc mboxfile failed unknown error")
		os.Exit(1)
	}

	//iterateMbox(reader, srvtp)

	if reader == nil {
		// empty mbox: done, so it is moved aside like any finished file
		globalBools.SetGB("feeder_done", true)
		globalBools.SetGB("done", true)
		if is_zip || is_zip7 || is_gzp {
			return mboxfiletrim
		}
		return mboxfile
	}

	// closed before main renames or removes it
	defer src.Close()

	// archives return the extracted file's name: main removes it after
	// moving the archive to .done
	if is_zip || is_zip7 || is_gzp {
		iterateMbox(reader, src, mboxfiletrim)
		return mboxfiletrim
	}
	iterateMbox(reader, src, mboxfile)
	return mboxfile
}

// skipDirFile reports whether the directory walk should skip fp: index
// and overview files, archives, sources and binaries, patches, and names
// with ")". Only the file's own name is tested. The old test ran on the
// whole path, so one parent directory with ")" or "/patch" in its name
// dropped its entire subtree (B5).
func skipDirFile(fp string) bool {
	name := strings.ToLower(filepath.Base(fp))
	if strings.Contains(name, ")") || strings.HasPrefix(name, "patch") || name == "index" {
		return true
	}
	for _, ext := range skipDirExts {
		if strings.HasSuffix(name, ext) {
			return true
		}
	}
	return false
}

var skipDirExts = []string{
	".#index", ".c", ".tbl", ".bat", ".com", ".exe", ".html", ".htm",
	".7z", ".zip", ".gz", ".xz", ".z", ".tgz", ".bak", ".brk",
	".overview", ".xover", ".msgids",
}

func dirExists(dir string) bool {
	info, err := os.Stat(dir)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// eofReader remembers whether its source has reported io.EOF. The mbox
// library also returns io.EOF when it finds no separator where the next
// message should start, so this is how iterateMbox tells the two apart.
type eofReader struct {
	r      io.Reader
	f      *os.File
	sawEOF bool
}

// Close closes the mbox file. It must be closed before the file is renamed
// or removed: Windows refuses both on an open file.
func (e *eofReader) Close() error {
	return e.f.Close()
}

func (e *eofReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err == io.EOF {
		e.sawEOF = true
	}
	return n, err
}

// unreadBytes drains src and returns how much of it the parser never asked
// for, once ReadMessage has returned io.EOF. A parser that stopped short
// with less than its own read buffer (4 KiB) left is not seen.
func unreadBytes(src *eofReader) int64 {
	if src.sawEOF {
		return 0
	}
	n, _ := io.Copy(io.Discard, src)
	return n
}

// openMboxFile returns nil, nil for an empty file: some archives hold a
// zero-byte mbox, which is nothing to import rather than a reason to stop.
func openMboxFile(mboxfile string) (*mbox.MboxStream, *eofReader) {
	fh, err := os.Open(mboxfile)
	if err != nil {
		log.Printf("Can't os.Open file %s", mboxfile)
		os.Exit(1)
	}
	if fi, err := fh.Stat(); err == nil && fi.Size() == 0 {
		log.Printf("openMboxFile: empty file '%s', nothing to import", mboxfile)
		fh.Close()
		return nil, nil
	}
	// was 1 GiB, allocated per file and filled on any mbox that large
	src := &eofReader{r: bufio.NewReaderSize(fh, 1024*1024), f: fh}
	mboxReader, err := mbox.CreateMboxStream(src)
	if err != nil {
		log.Printf("Can't create mbox reader: %#v", err)
		os.Exit(1)
	}
	return mboxReader, src
}

// readBody reads a message body into lines without their line endings.
// isbinary is set, and the body left out, when DROPBINARY is on and a line
// starts with "=ybegin".
func readBody(bodyReader io.Reader) (body []string, isbinary bool) {
	buffer := make([]byte, 64*1024)
	var part []byte // a line longer than buffer, read so far
	for {
		n, err := bodyReader.Read(buffer)
		if err != nil {
			if len(part) == 0 {
				return body, isbinary
			}
			n = 0 // the stream ended inside a line: keep what came
		} else if n == 0 {
			continue // io.Reader allows an empty read; it is not a line
		} else if buffer[n-1] != '\n' {
			// one Read returns at most len(buffer) bytes of a line; each
			// piece used to become a line of its own
			part = append(part, buffer[:n]...)
			continue
		}
		line := string(buffer[:n])
		if len(part) > 0 {
			line = string(part) + line
			part = part[:0]
		}
		if DROPBINARY && !isbinary && strings.HasPrefix(line, "=ybegin") {
			isbinary = true
		}
		// strip CR as well: ihave appends CRLF, so a surviving CR
		// goes out as "\r\r\n" on any mbox dos2unix did not touch.
		line = strings.TrimRight(line, "\r\n")
		if !isbinary {
			body = append(body, line)
		}
	}
}

// func iterateMbox(mboxReader *mbox.MboxStream, srvtp *textproto.Conn) {
func iterateMbox(mboxReader *mbox.MboxStream, src *eofReader, mboxfile string) {
	msgNumber := 0
	msgcnt := 0
	skipped := 0
	var lastReadErr, prevErr error
	stoppedEarly := false
	//msgidmap := make(map[string]int)
	for {
		//var message *Message
		//lines := make([]string, 0)

		message, err := mboxReader.ReadMessage()
		if err != nil {
			if err == io.EOF {
				log.Printf("[iterateMbox] EOF @ msgNumber=%d mboxfile=%s", msgNumber, mboxfile)
				break
			}
			// After a malformed message the library skips to the next
			// separator, so each error costs that one message. Parse
			// errors carry their line number: the same one twice in a
			// row means the reader is not advancing.
			if prevErr != nil && err.Error() == prevErr.Error() {
				log.Printf("[iterateMbox] ERROR: mboxreader not advancing file='%s' err='%v' msgNumber=%d", mboxfile, err, msgNumber)
				stoppedEarly = true
				break
			}
			skipped++
			lastReadErr, prevErr = err, err
			log.Printf("[iterateMbox] ERROR: mboxreader skipped a message file='%s' err='%v' msgNumber=%d", mboxfile, err, msgNumber)
			continue
		}
		prevErr = nil

		msgNumber++
		msgcnt++
		//
		if msgcnt >= 10000 {
			log.Printf("[iterateMbox] mbox read file='%s' msgnum=%d", mboxfile, msgNumber)
			msgcnt = 0
		}
		// Print our message summary, consisting of message number and subject.
		head := message.Headers()
		/*for _, line := range head {
			if strings.HasPrefix(line, "Message-ID: ") {

			}
		}*/
		msgid := ""
		headermap, keysorder, msgid, err := ParseHeaderKeys(head, LAXMID)

		if msgid == "" || err != nil {
			log.Printf("[iterateMbox] mbox %s msgid not found msgnum=%d \n map='%v' \n ko='%v' \n err='%v'", mboxfile, msgNumber, headermap, keysorder, err)
		}

		body, isbinary := readBody(message.BodyReader())

		if msgid != "" {
			err = nil
		}

		if msgid == "" || err != nil || isbinary {

			//log.Printf("[iterateMbox] E2) overview.GetMessageID msgNumber=%d msgid='%s' err='%v' isbinary=%t", msgNumber, msgid, err, isbinary)
			//time.Sleep(5 * time.Second)
			continue
		}
		body = body_remove_finalemptylines(body)
		if len(body) == 0 || len(head) == 0 {
			log.Printf("ERROR head=%d body=%d msgid='%s'", len(head), len(body), msgid)
			continue
		}

		//log.Printf("worker_channel <- msgid='%s' head=%d body=%d", msgid, len(head), len(body))

		// TODO clear msgidhashdb
		globalCounter.incCounter(0, "msgnums")
		worker_channel <- &ArticleItem{msgid: msgid, head: head, body: body}

	} // end for
	//globalCounter.setCounter("msgnums", msgNumber)
	if !stoppedEarly {
		if unread := unreadBytes(src); unread > 0 {
			log.Printf("[iterateMbox] ERROR: mbox parser stopped early file='%s' msgNumber=%d unread_bytes=%d, not marking done", mboxfile, msgNumber, unread)
			stoppedEarly = true
		}
	}
	nearMisses, nearMissSamples := mboxReader.NearMisses()
	log.Printf("[iterateMbox] Done read mbox file='%s' msgNumber=%d skipped=%d near-misses=%d stopped-early=%t last-err=%v", mboxfile, msgNumber, skipped, nearMisses, stoppedEarly, lastReadErr)
	// near-misses are body lines starting "From " or "#! rnews " that validid
	// rejected: either ordinary text, or a separator shape validid does not
	// know, in which case articles were merged. The samples tell which.
	for _, s := range nearMissSamples {
		log.Printf("[iterateMbox]   near-miss file='%s' line=%d %q", mboxfile, s.Line, s.Text)
	}

	//close(worker_channel)

	finishFeeding(stoppedEarly)
} // end func iterateMbox

// finishFeeding is called when the reader has queued its last article: it
// waits for the workers and decides whether the input is done. stoppedEarly
// means the reader did not get through the whole input.
func finishFeeding(stoppedEarly bool) {
	globalBools.SetGB("feeder_done", true)

	//log.Printf("Start sending.")
	for {
		workers := globalCounter.getCounter("worker_done")
		//msgnums := globalCounter.getCounter("msgnums")
		if workers != MaxWorkers {
			/*
				//if workers > 0 {
				if workers == MaxWorkers {
					log.Printf("Done Workers: %d", workers)
				}
			*/
			time.Sleep(100 * time.Millisecond)
			continue
		} else {
			break
		}
	}

	ttot := globalCounter.getCounter("total")
	tsentcnt := globalCounter.getCounter("sent")
	tnntpdup435 := globalCounter.getCounter("wnntpdup435")
	tnntp437 := globalCounter.getCounter("wnntperr437")
	tnntp999 := globalCounter.getCounter("tnntp999")
	terrcnt := globalCounter.getCounter("wnntperrcnt")

	log.Printf("Done sending iterateMbox. %d/%d dup=%d e437=%d err=%d filter=%d", tsentcnt, ttot, tnntpdup435, tnntp437, terrcnt, tnntp999)
	//if ttot > 0 && (tsentcnt == ttot || tsentcnt+tnntpdup435+tnntp999 == ttot || tsentcnt+tnntpdup435+tnntp437 == ttot ) && terrcnt == 0 {
	//if ttot > 0 && (tsentcnt == ttot || tsentcnt+tnntpdup435+tnntp999 == ttot) && terrcnt == 0 {
	// workers that gave up connecting leave articles queued but unsent
	if left := globalCounter.getCounter("msgnums"); left != 0 {
		log.Printf("ERROR: %d articles were never sent, not marking done", left)
	} else if terrcnt == 0 && !stoppedEarly {
		globalBools.SetGB("done", true)
	}
}

func worker(wid int) {
	//debug := false

	log.Printf("Launch Worker %d", wid)

	var srvtp *textproto.Conn
	var cliwriter *bufio.Writer
	var cerr error
	retries := 3600
loop_CB:
	for !DRYRUN { // a dry run sends nothing, so it needs no server
		srvtp, cliwriter, cerr = connectBackend(nntpserv)
		if cerr != nil {
			log.Printf("Worker %d) connect failed rserver=%s cerr='%v'", wid, nntpserv, cerr)
			if retries > 0 {
				time.Sleep(3 * time.Second)
				retries--
				continue loop_CB
			}
			globalCounter.incCounter(wid, "worker_done")
			return
		} else {
			log.Printf("Worker %d) connected rserver=%s", wid, nntpserv)
			break loop_CB
		}
	} // end for loop_CB

	code := 0
	wcnt, wtot := 0, 0
	werrcnt, wnntp999, wnntp437, wnntpdup435, wsentcnt := 0, 0, 0, 0, 0
	doneTicker := time.NewTicker(time.Second / 10)
	defer doneTicker.Stop()
loop_worker:
	for {
		select {
		case <-doneTicker.C:
			feeder_done := globalBools.GetGB("feeder_done")
			if feeder_done {
				msgnums := globalCounter.getCounter("msgnums")
				if msgnums == 0 {
					break loop_worker
				}
			}
			continue loop_worker

		case article := <-worker_channel:

			//if debug {
			//	log.Printf("Worker %d) got msgid='%s' --> IHAVE", wid, article.msgid)
			//}

			code = ihave(wid, article.msgid, article.head, article.body, srvtp, cliwriter)
			requeued := false
			switch code {
			case 235:
				wsentcnt++
			case 435:
				wnntpdup435++
			case 437:
				wnntp437++
			case 999:
				wnntp999++
			case 436, -999:
				// 436: the server wants it later. -999: the connection
				// failed. Either way it was not taken: requeue it with
				// backoff, a bounded number of times, rather than drop it.
				if article.tries < MAX_TRIES {
					article.tries++
					requeued = true
					log.Printf("Worker %d) code=%d requeue msgid='%s' try=%d/%d", wid, code, article.msgid, article.tries, MAX_TRIES)
					go func(article *ArticleItem, delay time.Duration) {
						time.Sleep(delay)
						worker_channel <- article
					}(article, time.Duration(article.tries)*retryDelay)
				} else {
					log.Printf("ERROR Worker %d) code=%d giving up msgid='%s' after %d tries", wid, code, article.msgid, article.tries)
					werrcnt++
				}
				if code == -999 {
					// the connection is unusable: close it and hand over
					// to a fresh worker, which reconnects
					closeBackend(srvtp, cliwriter)
					if !requeued {
						globalCounter.subCounter(wid, "msgnums", 1)
					}
					time.Sleep(retryDelay)
					go worker(wid)
					globalCounter.addCounter(wid, "sent", wsentcnt)
					globalCounter.addCounter(wid, "total", wtot)
					globalCounter.addCounter(wid, "wnntpdup435", wnntpdup435)
					globalCounter.addCounter(wid, "wnntperr437", wnntp437)
					globalCounter.addCounter(wid, "wnntperrcnt", werrcnt)
					globalCounter.addCounter(wid, "tnntp999", wnntp999)
					return
				}
				time.Sleep(retryDelay) // 436: server busy or throttled
			default:
				log.Printf("ihave returned werrcnt code=%d", code)
				werrcnt++
			} // end switch
			if !requeued {
				globalCounter.subCounter(wid, "msgnums", 1)
			}

			if wcnt >= 1000 {
				//globalCounter.subCounter(wid, "msgnums", 1000)
				msgnums := globalCounter.getCounter("msgnums")
				log.Printf("Worker %d) wtot=%d wq=%d dup=%d e437=%d err=%d sent=%d ttodo=%d", wid, wtot, len(worker_channel), wnntpdup435, wnntp437, werrcnt, wsentcnt, msgnums)
				wcnt = 0
			}
			//if debug {
			//	log.Printf("Worker %d) 'IHAVE '%s'' --> code=%d", wid, article.msgid, code)
			//}
			wcnt++
			wtot++
		} // end select worker_channel
	} // end for
	//globalCounter.subCounter(wid, "msgnums", wcnt)
	closeBackend(srvtp, cliwriter)
	globalCounter.incCounter(wid, "worker_done")
	log.Printf("Done Worker %d) totalWdone=%d maxW=%d wtot=%d sent=%d e435=%d e437=%d errcnt=%d filter=%d", wid, globalCounter.getCounter("worker_done"), MaxWorkers, wtot, wsentcnt, wnntpdup435, wnntp437, werrcnt, wnntp999)
	globalCounter.addCounter(wid, "sent", wsentcnt)
	globalCounter.addCounter(wid, "total", wtot)
	globalCounter.addCounter(wid, "wnntpdup435", wnntpdup435)
	globalCounter.addCounter(wid, "wnntperr437", wnntp437)
	globalCounter.addCounter(wid, "wnntperrcnt", werrcnt)
	globalCounter.addCounter(wid, "tnntp999", wnntp999)

	return
} // end func worker

func isspace(b byte) bool {
	return b < 33
}

func ihave(wid int, msgid string, head []string, bodylines []string, srvtp *textproto.Conn, CliWriter *bufio.Writer) int {
	/*
		if strings.HasPrefix(msgid, "<_txB3") { /,* @ab.jobs *,/
			log.Printf("--> IHAVE --> msgid='%s' head=%d body=%d", msgid, len(head), len(bodylines))
		}
	*/
	//time.Sleep(10 * time.Second)

	newhead, has_hdr_path, code := rewriteHead(msgid, head)
	if code != 0 {
		return code
	}

	if !DRYRUN {

		head_len := len(newhead)

		if head_len > 256 {
			print_lines(newhead)
			log.Printf("ERROR ^^^^ head_len=%d msgid='%s'", head_len, msgid)
			return 437
		}

		sendcmd, prehash := "", PREHASH_MODE
		if prehash {
			msgidhash := hash256(msgid)
			sendcmd = fmt.Sprintf("IHAVE %s %s", msgid, msgidhash)
		} else {
			sendcmd = fmt.Sprintf("IHAVE %s", msgid)
		}
		//id, err = srvtp.Cmd(sendcmd)
		//srvtp.StartResponse(id)

		/*
			if strings.HasPrefix(msgid, "<_txB3") {
				log.Printf("--> IHAVE --> msgid='%s' sendcmd='%s'", msgid, sendcmd)
			}
		*/

		_, err := io.WriteString(CliWriter, sendcmd+CRLF)
		if err != nil {
			log.Printf("ERROR Worker %d ihave failed sendcmd msgid='%s' err='%v'", wid, msgid, err)
			return -999
		}
		err = CliWriter.Flush()
		if err != nil {
			log.Printf("ERROR Worker %d ihave failed sendcmd Flush msgid='%s' err='%v'", wid, msgid, err)
			return -999
		}
		//if code, msg, err = srvtp.ReadCodeLine(335); err != nil {
		if code, _, err = srvtp.ReadCodeLine(335); err != nil {
			return responseCode(wid, msgid, code, err)
		}

		//srvtp.EndResponse(id)

		for _, line := range newhead {
			_, err := io.WriteString(CliWriter, line+CRLF)
			if err != nil {
				log.Printf("ERROR Worker %d ihave failed head msgid='%s' err='%v'", wid, msgid, err)
				return -999
			}
		}

		if !has_hdr_path {
			//now := fmt.Sprintf("%d", time.Now().UnixNano() / 1e9)
			sendline := fmt.Sprintf("Path: mbox2nntp%s!not-for-mail", my_mboxfile)
			_, err := io.WriteString(CliWriter, sendline+CRLF)
			if err != nil {
				log.Printf("ERROR Worker %d ihave failed !has_hdr_path msgid='%s' err='%v'", wid, msgid, err)
				return -999
			}
		}

		_, err = io.WriteString(CliWriter, CRLF)
		if err != nil {
			log.Printf("ERROR Worker %d ihave failed head/body separator msgid='%s' err='%v'", wid, msgid, err)
			return -999
		}

		for _, bline := range bodylines {
			// RFC 3977 3.1.1: ANY line starting with '.' gets one prepended.
			// The old code skipped lines starting with ".." so they arrived
			// one dot short.
			if strings.HasPrefix(bline, DOT) {
				bline = DOT + bline
			}

			/*
				badline := false
				if len(bline) >= 1 {

					switch(bline[len(bline)-1]) {
						case '\r':
							badline = true
							log.Printf("ERROR the-last char in bodyline cant be CR")

						case '\n':
							// DEBUG log.Printf("INFO 1st-last char in bodyline cant be LF... removing len_line=%d", len(bline))
							bline = bline[0:len(bline)-1]
							// DEBUG log.Printf("INFO line removed LF... len_line=%d", len(bline))

							if len(bline) >= 1 {
								if bline[len(bline)-1] == '\n' {
									log.Printf("ERROR 1LF removed but still last char in bodyline is a LF?! len_line=%d", len(bline))
									badline = true
								} else {
									if bline[len(bline)-1] == '\r' {
										log.Printf("ERROR 2nd-last char in bodyline cant be CR")
										badline = true
									}
								}
							}
					} // end switch
				}
				if badline {
					for i, line := range bodylines {
						fmt.Println(i, "=", line)
					}
					time.Sleep(15 * time.Second)
					os.Exit(1)
				}
			*/
			//bline = strings.Replace(bline, "\r", "", -1)
			//bline = strings.Replace(bline, "\n", "", -1)
			//bline = strings.Replace(bline, "\r\n", "", -1)
			// nul-char
			//bline = strings.Replace(bline, "\x00", "", -1)
			_, err = io.WriteString(CliWriter, bline+CRLF)
			if err != nil {
				log.Printf("ERROR Worker %d ihave failed body msgid='%s' err='%v'", wid, msgid, err)
				return -999
			}
		}

		_, err = io.WriteString(CliWriter, DOT+CRLF)
		if err != nil {
			log.Printf("ERROR Worker %d ihave failed final dot msgid='%s' err='%v'", wid, msgid, err)
			return -999
		}

		//CliWriter.Flush()
		//dw.Close()
		err = CliWriter.Flush()
		if err != nil {
			log.Printf("ERROR Worker %d ihave failed sendcmd Flush msgid='%s' err='%v'", wid, msgid, err)
			return -999
		}

		rcode, _, err := srvtp.ReadCodeLine(235)
		code = responseCode(wid, msgid, rcode, err)
	} else {
		code = 235
	}
	//os.Exit(0)
	//time.Sleep(100 * time.Millisecond) // DEBUG_SLEEP
	return code
} // end func ihave

// cleanHeadline replaces NUL, backspace, tab and newline in a header line
// with a space. It is overview.Clean_Headline without its final
// strings.Replace(line, "  ", " ", -1): a single pass, so four spaces became
// two, and it rewrote header content (Subjects, X-Trace, encoded words)
// that INN accepts as it is. Tabs still go, as they did.
func cleanHeadline(line string) string {
	if strings.ContainsAny(line, "\x00\010\t\n") {
		line = strings.NewReplacer("\x00", " ", "\010", " ", "\t", " ", "\n", " ").Replace(line)
	}
	return line
}

// isBadDateLine reports whether line starts like a date ("Thu, "), which
// some Google-made headers carry as a line of its own. It was the regex
// ^[A-Z][a-z][a-z],\  on a backtracking engine.
func isBadDateLine(line string) bool {
	return len(line) >= 5 &&
		line[0] >= 'A' && line[0] <= 'Z' &&
		line[1] >= 'a' && line[1] <= 'z' &&
		line[2] >= 'a' && line[2] <= 'z' &&
		line[3] == ',' && line[4] == ' '
}

// rewriteHead turns a parsed article's header lines into the ones IHAVE
// sends: Path gets our prefix, Xref/Expires/"From " lines and their
// continuations are dropped, Message-ID is reduced to the one being fed,
// colon-space is repaired. code is 0, or 437 when Subject or Date is missing.
func rewriteHead(msgid string, head []string) (newhead []string, has_hdr_path bool, code int) {
	has_hdr_date := false

	ignore_nextline := false
	has_subj := false
	has_msgid := false

	var messageids []string

	for i, line := range head {
		line = cleanHeadline(line)
		spaced_line := false
		if len(line) > 0 {
			spaced_line = isspace(line[0])
		} else {
			continue
		}
		spaced_nextline := false
		spaced_prevline := false
		nextline := i + 1
		prevline := i - 1
		//log.Printf("msgid='%s' line=%d", msgid, i)
		// get the next line if any
		if nextline < len(head) {
			if head[nextline] != "" {
				//str_nextline = head[nextline] // next line as string
				spaced_nextline = isspace(head[nextline][0]) // first char of next line
			}
		}

		// get previous line
		if prevline >= 0 && len(head[prevline]) > 0 {
			spaced_prevline = isspace(head[prevline][0])
		}

		// header starting with line "Thu, ... datestring" some google-headers are that broken, we ignore the line
		if isBadDateLine(line) {
			if spaced_prevline && !spaced_nextline {
				log.Printf("isBadDateLine: ignore headline=%d msgid='%s'", i, msgid)
				continue
			} else if spaced_prevline && spaced_nextline {
				// add a space
				log.Printf("isBadDateLine: headline=%d add space msgid='%s'", i, msgid)
				line = " " + line
			}
		}

		if ignore_nextline {
			if spaced_nextline {
				ignore_nextline = true
			} else {
				ignore_nextline = false
			}
			continue
		}

		// start doing checks
		if line[0] == 'F' && strings.HasPrefix(line, "From ") {
			if spaced_nextline {
				ignore_nextline = true
			}
			continue
		}

		if !spaced_line && strings.Index(line, ":") > 0 {
			// colon present
			header_key := strings.Split(line, ":")[0]

			// remove "key:" from line
			header_dat := strings.Replace(line, header_key+":", "", 1)

			if strings.TrimSpace(header_dat) == "" && !spaced_nextline {
				// empty value; this was len(header_dat) <= 1, which also
				// dropped one-character values written without a space
				// ("Lines:7")
				//if debug_header { log.Printf("   |short header_dat msgid='%s' ignore line='%s' nextline='%s'", s.messageid, line, str_nextline) }
				continue
			}

			if isspace(header_key[len(header_key)-1]) || !strings.HasPrefix(header_dat, " ") {
				// missing colon-space "key: dat"
				newline := strings.TrimSpace(header_key) + ": " + strings.TrimSpace(header_dat)
				//if debug_header { log.Printf("[%s/%s/%s/%d] ihave: [%s] | fixed colon-space msgid='%s' oldline='%s' newline='%s' nextline='%s'", s.sessionid, s.username, s.cmdstring, s.beconn.backendID, s.fakegroup, s.messageid, line, newline, str_nextline) }
				line = newline
			}

			header_key = strings.TrimSpace(header_key)
			header_dat = strings.TrimSpace(header_dat)
			header_key_L := strings.ToLower(header_key)

			if !has_subj && header_key_L == "subject" {
				// the text may be all on the continuation line
				if len(header_dat) > 0 || spaced_nextline {
					has_subj = true
				}
			}

			if header_key_L == "path" {
				has_hdr_path = true
				line = string(header_key + ": mbox2nntp" + my_mboxfile + "!" + header_dat)
			} else
			// check headers
			if header_key_L == "message-id" {
				if has_msgid {
					messageids = append(messageids, header_dat)
					continue
				}
				if header_dat == msgid {
					messageids = append(messageids, header_dat)
					has_msgid = true
					continue
				} else {
					continue
				}

			} else if header_key_L == "date" {
				has_hdr_date = true
			} else
			// ignore 'Expires' header
			if header_key_L == "expires" {
				continue
			} else
			// ignore 'Xref' header
			if header_key_L == "xref" {
				if spaced_nextline {
					ignore_nextline = true
				}
				continue
			}

			// end endless if else header checks

		} else {

			if !spaced_line {
				// no colon in header line
				//if debug_header { log.Printf("[%s/%s/%s/%d] ihave: [%s] | NO colon in header msgid='%s' line='%s' spaced_line=%t x0x='%x' x0s='%x'", s.sessionid, s.username, s.cmdstring, s.beconn.backendID, s.fakegroup, s.messageid, line, spaced_line, line[0], line[0]) }
				continue
			}
		}

		newhead = append(newhead, line)
	} // end for header, hline := range head {

	if len(messageids) >= 1 {
		if len(messageids) > 1 {
			found := false
			for _, amsgid := range messageids {
				if amsgid == msgid {
					//log.Printf("Found msgid='%s' in messageids", msgid)
					newhead = append(newhead, "Message-ID: "+msgid)
					has_msgid = true
					found = true
					break
				}
			}
			if !found {
				log.Printf("ERROR msgid='%s' got %d messageids=['%v']", msgid, len(messageids), messageids)
			}
		} else if messageids[0] == msgid {
			newhead = append(newhead, "Message-ID: "+msgid)
			has_msgid = true
		} else {
			log.Printf("ERROR msgid='%s' no message-id header?!", msgid)
		}

		//time.Sleep(5 * time.Second)
	}

	if !has_subj {
		log.Printf("Error msgid='%s' !has_subj", msgid)
		//head = append(head, "Subject: -")
		return nil, false, 437
	}

	if !has_msgid {
		newhead = append(newhead, "Message-ID: "+msgid)
	}

	// No Date: is a 437 (B4). A repair from NNTP-Posting-Date or the
	// separator was planned, never wired up, and would have nothing to
	// do: no article in the 5.2M-article corpus lacks Date:.
	if !has_hdr_date {
		log.Printf("Error msgid='%s' !has_hdr_date", msgid)
		return nil, false, 437
	}
	return newhead, has_hdr_path, 0
} // end func rewriteHead

// responseCode turns a ReadCodeLine result into ihave's return code. A
// *textproto.Error is the server answering with another code (435, 436,
// 437, ...): return that code. Anything else is the connection failing
// (EOF, reset, timeout, a garbled line). That used to come back as code 0,
// which cost the article and kept the dead connection: return -999 so the
// article is requeued on a new one.
func responseCode(wid int, msgid string, code int, err error) int {
	if err == nil {
		return code
	}
	var perr *textproto.Error
	if errors.As(err, &perr) {
		return perr.Code
	}
	log.Printf("ERROR Worker %d ihave read failed msgid='%s' err='%v'", wid, msgid, err)
	return -999
}

func hash256(astr string) string {
	ahash := sha256.Sum256([]byte(astr))
	return hex.EncodeToString(ahash[:])
} // end func hash256

func print_lines(lines []string) {
	for i, line := range lines {
		log.Printf("l%d) %s", i, line)
	}
}
