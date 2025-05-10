package main
import (
	//"config"
	"crypto/sha256"
	//"encoding/binary"
	"encoding/hex"
	"bufio"
	"bytes"
	"fmt"
	"path/filepath"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/textproto"
	"os"
	"os/exec"
	"strings"
	//"regexp"
	"strconv"
	"sync"
	"time"
	//"runtime"
	"unicode"
	//"math/rand"
	//"github.com/sam-falvo/mbox-giganews-archive.org-mod"
	"github.com/dlclark/regexp2"
	//"github.com/edsrzf/mmap-go"
	"github.com/go-while/go-utils"
	"github.com/go-while/go-lib-mbox2nntp"
	"github.com/go-while/nntp-overview"
	"tsm/GlobalBools" // "github.com/go-while/go-GlobalBools"
	//"nntp/data/overview"
)

const (
	DEBUG bool = false
	WORKER_CHAN_SIZE int = 1000000 // queue this many articles
	CRLF string = "\r\n"
	DOT string = "."
)


type ArticleItem struct {
	msgid       string
	head        []string
	body        []string
} // end ArticleItem struct

var (
	DROPBINARY = true
	LAXMID bool = true
	my_mboxfile string = ""
	PREHASH_MODE bool = true
	//MaxCPUs = 2
	MaxWorkers = 1
	tmpdir = "."
	nntpserv = ""
	addedgroups = make(map[string]bool)
	nntpdup435 = 0
	nntp437 = 0
	sentcnt, errcnt = 0, 0
	DRYRUN                  bool
	PREFIX                  string
	globalBools             tsm_GlobalBools.GlobalBools
	globalCounter           GlobalCounter
	worker_channel          chan *ArticleItem
	requeue_channel         chan *ArticleItem
	notify_chan             chan struct{}
	regex_isAlphaNumStart   = regexp2.MustCompile(`^[A-Za-z0-9]`, 0)
	regex_isbaddateline   = regexp2.MustCompile(`^[A-Z][a-z][a-z],\ `, 0)
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

func connectBackend(rserver string) (*textproto.Conn, *bufio.Writer, error) {
	//conn, err := net.DialTimeout("tcp4", rserver, 5 * time.Second)
	//conn, err := net.DialTimeout("tcp6", rserver, 5 * time.Second)
	conn, err := net.DialTimeout("tcp", rserver, 9 * time.Second)

	if err != nil {
		log.Printf("error net.Dial err=%v", err)
		return nil, nil, err
	}

	srvtp := textproto.NewConn(conn)
	CliWriter := bufio.NewWriter(conn)
	code, msg, err := srvtp.ReadCodeLine(20)

	if code != 200 && code != 201 {
		log.Printf("connectBackend code=%d msg='%s' err='%v'", code, msg, err)
		conn.Close()
		return nil, nil, err
	}

	return srvtp, CliWriter, err
}


func convert(mboxfile string) {
	if fileExists(".nodos2unix") {
		return
	}
	cmd := exec.Command("/usr/bin/convmv", "--notest","-f ISO-8859-1", "-t UTF-8", mboxfile)
	err := cmd.Run()
	if err != nil {
		log.Printf("convert failed mboxfile='%s' err='%v'", mboxfile, err)
		os.Exit(1)
	}
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func unrar(mboxfile string) {
	var err error
	var cmd *exec.Cmd
	if tmpdir == "." {
		cmd = exec.Command("/usr/bin/unrar6", "x", mboxfile)
	}else{
		log.Printf("unrar %s to %s", mboxfile, tmpdir)
		//ctmpdir := fmt.Sprintf("-d %s", tmpdir)
		cmd = exec.Command("/usr/bin/unrar6", "x", mboxfile, tmpdir)
	}

	err = cmd.Run()
	if err != nil {
		log.Printf("unrar failed mboxfile='%s' err='%v' =exit(1)", mboxfile, err)
		os.Exit(1)
	}
} // end func unrar

func unzip(mboxfile string) {
	var err error
	var cmd *exec.Cmd
	if tmpdir == "." {
		cmd = exec.Command("/usr/bin/unzip", "-n", mboxfile)
	}else{

		log.Printf("unzip %s to %s", mboxfile, tmpdir)
		//ctmpdir := fmt.Sprintf("-d %s", tmpdir)
		cmd = exec.Command("/usr/bin/unzip", "-n", "-d", tmpdir, mboxfile)

		/*
		if strings.HasSuffix(tmpdir, "/") {
			mboxfile = tmpdir + mboxfile
		} else {
			mboxfile = tmpdir + "/" + mboxfile
		}
		*/
	}

	err = cmd.Run()
	if err != nil {
		log.Printf("unzip failed mboxfile='%s' err='%v' =exit(1)", mboxfile, err)
		os.Exit(1)
	}
} // end func unzip

func unzip7(mboxfile string) {
	var err error
	var cmd *exec.Cmd

	if tmpdir == "." {
		cmd = exec.Command("/usr/bin/7z", "x", mboxfile, "-y")
	} else {

		log.Printf("unzip7 %s to %s", mboxfile, tmpdir)
		tmpstr := "-o"+tmpdir
		cmd = exec.Command("/usr/bin/7z", "x", tmpstr, mboxfile, "-y")
	}

	err = cmd.Run()
	if err != nil {
		log.Printf("unzip7 failed mboxfile='%s' err='%v' =exit(1)", mboxfile, err)
		os.Exit(1)
	}
} // end func unzip

func gunzip(mboxfile string) {
	var err error
	var cmd *exec.Cmd
	var out bytes.Buffer

	if tmpdir == "." {
		cmd = exec.Command("/bin/gzip", "--force", "--decompress", "--keep", mboxfile)
	} else {
		tmboxfile := strings.Replace(mboxfile, ".gz", "", -1)
		tpath := tmpdir + "/" + tmboxfile
		//args := []string{"-v", mboxfile,">",tpath}
		log.Printf("gunzip %s to %s/%s tpath=%s", mboxfile, tmpdir, tmboxfile, tpath)
		//cmd = exec.Command("/bin/gzip", "--decompress", "--keep", "--stdout", mboxfile, ">", tpath)

		cmd = exec.Command("/scripts/gunzip.sh", mboxfile, tpath)
		cmd.Stdout = &out
	}
	err = cmd.Run()
	if err != nil {
		log.Printf("gunzip failed mboxfile='%s' err='%v' stdout='%s'", mboxfile, err, out.String())
		os.Exit(1)
	}/* else {
		log.Printf("gunziped mboxfile='%s' stdout='%s'", mboxfile, err, out.String())
	}*/
} // end func unzip

func rmmbox(mboxfile string) {
	cmd := exec.Command("/bin/rm", mboxfile)
	err := cmd.Run()
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
			mvmbox(zip)
		}

		//rmmbox(mboxfile)
		return true
	}

	return false
} // end func mboxhasZ

func dos2unix(mboxfile string) {
	if fileExists(".nodos2unix") {
		return
	}
	log.Printf("proc dos2unix %s", mboxfile)
	cmd := exec.Command("/usr/bin/dos2unix", "-f", mboxfile)
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
	mmboxfile := fmt.Sprintf("%s.done",mboxfile)
	cmd := exec.Command("/bin/mv", mboxfile, mmboxfile)
	err := cmd.Run()
	if err != nil {
		log.Printf("mvmbox failed mboxfile='%s' err='%v'", mboxfile, err)
		os.Exit(1)
	} else {
		log.Printf("OK mvmbox: '%s'", mmboxfile)
	}
} // end func mvmbox

func inn2AddGroup(group string) {
	if addedgroups[group] {
		log.Printf("inn2AddGroup added before group'%s'", group)
		return
	}
	cmd := exec.Command("/usr/sbin/ctlinnd", "newgroup", group)
	_ = cmd.Run()
	addedgroups[group] = true
	/*
	if err != nil {
		log.Printf("inn2AddGroup failed group='%s' err='%v'", group, err)
		os.Exit(1)
	}
	*/
} // end func inn2AddGroup

func read_file(file_path string) string {
	/*
	if !test_first_byte(file_path, "what>32") {
		return ""
	}
	*/

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

	if lines[len(lines)-1] != '.' {
		lines = append(lines, '\r')
		lines = append(lines, '\n')
		lines = append(lines, '.')
		//log.Printf(" -> append '.' = %d bytes", len(lines))
	}



	//var headb []byte
	var header []string
	var body []string

	r1 := textproto.NewReader(bufio.NewReader(bytes.NewReader(lines)))
	msglines, err1 := r1.ReadDotLines()
	r1 = nil
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
		}
		if read_head {

			line = overview.Clean_Headline("?", line, false)
			if !has_hdr_msgid && line == "Message-ID:" {
				if strings.HasPrefix(msglines[i+1], " <") {
					line = "Message-ID: " + msglines[i+1]
					continue_nextline = true
				}
				has_hdr_msgid = true

			}
			if !has_hdr_subj && strings.HasPrefix(line,"Subject: ") && len(line) > 9 {
				has_hdr_subj = true
			}
			if !has_hdr_subj && line == "Subject: " {
				line = "Subject: -"
				has_hdr_subj = true
			}
			if !has_hdr_from && strings.HasPrefix(line,"From: ") && len(line) > 6 {
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
			if strings.HasPrefix(line, "=ybegin") {
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
	_, _, msgid, err := overview.ParseHeaderKeys(header, LAXMID)
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
	queued := len(worker_channel)
	markhi := WORKER_CHAN_SIZE/10
	if queued > markhi {
		globalBools.SetGB("process", true)
	}
	worker_channel <- &ArticleItem{ msgid, header, body }
	return msgid
} // end read_file

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
	requeue_channel = make(chan *ArticleItem, WORKER_CHAN_SIZE)
	notify_chan = make(chan struct{}, 1)
	globalCounter.initCounter("worker_done")

	mboxfile := ""
	//nntpserv := ""
	if len(os.Args) >= 3 && os.Args[1] != "" && os.Args[2] != "" {

		if strings.HasSuffix(os.Args[1], "/") && dirExists(os.Args[1]) {
			mboxfile = os.Args[1]
		} else
		if fileExists(os.Args[1]) {
			mboxfile = os.Args[1]
		}
		nntpserv = os.Args[2]

		if len(os.Args) >= 4 {
			MaxWorkers, err = strconv.Atoi(os.Args[3])
			if err != nil {
				log.Printf("ERROR: get MaxWorkers failed", err)
				os.Exit(1)
			}
		}

		if len(os.Args) >= 5 {
			atmpdir := os.Args[4]
			if atmpdir != "." {
				tmpdir = atmpdir
			}
		}
		log.Printf("tmpdir=%s", tmpdir)

		if len(os.Args) >= 6 {
			if os.Args[5] == "DRYRUN" {
				DRYRUN = true
			}

		}

		log.Printf("DRYRUN=%t", DRYRUN)

		if len(os.Args) == 7 {
			if os.Args[6] != "" {
				PREFIX = os.Args[6]
			}
		}

		if len(os.Args) >= 8 {
			if os.Args[7] != "" { // /path/to/.noDROPBINARY
				if utils.FileExists(os.Args[7]) {
					log.Printf(".noDROPBINARY = '%s'", os.Args[7])
					DROPBINARY = false
				}
			}
		}

	}

	if mboxfile == "" || nntpserv == "" || MaxWorkers <= 0 {
		log.Printf("Usage: %s file.mbox[.gz|.zip] nntp.server:119 WORKERS TMPDIR DRYRUN", os.Args[0])
		log.Printf("args: %v", os.Args)
		os.Exit(1)
	}

	log.Printf("MaxWorkers=%d",MaxWorkers)
	log.Printf("TMPDIR=%s",tmpdir)

	is_zip7 := false
	is_zip := false
	is_gzp := false
	is_mbx := false
	is_dir := false
	is_bag := false
	group := ""

	if strings.HasSuffix(mboxfile, ".mbox.zip") {
		is_zip = true
		is_mbx = true
		group = strings.Replace(mboxfile, ".mbox.zip", "", -1)

	} else
	if strings.HasSuffix(mboxfile, ".mbox.gz") {
		is_gzp = true
		is_mbx = true
		group = strings.Replace(mboxfile, ".mbox.gz", "", -1)

	} else
	if strings.HasSuffix(mboxfile, ".mbox.7z") {
		is_zip7 = true
		is_mbx = true
		group = strings.Replace(mboxfile, ".mbox.7z", "", -1)

	} else
	if strings.HasSuffix(mboxfile, ".mbox") {
		is_mbx = true
		group = strings.Replace(mboxfile, ".mbox", "", -1)

	} else
	if strings.HasSuffix(mboxfile, ".bag") {
		is_bag = true

	} else
	if strings.HasSuffix(mboxfile, "/") {
		if !dirExists(mboxfile) {
			log.Printf("ERROR: dir not found '%s'", mboxfile)
			os.Exit(1)
		}
		is_dir = true
	}

	if !is_mbx && !is_gzp && !is_zip && !is_zip7 && !is_dir && !is_bag {
		log.Printf("file %s is not an mboxfile?", mboxfile)
		os.Exit(1)
	}

	//inn2AddGroup(group)

	log.Printf("process mboxfile='%s' group='%s'", mboxfile, group)
	//os.Exit(1)

	mboxfileZ := ""
	if is_zip || is_zip7 || is_gzp || is_mbx || is_bag {
		mboxfileZ = mboxfile
		my_mboxfile = "-"+filepath.Base(mboxfile)
		log.Printf("mboxfileZ = %s", mboxfileZ)
	}

	for i := 1; i <= MaxWorkers; i++ {
		go worker(i)
	}

	log.Printf("START: mboxfile = %s", mboxfile)

	mboxfile = proc(mboxfile, is_zip, is_zip7, is_gzp, is_mbx, is_dir, is_bag)
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

	if is_zip || is_zip7 || is_gzp || is_bag {
		if tnntperr == 0 {
			mvmbox(mboxfileZ)
			if utils.FileExists(tmpdir+"/"+mboxfile) {
				log.Printf("cleanup: rm '"+tmpdir+"/"+mboxfile+"'")
				rmmbox(tmpdir+"/"+mboxfile)
			}
		}
	} else
	if is_mbx {
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

func make_mboxfilefiletrim(mboxfiletrim string) string {
	if tmpdir == "." {
		return mboxfiletrim
	} else {
		if strings.HasSuffix(tmpdir, "/") {
			return tmpdir + mboxfiletrim
		} else {
			return tmpdir + "/" + mboxfiletrim
		}
	}
} // end make_mboxfilefiletrim


func test_first_byte(file_path string, what string) bool {
	//return true
	//log.Printf("test_first_byte fp='%s'", file_path)
	//defer log.Printf("test_first_byte fp='%s' returned what='%s'", file_path, what)
	// what = "From: "
	// what = "what>32"
	if file_handle, mmap_handle, err := utils.MMAP_FILE(file_path, "ro"); err == nil {
		//log.Printf("TEST go")
		/*
		len_mmap := len(mmap_handle)
		if len_mmap <= 6 { // "From: "
			log.Printf("ERROR empty file? fp='%s' len=%d", file_path, len_mmap)
			overview.MMAP_CLOSE(file_path, file_handle, mmap_handle, "ro")
			return false
		}
		*/
		switch(what) {
			case "F":
				//log.Printf("TEST is? 'F'")
				if mmap_handle[0] == 'F' { // maybe is a msgid-file?
					utils.MMAP_CLOSE(file_path, file_handle, mmap_handle, "ro")
					return true
					/*
					astr := string(mmap_handle[0:5])
					if astr == "From " {
						//log.Printf("OK test_first_byte fp='%s' 'F'", file_path)
						overview.MMAP_CLOSE(file_path, file_handle, mmap_handle, "ro")
						return true
					}*/
				}
			case "what>32":
				//log.Printf("TEST is? 'A-Z'")
				if mmap_handle[0] >= 'A' && mmap_handle[0] <= 'Z' { // is ascii?
					//log.Printf("OK test_first_byte fp='%s' 'A-Z'", file_path)
					utils.MMAP_CLOSE(file_path, file_handle, mmap_handle, "ro")
					return true
				}
		}
		log.Printf("ERROR file[0] ? fp='%s' f[0]='%s'=%d", file_path, string(mmap_handle[0]), mmap_handle[0])
		utils.MMAP_CLOSE(file_path, file_handle, mmap_handle, "ro")
	} else {
		log.Printf("ERROR test_first_byte fp='%s' err='%v'", file_path, err)
	}

	//MMAP_CLOSE(file_path, file_handle, mmap_handle, "ro")
	//time.Sleep(1 * time.Second)

	return false
} // end func test_first_byte

//func proc(mboxfile string, is_zip bool, is_gzp bool, is_mbx bool, srvtp *textproto.Conn) string {
func proc(mboxfile string, is_zip bool, is_zip7 bool, is_gzp bool, is_mbx bool, is_dir bool, is_bag bool) string {
	var reader *mbox.MboxStream
	mboxfiletrim := ""

	if (!is_zip && !is_zip7 && !is_gzp && !is_dir && (is_mbx || is_bag)) {
		dos2unix(mboxfile)
		convert(mboxfile)
		//time.Sleep(5 * time.Second)
		reader = openMboxFile(mboxfile)

	} else
	if (is_zip) {
		// mbox is zip
		unzip(mboxfile)
		mboxfiletrim = strings.TrimSuffix(mboxfile, ".zip")
		mboxfiletrim = make_mboxfilefiletrim(mboxfiletrim)
		dos2unix(mboxfiletrim)
		convert(mboxfiletrim)
		if !fileExists(mboxfiletrim) {
			log.Printf("unzipped '%s' but not found '%s'", mboxfile, mboxfiletrim)
			os.Exit(1)
		}
		log.Printf("unzipped '%s' to '%s'", mboxfile, mboxfiletrim)
		reader = openMboxFile(mboxfiletrim)

	} else
	if (is_gzp) {
		// mbox is gzip
		gunzip(mboxfile)
		mboxfiletrim = strings.TrimSuffix(mboxfile, ".gz")
		mboxfiletrim = make_mboxfilefiletrim(mboxfiletrim)
		dos2unix(mboxfiletrim)
		convert(mboxfiletrim)
		if !fileExists(mboxfiletrim) {
			log.Printf("gunzipped '%s' but not found '%s'", mboxfile, mboxfiletrim)
			os.Exit(1)
		}
		log.Printf("gunzipped '%s' to '%s'", mboxfile, mboxfiletrim)
		reader = openMboxFile(mboxfiletrim)
	} else
	if (is_zip7) {
		// mbox is 7z
		unzip7(mboxfile)
		mboxfiletrim = strings.TrimSuffix(mboxfile, ".7z")
		mboxfiletrim = make_mboxfilefiletrim(mboxfiletrim)
		dos2unix(mboxfiletrim)
		convert(mboxfiletrim)
		if !fileExists(mboxfiletrim) {
			log.Printf("unzipped 7z '%s' but not found '%s'", mboxfile, mboxfiletrim)
			os.Exit(1)
		}
		log.Printf("unzipped 7z '%s' to '%s'", mboxfile, mboxfiletrim)
		reader = openMboxFile(mboxfiletrim)
	} else
	if (is_dir) {
		c := exec.Command("/bin/find.sh", mboxfile)
		output, err := c.Output()
		if err != nil {
			log.Fatalf("err=%v", err)
		}
		files := strings.Split(string(output), "\n")
		totf := len(files)
		log.Printf("dir=%s files=%d", mboxfile, totf)
		//time.Sleep(1 * time.Second)
		t, badfiles := 0, 0
		globalCounter.setCounter("msgnums", totf)
		//globalBools.SetGB("process", true)
		for i, fp := range files {
			badfile := false
			fpl := strings.ToLower(fp)
			//log.Printf("@file %d / %d) fp='%s'", i+1, totf, fp)
			if strings.Contains(fpl, ")") { badfile = true }
			if strings.Contains(fpl, "/patch") { badfile = true }
			if strings.HasSuffix(fpl, "/index") { badfile = true }
			if strings.HasSuffix(fpl, ".#index") { badfile = true }
			if strings.HasSuffix(fpl, ".c") { badfile = true }
			if strings.HasSuffix(fpl, ".tbl") { badfile = true }
			if strings.HasSuffix(fpl, ".bat") { badfile = true }
			if strings.HasSuffix(fpl, ".com") { badfile = true }
			if strings.HasSuffix(fpl, ".exe") { badfile = true }
			if strings.HasSuffix(fpl, ".html") { badfile = true }
			if strings.HasSuffix(fpl, ".htm") { badfile = true }
			if strings.HasSuffix(fpl, ".7z") { badfile = true }
			if strings.HasSuffix(fpl, ".zip") { badfile = true }
			if strings.HasSuffix(fpl, ".gz") { badfile = true }
			if strings.HasSuffix(fpl, ".xz") { badfile = true }
			if strings.HasSuffix(fpl, ".z") { badfile = true }
			if strings.HasSuffix(fpl, ".tgz") { badfile = true }
			if strings.HasSuffix(fpl, ".bak") { badfile = true }
			if strings.HasSuffix(fpl, ".brk") { badfile = true }
			if strings.HasSuffix(fpl, ".overview") { badfile = true }
			if strings.HasSuffix(fpl, ".xover") { badfile = true }
			if strings.HasSuffix(fpl, ".msgids") { badfile = true }
			if fp == "" { badfile = true }
			if badfile {
				badfiles++
				continue
			}
			msgid := read_file(fp)
			if msgid == "" {
				badfiles++
			}
			if t >= 1000 {
				log.Printf("read %d/%d dir='%s' queue=%d", i+1, totf, mboxfile, len(worker_channel))
				t = 0
			}
			t++
			//time.Sleep(1 * time.Second)
		}
		globalCounter.subCounter(0, "msgnums", badfiles)
		globalBools.SetGB("process", true)
		globalBools.SetGB("feeder_done", true)
		log.Printf("mbox2nntp done is_dir reading totalfiles=%d badfiles=%d", totf, badfiles)
		//close(worker_channel)
		return mboxfile
	} else {
		log.Printf("proc mboxfile failed unknown error")
		os.Exit(1)
	}

	//iterateMbox(reader, srvtp)

	if (is_zip || is_gzp){
		iterateMbox(reader,mboxfiletrim)
		return mboxfiletrim
	}
	iterateMbox(reader,mboxfile)
	return mboxfile
}

func dirExists(dir string) bool {
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

func openMboxFile(mboxfile string) (*mbox.MboxStream) {
	fh, err := os.Open(mboxfile)
	if err != nil {
		log.Printf("Can't os.Open file %s", mboxfile)
		os.Exit(1)
	}
	reader := bufio.NewReaderSize(fh, 1024*1024*1024)
	mboxReader, err := mbox.CreateMboxStream(reader)
	if err != nil {
		log.Printf("Can't create mbox reader: %#v", err)
		os.Exit(1)
	}
	return mboxReader
}

func oldGetMessageID(imsgid string) (string, error) {
	//log.Printf("DEBUG F) overview.GetMessageID: amsgidstr='%s'",amsgidstr)
	//amsgidstr := headstring["Message-ID"]
	//msgid := ""
	lastInd := -1

	msgid := strings.TrimSpace(imsgid)

	if strings.Contains(msgid," ") {
		if strings.HasPrefix(msgid, "<") && strings.HasSuffix(msgid, ">") {
			//msgid = strings.Replace(msgid, " ", "", -1)
			return msgid, nil
		}
		/*
		log.Printf("DEBUG msgid contains space amsgidstr='%s'", msgid)
		lastInd = strings.LastIndex(msgid, " ")
		if lastInd != -1 {
			msgid = string(msgid[:lastInd])
		}
		*/
	}

	if strings.HasSuffix(msgid,">#1/1") {
		lastInd = strings.LastIndex(msgid, "#")

		if lastInd != -1 {
			msgid = string(msgid[:lastInd])
		}

	} else

	if strings.HasPrefix(msgid, "<") && (strings.Contains(msgid,">?(")||strings.Contains(msgid,"> (")||strings.Contains(msgid,">(UMass-")||strings.Contains(msgid,">-(UMass-")) && strings.HasSuffix(msgid, ")") {
		split := strings.Split(msgid, ">")
		msgid = split[0] + ">"
	}

	if strings.HasPrefix(msgid, "<") && strings.HasSuffix(msgid, ">") {
		//log.Printf("DEBUG F) return overview.GetMessageID msgid='%s'", msgid)
		return msgid, nil
	}
	return "", fmt.Errorf("ERROR: overview.GetMessageID failed imsgid='%s'", imsgid)
} // func oldGetMessageID

//func iterateMbox(mboxReader *mbox.MboxStream, srvtp *textproto.Conn) {
func iterateMbox(mboxReader *mbox.MboxStream, mboxfile string) {
	var err error
	msgNumber := 0
	msgcnt := 0
	//msgidmap := make(map[string]int)
	process := false
	for {
		//var message *Message
		//lines := make([]string, 0)
		var body []string

		message, err := mboxReader.ReadMessage()
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("[iterateMbox] EOF @ msgNumber=%d mboxfile=%s", msgNumber, mboxfile)
				break
			} else {
				log.Printf("[iterateMbox] ERROR: mboxreader err='%#v' msgNumber=%d", err, msgNumber)
				continue
			}
		}

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
		headermap, keysorder, msgid, err := overview.ParseHeaderKeys(head, LAXMID)

		if msgid == "" || err != nil {
			log.Printf("[iterateMbox] mbox %s msgid not found msgnum=%d \n map='%v' \n ko='%v' \n err='%v'", mboxfile, msgNumber, headermap, keysorder, err)
		}

		//log.Printf("bodyreader")
		isbinary := false
		bodyReader := message.BodyReader()
		buffer := make([]byte, 64*1024)
		n := 0

		for {
			//log.Printf("msgnum=%d bodyreader len_lines=%d", msgNumber, len(lines))
			n, err = bodyReader.Read(buffer)
			if err != nil {
				//log.Printf("bodyReader err='%v' n=%d", err, n)
				break
			}
			/*
			if len(line) >= cap(buffer) {
				buffer = make([]byte, len(line))
			}
			*/
			line := string(buffer[0:n])
			if DROPBINARY && !isbinary && strings.HasPrefix(line, "=ybegin") {
				isbinary = true
				//break
			}
			if len(line) >= 1 && line[len(line)-1] == '\n' {
				line = line[0:len(line)-1]
			}
			if !isbinary {
				body = append(body, line)
			}
		}

		if msgid != "" { err = nil }
		globalBools.SetGB("process", true)

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

		queued := len(worker_channel)
		markhi := WORKER_CHAN_SIZE/100
		if markhi < 100 {
			markhi = 100
		}
		//marklo := WORKER_CHAN_SIZE/100*30
		if !process && queued > markhi {
			globalBools.SetGB("process", true)
			//time.Sleep(1 * time.Second)
			process = true
		}
		// TODO clear msgidhashdb
		globalCounter.incCounter(0, "msgnums")
		worker_channel <- &ArticleItem{ msgid, head, body }

	} // end for
	if !process {
		process = true
		globalBools.SetGB("process", process)
	}
	//globalCounter.setCounter("msgnums", msgNumber)
	//log.Printf("msgs=%d sent=%d errcnt=%d duplicate=%d rejected=%d", msgNumber, sentcnt, errcnt, nntpdup435, nntp437)
	log.Printf("[iterateMbox] Done read mbox file='%s' msgNumber=%d err=%v", mboxfile, msgNumber, err)

	//close(worker_channel)

	if err == io.EOF {

		log.Printf("Error reading.")


	} else if err != nil {
		log.Printf("Unable to read next message: %#v\n", err)
	}

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
		} else { break }
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
	if terrcnt == 0 {
		globalBools.SetGB("done", true)
	}
} // end func iterateMbox

func worker(wid int) {
	//debug := false

	wait_for_process:
	for {
		if !globalBools.GetGB("process") {
			//log.Printf("Sleep Worker %d", wid)
			time.Sleep(100 * time.Millisecond)
			continue wait_for_process
		} else {
			//log.Printf("PROCESS Worker %d", wid)
			break wait_for_process
		}
	}

	log.Printf("Launch Worker %d", wid)

	var srvtp *textproto.Conn
	var cliwriter *bufio.Writer
	var cerr error
	retries := 3600
	loop_CB:
	for {
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
	doneTicker := time.NewTimer(time.Second/10)
	//defer doneTicker.Stop()
	loop_worker:
	for {
		select {
			case <- doneTicker.C:
				feeder_done := globalBools.GetGB("feeder_done")
				if feeder_done {
					msgnums := globalCounter.getCounter("msgnums")
					if msgnums == 0 {
						break loop_worker
					}
				}
				//log.Printf("Worker %d) doneTicker: todo msgnums=%d feeder_done=%t", wid, msgnums, feeder_done)
				doneTicker = time.NewTimer(time.Second/10)
				continue loop_worker

			case article := <- worker_channel:

				//if debug {
				//	log.Printf("Worker %d) got msgid='%s' --> IHAVE", wid, article.msgid)
				//}

				code = ihave(wid, article.msgid, article.head, article.body, srvtp, cliwriter)
				switch code {
					case 235:
						wsentcnt++
					case 435:
						wnntpdup435++
					case 436:
						//wnntp436++
					case 437:
						wnntp437++
					case 999:
						wnntp999++
					case -999:
						// re-queue
						log.Printf("Worker %d) ihave failed! re-queue article.msgid='%s'", wid, article.msgid)
						go func(article *ArticleItem, worker_channel chan *ArticleItem){
							worker_channel <- article
						}(article, worker_channel)
						time.Sleep(time.Second)
						go worker(wid)
						globalCounter.addCounter(wid, "sent", wsentcnt)
						globalCounter.addCounter(wid, "total", wtot)
						globalCounter.addCounter(wid, "wnntpdup435", wnntpdup435)
						globalCounter.addCounter(wid, "wnntperr437", wnntp437)
						globalCounter.addCounter(wid, "wnntperrcnt", werrcnt)
						globalCounter.addCounter(wid, "tnntp999", wnntp999)
						return
					default:
						log.Printf("ihave returned werrcnt code=%d", code)
						werrcnt++
				} // end switch
				globalCounter.subCounter(wid, "msgnums", 1)

				if wcnt >= 1000 {
					//globalCounter.subCounter(wid, "msgnums", 1000)
					msgnums := globalCounter.getCounter("msgnums")
					log.Printf("Worker %d) wtot=%d wq=%d rq=%d dup=%d e437=%d err=%d sent=%d ttodo=%d", wid, wtot, len(worker_channel), len(requeue_channel), wnntpdup435, wnntp437, werrcnt, wsentcnt, msgnums)
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

func IsLetter(s string) bool {
	for i, r := range s {
		if unicode.IsLetter(r)||unicode.IsNumber(r)||unicode.IsSpace(r)||unicode.IsPunct(r)||r=='+'||r=='-'||r=='('||r==')' {
			continue
		} else {
			log.Printf("F isLetter s='%s' r='%s' i=%d", s, string(r), i)
			return false
		}
	}
	return true
}

func isspace(b byte) bool {
	return b < 33
}

func IsAlphaUpper(s string) bool {
	for _, r := range s {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

func ihave(wid int, msgid string, head []string, bodylines []string, srvtp *textproto.Conn, CliWriter *bufio.Writer) (int) {
	/*
	if strings.HasPrefix(msgid, "<_txB3") { /,* @ab.jobs *,/
		log.Printf("--> IHAVE --> msgid='%s' head=%d body=%d", msgid, len(head), len(bodylines))
	}
	*/
	test_DRY_RUN := false
	if test_DRY_RUN {
		log.Printf("#### ihave --> msgid='%s'")
		for i, line := range head {
			fmt.Println("h=",i, "=", line)
		}
		for i, line := range bodylines {
			fmt.Println("b=",i, "=", line)
		}
		return 435
	}
	//time.Sleep(10 * time.Second)


	code := 0
	has_hdr_path := false
	has_hdr_from := false
	has_hdr_date := false
	var newhead []string

	fix_msgid := false
	try_fix_date := false
	ignore_nextline := false
	has_subj := false
	has_msgid := false

	var messageids []string
	nntp_posting_date := ""

	for i, line := range head {
		line = overview.Clean_Headline("?", line, DEBUG)
		spaced_line := false
		if len(line) > 0 {
			spaced_line = isspace(line[0])
		} else {
			continue
		}
		spaced_nextline := false
		spaced_prevline := false
		nextline := i+1
		prevline := i-1
		//log.Printf("msgid='%s' line=%d", msgid, i)
		// get the next line if any
		if nextline < len(head)-1 {
			if head[nextline] != "" {
				//str_nextline = head[nextline] // next line as string
				spaced_nextline = isspace(head[nextline][0]) // first char of next line
			}
		}

		// get previous line
		if prevline > 0 && len(head[prevline]) > 0 {
			spaced_prevline = isspace(head[prevline][0])
		}

		// header starting with line "Thu, ... datestring" some google-headers are that broken, we ignore the line
		if isMatch, _ := regex_isbaddateline.MatchString(line); isMatch {
			if spaced_prevline && !spaced_nextline {
				log.Printf("regex_isbaddateline: ignore headline=%d msgid='%s'", i, msgid)
				continue
			} else
			if spaced_prevline && spaced_nextline {
				// add a space
				log.Printf("regex_isbaddateline: headline=%d add space msgid='%s'", i, msgid)
				line = " "+line
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

			if len(header_dat) <= 1 && !spaced_nextline {
				//header_dat is too short
				//if debug_header { log.Printf("   |short header_dat msgid='%s' ignore line='%s' nextline='%s'", s.messageid, line, str_nextline) }
				continue
			}

			if isspace(header_key[len(header_key)-1]) || !strings.HasPrefix(header_dat, " ") {
				// missing colon-space "key: dat"
				newline := strings.TrimSpace(header_key)+": "+strings.TrimSpace(header_dat)
				//if debug_header { log.Printf("[%s/%s/%s/%d] ihave: [%s] | fixed colon-space msgid='%s' oldline='%s' newline='%s' nextline='%s'", s.sessionid, s.username, s.cmdstring, s.beconn.backendID, s.fakegroup, s.messageid, line, newline, str_nextline) }
				line = newline
			}

			header_key = strings.TrimSpace(header_key)
			header_dat = strings.TrimSpace(header_dat)
			header_key_L := strings.ToLower(header_key)

			if !has_subj && header_key_L == "subject" {
				if len(header_dat) > 0 {
					has_subj = true
				}
			}


			if header_key_L == "path" {
				has_hdr_path = true
				line = string(header_key+": mbox2nntp"+my_mboxfile+"!"+header_dat)
			} else
			// check headers
			if strings.ToLower(header_key_L) == "message-id" {
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

			} else
			if header_key_L == "date" {
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

			// some weird articles in php.* have a message-id: <...@...> line following a spaced_line
			/*
			if !fix_msgid && spaced_prevline && spaced_nextline && header_key == "Message-ID" &&
				strings.HasSuffix(header_dat, ">") {
				//log.Printf("[%s/%s/%s/%d] ihave: [%s] | fixed msgid header spaced_prevline && spaced_nextline", s.sessionid, s.username, s.cmdstring, s.beconn.backendID, s.fakegroup)
				fix_msgid = true
				//time.Sleep(3 * time.Second)
				continue
			}
			*/

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
					has_msgid, fix_msgid = true, false
					found = true
					break
				}
			}
			if !found {
				log.Printf("ERROR msgid='%s' got %d messageids=['%v']", msgid, len(messageids), messageids)
			}
		} else
		if messageids[0] == msgid {
			newhead = append(newhead, "Message-ID: "+msgid)
			has_msgid, fix_msgid = true, false
		} else {
			log.Printf("ERROR msgid='%s' no message-id header?!", msgid)
		}

		//time.Sleep(5 * time.Second)
	}


	if !has_subj {
		log.Printf("Error msgid='%s' !has_subj", msgid)
		//head = append(head, "Subject: -")
		return 437
	}

	if fix_msgid || !has_msgid {
		newhead = append(newhead, "Message-ID: "+msgid)
	}


	if try_fix_date && nntp_posting_date != "" {
		newhead = append(newhead, "Date: "+nntp_posting_date)
		has_hdr_date = true
	}


	if !has_hdr_date {
		log.Printf("Error msgid='%s' !has_hdr_date", msgid)
		return 437
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
			//log.Printf("ERROR Worker %d) ihave asked msgid='%s' err='%v' code=%d msg='%s'", wid, msgid, err, code, msg)
			//srvtp.EndResponse(id)
			return code
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
				log.Printf("ERROR Worker %d ihave failed !has_hdr_path msgid='%s' err='%v'",wid , msgid, err)
				return -999
			}
		}

		if !has_hdr_from { }

		_, err = io.WriteString(CliWriter, CRLF)

		for _, bline := range bodylines {
			if len(bline) == 1 && bline == DOT {
				// dot-stuff single dot
				bline = ".."
			} else
			if len(bline) >= 2 && bline[0] == '.' && bline[1] != '.' {
				// dot-stuff any dot at beginning
				bline = "."+bline
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


		_, err = io.WriteString(CliWriter,DOT+CRLF)
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

		//rcode := 0
		if rcode, _, err := srvtp.ReadCodeLine(235); err != nil {

			if rcode != 235 {
				// DEBUG
				if rcode == 437 {
					/*
					print_debug := true

					if strings.HasPrefix(msg, "Unwanted character ") {
						print_debug = true
					} else
					if strings.HasPrefix(msg, "Bad ") {
						print_debug = true
					} else
					if strings.HasPrefix(msg, "Space ") {
						print_debug = true
					} else
					if strings.HasPrefix(msg, "Missing ") {
						print_debug = true
					}

					if print_debug {

						log.Printf("---> ORGHEAD DEBUG msgid='%s'", msgid)
						print_lines(head)
						log.Printf("# # # # # # #")
						log.Printf("---> NEWHEAD DEBUG msgid='%s'", msgid)
						print_lines(newhead)
						log.Printf("---> BODY DEBUG msgid='%s'", msgid)
						print_lines(bodylines)
						log.Printf("<--- END BODY DEBUG msgid='%s'", msgid)


					}
					*/
				}
				//log.Printf("mbox2nntp msgid='%s' rcode=%d msg='%s'", msgid, rcode, msg)
				//time.Sleep(5 * time.Second) // DEBUG_SLEEP
			}
			code = rcode

		} else {
			code = rcode
			//log.Printf("Worker %d OK ihave sent msgid='%s' code=%d err='%v'", wid, msgid, code, err)

		}
	} else {
		code = 235
	}
	//os.Exit(0)
	//time.Sleep(100 * time.Millisecond) // DEBUG_SLEEP
	return code
} // end func ihave

func hash256(astr string) string {
	ahash := sha256.Sum256([]byte(astr))
	return hex.EncodeToString(ahash[:])
} // end func hash256

func print_lines(lines []string){
	for i, line := range lines {
		log.Printf("l%d) %s", i, line)
	}
}
