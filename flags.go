package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-while/mbox2nntp/internal/unpack"
)

// options holds the command line. apply copies it into the package globals
// the rest of the importer reads.
type options struct {
	input      string
	server     string
	workers    int
	tmpdir     string
	dryRun     bool
	keepBinary bool
	noDos2unix bool
	dos2unix   string
	sevenZip   string
	xz         string
	zstd       string
	timeout    time.Duration
	maxTries   int
	prehash    bool
	laxMsgid   bool
	legacy     bool // given as positional arguments
	version    bool // -version: print it and exit
}

const usageHead = `Usage: %[1]s [flags] -server HOST:PORT INPUT
       %[1]s [flags] -dry-run INPUT

Feeds the articles in INPUT to an NNTP server with IHAVE.

INPUT is one of:
  NAME.mbox, NAME.bag   an mbox file
  NAME.txt              an mbox file, if its first line begins with "From "
  NAME.mbox.gz          gzip-compressed mbox
  NAME.mbox.zip         zip archive holding NAME.mbox
  NAME.mbox.7z          7z archive holding NAME.mbox
  NAME.usenet           a UAT archive (Usenet Archive Toolkit), needs zstd;
                        also NAME.usenet.xz and NAME.usenet.7z
  DIR                   a directory tree with one article per file

Flags:
`

func defaultOptions() *options {
	return &options{
		workers:  1,
		tmpdir:   ".",
		timeout:  2 * time.Minute,
		maxTries: 10,
		prehash:  true,
		laxMsgid: true,
	}
}

// parseArgs reads the command line, args without the program name. It
// returns flag.ErrHelp after printing the help for -h. The old positional
// form (INPUT SERVER [WORKERS [TMPDIR [DRYRUN [- [NODROPBINARY_FILE]]]]])
// is still accepted when the first two arguments are not flags.
func parseArgs(name string, args []string, out io.Writer) (*options, error) {
	if len(args) >= 2 && !strings.HasPrefix(args[0], "-") && !strings.HasPrefix(args[1], "-") {
		return parseLegacyArgs(args)
	}
	o := defaultOptions()
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&o.server, "server", "", "NNTP server as `HOST:PORT` (required, except with -dry-run)")
	fs.IntVar(&o.workers, "workers", o.workers, "parallel connections to the server")
	fs.StringVar(&o.tmpdir, "tmpdir", o.tmpdir, "`DIR` to unpack .gz, .zip and .7z archives into")
	fs.BoolVar(&o.dryRun, "dry-run", false, "parse and rewrite articles, but send nothing")
	fs.BoolVar(&o.keepBinary, "keep-binary", false, "send yEnc (=ybegin) articles instead of dropping them")
	fs.BoolVar(&o.noDos2unix, "no-dos2unix", false, "do not run -dos2unix (same as a .nodos2unix file in the working directory); kept for old scripts")
	fs.StringVar(&o.dos2unix, "dos2unix", o.dos2unix, "run the dos2unix at `PATH` on each mbox first, rewriting a plain .mbox in place; off by default, not needed")
	fs.StringVar(&o.sevenZip, "7z", "", "the 7-Zip program at `PATH` that unpacks .7z inputs (default: 7z, 7zz or 7za in PATH)")
	fs.StringVar(&o.xz, "xz", "", "the xz program at `PATH` for .usenet.xz inputs (default: xz in PATH, else 7-Zip)")
	fs.StringVar(&o.zstd, "zstd", "", "the zstd program at `PATH` for .usenet inputs (default: zstd in PATH)")
	fs.DurationVar(&o.timeout, "timeout", o.timeout, "limit on each read or write on a server connection")
	fs.IntVar(&o.maxTries, "max-tries", o.maxTries, "tries per article after 436 or a lost connection, then it counts as an error")
	fs.BoolVar(&o.prehash, "prehash", o.prehash, "send \"IHAVE <msgid> <sha256>\"; -prehash=false sends plain \"IHAVE <msgid>\"")
	fs.BoolVar(&o.laxMsgid, "lax-msgid", o.laxMsgid, "accept loosely formed Message-IDs")
	fs.BoolVar(&o.version, "version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), usageHead, name)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if o.version {
		return o, nil
	}
	switch fs.NArg() {
	case 0:
		return nil, errors.New("no INPUT given")
	case 1:
		o.input = fs.Arg(0)
	default:
		return nil, fmt.Errorf("one INPUT expected, got %q (flags go before INPUT)", fs.Args())
	}
	return o, o.check()
}

func parseLegacyArgs(args []string) (*options, error) {
	o := defaultOptions()
	o.legacy = true
	o.input, o.server = args[0], args[1]
	if len(args) >= 3 {
		n, err := strconv.Atoi(args[2])
		if err != nil {
			return nil, fmt.Errorf("WORKERS: %v", err)
		}
		o.workers = n
	}
	if len(args) >= 4 && args[3] != "" {
		o.tmpdir = args[3]
	}
	if len(args) >= 5 {
		o.dryRun = args[4] == "DRYRUN"
	}
	// args[5] was PREFIX, which nothing ever read.
	if len(args) >= 7 && args[6] != "" {
		o.keepBinary = fileExists(args[6])
	}
	if len(args) > 7 {
		return nil, fmt.Errorf("too many arguments: %q", args)
	}
	return o, o.check()
}

func (o *options) check() error {
	switch {
	case o.server == "" && !o.dryRun:
		return errors.New("-server is required")
	case o.input == "":
		return errors.New("INPUT is empty")
	case o.workers <= 0:
		return fmt.Errorf("-workers must be at least 1, got %d", o.workers)
	case o.maxTries <= 0:
		return fmt.Errorf("-max-tries must be at least 1, got %d", o.maxTries)
	case o.timeout <= 0:
		return fmt.Errorf("-timeout must be positive, got %v", o.timeout)
	}
	return nil
}

// apply sets the package globals from o. A directory INPUT gets the
// trailing "/" the rest of main uses to recognise one.
func (o *options) apply() {
	nntpserv = o.server
	MaxWorkers = o.workers
	tmpdir = o.tmpdir
	DRYRUN = o.dryRun
	DROPBINARY = !o.keepBinary
	NODOS2UNIX = o.noDos2unix || fileExists(".nodos2unix")
	DOS2UNIX = o.dos2unix
	unpack.SevenZip = o.sevenZip
	unpack.Xz = o.xz
	unpack.Zstd = o.zstd
	NNTP_TIMEOUT = o.timeout
	MAX_TRIES = o.maxTries
	PREHASH_MODE = o.prehash
	LAXMID = o.laxMsgid
	if !strings.HasSuffix(o.input, "/") {
		if fi, err := os.Stat(o.input); err == nil && fi.IsDir() {
			o.input += "/"
		}
	}
}
