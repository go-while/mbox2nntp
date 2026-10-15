# mbox2nntp — code review

**Status: tranche 1 is fixed and verified. Everything else below is still notes.**

Fixed in `mbox2nntp.go` and confirmed by a wire-level round trip against a fake NNTP
server plus direct `read_file` tests — each test was also run against the pre-fix code and
reproduced the original bug:

| finding | pre-fix behaviour reproduced | status |
|---|---|---|
| **A1** dot-stuffing | `..bar` arrived as `.bar`, `...baz` as `..baz` | fixed |
| **A2** blank body line | separator landed in `body[0]` | fixed |
| **A3** trailing CR | `\r\r\n` on the wire | fixed |
| **B1** dot terminator | article silently dropped | fixed |
| **C1** nil `FileInfo` | real `SIGSEGV` at `fileExists` | fixed |
| **C2** `msglines[i+1]` | unbounded index | fixed |

`go vet ./...` is now **clean** (the three `log.Printf` defects in H are fixed), and
`mbox2nntp_test.go` holds the three regression tests, which run under `go test ./...` with
vet enabled in ~7ms and need no network.

In `go-lib-mbox2nntp`, **I2's test suite is repaired and green** — 17 tests plus the
package example, the first time it has compiled since the fork. Repairing it turned up two
undocumented behaviour changes (header-less messages are now accepted; a leading
`: value-line` is salvaged as `X-Unknown:`), and it now carries a table test pinning every
separator shape `validid` accepts or rejects, which is the missing safety net for section J.

**J3 is applied**: `validid` now sits behind a byte-prefix gate at both call sites,
measured at ~76× on ordinary body lines with the per-line allocation gone, and guarded by
a differential test.

Still open: **A4, B2–B5, D, E, F, G, rest of H, I3, J1–J2 and J4–J6**.

Findings were verified three ways, not just read off the source: against the real corpus in
`/tank0/tmp/pad/mbox2nntp` (11 GB, 3 621 files, two archive.org ClariNet collections), by
running the parser and the `validid` regex directly, and by diffing the library against its
upstream `github.com/sam-falvo/mbox`.

## Context

`mbox2nntp.go` (1850 lines, single file) has been used in production to
import multiple TB of Usenet articles from archive.org backups into an INN2 server over
NNTP `IHAVE`. The request was a bug hunt.

The review found **three defects that silently corrupted article bodies on every import**,
several **silent article-loss paths**, two **crashes**, and a set of correctness/robustness
problems. Because the corruption is systematic (not sampling-dependent), articles already
injected from past runs are affected and may warrant a re-import of the affected classes.

The repo now has a `go.mod` (Go 1.27.1) resolving every dependency from GitHub — no
`replace` directives, and the pseudo-versions pin exactly the commits the local checkouts
sit on.

**The build now works.** `go build ./...` is silent and produces a ~6.7 MB binary from
published sources alone — no `replace` directives, so a fresh clone builds.

Getting there meant untangling a blocker that turned out to be **abandoned code**.
`nntp-storage/cycbuf_storage.go` calls `utils.CheckNumber64PowerOfTwo`, which existed in no
committed `go-utils` (confirmed with `git log -S` over the full history), and calls
`utils.CheckNumberPowerOfTwo` as a bool although the committed version returned `int`.
Since `nntp-overview` imports `nntp-storage` and `mbox2nntp` imports `nntp-overview`, the
whole chain failed to compile — since at least 2024-06, meaning the committed `mbox2nntp`
binary (2025-05-10) was built from sources that were never pushed. The newer `go-utils`
was recovered from another machine and pushed as `1fd38d8`
(`v0.0.0-20261015041029-1fd38d8cfc2d`), which unblocks everything.

Worth recording, because it shapes any future dependency work: **`cycbuf_storage.go` is
unfinished brainstorming code that never ran**, and it is the sole reference to
`CheckNumber64PowerOfTwo` anywhere in the tree. So the thing that blocked the build for
over a year was dead code, reached only transitively.

The same is true of the dependency weight. `mbox2nntp` uses exactly four symbols from
`nntp-overview` — `Clean_Headline`, `GetMessageID`, `ParseHeaderKeys`, `MMAP_CLOSE` — all
pure string/mmap helpers. `nntp-overview` in turn pulls in `nntp-storage` for 7 references
in a single stats block (`overview.go:527-532`). For that, the binary links `redigo`,
`go-sql-driver/mysql`, `mmap-go`, `johnsiilver/golib`, `x/sys` and `edwards25519`. The
heavy half of the tree is incidental. If it ever becomes a nuisance — slow builds, another
dep breaking, or wanting to vendor — the cheap fix is to split those four helpers out of
`nntp-overview`, not to patch around `nntp-storage`. Not worth doing now.

`gofmt -l` still flags `mbox2nntp.go`.

Two code paths exist and must be reasoned about separately:

- **mbox path** — `proc` → `openMboxFile` → `iterateMbox`, using
  `github.com/go-while/go-lib-mbox2nntp`.
- **directory path** (`arg1` ends in `/`) — `proc` → `/bin/find.sh` → `read_file` per file.

Both funnel into `worker_channel` → `worker()` → `ihave()`.

---

## A. Data corruption (highest priority — affects already-imported articles)

### A1. Dot-stuffing skips lines beginning with two or more dots  *(FIXED)*
`mbox2nntp.go:1714-1722`

```go
if len(bline) == 1 && bline == DOT {
        bline = ".."
} else if len(bline) >= 2 && bline[0] == '.' && bline[1] != '.' {
        bline = "." + bline
}
```

RFC 3977 §3.1.1 requires a `.` to be prepended to **any** line starting with `.`. The
`bline[1] != '.'` guard means `..foo` is transmitted verbatim and the receiver un-stuffs it
to `.foo`; `..` becomes `.`. Every body line starting with 2+ dots lost one dot — ASCII
art, `..` in quoted paths/diffs, ellipsis-leading lines.

**Fix:** drop the second condition entirely and collapse to one rule:

```go
if strings.HasPrefix(bline, ".") {
        bline = "." + bline
}
```

### A2. Directory path injects a spurious blank first body line into every article  *(FIXED)*
`mbox2nntp.go:491-495`

```go
if read_head && (line == "\r\n" || line == "") {
        read_head = false          // <-- no `continue`
}
if read_head { ... } else { body = append(body, line) }
```

The header/body separator line falls through to the `else` and becomes `body[0]`. `ihave`
then writes its own separator `CRLF` at `mbox2nntp.go:1712`, so **every article imported
via the directory path gained a leading empty line**.

**Fix:** add `continue` after `read_head = false`.

### A3. Trailing CR survives on the mbox path → `\r\r\n` on the wire  *(FIXED)*
`mbox2nntp.go:1174-1176`

```go
if len(line) >= 1 && line[len(line)-1] == '\n' {
        line = line[0 : len(line)-1]
}
```

Only LF is stripped; `ihave` then appends `CRLF`. For any CRLF-terminated mbox the body
lines go out as `...\r\r\n`. The same applies to header lines: the library's
`parseHeaders` does `strings.Replace(line, "\n", "", -1)` only
(`go-lib-mbox2nntp/mbox.go`), and `overview.Clean_Headline` does not strip `\r` either.
`dos2unix` is the only thing that saves this, and it is skipped whenever a `.nodos2unix`
sentinel file exists (`mbox2nntp.go:386`) and never runs on the directory path.

**Fix:** use `strings.TrimRight(line, "\r\n")` (or `TrimSuffix` LF then CR) in
`iterateMbox`, and strip a trailing `\r` in `ihave` before writing each header and body
line, so correctness does not depend on `dos2unix` having run.

### A4. mboxrd `>From ` escaping is never reversed
The library documents this explicitly (`go-lib-mbox2nntp/doc.go`): it "will not reverse
any >From-escaping". `iterateMbox` does not either, so every mboxrd-sourced body line that
originally began with `From ` was stored as `>From `.

**Fix:** decide per-source. Practical heuristic: on the mbox path, strip one leading `>`
from lines matching `^>*From `. Gate it behind a flag, since mboxo sources must not be
un-escaped.

---

## B. Silent article loss

### B1. Directory path drops every article whose file ends in `.`  *(FIXED)*
`mbox2nntp.go:450-455`

```go
if lines[len(lines)-1] != '.' {
        lines = append(lines, '\r', '\n', '.')
}
```

The check looks at the last **byte**, not the last **line**. A file ending
`...end of sentence.` (no trailing newline) already ends in `.`, so no dot terminator is
appended. `ReadDotLines` then hits EOF mid-message and returns `io.ErrUnexpectedEOF`
(`net/textproto/reader.go:364`), the function returns `""`, and the article is counted as
`badfiles` and discarded.

**Fix:** only treat it as terminated when the final line is exactly `.`:

```go
if !bytes.HasSuffix(lines, []byte("\n.")) && !bytes.Equal(lines, []byte(".")) {
        lines = append(lines, "\r\n."...)
}
```

### B2. `ihave` return code 436 is silently discarded
`mbox2nntp.go:1338-1339` — `case 436:` has an empty body, then
`mbox2nntp.go:1363` decrements `msgnums`. 436 means "transfer failed, try again later";
the article is dropped and not even counted as an error, so `terrcnt` stays 0 and the mbox
gets renamed `.done` (`mbox2nntp.go:824-838`).

**Fix:** treat 436 like -999 (requeue), or at minimum count it into `werrcnt` so the source
is not marked done.

### B3. Read errors report code 0, which is swallowed by `default:`
`mbox2nntp.go:1683` and `mbox2nntp.go:1787`

`textproto.ReadCodeLine` returns `code == 0` for I/O errors (connection reset, timeout) as
opposed to a protocol `*textproto.Error`. Both call sites propagate that 0 as the return
value, so `worker` hits `default:` → `werrcnt++` → **article discarded, connection kept**.
Only a subsequent *write* failure produces -999 and a reconnect.

**Fix:** in `ihave`, distinguish: if `err` is not a `*textproto.Error`, return `-999` so the
article is requeued and the connection rebuilt.

### B4. Articles with no `Date:` header are dropped, and the repair path is dead code
`mbox2nntp.go:1641-1644` returns 437. The intended repair at `mbox2nntp.go:1635` is
unreachable: `try_fix_date` is declared `false` at line 1443 and never assigned, and
`nntp_posting_date` (line 1449) is never populated — nothing ever reads
`NNTP-Posting-Date` / `X-Trace`. Same for `has_hdr_from`, whose only use is the empty block
`if !has_hdr_from { }` at `mbox2nntp.go:1710`.

**Fix:** actually populate `nntp_posting_date` from `NNTP-Posting-Date`, `X-Original-Date`,
or the mbox `From ` separator timestamp, and synthesize `From:` when missing (as
`read_file` already does at line 573).

### B5. Overly broad directory filter
`mbox2nntp.go:982` — `strings.Contains(fpl, ")")` rejects a file if **any** component of
its path contains `)`. A single parent directory named e.g. `alt.foo (part 2)` silently
drops its entire subtree. Likewise `/patch` (line 983) matches anywhere in the path.

**Fix:** apply these tests to `filepath.Base(fp)`, not the full path.

---

## C. Crashes

### C1. `fileExists` / `dirExists` nil-pointer dereference  *(FIXED)*
`mbox2nntp.go:234-240` and `mbox2nntp.go:1041-1047`

```go
info, err := os.Stat(filename)
if os.IsNotExist(err) { return false }
return !info.IsDir()          // info == nil for EACCES, ENOTDIR, ELOOP, ENAMETOOLONG
```

Any `Stat` error other than ENOENT panics. Archive.org spools routinely produce
over-long paths and odd permissions.

**Fix:** `if err != nil { return false }`. `utils.FileExists` is already used elsewhere in
this file (`mbox2nntp.go:715`, `827`) — check whether it handles this and, if so, delete
both local helpers in favour of it.

### C2. Index out of range in `read_file`  *(FIXED)*
`mbox2nntp.go:500` — `msglines[i+1]` with no bound check; panics when a truncated file's
last line is exactly `Message-ID:`. Guard with `i+1 < len(msglines)`.

---

## D. Connection handling

### D1. Socket leak on every reconnect
`connectBackend` (`mbox2nntp.go:198`) returns the `*textproto.Conn` but the worker never
closes it. On the -999 path (`mbox2nntp.go:1344-1358`) the worker abandons `srvtp` and
spawns a replacement — the old socket is never closed and the old `net.Conn` is
unreachable. A long run against a flaky server exhausts file descriptors.

**Fix:** return the `net.Conn` (or call `srvtp.Close()`) and close it before reconnecting
and on worker exit; send `QUIT` first.

### D2. No read/write deadlines
`net.DialTimeout` covers only the dial. Neither `srvtp.ReadCodeLine` call has a deadline,
so a stalled peer hangs a worker forever with no diagnostic — this is the classic "import
stopped overnight" symptom.

**Fix:** keep the `net.Conn` and `SetDeadline` around each request/response.

### D3. `connectBackend` can return `(nil, nil, nil)`
`mbox2nntp.go:212-216` — for a 2xx greeting that is neither 200 nor 201 (e.g. 250),
`ReadCodeLine(20)` succeeds with `err == nil`, the guard fires, and the function returns
three nils. The worker sees `cerr == nil`, breaks out of `loop_CB`, and dereferences a nil
`*textproto.Conn` in `ihave`.

**Fix:** synthesize an error in that branch.

---

## E. Header-processing bugs in `ihave`

- **`mbox2nntp.go:1465`** — `if nextline < len(head)-1` is off by one; should be
  `nextline < len(head)`. The last header line's successor is never inspected, so
  `spaced_nextline` is wrong at `i == len(head)-2` and trailing continuation lines of a
  dropped `Xref:`/`From ` header leak into `newhead`.
  The identical bug is copy-pasted into `nntp-overview/overview.go:2649`.
- **`mbox2nntp.go:1473`** — `if prevline > 0` should be `>= 0`; `head[0]` is never used as
  a previous line.
- **`mbox2nntp.go:1543`** — `strings.ToLower(header_key_L)` lowercases an
  already-lowercased value. Harmless, but delete it.
- **`mbox2nntp.go:1514`** — `len(header_dat) <= 1` drops any header whose value is a single
  character (e.g. `Lines: 7` is fine, but `X-No-Archive:1` style single-char values are
  dropped).
- **`overview.Clean_Headline`** (`nntp-overview/overview.go:1047`) does
  `strings.Replace(line, "  ", " ", -1)` — a single non-overlapping pass, so `a····b`
  becomes `a··b` rather than `a·b`. It is not idempotent and it rewrites header *content*
  (Subjects, `X-Trace`, base64 continuations). Consider limiting the collapse to leading
  whitespace of continuation lines.
- **`mbox2nntp.go:1712`** — `_, err = io.WriteString(CliWriter, CRLF)` ignores `err`,
  unlike every other write in the function.

---

## F. Path / cleanup bugs

- **`mbox2nntp.go:375`** — `mvmbox(zip)` inside the `has_zip7` branch; should be
  `mvmbox(zip7)`. The `.7z` archive is never marked done and the `.zip` is renamed twice.
- **`mbox2nntp.go:1033`** — `if (is_zip || is_gzp)` omits `is_zip7`, so `proc` returns the
  original `.7z` path instead of `mboxfiletrim`. Downstream logging and cleanup then act on
  the wrong file.
- **`mbox2nntp.go:827`** — `utils.FileExists(tmpdir + "/" + mboxfile)` where `mboxfile` is
  the value returned by `proc`, which already went through `make_mboxfilefiletrim` and is
  tmpdir-prefixed. The result is `tmpdir/tmpdir/name`, so **when `TMPDIR != "." the
  decompressed mbox is never deleted** — this is the "temp dir fills up" failure.
- **`mbox2nntp.go:925`, `940`, `954`** — `strings.TrimSuffix(mboxfile, ".zip")` keeps the
  source directory component, and `make_mboxfilefiletrim` then prepends `tmpdir`, producing
  `tmpdir//abs/path/to/foo.mbox`. Use `filepath.Base` before joining, and use
  `filepath.Join`. `gunzip` (line 315) additionally uses
  `strings.Replace(mboxfile, ".gz", "", -1)`, which strips `.gz` anywhere in the path.
- **`mbox2nntp.go:226`** — `convmv` is a **filename** re-encoder, not a content converter,
  and the arguments are malformed: `"-f ISO-8859-1"` is passed as a single argv element
  with a leading space. This step either no-ops or `os.Exit(1)`s. Either drop `convert()`
  or replace it with an in-process `charmap.ISO8859_1` decode of the mbox content.
- **`exec.Command("/bin/rm", mboxfile)`** (line 334), `/bin/mv` (line 404), and the
  unzip/gunzip calls pass filenames without a `--` separator. A file named `-rf` or
  `--help` is interpreted as a flag. Add `--`, or replace `rm`/`mv` with `os.Remove` /
  `os.Rename`.
- **`mbox2nntp.go:966-971`** — `/bin/find.sh` output is buffered whole via `c.Output()` and
  split on `"\n"`. For a multi-million-file spool this is hundreds of MB resident before
  any work starts, and filenames containing newlines break the split. Replace with
  `filepath.WalkDir`, which also removes the external script dependency.

---

## G. Resource / throughput

- **`WORKER_CHAN_SIZE = 1000000`** (`mbox2nntp.go:38`) with two channels of that size
  (`worker_channel`, `requeue_channel`). Workers do not start until `process` is set, which
  for the mbox path requires 10 000 queued items (`mbox2nntp.go:1200`) and for the directory
  path 100 000 (`mbox2nntp.go:630`). Until then the feeder fills the queue alone, holding up
  to 1 M fully-materialized `ArticleItem`s (two `[]string`s each) in RAM. At a 4 KB average
  that is ~4 GB. `requeue_channel` is allocated and **never used** except in a log line.
  **Fix:** reduce the queue to a few thousand, start workers immediately (delete the
  `process` gate), and delete `requeue_channel`.
- **`bufio.NewWriter(conn)`** (`mbox2nntp.go:209`) uses the 4 KB default, so a typical
  article causes several small writes. A 64 KB writer measurably improves TB-scale throughput.
- **`bufio.NewReaderSize(fh, 1024*1024*1024)`** (`mbox2nntp.go:1055`) allocates a **1 GiB**
  read buffer per mbox file. 1–4 MB is ample.
- **`go-lib-mbox2nntp/message.go`** runs `validid.MatchString` (a `regexp2` backtracking
  match) against **every body line** to detect the next `From ` separator. This is the
  hottest path in the whole importer. Replace with a `bytes.HasPrefix(prefetch, []byte("From "))`
  pre-check before falling back to the regex.
- **`doneTicker`** (`mbox2nntp.go:1309`, `1323`) is replaced with a fresh `time.NewTimer`
  and never `Stop()`ed. Use a single `time.Ticker`.
- **`GlobalCounter`** serializes every worker's per-article increment on one mutex. Switch
  the hot counters (`msgnums`, `sent`) to `atomic.Int64`.

---

## H. Build & hygiene

- **RESOLVED — the repo builds.** `go.mod` (module `github.com/go-while/mbox2nntp`,
  `go 1.27.1`) resolves every dependency from GitHub with **no `replace` directives**, so a
  fresh clone builds. `go build ./...` is silent; the binary is ~6.7 MB. The pseudo-versions
  pin exactly the commits the local sibling checkouts sit on, and the five sibling repos
  were left untouched.

- **RESOLVED — `tsm/GlobalBools` import.** The vendored package declares
  `package tsm_globalbools` (lowercase) while `mbox2nntp.go:65` and `:663` reference
  `tsm_GlobalBools`, and `tsm/GlobalBools` is not a valid module path. Both were fixed by
  one line at `mbox2nntp.go:32`:

  ```go
  tsm_GlobalBools "github.com/go-while/mbox2nntp/tsm/GlobalBools"
  ```

- **The dependency tree is mostly incidental.** `mbox2nntp` uses four symbols from
  `nntp-overview` — `Clean_Headline`, `GetMessageID`, `ParseHeaderKeys`, `MMAP_CLOSE`, all
  pure string/mmap helpers. `nntp-overview` pulls `nntp-storage` for 7 references in one
  stats block (`overview.go:527-532`), and that drags in `redigo`,
  `go-sql-driver/mysql`, `mmap-go`, `johnsiilver/golib`, `x/sys` and `edwards25519`.
  Worse, the thing that blocked the build for over a year — `CheckNumber64PowerOfTwo` —
  is referenced only from `nntp-storage/cycbuf_storage.go`, which is unfinished code that
  never ran. If the tree ever becomes a nuisance, split those four helpers out of
  `nntp-overview` rather than patching around `nntp-storage`.

- `mbox2nntp` (a 4.7 MB binary) is committed despite `.gitignore`. Remove it from the tree.
- **RESOLVED — `go vet ./...` is clean.** It had reported three `log.Printf` defects,
  all now fixed: a missing verb at `:687`, a missing argument at `:1426`, and a
  non-constant format string at `:830` — `log.Printf("cleanup: rm '"+tmpdir+"/"+mboxfile+"'")`,
  where a filename containing `%` (not rare in archive.org spools) was read as a verb.
  These also mattered because they failed the `go test` build, so no test could run at all.

  **Correction to an earlier draft:** that draft also predicted a `copylocks` warning at
  `mbox2nntp.go:663-664`. Vet does *not* flag it — assigning a fresh composite literal
  whose `sync.Mutex` is at its zero value is not a lock copy. The code is fine as written.
- `gofmt -l` flags the file. Run `gofmt -w`.
- **Argument parsing**: `mbox2nntp.go:707` uses `if len(os.Args) == 7` for `PREFIX` while
  the next block uses `>= 8`, so `PREFIX` is never set when 8 arguments are supplied. `PREFIX`
  is also never read anywhere. The usage string (line 725) omits arguments 6 and 7. Replace
  the positional chain with `flag`.
- **`DROPBINARY` is not honoured on the directory path**: `mbox2nntp.go:553` drops `=ybegin`
  unconditionally, while `mbox2nntp.go:1170` respects the flag.
- **Dead code to delete**: `unrar` (never called), `test_first_byte` (only referenced from a
  comment), `oldGetMessageID`, `IsLetter`, `IsAlphaUpper`, `inn2AddGroup` + `addedgroups`
  (unsynchronized map, never called), `notify_chan`, `requeue_channel`, `PREFIX`,
  `regex_isAlphaNumStart`, package-level `sentcnt`/`errcnt`/`nntpdup435`/`nntp437`,
  `fix_msgid`, `has_hdr_from`, `try_fix_date`, `nntp_posting_date`, and the `test_DRY_RUN`
  block at `mbox2nntp.go:1422`. Roughly 400 lines of commented-out code can go with them.

---

## I. Library (`go-lib-mbox2nntp`) vs. upstream `github.com/sam-falvo/mbox`

Diffed against the pristine upstream at `/tank0/efs/GO/src/github.com/sam-falvo/mbox`
(fork point `44cf0fa`, current `64bfa93`). Ignoring the tabs→4-spaces reindent, the fork
touches `mbox.go` and `message.go` only; `doc.go` and `mbox_test.go` are byte-identical to
upstream.

### I1. What the fork FIXED (real upstream bugs — do not revert these)

- **Header ordering.** Upstream returned `map[string][]string` from `Headers()`. Go
  randomises map iteration, so header order was scrambled on every read. For NNTP that is
  fatal — it reorders `Path:`/`Xref:` and changes the article byte-for-byte. The fork's
  change to `[]string` is the stated reason for the fork and it is correct.
- **Panic on any line over 1000 bytes.** Upstream `nextLine()`:

  ```go
  prefetch: make([]byte, 1000),          // CreateMboxStream
  slice, err := m.r.ReadSlice('\n')
  m.prefetch = m.prefetch[0:len(slice)]  // panics when len(slice) > 1000
  ```

  Any header or body line between 1001 and 4096 bytes panics with a slice-bounds error.
  Usenet `Path:` and `References:` headers routinely exceed 1000 bytes — the gna-clari
  sample below has `Path:` at ~370 chars and longer ones exist. The fork switched to
  `ReadBytes` and grows `prefetch` on demand. Genuine fix.
- **Panic on empty prefetch in `parseBlankLine`.** Upstream's
  `if (len(m.prefetch) > 1) || (m.prefetch[0] != '\n')` evaluates `m.prefetch[0]` even when
  the slice is empty. The fork's `if len(m.prefetch) > 0 && m.prefetch[0] != '\n'` is
  correct.

### I2. What the fork INTRODUCED

- **The test suite stopped compiling at the fork and had never run since.**  *(FIXED)*
  `mbox_test.go` was byte-identical to upstream and still expected
  `Headers() map[string][]string`:

  ```
  ./mbox_test.go:227:9: cannot convert "Subject" (untyped string constant) to type int
  FAIL	mbox [build failed]
  ```

  Separately, every fixture in it used `From foo@bar.com`, which the fork's `validid`
  rejects — `CreateMboxStream` on the upstream fixture returns `EOF`, which means
  `openMboxFile` would `os.Exit(1)`. So even after repairing the type errors the fixtures
  contradicted the parser. **This is why the regex history in section J is a stack of
  commented-out attempts: there was no way to change it and check.**

  Repaired: fixtures moved to `From nobody Thu Dec  8 12:55:04 2016` (the gna-clari shape),
  map assertions rewritten against the ordered `[]string` — including exact whole-slice
  comparisons, since preserving header order is the entire reason the fork changed the
  type — and the package example updated to scan by prefix the way a caller now must.
  Result: **17 tests and one example, all passing, `go vet` clean, `gofmt` clean.**

  Two behaviour changes surfaced only once the suite ran, neither previously recorded:

  - **Header-less messages are now accepted.** Upstream errored; the fork's `parseHeaders`
    breaks out of its loop as soon as the prefetched line is blank and returns an empty
    header set. This is what lets the 102 header-less stubs measured in J2 through instead
    of aborting the file — deliberate, but the exact opposite of what upstream's
    `TestMalformedMboxFile50` asserted. The test now asserts the fork's behaviour, with the
    reason written down.
  - **A leading `: value-line` is salvaged, not rejected.** `parseHeader` rewrites it to
    `X-Unknown: value-line`. Right call for archive import; again the opposite of
    upstream's `TestMalformedMboxFile70`.

  Two further notes from the repair. `TestMalformedMboxFile20/30/40` pass for the wrong
  reason: their fixtures carry no trailing newline, so they fail in `nextLine`'s
  `ReadBytes('\n')` with `io.EOF` long before reaching the sender checks the test names
  describe. And `CreateMboxStream` returns a **non-nil `*MboxStream` together with a
  non-nil error** when `extractSendingAddress` fails — it logs and falls through rather
  than nilling `m` as the `nextLine` branch above it does. Callers that check `m != nil`
  instead of `err != nil` will march on with a stream positioned at a line the parser just
  rejected.

  The new `TestValididSeparatorShapes` is the part worth keeping: a table of 19 separator
  shapes with their accept/reject verdicts, covering the real shapes, the two false
  positives and the four gaps from J2. Every prediction in J2 was confirmed by it. Change
  `validid` and the table tells you exactly which cell moved.

- **Inconsistent CR handling between a header's first line and its continuation lines.**
  `parseHeader` keeps the raw line for the first line (`line := string(m.prefetch)`,
  including `\r\n`) but runs continuations through
  `strings.TrimRight(string(m.prefetch), " \r\n\t\b\v")`. `parseHeaders` then strips only
  `"\n"`. Net: first lines keep a trailing `\r`, continuations do not. Upstream was
  consistent (`TrimSpace` on the value, `TrimRight` on continuations). This is the library
  half of **A3**.

- **A new unguarded index, while guarding the old one.** Upstream had one bare
  `m.prefetch[0]` in `parseHeaders`, on the success path. The fork guards that one
  (`len(m.prefetch) == 0 || m.prefetch[0] == '\n'`) but adds a *new* bare `m.prefetch[0]`
  inside the `err != nil` branch, which is reached precisely when `parseHeader` bailed on a
  short line. **Correction to an earlier draft of this review:** this is not actually
  reachable — `nextLine` only assigns `prefetch` from a successful `ReadBytes`, which
  always yields at least `"\n"`, and returns early otherwise, so `len(prefetch) == 0`
  never holds. Worth tidying for symmetry, not a live panic.

- **`> 5` → `> 6` in `bodyReader.Read`.**  *(FIXED)* Upstream's `len(prefetch) > 5` existed
  to make `prefetch[0:5]` safe. The fork removed the slice but kept a magic minimum and
  bumped it by one. No separator shape is ≤ 6 bytes so nothing broke, but the constant was
  meaningless — replaced by the prefix gate in J3.

### I3. Still open (present in both, not a regression)

- **`nextLine()` discards the final line of a file that does not end in a newline.** On
  `ReadBytes('\n')` error the returned bytes are thrown away along with the data.
- **`bodyReader.Read` can split a long line.** A body line longer than the caller's buffer
  (64 KiB in `iterateMbox`) comes back as two `Read` calls; `iterateMbox` treats each as a
  separate line and appends `CRLF` to both. Rare, but it hits exactly the long
  base64/uuencoded lines that matter.

---

## J. The regexes — what they do and what to keep

Checked against the real corpus in `/tank0/tmp/pad/mbox2nntp` (11 GB, 3 621 files, two
archive.org ClariNet collections).

### J1. `validid` is the most load-bearing line in the project

`mbox.go:21` is used in **two** places with **opposite** failure modes:

- `extractSendingAddress` (`mbox.go:120`) — decides where a message *starts*
- `bodyReader.Read` (`message.go:89`) — tested against **every body line** to decide where
  a body *ends*

A false positive **splits** an article in two and injects a fragment; a false negative
**merges** two articles and loses one. Those are not symmetric, which is why the fork
replaced upstream's broad `prefetch[0:5] == "From "` with a narrow allowlist. That was the
right call — upstream's version splits an article at any body line beginning `From `.

### J2. The allowlist is correct for this corpus — measured, not assumed

Two shapes are in use, one per collection:

| collection | archive | separator shape | regex branch |
|---|---|---|---|
| `gna-clari` | `.mbox.gz` | `From nobody Thu Dec  8 12:55:04 2016` | `nobody [A-Z][a-z][a-z] [A-Z][a-z][a-z] ` |
| `usenet-clari` | `.mbox.zip` | `From -191826460637372077` / `From 191826460637372077` | `-[0-9]{8}` / `[0-9]{8}` |

Running the real `validid` over both collections, using "the next line is a header line" as
ground truth:

```
gna-clari   (60 files, 8.4M lines)   true separators 95 087   false splits 102   missed 0
usenet-clari(60 files, 747K lines)   true separators  9 588   false splits   0   missed 0
```

The 102 are separators followed immediately by a blank line — headerless stubs, which
`ParseHeaderKeys` already rejects. So: **zero real false positives, zero misses across
~105 000 articles.** Leave the allowlist alone.

Gaps do exist for shapes not present here, confirmed by direct test — `From user@domain.com
Mon Sep  1 12:00:00 2003`, `From mailer-daemon ...`, `From 1234567` (the pattern hardcodes
8 digits), `From ???@??? Sun Jan  1 00:00:00 1995`. Each would silently merge. And
`From MAILER-DAEMON I got this bounce:` / `From nobody Thu 3 people replied` *do* match and
would split. None occur in this corpus; they matter for the next collection.

### J3. Gate the regex behind a byte-prefix check  *(FIXED)*

Every alternative is anchored to `^From ` or `^#! rnews `, so

```go
func couldBeSep(p []byte) bool {
	return bytes.HasPrefix(p, []byte("From ")) || bytes.HasPrefix(p, []byte("#! rnews "))
}
```

cannot hide a match (asserted by a test over all six branch shapes, and by a
`gate_hid_a_match=0` counter over all 9.1M corpus lines).

Applied as `couldBeSep` in `mbox.go`, used at both call sites — `extractSendingAddress`
and `bodyReader.Read`. It also retires the `len(prefetch) > 6` magic constant noted in I2:
the shortest string `validid` can accept is `#! rnews 1` at 10 bytes, so that bound never
excluded a match, and `TestValididMinimumMatchLength` now checks it.

Measured in-repo, `-benchtime=1s -count=6`, same `[]byte` input through both paths and both
paying for the `string()` conversion the call sites actually do:

| | ns/op (6 runs) | B/op | allocs/op |
|---|---|---|---|
| bare regex, ordinary body line | 1563–1986 | 208 | 2 |
| **gated, ordinary body line** | **19.9–23.8** | **0** | **0** |
| bare regex, actual separator | 1803–3362 | 192 | 2 |
| gated, actual separator | 1778–2048 | 192 | 2 |

So **~76× on the common case**, and the allocation disappears with it — the gate skips
the `string(prefetch)` conversion as well as the match. Separators are unchanged, as
expected: the gate passes and the regex runs anyway.

(An earlier draft of this section quoted 112× from a scratch benchmark that compared a
string literal against a `[]byte` input, so the bare side was not paying for the
conversion. The table above is the fair pair.)

Three tests guard it, all in `go-lib-mbox2nntp/mbox_test.go`:
`TestValididPrefixGate` (the one-way property — nothing `validid` accepts fails the gate),
`TestValididGateIsTransparent` (the differential test — gated and bare agree on every line
in the corpus plus every truncation of every accepted shape), and
`TestValididMinimumMatchLength`.

This is the hottest loop in the importer — it runs once per body line across every TB.

### J4. Instrument the near-misses

Add a counter for lines that start with `From ` but fail `validid`, and log a sample. That
turns "which branches do we still need" into a number you read off each run, instead of
guessing. Point it at one file per new archive.org collection before importing it.

### J5. Split the alternation into named patterns

Same behaviour, but each branch carries the collection that needed it:

```go
var sepPatterns = []string{
	`^#\! rnews [0-9]+`,                     // rnews batch files
	`^From MAILER-DAEMON `,                  // ...
	`^From news@gmane\.org [A-Z][a-z][a-z] [A-Z][a-z][a-z] `, // gmane
	`^From -?[0-9]{8,}`,                     // usenet-clari / Deja-Google dumps
	`^From nobody [A-Z][a-z][a-z] ...`,      // gna-clari / Giganews
}
var validid = regexp2.MustCompile(strings.Join(sepPatterns, "|"), 0)
```

### J6. The two regexes in `mbox2nntp.go`

- **`regex_isAlphaNumStart` (`mbox2nntp.go:70`) is never used.** Delete it.
- **`regex_isbaddateline` (`mbox2nntp.go:71`)** only acts when the *previous* line is a
  continuation (`mbox2nntp.go:1479-1487`). An unfolded `Thu, 1 Jan 1970 00:00:00 +0000`
  following a normal header falls through both branches, reaches the colon split at
  `mbox2nntp.go:1507`, and is emitted as a bogus header with
  `header_key = "Thu, 1 Jan 1970 00"` — which INN 437s. Note this interacts with the
  `prevline > 0` off-by-one in **E**: `spaced_prevline` is never true for `i <= 1`. It is
  also a 5-byte test (`^[A-Z][a-z][a-z], `) that does not need a backtracking engine.

---

## Suggested execution order

1. **A1, A2, A3, B1, C1, C2** — corruption and crashes, each a few lines. Do these first
   and together; they are independent of any restructuring.  *(DONE)*
2. **B2, B3, D1, D2, D3** — NNTP session correctness. Requires threading the `net.Conn`
   through `connectBackend` → `worker` → `ihave`.
3. **F** — path and cleanup fixes; switch to `filepath.Join`/`filepath.Base`, replace
   `find.sh` with `filepath.WalkDir`, add `--` to the exec calls.
4. **H** — unblock the build (`go-utils`), then `gofmt`, `go vet` clean, dead-code
   deletion, `flag`-based arguments.  *(build and vet DONE; gofmt, dead code and `flag`
   still open)*
5. **I2** — repair `mbox_test.go` so the library has a working test suite again. Do this
   *before* touching `validid`: it is the reason the regex could never be changed safely.
   *(DONE)*
6. **J3** — the byte-prefix gate in front of `validid`. Isolated, measured ~76× on the
   hottest loop, and provably cannot change behaviour.  *(DONE)*
7. **G, E, J4–J6, A4, B4, B5** — throughput, header handling, regex instrumentation and
   naming, and the optional `>From ` / missing-`Date:` repair work.

## Verification

There are no tests today (`go-lib-mbox2nntp/mbox_test.go` is the only test file in the tree,
and it covers the library, not this importer).

1. **Build clean**: `go build ./... && go vet ./... && gofmt -l .` — all silent.
2. **Golden-file unit tests** for the pure functions, which is where all the corruption
   lives and where no network is needed:
   - `read_file` on fixtures covering: file ending in `.` (B1), file ending in `.\n`,
     header-only file (C2), a body whose first line must not be blank (A2), body lines
     `.`, `..`, `..foo`, `.foo` (A1), CRLF and LF variants (A3).
   - a `stuffLine` helper extracted from `ihave`, table-driven over the same dot cases.
3. **Wire-level round trip** without touching INN: stand up a throwaway `net.Listener` that
   speaks 200/335/235, capture the bytes a worker sends for a fixture article, and assert
   the received article is byte-identical to the source after un-stuffing. This is the test
   that would have caught A1, A2 and A3 at once.
4. **Failure injection** against the same fake server: close the socket after 335 (expect
   -999 and a requeue, not `werrcnt++`), reply 436 (expect requeue), reply 250 to the
   greeting (expect a connection error, not a panic).
5. **DRYRUN over a real corpus**: run with `DRYRUN` against a directory of known article
   count and assert `total == sent` and `badfiles == 0` for a corpus known to be clean —
   the current code reports a nonzero `badfiles` for B1/B5 cases.
6. **End-to-end**: import one known mbox into a scratch INN2 spool, then `diff` a sample of
   articles fetched back via `ARTICLE <msgid>` against the mbox source.
