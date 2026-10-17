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

**J4 is applied, and running it over the full corpus found B6.** No separator is missed
anywhere — `validid` is complete for both collections — but one malformed message made the
importer silently drop the rest of its file. On the raw archives that is 112,828
usenet-clari articles (33.7%), because the parser did not recognise a CRLF blank line. The
importer's default `dos2unix` pass hides the CRLF part, so a default run lost 1,421, in 2
files. **B6 is fixed** in both the library and the importer: 2 articles lost in either mode,
and gna-clari unchanged.

**E1 corrupted a header in 82% of gna-clari articles** fed so far: a folded `Xref:` tail
was appended to the header before it (`Approved`, `Bytes`, `Path`, …). It is fixed; see E
for whether the server stored or rejected them.

**The remaining items are closed.**
- B5, H, I3, J5 and J6 are fixed.
- A4 and B4 are closed by measuring the corpus. No archive here is mbox-escaped, and no
  article lacks `Date:`, so a change in either would have repaired nothing and risked
  damage.
- J1–J2 were analysis with no action: the allowlist stays.

Nothing is open in the code. Operationally, re-feed the B6 and E1 files with the fixed build
(see B6 and E).

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

**Applied in two parts.** The body half went in with tranche 1 (`iterateMbox` trims
`"\r\n"`). The header half was missed at first, and showed up when a CRLF article was
captured on the wire without `dos2unix`: `From: C-ap@clari.net (AP)\r\r\n`. It is now fixed
in the library: `parseHeaders` trims `"\r\n"` (go-lib-mbox2nntp `f0e5b1c`). Without
`dos2unix`, that affects 12,064 usenet-clari articles in 281 files, and none in gna-clari.
`TestCRLFArticleOnTheWire` pins the whole path, parser to IHAVE. The directory path needed
no change, because `textproto.ReadDotLines` already strips CRLF.

`dos2unix` is now optional for correctness, but it stays the default. `main` checks for
`/usr/bin/dos2unix` at startup and exits with a clear message before anything is unpacked,
unless `.nodos2unix` is present. Before, a missing `dos2unix` failed only after the unzip,
and left the extracted file behind.

### A4. mboxrd `>From ` escaping is never reversed  *(CLOSED — measured: none of these archives is escaped)*
The library documents this explicitly (`go-lib-mbox2nntp/doc.go`): it "will not reverse
any >From-escaping". `iterateMbox` does not either, so every mboxrd-sourced body line that
originally began with `From ` was stored as `>From `.

**Fix:** decide per-source. Practical heuristic: on the mbox path, strip one leading `>`
from lines matching `^>*From `. Gate it behind a flag, since mboxo sources must not be
un-escaped.

**Measured, then closed without a code change: nothing here was escaped, so nothing may
be un-escaped.** Body lines matching `^>*From `, counted once per Message-ID across all four
sources:

| source | `From ` (unescaped) | `>From ` | `>>From ` |
|---|---|---|---|
| usenet-clari | 579 | 343 | 0 |
| gna-clari | 0 | 147 | 2 |
| akita (gz + 7z) | 0 | 44 | 0 |

- **usenet-clari** does no escaping at all. Its 579 unescaped lines are the prose
  near-misses from J4 (`From Table 1 ...`), which `validid` correctly leaves in the body.
- **gna-clari:** 68 Message-IDs carry such lines in both collections, and **all 68 are
  byte-identical**. mboxrd would have turned their `>From` into `>>From`. Apart from the
  "`>From PayPal:`" chain letter, gna-clari has just two `>From` lines. One,
  `>From Al Pacino to Woody Allen ...`, is also in usenet-clari, which escapes nothing,
  so the `>` was in the article as posted. A mail gateway quoted it before it ever
  reached Usenet.
- **akita:** the 44 are chain-letter lines (`>From your Paypal account ...`,
  `>From time to time ...`) and quoting (`>From Marc Balinsky`): article text, not mbox
  escaping.

So stripping a `>` would corrupt real article text in every collection measured. The
importer keeps sending body lines exactly as stored, as the library's `doc.go` says. Before
importing a new collection, rerun the count. A collection that escapes shows no unescaped
`From ` body lines and `>>From` lines in proportion. If one turns up, this is where its
flag goes.

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

### B2. `ihave` return code 436 is silently discarded  *(FIXED)*
`mbox2nntp.go:1338-1339` — `case 436:` has an empty body, then
`mbox2nntp.go:1363` decrements `msgnums`. 436 means "transfer failed, try again later";
the article is dropped and not even counted as an error, so `terrcnt` stays 0 and the mbox
gets renamed `.done` (`mbox2nntp.go:824-838`).

**Fix:** treat 436 like -999 (requeue), or at minimum count it into `werrcnt` so the source
is not marked done.

### B3. Read errors report code 0, which is swallowed by `default:`  *(FIXED)*
`mbox2nntp.go:1683` and `mbox2nntp.go:1787`

`textproto.ReadCodeLine` returns `code == 0` for I/O errors (connection reset, timeout) as
opposed to a protocol `*textproto.Error`. Both call sites propagate that 0 as the return
value, so `worker` hits `default:` → `werrcnt++` → **article discarded, connection kept**.
Only a subsequent *write* failure produces -999 and a reconnect.

**Fix:** in `ihave`, distinguish: if `err` is not a `*textproto.Error`, return `-999` so the
article is requeued and the connection rebuilt.

### B4. Articles with no `Date:` header are dropped, and the repair path is dead code  *(CLOSED — measured, nothing to repair)*
`mbox2nntp.go:1641-1644` returns 437. The intended repair at `mbox2nntp.go:1635` is
unreachable: `try_fix_date` is declared `false` at line 1443 and never assigned, and
`nntp_posting_date` (line 1449) is never populated — nothing ever reads
`NNTP-Posting-Date` / `X-Trace`. Same for `has_hdr_from`, whose only use is the empty block
`if !has_hdr_from { }` at `mbox2nntp.go:1710`.

**Fix:** actually populate `nntp_posting_date` from `NNTP-Posting-Date`, `X-Original-Date`,
or the mbox `From ` separator timestamp, and synthesize `From:` when missing (as
`read_file` already does at line 573).

**Measured, then closed:** across all 5,189,059 articles that reach `rewriteHead`
(gna-clari, usenet-clari and both akita files), **not one lacks `Date:`**. All 16 rejections
are a missing Subject: 1 in gna-clari and 15 in the akita gz files. A repair path would have
nothing to repair, and inventing a date from the separator line (which in gna-clari may be
the dump time, not the posting time) is worse than a visible 437. So the dead
`try_fix_date` / `nntp_posting_date` pair is deleted and a missing `Date:` stays a 437.
The `has_hdr_from` block in `ihave` had already gone with E. Revisit this if a new
collection logs `!has_hdr_date`.

The count is below the 334,696 messages B6 parses in usenet-clari, because `iterateMbox`
drops some messages before `rewriteHead`. In usenet-clari that is 86,008 with an empty
body: 85,736 cancel control messages, which is correct, and 272 articles whose body is
missing from the archive itself (such as a spam with `Lines: 2` and no body lines).
Another 326 are yEnc binaries, 1 has no Message-ID, and 23 zip entries are not mboxes.
None of the 272 lacks `Date:` either.

### B5. Overly broad directory filter  *(FIXED)*
`mbox2nntp.go:982` — `strings.Contains(fpl, ")")` rejects a file if **any** component of
its path contains `)`. A single parent directory named e.g. `alt.foo (part 2)` silently
drops its entire subtree. Likewise `/patch` (line 983) matches anywhere in the path.

**Fix:** apply these tests to `filepath.Base(fp)`, not the full path.

**Done:** the filter is now `skipDirFile(fp)` and tests only the lower-cased base name.
`/patch` became "name starts with `patch`" (`patch01`, `Patch02`), and `/index` became
"name is `index`". Every suffix test gives the same answer on the base name as on the
path, since none contains a `/`. `TestSkipDirFile` covers a `)` directory and a `patch`
directory, which used to drop their whole subtree, and fails if the test goes back to the
full path.

---

### B6. One malformed message silently drops the rest of its file  *(FIXED)*

Found by running the J4 instrumentation over all 2,252 corpus files with the same read loop
`iterateMbox` uses, then checking the result against a plain line-level count of true
separators. The scan read the archives raw, without the importer's `dos2unix` pass:

```
usenet-clari   true articles 334,698   parsed 221,870   LOST 112,828 (33.7%)
gna-clari      true articles 4,931,412  parsed 4,931,412  lost 0
```

**How much a real run lost depends on `dos2unix`.** `proc()` runs `dos2unix -f` on every
mbox before parsing unless a `.nodos2unix` file exists in the working directory, and that
turns the CRLF blank lines into plain ones. Re-measured on the 191 affected files after
`dos2unix -f`, as `proc()` does it: **1,421 of 138,903 lost, all in 2 files** — the two
messages with no blank line at all, plus everything after each. The 112,828 figure applies
to runs with `.nodos2unix`, and to any other user of the library.

All of the raw-mode loss is in 191 of usenet-clari's 891 files. The other 677 match article for
article. Worst cases: `clari.web.xcache.html` 44,247 of 44,291 lost,
`clari.web.xcache.small` 27,551 of 27,627, `clari.web.xcache.large` 20,898 of 20,972 —
and real news groups too: `clari.news.gov.international` loses 2,101 of 2,350.

**Mechanism — two bugs that compound.**

1. *Library:* the header/body boundary is only recognised as a line that is exactly `"\n"`
   (`parseHeaders`, `m.prefetch[0] == '\n'`). A CRLF message's blank line is `"\r\n"`.
   Because `isspace` is `b < 33`, that `\r` instead passes as a header *continuation line*,
   and the body is then read as headers until a line with no colon turns up, which fails
   with `Colon not found in expected 'key: value' syntax`. In the 19 cases where body lines
   happen to contain a colon (`NIV Bible:`, `Romans 3:10`, URLs), they are filed as headers
   first.
2. *Importer:* `iterateMbox` treats any non-EOF error from `ReadMessage` as skippable and
   `continue`s. But the stream is still positioned on the line that broke. The next
   `ReadMessage` runs `parseFrom` on it, that is not a separator, it returns `io.EOF`, and
   `iterateMbox` takes that as end of file. **One bad message silently discards every
   message after it in the file.** The log shows a normal `EOF @ msgNumber=N`.

Reproduced on a four-message mbox: a malformed header in message 2 loses messages 2, 3 and 4.

Classified across the corpus, by a line-level mirror of the parser's rules which flags
exactly the same 191 files: **1,778 malformed header blocks, 1,776 of them CRLF blank lines**,
2 genuinely with no blank line (`clari.world.mideast.iraq`, `clari.world.mideast.palestine`).
The CRLF messages are mixed into otherwise-LF files, which is why only some files are hit;
the one inspected byte-by-byte is a Google Groups export with `\r\n` headers. This is the parse-level half of A3 and the
I2 note on inconsistent CR handling: not just stray bytes on the wire, but lost articles.

**Fix — applied as go-lib-mbox2nntp `28f8574`, variant B.** Measured on the raw archives:

| | parsed | lost | lost % | files stopped early |
|---|---|---|---|---|
| current | 221,870 | 112,828 | 33.71% | 174 |
| A: accept `"\r\n"` as the blank line | 333,277 | 1,421 | 0.42% | 2 |
| **B: A + resync to the next separator after a header error** | **334,696** | **2** | **0.00%** | **0** |

- **A** is three one-line changes in `mbox.go`: an `isBlankLine` helper (`"\n"` or
  `"\r\n"`) used in both of `parseHeaders`'s boundary checks and in `parseBlankLine`, and a
  guard so the continuation loop in `parseHeader` stops at a blank line. Recovers 1,776 of
  1,778.
- **B** adds a `resync()` called from `ReadMessage` when `parseHeaders` (or
  `parseBlankLine`) fails: advance to the next line `validid` accepts, or to end of input.
  Then a bad message costs exactly one
  article, never the rest of the file. B loses precisely the 2 genuinely malformed messages,
  and no file parses more articles than it has separators, so nothing is over-split.
- After `dos2unix`, B gives 138,901 of 138,903 on the affected files: the same 2.
- Tests: `TestCRLFBlankLineEndsHeaders` and `TestResyncAfterMalformedHeaders`, both of
  which fail on the previous code.

**Importer side** (mbox2nntp, the same commit as the library bump):

- `iterateMbox` stops treating a parser that gives up early as a finished file. The file is
  read through an `eofReader`. When `ReadMessage` returns `io.EOF` but the source never
  reached EOF, the importer logs `ERROR: mbox parser stopped early ... unread_bytes=N` and
  does not set `done`, so `mvmbox` leaves the file in place rather than renaming it
  `.done`. This catches any future parser bug of this kind, not just this one.
- If the same error comes back twice in a row (errors carry their line number), the reader
  is not advancing: the importer stops and treats the file the same way, instead of
  spinning.
- Skipped messages are counted, and the `Done read` line now reports `skipped=`,
  `near-misses=`, `stopped-early=` and `last-err=`. The old `err=` there was a shadowed
  variable and was always `<nil>`.
- An empty mbox is logged and marked done instead of reaching `os.Exit(1)`.
- Tests: `TestUnreadBytesSeesEarlyStop` and `TestOpenMboxFileEmpty`.

End to end, the old and new binaries were run against a fake IHAVE server with
`.nodos2unix`:

| file | old | new |
|---|---|---|
| `clari.web.xcache.html` | 44 parsed, 44 sent | 44,291 parsed, 44,286 sent |
| `clari.world.mideast.iraq` | 3,752 parsed, 978 sent | 3,901 parsed (1 skipped), 1,127 sent |
| zero-byte archive | `exit 1` | skipped, moved to `.done`, `exit 0` |

The iraq gap between parsed and sent is not a loss. 2,774 of its messages are bodiless
`cmsg cancel` control messages, and `iterateMbox` drops those on `body=0`.

Regression check: the fix changes behaviour only where a `"\r\n"` line appears inside a
header block. Counted across the whole corpus: **gna-clari has zero**, so on that collection
it is provably a no-op. In usenet-clari there are 19, in 11 files, and the
line after each one is body text (`NIV Bible:`, `Romans 3:10`, URLs, a quoted
`El lunes, 26 de octubre de 2009 13:37:09 ...`), which today is filed as headers and the
fix correctly moves to the body — anything after the blank line is body, per RFC 5322. Near-miss samples after
the fix are still all prose, so recovering those bodies uncovers no hidden separator either.

Also from the same scan: 23 usenet-clari archives are **zero bytes**. No articles are lost,
but `CreateMboxStream` fails on them and `openMboxFile` calls `os.Exit(1)`, so one empty
file aborts the whole import run.

**Already-imported data:** what is missing from the server depends on how usenet-clari was
run. In a default run (with `dos2unix`) it is the 1,421 articles in
`clari.world.mideast.iraq` and `clari.world.mideast.palestine`. With `.nodos2unix` it is all
112,828, across the 191 files. Either way, re-feed with the fixed build. INN's history
rejects the articles it already has with 435, so a re-run is safe. Note that the affected
archives were renamed `.done` despite the loss.

Related, not fixed: on the plain `.mbox` path, `dos2unix -f` rewrites the source file in
place. That modifies the archive itself, not a temporary copy.

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

### D1. Socket leak on every reconnect  *(FIXED)*
`connectBackend` (`mbox2nntp.go:198`) returns the `*textproto.Conn` but the worker never
closes it. On the -999 path (`mbox2nntp.go:1344-1358`) the worker abandons `srvtp` and
spawns a replacement — the old socket is never closed and the old `net.Conn` is
unreachable. A long run against a flaky server exhausts file descriptors.

**Fix:** return the `net.Conn` (or call `srvtp.Close()`) and close it before reconnecting
and on worker exit; send `QUIT` first.

### D2. No read/write deadlines  *(FIXED)*
`net.DialTimeout` covers only the dial. Neither `srvtp.ReadCodeLine` call has a deadline,
so a stalled peer hangs a worker forever with no diagnostic — this is the classic "import
stopped overnight" symptom.

**Fix:** keep the `net.Conn` and `SetDeadline` around each request/response.

### D3. `connectBackend` can return `(nil, nil, nil)`  *(FIXED)*
`mbox2nntp.go:212-216` — `ReadCodeLine(20)` checks the first *two* digits, so it accepts
200–209. For a greeting in that range other than 200 or 201 (e.g. `205 closing
connection`), it succeeds with `err == nil`, the guard fires, and the function returns
three nils. (An earlier version of this note said "e.g. 250": a 250 greeting already
failed `ReadCodeLine` with an error, so it was never affected.) The worker sees `cerr == nil`, breaks out of `loop_CB`, and dereferences a nil
`*textproto.Conn` in `ihave`.

**Fix:** synthesize an error in that branch.

### B2, B3, D1–D3: how they were fixed

Applied together, because they share one fix: owning the connection.

- **D2:** `connectBackend` wraps the `net.Conn` in a `deadlineConn` that sets a fresh
  `NNTP_TIMEOUT` (2 min) deadline before every `Read` and `Write`. `ihave`'s signature is
  unchanged.
- **B3:** both `ReadCodeLine` calls in `ihave` go through `responseCode`. A
  `*textproto.Error` is the server answering, so its code is kept (435/436/437). Anything
  else (EOF, reset, timeout, a garbled line) returns `-999`.
- **B2:** in `worker`, 436 and `-999` requeue the article with linear backoff, up to
  `MAX_TRIES` (10). After that it counts into `werrcnt`, so the file is not marked done.
  `msgnums` is decremented only once an article is finished.
- **D1:** on `-999` the worker sends `QUIT` and closes the old connection before handing
  over to a fresh worker, and it does the same on normal exit (`closeBackend`).
- **D3:** the synthesized error, as above.
- **Also closed:** a worker that gave up connecting (after 3,600 retries) left its articles
  queued, but `iterateMbox` still saw every worker finished with `terrcnt == 0` and marked
  the file done. Now a non-zero `msgnums` at the end blocks `done`.

Tests run against a scripted fake server:

- `TestIhaveResponseCodes` covers a close after IHAVE, a close after the article, silence,
  a garbled reply, and 435/436/437.
- `TestConnectBackendUnexpectedGreeting` covers 205, 202, 250 and 400.
- `TestWorkerSurvivesASessionGoingWrong`: 436, then a drop, then silence, then accept. The
  article must arrive once, the silent connection must be closed by the client, and there
  must be no errors.
- `TestWorkerGivesUpAfterMaxTries` checks that a permanent 436 ends as one counted error.

Each fix was reverted in turn: B2, B3, D1, D2 and D3 are each caught by at least one of
these tests.

End to end on the archive.org file `0.akita-inu.(1825).mbox.7z` (1,545 articles in a clean
run), against a server that misbehaves on purpose:

| scenario | HEAD before the fix | fixed |
|---|---|---|
| 436 every 37th IHAVE, drop after every 101st article | 1,430 received; 40 lost silently to 436, 75 counted as errors; not done | 1,545 received, identical set; done |
| one silent connection | hung until killed at 360 s; not done | timed out at 2 min, requeued; 1,545 in 124 s; done |

The clean gz run (`0.akita-inu.20140617.mbox.gz`) is unchanged: 1,465 sent, 2 refused locally.

---

## E. Header-processing bugs in `ihave`

- *(FIXED)* **`mbox2nntp.go:1465`** — `if nextline < len(head)-1` is off by one; should be
  `nextline < len(head)`. The last header line's successor is never inspected, so
  `spaced_nextline` is wrong at `i == len(head)-2` and trailing continuation lines of a
  dropped `Xref:`/`From ` header leak into `newhead`.
  The identical bug is copy-pasted into `nntp-overview/overview.go:2649`.
- *(FIXED)* **`mbox2nntp.go:1473`** — `if prevline > 0` should be `>= 0`; `head[0]` is never used as
  a previous line.
- *(FIXED)* **`mbox2nntp.go:1543`** — `strings.ToLower(header_key_L)` lowercases an
  already-lowercased value. Harmless, but delete it.
- *(FIXED)* **`mbox2nntp.go:1514`** — `len(header_dat) <= 1` drops any header whose value is a single
  character (e.g. `Lines: 7` is fine, but `X-No-Archive:1` style single-char values are
  dropped).
- *(FIXED in mbox2nntp)* **`overview.Clean_Headline`** (`nntp-overview/overview.go:1047`) does
  `strings.Replace(line, "  ", " ", -1)` — a single non-overlapping pass, so `a····b`
  becomes `a··b` rather than `a·b`. It is not idempotent and it rewrites header *content*
  (Subjects, `X-Trace`, base64 continuations). Consider limiting the collapse to leading
  whitespace of continuation lines.
- *(FIXED)* **`mbox2nntp.go:1712`** — `_, err = io.WriteString(CliWriter, CRLF)` ignores `err`,
  unlike every other write in the function.


**How E was fixed.** The header loop moved out of `ihave` into
`rewriteHead(msgid, head)`, so it can be tested and measured without a connection. Then:

- E1 is now `nextline < len(head)` and E2 `prevline >= 0`.
- E4 drops a header only when its value is empty after trimming.
- The redundant `ToLower` is gone, and the head/body separator write is checked.
- E5: `cleanHeadline` replaces `overview.Clean_Headline` in `rewriteHead` and in
  `read_file`. It still turns NUL, backspace, tab and newline into spaces, but no longer
  collapses double spaces. `nntp-overview` itself is unchanged, because other programs use
  it, and its copy of the E1 off-by-one (`overview.go:2649`) is still there.
- Also fixed: a Subject whose text is all on its continuation line (`Subject:` followed by
  ` text`) got 437. `has_subj` now counts the continuation.

`TestRewriteHead` has a case for each fix, and reverting any one of them fails it.

**Measured on the whole corpus** (5,266,834 articles: usenet-clari, gna-clari and both
akita files) by running the old and fixed `rewriteHead` side by side:

| fix | articles whose header changes | 437 ↔ accepted |
|---|---|---|
| E1 | 4,051,260 | 0 |
| E5 | 68,202 (double spaces now kept) | 0 |
| E2, E4 | 0 | 0 |

**E1 is the largest data-corruption finding in this review.** In 4,051,257 of those
articles, the dropped header with a folded tail is `Xref:`. It is the second-to-last header
and its tail is the last line, so the old code kept the tail. A continuation line belongs
to the header before it, so on the wire the other server's group list was appended to
whatever header preceded `Xref:`. That header was:

| header | articles |
|---|---|
| `X-HTML2` | 1,225,333 |
| `Bytes` | 949,484 |
| `Approved` | 885,336 |
| `X-Original-Bytes` | 348,364 |
| `Path` | 345,779 |
| `Lines` | 134,473 |
| `Priority` | 87,355 |
| `Content-Type` | 42,712 |

For example, `Approved: clarinews@clari.net` was followed by
` clari.apbl.biz.headlines:276 clari.apbl.briefs:174`. By collection: gna-clari
4,051,049 of 4,928,790 (82%); `usenet-0.akita-inu` 208; usenet-clari and the akita `.7z`
none. The other 3 changes keep a folded `Note:` header that was dropped before.

**Already-imported data:** every gna-clari article fed through mbox2nntp went out like this.
INN either stored it with the extra text in that header, or rejected it with 437. A 437 is
counted as a rejection, not an error, so the file was still marked `.done`. Which one
happened is visible on the server, by looking at the headers of one imported gna-clari
article or at the 437 count in the logs. A re-feed does not repair stored articles: INN
answers 435 for message-ids it already has. Rejected ones would go through now.
---

## F. Path / cleanup bugs

- *(FIXED)* **`mbox2nntp.go:827`** — `utils.FileExists(tmpdir + "/" + mboxfile)` where `mboxfile` is
  the value returned by `proc`, which already went through `make_mboxfilefiletrim` and is
  tmpdir-prefixed. The result is `tmpdir/tmpdir/name`, so **when `TMPDIR != "." the
  decompressed mbox is never deleted** — this is the "temp dir fills up" failure.
- *(FIXED)* **`mbox2nntp.go:925`, `940`, `954`** — `strings.TrimSuffix(mboxfile, ".zip")` keeps the
  source directory component, and `make_mboxfilefiletrim` then prepends `tmpdir`, producing
  `tmpdir//abs/path/to/foo.mbox`. Use `filepath.Base` before joining, and use
  `filepath.Join`. `gunzip` (line 315) additionally uses
  `strings.Replace(mboxfile, ".gz", "", -1)`, which strips `.gz` anywhere in the path.
- **`convert()` / `convmv`** *(FIXED — removed)* — `convmv` re-encodes file **names**, never
  content. Run locally (convmv 2.06) exactly as `convert()` called it, on an mbox with
  Latin-1 text: checksum unchanged, `converted 0 files`. On a Latin-1 *name* it did rename
  the file, after which `openMboxFile` opened the old name and hit `os.Exit(1)`. So it was
  a no-op on the archive.org names at best, and an abort at worst. Content is deliberately
  not transcoded in its place: articles go out byte for byte and each declares its own
  charset in `Content-Type`, which a UTF-8 conversion would make false.
- *(FIXED)* **`exec.Command("/bin/rm", mboxfile)`** (line 334), `/bin/mv` (line 404), and the
  unzip/gunzip calls pass filenames without a `--` separator. A file named `-rf` or
  `--help` is interpreted as a flag. Add `--`, or replace `rm`/`mv` with `os.Remove` /
  `os.Rename`.
- *(FIXED)* **`mbox2nntp.go:966-971`** — `/bin/find.sh` output is buffered whole via `c.Output()` and
  split on `"\n"`. For a multi-million-file spool this is hundreds of MB resident before
  any work starts, and filenames containing newlines break the split. Replace with
  `filepath.WalkDir`, which also removes the external script dependency.
- **7z extraction is never cleaned up** *(FIXED)*. `proc()` returned the extracted file's
  name only for `is_zip || is_gzp`. For a `.mbox.7z` it returned the archive name instead,
  so `main`'s cleanup looked for the wrong file, and the extracted mbox stayed behind
  after the archive was moved to `.done`. Found on
  `FULL-USENET-BACKUP-2020-Oct-0.akita-inu.1825.mbox.7z`: 53 MB left behind per archive.
- **`mboxhasZ` moves `.zip` for a `.7z`** *(FIXED)*. Copy-paste: `if has_zip7 {
  mvmbox(zip) }`. For a plain `.mbox` with its `.7z` beside it, `mv` fails on the missing
  `.zip` and `mvmbox` exits 1 at the end of an otherwise complete run.


**How the rest of F was fixed:**

- **Extraction paths.** Everything unpacks into `tmpdir` (default `.`) under the archive's
  base name: `extractedPath(archive, ext)` = `filepath.Join(tmpdir,
  TrimSuffix(Base(archive), ext))`. `proc` returns that path, and `main` removes exactly
  that file. Two behaviour changes come with this:
  - A gz archive used to unpack beside its source when tmpdir was `.`, while zip and 7z
    unpacked into the working directory. Now all three go to tmpdir.
  - `unzip -n` (never overwrite) is now `-o`. A file left by an interrupted run may be
    truncated, and `-n` would have imported it. 7z already used `-y`.
- **Cleanup never touches a source.** The old `tmpdir+"/"+name` check also matched a
  plain `.bag` source, and deleted it whenever a run ended with no NNTP errors but was not
  marked done. Cleanup now runs only for zip, gz and 7z.
- **gunzip is in-process** (`compress/gzip`), which handles the usenet-* multi-member
  archives. The tmpdir branch called `/scripts/gunzip.sh`, which is in neither the repo nor
  on the test machine.
- **`rm`/`mv` are now `os.Remove`/`os.Rename`.** Every file name still passed to an
  external tool (unzip, 7z, dos2unix) goes through `argPath`, which prefixes `./` to names
  starting with `-`. `unrar` was never called and is deleted.
- **`find.sh` is replaced by `walkFiles`.** `find.sh` was `find "$1" -mindepth 1 -type f |
  sort -V`, so the order matters: numeric, article 2 before 10. `walkFiles` reads and sorts
  one directory at a time with a port of gnulib's `filevercmp`/`verrevcmp`, and walks
  depth-first. That reproduces `sort -V` over the full paths without holding them all.
  Like `find`, it does not follow symlinks and skips unreadable directories.
  `TestWalkFilesMatchesFindSh` (hand-picked edge cases) and
  `TestWalkFilesMatchesFindShGenerated` (20 seeded random trees) compare it against the
  real pipeline byte for byte. `read_file` now counts what it queues in `msgnums`,
  replacing the old preset to the total file count.

End to end, with a separate tmpdir and the archive in another directory:

| input | HEAD before the fix | fixed |
|---|---|---|
| `0.akita-inu.20140617.mbox.gz` | exit 1: `/scripts/gunzip.sh` missing | 1,465 sent, tmpdir empty, `.done` |
| `clari.world.mideast.palestine.mbox.zip` | exit 1: wrong path, extracted file left in tmpdir | 1,414 sent, tmpdir empty, `.done` |
| `0.akita-inu.(1825).mbox.7z` | exit 1: wrong path, extracted file left in tmpdir | 1,545 sent, tmpdir empty, `.done` |
| `-dash.mbox.zip` | exit 1: unzip took the name as options | 1,414 sent, `.done` |
| the gz turned into a spool dir, one worker | cannot run (`/bin/find.sh` missing) | 1,467 sent, in numeric file order |

---

## G. Resource / throughput

- *(FIXED)* **`WORKER_CHAN_SIZE = 1000000`** (`mbox2nntp.go:38`) with two channels of that size
  (`worker_channel`, `requeue_channel`). Workers do not start until `process` is set, which
  for the mbox path requires 10 000 queued items (`mbox2nntp.go:1200`) and for the directory
  path 100 000 (`mbox2nntp.go:630`). Until then the feeder fills the queue alone, holding up
  to 1 M fully-materialized `ArticleItem`s (two `[]string`s each) in RAM. At a 4 KB average
  that is ~4 GB. `requeue_channel` is allocated and **never used** except in a log line.
  **Fix:** reduce the queue to a few thousand, start workers immediately (delete the
  `process` gate), and delete `requeue_channel`.
- *(FIXED)* **`bufio.NewWriter(conn)`** (`mbox2nntp.go:209`) uses the 4 KB default, so a typical
  article causes several small writes. A 64 KB writer measurably improves TB-scale throughput.
- *(FIXED)* **`bufio.NewReaderSize(fh, 1024*1024*1024)`** (`mbox2nntp.go:1055`) allocates a **1 GiB**
  read buffer per mbox file. 1–4 MB is ample.
- *(FIXED as J3)* **`go-lib-mbox2nntp/message.go`** runs `validid.MatchString` (a `regexp2` backtracking
  match) against **every body line** to detect the next `From ` separator. This is the
  hottest path in the whole importer. Replace with a `bytes.HasPrefix(prefetch, []byte("From "))`
  pre-check before falling back to the regex.
- *(FIXED)* **`doneTicker`** (`mbox2nntp.go:1309`, `1323`) is replaced with a fresh `time.NewTimer`
  and never `Stop()`ed. Use a single `time.Ticker`.
- *(NOT DONE — not worth it)* **`GlobalCounter`** serializes every worker's per-article
  increment on one mutex. Switch the hot counters (`msgnums`, `sent`) to `atomic.Int64`.
  On reflection: that is one uncontended lock (~20 ns) per article, next to an IHAVE round
  trip per article, which takes milliseconds. There is nothing to gain.

**Applied:**

- The queue is now 4,096, workers start immediately (the `process` gate is gone), and
  `requeue_channel` is deleted.
- The NNTP writer is 64 KB and the mbox read buffer is 1 MB.
- `doneTicker` is a single `time.Ticker`.

Measured on `gna-clari/clari.web.news.briefs.mbox.gz` (73,332 articles), 4 workers, fake
IHAVE server:

| | peak RSS | wall | articles |
|---|---|---|---|
| before | 2,646 MB | 43.5 s | 73,332 |
| after | 121 MB | 43.9 s | 73,332, identical set |

Wall time is bound by the fake server, so this does not show what the 64 KB writer does
against a real INN.

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

  **RESOLVED — done, because the tree blocked Windows and macOS.** `nntp-mmap` does not
  build on Windows and `go-utils` (`syscall.Fallocate`) not on macOS. Only
  `ParseHeaderKeys` (and `GetMessageID` under it) was actually called; the other names
  appear in comments only. Both were copied into `headers.go` unchanged, and compared with
  the original on every header block in the corpus (10,555,466 comparisons, lax and
  strict, none different). `go.mod` now needs `regexp2` and `go-lib-mbox2nntp`. One bug came along with the copy, now fixed:
  `GetMessageID`'s strict mode tested the literal `"msgid"` for an `@`, so with
  `-lax-msgid=false` no Message-ID was accepted (0/0 on `0.akita-inu.20140617.mbox.gz`,
  now 1,465/1,467 as in lax mode).

- **RESOLVED — the committed `mbox2nntp` binary** is no longer tracked, and `.gitignore`
  names `/mbox2nntp`.
- **RESOLVED — `go vet ./...` is clean.** It had reported three `log.Printf` defects,
  all now fixed: a missing verb at `:687`, a missing argument at `:1426`, and a
  non-constant format string at `:830` — `log.Printf("cleanup: rm '"+tmpdir+"/"+mboxfile+"'")`,
  where a filename containing `%` (not rare in archive.org spools) was read as a verb.
  These also mattered because they failed the `go test` build, so no test could run at all.

  **Correction to an earlier draft:** that draft also predicted a `copylocks` warning at
  `mbox2nntp.go:663-664`. Vet does *not* flag it — assigning a fresh composite literal
  whose `sync.Mutex` is at its zero value is not a lock copy. The code is fine as written.
- **RESOLVED — `gofmt -l` flags the file.** `gofmt -w` was run on `mbox2nntp.go` and
  `tsm/GlobalBools/GlobalBools.go` as a commit of its own, with no other change in it.
- **RESOLVED — argument parsing**: `mbox2nntp.go:707` uses `if len(os.Args) == 7` for `PREFIX` while
  the next block uses `>= 8`, so `PREFIX` is never set when 8 arguments are supplied. `PREFIX`
  is also never read anywhere. The usage string (line 725) omits arguments 6 and 7. Replace
  the positional chain with `flag`.

  **Done differently:** `PREFIX` is deleted and its slot (argument 6) is documented as
  unused, so existing command lines keep working. The usage message now lists every
  argument, including `.noDROPBINARY`.

  **Then replaced with `flag`** (`flags.go`): `-server`, `-workers`, `-tmpdir`,
  `-dry-run`, `-keep-binary`, `-no-dos2unix`, `-dos2unix`, `-timeout`, `-max-tries`,
  `-prehash`, `-lax-msgid`. The last five were compile-time settings; their defaults are
  unchanged. The old positional form is still parsed, with a deprecation note in the log,
  when the first two arguments are not flags. A directory input no longer needs a trailing
  `/`. Writing the README turned up two dry-run bugs, both fixed. A dry run tried to
  connect anyway, for up to 3 hours, even though `ihave` never uses the connection in
  that mode. And it renamed its input to `.done` without sending anything, so a dry run
  was enough to skip a file in every later batch run. Now a dry run needs no server and
  never marks an input done. See `README.md`.
- **RESOLVED — runs on Windows and macOS.** Beyond the dependency tree above:
  `unzip` and `7z` were run from `/usr/bin`. `unzip` is replaced by `archive/zip` (all
  891 usenet-clari zips unpack byte-identical to it). For 7z there is no reader in the
  standard library, and the module takes no third-party code (`bodgit/sevenzip` was tried
  and dropped: 11 indirect modules), so 7-Zip is still run, found by `exec.LookPath`
  (`-7z PATH`, else `7z`, `7zz`, `7za`, and on Windows its install directory). It
  extracts only `NAME.mbox`, to stdout, after listing the archive to make sure that entry
  exists. `dos2unix` ran on every mbox, rewrote a plain `.mbox` in
  place, and was checked by Unix exec bits Windows never reports; it now runs only with
  `-dos2unix PATH`. Across all 2,262 corpus inputs (556 containing CR) every article goes
  out byte-identical without it. The mbox file was never closed, so on Windows the `.done`
  rename and the tmpdir cleanup would have failed after every article had been sent; it
  is now closed first. The `find | sort -V` comparison tests skip without GNU sort, and
  `.github/workflows/test.yml` runs the tests on all three systems.
- **RESOLVED — `DROPBINARY` is not honoured on the directory path**: `mbox2nntp.go:553` drops `=ybegin`
  unconditionally, while `mbox2nntp.go:1170` respects the flag. `read_file` now checks
  `DROPBINARY` too; `TestReadFileHonoursDropBinary` covers both settings.
- **RESOLVED — dead code to delete**: `unrar` (never called), `test_first_byte` (only referenced from a
  comment), `oldGetMessageID`, `IsLetter`, `IsAlphaUpper`, `inn2AddGroup` + `addedgroups`
  (unsynchronized map, never called), `notify_chan`, `requeue_channel`, `PREFIX`,
  `regex_isAlphaNumStart`, package-level `sentcnt`/`errcnt`/`nntpdup435`/`nntp437`,
  `fix_msgid`, `has_hdr_from`, `try_fix_date`, `nntp_posting_date`, and the `test_DRY_RUN`
  block at `mbox2nntp.go:1422`. Roughly 400 lines of commented-out code can go with them.

  **Done:** all of these are gone, apart from three. `requeue_channel` already went with G.
  `has_hdr_from` turned out to be live in `read_file`, where it synthesizes `From:`. The
  `try_fix_date` / `nntp_posting_date` pair belongs to B4. The `unicode` import went with
  `IsLetter`. The commented-out code is left alone, apart from blocks that referred to
  deleted names: deleting the author's notes was not in scope.

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

### I3. Present in both, not a regression  *(FIXED)*

- **`nextLine()` discards the final line of a file that does not end in a newline.** On
  `ReadBytes('\n')` error the returned bytes are thrown away along with the data.
- **`bodyReader.Read` can split a long line.** A body line longer than the caller's buffer
  (64 KiB in `iterateMbox`) comes back as two `Read` calls; `iterateMbox` treats each as a
  separate line and appends `CRLF` to both. Rare, but it hits exactly the long
  base64/uuencoded lines that matter.

**Fixed, as a precaution:** the corpus has neither case. No body line reaches 64 KiB, and
every one of the 2,261 files ends in a newline. Both fixes are for future collections.

- Library `605980f`: `nextLine` keeps the last line on `io.EOF` and terminates it with
  `\n`. `TestFinalLineWithoutNewline` covers it. The importer picks this up when its
  `go.mod` moves to that commit, after the library is pushed.
- Importer: the body loop is now `readBody`, which joins `Read` pieces until a line ends
  in `\n`. `TestReadBodyLongLine` uses a 200 KiB line, a line that exactly fills the
  buffer, and a missing final newline; before the fix the 200 KiB line came back as 4 lines.

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

### J4. Instrument the near-misses  *(FIXED)*

`MboxStream.NearMisses()` in `go-lib-mbox2nntp` (`c09fa7f`) returns the count of body lines
that pass the `couldBeSep` gate but fail `validid`, plus the first five with line numbers.
`iterateMbox` reports the count on its per-file `Done read` line and logs the samples when
it is non-zero. Point it at one file per new archive.org collection before importing it.

Run over the whole corpus: **468 near-misses in 155 files, all in usenet-clari, and all 382
samples are prose** (`From Table 1 ...`, `From my point of view ...`,
`From 1995 I had some Polish friends ...`). None is separator-shaped, so `validid` misses
nothing in either collection — J2's conclusion now holds on all 2,252 files, not 60.
After the B6 fix recovers the lost bodies it is 614 near-misses, still all prose.

The same run is what found B6.

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

- **RESOLVED — `regex_isAlphaNumStart` (`mbox2nntp.go:70`) is never used.** Deleted with H.
- **`regex_isbaddateline` (`mbox2nntp.go:71`)** only acts when the *previous* line is a
  continuation (`mbox2nntp.go:1479-1487`). An unfolded `Thu, 1 Jan 1970 00:00:00 +0000`
  following a normal header falls through both branches, reaches the colon split at
  `mbox2nntp.go:1507`, and is emitted as a bogus header with
  `header_key = "Thu, 1 Jan 1970 00"` — which INN 437s. Note this interacts with the
  `prevline > 0` off-by-one in **E**: `spaced_prevline` is never true for `i <= 1`. It is
  also a 5-byte test (`^[A-Z][a-z][a-z], `) that does not need a backtracking engine.

  **Resolved:** it is now `isBadDateLine`, a byte test, and `TestIsBadDateLineMatchesRegex`
  checks it against the old regex on hand-picked and 20,000 random inputs. With this,
  `mbox2nntp.go` no longer imports `regexp2`. The off-by-one went with E2. The case of an
  unfolded date line after a normal header was measured rather than guessed: the harness
  checked every header line `rewriteHead` emits, in all 5,189,043 accepted articles. **None has an
  invalid field name** (empty, or containing a space or a control byte). So the bogus
  `Thu, 1 Jan 1970 00:` header does not occur in this corpus, and the branch logic is left
  as it is.

---

## Suggested execution order

1. **A1, A2, A3, B1, C1, C2** — corruption and crashes, each a few lines. Do these first
   and together; they are independent of any restructuring.  *(DONE)*
2. **B2, B3, D1, D2, D3** — NNTP session correctness.  *(DONE — via a deadline-setting
   connection wrapper rather than threading the `net.Conn` through)*
3. **F** — path and cleanup fixes; switch to `filepath.Join`/`filepath.Base`, replace
   `find.sh` with `filepath.WalkDir`, add `--` to the exec calls.  *(DONE)*
4. **H** — unblock the build (`go-utils`), then `gofmt`, `go vet` clean, dead-code
   deletion, `flag`-based arguments.  *(DONE — `flag`-based, with the old positional form
   still accepted)*
5. **I2** — repair `mbox_test.go` so the library has a working test suite again. Do this
   *before* touching `validid`: it is the reason the regex could never be changed safely.
   *(DONE)*
6. **J3** — the byte-prefix gate in front of `validid`. Isolated, measured ~76× on the
   hottest loop, and provably cannot change behaviour.  *(DONE)*
7. **J4** — near-miss instrumentation.  *(DONE — and it found B6)*
8. **B6** — CRLF blank line + resync in the library; early-stop detection, the
   stuck-loop guard and empty-file handling in the importer.  *(DONE — re-feed the
   affected files; see B6)*
9. **G, E, J5–J6, A4, B4, B5** — throughput, header handling, regex naming, and the
   optional `>From ` / missing-`Date:` repair work.  *(DONE — A4 and B4 closed by
   measurement, no change needed)*

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
