# mbox2nntp

Feeds Usenet archives in mbox form to an NNTP server, one article at a time, with
`IHAVE`. It was written to load the archive.org Usenet collections (`gna-clari`,
`usenet-clari`, the `usenet-*` and `FULL-USENET-BACKUP-*` items) into INN2. It also reads
the archives of the [Usenet Archive Toolkit](https://github.com/wolfpld/usenetarchive)
(see [UAT archives](#uat-archives)), and `ncexport` turns the NewsCache spools of the
`textusenet-backup` items into mbox files (see [NewsCache spools](#newscache-spools-textusenet-backup)).
`iafetch` downloads the archive.org items in import order and remembers what it got (see
[Downloading from archive.org](#downloading-from-archiveorg)).

Each run takes one input: an mbox, a compressed mbox, a UAT archive, or a directory of
article files.

## Download

[Releases](https://github.com/go-while/mbox2nntp/releases) have ready-built `mbox2nntp`,
`uatexport`, `ncexport` and `iafetch` for Linux, Windows, macOS (amd64, arm64) and FreeBSD (amd64), with a SHA-256
checksum file. `mbox2nntp -version` prints which one you have. A release is built by
`.github/workflows/release.yml` when a `v*` tag is pushed, or by running that workflow from
the Actions tab with a version to tag.

## Build

Runs on Linux, Windows and macOS, as one binary. `.gz` and `.zip` archives are unpacked
in-process. `.7z` archives need the 7-Zip program, and UAT archives the `zstd` program,
because Go's standard library reads neither format (see [7z archives](#7z-archives) and
[UAT archives](#uat-archives)).

```
go build -o mbox2nntp .                                 # for this machine
GOOS=windows GOARCH=amd64 go build -o mbox2nntp.exe .   # Windows
GOOS=darwin  GOARCH=arm64 go build -o mbox2nntp-mac .   # macOS, Apple silicon
GOOS=darwin  GOARCH=amd64 go build -o mbox2nntp-mac .   # macOS, Intel
go build -o uatexport ./cmd/uatexport                   # the UAT exporter
go build -o ncexport ./cmd/ncexport                     # the NewsCache exporter
go -C cmd/iafetch build -o ../../iafetch .             # the archive.org downloader
```

`./build_All.sh` builds all four at once, stamped with the `git describe` version; it
takes an output directory, and honours `GOOS`/`GOARCH`
(`GOOS=windows GOARCH=amd64 ./build_All.sh` gives the `.exe` files).

Needs Go 1.27.3 or later. Apart from the Go standard library, mbox2nntp's only dependency is
the mbox parser [`go-lib-mbox2nntp`](https://github.com/go-while/go-lib-mbox2nntp), a fork of
`github.com/sam-falvo/mbox`. `iafetch` uses [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite),
a SQLite written in pure Go, so it needs no C compiler and no SQLite library. It brings
third-party modules with it, so iafetch is a Go module of its own (`cmd/iafetch/go.mod`):
mbox2nntp and the other commands do not depend on them. Build, vet and test it from its
directory, or with `go -C cmd/iafetch ...`.

## Usage

```
mbox2nntp [flags] -server HOST:PORT INPUT
mbox2nntp [flags] -dry-run INPUT
```

| INPUT | meaning |
|---|---|
| `NAME.mbox`, `NAME.bag` | an mbox file |
| `NAME.txt` | an mbox file, if its first line begins with `From `, such as the Google Groups exports in the `comp_sys_atari` items on archive.org; any other `.txt` is refused |
| `NAME.mbox.gz` | gzip-compressed mbox, one stream or many concatenated members |
| `NAME.mbox.zip` | zip archive holding `NAME.mbox` |
| `NAME.mbox.7z` | 7z archive holding `NAME.mbox` |
| `NAME.usenet` | a [UAT archive](#uat-archives), one newsgroup; also `NAME.usenet.xz` and `NAME.usenet.7z` |
| `DIR` | a directory tree with one article per file, such as an INN spool; plain articles, or wire format (dot-stuffed, ending in a `.` line) |

| flag | default | |
|---|---|---|
| `-server HOST:PORT` | (required) | the NNTP server; not needed with `-dry-run` |
| `-workers N` | `1` | parallel connections |
| `-tmpdir DIR` | `.` | where `.gz`, `.zip` and `.7z` inputs are unpacked |
| `-7z PATH` | `7z`, `7zz` or `7za` | the 7-Zip program for `.7z` inputs |
| `-zstd PATH` | `zstd` | the zstd program for UAT archives |
| `-xz PATH` | `xz`, else 7-Zip | the xz program for `.usenet.xz` inputs |
| `-dry-run` | off | parse and rewrite articles, but connect to no server and send nothing; the input is never marked done |
| `-keep-binary` | off | send yEnc (`=ybegin`) articles instead of dropping them |
| `-dos2unix PATH` | none | run this `dos2unix` on each mbox first, as older versions always did; not needed, see below |
| `-no-dos2unix` | off | do not run `-dos2unix`; a `.nodos2unix` file in the working directory does the same. Kept for old scripts |
| `-timeout D` | `2m` | limit on each read or write on a server connection |
| `-max-tries N` | `10` | tries per article after a `436` or a lost connection |
| `-prehash` | on | send `IHAVE <msgid> <sha256-of-msgid>`; `-prehash=false` sends plain `IHAVE <msgid>` |
| `-lax-msgid` | on | accept loosely formed Message-IDs |

Examples:

```
mbox2nntp -server localhost:119 -workers 4 -tmpdir /var/tmp/m2n clari.world.top.mbox.gz
mbox2nntp -dry-run 0.akita-inu.20140617.mbox.gz
mbox2nntp -server localhost:119 /srv/oldspool/comp/lang/
mbox2nntp.exe -server news.example:119 -tmpdir D:\tmp C:\archives\clari.world.top.mbox.zip
mbox2nntp.exe -server news.example:119 C:\spool\comp\lang
```

The input is only read, never changed, apart from the `.done` rename below.

**dos2unix:** older versions ran `/usr/bin/dos2unix -f` on every mbox, which rewrote a
plain `NAME.mbox` or `NAME.bag` in place. The parser handles CRLF line endings itself:
across the whole archive.org corpus (gna-clari, usenet-clari and akita-inu) every article
comes out byte-identical with and without it, so it is no longer run. If you want it
anyway, `-dos2unix` takes its path or name: `dos2unix` from a Linux distribution or
Homebrew (`brew install dos2unix`), or `dos2unix.exe` on Windows, which Git for Windows
ships in `C:\Program Files\Git\usr\bin`. It then rewrites a plain mbox in place, as
before.

### 7z archives

A `.7z` input is unpacked by running 7-Zip: the `-7z` program, or else the first of `7z`,
`7zz` and `7za` in `PATH`, and on Windows also `C:\Program Files\7-Zip\7z.exe`. Only the
entry `NAME.mbox` is extracted. 7-Zip checks the CRC, and a damaged archive is a fatal
error.

- **Linux:** the `7zip` package (Debian, Ubuntu), or `p7zip`.
- **macOS:** `brew install sevenzip`, which installs `7zz`.
- **Windows:** the 7-Zip installer from [7-zip.org](https://www.7-zip.org/).

`.gz`, `.zip`, plain mbox and directory inputs need no other program.

### UAT archives

The [Usenet Archive Toolkit](https://github.com/wolfpld/usenetarchive) (UAT) keeps one
newsgroup per `GROUP.usenet` file. Two collections are published:

- [usenet.nereid.pl](https://usenet.nereid.pl/): 333 Polish groups as `.usenet.xz`, 41 GB;
- archive.org [`usenet-uat-pc-games`](https://archive.org/details/usenet-uat-pc-games):
  39 groups, games and `alt.folklore.computers`, as `.usenet.7z`, 6.5 GB.

Each message in the archive is compressed with zstd and a dictionary. mbox2nntp runs the
`zstd` program once over all of them (`-zstd PATH`, else `zstd` in `PATH`):

- **Linux:** the `zstd` package. **macOS:** `brew install zstd`.
- **Windows:** `zstd.exe` from the [zstd releases](https://github.com/facebook/zstd/releases),
  or `choco install zstandard`.

`.usenet.xz` also needs `xz`, or 7-Zip; `.usenet.7z` needs 7-Zip.

**The articles are not the originals.** The toolkit rewrote every message when it built the
archive: it decoded the headers to UTF-8, kept only the text part of the body (attachments
and HTML are gone) and converted it to UTF-8, and dropped `Content-Type` and
`Content-Transfer-Encoding`. It also removed spam, duplicates, and articles not posted to
the group. mbox2nntp makes them valid articles again:

- `Content-Type: text/plain; charset=UTF-8` and `Content-Transfer-Encoding: 8bit` (`7bit`
  for a plain ASCII body), and `MIME-Version: 1.0` where it is missing;
- non-ASCII header text goes back into RFC 2047 encoded-words: whole runs of words in
  `Subject:`, `Organization:` and other free-text fields, the display names in `From:`,
  `Reply-To:` and other address fields;
- `Message-ID`, `References`, `Newsgroups`, `Path`, `Date` and the other structured fields
  are never changed. Header fields in plain ASCII are kept byte for byte.

The `Done read` line counts the re-encoded fields, and names any field left with non-ASCII
in it.

**Feed UAT archives last.** Their articles keep their original Message-IDs. A server that
already holds the original from another archive answers `435`, and keeps the original; a
server that gets the UAT copy first keeps the rewritten one.

To write a UAT archive out as an mbox file or as one file per article, for use elsewhere,
there is `uatexport` (`go build ./cmd/uatexport`):

```
uatexport alt.pl.fan.gophi.usenet.xz                     # writes alt.pl.fan.gophi.mbox
uatexport -gzip alt.pl.fan.gophi.usenet.xz               # writes alt.pl.fan.gophi.mbox.gz
uatexport -gzip=9 -o /srv/mbox/gophi.mbox.gz alt.pl.fan.gophi.usenet.xz
uatexport -format dir -o spool/ rec.games.rogue.usenet.7z
uatexport -raw ...                                       # as the toolkit stores them
```

`-gzip` compresses the mbox in-process (compress/gzip) at level 6; `-gzip=N` sets the
level, 1 to 9. mbox2nntp reads the `.mbox.gz` as it reads any other. uatexport never
replaces an existing output: it writes to a hidden temporary name next to it
(`.NAME.*.tmp`) and renames it when complete, so a stopped run leaves no output that
looks finished. Interrupted with Ctrl-C, it removes its temporary files.

**Storage.** Measured on 7 groups (3 from nereid, 4 from archive.org, 56 MB of archives),
per byte of `.usenet.xz`/`.7z`: the unpacked `.usenet` takes 1.9, the `.mbox` 2.4,
the `.mbox.gz` 0.56 (level 1: 0.64, level 9: 0.54), and `.mbox.xz` 0.34. A `.mbox.gz`
is smaller than the archive because the archive also holds the toolkit's search
indexes. `-gzip` makes an export about 1.7 times slower than a plain one (`-gzip=9`: 2.5
times). Reading the `.mbox.gz` with mbox2nntp is as fast as reading the archive.

To feed a server you need no mbox at all: give mbox2nntp the archive. Keep the
`.usenet.xz`/`.7z` files: they are the toolkit's own data, and an exported mbox is not the
same, since mbox2nntp does not undo the `>From ` quoting of mboxrd.

To download a whole collection and export every group, there is
[`contrib/uat-fetch-export.sh`](contrib/uat-fetch-export.sh) (bash and curl; `ia` or `jq`
for archive.org). It fetches the nereid directory listing and the archive.org metadata
(one request each), downloads the archives one at a time, with a pause between files
(`-p`, 2 s), an optional bandwidth cap (`-r 2M`, curl `--limit-rate`) and retries,
resumes interrupted downloads, and checks them (archive.org by md5, nereid by size and
`xz -t`). Then it runs uatexport on each, to `GROUP.mbox.gz` by default. Downloads and
exports that are complete are skipped, so it can be stopped and run again.

```
go build -o uatexport ./cmd/uatexport
contrib/uat-fetch-export.sh -n                          # what it would do, with sizes
contrib/uat-fetch-export.sh -s archive --sort size -l 3 # the 3 smallest archive.org groups
contrib/uat-fetch-export.sh -s nereid -m '^pl\.comp\.' -d /srv/uat -o /srv/mbox -r 5M
contrib/uat-fetch-export.sh --download-only             # only the archives
contrib/uat-fetch-export.sh -h                          # all options
```

For both collections (372 groups, 45.8 GB of archives) expect about 25 GB of
`.mbox.gz` (115 GB as plain `.mbox`), and room to unpack the largest archive,
`pl.soc.polityka` (1.9 GB, about 3.5 GB unpacked). `--delete-archive` removes each archive
once its group is exported.

Other programs can read UAT archives with the package `github.com/go-while/mbox2nntp/uat`
(standard library only, plus the zstd program); `uat/uattest` writes small archives for
their tests.

The mbox is mboxrd (`From ` body lines get a `>`), with a `From nobody DATE` line that
mbox2nntp's parser reads. The articles get the same changes as above. To send a UAT archive
to a server, give mbox2nntp the `.usenet` file, not the exported mbox: it does not un-escape
`>From ` lines. An exported directory works as a `DIR` input.

### NewsCache spools (textusenet-backup)

The archive.org `textusenet-backup_*` items (30 items, 977 GB) hold `.tar.gz` files, one per
hierarchy (`97comp.tar.gz`, `rainbow.tar.gz`, ...), each the spool of a
[NewsCache](https://github.com/hstraub/NewsCache) server: one directory per newsgroup
(`rainbow/test` for `rainbow.test`) with a `.db` file and `.artN` files. `ncexport` writes
such a spool out as one mbox file:

```
go build -o ncexport ./cmd/ncexport
ncexport -gzip rainbow.tar.gz                           # writes rainbow.mbox.gz
curl -sL https://archive.org/download/textusenet-backup_9/97comp.tar.gz |
    ncexport -gzip -o 97comp.mbox.gz -                  # straight from the download
ncexport -o comp.mbox /var/spool/newscache/comp         # an unpacked spool directory
```

A tarball is read as a stream in one pass: nothing is unpacked to disk, and it can come
from a pipe. The output appears under its name only when complete, as with uatexport, and
an existing one is never replaced.

- **Where the articles are.** NewsCache stores an article of 15 KB or less inside the `.db`
  and a bigger one in `.artN`. The server behind the backup ran a modified NewsCache that
  put every article in an `.artN` file. ncexport reads both.
- **What is changed.** NewsCache keeps an article as the server sent it: CRLF line ends and
  leading dots doubled. ncexport turns line ends into LF and doubled dots back into single
  ones. Nothing else changes. A few damaged yEnc articles also have lines with a single
  leading dot, which cannot come from doubling: those lines are kept as they are, and the
  summary counts the articles.
- **Crossposts.** An article crossposted to several groups is in each group's directory.
  ncexport writes it once, the first time its Message-ID turns up; `-dedup=false` writes
  every copy.
- **Order.** A tarball's articles are written in the tarball's order, a directory's group by
  group and by article number. The mbox is mboxrd, with `From nobody DATE` taken from the
  Date header, as uatexport writes it.
- **The summary.** It counts the groups, the `.art` files and the articles inside `.db`
  files. It also counts the crossposted copies skipped, and the articles the `.db` lists
  but the spool does not have (overview only, never fetched by the cache).

Checked on `97comp.tar.gz` (757 articles) and `rainbow.tar.gz` (518 articles from 616
files in 34 groups). Every article mbox2nntp's parser reads back from the mbox equals the
stored one, with line ends and dots undone. Then give the `.mbox.gz` to mbox2nntp. Expect
it to skip articles with an empty body (24 in `97comp`), which some spam has, and yEnc
binaries unless `-keep-binary` is given.

Other programs can read NewsCache spools with the package
`github.com/go-while/mbox2nntp/newscache` (standard library only); `newscache/newscachetest`
builds `.db` files for their tests.

### Downloading from archive.org

`iafetch` downloads the items of [`archive.org/import-order.csv`](archive.org/import-order.csv)
(or any CSV with an `identifier` column) in list order, several files at a time, to
`DIR/ITEM/FILE`. A SQLite database records every file it downloaded and the state of
every item. **An item recorded as done is skipped from then on without asking archive.org,
whether or not its files are still there**, so imported files can be moved or deleted and
the next run goes on with the items after them.

```
go -C cmd/iafetch build -o ../../iafetch .
iafetch -n archive.org/import-order.csv                 # dry run: what would come, how big
iafetch -tier A archive.org/import-order.csv            # the early era first
iafetch -j 8 -o /data/ia -db /data/iafetch.sqlite archive.org/import-order.csv
iafetch -status -db /data/iafetch.sqlite                # what the database records
```

- **What is downloaded.** The files of the upload, by the item's archive.org metadata: not
  the files archive.org makes itself (`_meta.xml`, `_files.xml`, `_meta.sqlite`,
  `.torrent`), nor the replaced versions it keeps under `history/` (most
  `FULL-USENET-BACKUP-*` items have one, as big as the current file). `-include` and `-exclude` choose among them by regexp, `-tier`, `-list`,
  `-match`, `-from`, `-to` and `-limit` choose the items. Tier `X` (not Usenet) is left out
  unless `-tier` names it.
- **Order and parallelism.** Items are taken by the list's `order` column; their files go to
  `-j` workers (default 4), so a big item's files download side by side and the next item
  starts as soon as a worker is free.
- **Checking.** A file is written to `FILE.part` and renamed to `FILE` only when its size
  and md5 match the metadata. A stopped run (Ctrl-C) leaves `.part` files, which the next
  run resumes with a range request. Failed requests, including the 500s archive.org
  sometimes answers, are retried with a pause of 5 s doubling up to 5m20s (`-retries`). A
  file that already exists under its name is checked by md5 and kept if right; a different
  file there is an error and is never replaced.
- **The database** (`-db`, default `./iafetch.sqlite`) has a table `items` (identifier,
  order, tier, list, status, files, bytes, note) and a table `files` (identifier, name,
  size, md5, path, time). An item is `done`, `incomplete` (a file failed: the next run gets
  what is missing), `dark` (taken down on archive.org), `missing` (no such item) or `empty`
  (no files to download). `dark`, `missing` and `empty` items are skipped until
  `-retry-unavailable`. To fetch an item again, delete its rows:
  `sqlite3 iafetch.sqlite "DELETE FROM items WHERE identifier='X'; DELETE FROM files WHERE identifier='X'"`.
  Keep the database outside the download directory if files are moved from there in bulk.

Some items in the list are taken down on archive.org and not in the list at all, such as
`textusenet-backup_a`, `_c`, `_e`, `_g`, `_h`, `_n` and `_u`; `textusenet-backup_0` and
`_f` are listed but have no files and are recorded as `empty`.

### The old positional form

The arguments used before the flags existed still work, with a deprecation note in the
log:

```
mbox2nntp INPUT HOST:PORT [WORKERS [TMPDIR [DRYRUN [- [NODROPBINARY_FILE]]]]]
```

The fifth argument is the literal word `DRYRUN` to dry-run, and anything else to send.
The sixth was never read. If the seventh names an existing file, yEnc articles are sent;
that is the old `.noDROPBINARY` switch, now `-keep-binary`.

## What happens to an input

When every article has been answered and none failed, the input is renamed to
`INPUT.done`, so a batch script can rerun over a directory and skip finished files.

- For a plain `NAME.mbox` with an archive beside it (`NAME.mbox.gz`, `.zip` or `.7z`), the
  archive is renamed instead.
- A UAT archive (`.usenet`, `.usenet.xz`, `.usenet.7z`) is renamed like an mbox.
- The copy unpacked into `-tmpdir` is removed after the rename.
- A directory input is never renamed.

An input is **not** marked done when any article ended in an error (or ran out of
`-max-tries`), when articles were never sent because no worker could connect, or when the
mbox parser stopped before the end of the file. Rerun it: articles the server already has
come back as `435` and cost little.

The process exits 0 when it finishes a run, whether or not the input was marked done; the
`.done` rename is the signal. It exits 2 on a command-line error, and 1 on a fatal
error (an input that cannot be opened or unpacked, a `-dos2unix` that cannot be found, no
7-Zip for a `.7z` input, no zstd for a UAT archive).

## What is sent

Articles are sent as stored, apart from these changes to the header:

- `Path:` gets `mbox2nntp-INPUTNAME!` prepended; an article without one gets
  `Path: mbox2nntp-INPUTNAME!not-for-mail`. In directory mode `INPUTNAME` is empty, giving
  `mbox2nntp!`.
- `Xref:`, `Expires:` and stray mbox `From ` lines are dropped, with their continuation
  lines.
- `Message-ID:` is reduced to the one the article is fed under.
- `Key:value` without a space after the colon is repaired to `Key: value`.
- NUL, backspace and tab in header lines become spaces.

Body lines are sent exactly as stored, dot-stuffed for the wire. `>From ` lines are not
un-escaped: none of the archives measured so far is mbox-escaped, and in all of them a
`>From ` line is part of the article as posted.

Articles are skipped, not sent, when they have:

- no Message-ID
- no body, which in practice means cancel control messages
- a yEnc body, unless `-keep-binary`
- no `Date:`, or no `Subject:` (counted as `437`). Directory mode adds `Subject: --` and
  `From: anonymous` where they are missing, and drops `Distribution: local`.
- more than 256 header lines (counted as `437`)

Directory mode also skips files by name: index and overview files, archives, sources and
binaries, `patch*`, and names containing `)`.

## Server replies

| reply | meaning | counted as |
|---|---|---|
| `235` | accepted | `sent` |
| `435` | already there | `dup` |
| `437` | rejected | `e437`; not an error, the input can still be marked done |
| `436` | try later | retried with backoff, up to `-max-tries` |
| lost connection or timeout | | the article is retried on a new connection, up to `-max-tries` |
| anything else | | `err`; the input is not marked done |

## Reading the log

The summary line at the end:

```
Done sending. 1465/1467 dup=0 e437=2 err=0 filter=0
```

That is sent out of total, then duplicates, rejections and errors. In a dry run, `sent`
counts the articles that would have been offered. `filter` is a leftover
and always 0. For mbox inputs, the
parser reports per file:

```
[iterateMbox] Done read mbox file='...' msgNumber=1467 skipped=0 near-misses=0 stopped-early=false last-err=<nil>
```

- `skipped`: malformed messages the parser stepped over. The rest of the file is still
  read.
- `stopped-early=true`: the parser lost its place before the end of the file, so the input
  is not marked done. Look at `last-err`.
- `near-misses`: body lines starting `From ` or `#! rnews ` that were *not* taken as a
  message separator. Up to five are logged with line numbers. In ordinary prose this is
  normal (`From Table 1 ...`). If the samples look like separators (`From user@host Thu
  Dec  8 ...`), the collection uses a separator shape the parser does not know, and
  articles were merged. Fix that in `go-lib-mbox2nntp` (`sepPatterns` in `mbox.go`)
  before importing it.

## Before importing a new collection

Run one file with `-dry-run` (no server needed) and read the `Done read` line. Its near-miss count and
samples show whether every message separator was recognised. The separator shapes known
today are listed, with the collection that needs each, in `sepPatterns` in the library.

## Tests

```
go test ./...
```

The tests need no network, server or corpus. They cover article corruption on the wire,
connection failures against a scripted fake server, header rewriting, archive handling, UAT
archives and their header encoding, and the command line. The 7z, xz and UAT tests need
7-Zip, xz and zstd, and skip without them. GitHub Actions runs them on Linux, Windows and macOS
(`.github/workflows/test.yml`).

`review-this-messy-codebase-cheeky-corbato.md` is the code review this version came out
of. It lists every bug found and fixed, with measurements against the archive.org corpus,
and says which already-imported articles were affected.

## License

See [LICENSE](LICENSE).
