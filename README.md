# mbox2nntp

Feeds Usenet archives in mbox form to an NNTP server, one article at a time, with
`IHAVE`. It was written to load the archive.org Usenet collections (`gna-clari`,
`usenet-clari`, the `usenet-*` and `FULL-USENET-BACKUP-*` items) into INN2.

Each run takes one input: an mbox, a compressed mbox, or a directory of article files.

## Build

```
go build -o mbox2nntp .
```

Needs Go 1.27.1 or later. All dependencies resolve from GitHub. The mbox parser is
[`go-lib-mbox2nntp`](https://github.com/go-while/go-lib-mbox2nntp), a fork of
`github.com/sam-falvo/mbox`.

External programs, used only for some inputs:

| program | used for |
|---|---|
| `/usr/bin/dos2unix` | every mbox input, unless `-no-dos2unix` (checked at startup) |
| `/usr/bin/unzip` | `.mbox.zip` |
| `/usr/bin/7z` | `.mbox.7z` |

`.mbox.gz` is unpacked in-process, and directories are walked in-process.

## Usage

```
mbox2nntp [flags] -server HOST:PORT INPUT
mbox2nntp [flags] -dry-run INPUT
```

| INPUT | meaning |
|---|---|
| `NAME.mbox`, `NAME.bag` | an mbox file |
| `NAME.mbox.gz` | gzip-compressed mbox, one stream or many concatenated members |
| `NAME.mbox.zip` | zip archive holding `NAME.mbox` |
| `NAME.mbox.7z` | 7z archive holding `NAME.mbox` |
| `DIR` | a directory tree with one article per file, such as an INN spool |

| flag | default | |
|---|---|---|
| `-server HOST:PORT` | (required) | the NNTP server; not needed with `-dry-run` |
| `-workers N` | `1` | parallel connections |
| `-tmpdir DIR` | `.` | where `.gz`, `.zip` and `.7z` inputs are unpacked |
| `-dry-run` | off | parse and rewrite articles, but connect to no server and send nothing; the input is never marked done |
| `-keep-binary` | off | send yEnc (`=ybegin`) articles instead of dropping them |
| `-no-dos2unix` | off | skip `dos2unix`; a `.nodos2unix` file in the working directory does the same |
| `-dos2unix PATH` | `/usr/bin/dos2unix` | where `dos2unix` is |
| `-timeout D` | `2m` | limit on each read or write on a server connection |
| `-max-tries N` | `10` | tries per article after a `436` or a lost connection |
| `-prehash` | on | send `IHAVE <msgid> <sha256-of-msgid>`; `-prehash=false` sends plain `IHAVE <msgid>` |
| `-lax-msgid` | on | accept loosely formed Message-IDs |

Examples:

```
mbox2nntp -server localhost:119 -workers 4 -tmpdir /var/tmp/m2n clari.world.top.mbox.gz
mbox2nntp -dry-run 0.akita-inu.20140617.mbox.gz
mbox2nntp -server localhost:119 /srv/oldspool/comp/lang/
```

**A plain `NAME.mbox` or `NAME.bag` is changed in place:** `dos2unix -f` rewrites the
file itself. Archives
are unpacked into `-tmpdir` first, so their contents are converted there. Use
`-no-dos2unix` or work on a copy if the source must stay byte-identical. The parser
handles CRLF itself, so the importer works without `dos2unix`; it is kept on by default
because that is how the existing imports were run.

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
- The copy unpacked into `-tmpdir` is removed after the rename.
- A directory input is never renamed.

An input is **not** marked done when any article ended in an error (or ran out of
`-max-tries`), when articles were never sent because no worker could connect, or when the
mbox parser stopped before the end of the file. Rerun it: articles the server already has
come back as `435` and cost little.

The process exits 0 when it finishes a run, whether or not the input was marked done; the
`.done` rename is the signal. It exits 2 on a command-line error, and 1 on a fatal
error (an input that cannot be opened or unpacked, a missing `dos2unix`).

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
connection failures against a scripted fake server, header rewriting, archive handling and
the command line.

`review-this-messy-codebase-cheeky-corbato.md` is the code review this version came out
of. It lists every bug found and fixed, with measurements against the archive.org corpus,
and says which already-imported articles were affected.

## License

See [LICENSE](LICENSE).
