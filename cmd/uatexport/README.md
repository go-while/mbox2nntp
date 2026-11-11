# uatexport

Writes a [Usenet Archive Toolkit](https://github.com/wolfpld/usenetarchive) (UAT) archive
out as an mbox file or as one file per article, for use elsewhere. Part of
[mbox2nntp](../../README.md); to feed a UAT archive to a server, give it to mbox2nntp
as it is (see [UAT archives](../../README.md#uat-archives) there, also for what the
toolkit changed in the articles).

```
go build -o uatexport ./cmd/uatexport                   # from the repository root
uatexport -h                                            # all flags
```

## Usage

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

## Storage

Measured on 7 groups (3 from nereid, 4 from archive.org, 56 MB of archives),
per byte of `.usenet.xz`/`.7z`: the unpacked `.usenet` takes 1.9, the `.mbox` 2.4,
the `.mbox.gz` 0.56 (level 1: 0.64, level 9: 0.54), and `.mbox.xz` 0.34. A `.mbox.gz`
is smaller than the archive because the archive also holds the toolkit's search
indexes. `-gzip` makes an export about 1.7 times slower than a plain one (`-gzip=9`: 2.5
times). Reading the `.mbox.gz` with mbox2nntp is as fast as reading the archive.

To feed a server you need no mbox at all: give mbox2nntp the archive. Keep the
`.usenet.xz`/`.7z` files: they are the toolkit's own data. An exported mbox works as input
too (see [The mbox format](#the-mbox-format)), but it holds only the articles, re-encoded.

## Downloading a whole collection

To download a whole collection and export every group, there is
[`contrib/uat-fetch-export.sh`](../../contrib/uat-fetch-export.sh) (bash and curl; `ia` or `jq`
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

## The uat package

Other programs can read UAT archives with the package `github.com/go-while/mbox2nntp/uat`
(standard library only, plus the zstd program); `uat/uattest` writes small archives for
their tests.

## The mbox format

The mbox is mboxrd: header and body lines starting `From ` or `#! rnews ` after any number
of `>` get one more `>` (mbox2nntp's parser takes both for the start of a message). Each article has a
`From nobody DATE` line that mbox2nntp's parser reads, and ends its header with
`X-Mbox2nntp-Format: mboxrd`. The articles get the same changes as when mbox2nntp sends a UAT archive
([UAT archives](../../README.md#uat-archives)). mbox2nntp reads
such an mbox back exactly: the field makes it take one `>` off those lines again, and it
is not sent. An exported directory works as a `DIR` input.
