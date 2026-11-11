# ncexport

Turns NewsCache spools into mbox files.

The archive.org `textusenet-backup_*` items (30 items, 977 GB) hold `.tar.gz` files, one per
hierarchy (`97comp.tar.gz`, `rainbow.tar.gz`, ...), each the spool of a
[NewsCache](https://github.com/hstraub/NewsCache) server: one directory per newsgroup
(`rainbow/test` for `rainbow.test`) with a `.db` file and `.artN` files. `ncexport` writes
such a spool out as one mbox per group, in a directory, for [mbox2nntp](../../README.md)
to feed to a server.

## Usage

```
go build -o ncexport ./cmd/ncexport
ncexport -gzip rainbow.tar.gz                           # rainbow/rainbow.test.mbox.gz, ...
curl -sL https://archive.org/download/textusenet-backup_9/97comp.tar.gz |
    ncexport -gzip -o 97comp -                          # straight from the download
ncexport -o comp /var/spool/newscache/comp              # an unpacked spool directory
ncexport -gzip -max-db 64G -tmpdir /big/tmp r.tar.gz    # .db files up to 64 GiB
ncexport -h                                             # all flags
```

## How it works

A tarball is read as a stream in one pass, and it can come from a pipe. Nothing is
unpacked to disk, except a `.db` over 1 MiB, which goes to a temporary file while it is
read: in `-tmpdir`, by default the output's directory, not the system's temporary
directory, which is often small or in RAM. It needs space for the largest `.db`. The
output directory is built under a temporary name and appears under its own only when
complete, as with [uatexport](../uatexport/README.md), and an existing one is never replaced.

- **Where the articles are.** NewsCache stores an article of 15 KB or less inside the `.db`
  and a bigger one in `.artN`. The server behind the backup ran a modified NewsCache that
  put every article in an `.artN` file. ncexport reads both.
- **What is changed.** NewsCache keeps an article as the server sent it: CRLF line ends and
  leading dots doubled. ncexport turns line ends into LF and doubled dots back into single
  ones. Nothing else changes. A few damaged yEnc articles also have lines with a single
  leading dot, which cannot come from doubling: those lines are kept as they are, and the
  summary counts the articles.
- **One mbox per group.** `GROUP.mbox`, or `GROUP.mbox.gz` with `-gzip`. In a file name,
  characters other than letters, digits and `._+-` become `_`. Only one group file is open
  at a time. A group the tarball holds in several places (51 of 352 in `sci.tar.gz`) is
  appended to, gzipped as one more gzip member, which gzip readers (mbox2nntp's, `zcat`)
  read as one stream. The mbox is mboxrd, with `From nobody DATE` taken from the Date
  header, as uatexport writes it.
- **Crossposts.** An article crossposted to several groups is in each group's directory,
  and goes to each group's mbox. Within one group's mbox an article is written once, the
  first time its Message-ID turns up; `-dedup=false` writes every copy. Memory for that is
  bounded by the largest group.
- **Order.** A tarball's articles are written in the tarball's order, a directory's group by
  group and by article number.
- **The summary.** It counts the articles, the group files, the groups, the `.art` files
  and the articles inside `.db` files. It also counts duplicates skipped within a group,
  and the articles a `.db` lists without an `.art` file.
- **Listed but not stored: `-missing FILE`.** A group's `.db` lists every article by its
  overview line, but NewsCache stored a crossposted article once, under one of its groups.
  So most listed articles without an `.art` file are in the export after all, from another
  group. In `sci.tar.gz` (textusenet-backup_s) that is 2,003,281 of 2,136,368. `-missing
  FILE` looks each one up by Message-ID and writes the others, the articles that are
  really missing, to FILE: one line each with group, number, date, subject, from and
  Message-ID, tab-separated, to find them in other archives. The summary then splits the
  count. Until the end, the overview fields of the listed articles wait in a temporary file
  in `-tmpdir`, about 150 bytes each, as a tarball holds a group's `.db` before its
  articles, and every exported Message-ID is kept in memory for the lookup (about
  100 bytes each). Articles crossposted to a hierarchy in another tarball count as
  missing.
- **The `.db`.** It is read one record at a time, never whole: the table of record offsets
  through a buffer, then each record where the table points. NewsCache places records and
  the table anywhere in the file, so it cannot be read front to back. A big group's `.db`
  costs time, not memory.
- **Size limits.** An article over 1 MiB, as an `.artN` file or inside a `.db`, is skipped
  with a warning and counted in the summary. Text articles are mostly under 16 KB, and
  servers rarely take one over 256 KB. uatexport skips UAT messages over 1 MiB the same
  way.
- **`-max-db SIZE`.** A `.db` over it (default `4G`, the most a 32-bit NewsCache can
  address) is skipped, and the summary says how many, as their articles are then missing.
  A 64-bit NewsCache has no such bound: raise it (`-max-db 64G`), or `-max-db 0` for no
  limit. A big `.db` costs time and, from a tarball, temporary disk space, not memory.

## Feeding the export to a server

Checked on `97comp.tar.gz` (757 articles) and `rainbow.tar.gz` (518 articles from 616
files in 34 groups). Every article mbox2nntp's parser reads back from the mbox equals the
stored one, with line ends and dots undone. Then give each group's `.mbox.gz` to
mbox2nntp, one run per file (`for f in 97comp/*.mbox.gz; do mbox2nntp -server … "$f"; done`);
each is renamed `.done` on its own. Expect it to skip articles with an empty body (24 in
`97comp`), which some spam has, and yEnc binaries unless `-keep-binary` is given.

## The newscache package

Other programs can read NewsCache spools with the package
`github.com/go-while/mbox2nntp/newscache` (standard library only); `newscache/newscachetest`
builds `.db` files for their tests.
