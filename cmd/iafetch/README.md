# iafetch

Downloads the archive.org Usenet items.

This page describes iafetch as on `main` (see the [CHANGELOG](../../CHANGELOG.md)). The
iafetch of release v0.3.2 is older: there `-j` counts files (default 4), and `-host-conns`,
`-item-conns`, `-piece`, `-conns`, `-meta` and `-nodes` do not exist yet.

`iafetch` downloads the items of [`archive.org/import-order.csv`](../../archive.org/import-order.csv)
(or any CSV with an `identifier` column) in list order, several items at a time, to
`DIR/ITEM/FILE`, for [mbox2nntp](../../README.md) to import. A SQLite database records every file it downloaded and the state of
every item. **An item recorded as done is skipped from then on without asking archive.org,
whether or not its files are still there**, so imported files can be moved or deleted and
the next run goes on with the items after them.

## Usage

```
go -C cmd/iafetch build -o ../../iafetch .
iafetch -n archive.org/import-order.csv                 # dry run: what would come, how big (see below)
iafetch -tier A archive.org/import-order.csv            # the early era first
iafetch -j 8 -o /data/ia -db /data/iafetch.sqlite archive.org/import-order.csv
iafetch -j 512 -meta 32 -o /data/ia archive.org/import-order.csv  # a whole 1 Gbit/s line
iafetch -status -db /data/iafetch.sqlite                # what the database records
iafetch -h                                              # all flags
```

iafetch is a Go module of its own (`cmd/iafetch/go.mod`), because of its SQLite
([`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite), pure Go, no C compiler
needed). Build, vet and test it from this directory, or with `go -C cmd/iafetch ...` from
the repository root.

## How it works

- **What is downloaded.** The files of the upload, by the item's archive.org metadata: not
  the files archive.org makes itself (`_meta.xml`, `_files.xml`, `_meta.sqlite`,
  `.torrent`), nor the replaced versions it keeps under `history/` (most
  `FULL-USENET-BACKUP-*` items have one, as big as the current file). `-include` and `-exclude` choose among them by regexp, `-tier`, `-list`,
  `-match`, `-from`, `-to` and `-limit` choose the items. Tier `X` (not Usenet) is left out
  unless `-tier` names it.
- **Order and parallelism.** Items are taken by the list's `order` column and go to `-j`
  workers (default 64). A worker downloads the files of its item one after another, over one
  connection, and takes the next item when done. Meanwhile the metadata of the next items
  is fetched, `-meta` requests at once (default 8) for up to 4 × `-meta` items ahead. An
  item goes to a worker as soon as its metadata is in, so the list order holds only roughly:
  archive.org's metadata requests sometimes hang for minutes, and in strict order one of them
  held up every item behind it.
- **Being gentle.** Each item lives on two data nodes. A request goes to the less busy of
  the two, and waits while both have `-host-conns` requests out (default 4; `0` for no
  limit). Consecutive items are spread well: in any 256 of them no node holds more than 12,
  out of 318 nodes.
- **Speed.** archive.org gives one connection well under 1 MB/s, so a fast line needs a few
  hundred items at once. `-item-conns N` (default 1) gives an item N connections: a file
  larger than `-piece` MB (default 32; `0` for never) is then fetched in pieces of that
  size, N side by side, each written to its place in the `.part` file. `-conns` caps the
  connections in all (default `-j` times `-item-conns`).
  Files come straight from the data nodes the item's metadata names (`workable_servers`,
  `dir`): `archive.org/download/` only redirects there, and with many downloads at once
  those redirects are what limits the rate. `/download/` is asked when a node does not
  have the file, or once each node has failed; `-nodes=false` always goes through it.
  Requests use HTTP/1.1, one TCP connection each: over the nodes' HTTP/2 the requests to one
  node shared one connection and its speed. Measured on a 1 Gbit/s line: about 50 MB/s
  through `/download/`; from the nodes 72 MB/s over HTTP/2 and 116 MB/s over HTTP/1.1, with
  `-j 512 -meta 32`.
- **Checking.** A file is written to `FILE.part` and renamed to `FILE` only when its size
  and md5 match the metadata. A stopped run (Ctrl-C) leaves `.part` files, which the next
  run resumes with a range request; for a file fetched in pieces, the finished pieces are
  recorded in the database and only the missing ones are fetched. Failed requests, including the 500s archive.org
  sometimes answers, are retried with a pause of 5 s doubling up to 5m20s (`-retries`). So
  is a request that gets no data for 2 minutes (`-stall`), resuming the part. A
  file that already exists under its name is checked by md5 and kept if right; a different
  file there is an error and is never replaced.
- **The database** (`-db`, default `./iafetch.sqlite`) has a table `items` (identifier,
  ord, tier, list, status, files, bytes, note, updated), a table `files` (identifier, name,
  size, md5, path, done_at) and a table `pieces` (identifier, name, piece size, piece) for files
  being fetched in pieces, emptied when each is done. `path` is as given by `-o`: give an
  absolute `-o` when other programs read the database. An item is `done`, `incomplete` (a file failed: the next run gets
  what is missing), `dark` (taken down on archive.org), `missing` (no such item) or `empty`
  (no files to download). `dark`, `missing` and `empty` items are skipped until
  `-retry-unavailable`. To fetch an item again, delete its rows:
  `sqlite3 iafetch.sqlite "DELETE FROM items WHERE identifier='X'; DELETE FROM files WHERE identifier='X'"`.
  Keep the database outside the download directory if files are moved from there in bulk.
- **An item's row in `items` is written when the item ends.** While it downloads, its
  finished files are already in `files`, but it has no `items` row yet: join with a LEFT
  JOIN to count them.
- **`done` is per item, not per filter.** An item fetched with `-include` or `-exclude` is
  `done` once the chosen files are in, or `empty` if none matched, and a later run without
  the filter skips it. To get its other files, delete its `items` row (or use
  `-retry-unavailable` for an `empty` one).
- **`-n` shows the list's sizes.** It adds up the `item_size` column of the CSV, which is
  archive.org's size of the whole item: with the files archive.org makes itself and the old
  versions under `history/`, which iafetch does not download. The real download is smaller;
  for the archive.org list, 7.95 TB against the 10.2 TB `-n` shows.

## Items not on archive.org

Some items in the list are taken down on archive.org and not in the list at all, such as
`textusenet-backup_a`, `_c`, `_e`, `_g`, `_h`, `_n` and `_u`; `textusenet-backup_0` and
`_f` are listed but have no files and are recorded as `empty`.
