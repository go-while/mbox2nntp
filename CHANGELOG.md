# Changelog

## v0.3.2 (2026-10-22)

- **ncexport writes one mbox per group** (`GROUP.mbox` or `GROUP.mbox.gz`), in a directory built under a temporary name and renamed when complete: `NAME/`, or `-o DIR`. The single mbox for a whole hierarchy is gone. A group the tarball holds in several places is appended to.
- **ncexport dedup is per group:** an article crossposted to several groups goes to each group's mbox, and only a repeat within one group is skipped. Memory for the dedup is bounded by the largest group. With `-missing`, every exported Message-ID is still kept, for the crosspost lookup.

## v0.3.1 (2026-10-22)

- go-lib-mbox2nntp `73e35b8`: the parser also takes the separator line most mbox writers use (RFC 4155), `From user@example.com Thu Dec  8 12:55:04 2016`, as a whole line in exactly that form, with a year of 1 to 4 digits. Mboxes from mutt, Thunderbird, Python's `mailbox` and usenet_archiver were not read at all before, or were merged into the message before. On the archive.org corpus (5.28 million messages) no message changes.
- uatexport and ncexport end each article's header with `X-Mbox2nntp-Format: mboxrd`. mbox2nntp then takes one `>` off the quoted `From ` and `#! rnews ` lines and drops the field, so an exported mbox goes to a server unchanged. Archive mboxes without the field are sent as stored, as before.
- `#! rnews ` lines are quoted after any number of `>`, like `From ` lines. Before, `>#! rnews 5` was written as it was, and could not be told apart from a quoted `#! rnews 5`.

## v0.3.0 (2026-10-22)

**mbox2nntp**
- New counters `dropped` and `binary` in the summary and the per-file line:
  - `dropped`: messages read but never offered, for lacking a Message-ID or a body, or failing to parse;
  - `binary`: yEnc articles left out.

  Neither stops the input from being marked done, unless every message was dropped and none offered.
- In directory mode, a file over 1 MiB counts as a 437, as in mbox mode.
- The input file name in our `Path` element keeps only the characters RFC 5536 allows there. Every other character becomes `_`.
- When unpacking fails to write, for example on a full disk, the error now says so, not "signal: killed".

**ncexport**
- A `.db` is read one record at a time, never whole: memory no longer grows with its size.
- `-max-db SIZE` (default `4G`, `0` for no limit) replaces the fixed 256 MiB limit. A 64-bit NewsCache, as in the textusenet backup, can write bigger `.db` files. Skipped `.db` files are counted on their own, and the summary says their articles are missing.
- `-tmpdir DIR` sets where a big `.db` from a tarball is copied while it is read. The default is the output's directory, not the system's temporary directory.
- `-missing FILE` writes the articles a `.db` lists that are in neither the spool nor the export. One crossposted to another group is found there by Message-ID and is not written. The summary splits the count.
- The dedup list kept every article's text in memory, not just its Message-ID. On `rss.tar.gz` peak memory went from 930 MB to 104 MB.
- Ctrl-C and SIGTERM remove all temporary files.
- A `.db` or a spool directory given as a symlink is read correctly.
- A crafted `.db` can no longer turn a small file into gigabytes of articles.
- An article over 1 MiB is skipped and counted, but no longer reported as missing.

**newscache package:** API changes, for anyone importing it.
- `Record` has a new field `Size`. Unkeyed `Record{...}` literals no longer compile.
- `ParseDB` leaves `Data` nil for a record over `MaxArticleSize`.
- `Stats.TooBig` counts articles only. Skipped `.db` files are in the new `Stats.BigDBs`.
- `MaxDBSize` is now 4 GiB, up from 256 MiB.
- `ReadTar` copies a `.db` over 1 MiB to a temporary file in `TempDir`.
- New: `ReadDB`, `Listed`, `Missing`, `TempDir`.

## v0.2.3 (2026-10-22)

Fixes from a code review. If you run mbox2nntp with the default tmpdir (`.`), update: v0.2.2 can delete a `NAME.mbox` that sits next to the `NAME.mbox.gz` you import.

**mbox2nntp**
- Archives are unpacked into a new directory of the run's own, `tmpdir/.mbox2nntp-*`, and only that directory is removed. It is removed whatever the outcome, also on exit 1 and on SIGINT or SIGTERM. Before, an unpacked `NAME.mbox` replaced and then deleted a file of that name next to the archive, and two runs sharing a tmpdir overwrote each other's copy.
- A worker adds its counts to the totals before it reports done. Before, a run could report 0/0, or miss its last error and rename the input to `.done`.
- Header lines are dot-stuffed too, as RFC 3977 3.1.1 asks. Before, a header line starting with `.` lost the dot at the server.
- The Message-ID comes from the first Message-ID field only. Two such fields used to give `<a@b> <a@b>`. Over the 5.28 million messages of the test corpus, 3 IDs change.
- A dropped header field (Message-ID copy, Expires, a line without a colon) takes its continuation lines with it, so they no longer join the field before. A continuation line of only spaces no longer drops the article.
- A message with headers and no body no longer merges into the next one.
- An article is never sent cut short. A read error stops the file, and the input is not marked done.
- **Size limits:**
  - An article over 1 MiB is not sent. It is counted as a 437 and as `too-big`, and the input can still be marked done.
  - A line over 1 MiB stops the file, and the input is not marked done.
  - In directory mode, files over 1 MiB are not read.
  - In UAT archives, a message over 1 MiB is skipped and counted. Before, a 99 KB crafted `.usenet` could make mbox2nntp allocate 3 GiB.

**uatexport and ncexport**
- Body lines starting `#! rnews ` are quoted with `>`, like `From ` lines. Before, the importer split such articles there.
- uatexport uses the archive's group name for the default output file only if it is a safe file name. Before, a name like `../../x` could write anywhere.
- ncexport checks the gzip trailer of a `.tar.gz`, so a damaged tarball no longer passes unnoticed.
- ncexport skips and counts `.artN` files over 1 MiB and `.db` files over 256 MiB. uatexport skips and counts messages over 1 MiB.

**iafetch**
- A download that receives no data for `-stall` (default 2m) is given up and retried, resuming the `.part`. Before, a stalled server could hang the whole run.
- No panic when two workers hit different kinds of database error.
- A file `X` is not downloaded through `X.part` when the item also has a file named `X.part`. Before, the two overwrote each other.
- A `.part` longer than the file restarts with a fresh MD5 state. Before, it cost a second download.

## Earlier versions

See the [GitHub releases](https://github.com/go-while/mbox2nntp/releases).
