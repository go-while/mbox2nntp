#!/usr/bin/env bash
# uat-fetch-export.sh: download the published UAT archives (Usenet Archive
# Toolkit, one newsgroup per GROUP.usenet file) and export each with
# uatexport, by default to GROUP.mbox.gz.
#
# Sources:
#   nereid   https://usenet.nereid.pl/              333 GROUP.usenet.xz
#   archive  archive.org item usenet-uat-pc-games    39 GROUP.usenet.7z
# galaxy.7z (not a group) and the other files are skipped.
#
# It is gentle with the servers: one request at a time, a pause between
# files, optional bandwidth cap, retries with backoff. The file lists come
# from one request per source: the nereid directory listing (sizes rounded
# to 0.1 KiB/MiB/GiB) and the archive.org metadata (exact size and md5).
#
# Downloads resume (curl -C -) from NAME.part and are renamed to NAME only
# when verified: archive.org by md5, nereid by size (roughly, the listing
# rounds) and "xz -t". An export is written to a temporary name and renamed
# when uatexport succeeds. So a run can be stopped at any time and started
# again: finished downloads and exports are skipped.
#
# Needs: bash, curl, uatexport (go build ./cmd/uatexport in the mbox2nntp
# repository), zstd, xz and 7-Zip (7z, 7zz or 7za); for archive.org the ia
# command (internetarchive), or else jq.
#
# Run with -h for the options.

set -euo pipefail
shopt -s nullglob

prog=${0##*/}

NEREID_URL=https://usenet.nereid.pl/
IA_ITEM=usenet-uat-pc-games
IA_URL=https://archive.org/download/$IA_ITEM/
USER_AGENT="uat-fetch-export.sh (mbox2nntp; one request at a time)"

# What uatexport output takes per byte of archive, measured on 7 groups
# from both sources (see the README): .usenet unpacked 1.9, .mbox 2.5,
# .mbox.gz 0.55 (level 1: 0.65). In percent.
PCT_USENET=195
PCT_MBOX=255
PCT_GZ=57
PCT_GZ1=66

usage() {
	cat <<EOF
Usage: $prog [options]

Downloads the UAT group archives and exports each with uatexport.

  -s, --source SRC       nereid, archive (archive.org) or all (default: all)
  -d, --download-dir DIR archives go here (env UAT_DOWNLOAD_DIR, default ./uat-download)
  -o, --output-dir DIR   exports go here (env UAT_OUTPUT_DIR, default ./uat-mbox)
  -f, --format FMT       mbox.gz (default), mbox or dir: GROUP.mbox.gz, GROUP.mbox, GROUP/
  -z, --gzip-level N     gzip level 1 to 9 for mbox.gz (default 6)
      --raw              pass -raw to uatexport: messages as the archive stores them
  -m, --match REGEX      only groups whose name matches this bash regex (=~)
  -l, --limit N          only the first N groups (after --match and --sort)
      --sort KEY         name (default) or size: size takes the smallest first
  -n, --dry-run          only print what would be done; fetches the two file lists
      --download-only    download and verify, do not export
      --delete-archive   delete an archive once its export succeeded (default: keep)
      --keep-archive     keep the archives (the default)
      --verify           check archives that are already complete again (md5, xz -t)
  -p, --pause SECONDS    pause between two downloads (env UAT_PAUSE, default 2)
  -r, --limit-rate RATE  bandwidth cap, passed to curl --limit-rate, e.g. 2M (env UAT_LIMIT_RATE)
      --retries N        tries per download, with growing pauses (default 3)
      --retry-delay S    seconds before the first retry, doubled each time (default 10)
  -u, --uatexport PATH   the uatexport program (env UATEXPORT, default: uatexport in PATH)
  -t, --tmpdir DIR       where uatexport unpacks an archive, about 2x its size
                         (default: the download directory)
  -h, --help             this text

Build uatexport in the mbox2nntp repository with:
  go build -o uatexport ./cmd/uatexport

Examples:
  $prog -n                                   # what would be done, and the sizes
  $prog -s archive --sort size -l 3          # the 3 smallest archive.org groups
  $prog -s nereid -m '^pl\.comp\.' -r 5M     # the pl.comp.* groups at 5 MB/s
EOF
}

die() { printf '%s: %s\n' "$prog" "$*" >&2; exit 2; }
log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# human prints a byte count with a binary unit.
human() {
	awk -v b="$1" 'BEGIN {
		split("B KiB MiB GiB TiB", u, " "); i = 1
		while (b >= 1024 && i < 5) { b /= 1024; i++ }
		printf (i == 1 ? "%d %s" : "%.1f %s"), b, u[i]
	}'
}

filesize() { stat -c %s -- "$1" 2>/dev/null || stat -f %z -- "$1"; }

md5of() {
	if command -v md5sum >/dev/null; then
		md5sum -- "$1" | awk '{print $1}'
	else
		md5 -q -- "$1"
	fi
}

# urlencode percent-encodes everything but unreserved characters.
urlencode() {
	local s=$1 out= c i
	for ((i = 0; i < ${#s}; i++)); do
		c=${s:i:1}
		case $c in
		[A-Za-z0-9._~-]) out+=$c ;;
		*) out+=$(printf '%%%02X' "'$c") ;;
		esac
	done
	printf '%s' "$out"
}

# validname accepts only names that are safe as one file name.
validname() {
	[[ $1 =~ ^[A-Za-z0-9][A-Za-z0-9._+-]*\.usenet\.(xz|7z)$ ]] && [[ $1 != *..* ]]
}

# defaults, then the environment, then the options
source_sel=all
dldir=${UAT_DOWNLOAD_DIR:-./uat-download}
outdir=${UAT_OUTPUT_DIR:-./uat-mbox}
format=mbox.gz
gzlevel=6
raw=0
match=
limit=0
sortkey=name
dryrun=0
downloadonly=0
deletearchive=0
verify=0
pause=${UAT_PAUSE:-2}
limitrate=${UAT_LIMIT_RATE:-}
retries=3
retrydelay=10
uatexport=${UATEXPORT:-}
tmpbase=

need() { [[ $# -ge 2 && -n $2 ]] || die "$1 needs a value"; }
while [[ $# -gt 0 ]]; do
	case $1 in
	-s | --source) need "$@"; source_sel=$2; shift ;;
	-d | --download-dir) need "$@"; dldir=$2; shift ;;
	-o | --output-dir) need "$@"; outdir=$2; shift ;;
	-f | --format) need "$@"; format=$2; shift ;;
	-z | --gzip-level) need "$@"; gzlevel=$2; shift ;;
	--raw) raw=1 ;;
	-m | --match) need "$@"; match=$2; shift ;;
	-l | --limit) need "$@"; limit=$2; shift ;;
	--sort) need "$@"; sortkey=$2; shift ;;
	-n | --dry-run) dryrun=1 ;;
	--download-only) downloadonly=1 ;;
	--delete-archive) deletearchive=1 ;;
	--keep-archive) deletearchive=0 ;;
	--verify) verify=1 ;;
	-p | --pause) need "$@"; pause=$2; shift ;;
	-r | --limit-rate) need "$@"; limitrate=$2; shift ;;
	--retries) need "$@"; retries=$2; shift ;;
	--retry-delay) need "$@"; retrydelay=$2; shift ;;
	-u | --uatexport) need "$@"; uatexport=$2; shift ;;
	-t | --tmpdir) need "$@"; tmpbase=$2; shift ;;
	-h | --help) usage; exit 0 ;;
	--) shift; break ;;
	*) usage >&2; die "unknown option: $1" ;;
	esac
	shift
done
[[ $# -eq 0 ]] || die "no arguments expected, got: $*"

case $source_sel in
nereid | all) ;;
archive | archive.org | ia | pcgames) source_sel=archive ;;
*) die "--source: nereid, archive or all" ;;
esac
case $format in
mbox.gz) ext=.mbox.gz pct=$PCT_GZ ;;
mbox) ext=.mbox pct=$PCT_MBOX ;;
dir) ext= pct=$PCT_MBOX ;;
*) die "--format: mbox.gz, mbox or dir" ;;
esac
[[ $gzlevel =~ ^[1-9]$ ]] || die "--gzip-level: 1 to 9"
[[ $format == mbox.gz && $gzlevel == 1 ]] && pct=$PCT_GZ1
[[ $limit =~ ^[0-9]+$ ]] || die "--limit: a number"
[[ $retries =~ ^[1-9][0-9]*$ ]] || die "--retries: a number from 1"
[[ $retrydelay =~ ^[0-9]+$ ]] || die "--retry-delay: seconds"
[[ $pause =~ ^[0-9]+([.][0-9]+)?$ ]] || die "--pause: seconds"
[[ $sortkey == name || $sortkey == size ]] || die "--sort: name or size"
if [[ -n $match ]]; then
	# an invalid regex makes [[ =~ ]] return 2
	rc=0; [[ x =~ $match ]] || rc=$?
	[[ $rc -le 1 ]] || die "--match: invalid regex: $match"
fi

command -v curl >/dev/null || die "curl not found"

if [[ $downloadonly -eq 0 ]]; then
	if [[ -z $uatexport ]]; then
		uatexport=$(command -v uatexport || true)
		[[ -n $uatexport ]] || die "uatexport not found in PATH: build it with 'go build -o uatexport ./cmd/uatexport' in the mbox2nntp repository, and give its path with -u"
	fi
	[[ -x $uatexport ]] || die "uatexport: not an executable: $uatexport"
	help=$("$uatexport" -h 2>&1 || true)
	if [[ $format == mbox.gz && $help != *-gzip* ]]; then
		die "$uatexport has no -gzip: rebuild it, or use --format mbox"
	fi
fi

tmpfile=$(mktemp "${TMPDIR:-/tmp}/uat-fetch-export.XXXXXX")
runtmp=
cleanup() {
	rm -f -- "$tmpfile" "$tmpfile".*
	[[ -z $runtmp ]] || rm -rf -- "$runtmp"
}
trap cleanup EXIT
trap 'log "interrupted"; exit 130' INT TERM

# --- file lists --------------------------------------------------------
# list.tsv: source TAB name TAB bytes TAB exact(1|0) TAB md5 TAB url

list=$tmpfile.list
: >"$list"

# curl_get URL FILE: one GET with retries, for the file lists.
curl_get() {
	curl -fsSL -A "$USER_AGENT" --connect-timeout 30 \
		--retry "$retries" --retry-delay "$retrydelay" --retry-connrefused \
		-o "$2" -- "$1"
}

skipped_names=0
add_entry() { # source name bytes exact md5 url
	if [[ $2 == galaxy.7z ]]; then
		return
	fi
	if ! validname "$2"; then
		log "skipping file with an unexpected name: $(printf '%q' "$2")"
		skipped_names=$((skipped_names + 1))
		return
	fi
	printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$@" >>"$list"
}

if [[ $source_sel == nereid || $source_sel == all ]]; then
	html=$tmpfile.nereid
	log "fetching the file list of $NEREID_URL"
	curl_get "$NEREID_URL" "$html" || die "could not fetch $NEREID_URL"
	# rows: <a href="..." title="NAME">NAME</a></td><td class="size">8.0 MiB</td>
	n=0
	while IFS=$'\t' read -r name size; do
		name=${name//&amp;/&}
		bytes=$(awk -v s="$size" 'BEGIN {
			split(s, a, " "); u = 1
			if (a[2] == "KiB") u = 1024; else if (a[2] == "MiB") u = 1048576
			else if (a[2] == "GiB") u = 1073741824; else if (a[2] == "TiB") u = 1099511627776
			printf "%.0f", a[1] * u }')
		add_entry nereid "$name" "$bytes" 0 - "$NEREID_URL$(urlencode "$name")"
		n=$((n + 1))
	done < <(sed -n 's/.*title="\([^"]*\.usenet\.xz\)">[^<]*<\/a><\/td><td class="size">\([^<]*\)<.*/\1\t\2/p' "$html")
	[[ $n -gt 0 ]] || die "no .usenet.xz files in the listing of $NEREID_URL: has its layout changed?"
fi

if [[ $source_sel == archive || $source_sel == all ]]; then
	meta=$tmpfile.ia
	log "fetching the file list of archive.org item $IA_ITEM"
	if command -v ia >/dev/null; then
		ia list -c name,size,md5 "$IA_ITEM" >"$meta" || die "ia list $IA_ITEM failed"
	elif command -v jq >/dev/null; then
		curl_get "https://archive.org/metadata/$IA_ITEM" "$meta.json" || die "could not fetch the metadata of $IA_ITEM"
		jq -r '.files[] | [.name, (.size // "0"), (.md5 // "-")] | @tsv' "$meta.json" >"$meta"
	else
		die "archive.org needs the ia command (pip install internetarchive) or jq"
	fi
	n=0
	while IFS=$'\t' read -r name size md5; do
		[[ $name == *.usenet.7z || $name == galaxy.7z ]] || continue
		[[ $size =~ ^[0-9]+$ && $md5 =~ ^[0-9a-f]{32}$ ]] || { log "skipping $(printf '%q' "$name"): no size or md5 in the metadata"; continue; }
		add_entry archive "$name" "$size" 1 "$md5" "$IA_URL$(urlencode "$name")"
		n=$((n + 1))
	done <"$meta"
	[[ $n -gt 0 ]] || die "no .usenet.7z files in the metadata of $IA_ITEM"
fi

# select: --match, --sort, --limit
sel=$tmpfile.sel
: >"$sel"
while IFS=$'\t' read -r src name bytes exact md5 url; do
	group=${name%.usenet.*}
	if [[ -n $match && ! $group =~ $match ]]; then
		continue
	fi
	printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$src" "$name" "$bytes" "$exact" "$md5" "$url" >>"$sel"
done <"$list"
if [[ $sortkey == size ]]; then
	sort -t $'\t' -k3,3n -k2,2 -o "$sel" -- "$sel"
else
	sort -t $'\t' -k2,2 -k1,1 -o "$sel" -- "$sel"
fi
if [[ $limit -gt 0 ]]; then
	head -n "$limit" -- "$sel" >"$sel.tmp"
	mv -- "$sel.tmp" "$sel"
fi
total=$(wc -l <"$sel" | tr -d ' ')
[[ $total -gt 0 ]] || die "no groups selected"

# --- state of each group, and the estimate -------------------------------

# tolerance of a nereid listing size: half of the last digit shown, 0.05
# of its unit; the listing rounds.
tolerance() {
	local b=$1
	if ((b >= 1073741824)); then echo $((1073741824 / 20 + 1))
	elif ((b >= 1048576)); then echo $((1048576 / 20 + 1))
	elif ((b >= 1024)); then echo $((1024 / 20 + 1))
	else echo 0
	fi
}

# size_ok FILE BYTES EXACT: the file has the size from the list
size_ok() {
	local have
	have=$(filesize "$1") || return 1
	if [[ $3 == 1 ]]; then
		[[ $have -eq $2 ]]
	else
		local d=$((have - $2)) t
		t=$(tolerance "$2")
		((d < 0)) && d=$((-d))
		((d <= t))
	fi
}

output_of() { printf '%s/%s%s' "$outdir" "$1" "$ext"; }

# outsize prints the size of an output file, or the disk use of a directory.
outsize() {
	if [[ -f $1 ]]; then
		filesize "$1"
	else
		du -sk -- "$1" | awk '{print $1 * 1024}'
	fi
}

sum_all=0 sum_dl=0 sum_out=0 n_dl=0 n_out=0 n_done=0 max_arch=0
while IFS=$'\t' read -r src name bytes exact md5 url; do
	group=${name%.usenet.*}
	sum_all=$((sum_all + bytes))
	((bytes > max_arch)) && max_arch=$bytes
	out=$(output_of "$group")
	if [[ -e $out ]]; then
		n_done=$((n_done + 1))
		continue
	fi
	if [[ ! -f $dldir/$name ]]; then
		part=0
		[[ -f $dldir/$name.part ]] && part=$(filesize "$dldir/$name.part")
		sum_dl=$((sum_dl + bytes - part))
		n_dl=$((n_dl + 1))
	fi
	if [[ $downloadonly -eq 0 ]]; then
		sum_out=$((sum_out + bytes * pct / 100))
		n_out=$((n_out + 1))
	fi
done <"$sel"

freespace() { df -Pk -- "$1" 2>/dev/null | awk 'NR == 2 {printf "%.0f", $4 * 1024}'; }
existing_parent() { local d=$1; while [[ ! -d $d ]]; do d=$(dirname -- "$d"); done; printf '%s' "$d"; }

log "$total groups selected, archives $(human "$sum_all") in all; $n_done already exported"
log "to download: $n_dl archives, $(human "$sum_dl") (approximate for nereid)"
if [[ $downloadonly -eq 0 ]]; then
	log "to export: $n_out groups as $format, about $(human "$sum_out") of output (${pct}% of the archive size, measured)"
	log "temporary space: about $(human $((max_arch * PCT_USENET / 100))) for the largest archive unpacked"
fi
log "free: download dir $(human "$(freespace "$(existing_parent "$dldir")")"), output dir $(human "$(freespace "$(existing_parent "$outdir")")")"

# --- work -----------------------------------------------------------------

# verify FILE SRC BYTES EXACT MD5: what a finished download must pass
verify() {
	local f=$1 src=$2 bytes=$3 exact=$4 md5=$5
	if ! size_ok "$f" "$bytes" "$exact"; then
		log "$f: size $(filesize "$f"), the list says $bytes$([[ $exact == 1 ]] || echo ' (rounded)')"
		return 1
	fi
	if [[ $exact == 1 ]]; then
		local got
		got=$(md5of "$f")
		if [[ $got != "$md5" ]]; then
			log "$f: md5 $got, the metadata says $md5"
			return 1
		fi
	elif command -v xz >/dev/null; then
		xz -t -- "$f" || { log "$f: xz -t failed"; return 1; }
	else
		7z t -bd -- "$f" >/dev/null || { log "$f: 7z t failed"; return 1; }
	fi
}

curlopts=(-fL -A "$USER_AGENT" --connect-timeout 30 --retry 3 --retry-delay "$retrydelay" --retry-connrefused)
[[ -n $limitrate ]] && curlopts+=(--limit-rate "$limitrate")
if [[ -t 1 ]]; then curlopts+=(--progress-bar); else curlopts+=(-sS); fi

did_request=0
pause_between() {
	if [[ $did_request -eq 1 && $pause != 0 ]]; then
		sleep "$pause"
	fi
	did_request=1
}

# download SRC NAME BYTES EXACT MD5 URL: into $dldir/NAME, resuming NAME.part
download() {
	local src=$1 name=$2 bytes=$3 exact=$4 md5=$5 url=$6
	local dst=$dldir/$name part=$dldir/$name.part try=1 delay=$retrydelay rc
	while :; do
		# a .part with the full size from the exact list needs no request
		if [[ $exact == 1 && -f $part ]] && (($(filesize "$part") >= bytes)); then
			rc=0
		else
			pause_between
			log "$name: downloading $(human "$bytes")$([[ -f $part ]] && echo ", resuming at $(human "$(filesize "$part")")")"
			rc=0
			curl "${curlopts[@]}" -C - -o "$part" -- "$url" || rc=$?
		fi
		if [[ $rc -eq 0 ]]; then
			if verify "$part" "$src" "$bytes" "$exact" "$md5"; then
				mv -- "$part" "$dst"
				return 0
			fi
			log "$name: the download is bad, starting over"
			rm -f -- "$part"
		else
			log "$name: curl failed (exit $rc)"
		fi
		if ((try >= retries)); then
			return 1
		fi
		log "$name: try $((try + 1)) of $retries in ${delay}s"
		sleep "$delay"
		try=$((try + 1)) delay=$((delay * 2))
	done
}

if [[ $dryrun -eq 0 ]]; then
	mkdir -p -- "$dldir"
	[[ $downloadonly -eq 1 ]] || mkdir -p -- "$outdir"
	if command -v flock >/dev/null; then
		exec 9>"$dldir/.uat-fetch-export.lock"
		flock -n 9 || die "another run uses $dldir"
	fi
	if [[ $downloadonly -eq 0 ]]; then
		runtmp=$(mktemp -d "${tmpbase:-$dldir}/.uat-export-tmp.XXXXXX")
	fi
fi

n_ok=0 n_fail=0 n_skip=0 i=0
while IFS=$'\t' read -r src name bytes exact md5 url <&3; do
	i=$((i + 1))
	group=${name%.usenet.*}
	arch=$dldir/$name
	out=$(output_of "$group")
	tag="[$i/$total] $group"

	if [[ $downloadonly -eq 0 && -e $out ]]; then
		[[ $dryrun -eq 1 ]] && log "$tag: exported already, $(human "$(outsize "$out")"): skip"
		n_skip=$((n_skip + 1))
		continue
	fi

	if [[ $dryrun -eq 1 ]]; then
		if [[ -f $arch ]]; then
			what="have the archive"
		elif [[ -f $arch.part ]]; then
			what="resume the download at $(human "$(filesize "$arch.part")") of $(human "$bytes")"
		else
			what="download $(human "$bytes") from $url"
		fi
		[[ $downloadonly -eq 1 ]] || what+=", export to $out"
		log "$tag: would $what"
		continue
	fi

	if [[ -f $arch ]]; then
		if [[ $verify -eq 1 ]] && ! verify "$arch" "$src" "$bytes" "$exact" "$md5"; then
			log "$tag: FAILED: $arch does not verify; delete it to download it again"
			n_fail=$((n_fail + 1))
			continue
		fi
	elif ! download "$src" "$name" "$bytes" "$exact" "$md5" "$url"; then
		log "$tag: FAILED: download"
		n_fail=$((n_fail + 1))
		continue
	fi
	if [[ $downloadonly -eq 1 ]]; then
		log "$tag: archive $(human "$(filesize "$arch")")"
		n_ok=$((n_ok + 1))
		continue
	fi

	# export to a temporary name next to the output, renamed when complete
	tmpout=$outdir/.$group$ext.part
	rm -rf -- "$tmpout"
	args=(-tmpdir "$runtmp" -o "$tmpout")
	[[ $format == mbox.gz ]] && args+=("-gzip=$gzlevel")
	[[ $format == dir ]] && args+=(-format dir)
	[[ $raw -eq 1 ]] && args+=(-raw)
	elog=$tmpfile.export.log
	start=$(date +%s)
	rc=0
	"$uatexport" "${args[@]}" -- "$arch" 2>"$elog" || rc=$?
	secs=$(($(date +%s) - start))
	if [[ $rc -ne 0 ]]; then
		rm -rf -- "$tmpout"
		log "$tag: FAILED: uatexport exit $rc: $(tail -n 1 -- "$elog")"
		n_fail=$((n_fail + 1))
		continue
	fi
	if [[ -e $out ]]; then
		rm -rf -- "$tmpout"
		log "$tag: FAILED: $out appeared during the export"
		n_fail=$((n_fail + 1))
		continue
	fi
	mv -- "$tmpout" "$out"
	msgs=?
	last=$(tail -n 1 -- "$elog")
	re=': ([0-9]+) articles to '
	[[ $last =~ $re ]] && msgs=${BASH_REMATCH[1]}
	log "$tag: $msgs messages, $(human "$(outsize "$out")") $format from $(human "$(filesize "$arch")") archive in ${secs}s"
	n_ok=$((n_ok + 1))
	if [[ $deletearchive -eq 1 ]]; then
		rm -f -- "$arch"
	fi
done 3<"$sel"

if [[ $dryrun -eq 1 ]]; then
	log "dry run: nothing downloaded or written"
	exit 0
fi
log "done: $n_ok ok, $n_skip skipped (exported already), $n_fail failed$([[ $skipped_names -eq 0 ]] || echo ", $skipped_names odd names ignored")"
[[ $n_fail -eq 0 ]]
