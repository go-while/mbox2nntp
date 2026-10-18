#!/usr/bin/env bash
# Builds mbox2nntp, uatexport, ncexport and iafetch into the repository
# root (or DIR), for this machine or for GOOS/GOARCH when they are set.
# The version comes from git describe, as v0.2.1 or v0.2.1-3-gabc1234-dirty.
#
#   ./build_All.sh                              # for this machine
#   ./build_All.sh /usr/local/bin               # into another directory
#   GOOS=windows GOARCH=amd64 ./build_All.sh    # mbox2nntp.exe, ...
#
# Release archives for every platform: .github/release-build.sh TAG
set -euo pipefail

cd "$(dirname "$0")"
out=${1:-.}
mkdir -p "$out"

ver=$(git describe --tags --dirty --always 2>/dev/null || echo dev)
ldflags="-s -w -X github.com/go-while/mbox2nntp/internal/version.Version=$ver"
ext=""
[[ $(go env GOOS) == windows ]] && ext=".exe"

for cmd in mbox2nntp:. uatexport:./cmd/uatexport ncexport:./cmd/ncexport iafetch:./cmd/iafetch; do
	name=${cmd%%:*}
	CGO_ENABLED=0 go build -trimpath -ldflags "$ldflags" -o "$out/$name$ext" "${cmd#*:}"
	echo "built $out/$name$ext ($ver $(go env GOOS)/$(go env GOARCH))"
done
