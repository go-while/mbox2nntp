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
out=$(cd "$out" && pwd) # go -C below changes directory

ver=$(git describe --tags --dirty --always 2>/dev/null || echo dev)
# main.buildVersion is iafetch's; the linker ignores it in the others
ldflags="-s -w -X github.com/go-while/mbox2nntp/internal/version.Version=$ver -X main.buildVersion=$ver"
ext=""
[[ $(go env GOOS) == windows ]] && ext=".exe"

# NAME:MODULE_DIR:PACKAGE; iafetch is a module of its own (cmd/iafetch/go.mod)
for cmd in mbox2nntp:.:. uatexport:.:./cmd/uatexport ncexport:.:./cmd/ncexport iafetch:cmd/iafetch:.; do
	IFS=: read -r name mod pkg <<<"$cmd"
	CGO_ENABLED=0 go -C "$mod" build -trimpath -ldflags "$ldflags" -o "$out/$name$ext" "$pkg"
	echo "built $out/$name$ext ($ver $(go env GOOS)/$(go env GOARCH))"
done
