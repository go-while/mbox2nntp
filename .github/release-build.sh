#!/usr/bin/env bash
# Builds the release archives into dist/: mbox2nntp, uatexport and ncexport for each
# target, with README.md and LICENSE, plus a SHA-256 checksum file.
# Used by .github/workflows/release.yml; runs the same on a Linux machine.
#
#   .github/release-build.sh v0.1.0
set -euo pipefail

tag=${1:?usage: release-build.sh TAG}
if [[ ! $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
	echo "tag '$tag' is not a version like v0.1.0" >&2
	exit 2
fi

targets="linux/amd64 linux/arm64 windows/amd64 windows/arm64 darwin/amd64 darwin/arm64 freebsd/amd64"
ldflags="-s -w -X github.com/go-while/mbox2nntp/internal/version.Version=$tag"

rm -rf dist
mkdir -p dist
for target in $targets; do
	goos=${target%/*}
	goarch=${target#*/}
	ext=""
	[[ $goos == windows ]] && ext=".exe"
	name="mbox2nntp_${tag}_${goos}_${goarch}"
	dir="dist/$name"
	mkdir -p "$dir"
	for cmd in mbox2nntp:. uatexport:./cmd/uatexport ncexport:./cmd/ncexport; do
		CGO_ENABLED=0 GOOS=$goos GOARCH=$goarch \
			go build -trimpath -ldflags "$ldflags" -o "$dir/${cmd%%:*}$ext" "${cmd#*:}"
	done
	cp README.md LICENSE "$dir/"
	if [[ $goos == windows ]]; then
		(cd dist && zip -q -r "$name.zip" "$name")
	else
		tar -C dist -czf "dist/$name.tar.gz" "$name"
	fi
	rm -rf "$dir"
	echo "built $name"
done
(cd dist && sha256sum ./*.tar.gz ./*.zip | sed 's| \./| |' > "mbox2nntp_${tag}_checksums.txt")
ls -l dist
