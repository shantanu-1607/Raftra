#!/usr/bin/env bash
# install-server.sh [VERSION] — download raftra-server from GitHub Releases,
# verify its SHA-256 checksum and install it to /usr/local/bin. VERSION is a
# tag such as v0.2.0; the default is the latest release. Run as root.
set -euo pipefail

REPO=shantanu-1607/Raftra
VERSION=${1:-latest}
arch=$(dpkg --print-architecture) # amd64 or arm64
asset="raftra-server_linux_${arch}.tar.gz"
if [ "$VERSION" = latest ]; then
	base="https://github.com/$REPO/releases/latest/download"
else
	base="https://github.com/$REPO/releases/download/$VERSION"
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Downloading $asset ($VERSION) ..."
curl -fsSL -o "$tmp/$asset" "$base/$asset"
curl -fsSL -o "$tmp/checksums.txt" "$base/checksums.txt"
(cd "$tmp" && grep " $asset\$" checksums.txt | sha256sum -c --quiet -) || {
	echo "Checksum mismatch for $asset: refusing to install" >&2
	exit 1
}
tar -xzf "$tmp/$asset" -C "$tmp" raftra-server
install -m 755 "$tmp/raftra-server" /usr/local/bin/raftra-server
echo "Installed /usr/local/bin/raftra-server ($VERSION)"
