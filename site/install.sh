#!/bin/sh
# Raftra CLI installer (macOS and Linux).
#   curl -fsSL https://raw.githubusercontent.com/shantanu-1607/Raftra/main/site/install.sh | sh
# Environment overrides:
#   RAFTRA_INSTALL_DIR     where to install (default: $HOME/.local/bin)
#   RAFTRA_DOWNLOAD_BASE   where to download from (default: the latest GitHub release)
set -eu

REPO="shantanu-1607/Raftra"
BIN="raftra-cli"
INSTALL_DIR="${RAFTRA_INSTALL_DIR:-$HOME/.local/bin}"
BASE="${RAFTRA_DOWNLOAD_BASE:-https://github.com/$REPO/releases/latest/download}"

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
  linux|darwin) ;;
  *)
    echo "Unsupported OS: $os. Windows users: download the .zip from https://github.com/$REPO/releases/latest" >&2
    exit 1
    ;;
esac

arch=$(uname -m)
case "$arch" in
  x86_64|amd64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *)
    echo "Unsupported CPU architecture: $arch" >&2
    exit 1
    ;;
esac

asset="${BIN}_${os}_${arch}.tar.gz"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Downloading $asset ..."
curl -fsSL "$BASE/$asset" -o "$tmp/$asset"
curl -fsSL "$BASE/checksums.txt" -o "$tmp/checksums.txt"

expected=$(grep " $asset\$" "$tmp/checksums.txt" | awk '{print $1}')
if [ -z "$expected" ]; then
  echo "No checksum listed for $asset" >&2
  exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp/$asset" | awk '{print $1}')
else
  actual=$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')
fi
if [ "$expected" != "$actual" ]; then
  echo "Checksum mismatch for $asset: refusing to install" >&2
  exit 1
fi

tar -xzf "$tmp/$asset" -C "$tmp" "$BIN"
mkdir -p "$INSTALL_DIR"
mv "$tmp/$BIN" "$INSTALL_DIR/$BIN"
chmod +x "$INSTALL_DIR/$BIN"
echo "Installed $BIN to $INSTALL_DIR/$BIN"

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo "Note: $INSTALL_DIR is not on your PATH. Add this line to your shell profile:"
    echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
    ;;
esac
echo "Try it:  $BIN status"
