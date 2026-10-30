#!/usr/bin/env bash
# Install tunnelbench on a Linux server.
#
#   Online:  bash <(curl -fsSL https://raw.githubusercontent.com/Art4Man/tunnelbench/main/install.sh)
#   Offline: copy tunnelbench_linux_<arch>.tar.gz and SHA256SUMS next to this
#            script (e.g. with scp) and run: sudo bash install.sh
set -euo pipefail

REPO="Art4Man/tunnelbench"
DEST="/usr/local/bin"

case "$(uname -m)" in
  x86_64 | amd64) ARCH=amd64 ;;
  aarch64 | arm64) ARCH=arm64 ;;
  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac
ARCHIVE="tunnelbench_linux_${ARCH}.tar.gz"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd || pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

if [[ -f "$HERE/$ARCHIVE" && -f "$HERE/SHA256SUMS" ]]; then
  echo "Installing from local files in $HERE"
  cp "$HERE/$ARCHIVE" "$HERE/SHA256SUMS" "$WORK/"
else
  BASE="https://github.com/${REPO}/releases/latest/download"
  echo "Downloading $ARCHIVE from GitHub"
  curl -fsSL -o "$WORK/$ARCHIVE" "$BASE/$ARCHIVE"
  curl -fsSL -o "$WORK/SHA256SUMS" "$BASE/SHA256SUMS"
fi

cd "$WORK"
grep " ${ARCHIVE}\$" SHA256SUMS | sha256sum -c - || { echo "checksum mismatch; not installing" >&2; exit 1; }
tar -xzf "$ARCHIVE" tunnelbench
SUDO=""
[[ $EUID -ne 0 ]] && SUDO="sudo"
$SUDO install -m 0755 tunnelbench "$DEST/tunnelbench"
echo "Installed: $("$DEST/tunnelbench" version)"
