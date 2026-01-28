#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

DIST_DIR="${DIST_DIR:-$ROOT/dist}"
mkdir -p "$DIST_DIR"

workspace_version() {
  awk '
    /^\[workspace\.package\]/ { in_pkg = 1; next }
    in_pkg && /^\[/ { in_pkg = 0 }
    in_pkg {
      if (match($0, /^version[[:space:]]*=[[:space:]]*"([^"]+)"/, m)) {
        print m[1];
        exit
      }
    }
  ' Cargo.toml
}

platform_arch() {
  local arch
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *) echo "$arch" ;;
  esac
}

TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
mkdir -p "$TARGET_DIR"
if ! : >"$TARGET_DIR/.wesoforge-write-test" 2>/dev/null; then
  echo "warning: CARGO_TARGET_DIR is not writable ($TARGET_DIR); using $ROOT/target instead" >&2
  TARGET_DIR="$ROOT/target"
  mkdir -p "$TARGET_DIR"
fi
rm -f "$TARGET_DIR/.wesoforge-write-test" >/dev/null 2>&1 || true
export CARGO_TARGET_DIR="$TARGET_DIR"

echo "Building WesoForge CLI (wesoforge)..." >&2
CARGO_ARGS=(build -p bbr-client --release --features prod-backend)
if [[ "${CARGO_LOCKED:-0}" == "1" ]]; then
  CARGO_ARGS+=(--locked)
fi
if [[ "${CARGO_OFFLINE:-0}" == "1" ]]; then
  CARGO_ARGS+=(--offline)
fi
cargo "${CARGO_ARGS[@]}"

VERSION="$(workspace_version)"
if [[ -z "${VERSION:-}" ]]; then
  echo "error: failed to determine workspace version from Cargo.toml" >&2
  exit 1
fi
ARCH="$(platform_arch)"

BIN_SRC="$TARGET_DIR/release/wesoforge"
BIN_DST="$DIST_DIR/WesoForge-cli_Linux_${VERSION}_${ARCH}"

if [[ ! -f "$BIN_SRC" ]]; then
  echo "error: expected binary not found at: $BIN_SRC" >&2
  exit 1
fi

install -m 0755 "$BIN_SRC" "$BIN_DST"
echo "Wrote: $BIN_DST" >&2
