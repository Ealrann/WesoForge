#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "error: AppImage builds are only supported on Linux." >&2
  exit 1
fi

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

if ! command -v pnpm >/dev/null 2>&1; then
  echo "error: pnpm not found (needed to build the Svelte frontend)." >&2
  exit 1
fi

if [[ ! -d ui/node_modules ]]; then
  pnpm -C ui install
fi

if ! cargo tauri --help >/dev/null 2>&1; then
  cat >&2 <<'EOF'
error: `cargo tauri` not found.

Install the Tauri CLI (cargo subcommand) first, for example:
  cargo install tauri-cli
EOF
  exit 1
fi

DIST_DIR="${DIST_DIR:-$ROOT/dist}"
mkdir -p "$DIST_DIR"

echo "Building WesoForge GUI AppImage..." >&2
(
  cd crates/client-gui
  # linuxdeploy's bundled `strip` can be too old for modern `.relr.dyn` ELF sections.
  # Skip stripping to keep the AppImage build reliable across distros.
  export NO_STRIP=1
  cargo tauri build --features prod-backend --bundles appimage -- --locked
)

APPIMAGE_DIR="$TARGET_DIR/release/bundle/appimage"
APPIMAGE_SRC="$(ls -1t "$APPIMAGE_DIR"/*.AppImage 2>/dev/null | head -n 1 || true)"
if [[ -z "$APPIMAGE_SRC" ]]; then
  echo "error: no AppImage found under: $APPIMAGE_DIR" >&2
  exit 1
fi

VERSION="$(workspace_version)"
if [[ -z "${VERSION:-}" ]]; then
  echo "error: failed to determine workspace version from Cargo.toml" >&2
  exit 1
fi
ARCH="$(platform_arch)"

APPIMAGE_DST="$DIST_DIR/WesoForge-gui_${VERSION}_${ARCH}.AppImage"
install -m 0755 "$APPIMAGE_SRC" "$APPIMAGE_DST"
echo "Wrote: $APPIMAGE_DST" >&2
