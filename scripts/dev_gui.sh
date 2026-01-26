#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v pnpm >/dev/null 2>&1; then
  echo "error: pnpm not found (needed to build the GUI frontend)" >&2
  exit 1
fi

if [[ ! -d ui/node_modules ]]; then
  pnpm -C ui install
fi

export WEBKIT_DISABLE_DMABUF_RENDERER=1
export GDK_BACKEND=wayland

cleanup() {
  if [[ -n "${VITE_PID:-}" ]]; then
    kill "$VITE_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

pnpm -C ui dev >/dev/null 2>&1 &
VITE_PID="$!"

echo "Starting Vite dev server (pid=$VITE_PID)..." >&2
for _ in {1..60}; do
  if curl -fsS "http://localhost:5173" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

if ! curl -fsS "http://localhost:5173" >/dev/null 2>&1; then
  echo "error: Vite dev server did not start on http://localhost:5173" >&2
  exit 1
fi

exec cargo run -p bbr-client-gui -- "$@"
