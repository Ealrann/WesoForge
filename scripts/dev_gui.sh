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

VITE_HOST="127.0.0.1"
VITE_PORT="5173"
VITE_LOG="$(mktemp -t wesoforge-vite.XXXXXX.log)"

cleanup() {
  if [[ -n "${VITE_PID:-}" ]]; then
    kill "$VITE_PID" >/dev/null 2>&1 || true
  fi
  rm -f "$VITE_LOG" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

if ss -ltnp 2>/dev/null | grep -qE ":${VITE_PORT}\\s"; then
  existing_pids="$(
    ss -ltnp 2>/dev/null \
      | grep -E ":${VITE_PORT}\\s" \
      | grep -oE "pid=[0-9]+" \
      | cut -d= -f2 \
      | sort -u \
      || true
  )"

  if [[ -n "$existing_pids" ]]; then
    echo "Port ${VITE_PORT} is already in use; attempting to stop the existing Vite dev server(s)..." >&2
    for pid in $existing_pids; do
      cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
      if [[ "$cmd" == *"vite"* ]]; then
        kill "$pid" >/dev/null 2>&1 || true
      fi
    done
  fi

  for _ in {1..50}; do
    if ! ss -ltnp 2>/dev/null | grep -qE ":${VITE_PORT}\\s"; then
      break
    fi
    sleep 0.1
  done
fi

if ss -ltnp 2>/dev/null | grep -qE ":${VITE_PORT}\\s"; then
  echo "error: port ${VITE_PORT} is still in use; please stop the process using it and retry." >&2
  exit 1
fi

pnpm -C ui dev --host "$VITE_HOST" --port "$VITE_PORT" >"$VITE_LOG" 2>&1 &
VITE_PID="$!"

echo "Starting Vite dev server (pid=$VITE_PID)..." >&2
for _ in {1..60}; do
  if ! kill -0 "$VITE_PID" >/dev/null 2>&1; then
    echo "error: Vite dev server exited early. Logs:" >&2
    tail -n 200 "$VITE_LOG" >&2 || true
    exit 1
  fi
  if curl -fsS "http://${VITE_HOST}:${VITE_PORT}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

if ! curl -fsS "http://${VITE_HOST}:${VITE_PORT}" >/dev/null 2>&1; then
  echo "error: Vite dev server did not start on http://${VITE_HOST}:${VITE_PORT}. Logs:" >&2
  tail -n 200 "$VITE_LOG" >&2 || true
  exit 1
fi

exec cargo run -p bbr-client-gui -- "$@"
