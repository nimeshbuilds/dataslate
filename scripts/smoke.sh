#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
DATASLATE_PYTHON="${PYTHON:-python3}"
DATASLATE_SMOKE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dataslate-smoke.XXXXXX")"
DATASLATE_SMOKE_PID=""
cleanup() {
  if [[ -n "$DATASLATE_SMOKE_PID" ]]; then
    kill "$DATASLATE_SMOKE_PID" 2>/dev/null || true
    wait "$DATASLATE_SMOKE_PID" 2>/dev/null || true
  fi
  rm -rf "$DATASLATE_SMOKE_DIR"
}
trap cleanup EXIT
DATASLATE_SMOKE_PORT="$($DATASLATE_PYTHON -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
(cd api && go build -o "$DATASLATE_SMOKE_DIR/api" ./cmd/dataslate-api)
DATASLATE_AUTH_MODE=development DATASLATE_ADDR="127.0.0.1:$DATASLATE_SMOKE_PORT" DATASLATE_DATABASE_URL= "$DATASLATE_SMOKE_DIR/api" >"$DATASLATE_SMOKE_DIR/api.log" 2>&1 &
DATASLATE_SMOKE_PID=$!
for attempt in $(seq 1 50); do
  if ! kill -0 "$DATASLATE_SMOKE_PID" 2>/dev/null; then
    cat "$DATASLATE_SMOKE_DIR/api.log"
    exit 1
  fi
  if curl --silent --fail "http://127.0.0.1:$DATASLATE_SMOKE_PORT/readyz" >/dev/null; then
    break
  fi
  sleep 0.2
done
PYTHONPATH=sdk/python/src "$DATASLATE_PYTHON" scripts/smoke_api.py "http://127.0.0.1:$DATASLATE_SMOKE_PORT"
