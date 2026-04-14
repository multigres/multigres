#!/usr/bin/env bash
# Copyright 2026 Supabase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Manages the port pool server for local development.
#
# Usage:
#   scripts/portpool.sh start    # Ensure the server is running (idempotent)
#   scripts/portpool.sh stop     # Stop the server
#   scripts/portpool.sh status   # Check if the server is running
#
# The start command is designed to be called before every test run:
#   scripts/portpool.sh start && MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock go test ./...

set -euo pipefail

SOCKET="/tmp/multigres-port-pool.sock"
PIDFILE="/tmp/multigres-port-pool.pid"
BINARY="bin/portpoolserver"

is_running() {
  if [[ -f "$PIDFILE" ]]; then
    local pid
    pid=$(cat "$PIDFILE")
    if kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
    rm -f "$PIDFILE"
  fi
  return 1
}

cmd_start() {
  if is_running; then
    return 0
  fi

  if [[ ! -x "$BINARY" ]]; then
    echo "warning: $BINARY not found; run 'make build' first. Port allocation will not be coordinated." >&2
    return 0
  fi

  rm -f "$SOCKET"
  "$BINARY" --socket "$SOCKET" &
  echo $! >"$PIDFILE"
}

cmd_stop() {
  if ! is_running; then
    rm -f "$SOCKET"
    return 0
  fi

  kill "$(cat "$PIDFILE")" 2>/dev/null || true
  rm -f "$PIDFILE" "$SOCKET"
}

cmd_status() {
  if is_running; then
    echo "running (pid $(cat "$PIDFILE"))"
  else
    echo "not running"
  fi
}

case "${1:-}" in
start) cmd_start ;;
stop) cmd_stop ;;
status) cmd_status ;;
*)
  echo "Usage: $0 {start|stop|status}" >&2
  exit 1
  ;;
esac
