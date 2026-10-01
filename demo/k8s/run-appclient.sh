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

# Run an appclient role for the Multigres Migrator migration demo, in the foreground
# (each role in its own terminal). The source is the freestanding container
# started by ./launch-migration-source.sh (host port 5433); the Multigres
# gateway is the port-forward from ./launch-multigres-cluster.sh (localhost:15432).
#
# Roles:
#   write          continuous ledger writes against the SOURCE; SIGUSR1 fails
#                  the app over to the gateway.
#   failover       cut the running `write` client over to the gateway (sends it
#                  SIGUSR1) — the command-line trigger for the app failover. After
#                  this, writes go to the gateway and buffer until the migration
#                  is activated (serving), then flow.
#   watch-source   live summary of the SOURCE ledger (first WATCH_LIMIT accounts by id).
#   watch-gateway  live summary of the MULTIGRES ledger (same window; fills in once streaming).
#
# Prereqs: source up (write / watch-source), gateway port-forward up (write's
# failover target / watch-gateway).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Config (override via env).
SRC_HOST_PORT="${SRC_HOST_PORT:-5433}"
SRC_PASSWORD="${SRC_PASSWORD:-sourcepass}"
GW_HOST="${GW_HOST:-localhost}"
GW_PORT="${GW_PORT:-15432}"
GW_PASSWORD="${GW_PASSWORD:-postgres}"
TABLE="${TABLE:-public.accounts}"
ACCOUNTS="${ACCOUNTS:-1000}"
INTERVAL="${INTERVAL:-50ms}"
# Rows the watch panes render. The ledger is large (ACCOUNTS), but the panes show
# only the first WATCH_LIMIT accounts ordered by id — a small window that fits a
# tmux quadrant. Both watch roles use the same limit and ordering, so the source
# and gateway panes always display the identical subset.
WATCH_LIMIT="${WATCH_LIMIT:-12}"

RED='\033[0;31m'
NC='\033[0m'

SOURCE_DSN="host=localhost port=${SRC_HOST_PORT} user=postgres password=${SRC_PASSWORD} dbname=postgres sslmode=disable"
GATEWAY_DSN="host=${GW_HOST} port=${GW_PORT} user=postgres password=${GW_PASSWORD} dbname=postgres sslmode=disable"

usage() {
  echo "usage: $0 {write|failover|watch-source|watch-gateway}" >&2
  exit 2
}

[[ $# -eq 1 ]] || usage

if [[ ! -x "$REPO_ROOT/bin/appclient" ]]; then
  (cd "$REPO_ROOT" && go build -o bin/appclient ./demo/appclient)
fi
APP="$REPO_ROOT/bin/appclient"

case "$1" in
write)
  echo "writing to SOURCE (localhost:${SRC_HOST_PORT}); fails over to gateway (localhost:${GW_PORT}) on SIGUSR1"
  echo "  cut over with:  $0 failover"
  exec "$APP" write \
    --source-dsn "$SOURCE_DSN" \
    --target-dsn "$GATEWAY_DSN" \
    --table "$TABLE" --accounts "$ACCOUNTS" --interval "$INTERVAL" --verify-interval 5s \
    --transfers-only
  ;;
watch-source)
  exec "$APP" watch --source-dsn "$SOURCE_DSN" --target-dsn "$GATEWAY_DSN" --table "$TABLE" --limit "$WATCH_LIMIT"
  ;;
watch-gateway)
  exec "$APP" watch --source-dsn "$GATEWAY_DSN" --table "$TABLE" --limit "$WATCH_LIMIT"
  ;;
failover)
  # Signal the running (background) write client to cut over from the source to
  # the gateway. || true so `set -e` does not abort when pgrep finds nothing.
  pids=$(pgrep -f 'appclient write' || true)
  if [[ -z "$pids" ]]; then
    echo -e "${RED}no 'appclient write' process is running — start the dashboard (or '$0 write') first${NC}" >&2
    exit 1
  fi
  # shellcheck disable=SC2086 # word-split the pid list intentionally
  kill -USR1 $pids
  echo "failed the write client over to the gateway (SIGUSR1 -> pid(s): $pids)"
  echo "  writes now target the gateway; they buffer until the migration is activated (serving), then flow."
  ;;
*)
  echo -e "${RED}unknown role: $1${NC}" >&2
  usage
  ;;
esac
