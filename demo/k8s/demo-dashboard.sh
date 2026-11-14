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

# Four-pane tmux dashboard for the migration demo:
#
#   +-------------------------+-------------------------+
#   | 1: source psql console  | 2: target psql console  |
#   +-------------------------+-------------------------+
#   | 3: source balance       | 4: target (Multigres)   |
#   +-------------------------+-------------------------+
#
# The two top panes are plain shells with libpq env (PGHOST/PGPORT/PGUSER/
# PGPASSWORD/PGDATABASE) preset — pane 1 for the source, pane 2 for the Multigres
# gateway — so `psql` connects with no options. The two bottom panes are the live
# source/target balance views (unchanged). The automated write client runs in the
# background (log: $WRITE_LOG) so the balances keep moving.
#
# Prereqs: the cluster up with port-forwards (18070 admin gRPC, 15432 gateway),
# the source started (./launch-migration-source.sh), and tmux + jq + psql +
# bin/multigres.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Config (override via env).
ADMIN="${ADMIN_SERVER:-localhost:18070}"
SESSION="${SESSION:-migration-demo}"
SRC_NAME="${SRC_NAME:-pg-source}"
KIND_NETWORK="${KIND_NETWORK:-kind}"
SRC_PASSWORD="${SRC_PASSWORD:-sourcepass}"
SRC_HOST_PORT="${SRC_HOST_PORT:-5433}"
# Multigres gateway (target) — host-reachable port-forward for the psql console.
GW_HOST="${GW_HOST:-localhost}"
GW_PORT="${GW_PORT:-15432}"
GW_PASSWORD="${GW_PASSWORD:-postgres}"
TABLE="${TABLE:-public.accounts}"
WRITE_LOG="${WRITE_LOG:-/tmp/appclient-write.log}"
# psql client. A bare `psql` is often not on PATH (Homebrew, for one, installs it
# keg-only as psql-<major>), and where it lives varies per machine — so set PSQL
# to the binary to use (a name on PATH like `psql-18`, or an absolute path). The
# console panes get a `psql` shim that dispatches to it, so you still type `psql`.
PSQL="${PSQL:-psql}"
MG="$REPO_ROOT/bin/multigres"

# Auto-reset the Multigres (target) side on start so a fresh run always boots
# cleanly. A prior run leaves the copied target table behind (drop-migration does
# not remove it), and a new migration's schema-copy does a plain CREATE TABLE that
# fails with "relation already exists" — so without this the demo won't bootstrap.
# Default: drop leftover migrations + the copied target table, leave the source
# as-is (--keep-source). --reset-source also recreates+reseeds the source (slower);
# --no-reset skips the reset entirely.
RESET=1
RESET_SOURCE=0
for arg in "$@"; do
  case "$arg" in
  --no-reset) RESET=0 ;;
  --reset-source) RESET_SOURCE=1 ;;
  -h | --help)
    echo "usage: $0 [--no-reset] [--reset-source]" >&2
    echo "  (default: reset the target side — drop migrations + copied table — keeping the source)" >&2
    exit 0
    ;;
  *)
    echo "unknown arg: $arg (try --no-reset or --reset-source)" >&2
    exit 2
    ;;
  esac
done

for bin in tmux jq; do
  if ! command -v "$bin" >/dev/null; then
    echo "$bin is required (brew install $bin)." >&2
    exit 1
  fi
done
if ! command -v "$PSQL" >/dev/null; then
  echo "psql client '$PSQL' not found — set PSQL to your psql binary, e.g. PSQL=psql-18 $0" >&2
  exit 1
fi

if ! [[ -x "$MG" ]]; then
  echo "multigres binary not found at $MG — build it with 'make build'." >&2
  exit 1
fi

# In-cluster source IP for the generated SRC_DSN. Guard the case where the source
# container isn't up yet: warn and fall back to a visible placeholder rather than
# silently generating an unusable host= value.
SRC_IP=""
if docker inspect "$SRC_NAME" >/dev/null 2>&1; then
  SRC_IP=$(docker inspect "$SRC_NAME" | jq -r --arg net "$KIND_NETWORK" '.[0].NetworkSettings.Networks[$net].IPAddress // empty')
fi
if [[ -z "$SRC_IP" ]]; then
  echo "warning: source '$SRC_NAME' not found on network '$KIND_NETWORK' — start ./launch-migration-source.sh first." >&2
  echo "         SRC_DSN will use a placeholder host until then." >&2
  SRC_IP="<SRC_IP>"
fi
SRC_DSN="host=$SRC_IP port=5432 user=postgres password=$SRC_PASSWORD dbname=postgres sslmode=disable"
# Host-reachable source address (docker port-forward) for psql run from the host.
SRC_LOCAL_DSN="host=localhost port=$SRC_HOST_PORT user=postgres password=$SRC_PASSWORD dbname=postgres sslmode=disable"

# Generate a helpers file the command pane sources: the resolved source DSNs plus
# an `mg` wrapper (bin/multigres with --admin-server prefilled). It is GENERATED
# rather than a committed file so the environment-specific values (source IP,
# admin address) are always current, and it is sourced by absolute path so there
# is no coupling to the pane's working directory. `mg` uses the absolute binary
# path so it works from any cwd. The command reference lives in the runbook
# (demo-migration-runbook.md), so the pane stays uncluttered.
HELPERS="${TMPDIR:-/tmp}/${SESSION}-helpers.sh"
{
  printf '%s\n' "# Auto-generated by demo-dashboard.sh — sourced in the console panes. Do not edit."
  printf 'SRC_DSN="%s"\n' "$SRC_DSN"
  # In-cluster source host on its own — the only per-run-variable field in the
  # gateway `CREATE CONNECTION` OPTIONS. Handy as `psql -v src_host="$SRC_HOST"`.
  printf 'SRC_HOST="%s"\n' "$SRC_IP"
  printf 'SRC_LOCAL_DSN="%s"\n' "$SRC_LOCAL_DSN"
  printf 'mg() { "%s" "$@" --admin-server "%s"; }\n' "$MG" "$ADMIN"
  # psql shim so typing `psql` in a console pane dispatches to $PSQL (which may be
  # psql-18 etc.). A function, not an alias: it survives a leading VAR=x prefix.
  printf 'psql() { command "%s" "$@"; }\n' "$PSQL"
  # failover-app: one-word trigger to cut the background write client over to the
  # gateway (the pane cwd is the repo root, so call run-appclient.sh by abs path).
  printf 'failover-app() { "%s/run-appclient.sh" failover; }\n' "$SCRIPT_DIR"
} >"$HELPERS"

# Reset the target side before launching the panes, so a re-run of the demo never
# trips over a leftover copied table. reset-demo.sh stops any stray writer, drops
# every migration, and drops the copied target table; --keep-source leaves the
# standalone source untouched (default) unless --reset-source was passed. Guarded
# so a reset hiccup warns but does not abort the dashboard.
if [[ "$RESET" == "1" ]]; then
  echo "resetting the target side before start (pass --no-reset to skip)..."
  # Explicit branches rather than an empty array: bash 3.2 (macOS default) errors
  # on "${arr[@]}" for an empty array under `set -u`.
  reset_ok=1
  if [[ "$RESET_SOURCE" == "1" ]]; then
    "$SCRIPT_DIR/reset-demo.sh" || reset_ok=0
  else
    "$SCRIPT_DIR/reset-demo.sh" --keep-source || reset_ok=0
  fi
  if [[ "$reset_ok" == "0" ]]; then
    echo "warning: reset-demo.sh reported a problem — continuing; drop $TABLE manually if the migration won't bootstrap." >&2
  fi
fi

# Build a 2x2 layout: 0=top-left, 1=top-right, 2=bottom-left, 3=bottom-right.
tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -c "$REPO_ROOT"
tmux split-window -h -t "$SESSION:0" -c "$REPO_ROOT"
tmux split-window -v -t "$SESSION:0.0" -c "$REPO_ROOT"
tmux split-window -v -t "$SESSION:0.1" -c "$REPO_ROOT"
tmux select-layout -t "$SESSION:0" tiled

tmux set-window-option -t "$SESSION:0" pane-border-status top
tmux set-window-option -t "$SESSION:0" pane-border-format " #{pane_title} "
tmux select-pane -t "$SESSION:0.0" -T "1: source psql (run: psql)"
tmux select-pane -t "$SESSION:0.1" -T "2: target psql — Multigres gateway (run: psql)"
tmux select-pane -t "$SESSION:0.2" -T "3: source balance"
tmux select-pane -t "$SESSION:0.3" -T "4: target (Multigres) balance"

# Panes 3/4: source + target balance views, each wrapped in an until-loop so they
# self-heal — a watch exits non-zero when its ledger table is missing (the
# target's only exists once the migration streams) and the loop retries until it
# appears; a clean Ctrl-C exits 0 and stops looping.
tmux send-keys -t "$SESSION:0.2" "until '$SCRIPT_DIR/run-appclient.sh' watch-source; do echo 'waiting for source ledger…'; sleep 2; done" C-m
tmux send-keys -t "$SESSION:0.3" "until '$SCRIPT_DIR/run-appclient.sh' watch-gateway; do echo 'waiting for target ledger (start the migration)…'; sleep 2; done" C-m

# Pane 1 (source psql console): start the write client in the background (same
# self-healing loop, so it waits out a not-yet-seeded source) — it drives the
# balances in panes 3/4 — then source the generated helpers (mg / SRC_DSN /
# SRC_HOST / SRC_LOCAL_DSN) and preset the libpq env so `psql` connects to the SOURCE with no
# options. Leaves an interactive shell.
tmux send-keys -t "$SESSION:0.0" "(until '$SCRIPT_DIR/run-appclient.sh' write; do sleep 2; done) >'$WRITE_LOG' 2>&1 & disown" C-m
tmux send-keys -t "$SESSION:0.0" "source '$HELPERS'; export PGHOST=localhost PGPORT='$SRC_HOST_PORT' PGUSER=postgres PGPASSWORD='$SRC_PASSWORD' PGDATABASE=postgres PGSSLMODE=disable" C-m
tmux send-keys -t "$SESSION:0.0" "clear; echo 'SOURCE psql ready — run: psql   (mg / SRC_DSN loaded · writer log: $WRITE_LOG)'" C-m

# Pane 2 (target psql console): preset the libpq env so `psql` connects to the
# Multigres gateway with no options. This is where you drive the migration via the
# gateway SQL DDL interface (CREATE CONNECTION / CREATE MIGRATION / ALTER
# MIGRATION … ACTIVATE, etc.).
tmux send-keys -t "$SESSION:0.1" "source '$HELPERS'; export PGHOST='$GW_HOST' PGPORT='$GW_PORT' PGUSER=postgres PGPASSWORD='$GW_PASSWORD' PGDATABASE=postgres PGSSLMODE=disable" C-m
tmux send-keys -t "$SESSION:0.1" "clear; echo 'TARGET psql ready (Multigres gateway) — run: psql'" C-m
tmux select-pane -t "$SESSION:0.0"

if [[ -n "${TMUX:-}" ]]; then
  echo "Already inside tmux. Attach with: tmux switch-client -t $SESSION"
else
  tmux attach -t "$SESSION"
fi
