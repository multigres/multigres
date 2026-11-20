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

# Continuously show the migrations in the cluster and their state, as a compact
# table, using watch(1). Used as one pane of ./demo-dashboard.sh, but runnable on
# its own. `--once` renders a single frame (this is what watch re-invokes).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ADMIN="${ADMIN_SERVER:-localhost:18070}"
INTERVAL="${INTERVAL:-1}"
MG="${MULTIGRES_BIN:-$REPO_ROOT/bin/multigres}"

# Render {"migrations":[...]} (protojson, snake_case) as ID/PHASE/DIR/COPY/... .
JQ='(.migrations // []) as $m
| if ($m|length)==0 then "(none)"
  else
    (["ID","PHASE","DIR","COPY","CAUGHT_UP","TABLES"] | @tsv),
    ($m[] | [
       .id,
       (.phase // "?" | sub("MIGRATION_PHASE_";"")),
       (.active_direction // "?" | sub("MIGRATION_DIRECTION_";"")),
       "\(.ready_relations // 0)/\(.total_relations // 0)",
       (.caught_up // false | tostring),
       ((.tables // []) | join(","))
     ] | @tsv)
  end'

render() {
  printf 'admin %s\n\n' "$ADMIN"
  if out=$("$MG" list-migrations --admin-server "$ADMIN" 2>&1); then
    printf '%s\n' "$out" | jq -r "$JQ" | column -t -s "$(printf '\t')"
  else
    printf 'list-migrations failed:\n%s\n' "$out"
  fi
}

# watch re-invokes the script with --once to redraw each frame.
if [[ "${1:-}" == "--once" ]]; then
  render
  exit 0
fi

command -v watch >/dev/null || {
  echo "watch(1) is required (brew install watch)." >&2
  exit 1
}
command -v jq >/dev/null || {
  echo "jq is required (brew install jq)." >&2
  exit 1
}
if [[ ! -x "$MG" ]]; then
  echo "multigres binary not found at $MG — build it with 'make build' from the repo root." >&2
  exit 1
fi

# -t: no watch header (render prints its own); ADMIN/MG carry via the environment.
exec watch -t -n "$INTERVAL" "$0" --once
