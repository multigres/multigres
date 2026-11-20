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

# Soft-reset the migration demo to a clean slate WITHOUT touching the kind
# cluster or the infra: it drops any migration, stops the writer, drops the
# copied target table, and recreates+reseeds the standalone source. The
# multipoolers, multiorch, multigateway, etcd, observability, and all
# port-forwards keep running. (Use ./teardown.sh only to destroy the cluster.)
#
# Steps:
#   1. stop the background write client
#   2. drop every migration (--force: works even when stalled/streaming; also
#      tears down the subscription/publication/slot)
#   3. drop the copied target table (drop-migration leaves it, and a fresh
#      migration's schema-copy does CREATE TABLE, which would fail if it exists)
#   4. recreate + reseed the source (unless --keep-source)
#
# Usage: ./reset-demo.sh [--keep-source]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Config (override via env).
ADMIN="${ADMIN_SERVER:-localhost:18070}"
GW_HOST="${GW_HOST:-localhost}"
GW_PORT="${GW_PORT:-15432}"
GW_PASSWORD="${GW_PASSWORD:-postgres}"
TARGET_DB="${TARGET_DB:-postgres}"
TABLE="${TABLE:-public.accounts}"
SRC_NAME="${SRC_NAME:-pg-source}"
# psql client (see demo-dashboard.sh). Bare `psql` is often not on PATH; override
# per machine, e.g. PSQL=psql-18. Inherited when run from demo-dashboard.sh.
PSQL="${PSQL:-psql}"
MG="$REPO_ROOT/bin/multigres"

KEEP_SOURCE=0
for arg in "$@"; do
  case "$arg" in
  --keep-source) KEEP_SOURCE=1 ;;
  -h | --help)
    sed -n '16,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
  *)
    echo "unknown arg: $arg (try --keep-source)" >&2
    exit 2
    ;;
  esac
done

command -v jq >/dev/null || {
  echo "jq is required (brew install jq)." >&2
  exit 1
}
[[ -x "$MG" ]] || {
  echo "multigres binary not found at $MG — build it with 'make build'." >&2
  exit 1
}

# 1. Stop the background writer (harmless if none is running).
if pkill -f 'appclient write' 2>/dev/null; then
  echo "[reset] stopped the write client"
else
  echo "[reset] no write client running"
fi

# 2. Drop every migration, forcibly.
ids="$("$MG" list-migrations --admin-server "$ADMIN" 2>/dev/null | jq -r '.migrations[]?.id' 2>/dev/null || true)"
if [[ -z "$ids" ]]; then
  echo "[reset] no migrations to drop"
else
  for id in $ids; do
    echo "[reset] dropping migration $id"
    "$MG" drop-migration --admin-server "$ADMIN" --id "$id" --force >/dev/null ||
      echo "[reset]   warning: drop failed for $id (continuing)"
  done
fi

# 3. Drop the copied target table so the next migration's schema-copy succeeds.
if command -v "$PSQL" >/dev/null; then
  PGPASSWORD="$GW_PASSWORD" "$PSQL" "host=$GW_HOST port=$GW_PORT user=postgres dbname=$TARGET_DB sslmode=disable" \
    -v ON_ERROR_STOP=1 -c "DROP TABLE IF EXISTS $TABLE" >/dev/null &&
    echo "[reset] dropped target table $TABLE" ||
    echo "[reset]   warning: could not drop $TABLE (drop it manually before re-migrating)"
else
  echo "[reset]   psql client '$PSQL' not found; drop target table $TABLE manually (or set PSQL)" >&2
fi

# 4. Recreate + reseed the standalone source.
if [[ "$KEEP_SOURCE" == "1" ]]; then
  echo "[reset] keeping source '$SRC_NAME' as-is (--keep-source)"
else
  echo "[reset] recreating + reseeding source '$SRC_NAME'..."
  "$SCRIPT_DIR/launch-migration-source.sh"
fi

echo "[reset] done — cluster and infra untouched. Re-run the demo (./demo-dashboard.sh) when ready."
