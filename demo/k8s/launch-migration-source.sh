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

# Start (or restart) the freestanding migration SOURCE — a plain PGDG
# postgres:17 container with logical replication enabled — on the kind Docker
# network, and seed it with the appclient ledger. This is the external database
# a Multigres Migrator migration imports FROM; nothing multigres-specific runs in it.
#
# Run after the cluster is up (launch-infra.sh + launch-multigres-cluster.sh).
# Then drive the migration with the create-migration command printed at the end,
# and start the workload with ./run-appclient.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Config (override via env).
SRC_NAME="${SRC_NAME:-pg-source}"
SRC_PASSWORD="${SRC_PASSWORD:-sourcepass}"
SRC_HOST_PORT="${SRC_HOST_PORT:-5433}"    # published on the host for appclient/psql
KIND_CLUSTER="${KIND_CLUSTER:-multidemo}" # kind cluster name (from kind.yaml)
KIND_NETWORK="${KIND_NETWORK:-kind}"      # docker network kind creates for the cluster
PG_IMAGE="${PG_IMAGE:-postgres:17}"
TABLE="${TABLE:-public.accounts}"
# Seed a large ledger so the demo migrates a big table; the watch panes show only
# a small window of it (see run-appclient.sh WATCH_LIMIT). Accounts get sequential
# ids 1..ACCOUNTS, so the window (lowest ids) is a stable, identical subset on both
# the source and the target.
ACCOUNTS="${ACCOUNTS:-1000}"
INITIAL_BALANCE="${INITIAL_BALANCE:-1000}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# The cluster must be up (the kind Docker network lingers after `kind delete`,
# so check the cluster itself, not just the network). The container then sits on
# that network so the in-cluster target Postgres can dial it for the
# subscription CONNECTION.
if ! kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
  echo -e "${RED}kind cluster '$KIND_CLUSTER' not found — run ./launch-infra.sh && ./launch-multigres-cluster.sh first.${NC}"
  exit 1
fi
if ! docker network inspect "$KIND_NETWORK" >/dev/null 2>&1; then
  echo -e "${RED}Docker network '$KIND_NETWORK' not found (cluster up but network missing?).${NC}"
  exit 1
fi

echo -e "${BLUE}Starting migration source '$SRC_NAME' ($PG_IMAGE) on network '$KIND_NETWORK'...${NC}"
docker rm -f "$SRC_NAME" >/dev/null 2>&1 || true
docker run -d --name "$SRC_NAME" --network "$KIND_NETWORK" -p "${SRC_HOST_PORT}:5432" \
  -e POSTGRES_PASSWORD="$SRC_PASSWORD" \
  "$PG_IMAGE" \
  -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10 >/dev/null

echo -n "Waiting for the source to accept connections"
for _ in $(seq 1 30); do
  if docker exec "$SRC_NAME" pg_isready -U postgres >/dev/null 2>&1; then break; fi
  echo -n "."
  sleep 1
done
echo
docker exec "$SRC_NAME" pg_isready -U postgres

# Build appclient (once) and seed the ledger on the source.
if [[ ! -x "$REPO_ROOT/bin/appclient" ]]; then
  echo -e "${BLUE}Building appclient...${NC}"
  (cd "$REPO_ROOT" && go build -o bin/appclient ./demo/appclient)
fi
echo -e "${BLUE}Seeding ledger $TABLE ($ACCOUNTS accounts x $INITIAL_BALANCE)...${NC}"
"$REPO_ROOT/bin/appclient" setup \
  --source-dsn "host=localhost port=${SRC_HOST_PORT} user=postgres password=${SRC_PASSWORD} dbname=postgres sslmode=disable" \
  --table "$TABLE" --accounts "$ACCOUNTS" --initial-balance "$INITIAL_BALANCE"

# The in-cluster address the migration's subscription must use (pod DNS can't
# resolve the container name, so use the IP on the kind network).
SRC_IP="$(docker inspect "$SRC_NAME" | jq -r --arg net "$KIND_NETWORK" '.[0].NetworkSettings.Networks[$net].IPAddress')"

echo
echo -e "${GREEN}Migration source ready.${NC}"
echo "  host address (appclient/psql):     host=localhost port=${SRC_HOST_PORT} user=postgres password=${SRC_PASSWORD} dbname=postgres"
echo -e "  in-cluster address (subscription): ${YELLOW}host=${SRC_IP} port=5432${NC} user=postgres password=${SRC_PASSWORD} dbname=postgres"
echo
echo "Create the migration (multiadmin gRPC is port-forwarded on localhost:18070):"
echo -e "  ${BLUE}bin/multigres create-migration --admin-server localhost:18070 \\
    --source-dsn \"host=${SRC_IP} port=5432 user=postgres password=${SRC_PASSWORD} dbname=postgres sslmode=disable\" \\
    --target-database postgres --tables ${TABLE}${NC}"
echo
echo "Then: bin/multigres start-migration --admin-server localhost:18070 --id <id>"
echo "Workload: ./run-appclient.sh write   (T2) ·   ./run-appclient.sh watch-gateway   (T3)"
