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
#
# Bootstraps and supervises an all-in-one Multigres cluster inside a single
# container. `multigres cluster start` launches every service as a child
# process and then returns, so this script keeps PID 1 alive afterwards and
# tears the cluster down cleanly on SIGTERM/SIGINT.

set -euo pipefail

CONFIG_PATH="${MULTIGRES_CONFIG_PATH:-/multigres/cluster}"

# Number of cells to run. Each cell is a full stack: one PostgreSQL + pgctld,
# one multipooler, one multiorch, and one multigateway. The local provisioner
# bootstraps the shard with an AtLeastN(2) durability policy, so the cluster
# needs at least 2 poolers to elect a leader — a single cell never becomes
# ready. `multigres cluster init` generates three cells, so the supported range
# is 2-3. Default 2 is the minimum that serves queries; use 3 for a cluster
# that tolerates losing a node.
NUM_CELLS="${MULTIGRES_NUM_CELLS:-2}"

# Port the zone1 multigateway listens on for the PostgreSQL wire protocol.
# Defaults to multigres' standard 15432. Set to 5432 to make the container a
# drop-in PostgreSQL on the default port (e.g. as Supabase Storage's
# `tenant_db`). Additional cells use consecutive ports (base+1, base+2).
GATEWAY_PG_PORT="${MULTIGRES_GATEWAY_PG_PORT:-15432}"

# trim_cells rewrites a generated multigres.yaml in place, keeping only the
# first ${NUM_CELLS} cells. It deletes the extra cell blocks from both the
# provisioner-config `cells:` map and the `topology.cells:` list. Keeping
# `cluster init` as the source of truth (rather than hand-writing the config)
# means new config fields are picked up automatically.
trim_cells() {
  local file="$1" keep="$2" tmp
  tmp="$(mktemp)"
  awk -v keep="${keep}" '
    function zonenum(s,  t) { t = s; gsub(/[^0-9]/, "", t); return t + 0 }
    # Inside a cell-map block being deleted: drop deeper-indented lines, stop
    # at the next line indented 8 spaces or less.
    skip_map == 1 { if ($0 ~ /^         /) { next } else { skip_map = 0 } }
    /^        zone[0-9]+:[[:space:]]*$/ { if (zonenum($0) > keep) { skip_map = 1; next } }
    # topology.cells list entry plus its following root-path line.
    /^            - name: zone[0-9]+[[:space:]]*$/ { if (zonenum($0) > keep) { skip_topo = 1; next } }
    skip_topo == 1 { skip_topo = 0; if ($0 ~ /^              root-path:/) { next } }
    { print }
  ' "${file}" >"${tmp}"
  mv "${tmp}" "${file}"
}

# set_gateway_ports rewrites each kept cell's multigateway pg-port to
# ${GATEWAY_PG_PORT}+offset. `cluster init` generates these as 15432, 15433,
# 15434 (one per cell), which are distinct from the multipooler/pgctld pg-port
# (25432+), so anchoring on the exact generated value only matches the gateway.
set_gateway_ports() {
  local file="$1" base="$2" keep="$3" i src dst
  for i in $(seq 1 "${keep}"); do
    src=$((15432 + i - 1))
    dst=$((base + i - 1))
    sed -i "s/^\(                pg-port: \)${src}\$/\1${dst}/" "${file}"
  done
}

shutdown() {
  echo "==> Received shutdown signal, stopping Multigres cluster..."
  multigres cluster stop --config-path "${CONFIG_PATH}" || true
  exit 0
}
trap shutdown TERM INT

if ! [[ "${NUM_CELLS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "MULTIGRES_NUM_CELLS must be a positive integer, got '${NUM_CELLS}'" >&2
  exit 1
fi
if [ "${NUM_CELLS}" -lt 2 ]; then
  echo "==> MULTIGRES_NUM_CELLS=${NUM_CELLS} is below the minimum of 2 (the shard needs at least 2 poolers to elect a leader); using 2."
  NUM_CELLS=2
fi
if [ "${NUM_CELLS}" -gt 3 ]; then
  echo "==> MULTIGRES_NUM_CELLS=${NUM_CELLS} exceeds the 3 cells 'cluster init' generates; using 3."
  NUM_CELLS=3
fi
if ! [[ "${GATEWAY_PG_PORT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "MULTIGRES_GATEWAY_PG_PORT must be a positive integer, got '${GATEWAY_PG_PORT}'" >&2
  exit 1
fi

mkdir -p "${CONFIG_PATH}"

# `cluster init` refuses to overwrite an existing config, so only initialize
# when there isn't one (e.g. on first start, or every start when the working
# directory is ephemeral).
if [ ! -f "${CONFIG_PATH}/multigres.yaml" ]; then
  echo "==> Initializing Multigres cluster configuration (${NUM_CELLS} cell(s)) in ${CONFIG_PATH}..."
  multigres cluster init --config-path "${CONFIG_PATH}"
  trim_cells "${CONFIG_PATH}/multigres.yaml" "${NUM_CELLS}"
  set_gateway_ports "${CONFIG_PATH}/multigres.yaml" "${GATEWAY_PG_PORT}" "${NUM_CELLS}"
fi

echo "==> Starting Multigres cluster..."
multigres cluster start --config-path "${CONFIG_PATH}" --wait-for-bootstrap

echo "==> Multigres cluster is ready."
echo "    - PostgreSQL (zone1 multigateway): port ${GATEWAY_PG_PORT}"
echo "    - multigateway HTTP:               port 15100"
echo "    - multiadmin HTTP:                 port 15000"

# Keep PID 1 alive so the orphaned cluster processes keep running. Exit
# non-zero if the gateway dies so the container is marked unhealthy / restarted.
# `sleep & wait` (instead of a bare sleep) lets the signal trap fire promptly.
while true; do
  if ! pgrep -x multigateway >/dev/null 2>&1; then
    echo "==> multigateway is no longer running; exiting." >&2
    exit 1
  fi
  sleep 5 &
  wait $!
done
