#!/usr/bin/env bash
# Copyright 2025 Supabase, Inc.
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

set -euo pipefail

CONTEXT="kind-multidemo"
NAMESPACE="default"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting multigres cluster port-forwards...${NC}"

# Start all port-forwards in the background and capture PIDs
# kubectl will automatically stop these when the underlying resources are deleted
pids=()

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward service/multigateway 15432:15432 >/dev/null 2>&1 &
pids+=($!)

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward pod/multipooler-zone1-0 15433:5432 >/dev/null 2>&1 &
pids+=($!)

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward pod/multipooler-zone1-1 15434:5432 >/dev/null 2>&1 &
pids+=($!)

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward pod/multipooler-zone1-2 15435:5432 >/dev/null 2>&1 &
pids+=($!)

# Wait a moment for port-forwards to initialize
sleep 0.5

# Verify port-forwards are running
failed=0
for pid in "${pids[@]}"; do
  if ! kill -0 "$pid" 2>/dev/null; then
    ((failed++))
  fi
done

echo ""
if [ $failed -eq 0 ]; then
  echo -e "${GREEN}Multigres cluster port-forwards started successfully!${NC}"
else
  echo -e "${YELLOW}Warning: $failed port-forward(s) failed to start${NC}"
  echo -e "${YELLOW}This may be normal if pods are not yet ready or ports are already in use${NC}"
fi

echo ""
echo "========================================="
echo "Multigres Cluster Access URLs:"
echo "========================================="
echo ""
echo "PostgreSQL (via multigateway):"
echo "  psql --host=localhost --port=15432 -U postgres -d postgres"
echo ""
echo "Direct pooler access:"
echo "  psql --host=localhost --port=15433 -U postgres -d postgres  (zone1-0)"
echo "  psql --host=localhost --port=15434 -U postgres -d postgres  (zone1-1)"
echo "  psql --host=localhost --port=15435 -U postgres -d postgres  (zone1-2)"
echo ""
