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

echo -e "${BLUE}Starting infrastructure port-forwards...${NC}"

# Start all port-forwards in the background and capture PIDs
# kubectl will automatically stop these when the underlying resources are deleted
pids=()

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward service/multiadmin-web 18100:18100 >/dev/null 2>&1 &
pids+=($!)

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward service/multiadmin 18000:18000 >/dev/null 2>&1 &
pids+=($!)

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward service/multiadmin 18070:18070 >/dev/null 2>&1 &
pids+=($!)

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward service/observability 3000:3000 >/dev/null 2>&1 &
pids+=($!)

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward service/observability 9090:9090 >/dev/null 2>&1 &
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
  echo -e "${GREEN}Infrastructure port-forwards started successfully!${NC}"
else
  echo -e "${YELLOW}Warning: $failed port-forward(s) failed to start${NC}"
  echo -e "${YELLOW}This may be normal if services are not yet ready or ports are already in use${NC}"
fi

echo ""
echo "========================================="
echo "Infrastructure Access URLs:"
echo "========================================="
echo ""
echo "Multiadmin Web UI:"
echo "  http://localhost:18100"
echo ""
echo "Multiadmin API:"
echo "  REST API:   http://localhost:18000"
echo "  gRPC API:   localhost:18070"
echo ""
echo "Observability:"
echo "  Grafana:    http://localhost:3000/dashboards"
echo "  Prometheus: http://localhost:9090"
echo ""
