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

# Updates the Grafana dashboard ConfigMap and triggers a reload.
# Run from the kind_demo directory.

set -e

if [[ $(basename "$PWD") != "kind_demo" ]]; then
  echo "Error: This script must be run from the kind_demo directory"
  exit 1
fi

echo "Updating grafana-dashboard-multigres ConfigMap..."
kubectl create configmap grafana-dashboard-multigres \
  --from-file=multigres.json=observability/grafana-dashboard.json \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Restarting observability deployment to reload dashboards..."
kubectl rollout restart deployment/observability
kubectl rollout status deployment/observability --timeout=60s

echo ""
echo "Dashboard updated successfully."
