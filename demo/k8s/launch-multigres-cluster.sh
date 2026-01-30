#!/usr/bin/env zsh
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

set -ex

# Kind cluster demo - Core multigres components
# Prerequisites: launch-infra.sh must have been run successfully

# Ensure we're in the k8s directory.
if [[ $(basename "$PWD") != "k8s" ]]; then
  echo "Error: This script must be run from the demo/k8s directory"
  exit 1
fi

# Deploy core multigres components
# Once the multipoolers come up, multiorch will bootstrap the cluster
# and elect a primary.
kubectl apply -f k8s-multipooler-statefulset.yaml
kubectl apply -f k8s-multiorch.yaml
kubectl apply -f k8s-multigateway.yaml

kubectl wait --for=condition=ready pod -l app=multipooler --timeout=180s
kubectl wait --for=condition=ready pod -l app=multiorch --timeout=120s
kubectl wait --for=condition=ready pod -l app=multigateway --timeout=120s

set +x
echo ""
echo "========================================="
echo "Multigres Cluster Ready"
echo "========================================="
echo ""
echo "Core components launched:"
echo "  - multipooler (connection pooling)"
echo "  - multiorch (orchestration and failover)"
echo "  - multigateway (PostgreSQL proxy)"
echo ""

# Start multigres cluster port-forwards
./port-forward-multigres-cluster.sh
