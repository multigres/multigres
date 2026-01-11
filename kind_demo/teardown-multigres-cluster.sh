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

# Teardown multigres cluster components
# This removes only the core cluster components while leaving infrastructure intact
# (etcd, observability, cert-manager, multiadmin remain running)

# Ensure we're in the kind_demo directory.
if [[ $(basename "$PWD") != "kind_demo" ]]; then
  echo "Error: This script must be run from the kind_demo directory"
  exit 1
fi

# Stop multigres cluster port-forwards first
echo "Stopping multigres cluster port-forwards..."
./port-forward-multigres-cluster.sh stop || true

# Delete core multigres components in reverse order
kubectl delete -f k8s-multigateway.yaml --ignore-not-found=true
kubectl delete -f k8s-multiorch.yaml --ignore-not-found=true
kubectl delete -f k8s-multipooler-statefulset.yaml --ignore-not-found=true

# Wait for pods to be fully deleted
echo "Waiting for pods to terminate..."
kubectl wait --for=delete pod -l app=multigateway --timeout=60s || true
kubectl wait --for=delete pod -l app=multiorch --timeout=60s || true
kubectl wait --for=delete pod -l app=multipooler --timeout=120s || true

set +x
echo ""
echo "========================================="
echo "Multigres Cluster Teardown Complete"
echo "========================================="
echo ""
echo "Removed components:"
echo "  - multigateway (PostgreSQL proxy)"
echo "  - multiorch (orchestration and failover)"
echo "  - multipooler (connection pooling)"
echo ""
echo "Infrastructure still running:"
echo "  - Kind cluster"
echo "  - etcd"
echo "  - Observability stack"
echo "  - cert-manager"
echo "  - multiadmin services"
echo ""
echo "To redeploy the cluster: ./launch-multigres-cluster.sh"
echo "To tear down everything: kind delete cluster --name=multidemo"
echo ""
