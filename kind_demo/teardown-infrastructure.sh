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

# Teardown infrastructure components
# This removes infrastructure deployed by launch-infra.sh
# (multiadmin, cert-manager, observability, etcd, etc.)

# Ensure we're in the kind_demo directory.
if [[ $(basename "$PWD") != "kind_demo" ]]; then
  echo "Error: This script must be run from the kind_demo directory"
  exit 1
fi

# Stop infrastructure port-forwards first
echo "Stopping infrastructure port-forwards..."
./port-forward-infrastructure.sh stop || true

# Delete infrastructure components in reverse order
kubectl delete -f k8s-multiadmin-web.yaml --ignore-not-found=true
kubectl delete -f k8s-multiadmin.yaml --ignore-not-found=true

kubectl delete -f k8s-pgbackrest-certs.yaml --ignore-not-found=true

# Delete cert-manager
echo "Removing cert-manager..."
kubectl delete -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.0/cert-manager.yaml --ignore-not-found=true

kubectl delete -f k8s-createclustermetadata-job.yaml --ignore-not-found=true

kubectl delete -f k8s-observability.yaml --ignore-not-found=true
kubectl delete configmap grafana-dashboard-multigres --ignore-not-found=true

kubectl delete -f k8s-etcd.yaml --ignore-not-found=true

# Wait for pods to be fully deleted
echo "Waiting for pods to terminate..."
kubectl wait --for=delete pod -l app=multiadmin-web --timeout=60s || true
kubectl wait --for=delete pod -l app=multiadmin --timeout=60s || true
kubectl wait --for=delete pod -l app=etcd --timeout=60s || true
kubectl wait --for=delete namespace cert-manager --timeout=120s || true

set +x
echo ""
echo "========================================="
echo "Infrastructure Teardown Complete"
echo "========================================="
echo ""
echo "Removed components:"
echo "  - multiadmin services"
echo "  - pgBackRest certificates"
echo "  - cert-manager"
echo "  - Cluster metadata job"
echo "  - Observability stack (Prometheus, Tempo, Loki, Grafana)"
echo "  - etcd"
echo ""
echo "Kind cluster still running (empty)"
echo ""
echo "To remove the Kind cluster: kind delete cluster --name=multidemo"
echo ""
