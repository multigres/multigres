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

# Kind cluster demo - Infrastructure and supporting services
# Prerequisites: docker compose, kind, kubectl
# multigres images must be built first
# Edit kind.yaml

# Ensure we're in the k8s directory.
# This is because we use a relative path name to the data files.
if [[ $(basename "$PWD") != "k8s" ]]; then
  echo "Error: This script must be run from the demo/k8s directory"
  exit 1
fi

# Initialize the cluster with etcd
kind create cluster --config=kind.yaml --name=multidemo

# Increase limits on all Kind nodes for high-connection workloads
for node in $(kind get nodes --name=multidemo); do
  docker exec "$node" sh -c "sysctl -w fs.file-max=2097152"
  docker exec "$node" sh -c "sysctl -w fs.nr_open=2097152"
  docker exec "$node" sh -c "sysctl -w net.core.somaxconn=65535"
  docker exec "$node" sh -c "sysctl -w net.ipv4.ip_local_port_range='1024 65535'"
done

kind load docker-image multigres/multigres multigres/pgctld-postgres multigres/multiadmin-web --name=multidemo
# This single etcd will be used for both the global topo and cell topo.
kubectl apply -f k8s-etcd.yaml
kubectl rollout status statefulset/etcd --timeout=120s

# Deploy observability stack (Prometheus, Tempo, Loki, Grafana)
# The otel-config, sampling-config, and grafana-datasources ConfigMaps must exist before services that reference them.
kubectl create configmap grafana-dashboard-multigres --from-file=multigres.json=../observability/grafana-dashboard.json --save-config
kubectl create configmap grafana-datasources --from-file=datasources.yml=../observability/grafana-datasources.yml --save-config
kubectl create configmap sampling-config --from-file=sampling-config.yaml=../observability/sampling-config.yaml --save-config
kubectl apply -f k8s-observability.yaml

# We're launching this as a job. The operator will just invoke this CLI.
# For this, it must add the multigres binary to its image.
kubectl apply -f k8s-createclustermetadata-job.yaml

# Install cert-manager for certificate management
echo "Installing cert-manager..."
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.0/cert-manager.yaml
kubectl wait --for=condition=Available --timeout=300s \
  -n cert-manager deployment/cert-manager \
  deployment/cert-manager-webhook \
  deployment/cert-manager-cainjector

# Verify the cert-manager webhook is ready by testing certificate validation
echo "Testing webhook connectivity..."
max_attempts=5
attempt=1
while [ $attempt -le $max_attempts ]; do
  if kubectl apply --dry-run=server -f k8s-pgbackrest-certs.yaml >/dev/null 2>&1; then
    echo "Webhook is ready and accepting certificate requests"
    break
  fi
  if [ $attempt -eq $max_attempts ]; then
    echo "Timeout: webhook not accepting certificate requests after $max_attempts attempts"
    exit 1
  fi
  echo "Webhook not ready, waiting... (attempt $attempt/$max_attempts)"
  sleep 5
  attempt=$((attempt + 1))
done

# Deploy pgBackRest certificates using cert-manager
echo "Creating pgBackRest TLS certificates..."
kubectl apply -f k8s-pgbackrest-certs.yaml
kubectl wait --for=condition=ready certificate/multigres-ca --timeout=120s
kubectl wait --for=condition=ready certificate/pgbackrest-cert --timeout=120s

# Make sure the cluster metadata job is complete before proceeding
kubectl wait --for=condition=complete job/createclustermetadata --timeout=120s

# Deploy multiadmin services
kubectl apply -f k8s-multiadmin.yaml
kubectl apply -f k8s-multiadmin-web.yaml
kubectl rollout status deployment/multiadmin --timeout=120s
kubectl rollout status deployment/multiadmin-web --timeout=120s
kubectl rollout status deployment/observability --timeout=120s

set +x
echo ""
echo "========================================="
echo "Infrastructure Ready"
echo "========================================="
echo ""
echo "Infrastructure components launched:"
echo "  - Kind cluster"
echo "  - etcd"
echo "  - Observability stack (Prometheus, Tempo, Loki, Grafana)"
echo "  - cert-manager"
echo "  - Cluster metadata"
echo "  - multiadmin and multiadmin-web"
echo ""
echo "Next step: Run ./launch-multigres-cluster.sh to deploy core multigres components"
echo "  (multipooler, multiorch, multigateway)"
echo ""

# Start infrastructure port-forwards
./port-forward-infra.sh
