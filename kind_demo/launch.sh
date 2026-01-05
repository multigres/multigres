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

# Kind cluster demo
# Prerequisites: docker compose, kind, kubectl
# multigres images must be built first
# Edit kind.yaml

# Ensure we're in the kind_demo directory.
# This is because we use a relative path name to the data files.
if [[ $(basename "$PWD") != "kind_demo" ]]; then
  echo "Error: This script must be run from the kind_demo directory"
  exit 1
fi

# Initialize the cluster with etcd
kind create cluster --config=kind.yaml --name=multidemo
kind load docker-image multigres/multigres multigres/pgctld-postgres multigres/multiadmin-web --name=multidemo
# This single etcd will be used for both the global topo and cell topo.
kubectl apply -f k8s-etcd.yaml
kubectl wait --for=condition=ready pod -l app=etcd --timeout=120s

# Deploy observability stack (Prometheus, Jaeger, Grafana)
# The otel-config ConfigMap must exist before services that reference it.
# We don't have to wait for the observability stack to be ready.
kubectl create configmap grafana-dashboard-multigres --from-file=multigres.json=observability/grafana-dashboard.json --save-config
kubectl apply -f k8s-observability.yaml

# We're launching this as a job. The operator will just invoke this CLI.
# For this, it must add the multigres binary to its image.
kubectl apply -f k8s-createclustermetadata-job.yaml
kubectl apply -f k8s-generate-certs-job.yaml
kubectl wait --for=condition=complete job/createclustermetadata --timeout=120s
kubectl wait --for=condition=complete job/generate-pgbackrest-certs --timeout=120s

# Once the cluster metadata is ready, launch all componentes at once.
# Once the multipoolers come up, multiorch will bootstrap the cluster
# and elect a primary.
kubectl apply -f k8s-multipooler-statefulset.yaml
kubectl apply -f k8s-multiorch.yaml
kubectl apply -f k8s-multigateway.yaml
kubectl apply -f k8s-multiadmin.yaml
kubectl apply -f k8s-multiadmin-web.yaml
kubectl wait --for=condition=ready pod -l app=multipooler --timeout=180s
kubectl wait --for=condition=ready pod -l app=multiorch --timeout=120s
kubectl wait --for=condition=ready pod -l app=multigateway --timeout=120s
kubectl wait --for=condition=ready pod -l app=multiadmin --timeout=120s
kubectl wait --for=condition=ready pod -l app=multiadmin-web --timeout=120s

set +x
echo ""
echo "========================================="
echo "Components launched successfully!"
echo "========================================="
echo ""
echo "PostgreSQL access:"
echo "  kubectl port-forward service/multigateway 15432:15432"
echo "  psql --host=localhost --port=15432 -U postgres -d postgres"
echo ""
echo "Multiadmin Web UI:"
echo "  kubectl port-forward service/multiadmin-web 18100:18100"
echo "  Web UI:     http://localhost:18100"
echo ""
echo "Multiadmin API access:"
echo "  kubectl port-forward service/multiadmin 18000:18000 18070:18070"
echo "  REST API:   http://localhost:18000"
echo "  gRPC API:   localhost:18070"
echo ""
echo "Observability access:"
echo "  kubectl port-forward service/observability 3000:3000 9090:9090 16686:16686"
echo "  Grafana:    http://localhost:3000/dashboards"
echo "  Prometheus: http://localhost:9090"
echo "  Jaeger:     http://localhost:16686"
echo ""
