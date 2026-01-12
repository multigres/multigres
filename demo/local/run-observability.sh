#!/usr/bin/env bash
# Copyright 2025 Supabase, Inc.
# Run local observability stack for Multigres development

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_NAME="multigres-observability"

echo "=== Starting Multigres Observability Stack ==="
echo ""

# Check if docker is running
if ! docker info >/dev/null 2>&1; then
  echo "Error: Docker is not running. Please start Docker and try again."
  exit 1
fi

# Check if container is already running
if docker ps -q -f name="$CONTAINER_NAME" | grep -q .; then
  echo "Error: Container '$CONTAINER_NAME' is already running"
  echo ""
  echo "To stop it: docker stop $CONTAINER_NAME"
  echo "Or kill the existing process with Ctrl-C"
  exit 1
fi

# Remove any stopped container with the same name
docker rm "$CONTAINER_NAME" 2>/dev/null || true

# Determine if we should use -it based on TTY
if [ -t 0 ]; then
  INTERACTIVE_FLAGS="-it"
else
  INTERACTIVE_FLAGS=""
fi

echo "Starting otel-lgtm container..."
echo ""
echo "Once started, access:"
echo "  Grafana Dashboard: http://localhost:3000/d/multigres-overview"
echo "  Grafana Home:      http://localhost:3000"
echo "  Prometheus UI:     http://localhost:9090"
echo ""
echo "API Endpoints:"
echo "  OTLP (HTTP):       http://localhost:4318"
echo "  Tempo:             http://localhost:3200"
echo "  Loki:              http://localhost:3100"
echo ""
echo "Press Ctrl-C to stop"
echo ""

# Create a temporary directory for dashboard provisioning
DASHBOARDS_DIR=$(mktemp -d)
trap 'rm -rf "$DASHBOARDS_DIR"' EXIT

# Copy dashboard to temp directory with expected name
cp "$SCRIPT_DIR/../observability/grafana-dashboard.json" "$DASHBOARDS_DIR/multigres.json"

# Create dashboard provisioning config (replaces default otel-lgtm dashboards)
cat >"$DASHBOARDS_DIR/dashboards.yaml" <<EOF
apiVersion: 1
providers:
  - name: "Multigres"
    orgId: 1
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /otel-lgtm/multigres-dashboard.json
      foldersFromFilesStructure: false
EOF

# Run in foreground - user can kill with Ctrl-C
docker run \
  $INTERACTIVE_FLAGS \
  --rm \
  --name "$CONTAINER_NAME" \
  -p 3000:3000 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 9090:9090 \
  -p 3100:3100 \
  -p 3200:3200 \
  -e GF_AUTH_ANONYMOUS_ENABLED=true \
  -e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
  -e GF_AUTH_DISABLE_LOGIN_FORM=true \
  -e GF_SECURITY_ADMIN_PASSWORD=admin \
  -e GF_SERVER_HTTP_PORT=3000 \
  -e GF_SERVER_DOMAIN=localhost \
  -e GF_FEATURE_TOGGLES_ENABLE=traceqlEditor,traceQLStreaming,correlations \
  -v "$SCRIPT_DIR/config/grafana-datasources.yml:/etc/grafana/provisioning/datasources/multigres.yaml:ro" \
  -v "$DASHBOARDS_DIR/dashboards.yaml:/otel-lgtm/grafana/conf/provisioning/dashboards/grafana-dashboards.yaml:ro" \
  -v "$DASHBOARDS_DIR/multigres.json:/otel-lgtm/multigres-dashboard.json:ro" \
  grafana/otel-lgtm:0.13.0
