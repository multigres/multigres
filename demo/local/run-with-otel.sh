#!/usr/bin/env bash
# Copyright 2025 Supabase, Inc.
# Wrapper script to run commands with OTEL environment variables

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if observability stack is running
if ! curl -s http://localhost:3000/api/health >/dev/null 2>&1; then
  echo "Warning: Grafana is not responding at http://localhost:3000"
  echo "The observability stack may not be running."
  echo ""
  echo "Start it with:"
  echo "  cd $SCRIPT_DIR"
  echo "  ./run-observability.sh"
  echo ""
  read -p "Continue anyway? (y/N) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
  fi
fi

# Set OTEL environment variables using unified endpoint (if not already set)
# All signals (traces, metrics, logs) go to the same OTLP endpoint
# The OTel Collector routes them to appropriate backends
export OTEL_METRIC_EXPORT_INTERVAL="${OTEL_METRIC_EXPORT_INTERVAL:-5000}"
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4318}"
export OTEL_EXPORTER_OTLP_PROTOCOL="${OTEL_EXPORTER_OTLP_PROTOCOL:-http/protobuf}"
export OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE="${OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE:-cumulative}"
export OTEL_TRACES_EXPORTER="${OTEL_TRACES_EXPORTER:-otlp}"
export OTEL_METRICS_EXPORTER="${OTEL_METRICS_EXPORTER:-otlp}"
export OTEL_LOGS_EXPORTER="${OTEL_LOGS_EXPORTER:-otlp}"
export OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION="${OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION:-base2_exponential_bucket_histogram}"
# Custom file-based sampling configuration
export OTEL_TRACES_SAMPLER="${OTEL_TRACES_SAMPLER:-multigres_custom}"
export OTEL_TRACES_SAMPLER_CONFIG="${OTEL_TRACES_SAMPLER_CONFIG:-$SCRIPT_DIR/../observability/sampling-config.yaml}"

echo "=== Multigres with Observability ==="
echo ""
echo "OTEL Configuration:"
echo "  Unified Endpoint: $OTEL_EXPORTER_OTLP_ENDPOINT"
echo "  Protocol:         $OTEL_EXPORTER_OTLP_PROTOCOL"
echo "  Sampler:          $OTEL_TRACES_SAMPLER"
echo "  Exporters:        traces, metrics, logs"
echo ""

exec "$@"
