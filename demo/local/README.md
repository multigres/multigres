# Local Development

Scripts and configuration for running Multigres locally.

## Quick Start

```bash
# From repository root
multigres cluster start --config-path /path/to/multigres_local
```

## Observability (Optional)

For development with metrics, traces, and logs visualization:

### 1. Start observability stack

```bash
cd demo/local
./run-observability.sh
```

The container runs in the foreground. Press **Ctrl-C** to stop it.

### 2. Start Multigres with telemetry

```bash
cd demo/local
./multigres-with-otel.sh --config-path /path/to/multigres_local
```

Uses `go run` by default - no rebuild needed after code changes.

### 3. View telemetry

- **Grafana Dashboard**: http://localhost:3000/d/multigres-overview
- **Prometheus UI**: http://localhost:9090

### What's Included

The observability stack (`grafana/otel-lgtm:0.13.0`) provides:

- Grafana dashboards with full trace/log/metric correlation
- Tempo (distributed tracing)
- Loki (log aggregation)
- Mimir/Prometheus (metrics)
- OpenTelemetry Collector (OTLP ingestion)

Dashboard is shared with the k8s demo at `demo/observability/grafana-dashboard.json`.
