# Local Development

Scripts and configuration for running Multigres locally.

## Quick Start

```bash
# From repository root
multigres cluster start --config-path /path/to/multigres_local
```

## Observability (Optional)

For development with metrics, traces, and logs visualization.

### Start

```bash
# 1. Start observability stack (runs in foreground — use a separate terminal)
demo/local/run-observability.sh

# 2. Start cluster with OTel export (separate terminal)
demo/local/multigres-with-otel.sh cluster start --config-path /path/to/multigres_local

# 3. Generate traffic (optional — pgbench with progress every 5s)
PGPASSWORD=postgres pgbench -h localhost -p 15432 -U postgres -i postgres
PGPASSWORD=postgres pgbench -h localhost -p 15432 -U postgres -c 4 -j 2 -T 300 -P 5 postgres
```

`multigres-with-otel.sh` uses `go run` by default — no rebuild needed after code changes.

### View Telemetry

- **Grafana Dashboard**: <http://localhost:3000/d/multigres-overview>
- **Grafana Explore** (ad-hoc queries): <http://localhost:3000/explore>
- **Prometheus UI**: <http://localhost:9090>

### Teardown

Stop in this order to avoid OTel export errors at shutdown:

```bash
# 1. Stop the cluster
./bin/multigres cluster stop --config-path /path/to/multigres_local

# 2. Stop the observability stack (Ctrl-C in run-observability.sh terminal, or:)
docker rm -f multigres-observability
```

### Full Restart

```bash
# 1. Stop everything
./bin/multigres cluster stop --config-path /path/to/multigres_local
docker rm -f multigres-observability

# 2. Start everything
demo/local/run-observability.sh          # terminal 1
demo/local/multigres-with-otel.sh cluster start --config-path /path/to/multigres_local  # terminal 2
```

### Ports

| Service | Port |
|---|---|
| Grafana | 3000 |
| OTLP (HTTP) | 4318 |
| Prometheus | 9090 |
| Loki | 3100 |
| Tempo | 3200 |
