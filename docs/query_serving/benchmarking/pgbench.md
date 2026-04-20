# pgbench Benchmarking

## Overview

We run [pgbench](https://www.postgresql.org/docs/current/pgbench.html) benchmarks to
measure multigateway's query-serving overhead against direct PostgreSQL and PgBouncer.
The benchmark suite lives in `go/test/endtoend/queryserving/benchmarking/` and runs as a
standard Go integration test.

Metrics are computed from pgbench's per-transaction log (`--log`) rather than parsing
text output. This gives us TPS, average latency, P50, and P99 directly from the raw
data.

## Architecture

```text
                ┌──────────────┐
                │   pgbench    │
                │  (workload   │
                │  generator)  │
                └──┬───┬───┬───┘
                   │   │   │
         ┌─────────┘   │   └─────────┐
         ▼             ▼             ▼
   ┌──────────┐  ┌────────────┐  ┌──────────┐
   │ postgres │  │multigateway│  │pgbouncer │
   │ (direct) │  │  (proxy)   │  │ (proxy)  │
   └────┬─────┘  └─────┬──────┘  └────┬─────┘
        │               │              │
        └───────┬───────┘              │
                ▼                      ▼
          ┌──────────┐           ┌──────────┐
          │ postgres │           │ postgres │
          │ (same    │           │ (same    │
          │ backend) │           │ backend) │
          └──────────┘           └──────────┘
```

All three targets point at the same PostgreSQL backend (the primary pgctld node in the
test cluster), so differences in throughput and latency are purely proxy overhead.

## Targets

| Target           | Description                                            | Connection path                               |
| ---------------- | ------------------------------------------------------ | --------------------------------------------- |
| **postgres**     | Direct connection to PostgreSQL, bypassing all proxies | pgbench → pgctld PG port                      |
| **multigateway** | Connection through Multigres                           | pgbench → multigateway → multipooler → pgctld |
| **pgbouncer**    | Connection through PgBouncer (transaction mode)        | pgbench → pgbouncer → pgctld PG port          |

PgBouncer is optional — if the `pgbouncer` binary is not on `PATH`, those benchmarks are
silently skipped. Install with `brew install pgbouncer` (macOS) or `apt install pgbouncer`
(Ubuntu).

## Scenarios

The suite generates a cross-product of scenarios:

| Dimension    | Values                                         | Notes                                          |
| ------------ | ---------------------------------------------- | ---------------------------------------------- |
| **Mode**     | sustained, churn (`-C`)                        | Churn creates a new connection per transaction |
| **Protocol** | simple (`-M simple`), extended (`-M extended`) | Extended protocol uses Parse/Bind/Execute      |
| **Clients**  | Configurable (default: 1, 10, 50)              | Churn scenarios limited to clients ≤ 10        |

This produces ~12 scenarios per target. With 3 targets and 30s duration each, a full
run takes ~20 minutes.

### Known limitations

- **Extended protocol + multiple clients through multigateway** fails with `cursor
already exists` errors. This is a known limitation of transaction-mode connection
  pooling with the extended query protocol and is tracked for improvement.
- **50 clients** can exceed `max_connections` on small test clusters. The test
  gracefully skips failed scenarios.

## Running locally

### Quick run (minimal)

```bash
RUN_PGBENCH=1 PGBENCH_DURATION=5 PGBENCH_CLIENTS=1,2 \
  go test -v -run TestPgBench ./go/test/endtoend/queryserving/benchmarking/... -timeout 30m
```

### Full run

```bash
RUN_PGBENCH=1 PGBENCH_DURATION=30 PGBENCH_CLIENTS=1,10,50 \
  go test -v -run TestPgBench ./go/test/endtoend/queryserving/benchmarking/... -timeout 60m
```

### Using the mt-dev skill

```bash
# Build first (integration test requirement)
make build

# Then run
/mt-dev integration queryserving/benchmarking TestPgBench
```

Note: `RUN_PGBENCH=1` must be set in the environment for the test to run. Without it,
the test is skipped.

### Environment variables

| Variable           | Default   | Description                          |
| ------------------ | --------- | ------------------------------------ |
| `RUN_PGBENCH`      | (unset)   | Set to `1` to enable benchmark tests |
| `PGBENCH_DURATION` | `30`      | Seconds per scenario                 |
| `PGBENCH_CLIENTS`  | `1,10,50` | Comma-separated client counts        |

## How metrics are computed

pgbench writes a per-transaction log file via `--log`. Each line contains:

```text
client_id  txn_no  latency_us  script_no  epoch_secs  epoch_usecs
```

We read the `latency_us` column (index 2) from every line and compute:

- **TPS** = transaction count / duration seconds
- **Avg latency** = mean of all latency values (converted to ms)
- **P50** = median latency (linear interpolation)
- **P99** = 99th percentile latency (linear interpolation)

This approach avoids parsing pgbench's text output (which varies across PG versions) and
gives us percentile data that the text summary does not include.

## Output

Reports are written to `/tmp/multigres_pgbench_results/<timestamp>/`:

| File                        | Format            | Purpose                                             |
| --------------------------- | ----------------- | --------------------------------------------------- |
| `results.json`              | JSON              | Machine-readable results for CI baseline comparison |
| `benchmark-report.md`       | Markdown          | Human-readable comparison tables                    |
| `logs/<scenario>/<target>/` | pgbench log files | Raw per-transaction data                            |

### Markdown report example

```markdown
## Sustained Load — Simple Protocol

| Clients | postgres TPS | postgres Avg | postgres P99 | multigateway TPS | multigateway Avg | multigateway P99 | pgbouncer TPS | pgbouncer Avg | pgbouncer P99 | Overhead |
| ------- | ------------ | ------------ | ------------ | ---------------- | ---------------- | ---------------- | ------------- | ------------- | ------------- | -------- |
| 1       | 3213         | 0.31 ms      | 0.81 ms      | 696              | 1.43 ms          | 4.62 ms          | 2323          | 0.43 ms       | 1.09 ms       | 78.3%    |
| 10      | 8676         | 1.13 ms      | 3.07 ms      | 1403             | 7.09 ms          | 15.83 ms         | 7069          | 1.40 ms       | 3.26 ms       | 83.8%    |
```

The **Overhead** column shows `(1 - multigateway_tps / postgres_tps) * 100`.

## CI integration

The benchmark runs weekly via `.github/workflows/test-pgbench.yml`:

- **Schedule:** Monday 6:00 AM UTC
- **Triggers:** Weekly cron, `workflow_dispatch`, PR label `Run PgBench Tests`
- **Baseline:** Results are cached via `actions/cache`; each run is compared against the
  previous baseline
- **Regression detection:** `.github/scripts/detect-pgbench-regressions.sh` flags
  multigateway scenarios where TPS drops by more than 5% vs baseline
- **Slack notifications:**
  - **Weekly summary** — average overhead %, per-scenario TPS table
  - **Regression alert** — scenarios with >5% TPS drop
  - **Infrastructure failure** — test harness didn't produce results
- **Webhook secret:** `SLACK_PGBENCH_WEBHOOK_URL` (must be configured in repo settings)

## File structure

```text
go/test/endtoend/queryserving/benchmarking/
├── main_test.go          # TestMain, shared cluster setup (2-node + multigateway)
├── pgbench_test.go       # TestPgBench orchestrator
├── pgbench_runner.go     # PgBenchRunner: init, run scenarios, compute metrics
├── pgbouncer.go          # Optional PgBouncer lifecycle (SCRAM auth, config gen)
└── report.go             # JSON + Markdown report generation

.github/
├── workflows/test-pgbench.yml               # Weekly CI workflow
└── scripts/detect-pgbench-regressions.sh    # TPS regression detection
```

## Adding new scenarios

To add a new benchmark scenario, modify `DefaultScenarios()` in `pgbench_runner.go`.
For example, to add a read-only workload:

```go
scenarios = append(scenarios, ScenarioConfig{
    Name:     "readonly_10c_simple",
    Clients:  10,
    Duration: duration,
    Protocol: "simple",
    // Add custom pgbench flags via a new field if needed
})
```

Custom SQL workloads can be supported by extending `RunScenario` to accept a `-f`
script path instead of using the built-in TPC-B workload.
