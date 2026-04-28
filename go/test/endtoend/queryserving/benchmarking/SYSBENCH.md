# TestSysBench — sysbench oltp_point_select harness

`TestSysBench` (in `sysbench_test.go`) drives sysbench
[`oltp_point_select`](https://github.com/akopytov/sysbench) over the pgsql
driver against multigateway, while sampling per-process CPU and capturing
CPU pprofs.

This is a manual perf tool — like `TestPgBench`, it skips by default.

## Quick start

```bash
RUN_BENCHMARKS=1 CAPTURE_PPROF=1 \
SYSBENCH_CLIENTS=1,8,32 SYSBENCH_DURATION=60 \
go test -v -timeout 30m -run TestSysBench \
  ./go/test/endtoend/queryserving/benchmarking/
```

Output bundle:

```text
/tmp/multigres_sysbench_results/<utc-stamp>/
├── benchmark-report.md
├── results.json
├── logs/prepare/<target>.txt
├── logs/<scenario>/<target>.txt
├── cpu/<scenario>/<target>/processes.json
└── pprof/<scenario>/<target>/cpu-<service>.pb.gz
```

## Why `prepared` and `simple` ps modes

`oltp_point_select` issues `SELECT c FROM sbtestN WHERE id = ?`. With
`--db-ps-mode=auto` (the sysbench default — what we call `prepared` here),
each worker prepares the statement once via `PQprepare` and re-executes it
thousands of times. With `--db-ps-mode=disable` (`simple`) sysbench sends
a fully-formatted SELECT every transaction, forcing the pgsql server (and
multigateway, in our case) to re-parse it.

Running both lets us isolate parser-per-query cost from everything else
proxy/pooler do.

## Environment variables

| var                          | default                                       | notes                                                       |
| ---------------------------- | --------------------------------------------- | ----------------------------------------------------------- |
| `RUN_BENCHMARKS`             | unset                                         | required — must be `1`                                      |
| `CAPTURE_PPROF`              | unset                                         | when `1`, capture CPU pprof per scenario                    |
| `CAPTURE_HEAP`               | unset                                         | when `1`, also capture heap/allocs/goroutine snapshots      |
| `SYSBENCH_CLIENTS`           | `1,8,32`                                      | comma-separated thread counts                               |
| `SYSBENCH_DURATION`          | `60`                                          | seconds per scenario                                        |
| `SYSBENCH_PS_MODES`          | `prepared,simple`                             | subset of `{prepared,simple}`                               |
| `SYSBENCH_TARGETS`           | `multigateway`                                | subset of `{multigateway,postgres,pgbouncer}`               |
| `SYSBENCH_TABLES`            | `4`                                           | sysbench `--tables`                                         |
| `SYSBENCH_TABLE_SIZE`        | `100000`                                      | sysbench `--table-size`                                     |
| `SYSBENCH_QUIESCE_SECONDS`   | `5`                                           | sleep between scenarios                                     |
| `SYSBENCH_RESULTS_DIR`       | `/tmp/multigres_sysbench_results/<utc-stamp>` | override output directory                                   |
| `PGBENCH_PG_MAX_CONNECTIONS` | unset                                         | bump postgres `max_connections` (shared with `TestPgBench`) |

## Skip behavior

The test self-skips if any of:

- `RUN_BENCHMARKS=1` is unset.
- `sysbench` binary is missing on PATH.
- `sysbench` is present but lacks the pgsql driver. The skip message tells
  you exactly how to rebuild — sysbench needs `./configure --with-pgsql`.

## Loud failures

These are intentional `t.Errorf`/`t.Fatalf` rather than silent zeros, so a
flaky run fails the test instead of producing misleading numbers:

- Sysbench summary parse failure (missing `transactions:` or `99th
percentile:` line).
- Zero TPS or zero transactions in any scenario.
- Empty (0-byte) CPU pprof file when `CAPTURE_PPROF=1` is set.

## Caveats

- Sysbench reports per-percentile latency in milliseconds with two decimals.
  On point-select workloads the p99 frequently reads `0.00` because the
  actual p99 is sub-millisecond. This is captured verbatim — interpret with
  care.
- The shared cluster setup (`getSharedSetup` in `main_test.go`) is reused
  across `TestPgBench` and `TestSysBench`. Running both in the same `go
test` invocation will share one multigres cluster.
- Postgres has 4 sysbench tables (`sbtest1..sbtest4`) with 100k rows each
  by default. They're left in place after the test — re-running
  `TestSysBench` in the same cluster is fine; the cluster is torn down by
  the package-level `TestMain`.
