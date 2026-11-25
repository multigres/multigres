# Gateway Query Observability

## Overview

Multigateway records every completed client statement in three places: OTel
metrics, a per-query-shape statistics registry, and a structured query log. All
three are fed from one function, `recordQueryCompletion` in
`go/services/multigateway/handler/handler.go`, which runs for both the simple
and the extended protocol.

## Query fingerprints

Statements the plan cache can hold (`SELECT`, `INSERT`, `UPDATE`, `DELETE`; see
[plan_cache_design.md](./plan_cache_design.md#cacheable-statements)) have a
normalized SQL text and a fingerprint, a hash of that text. Statements that are
planned directly (transaction control, `SET`, DDL, and so on, plus every
statement on a `multigres.unsafe_connection` session) have no fingerprint.

Putting every fingerprint in a metric label would make cardinality unbounded,
so the `query.fingerprint` label takes one of three kinds of value:

| Label value   | Used for                                               |
| ------------- | ------------------------------------------------------ |
| a fingerprint | A fingerprint currently tracked by the registry        |
| `__other__`   | A fingerprinted statement the registry is not tracking |
| `__utility__` | A statement with no fingerprint                        |

## Query registry

`handler/queryregistry` keeps aggregated statistics per fingerprint: calls,
errors, total/min/max duration, a duration histogram (from which p50 and p99
are derived), total rows, and last-seen time. It is a pg_stat_statements-like
view of what the gateway has served.

- The registry is a W-TinyLFU cache (the same `theine` store the plan cache
  uses) bounded by memory, not entry count. A fingerprint seen once is not
  necessarily admitted; one that becomes popular displaces a cold one.
- One registry is shared by the primary and replica-reads handlers, so a query
  shape aggregates to one entry regardless of the listener it arrived on.
- A sampler goroutine records per-fingerprint trends (call rate, total time,
  p50, p99, row rate). `DefaultConfig` samples every 10 seconds and keeps 60
  samples.
- The stored normalized SQL is truncated to `--query-metrics-sql-max-bytes`.

The registry is read through the `MultigatewayManager.GetQueryRegistry` gRPC
(`go/services/multigateway/manager.go`). The request takes `limit` (top
fingerprints by call count; `0` returns all) and `min_calls`. Multiadmin
proxies it as `GetGatewayQueries`.

Setting `--query-metrics-memory=0` disables the registry. Fingerprinted
statements are then all labelled `__other__`.

## Metrics

| Metric                            | Kind      | Description                                                                          |
| --------------------------------- | --------- | ------------------------------------------------------------------------------------ |
| `mg.gateway.query.duration`       | histogram | Total statement duration at the gateway                                              |
| `mg.gateway.query.errors`         | counter   | Statement errors, by SQLSTATE and error source                                       |
| `mg.gateway.query.rows_returned`  | histogram | Rows returned per statement                                                          |
| `mg.gateway.query.table_queries`  | counter   | Statements per table                                                                 |
| `mg.gateway.query.parse.duration` | histogram | SQL parse time. Not recorded on the extended-protocol Execute path                   |
| `mg.gateway.query.plan.duration`  | histogram | Planning time. Not recorded for statements that were not planned                     |
| `mg.gateway.query.exec.duration`  | histogram | Downstream execution time. Not recorded when an error stopped before execution       |
| `mg.gateway.query.info`           | gauge     | One series per tracked fingerprint, labelled with the fingerprint and normalized SQL |
| `mg.gateway.query.log.emits`      | counter   | Query log records emitted, by level                                                  |

`mg.gateway.query.info` exists so dashboards can join the fingerprint label of
the other metrics to SQL text. It also emits series for `__other__` and
`__utility__`.

The registry's histogram uses the same bucket boundaries as
`mg.gateway.query.duration` (0.5 ms to 10 s), so percentiles from the registry
and from the metric agree.

## Query log

`emitQueryLog` (`handler/querylog.go`) writes one `query completed` record per
statement:

- A statement that failed, or took at least
  `constants.DefaultSlowQueryThreshold` (1 second), is logged at `WARN`. A slow
  statement carries `slow_query=true`.
- Every other statement is logged at `DEBUG`, so it is visible only with
  `--log-level=debug`, and is subject to `--query-log-sample-rate`: `0` applies
  no sampling, `1` emits every statement, and `N > 1` emits every Nth.

Attributes: `db.namespace`, `db.operation.name`, `db.query.protocol`
(`simple` or `extended`), `db.user`, `duration.total`, `duration.parse`,
`duration.plan`, `duration.execute` (seconds), `rows_returned`, and when
available `db.plan.type` and `db.tables_used`. A failed statement adds `error`,
`sqlstate` and `error.source`. The record does not include the SQL text.

See [logging.md](../general/logging.md) for the logging conventions these
records follow.

## Configuration

| Flag                            | Env var                          | Default          | Description                                                    |
| ------------------------------- | -------------------------------- | ---------------- | -------------------------------------------------------------- |
| `--query-metrics-memory`        | `MT_QUERY_METRICS_MEMORY`        | `8388608` (8 MB) | Memory budget for the registry in bytes. `0` disables it       |
| `--query-metrics-sql-max-bytes` | `MT_QUERY_METRICS_SQL_MAX_BYTES` | `4096`           | Maximum bytes of normalized SQL stored per tracked fingerprint |
| `--query-log-sample-rate`       | `MT_QUERY_LOG_SAMPLE_RATE`       | `0`              | 1/N sampling of `DEBUG` query log records                      |
