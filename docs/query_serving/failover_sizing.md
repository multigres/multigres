# Sizing the Failover Buffer and Drain

## Overview

This guide is for operators who need to pick concrete values for the
failover-related flags so that planned failovers complete without
client-visible errors. It tells you which metrics to look at, how to
translate them into flag values, and how to validate that the values
hold up under your workload.

The two subsystems being sized are:

- The **multigateway failover buffer**, which holds PRIMARY queries
  while the old pooler is no longer serving and the new one has not
  taken over yet. See [Failover Buffering](./failover_buffering.md) for
  the design.
- The **multipooler graceful drain**, which waits for in-flight
  reserved connections to finish before completing a `NOT_SERVING`
  transition. See [Graceful Drain](./graceful_drain_design.md) for the
  design.

These two systems are coupled: the gateway holds queries for as long
as the failover lasts, and the pooler drain is part of that failover
window. Sizing one without the other will produce errors at the
boundary.

## What You're Sizing

```text
Total failover window (gateway buffers requests for this long)
│
├── Pooler drain time (--connpool-drain-grace-period bounds this)
│   │
│   ├── Reserved connections finish naturally  (the graceful case)
│   └── Reserved connections force-closed      (grace period exceeded)
│
└── New-primary election + topology propagation + first health-stream signal
```

The flags you're setting:

| Flag                                  | Service      | Question it answers                              |
| ------------------------------------- | ------------ | ------------------------------------------------ |
| `--connpool-drain-grace-period`       | multipooler  | How long to wait for in-flight queries to finish |
| `--buffer-window`                     | multigateway | How long a single buffered request waits         |
| `--buffer-max-failover-duration`      | multigateway | Total time a shard can be in BUFFERING state     |
| `--buffer-size`                       | multigateway | Max concurrent buffered requests, global         |
| `--buffer-min-time-between-failovers` | multigateway | Cooldown between failover detections per shard   |
| `--buffer-drain-concurrency`          | multigateway | Retry parallelism when failover ends             |

The two failure modes you're avoiding:

1. **Buffer overrun** — too many concurrent queries arrive during a
   failover, exceeding `--buffer-size`, and the oldest get evicted
   with `MTB01`. Visible to clients as an error.
2. **Window exceeded** — the failover takes longer than
   `--buffer-window` or `--buffer-max-failover-duration`, and waiting
   queries are evicted with `MTB02`. Also visible to clients.

## The Metrics You Need

Run with `--buffer-enabled` and a permissive `--buffer-window` (e.g.
60s) in a staging environment that mirrors production traffic, then
trigger several planned failovers to populate the histograms.

### Gateway side

| Metric                                  | Type      | Question it answers                                            |
| --------------------------------------- | --------- | -------------------------------------------------------------- |
| `multigateway.buffer.queue.depth`       | Gauge     | How full is the buffer at this instant?                        |
| `multigateway.buffer.failover.duration` | Histogram | How long did this failover take (gateway-perceived)?           |
| `multigateway.buffer.wait.duration`     | Histogram | How long did each buffered request wait?                       |
| `multigateway.buffer.requests.evicted`  | Counter   | How many requests were evicted, by `reason`?                   |
| `multigateway.buffer.requests.buffered` | Counter   | How many requests entered the buffer (rate of buffer ingress)? |
| `multigateway.buffer.failovers`         | Counter   | How many failovers were detected per shard?                    |

### Pooler side

| Metric                                  | Type      | Question it answers                               |
| --------------------------------------- | --------- | ------------------------------------------------- |
| `mg.pooler.drain.duration`              | Histogram | How long did the drain phase take?                |
| `mg.pooler.drain.outcome`               | Counter   | Was the drain graceful or force-closed? (by attr) |
| `mg.pooler.drain.force_closed`          | Counter   | How many reserved connections got force-closed?   |
| `mg.pooler.reserved.active_connections` | Gauge     | How many reserved connections at drain time?      |

## The Sizing Procedure

Do this in order. Each step depends on the previous one's data.

### Step 1: Pick `--connpool-drain-grace-period`

The pooler's drain time is bounded by this flag. You want it large
enough that the **graceful** outcome dominates, but not so large that
a failover stalls for an obviously stuck transaction.

```promql
# What fraction of drains are getting force-closed today?
sum(rate(mg_pooler_drain_outcome_total{outcome="force_close"}[1h]))
/
sum(rate(mg_pooler_drain_outcome_total[1h]))
```

- If this ratio is **> 0** for non-pathological drains, raise
  `--connpool-drain-grace-period`.
- If it's **always 0** and your drain.duration p99 is well under the
  grace period, you can lower the grace period to shrink the failover
  window.

```promql
# p99 of graceful drain duration
histogram_quantile(0.99, sum(rate(mg_pooler_drain_duration_bucket[1h])) by (le))
```

Set `--connpool-drain-grace-period` to roughly `2 × p99(drain.duration)`
on the graceful path. The 2× gives you margin for traffic spikes.

### Step 2: Pick `--buffer-window` and `--buffer-max-failover-duration`

The gateway needs to buffer for longer than the **entire** failover,
not just the drain. Use the gateway-side metric directly — it already
includes drain + election + topology propagation.

```promql
# p99 of total failover duration as seen by the gateway, per shard
histogram_quantile(0.99,
  sum(rate(multigateway_buffer_failover_duration_bucket[1h])) by (le, shard_key))
```

Pick `--buffer-window` to comfortably exceed this — `2 × p99` is a
safe starting point.

Pick `--buffer-max-failover-duration` to be `>= --buffer-window`. This
is enforced by config validation. It's the hard upper bound on how
long the entire shard's buffer is allowed to wait.

### Step 3: Pick `--buffer-size`

The cap on concurrent buffered queries. Drive this off the gauge:

```promql
# Peak buffer depth observed during real failovers
max_over_time(multigateway_buffer_queue_depth[7d])
```

Set `--buffer-size` to `2 × max_over_time(...)` to leave headroom for
traffic above the historic peak. Each entry costs ~5-10 KiB of memory
(goroutine stack + struct overhead + closure-captured SQL); a buffer
of 10,000 entries is roughly 50-100 MiB.

### Step 4: Validate

After deploying the new values, watch these queries for a week:

```promql
# Should stay flat at zero for healthy operation.
rate(multigateway_buffer_requests_evicted_total{reason="window_exceeded"}[5m])
rate(multigateway_buffer_requests_evicted_total{reason="buffer_full"}[5m])
rate(multigateway_buffer_requests_evicted_total{reason="max_duration"}[5m])
```

Any non-zero rate means at least one query failed because of buffer
limits — either size, per-request window, or shard-level max
duration. Each maps to a specific flag to raise.

## Worked Example

A workload with ~500 queries/sec on a single shard, planned failovers
every few weeks.

After 2 weeks of staging with permissive defaults:

| Observation                                            | Value  |
| ------------------------------------------------------ | ------ |
| `p99(mg.pooler.drain.duration)` on graceful path       | 600 ms |
| `force_close` ratio                                    | 0      |
| `p99(multigateway.buffer.failover.duration)` per shard | 4.2 s  |
| `max_over_time(queue.depth)` over 7 days               | 950    |

Picks:

- `--connpool-drain-grace-period` = **2s** (≈ 3× p99 graceful drain)
- `--buffer-window` = **10s** (≈ 2.4× p99 failover duration)
- `--buffer-max-failover-duration` = **15s** (window + margin)
- `--buffer-size` = **2000** (≈ 2× observed peak)
- `--buffer-min-time-between-failovers` = **1m** (default; prevents
  rapid re-entry under flapping)

Memory budget at peak: `2000 × ~8 KiB ≈ 16 MiB` per gateway instance.

## Continuous Validation

Alert on **any** of these — they all map to client-visible failures
during a failover:

```promql
# Buffer ran out of capacity.
rate(multigateway_buffer_requests_evicted_total{reason="buffer_full"}[5m]) > 0

# Failover took longer than --buffer-window or --buffer-max-failover-duration.
rate(multigateway_buffer_requests_evicted_total{reason=~"window_exceeded|max_duration"}[5m]) > 0

# Pooler drain exceeded grace period and force-closed connections.
rate(mg_pooler_drain_outcome_total{outcome="force_close"}[1h]) > 0
```

For dashboards, track the histograms at p50/p90/p99 over a 24h window
so you can spot trends before they become alerts.

## Common Pitfalls

- **Sizing from defaults without measurement.** The defaults assume a
  failover under 10 s; if your consensus layer is slower or your
  pooler holds long transactions, the defaults will silently produce
  evictions. Always measure first.
- **Setting `--buffer-window` smaller than `--connpool-drain-grace-period`.**
  The gateway buffer must outlast the pooler drain, otherwise queries
  expire before the new primary is even ready to accept them.
- **Ignoring `--buffer-min-time-between-failovers`.** During flapping,
  this flag prevents the gateway from re-entering BUFFERING and
  thrashing. The default of 1 minute is conservative; lower it only
  if you have legitimate fast failover scenarios.
- **Sizing `--buffer-size` from query rate alone.** Depth is rate ×
  duration, but it also depends on retry concurrency
  (`--buffer-drain-concurrency`). Measure the gauge — don't compute
  it from rate.
- **Confusing `wait.duration` with `failover.duration`.**
  `wait.duration` is per-request; some requests arrive late in the
  failover and wait briefly. `failover.duration` is per-shard-per-event.
  Size `--buffer-window` against `failover.duration`, not
  `wait.duration`.

## References

- [Failover Buffering design](./failover_buffering.md)
- [Graceful Drain design](./graceful_drain_design.md)
- Metric source: `go/services/multigateway/buffer/metrics.go`,
  `go/services/multipooler/poolerserver/metrics.go`
