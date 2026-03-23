# Failover Buffering

## Overview

During planned PRIMARY failovers, there is a window where the old
primary has stopped serving but the new primary hasn't started yet.
Without buffering, every in-flight and new query during this window
fails with an error visible to the application.

Failover buffering holds PRIMARY queries in the gateway during this
window and retries them once a new primary is available, achieving
zero application-visible errors during planned failovers.

Key capabilities:

- **Zero-downtime planned failovers**: queries are transparently
  held and retried — applications see no errors
- **Global FIFO eviction**: when the buffer is full, the oldest
  request globally is evicted (not per-shard), ensuring fair resource
  usage across shards
- **Per-shard independence**: each shard has its own state machine so
  failovers on different shards don't interfere with each other
- **Health-stream-driven**: buffering stops when the streaming health
  check from the new primary reports SERVING

## Background

Multigres uses a multigateway → multipooler → PostgreSQL architecture.
When the orchestrator demotes a primary (e.g., during a planned
failover), the multipooler transitions to `NOT_SERVING` and starts
returning `MTF01` ("planned failover in progress") errors. The
gateway uses these errors to trigger buffering.

Buffering stops when the gateway's streaming health check from the
new primary reports it as serving. This is a direct signal from the
pooler itself — no topology propagation delay.

## Architecture

```text
                         Client
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│                     MultiGateway                         │
│                                                          │
│  PoolerGateway.withBuffering()                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │ 1. Proactive: WaitIfAlreadyBuffering()             │  │
│  │    (skip round-trip if shard is already failing)    │  │
│  │                                                    │  │
│  │ 2. GetConnection → Execute query                   │  │
│  │                                                    │  │
│  │ 3. Reactive: classifyError()                       │  │
│  │    MTF01 or 25006 → WaitForFailoverEnd()           │  │
│  │    (buffer and wait for new primary)               │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  Buffer (global FIFO queue + per-shard state machines)   │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐        │
│  │ shard 0     │ │ shard 1     │ │ shard 2     │        │
│  │ IDLE →      │ │ BUFFERING → │ │ IDLE        │        │
│  │ BUFFERING → │ │ DRAINING →  │ │             │        │
│  │ DRAINING →  │ │ IDLE        │ │             │        │
│  │ IDLE        │ │             │ │             │        │
│  └─────────────┘ └─────────────┘ └─────────────┘        │
│                                                          │
│  LoadBalancer                                            │
│  ┌────────────────────────────────────────────────────┐  │
│  │ onPoolerHealthUpdate()                             │  │
│  │   new primary detected → onPrimaryServing()        │  │
│  │   → buffer.StopBuffering(shard)                    │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
                           │ gRPC
                           ▼
              ┌───────────────────────┐
              │ MultiPooler           │
              │                       │
              │ StartRequest()        │
              │ → MTF01 if NOT_SERVING│
              │ → MTF01 if draining   │
              │                       │
              │ StreamPoolerHealth()  │
              │ → ServingStatus       │
              │ → PrimaryObservation  │
              └───────────────────────┘
```

## Design

### Error Classification

The gateway's `classifyError` determines whether an error should
trigger buffering. Only PRIMARY queries are buffered, and only for
two error codes:

| Error   | Source                       | Meaning                                                                                   |
| ------- | ---------------------------- | ----------------------------------------------------------------------------------------- |
| `MTF01` | multipooler `StartRequest()` | Pooler is not serving or is draining during failover                                      |
| `25006` | PostgreSQL                   | `read_only_sql_transaction` — in-flight query hit a primary that has already been demoted |

All other errors pass through to the application.

### Buffering Loop

`PoolerGateway.withBuffering()` wraps every query execution in a
retry loop (capped at `MaxBufferingRetries`):

1. **Proactive check**: call `WaitIfAlreadyBuffering()` — if the
   shard is already known to be failing over, wait before sending
   any query (avoids a wasted round-trip)
2. **Get connection**: `LoadBalancer.GetConnection(target)`
3. **Execute**: run the query on the connection
4. **Classify error**: if MTF01 or 25006, call
   `WaitForFailoverEnd()` to buffer and retry

### Per-Shard State Machine

Each shard has an independent state machine:

```text
IDLE ──────────► BUFFERING ──────────► DRAINING ──────────► IDLE
     first MTF01          StopBuffering()       all entries
     error arrives        (health stream        retried
                           detects new primary)
```

- **IDLE**: no failover in progress. Requests pass through normally.
- **BUFFERING**: failover detected. New requests are queued in the
  global FIFO. A timing guard (`MinTimeBetweenFailovers`) prevents
  rapid re-entry.
- **DRAINING**: new primary is available. Buffered entries are
  extracted from the global queue and retried with configurable
  concurrency (`DrainConcurrency`).

### Global FIFO Queue

All shards share a single FIFO queue backed by a weighted semaphore
for capacity management. When the buffer is full:

1. The oldest entry globally is evicted (receives `MTB01`)
2. Its semaphore slot is transferred to the new entry

This design ensures fair resource usage: a shard with many buffered
requests doesn't starve other shards of buffer capacity.

### Stopping Buffering via Streaming Health Checks

Each `PoolerConnection` in the gateway maintains a streaming health
check (`StreamPoolerHealth` RPC) to the multipooler it connects to.
When the health stream reports a new primary (via
`PrimaryObservation` with a higher term), the `LoadBalancer` updates
its cached primary and invokes the `onPrimaryServing` callback, which
calls `buffer.StopBuffering()` for that shard.

This approach has several advantages:

- **Direct signal**: the health stream comes from the pooler itself,
  not via etcd propagation
- **Lower latency**: no topology watch delay
- **Consistent**: the same mechanism that determines query routing
  (health-based primary cache) also stops buffering

### Eviction and Safety Rails

Several mechanisms prevent unbounded buffering:

| Mechanism             | Error       | Trigger                                                   |
| --------------------- | ----------- | --------------------------------------------------------- |
| Buffer full           | `MTB01`     | Global queue at capacity — oldest entry evicted           |
| Per-request window    | `MTB02`     | Individual request buffered longer than `Window`          |
| Max failover duration | `MTB02`     | Entire shard buffered longer than `MaxFailoverDuration`   |
| Gateway shutdown      | `MTB03`     | Gateway is shutting down — all entries evicted            |
| Timing guard          | (no buffer) | `MinTimeBetweenFailovers` not elapsed since last failover |

## Configuration

| Flag                                  | Default | Description                                            |
| ------------------------------------- | ------- | ------------------------------------------------------ |
| `--buffer-enabled`                    | `false` | Enable failover buffering                              |
| `--buffer-window`                     | `10s`   | Max time a single request stays buffered               |
| `--buffer-size`                       | `1000`  | Max total buffered requests (global across all shards) |
| `--buffer-max-failover-duration`      | `20s`   | Force-stop buffering if failover exceeds this          |
| `--buffer-min-time-between-failovers` | `1m`    | Min gap between failovers per shard                    |
| `--buffer-drain-concurrency`          | `1`     | Parallel retries during drain phase                    |

## Testing

- **`buffer/buffer_test.go`**: unit tests for buffering lifecycle,
  global eviction, window timeout, max failover duration, timing
  guard, multi-shard independence, and retry completion signaling
- **`poolergateway/pooler_gateway_test.go`**: unit tests for
  `classifyError` and `withBuffering` retry loop
- **`queryserving/buffer_test.go`**: end-to-end integration test
  that runs continuous writes through a multigateway, triggers a
  planned failover, and asserts zero failed writes
