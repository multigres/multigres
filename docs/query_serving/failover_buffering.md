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
│                     Multigateway                         │
│                                                          │
│  PoolerGateway.withBuffering()                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │ 1. Proactive: WaitIfAlreadyBuffering()             │  │
│  │    (skip round-trip if shard is already failing)    │  │
│  │                                                    │  │
│  │ 2. GetConnection → Execute query                   │  │
│  │                                                    │  │
│  │ 3. Reactive: classifyError()                       │  │
│  │    no-primary, MTF01, safe 25006/0A000             │  │
│  │    → WaitForFailoverEnd()                          │  │
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
              │ Multipooler           │
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
trigger buffering. Only leader-routed queries are buffered:

| Error                              | Source                             | Meaning                                                                        |
| ---------------------------------- | ---------------------------------- | ------------------------------------------------------------------------------ |
| no writable primary                | gateway routing                    | No leader is currently available                                               |
| `MTF01`                            | multipooler `StartRequest()`       | Pooler is not serving or is draining during failover                           |
| internal pre-execution unavailable | multipooler connection acquisition | PostgreSQL was unavailable before query execution or reservation state changed |
| `25006`                            | PostgreSQL                         | Replay-safe request hit a primary that has already been demoted                |
| exact recovery `0A000`             | PostgreSQL                         | A deferred `BEGIN READ WRITE` reached a standby before its first statement     |

`MTF02` (a fenced target, below) is deliberately absent: it is never
buffered.

`25006` and recovery `0A000` are replay-safe only for a single autocommit
query or for the first statement of a deferred explicit transaction that was
not declared `READ ONLY`. Other occurrences pass through to the application.
The pre-execution marker is safe by construction: the multipooler emits it only
before the request can change backend state. It remains internal across gRPC;
clients receive the original PostgreSQL diagnostic, or `57P03` when the cause
was a transport failure.

### Buffering Loop

`PoolerGateway.withBuffering()` wraps every query execution in a
retry loop (capped at `MaxBufferingRetries`):

1. **Proactive check**: call `WaitIfAlreadyBuffering()` — if the
   shard is already known to be failing over, wait before sending
   any query (avoids a wasted round-trip)
2. **Get connection**: `LoadBalancer.GetConnection(target)`
3. **Execute**: run the query on the connection
4. **Classify error**: for a buffer-worthy availability error, call
   `WaitForFailoverEnd()` and retry

### Per-Shard State Machine

Each shard has an independent state machine:

```text
IDLE ──────────► BUFFERING ──────────► DRAINING ──────────► IDLE
     first retryable      StopBuffering()       all entries
     error arrives        (health stream        retried
                           detects new primary)
```

- **IDLE**: no failover in progress. Requests pass through normally.
- **BUFFERING**: failover detected. New requests are queued in the
  global FIFO. A timing guard (`MinTimeBetweenFailovers`) prevents
  rapid re-entry.
- **DRAINING**: a primary has been observed. Buffered entries are extracted
  and retried with configurable concurrency (`DrainConcurrency`). A subsequent
  failure is a new incident and remains subject to `MinTimeBetweenFailovers`.

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
When a health stream self-reports `PRIMARY` and `SERVING`, the
`LoadBalancer` updates its cached primary and invokes the drain callback, which
calls `buffer.StopBuffering()` for that shard. Topology-only updates do not
trigger drains because their corresponding health snapshot may still be stale.

This approach has several advantages:

- **Direct signal**: the health stream comes from the pooler itself,
  not via etcd propagation
- **Lower latency**: no topology watch delay
- **Consistent**: the same mechanism that determines query routing
  (health-based primary cache) also stops buffering

### Eviction and Safety Rails

Several mechanisms prevent unbounded buffering:

| Mechanism             | Error        | Trigger                                                   |
| --------------------- | ------------ | --------------------------------------------------------- |
| Buffer full           | `MTB01`      | Global queue at capacity — oldest entry evicted           |
| Per-request window    | `MTB02`      | Individual request buffered longer than `Window`          |
| Max failover duration | retry result | Entire shard force-drained after `MaxFailoverDuration`    |
| Gateway shutdown      | `MTB03`      | Gateway is shutting down — all entries evicted            |
| Timing guard          | (no buffer)  | `MinTimeBetweenFailovers` not elapsed since last failover |

### Fenced Targets (`MTF02`)

While an operation moves a target's data, its poolers can be fenced: they
refuse new application work so that two placements of the same data never take
writes at once. A pooler refuses fenced work with `MTF02` before running any of
it.

A fenced shard never gets a new leader, so `MTF02` does not go through the
failover buffer. The change that lifts a fence (a routing switch, a revert, an
unfence) always advances the catalog's `routing_version`, and it can change the
statement's plan, not only its targets: table rules apply when a statement is
planned. So the executor, like Vitess's, retries a fenced statement in every
mode:

1. Wait until the sharding catalog cache has installed a snapshot whose
   `routing_version` is greater than the one the refused attempt planned
   against (`shardingcatalogcache.Cache.WaitForRoutingVersion`, which triggers
   a fetch at once).
2. Plan the statement again.
3. Run it again.

A statement is not run again when the session is in a transaction, explicit or
implicit, or when output has already reached the client (an earlier Route of
the plan may have streamed rows). The client then gets the `MTF02`, which tells
it to retry. A gateway a version behind the fence may run the statement once on
the snapshot that put the fence up and be refused again; its next wait reaches
the snapshot that lifts it.

The waits of one statement share one `--buffer-window`, whether or not
`--buffer-enabled` is set, and a statement runs again at most three times. When
either bound runs out, the statement fails with the last `MTF02`. A single
pooler (minigres) has nothing to fence, so its gateway never retries. Work on
an existing reserved connection is never fenced, since the pooler always admits
it.
`mg.executor.fence.wait.duration` records each fenced statement's wait and its
outcome.

## Configuration

| Flag                                  | Default | Description                                                                                                         |
| ------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------- |
| `--buffer-enabled`                    | `false` | Enable failover buffering                                                                                           |
| `--buffer-window`                     | `10s`   | Max time a single request stays buffered, or waits for routing past a fence (`MTF02`, even with buffering disabled) |
| `--buffer-size`                       | `1000`  | Max total buffered requests (global across all shards)                                                              |
| `--buffer-max-failover-duration`      | `20s`   | Force-stop buffering if failover exceeds this                                                                       |
| `--buffer-min-time-between-failovers` | `1m`    | Min gap between failovers per shard                                                                                 |
| `--buffer-drain-concurrency`          | `1`     | Parallel retries during drain phase                                                                                 |

## Testing

- **`buffer/buffer_test.go`**: unit tests for buffering lifecycle,
  global eviction, window timeout, max failover duration, timing
  guard, multi-shard independence, and retry completion signaling
- **`poolergateway/pooler_gateway_test.go`**: unit tests for
  `classifyError` and `withBuffering` retry loop
- **`test/endtoend/queryserving/buffer_test.go`**: end-to-end
  integration test that runs continuous writes through a
  multigateway, triggers a planned failover, and asserts zero
  failed writes
