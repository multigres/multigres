# Query Plan Cache in Multigres

## Overview

This document describes the design of the query plan cache in Multigres. The
plan cache stores the result of query planning (routing decisions, shard
targeting, etc.) so that repeated queries with different literal values can
skip planning and reuse a previously computed plan.

The cache lives inside multigateway and is shared across all client connections
on a single gateway instance.

## Background: Why Cache Plans?

For every query that arrives at multigateway, the executor must:

1. Parse the SQL text into an AST
2. Analyze the AST to pick a route (which shard, which primitive)
3. Execute the chosen primitive

Steps 1 and 2 are collectively called _planning_. Planning is deterministic for
a given query shape — `SELECT * FROM users WHERE id = 1` and
`SELECT * FROM users WHERE id = 99` produce the same routing decision. Paying
the planning cost on every execution is wasteful when the same query pattern
arrives thousands of times per second.

The plan cache avoids replanning by mapping a normalized query string to a
previously computed `Plan` object.

## Query Normalization

### The Problem

Literal values in a query change its text without changing its plan. To build
a shared cache key across executions with different literals, we strip literals
out and replace them with positional parameter markers.

### The `Normalize` Function

`go/common/parser/ast/normalizer.go` provides `Normalize(stmt)`:

- Walks the AST and replaces every `A_Const` leaf (a literal value) with a
  `$1`, `$2`, ... parameter reference in order of appearance
- Returns the normalized SQL string (used as the cache key) and the extracted
  bind values (used at execution time)

**Example**:

```sql
Input:  SELECT * FROM users WHERE id = 42 AND region = 'us-east'
Output: SELECT * FROM users WHERE id = $1 AND region = $2
Bind:   [42, 'us-east']
```

### Skipped Cases

Normalization is intentionally skipped for nodes where the literal value
affects planning, not just execution:

| Node type                 | Reason                                                                          |
| ------------------------- | ------------------------------------------------------------------------------- |
| `NULL` constants          | Semantic keyword; `WHERE x = NULL` and `WHERE x = $1` can mean different things |
| `VariableSetStmt`         | `SET work_mem = '64MB'` — value is part of the command                          |
| `VariableShowStmt`        | `SHOW <variable>` — name is the operand                                         |
| `DefElem`                 | Used inside DDL option lists where literal values affect the DDL itself         |
| `SELECT INTO` temp tables | Temporary table semantics depend on literal names                               |

Queries that cannot be normalized fall through to the normal (uncached) path.

### Cross-Protocol Unification

PostgreSQL clients speak two protocols:

- **Simple protocol**: Query arrives as a complete SQL string with literal
  values embedded (`SELECT * FROM t WHERE id = 42`)
- **Extended protocol**: Query arrives pre-parameterized (`SELECT * FROM t WHERE id = $1`)
  with bind values sent separately in the Bind message

After normalization, both produce the same cache key. This means a plan built
from a simple-protocol execution can be reused for an extended-protocol
execution of the same query shape and vice versa.

### SQL Reconstruction

At execution time, the route primitive needs the final SQL to send to the
backend. The normalized AST is stored in the `Plan` (as `NormalizedAST`) and
at execution time `ReconstructSQL(normalizedAST, bindVars)` substitutes the
current bind values back in. This avoids re-parsing the query on every
execution.

## Cache Implementation

### Eviction: W-TinyLFU

The cache is backed by a W-TinyLFU (Window Tiny Least Frequently Used)
implementation vendored in `go/common/cache/theine/`.

W-TinyLFU maintains two regions:

- **Window cache** (1% of capacity): Admits all new entries; acts as a
  probationary zone
- **Main cache** (99% of capacity): Only accepts entries that pass a frequency
  filter (the TinyLFU sketch)

The frequency filter uses a Count-Min Sketch and a Bloom filter doorkeeper:

- The **Bloom filter** quickly rejects one-time queries from entering the main
  cache at all, protecting against cache pollution by rare queries
- The **Count-Min Sketch** tracks approximate access frequency and decides
  whether a candidate entry is more valuable than the entry it would evict

This eviction policy is well-suited to database query workloads, where a small
hot set of queries dominates traffic and one-off queries (e.g., ad-hoc
analytical queries) should not evict popular plans.

### Concurrency

The store is partitioned into independent shards. Each shard has its own lock,
so concurrent reads and writes across different shards proceed without
contention. A shard is selected by hashing the cache key.

A `singleflight` group prevents thundering-herd problems: if many goroutines
miss the cache simultaneously for the same key, only one plans the query; the
rest wait for the result.

### Memory Accounting

The cache is capacity-bounded by memory. Each `Plan` reports its memory cost
via `CachedSize(bool) int64`, which accounts for:

- The `Plan` struct overhead
- The length of the normalized SQL string
- An estimate of the normalized AST tree (~10× the query string length)

The cache enforces the configured memory limit and evicts entries when the
limit is reached.

## Invalidation

Schema changes (DDL) can invalidate all cached plans. Iterating through the
entire cache to remove entries would be O(n) and would require holding locks.

Instead, the cache uses **epoch-based invalidation**:

- Each cache entry is tagged with the epoch at the time of insertion
- A global epoch counter is stored atomically alongside the cache
- `Invalidate()` increments the epoch counter in O(1)
- `Get()` compares the entry's epoch against the current epoch; a stale entry
  is treated as a cache miss and the caller replans

This means DDL statements (ALTER TABLE, CREATE INDEX, DROP TABLE, etc.) call
`Invalidate()`, and all subsequent `Get()` calls see a clean slate without
any locking or iteration.

## Configuration

| Flag                  | Env var                | Default          | Description                                                        |
| --------------------- | ---------------------- | ---------------- | ------------------------------------------------------------------ |
| `--plan-cache-memory` | `MT_PLAN_CACHE_MEMORY` | `4194304` (4 MB) | Maximum memory for the plan cache in bytes. Set to `0` to disable. |

The plan cache is initialized in `go/services/multigateway/init.go` and passed
to the executor at startup. Setting `planCacheMemory` to `0` disables caching
entirely; all queries fall through to the uncached planning path.

## Executor Integration

### Cacheable Statements

Only DML statements that go through `planDefault()` are cached:

- `SELECT` (excluding `SELECT INTO` temporary tables)
- `INSERT`
- `UPDATE`
- `DELETE`

Statements that bypass the cache:

- DDL (`CREATE`, `ALTER`, `DROP`, `TRUNCATE`, …)
- Transaction control (`BEGIN`, `COMMIT`, `ROLLBACK`)
- Session commands (`SET`, `SHOW`)
- `LISTEN` / `NOTIFY`
- Any statement where normalization is skipped

### Simple Protocol Path (`StreamExecute`)

```text
Client sends: SELECT * FROM orders WHERE id = 7
                          │
                     Parse → AST
                          │
                     Normalize
                          │
                  cache key: "SELECT * FROM orders WHERE id = $1"
                  bind vals: [7]
                          │
              ┌───── Cache lookup ─────┐
              │ HIT                   │ MISS
              │                       │
         Reuse plan              Plan normalized SQL
              │                       │
              └──────────┬────────────┘
                         │
              ReconstructSQL(normalizedAST, [7])
                         │
              Execute → backend SQL: SELECT * FROM orders WHERE id = 7
```

### Extended Protocol Path (`PortalStreamExecute`)

Extended protocol queries already arrive with `$1`, `$2`, … placeholders and
bind values in separate Bind messages. The normalized SQL is effectively the
parameterized query text itself. The executor looks up the cache using that
text, and on a miss plans it and caches the result. On subsequent executions
(or on simple-protocol executions of the same shape), the cached plan is
reused.

### ExecuteResult

The `ExecuteResult` struct returned by both paths includes a `CacheHit bool`
field, allowing callers and tests to observe whether a given execution used the
plan cache.

## Metrics

Two OpenTelemetry counters are emitted:

| Metric                | Description                                    |
| --------------------- | ---------------------------------------------- |
| `mg.plancache.hits`   | Number of executions that reused a cached plan |
| `mg.plancache.misses` | Number of executions that required planning    |

In addition, `plancache.Evictions()` returns the total number of entries
evicted by the W-TinyLFU policy, useful for tuning the memory limit.

## Data Flow Summary

```text
                   ┌──────────────┐
Client query ─────►│   Executor   │
                   └──────┬───────┘
                          │
                    Normalize SQL
                          │
                   ┌──────▼───────┐   HIT    ┌─────────────┐
                   │  Plan Cache  │──────────►│ Cached Plan │
                   └──────┬───────┘           └──────┬──────┘
                     MISS │                          │
                          │                          │
                   ┌──────▼───────┐                  │
                   │   Planner    │                  │
                   └──────┬───────┘                  │
                          │ store plan                │
                          ▼                          │
                   ┌─────────────────────────────────▼──────┐
                   │  ReconstructSQL(normalizedAST, bindVars)│
                   └────────────────────┬───────────────────┘
                                        │
                                 Execute on backend
```

## Design Trade-offs

### Shared Cache vs. Per-Connection Cache

The plan cache is shared across all client connections on a gateway instance.
This maximizes cache utility — a plan computed for one connection is
immediately available to all others. The downside is that the cache must be
thread-safe, which is handled by shard-level locking inside Theine.

### Normalized SQL as Cache Key

Using the normalized SQL string as the cache key is simple and correct. An
alternative would be to use the AST directly (e.g., a structural hash), but
string keys are easier to reason about, trivially comparable, and avoid the
complexity of defining equality over AST nodes.

### Rough AST Size Estimate in `CachedSize`

The `CachedSize` function estimates AST memory as approximately 10× the query
string length. This is a deliberate approximation — computing an exact size
would require walking the entire AST tree on every cache insertion, adding
latency on the miss path. The approximation keeps memory accounting fast while
remaining directionally correct for capacity management.

### No Per-Table Invalidation

Currently, `Invalidate()` clears the entire cache. A more targeted approach
would only invalidate plans that reference the affected table. This was
intentionally left as a future optimization: DDL is rare, and the cache warms
up quickly after an invalidation event.

## Future Work

### Schema Tracking for Cache Invalidation

`planCache.Invalidate()` exists and works correctly, but nothing calls it yet.
The next step is to wire up DDL detection and schema-change events so that
every DDL statement (ALTER TABLE, CREATE INDEX, DROP TABLE, etc.) triggers an
epoch increment. This is safe to do lazily: `Invalidate()` is O(1), and the
cache warms up quickly after a schema change because DML query shapes are
stable.

### Generated `CachedSize`

The current `Plan.CachedSize()` is a hand-written estimate that uses a
rough multiplier for the normalized AST tree size. This approximation is
acceptable for capacity management but can drift if the plan structure changes.
The right fix is to auto-generate `CachedSize` using a code generator (similar
to Vitess's `cachedsize` tool) that walks all primitive types and AST node
types and emits an exact byte accounting. This would make memory limits
precise and remove the need to update `CachedSize` manually as the plan
structure evolves.
