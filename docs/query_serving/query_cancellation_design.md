# Query Cancellation

## Overview

Multigres supports cross-gateway query cancellation so that a client
can cancel a running query even when the cancel request arrives at a
different gateway than the one executing the query. In PostgreSQL's
protocol, cancel requests are sent on a brand-new TCP connection with
no guarantee it reaches the same server, a fundamental challenge in a
multi-gateway deployment.

Key capabilities:

- **Virtual PIDs** that encode a gateway prefix, enabling stateless
  routing of cancel requests to the correct gateway
- **Cross-gateway forwarding** via gRPC when a cancel request arrives
  at a gateway that does not own the connection
- **Best-effort semantics** matching PostgreSQL's cancel protocol — no
  response, no retries, no guarantee of cancellation
- **SQLSTATE `57014` compatibility** with distinct messages for user
  cancel vs statement timeout
- **Secret key authentication** preventing unauthorized cancellation
  of arbitrary queries

## Background

PostgreSQL's `CancelRequest` protocol works as follows:

1. During connection startup, the backend sends a `BackendKeyData`
   message containing a **process ID** (PID) and a **secret key**.
2. To cancel a running query, the client opens a **separate TCP
   connection** and sends a `CancelRequest` message with the saved PID
   and secret key.
3. The server looks up the connection by PID, verifies the secret key,
   and signals the backend process.
4. The cancel connection is **always closed immediately**, there is no
   response indicating success or failure.
5. Cancellation is **best-effort**: the query may complete before the
   signal is processed, or the signal may be lost entirely.

In a single-server deployment the PID is the actual PostgreSQL backend
process ID. In Multigres, clients connect to gateways that proxy to
pooled backend connections, so real backend PIDs are not meaningful to
clients. The gateway must synthesize virtual PIDs that allow routing
cancel requests back to the correct gateway and connection.

## Architecture

```text
                      Client
                        │
                        │ CancelRequest(virtual PID, secret key)
                        │ (new TCP connection)
                        │
                        ▼
        ┌───────────────────────────────────┐
        │       Gateway A (receiver)        │
        │                                   │
        │  1. Decode prefix from virtual PID│
        │  2. Is prefix == my prefix?       │
        └──────────┬───────────┬────────────┘
                   │           │
              YES  │           │  NO
                   │           │
                   ▼           ▼
             ┌─────────┐  ┌──────────────────┐
             │  Local   │  │  Lookup prefix   │
             │  cancel  │  │  in cache        │
             └────┬─────┘  └────────┬─────────┘
                  │                 │
                  │                 ▼
                  │     ┌───────────────────────┐
                  │     │  Forward via gRPC     │
                  │     └───────────┬───────────┘
                  │                 │
                  │                 ▼
                  │     ┌───────────────────────┐
                  │     │  Gateway B (owner)    │
                  │     │                       │
                  │     │  gRPC CancelQuery     │
                  │     │  → local cancel       │
                  │     └───────────┬───────────┘
                  │                 │
                  ▼                 ▼
            ┌───────────────────────────┐
            │  Verify secret key        │
            │  Cancel query context     │
            │  (errQueryCanceled)       │
            └─────────────┬─────────────┘
                          │
                          ▼
            Query handler observes ctx.Done()
            → returns SQLSTATE 57014 to client
```

The cancel TCP connection is closed immediately after dispatching,
regardless of outcome, matching PostgreSQL's protocol behavior.

## PID Encoding

Gateways do not own real PostgreSQL backend PIDs, a pooled backend
connection may be shared across many client sessions. Instead, each
gateway assigns **virtual PIDs** that encode two pieces of information
in a single 32-bit integer:

```text
┌─────────────────────────────────────────────┐
│              32-bit Virtual PID             │
├─────────┬────────────────┬──────────────────┤
│  Bit 31 │  Bits 20–30    │  Bits 0–19       │
│  (zero) │  Gateway       │  Local Connection│
│         │  Prefix        │  ID              │
├─────────┼────────────────┼──────────────────┤
│  0      │  0–2,047       │  0–1,048,575     │
└─────────┴────────────────┴──────────────────┘
```

<!-- markdownlint-disable MD013 -->

| Field               | Bits | Range       | Purpose                                       |
| ------------------- | ---- | ----------- | --------------------------------------------- |
| Reserved (zero)     | 1    | 0           | Keeps PID positive as PostgreSQL signed Int32 |
| Gateway prefix      | 11   | 1–2,047     | Identifies which gateway owns the connection  |
| Local connection ID | 20   | 0–1,048,575 | Identifies the connection within that gateway |

<!-- markdownlint-enable MD013 -->

This gives a capacity of ~2K gateways × ~1M concurrent connections
per gateway, which is well beyond practical deployment sizes. Bit 31
is kept clear so that PIDs are always positive when interpreted as
PostgreSQL's signed Int32, avoiding negative PIDs in monitoring tools
and client libraries (pgAdmin, Datadog, pgx, lib/pq, PgBouncer).

**Prefix assignment:** When a gateway registers in the topology, it
selects a random unused prefix from the available range, then verifies
no collision exists. On restart, it reuses its previously registered
prefix. The prefix is stored in the topology alongside the gateway's
hostname and gRPC port, making it discoverable by other gateways.

**Why virtual PIDs?** The encoding enables **stateless routing**, any
gateway that receives a cancel request can determine the owning
gateway purely from the PID value, without maintaining a global
connection registry or contacting a coordination service.

## Cross-Gateway Routing

When a cancel request arrives, the cancel manager extracts the
11-bit prefix and compares it to its own. If they match, the cancel
is handled locally. Otherwise, the request must be forwarded.

### Prefix cache

Each gateway maintains a **prefix cache** mapping gateway prefixes to
gRPC addresses. The cache is designed for the read-heavy cancel path:

- **Lock-free reads:** The cache is stored behind an atomic pointer.
  Cancel requests read the current map without acquiring any lock.
- **Atomic replacement:** Rebuilds produce a new map and swap it in
  with a single atomic store. No partial updates are visible.
- **Lazy rebuild on miss:** If a cancel request references an unknown
  prefix (e.g., a newly added gateway), the cache is rebuilt
  immediately from the topology before retrying.
- **Periodic refresh:** A background goroutine rebuilds the cache
  every 5 minutes to pick up topology changes proactively.

The cache is built by scanning all cells in the topology for
registered gateways and extracting their PID prefix and gRPC port.

### Forwarding

Once the target gateway's gRPC address is resolved from the prefix
cache, the cancel manager forwards the request via gRPC:

```text
CancelQueryRequest {
    process_id:  <original virtual PID>
    secret_key:  <original secret key>
}
```

The receiving gateway's gRPC handler invokes the same local cancel
logic, look up the connection by PID, verify the secret key, and
cancel the query context.

**Best-effort semantics:** Forwarding does not retry on failure. If
the target gateway is unreachable or the connection has already
closed, the cancel is silently dropped. This matches PostgreSQL's
cancel semantics where cancellation is never guaranteed.

**gRPC connection management:** Connections to peer gateways are
cached and reused. An idle timeout automatically closes unused
connections, so gateways that rarely forward cancels do not hold
open long-lived connections.

## Query Context Cancellation

When a cancel request reaches the owning gateway and passes secret
key verification, the system cancels the query through Go's context
cancellation:

1. **Query start:** Before executing a query, the connection creates a
   child context with `context.WithCancelCause`. The cancel function
   is stored on the connection.
2. **Cancel signal:** The cancel handler calls the stored cancel
   function with a query-canceled sentinel error as the cause.
3. **Handler observes cancellation:** The query handler sees
   `ctx.Done()` and returns. The connection checks the context's
   cancel cause to determine the appropriate error.
4. **gRPC propagation:** Because the query context is the same context
   passed through gRPC to the multipooler, the cancellation
   propagates automatically — the multipooler observes the cancelled
   context and can cancel the backend query.
5. **Cleanup:** After each query, the cancel function is cleared
   (via defer) so that late-arriving cancel requests for a completed
   query are harmless no-ops.

This design means there is no window where a cancel request can
affect a _subsequent_ query on the same connection, the cancel
function only exists for the duration of the active query.

## Error Handling

Both query cancellation and statement timeout produce SQLSTATE
`57014`, matching PostgreSQL's `query_canceled` error class. The
message text distinguishes the cause:

<!-- markdownlint-disable MD013 -->

| Cause             | SQLSTATE | Message                                        |
| ----------------- | -------- | ---------------------------------------------- |
| Cancel request    | `57014`  | `canceling statement due to user request`      |
| Statement timeout | `57014`  | `canceling statement due to statement timeout` |

<!-- markdownlint-enable MD013 -->

**Priority:** When a cancel request and a statement timeout race
(both can trigger on the same query), the cancel cause takes
priority. The error resolution logic checks for the cancel sentinel
in the context cause first, and only falls back to deadline-exceeded
handling if no explicit cancel was issued. This ensures that a user
who explicitly cancelled a query sees "user request" rather than a
potentially confusing timeout message.

## Future Work

- **Cancel metrics:** Instrument cancel request counts, forwarding
  latency, and success/failure rates for observability.
- **Rate limiting:** Protect against cancel request floods that could
  be used to probe virtual PIDs or overload the prefix cache rebuild
  path.
- **Cancel forwarding to multipooler:** Issue a PostgreSQL-level
  cancel to the backend via the multipooler's admin connection, in
  addition to context cancellation, to promptly free backend
  resources for long-running queries.
