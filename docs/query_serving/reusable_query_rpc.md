# Reusable query RPC transport

Reusable query streams are always enabled.
`--grpc-stream-workers=0` restores gRPC's goroutine-per-stream default
without changing the query transport or its completion acknowledgement.

The gateway sends sequential SQL, portal, COPY and unary query-service operations
over one bidirectional `ExecuteStream` RPC. This reuses HTTP/2 stream state and a
grown server-handler stack; it does **not** merge SQL operations, pipeline
transactions, change durability, or pool PostgreSQL sessions differently.
Elastic regular/reserved quotas remain unchanged.

Each lease is exclusive until an explicit completion frame. Every operation
carries its original target, caller identity, execution/reservation options,
trace/baggage context, and remaining deadline. The corresponding existing pooler handler runs
admission and reservation validation on every operation. SQL errors retain
their gRPC status and PostgreSQL diagnostic details.

The client waits for a readiness handshake advertising supported operation
types before sending work. Servers must implement ExecuteStream and advertise
support for the requested operation. A missing RPC, missing capability list, or
unsupported operation returns UNIMPLEMENTED before submission; it does not select
a dedicated RPC.
The original request/response field numbers remain unchanged for wire compatibility. There is no automatic
replay after sending SQL, including after an ambiguous send/receive failure.
Cancellation, an abandoned callback, or a malformed response discards the
leased stream. A completed SQL error may return an otherwise healthy stream.
Late cancellation from a completed operation cannot cancel its next borrower.

Each gateway-to-pooler connection retains at most 32 idle streams, expiring
after 30 seconds; a stream retires after 1,024 operations. Active leases are
not capped by this idle cache and do not replace backend pool admission.
Closing the owning connection cancels both idle and active leases. When the
underlying gRPC connection leaves READY (pooler restart, network loss) the
pool drops every idle stream immediately: an idle stream on a dead transport
would otherwise fail its next `Send`, which is a post-submission error and is
never retried, whereas opening a fresh stream fails before SQL is sent and
takes the ordinary retryable pre-execution path. A stream is not tied to a
reservation; reservation IDs and lifecycle remain explicit.
Notifications, replication, authentication and health RPCs retain their
existing protocols.

## Operation envelopes

The request and response envelopes use typed protobuf oneofs. StreamExecute and
PortalStreamExecute may send multiple results and diagnostics before completion.
ExecuteQuery, Describe, ConcludeTransaction, DiscardTempTables and
ReleaseReservedConnection send exactly one typed response on success, followed
by completion. On failure, unary responses are omitted and the original gRPC
status/details are preserved, including surviving reservation state attached to
transaction errors. The gateway never returns a unary response before consuming
completion, and rejects missing, duplicate or mismatched responses.

The shared server loop owns framing, telemetry propagation and operation
contexts; adapters dispatch to the existing handlers. The gateway's stream pool
owns capability negotiation, exclusive leasing, cancellation and retirement.
Typed receivers adapt the transport to the existing QueryService interface.
COPY FROM STDIN and COPY TO STDOUT use the existing COPY request/response
messages inside the shared envelope. INITIATE starts the operation; subsequent
DATA, DONE and FAIL requests use `copy_input`. The lease remains exclusive
through READY, data transfer, and the terminal RESULT/ERROR plus completion.
`CloseSend` sends an explicit `copy_input.end` instead of closing the transport.
The server discards any unread COPY input through this boundary before reading
the next operation. This includes data queued before an early backend error.
Completion may precede the input boundary so an admission failure cannot deadlock
against a client waiting for READY. The client must send the boundary and receive
completion before returning the lease. ERROR payloads retain their diagnostics,
notices and surviving reservation state, even when completion also reports an error.
If COPY FROM fails after READY but before DONE/FAIL, the server marks completion with `retire`
and closes the transport to interrupt a client still uploading. That transport
is discarded; terminal diagnostics and reservation state can still be read.
Abandoned output callbacks and incomplete/malformed conversations also discard the lease.
Peers that do not advertise COPY support are rejected before submission.

Statement deadlines use the existing per-operation timeout and completion fields.
The gateway owns the timeout policy; the pooler's operation context expires and
its existing handler cancels/drains the backend operation. The gateway keeps the
transport open until completion acknowledges that the handler has returned,
including its cleanup. It then returns the statement timeout. Late result rows
are discarded while reservation updates are retained. Callback failures at the
deadline also wait for completion before allowing `ReadyForQuery`.

The cleanup wait is bounded to 10 seconds after the gateway deadline, allowing
for the pooler's existing 5-second backend cancellation and 2-second drain bounds.
Missing completion, a broken transport during timeout recovery, or expiry of that
bound produces FATAL SQLSTATE 08006 and closes the client session without
`ReadyForQuery`. Dedicated RPC fallbacks cannot acknowledge timeout cleanup and
also fail the session on timeout. A transport handshake that times out before
submission still returns an ordinary statement timeout.

COPY input reads observe the operation context independently of the transport,
so an idle upload can enter the existing abort/drain path without destroying the
completion channel. One input reader per COPY stops at the explicit input
boundary; timed-out COPY transports retire after completion. COPY handlers or
backend I/O that cannot finish within the cleanup bound follow the same fatal
failure path. Finalization and output callbacks preserve terminal reservation
state while waiting. This does not change how the frontend reads COPY input.

Explicit cancellation (including CancelRequest and client disconnect) still
cancels the transport immediately. The current protocol has no independent
operation-cancel message, so acknowledgement for explicit cancellation remains
a separate change. A fatal cleanup failure always takes precedence over a
concurrent CancelRequest when reporting the result to the client.
Unary operations retain their conservative retry classification; no operation
is replayed after submission, even when its completion status is UNAVAILABLE.

Requests carrying outgoing RPC metadata or nonstandard propagation use the
dedicated per-call path. Servers reject reuse with authorization headers before
the readiness handshake using a structured error detail. This also covers credentials
added by gRPC after the gateway has selected a transport; a plain UNIMPLEMENTED
error never selects a dedicated RPC. This preserves per-call credential validation for
the standard authenticated path; deployments adding custom call-level
interceptors/credentials must assess their compatibility with transport reuse.

Outgoing metadata, unsupported propagation, or an explicit server requirement
for per-call authorization selects a dedicated RPC before submission. Successful fallback
operations behave normally, but an in-flight statement timeout closes the client
session with FATAL SQLSTATE 08006 because cleanup cannot be acknowledged.
The `legacy_*` transport metrics below identify which fallback is being used.

## Interceptor audit

A long-lived `ExecuteStream` runs the server's per-RPC machinery once per
transport instead of once per statement. The multipooler server registers
exactly two such hooks (`servenv/grpc_server.go`); nothing in
`go/services/multipooler` adds interceptors of its own.

| Hook                          | Scope                                 | Effect under reuse                                                                                                                                                                                                                             |
| ----------------------------- | ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Auth interceptor, mTLS plugin | Peer certificate, connection-scoped   | Identity is a property of the TLS connection carrying the stream; validating it once per transport is equivalent to once per call.                                                                                                             |
| Auth interceptor, JWT plugin  | `authorization` metadata, call-scoped | Token expiry would otherwise widen to the stream lifetime. `Serve` refuses reuse before the ready frame with `UNIMPLEMENTED` and a structured authorization requirement; the gateway selects a dedicated RPC so tokens are validated per call. |
| `otelgrpc` stats handler      | Per RPC                               | Produces one transport span and `rpc.*` measurements per stream. `Serve` reconstructs a per-operation server span from the request's propagation fields so SQL traces stay per statement.                                                      |

Admission, fencing, reservation ownership, session settings and diagnostics are
not interceptor concerns: they live inside the existing query-service handlers,
which `Serve` invokes for each typed operation. These handlers currently use
status details, not per-call gRPC headers/trailers; any future header/trailer
semantics must be represented per operation before enabling reuse for them.

`--grpc-stream-workers=64` enables reusable server workers. Zero restores
gRPC's default goroutine-per-stream behavior. Busy workers fall back to new
goroutines, so the setting is not a SQL concurrency limit. Long-lived
replication keepalive settings are unchanged.

## Observability

SQL-level gateway, scatter and pooler histograms/counters retain their names,
labels and buckets. Immutable attribute sets are cached for the finite
pool-type/status/outcome vocabulary and a bounded scatter cache; disabled
instruments skip attribute construction. Error classification is unchanged.

Standard `rpc.*` measurements for ExecuteStream describe **transport streams**,
not individual SQL operations: their duration includes idle reuse time and
their counts cannot be used as query throughput. Use `mg.pooler.query.*`,
`mg.gateway.query.*` and scatter metrics for query latency/count comparisons.
Per-operation pooler spans reconstruct the request's tracing context; the
enclosing transport span has a separate lifetime. Dashboards must distinguish
SQL operations from RPC streams. Transport framing and message metrics still
remain enabled.

The gateway pool exports its own low-cardinality instruments. Attribute values
come from fixed vocabularies; SQL text, reservation, caller and stream
identifiers are never labels.

`legacy_authorization` means the server requires per-call authorization.
`legacy_metadata` and `legacy_propagation` preserve request metadata and context
that cannot be carried in the operation envelope.

| Instrument                           | Attributes                                                                          | Meaning                                                                                                                       |
| ------------------------------------ | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `mg.gateway.query_stream.operations` | `transport=new\|reused\|legacy_authorization\|legacy_metadata\|legacy_propagation`  | Operations by transport; `new` counts submitted operations on new streams, `legacy_*` counts dedicated-RPC fallbacks and why. |
| `mg.gateway.query_stream.discards`   | `reason=cancelled\|incomplete\|transport\|retired\|idle_full\|idle_expired\|closed` | Streams closed, by cause. Normal retirement appears here, not as a failure.                                                   |
| `mg.gateway.query_stream.active`     | none                                                                                | Streams currently leased to an operation.                                                                                     |
| `mg.gateway.query_stream.idle`       | none                                                                                | Streams retained idle for reuse.                                                                                              |
