# Reusable query RPC transport

Rollback: `--query-stream-reuse=false` on multigateway restores per-call
query-service RPCs; `--grpc-stream-workers=0` on any server restores gRPC's
goroutine-per-stream default. Both are independent.

The gateway sends sequential SQL, portal and unary query-service operations
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
types before sending work. A server without ExecuteStream returns UNIMPLEMENTED;
a server without a capability list supports only the original StreamExecute
operation. Unsupported operations select their dedicated RPC before submission.
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
COPY, notifications, replication, authentication and health RPCs retain their
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
COPY is intentionally excluded because it needs an interactive operation
lifecycle rather than a request followed by results.

This is a transport-only refactor. Deadline expiration and explicit cancellation
still cancel the active transport immediately; the gateway does not yet wait
for backend cancellation/cleanup. A later wait-for-cancel change can build on the
existing timeout and completion fields without coupling it to this migration.
Unary operations retain their conservative retry classification; no operation
is replayed after submission, even when its completion status is UNAVAILABLE.

Requests carrying outgoing RPC metadata or nonstandard propagation use the
legacy per-call path. Servers reject reuse with authorization headers before
the readiness handshake. This preserves per-call credential validation for
the standard authenticated path; deployments adding custom call-level
interceptors/credentials must assess them before enabling transport reuse.
`--query-stream-reuse=false` (or `MT_QUERY_STREAM_REUSE=false`) disables reuse
on gateway restart, without reverting the worker improvements.

## Interceptor audit

A long-lived `ExecuteStream` runs the server's per-RPC machinery once per
transport instead of once per statement. The multipooler server registers
exactly two such hooks (`servenv/grpc_server.go`); nothing in
`go/services/multipooler` adds interceptors of its own.

| Hook                          | Scope                                 | Effect under reuse                                                                                                                                                                                                        |
| ----------------------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Auth interceptor, mTLS plugin | Peer certificate, connection-scoped   | Identity is a property of the TLS connection carrying the stream; validating it once per transport is equivalent to once per call.                                                                                        |
| Auth interceptor, JWT plugin  | `authorization` metadata, call-scoped | Token expiry would otherwise widen to the stream lifetime. `Serve` refuses reuse with `UNIMPLEMENTED` before the ready frame, so the gateway falls back to a dedicated RPC and the token is validated per call as before. |
| `otelgrpc` stats handler      | Per RPC                               | Produces one transport span and `rpc.*` measurements per stream. `Serve` reconstructs a per-operation server span from the request's propagation fields so SQL traces stay per statement.                                 |

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

| Instrument                           | Attributes                                                                          | Meaning                                                                                                                       |
| ------------------------------------ | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `mg.gateway.query_stream.operations` | `transport=new\|reused\|legacy_unsupported\|legacy_metadata\|legacy_propagation`    | Operations by transport; `new` counts submitted operations on new streams, `legacy_*` counts dedicated-RPC fallbacks and why. |
| `mg.gateway.query_stream.discards`   | `reason=cancelled\|incomplete\|transport\|retired\|idle_full\|idle_expired\|closed` | Streams closed, by cause. Normal retirement appears here, not as a failure.                                                   |
| `mg.gateway.query_stream.active`     | none                                                                                | Streams currently leased to an operation.                                                                                     |
| `mg.gateway.query_stream.idle`       | none                                                                                | Streams retained idle for reuse.                                                                                              |
