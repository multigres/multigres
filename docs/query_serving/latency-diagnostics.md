# Bounded latency diagnostics

Enable `--latency-diagnostics` on the multigateway and relevant multipoolers
(or Minigres) to expose `/debug/latency` on the existing diagnostic HTTP port.
`--pprof-http` must also be enabled. The new flag defaults to false. The endpoint
uses the same authentication middleware as pprof; services without an HTTP auth
plugin still rely on a private listener/network. Do not expose this port publicly.
No project settings, database schema, query semantics or retry policy change.

The goal is to locate missing time with correlated evidence, not assert a
100%-certain inside/outside verdict from server telemetry alone. Delays before a
server goroutine runs and after its socket write returns still require matched
client/kernel evidence. More than one cause can contribute to the same delay.

## Design

The recorder supplements existing metrics and OpenTelemetry spans with short,
operator-requested captures of protocol boundaries. It uses a local bounded
buffer so a capture works even when no tracing exporter is configured, and it
can report dropped events and unfinished intervals directly. It does not add a
second exporter, persistent storage, or high-cardinality metric series.

The core `go/tools/latencydiag` package owns admission, sampling, correlation and
capture lifecycle. The `grpcdiag` subpackage adapts dedicated gRPC calls without
making the PostgreSQL protocol implementation depend on gRPC. Reused streams
carry identity in their existing per-operation baggage envelope; no protobuf or
transport-selection change is required. PostgreSQL hooks sit in the original
methods and preserve callback nil/error semantics and child contexts.

Inactive hooks allocate no diagnostic spans. Sampled spans do allocate contexts,
read clocks and synchronize admission/completion; full capture is therefore not
free. Runtime tracing and socket/host collection are separate, optional costs.
The event buffer is transferred to the download on stop, so idle connections
holding old contexts cannot retain it. Cleanup is safe on cancellation and early
handler exit.

## Capture a benchmark window

Start captures concurrently against the actual gateway and every pooler that may
serve the workload. Give them the same `id`, unique to that capture batch. Use direct pod/service diagnostic
endpoints or local port forwards; a load-balanced endpoint cannot guarantee which
process you capture. Pod hostname, PID, build revision, Go version, GOMAXPROCS and
capture clocks appear in each archive. Time alignment across hosts is approximate;
local durations use Go's monotonic clock.

For the low-overhead mode, collect sampled messages without runtime tracing:

```sh
curl --fail --max-time 45 \
  'http://127.0.0.1:18080/debug/latency?id=run42&seconds=10&sample_every=100' \
  -o gateway.zip &
curl --fail --max-time 45 \
  'http://127.0.0.1:18081/debug/latency?id=run42&seconds=10&sample_every=100' \
  -o pooler.zip &
wait
python3 scripts/summarize_latency.py gateway.zip pooler.zip > summary.json
```

Use your existing diagnostic HTTP authentication configuration where applicable.
The capture starts when the request reaches that process. Warm the workload before
collecting; a read already blocked at capture start is not backdated. Start the
pooler capture slightly earlier and stop it later if you need fewer unmatched RPCs
at window edges. A matching capture ID is required to import remote correlation.

For a separate, short diagnostic experiment, use
`seconds=5&sample_every=1&trace=1&sockets=1`. This records every message admitted
during the window, Go runtime traces and periodic TCP snapshots. It costs more;
do not use its throughput as an uninstrumented benchmark result. Runtime tracing
is process-wide even when message sampling is enabled. Existing pprof tracing and
this trace mode cannot run simultaneously. Sampling changes neither connection
selection nor the reusable/dedicated RPC transport choice.

Parameters and limits:

| Parameter | Default | Limit |
| --- | --- | --- |
| `id` | Generated | 1–64 letters, digits, `_` or `-` |
| `seconds` | 10 | 1–60 |
| `sample_every` | 100 | 1–1,000,000; hashed message sampling |
| `max_events` | 100,000 | 1–200,000 completed intervals |
| `trace` | 0 | 0 or 1; trace data capped at 32 MiB |
| `sockets` | 0 | 0 or 1; at most 256 frontend/backend sockets, sampled once/sec |

One capture/download per process is allowed. Capture stops automatically or on
request cancellation. No synchronous file/network export occurs on the query
path. Active sampled spans use a bounded in-memory event buffer; overflow counts
are retained. Downloads have a bounded write deadline. Completed event storage is
released from the collector on stop, including when a connection remains idle.
SQL strings, bind values, passwords, cancellation keys and error messages are not
recorded. PostgreSQL error flags/SQLSTATEs, protocol kinds, correlation IDs and
socket addresses are recorded. There are no per-connection Prometheus labels.

## Evidence in the archive

- `events.json`: instance-qualified spans with parent/root IDs, connection ID and
  message sequence encoded in each root, protocol cycle number, protocol byte and
  transaction status before/after the message. The connection ID is the frontend
  BackendKeyData PID (also available through `pg_backend_pid()`); the secret
  cancellation key is never included. Keep the driver's worker-to-PID mapping.
- `manifest.json`: host/runtime identity, periodic GC/scheduler/CPU statistics,
  sampling/loss/window coverage and optional socket snapshots. Linux snapshots
  include TCP RTT, retransmits, unacked segments and unsent bytes, plus available
  cgroup CPU/pressure data. Missing files or unsupported OS/transport are explicit.
  These are snapshots, not exact packet-arrival or host scheduler timestamps.
- `runtime.trace` when requested: use `go tool trace runtime.trace`, or
  `go tool trace -pprof=sched runtime.trace > sched.pprof`. Tasks/regions carry
  diagnostic root/span IDs, enabling correlation with runnable delays, blocking,
  network waits and GC. A truncated trace is not usable evidence.

The recorded boundaries include:

| Phase | Meaning |
| --- | --- |
| `frontend.read_type` | Wait for the next message type, including idle/think time and scheduling |
| `frontend.read_length`, `frontend.read_body` | Buffered/header/body receive work |
| `frontend.dispatch` | Parse/Bind/Describe/Execute/Close/Sync/simple-query dispatch, decoding and downstream work |
| `frontend.encode_result` | Result callback encoding/buffering, including any nested flush |
| `frontend.flush*`, `frontend.transport_write` | Explicit flush/lock and actual transport Write calls, including implicit buffer-overflow writes |
| `frontend.transport_read` | Reads below bufio and above TLS, when present |
| `rpc.client*`, `rpc.server*` | Dedicated RPCs, with remote parent linkage |
| `rpc.operation.client/server` | Individual operations on reused streams; per-operation W3C baggage prevents identity leakage between leases |
| `rpc.*send/receive` | Transport-facing send/receive calls; API return is not proof of peer delivery |
| `pool.acquire` | Connection-pool acquisition, including its waits |
| `postgres.*`, `postgres.response` | PostgreSQL operation and response processing; includes PostgreSQL, transport and decoding time |
| `postgres.result_callback` | Result forwarding/backpressure within backend response processing |
| `transaction.*` | Retrospective transaction duration at conclusion, including inter-statement gaps |
| `frontend.error_response` | Emitted SQLSTATE and error flag, without error text |

Nested and concurrent intervals overlap. Never sum phase durations or subtract
independent p99 values. Transaction intervals can begin before both their parent
message and the capture; they are retrospective annotations, not nested query
execution. Retry/reprepare/reconnect work appears as repeated operation spans
(including `postgres.Reconnect`), rather than being collapsed into one query.
Logical replication tunnels and startup/authentication are outside this capture.
COPY has outer message/RPC timings but does not have a complete per-chunk backend
breakdown. Notification writers may overlap the serving goroutine's phase.

A client RPC duration minus its matched pooler operation duration includes
Multigres transport and scheduling, network time, buffering and decoding; it does
**not** mean that the difference occurred outside Multigres. A slow frontend read
alone likewise cannot distinguish a slow driver from a server goroutine that was
not scheduled. Kernel receive/send timestamps and matched driver request timing
are necessary to resolve those cases. The summary deliberately reports
`inconclusive_without_matched_client_and_transport_evidence` instead of claiming
certainty. Inspect dropped/unfinished counts, missing peers, window overlap,
trace truncation, propagation drops and sample coverage before drawing conclusions.
Client operation attempts can also lack a peer when acquisition fails or the
transport falls back before submitting an operation; an unmatched span is not
proof of packet loss.
Failed-span counts are not transaction-error counts: nested spans can describe the
same failure, and disconnect/EOF reads can also be marked failed. Use emitted
SQLSTATEs and root IDs to identify distinct query failures.

## Local validation and overhead

Follow `.claude/skills/mt-dev/SKILL.md`. Relevant commands:

```sh
go test -short -race ./go/tools/latencydiag/... ./go/common/pgprotocol/... \
  ./go/common/queryrpc ./go/common/servenv ./go/tools/grpccommon \
  ./go/services/multigateway/poolergateway \
  ./go/services/multigateway/engine ./go/services/multipooler/internal/executor
make build
scripts/portpool.sh start
MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock \
  go test ./go/test/endtoend/minigressetup \
  -run '^TestMinigres_LatencyDiagnostics$' -count=1 -timeout=5m -v
go test -short -run '^$' -bench '^BenchmarkDiagnosticRoundTrip' \
  -benchtime=200ms -count=7 ./go/common/pgprotocol/server
go test -short -run '^$' -bench '^BenchmarkProbe' \
  -benchtime=200ms -count=7 ./go/tools/latencydiag
```

The end-to-end test exercises simple/prepared queries, transactions, error paths,
reused RPC streams and a real local PostgreSQL process. It requires all expected
layers and correlation, checks event loss, and verifies query/parameter sentinels
are absent from every archive entry. The low-level tests cover concurrency,
cancellation, bounded storage, endpoint opt-in/auth, propagation and late spans.
Injected delays exercise actual frontend I/O, PostgreSQL response handling,
result callbacks and the executor's pool-acquisition boundary. Tests assert the
blocked interval is included in the expected span and preserve parent linkage;
they do not infer a universal inside/outside verdict from elapsed time alone.

Additional checks:

```sh
go test -short -p=4 ./go/...
go test -short -race -run 'TestDiagnostic|TestGRPC|TestIncoming|TestStop' \
  -count=10 ./go/tools/latencydiag/... ./go/common/pgprotocol/client \
  ./go/common/pgprotocol/server ./go/services/multipooler/internal/executor
python3 -B -m unittest discover -s scripts -p test_summarize_latency.py
go test -short -run '^$' -bench '^BenchmarkDiagnosticConcurrentRoundTrip$' \
  -benchtime=10000x -count=3 ./go/common/pgprotocol/server
```

The concurrent benchmark covers 1, 8 and 32 connections and fails if its capture
buffer overflows. Its `ns/op` is aggregate wall time divided by completed
operations, not individual request latency. Run it on the same platform and with
the same concurrency/CPU settings in both revisions.

Measure disabled versus the same loopback benchmark copied into the unmodified
base revision, then compare disabled/sampled/full modes on the changed revision.
Run serially on an otherwise idle host, repeat and interleave builds to expose
noise. These synthetic local measurements are not a substitute for repeated
TPC-C controls on the target Linux deployment. Keep full tracing out of official
performance comparison windows until its overhead there has been quantified.
