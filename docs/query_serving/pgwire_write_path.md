# pgwire write path: architecture and rationale

This doc describes how multigres encodes outbound PostgreSQL wire protocol
packets. There are two write paths in the codebase:

- `go/common/pgprotocol/server/` — multigateway emits responses to clients.
- `go/common/pgprotocol/client/` — multipooler sends requests to postgres.

Both sides use the same in-place encoding pattern, with a couple of small
differences forced by the threading model on each side. The shape chosen
here is opinionated — it diverges from how Vitess does the equivalent work
for MySQL — and the reasoning is non-obvious enough that it's worth
spelling out so the next person doesn't have to rederive it.

If you only read one section, read [The path of a byte](#the-path-of-a-byte).

## Goals

The pgwire write path runs on every query response. For the workload we care
about (high-fanout pgbench-style and small-OLTP traffic), it's hot enough that
small per-message inefficiencies show up as percent-of-CPU in profiles. The
goals, in priority order, are:

1. **Minimize per-packet overhead.** No per-message allocations, no extra
   memory copies, no locks held longer than they need to be.
2. **Bounded per-connection memory.** A multigateway with tens of thousands of
   idle client connections must not pin tens of thousands of buffers. Idle
   connections cost approximately zero bytes of buffer memory.
3. **Match the kernel's write granularity, not the protocol's.** PostgreSQL
   wire messages are mostly tens of bytes. We want to coalesce them into
   syscalls of kilobytes, not the other way around.
4. **No surprises under contention.** The async notification pusher writes
   from a separate goroutine; correctness must not depend on timing.

## The path of a byte

A typical pgbench TPC-B SELECT response is four pgwire messages:
RowDescription (`T`), DataRow (`D`), CommandComplete (`C`), ReadyForQuery
(`Z`). Total payload across the four is around 50 bytes. Here is what happens
to each byte from when our handler decides to send it to when it leaves the
machine:

1. The handler calls `c.writeDataRow(row)` (or one of its peers).
2. The writer pre-computes the exact body length, then calls
   `c.startPacket(msgType, bodyLen)`.
3. `startPacket` acquires `c.bufMu` and reserves space inside the connection's
   `bufferedWriter` (a `*bufio.Writer` with a 16 KB internal buffer). It
   writes the 5-byte type+length header in place and returns a `[]byte`
   slice that aliases the next `5 + bodyLen` bytes of the bufio writer's
   own internal storage.
4. The writer body uses the in-place encoders (`writeInt16At`,
   `writeInt32At`, `writeStringAt`, etc.) to fill in the body bytes
   directly into that slice. The bytes land in their final in-memory
   destination on first write — there is no intermediate buffer.
5. `c.writePacket(buf)` calls `bufferedWriter.Write(buf)`, which on the
   fast path is a self-copy onto the same memory region (effectively
   just advances bufio's internal write cursor by `len(buf)`). The lock
   is released.
6. Successive packets in the same response (RowDescription, then DataRow,
   then CommandComplete, then ReadyForQuery) all land in the same 16 KB
   bufio buffer, byte-adjacent.
7. When the handler is done, `handleQuery` calls `c.flush()`, which calls
   `bufferedWriter.Flush()`, which issues a single `write()` syscall
   carrying all four messages at once.
8. After the handler returns, `endWriterBuffering` returns the
   `*bufio.Writer` to the listener-level `sync.Pool`. The connection is
   now idle and holds **zero** buffer memory until the next query.

The TL;DR: bytes are written **once**, into the buffer they will be flushed
from. Multiple messages are coalesced into one syscall by bufio's normal
buffering. Buffers are pooled at the listener level and only checked out
during a query.

## The two non-obvious decisions

### Decision 1: Encode directly into `bufferedWriter.AvailableBuffer()`

`bufio.Writer.AvailableBuffer()` (added in Go 1.18) returns
`b.buf[b.n:][:0]` — a zero-length slice whose capacity is the unused space
in the bufio writer's internal byte array. The slice header points at the
next byte bufio is going to fill.

We use it like this:

```go
avail := c.bufferedWriter.AvailableBuffer()
if cap(avail) >= totalLen {
    buf := avail[:totalLen]            // slice into bufio's storage
    buf[0] = msgType
    binary.BigEndian.PutUint32(buf[1:5], uint32(4+bodyLen))
    // ... caller fills buf[5:totalLen] in place ...
    c.bufferedWriter.Write(buf)        // advance bufio's cursor
}
```

When `Write(buf)` runs, internally it does
`copy(b.buf[b.n:], buf); b.n += n`. But `buf` IS `b.buf[b.n:b.n+totalLen]`
— src and dst alias the same memory, so the `copy` is a self-copy
(effectively a no-op) and the only side effect that matters is `b.n +=
totalLen`. We never copy the bytes through any intermediate buffer.

Why this matters: the obvious-looking alternative is to assemble the
packet in a separate buffer (e.g. one from a `sync.Pool`) and then call
`bufferedWriter.Write(buf)` to copy it in. That works, but it pays:

- A pool round-trip per packet (`Get` + `Put` are atomic ops).
- A real memmove from the pool buffer into bufio's buffer.
- Likely a small heap allocation churn if buffer sizes don't match
  pool buckets.

For tiny messages (a 6-byte ReadyForQuery) the pool round-trip alone
dominates the cost of the message. By encoding directly into bufio's
storage, we eliminate it entirely.

This is the largest single win in the current design and it's what
distinguishes our pattern from Vitess's MySQL-side
`startEphemeralPacket`/`writeEphemeralPacket`. Vitess's pattern was
written before Go 1.18 (no `AvailableBuffer`) and for a protocol with a
hard 16 MB packet split rule that benefits from assembling in a separate
buffer first. Neither constraint applies to us.

### Decision 2: Hold `bufMu` across the encoding window

The lock is acquired in `startPacket` and released in `writePacket`. The
encoding loop runs under the lock. That looks heavier than the obvious
alternative ("only lock the `Write` call at the end"), and it is —
slightly. It buys correctness against the async notification pusher.

The notification pusher is a goroutine started by
`EnableAsyncNotifications`. When postgres pushes a `NOTIFY` to us, we
synthesize a `NotificationResponse` (`A`) packet and write it on the same
connection. It is the only writer that can race with the synchronous
query handler.

If `startPacket` reserved space in `AvailableBuffer()` without holding
the lock, this could happen:

```text
T0: synchronous handler: startPacket reserves bytes [b.n .. b.n+30].
T1: notification pusher: takes the lock, writes a 50-byte A packet,
                          advances b.n by 50.
T2: synchronous handler: encodes its body into [oldB.n .. oldB.n+30],
                          but b.n is now elsewhere — we've overwritten
                          notification bytes mid-flight.
T3: synchronous handler: Write(buf) advances b.n further; the
                          notification packet has corrupted body.
```

Holding `bufMu` across the whole `startPacket → encode → writePacket`
window closes that window. The notification pusher waits its turn.

This is cheap because:

- Encoding takes tens of nanoseconds. The lock hold is negligible.
- Notifications are rare. Contention in practice is essentially zero.
- We were already taking the lock once per packet in the old design;
  now it's the same one Lock+Unlock pair, just spanning a slightly
  larger region.

### Decision 3: Slow path borrows from `listener.bufPool`, no per-connection scratch

`startPacket` falls through to a slow path when either:

- No `bufferedWriter` is set (pre-startup phase, before the first
  query), or
- The packet wouldn't fit in the bufio writer's currently-available
  capacity (a wide-row DataRow >16 KB, a large error detail, etc.).

On the slow path we borrow a buffer from the listener-level
`bufPool` — the same pool the read path uses to stage inbound message
bodies. `writePacket` returns the buffer to the pool after the
`bufferedWriter.Write` (or direct `conn.Write`) completes.

We do **not** keep a per-`Conn` scratch buffer that grows monotonically.
A multigateway runs with tens of thousands of client connections;
anything pinned per-connection gets multiplied by that fanout. A 1 MB
scratch buffer per connection would be 10 GB of pinned heap at 10k
connections to handle a one-time large packet a connection might never
see again. The listener-level pool sidesteps that — buffers amortize
across all connections, and `sync.Pool`'s GC integration evicts unused
ones during quiet periods.

Why use the bufpool at all instead of a one-shot `make`? Because the
read and write sides see the same byte stream — multipooler reads
postgres's responses, multigateway writes the same responses to the
client. Any workload that hits the read path with large messages
(wide rows, BYTEA, COPY OUT) hits the write path's slow path at the
same rate. If those workloads are common, `make`-per-call would
allocator-thrash; the pool keeps the steady-state allocation count
near zero. If those workloads are rare, the extra `Get`/`Put` round-
trip through the pool costs ~tens of nanoseconds per occurrence, which
is invisible. The risk-reward is asymmetric: the pool is a small loss
in the rare-slow-path case and a large win in the frequent-slow-path
case, so we pay the small cost unconditionally.

The pool's bucket sizing (powers of 2 from 16 KB to 64 MB) does mean a
17 KB packet borrows a 32 KB bucket. That's a small per-buffer
overhead but the pool is shared across all connections, so it's
self-limiting; pgbouncer accepts a similar trade with their slab
allocator.

## Per-connection memory model

| connection state                   | pinned per-connection buffer memory                                             |
| ---------------------------------- | ------------------------------------------------------------------------------- |
| just connected, before first query | 0                                                                               |
| executing a query                  | 16 KB (bufio buffer checked out from listener pool)                             |
| between queries (idle)             | 0 (bufio returned to pool)                                                      |
| oversize packet in flight          | 16 KB + listener-pool buffer (returned to listener.bufPool after `writePacket`) |

The 16 KB is the only steady-state buffer cost, and it's only pinned
during query execution. At 10k client connections with 200 in-flight
queries at any moment, total bufio memory ≈ 200 × 16 KB = 3.2 MB. The
remaining 9,800 idle connections cost nothing on the buffer side.

This is strictly better than pgbouncer's "4 KB × every-connection-that-
ever-sent-data" model, and matches the philosophy behind their
`sbuf_try_resync(release=true)` path that frees the IOBuf when the
connection goes idle.

## What `bufio.Writer` is doing for us, and why we still need it

It would be tempting, given that we encode in place, to write straight
to the `net.Conn` and skip bufio entirely. We don't, for two reasons.

First, bufio is what coalesces multiple pgwire messages into one
syscall. A pgbench query response is four small messages totaling ~50
bytes. Without bufio, that's four `write()` syscalls. With bufio, all
four land in the 16 KB buffer and a single `flush()` at the end issues
one syscall.

Second, `AvailableBuffer()` is the storage we encode into. If we didn't
have a bufio writer, we'd need our own analogue — a per-connection
byte slice that we write into and periodically flush. That's just a
worse-built bufio.Writer.

So bufio earns its keep on three different axes simultaneously:

- It's the syscall-coalescing buffer.
- It's the encoding target (via `AvailableBuffer`).
- It's the thing we pool at the listener level so idle connections
  don't pin memory.

## Client side (multipooler → postgres)

The client-side write path in `go/common/pgprotocol/client/` uses the
same in-place encoding pattern with two intentional differences,
forced by the threading model on that side.

**Difference 1: `startPacket` does NOT acquire a mutex.** On the
server side, `bufMu` spans the encoding window so the async
notification pusher can't interleave bytes mid-packet. The client
side has no async pusher analog — each `*Conn` is checked out to one
goroutine at a time by the pooler, and the high-level operation
(`Query`, `BindAndExecute`, `DescribePrepared`, …) already holds
`bufmu` across the entire request/response cycle before calling any
of the write helpers. Adding a lock acquisition inside `startPacket`
would deadlock on the second call within a single operation. So the
client-side `startPacket`/`writePacket` rely on the caller's lock and
add none of their own.

**Difference 2: `writePacket` does NOT flush.** Extended-protocol
operations pipeline multiple messages — `Parse + Bind + Execute +
Sync`, or `Bind + Describe + Sync` — and flush exactly once at the
end. Pushing a flush inside every `writePacket` would issue a syscall
per message and defeat the coalescing. The exception is
`writeQueryMessage` (simple-protocol Query is a single-shot send), which
calls `flush()` itself after `writePacket`.

**Buffer pool location.** The client side has no per-listener context
— each `*Conn` is created standalone via `Connect()`. The slow-path
buffer pool therefore lives at package scope
(`var bufPool = bufpool.New(16*1024, 64*1024*1024)` in `packet.go`)
and is shared across all client connections in the process. Same
sizing as the server side. This works well because the cardinality
on the client side is low (each multipooler holds at most a few
hundred connections to postgres) — pool contention is negligible.

**Cardinality difference matters less than you'd think.** It's
tempting to argue "since multipooler has only ~100 connections to
postgres, we could afford richer per-connection state on this side."
We don't, for two reasons. First, keeping both sides identical means
the same diagnostic mental model applies, the same benchmark file
template fits, and a future reader doesn't need to rederive why one
side differs from the other. Second, the pgbouncer comparison
([Decision 3](#decision-3-slow-path-borrows-from-listenerbufpool-no-per-connection-scratch))
applies just as well here: pgbouncer treats client-facing and server-
facing connections identically because the per-connection cost is
already small enough that asymmetric optimization isn't worth the
complexity. We're following the same logic.

**One encoding API, used everywhere — on both sides.** Every writer
in the package — server and client, hot path and cold — goes through
`startPacket` / `writePacket` and the `writeXxxAt` encoders.

On the **client** side that means `writeQueryMessage`, `writeParse`,
`writeBind`, `writeExecute`, `writeDescribe`, `writeClose`,
`writeSync`, `writeFlush`, `WriteCopyData` / `Done` / `Fail`,
`writeTerminate`, and the SCRAM exchange.

On the **server** side that means `writeRowDescription`,
`writeDataRow`, `writeCommandComplete`, `writeReadyForQuery`,
`writeEmptyQueryResponse`, `writeParameterDescription`,
`writeErrorOrNotice`, `WriteCopyInResponse`, the auth-flow writers
(`sendAuthenticationSASL` / `Continue` / `Final` / `Ok`,
`sendBackendKeyData`, `sendParameterStatus`), and the async
notification pusher (`writeNotificationResponseMsg`). The thin
`writeMessage(msgType, body)` helper exists only for the
already-have-a-body-as-`[]byte` callers (body-less control messages
like `ParseComplete` pass nil) and routes through the same primitives.

`MessageWriter` (the append-style body builder used by older code) is
gone from production on both sides. It's now in a `_test.go` file —
tests still use it to construct synthetic byte fixtures for parser
tests, but it's never compiled into the production binary. The older
`writeMessage` / `writeMessageNoFlush` / `writeByte` / `writeUint32`
methods on `*Conn` (client side) are also deleted.

Two narrow exceptions on the server side, both intentional:

- The startup-flow `sendStartupMessage` and `writeSSLRequest` (client
  side) are not pgwire packets — they have no message-type byte, just
  a length + body. They use `AvailableBuffer` + bufpool inline but
  skip `startPacket` (which would write the 5-byte type+length
  header).
- `writeRawByte` (server side) sends the single SSL/GSSENC
  negotiation response byte ('S' / 'N'), which has no length prefix
  at all.

`InitiateCopyFromStdin` previously hand-rolled a Q message; it now
just calls `writeQueryMessage`.

## Reads

Reads share a lot of structure with writes — same `bufio.Reader`
fronting the socket, same listener-level `bufpool` on the server
side. The two key fixes mirror what the write side did.

**Type and length use Peek + Discard, not `io.ReadFull`.** The
naive shape — `var hdr [4]byte; io.ReadFull(c.bufferedReader, hdr[:])`
— forces the stack-local slice header to the heap on every call,
because `io.ReadFull` takes an `io.Reader` interface and the compiler
can't prove the implementation doesn't retain the slice. Switching
to `c.bufferedReader.Peek(4)` reads the bytes directly from bufio's
internal buffer (no intermediate slice), and `Discard(4)` advances
the cursor. `bufio.Reader` is a concrete type, so both calls
devirtualize and nothing escapes. The single-byte case
(`ReadMessageType`) similarly uses `bufferedReader.ReadByte()`. Per
read this drops two heap allocations.

**Server-side body buffers come from `listener.bufPool`, with the
pool pointer stashed on `Conn`.** Same escape-analysis trick as the
write-side `outboundPoolBuf`: `readMessageBody` stores the pool's
`*[]byte` in `c.inboundPoolBuf`, and the parameterless
`returnReadBuffer()` releases it from there. The previous shape
(`returnReadBuffer(buf []byte)` taking `&buf` of a parameter) forced
the slice header to the heap on every call. After the fix, the
server-side read path is **0 allocs/op** for everything except
parses that copy strings out of the body — which are unavoidable.

**MessageReader returns by value, not pointer.** `NewMessageReader`
used to return `*MessageReader`, which heap-allocated the 32-byte
struct on every parse. Now it returns `MessageReader` (value); the
struct lives on the caller's stack, methods take pointer receivers
that auto-address it. Helpers that previously took
`*MessageReader` parameters now take `MessageReader` by value.

**Why the client side does NOT pool body buffers.** It would seem
symmetric with the server side, but `parseDataRow` returns
`sqltypes.Value` slices that alias _into_ the body buffer — and
those values accumulate across multiple `readMessage` calls before
being handed up to the user callback. Pooling the body would
require copying every column value out before recycling, which is
strictly more allocations than the current single `make([]byte,
length)` per body. The server-side equivalent (Bind params) is safe
because `ParamsToProto` copies the param bytes into the portal
proto before `defer returnReadBuffer()` fires. The asymmetry is
intentional and documented in `readMessageBody`'s comment on the
client side.

The client-side read path therefore still costs **1 alloc per
message** (the body). All other read overhead — type, length,
MessageReader struct — is gone.

| benchmark                         | before                    | after              | speedup   |
| --------------------------------- | ------------------------- | ------------------ | --------- |
| (server) ReadMessage_Sync         | 14.7 ns / 32 B / 3 allocs | 1.2 ns / 0 / 0     | **12.5×** |
| (server) ReadMessage_Bind_4param  | 20.9 ns / 32 B / 3 allocs | 5.6 ns / 0 / 0     | **3.7×**  |
| (server) ReadMessage_Bind_16param | 22.5 ns / 32 B / 3 allocs | 6.9 ns / 0 / 0     | **3.3×**  |
| (server) ReadAndParseBind_4param  | 29.6 ns / 36 B / 4 allocs | 17.0 ns / 4 B / 1  | 1.7×      |
| (client) ReadMessage_DataRow_4col | 20.7 ns / 68 B / 2 allocs | 15.7 ns / 64 B / 1 | 1.3×      |
| (client) ReadAndParseDataRow_4col | 19.6 ns / 68 B / 2 allocs | 17.7 ns / 64 B / 1 | 1.1×      |

## Code map

### Server-side files

- `go/common/pgprotocol/server/packet.go`
  - `startPacket(msgType, bodyLen) ([]byte, int)` — reserves space,
    acquires `bufMu`, returns either a slice into `bufferedWriter`'s
    internal buffer (fast path) or a slice borrowed from
    `listener.bufPool` (slow path).
  - `writePacket(buf) error` — commits the packet and releases
    `bufMu`. On the fast path the `bufferedWriter.Write` is a
    self-copy; on the slow path it's a real Write through bufio plus
    a `bufPool.Put` to recycle the borrowed buffer.
  - `writeByteAt`, `writeInt16At`, `writeInt32At`, `writeStringAt`,
    `writeBytesAt` — in-place body encoders. Plain functions, no
    interfaces, the compiler inlines them and they don't escape
    arguments.
  - `writeMessage(msgType, body)` — thin convenience wrapper for the
    handful of callers that already have the body materialized as a
    `[]byte` (body-less control messages like `ParseComplete` /
    `BindComplete` / `NoData` / `CloseComplete` pass nil here).
    Routes through `startPacket`/`writePacket` underneath, so it
    gets the same single-Write fast path.
  - `writeRawByte(b)` — sends a single non-pgwire byte. Used only
    for the SSL/GSSENC negotiation response ('S'/'N'), which is a
    raw byte on the wire with no protocol framing.
- `go/common/pgprotocol/server/query.go` — concrete writers:
  `writeRowDescription`, `writeDataRow`, `writeCommandComplete`,
  `writeReadyForQuery`, `writeEmptyQueryResponse`,
  `writeParameterDescription`, `writeErrorOrNotice`,
  `WriteCopyInResponse`. Each pre-computes the body size, calls
  `startPacket`, encodes in place, calls `writePacket`.
- `go/common/pgprotocol/server/conn.go` — `Conn` struct,
  `bufferedWriter` lifecycle (`startWriterBuffering` /
  `endWriterBuffering`), `bufMu` field, async notification pusher.
- `go/common/pgprotocol/server/listener.go` — `writersPool`
  (`sync.Pool` of `*bufio.Writer`, 16 KB each), `bufPool` for slow-
  path packet bodies.
- `go/common/pgprotocol/server/packet_bench_test.go` — local
  microbenchmarks of the write path. Use to verify any future changes
  don't regress encoder cost.

### Client-side files

- `go/common/pgprotocol/client/packet.go`
  - Package-level `bufPool = bufpool.New(16*1024, 64*1024*1024)` —
    shared across all client connections in the process; replaces the
    per-listener pool the server side has.
  - `startPacket(msgType, bodyLen) ([]byte, int)` — same shape as
    the server side, but does **not** acquire any mutex (the high-
    level operation already holds `bufmu`).
  - `writePacket(buf) error` — same shape, but does **not** flush.
    Caller is responsible for calling `flush()` once after a sequence
    of pipelined writes.
  - `writeByteAt`, `writeInt16At`, `writeInt32At`, `writeUint32At`,
    `writeStringAt`, `writeBytesAt`, `writeByteStringAt` — in-place
    body encoders. (`writeByteStringAt` writes the length-prefixed
    `int32 + bytes` form used in Bind parameters; the server side
    doesn't need it.)
  - `writeTerminate` — uses startPacket/writePacket directly.
- `go/common/pgprotocol/client/query.go` — `writeQueryMessage`
  (simple-protocol `Q`). Self-flushing.
- `go/common/pgprotocol/client/extended.go` — extended-protocol
  writers: `writeParse`, `writeBind`, `writeExecute`, `writeDescribe`,
  `writeClose`, `writeSync`, `writeFlush`. None of these flush; the
  high-level caller flushes once after the pipelined sequence.
- `go/common/pgprotocol/client/startup.go` — `sendStartupMessage` and
  `writeSSLRequest` are encoded inline (no message-type byte) using
  `AvailableBuffer` directly with the bufpool as fallback.
- `go/common/pgprotocol/client/scram.go` — SCRAM handshake messages
  (`sendClientFirst`, `sendClientFinal`) use `startPacket`.
- `go/common/pgprotocol/client/conn.go` — `Conn` struct,
  `bufferedWriter` allocated in `resetConn` (held for connection
  lifetime; client cardinality is bounded so this is fine), `bufmu`
  held by the high-level operation, `outboundPoolBuf` field for
  slow-path bufpool tracking. Also `WriteCopyData`/`Done`/`Fail` use
  `startPacket`.
- `go/common/pgprotocol/client/test_helpers_test.go` — test-only
  `MessageWriter` type, used by parser tests to construct synthetic
  server-response byte fixtures. Not compiled into the production
  binary.
- `go/common/pgprotocol/client/packet_bench_test.go` — local
  microbenchmarks symmetric to the server-side suite.

## Pitfalls when modifying this code

- **Do not call `startPacket` without a matching `writePacket`.** On
  the server side, `bufMu` will be held forever. On the client side,
  any pool buffer borrowed for the slow path will leak. Treat the
  pair like `Lock` / `Unlock`.
- **Do not hold a slice returned by `startPacket` past the matching
  `writePacket`.** On the fast path the slice aliases bufio's internal
  storage, which gets reused on the next message. On the slow path the
  backing array goes back to the bufpool and can be handed to
  another packet on the very next `Get`. Either way, retaining it
  is a bug.
- **`writeXxxAt` helpers are unsafe by design.** They write at
  `buf[pos:]` without bounds-checking the body length. Correctness
  depends on the body-size pre-pass in the writer. If you add a new
  field to a packet body, update the body-length computation at the
  top of the writer accordingly.
- **The server-side lock spans encoding.** If you add a server-side
  writer that does I/O or blocking work mid-encoding (e.g. waiting on
  a channel), you'll serialize the whole connection on it. Pre-
  compute everything before `startPacket`.
- **The client-side lock is held by the caller, not by `startPacket`.**
  If you call a client-side write helper from somewhere that doesn't
  already hold `bufmu`, you'll race with concurrent operations on the
  same `*Conn`. All existing callers acquire `bufmu` at the top of
  their high-level operation; new callers should do the same.
- **Adding a new outbound message type:** follow the existing pattern
  — pre-compute body size, call `startPacket`, encode in place with
  the `writeXxxAt` helpers, call `writePacket`. The server side keeps
  a thin `writeMessage(msgType, body)` helper for the few cases where
  the body is already materialized as a `[]byte` (body-less control
  messages like `ParseComplete` / `BindComplete` / `NoData` /
  `CloseComplete` pass nil) — it routes through the same
  `startPacket`/`writePacket` underneath, so it gets the same
  fast-path treatment. New code that builds bodies field-by-field
  should use the encoders directly to skip the intermediate buffer.
  The client side has no such helper — `startPacket` / `writePacket`
  is the only API.

- **`writeRawByte` is the SSL/GSSENC escape hatch, not a general
  helper.** It exists only to send the single 'S'/'N' negotiation
  response, which has no length prefix and no message-type framing —
  it is not a pgwire packet. Don't use it for anything else; if you
  need a 1-byte packet, that's still a packet (`startPacket(t, 0)`).
