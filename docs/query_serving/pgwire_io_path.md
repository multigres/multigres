# pgwire I/O path: design and rationale

How multigres encodes and decodes PostgreSQL wire protocol packets. Two
parallel implementations:

- `go/common/pgprotocol/server/` — multigateway ↔ clients.
- `go/common/pgprotocol/client/` — multipooler ↔ postgres.

Both sides share the same in-place encoding pattern with small,
deliberate differences forced by their threading models. This doc
records the design choices that aren't obvious from reading the code.

## Goals

The path runs on every query. For the workloads we care about
(pgbench-style, small-OLTP) it's hot enough that per-message
inefficiencies show up as percent-of-CPU in profiles. In priority order:

1. No per-packet allocations, no extra copies, minimal lock hold.
2. Bounded per-connection memory — a multigateway with tens of thousands
   of idle connections must not pin tens of thousands of buffers.
3. Coalesce many small protocol messages into one syscall.
4. Correct under contention with the async notification pusher.

## Write path

### The path of a byte

A typical pgbench TPC-B response is four messages: RowDescription,
DataRow, CommandComplete, ReadyForQuery.

1. Handler calls e.g. `c.writeDataRow(row)`.
2. The writer pre-computes `bodyLen` and calls
   `startPacket(msgType, bodyLen)`, which acquires `bufMu` (server only)
   and returns a `[]byte` slice aliasing the next `5 + bodyLen` bytes
   of `bufferedWriter`'s 16 KB internal buffer, with the type+length
   header already written.
3. The body is filled in place via `writeInt16At` / `writeStringAt` /
   etc. The bytes land in their final destination on first write.
4. `writePacket(buf, pos)` asserts `pos == len(buf)` (catches bodyLen
   miscalculations loudly), then calls `bufferedWriter.Write(buf)` — a
   self-copy that just advances bufio's cursor — and releases the lock
   and any pool buffer via `defer`.
5. Successive packets land byte-adjacent in the same 16 KB buffer.
   `flush()` issues a single `write()` syscall for all of them.
6. After the handler returns, `endWriterBuffering` returns the
   `*bufio.Writer` to a `sync.Pool`; the idle connection holds zero
   buffer memory.

### Why encode into `AvailableBuffer()`

`bufio.Writer.AvailableBuffer()` (Go 1.18+) returns `b.buf[b.n:][:0]`
— a zero-length slice over the bufio writer's unused capacity. We
take `avail[:totalLen]`, fill it, and call `Write(buf)`; internally
that's `copy(b.buf[b.n:], buf)` where src and dst alias, so the copy
is a no-op and only `b.n` advances. Bytes are written exactly once,
into the buffer they will eventually be flushed from.

The alternative — assemble into a separate buffer, then `Write` it
into bufio — pays a pool round-trip and a real memmove per packet.
For a 6-byte ReadyForQuery the round-trip dominates the message cost.

This is why our pattern diverges from Vitess's MySQL
`startEphemeralPacket` / `writeEphemeralPacket`: that pattern predates
`AvailableBuffer` and serves a protocol with hard 16 MB packet splits
that benefits from staging in a separate buffer. Neither constraint
applies here.

### Why `bufMu` spans the encoding window (server side)

`startPacket` acquires `bufMu`; `writePacket` releases it. Encoding
runs under the lock. The async notification pusher
(`EnableAsyncNotifications`) is the only writer that races with the
synchronous handler — without the lock spanning encoding, it could
take the lock between our space reservation and our body write,
advance `b.n`, and clobber our packet in flight. Lock hold is
~tens of nanoseconds; notifications are rare; contention is
effectively zero.

### Slow path: `bufpool`, not per-`Conn` scratch

`startPacket` falls through to a slow path when the packet wouldn't fit
in the bufio writer's available capacity, or no `bufferedWriter` is
attached yet (pre-startup). On the slow path it borrows from a
`bufpool.Pool` (16 KB → 64 MB power-of-two buckets); `writePacket`
returns the buffer after the underlying `Write`.

We do **not** pin a per-`Conn` scratch buffer. With 10k connections, a
1 MB scratch each would be 10 GB of pinned heap to handle one-time
large packets a connection might never see again. The listener-level
pool amortizes across connections; `sync.Pool`'s GC integration evicts
unused buckets during quiet periods.

Server side uses `listener.bufPool`; client side uses a package-level
`bufPool` (clients have no listener context). Same sizing.

### Per-connection memory

| state                     | pinned buffer memory                 |
| ------------------------- | ------------------------------------ |
| connected, before query   | 0                                    |
| executing a query         | 16 KB (bufio, checked out from pool) |
| idle between queries      | 0                                    |
| oversize packet in flight | 16 KB + one bufpool bucket           |

At 10k connections with 200 in flight, total bufio memory ≈ 3.2 MB.
The other 9,800 idle connections cost nothing.

### Client-side differences

Two intentional asymmetries with the server:

- **`startPacket` does not lock.** Each client `*Conn` is checked out
  to one goroutine at a time, and the high-level operation (`Query`,
  `BindAndExecute`, …) holds `bufmu` across the entire request. Adding
  a lock acquisition inside `startPacket` would deadlock on the second
  call within one operation.
- **`writePacket` does not flush.** Extended-protocol operations
  pipeline several messages (`Parse + Bind + Execute + Sync`) and flush
  exactly once at the end. A flush per `writePacket` would defeat the
  coalescing.

## Read path

Same `bufio.Reader` fronting the socket, same listener `bufpool` on
the server. Three fixes mirror the write side:

1. **`Peek` + `Discard` instead of `io.ReadFull`** for the type byte
   and 4-byte length. `io.ReadFull` takes an `io.Reader` interface, so
   the compiler can't prove the slice doesn't escape — every call
   heap-allocates the slice header. `bufio.Reader`'s concrete-typed
   methods devirtualize and don't escape. Drops 2 allocs/op.
2. **Server-side body buffers come from `listener.bufPool`**, with the
   pool's `*[]byte` stashed in `c.inboundPoolBuf`. The previous shape
   (`returnReadBuffer(buf []byte)` taking `&buf` of a parameter) forced
   the slice header onto the heap on every call. Storing the pointer
   on `Conn` and reading it back via parameterless
   `returnReadBuffer()` avoids that.
3. **`MessageReader` returned by value, not pointer.** The 32-byte
   struct lives on the caller's stack; methods take pointer receivers
   that auto-address it. Helpers walking the same reader as the caller
   take `*MessageReader` (a by-value parameter would copy and silently
   lose cursor mutations).

### Why the client does NOT pool body buffers

`parseDataRow` returns `sqltypes.Value` slices that alias _into_ the
body buffer, and they accumulate across multiple `readMessage` calls
before the user callback fires. Pooling would require copying every
column value out before recycling — strictly more allocations than the
current `make` per body. Server-side Bind params are safe because
`ParamsToProto` copies before `defer returnReadBuffer()` fires; the
asymmetry is intentional and called out in `client/packet.go`'s
`readMessageBody` comment.

After the fixes:

- Server read path: 0 allocs/op except for parses that copy strings out
  of the body (unavoidable).
- Client read path: 1 alloc/op (the body), which is the design.

## Files

### Server

- `server/packet.go` — `startPacket`, `writePacket`, in-place encoders
  (`writeByteAt`, `writeInt16At`, `writeInt32At`, `writeStringAt`,
  `writeBytesAt`), thin `writeMessage(msgType, body)` wrapper for the
  body-already-materialized callers (`ParseComplete`, `BindComplete`,
  `NoData`, `CloseComplete`), `writeRawByte` for the SSL/GSSENC
  negotiation byte (which is not a pgwire packet), `MessageReader`,
  read helpers.
- `server/query.go` — concrete writers (`writeRowDescription`,
  `writeDataRow`, `writeCommandComplete`, `writeReadyForQuery`,
  `writeEmptyQueryResponse`, `writeParameterDescription`,
  `writeErrorOrNotice`, `WriteCopyInResponse`).
- `server/conn.go` — `Conn` struct, `bufMu`, `bufferedWriter`
  lifecycle (`startWriterBuffering` / `endWriterBuffering`), async
  notification pusher.
- `server/listener.go` — pooled `*bufio.Writer`s and `bufPool`.

### Client

- `client/packet.go` — package-level `bufPool`, `startPacket` (no
  lock), `writePacket` (no flush), in-place encoders (adds
  `writeUint32At`, `writeByteStringAt` for length-prefixed Bind
  params), `MessageReader`, read helpers.
- `client/query.go` — `writeQueryMessage` (simple-protocol Q,
  self-flushing).
- `client/extended.go` — `writeParse`, `writeBind`, `writeExecute`,
  `writeDescribe`, `writeClose`, `writeSync`, `writeFlush`. None
  flush; the high-level caller flushes once after the pipelined
  sequence.
- `client/startup.go`, `client/scram.go` — startup and SCRAM
  messages. The startup packet has no message-type byte so it encodes
  directly against `AvailableBuffer` rather than `startPacket`.
- `client/conn.go` — `Conn` struct, `bufmu`, `bufferedWriter`
  allocated in `resetConn` (held for connection lifetime),
  `outboundPoolBuf` field, `WriteCopyData` / `Done` / `Fail`.

Microbenchmarks live in `*_bench_test.go` and `*_read_bench_test.go`
on both sides — run them before and after touching this code.

## Pitfalls

- **`startPacket` and `writePacket` are paired like `Lock`/`Unlock`.**
  Skipping `writePacket` leaks the server's `bufMu` and any borrowed
  pool buffer. Cleanup runs in a `defer` inside `writePacket`, so
  panics during encoding are safe — but only if `writePacket` is
  reached.
- **Don't retain the slice from `startPacket` past `writePacket`.** On
  the fast path it aliases bufio's internal storage (reused on the
  next message); on the slow path it goes back to the bufpool.
- **`writeXxxAt` helpers are unbounded by design.** They write at
  `buf[pos:]` with no length check — speed depends on the bodyLen
  pre-pass being correct. `writePacket` panics if `pos != len(buf)`,
  which catches the mismatch in tests rather than on the wire.
- **Server `bufMu` spans encoding.** Don't do I/O or block in a body
  encoder; you'll serialize the connection. Pre-compute everything
  before `startPacket`.
- **Client `bufmu` is held by the caller, not by `startPacket`.** New
  client write helpers must be called from a context that already
  holds `bufmu`.
- **`writeRawByte` (server) is the SSL/GSSENC escape hatch only.** A
  1-byte pgwire packet is still a packet — use `startPacket(t, 0)`.
