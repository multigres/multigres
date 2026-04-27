# pgwire write path: architecture and rationale

This doc describes how multigres encodes outbound PostgreSQL wire protocol
packets on the client-facing side (`go/common/pgprotocol/server/`). The shape
chosen here is opinionated — it diverges from how Vitess does the equivalent
work for MySQL — and the reasoning is non-obvious enough that it's worth
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

## What about reads?

Reads (parsing incoming pgwire messages) are out of scope for this doc;
this covers the write side only. The read side currently uses a
listener-pooled `bufio.Reader` that's allocated in `newConn` and held
for the connection's lifetime. That's different from the write side
(which only holds the bufio.Writer during a query) and is a known
asymmetry — at high client-connection counts it's worth tightening,
but it's a separate change.

## Code map

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
  (`sync.Pool` of `*bufio.Writer`, 16 KB each).
- `go/common/pgprotocol/server/packet_bench_test.go` — local
  microbenchmarks of the write path. Use to verify any future changes
  don't regress encoder cost.

## Pitfalls when modifying this code

- **Do not call `startPacket` without a matching `writePacket`.** The
  lock will be held forever. Treat the pair like `Lock` / `Unlock`.
- **Do not hold a slice returned by `startPacket` past the matching
  `writePacket`.** On the fast path the slice aliases bufio's internal
  storage, which gets reused on the next message. On the slow path the
  backing array goes back to `listener.bufPool` and can be handed to
  another connection on the very next `Get`. Either way, retaining it
  is a bug.
- **`writeXxxAt` helpers are unsafe by design.** They write at
  `buf[pos:]` without bounds-checking the body length. Correctness
  depends on the body-size pre-pass in the writer. If you add a new
  field to a packet body, update the body-length computation at the
  top of the writer accordingly.
- **The lock spans encoding.** If you add a writer that does I/O or
  blocking work mid-encoding (e.g. waiting on a channel), you'll
  serialize the whole connection on it. Pre-compute everything before
  `startPacket`.
- **Adding a new outbound message type:** follow the existing pattern
  — pre-compute body size, `startPacket`, in-place encode,
  `writePacket`. Don't reach for `writeMessage` (the cold path used
  for body-less messages like `ParseComplete`); it doesn't get the
  same fast-path treatment.
