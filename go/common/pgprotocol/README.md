# pgprotocol - PostgreSQL Wire Protocol Implementation

This package provides a complete implementation of the PostgreSQL wire
protocol for Go, enabling PostgreSQL clients to connect to multigres and
execute queries.

## Overview

The `pgprotocol` package is designed as a standalone, reusable library that
handles all aspects of the PostgreSQL wire protocol, including connection
establishment, authentication, query execution, and result streaming.

## Package Structure

```text
pgprotocol/
├── protocol/          # Protocol constants and message types
├── bufpool/           # High-performance buffer pooling
├── server/            # Server-side protocol implementation
└── client/            # Client-side implementation
```

## Key Features

### 1. Protocol Constants (`protocol/`)

- Complete set of PostgreSQL message type constants
- Authentication method codes (Trust, MD5, SCRAM-SHA-256, etc.)
- Error/Notice field codes
- Protocol version validation

### 2. Buffer Pooling (`bufpool/`)

- **High Performance**: Buffer pooling with exponential bucket sizing
- **Memory Efficient**: Reuses buffers through `sync.Pool` to reduce GC pressure
- **Concurrent Safe**: Thread-safe allocation and deallocation
- **Flexible**: Supports buffers from 16KB to 64MB

**Example:**

```go
pool := bufpool.New(16*1024, 64*1024*1024) // 16KB min, 64MB max
buf := pool.Get(4096)                       // Get 4KB buffer
defer pool.Put(buf)                         // Return to pool
// Use buffer...
```

### 3. Server Implementation (`server/`)

#### Listener

Accepts and manages PostgreSQL client connections:

```go
listener, err := server.NewListener(server.ListenerConfig{
    Address: ":5432",
    Handler: myQueryHandler,
    Logger:  slog.Default(),
})
if err != nil {
    log.Fatal(err)
}
defer listener.Close()

// Start accepting connections
if err := listener.Serve(); err != nil {
    log.Fatal(err)
}
```

#### Connection

Manages individual client connections with:

- Buffered I/O with pooled readers/writers
- Protocol version negotiation
- Transaction state tracking
- Graceful connection lifecycle

### 4. Packet I/O (`server/packet.go`)

#### MessageReader

Helper for reading protocol messages:

```go
r := server.NewMessageReader(msgBody)
username, err := r.ReadString()
database, err := r.ReadString()
numParams, err := r.ReadInt16()
```

#### MessageWriter

Helper for building protocol messages:

```go
w := server.NewMessageWriter()
w.WriteString("postgres")
w.WriteInt32(12345)
w.WriteByte('I')
messageBody := w.Bytes()
```

## Performance Optimizations

1. **Buffer Pooling**: Reduces GC pressure by reusing buffers
2. **Bucket Strategy**: Exponential sizing (1KB → 2KB → 4KB → ...)
   minimizes wasted space
3. **Reader/Writer Pooling**: Reuses `bufio.Reader` and `bufio.Writer`
   instances
4. **Zero-Copy**: Direct writes to network buffers where possible
5. **Batched Writes**: Buffered writer with configurable flush delay

## Testing

All components are thoroughly tested with testify assertions:

```bash
# Run all tests
go test ./go/common/pgprotocol/...

# Run with coverage
go test -cover ./go/common/pgprotocol/...

# Run benchmarks
go test -bench=. ./go/common/pgprotocol/bufpool/
```

## Design Principles

### 1. Separation of Concerns

- **Protocol layer** handles wire format encoding/decoding
- **Handler interface** abstracts query execution
- **Buffer pooling** is independent and reusable

### 2. Testability

- Clean interfaces enable easy mocking
- Comprehensive unit tests with testify
- Table-driven tests for boundary conditions

### 3. PostgreSQL Compatibility

- Based on official PostgreSQL protocol documentation

## References

- [PostgreSQL Protocol Documentation](https://www.postgresql.org/docs/current/protocol.html)
