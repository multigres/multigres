# PostgreSQL Wire Protocol - Architecture Documentation

## Package Structure

```
go/pgprotocol/              # Parent package for all PG protocol code
├── bufpool/                # Buffer pooling utilities (shared between client/server)
│   ├── pool.go            # Bucket pool implementation (from Vitess pattern)
│   └── pool_test.go       # Unit tests
├── protocol/               # Protocol constants and message types (shared)
│   ├── constants.go       # Message type constants, auth constants
│   ├── messages.go        # Common message structures
│   └── protocol.go        # Protocol version definitions
├── server/                 # Server-side protocol implementation
│   ├── conn.go            # Connection handling with buffer management
│   ├── listener.go        # Listener for accepting connections
│   ├── handler.go         # Query handler interface
│   ├── messages.go        # Message parsing/encoding
│   ├── auth.go            # Authentication flows
│   └── *_test.go          # Unit tests
└── client/                 # Future: Client-side protocol (for testing, etc.)
```

## Design Decisions

### 1. Package Organization
- **Rationale**: Separate client and server implementations under pgprotocol parent
- **Benefits**:
  - Share common code (bufpool, constants, message types)
  - Allow future client implementation for testing or client proxy scenarios
  - Clean separation of concerns

### 2. Buffer Pooling Strategy
Inspired by Vitess `go/mysql/conn.go`:

- **Bucket Pool**: Variable-sized buffers using exponential sizing (16KB, 32KB, 64KB, etc.)
  - Reduces GC pressure for packet buffers
  - Efficient for various message sizes

- **Reader/Writer Pools**: Reuse `bufio.Reader` and `bufio.Writer` objects
  - Uses Go's `sync.Pool`
  - Minimizes allocation overhead
  - 16KB buffer size (connBufferSize)

- **Ephemeral Buffers**: Temporary buffers for single packet read/write
  - Allocated from bucket pool
  - Returned immediately after use

### 3. Connection State Management
Similar to postgres.c `PostgresMain()` loop:

```
1. Accept connection
2. Startup handshake (parse StartupMessage)
3. Authentication (trust, password, SCRAM)
4. Send initial ParameterStatus messages
5. Main command loop:
   - Read message type (1 byte)
   - Read message length (4 bytes)
   - Read message body
   - Dispatch to handler based on type
   - Send response
   - Send ReadyForQuery
6. Handle termination or error
7. Close connection
```

### 4. Handler Interface
Abstract query execution to decouple protocol from query routing:

```go
type Handler interface {
    // HandleQuery processes a simple query and returns results
    HandleQuery(ctx context.Context, query string) (*QueryResult, error)

    // HandleParse prepares a statement
    HandleParse(ctx context.Context, name, query string, paramTypes []int32) error

    // HandleBind binds parameters to a prepared statement
    HandleBind(ctx context.Context, portal, stmt string, params [][]byte) error

    // HandleExecute executes a bound portal
    HandleExecute(ctx context.Context, portal string, maxRows int32) (*QueryResult, error)

    // HandleClose closes a statement or portal
    HandleClose(ctx context.Context, typ byte, name string) error
}
```

**Benefits**:
- Multigateway can implement this interface to route to multipooler
- Easy to mock for testing
- Protocol layer doesn't need to know about gRPC or routing

### 5. Message Format
PostgreSQL wire protocol message structure:

```
Startup phase:
- No type byte
- 4-byte length (includes self)
- Message body

Regular messages:
- 1-byte type ('Q', 'P', 'B', 'E', etc.)
- 4-byte length (includes self, excludes type byte)
- Message body
```

Network byte order (big-endian) for all multi-byte integers.

### 6. Error Handling Strategy
- Parse errors → ErrorResponse message, close connection
- Query errors → ErrorResponse message, continue connection
- Extended query errors → ErrorResponse, skip until Sync message
- Fatal errors → close connection immediately

### 7. Transaction State Tracking
ReadyForQuery message includes transaction status:
- 'I' = Idle (no transaction)
- 'T' = In transaction block
- 'E' = In failed transaction block

Server must track transaction state for correct status reporting.

### 8. Integration with Multigateway

```
┌─────────────┐
│   psql      │
│  (client)   │
└──────┬──────┘
       │ PostgreSQL Wire Protocol
       ▼
┌─────────────────────────────────┐
│      Multigateway               │
│  ┌───────────────────────────┐  │
│  │ pgprotocol/server         │  │
│  │ - Listener on port 5432   │  │
│  │ - Protocol parsing        │  │
│  │ - Handler implementation  │  │
│  └───────────┬───────────────┘  │
│              │                   │
│  ┌───────────▼───────────────┐  │
│  │ PoolerDiscovery           │  │
│  │ - Find multipooler        │  │
│  └───────────┬───────────────┘  │
└──────────────┼───────────────────┘
               │ gRPC
               ▼
       ┌────────────────┐
       │  Multipooler   │
       └────────────────┘
```

## Performance Considerations

1. **Buffer Reuse**: Critical for high-throughput scenarios
2. **Zero-copy**: Where possible, write directly to network buffers
3. **Batch Writes**: Use buffered writer, flush strategically
4. **Connection Pooling**: Reuse goroutines for connection handling
5. **Lock Contention**: Minimize locks on hot paths

## Testing Strategy

### Testing Framework
We use **testify/assert** for all unit tests:
- **Rationale**: Cleaner, more readable assertions compared to manual if/error checks
- **Benefits**:
  - Better error messages with automatic context
  - Fluent assertion API (assert.Equal, assert.NoError, etc.)
  - Consistent testing style across the codebase
  - Reduces boilerplate code
- **Convention**: Import as `"github.com/stretchr/testify/assert"`

Example:
```go
func TestExample(t *testing.T) {
    result := someFunction()
    assert.NoError(t, result.err)
    assert.Equal(t, expected, result.value)
    assert.NotNil(t, result.data)
}
```

### Unit Tests
- Buffer pool allocation/deallocation
- Message parsing (each message type)
- Message encoding (each message type)
- Protocol state machine

### Integration Tests
- psql connection and simple query
- pgx driver (Go)
- psycopg2 driver (Python)
- Prepared statements
- Transaction handling
- Error scenarios
- Connection drops

### Performance Tests
- Buffer pool efficiency
- Concurrent connection handling
- Query throughput
- Memory usage profiling

## References

- PostgreSQL Protocol Documentation: https://www.postgresql.org/docs/current/protocol.html
- Postgres source: `src/backend/tcop/postgres.c`
- Postgres protocol constants: `src/include/libpq/protocol.h`
- Vitess MySQL protocol: `go/mysql/conn.go`
- Vitess buffer pool: `go/bucketpool/bucketpool.go`
