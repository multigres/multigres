# PostgreSQL Wire Protocol Implementation

This package implements the PostgreSQL wire protocol version 3.0 for Multigres, allowing the multigateway to masquerade as a PostgreSQL server and accept native PostgreSQL client connections.

## Features

### Implemented
- PostgreSQL wire protocol v3.0 message parsing and serialization
- Startup message handling with parameter negotiation
- Authentication methods: trust, cleartext password, MD5
- Simple query protocol
- Basic extended query protocol structure
- SSL/TLS negotiation framework
- Connection cancellation support
- Error response formatting with SQLSTATE codes
- Server parameter status messages
- Transaction status tracking

### In Progress
- Query forwarding to multipooler
- Full extended query protocol implementation
- TLS connection upgrade
- SASL/SCRAM authentication

### Planned
- Connection pooling integration
- Query routing based on database/table
- Read/write splitting
- Prepared statement caching
- COPY protocol support
- Notification/LISTEN support

## Architecture

The implementation consists of several key components:

### `protocol.go`
Core protocol definitions and message types:
- Message type constants for frontend/backend communication
- Authentication type definitions
- Message parsing and serialization functions
- `MessageReader` and `MessageWriter` for protocol I/O

### `server.go`
PostgreSQL server implementation:
- `Server` - Main server that listens for connections
- `Connection` - Represents a client connection with state
- `Handler` interface - Processes protocol messages
- Authentication handling
- Connection lifecycle management

### Integration with Multigateway
The multigateway uses this package to:
1. Listen on a PostgreSQL port (default 5432)
2. Accept client connections using native PostgreSQL protocol
3. Authenticate clients (currently using trust auth)
4. Process simple queries locally or forward to multipooler
5. Maintain connection state and transaction status

## Usage

### Starting the Server

```go
import "github.com/multigres/multigres/go/postgres"

// Create server configuration
config := postgres.ServerConfig{
    Address:    ":5432",
    AuthMethod: "trust",
    Parameters: map[string]string{
        "server_version": "15.0 (Multigres)",
        "server_encoding": "UTF8",
    },
    MaxConnections: 100,
}

// Create handler that processes queries
handler := &MyPostgresHandler{}

// Create and start server
server := postgres.NewServer(config, handler, logger)
server.Listen()
go server.Serve()
```

### Implementing a Handler

```go
type MyPostgresHandler struct {}

func (h *MyPostgresHandler) HandleQuery(ctx context.Context, conn *postgres.Connection, query string) error {
    // Process or forward the query
    // Send results back to client using conn.Writer
    return nil
}
```

## Testing

### Unit Tests
```bash
go test ./go/postgres/...
```

### Integration Test
```bash
# Start test server
./test_postgres_server.sh

# Connect with psql
psql -h localhost -p 15432 -U test -d test

# Try queries
SELECT 1;
SELECT version();
SHOW server_version;
```

## Protocol Compatibility

The implementation targets PostgreSQL protocol version 3.0, which is compatible with:
- PostgreSQL 7.4 and later
- All modern PostgreSQL drivers (libpq, JDBC, Go pq, Python psycopg2, etc.)
- PostgreSQL tools (psql, pgAdmin, DBeaver, etc.)

## Security Considerations

Current implementation:
- Supports trust (no auth), cleartext password, and MD5 authentication
- MD5 is deprecated in PostgreSQL; SCRAM-SHA-256 support is planned
- TLS/SSL support framework is in place but not fully implemented
- Connection limits to prevent DoS

## Performance

The implementation focuses on:
- Minimal memory allocations during message parsing
- Efficient binary protocol handling
- Connection pooling preparation for multipooler integration
- Configurable timeouts and connection limits

## Next Steps

1. **Query Forwarding**: Implement forwarding of queries to multipooler
2. **Connection Pooling**: Integrate with multipooler's connection pool
3. **Extended Protocol**: Full support for prepared statements
4. **Authentication**: Add SCRAM-SHA-256 support
5. **TLS**: Complete SSL/TLS implementation
6. **Monitoring**: Add metrics and tracing