# PostgreSQL Wire Protocol - Testing Guide

## Test Coverage Checklist

### Phase 1: Foundation & Infrastructure

#### Buffer Pool Tests
- [ ] Allocate buffer from pool
- [ ] Return buffer to pool
- [ ] Get correctly sized bucket
- [ ] Handle oversized allocations
- [ ] Pool reuse (allocate, free, reallocate same bucket)
- [ ] Concurrent allocation/deallocation
- [ ] Reader/Writer pool Get and Put

#### Packet I/O Tests
- [ ] Read message header (type + length)
- [ ] Read message body
- [ ] Write message with header
- [ ] Handle partial reads
- [ ] Handle network errors
- [ ] Buffer pooling integration (buffers returned after use)
- [ ] Large messages (> buffer size)
- [ ] Small messages (< buffer size)

---

### Phase 2: Startup Handshake

#### Unit Tests
- [ ] Parse valid StartupMessage
- [ ] Parse SSL negotiation request
- [ ] Extract user parameter
- [ ] Extract database parameter
- [ ] Extract application_name parameter
- [ ] Reject invalid protocol version
- [ ] Reject oversized startup packet
- [ ] Send AuthenticationOk message
- [ ] Send BackendKeyData message
- [ ] Send ParameterStatus messages
- [ ] Send ReadyForQuery message

#### Integration Tests
- [ ] psql connects successfully (trust auth)
- [ ] Connection shows in pg_stat_activity style output
- [ ] psql receives server version
- [ ] psql shows correct database name
- [ ] Connection can be cleanly terminated
- [ ] Reject SSL request gracefully
- [ ] Multiple concurrent connections

---

### Phase 3: Simple Query Protocol

#### Unit Tests - Message Parsing
- [ ] Parse simple query ('Q') message
- [ ] Parse terminate ('X') message
- [ ] Handle empty query string
- [ ] Handle long query string

#### Unit Tests - Message Encoding
- [ ] Encode RowDescription message
  - [ ] Multiple fields
  - [ ] Various data types (int4, text, bool, etc.)
  - [ ] Table OID and column attribute numbers
- [ ] Encode DataRow message
  - [ ] NULL values
  - [ ] Text format values
  - [ ] Binary format values
  - [ ] Multiple columns
- [ ] Encode CommandComplete message
  - [ ] SELECT with row count
  - [ ] INSERT with OID and row count
  - [ ] UPDATE with row count
  - [ ] DELETE with row count
  - [ ] Other commands
- [ ] Encode ReadyForQuery message
  - [ ] Idle state ('I')
  - [ ] Transaction state ('T')
  - [ ] Error state ('E')
- [ ] Encode ErrorResponse message
  - [ ] All required fields (severity, code, message)
  - [ ] Optional fields (detail, hint, position)
  - [ ] Multiple errors
- [ ] Encode NoticeResponse message

#### Integration Tests
- [ ] psql executes: `SELECT 1;`
- [ ] psql executes: `SELECT * FROM pg_type LIMIT 5;`
- [ ] psql executes: `SELECT 1, 'hello', true, NULL;`
- [ ] psql executes multi-row query
- [ ] psql handles empty result set: `SELECT * FROM pg_type WHERE false;`
- [ ] psql receives error for invalid query
- [ ] psql receives error for non-existent table
- [ ] Multiple queries in sequence
- [ ] Query interruption (Ctrl+C in psql)

---

### Phase 4: Extended Query Protocol

#### Unit Tests - Parse Message
- [ ] Parse with statement name
- [ ] Parse unnamed statement
- [ ] Parse with parameter types
- [ ] Parse without parameter types

#### Unit Tests - Bind Message
- [ ] Bind with all parameters
- [ ] Bind with NULL parameters
- [ ] Bind with text format
- [ ] Bind with binary format
- [ ] Bind to named portal
- [ ] Bind to unnamed portal

#### Unit Tests - Execute Message
- [ ] Execute with max_rows = 0 (all rows)
- [ ] Execute with max_rows > 0 (limited)
- [ ] Execute named portal
- [ ] Execute unnamed portal

#### Unit Tests - Other Messages
- [ ] Describe statement ('S')
- [ ] Describe portal ('P')
- [ ] Close statement ('S')
- [ ] Close portal ('P')
- [ ] Sync message
- [ ] Flush message

#### Integration Tests
- [ ] pgx (Go) prepared statement
- [ ] psycopg2 (Python) prepared statement
- [ ] Parameterized query: `SELECT $1::int + $2::int`
- [ ] Multiple parameters of different types
- [ ] Reuse prepared statement multiple times
- [ ] Named vs unnamed statements
- [ ] Close and recreate statement
- [ ] Portal suspension (max_rows limit)
- [ ] Sync after error recovery

---

### Phase 5: Authentication

#### Unit Tests
- [ ] Trust authentication
- [ ] Clear text password request
- [ ] MD5 password challenge/response
- [ ] SCRAM-SHA-256 authentication flow
  - [ ] Initial client message
  - [ ] Server first message
  - [ ] Client final message
  - [ ] Server final message
- [ ] Authentication failure

#### Integration Tests
- [ ] psql trust auth (no password)
- [ ] psql clear text password
- [ ] psql MD5 authentication
- [ ] psql SCRAM-SHA-256 authentication
- [ ] pgx with various auth methods
- [ ] Wrong password rejection
- [ ] Non-existent user rejection

---

### Phase 6: Advanced Features

#### COPY Protocol Tests
- [ ] COPY TO (CopyOut)
- [ ] COPY FROM (CopyIn)
- [ ] CopyData messages
- [ ] CopyDone message
- [ ] CopyFail message
- [ ] COPY with CSV format
- [ ] COPY with BINARY format

#### Transaction Tests
- [ ] BEGIN transaction
- [ ] COMMIT transaction
- [ ] ROLLBACK transaction
- [ ] Implicit transaction for single query
- [ ] ReadyForQuery shows correct state
- [ ] Transaction error handling
- [ ] Nested savepoints

#### Connection Management Tests
- [ ] Graceful connection close
- [ ] Forced connection close
- [ ] Keep-alive handling
- [ ] Connection timeout
- [ ] Idle connection handling

---

### Phase 7: End-to-End Validation

#### Protocol Conformance
- [ ] PostgreSQL JDBC driver works
- [ ] Node.js pg driver works
- [ ] Python psycopg2 works
- [ ] Go pgx driver works
- [ ] Rust postgres driver works
- [ ] Protocol analyzer shows valid messages

#### Error Scenarios
- [ ] Client disconnects during query
- [ ] Client sends malformed message
- [ ] Client sends wrong protocol version
- [ ] Server shutdown during query
- [ ] Query timeout
- [ ] Out of memory during query
- [ ] Network timeout
- [ ] TCP connection reset

#### Concurrent Operations
- [ ] 100 concurrent connections
- [ ] 1000 concurrent connections
- [ ] Concurrent queries on same connection (should be serialized)
- [ ] Connection pool exhaustion
- [ ] Rapid connect/disconnect

#### Performance Tests
- [ ] Baseline: SELECT 1 (latency)
- [ ] Baseline: SELECT 1 (throughput)
- [ ] Large result set (10K rows)
- [ ] Large result set (100K rows)
- [ ] Prepared statement reuse (vs simple query)
- [ ] Buffer pool hit rate
- [ ] Memory usage under load
- [ ] CPU usage under load
- [ ] Compare with native Postgres performance

---

## Test Commands

### Manual Testing with psql
```bash
# Connect to multigateway
psql -h localhost -p 5432 -U testuser -d testdb

# Simple queries
SELECT 1;
SELECT version();
SELECT * FROM pg_type LIMIT 5;

# Error handling
SELECT * FROM nonexistent_table;

# Multi-row results
SELECT generate_series(1, 100);
```

### Testing with Go pgx
```go
conn, err := pgx.Connect(ctx, "postgres://testuser@localhost:5432/testdb")
// Run queries
```

### Testing with Python psycopg2
```python
import psycopg2
conn = psycopg2.connect(host="localhost", port=5432, user="testuser", database="testdb")
cur = conn.cursor()
cur.execute("SELECT 1")
```

### Protocol Analysis
```bash
# Capture traffic
tcpdump -i lo0 -w pg-protocol.pcap port 5432

# Analyze with Wireshark
wireshark pg-protocol.pcap
```

---

## Success Criteria

### Minimum Viable Product (Phase 1-3)
- ✅ All unit tests pass
- ✅ psql can connect
- ✅ psql can execute SELECT queries
- ✅ Error messages display correctly
- ✅ Connection cleanup works

### Production Ready (Phase 1-7)
- ✅ All unit and integration tests pass
- ✅ At least 3 different client libraries work
- ✅ Handles 1000 concurrent connections
- ✅ Performance within 20% of native Postgres for simple queries
- ✅ No memory leaks under load
- ✅ Graceful error handling
- ✅ Protocol conformance validated

---

## Test Automation

### Unit Tests
```bash
# Run all unit tests
go test ./go/pgprotocol/...

# Run with coverage
go test -cover ./go/pgprotocol/...

# Run with race detector
go test -race ./go/pgprotocol/...
```

### Integration Tests
```bash
# Run integration tests (requires running multigateway)
go test -tags=integration ./go/pgprotocol/server/...
```

### Benchmark Tests
```bash
# Run benchmarks
go test -bench=. ./go/pgprotocol/bufpool/
go test -bench=. ./go/pgprotocol/server/
```
