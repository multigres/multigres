// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package client provides a PostgreSQL wire protocol client implementation.
// This is used for MultiPooler -> PostgreSQL communication.
//
// # Connection Lifecycle
//
// Use [Connect] to establish a connection to PostgreSQL, then execute queries
// with [Conn.Query] or [Conn.QueryStreaming]. The extended query protocol is
// available via [Conn.Prepare], [Conn.DescribeStatement], and [Conn.Execute].
//
// # PostgreSQL Diagnostic Errors
//
// [PgDiagnosticError] is returned when PostgreSQL sends an ErrorResponse message.
// It wraps [sqltypes.PgDiagnostic] and captures all 14 PostgreSQL error fields:
//
//   - Severity, Code (SQLSTATE), Message (required fields)
//   - Detail, Hint (optional explanatory text)
//   - Position (cursor position for syntax errors)
//   - InternalPosition, InternalQuery (for internal queries)
//   - Where (PL/pgSQL call stack)
//   - Schema, Table, Column, DataType, Constraint (object identifiers)
//
// The error (*sqltypes.PgDiagnostic) can be converted to [mterrors.PgError]
// to propagate through gRPC while preserving all diagnostic fields:
//
//	var diag *sqltypes.PgDiagnostic
//	if errors.As(err, &diag) {
//	    mtErr := mterrors.NewPgError(diag)
//	    // mtErr preserves all PostgreSQL error fields through gRPC
//	}
//
// Use the SQLSTATE() method to check for specific error codes:
//
//	if diag.SQLSTATE() == "42P01" {
//	    // Handle undefined table error
//	}
//
// See: https://www.postgresql.org/docs/current/protocol-error-fields.html
package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

const (
	// connBufferSize is the size of read and write buffers.
	connBufferSize = 16 * 1024
)

// Config holds the configuration for connecting to a PostgreSQL server.
type Config struct {
	// Host is the server hostname or IP address (for TCP connections).
	// Ignored if SocketFile is set.
	Host string

	// Port is the server port number (for TCP connections).
	// Ignored if SocketFile is set.
	Port int

	// SocketFile is the full path to the PostgreSQL Unix socket file.
	// If set, Unix socket connection is used instead of TCP.
	// Example: /var/run/postgresql/.s.PGSQL.5432
	// If empty, TCP connection to Host:Port is used.
	SocketFile string

	// User is the PostgreSQL user name.
	User string

	// Password is the user's password (optional for trust auth).
	Password string

	// Database is the database name to connect to.
	Database string

	// Parameters are additional connection parameters.
	Parameters map[string]string

	// TLSConfig is the TLS configuration for SSL connections.
	// Only used for TCP connections. If nil, SSL is not used.
	TLSConfig *tls.Config

	// DialTimeout is the timeout for establishing the connection.
	DialTimeout time.Duration
}

// Conn represents a client connection to a PostgreSQL server.
// It handles the wire protocol encoding/decoding and connection state management.
type Conn struct {
	// conn is the underlying network connection.
	conn net.Conn

	// bufferedReader is used for reading from the connection.
	bufferedReader *bufio.Reader

	// bufferedWriter is used for writing to the connection.
	bufferedWriter *bufio.Writer

	// bufmu protects bufferedReader and bufferedWriter during concurrent access.
	bufmu sync.Mutex

	// config is the connection configuration.
	config *Config

	// Backend key data received from the server.
	processID uint32
	secretKey uint32

	// Server parameters received during startup.
	serverParams map[string]string

	// txnStatus is the current transaction status.
	txnStatus byte

	// state stores connection-specific information.
	// Callers can store their own state here by calling SetConnectionState.
	state any

	// closed indicates whether the connection has been closed.
	closed atomic.Bool

	// ctx is the context for this connection.
	ctx    context.Context
	cancel context.CancelFunc
}

// Connect establishes a new connection to a PostgreSQL server.
// If config.SocketFile is set, connects via Unix socket.
// Otherwise, connects via TCP to config.Host:config.Port.
func Connect(ctx context.Context, config *Config) (*Conn, error) {
	dialer := &net.Dialer{
		Timeout: config.DialTimeout,
	}

	var netConn net.Conn
	var err error

	if config.SocketFile != "" {
		// Unix socket connection.
		netConn, err = dialer.DialContext(ctx, "unix", config.SocketFile)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Unix socket %s: %w", config.SocketFile, err)
		}
	} else {
		// TCP connection.
		address := fmt.Sprintf("%s:%d", config.Host, config.Port)
		netConn, err = dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
		}
	}

	// Create the connection object.
	connCtx, cancel := context.WithCancel(ctx)
	c := &Conn{
		conn:           netConn,
		bufferedReader: bufio.NewReaderSize(netConn, connBufferSize),
		bufferedWriter: bufio.NewWriterSize(netConn, connBufferSize),
		config:         config,
		serverParams:   make(map[string]string),
		txnStatus:      protocol.TxnStatusIdle,
		ctx:            connCtx,
		cancel:         cancel,
	}

	// Perform the startup handshake.
	if err := c.startup(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("startup failed: %w", err)
	}

	return c, nil
}

// Close closes the connection.
func (c *Conn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil // Already closed.
	}

	c.cancel()

	// Send Terminate message (best effort).
	_ = c.writeTerminate()
	_ = c.flush()

	return c.conn.Close()
}

// IsClosed returns true if the connection has been closed.
func (c *Conn) IsClosed() bool {
	return c.closed.Load()
}

// ProcessID returns the backend process ID.
func (c *Conn) ProcessID() uint32 {
	return c.processID
}

// SecretKey returns the backend secret key for query cancellation.
func (c *Conn) SecretKey() uint32 {
	return c.secretKey
}

// ServerParams returns the server parameters received during startup.
func (c *Conn) ServerParams() map[string]string {
	return c.serverParams
}

// TxnStatus returns the current transaction status.
func (c *Conn) TxnStatus() byte {
	return c.txnStatus
}

// Context returns the connection's context.
func (c *Conn) Context() context.Context {
	return c.ctx
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// SetDeadline sets the read and write deadlines on the connection.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline on the connection.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline on the connection.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// GetConnectionState returns the connection-specific state.
func (c *Conn) GetConnectionState() any {
	return c.state
}

// SetConnectionState sets the connection-specific state.
// This allows callers to store their own state per connection.
func (c *Conn) SetConnectionState(state any) {
	c.state = state
}

// flush flushes any buffered writes.
func (c *Conn) flush() error {
	return c.bufferedWriter.Flush()
}

// WriteCopyData sends a CopyData ('d') message to PostgreSQL.
// The data should already be appropriately sized by upstream layers
// (client chunking, gRPC message limits, protocol reading).
func (c *Conn) WriteCopyData(data []byte) error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	// Write message: 'd' + length (4 bytes) + data
	msgLen := 4 + len(data)
	if err := c.bufferedWriter.WriteByte(protocol.MsgCopyData); err != nil {
		return fmt.Errorf("failed to write CopyData type: %w", err)
	}
	if err := c.writeUint32(uint32(msgLen)); err != nil {
		return fmt.Errorf("failed to write CopyData length: %w", err)
	}
	if _, err := c.bufferedWriter.Write(data); err != nil {
		return fmt.Errorf("failed to write CopyData body: %w", err)
	}

	return c.flush()
}

// WriteCopyDone sends a CopyDone ('c') message to PostgreSQL
// This signals that all COPY data has been sent
func (c *Conn) WriteCopyDone() error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	if err := c.bufferedWriter.WriteByte(protocol.MsgCopyDone); err != nil {
		return fmt.Errorf("failed to write CopyDone type: %w", err)
	}
	if err := c.writeUint32(4); err != nil { // Length only (no body)
		return fmt.Errorf("failed to write CopyDone length: %w", err)
	}

	return c.flush()
}

// WriteCopyFail sends a CopyFail ('f') message to PostgreSQL
// This aborts the COPY operation with the given error message
func (c *Conn) WriteCopyFail(errorMsg string) error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	msgBytes := append([]byte(errorMsg), 0) // Null-terminated
	msgLen := 4 + len(msgBytes)

	if err := c.bufferedWriter.WriteByte(protocol.MsgCopyFail); err != nil {
		return fmt.Errorf("failed to write CopyFail type: %w", err)
	}
	if err := c.writeUint32(uint32(msgLen)); err != nil {
		return fmt.Errorf("failed to write CopyFail length: %w", err)
	}
	if _, err := c.bufferedWriter.Write(msgBytes); err != nil {
		return fmt.Errorf("failed to write CopyFail body: %w", err)
	}

	return c.flush()
}

// ReadCopyDoneResponse reads the CommandComplete response after WriteCopyDone()
// Returns the command tag (e.g., "COPY 100") and rows affected
// Note: In simple query protocol, PostgreSQL sends CommandComplete followed by ReadyForQuery
// We need to consume both messages to clear the buffer
func (c *Conn) ReadCopyDoneResponse(ctx context.Context) (string, uint64, error) {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	var commandTag string
	var rowsAffected uint64
	gotCommandComplete := false

	// Read messages until we get both CommandComplete and ReadyForQuery
	for {
		msgType, err := c.readMessageType()
		if err != nil {
			return "", 0, fmt.Errorf("failed to read message type: %w", err)
		}

		length, err := c.readMessageLength()
		if err != nil {
			return "", 0, fmt.Errorf("failed to read message length: %w", err)
		}

		body, err := c.readMessageBody(length)
		if err != nil {
			return "", 0, fmt.Errorf("failed to read message body: %w", err)
		}

		switch msgType {
		case protocol.MsgCommandComplete:
			// Parse command tag
			if len(body) == 0 {
				return "", 0, errors.New("empty CommandComplete body")
			}
			// Command tag is null-terminated string
			tag := string(body)
			if len(tag) > 0 && tag[len(tag)-1] == 0 {
				tag = tag[:len(tag)-1]
			}

			// Parse rows affected from tag (e.g., "COPY 100" -> 100)
			var rows uint64
			if len(tag) > 5 && tag[:4] == "COPY" {
				_, _ = fmt.Sscanf(tag[5:], "%d", &rows)
			}

			commandTag = tag
			rowsAffected = rows
			gotCommandComplete = true
			// Continue reading to get ReadyForQuery

		case protocol.MsgErrorResponse:
			// Parse error message
			return "", 0, c.parseError(body)

		case protocol.MsgNoticeResponse:
			// Ignore notices
			continue

		case protocol.MsgReadyForQuery:
			// End of response - if we got CommandComplete, return success
			if gotCommandComplete {
				return commandTag, rowsAffected, nil
			}
			// Otherwise, we got ReadyForQuery without CommandComplete (error case)
			return "", 0, errors.New("received ReadyForQuery without CommandComplete")

		default:
			return "", 0, fmt.Errorf("unexpected message type after CopyDone: '%c'", msgType)
		}
	}
}

// ReadCopyInResponse reads and parses a CopyInResponse ('G') message from PostgreSQL
// This message is sent in response to a COPY FROM STDIN command
// Returns the overall format and per-column formats
func (c *Conn) ReadCopyInResponse() (format int16, columnFormats []int16, err error) {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	msgType, err := c.readMessageType()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message type: %w", err)
	}
	if msgType != protocol.MsgCopyInResponse {
		return 0, nil, fmt.Errorf("expected CopyInResponse ('G'), got '%c'", msgType)
	}

	length, err := c.readMessageLength()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message length: %w", err)
	}

	body, err := c.readMessageBody(length)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message body: %w", err)
	}

	if len(body) < 3 {
		return 0, nil, fmt.Errorf("CopyInResponse body too short: %d bytes", len(body))
	}

	// Read format (Int8, 1 byte) - 0=text, 1=binary
	format = int16(body[0])

	// Read number of columns (Int16, 2 bytes, big-endian)
	numCols := int16(uint16(body[1])<<8 | uint16(body[2]))

	// Read format codes for each column (Int16 each)
	columnFormats = make([]int16, numCols)
	offset := 3
	for i := 0; i < int(numCols); i++ {
		if offset+2 > len(body) {
			return 0, nil, errors.New("CopyInResponse body too short for column formats")
		}
		columnFormats[i] = int16(uint16(body[offset])<<8 | uint16(body[offset+1]))
		offset += 2
	}

	return format, columnFormats, nil
}

// InitiateCopyFromStdin sends a COPY FROM STDIN query and reads the CopyInResponse.
// This is a special operation that doesn't follow the normal query flow.
// Returns the COPY format and column formats from the CopyInResponse.
func (c *Conn) InitiateCopyFromStdin(ctx context.Context, copyQuery string) (format int16, columnFormats []int16, err error) {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	// Send the COPY query
	w := NewMessageWriter()
	w.WriteString(copyQuery)
	if err := c.writeMessage(protocol.MsgQuery, w.Bytes()); err != nil {
		return 0, nil, fmt.Errorf("failed to send COPY query: %w", err)
	}

	// Loop through messages until we get CopyInResponse or an error
	// We need to skip NoticeResponse and ParameterStatus messages
	// This is similar to processQueryResponses but simplified for COPY initiation
	for {
		msgType, err := c.readMessageType()
		if err != nil {
			return 0, nil, fmt.Errorf("failed to read message type: %w", err)
		}

		bodyLen, err := c.readMessageLength()
		if err != nil {
			return 0, nil, fmt.Errorf("failed to read message length: %w", err)
		}

		body, err := c.readMessageBody(bodyLen)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to read message body: %w", err)
		}

		switch msgType {
		case protocol.MsgCopyInResponse:
			// Parse CopyInResponse body using manual byte array indexing
			// Format: Int8 (format) + Int16 (numCols) + Int16[numCols] (column formats)
			if len(body) < 3 {
				return 0, nil, fmt.Errorf("CopyInResponse body too short: %d bytes", len(body))
			}

			// Read format (Int8, 1 byte) - 0=text, 1=binary
			format = int16(body[0])

			// Read number of columns (Int16, 2 bytes, big-endian)
			numCols := int16(uint16(body[1])<<8 | uint16(body[2]))

			// Read format codes for each column (Int16 each)
			columnFormats = make([]int16, numCols)
			offset := 3
			for i := 0; i < int(numCols); i++ {
				if offset+2 > len(body) {
					return 0, nil, errors.New("CopyInResponse body too short for column formats")
				}
				columnFormats[i] = int16(uint16(body[offset])<<8 | uint16(body[offset+1]))
				offset += 2
			}

			return format, columnFormats, nil

		case protocol.MsgErrorResponse:
			// Parse and return the error
			return 0, nil, c.parseError(body)

		case protocol.MsgNoticeResponse:
			// Skip notices (PostgreSQL might send notices before CopyInResponse)
			continue

		case protocol.MsgParameterStatus:
			// Handle parameter status updates, ignore errors
			_ = c.handleParameterStatus(body)
			continue

		case protocol.MsgReadyForQuery:
			// If we get ReadyForQuery before CopyInResponse, the query failed
			// but we didn't get an ErrorResponse (which shouldn't happen)
			return 0, nil, errors.New("received ReadyForQuery before CopyInResponse - query may have failed without error")

		default:
			return 0, nil, fmt.Errorf("unexpected message type during COPY initiation: '%c' (0x%02x)", msgType, msgType)
		}
	}
}

// ReadCopyOutResponse reads and parses a CopyOutResponse ('H') message from PostgreSQL
// This message is sent in response to a COPY TO STDOUT command
// Returns the overall format and per-column formats
func (c *Conn) ReadCopyOutResponse() (format int16, columnFormats []int16, err error) {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	msgType, err := c.readMessageType()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message type: %w", err)
	}
	if msgType != protocol.MsgCopyOutResponse {
		return 0, nil, fmt.Errorf("expected CopyOutResponse ('H'), got '%c'", msgType)
	}

	length, err := c.readMessageLength()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message length: %w", err)
	}

	body, err := c.readMessageBody(length)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message body: %w", err)
	}

	if len(body) < 3 {
		return 0, nil, fmt.Errorf("CopyOutResponse body too short: %d bytes", len(body))
	}

	// Read format (Int8, 1 byte) - 0=text, 1=binary
	format = int16(body[0])

	// Read number of columns (Int16, 2 bytes, big-endian)
	numCols := int16(uint16(body[1])<<8 | uint16(body[2]))

	// Read format codes for each column (Int16 each)
	columnFormats = make([]int16, numCols)
	offset := 3
	for i := 0; i < int(numCols); i++ {
		if offset+2 > len(body) {
			return 0, nil, errors.New("CopyOutResponse body too short for column formats")
		}
		columnFormats[i] = int16(uint16(body[offset])<<8 | uint16(body[offset+1]))
		offset += 2
	}

	return format, columnFormats, nil
}

// ReadCopyData reads a CopyData ('d') message from PostgreSQL.
// This is used during COPY TO STDOUT to receive data rows.
// Returns the data bytes, or io.EOF when CopyDone is received to signal end of stream.
func (c *Conn) ReadCopyData() ([]byte, error) {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	msgType, err := c.readMessageType()
	if err != nil {
		return nil, fmt.Errorf("failed to read message type: %w", err)
	}

	// CopyDone signals end of COPY data stream
	if msgType == protocol.MsgCopyDone {
		length, err := c.readMessageLength()
		if err != nil {
			return nil, fmt.Errorf("failed to read CopyDone length: %w", err)
		}
		if length != 4 {
			return nil, fmt.Errorf("invalid CopyDone length: %d (expected 4)", length)
		}
		return nil, io.EOF
	}

	if msgType != protocol.MsgCopyData {
		return nil, fmt.Errorf("expected CopyData ('d') or CopyDone ('c'), got '%c'", msgType)
	}

	length, err := c.readMessageLength()
	if err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}

	data, err := c.readMessageBody(length)
	if err != nil {
		return nil, fmt.Errorf("failed to read CopyData body: %w", err)
	}

	return data, nil
}
