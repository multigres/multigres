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

package server

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	"github.com/multigres/multigres/go/common/sqltypes"
)

const (
	// connBufferSize is the size of read and write buffers.
	connBufferSize = 16 * 1024

	// defaultFlushDelay is the default delay before auto-flushing buffered writes.
	defaultFlushDelay = 100 * time.Millisecond
)

// errQueryCanceled is the sentinel used as the cancel cause for CancelRequest.
// Must be a single instance: CancelQuery sets it via context.WithCancelCause,
// and queryContextError checks it with errors.Is (pointer equality).
var errQueryCanceled = mterrors.NewQueryCanceled()

// Conn represents the server side connection with a PostgreSQL client.
// It handles the wire protocol encoding/decoding and connection state management.
type Conn struct {
	// conn is the underlying network connection.
	conn net.Conn

	// bufferedReader is used for reading from the connection.
	bufferedReader *bufio.Reader

	// bufferedWriter is used for writing to the connection.
	bufferedWriter *bufio.Writer

	// bufMu protects bufferedReader, bufferedWriter, and the
	// startPacket→writePacket critical section. It is held across
	// in-place packet encoding when startPacket reserves space inside
	// the bufferedWriter, so the body can't be split by an interleaved
	// write from the async notification pusher.
	bufMu sync.Mutex

	// outboundPoolBuf holds the listener-level bufpool buffer that
	// startPacket grabbed for an oversize (slow-path) packet. Non-nil
	// only between startPacket and writePacket, and only on the slow
	// path. writePacket returns it to the pool. Protected by bufMu.
	//
	// Stored as the original *[]byte the pool returned (rather than
	// taking &buf inside writePacket) because passing the address of a
	// stack-local slice to bufPool.Put would force the slice header to
	// escape to the heap on every call, even when the slow path
	// doesn't fire — sync.Pool retains its arguments. Going through
	// this field, which already points at heap memory the pool owns,
	// avoids that escape.
	outboundPoolBuf *[]byte

	// listener is a reference to the listener that accepted this connection.
	listener *Listener

	// handler processes queries for this connection.
	handler Handler

	// hashProvider provides password hashes for SCRAM authentication.
	hashProvider scram.PasswordHashProvider

	// trustAuthProvider enables trust authentication for testing.
	// When set and AllowTrustAuth() returns true, password auth is skipped.
	trustAuthProvider TrustAuthProvider

	// tlsConfig holds the TLS configuration for SSL connections.
	// When set, the server accepts SSLRequest and upgrades to TLS.
	// When nil, SSLRequest is declined with 'N'.
	tlsConfig *tls.Config

	// sslDone indicates that an SSLRequest has already been handled
	// (accepted or declined) for this connection. Prevents double negotiation.
	sslDone bool

	// gssDone indicates that a GSSENCRequest has already been handled
	// for this connection. Prevents double negotiation.
	gssDone bool

	// logger for connection-specific logging.
	logger *slog.Logger

	// connectionID is a unique identifier for this connection.
	connectionID uint32

	// backendKeyData is the secret key for this backend, used for cancellation.
	backendKeyData uint32

	// notifPush holds the async notification pusher state.
	notifPush *notifPusher

	// Startup parameters sent by the client.
	user     string
	database string
	params   map[string]string

	// protocolVersion is the negotiated protocol version.
	protocolVersion protocol.ProtocolVersion

	// Current transaction state.
	txnStatus protocol.TransactionStatus

	// state holds handler-specific connection state.
	// Handlers can store their own state here by calling SetConnectionState.
	// This allows different handler implementations to maintain their own state.
	state any

	// queryCancelMu protects queryCancelFunc.
	queryCancelMu sync.Mutex
	// queryCancelFunc cancels the current in-flight query context.
	// Set by BeginQueryCancel, cleared by EndQueryCancel.
	queryCancelFunc context.CancelCauseFunc

	// closed indicates whether the connection has been closed.
	closed atomic.Bool

	// flushTimer is used for auto-flushing buffered writes.
	flushTimer *time.Timer

	// flushDelay is the delay before auto-flushing.
	flushDelay time.Duration

	// ctx is the context for this connection, cancelled when connection closes.
	ctx    context.Context
	cancel context.CancelFunc
}

// newConn creates a new connection.
func newConn(netConn net.Conn, listener *Listener, connectionID uint32) *Conn {
	ctx, cancel := context.WithCancel(context.TODO())

	c := &Conn{
		conn:           netConn,
		listener:       listener,
		connectionID:   connectionID,
		backendKeyData: generateBackendKey(),
		logger:         listener.logger.With("connection_id", connectionID),
		txnStatus:      protocol.TxnStatusIdle,
		flushDelay:     defaultFlushDelay,
		ctx:            ctx,
		cancel:         cancel,
		params:         make(map[string]string),
		state:          nil, // Handler will initialize its own state
	}

	// Use pooled readers.
	c.bufferedReader = listener.readersPool.Get().(*bufio.Reader)
	c.bufferedReader.Reset(netConn)

	return c
}

// Close closes the connection and releases resources.
func (c *Conn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		// Already closed.
		return nil
	}

	c.cancel()

	// Clean up handler-specific state (if any).
	// The state is set to nil so handlers should handle nil-checking.
	c.state = nil

	// Return pooled resources.
	c.returnReader()
	// End writer buffering (flushes and returns to pool).
	c.endWriterBuffering()

	return c.conn.Close()
}

// SetTxnStatus sets the protocol-level transaction status indicator.
// This value is sent in ReadyForQuery ('Z') messages to inform the client
// of the current transaction state:
//   - protocol.TxnStatusIdle ('I'): not in a transaction
//   - protocol.TxnStatusInBlock ('T'): in a transaction block
//   - protocol.TxnStatusFailed ('E'): in a failed transaction block
func (c *Conn) SetTxnStatus(status protocol.TransactionStatus) {
	c.txnStatus = status
}

// TxnStatus returns the current protocol-level transaction status.
func (c *Conn) TxnStatus() protocol.TransactionStatus {
	return c.txnStatus
}

// IsInTransaction returns true if the connection is currently in a transaction.
func (c *Conn) IsInTransaction() bool {
	return c.txnStatus == protocol.TxnStatusInBlock || c.txnStatus == protocol.TxnStatusFailed
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// ConnectionID returns the connection ID.
func (c *Conn) ConnectionID() uint32 {
	return c.connectionID
}

// Handler returns the protocol handler for this connection.
func (c *Conn) Handler() Handler {
	return c.handler
}

// User returns the authenticated user.
func (c *Conn) User() string {
	return c.user
}

// Database returns the database name.
func (c *Conn) Database() string {
	return c.database
}

// GetStartupParams returns the startup parameters sent by the client,
// excluding 'user' and 'database' which are handled separately.
func (c *Conn) GetStartupParams() map[string]string {
	result := make(map[string]string, len(c.params))
	for k, v := range c.params {
		if k == "user" || k == "database" {
			continue
		}
		result[k] = v
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// Context returns the connection's context.
func (c *Conn) Context() context.Context {
	return c.ctx
}

// GetConnectionState returns the handler-specific connection state.
// Returns nil if no state has been set.
func (c *Conn) GetConnectionState() any {
	return c.state
}

// SetConnectionState sets the handler-specific connection state.
// This allows handlers to store their own state per connection.
func (c *Conn) SetConnectionState(state any) {
	c.state = state
}

// BackendKeyData returns the secret key for this connection.
func (c *Conn) BackendKeyData() uint32 {
	return c.backendKeyData
}

// BeginQueryCancel creates a child context for the current query that can be
// independently canceled. Returns the query context that should be passed to
// handler methods. Must be paired with EndQueryCancel.
func (c *Conn) BeginQueryCancel() context.Context {
	c.queryCancelMu.Lock()
	defer c.queryCancelMu.Unlock()

	queryCtx, cancel := context.WithCancelCause(c.ctx)
	c.queryCancelFunc = cancel
	return queryCtx
}

// EndQueryCancel clears the query cancel function, signaling that no query
// is in flight. Calls the cancel function with nil to release context resources.
func (c *Conn) EndQueryCancel() {
	c.queryCancelMu.Lock()
	defer c.queryCancelMu.Unlock()

	if c.queryCancelFunc != nil {
		c.queryCancelFunc(nil)
		c.queryCancelFunc = nil
	}
}

// CancelQuery cancels the in-flight query on this connection.
// Returns true if a query was in flight and was canceled.
func (c *Conn) CancelQuery() bool {
	c.queryCancelMu.Lock()
	defer c.queryCancelMu.Unlock()

	if c.queryCancelFunc != nil {
		c.queryCancelFunc(errQueryCanceled)
		c.queryCancelFunc = nil
		return true
	}
	return false
}

// queryContextError maps context-related errors to the appropriate PostgreSQL
// protocol error. Cancel requests take priority since they indicate explicit
// user action. If no context-related error is detected, the original error is
// returned unchanged.
func queryContextError(queryCtx context.Context, err error) error {
	if err == nil {
		return nil
	}
	// CancelRequest sets errQueryCanceled as the cause on the query context.
	if errors.Is(context.Cause(queryCtx), errQueryCanceled) {
		return errQueryCanceled
	}
	// Statement timeout: the handler applies context.WithTimeout for statement
	// timeouts, and DeadlineExceeded propagates up through the error chain.
	if errors.Is(err, context.DeadlineExceeded) {
		return mterrors.NewStatementTimeout()
	}
	return err
}

// returnReader returns the buffered reader to the pool.
func (c *Conn) returnReader() {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedReader != nil {
		c.bufferedReader.Reset(nil)
		c.listener.readersPool.Put(c.bufferedReader)
		c.bufferedReader = nil
	}
}

// startWriterBuffering begins buffering writes.
func (c *Conn) startWriterBuffering() {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedWriter == nil {
		c.bufferedWriter = c.listener.writersPool.Get().(*bufio.Writer)
		c.bufferedWriter.Reset(c.conn)
	}
}

// getWriter returns the writer to use (buffered or direct).
func (c *Conn) getWriter() io.Writer {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedWriter != nil {
		return c.bufferedWriter
	}
	return c.conn
}

// flush flushes any buffered writes.
func (c *Conn) flush() error {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedWriter != nil {
		return c.bufferedWriter.Flush()
	}
	return nil
}

// Flush flushes any buffered writes to the client.
// This is exposed for external callers (like gateway handler) that need to
// ensure messages are sent immediately (e.g., CopyInResponse).
func (c *Conn) Flush() error {
	return c.flush()
}

// endWriterBuffering ends write buffering and returns the writer to the pool.
// This should be called after each query to release the buffer back to the pool.
func (c *Conn) endWriterBuffering() {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedWriter != nil {
		// Flush any remaining buffered data.
		_ = c.bufferedWriter.Flush()

		// Return to pool.
		c.bufferedWriter.Reset(nil)
		c.listener.writersPool.Put(c.bufferedWriter)
		c.bufferedWriter = nil
	}
}

// generateBackendKey generates a cryptographically secure random backend key for cancellation.
// The backend key is sent to the client in the BackendKeyData message and is used
// to authenticate query cancellation requests.
func generateBackendKey() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback to time-based key if crypto/rand fails (should never happen).
		// This maintains backward compatibility with the TODO implementation.
		return uint32(time.Now().UnixNano() & 0xFFFFFFFF)
	}
	return binary.BigEndian.Uint32(b[:])
}

// serve is the main command processing loop for the connection.
// It reads messages from the client and processes them until the connection is closed.
func (c *Conn) serve() error {
	// TODO: Add startup phase timeout (equivalent to PostgreSQL's authentication_timeout).
	// Set c.conn.SetDeadline() here and clear it after handleStartup() returns.
	// Without this, a client can hold a goroutine indefinitely by stalling during
	// SSL handshake, startup packet reading, or SCRAM authentication exchange.

	// First, handle the startup phase.
	if err := c.handleStartup(); err != nil {
		if errors.Is(err, io.EOF) {
			c.logger.Debug("client disconnected before startup")
			return nil
		}
		c.logger.Error("startup failed", "error", err)
		// Try to send an error response before closing.
		// If the error is already a PgDiagnostic (e.g., duplicate SSLRequest
		// with native SQLSTATE), send it directly. Otherwise, wrap with MTE01.
		var diag *mterrors.PgDiagnostic
		if errors.As(err, &diag) {
			_ = c.writePgDiagnosticResponse(protocol.MsgErrorResponse, diag)
		} else {
			_ = c.writeError(mterrors.MTE01.NewWithDetail(err.Error()))
		}
		_ = c.flush()
		return err
	}

	// Main command loop.
	for {
		// Check if connection is closed.
		if c.closed.Load() {
			return nil
		}

		// Read the message type (1 byte).
		msgType, err := c.ReadMessageType()
		if err != nil {
			// EOF or connection error - close gracefully.
			if errors.Is(err, io.EOF) {
				c.logger.Debug("client closed connection")
				return nil
			}
			c.logger.Error("error reading message type", "error", err)
			return err
		}

		// Process the message based on type.
		if err := c.handleMessage(msgType); err != nil {
			if errors.Is(err, io.EOF) {
				c.logger.Debug("client closed connection")
				return nil
			}
			c.logger.Error("error handling message", "type", string(msgType), "error", err)
			// Send error response and continue (unless it's a fatal error).
			_ = c.writeError(mterrors.MTD03.NewWithDetail(err.Error()))
			_ = c.writeReadyForQuery()
			_ = c.flush()
			// For now, close connection on any error.
			return err
		}
	}
}

// handleMessage processes a single message from the client.
func (c *Conn) handleMessage(msgType byte) error {
	switch msgType {
	case protocol.MsgQuery:
		return c.handleQuery()

	case protocol.MsgParse:
		return c.handleParse()

	case protocol.MsgBind:
		return c.handleBind()

	case protocol.MsgExecute:
		return c.handleExecute()

	case protocol.MsgDescribe:
		return c.handleDescribe()

	case protocol.MsgClose:
		return c.handleClose()

	case protocol.MsgSync:
		return c.handleSync()

	case protocol.MsgFlush:
		return c.flush()

	case protocol.MsgTerminate:
		c.logger.Debug("received termination message")
		return io.EOF // Signal connection should close

	default:
		return fmt.Errorf("unsupported message type: %c (0x%02x)", msgType, msgType)
	}
}

// handleQuery handles a 'Q' (Query) message - simple query protocol.
// Supports multiple statements in a single query (e.g., "SELECT 1; SELECT 2;").
func (c *Conn) handleQuery() error {
	// Start write buffering on first query (if not already started).
	// This avoids holding buffers during startup/idle time.
	c.startWriterBuffering()

	// Return the writer buffer after query completes.
	// This ensures idle connections don't hold buffers between queries.
	defer c.endWriterBuffering()

	// Read the query string.
	queryStr, err := c.readQueryMessage()
	if err != nil {
		return fmt.Errorf("reading query message: %w", err)
	}

	c.logger.Debug("received query", "query", queryStr)

	// Create a cancelable query context so cancel requests can interrupt this query.
	queryCtx := c.BeginQueryCancel()
	defer c.EndQueryCancel()

	// Track state for current result set.
	// This is reset when we complete a result set (when CommandTag is set).
	sentRowDescription := false

	// Execute the query via the handler with streaming callback.
	// The callback will be invoked multiple times for:
	// 1. Large result sets (streamed in chunks)
	// 2. Multiple statements in a single query (each potentially with large result sets)
	err = c.handler.HandleQuery(queryCtx, c, queryStr, func(ctx context.Context, result *sqltypes.Result) error {
		// Handle empty query (nil result signals empty query).
		if result == nil {
			return c.writeEmptyQueryResponse()
		}

		// Send notices immediately (zero-buffering delivery).
		// Notices may arrive as standalone Results (no rows/CommandTag) from the gRPC
		// streaming path, or bundled with the final Result that has a CommandTag.
		for _, notice := range result.Notices {
			if err := c.writeNoticeResponse(notice); err != nil {
				return fmt.Errorf("writing notice response: %w", err)
			}
		}

		// On first callback with fields for this result set, send RowDescription.
		// Use nil check (not len > 0) because zero-column results (e.g., "select union select")
		// have a non-nil empty Fields slice and still require a RowDescription message.
		if !sentRowDescription && result.Fields != nil {
			if err := c.writeRowDescription(result.Fields); err != nil {
				return fmt.Errorf("writing row description: %w", err)
			}
			sentRowDescription = true
		}

		// Send all data rows in this chunk.
		for _, row := range result.Rows {
			if err := c.writeDataRow(row); err != nil {
				return fmt.Errorf("writing data row: %w", err)
			}
		}

		// If CommandTag is set, this is the last packet of the current result set.
		if result.CommandTag != "" {
			if err := c.writeCommandComplete(result.CommandTag); err != nil {
				return fmt.Errorf("writing command complete: %w", err)
			}

			// Reset state for next result set (if any).
			sentRowDescription = false
		}

		return nil
	})
	if err != nil {
		err = queryContextError(queryCtx, err)
		c.logger.Error("query execution failed", "query", queryStr, "error", err)
		if writeErr := c.writeError(err); writeErr != nil {
			return writeErr
		}
	}

	// Send ReadyForQuery after all statements have been processed.
	if err := c.writeReadyForQuery(); err != nil {
		return err
	}

	return c.flush()
}

// handleParse handles a 'P' (Parse) message - extended query protocol.
// Parse message format:
// - Statement name (string, null-terminated)
// - Query string (string, null-terminated)
// - Number of parameter data types (int16)
// - Parameter data type OIDs ([]uint32)
func (c *Conn) handleParse() error {
	c.startWriterBuffering()
	defer c.endWriterBuffering()

	// Read message length.
	bodyLen, err := c.ReadMessageLength()
	if err != nil {
		return fmt.Errorf("failed to read Parse message length: %w", err)
	}

	// Read message body.
	buf, err := c.readMessageBody(bodyLen)
	if err != nil {
		return fmt.Errorf("failed to read Parse message body: %w", err)
	}
	defer c.returnReadBuffer(buf)

	// Parse the message.
	reader := NewMessageReader(buf)

	stmtName, err := reader.ReadString()
	if err != nil {
		return fmt.Errorf("failed to read statement name: %w", err)
	}

	queryStr, err := reader.ReadString()
	if err != nil {
		return fmt.Errorf("failed to read query string: %w", err)
	}

	paramCount, err := reader.ReadInt16()
	if err != nil {
		return fmt.Errorf("failed to read parameter count: %w", err)
	}

	paramTypes := make([]uint32, paramCount)
	for i := range paramCount {
		oid, err := reader.ReadUint32()
		if err != nil {
			return fmt.Errorf("failed to read parameter type %d: %w", i, err)
		}
		paramTypes[i] = oid
	}

	c.logger.Debug("parse", "name", stmtName, "query", queryStr, "param_count", paramCount)

	// Call the handler to validate and prepare the statement.
	// The handler is responsible for storing any state it needs.
	if err := c.handler.HandleParse(c.ctx, c, stmtName, queryStr, paramTypes); err != nil {
		if writeErr := c.writeError(mterrors.MTD04.NewWithDetail(err.Error())); writeErr != nil {
			return writeErr
		}
		// Do NOT send ReadyForQuery here. In the extended query protocol, the client
		// pipelines Parse + Describe + Sync (or Parse + Bind + Execute + Sync).
		// ReadyForQuery must only be sent in response to Sync. Sending it here would
		// cause protocol desynchronization: pgx would read the premature ReadyForQuery
		// and think the pipeline is done, but stale responses from subsequent messages
		// (Describe, Sync) would corrupt the next query's response stream.
		return c.flush()
	}

	// Send ParseComplete message.
	if err := c.writeMessage(protocol.MsgParseComplete, nil); err != nil {
		return fmt.Errorf("failed to write ParseComplete: %w", err)
	}

	return c.flush()
}

// handleBind handles a 'B' (Bind) message - extended query protocol.
func (c *Conn) handleBind() error {
	c.startWriterBuffering()
	defer c.endWriterBuffering()

	// Read message length.
	bodyLen, err := c.ReadMessageLength()
	if err != nil {
		return fmt.Errorf("failed to read Bind message length: %w", err)
	}

	// Read message body.
	buf, err := c.readMessageBody(bodyLen)
	if err != nil {
		return fmt.Errorf("failed to read Bind message body: %w", err)
	}
	defer c.returnReadBuffer(buf)

	// Parse the message.
	reader := NewMessageReader(buf)

	portalName, err := reader.ReadString()
	if err != nil {
		return fmt.Errorf("failed to read portal name: %w", err)
	}

	stmtName, err := reader.ReadString()
	if err != nil {
		return fmt.Errorf("failed to read statement name: %w", err)
	}

	// Read parameter format codes
	paramFormatCount, err := reader.ReadInt16()
	if err != nil {
		return fmt.Errorf("failed to read parameter format count: %w", err)
	}
	paramFormats := make([]int16, paramFormatCount)
	for i := range paramFormatCount {
		format, err := reader.ReadInt16()
		if err != nil {
			return fmt.Errorf("failed to read parameter format: %w", err)
		}
		paramFormats[i] = format
	}

	// Read parameters
	paramCount, err := reader.ReadInt16()
	if err != nil {
		return fmt.Errorf("failed to read parameter count: %w", err)
	}
	params := make([][]byte, paramCount)
	for i := range paramCount {
		param, err := reader.ReadByteString()
		if err != nil {
			return fmt.Errorf("failed to read parameter: %w", err)
		}
		params[i] = param
	}

	// Read result format codes
	resultFormatCount, err := reader.ReadInt16()
	if err != nil {
		return fmt.Errorf("failed to read result format count: %w", err)
	}
	resultFormats := make([]int16, resultFormatCount)
	for i := range resultFormatCount {
		format, err := reader.ReadInt16()
		if err != nil {
			return fmt.Errorf("failed to read result format: %w", err)
		}
		resultFormats[i] = format
	}

	c.logger.Debug("bind", "portal", portalName, "statement", stmtName, "param_count", len(params))

	// Call the handler to create and bind the portal with parameters.
	if err := c.handler.HandleBind(c.ctx, c, portalName, stmtName, params, paramFormats, resultFormats); err != nil {
		if writeErr := c.writeError(mterrors.MTD05.NewWithDetail(err.Error())); writeErr != nil {
			return writeErr
		}
		// Do NOT send ReadyForQuery here — same reasoning as handleParse.
		// ReadyForQuery is sent only in response to Sync.
		return c.flush()
	}

	// Send BindComplete message.
	if err := c.writeMessage(protocol.MsgBindComplete, nil); err != nil {
		return fmt.Errorf("failed to write BindComplete: %w", err)
	}

	return c.flush()
}

// handleExecute handles an 'E' (Execute) message - extended query protocol.
// Execute message format:
// - Portal name (string, null-terminated)
// - Max rows to return (int32, 0 = no limit)
func (c *Conn) handleExecute() error {
	c.startWriterBuffering()
	defer c.endWriterBuffering()

	// Read message length.
	bodyLen, err := c.ReadMessageLength()
	if err != nil {
		return fmt.Errorf("failed to read Execute message length: %w", err)
	}

	// Read message body.
	buf, err := c.readMessageBody(bodyLen)
	if err != nil {
		return fmt.Errorf("failed to read Execute message body: %w", err)
	}
	defer c.returnReadBuffer(buf)

	// Parse the message.
	reader := NewMessageReader(buf)

	portalName, err := reader.ReadString()
	if err != nil {
		return fmt.Errorf("failed to read portal name: %w", err)
	}

	maxRows, err := reader.ReadInt32()
	if err != nil {
		return fmt.Errorf("failed to read max rows: %w", err)
	}

	c.logger.Debug("execute", "portal", portalName, "max_rows", maxRows)

	// Create a cancelable query context so cancel requests can interrupt this execution.
	queryCtx := c.BeginQueryCancel()
	defer c.EndQueryCancel()

	// Track state for streaming results.
	sentRowDescription := false

	// Call the handler to execute the portal with streaming callback.
	// The handler is responsible for retrieving the portal and executing it.
	err = c.handler.HandleExecute(queryCtx, c, portalName, maxRows, func(ctx context.Context, result *sqltypes.Result) error {
		// Send notices immediately (zero-buffering delivery).
		for _, notice := range result.Notices {
			if err := c.writeNoticeResponse(notice); err != nil {
				return fmt.Errorf("writing notice response: %w", err)
			}
		}

		// On first callback with fields, send RowDescription.
		// Use nil check (not len > 0) because zero-column results still require RowDescription.
		if !sentRowDescription && result.Fields != nil {
			if err := c.writeRowDescription(result.Fields); err != nil {
				return fmt.Errorf("writing row description: %w", err)
			}
			sentRowDescription = true
		}

		// Send all data rows in this chunk.
		for _, row := range result.Rows {
			if err := c.writeDataRow(row); err != nil {
				return fmt.Errorf("writing data row: %w", err)
			}
		}

		// If CommandTag is set, this is the last packet.
		if result.CommandTag != "" {
			if err := c.writeCommandComplete(result.CommandTag); err != nil {
				return fmt.Errorf("writing command complete: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		err = queryContextError(queryCtx, err)
		if writeErr := c.writeError(err); writeErr != nil {
			return writeErr
		}
		return c.flush()
	}

	return c.flush()
}

// handleDescribe handles a 'D' (Describe) message - extended query protocol.
// Describes either a prepared statement ('S') or a portal ('P').
func (c *Conn) handleDescribe() error {
	c.startWriterBuffering()
	defer c.endWriterBuffering()

	// Read message length.
	msgLen, err := c.ReadMessageLength()
	if err != nil {
		return fmt.Errorf("failed to read Describe message length: %w", err)
	}

	// Read describe type (1 byte): 'S' for statement, 'P' for portal.
	typ, err := c.bufferedReader.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read describe type: %w", err)
	}

	// Calculate remaining length for name.
	// msgLen is already the body length (excluding the 4-byte length field).
	nameLen := msgLen - 1 // Subtract only the type byte.

	// Read name (null-terminated string).
	nameBuf := make([]byte, nameLen)
	if _, err := c.bufferedReader.Read(nameBuf); err != nil {
		return fmt.Errorf("failed to read describe name: %w", err)
	}

	// Remove null terminator.
	name := string(nameBuf[:len(nameBuf)-1])

	c.logger.Debug("describe", "type", string(typ), "name", name)

	// Call the handler.
	desc, err := c.handler.HandleDescribe(c.ctx, c, typ, name)
	if err != nil {
		if writeErr := c.writeError(mterrors.MTD06.NewWithDetail(err.Error())); writeErr != nil {
			return writeErr
		}
		return c.flush()
	}

	// Send ParameterDescription only for statement describes ('S').
	// PostgreSQL protocol: Describe('S') returns ParameterDescription + RowDescription/NoData,
	// but Describe('P') returns only RowDescription/NoData — no ParameterDescription.
	if typ == 'S' {
		if err := c.writeParameterDescription(desc.Parameters); err != nil {
			return fmt.Errorf("failed to write parameter description: %w", err)
		}
	}

	// Send RowDescription or NoData based on whether we have field info.
	// Use nil check (not len > 0) because zero-column results (e.g., "SELECT FROM foo")
	// have a non-nil empty Fields slice and still require a RowDescription message.
	if desc.Fields != nil {
		if err := c.writeRowDescription(desc.Fields); err != nil {
			return fmt.Errorf("failed to write row description: %w", err)
		}
	} else {
		// Send NoData when there are no fields (e.g., DML statements).
		if err := c.writeMessage(protocol.MsgNoData, nil); err != nil {
			return fmt.Errorf("failed to write NoData: %w", err)
		}
	}

	return c.flush()
}

// handleClose handles a 'C' (Close) message - extended query protocol.
// Closes either a prepared statement ('S') or a portal ('P').
func (c *Conn) handleClose() error {
	c.startWriterBuffering()
	defer c.endWriterBuffering()

	// Read message length.
	msgLen, err := c.ReadMessageLength()
	if err != nil {
		return fmt.Errorf("failed to read Close message length: %w", err)
	}

	// Read close type (1 byte): 'S' for statement, 'P' for portal.
	typ, err := c.bufferedReader.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read close type: %w", err)
	}

	// Calculate remaining length for name.
	// msgLen is already the body length (excluding the 4-byte length field).
	nameLen := msgLen - 1 // Subtract only the type byte.

	// Read name (null-terminated string).
	nameBuf := make([]byte, nameLen)
	if _, err := c.bufferedReader.Read(nameBuf); err != nil {
		return fmt.Errorf("failed to read close name: %w", err)
	}

	// Remove null terminator.
	name := string(nameBuf[:len(nameBuf)-1])

	c.logger.Debug("close", "type", string(typ), "name", name)

	// Call the handler.
	if err := c.handler.HandleClose(c.ctx, c, typ, name); err != nil {
		if writeErr := c.writeError(mterrors.MTD07.NewWithDetail(err.Error())); writeErr != nil {
			return writeErr
		}
		return c.flush()
	}

	// Send CloseComplete.
	if err := c.writeMessage(protocol.MsgCloseComplete, nil); err != nil {
		return fmt.Errorf("failed to write CloseComplete: %w", err)
	}

	return c.flush()
}

// handleSync handles an 'S' (Sync) message - extended query protocol.
// Sync indicates the end of an extended query cycle and transaction boundary.
// Always sends ReadyForQuery in response.
func (c *Conn) handleSync() error {
	c.startWriterBuffering()
	defer c.endWriterBuffering()

	// Read (and discard) message length.
	if _, err := c.ReadMessageLength(); err != nil {
		return fmt.Errorf("failed to read Sync message length: %w", err)
	}

	c.logger.Debug("sync")

	// Call the handler.
	if err := c.handler.HandleSync(c.ctx, c); err != nil {
		// Even if handler returns error, we still send ReadyForQuery after Sync.
		if writeErr := c.writeError(mterrors.MTD08.NewWithDetail(err.Error())); writeErr != nil {
			return writeErr
		}
	}

	// Always send ReadyForQuery after Sync.
	if err := c.writeReadyForQuery(); err != nil {
		return fmt.Errorf("failed to write ReadyForQuery: %w", err)
	}

	return c.flush()
}

// notifPusher holds state for async notification delivery.
type notifPusher struct {
	ch     chan *sqltypes.Notification
	cancel context.CancelFunc
}

// EnableAsyncNotifications starts a background goroutine that delivers
// notifications from notifCh to the client socket. Must be called at most once.
// Returns a channel that the caller should send notifications to.
func (c *Conn) EnableAsyncNotifications(ctx context.Context) chan<- *sqltypes.Notification {
	ch := make(chan *sqltypes.Notification, 256)
	ctx, cancel := context.WithCancel(ctx)
	c.notifPush = &notifPusher{ch: ch, cancel: cancel}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notif, ok := <-ch:
				if !ok {
					return
				}
				c.startWriterBuffering()
				c.bufMu.Lock()
				if c.bufferedWriter == nil {
					c.bufMu.Unlock()
					return
				}
				err := c.writeNotificationResponseMsg(notif.PID, notif.Channel, notif.Payload)
				if err == nil {
					err = c.bufferedWriter.Flush()
				}
				c.bufMu.Unlock()
				if err != nil {
					c.logger.Error("failed to push notification", "error", err)
					return
				}
			}
		}
	}()

	return ch
}

// StopAsyncNotifications stops the background notification pusher.
func (c *Conn) StopAsyncNotifications() {
	if c.notifPush != nil {
		c.notifPush.cancel()
		c.notifPush = nil
	}
}

// FlushPendingNotifications drains all pending notifications from the async pusher
// channel and writes them to the client socket. This is called synchronously after
// each query completes (before ReadyForQuery) to deliver any notifications that
// arrived during query execution.
//
// This is the safe synchronous path: it holds bufMu while writing, preventing
// races with the async pusher goroutine which also writes with bufMu held.
func (c *Conn) FlushPendingNotifications() error {
	if c.notifPush == nil {
		return nil
	}
	c.startWriterBuffering()
	c.bufMu.Lock()
	defer c.bufMu.Unlock()
	if c.bufferedWriter == nil {
		return nil
	}
	for {
		select {
		case notif, ok := <-c.notifPush.ch:
			if !ok || notif == nil {
				return c.bufferedWriter.Flush()
			}
			if err := c.writeNotificationResponseMsg(notif.PID, notif.Channel, notif.Payload); err != nil {
				return err
			}
		default:
			return c.bufferedWriter.Flush()
		}
	}
}

// writeNotificationResponseMsg writes a NotificationResponse without locking bufMu.
// Caller must hold bufMu.
func (c *Conn) writeNotificationResponseMsg(pid int32, channel, payload string) error {
	// 'A' (NotificationResponse): int32 pid, string channel, string payload
	msgLen := 4 + 4 + len(channel) + 1 + len(payload) + 1
	if err := c.bufferedWriter.WriteByte(protocol.MsgNotificationResponse); err != nil {
		return err
	}
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(msgLen))
	if _, err := c.bufferedWriter.Write(buf); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(buf, uint32(pid))
	if _, err := c.bufferedWriter.Write(buf); err != nil {
		return err
	}
	if _, err := c.bufferedWriter.WriteString(channel); err != nil {
		return err
	}
	if err := c.bufferedWriter.WriteByte(0); err != nil {
		return err
	}
	if _, err := c.bufferedWriter.WriteString(payload); err != nil {
		return err
	}
	return c.bufferedWriter.WriteByte(0)
}
