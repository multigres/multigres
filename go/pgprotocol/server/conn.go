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
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/protocol"
)

const (
	// connBufferSize is the size of read and write buffers.
	connBufferSize = 16 * 1024

	// defaultFlushDelay is the default delay before auto-flushing buffered writes.
	defaultFlushDelay = 100 * time.Millisecond
)

// Conn represents the server side connection with a PostgreSQL client.
// It handles the wire protocol encoding/decoding and connection state management.
type Conn struct {
	// conn is the underlying network connection.
	conn net.Conn

	// bufferedReader is used for reading from the connection.
	bufferedReader *bufio.Reader

	// bufferedWriter is used for writing to the connection.
	bufferedWriter *bufio.Writer

	// bufMu protects bufferedReader and bufferedWriter.
	bufMu sync.Mutex

	// listener is a reference to the listener that accepted this connection.
	listener *Listener

	// handler processes queries for this connection.
	handler Handler

	// logger for connection-specific logging.
	logger *slog.Logger

	// connectionID is a unique identifier for this connection.
	connectionID uint32

	// backendKeyData is the secret key for this backend, used for cancellation.
	backendKeyData uint32

	// Startup parameters sent by the client.
	user     string
	database string
	params   map[string]string

	// protocolVersion is the negotiated protocol version.
	protocolVersion protocol.ProtocolVersion

	// Current transaction state.
	txnStatus byte

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
	ctx, cancel := context.WithCancel(context.Background())

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

	// Return pooled resources.
	c.returnReader()
	// End writer buffering (flushes and returns to pool).
	c.endWriterBuffering()

	return c.conn.Close()
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

// User returns the authenticated user.
func (c *Conn) User() string {
	return c.user
}

// Database returns the database name.
func (c *Conn) Database() string {
	return c.database
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
	// First, handle the startup phase.
	if err := c.handleStartup(); err != nil {
		c.logger.Error("startup failed", "error", err)
		// Try to send an error response before closing.
		_ = c.writeErrorResponse("FATAL", "08P01", "connection startup failed", err.Error(), "")
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
		msgType, err := c.readMessageType()
		if err != nil {
			// EOF or connection error - close gracefully.
			if err == io.EOF {
				c.logger.Debug("client closed connection")
				return nil
			}
			c.logger.Error("error reading message type", "error", err)
			return err
		}

		// Process the message based on type.
		if err := c.handleMessage(msgType); err != nil {
			c.logger.Error("error handling message", "type", string(msgType), "error", err)
			// Send error response and continue (unless it's a fatal error).
			_ = c.writeErrorResponse("ERROR", "XX000", "internal error", err.Error(), "")
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

	// Handle empty query.
	queryStr = strings.TrimSpace(queryStr)
	if queryStr == "" {
		if err := c.writeEmptyQueryResponse(); err != nil {
			return err
		}
		if err := c.writeReadyForQuery(); err != nil {
			return err
		}
		return c.flush()
	}

	// Track state for current result set.
	// This is reset when we complete a result set (when CommandTag is set).
	sentRowDescription := false

	// Execute the query via the handler with streaming callback.
	// The callback will be invoked multiple times for:
	// 1. Large result sets (streamed in chunks)
	// 2. Multiple statements in a single query (each potentially with large result sets)
	err = c.handler.HandleQuery(c, queryStr, func(result *query.QueryResult) error {
		// On first callback with fields for this result set, send RowDescription.
		if !sentRowDescription && len(result.Fields) > 0 {
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
		// Send CommandComplete and reset state for the next result set.
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
		// Send error response.
		c.logger.Error("query execution failed", "query", queryStr, "error", err)
		if err := c.writeErrorResponse("ERROR", "42000", "query execution failed", err.Error(), ""); err != nil {
			return err
		}
	}

	// Send ReadyForQuery after all statements have been processed.
	if err := c.writeReadyForQuery(); err != nil {
		return err
	}

	return c.flush()
}

// handleParse handles a 'P' (Parse) message - extended query protocol.
func (c *Conn) handleParse() error {
	// TODO(GuptaManan100): Implement extended query protocol Parse.
	return fmt.Errorf("extended query protocol not yet implemented")
}

// handleBind handles a 'B' (Bind) message - extended query protocol.
func (c *Conn) handleBind() error {
	// TODO(GuptaManan100): Implement extended query protocol Bind.
	return fmt.Errorf("extended query protocol not yet implemented")
}

// handleExecute handles an 'E' (Execute) message - extended query protocol.
func (c *Conn) handleExecute() error {
	// TODO(GuptaManan100): Implement extended query protocol Execute.
	return fmt.Errorf("extended query protocol not yet implemented")
}

// handleDescribe handles a 'D' (Describe) message - extended query protocol.
func (c *Conn) handleDescribe() error {
	// TODO(GuptaManan100): Implement extended query protocol Describe.
	return fmt.Errorf("extended query protocol not yet implemented")
}

// handleClose handles a 'C' (Close) message - extended query protocol.
func (c *Conn) handleClose() error {
	// TODO(GuptaManan100): Implement extended query protocol Close.
	return fmt.Errorf("extended query protocol not yet implemented")
}

// handleSync handles an 'S' (Sync) message - extended query protocol.
func (c *Conn) handleSync() error {
	// TODO(GuptaManan100): Implement extended query protocol Sync.
	return fmt.Errorf("extended query protocol not yet implemented")
}
