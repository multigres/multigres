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
package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/pgprotocol/protocol"
)

const (
	// connBufferSize is the size of read and write buffers.
	connBufferSize = 16 * 1024
)

// Config holds the configuration for connecting to a PostgreSQL server.
type Config struct {
	// Host is the server hostname or IP address.
	Host string

	// Port is the server port number.
	Port int

	// User is the PostgreSQL user name.
	User string

	// Password is the user's password (optional for trust auth).
	Password string

	// Database is the database name to connect to.
	Database string

	// Parameters are additional connection parameters.
	Parameters map[string]string

	// TLSConfig is the TLS configuration for SSL connections.
	// If nil, SSL is not used.
	TLSConfig *tls.Config

	// DialTimeout is the timeout for establishing the TCP connection.
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
func Connect(ctx context.Context, config *Config) (*Conn, error) {
	// Connect to the server.
	dialer := &net.Dialer{
		Timeout: config.DialTimeout,
	}
	address := fmt.Sprintf("%s:%d", config.Host, config.Port)
	netConn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
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
