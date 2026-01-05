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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/multigres/multigres/go/common/pgprotocol/bufpool"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
)

// Listener listens for incoming PostgreSQL client connections.
type Listener struct {
	// listener is the network listener.
	listener net.Listener

	// handler processes queries for connections.
	handler Handler

	// hashProvider provides password hashes for SCRAM authentication.
	hashProvider scram.PasswordHashProvider

	// trustAuthProvider enables trust authentication for testing.
	// When set and AllowTrustAuth() returns true, password auth is skipped.
	trustAuthProvider TrustAuthProvider

	// logger for logging.
	logger *slog.Logger

	// readersPool pools bufio.Reader objects.
	readersPool *sync.Pool

	// writersPool pools bufio.Writer objects.
	writersPool *sync.Pool

	// bufPool pools byte buffers for packet I/O.
	bufPool *bufpool.Pool

	// nextConnectionID is an atomic counter for assigning connection IDs.
	nextConnectionID atomic.Uint32

	// wg tracks active connection handlers.
	wg sync.WaitGroup

	// ctx is the context for the listener, cancelled when Close is called.
	ctx    context.Context
	cancel context.CancelFunc
}

// TrustAuthProvider determines whether trust authentication is allowed.
// When this interface is provided, connections can skip password authentication.
// This is intended for testing scenarios to simulate Unix socket trust auth.
//
// WARNING: This should only be used in tests. Production code should not
// provide a TrustAuthProvider, ensuring all connections use SCRAM authentication.
type TrustAuthProvider interface {
	// AllowTrustAuth returns true if the given user/database connection
	// should be allowed with trust authentication (no password).
	AllowTrustAuth(ctx context.Context, user, database string) bool
}

// ListenerConfig holds configuration for the listener.
type ListenerConfig struct {
	// Address to listen on (e.g., "localhost:5432").
	Address string

	// Handler processes queries.
	Handler Handler

	// HashProvider provides password hashes for SCRAM authentication.
	// Required unless TrustAuthProvider is set.
	HashProvider scram.PasswordHashProvider

	// TrustAuthProvider enables trust authentication for testing.
	// When set, connections that pass AllowTrustAuth() skip password auth.
	// This is intended for testing to simulate Unix socket trust auth.
	// Production code should NOT set this field.
	TrustAuthProvider TrustAuthProvider

	// Logger for logging (optional, defaults to slog.Default()).
	Logger *slog.Logger
}

// NewListener creates a new PostgreSQL protocol listener.
func NewListener(config ListenerConfig) (*Listener, error) {
	if config.Handler == nil {
		return nil, errors.New("handler is required")
	}

	// HashProvider is required unless TrustAuthProvider is set
	if config.HashProvider == nil && config.TrustAuthProvider == nil {
		return nil, fmt.Errorf("hash provider is required (or TrustAuthProvider for testing)")
	}

	netListener, err := net.Listen("tcp", config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", config.Address, err)
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.TODO())

	l := &Listener{
		listener:          netListener,
		handler:           config.Handler,
		hashProvider:      config.HashProvider,
		trustAuthProvider: config.TrustAuthProvider,
		logger:            logger,
		ctx:               ctx,
		cancel:            cancel,
	}

	// Initialize buffer pools.
	l.readersPool = &sync.Pool{
		New: func() any {
			return bufio.NewReaderSize(nil, connBufferSize)
		},
	}
	l.writersPool = &sync.Pool{
		New: func() any {
			return bufio.NewWriterSize(nil, connBufferSize)
		},
	}
	l.bufPool = bufpool.New(16*1024, 64*1024*1024) // 16 KB to 64 MB

	logger.Info("PostgreSQL listener started", "address", config.Address)

	return l, nil
}

// Serve accepts and handles incoming connections.
// This method blocks until the listener is closed or an error occurs.
func (l *Listener) Serve() error {
	for {
		netConn, err := l.listener.Accept()
		if err != nil {
			select {
			case <-l.ctx.Done():
				// Listener was closed.
				return nil
			default:
				l.logger.Error("failed to accept connection", "error", err)
				continue
			}
		}

		// Assign connection ID and create connection.
		connID := l.nextConnectionID.Add(1)
		conn := newConn(netConn, l, connID)
		conn.handler = l.handler
		conn.hashProvider = l.hashProvider
		conn.trustAuthProvider = l.trustAuthProvider

		// Handle connection in a new goroutine.
		l.wg.Go(func() {
			l.handleConnection(conn)
		})
	}
}

// handleConnection handles a single client connection.
func (l *Listener) handleConnection(conn *Conn) {
	// Catch panics and ensure cleanup happens in all cases.
	defer func() {
		if x := recover(); x != nil {
			conn.logger.Error("panic in connection handler",
				"panic", x,
				"remote_addr", conn.RemoteAddr())
		}

		// Clean up connection resources.
		if err := conn.Close(); err != nil {
			conn.logger.Error("error closing connection", "error", err)
		}
	}()

	conn.logger.Info("connection accepted", "remote_addr", conn.RemoteAddr())

	// Serve the connection (startup + command loop).
	if err := conn.serve(); err != nil {
		if !errors.Is(err, io.EOF) {
			conn.logger.Error("connection error", "error", err)
		}
	}

	conn.logger.Info("connection closed")
}

// Close closes the listener and waits for all connections to finish.
func (l *Listener) Close() error {
	l.cancel()
	err := l.listener.Close()
	l.wg.Wait()
	l.logger.Info("PostgreSQL listener stopped")
	return err
}

// Addr returns the listener's network address.
func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}
