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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/multigres/multigres/go/common/pgprotocol/bufpool"
	"github.com/multigres/multigres/go/common/pgprotocol/pid"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
)

// Listener listens for incoming PostgreSQL client connections.
type Listener struct {
	// listener is the network listener.
	listener net.Listener

	// handler processes queries for connections.
	handler Handler

	// cancelHandler handles cancel requests, potentially routing to remote gateways.
	// When nil, cancel requests are handled locally only.
	cancelHandler CancelHandler

	// hashProvider provides password hashes for SCRAM authentication.
	hashProvider scram.PasswordHashProvider

	// trustAuthProvider enables trust authentication for testing.
	// When set and AllowTrustAuth() returns true, password auth is skipped.
	trustAuthProvider TrustAuthProvider

	// tlsConfig holds the TLS configuration for SSL connections.
	// When set, the server accepts SSLRequest and upgrades to TLS.
	// When nil, SSLRequest is declined with 'N'.
	tlsConfig *tls.Config

	// certAuthMode controls client-certificate authentication for connections
	// accepted by this listener. See docs/query_serving/client_cert_auth_design.md.
	certAuthMode CertAuthMode

	// certIdentitySource selects which cert field (CN or DN) is compared
	// against the requested DB user under verify-full cert auth.
	certIdentitySource CertIdentitySource

	// logger for logging.
	logger *slog.Logger

	// readersPool pools bufio.Reader objects.
	readersPool *sync.Pool

	// writersPool pools bufio.Writer objects.
	writersPool *sync.Pool

	// bufPool pools byte buffers for packet I/O.
	bufPool *bufpool.Pool

	// gatewayID is the PID prefix from topology, encoded into the upper bits
	// of connection PIDs for cross-gateway cancel request routing.
	gatewayID uint32

	// nextConnectionID is the counter for assigning local connection IDs.
	// Protected by connsMu.
	nextConnectionID uint32

	// connsMu protects conns and nextConnectionID.
	connsMu sync.Mutex
	// conns maps encoded PID to active connections for cancel request lookup.
	conns map[uint32]*Conn

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

	// GatewayID is the PID prefix assigned from topology for this gateway.
	// Encoded into the upper bits of connection PIDs for cross-gateway cancel routing.
	// When 0, connection IDs are used as-is (backward compatible).
	GatewayID uint32

	// HashProvider provides password hashes for SCRAM authentication.
	// Required unless TrustAuthProvider is set.
	HashProvider scram.PasswordHashProvider

	// TrustAuthProvider enables trust authentication for testing.
	// When set, connections that pass AllowTrustAuth() skip password auth.
	// This is intended for testing to simulate Unix socket trust auth.
	// Production code should NOT set this field.
	TrustAuthProvider TrustAuthProvider

	// TLSConfig enables SSL for client connections.
	// When set, the listener accepts SSLRequest and upgrades connections to TLS.
	// When nil, SSLRequest is declined with 'N' (plaintext only).
	TLSConfig *tls.Config

	// ClientCertAuthMode controls PostgreSQL-compatible client certificate
	// authentication. When CertAuthModeVerifyFull, TLSConfig MUST be set and
	// MUST require a verified peer cert (ClientAuth=RequireAndVerifyClientCert).
	// The empty string is treated as CertAuthModeNone.
	ClientCertAuthMode CertAuthMode

	// ClientCertIdentitySource selects which cert field is compared to the
	// requested DB user under verify-full cert auth. The empty string is
	// treated as CertIdentityCN (matching PostgreSQL's default).
	ClientCertIdentitySource CertIdentitySource

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
		return nil, errors.New("hash provider is required (or TrustAuthProvider for testing)")
	}

	// verify-full cert auth requires a TLS config with client-cert verification
	// enabled at the handshake layer, so the auth code can assume a verified
	// peer cert is present.
	if config.ClientCertAuthMode == CertAuthModeVerifyFull {
		if config.TLSConfig == nil {
			return nil, errors.New("client cert auth mode verify-full requires TLSConfig")
		}
		if config.TLSConfig.ClientAuth != tls.RequireAndVerifyClientCert {
			return nil, errors.New("client cert auth mode verify-full requires TLSConfig.ClientAuth = RequireAndVerifyClientCert")
		}
	}

	// Default identity source is CN (matches PostgreSQL's clientcertname default).
	identitySource := config.ClientCertIdentitySource
	if identitySource == "" {
		identitySource = CertIdentityCN
	}
	authMode := config.ClientCertAuthMode
	if authMode == "" {
		authMode = CertAuthModeNone
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
		listener:           netListener,
		handler:            config.Handler,
		hashProvider:       config.HashProvider,
		trustAuthProvider:  config.TrustAuthProvider,
		tlsConfig:          config.TLSConfig,
		certAuthMode:       authMode,
		certIdentitySource: identitySource,
		logger:             logger,
		gatewayID:          config.GatewayID,
		conns:              make(map[uint32]*Conn),
		ctx:                ctx,
		cancel:             cancel,
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

		// Assign a unique connection PID, skipping IDs already in use.
		connID, ok := l.assignConnectionID()
		if !ok {
			l.logger.Error("no available connection ID, rejecting connection",
				"remote_addr", netConn.RemoteAddr())
			netConn.Close()
			continue
		}
		conn := newConn(netConn, l, connID)
		conn.handler = l.handler
		conn.hashProvider = l.hashProvider
		conn.trustAuthProvider = l.trustAuthProvider
		conn.tlsConfig = l.tlsConfig
		conn.certAuthMode = l.certAuthMode
		conn.certIdentitySource = l.certIdentitySource

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

		// Unregister from cancel lookup before cleaning up.
		l.UnregisterConn(conn.ConnectionID())

		// Notify the handler so it can clean up connection-specific state
		// (e.g., rollback active transactions, release reserved connections).
		// This must run before conn.Close() which clears the connection state.
		if conn.handler != nil {
			conn.handler.ConnectionClosed(conn)
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

// CloseListener closes only the TCP listener and stops accepting new connections.
// Existing connections are not affected. This is useful for testing reconnection
// failure scenarios where initial connections should keep working but new dial
// attempts should fail.
func (l *Listener) CloseListener() error {
	l.cancel()
	return l.listener.Close()
}

// Close closes the listener and waits for all connections to finish.
func (l *Listener) Close() error {
	l.cancel()
	err := l.listener.Close()
	l.wg.Wait()
	l.logger.Info("PostgreSQL listener stopped")
	return err
}

// assignConnectionID returns the next unused PID for a new connection.
// It encodes the gateway prefix into the upper bits and skips PIDs that
// are already in use or have a zero local ID (PID 0 is reserved in PostgreSQL).
func (l *Listener) assignConnectionID() (uint32, bool) {
	l.connsMu.Lock()
	defer l.connsMu.Unlock()

	for range pid.MaxLocalConnID {
		localID := l.nextLocalID()
		encodedPID := pid.EncodePID(l.gatewayID, localID)

		if _, exists := l.conns[encodedPID]; !exists {
			return encodedPID, true
		}
	}
	return 0, false
}

// nextLocalID increments the connection counter, wrapping back to 1
// when it reaches MaxLocalConnID. This keeps the counter bounded and avoids
// producing localID=0 (which is reserved in PostgreSQL).
// Must be called with connsMu held.
func (l *Listener) nextLocalID() uint32 {
	l.nextConnectionID++
	if l.nextConnectionID > pid.MaxLocalConnID {
		l.nextConnectionID = 1
	}
	return l.nextConnectionID
}

// SetCancelHandler sets the handler for cancel requests.
// Must be called before Serve().
func (l *Listener) SetCancelHandler(ch CancelHandler) {
	l.cancelHandler = ch
}

// RegisterConn registers a connection in the conn map for cancel request lookup.
func (l *Listener) RegisterConn(conn *Conn) {
	l.connsMu.Lock()
	defer l.connsMu.Unlock()
	l.conns[conn.connectionID] = conn
}

// UnregisterConn removes a connection from the conn map.
func (l *Listener) UnregisterConn(pid uint32) {
	l.connsMu.Lock()
	defer l.connsMu.Unlock()
	delete(l.conns, pid)
}

// CancelLocalConnection looks up a connection by PID, verifies the secret key,
// and cancels its in-flight query. Returns true if the cancel was successful.
func (l *Listener) CancelLocalConnection(pid, secret uint32) bool {
	l.connsMu.Lock()
	conn, ok := l.conns[pid]
	l.connsMu.Unlock()

	if !ok {
		return false
	}
	if conn.BackendKeyData() != secret {
		l.logger.Warn("cancel request secret mismatch", "pid", pid)
		return false
	}
	return conn.CancelQuery()
}

// ConnectionCount returns the number of active client connections.
func (l *Listener) ConnectionCount() int {
	l.connsMu.Lock()
	defer l.connsMu.Unlock()
	return len(l.conns)
}

// Addr returns the listener's network address.
func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}
