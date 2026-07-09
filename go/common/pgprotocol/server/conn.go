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
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
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

// errAuthenticationTimeout is the sentinel returned by serve() when the
// startup-phase deadline (authentication_timeout) fires. handleConnection
// matches on this specifically rather than the generic
// os.ErrDeadlineExceeded so that deadlines added in the future for other
// reasons (idle timeout, per-query socket deadline, etc.) are not silently
// suppressed by the same log-noise filter.
var errAuthenticationTimeout = errors.New("authentication timeout")

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

	// inboundPoolBuf is the read-path equivalent of outboundPoolBuf:
	// holds the listener-level bufpool buffer that readMessageBody
	// grabbed for the most recent message body. Non-nil between
	// readMessageBody and returnReadBuffer. Same escape-avoidance
	// rationale — the parameterless returnReadBuffer reads this field
	// instead of taking &buf, so callers don't pay a 24-byte slice-
	// header heap alloc per message.
	//
	// Reads on a Conn are sequential (one in flight at a time), so a
	// single field is enough.
	inboundPoolBuf *[]byte

	// listener is a reference to the listener that accepted this connection.
	listener *Listener

	// handler processes queries for this connection.
	handler Handler

	// credentialProvider supplies the SCRAM hash and rolreplication flag
	// for the authenticating role. A single lookup feeds both SCRAM and
	// the post-auth replication-role gate; the result is cached on
	// credentials below so the gate doesn't have to round-trip again.
	credentialProvider CredentialProvider

	// credentials is the result of a successful credentialProvider lookup,
	// populated before SCRAM starts. The replication-role gate reads
	// IsReplicationRole from here so neither path has to round-trip a
	// second time.
	credentials *Credentials

	// trustAuthProvider enables trust authentication for testing.
	// When set and AllowTrustAuth() returns true, password auth is skipped.
	trustAuthProvider TrustAuthProvider

	// tlsConfig holds the TLS configuration for SSL connections.
	// When set, the server accepts SSLRequest and upgrades to TLS.
	// When nil, SSLRequest is declined with 'N'.
	tlsConfig *tls.Config

	// requireTLS rejects a plaintext StartupMessage. Copied from the
	// listener at accept time. Cancel requests bypass this check.
	requireTLS bool

	// authMetrics receives auth- and TLS-path metric events for this
	// connection. Never nil — a noop is substituted at listener
	// construction when the caller did not supply one — so startup-phase
	// code can call methods unconditionally.
	authMetrics AuthMetricsRecorder

	// sslDone indicates that an SSLRequest has already been handled
	// (accepted or declined) for this connection. Prevents double negotiation.
	sslDone bool

	// tlsHandshakeComplete is set true once handleSSLRequest has accepted
	// SSL ('S') AND the TLS handshake has finished — i.e., c.conn has been
	// reassigned to the *tls.Conn. Used during the auth-timeout error path
	// to decide whether plaintext writes are still intelligible to the
	// client. Tracking this explicitly (rather than type-asserting c.conn)
	// keeps the check robust if c.conn is ever wrapped in instrumentation.
	tlsHandshakeComplete bool

	// tlsServerCert is the parsed leaf certificate offered to the client
	// during the TLS handshake. Captured once at handshake completion and
	// used to compute the tls-server-end-point channel binding hash for
	// SCRAM-SHA-256-PLUS. Nil for plaintext connections.
	tlsServerCert *x509.Certificate

	// directTLSFailed is set when a direct-TLS attempt (first byte 0x16,
	// PG 17 sslnegotiation=direct) was detected but could not be completed
	// — either TLS is not configured or the handshake itself failed. The
	// client is speaking raw TLS on the wire in both cases, so serve()'s
	// best-effort plaintext ErrorResponse would surface as garbage;
	// canSendPlaintextStartupError consults this flag to suppress it,
	// matching PostgreSQL's silent rejection of failed direct SSL startups.
	directTLSFailed bool

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

	// replicationMode reflects the parsed `replication` startup parameter.
	// Default ReplicationOff means a normal SQL session. ReplicationPhysical
	// or ReplicationLogical require pg_authid.rolreplication=true on the
	// authenticated role; the post-auth verifier enforces that.
	replicationMode ReplicationMode

	// SCRAM-SHA-256 keys extracted during the client handshake, used for
	// passthrough authentication to the backing PostgreSQL. Nil for non-SCRAM
	// sessions. Zeroized in Close.
	scramClientKey []byte
	scramServerKey []byte

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

	// deferredPortalDescribe captures Describe('P') messages so an immediately
	// following Execute on the same portal can fold both into one backend
	// call via HandleExecute(includeDescribe=true). When the deferred state
	// is held and a non-Execute message arrives, resolveDeferredPortalDescribe
	// flushes it through HandleDescribe so the wire stays well-formed.
	//
	// Set by handleDescribe('P'); cleared by handleExecute on a name match,
	// by resolveDeferredPortalDescribe otherwise. Per-Conn state is fine —
	// the protocol guarantees one in-flight request per connection.
	deferredPortalDescribe     bool
	deferredPortalDescribeName string

	// discardingUntilSync is the extended-query error-recovery flag. Set
	// when an ErrorResponse is emitted from any extended-query handler
	// (Parse, Bind, Describe, Execute, Close); cleared only when
	// handleMessage observes Sync. While set, every other inbound message
	// — Parse/Bind/Describe/Execute/Close, a pipelined simple Query, or
	// anything else — is read off the wire and silently discarded — no
	// handler runs, no reply frame is emitted — matching PostgreSQL's
	// "reads and discards messages until a Sync message is reached" rule.
	// Without this gate, a CloseComplete (or ParseComplete, BindComplete,
	// etc.) emitted after ErrorResponse in the same pipelined batch
	// crashes strict drivers like Postgrex.
	discardingUntilSync bool

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

	// Zeroize SCRAM passthrough keys so a post-mortem or memory dump cannot
	// recover credentials for this session after close.
	for i := range c.scramClientKey {
		c.scramClientKey[i] = 0
	}
	for i := range c.scramServerKey {
		c.scramServerKey[i] = 0
	}
	c.scramClientKey = nil
	c.scramServerKey = nil

	// Return pooled resources.
	c.returnReader()
	// Defensive cleanup: if a handler panicked between readMessageBody
	// and its returnReadBuffer call, the inbound pool buffer is still
	// stashed on the Conn — release it now so the pool can recycle it.
	c.returnReadBuffer()
	// Same defense for the write side: a panic between startPacket and
	// writePacket leaves outboundPoolBuf set (writePacket's defer never
	// fires) and bufMu held. We can't re-lock here without deadlocking
	// our own goroutine, but Close runs after concurrent access has
	// stopped so an unlocked Put is safe.
	c.returnOutboundBuffer()
	// End writer buffering (flushes and returns to pool). Flush errors
	// during teardown are uninteresting — we're closing the socket
	// next anyway — so swallow them here.
	_ = c.endWriterBuffering()

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

// ReplicationMode returns the parsed `replication` startup parameter for this
// connection. ReplicationOff means the client did not request a replication
// connection (or sent replication=false).
func (c *Conn) ReplicationMode() ReplicationMode {
	return c.replicationMode
}

// ScramClientKey returns the SCRAM-SHA-256 ClientKey extracted during the
// client's authentication handshake, or nil if the session did not
// authenticate via SCRAM. Used for passthrough auth to backend PostgreSQL.
func (c *Conn) ScramClientKey() []byte {
	return c.scramClientKey
}

// ScramServerKey returns the SCRAM-SHA-256 ServerKey from the user's
// verifier, or nil if the session did not authenticate via SCRAM.
func (c *Conn) ScramServerKey() []byte {
	return c.scramServerKey
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

// endWriterBuffering flushes any remaining buffered data and returns
// the writer to the pool. Returns the Flush error (broken-pipe, write
// timeout, etc.) so callers in the serve() loop can propagate it for
// connection-level cleanup; the writer is still detached and pooled
// even when Flush fails, so the connection state is consistent.
func (c *Conn) endWriterBuffering() error {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedWriter == nil {
		return nil
	}

	flushErr := c.bufferedWriter.Flush()

	// Return to pool regardless of flush outcome — the writer is in
	// a defined state (Reset clears its internal cursors), and
	// stranding it on the Conn would leak the pooled buffer.
	c.bufferedWriter.Reset(nil)
	c.listener.writersPool.Put(c.bufferedWriter)
	c.bufferedWriter = nil
	return flushErr
}

// canSendPlaintextStartupError reports whether a best-effort plaintext
// ErrorResponse during the startup phase would be intelligible to the
// client. If the client sent SSLRequest and the server already answered
// 'S' but the TLS handshake hasn't completed, the underlying conn is
// still plaintext while the client is waiting for encrypted bytes — so
// any plaintext write would surface as garbage. Likewise a failed
// direct-TLS attempt (directTLSFailed): the client opened with a TLS
// ClientHello, so only TLS alerts — already sent by crypto/tls where
// applicable — make sense on the wire. Every other startup state (raw
// plaintext or fully upgraded TLS) can carry the reply.
func (c *Conn) canSendPlaintextStartupError() bool {
	if c.directTLSFailed {
		return false
	}
	return !(c.sslDone && c.tlsConfig != nil && !c.tlsHandshakeComplete)
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
//
// Write buffering is managed at this level rather than per-handler. The
// pgwire client pipelines a batch of extended-protocol messages
// (Parse + Bind + Describe + Execute + Sync) and only blocks reading
// after Sync — so coalescing the six reply messages into one syscall
// per batch matches what the client is actually waiting on, and drops
// the syscall.write count by ~5x on sysbench prepared workloads.
//
// The window opens lazily via startWriterBuffering() before each
// handleMessage call (idempotent: no-op if already attached) and is
// released back to the writer pool on flush boundaries — Sync, Flush,
// simple Query, and the error path. That keeps the steady-state for
// pipelined batches at one buffer / one flush, while still returning
// the writer to the pool when the connection genuinely idles between
// batches.
func (c *Conn) serve() error {
	// Bound the startup phase (SSL/GSS negotiation, StartupMessage, SCRAM
	// exchange) — equivalent to PostgreSQL's authentication_timeout. A
	// stalled or malicious client cannot pin this goroutine past the
	// deadline. The deadline is cleared once authentication completes so
	// the main command loop runs without an I/O timeout.
	startupDeadlineSet := false
	if d := c.listener.authenticationTimeout; d > 0 {
		if err := c.conn.SetDeadline(time.Now().Add(d)); err == nil {
			startupDeadlineSet = true
		} else {
			c.logger.Warn("failed to set startup deadline", "error", err)
		}
	}

	// First, handle the startup phase.
	if err := c.handleStartup(); err != nil {
		if errors.Is(err, io.EOF) {
			c.logger.Debug("client disconnected before startup")
			return nil
		}
		// errAuthRejected: a FATAL was already sent during the auth flow;
		// no AuthenticationOk was emitted and the connection was not
		// registered. Skip the command loop entirely so a misbehaving
		// client cannot send messages on a half-completed session, and
		// don't write a second error frame.
		if errors.Is(err, errAuthRejected) {
			c.logger.Debug("client rejected during startup; FATAL already sent")
			return nil
		}
		// Map a deadline-exceeded I/O error during startup to a clean
		// PG-format FATAL with SQLSTATE 08006 so libpq surfaces it as
		// "canceling authentication due to timeout" instead of a raw
		// I/O timeout. The startup deadline is still active here — any
		// write would inherit the expired deadline and fail — so clear
		// it first using a brief grace window for the error reply.
		if startupDeadlineSet && errors.Is(err, os.ErrDeadlineExceeded) {
			c.logger.Warn("startup phase timed out",
				"timeout", c.listener.authenticationTimeout,
				"remote_addr", c.RemoteAddr())
			if c.canSendPlaintextStartupError() {
				// Brief grace window: long enough to flush the error
				// to a well-behaved client, short enough that a wedged
				// client can't keep this goroutine pinned.
				_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				_ = c.writePgDiagnosticResponse(protocol.MsgErrorResponse, mterrors.NewAuthenticationTimeout())
				_ = c.flush()
			}
			// Return the sentinel so handleConnection can suppress its
			// redundant Error log without also swallowing unrelated
			// deadline-exceeded errors that future code paths may surface.
			return errAuthenticationTimeout
		}
		c.logger.Error("startup failed", "error", err)
		// Set a brief write-deadline grace window so the best-effort
		// error reply doesn't race against the still-active auth
		// deadline (which may fire mid-write on a slow client).
		if startupDeadlineSet {
			_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		}
		// Try to send an error response before closing — but only if
		// it would be intelligible. Same SSL-accepted-but-handshake-
		// pending guard as the timeout path above: a plaintext
		// ErrorResponse on a connection where the client expects
		// encrypted bytes would surface as garbage.
		if c.canSendPlaintextStartupError() {
			// If the error is already a PgDiagnostic (e.g., duplicate
			// SSLRequest with native SQLSTATE), send it directly.
			// Otherwise, wrap with MTE01.
			var diag *mterrors.PgDiagnostic
			if errors.As(err, &diag) {
				_ = c.writePgDiagnosticResponse(protocol.MsgErrorResponse, diag)
			} else {
				_ = c.writeError(mterrors.MTE01.NewWithDetail(err.Error()))
			}
			_ = c.flush()
		}
		return err
	}

	// Cancel requests close the connection inside handleStartup and
	// return nil — there is no authenticated session to enter the
	// command loop for, and SetDeadline on the closed fd would fail
	// and surface as a spurious Error log. Bail out cleanly here.
	if c.closed.Load() {
		return nil
	}

	// Authentication complete — clear the startup deadline before the
	// command loop. Subsequent reads must not inherit the auth-phase
	// deadline (idle clients are normal, not a protocol violation). If
	// the clear fails (rare — typically only when the fd is already
	// closed), abort instead of entering the loop with a stale deadline
	// that would trip the very next read.
	if startupDeadlineSet {
		if err := c.conn.SetDeadline(time.Time{}); err != nil {
			c.logger.Error("failed to clear startup deadline; tearing down connection",
				"error", err)
			return fmt.Errorf("clear startup deadline: %w", err)
		}
	}

	// Main command loop. Startup already sent ReadyForQuery, so the first
	// frontend message is the start of a new command cycle. For extended query
	// protocol, only Sync completes that cycle and returns to ReadyForQuery;
	// Parse/Bind/Execute/Flush/etc. remain within the same cycle.
	waitingForCommand := true
	for {
		// Check if connection is closed.
		if c.closed.Load() {
			return nil
		}

		// Read the message type (1 byte). If the handler manages
		// idle_session_timeout, arm a read deadline only while waiting for the
		// first client message of a new command cycle in an idle transaction state.
		// Clear it as soon as a byte arrives so message-body reads and query
		// execution are not accidentally governed by the idle-session clock.
		idleDeadlineArmed, err := c.armIdleSessionTimeout(waitingForCommand)
		if err != nil {
			c.logger.Error("failed to arm idle_session_timeout", "error", err)
			return err
		}
		msgType, err := c.ReadMessageType()
		if err == nil && idleDeadlineArmed {
			if clearErr := c.conn.SetReadDeadline(time.Time{}); clearErr != nil {
				c.logger.Error("failed to clear idle_session_timeout read deadline", "error", clearErr)
				return clearErr
			}
		}
		if err != nil {
			if idleDeadlineArmed && isNetTimeout(err) {
				return c.handleIdleSessionTimeout()
			}
			// EOF or connection error - close gracefully.
			if errors.Is(err, io.EOF) {
				c.logger.Debug("client closed connection")
				return nil
			}
			c.logger.Error("error reading message type", "error", err)
			return err
		}
		waitingForCommand = false

		// Open a buffering window if one isn't already attached. Per-
		// handler bodies no longer manage this themselves; they just
		// write packets and let the loop decide when to flush.
		c.startWriterBuffering()

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
			// Best-effort flush of the error reply. Already returning
			// the original handler error, so swallow flush errors here.
			_ = c.endWriterBuffering()
			// For now, close connection on any error.
			return err
		}

		// Flush at end-of-batch boundaries. For pipelined messages
		// (Parse / Bind / Describe / Execute / Close) the buffer
		// stays attached so the next inbound message in the same
		// batch lands in the same flush. Flush errors here mean the
		// socket is broken — propagate so serve() can tear down the
		// connection instead of looping until the next ReadMessageType
		// fails (which would lose the original write-error context).
		// MsgSync and MsgQuery are normally end-of-batch boundaries: flush
		// and release the writer back to the pool so an idle connection
		// doesn't pin a 16 KB writer between batches, and let the next
		// loop iteration arm idle_session_timeout.
		//
		// A MsgQuery pipelined during extended-query error drain is the
		// exception: maybeDispatchDrain discards it like any other message
		// without dispatching or clearing c.discardingUntilSync, so it is
		// NOT a boundary — Sync is still pending. Treating it as one here
		// would let idle_session_timeout arm before the client sends the
		// required Sync, closing the connection out from under a client
		// that is still mid-batch.
		//
		// MsgFlush itself flushes inside handleMessage (and stays
		// buffered, since more pipelined messages typically follow);
		// it doesn't need a release here.
		isBatchBoundary := msgType == protocol.MsgSync ||
			(msgType == protocol.MsgQuery && !c.discardingUntilSync)
		if isBatchBoundary {
			if err := c.endWriterBuffering(); err != nil {
				c.logger.Error("flush at batch boundary failed", "type", string(msgType), "error", err)
				return err
			}
			waitingForCommand = true
		}
	}
}

// armIdleSessionTimeout applies the effective idle_session_timeout as a read
// deadline while the server is waiting for the first client message of a new
// command cycle. PostgreSQL's idle_session_timeout is armed around
// ReadyForQuery/ReadCommand, not between every extended-protocol message, and it
// applies only when the session is idle outside a transaction.
// Returns true when a timeout deadline was armed for the next ReadMessageType.
func (c *Conn) armIdleSessionTimeout(waitingForCommand bool) (bool, error) {
	if !waitingForCommand {
		return false, nil
	}
	provider, ok := c.handler.(IdleSessionTimeoutProvider)
	if !ok {
		return false, nil
	}
	if c.txnStatus != protocol.TxnStatusIdle {
		return false, nil
	}

	timeout := provider.IdleSessionTimeout(c)
	if timeout <= 0 {
		return false, nil
	}
	return true, c.conn.SetReadDeadline(time.Now().Add(timeout))
}

func isNetTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// handleIdleSessionTimeout sends the PostgreSQL-native FATAL 57P05 error and
// then lets the connection teardown path close the socket. Write/flush errors
// are not returned: the timeout already decided to terminate the session, and a
// concurrent client disconnect should not surface as a noisy handler error.
func (c *Conn) handleIdleSessionTimeout() error {
	c.logger.Debug("idle_session_timeout expired; closing connection")
	_ = c.conn.SetReadDeadline(time.Time{})
	// Brief grace window: long enough to flush the FATAL to a well-behaved
	// client, short enough that a wedged client can't keep this goroutine
	// pinned after the idle-session timeout has already decided to terminate
	// the session.
	_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	c.startWriterBuffering()
	_ = c.writePgDiagnosticResponse(protocol.MsgErrorResponse, mterrors.NewIdleSessionTimeout())
	_ = c.endWriterBuffering()
	return nil
}

// handleMessage processes a single message from the client.
//
// A Describe('P') is held by handleDescribe in case the very next message is
// Execute on the same portal — that pair is fused into one backend call. Any
// other message (including a follow-up Describe of either type) flushes the
// held state here first so the wire reply order matches what an unfused
// Describe(P)+Execute would produce.
//
// Extended-query error recovery: once an ErrorResponse has been emitted in
// the current batch, c.discardingUntilSync is set and every subsequent
// message — Parse / Bind / Describe / Execute / Close, or a pipelined
// simple Query — is read off the wire and discarded here without
// dispatching to a handler. Sync is the only message that clears the flag
// and proceeds to handleSync (which emits the ReadyForQuery the client is
// waiting on). This matches PostgreSQL's documented behavior and prevents
// the post-error frames (notably CloseComplete) that crash strict drivers.
func (c *Conn) handleMessage(msgType byte) error {
	if handled, err := c.maybeDispatchDrain(msgType); handled {
		return err
	}

	switch msgType {
	case protocol.MsgExecute:
		// handleExecute consumes the deferred describe directly (it folds
		// it into the backend call rather than flushing it separately).
		// handleExecute itself re-checks the drain flag after the
		// fallback resolveDeferredPortalDescribe path.
		return c.handleExecute()

	case protocol.MsgTerminate:
		c.logger.Debug("received termination message")
		return io.EOF // Signal connection should close
	}

	// Every other message must run after any pending portal describe is
	// flushed so the wire stays well-formed.
	if err := c.resolveDeferredPortalDescribe(); err != nil {
		return err
	}
	// resolveDeferredPortalDescribe may have flushed via HandleDescribe
	// and entered drain mode by emitting an ErrorResponse. In that case
	// the inbound message that triggered the flush (Parse/Bind/Close/…)
	// must also be discarded — otherwise its reply frame would land
	// between ErrorResponse and ReadyForQuery and crash strict drivers.
	if handled, err := c.maybeDispatchDrain(msgType); handled {
		return err
	}

	switch msgType {
	case protocol.MsgQuery:
		return c.handleQuery()

	case protocol.MsgParse:
		return c.handleParse()

	case protocol.MsgBind:
		return c.handleBind()

	case protocol.MsgDescribe:
		return c.handleDescribe()

	case protocol.MsgClose:
		return c.handleClose()

	case protocol.MsgSync:
		return c.handleSync()

	case protocol.MsgFlush:
		// Read and discard the message length (Int32, always 4: just
		// the length field itself). The 'H' type byte was already
		// consumed by serve()'s ReadMessageType; if we don't consume
		// the length here, the next ReadMessageType picks up 0x00
		// and treats it as a bogus type.
		if _, err := c.ReadMessageLength(); err != nil {
			return fmt.Errorf("failed to read Flush message length: %w", err)
		}
		// Push any buffered bytes (including a deferred Describe('P')
		// that resolveDeferredPortalDescribe flushed into the buffer
		// just above) out to the client. We do this here rather than
		// in serve()'s post-dispatch so direct callers of
		// handleMessage(MsgFlush) — e.g. the unit tests in
		// extended_query_test.go — see the same client-visible
		// behavior as the production read loop. The buffer stays
		// attached: more pipelined messages typically follow.
		return c.flush()

	case protocol.MsgFunctionCall:
		// Fast-path FunctionCall is not implemented yet, but it is still a
		// normal length-prefixed frontend message. Drain the frame before
		// rejecting it so we do not close the socket while the client is still
		// writing the rest of the packet, which can surface as ECONNRESET/SIGPIPE.
		if err := c.discardMessageBody(); err != nil {
			return fmt.Errorf("failed to discard unsupported FunctionCall message: %w", err)
		}
		return fmt.Errorf("unsupported message type: %c (0x%02x)", msgType, msgType)

	default:
		return fmt.Errorf("unsupported message type: %c (0x%02x)", msgType, msgType)
	}
}

// handleQuery handles a 'Q' (Query) message - simple query protocol.
// Supports multiple statements in a single query (e.g., "SELECT 1; SELECT 2;").
//
// The buffering window is opened by the serve() loop and torn down
// (flushed + writer returned to the pool) after this returns, since
// MsgQuery is treated as a flush boundary. So we just write packets
// here and let the loop handle the syscall.
func (c *Conn) handleQuery() error {
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
			if err := c.flush(); err != nil {
				return fmt.Errorf("flushing notice response: %w", err)
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
	// serve() flushes the buffer once we return.
	return c.writeReadyForQuery()
}

// preserveExtendedQueryError returns err unchanged when it already carries a
// structured PostgreSQL diagnostic, so the client sees the real SQLSTATE the
// handler produced (e.g. 42601 syntax_error from the parser on Parse, 26000
// invalid_sql_statement_name for a missing prepared statement on Bind/Describe,
// 34000 invalid_cursor_name for a missing portal on Describe). Only an opaque
// error with no SQLSTATE is wrapped with fallback, which marks a genuinely
// internal failure of the extended-query step. Drivers branch on these
// SQLSTATEs, so blanket-wrapping a structured error as an internal MTDxx code
// (which clients read as a proxy bug) would be a real protocol divergence from
// PostgreSQL — see the pgproto conformance suite's bind_before_parse and
// describe_unknown corpus files.
func preserveExtendedQueryError(err error, fallback *mterrors.MTError) error {
	var diag *mterrors.PgDiagnostic
	if errors.As(err, &diag) {
		return err
	}
	return fallback.NewWithDetail(err.Error())
}

// handleParse handles a 'P' (Parse) message - extended query protocol.
// Parse message format:
// - Statement name (string, null-terminated)
// - Query string (string, null-terminated)
// - Number of parameter data types (int16)
// - Parameter data type OIDs ([]uint32)
//
// The buffering window is opened by serve(); we leave it attached on
// return so the next pipelined message in the same batch lands in
// the same flush. Sync (or an explicit Flush) is what pushes bytes
// to the client.
func (c *Conn) handleParse() error {
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
	defer c.returnReadBuffer()

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
		// Do NOT send ReadyForQuery here. In the extended query protocol, the client
		// pipelines Parse + Describe + Sync (or Parse + Bind + Execute + Sync).
		// ReadyForQuery must only be sent in response to Sync. Sending it here would
		// cause protocol desynchronization: pgx would read the premature ReadyForQuery
		// and think the pipeline is done, but stale responses from subsequent messages
		// (Describe, Sync) would corrupt the next query's response stream.
		// The error packet stays buffered until Sync flushes the batch — same shape
		// as upstream PostgreSQL, which also defers error delivery to Sync/Flush.
		//
		// Preserve a structured PostgreSQL diagnostic (e.g. 42601 syntax_error
		// from the parser); only opaque errors are wrapped as MTD04, a genuinely
		// internal Parse failure. See preserveExtendedQueryError.
		return c.writeExtendedQueryError(preserveExtendedQueryError(err, mterrors.MTD04))
	}

	// Send ParseComplete message. Stays buffered for the rest of the batch.
	return c.writeMessage(protocol.MsgParseComplete, nil)
}

// handleBind handles a 'B' (Bind) message - extended query protocol.
// The serve() loop owns the buffering window; this handler just
// writes packets and returns.
func (c *Conn) handleBind() error {
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
	defer c.returnReadBuffer()

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
		// Do NOT send ReadyForQuery here — same reasoning as handleParse.
		// ReadyForQuery is sent only in response to Sync. The error packet
		// stays buffered until Sync flushes the batch.
		//
		// Preserve a structured diagnostic (e.g. 26000 for a Bind referencing a
		// prepared statement that was never Parsed); only opaque errors become
		// MTD05. See preserveExtendedQueryError.
		return c.writeExtendedQueryError(preserveExtendedQueryError(err, mterrors.MTD05))
	}

	// Send BindComplete message. Stays buffered for the rest of the batch.
	return c.writeMessage(protocol.MsgBindComplete, nil)
}

// handleExecute handles an 'E' (Execute) message - extended query protocol.
// Execute message format:
// - Portal name (string, null-terminated)
// - Max rows to return (int32, 0 = no limit)
//
// serve() owns the buffering window. The reply (zero-or-more DataRow
// + CommandComplete) stays in the buffer until the trailing Sync
// flushes the batch.
func (c *Conn) handleExecute() error {
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
	defer c.returnReadBuffer()

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

	// If a Describe('P') was deferred for this exact portal, fold it into
	// this Execute call — the handler/executor will fetch RowDescription
	// alongside the data rows in one backend round trip. A mismatched
	// portal name forces the deferred describe to flush via the handler
	// before this Execute proceeds.
	includeDescribe := false
	if c.deferredPortalDescribe {
		if c.deferredPortalDescribeName == portalName {
			includeDescribe = true
			c.deferredPortalDescribe = false
			c.deferredPortalDescribeName = ""
		} else {
			if err := c.resolveDeferredPortalDescribe(); err != nil {
				return err
			}
			// If the deferred Describe flush errored, we're now in
			// drain mode. Don't run HandleExecute — its
			// RowDescription / DataRow / CommandComplete frames
			// would leak between ErrorResponse and ReadyForQuery.
			if c.discardingUntilSync {
				return nil
			}
		}
	}

	// Create a cancelable query context so cancel requests can interrupt this execution.
	queryCtx := c.BeginQueryCancel()
	defer c.EndQueryCancel()

	// Track state for streaming results.
	sentRowDescription := false

	// Call the handler to execute the portal with streaming callback.
	// The handler is responsible for retrieving the portal and executing it.
	err = c.handler.HandleExecute(queryCtx, c, portalName, maxRows, includeDescribe, func(ctx context.Context, result *sqltypes.Result) error {
		// A nil result signals an empty / comment-only query, mirroring the
		// simple-query path. PostgreSQL answers Execute of an empty-string
		// portal with EmptyQueryResponse. If a Describe('P') was folded into
		// this Execute, its answer (NoData for an empty portal) is owed first.
		if result == nil {
			if includeDescribe && !sentRowDescription {
				if err := c.writeMessage(protocol.MsgNoData, nil); err != nil {
					return fmt.Errorf("writing no data: %w", err)
				}
			}
			return c.writeEmptyQueryResponse()
		}

		// Send notices immediately (zero-buffering delivery).
		for _, notice := range result.Notices {
			if err := c.writeNoticeResponse(notice); err != nil {
				return fmt.Errorf("writing notice response: %w", err)
			}
			if err := c.flush(); err != nil {
				return fmt.Errorf("flushing notice response: %w", err)
			}
		}

		// On first callback with fields, send RowDescription.
		// Use nil check (not len > 0) because zero-column results still require RowDescription.
		// When a folded Describe('P') is in flight, the same RowDescription resolves it.
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
			// DML folded with a deferred Describe never sends Fields through
			// the callback, but the protocol still requires NoData before
			// CommandComplete. Emit it here so the wire order matches a
			// non-folded Describe(P)+Execute sequence.
			if includeDescribe && !sentRowDescription {
				if err := c.writeMessage(protocol.MsgNoData, nil); err != nil {
					return fmt.Errorf("writing no data: %w", err)
				}
			}
			if err := c.writeCommandComplete(result.CommandTag); err != nil {
				return fmt.Errorf("writing command complete: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		err = queryContextError(queryCtx, err)
		return c.writeExtendedQueryError(err)
	}

	return nil
}

// handleDescribe handles a 'D' (Describe) message - extended query protocol.
// Describes either a prepared statement ('S') or a portal ('P').
// serve() owns the buffering window; reply packets stay buffered
// until Sync flushes the batch.
func (c *Conn) handleDescribe() error {
	// Read message length.
	msgLen, err := c.ReadMessageLength()
	if err != nil {
		return fmt.Errorf("failed to read Describe message length: %w", err)
	}
	if msgLen < 2 {
		return errors.New("invalid describe message: missing type or name")
	}

	// Read the full body (type byte + null-terminated name) into a
	// pooled buffer.
	buf, err := c.readMessageBody(msgLen)
	if err != nil {
		return fmt.Errorf("failed to read describe body: %w", err)
	}
	defer c.returnReadBuffer()

	if buf[len(buf)-1] != 0 {
		return errors.New("invalid describe message: name missing null terminator")
	}
	typ := buf[0]
	// String() copies, so the body buffer can be returned to the pool.
	name := string(buf[1 : len(buf)-1])

	c.logger.Debug("describe", "type", string(typ), "name", name)

	// Describe('P') is captured and held: if the next message is Execute on
	// the same portal, the two fold into a single backend call. Otherwise
	// the next non-Execute handler (including a follow-up Describe of any
	// type) flushes the held state via resolveDeferredPortalDescribe in
	// handleMessage before its own reply runs. No writes happen here, so
	// skip buffer acquisition entirely.
	if typ == 'P' {
		c.deferredPortalDescribe = true
		c.deferredPortalDescribeName = name
		return nil
	}

	c.startWriterBuffering()
	// Flush errors at this teardown are best-effort — if the handler
	// itself returned an error we already have it in flight, and if
	// not, serve()'s post-dispatch will see the next ReadMessageType
	// fail and tear down the connection with the right context.
	defer func() { _ = c.endWriterBuffering() }()

	// Statement describe ('S') — call handler synchronously. Any prior
	// Describe('P') was already flushed by handleMessage before this call.
	desc, err := c.handler.HandleDescribe(c.ctx, c, typ, name)
	if err != nil {
		// Preserve a structured diagnostic — 26000 for an unknown prepared
		// statement, 34000 for an unknown portal — so the two cases stay
		// distinguishable; only opaque errors become MTD06.
		return c.writeExtendedQueryError(preserveExtendedQueryError(err, mterrors.MTD06))
	}

	// Send ParameterDescription only for statement describes ('S').
	// PostgreSQL protocol: Describe('S') returns ParameterDescription + RowDescription/NoData,
	// but Describe('P') returns only RowDescription/NoData — no ParameterDescription.
	// The 'P' branch above already returned, so this is reachable only for
	// 'S' under the contract; the explicit check is defense in depth in case
	// a future caller routes a different byte through this path.
	if typ == 'S' {
		if err := c.writeParameterDescription(desc.Parameters); err != nil {
			return fmt.Errorf("failed to write parameter description: %w", err)
		}
	}

	// Send RowDescription or NoData based on whether we have field info.
	// Use nil check (not len > 0) because zero-column results (e.g., "SELECT FROM foo")
	// have a non-nil empty Fields slice and still require a RowDescription message.
	if desc.Fields != nil {
		return c.writeRowDescription(desc.Fields)
	}
	// Send NoData when there are no fields (e.g., DML statements).
	return c.writeMessage(protocol.MsgNoData, nil)
}

// resolveDeferredPortalDescribe flushes a pending Describe('P') by calling
// the handler synchronously and writing the wire-correct RowDescription
// (or NoData) before any subsequent message's reply is generated. Called
// from every non-Execute handler that runs while a deferred Describe is
// outstanding.
//
// No-op when nothing is deferred.
func (c *Conn) resolveDeferredPortalDescribe() error {
	if !c.deferredPortalDescribe {
		return nil
	}
	name := c.deferredPortalDescribeName
	c.deferredPortalDescribe = false
	c.deferredPortalDescribeName = ""
	desc, err := c.handler.HandleDescribe(c.ctx, c, 'P', name)
	if err != nil {
		// Preserve a structured diagnostic (34000 for an unknown portal); only
		// opaque errors become MTD06. See preserveExtendedQueryError.
		return c.writeExtendedQueryError(preserveExtendedQueryError(err, mterrors.MTD06))
	}
	if desc != nil && desc.Fields != nil {
		return c.writeRowDescription(desc.Fields)
	}
	return c.writeMessage(protocol.MsgNoData, nil)
}

// handleClose handles a 'C' (Close) message - extended query protocol.
// Closes either a prepared statement ('S') or a portal ('P'). serve()
// owns the buffering window; CloseComplete stays buffered until the
// trailing Sync flushes the batch.
func (c *Conn) handleClose() error {
	// Read message length.
	msgLen, err := c.ReadMessageLength()
	if err != nil {
		return fmt.Errorf("failed to read Close message length: %w", err)
	}
	if msgLen < 2 {
		return errors.New("invalid close message: missing type or name")
	}

	// Read the full body (type byte + null-terminated name) into a
	// pooled buffer.
	buf, err := c.readMessageBody(msgLen)
	if err != nil {
		return fmt.Errorf("failed to read close body: %w", err)
	}
	defer c.returnReadBuffer()

	if buf[len(buf)-1] != 0 {
		return errors.New("invalid close message: name missing null terminator")
	}
	typ := buf[0]
	// String() copies, so the body buffer can be returned to the pool.
	name := string(buf[1 : len(buf)-1])

	c.logger.Debug("close", "type", string(typ), "name", name)

	// Call the handler.
	if err := c.handler.HandleClose(c.ctx, c, typ, name); err != nil {
		return c.writeExtendedQueryError(mterrors.MTD07.NewWithDetail(err.Error()))
	}

	// Send CloseComplete. Stays buffered for the rest of the batch.
	return c.writeMessage(protocol.MsgCloseComplete, nil)
}

// maybeDispatchDrain is the extended-query error-drain gate. When
// c.discardingUntilSync is set it routes the current inbound message
// without dispatching to its normal handler — Sync is the only drain
// boundary and clears the flag, signaling the caller to fall through
// so handleSync emits ReadyForQuery; Flush flushes buffered bytes
// without writing a reply; Terminate falls through to the normal
// teardown; and every other message type (Parse, Bind, Describe,
// Execute, Close, Query, and anything else) gets its body drained off
// the wire.
//
// Returns (handled=true, err) when the message was absorbed here and
// the caller should return immediately; (handled=false, nil) when the
// caller should continue to its normal dispatch.
//
// Called twice from handleMessage — once at the top for messages that
// arrive while drain mode is already set, and once again after
// resolveDeferredPortalDescribe runs (which can itself enter drain
// mode by flushing a failing deferred Describe).
func (c *Conn) maybeDispatchDrain(msgType byte) (handled bool, err error) {
	if !c.discardingUntilSync {
		return false, nil
	}
	switch msgType {
	case protocol.MsgSync:
		// Sync is the ONLY drain boundary. Per the PostgreSQL protocol, after an
		// extended-query error the backend "reads and discards messages until a Sync
		// is reached, then issues ReadyForQuery". Clear the flag and fall through so
		// handleSync emits ReadyForQuery.
		c.discardingUntilSync = false
		return false, nil
	case protocol.MsgTerminate:
		// Connection teardown — fall through to normal dispatch.
		return false, nil
	case protocol.MsgFlush:
		// Flush still pushes any buffered ErrorResponse to the client
		// but writes no reply of its own. Mirror the length-consume +
		// flush dance that handleMessage's MsgFlush branch does.
		if _, err := c.ReadMessageLength(); err != nil {
			return true, fmt.Errorf("failed to read Flush message length: %w", err)
		}
		return true, c.flush()
	default:
		// Discard until Sync. The PostgreSQL protocol discards ALL messages, not an
		// enumerated subset — e.g. a simple Query ('Q') pipelined after an
		// extended-query error (Postgrex mode: :savepoint's "RELEASE SAVEPOINT
		// postgrex_query") must be skipped like any other message; executing it
		// would emit extra frames between ErrorResponse and the Sync's
		// ReadyForQuery, desyncing strict clients. drainExtendedQueryMessage is
		// generic per-message framing, so this covers Parse/Bind/Describe/Execute/
		// Close/Query and any other message type uniformly.
		return true, c.drainExtendedQueryMessage()
	}
}

// drainExtendedQueryMessage reads and discards a single extended-query
// message body while the connection is in error-drain mode. The bytes
// must still be consumed off the wire so the next ReadMessageType aligns
// to the next message header — only the handler call and the reply
// frame are suppressed. readMessageBody handles zero-length bodies as a
// no-op, so no special case is needed here.
func (c *Conn) drainExtendedQueryMessage() error {
	bodyLen, err := c.ReadMessageLength()
	if err != nil {
		return fmt.Errorf("read length while draining: %w", err)
	}
	if _, err := c.readMessageBody(bodyLen); err != nil {
		return fmt.Errorf("read body while draining: %w", err)
	}
	c.returnReadBuffer()
	return nil
}

// writeExtendedQueryError writes an ErrorResponse and enters error-drain
// mode. PostgreSQL's protocol spec requires that after an extended-query
// message errors, the backend discards every subsequent message until
// Sync and only then emits ReadyForQuery. Strict drivers (Postgrex, JDBC)
// treat any frame between ErrorResponse and ReadyForQuery as protocol
// corruption and drop the connection.
//
// Use this from any handler that processes an extended-query inbound
// message (Parse, Bind, Describe, Execute, Close) when reporting an
// error to the client. Do NOT use it from handleQuery (simple Query is
// its own self-contained ErrorResponse + ReadyForQuery flow) or from
// handleSync (Sync itself emits ReadyForQuery and is the boundary the
// drain ends at).
func (c *Conn) writeExtendedQueryError(err error) error {
	c.discardingUntilSync = true
	return c.writeError(err)
}

// handleSync handles an 'S' (Sync) message - extended query protocol.
// Sync indicates the end of an extended query cycle and transaction boundary.
// Always sends ReadyForQuery in response.
//
// Sync is a flush boundary: serve() will release the buffering window
// (flushing any queued ParseComplete / BindComplete / RowDescription /
// DataRow / CommandComplete from earlier messages in the batch, plus
// the ReadyForQuery we write here) once we return.
func (c *Conn) handleSync() error {
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
	return c.writeReadyForQuery()
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
				// writeNotificationResponseMsg acquires bufMu through
				// startPacket/writePacket; each notification packet is
				// committed atomically under that lock, so it can't be
				// interleaved with a synchronous handler's packet.
				if err := c.writeNotificationResponseMsg(notif.PID, notif.Channel, notif.Payload); err != nil {
					c.logger.ErrorContext(ctx, "failed to push notification", "error", err)
					return
				}
				if err := c.flush(); err != nil {
					c.logger.ErrorContext(ctx, "failed to flush notification", "error", err)
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

// FlushPendingNotifications drains all pending notifications from
// the async pusher channel and writes them to the client socket.
// Called synchronously after each query completes (before
// ReadyForQuery) to deliver notifications that arrived during query
// execution.
//
// Each notification is written via writeNotificationResponseMsg,
// which acquires bufMu inside startPacket/writePacket per packet.
// Multiple notifications may interleave with the synchronous query
// handler's writes between packets, but every individual packet is
// committed atomically under the lock, so packet bodies can never be
// split.
func (c *Conn) FlushPendingNotifications() error {
	if c.notifPush == nil {
		return nil
	}
	c.startWriterBuffering()
	for {
		select {
		case notif, ok := <-c.notifPush.ch:
			if !ok || notif == nil {
				return c.flush()
			}
			if err := c.writeNotificationResponseMsg(notif.PID, notif.Channel, notif.Payload); err != nil {
				return err
			}
		default:
			return c.flush()
		}
	}
}

// writeNotificationResponseMsg writes a NotificationResponse ('A')
// packet: int32 pid, null-terminated channel, null-terminated payload.
// Goes through startPacket/writePacket — caller does NOT need to hold
// bufMu (the helpers manage it).
func (c *Conn) writeNotificationResponseMsg(pid int32, channel, payload string) error {
	bodyLen := 4 + len(channel) + 1 + len(payload) + 1
	buf, pos := c.startPacket(protocol.MsgNotificationResponse, bodyLen)
	pos = writeInt32At(buf, pos, pid)
	pos = writeStringAt(buf, pos, channel)
	pos = writeStringAt(buf, pos, payload)
	return c.writePacket(buf, pos)
}
