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
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
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

	// ScramClientKey and ScramServerKey enable SCRAM-SHA-256 passthrough
	// authentication. When both are set, the client uses them to produce a
	// valid SCRAM proof without knowing the plaintext password. Both must
	// be 32 bytes (HMAC-SHA-256 output). If set, they take precedence over
	// Password when the server requests SASL. Leave nil for the standard
	// password-based path.
	ScramClientKey []byte
	ScramServerKey []byte

	// Database is the database name to connect to.
	Database string

	// Parameters are additional connection parameters.
	Parameters map[string]string

	// SSLMode controls libpq-style sslmode behavior on TCP connections.
	// Empty string is treated as SSLModeDisable; only consulted when SocketFile
	// is unset. The dial path uses this together with TLSConfig to decide
	// whether to send SSLRequest, whether to fall back to plaintext on refusal,
	// and which verification rules apply.
	SSLMode SSLMode

	// SSLNegotiation selects how TLS is established on TCP connections
	// (libpq sslnegotiation, PostgreSQL 17+). Empty string and "postgres"
	// use the classic SSLRequest → 'S'/'N' negotiation; "direct" starts the
	// TLS handshake immediately after the TCP dial with mandatory ALPN
	// ("postgresql"). Direct requires a TLS-enforcing SSLMode (require /
	// verify-ca / verify-full); startup fails otherwise, matching libpq.
	SSLNegotiation SSLNegotiation

	// TLSConfig is the TLS configuration for SSL connections.
	// Only used for TCP connections. Pair with SSLMode; for prefer/require/
	// verify-ca/verify-full this must be non-nil. Built via BuildTLSConfig.
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

	// bufMu serializes the multi-step request/response sequences that
	// write through bufferedWriter and read from bufferedReader. The
	// client side is single-owner, so this is about ensuring whole
	// extended-protocol pipelines (Parse+Bind+Describe+Execute+Sync)
	// land contiguously rather than guarding against concurrent I/O.
	bufMu sync.Mutex

	// outboundPoolBuf holds the package-level bufpool buffer that
	// startPacket grabbed for an oversize (slow-path) packet. Non-nil
	// only between startPacket and writePacket, and only on the slow
	// path. writePacket returns it to the pool. Protected by the
	// caller-held bufMu.
	//
	// Stored as the original *[]byte the pool returned (rather than
	// taking &buf inside writePacket) because passing the address of a
	// stack-local slice to bufPool.Put would force the slice header to
	// escape to the heap on every call, even when the slow path
	// doesn't fire — sync.Pool retains its arguments. Going through
	// this field, which already points at heap memory the pool owns,
	// avoids that escape.
	outboundPoolBuf *[]byte

	// config is the connection configuration.
	config *Config

	// Backend key data received from the server.
	processID uint32
	secretKey uint32

	// Server parameters received during startup.
	serverParams map[string]string

	// txnStatus is the current transaction status.
	txnStatus protocol.TransactionStatus

	// state stores connection-specific information.
	// Callers can store their own state here by calling SetConnectionState.
	state any

	// closed indicates whether the connection has been closed.
	closed atomic.Bool

	// ctx is the context for this connection.
	ctx    context.Context
	cancel context.CancelFunc

	// replState tracks the copy-both replication lifecycle. Zero value
	// (replStreamIdle) is correct for every non-replication connection.
	replState replStreamState
}

// Connect establishes a new connection to a PostgreSQL server.
// If config.SocketFile is set, connects via Unix socket.
// Otherwise, connects via TCP to config.Host:config.Port.
// ctx is used for dial and startup operations.
// poolCtx is used as the parent for the connection's lifetime context,
// allowing pool-managed connections to be tied to the pool's lifecycle.
func Connect(ctx context.Context, poolCtx context.Context, config *Config) (*Conn, error) {
	netConn, err := dial(ctx, config)
	if err != nil {
		return nil, err
	}

	// Create the connection object.
	// The connection's lifetime is tied to poolCtx, not the caller's ctx.
	connCtx, cancel := context.WithCancel(poolCtx)
	c := &Conn{
		config: config,
		ctx:    connCtx,
		cancel: cancel,
	}
	c.resetConn(netConn)

	// Perform the startup handshake.
	if err := c.startup(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("startup failed: %w", err)
	}

	return c, nil
}

// Reconnect replaces the underlying network connection in-place.
// It closes the old (broken) socket, dials a new connection using the stored
// config, and performs the PostgreSQL startup handshake. The Conn object
// identity is preserved so callers (like pool wrappers) continue to work.
//
// Reconnect is not concurrency-safe. It relies on the pool's single-owner
// model: the connection is checked out to exactly one goroutine, and only
// that goroutine calls Reconnect (from the retry loop).
//
// The caller is responsible for re-applying any session state (settings,
// prepared statements) after a successful reconnect.
func (c *Conn) Reconnect(ctx context.Context) error {
	// Close the raw socket. Best-effort since the connection may already be broken.
	// We intentionally don't call c.Close() because that cancels the lifetime
	// context (derived from poolCtx) which we want to keep alive.
	_ = c.conn.Close()

	// Reset the closed flag so the connection is usable again.
	c.closed.Store(false)

	netConn, err := dial(ctx, c.config)
	if err != nil {
		c.closed.Store(true)
		return fmt.Errorf("reconnect dial failed: %w", err)
	}

	c.resetConn(netConn)

	// Perform the startup handshake on the new connection.
	if err := c.startup(ctx); err != nil {
		c.Close()
		return fmt.Errorf("reconnect startup failed: %w", err)
	}

	return nil
}

// dial establishes a network connection using the given config.
// Uses Unix socket if config.SocketFile is set, otherwise TCP.
func dial(ctx context.Context, config *Config) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout: config.DialTimeout,
	}
	if config.SocketFile != "" {
		conn, err := dialer.DialContext(ctx, "unix", config.SocketFile)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Unix socket %s: %w", config.SocketFile, err)
		}
		return conn, nil
	}
	address := fmt.Sprintf("%s:%d", config.Host, config.Port)
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}
	return conn, nil
}

// resetConn replaces the connection internals with a new network connection.
// This resets the buffered reader/writer, server params, and transaction status.
func (c *Conn) resetConn(netConn net.Conn) {
	c.conn = netConn
	c.bufferedReader = bufio.NewReaderSize(netConn, connBufferSize)
	c.bufferedWriter = bufio.NewWriterSize(netConn, connBufferSize)
	c.serverParams = make(map[string]string)
	c.txnStatus = protocol.TxnStatusIdle
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

	// Defensive cleanup: if a writer panicked between startPacket and
	// writePacket, the slow-path pool buffer is still stashed on the
	// Conn (writePacket's defer never fired). Close runs after
	// concurrent access has stopped, so an unlocked Put is safe.
	c.returnOutboundBuffer()

	return c.conn.Close()
}

// ForceClose closes the underlying network connection without writing a
// Terminate message. This is safe to call concurrently with ongoing
// reads/writes — it will cause them to fail with an I/O error.
//
// Use this instead of Close when you need to unblock a goroutine that is
// mid-read/write on the connection, since Close writes to the buffered
// writer and would race with the concurrent operation.
func (c *Conn) ForceClose() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil // Already closed.
	}

	c.cancel()
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
func (c *Conn) TxnStatus() protocol.TransactionStatus {
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

// DetachConn detaches the underlying network connection from this Conn and
// returns it along with any bytes the read buffer had already read ahead.
// After a successful call the Conn is closed for protocol use: its Close/
// ForceClose will not touch the returned socket, which the caller now owns.
//
// The caller MUST treat `buffered` as bytes that arrived before any subsequent
// socket read: prepend them to the read stream (e.g. io.MultiReader(bytes.NewReader(buffered), raw)).
// The write buffer is flushed before detaching, so writes may go straight to raw.
//
// DetachConn does NO connection-pool accounting — it operates purely at the
// protocol layer and knows nothing about any pool this Conn may belong to. When
// a Conn is owned by a pooled wrapper (e.g. reserved.Conn), that wrapper's own
// release path remains solely responsible for freeing its capacity slot and
// updating live counts; detaching neither frees nor leaks a slot. The ordering
// here is what keeps that release safe: `closed` is set to true before the
// socket is handed off, so the wrapper's later Close/Recycle sees an
// already-closed Conn, skips touching the (now caller-owned) socket, and still
// frees its slot exactly once. The caller owns and must close `raw`.
func (c *Conn) DetachConn() (raw net.Conn, buffered []byte, err error) {
	if c.closed.Load() {
		return nil, nil, errors.New("pgclient: DetachConn on closed connection")
	}
	if err := c.flush(); err != nil {
		return nil, nil, fmt.Errorf("pgclient: flush before hijack: %w", err)
	}
	if n := c.bufferedReader.Buffered(); n > 0 {
		peeked, err := c.bufferedReader.Peek(n)
		if err != nil {
			return nil, nil, fmt.Errorf("pgclient: peek buffered bytes: %w", err)
		}
		buffered = append([]byte(nil), peeked...) // copy; reader memory is reused
		if _, err := c.bufferedReader.Discard(n); err != nil {
			return nil, nil, fmt.Errorf("pgclient: discard buffered bytes: %w", err)
		}
	}
	raw = c.conn
	c.conn = nil         // prevent Close() from closing the hijacked socket
	c.closed.Store(true) // Conn is no longer usable for protocol I/O
	// Release the connection's context here: setting closed=true makes a later
	// Close()/ForceClose() short-circuit on the closed CAS before they reach
	// c.cancel(), so this is the only place the child context derived from
	// poolCtx in Connect() gets cancelled. Skipping it would leak that child in
	// poolCtx's children map for the pool's lifetime (one per detach). The
	// detached raw socket is a plain net.Conn and is unaffected by this cancel.
	c.cancel()
	return raw, buffered, nil
}

// WriteCopyData sends a CopyData ('d') message to PostgreSQL.
// The data should already be appropriately sized by upstream layers
// (client chunking, gRPC message limits, protocol reading).
func (c *Conn) WriteCopyData(data []byte) error {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	buf, pos := c.startPacket(protocol.MsgCopyData, len(data))
	pos = writeBytesAt(buf, pos, data)
	if err := c.writePacket(buf, pos); err != nil {
		return fmt.Errorf("failed to write CopyData: %w", err)
	}
	return c.flush()
}

// WriteCopyDone sends a CopyDone ('c') message to PostgreSQL
// This signals that all COPY data has been sent
func (c *Conn) WriteCopyDone() error {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	buf, pos := c.startPacket(protocol.MsgCopyDone, 0)
	if err := c.writePacket(buf, pos); err != nil {
		return fmt.Errorf("failed to write CopyDone: %w", err)
	}
	return c.flush()
}

// WriteCopyFail sends a CopyFail ('f') message to PostgreSQL
// This aborts the COPY operation with the given error message
func (c *Conn) WriteCopyFail(errorMsg string) error {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	bodyLen := len(errorMsg) + 1 // null terminator
	buf, pos := c.startPacket(protocol.MsgCopyFail, bodyLen)
	pos = writeStringAt(buf, pos, errorMsg)
	if err := c.writePacket(buf, pos); err != nil {
		return fmt.Errorf("failed to write CopyFail: %w", err)
	}
	return c.flush()
}

// ReadCopyDoneResponse reads the CommandComplete response after WriteCopyDone().
// Returns the command tag (e.g., "COPY 100"), rows affected, and any
// NoticeResponse / InfoResponse diagnostics received between CopyDone and
// ReadyForQuery. Notices come from BEFORE/AFTER row triggers that fire during
// COPY FROM STDIN (e.g. RAISE NOTICE) and from COPY progress reporting; the
// gateway forwards them to the client as NoticeResponse frames before the
// final CommandComplete so client-side test fixtures see the expected
// NOTICE / INFO lines.
//
// Note: In simple query protocol, PostgreSQL sends CommandComplete followed
// by ReadyForQuery — both messages must be consumed to clear the buffer.
func (c *Conn) ReadCopyDoneResponse(ctx context.Context) (string, uint64, []*mterrors.PgDiagnostic, error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	var commandTag string
	var rowsAffected uint64
	var notices []*mterrors.PgDiagnostic
	gotCommandComplete := false

	// Read messages until we get both CommandComplete and ReadyForQuery
	for {
		msgType, err := c.readMessageType()
		if err != nil {
			return "", 0, notices, fmt.Errorf("failed to read message type: %w", err)
		}

		length, err := c.readMessageLength()
		if err != nil {
			return "", 0, notices, fmt.Errorf("failed to read message length: %w", err)
		}

		body, err := c.readMessageBody(length)
		if err != nil {
			return "", 0, notices, fmt.Errorf("failed to read message body: %w", err)
		}

		switch msgType {
		case protocol.MsgCommandComplete:
			// Parse command tag
			if len(body) == 0 {
				return "", 0, notices, errors.New("empty CommandComplete body")
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
			// Parse the error, then drain the trailing ReadyForQuery so the
			// connection is left in a clean state and is safe to return to the
			// pool. Without this, the next operation on this socket would see
			// the leftover RFQ as its first response and fail. waitForReadyForQuery
			// also updates txnStatus from the RFQ payload, which we want even on
			// the error path so callers can observe TxnStatusFailed when COPY
			// finalization fails inside a transaction.
			pgErr := c.parseError(body)
			_ = c.waitForReadyForQuery(ctx)
			return "", 0, notices, pgErr

		case protocol.MsgNoticeResponse:
			// Collect notices for forwarding. Trigger RAISE NOTICE output and
			// INFO progress events arrive here between CopyDone and the final
			// CommandComplete.
			notices = append(notices, c.parseNotice(body))

		case protocol.MsgReadyForQuery:
			// End of response - if we got CommandComplete, return success
			if gotCommandComplete {
				c.txnStatus = protocol.TransactionStatus(body[0])
				return commandTag, rowsAffected, notices, nil
			}
			// Otherwise, we got ReadyForQuery without CommandComplete (error case)
			return "", 0, notices, errors.New("received ReadyForQuery without CommandComplete")

		default:
			return "", 0, notices, fmt.Errorf("unexpected message type after CopyDone: '%c'", msgType)
		}
	}
}

// ReadCopyFailResponse reads the expected ErrorResponse + ReadyForQuery sequence
// after sending CopyFail. Unlike ReadCopyDoneResponse, this treats ErrorResponse
// as the expected (normal) response and continues reading until ReadyForQuery,
// leaving the connection in a clean protocol state. Notices received in this
// window are collected and returned so callers can forward trigger output
// that fired before the abort.
func (c *Conn) ReadCopyFailResponse(ctx context.Context) ([]*mterrors.PgDiagnostic, error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	var notices []*mterrors.PgDiagnostic
	gotError := false

	for {
		msgType, err := c.readMessageType()
		if err != nil {
			return notices, fmt.Errorf("failed to read message type: %w", err)
		}

		length, err := c.readMessageLength()
		if err != nil {
			return notices, fmt.Errorf("failed to read message length: %w", err)
		}

		body, err := c.readMessageBody(length)
		if err != nil {
			return notices, fmt.Errorf("failed to read message body: %w", err)
		}

		switch msgType {
		case protocol.MsgErrorResponse:
			// Expected after CopyFail — consume it and continue to ReadyForQuery.
			_ = body
			gotError = true

		case protocol.MsgNoticeResponse:
			notices = append(notices, c.parseNotice(body))

		case protocol.MsgReadyForQuery:
			c.txnStatus = protocol.TransactionStatus(body[0])
			if gotError {
				return notices, nil // clean abort: ErrorResponse + ReadyForQuery consumed
			}
			return notices, errors.New("received ReadyForQuery without ErrorResponse after CopyFail")

		default:
			return notices, fmt.Errorf("unexpected message type after CopyFail: '%c'", msgType)
		}
	}
}

// ReadCopyInResponse reads and parses a CopyInResponse ('G') message from PostgreSQL
// This message is sent in response to a COPY FROM STDIN command
// Returns the overall format and per-column formats
func (c *Conn) ReadCopyInResponse() (format int16, columnFormats []int16, err error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

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

	return parseCopyResponseBody("CopyInResponse", body)
}

func parseCopyResponseBody(responseType string, body []byte) (format int16, columnFormats []int16, err error) {
	if len(body) < 3 {
		return 0, nil, fmt.Errorf("%s body too short: %d bytes", responseType, len(body))
	}

	// Body layout: Int8(format) + Int16(numCols) + Int16[numCols].
	format = int16(body[0])
	numCols := int16(uint16(body[1])<<8 | uint16(body[2]))

	columnFormats = make([]int16, numCols)
	offset := 3
	for i := 0; i < int(numCols); i++ {
		if offset+2 > len(body) {
			return 0, nil, fmt.Errorf("%s body too short for column formats", responseType)
		}
		columnFormats[i] = int16(uint16(body[offset])<<8 | uint16(body[offset+1]))
		offset += 2
	}

	return format, columnFormats, nil
}

// InitiateCopyFromStdin sends a COPY FROM STDIN query and reads the CopyInResponse.
// This is a special operation that doesn't follow the normal query flow.
// Returns the COPY format, column formats from the CopyInResponse, and any
// NoticeResponse diagnostics received before the CopyInResponse (e.g. notices
// from BEFORE STATEMENT triggers that fire at COPY initiation).
func (c *Conn) InitiateCopyFromStdin(ctx context.Context, copyQuery string) (format int16, columnFormats []int16, notices []*mterrors.PgDiagnostic, err error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	// Send the COPY query (simple-protocol Q message + flush).
	if err := c.writeQueryMessage(copyQuery); err != nil {
		return 0, nil, notices, fmt.Errorf("failed to send COPY query: %w", err)
	}

	// Loop through messages until we get CopyInResponse or an error.
	// Notices and ParameterStatus updates may interleave.
	for {
		msgType, err := c.readMessageType()
		if err != nil {
			return 0, nil, notices, fmt.Errorf("failed to read message type: %w", err)
		}

		bodyLen, err := c.readMessageLength()
		if err != nil {
			return 0, nil, notices, fmt.Errorf("failed to read message length: %w", err)
		}

		body, err := c.readMessageBody(bodyLen)
		if err != nil {
			return 0, nil, notices, fmt.Errorf("failed to read message body: %w", err)
		}

		switch msgType {
		case protocol.MsgCopyInResponse:
			format, columnFormats, err = parseCopyResponseBody("CopyInResponse", body)
			if err != nil {
				return 0, nil, notices, err
			}
			return format, columnFormats, notices, nil

		case protocol.MsgErrorResponse:
			// Parse the error, then drain the trailing ReadyForQuery so
			// the connection is left in a clean state and is safe to
			// return to the pool. Without this, the next operation on
			// this socket would see the leftover RFQ as its first
			// response and fail with "received ReadyForQuery before X".
			// waitForReadyForQuery also updates txnStatus from the RFQ
			// payload, which we want even on the error path.
			pgErr := c.parseError(body)
			_ = c.waitForReadyForQuery(ctx)
			return 0, nil, notices, pgErr

		case protocol.MsgNoticeResponse:
			notices = append(notices, c.parseNotice(body))

		case protocol.MsgParameterStatus:
			// Handle parameter status updates, ignore errors
			_ = c.handleParameterStatus(body)
			continue

		case protocol.MsgReadyForQuery:
			// If we get ReadyForQuery before CopyInResponse, the query failed
			// but we didn't get an ErrorResponse (which shouldn't happen)
			return 0, nil, notices, errors.New("received ReadyForQuery before CopyInResponse - query may have failed without error")

		default:
			return 0, nil, notices, fmt.Errorf("unexpected message type during COPY initiation: '%c' (0x%02x)", msgType, msgType)
		}
	}
}

// InitiateCopyToStdout sends a COPY ... TO STDOUT query and reads the
// CopyOutResponse. The caller is then expected to drive the response stream
// with ReadCopyOutMessage until io.EOF (CopyDone), followed by
// FinishCopyToStdout to consume the trailing CommandComplete + ReadyForQuery.
// Returns the COPY format, per-column formats from the CopyOutResponse, and
// any NoticeResponse diagnostics seen before the CopyOutResponse arrived.
//
// Modeled after InitiateCopyFromStdin — same Q-then-flush, same notice /
// ParameterStatus / ErrorResponse handling.
func (c *Conn) InitiateCopyToStdout(ctx context.Context, copyQuery string) (format int16, columnFormats []int16, notices []*mterrors.PgDiagnostic, err error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if err := c.writeQueryMessage(copyQuery); err != nil {
		return 0, nil, notices, fmt.Errorf("failed to send COPY query: %w", err)
	}

	for {
		msgType, err := c.readMessageType()
		if err != nil {
			return 0, nil, notices, fmt.Errorf("failed to read message type: %w", err)
		}

		bodyLen, err := c.readMessageLength()
		if err != nil {
			return 0, nil, notices, fmt.Errorf("failed to read message length: %w", err)
		}

		body, err := c.readMessageBody(bodyLen)
		if err != nil {
			return 0, nil, notices, fmt.Errorf("failed to read message body: %w", err)
		}

		switch msgType {
		case protocol.MsgCopyOutResponse:
			format, columnFormats, err = parseCopyResponseBody("CopyOutResponse", body)
			if err != nil {
				return 0, nil, notices, err
			}
			return format, columnFormats, notices, nil

		case protocol.MsgErrorResponse:
			pgErr := c.parseError(body)
			_ = c.waitForReadyForQuery(ctx)
			return 0, nil, notices, pgErr

		case protocol.MsgNoticeResponse:
			notices = append(notices, c.parseNotice(body))

		case protocol.MsgParameterStatus:
			_ = c.handleParameterStatus(body)

		case protocol.MsgReadyForQuery:
			return 0, nil, notices, errors.New("received ReadyForQuery before CopyOutResponse - query may have failed without error")

		default:
			return 0, nil, notices, fmt.Errorf("unexpected message type during COPY TO initiation: '%c' (0x%02x)", msgType, msgType)
		}
	}
}

// CopyOutMessage represents a single message read from the COPY TO STDOUT
// response stream. Exactly one of Data or Notice is set; Done is true when
// PG sent CopyDone (Data and Notice will both be nil in that case).
type CopyOutMessage struct {
	Data   []byte
	Notice *mterrors.PgDiagnostic
	Done   bool
}

// ReadCopyOutMessage reads the next message in the COPY TO STDOUT stream.
// Returns a CopyOutMessage containing a CopyData chunk, a NoticeResponse
// diagnostic, or Done=true on CopyDone. CommandComplete/ReadyForQuery are
// not consumed here — call FinishCopyToStdout once Done is observed.
//
// Distinct from ReadCopyData (which only reads CopyData / CopyDone) because
// trigger output and INFO progress messages can be interleaved with data
// rows during a COPY ... TO STDOUT.
//
// ctx is used only on the ErrorResponse path to drain the trailing
// ReadyForQuery; reads themselves are framed at the wire layer and don't
// take a context.
func (c *Conn) ReadCopyOutMessage(ctx context.Context) (CopyOutMessage, error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	for {
		msgType, err := c.readMessageType()
		if err != nil {
			return CopyOutMessage{}, fmt.Errorf("failed to read message type: %w", err)
		}

		length, err := c.readMessageLength()
		if err != nil {
			return CopyOutMessage{}, fmt.Errorf("failed to read message length: %w", err)
		}

		switch msgType {
		case protocol.MsgCopyData:
			data, err := c.readMessageBody(length)
			if err != nil {
				return CopyOutMessage{}, fmt.Errorf("failed to read CopyData body: %w", err)
			}
			return CopyOutMessage{Data: data}, nil

		case protocol.MsgCopyDone:
			if length != 0 {
				return CopyOutMessage{}, fmt.Errorf("invalid CopyDone length: %d (expected 0)", length)
			}
			return CopyOutMessage{Done: true}, nil

		case protocol.MsgNoticeResponse:
			body, err := c.readMessageBody(length)
			if err != nil {
				return CopyOutMessage{}, fmt.Errorf("failed to read NoticeResponse body: %w", err)
			}
			return CopyOutMessage{Notice: c.parseNotice(body)}, nil

		case protocol.MsgErrorResponse:
			body, err := c.readMessageBody(length)
			if err != nil {
				return CopyOutMessage{}, fmt.Errorf("failed to read ErrorResponse body: %w", err)
			}
			pgErr := c.parseError(body)
			_ = c.waitForReadyForQuery(ctx)
			return CopyOutMessage{}, pgErr

		default:
			// Drain unexpected body bytes before returning the error to keep
			// the protocol position valid for callers that recover.
			_, _ = c.readMessageBody(length)
			return CopyOutMessage{}, fmt.Errorf("unexpected message type during COPY TO stream: '%c' (0x%02x)", msgType, msgType)
		}
	}
}

// FinishCopyToStdout reads the trailing CommandComplete + ReadyForQuery that
// PG sends after CopyDone in the COPY TO STDOUT flow. Returns the command
// tag (e.g. "COPY 5"), rows affected, and any NoticeResponse diagnostics
// that arrived between CopyDone and the final ReadyForQuery — same flow as
// ReadCopyDoneResponse, but the COPY-out caller has already consumed
// CopyDone via ReadCopyOutMessage.
func (c *Conn) FinishCopyToStdout(ctx context.Context) (string, uint64, []*mterrors.PgDiagnostic, error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	var commandTag string
	var rowsAffected uint64
	var notices []*mterrors.PgDiagnostic
	gotCommandComplete := false

	for {
		msgType, err := c.readMessageType()
		if err != nil {
			return "", 0, notices, fmt.Errorf("failed to read message type: %w", err)
		}

		length, err := c.readMessageLength()
		if err != nil {
			return "", 0, notices, fmt.Errorf("failed to read message length: %w", err)
		}

		body, err := c.readMessageBody(length)
		if err != nil {
			return "", 0, notices, fmt.Errorf("failed to read message body: %w", err)
		}

		switch msgType {
		case protocol.MsgCommandComplete:
			if len(body) == 0 {
				return "", 0, notices, errors.New("empty CommandComplete body")
			}
			tag := string(body)
			if len(tag) > 0 && tag[len(tag)-1] == 0 {
				tag = tag[:len(tag)-1]
			}
			var rows uint64
			if len(tag) > 5 && tag[:4] == "COPY" {
				_, _ = fmt.Sscanf(tag[5:], "%d", &rows)
			}
			commandTag = tag
			rowsAffected = rows
			gotCommandComplete = true

		case protocol.MsgErrorResponse:
			pgErr := c.parseError(body)
			_ = c.waitForReadyForQuery(ctx)
			return "", 0, notices, pgErr

		case protocol.MsgNoticeResponse:
			notices = append(notices, c.parseNotice(body))

		case protocol.MsgReadyForQuery:
			c.txnStatus = protocol.TransactionStatus(body[0])
			if gotCommandComplete {
				return commandTag, rowsAffected, notices, nil
			}
			return "", 0, notices, errors.New("received ReadyForQuery without CommandComplete")

		default:
			return "", 0, notices, fmt.Errorf("unexpected message type after CopyDone in COPY TO STDOUT: '%c'", msgType)
		}
	}
}

// ReadCopyOutResponse reads and parses a CopyOutResponse ('H') message from PostgreSQL
// This message is sent in response to a COPY TO STDOUT command
// Returns the overall format and per-column formats
func (c *Conn) ReadCopyOutResponse() (format int16, columnFormats []int16, err error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

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

	return parseCopyResponseBody("CopyOutResponse", body)
}

// ReadCopyData reads a CopyData ('d') message from PostgreSQL.
// This is used during COPY TO STDOUT to receive data rows.
// Returns the data bytes, or io.EOF when CopyDone is received to signal end of stream.
func (c *Conn) ReadCopyData() ([]byte, error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

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
