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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// StartupMessage represents a parsed startup message from the client.
type StartupMessage struct {
	ProtocolVersion uint32
	Parameters      map[string]string
}

// ReplicationMode captures the value of the `replication` startup parameter
// per PostgreSQL's replication protocol. The default (parameter omitted or
// "false") is ReplicationOff: a normal SQL connection.
//
// See: https://www.postgresql.org/docs/17/protocol-replication.html
type ReplicationMode int

const (
	// ReplicationOff means the client did not request a replication
	// connection, or sent replication=false. The session uses the standard
	// extended-query / simple-query protocol.
	ReplicationOff ReplicationMode = iota

	// ReplicationPhysical (replication=true / on / 1 / yes) opens a physical
	// walsender stream. The role must have rolreplication=true (or
	// rolsuper=true) in pg_authid.
	ReplicationPhysical

	// ReplicationLogical (replication=database) opens a logical-replication
	// walsender connected to a specific database. Same role requirement as
	// physical replication.
	ReplicationLogical
)

// parseReplicationMode interprets the `replication` startup parameter using
// PostgreSQL's parsing rules (src/backend/utils/misc/guc.c parse_bool_with_len).
// PG accepts case-insensitive on/off, true/false, yes/no, 1/0, and unique
// boolean prefixes, plus the literal "database" for logical-replication
// connections.
//
// Returns an InvalidParameterValue PgDiagnostic for unrecognized values so
// the gateway can reject them at the same protocol stage PostgreSQL would.
func parseReplicationMode(value string) (ReplicationMode, error) {
	if value == "" {
		return ReplicationOff, nil
	}
	if strings.EqualFold(value, "database") {
		return ReplicationLogical, nil
	}
	if b, ok := sqltypes.ParseBool(value); ok {
		if b {
			return ReplicationPhysical, nil
		}
		return ReplicationOff, nil
	}
	return ReplicationOff, mterrors.NewPgError(
		"FATAL", mterrors.PgSSInvalidParameterValue,
		fmt.Sprintf("invalid value for parameter \"replication\": \"%s\"", value),
		"Valid values are: \"false\", \"true\", \"database\".",
	)
}

// handleStartup handles the initial connection startup phase.
// This includes SSL/GSSAPI encryption negotiation and processing the startup message.
// Returns an error if the startup fails.
//
// Direct-TLS detection runs first, before any startup packet is read — the
// same ordering as PostgreSQL 17, where ProcessSSLStartup peeks the first
// byte ahead of ProcessStartupPacket (src/backend/tcop/backend_startup.c).
// A client that opens with a TLS ClientHello (libpq sslnegotiation=direct,
// or an Envoy edge that answered the SSLRequest itself and forwards the raw
// TLS stream) is upgraded in maybeHandleDirectTLS; everyone else falls
// through to the classic startup-packet dispatch untouched.
func (c *Conn) handleStartup() error {
	if err := c.maybeHandleDirectTLS(); err != nil {
		return err
	}
	return c.readAndDispatchStartup()
}

// readAndDispatchStartup reads a startup packet and dispatches based on protocol code.
// This method is called both for the initial startup and after encryption negotiation
// (SSL or GSSAPI) to handle the fallback ordering defined in the PostgreSQL protocol:
// a client may try GSSENCRequest after SSLRequest is declined, or vice versa.
// See: https://www.postgresql.org/docs/17/protocol-flow.html
func (c *Conn) readAndDispatchStartup() error {
	buf, err := c.readStartupPacket()
	if err != nil {
		return fmt.Errorf("failed to read startup packet: %w", err)
	}
	defer c.returnReadBuffer()

	reader := NewMessageReader(buf)
	protocolCode, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read protocol code: %w", err)
	}

	switch protocolCode {
	case protocol.SSLRequestCode:
		if c.sslDone {
			return mterrors.NewPgError("FATAL", "0A000", "duplicate SSLRequest: SSL negotiation already completed", "")
		}
		return c.handleSSLRequest()

	case protocol.GSSENCRequestCode:
		if c.gssDone {
			return mterrors.NewPgError("FATAL", "0A000", "duplicate GSSENCRequest: GSSAPI encryption negotiation already completed", "")
		}
		return c.handleGSSENCRequest()

	case protocol.CancelRequestCode:
		return c.handleCancelRequest(&reader)

	case protocol.ProtocolVersionNumber:
		if c.requireTLS && !c.tlsHandshakeComplete {
			c.logger.Warn("rejecting plaintext connection: TLS required",
				"remote_addr", c.RemoteAddr())
			// Distinguish the two ways a client can hit this gate so the
			// fleet-validation alert (MUL-420) can tell "client never
			// asked for TLS" from "server declined the SSLRequest" — the
			// latter would indicate misconfiguration that --pg-require-ssl
			// alone won't catch.
			reason := PlaintextRejectedReasonNoSSLRequest
			if c.sslDone {
				reason = PlaintextRejectedReasonTLSDisabledByServer
			}
			c.metrics().RecordPlaintextRejected(c.ctx, reason)
			// Returning a *PgDiagnostic lets serve() emit the FATAL
			// ErrorResponse via its standard startup-error path (which
			// also clears the auth deadline and closes the connection).
			return mterrors.NewPgError("FATAL", mterrors.PgSSInvalidAuthSpec,
				"no encryption: TLS is required for this connection", "")
		}
		return c.handleStartupMessage(protocolCode, &reader)

	default:
		return fmt.Errorf("unsupported protocol version: %d", protocolCode)
	}
}

// handleSSLRequest handles an SSL negotiation request from the client.
// If TLS is configured, accepts with 'S' and upgrades the connection to TLS.
// If TLS is not configured, declines with 'N'.
// After responding, reads the next startup packet which may be a StartupMessage
// or a GSSENCRequest (per PostgreSQL protocol fallback ordering).
func (c *Conn) handleSSLRequest() error {
	c.sslDone = true

	if c.tlsConfig == nil || c.tlsHandshakeComplete {
		if c.tlsHandshakeComplete {
			// SSLRequest arriving inside an already-established TLS tunnel
			// (the client connected with direct TLS and then sent an
			// SSLRequest anyway). PostgreSQL declines with 'N' here rather
			// than erroring — the `port->ssl_in_use` check in
			// ProcessStartupPacket — so the client falls through to a
			// regular StartupMessage on the existing encrypted channel.
			c.logger.Debug("client requested SSL inside TLS tunnel, declining")
		} else {
			// No TLS configured, decline SSL.
			c.logger.Debug("client requested SSL, declining (no TLS config)")
			c.metrics().RecordSSLRequestDeclined(c.ctx)
		}
		if err := c.writeRawByte('N'); err != nil {
			return fmt.Errorf("failed to send SSL response: %w", err)
		}
		if err := c.flush(); err != nil {
			return fmt.Errorf("failed to flush SSL response: %w", err)
		}
		// Read next packet — could be StartupMessage or GSSENCRequest (fallback).
		return c.readAndDispatchStartup()
	}

	// Accept SSL and upgrade to TLS.
	c.logger.Debug("client requested SSL, accepting")
	if err := c.writeRawByte('S'); err != nil {
		return fmt.Errorf("failed to send SSL response: %w", err)
	}
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush SSL response: %w", err)
	}

	// Buffer-stuffing attack prevention (CVE-2021-23222):
	// If there is buffered data after we sent 'S' but before the TLS handshake,
	// a MITM may have injected unencrypted data.
	if c.bufferedReader.Buffered() > 0 {
		return fmt.Errorf("received unencrypted data after SSL request: possible man-in-the-middle attack (buffered %d bytes)", c.bufferedReader.Buffered())
	}

	tlsConn, err := c.completeTLSHandshake(c.conn, c.tlsConfig, TLSNegotiationNegotiated)
	if err != nil {
		return err
	}
	connState := tlsConn.ConnectionState()
	c.metrics().RecordTLSConnection(c.ctx, TLSNegotiationNegotiated, connState.Version, connState.CipherSuite)

	// Read the actual startup message over the encrypted connection.
	return c.readAndDispatchStartup()
}

// completeTLSHandshake runs the server-side TLS handshake on transport using
// the supplied base config, records the handshake-attempt metrics tagged with
// the negotiation style (negotiated vs direct), and on success installs the TLS
// connection as this connection's transport: c.conn is swapped to the
// *tls.Conn, the buffered reader is reset onto it, and tlsHandshakeComplete
// is set. It also captures the server leaf certificate so SCRAM-SHA-256-PLUS
// channel binding can be advertised.
//
// transport is the conn the TLS engine should read/write — c.conn for the
// negotiated path, or a replay wrapper for the direct path where part of the
// ClientHello already sits in the buffered reader.
func (c *Conn) completeTLSHandshake(transport net.Conn, baseCfg *tls.Config, negotiation string) (*tls.Conn, error) {
	// Wrap the TLS config so dynamic-cert deployments (GetCertificate /
	// GetConfigForClient, typical for SNI multi-tenant edges) capture the
	// actually-selected leaf cert into c.tlsServerCert during the handshake.
	// Static Certificates[0] deployments are handled by the post-handshake
	// fallback below and skip the wrapper.
	handshakeCfg := wrapTLSConfigForCertCapture(baseCfg, c.captureTLSServerCert)

	// Perform TLS handshake. Time it for mg.gateway.tls.handshake.duration
	// and classify the outcome so the fleet-validation alert can tell a
	// crypto failure from a client tear-down. net.ErrClosed / io.EOF map
	// to client_aborted; everything else is treated as a handshake_failure.
	tlsConn := tls.Server(transport, handshakeCfg)
	handshakeStart := time.Now()
	if err := tlsConn.Handshake(); err != nil {
		outcome := TLSOutcomeHandshakeFailure
		if isClientAbortError(err) {
			outcome = TLSOutcomeClientAborted
		}
		c.metrics().RecordTLSHandshake(c.ctx, negotiation, outcome, time.Since(handshakeStart))
		return nil, fmt.Errorf("TLS handshake failed: %w", err)
	}
	handshakeDuration := time.Since(handshakeStart)
	c.metrics().RecordTLSHandshake(c.ctx, negotiation, TLSOutcomeSuccess, handshakeDuration)
	connState := tlsConn.ConnectionState()

	// Replace the underlying connection and reset the buffered reader
	// to read from the TLS connection. The buffered writer is nil during
	// startup (lazy init via startWriterBuffering), so writeRawByte
	// falls back to c.conn directly — after this swap, writes go
	// through TLS.
	c.conn = tlsConn
	c.bufferedReader.Reset(tlsConn)
	c.tlsHandshakeComplete = true

	// Static-cert fallback: when the deployment only populates Certificates,
	// the dynamic wrapper above is a no-op and the cert was not captured.
	// Recover it from Certificates[0] here. A parse failure is non-fatal —
	// auth simply falls back to SCRAM-SHA-256 without channel binding.
	if c.tlsServerCert == nil && len(baseCfg.Certificates) > 0 {
		cert := &baseCfg.Certificates[0]
		switch {
		case cert.Leaf != nil:
			c.tlsServerCert = cert.Leaf
		case len(cert.Certificate) > 0:
			if parsed, err := x509.ParseCertificate(cert.Certificate[0]); err == nil {
				c.tlsServerCert = parsed
			} else {
				c.logger.Warn("failed to parse TLS leaf cert for channel binding", "err", err)
			}
		}
	}

	c.logger.Info("TLS connection established",
		"negotiation", negotiation,
		"version", connState.Version,
		"cipher_suite", tls.CipherSuiteName(connState.CipherSuite))

	return tlsConn, nil
}

// handleGSSENCRequest handles a GSSAPI encryption request.
// We don't support GSSAPI encryption, so we always decline with 'N'.
// After declining, reads the next startup packet which may be a StartupMessage
// or an SSLRequest (per PostgreSQL protocol fallback ordering).
func (c *Conn) handleGSSENCRequest() error {
	c.gssDone = true

	c.logger.Debug("client requested GSSAPI encryption, declining")
	if err := c.writeRawByte('N'); err != nil {
		return fmt.Errorf("failed to send GSSENC response: %w", err)
	}
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush GSSENC response: %w", err)
	}

	// Read next packet — could be StartupMessage or SSLRequest (fallback).
	return c.readAndDispatchStartup()
}

// handleCancelRequest handles a query cancellation request.
// This is sent by clients to cancel a running query on another connection.
// Per PostgreSQL protocol, the cancel connection is always closed after processing.
func (c *Conn) handleCancelRequest(reader *MessageReader) error {
	// Read the process ID (connection ID).
	processID, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read process ID: %w", err)
	}

	// Read the secret key.
	secretKey, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read secret key: %w", err)
	}

	c.logger.Info("received cancel request", "process_id", processID)

	if c.listener.cancelHandler != nil {
		// Delegate to the cancel handler which may forward to a remote gateway.
		c.listener.cancelHandler.HandleCancelRequest(c.ctx, processID, secretKey)
	} else {
		// No cross-gateway handler (e.g., tests); try to cancel locally.
		c.listener.CancelLocalConnection(processID, secretKey)
	}

	return c.Close()
}

// splitOptionsTokens splits a PGOPTIONS string on unescaped whitespace.
// Backslash-escaped characters (e.g. `\ ` for a literal space) are preserved
// with the backslash removed.
func splitOptionsTokens(s string) []string {
	var tokens []string
	var cur strings.Builder
	escaped := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if escaped {
			cur.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == ' ' || ch == '\t' {
			if cur.Len() > 0 {
				tokens = append(tokens, cur.String())
				cur.Reset()
			}
			continue
		}
		cur.WriteByte(ch)
	}
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}
	return tokens
}

// parseOptions parses a PGOPTIONS string into individual key-value pairs.
// It supports:
//   - `-c key=value` and `-ckey=value`
//   - `--key=value` (hyphens in key converted to underscores)
//   - Multiple flags in a single string
func parseOptions(options string) (map[string]string, error) {
	tokens := splitOptionsTokens(options)
	result := make(map[string]string)

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		switch {
		case tok == "-c":
			// -c key=value (space-separated)
			i++
			if i >= len(tokens) {
				return nil, errors.New("missing value after -c")
			}
			key, value, ok := strings.Cut(tokens[i], "=")
			if !ok || key == "" {
				return nil, fmt.Errorf("invalid -c option: %q", tokens[i])
			}
			result[key] = value
		case strings.HasPrefix(tok, "-c"):
			// -ckey=value (no space)
			rest := tok[2:]
			key, value, ok := strings.Cut(rest, "=")
			if !ok || key == "" {
				return nil, fmt.Errorf("invalid -c option: %q", rest)
			}
			result[key] = value
		case strings.HasPrefix(tok, "--"):
			// --key=value
			rest := tok[2:]
			key, value, ok := strings.Cut(rest, "=")
			if !ok || key == "" {
				return nil, fmt.Errorf("invalid -- option: %q", tok)
			}
			// Convert hyphens to underscores in key.
			key = strings.ReplaceAll(key, "-", "_")
			result[key] = value
		default:
			return nil, fmt.Errorf("unsupported option flag: %q", tok)
		}
	}

	return result, nil
}

// handleStartupMessage processes a startup message and extracts connection parameters.
func (c *Conn) handleStartupMessage(protocolVersion uint32, reader *MessageReader) error {
	c.logger.Debug("parsing startup message", "protocol_version", protocolVersion)

	// Store the protocol version.
	c.protocolVersion = protocol.ProtocolVersion(protocolVersion)

	// Parse key-value pairs until we hit a null byte.
	for reader.Remaining() > 0 {
		// Read the key.
		key, err := reader.ReadString()
		if err != nil {
			return fmt.Errorf("failed to read parameter key: %w", err)
		}

		// Empty key means we've reached the end.
		if key == "" {
			break
		}

		// Read the value.
		value, err := reader.ReadString()
		if err != nil {
			return fmt.Errorf("failed to read parameter value for key %q: %w", key, err)
		}

		// Store the parameter.
		c.params[key] = value

		c.logger.Debug("startup parameter", "key", key, "value", value)
	}

	// Parse PGOPTIONS if present.
	if options, ok := c.params["options"]; ok {
		parsed, err := parseOptions(options)
		if err != nil {
			return fmt.Errorf("failed to parse options: %w", err)
		}
		// `replication` is a startup-phase-only protocol parameter; PG does
		// not accept it as a `-c replication=...` GUC inside PGOPTIONS.
		// Drop it here so the gateway doesn't honor a source PG would
		// reject. The auth gate is unaffected either way (rolreplication
		// is enforced when the direct startup field is set).
		delete(parsed, "replication")
		maps.Copy(c.params, parsed)
		delete(c.params, "options")
	}

	// Extract required parameters.
	c.user = c.params["user"]
	c.database = c.params["database"]

	// Default database to user if not specified.
	if c.database == "" {
		c.database = c.user
	}

	// Parse the optional `replication` startup parameter. Replication
	// connections (physical or logical) follow the same auth path but the
	// role must additionally satisfy pg_authid.rolreplication=true.
	// On parse failure, return the PgDiagnostic; serve()'s startup-error
	// path writes it to the client and closes the connection — matching
	// PG's behavior of rejecting unrecognized `replication` values before
	// authentication runs.
	//
	// Strip the key from c.params after parsing: `replication` is a
	// protocol-only startup parameter, not a GUC. Leaving it in the map
	// would let it flow through GetStartupParams → session settings →
	// `SET SESSION "replication" = ...` on the backend, which PG rejects
	// as unrecognized. The same reason `options` is deleted just above.
	replicationMode, err := parseReplicationMode(c.params["replication"])
	if err != nil {
		return err
	}
	delete(c.params, "replication")
	c.replicationMode = replicationMode

	c.logger.Info("startup message parsed",
		"user", c.user,
		"database", c.database,
		"replication", c.replicationMode != ReplicationOff)

	// Now perform authentication.
	return c.authenticate()
}

// errAuthRejected signals that the auth flow rejected the client and a FATAL
// message has already been written. The error propagates up to serve(),
// which recognizes it and closes the connection cleanly without entering
// the command loop or writing a second error frame. Without this propagation
// the connection would proceed to the command loop after a rejection — a
// well-behaved client closes after FATAL and the next read returns EOF, but
// a malicious or buggy client could attempt to send messages on a session
// where AuthenticationOk was never emitted and RegisterConn was never called.
//
// All sendAuthError-style helpers return this on a successful FATAL write so
// the post-auth completion sequence is skipped uniformly.
var errAuthRejected = errors.New("auth rejected; FATAL already sent")

// authenticate performs authentication with the client.
// If a TrustAuthProvider is configured and allows the user, trust auth is used.
// Otherwise, SCRAM-SHA-256 authentication is performed.
//
// On success, this also enforces post-auth role attribute checks (today
// rolreplication for replication startup connections) and emits the
// AuthenticationOk → BackendKeyData → ParameterStatus → ReadyForQuery
// completion sequence. The role-attribute check runs *before*
// AuthenticationOk; native PostgreSQL sequences the same check as
// SASLFinal → AuthenticationOk → (InitPostgres rolreplication check) → FATAL,
// so multigres collapses two server-to-client frames into one for rejected
// replication clients. libpq, pgx, and JDBC all accept ErrorResponse at this
// stage either way — the wire-visible difference is one fewer frame and no
// successful-handshake-then-rejection optic for the client.
func (c *Conn) authenticate() (err error) {
	// Track the outcome label for mg.gateway.auth.attempts. Each rejection
	// site below assigns a specific value before returning so we don't lose
	// the cause when sendAuthError-style helpers fold it into errAuthRejected.
	outcome := AuthOutcomeSuccess
	defer func() {
		c.metrics().RecordAuthAttempt(c.ctx, outcome)
	}()

	// Check if trust auth is allowed for this connection. errAuthRejected
	// is propagated unchanged so serve() can short-circuit out of the
	// startup phase without entering the command loop.
	if c.trustAuthProvider != nil && c.trustAuthProvider.AllowTrustAuth(c.ctx, c.user, c.database) {
		if err = c.authenticateTrust(); err != nil {
			outcome = classifyAuthError(err)
			return err
		}
	} else {
		// authenticateSCRAM returns a specific outcome label alongside
		// the error so this caller doesn't have to reverse-engineer it
		// from errAuthRejected (which folds the cause).
		var scramOutcome string
		scramOutcome, err = c.authenticateSCRAM()
		outcome = scramOutcome
		if err != nil {
			return err
		}
	}

	// Replication startup parameter requires rolreplication=true on the role.
	// Done post-auth so we don't leak which roles exist for unauthenticated
	// clients.
	if err = c.verifyReplicationRole(); err != nil {
		// SCRAM succeeded but the role lacks rolreplication. Tagged as
		// its own outcome so MUL-420's fleet validation alert can
		// distinguish role-attribute rejections from login_disabled
		// without scanning logs.
		outcome = AuthOutcomeReplicationRoleRequired
		return err
	}

	return c.finishAuth()
}

// classifyAuthError maps an auth-path error to the closed set of outcome
// labels used by mg.gateway.auth.attempts and mg.gateway.auth.scram.duration.
// Order matters: the SCRAM sentinels must be checked before the generic
// errAuthRejected fallback so cause-specific labels survive the wrapping
// done by sendAuthError / sendScramFatal.
func classifyAuthError(err error) string {
	switch {
	case err == nil:
		return AuthOutcomeSuccess
	case errors.Is(err, scram.ErrAuthenticationFailed):
		return AuthOutcomeBadPassword
	case errors.Is(err, scram.ErrUserNotFound):
		return AuthOutcomeUserNotFound
	case errors.Is(err, scram.ErrLoginDisabled):
		return AuthOutcomeLoginDisabled
	case errors.Is(err, scram.ErrPasswordExpired):
		return AuthOutcomePasswordExpired
	case errors.Is(err, scram.ErrSASLProtocol),
		errors.Is(err, scram.ErrChannelBindingNegotiation),
		errors.Is(err, scram.ErrChannelBindingCheck),
		errors.Is(err, scram.ErrAuthzidNotSupported):
		return AuthOutcomeProtocolError
	default:
		return AuthOutcomeInternal
	}
}

// authenticateTrust performs trust authentication (no password required).
// This is used in tests to simulate Unix socket trust authentication.
//
// Trust auth has no over-the-wire negotiation step before AuthenticationOk
// — the caller (authenticate) is responsible for the success sequence.
func (c *Conn) authenticateTrust() error {
	c.logger.Debug("authenticating client", "method", "trust")
	return nil
}

// finishAuth emits the post-authentication completion sequence shared by
// both trust and SCRAM paths: AuthenticationOk, BackendKeyData, the
// initial ParameterStatus run, and ReadyForQuery.
func (c *Conn) finishAuth() error {
	if err := c.sendAuthenticationOk(); err != nil {
		return fmt.Errorf("failed to send AuthenticationOk: %w", err)
	}

	// Send BackendKeyData for query cancellation.
	if err := c.sendBackendKeyData(); err != nil {
		return fmt.Errorf("failed to send BackendKeyData: %w", err)
	}

	// Register connection for cancel request lookup now that the client knows the PID.
	c.listener.RegisterConn(c)

	// Send initial ParameterStatus messages.
	if err := c.sendParameterStatuses(); err != nil {
		return fmt.Errorf("failed to send ParameterStatus messages: %w", err)
	}

	// Notify handlers that opted into the established hook *before*
	// sending ReadyForQuery. Otherwise the client returns from Connect
	// as soon as it sees ReadyForQuery, races ahead, and may observe
	// the conn before the hook records per-connection startup state.
	if h, ok := c.handler.(ConnectionEstablishedHandler); ok {
		h.ConnectionEstablished(c)
	}

	// Send ReadyForQuery to indicate we're ready to receive commands.
	if err := c.sendReadyForQuery(); err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	c.logger.Info("authentication complete", "user", c.user)
	return nil
}

// verifyReplicationRole enforces pg_authid.rolreplication for clients that
// requested a replication startup connection (replication=true /
// replication=database). Skipped for normal sessions.
//
// The flag was fetched alongside the SCRAM hash during authenticateSCRAM
// and cached on c.credentials, so this gate is a constant-time field
// check rather than a second pooler round-trip.
//
// For the trust-auth path c.credentials is unset, so we fall back to
// fetching from the credential provider here. Trust auth is test-only.
//
// Mismatches produce PG's exact wording with SQLSTATE 42501, matching
// what native PostgreSQL emits in walsender startup. The error is sent
// FATAL so libpq tears down the connection.
func (c *Conn) verifyReplicationRole() error {
	if c.replicationMode == ReplicationOff {
		return nil
	}

	// Use cached credentials from SCRAM if available.
	if c.credentials != nil {
		if !c.credentials.IsReplicationRole {
			c.logger.Warn("authentication failed: role lacks rolreplication",
				"user", c.user)
			return c.sendReplicationRoleError()
		}
		return nil
	}

	// Trust-auth path: no SCRAM lookup happened, so we need to fetch the
	// flag here. Without a credential provider configured, fail closed.
	if c.credentialProvider == nil {
		c.logger.Warn("rejecting replication connection: no credential provider configured",
			"user", c.user)
		return c.sendReplicationRoleError()
	}

	creds, err := c.credentialProvider.GetCredentials(c.ctx, c.user, c.database)
	if err != nil {
		// Lookup failure (including ErrUserNotFound / ErrLoginDisabled /
		// ErrPasswordExpired): fail closed with a generic FATAL so we
		// don't leak which roles exist. Operators see the underlying
		// error in the logs.
		c.logger.Error("replication role verification failed",
			"user", c.user, "error", err)
		return c.sendReplicationRoleError()
	}
	if !creds.IsReplicationRole {
		c.logger.Warn("authentication failed: role lacks rolreplication",
			"user", c.user)
		return c.sendReplicationRoleError()
	}
	c.credentials = creds
	return nil
}

// sendReplicationRoleError emits PG's exact wording for a replication-role
// rejection. SQLSTATE 42501 (insufficient_privilege) matches the error
// PostgreSQL raises in walsender setup when the role lacks rolreplication
// and is not a superuser. Returns errAuthRejected on success.
func (c *Conn) sendReplicationRoleError() error {
	if err := c.writeError(mterrors.NewPgError(
		"FATAL", mterrors.PgSSInsufficientPrivilege,
		"must be superuser or replication role to start walsender",
		"",
	)); err != nil {
		return err
	}
	if err := c.flush(); err != nil {
		return err
	}
	return errAuthRejected
}

// authenticateSCRAM performs SCRAM-SHA-256 authentication with the client.
//
// Credentials are fetched once up front via the credential provider; the
// SCRAM hash drives the handshake and the IsReplicationRole flag is cached
// on the connection for the later post-auth gate, so one lookup suffices
// for both. Lookup-time sentinels (login-disabled, expired, missing user)
// are mapped to the matching native-PG error here, before any SASL frames
// are emitted, so we don't reveal which case applied.
//
// Returns the outcome label for mg.gateway.auth.attempts alongside the
// error. The label survives the errAuthRejected wrapping that
// sendAuthError-style helpers apply, so the caller does not have to
// reverse-engineer the cause from the returned error. The SCRAM handshake
// duration (client-first to server-final) is recorded only on paths that
// actually exchange SASL frames; credential-lookup failures exit before
// any frame is emitted and are observed via the separate
// mg.gateway.auth.credential_lookup.duration metric.
func (c *Conn) authenticateSCRAM() (outcome string, err error) {
	c.logger.Debug("authenticating client", "method", "scram-sha-256")

	creds, err := c.credentialProvider.GetCredentials(c.ctx, c.user, c.database)
	if err != nil {
		// rolcanlogin=false: emit PG's exact wording with SQLSTATE 28000
		// (invalid_authorization_specification), matching native PG.
		if errors.Is(err, scram.ErrLoginDisabled) {
			c.logger.Warn("authentication failed: role not permitted to log in", "user", c.user)
			return AuthOutcomeLoginDisabled, c.sendLoginDisabledError()
		}
		// Expired rolvaliduntil and unknown-user both surface as the opaque
		// "password authentication failed" message (28P01), matching PG's
		// convention of not disclosing why auth failed.
		if errors.Is(err, scram.ErrUserNotFound) {
			c.logger.Warn("authentication failed: user not found", "user", c.user)
			return AuthOutcomeUserNotFound, c.sendAuthError("password authentication failed for user \"" + c.user + "\"")
		}
		if errors.Is(err, scram.ErrPasswordExpired) {
			c.logger.Warn("authentication failed: password expired", "user", c.user)
			return AuthOutcomePasswordExpired, c.sendAuthError("password authentication failed for user \"" + c.user + "\"")
		}
		// Generic credential-lookup failure. If the upstream returned a
		// PgDiagnostic (e.g. "planned failover in progress" from the
		// pooler), forward it so the client can distinguish a transient
		// cluster condition from a wrong password and act accordingly
		// (retry, alert, etc.). For all other errors (transport, parse,
		// pooler unreachable) fail closed with the opaque password-auth
		// message so the client does not learn whether the user exists.
		c.logger.Error("credential lookup failed", "user", c.user, "error", err)
		var pgDiag *mterrors.PgDiagnostic
		if errors.As(err, &pgDiag) {
			// Auth-phase errors must be FATAL to signal connection teardown.
			fatal := *pgDiag
			fatal.Severity = "FATAL"
			if werr := c.writeError(&fatal); werr != nil {
				return AuthOutcomeLookupError, werr
			}
			if werr := c.flush(); werr != nil {
				return AuthOutcomeLookupError, werr
			}
			return AuthOutcomeLookupError, errAuthRejected
		}
		return AuthOutcomeLookupError, c.sendAuthError("password authentication failed for user \"" + c.user + "\"")
	}
	c.credentials = creds

	// Start the SCRAM handshake timer here so the histogram captures only
	// the SASL exchange (client-first → server-final), not the upstream
	// credential lookup which has its own metric.
	scramStart := time.Now()
	defer func() {
		c.metrics().RecordSCRAMDuration(c.ctx, outcome, time.Since(scramStart))
	}()

	// Create the SCRAM authenticator with the pre-fetched hash.
	auth := scram.NewScramAuthenticator(creds.Hash, c.database)

	// Tell the authenticator whether we're over TLS regardless of whether
	// we manage to compute a cbind hash — the downgrade-attempt detection
	// (gs2 flag "y") must fire on any TLS connection.
	auth.SetOverTLS(c.tlsHandshakeComplete)

	// Attach channel binding context when the connection is over TLS so the
	// authenticator advertises SCRAM-SHA-256-PLUS in addition to SCRAM-SHA-256.
	// Plaintext sessions skip this and continue to advertise SCRAM-SHA-256 only.
	if c.tlsServerCert != nil {
		cbHash, hashErr := scram.ComputeTLSServerEndPointHash(c.tlsServerCert)
		if hashErr != nil {
			// Don't fail the connection — log and fall back to plain
			// SCRAM-SHA-256 so unusual certs (e.g. unsupported signature
			// algorithms) still permit auth, matching PG's permissive
			// behavior. The downgrade-detection gate on overTLS still
			// fires here.
			c.logger.Warn("failed to compute tls-server-end-point hash, falling back to SCRAM-SHA-256 only", "err", hashErr)
		} else {
			auth.SetChannelBinding(&scram.ChannelBinding{TLSServerEndPointHash: cbHash})
		}
	}

	// Send AuthenticationSASL with supported mechanisms.
	mechanisms := auth.StartAuthentication()
	advertised := make(map[string]struct{}, len(mechanisms))
	for _, m := range mechanisms {
		advertised[m] = struct{}{}
	}
	if err := c.sendAuthenticationSASL(mechanisms); err != nil {
		return AuthOutcomeInternal, fmt.Errorf("failed to send AuthenticationSASL: %w", err)
	}
	if err := c.flush(); err != nil {
		return AuthOutcomeInternal, fmt.Errorf("failed to flush AuthenticationSASL: %w", err)
	}

	// Read SASLInitialResponse (chosen mechanism + client-first-message).
	selectedMechanism, clientFirstMessage, err := c.readSASLInitialResponse(advertised)
	if err != nil {
		return AuthOutcomeProtocolError, fmt.Errorf("failed to read SASLInitialResponse: %w", err)
	}

	// Process client-first-message and generate server-first-message.
	// Pass the username from the startup message as fallback for clients that
	// send empty username in SCRAM (like pgx).
	serverFirstMessage, err := auth.HandleClientFirst(selectedMechanism, clientFirstMessage, c.user)
	if err != nil {
		if handled, ferr := c.mapSCRAMProtocolError(err); handled {
			c.logger.Warn("authentication failed: SCRAM protocol violation in client-first", "user", c.user, "err", err)
			return AuthOutcomeProtocolError, ferr
		}
		return AuthOutcomeProtocolError, fmt.Errorf("failed to handle client-first-message: %w", err)
	}

	// Send AuthenticationSASLContinue with server-first-message.
	if err := c.sendAuthenticationSASLContinue(serverFirstMessage); err != nil {
		return AuthOutcomeInternal, fmt.Errorf("failed to send AuthenticationSASLContinue: %w", err)
	}
	if err := c.flush(); err != nil {
		return AuthOutcomeInternal, fmt.Errorf("failed to flush AuthenticationSASLContinue: %w", err)
	}

	// Read SASLResponse (contains client-final-message).
	clientFinalMessage, err := c.readSASLResponse()
	if err != nil {
		return AuthOutcomeProtocolError, fmt.Errorf("failed to read SASLResponse: %w", err)
	}

	// Verify client proof and generate server signature.
	serverFinalMessage, err := auth.HandleClientFinal(clientFinalMessage)
	if err != nil {
		if errors.Is(err, scram.ErrAuthenticationFailed) {
			c.logger.Warn("authentication failed: invalid password", "user", c.user)
			return AuthOutcomeBadPassword, c.sendAuthError("password authentication failed for user \"" + c.user + "\"")
		}
		if handled, ferr := c.mapSCRAMProtocolError(err); handled {
			c.logger.Warn("authentication failed: SCRAM protocol violation in client-final", "user", c.user, "err", err)
			return AuthOutcomeProtocolError, ferr
		}
		return AuthOutcomeProtocolError, fmt.Errorf("failed to handle client-final-message: %w", err)
	}

	// Capture keys for SCRAM passthrough to the backing PostgreSQL.
	c.scramClientKey, c.scramServerKey = auth.ExtractedKeys()

	// Send AuthenticationSASLFinal with server signature. SCRAM ends here;
	// the caller (authenticate) continues with the post-auth role-attribute
	// check and the AuthenticationOk → ReadyForQuery completion sequence.
	if err := c.sendAuthenticationSASLFinal(serverFinalMessage); err != nil {
		return AuthOutcomeInternal, fmt.Errorf("failed to send AuthenticationSASLFinal: %w", err)
	}

	c.logger.Debug("scram authentication succeeded", "user", c.user)
	return AuthOutcomeSuccess, nil
}

// sendAuthenticationSASL sends AuthenticationSASL message with supported mechanisms.
func (c *Conn) sendAuthenticationSASL(mechanisms []string) error {
	bodyLen := 4 // AuthSASL int32
	for _, mech := range mechanisms {
		bodyLen += len(mech) + 1
	}
	bodyLen++ // Trailing null terminator for the mechanism list.
	buf, pos := c.startPacket(protocol.MsgAuthenticationRequest, bodyLen)
	pos = writeInt32At(buf, pos, protocol.AuthSASL)
	for _, mech := range mechanisms {
		pos = writeStringAt(buf, pos, mech)
	}
	pos = writeByteAt(buf, pos, 0)
	return c.writePacket(buf, pos)
}

// sendAuthenticationSASLContinue sends AuthenticationSASLContinue with server data.
func (c *Conn) sendAuthenticationSASLContinue(data string) error {
	bodyLen := 4 + len(data)
	buf, pos := c.startPacket(protocol.MsgAuthenticationRequest, bodyLen)
	pos = writeInt32At(buf, pos, protocol.AuthSASLContinue)
	pos = writeBytesAt(buf, pos, []byte(data))
	return c.writePacket(buf, pos)
}

// sendAuthenticationSASLFinal sends AuthenticationSASLFinal with server signature.
func (c *Conn) sendAuthenticationSASLFinal(data string) error {
	bodyLen := 4 + len(data)
	buf, pos := c.startPacket(protocol.MsgAuthenticationRequest, bodyLen)
	pos = writeInt32At(buf, pos, protocol.AuthSASLFinal)
	pos = writeBytesAt(buf, pos, []byte(data))
	return c.writePacket(buf, pos)
}

// readSASLInitialResponse reads SASLInitialResponse from the client.
// Returns the SASL mechanism the client picked and the SASL data
// (client-first-message for SCRAM). The mechanism is validated against the
// set the server advertised — anything else is rejected so a malicious or
// confused client can't downgrade past what was offered.
func (c *Conn) readSASLInitialResponse(advertised map[string]struct{}) (string, string, error) {
	msgType, err := c.ReadMessageType()
	if err != nil {
		return "", "", fmt.Errorf("failed to read message type: %w", err)
	}
	if msgType != protocol.MsgPasswordMsg {
		return "", "", fmt.Errorf("expected SASLInitialResponse ('p'), got '%c'", msgType)
	}

	length, err := c.ReadMessageLength()
	if err != nil {
		return "", "", fmt.Errorf("failed to read message length: %w", err)
	}

	body, err := c.readMessageBody(length)
	if err != nil {
		return "", "", fmt.Errorf("failed to read message body: %w", err)
	}
	defer c.returnReadBuffer()

	reader := NewMessageReader(body)

	// Read mechanism name.
	mechanism, err := reader.ReadString()
	if err != nil {
		return "", "", fmt.Errorf("failed to read mechanism: %w", err)
	}
	if _, ok := advertised[mechanism]; !ok {
		return "", "", fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}

	// Read data length.
	dataLen, err := reader.ReadInt32()
	if err != nil {
		return "", "", fmt.Errorf("failed to read data length: %w", err)
	}

	// Handle case where client sends no initial data (length = -1).
	// This shouldn't happen for SCRAM-SHA-256, but some clients may do this.
	if dataLen == -1 {
		return "", "", errors.New("client sent SASLInitialResponse with no initial data (length=-1)")
	}
	if dataLen < 0 {
		return "", "", fmt.Errorf("invalid SASL data length: %d", dataLen)
	}

	// Read SASL data.
	data, err := reader.ReadBytes(int(dataLen))
	if err != nil {
		return "", "", fmt.Errorf("failed to read SASL data: %w", err)
	}

	return mechanism, string(data), nil
}

// readSASLResponse reads SASLResponse from the client.
// Returns the SASL data (client-final-message for SCRAM).
func (c *Conn) readSASLResponse() (string, error) {
	msgType, err := c.ReadMessageType()
	if err != nil {
		return "", fmt.Errorf("failed to read message type: %w", err)
	}
	if msgType != protocol.MsgPasswordMsg {
		return "", fmt.Errorf("expected SASLResponse ('p'), got '%c'", msgType)
	}

	length, err := c.ReadMessageLength()
	if err != nil {
		return "", fmt.Errorf("failed to read message length: %w", err)
	}

	body, err := c.readMessageBody(length)
	if err != nil {
		return "", fmt.Errorf("failed to read message body: %w", err)
	}
	defer c.returnReadBuffer()

	// The entire body is the SASL data.
	return string(body), nil
}

// sendAuthError sends a FATAL authentication-failure response. On a successful
// write it returns errAuthRejected so authenticate() short-circuits the
// post-auth completion sequence; an actual write/flush error is propagated.
func (c *Conn) sendAuthError(message string) error {
	if err := c.writeError(mterrors.NewPgError("FATAL", mterrors.PgSSAuthFailed, message, "")); err != nil {
		return err
	}
	if err := c.flush(); err != nil {
		return err
	}
	return errAuthRejected
}

// sendLoginDisabledError sends the FATAL error PostgreSQL emits when a role
// with rolcanlogin=false attempts to authenticate (SQLSTATE 28000). The
// message format matches native PG verbatim so libpq-compatible clients
// parse it identically. Returns errAuthRejected on success.
func (c *Conn) sendLoginDisabledError() error {
	msg := "role \"" + c.user + "\" is not permitted to log in"
	if err := c.writeError(mterrors.NewPgError("FATAL", mterrors.PgSSInvalidAuthSpec, msg, "")); err != nil {
		return err
	}
	if err := c.flush(); err != nil {
		return err
	}
	return errAuthRejected
}

// mapSCRAMProtocolError converts a SCRAM authenticator error into the exact
// PostgreSQL-style FATAL the client expects to see on the wire.
//
// Returns handled=true iff the error matched a SCRAM protocol class and a
// FATAL has been written. The caller MUST then return ferr up the stack so
// authenticate() short-circuits — ferr is errAuthRejected on success, or the
// underlying write error on failure.
//
// Returns handled=false, nil when the error is not a SCRAM protocol error
// — caller should continue with other classifications.
//
// SQLSTATE + message mapping comes straight from PG17 auth-scram.c so libpq
// and any other PG-compatible client see identical diagnostics.
func (c *Conn) mapSCRAMProtocolError(err error) (handled bool, ferr error) {
	if err == nil {
		return false, nil
	}
	switch {
	case errors.Is(err, scram.ErrChannelBindingNegotiation):
		return true, c.sendScramFatal(mterrors.PgSSInvalidAuthSpec,
			"SCRAM channel binding negotiation error",
			"The client supports SCRAM channel binding but thinks the server does not.  However, this server does support channel binding.")
	case errors.Is(err, scram.ErrChannelBindingCheck):
		return true, c.sendScramFatal(mterrors.PgSSInvalidAuthSpec,
			"SCRAM channel binding check failed", "")
	case errors.Is(err, scram.ErrAuthzidNotSupported):
		return true, c.sendScramFatal(mterrors.PgSSFeatureNotSupported,
			"client uses authorization identity, but it is not supported", "")
	case errors.Is(err, scram.ErrSASLProtocol):
		var sp *scram.SASLProtocolError
		if errors.As(err, &sp) {
			msg := sp.Msg
			if msg == "" {
				msg = "malformed SCRAM message"
			}
			return true, c.sendScramFatal(mterrors.PgSSProtocolViolation, msg, sp.Detail)
		}
		return true, c.sendScramFatal(mterrors.PgSSProtocolViolation, "malformed SCRAM message", "")
	}
	return false, nil
}

// sendScramFatal emits a FATAL ErrorResponse with the supplied SQLSTATE,
// errmsg, and (optional) errdetail. Returns errAuthRejected on success so
// authenticate() short-circuits, matching the sendAuthError pattern.
func (c *Conn) sendScramFatal(sqlState, msg, detail string) error {
	if err := c.writeError(mterrors.NewPgError("FATAL", sqlState, msg, detail)); err != nil {
		return err
	}
	if err := c.flush(); err != nil {
		return err
	}
	return errAuthRejected
}

// sendAuthenticationOk sends an AuthenticationOk message to the client.
func (c *Conn) sendAuthenticationOk() error {
	buf, pos := c.startPacket(protocol.MsgAuthenticationRequest, 4)
	pos = writeInt32At(buf, pos, protocol.AuthOk)
	return c.writePacket(buf, pos)
}

// sendBackendKeyData sends the BackendKeyData message.
// This contains the process ID (connection ID) and secret key for query cancellation.
func (c *Conn) sendBackendKeyData() error {
	buf, pos := c.startPacket(protocol.MsgBackendKeyData, 8)
	pos = writeUint32At(buf, pos, c.connectionID)
	pos = writeUint32At(buf, pos, c.backendKeyData)
	return c.writePacket(buf, pos)
}

// sendParameterStatuses sends initial ParameterStatus messages to the client.
// These inform the client about server settings.
func (c *Conn) sendParameterStatuses() error {
	// Send standard parameters that clients expect.
	parameters := map[string]string{
		"server_version":              "17.0 (multigres)", // Pretend to be PostgreSQL 17
		"server_encoding":             "UTF8",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
	}

	for key, value := range parameters {
		if err := c.sendParameterStatus(key, value); err != nil {
			return err
		}
	}

	return nil
}

// sendParameterStatus sends a single ParameterStatus message and records the
// value as the one the client now holds, so reportParameterStatus can tell a
// real change from a no-op.
func (c *Conn) sendParameterStatus(name, value string) error {
	bodyLen := len(name) + 1 + len(value) + 1
	buf, pos := c.startPacket(protocol.MsgParameterStatus, bodyLen)
	pos = writeStringAt(buf, pos, name)
	pos = writeStringAt(buf, pos, value)
	if err := c.writePacket(buf, pos); err != nil {
		return err
	}
	if c.reportedParams == nil {
		c.reportedParams = make(map[string]string)
	}
	c.reportedParams[name] = value
	return nil
}

// reportParameterStatus sends a ParameterStatus only when value differs from
// what this connection last told the client, mirroring PostgreSQL: a GUC_REPORT
// parameter is re-reported on change, not on every SET that names it. A
// parameter with no recorded value (never sent at startup, e.g. application_name)
// is always reported the first time.
func (c *Conn) reportParameterStatus(name, value string) error {
	if prev, ok := c.reportedParams[name]; ok && prev == value {
		return nil
	}
	return c.sendParameterStatus(name, value)
}

// isClientAbortError reports whether a TLS handshake error looks like the
// client closed the connection (network teardown, EOF, or use of an already-
// closed socket) rather than a server-side or crypto failure. Used to split
// the tls.handshake.duration outcome label so the fleet-validation alert
// can suppress noise from clients that hang up mid-handshake.
//
// Timeouts are intentionally excluded: a deadline-exceeded error during
// the TLS handshake is ambiguous between a stalled client and the server's
// own authentication_timeout firing on c.conn. Tagging timeouts as
// handshake_failure (the default) is more accurate than blaming the
// client; operators alerting on a sustained handshake_failure rate will
// still spot stalled-client patterns via the timing distribution.
func isClientAbortError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	return false
}

// sendReadyForQuery sends a ReadyForQuery message to indicate the server is ready.
func (c *Conn) sendReadyForQuery() error {
	if err := c.writeReadyForQuery(); err != nil {
		return err
	}
	// Flush to ensure the client receives the message immediately.
	return c.flush()
}
