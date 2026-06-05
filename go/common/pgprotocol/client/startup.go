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

package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"slices"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// startup performs the connection startup handshake.
// This includes SSL negotiation (if configured), sending the startup message,
// and handling authentication.
func (c *Conn) startup(ctx context.Context) error {
	// Honor libpq-style sslmode. Unix-socket connections always run plaintext,
	// matching libpq behavior.
	if c.config.SocketFile == "" && c.config.SSLMode.AttemptsTLS() {
		if err := c.negotiateSSL(ctx); err != nil {
			return fmt.Errorf("SSL negotiation failed: %w", err)
		}
	}

	// Send the startup message.
	if err := c.sendStartupMessage(); err != nil {
		return fmt.Errorf("failed to send startup message: %w", err)
	}

	// Process authentication and startup responses.
	if err := c.processStartupResponses(ctx); err != nil {
		return err
	}

	return nil
}

// negotiateSSL implements the libpq SSLRequest handshake and, on server
// acceptance, upgrades the underlying connection to TLS using c.config.TLSConfig.
//
// Caller guarantees c.config.SSLMode.AttemptsTLS() is true and c.config.SocketFile
// is empty. On a tolerant mode (prefer) where the server returns 'N', this
// returns nil and leaves the plaintext connection in place so startup can
// continue. Strict modes (require, verify-ca, verify-full) error on 'N'.
func (c *Conn) negotiateSSL(ctx context.Context) error {
	if err := c.writeSSLRequest(); err != nil {
		return fmt.Errorf("failed to send SSL request: %w", err)
	}
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush SSL request: %w", err)
	}

	response, err := c.bufferedReader.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read SSL response: %w", err)
	}

	switch response {
	case 'N':
		if c.config.SSLMode.RequiresTLS() {
			return fmt.Errorf("server does not support SSL but sslmode=%s requires it", c.config.SSLMode)
		}
		// prefer: continue on plaintext.
		return nil
	case 'S':
		// fall through to TLS upgrade.
	case 'E':
		// PostgreSQL may respond with an ErrorResponse if SSLRequest is
		// rejected outright (e.g. unsupported protocol version on the
		// server). The body is on the buffered reader following the 'E'
		// message-type byte; surface a clear failure rather than trying
		// to parse it here.
		return errors.New("server returned ErrorResponse to SSLRequest")
	default:
		return fmt.Errorf("unexpected SSL response: %q", response)
	}

	// Server accepted TLS — upgrade in place. The buffered reader must not
	// hold any post-'S' bytes, since they would belong to the TLS record
	// stream and not the plaintext connection.
	if c.bufferedReader.Buffered() > 0 {
		return errors.New("unexpected data after SSL acceptance")
	}
	if c.config.TLSConfig == nil {
		return errors.New("TLS config is nil but sslmode requested SSL")
	}

	tlsConn := tls.Client(c.conn, c.config.TLSConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("TLS handshake failed: %w", err)
	}
	c.conn = tlsConn
	c.bufferedReader = bufio.NewReaderSize(tlsConn, connBufferSize)
	c.bufferedWriter = bufio.NewWriterSize(tlsConn, connBufferSize)
	return nil
}

// sendStartupMessage sends the startup message to the server.
//
// The startup packet has no message type byte — only a 4-byte length
// (including itself) followed by the body — so it's encoded inline
// here instead of through startPacket (which always writes a 5-byte
// type+length header).
//
// Called only during single-threaded connection setup, so it does not
// acquire bufMu (no other writer can race on this connection yet).
func (c *Conn) sendStartupMessage() error {
	// Pre-compute body length so we can encode in place.
	bodyLen := 4 // protocol version
	bodyLen += len("user") + 1 + len(c.config.User) + 1
	if c.config.Database != "" {
		bodyLen += len("database") + 1 + len(c.config.Database) + 1
	}
	for key, value := range c.config.Parameters {
		bodyLen += len(key) + 1 + len(value) + 1
	}
	bodyLen++ // trailing null terminator for the parameter list

	totalLen := 4 + bodyLen // length field includes itself

	// Reserve a buffer: AvailableBuffer fast path, plain make slow path.
	// We don't borrow from bufPool here because startup messages are
	// small and one-shot per connection — pinning a 16 KB pool bucket
	// for a few hundred bytes wastes memory.
	var buf []byte
	if avail := c.bufferedWriter.AvailableBuffer(); cap(avail) >= totalLen {
		buf = avail[:totalLen]
	} else {
		buf = make([]byte, totalLen)
	}

	pos := 0
	pos = writeUint32At(buf, pos, uint32(totalLen))
	pos = writeUint32At(buf, pos, protocol.ProtocolVersionNumber)
	pos = writeStringAt(buf, pos, "user")
	pos = writeStringAt(buf, pos, c.config.User)
	if c.config.Database != "" {
		pos = writeStringAt(buf, pos, "database")
		pos = writeStringAt(buf, pos, c.config.Database)
	}
	for key, value := range c.config.Parameters {
		pos = writeStringAt(buf, pos, key)
		pos = writeStringAt(buf, pos, value)
	}
	writeByteAt(buf, pos, 0)

	if _, err := c.bufferedWriter.Write(buf); err != nil {
		return err
	}
	return c.flush()
}

// processStartupResponses processes all messages until ReadyForQuery.
func (c *Conn) processStartupResponses(ctx context.Context) error {
	for {
		// Check context.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read message.
		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		// Process based on message type.
		switch msgType {
		case protocol.MsgAuthenticationRequest:
			if err := c.handleAuthenticationRequest(body); err != nil {
				return err
			}

		case protocol.MsgBackendKeyData:
			if err := c.handleBackendKeyData(body); err != nil {
				return err
			}

		case protocol.MsgParameterStatus:
			if err := c.handleParameterStatus(body); err != nil {
				return err
			}

		case protocol.MsgReadyForQuery:
			if err := c.handleReadyForQuery(body); err != nil {
				return err
			}
			// Startup complete.
			return nil

		case protocol.MsgErrorResponse:
			return c.parseError(body)

		case protocol.MsgNoticeResponse:
			// Ignore notices during startup.

		default:
			return fmt.Errorf("unexpected message type during startup: %c (0x%02x)", msgType, msgType)
		}
	}
}

// handleAuthenticationRequest handles an AuthenticationRequest message.
func (c *Conn) handleAuthenticationRequest(body []byte) error {
	if len(body) < 4 {
		return errors.New("authentication message too short")
	}

	reader := NewMessageReader(body)
	authType, err := reader.ReadInt32()
	if err != nil {
		return fmt.Errorf("failed to read auth type: %w", err)
	}

	switch authType {
	case protocol.AuthOk:
		// Authentication successful, nothing more to do.
		return nil

	case protocol.AuthCleartextPassword:
		return errors.New("server requested cleartext password authentication, which is not supported for security reasons")

	case protocol.AuthMD5Password:
		return errors.New("server requested MD5 password authentication, which is not supported for security reasons")

	case protocol.AuthSASL:
		// Read available SASL mechanisms.
		var mechanisms []string
		for reader.Remaining() > 0 {
			mech, err := reader.ReadString()
			if err != nil {
				return fmt.Errorf("failed to read SASL mechanism: %w", err)
			}
			if mech == "" {
				break
			}
			mechanisms = append(mechanisms, mech)
		}

		// Check if SCRAM-SHA-256 is supported.
		if !slices.Contains(mechanisms, "SCRAM-SHA-256") {
			return fmt.Errorf("server does not support SCRAM-SHA-256 (available: %v)", mechanisms)
		}

		// Prefer SCRAM passthrough when keys are present; otherwise fall back
		// to password-based SCRAM. Passthrough lets a proxy authenticate on a
		// session's behalf without ever holding the plaintext password.
		var scramClient *scramClient
		if len(c.config.ScramClientKey) > 0 && len(c.config.ScramServerKey) > 0 {
			scramClient = newScramClientWithKeys(c, c.config.User, c.config.ScramClientKey, c.config.ScramServerKey)
		} else {
			scramClient = newScramClient(c, c.config.User, c.config.Password)
		}
		return scramClient.authenticate()

	default:
		return fmt.Errorf("unsupported authentication method: %d", authType)
	}
}

// handleBackendKeyData handles a BackendKeyData message.
func (c *Conn) handleBackendKeyData(body []byte) error {
	if len(body) < 8 {
		return errors.New("backend key data message too short")
	}

	reader := NewMessageReader(body)

	processID, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read process ID: %w", err)
	}

	secretKey, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read secret key: %w", err)
	}

	c.processID = processID
	c.secretKey = secretKey
	return nil
}

// handleParameterStatus handles a ParameterStatus message.
func (c *Conn) handleParameterStatus(body []byte) error {
	reader := NewMessageReader(body)

	name, err := reader.ReadString()
	if err != nil {
		return fmt.Errorf("failed to read parameter name: %w", err)
	}

	value, err := reader.ReadString()
	if err != nil {
		return fmt.Errorf("failed to read parameter value: %w", err)
	}

	c.serverParams[name] = value
	return nil
}

// handleReadyForQuery handles a ReadyForQuery message.
func (c *Conn) handleReadyForQuery(body []byte) error {
	if len(body) < 1 {
		return errors.New("ready for query message too short")
	}

	c.txnStatus = protocol.TransactionStatus(body[0])
	return nil
}

// writeSSLRequest writes an SSL negotiation request.
//
// SSLRequest has no message type byte — just length (8) +
// SSLRequestCode. Encoded as a single 8-byte slice into the
// bufferedWriter's available space so it goes out in one Write.
//
// Called only during single-threaded connection setup, so it does not
// acquire bufMu (no other writer can race on this connection yet).
func (c *Conn) writeSSLRequest() error {
	// 8-byte request: AvailableBuffer fast path, stack-local fallback.
	// bufPool's smallest bucket is 16 KB — vastly oversized for an
	// 8-byte one-shot request — so we use a fixed-size array on the
	// slow path instead of borrowing from the pool.
	var stack [8]byte
	avail := c.bufferedWriter.AvailableBuffer()
	var buf []byte
	if cap(avail) >= 8 {
		buf = avail[:8]
	} else {
		buf = stack[:]
	}
	pos := 0
	pos = writeUint32At(buf, pos, 8)
	writeUint32At(buf, pos, protocol.SSLRequestCode)
	_, err := c.bufferedWriter.Write(buf)
	return err
}
