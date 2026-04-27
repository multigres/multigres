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
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// startup performs the connection startup handshake.
// This includes SSL negotiation (if configured), sending the startup message,
// and handling authentication.
func (c *Conn) startup(ctx context.Context) error {
	// Handle SSL if configured.
	if c.config.TLSConfig != nil {
		if err := c.negotiateSSL(); err != nil {
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

// negotiateSSL requests SSL from the server.
func (c *Conn) negotiateSSL() error {
	// Send SSLRequest message.
	if err := c.writeSSLRequest(); err != nil {
		return fmt.Errorf("failed to send SSL request: %w", err)
	}
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush SSL request: %w", err)
	}

	// Read the server's response (single byte: 'S' or 'N').
	response, err := c.bufferedReader.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read SSL response: %w", err)
	}

	if response == 'N' {
		return errors.New("server does not support SSL")
	}
	if response != 'S' {
		return fmt.Errorf("unexpected SSL response: %c", response)
	}

	// Upgrade to TLS.
	// TODO: Implement TLS upgrade when needed.
	return errors.New("TLS upgrade not yet implemented")
}

// sendStartupMessage sends the startup message to the server.
//
// The startup packet has no message type byte — only a 4-byte length
// (including itself) followed by the body — so it's encoded inline
// here instead of through startPacket (which always writes a 5-byte
// type+length header).
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

	// Reserve a buffer: AvailableBuffer fast path, bufpool slow path.
	var buf []byte
	var poolBuf *[]byte
	if avail := c.bufferedWriter.AvailableBuffer(); cap(avail) >= totalLen {
		buf = avail[:totalLen]
	} else {
		poolBuf = bufPool.Get(totalLen)
		buf = *poolBuf
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
	pos = writeByteAt(buf, pos, 0)
	_ = pos

	if _, err := c.bufferedWriter.Write(buf); err != nil {
		if poolBuf != nil {
			bufPool.Put(poolBuf)
		}
		return err
	}
	if poolBuf != nil {
		bufPool.Put(poolBuf)
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

		// Perform SCRAM-SHA-256 authentication.
		scram := newScramClient(c, c.config.User, c.config.Password)
		return scram.authenticate()

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
func (c *Conn) writeSSLRequest() error {
	avail := c.bufferedWriter.AvailableBuffer()
	var buf []byte
	var poolBuf *[]byte
	if cap(avail) >= 8 {
		buf = avail[:8]
	} else {
		poolBuf = bufPool.Get(8)
		buf = *poolBuf
	}
	pos := 0
	pos = writeUint32At(buf, pos, 8)
	pos = writeUint32At(buf, pos, protocol.SSLRequestCode)
	_ = pos
	_, err := c.bufferedWriter.Write(buf)
	if poolBuf != nil {
		bufPool.Put(poolBuf)
	}
	return err
}
