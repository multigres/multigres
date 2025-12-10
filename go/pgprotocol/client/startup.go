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
	"crypto/md5" //nolint:gosec // MD5 is required by PostgreSQL's legacy authentication protocol
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/multigres/multigres/go/pgprotocol/protocol"
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
		return fmt.Errorf("server does not support SSL")
	}
	if response != 'S' {
		return fmt.Errorf("unexpected SSL response: %c", response)
	}

	// Upgrade to TLS.
	// TODO: Implement TLS upgrade when needed.
	return fmt.Errorf("TLS upgrade not yet implemented")
}

// sendStartupMessage sends the startup message to the server.
func (c *Conn) sendStartupMessage() error {
	w := NewMessageWriter()

	// Protocol version (3.0).
	w.WriteUint32(protocol.ProtocolVersionNumber)

	// User parameter (required).
	w.WriteString("user")
	w.WriteString(c.config.User)

	// Database parameter (optional, defaults to user name on server).
	if c.config.Database != "" {
		w.WriteString("database")
		w.WriteString(c.config.Database)
	}

	// Additional parameters.
	for key, value := range c.config.Parameters {
		w.WriteString(key)
		w.WriteString(value)
	}

	// Null terminator for parameter list.
	w.WriteByte(0)

	// Write the startup packet (no message type, just length + body).
	body := w.Bytes()
	length := uint32(4 + len(body)) // length includes itself

	// Write length.
	if err := c.writeUint32(length); err != nil {
		return err
	}

	// Write body.
	if _, err := c.bufferedWriter.Write(body); err != nil {
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
		return fmt.Errorf("authentication message too short")
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
		return c.sendPasswordMessage(c.config.Password)

	case protocol.AuthMD5Password:
		// Read the 4-byte salt.
		salt, err := reader.ReadBytes(4)
		if err != nil {
			return fmt.Errorf("failed to read MD5 salt: %w", err)
		}
		return c.sendMD5PasswordMessage(c.config.Password, salt)

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

// sendPasswordMessage sends a cleartext password message.
func (c *Conn) sendPasswordMessage(password string) error {
	w := NewMessageWriter()
	w.WriteString(password)
	return c.writeMessage(protocol.MsgPasswordMsg, w.Bytes())
}

// sendMD5PasswordMessage sends an MD5 hashed password message.
// Note: MD5 authentication is a legacy PostgreSQL protocol requirement.
func (c *Conn) sendMD5PasswordMessage(password string, salt []byte) error {
	// MD5 password format: "md5" + md5(md5(password + user) + salt)

	// First hash: md5(password + user)
	h1 := md5.New() //nolint:gosec // Required by PostgreSQL protocol
	h1.Write([]byte(password))
	h1.Write([]byte(c.config.User))
	hash1 := hex.EncodeToString(h1.Sum(nil))

	// Second hash: md5(hash1 + salt)
	h2 := md5.New() //nolint:gosec // Required by PostgreSQL protocol
	h2.Write([]byte(hash1))
	h2.Write(salt)
	hash2 := hex.EncodeToString(h2.Sum(nil))

	// Final password: "md5" + hash2
	md5Password := "md5" + hash2

	return c.sendPasswordMessage(md5Password)
}

// handleBackendKeyData handles a BackendKeyData message.
func (c *Conn) handleBackendKeyData(body []byte) error {
	if len(body) < 8 {
		return fmt.Errorf("backend key data message too short")
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
		return fmt.Errorf("ready for query message too short")
	}

	c.txnStatus = body[0]
	return nil
}

// writeSSLRequest writes an SSL negotiation request.
func (c *Conn) writeSSLRequest() error {
	// SSLRequest message format:
	// - Length (4 bytes): 8
	// - SSLRequestCode (4 bytes)

	if err := c.writeUint32(8); err != nil {
		return err
	}
	return c.writeUint32(protocol.SSLRequestCode)
}
