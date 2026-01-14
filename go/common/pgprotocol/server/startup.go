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
	"fmt"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// StartupMessage represents a parsed startup message from the client.
type StartupMessage struct {
	ProtocolVersion uint32
	Parameters      map[string]string
}

// handleStartup handles the initial connection startup phase.
// This includes SSL negotiation and processing the startup message.
// Returns an error if the startup fails.
func (c *Conn) handleStartup() error {
	// Read the first startup packet (could be SSL request, startup message, etc.)
	buf, err := c.readStartupPacket()
	if err != nil {
		return fmt.Errorf("failed to read startup packet: %w", err)
	}
	defer c.returnReadBuffer(buf)

	// Parse the protocol version/code from the packet.
	reader := NewMessageReader(buf)
	protocolCode, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read protocol code: %w", err)
	}

	// Handle special protocol codes.
	switch protocolCode {
	case protocol.SSLRequestCode:
		// Client is requesting SSL. We don't support SSL yet, so decline.
		return c.handleSSLRequest()

	case protocol.GSSENCRequestCode:
		// Client is requesting GSSAPI encryption. We don't support it, so decline.
		return c.handleGSSENCRequest()

	case protocol.CancelRequestCode:
		// This is a cancel request, not a regular connection startup.
		return c.handleCancelRequest(reader)

	case protocol.ProtocolVersionNumber:
		// This is a normal startup message with protocol version 3.0.
		return c.handleStartupMessage(protocolCode, reader)

	default:
		return fmt.Errorf("unsupported protocol version: %d", protocolCode)
	}
}

// handleSSLRequest handles an SSL negotiation request.
// We currently don't support SSL, so we send 'N' (no SSL) and then
// wait for the client to send the actual startup message.
func (c *Conn) handleSSLRequest() error {
	c.logger.Debug("client requested SSL, declining")

	// Send 'N' to decline SSL.
	writer := c.getWriter()
	if err := c.writeByte(writer, 'N'); err != nil {
		return fmt.Errorf("failed to send SSL response: %w", err)
	}

	// Flush the response immediately.
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush SSL response: %w", err)
	}

	// Now read the actual startup message.
	buf, err := c.readStartupPacket()
	if err != nil {
		return fmt.Errorf("failed to read startup message after SSL: %w", err)
	}
	defer c.returnReadBuffer(buf)

	reader := NewMessageReader(buf)
	protocolCode, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read protocol code: %w", err)
	}

	if protocolCode != protocol.ProtocolVersionNumber {
		return fmt.Errorf("expected protocol version %d, got %d", protocol.ProtocolVersionNumber, protocolCode)
	}

	return c.handleStartupMessage(protocolCode, reader)
}

// handleGSSENCRequest handles a GSSAPI encryption request.
// We don't support GSSAPI encryption, so we send 'N' (no GSSENC) and then
// wait for the client to send the actual startup message.
func (c *Conn) handleGSSENCRequest() error {
	c.logger.Debug("client requested GSSAPI encryption, declining")

	// Send 'N' to decline GSSENC.
	writer := c.getWriter()
	if err := c.writeByte(writer, 'N'); err != nil {
		return fmt.Errorf("failed to send GSSENC response: %w", err)
	}

	// Flush the response immediately.
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush GSSENC response: %w", err)
	}

	// Now read the actual startup message.
	buf, err := c.readStartupPacket()
	if err != nil {
		return fmt.Errorf("failed to read startup message after GSSENC: %w", err)
	}
	defer c.returnReadBuffer(buf)

	reader := NewMessageReader(buf)
	protocolCode, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read protocol code: %w", err)
	}

	if protocolCode != protocol.ProtocolVersionNumber {
		return fmt.Errorf("expected protocol version %d, got %d", protocol.ProtocolVersionNumber, protocolCode)
	}

	return c.handleStartupMessage(protocolCode, reader)
}

// handleCancelRequest handles a query cancellation request.
// This is sent by clients to cancel a running query on another connection.
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

	c.logger.Info("received cancel request", "process_id", processID, "secret_key", secretKey)

	// TODO(GuptaManan100): Implement query cancellation.
	// For now, we just close the connection as per protocol spec.
	// The client should not expect a response to a cancel request.
	return c.Close()
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

	// Extract required parameters.
	c.user = c.params["user"]
	c.database = c.params["database"]

	// Default database to user if not specified.
	if c.database == "" {
		c.database = c.user
	}

	c.logger.Info("startup message parsed", "user", c.user, "database", c.database)

	// Now perform authentication.
	return c.authenticate()
}

// authenticate performs the authentication handshake with the client.
// For now, we only implement "trust" authentication (no password required).
func (c *Conn) authenticate() error {
	c.logger.Debug("authenticating client", "method", "trust")

	// Send AuthenticationOk (trust authentication).
	if err := c.sendAuthenticationOk(); err != nil {
		return fmt.Errorf("failed to send AuthenticationOk: %w", err)
	}

	// Send BackendKeyData for query cancellation.
	if err := c.sendBackendKeyData(); err != nil {
		return fmt.Errorf("failed to send BackendKeyData: %w", err)
	}

	// Send initial ParameterStatus messages.
	if err := c.sendParameterStatuses(); err != nil {
		return fmt.Errorf("failed to send ParameterStatus messages: %w", err)
	}

	// Send ReadyForQuery to indicate we're ready to receive commands.
	if err := c.sendReadyForQuery(); err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	c.logger.Info("authentication complete")
	return nil
}

// sendAuthenticationOk sends an AuthenticationOk message to the client.
func (c *Conn) sendAuthenticationOk() error {
	w := NewMessageWriter()
	w.WriteInt32(protocol.AuthOk)
	return c.writeMessage(protocol.MsgAuthenticationRequest, w.Bytes())
}

// sendBackendKeyData sends the BackendKeyData message.
// This contains the process ID (connection ID) and secret key for query cancellation.
func (c *Conn) sendBackendKeyData() error {
	w := NewMessageWriter()
	w.WriteUint32(c.connectionID)   // Process ID
	w.WriteUint32(c.backendKeyData) // Secret key
	return c.writeMessage(protocol.MsgBackendKeyData, w.Bytes())
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

// sendParameterStatus sends a single ParameterStatus message.
func (c *Conn) sendParameterStatus(name, value string) error {
	w := NewMessageWriter()
	w.WriteString(name)
	w.WriteString(value)
	return c.writeMessage(protocol.MsgParameterStatus, w.Bytes())
}

// sendReadyForQuery sends a ReadyForQuery message to indicate the server is ready.
func (c *Conn) sendReadyForQuery() error {
	w := NewMessageWriter()
	w.WriteByte(c.txnStatus)
	if err := c.writeMessage(protocol.MsgReadyForQuery, w.Bytes()); err != nil {
		return err
	}
	// Flush to ensure the client receives the message immediately.
	return c.flush()
}
