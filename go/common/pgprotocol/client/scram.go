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
	"fmt"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
)

// scramClient handles the SCRAM-SHA-256 authentication flow over a connection.
// It wraps scram.SCRAMClient and adds protocol I/O handling.
type scramClient struct {
	conn   *Conn
	client *scram.SCRAMClient
}

// newScramClient creates a new SCRAM client using password-based authentication.
func newScramClient(conn *Conn, username, password string) *scramClient {
	return &scramClient{
		conn:   conn,
		client: scram.NewSCRAMClientWithPassword(username, password),
	}
}

// newScramClientWithKeys creates a SCRAM client using pre-computed keys.
// This enables SCRAM passthrough authentication where keys were extracted during
// client authentication and are reused for backend authentication.
func newScramClientWithKeys(conn *Conn, username string, clientKey, serverKey []byte) *scramClient {
	return &scramClient{
		conn:   conn,
		client: scram.NewSCRAMClientWithKeys(username, clientKey, serverKey),
	}
}

// authenticate performs the full SCRAM-SHA-256 authentication exchange.
func (s *scramClient) authenticate() error {
	// Step 1: Generate and send client-first message.
	if err := s.sendClientFirst(); err != nil {
		return fmt.Errorf("SCRAM client-first failed: %w", err)
	}

	// Step 2: Receive and process server-first message.
	serverFirst, err := s.receiveServerFirst()
	if err != nil {
		return fmt.Errorf("SCRAM server-first failed: %w", err)
	}

	// Step 3: Generate and send client-final message.
	if err := s.sendClientFinal(serverFirst); err != nil {
		return fmt.Errorf("SCRAM client-final failed: %w", err)
	}

	// Step 4: Receive and verify server-final message.
	if err := s.receiveServerFinal(); err != nil {
		return fmt.Errorf("SCRAM server-final failed: %w", err)
	}

	return nil
}

// sendClientFirst sends the SASLInitialResponse with client-first message.
func (s *scramClient) sendClientFirst() error {
	clientFirstMessage, err := s.client.ClientFirstMessage()
	if err != nil {
		return err
	}

	// Send SASLInitialResponse message.
	w := NewMessageWriter()
	w.WriteString(scram.ScramSHA256Mechanism)
	w.WriteInt32(int32(len(clientFirstMessage)))
	w.WriteBytes([]byte(clientFirstMessage))

	return s.conn.writeMessage(protocol.MsgPasswordMsg, w.Bytes())
}

// receiveServerFirst receives and parses the AuthenticationSASLContinue message.
func (s *scramClient) receiveServerFirst() (string, error) {
	// Read message from server.
	msgType, body, err := s.conn.readMessage()
	if err != nil {
		return "", fmt.Errorf("failed to read message: %w", err)
	}

	// Check message type.
	if msgType == protocol.MsgErrorResponse {
		return "", s.conn.parseError(body)
	}
	if msgType != protocol.MsgAuthenticationRequest {
		return "", fmt.Errorf("expected AuthenticationRequest, got %c", msgType)
	}

	// Parse authentication request.
	reader := NewMessageReader(body)
	authType, err := reader.ReadInt32()
	if err != nil {
		return "", fmt.Errorf("failed to read auth type: %w", err)
	}
	if authType != protocol.AuthSASLContinue {
		return "", fmt.Errorf("expected AuthSASLContinue, got %d", authType)
	}

	// Get server-first-message (all remaining bytes).
	serverData, err := reader.ReadBytes(reader.Remaining())
	if err != nil {
		return "", fmt.Errorf("failed to read server data: %w", err)
	}

	return string(serverData), nil
}

// sendClientFinal computes the proof and sends the client-final message.
func (s *scramClient) sendClientFinal(serverFirst string) error {
	clientFinalMessage, err := s.client.ProcessServerFirst(serverFirst)
	if err != nil {
		return err
	}

	// Send SASLResponse message.
	w := NewMessageWriter()
	w.WriteBytes([]byte(clientFinalMessage))

	return s.conn.writeMessage(protocol.MsgPasswordMsg, w.Bytes())
}

// receiveServerFinal receives and verifies the AuthenticationSASLFinal message.
func (s *scramClient) receiveServerFinal() error {
	// Read message from server.
	msgType, body, err := s.conn.readMessage()
	if err != nil {
		return fmt.Errorf("failed to read message: %w", err)
	}

	// Check message type.
	if msgType == protocol.MsgErrorResponse {
		return s.conn.parseError(body)
	}
	if msgType != protocol.MsgAuthenticationRequest {
		return fmt.Errorf("expected AuthenticationRequest, got %c", msgType)
	}

	// Parse authentication request.
	reader := NewMessageReader(body)
	authType, err := reader.ReadInt32()
	if err != nil {
		return fmt.Errorf("failed to read auth type: %w", err)
	}
	if authType != protocol.AuthSASLFinal {
		return fmt.Errorf("expected AuthSASLFinal, got %d", authType)
	}

	// Get server-final-message (all remaining bytes).
	serverFinalData, err := reader.ReadBytes(reader.Remaining())
	if err != nil {
		return fmt.Errorf("failed to read server final data: %w", err)
	}

	// Verify server signature.
	return s.client.VerifyServerFinal(string(serverFinalData))
}
