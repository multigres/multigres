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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/pbkdf2"

	"github.com/multigres/multigres/go/pgprotocol/protocol"
)

const (
	// scramSHA256Mechanism is the SASL mechanism name for SCRAM-SHA-256.
	scramSHA256Mechanism = "SCRAM-SHA-256"

	// scramNonceLength is the length of the client nonce in bytes.
	scramNonceLength = 24
)

// scramClient handles the SCRAM-SHA-256 authentication flow.
type scramClient struct {
	conn     *Conn
	username string
	password string

	// State maintained across the authentication exchange.
	clientNonce            string
	clientFirstMessageBare string
	serverFirstMessage     string
	saltedPassword         []byte
}

// newScramClient creates a new SCRAM client for authentication.
func newScramClient(conn *Conn, username, password string) *scramClient {
	return &scramClient{
		conn:     conn,
		username: username,
		password: password,
	}
}

// authenticate performs the full SCRAM-SHA-256 authentication exchange.
func (s *scramClient) authenticate() error {
	// Step 1: Generate and send client-first message.
	if err := s.sendClientFirst(); err != nil {
		return fmt.Errorf("SCRAM client-first failed: %w", err)
	}

	// Step 2: Receive and process server-first message.
	if err := s.receiveServerFirst(); err != nil {
		return fmt.Errorf("SCRAM server-first failed: %w", err)
	}

	// Step 3: Generate and send client-final message.
	if err := s.sendClientFinal(); err != nil {
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
	// Generate client nonce.
	nonceBytes := make([]byte, scramNonceLength)
	if _, err := rand.Read(nonceBytes); err != nil {
		return fmt.Errorf("failed to generate nonce: %w", err)
	}
	s.clientNonce = base64.StdEncoding.EncodeToString(nonceBytes)

	// Build client-first-message-bare: n=<username>,r=<nonce>
	// Username needs to be escaped: '=' -> '=3D', ',' -> '=2C'
	escapedUsername := strings.ReplaceAll(s.username, "=", "=3D")
	escapedUsername = strings.ReplaceAll(escapedUsername, ",", "=2C")
	s.clientFirstMessageBare = fmt.Sprintf("n=%s,r=%s", escapedUsername, s.clientNonce)

	// client-first-message: n,,<client-first-message-bare>
	// "n,," means no channel binding (n = client doesn't support, y = client supports but not required)
	clientFirstMessage := "n,," + s.clientFirstMessageBare

	// Send SASLInitialResponse message.
	w := NewMessageWriter()
	w.WriteString(scramSHA256Mechanism)
	w.WriteInt32(int32(len(clientFirstMessage)))
	w.WriteBytes([]byte(clientFirstMessage))

	return s.conn.writeMessage(protocol.MsgPasswordMsg, w.Bytes())
}

// receiveServerFirst receives and parses the AuthenticationSASLContinue message.
func (s *scramClient) receiveServerFirst() error {
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
	if authType != protocol.AuthSASLContinue {
		return fmt.Errorf("expected AuthSASLContinue, got %d", authType)
	}

	// Get server-first-message (all remaining bytes).
	serverData, err := reader.ReadBytes(reader.Remaining())
	if err != nil {
		return fmt.Errorf("failed to read server data: %w", err)
	}
	s.serverFirstMessage = string(serverData)

	return nil
}

// sendClientFinal computes the proof and sends the client-final message.
func (s *scramClient) sendClientFinal() error {
	// Parse server-first-message: r=<nonce>,s=<salt>,i=<iterations>
	parts := strings.Split(s.serverFirstMessage, ",")
	var serverNonce, saltB64 string
	var iterations int

	for _, part := range parts {
		if strings.HasPrefix(part, "r=") {
			serverNonce = part[2:]
		} else if strings.HasPrefix(part, "s=") {
			saltB64 = part[2:]
		} else if strings.HasPrefix(part, "i=") {
			var err error
			iterations, err = strconv.Atoi(part[2:])
			if err != nil {
				return fmt.Errorf("invalid iteration count: %w", err)
			}
		}
	}

	// Validate server nonce starts with client nonce.
	if !strings.HasPrefix(serverNonce, s.clientNonce) {
		return fmt.Errorf("server nonce doesn't start with client nonce")
	}

	// Decode salt.
	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return fmt.Errorf("failed to decode salt: %w", err)
	}

	// Compute SaltedPassword = Hi(password, salt, iterations).
	s.saltedPassword = pbkdf2.Key([]byte(s.password), salt, iterations, sha256.Size, sha256.New)

	// Compute ClientKey = HMAC(SaltedPassword, "Client Key").
	clientKey := hmacSHA256(s.saltedPassword, []byte("Client Key"))

	// Compute StoredKey = H(ClientKey).
	storedKey := sha256Sum(clientKey)

	// Build client-final-message-without-proof.
	// c=<base64(channel-binding)>,r=<nonce>
	// For "n,," channel binding, biws = base64("n,,") = "biws"
	channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,"))
	clientFinalWithoutProof := fmt.Sprintf("c=%s,r=%s", channelBinding, serverNonce)

	// Compute AuthMessage = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof.
	authMessage := s.clientFirstMessageBare + "," + s.serverFirstMessage + "," + clientFinalWithoutProof

	// Compute ClientSignature = HMAC(StoredKey, AuthMessage).
	clientSignature := hmacSHA256(storedKey, []byte(authMessage))

	// Compute ClientProof = ClientKey XOR ClientSignature.
	clientProof := xorBytes(clientKey, clientSignature)

	// Build client-final-message: <client-final-without-proof>,p=<base64(ClientProof)>.
	clientFinalMessage := clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(clientProof)

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
	serverFinalMessage := string(serverFinalData)

	// Parse server signature from server-final-message: v=<signature>.
	if !strings.HasPrefix(serverFinalMessage, "v=") {
		return fmt.Errorf("invalid server-final-message format")
	}
	serverSignatureB64 := serverFinalMessage[2:]
	serverSignature, err := base64.StdEncoding.DecodeString(serverSignatureB64)
	if err != nil {
		return fmt.Errorf("failed to decode server signature: %w", err)
	}

	// Compute expected server signature.
	// ServerKey = HMAC(SaltedPassword, "Server Key")
	serverKey := hmacSHA256(s.saltedPassword, []byte("Server Key"))

	// Build AuthMessage again (same as in sendClientFinal).
	channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,"))
	// We need to reconstruct serverNonce from server-first-message
	var serverNonce string
	for part := range strings.SplitSeq(s.serverFirstMessage, ",") {
		if strings.HasPrefix(part, "r=") {
			serverNonce = part[2:]
			break
		}
	}
	clientFinalWithoutProof := fmt.Sprintf("c=%s,r=%s", channelBinding, serverNonce)
	authMessage := s.clientFirstMessageBare + "," + s.serverFirstMessage + "," + clientFinalWithoutProof

	// ServerSignature = HMAC(ServerKey, AuthMessage)
	expectedServerSignature := hmacSHA256(serverKey, []byte(authMessage))

	// Verify server signature.
	if !hmac.Equal(serverSignature, expectedServerSignature) {
		return fmt.Errorf("server signature verification failed")
	}

	return nil
}

// hmacSHA256 computes HMAC-SHA-256.
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// sha256Sum computes SHA-256 hash.
func sha256Sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

// xorBytes XORs two byte slices of equal length.
func xorBytes(a, b []byte) []byte {
	result := make([]byte, len(a))
	for i := range a {
		result[i] = a[i] ^ b[i]
	}
	return result
}
