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

package scram

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors for SCRAM authentication.
var (
	// ErrUserNotFound indicates the user does not exist.
	ErrUserNotFound = errors.New("user not found")

	// ErrAuthenticationFailed indicates the password proof was invalid.
	ErrAuthenticationFailed = errors.New("authentication failed")
)

// PasswordHashProvider is an interface for retrieving password hashes.
// This abstraction allows the authenticator to be used with different
// storage backends (PostgreSQL, cache, etc.).
type PasswordHashProvider interface {
	// GetPasswordHash retrieves the SCRAM-SHA-256 hash for a user in a database.
	// Returns ErrUserNotFound if the user does not exist.
	GetPasswordHash(ctx context.Context, username, database string) (*ScramHash, error)
}

// authenticatorState tracks the current state of the SCRAM handshake.
type authenticatorState int

const (
	stateInitial authenticatorState = iota
	stateStarted
	stateClientFirstReceived
	stateAuthenticated
	stateFailed
)

// ScramAuthenticator handles SCRAM-SHA-256 authentication.
// It implements the server side of the SCRAM protocol as defined in RFC 5802.
//
// Usage:
//  1. Call StartAuthentication() to get the list of supported mechanisms.
//  2. After receiving client-first-message, call HandleClientFirst().
//  3. After receiving client-final-message, call HandleClientFinal().
//  4. Check IsAuthenticated() to see if auth succeeded.
//  5. Call ExtractedKeys() to get SCRAM keys for passthrough authentication.
//
// The authenticator maintains state between calls and enforces valid
// state transitions to prevent protocol errors.
//
// Thread Safety: ScramAuthenticator is NOT thread-safe. Each connection must
// use its own authenticator instance. Do not share authenticators across
// goroutines or reuse them for multiple concurrent authentication attempts.
// The Reset() method allows reusing an authenticator sequentially, but only
// after the previous authentication has completed.
type ScramAuthenticator struct {
	provider PasswordHashProvider

	// Database for credential lookup.
	database string

	// Current state of the authentication handshake.
	state authenticatorState

	// Cached hash from the password provider.
	hash *ScramHash

	// Username being authenticated.
	username string

	// Nonce values for replay protection.
	clientNonce   string
	combinedNonce string

	// Messages needed for AuthMessage computation.
	clientFirstMessageBare string
	serverFirstMessage     string

	// extractedClientKey is the ClientKey recovered from the client's proof.
	// This can be used for SCRAM passthrough authentication to backends.
	// See RFC 5802: ClientKey = ClientProof XOR ClientSignature.
	extractedClientKey []byte
}

// NewScramAuthenticator creates a new SCRAM authenticator with the given
// password hash provider and database name for credential lookup.
//
// Panics if provider is nil.
func NewScramAuthenticator(provider PasswordHashProvider, database string) *ScramAuthenticator {
	if provider == nil {
		panic("auth: password hash provider cannot be nil")
	}
	return &ScramAuthenticator{
		provider: provider,
		database: database,
		state:    stateInitial,
	}
}

// StartAuthentication begins the SCRAM authentication process.
// Returns the list of supported SASL mechanisms.
//
// This corresponds to sending AuthenticationSASL (auth type 10) to the client.
func (a *ScramAuthenticator) StartAuthentication() []string {
	a.state = stateStarted
	return []string{ScramSHA256Mechanism}
}

// HandleClientFirst processes the client-first-message from the client.
// Returns the server-first-message to send back.
//
// The client-first-message comes from SASLInitialResponse and contains
// the GS2 header and client-first-message-bare (username, client nonce).
//
// This method looks up the user's password hash and generates the
// server-first-message containing the combined nonce, salt, and iteration count.
func (a *ScramAuthenticator) HandleClientFirst(ctx context.Context, clientFirstMessage string) (string, error) {
	// Verify state.
	if a.state != stateStarted {
		return "", fmt.Errorf("auth: invalid state for HandleClientFirst (expected started, got %d)", a.state)
	}

	// Parse the client-first-message.
	parsed, err := parseClientFirstMessage(clientFirstMessage)
	if err != nil {
		a.state = stateFailed
		return "", fmt.Errorf("auth: invalid client-first-message: %w", err)
	}

	// Store values for later verification.
	a.username = parsed.username
	a.clientNonce = parsed.clientNonce
	a.clientFirstMessageBare = parsed.clientFirstMessageBare

	// Look up the password hash for this user.
	hash, err := a.provider.GetPasswordHash(ctx, a.username, a.database)
	if err != nil {
		a.state = stateFailed
		if errors.Is(err, ErrUserNotFound) {
			return "", ErrUserNotFound
		}
		return "", fmt.Errorf("auth: failed to get password hash: %w", err)
	}
	a.hash = hash

	// Generate the server-first-message.
	serverFirstMessage, combinedNonce, err := generateServerFirstMessage(
		a.clientNonce,
		a.hash.Salt,
		a.hash.Iterations,
	)
	if err != nil {
		a.state = stateFailed
		return "", fmt.Errorf("auth: failed to generate server-first-message: %w", err)
	}

	a.serverFirstMessage = serverFirstMessage
	a.combinedNonce = combinedNonce
	a.state = stateClientFirstReceived

	return serverFirstMessage, nil
}

// HandleClientFinal processes the client-final-message from the client.
// Returns the server-final-message to send back on success.
//
// The client-final-message contains the channel binding, combined nonce,
// and client proof. This method verifies the proof and, if valid, returns
// the server signature for mutual authentication.
//
// Returns ErrAuthenticationFailed if the proof is invalid.
func (a *ScramAuthenticator) HandleClientFinal(clientFinalMessage string) (string, error) {
	// Verify state.
	if a.state != stateClientFirstReceived {
		return "", fmt.Errorf("auth: invalid state for HandleClientFinal (expected client-first-received, got %d)", a.state)
	}

	// Parse the client-final-message.
	parsed, err := parseClientFinalMessage(clientFinalMessage)
	if err != nil {
		a.state = stateFailed
		return "", fmt.Errorf("auth: invalid client-final-message: %w", err)
	}

	// Verify the nonce matches.
	if parsed.nonce != a.combinedNonce {
		a.state = stateFailed
		return "", fmt.Errorf("auth: nonce mismatch (possible replay attack)")
	}

	// Verify the nonce starts with our client nonce (extra safety check).
	if !strings.HasPrefix(parsed.nonce, a.clientNonce) {
		a.state = stateFailed
		return "", fmt.Errorf("auth: combined nonce does not start with client nonce")
	}

	// Build the AuthMessage for verification.
	authMessage := buildAuthMessage(
		a.clientFirstMessageBare,
		a.serverFirstMessage,
		parsed.clientFinalMessageWithoutProof,
	)

	// Verify the client proof and extract the ClientKey for passthrough authentication.
	extractedClientKey, err := ExtractAndVerifyClientProof(a.hash.StoredKey, authMessage, parsed.proof)
	if err != nil {
		a.state = stateFailed
		// Return the specific error (ErrAuthenticationFailed for wrong password, or other errors)
		return "", err
	}
	a.extractedClientKey = extractedClientKey

	// Authentication successful! Compute the server signature.
	serverSignature := ComputeServerSignature(a.hash.ServerKey, authMessage)
	serverFinalMessage := generateServerFinalMessage(serverSignature)

	a.state = stateAuthenticated
	return serverFinalMessage, nil
}

// IsAuthenticated returns true if the authentication completed successfully.
func (a *ScramAuthenticator) IsAuthenticated() bool {
	return a.state == stateAuthenticated
}

// AuthenticatedUser returns the username that was successfully authenticated.
// Returns an empty string if authentication has not completed successfully.
func (a *ScramAuthenticator) AuthenticatedUser() string {
	if a.state != stateAuthenticated {
		return ""
	}
	return a.username
}

// ExtractedKeys returns the SCRAM keys extracted during authentication.
// These can be used for passthrough authentication to backend PostgreSQL servers.
//
// ClientKey is recovered from the client's proof (ClientKey = ClientProof XOR ClientSignature).
// ServerKey comes from the stored password hash.
//
// Returns nil, nil if authentication has not completed successfully.
func (a *ScramAuthenticator) ExtractedKeys() (clientKey, serverKey []byte) {
	if a.state != stateAuthenticated {
		return nil, nil
	}
	return a.extractedClientKey, a.hash.ServerKey
}

// Reset clears the authenticator state, allowing it to be reused for
// another authentication attempt.
func (a *ScramAuthenticator) Reset() {
	a.state = stateInitial
	a.hash = nil
	a.username = ""
	a.clientNonce = ""
	a.combinedNonce = ""
	a.clientFirstMessageBare = ""
	a.serverFirstMessage = ""
	a.extractedClientKey = nil
}
