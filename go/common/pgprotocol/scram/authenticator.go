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
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors for SCRAM authentication.
var (
	// ErrUserNotFound indicates the user does not exist. Callers fetching
	// credentials before constructing a ScramAuthenticator should return
	// this so the gateway can emit PG's "password authentication failed"
	// (SQLSTATE 28P01) without exposing user existence.
	ErrUserNotFound = errors.New("user not found")

	// ErrAuthenticationFailed indicates the password proof was invalid.
	ErrAuthenticationFailed = errors.New("authentication failed")

	// ErrLoginDisabled indicates the role has rolcanlogin=false in pg_authid.
	// Caller should emit PG's "role \"X\" is not permitted to log in" with
	// SQLSTATE 28000, matching native PostgreSQL.
	ErrLoginDisabled = errors.New("role not permitted to log in")

	// ErrPasswordExpired indicates the role's rolvaliduntil is in the past.
	// Caller should emit the opaque "password authentication failed" message
	// with SQLSTATE 28P01, matching native PostgreSQL's handling of expired
	// passwords.
	ErrPasswordExpired = errors.New("password expired")

	// ErrChannelBindingNegotiation indicates the client signaled GS2 flag
	// "y" while the server actually advertised SCRAM-SHA-256-PLUS — a
	// classic downgrade attempt. Per PostgreSQL, caller emits SQLSTATE
	// 28000 with "SCRAM channel binding negotiation error".
	ErrChannelBindingNegotiation = errors.New("scram: channel binding negotiation error")

	// ErrChannelBindingCheck indicates the cbind data the client echoed
	// in client-final-message does not match the value the server expects
	// (gs2-header + cbind-data for PLUS, or "biws"/"eSws" for base). Per
	// PostgreSQL, caller emits SQLSTATE 28000 with "SCRAM channel binding
	// check failed".
	ErrChannelBindingCheck = errors.New("scram: channel binding check failed")

	// ErrSASLProtocol indicates a SCRAM-level protocol violation tied to
	// channel binding: PLUS mechanism with non-"p=" flag, base mechanism
	// with "p=" flag, unsupported binding type, malformed messages. Per
	// PostgreSQL, caller emits SQLSTATE 08P01 with "malformed SCRAM
	// message" (or the more specific "unsupported SCRAM channel-binding
	// type" variant) plus an error detail.
	ErrSASLProtocol = errors.New("scram: protocol violation")

	// ErrAuthzidNotSupported indicates the client included a SASL
	// authorization identity ("a=...") in its gs2-header. PostgreSQL
	// rejects this with SQLSTATE 0A000 (feature_not_supported).
	ErrAuthzidNotSupported = errors.New("scram: authzid not supported")
)

// SASLProtocolError wraps ErrSASLProtocol with a PG-style detail message so
// the listener can emit "malformed SCRAM message" with errdetail("...")
// verbatim — matching libpq-readable output. Use as the wire-facing detail
// only; the sentinel chain still exposes ErrSASLProtocol via errors.Is.
type SASLProtocolError struct {
	// Detail is the PostgreSQL errdetail() string — keep it byte-identical
	// to the PG source so client-side log parsers (and humans) can't tell
	// they're talking to multigres.
	Detail string
	// Msg overrides the primary errmsg(); empty means "malformed SCRAM
	// message" (PG's default for cbind protocol errors).
	Msg string
}

func (e *SASLProtocolError) Error() string {
	if e.Msg != "" {
		return "scram: " + e.Msg + ": " + e.Detail
	}
	return "scram: malformed SCRAM message: " + e.Detail
}

// Unwrap lets errors.Is(err, ErrSASLProtocol) succeed.
func (e *SASLProtocolError) Unwrap() error { return ErrSASLProtocol }

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
	// Database for credential lookup context (used in logs).
	database string

	// Current state of the authentication handshake.
	state authenticatorState

	// SCRAM hash for the authenticating user, supplied by the caller at
	// construction time. The caller is responsible for fetching it from
	// the credential source before SCRAM starts.
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

	// channelBinding holds the TLS channel binding context. When non-nil and
	// TLSServerEndPointHash is populated, the authenticator advertises
	// SCRAM-SHA-256-PLUS in addition to SCRAM-SHA-256.
	channelBinding *ChannelBinding

	// overTLS records whether the underlying connection is TLS — regardless
	// of whether cbind material was successfully captured. PG triggers its
	// "channel binding negotiation error" path on `ssl_in_use`, not on
	// whether PLUS was actually advertisable. Tracking these independently
	// ensures the `y`-flag downgrade is still rejected when the server is
	// over TLS but failed to compute a cbind hash (e.g. unsupported cert
	// signature algorithm) — otherwise the fallback to base SCRAM would
	// silently accept a downgrade attempt.
	overTLS bool

	// selectedMechanism is the SASL mechanism the client picked
	// (SCRAM-SHA-256 or SCRAM-SHA-256-PLUS). Captured in HandleClientFirst
	// and used in HandleClientFinal to verify the cbind data the client
	// echoes back.
	selectedMechanism string

	// clientGS2Header is the literal gs2-header prefix from the client's
	// first message. Used to verify cbind data in client-final-message.
	clientGS2Header string
}

// NewScramAuthenticator creates a new SCRAM authenticator for the given
// pre-fetched password hash and database name. The caller must look up the
// hash (e.g. via a credential provider) before constructing the authenticator
// so a single credential fetch can satisfy both SCRAM and any subsequent
// role-attribute checks the caller wants to perform.
//
// Panics if hash is nil. Unknown-user / login-disabled / password-expired
// cases must be handled by the caller before reaching this constructor.
func NewScramAuthenticator(hash *ScramHash, database string) *ScramAuthenticator {
	if hash == nil {
		panic("auth: scram hash cannot be nil")
	}
	return &ScramAuthenticator{
		hash:     hash,
		database: database,
		state:    stateInitial,
	}
}

// SetChannelBinding attaches TLS channel binding context to the authenticator.
// Must be called before StartAuthentication; calling it afterwards panics, as
// the advertised mechanism list would already be locked in. When the binding
// hash is non-empty the authenticator advertises SCRAM-SHA-256-PLUS in
// addition to SCRAM-SHA-256 and enforces the corresponding GS2 flag rules
// (RFC 5802 §6).
//
// Pass nil to clear any previously set binding.
func (a *ScramAuthenticator) SetChannelBinding(cb *ChannelBinding) {
	if a.state != stateInitial {
		panic("scram: SetChannelBinding called after StartAuthentication")
	}
	a.channelBinding = cb
}

// SetOverTLS records that the underlying connection is TLS-encrypted. Must
// be called before StartAuthentication. This is independent of whether
// SetChannelBinding succeeded — downgrade detection still needs to fire
// over TLS even when cbind material couldn't be derived.
func (a *ScramAuthenticator) SetOverTLS(overTLS bool) {
	if a.state != stateInitial {
		panic("scram: SetOverTLS called after StartAuthentication")
	}
	a.overTLS = overTLS
}

// hasChannelBinding reports whether channel binding material is available.
func (a *ScramAuthenticator) hasChannelBinding() bool {
	return a.channelBinding != nil && len(a.channelBinding.TLSServerEndPointHash) > 0
}

// StartAuthentication begins the SCRAM authentication process.
// Returns the list of supported SASL mechanisms. When channel binding is
// available, SCRAM-SHA-256-PLUS is listed first so libpq-style clients pick
// the stronger mechanism by default.
//
// This corresponds to sending AuthenticationSASL (auth type 10) to the client.
func (a *ScramAuthenticator) StartAuthentication() []string {
	a.state = stateStarted
	if a.hasChannelBinding() {
		return []string{ScramSHA256PlusMechanism, ScramSHA256Mechanism}
	}
	return []string{ScramSHA256Mechanism}
}

// HandleClientFirst processes the client-first-message from the client.
// Returns the server-first-message to send back.
//
// The mechanism is the SASL mechanism the client selected in the
// SASLInitialResponse (SCRAM-SHA-256 or SCRAM-SHA-256-PLUS); the
// authenticator validates it against what StartAuthentication advertised and
// against the GS2 channel-binding flag the client carries in the
// client-first-message.
//
// The client-first-message comes from SASLInitialResponse and contains
// the GS2 header and client-first-message-bare (username, client nonce).
//
// The startupMessageUsername is used when the client sends an empty username in the
// client-first-message. PostgreSQL allows this and uses the username from
// the startup message. This parameter should be the username from the startup
// message.
//
// The user's password hash was supplied at construction time; this method
// generates the server-first-message containing the combined nonce, salt,
// and iteration count from that hash.
func (a *ScramAuthenticator) HandleClientFirst(mechanism, clientFirstMessage, startupMessageUsername string) (string, error) {
	// Verify state.
	if a.state != stateStarted {
		return "", fmt.Errorf("auth: invalid state for HandleClientFirst (expected started, got %d)", a.state)
	}

	// Validate mechanism against what we advertised.
	switch mechanism {
	case ScramSHA256Mechanism:
	case ScramSHA256PlusMechanism:
		if !a.hasChannelBinding() {
			a.state = stateFailed
			return "", fmt.Errorf("auth: client selected %s but server did not advertise it", ScramSHA256PlusMechanism)
		}
	default:
		a.state = stateFailed
		return "", fmt.Errorf("auth: unsupported SASL mechanism: %q", mechanism)
	}

	// Parse the client-first-message.
	parsed, err := parseClientFirstMessage(clientFirstMessage)
	if err != nil {
		a.state = stateFailed
		return "", fmt.Errorf("auth: invalid client-first-message: %w", err)
	}

	// PG rejects SASL authzid outright (see auth-scram.c
	// read_client_first_message). Mirror that for parity.
	if parsed.authzid != "" {
		a.state = stateFailed
		return "", ErrAuthzidNotSupported
	}

	// Cross-check the GS2 channel-binding flag against the chosen mechanism.
	// Enforces RFC 5802 §6 and detects downgrade attempts.
	if err := a.validateGS2Flag(mechanism, parsed); err != nil {
		a.state = stateFailed
		return "", err
	}
	a.selectedMechanism = mechanism
	a.clientGS2Header = parsed.gs2Header

	// PostgreSQL always ignores the username from client-first-message
	// and uses the startup message username. This is because not all UTF-8
	// strings are valid postgres usernames.
	// See https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256
	// We still parse the name here for protocol validation, but intentionally ignore it.
	_ = parsed.username // Parsed but ignored per PostgreSQL behavior

	// Always use the username from the startup message.
	username := startupMessageUsername
	if username == "" {
		a.state = stateFailed
		return "", errors.New("auth: no username provided in startup message")
	}

	// Store values for later verification.
	a.username = username
	a.clientNonce = parsed.clientNonce
	a.clientFirstMessageBare = parsed.clientFirstMessageBare

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

// validateGS2Flag enforces the RFC 5802 §6 rules tying the SASL mechanism
// the client selected to the GS2 channel-binding flag it sent. Error
// messages and SQLSTATE classes mirror PostgreSQL 17 (auth-scram.c) so
// libpq-compatible clients see byte-identical diagnostics.
func (a *ScramAuthenticator) validateGS2Flag(mechanism string, parsed *clientFirstMessage) error {
	switch mechanism {
	case ScramSHA256PlusMechanism:
		switch parsed.gs2CbindFlag {
		case "n", "y":
			return &SASLProtocolError{
				Detail: "The client selected SCRAM-SHA-256-PLUS, but the SCRAM message does not include channel binding data.",
			}
		}
		if parsed.channelBindingType != ChannelBindingTypeTLSServerEndPoint {
			return &SASLProtocolError{
				Msg: fmt.Sprintf("unsupported SCRAM channel-binding type %q", parsed.channelBindingType),
			}
		}
	case ScramSHA256Mechanism:
		switch parsed.gs2CbindFlag {
		case "n":
			// no binding — fine.
		case "y":
			if a.overTLS {
				// Connection is TLS — server can do cbind — but client claims
				// it didn't advertise. PG returns 28000 here (distinct from
				// the 08P01 used for other cbind protocol errors).
				return ErrChannelBindingNegotiation
			}
		default:
			// gs2 flag starts with "p=" → cbind requested without picking PLUS.
			return &SASLProtocolError{
				Detail: "The client selected SCRAM-SHA-256 without channel binding, but the SCRAM message includes channel binding data.",
			}
		}
	}
	return nil
}

// verifyChannelBinding recomputes the expected c= value the client should
// have sent and compares it in constant time. Note: the cbind data is not
// itself secret (the cert hash is public; the gs2-header echoes flags
// already on the wire) and PG uses a plain strcmp. Constant-time here is
// defense-in-depth.
//
//   - PLUS: base64("p=tls-server-end-point,," || cbind-data). Mismatch is
//     ErrChannelBindingCheck (28000 "SCRAM channel binding check failed").
//   - Base SCRAM-SHA-256: PG accepts only the two literals "biws" (= n,,)
//     and "eSws" (= y,,), each tied to the matching original gs2 flag.
//     Anything else is a protocol violation (08P01).
func (a *ScramAuthenticator) verifyChannelBinding(clientCBindB64 string) error {
	if a.selectedMechanism == ScramSHA256PlusMechanism {
		expected := append([]byte(a.clientGS2Header), a.channelBinding.TLSServerEndPointHash...)
		expectedB64 := base64.StdEncoding.EncodeToString(expected)
		if subtle.ConstantTimeCompare([]byte(clientCBindB64), []byte(expectedB64)) != 1 {
			return ErrChannelBindingCheck
		}
		return nil
	}

	switch {
	case clientCBindB64 == "biws" && a.clientGS2Header == "n,,":
		return nil
	case clientCBindB64 == "eSws" && a.clientGS2Header == "y,,":
		return nil
	default:
		return &SASLProtocolError{
			Msg: "unexpected SCRAM channel-binding attribute in client-final-message",
		}
	}
}

// HandleClientFinal processes the client-final-message from the client.
// Returns the server-final-message to send back on success.
//
// The client-final-message contains the channel binding, combined nonce,
// and client proof. This method verifies the proof and, if valid, returns
// the server signature for mutual authentication.
//
// Returns ErrAuthenticationFailed if the proof is invalid.
// Returns ErrChannelBindingCheck (PLUS) or ErrSASLProtocol (base) when the
// cbind data the client echoes back does not match the gs2-header (+ TLS
// cert hash for PLUS) the server expects.
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

	// Verify the channel binding data the client echoed back. The client
	// sends base64(gs2-header [|| cbind-data]); we recompute the same bytes
	// and compare. Mismatch means either the gs2-header was tampered with
	// mid-handshake or, for PLUS, the cbind hash diverges from the TLS cert
	// the client actually saw.
	if err := a.verifyChannelBinding(parsed.channelBinding); err != nil {
		a.state = stateFailed
		return "", err
	}

	// Verify the nonce matches.
	if parsed.nonce != a.combinedNonce {
		a.state = stateFailed
		return "", errors.New("auth: nonce mismatch (possible replay attack)")
	}

	// Verify the nonce starts with our client nonce (extra safety check).
	if !strings.HasPrefix(parsed.nonce, a.clientNonce) {
		a.state = stateFailed
		return "", errors.New("auth: combined nonce does not start with client nonce")
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
// another authentication attempt with the same hash. The channel binding
// context and overTLS flag are preserved — they're tied to the TLS session,
// not to a particular handshake attempt. The extracted ClientKey is
// zeroized before being nilled so a memory dump after Reset cannot recover
// the previous session's secret.
func (a *ScramAuthenticator) Reset() {
	for i := range a.extractedClientKey {
		a.extractedClientKey[i] = 0
	}
	a.state = stateInitial
	a.username = ""
	a.clientNonce = ""
	a.combinedNonce = ""
	a.clientFirstMessageBare = ""
	a.serverFirstMessage = ""
	a.extractedClientKey = nil
	a.selectedMechanism = ""
	a.clientGS2Header = ""
}
