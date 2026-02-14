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
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"

	"github.com/xdg-go/stringprep"
	"golang.org/x/crypto/pbkdf2"
)

const (
	// sha256Size is the output size of SHA-256 in bytes.
	sha256Size = 32

	// clientKeyLiteral is the string "Client Key" used in SCRAM.
	clientKeyLiteral = "Client Key"

	// serverKeyLiteral is the string "Server Key" used in SCRAM.
	serverKeyLiteral = "Server Key"
)

// normalizePassword normalizes a password using SASLprep (RFC 4013).
//
// SASLprep applies stringprep with the SASLprep profile, which includes:
// - Character mapping (e.g., soft hyphens removed, non-ASCII spaces → ASCII space)
// - NFKC normalization (compatibility normalization)
// - Prohibited character checks
// - Bidirectional text validation
//
// If normalization fails (invalid UTF-8, prohibited characters, bidirectional check
// failures, etc.), the raw password is returned unchanged. This matches PostgreSQL's
// lenient behavior where invalid passwords are allowed to pass through without
// normalization.
//
// Implementation choice: We use xdg-go/stringprep (RFC 4013 SASLprep) instead of the
// more modern golang.org/x/text/secure/precis (RFC 7613 PRECIS OpaqueString) to maximize
// compatibility with PostgreSQL. While PRECIS supersedes SASLprep in the standards,
// PostgreSQL still uses the older SASLprep implementation. The key difference is
// normalization form: SASLprep uses NFKC while PRECIS uses NFC, which can produce
// different results for certain Unicode characters (e.g., U+00AA FEMININE ORDINAL → 'a'
// in NFKC but unchanged in NFC). We prioritize PostgreSQL compatibility over using the
// newer standard.
//
// Returns the normalized password (or raw password if normalization fails).
func normalizePassword(password string) string {
	normalized, err := stringprep.SASLprep.Prepare(password)
	if err != nil {
		// PostgreSQL allows passwords invalid according to SASLprep.
		// Return raw password to maintain compatibility.
		return password
	}
	return normalized
}

// ComputeSaltedPassword computes the SCRAM SaltedPassword using PBKDF2.
// SaltedPassword = Hi(Normalize(password), salt, iterations)
// Where Hi is PBKDF2 with HMAC-SHA-256.
//
// Passwords are normalized using SASLprep (RFC 4013), which applies stringprep with
// NFKC normalization and character mapping. If normalization fails (invalid UTF-8,
// prohibited characters, bidirectional check failures), the raw password is used
// unchanged, matching PostgreSQL's lenient behavior.
func ComputeSaltedPassword(password string, salt []byte, iterations int) []byte {
	normalizedPassword := normalizePassword(password)
	return pbkdf2.Key([]byte(normalizedPassword), salt, iterations, sha256Size, sha256.New)
}

// ComputeClientKey computes ClientKey = HMAC(SaltedPassword, "Client Key").
func ComputeClientKey(saltedPassword []byte) []byte {
	return hmacSHA256(saltedPassword, []byte(clientKeyLiteral))
}

// ComputeStoredKey computes StoredKey = H(ClientKey) where H is SHA-256.
func ComputeStoredKey(clientKey []byte) []byte {
	h := sha256.Sum256(clientKey)
	return h[:]
}

// ComputeServerKey computes ServerKey = HMAC(SaltedPassword, "Server Key").
func ComputeServerKey(saltedPassword []byte) []byte {
	return hmacSHA256(saltedPassword, []byte(serverKeyLiteral))
}

// ComputeClientSignature computes ClientSignature = HMAC(StoredKey, AuthMessage).
func ComputeClientSignature(storedKey []byte, authMessage string) []byte {
	return hmacSHA256(storedKey, []byte(authMessage))
}

// computeClientProof computes ClientProof = ClientKey XOR ClientSignature.
func computeClientProof(clientKey, clientSignature []byte) ([]byte, error) {
	return xorBytes(clientKey, clientSignature)
}

// ComputeServerSignature computes ServerSignature = HMAC(ServerKey, AuthMessage).
func ComputeServerSignature(serverKey []byte, authMessage string) []byte {
	return hmacSHA256(serverKey, []byte(authMessage))
}

// ExtractAndVerifyClientProof verifies the client's proof and extracts the ClientKey.
// This is used for SCRAM key passthrough: after verifying a client, we can use the
// extracted ClientKey to authenticate as that user to PostgreSQL.
//
// The verification process:
// 1. Compute ClientSignature = HMAC(StoredKey, AuthMessage)
// 2. Recover ClientKey = ClientProof XOR ClientSignature
// 3. Compute RecoveredStoredKey = H(ClientKey)
// 4. Verify RecoveredStoredKey == StoredKey
//
// Returns the extracted ClientKey on successful verification (error == nil).
// Returns ErrAuthenticationFailed if the proof is invalid (wrong password).
// Returns other errors for unexpected conditions (malformed proof, length mismatches).
// Uses constant-time comparison to prevent timing attacks.
func ExtractAndVerifyClientProof(storedKey []byte, authMessage string, clientProof []byte) ([]byte, error) {
	if len(clientProof) != sha256Size {
		return nil, fmt.Errorf("invalid proof length: expected %d, got %d", sha256Size, len(clientProof))
	}

	// Compute ClientSignature
	clientSignature := ComputeClientSignature(storedKey, authMessage)

	// Recover ClientKey = ClientProof XOR ClientSignature
	recoveredClientKey, err := xorBytes(clientProof, clientSignature)
	if err != nil {
		// This should never happen - we validated proof length above and clientSignature
		// is always sha256Size bytes. This indicates a programming error.
		return nil, fmt.Errorf("failed to recover client key: %w", err)
	}

	// Compute H(recoveredClientKey)
	recoveredStoredKey := ComputeStoredKey(recoveredClientKey)

	// Constant-time comparison to prevent timing attacks
	if subtle.ConstantTimeCompare(storedKey, recoveredStoredKey) != 1 {
		// This is the expected failure case: client provided wrong password
		return nil, ErrAuthenticationFailed
	}

	return recoveredClientKey, nil
}

// buildAuthMessage constructs the AuthMessage for SCRAM.
// AuthMessage = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
func buildAuthMessage(clientFirstMessageBare, serverFirstMessage, clientFinalMessageWithoutProof string) string {
	return clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof
}

// computeChannelBindingData computes the channel binding data for SCRAM.
// For no channel binding (gs2-header = "n,,"), this returns the bytes of "n,,".
func computeChannelBindingData(channelBindingType string) []byte {
	// For no channel binding, the GS2 header is "n,,"
	// This is what gets base64-encoded to "biws" in the client-final-message.
	if channelBindingType == "" {
		return []byte("n,,")
	}
	// Channel binding not yet supported
	return []byte("n,,")
}

// hmacSHA256 computes HMAC-SHA-256(key, message).
func hmacSHA256(key, message []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(message)
	return h.Sum(nil)
}

// xorBytes returns a XOR b.
// Returns an error if a and b have different lengths.
func xorBytes(a, b []byte) ([]byte, error) {
	if len(a) != len(b) {
		return nil, fmt.Errorf("xorBytes: length mismatch (a=%d, b=%d)", len(a), len(b))
	}

	result := make([]byte, len(a))
	for i := range a {
		result[i] = a[i] ^ b[i]
	}
	return result, nil
}
