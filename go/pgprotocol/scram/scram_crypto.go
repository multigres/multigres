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

// ComputeSaltedPassword computes the SCRAM SaltedPassword using PBKDF2.
// SaltedPassword = Hi(Normalize(password), salt, iterations)
// Where Hi is PBKDF2 with HMAC-SHA-256.
//
// Note: This implementation does not perform SASLprep normalization of the password.
// PostgreSQL also does not enforce strict SASLprep, so this is compatible.
func ComputeSaltedPassword(password string, salt []byte, iterations int) []byte {
	return pbkdf2.Key([]byte(password), salt, iterations, sha256Size, sha256.New)
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

// ComputeClientProof computes ClientProof = ClientKey XOR ClientSignature.
func ComputeClientProof(clientKey, clientSignature []byte) ([]byte, error) {
	return xorBytes(clientKey, clientSignature)
}

// ComputeServerSignature computes ServerSignature = HMAC(ServerKey, AuthMessage).
func ComputeServerSignature(serverKey []byte, authMessage string) []byte {
	return hmacSHA256(serverKey, []byte(authMessage))
}

// VerifyClientProof verifies the client's proof against the stored key.
// The verification process:
// 1. Compute ClientSignature = HMAC(StoredKey, AuthMessage)
// 2. Recover ClientKey = ClientProof XOR ClientSignature
// 3. Compute RecoveredStoredKey = H(ClientKey)
// 4. Verify RecoveredStoredKey == StoredKey
//
// Returns nil on successful verification.
// Returns ErrAuthenticationFailed if the proof is invalid (wrong password).
// Returns other errors for unexpected conditions.
// Uses constant-time comparison to prevent timing attacks.
func VerifyClientProof(storedKey []byte, authMessage string, clientProof []byte) error {
	_, err := ExtractAndVerifyClientProof(storedKey, authMessage, clientProof)
	return err
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

// ComputeChannelBindingData computes the channel binding data for SCRAM.
// For no channel binding (gs2-header = "n,,"), this returns the bytes of "n,,".
func ComputeChannelBindingData(channelBindingType string) []byte {
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
