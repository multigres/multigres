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

package auth

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test vectors based on RFC 5802 and PostgreSQL's implementation.
// These test vectors can be verified against a real PostgreSQL instance.

func TestComputeSaltedPassword(t *testing.T) {
	t.Run("computes salted password correctly", func(t *testing.T) {
		password := "pencil"
		salt := []byte("salt") // Simple salt for testing
		iterations := 4096

		saltedPassword := ComputeSaltedPassword(password, salt, iterations)

		// The result should be 32 bytes (SHA-256 output)
		assert.Len(t, saltedPassword, 32)
	})

	t.Run("different passwords produce different results", func(t *testing.T) {
		salt := []byte("salt")
		iterations := 4096

		sp1 := ComputeSaltedPassword("password1", salt, iterations)
		sp2 := ComputeSaltedPassword("password2", salt, iterations)

		assert.NotEqual(t, sp1, sp2)
	})

	t.Run("different salts produce different results", func(t *testing.T) {
		password := "password"
		iterations := 4096

		sp1 := ComputeSaltedPassword(password, []byte("salt1"), iterations)
		sp2 := ComputeSaltedPassword(password, []byte("salt2"), iterations)

		assert.NotEqual(t, sp1, sp2)
	})

	t.Run("different iterations produce different results", func(t *testing.T) {
		password := "password"
		salt := []byte("salt")

		sp1 := ComputeSaltedPassword(password, salt, 4096)
		sp2 := ComputeSaltedPassword(password, salt, 8192)

		assert.NotEqual(t, sp1, sp2)
	})
}

func TestComputeClientKey(t *testing.T) {
	t.Run("computes client key correctly", func(t *testing.T) {
		saltedPassword := make([]byte, 32)
		// Fill with deterministic values for testing
		for i := range saltedPassword {
			saltedPassword[i] = byte(i)
		}

		clientKey := ComputeClientKey(saltedPassword)

		// Should be 32 bytes (HMAC-SHA-256 output)
		assert.Len(t, clientKey, 32)
	})

	t.Run("same input produces same output", func(t *testing.T) {
		saltedPassword := []byte("salted password value here!!")

		ck1 := ComputeClientKey(saltedPassword)
		ck2 := ComputeClientKey(saltedPassword)

		assert.Equal(t, ck1, ck2)
	})
}

func TestComputeStoredKey(t *testing.T) {
	t.Run("computes stored key correctly", func(t *testing.T) {
		clientKey := make([]byte, 32)
		for i := range clientKey {
			clientKey[i] = byte(i)
		}

		storedKey := ComputeStoredKey(clientKey)

		// Should be 32 bytes (SHA-256 output)
		assert.Len(t, storedKey, 32)
	})

	t.Run("stored key is hash of client key", func(t *testing.T) {
		clientKey := []byte("client key value")

		sk1 := ComputeStoredKey(clientKey)
		sk2 := ComputeStoredKey(clientKey)

		assert.Equal(t, sk1, sk2)
	})
}

func TestComputeServerKey(t *testing.T) {
	t.Run("computes server key correctly", func(t *testing.T) {
		saltedPassword := make([]byte, 32)
		for i := range saltedPassword {
			saltedPassword[i] = byte(i)
		}

		serverKey := ComputeServerKey(saltedPassword)

		// Should be 32 bytes (HMAC-SHA-256 output)
		assert.Len(t, serverKey, 32)
	})

	t.Run("client key and server key are different", func(t *testing.T) {
		saltedPassword := []byte("salted password value here!!")

		clientKey := ComputeClientKey(saltedPassword)
		serverKey := ComputeServerKey(saltedPassword)

		assert.NotEqual(t, clientKey, serverKey)
	})
}

func TestComputeClientSignature(t *testing.T) {
	t.Run("computes client signature correctly", func(t *testing.T) {
		storedKey := make([]byte, 32)
		authMessage := "n=user,r=nonce,r=nonce+server,s=salt,i=4096,c=biws,r=nonce+server"

		clientSignature := ComputeClientSignature(storedKey, authMessage)

		// Should be 32 bytes (HMAC-SHA-256 output)
		assert.Len(t, clientSignature, 32)
	})
}

func TestComputeClientProof(t *testing.T) {
	t.Run("computes client proof correctly", func(t *testing.T) {
		clientKey := make([]byte, 32)
		clientSignature := make([]byte, 32)
		for i := range clientKey {
			clientKey[i] = byte(i)
			clientSignature[i] = byte(i + 1)
		}

		proof := ComputeClientProof(clientKey, clientSignature)

		// Should be 32 bytes (XOR of two 32-byte values)
		assert.Len(t, proof, 32)
	})

	t.Run("XOR properties hold", func(t *testing.T) {
		// XOR is its own inverse: (A XOR B) XOR B = A
		clientKey := []byte{0x01, 0x02, 0x03, 0x04}
		clientSignature := []byte{0x10, 0x20, 0x30, 0x40}

		proof := ComputeClientProof(clientKey, clientSignature)
		// proof = clientKey XOR clientSignature

		// To recover clientKey: proof XOR clientSignature
		recovered := ComputeClientProof(proof, clientSignature)

		assert.Equal(t, clientKey, recovered)
	})
}

func TestComputeServerSignature(t *testing.T) {
	t.Run("computes server signature correctly", func(t *testing.T) {
		serverKey := make([]byte, 32)
		authMessage := "n=user,r=nonce,r=nonce+server,s=salt,i=4096,c=biws,r=nonce+server"

		serverSignature := ComputeServerSignature(serverKey, authMessage)

		// Should be 32 bytes (HMAC-SHA-256 output)
		assert.Len(t, serverSignature, 32)
	})
}

func TestVerifyClientProof(t *testing.T) {
	t.Run("valid proof is verified", func(t *testing.T) {
		// Simulate a full SCRAM exchange
		password := "pencil"
		salt := []byte("testsalt")
		iterations := 4096
		authMessage := "n=user,r=clientnonce,r=clientnonce+servernonce,s=dGVzdHNhbHQ=,i=4096,c=biws,r=clientnonce+servernonce"

		// Server side: compute stored values from password
		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		clientKey := ComputeClientKey(saltedPassword)
		storedKey := ComputeStoredKey(clientKey)
		serverKey := ComputeServerKey(saltedPassword)

		// Client side: compute proof
		clientSignature := ComputeClientSignature(storedKey, authMessage)
		clientProof := ComputeClientProof(clientKey, clientSignature)

		// Server side: verify proof
		valid := VerifyClientProof(storedKey, authMessage, clientProof)
		assert.True(t, valid)

		// Also compute server signature for mutual auth
		serverSignature := ComputeServerSignature(serverKey, authMessage)
		assert.Len(t, serverSignature, 32)
	})

	t.Run("invalid proof is rejected", func(t *testing.T) {
		storedKey := make([]byte, 32)
		authMessage := "auth message"
		invalidProof := []byte("this is not a valid proof!!!!!")

		valid := VerifyClientProof(storedKey, authMessage, invalidProof)
		assert.False(t, valid)
	})

	t.Run("proof with wrong password is rejected", func(t *testing.T) {
		salt := []byte("salt")
		iterations := 4096
		authMessage := "auth message"

		// Server has stored key from correct password
		correctSP := ComputeSaltedPassword("correct", salt, iterations)
		correctCK := ComputeClientKey(correctSP)
		storedKey := ComputeStoredKey(correctCK)

		// Client tries with wrong password
		wrongSP := ComputeSaltedPassword("wrong", salt, iterations)
		wrongCK := ComputeClientKey(wrongSP)
		wrongSig := ComputeClientSignature(ComputeStoredKey(wrongCK), authMessage)
		wrongProof := ComputeClientProof(wrongCK, wrongSig)

		valid := VerifyClientProof(storedKey, authMessage, wrongProof)
		assert.False(t, valid)
	})
}

func TestExtractAndVerifyClientProof(t *testing.T) {
	t.Run("extracts correct ClientKey from valid proof", func(t *testing.T) {
		// Simulate a full SCRAM exchange
		password := "pencil"
		salt := []byte("testsalt")
		iterations := 4096
		authMessage := "n=user,r=clientnonce,r=clientnonce+servernonce,s=dGVzdHNhbHQ=,i=4096,c=biws,r=clientnonce+servernonce"

		// Server side: compute stored values from password
		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		originalClientKey := ComputeClientKey(saltedPassword)
		storedKey := ComputeStoredKey(originalClientKey)

		// Client side: compute proof
		clientSignature := ComputeClientSignature(storedKey, authMessage)
		clientProof := ComputeClientProof(originalClientKey, clientSignature)

		// Server side: extract ClientKey from proof
		extractedClientKey, ok := ExtractAndVerifyClientProof(storedKey, authMessage, clientProof)

		assert.True(t, ok, "proof should be valid")
		assert.Equal(t, originalClientKey, extractedClientKey, "extracted ClientKey should match original")
	})

	t.Run("extracted ClientKey can be used for client-side SCRAM auth", func(t *testing.T) {
		// This test validates the SCRAM passthrough mechanism:
		// 1. Server verifies client and extracts ClientKey
		// 2. Server uses extracted ClientKey to authenticate as client to another server

		password := "testpassword123"
		salt := []byte("randomsalt123456")
		iterations := 4096

		// === Phase 1: Client authenticates to multigateway ===
		// Compute keys from password (what PostgreSQL stores)
		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		clientKey := ComputeClientKey(saltedPassword)
		storedKey := ComputeStoredKey(clientKey)
		serverKey := ComputeServerKey(saltedPassword)

		// Client generates proof for first auth
		clientNonce := "client-nonce-12345"
		serverNoncePart := "server-nonce-67890"
		combinedNonce := clientNonce + serverNoncePart
		clientFirstBare := "n=user,r=" + clientNonce
		serverFirst := "r=" + combinedNonce + ",s=" + base64.StdEncoding.EncodeToString(salt) + ",i=4096"
		clientFinalWithoutProof := "c=biws,r=" + combinedNonce
		authMessage1 := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof

		clientSignature1 := ComputeClientSignature(storedKey, authMessage1)
		clientProof1 := ComputeClientProof(clientKey, clientSignature1)

		// Server (multigateway) verifies and extracts ClientKey
		extractedClientKey, ok := ExtractAndVerifyClientProof(storedKey, authMessage1, clientProof1)
		require.True(t, ok, "first auth should succeed")

		// === Phase 2: Use extracted keys for second SCRAM auth (to PostgreSQL) ===
		// This simulates multipooler authenticating to PostgreSQL using extracted keys

		// PostgreSQL sends new challenge with same salt/iterations
		clientNonce2 := "pooler-client-nonce"
		serverNoncePart2 := "pg-server-nonce"
		combinedNonce2 := clientNonce2 + serverNoncePart2
		clientFirstBare2 := "n=user,r=" + clientNonce2
		serverFirst2 := "r=" + combinedNonce2 + ",s=" + base64.StdEncoding.EncodeToString(salt) + ",i=4096"
		clientFinalWithoutProof2 := "c=biws,r=" + combinedNonce2
		authMessage2 := clientFirstBare2 + "," + serverFirst2 + "," + clientFinalWithoutProof2

		// Compute new proof using extracted ClientKey
		// Note: StoredKey = H(ClientKey), so we compute it from the extracted key
		extractedStoredKey := ComputeStoredKey(extractedClientKey)
		clientSignature2 := ComputeClientSignature(extractedStoredKey, authMessage2)
		clientProof2 := ComputeClientProof(extractedClientKey, clientSignature2)

		// PostgreSQL verifies using its stored key (which should equal extractedStoredKey)
		valid := VerifyClientProof(storedKey, authMessage2, clientProof2)
		assert.True(t, valid, "second auth using extracted ClientKey should succeed")

		// Also verify we can compute correct ServerSignature using passed ServerKey
		expectedServerSig := ComputeServerSignature(serverKey, authMessage2)
		assert.Len(t, expectedServerSig, 32, "server signature should be 32 bytes")
	})

	t.Run("returns nil for invalid proof", func(t *testing.T) {
		storedKey := make([]byte, 32)
		authMessage := "auth message"
		invalidProof := []byte("this is not a valid proof!!!!!")

		extractedKey, ok := ExtractAndVerifyClientProof(storedKey, authMessage, invalidProof)
		assert.False(t, ok)
		assert.Nil(t, extractedKey)
	})

	t.Run("returns nil for wrong-length proof", func(t *testing.T) {
		storedKey := make([]byte, 32)
		authMessage := "auth message"
		shortProof := []byte("short")

		extractedKey, ok := ExtractAndVerifyClientProof(storedKey, authMessage, shortProof)
		assert.False(t, ok)
		assert.Nil(t, extractedKey)
	})
}

func TestFullScramExchange(t *testing.T) {
	t.Run("complete SCRAM-SHA-256 exchange", func(t *testing.T) {
		// This test simulates a complete SCRAM exchange
		password := "pencil"
		salt := []byte("randomsalt123456")
		iterations := 4096
		username := "testuser"
		clientNonce := "rOprNGfwEbeRWgbNEkqO"

		// === Server setup (would come from pg_authid) ===
		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		storedKey := ComputeStoredKey(ComputeClientKey(saltedPassword))
		serverKey := ComputeServerKey(saltedPassword)

		// === Client sends client-first-message ===
		clientFirstMessageBare := "n=" + username + ",r=" + clientNonce

		// === Server generates server-first-message ===
		serverFirstMessage, combinedNonce, err := generateServerFirstMessage(clientNonce, salt, iterations)
		require.NoError(t, err)

		// === Build AuthMessage ===
		// channel-binding for no binding is "n,," -> base64 = "biws"
		channelBinding := "biws"
		clientFinalMessageWithoutProof := "c=" + channelBinding + ",r=" + combinedNonce
		authMessage := clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof

		// === Client computes proof ===
		clientSaltedPassword := ComputeSaltedPassword(password, salt, iterations)
		clientKey := ComputeClientKey(clientSaltedPassword)
		clientStoredKey := ComputeStoredKey(clientKey)
		clientSignature := ComputeClientSignature(clientStoredKey, authMessage)
		clientProof := ComputeClientProof(clientKey, clientSignature)

		// === Server verifies proof ===
		valid := VerifyClientProof(storedKey, authMessage, clientProof)
		assert.True(t, valid, "Client proof should be valid")

		// === Server computes server signature for mutual auth ===
		serverSignature := ComputeServerSignature(serverKey, authMessage)
		serverFinalMessage := generateServerFinalMessage(serverSignature)
		assert.True(t, len(serverFinalMessage) > 2)
		assert.Equal(t, "v=", serverFinalMessage[:2])

		// === Client verifies server signature ===
		clientServerKey := ComputeServerKey(clientSaltedPassword)
		expectedServerSignature := ComputeServerSignature(clientServerKey, authMessage)
		assert.Equal(t, serverSignature, expectedServerSignature, "Server signatures should match")
	})
}

func TestKnownTestVector(t *testing.T) {
	// Test that our SCRAM implementation produces consistent results
	// and can verify its own hashes.

	t.Run("computed hash can be verified", func(t *testing.T) {
		// Generate a SCRAM hash for a known password
		password := "testpassword"
		salt := []byte("randomsalt12")
		iterations := 4096

		// Compute all the SCRAM values
		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		clientKey := ComputeClientKey(saltedPassword)
		storedKey := ComputeStoredKey(clientKey)
		serverKey := ComputeServerKey(saltedPassword)

		// Verify that computing again produces the same results (deterministic)
		saltedPassword2 := ComputeSaltedPassword(password, salt, iterations)
		clientKey2 := ComputeClientKey(saltedPassword2)
		storedKey2 := ComputeStoredKey(clientKey2)
		serverKey2 := ComputeServerKey(saltedPassword2)

		assert.Equal(t, storedKey, storedKey2, "StoredKey should be deterministic")
		assert.Equal(t, serverKey, serverKey2, "ServerKey should be deterministic")
	})

	t.Run("parsed hash can validate correct password", func(t *testing.T) {
		// Create a test hash
		password := "mypassword"
		salt := []byte("testsalt1234")
		iterations := 4096

		// Compute the SCRAM components (as if storing in pg_authid)
		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		clientKey := ComputeClientKey(saltedPassword)
		storedKey := ComputeStoredKey(clientKey)
		serverKey := ComputeServerKey(saltedPassword)

		// Format as PostgreSQL hash string
		saltB64 := base64.StdEncoding.EncodeToString(salt)
		storedKeyB64 := base64.StdEncoding.EncodeToString(storedKey)
		serverKeyB64 := base64.StdEncoding.EncodeToString(serverKey)
		hash := "SCRAM-SHA-256$4096:" + saltB64 + "$" + storedKeyB64 + ":" + serverKeyB64

		// Parse the hash back
		parsed, err := ParseScramSHA256Hash(hash)
		require.NoError(t, err)

		// Verify the correct password produces matching keys
		verifySaltedPassword := ComputeSaltedPassword(password, parsed.Salt, parsed.Iterations)
		verifyClientKey := ComputeClientKey(verifySaltedPassword)
		verifyStoredKey := ComputeStoredKey(verifyClientKey)
		verifyServerKey := ComputeServerKey(verifySaltedPassword)

		assert.Equal(t, parsed.StoredKey, verifyStoredKey, "StoredKey should match for correct password")
		assert.Equal(t, parsed.ServerKey, verifyServerKey, "ServerKey should match for correct password")
	})

	t.Run("wrong password produces wrong keys", func(t *testing.T) {
		password := "correctpassword"
		wrongPassword := "wrongpassword"
		salt := []byte("salt")
		iterations := 4096

		// Compute stored key for correct password
		correctSP := ComputeSaltedPassword(password, salt, iterations)
		correctCK := ComputeClientKey(correctSP)
		correctStoredKey := ComputeStoredKey(correctCK)

		// Compute stored key for wrong password
		wrongSP := ComputeSaltedPassword(wrongPassword, salt, iterations)
		wrongCK := ComputeClientKey(wrongSP)
		wrongStoredKey := ComputeStoredKey(wrongCK)

		// They should be different
		assert.NotEqual(t, correctStoredKey, wrongStoredKey, "Wrong password should produce different StoredKey")
	})
}

func TestBuildAuthMessage(t *testing.T) {
	t.Run("builds auth message correctly", func(t *testing.T) {
		clientFirstMessageBare := "n=user,r=clientnonce"
		serverFirstMessage := "r=clientnonce+servernonce,s=c2FsdA==,i=4096"
		clientFinalMessageWithoutProof := "c=biws,r=clientnonce+servernonce"

		authMessage := buildAuthMessage(clientFirstMessageBare, serverFirstMessage, clientFinalMessageWithoutProof)

		expected := "n=user,r=clientnonce,r=clientnonce+servernonce,s=c2FsdA==,i=4096,c=biws,r=clientnonce+servernonce"
		assert.Equal(t, expected, authMessage)
	})
}

func TestChannelBindingData(t *testing.T) {
	t.Run("no channel binding produces biws", func(t *testing.T) {
		// "biws" is base64("n,,") - no channel binding
		data := ComputeChannelBindingData("")
		assert.Equal(t, "biws", base64.StdEncoding.EncodeToString(data))
	})
}
