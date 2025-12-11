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
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSCRAMClient_PasswordMode(t *testing.T) {
	t.Run("full exchange with password", func(t *testing.T) {
		// Server setup: generate SCRAM credentials.
		password := "testpassword123"
		salt := []byte("randomsalt123456")
		iterations := 4096

		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		storedKey := ComputeStoredKey(ComputeClientKey(saltedPassword))
		serverKey := ComputeServerKey(saltedPassword)

		// Client creates first message.
		client := NewSCRAMClientWithPassword("testuser", password)
		clientFirst, err := client.ClientFirstMessage()
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(clientFirst, "n,,n=testuser,r="))

		// Parse client nonce for server response.
		clientFirstBare := clientFirst[3:] // Remove "n,,"
		parsed, err := parseClientFirstMessage(clientFirst)
		require.NoError(t, err)

		// Server generates server-first-message.
		serverFirst, combinedNonce, err := generateServerFirstMessage(parsed.ClientNonce, salt, iterations)
		require.NoError(t, err)

		// Client processes server-first and generates client-final.
		clientFinal, err := client.ProcessServerFirst(serverFirst)
		require.NoError(t, err)
		assert.Contains(t, clientFinal, "c=biws")
		assert.Contains(t, clientFinal, "r="+combinedNonce)
		assert.Contains(t, clientFinal, ",p=")

		// Server verifies client proof.
		clientFinalParsed, err := parseClientFinalMessage(clientFinal)
		require.NoError(t, err)

		authMessage := buildAuthMessage(clientFirstBare, serverFirst, clientFinalParsed.ClientFinalMessageWithoutProof)
		err = VerifyClientProof(storedKey, authMessage, clientFinalParsed.Proof)
		assert.NoError(t, err, "server should verify client proof")

		// Server generates server-final-message.
		serverSignature := ComputeServerSignature(serverKey, authMessage)
		serverFinal := generateServerFinalMessage(serverSignature)

		// Client verifies server signature (mutual auth).
		err = client.VerifyServerFinal(serverFinal)
		assert.NoError(t, err, "client should verify server signature")
	})

	t.Run("wrong password fails", func(t *testing.T) {
		password := "correctpassword"
		salt := []byte("randomsalt123456")
		iterations := 4096

		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		storedKey := ComputeStoredKey(ComputeClientKey(saltedPassword))

		// Client with wrong password.
		client := NewSCRAMClientWithPassword("testuser", "wrongpassword")
		clientFirst, err := client.ClientFirstMessage()
		require.NoError(t, err)

		parsed, err := parseClientFirstMessage(clientFirst)
		require.NoError(t, err)

		serverFirst, _, err := generateServerFirstMessage(parsed.ClientNonce, salt, iterations)
		require.NoError(t, err)

		clientFinal, err := client.ProcessServerFirst(serverFirst)
		require.NoError(t, err)

		// Server should reject the proof.
		clientFinalParsed, err := parseClientFinalMessage(clientFinal)
		require.NoError(t, err)

		clientFirstBare := clientFirst[3:]
		authMessage := buildAuthMessage(clientFirstBare, serverFirst, clientFinalParsed.ClientFinalMessageWithoutProof)
		err = VerifyClientProof(storedKey, authMessage, clientFinalParsed.Proof)
		assert.Error(t, err, "server should reject wrong password")
	})
}

func TestSCRAMClient_PassthroughMode(t *testing.T) {
	t.Run("full exchange with extracted keys", func(t *testing.T) {
		// This test validates the SCRAM passthrough mechanism:
		// 1. Client authenticates to proxy with password
		// 2. Proxy extracts ClientKey from the proof
		// 3. Proxy uses extracted ClientKey to authenticate to PostgreSQL

		password := "testpassword123"
		salt := []byte("randomsalt123456")
		iterations := 4096

		// Original client authenticates to proxy
		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		clientKey := ComputeClientKey(saltedPassword)
		storedKey := ComputeStoredKey(clientKey)
		serverKey := ComputeServerKey(saltedPassword)

		// Original client sends proof.
		originalClient := NewSCRAMClientWithPassword("testuser", password)
		clientFirst1, err := originalClient.ClientFirstMessage()
		require.NoError(t, err)

		parsed1, err := parseClientFirstMessage(clientFirst1)
		require.NoError(t, err)

		serverFirst1, _, err := generateServerFirstMessage(parsed1.ClientNonce, salt, iterations)
		require.NoError(t, err)

		clientFinal1, err := originalClient.ProcessServerFirst(serverFirst1)
		require.NoError(t, err)

		// Proxy verifies and extracts ClientKey.
		clientFinalParsed1, err := parseClientFinalMessage(clientFinal1)
		require.NoError(t, err)

		clientFirstBare1 := clientFirst1[3:]
		authMessage1 := buildAuthMessage(clientFirstBare1, serverFirst1, clientFinalParsed1.ClientFinalMessageWithoutProof)
		extractedClientKey, err := ExtractAndVerifyClientProof(storedKey, authMessage1, clientFinalParsed1.Proof)
		require.NoError(t, err, "proxy should verify original client")

		// Verify extracted key matches original.
		assert.Equal(t, clientKey, extractedClientKey, "extracted ClientKey should match original")

		// === Phase 2: Proxy authenticates to PostgreSQL using extracted keys ===
		// Create a new client using extracted keys (no password!).
		passthroughClient := NewSCRAMClientWithKeys("testuser", extractedClientKey, serverKey)

		clientFirst2, err := passthroughClient.ClientFirstMessage()
		require.NoError(t, err)

		parsed2, err := parseClientFirstMessage(clientFirst2)
		require.NoError(t, err)

		// PostgreSQL sends its own challenge (with same salt/iterations).
		serverFirst2, _, err := generateServerFirstMessage(parsed2.ClientNonce, salt, iterations)
		require.NoError(t, err)

		clientFinal2, err := passthroughClient.ProcessServerFirst(serverFirst2)
		require.NoError(t, err)

		// PostgreSQL verifies the proof.
		clientFinalParsed2, err := parseClientFinalMessage(clientFinal2)
		require.NoError(t, err)

		clientFirstBare2 := clientFirst2[3:]
		authMessage2 := buildAuthMessage(clientFirstBare2, serverFirst2, clientFinalParsed2.ClientFinalMessageWithoutProof)
		err = VerifyClientProof(storedKey, authMessage2, clientFinalParsed2.Proof)
		assert.NoError(t, err, "PostgreSQL should verify passthrough client")

		// PostgreSQL sends server signature.
		serverSignature2 := ComputeServerSignature(serverKey, authMessage2)
		serverFinal2 := generateServerFinalMessage(serverSignature2)

		// Passthrough client verifies server signature.
		err = passthroughClient.VerifyServerFinal(serverFinal2)
		assert.NoError(t, err, "passthrough client should verify server signature")
	})

	t.Run("passthrough with different nonce succeeds", func(t *testing.T) {
		// The key insight: each SCRAM exchange uses different nonces,
		// but the same ClientKey/ServerKey work because they're derived
		// from the password, not the nonce.

		password := "mypassword"
		salt := []byte("salt1234567890ab")
		iterations := 4096

		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		clientKey := ComputeClientKey(saltedPassword)
		storedKey := ComputeStoredKey(clientKey)
		serverKey := ComputeServerKey(saltedPassword)

		// Multiple authentications with the same extracted keys should all succeed.
		for i := range 3 {
			client := NewSCRAMClientWithKeys("user", clientKey, serverKey)

			clientFirst, err := client.ClientFirstMessage()
			require.NoError(t, err)

			parsed, err := parseClientFirstMessage(clientFirst)
			require.NoError(t, err)

			serverFirst, _, err := generateServerFirstMessage(parsed.ClientNonce, salt, iterations)
			require.NoError(t, err)

			clientFinal, err := client.ProcessServerFirst(serverFirst)
			require.NoError(t, err)

			clientFinalParsed, err := parseClientFinalMessage(clientFinal)
			require.NoError(t, err)

			clientFirstBare := clientFirst[3:]
			authMessage := buildAuthMessage(clientFirstBare, serverFirst, clientFinalParsed.ClientFinalMessageWithoutProof)
			err = VerifyClientProof(storedKey, authMessage, clientFinalParsed.Proof)
			assert.NoError(t, err, "attempt %d should succeed", i+1)
		}
	})
}

func TestSCRAMClient_EdgeCases(t *testing.T) {
	t.Run("rejects invalid server nonce", func(t *testing.T) {
		client := NewSCRAMClientWithPassword("user", "pass")
		_, err := client.ClientFirstMessage()
		require.NoError(t, err)

		// Server sends nonce that doesn't start with client nonce.
		badServerFirst := "r=differentnonce,s=" + base64.StdEncoding.EncodeToString([]byte("salt")) + ",i=4096"
		_, err = client.ProcessServerFirst(badServerFirst)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nonce")
	})

	t.Run("rejects invalid server final", func(t *testing.T) {
		password := "testpass"
		salt := []byte("salt12345678")
		iterations := 4096

		saltedPassword := ComputeSaltedPassword(password, salt, iterations)
		serverKey := ComputeServerKey(saltedPassword)

		client := NewSCRAMClientWithPassword("user", password)
		clientFirst, err := client.ClientFirstMessage()
		require.NoError(t, err)

		parsed, err := parseClientFirstMessage(clientFirst)
		require.NoError(t, err)

		serverFirst, _, err := generateServerFirstMessage(parsed.ClientNonce, salt, iterations)
		require.NoError(t, err)

		_, err = client.ProcessServerFirst(serverFirst)
		require.NoError(t, err)

		// Send wrong server signature.
		wrongServerKey := ComputeServerKey(ComputeSaltedPassword("wrongpass", salt, iterations))
		authMessage := client.authMessage
		wrongSig := ComputeServerSignature(wrongServerKey, authMessage)
		wrongServerFinal := generateServerFinalMessage(wrongSig)

		err = client.VerifyServerFinal(wrongServerFinal)
		assert.Error(t, err, "should reject wrong server signature")

		// Correct server signature should work.
		correctSig := ComputeServerSignature(serverKey, authMessage)
		correctServerFinal := generateServerFinalMessage(correctSig)
		err = client.VerifyServerFinal(correctServerFinal)
		assert.NoError(t, err)
	})

	t.Run("handles special characters in username", func(t *testing.T) {
		// Username with comma and equals (need SASL encoding).
		client := NewSCRAMClientWithPassword("user=with,special", "pass")
		clientFirst, err := client.ClientFirstMessage()
		require.NoError(t, err)

		// Verify special chars are encoded.
		assert.Contains(t, clientFirst, "n=user=3Dwith=2Cspecial")
	})
}
