// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scram

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestHash creates a ScramHash for a given password, salt, and iterations.
// This simulates what would be stored in pg_authid.
func createTestHash(password string, salt []byte, iterations int) *ScramHash {
	saltedPassword := ComputeSaltedPassword(password, salt, iterations)
	clientKey := ComputeClientKey(saltedPassword)
	storedKey := ComputeStoredKey(clientKey)
	serverKey := ComputeServerKey(saltedPassword)
	return &ScramHash{
		Iterations: iterations,
		Salt:       salt,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
	}
}

// extractClientNonce extracts the client nonce from a client-first-message.
// The message format is: n,,n=<username>,r=<nonce>
func extractClientNonce(clientFirstMessage string) string {
	// Skip GS2 header "n,,"
	bare := strings.TrimPrefix(clientFirstMessage, "n,,")
	for part := range strings.SplitSeq(bare, ",") {
		if strings.HasPrefix(part, "r=") {
			return part[2:]
		}
	}
	return ""
}

func TestNewScramAuthenticator(t *testing.T) {
	t.Run("creates authenticator with valid hash", func(t *testing.T) {
		hash := createTestHash("password", []byte("testsalt12345678"), 4096)
		auth := NewScramAuthenticator(hash, "testdb")
		require.NotNil(t, auth)
	})

	t.Run("panics with nil hash", func(t *testing.T) {
		assert.Panics(t, func() {
			NewScramAuthenticator(nil, "testdb")
		})
	})
}

func TestScramAuthenticator_StartAuthentication(t *testing.T) {
	t.Run("returns SASL mechanism list", func(t *testing.T) {
		hash := createTestHash("password", []byte("testsalt12345678"), 4096)
		auth := NewScramAuthenticator(hash, "testdb")

		mechanisms := auth.StartAuthentication()

		assert.Contains(t, mechanisms, ScramSHA256Mechanism)
	})
}

func TestScramAuthenticator_HandleClientFirst(t *testing.T) {
	testSalt := []byte("testsalt12345678")
	testIterations := 4096
	testPassword := "testpassword"

	t.Run("valid client-first-message", func(t *testing.T) {
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		client := NewSCRAMClientWithPassword("testuser", testPassword)
		clientFirstMessage, err := client.ClientFirstMessage()
		require.NoError(t, err)
		clientNonce := extractClientNonce(clientFirstMessage)

		serverFirstMessage, err := auth.HandleClientFirst(clientFirstMessage, "testuser")
		require.NoError(t, err)

		// Server-first-message should contain: r=<combined-nonce>, s=<salt-b64>, i=<iterations>
		assert.Contains(t, serverFirstMessage, "r="+clientNonce) // Should start with client nonce
		assert.Contains(t, serverFirstMessage, "s=")
		assert.Contains(t, serverFirstMessage, "i=")
	})

	t.Run("invalid client-first-message - empty", func(t *testing.T) {
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		_, err := auth.HandleClientFirst("", "")
		require.Error(t, err)
	})

	t.Run("invalid client-first-message - missing nonce", func(t *testing.T) {
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		// Note: empty username is now allowed (uses fallback), but missing nonce is still an error
		_, err := auth.HandleClientFirst("n,,n=user", "user")
		require.Error(t, err)
	})

	t.Run("empty username in client-first-message uses startup message username", func(t *testing.T) {
		// pgx sends empty username in SCRAM client-first-message (n,,n=,r=...)
		// and expects the server to use the username from the startup message.
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		// Client sends empty username in SCRAM, startup message has "startupuser"
		clientFirstMessage := "n,,n=,r=clientnonce12345"
		serverFirstMessage, err := auth.HandleClientFirst(clientFirstMessage, "startupuser")
		require.NoError(t, err)
		assert.Contains(t, serverFirstMessage, "r=clientnonce12345") // Combined nonce starts with client nonce
	})

	t.Run("empty username with no startup message username returns error", func(t *testing.T) {
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		// Client sends empty username with no startup message username - should fail
		clientFirstMessage := "n,,n=,r=clientnonce12345"
		_, err := auth.HandleClientFirst(clientFirstMessage, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "startup message")
	})

	t.Run("client-first-message username is ignored, startup message username always used", func(t *testing.T) {
		// PostgreSQL ALWAYS ignores the username from client-first-message and uses
		// the startup message username. This test verifies that behavior.
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		// Client sends "wronguser" in client-first-message but "actualuser" in startup message.
		// PostgreSQL behavior: ALWAYS use startup message username.
		clientFirstMessage := "n,,n=wronguser,r=clientnonce12345"
		serverFirstMessage, err := auth.HandleClientFirst(clientFirstMessage, "actualuser")
		require.NoError(t, err)
		assert.Contains(t, serverFirstMessage, "r=clientnonce12345")

		// Verify that the startup-message username was stored, regardless of
		// what the client put in client-first-message.
		assert.Equal(t, "actualuser", auth.username)
	})

	t.Run("channel binding requested but not supported", func(t *testing.T) {
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		// Client requests channel binding (p=tls-server-end-point)
		clientFirstMessage := "p=tls-server-end-point,,n=testuser,r=clientnonce"

		_, err := auth.HandleClientFirst(clientFirstMessage, "testuser")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel binding")
	})
}

func TestScramAuthenticator_HandleClientFinal(t *testing.T) {
	testSalt := []byte("testsalt12345678")
	testIterations := 4096
	testPassword := "correctpassword"

	t.Run("valid proof - authentication succeeds", func(t *testing.T) {
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		// Use SCRAMClient for the full exchange.
		client := NewSCRAMClientWithPassword("testuser", testPassword)

		// Step 1: Client first message.
		clientFirstMessage, err := client.ClientFirstMessage()
		require.NoError(t, err)
		serverFirstMessage, err := auth.HandleClientFirst(clientFirstMessage, "testuser")
		require.NoError(t, err)

		// Step 2: Client final message with correct password.
		clientFinalMessage, err := client.ProcessServerFirst(serverFirstMessage)
		require.NoError(t, err)

		// Step 3: Server verifies and returns server signature.
		serverFinalMessage, err := auth.HandleClientFinal(clientFinalMessage)
		require.NoError(t, err)

		// Server final should be "v=<server-signature-b64>"
		assert.True(t, len(serverFinalMessage) > 2)
		assert.Equal(t, "v=", serverFinalMessage[:2])

		// Authenticator should report success.
		assert.True(t, auth.IsAuthenticated())
		assert.Equal(t, "testuser", auth.AuthenticatedUser())

		// Client should be able to verify server signature.
		err = client.VerifyServerFinal(serverFinalMessage)
		require.NoError(t, err)
	})

	t.Run("invalid proof - wrong password", func(t *testing.T) {
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		// Client uses wrong password.
		client := NewSCRAMClientWithPassword("testuser", "wrongpassword")

		// Step 1: Client first message.
		clientFirstMessage, err := client.ClientFirstMessage()
		require.NoError(t, err)
		serverFirstMessage, err := auth.HandleClientFirst(clientFirstMessage, "testuser")
		require.NoError(t, err)

		// Step 2: Client final message with WRONG password.
		clientFinalMessage, err := client.ProcessServerFirst(serverFirstMessage)
		require.NoError(t, err)

		// Step 3: Server should reject.
		_, err = auth.HandleClientFinal(clientFinalMessage)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrAuthenticationFailed))

		// Authenticator should report failure.
		assert.False(t, auth.IsAuthenticated())
	})

	t.Run("invalid client-final-message - empty", func(t *testing.T) {
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		client := NewSCRAMClientWithPassword("testuser", testPassword)
		clientFirstMessage, err := client.ClientFirstMessage()
		require.NoError(t, err)
		_, err = auth.HandleClientFirst(clientFirstMessage, "testuser")
		require.NoError(t, err)

		_, err = auth.HandleClientFinal("")
		require.Error(t, err)
	})

	t.Run("nonce mismatch - server nonce tampered", func(t *testing.T) {
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		client := NewSCRAMClientWithPassword("testuser", testPassword)
		clientFirstMessage, err := client.ClientFirstMessage()
		require.NoError(t, err)
		_, err = auth.HandleClientFirst(clientFirstMessage, "testuser")
		require.NoError(t, err)

		// Send a client-final-message with a different nonce (attacker trying to replay).
		tamperedFinalMessage := "c=biws,r=differentnonce,p=fakeproof"
		_, err = auth.HandleClientFinal(tamperedFinalMessage)
		require.Error(t, err)
	})

	t.Run("called without HandleClientFirst - state error", func(t *testing.T) {
		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		// Skip HandleClientFirst, go directly to HandleClientFinal.
		_, err := auth.HandleClientFinal("c=biws,r=nonce,p=proof")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "state")
	})
}

func TestScramAuthenticator_FullExchange(t *testing.T) {
	// This test validates complete SCRAM exchange between SCRAMClient and ScramAuthenticator.

	t.Run("complete successful authentication", func(t *testing.T) {
		testSalt := []byte("randomsalt123456")
		testIterations := 4096
		testPassword := "pencil"

		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")

		// 1. Server sends mechanism list (AuthSASL).
		mechanisms := auth.StartAuthentication()
		assert.Contains(t, mechanisms, "SCRAM-SHA-256")

		// 2. Client sends SASLInitialResponse with client-first-message.
		client := NewSCRAMClientWithPassword("user", testPassword)
		clientFirstMessage, err := client.ClientFirstMessage()
		require.NoError(t, err)
		clientNonce := extractClientNonce(clientFirstMessage)

		serverFirstMessage, err := auth.HandleClientFirst(clientFirstMessage, "user")
		require.NoError(t, err)

		// 3. Verify server-first-message format.
		parsedNonce, parsedSalt, parsedIterations, err := parseServerFirstMessage(serverFirstMessage)
		require.NoError(t, err)
		assert.True(t, len(parsedNonce) > len(clientNonce)) // Combined nonce is longer
		assert.Equal(t, clientNonce, parsedNonce[:len(clientNonce)])
		assert.Equal(t, testSalt, parsedSalt)
		assert.Equal(t, testIterations, parsedIterations)

		// 4. Client computes and sends client-final-message.
		clientFinalMessage, err := client.ProcessServerFirst(serverFirstMessage)
		require.NoError(t, err)

		// 5. Server verifies and sends server-final-message.
		serverFinalMessage, err := auth.HandleClientFinal(clientFinalMessage)
		require.NoError(t, err)

		// 6. Client verifies server signature (mutual authentication).
		err = client.VerifyServerFinal(serverFinalMessage)
		require.NoError(t, err)

		// 7. Check final state.
		assert.True(t, auth.IsAuthenticated())
		assert.Equal(t, "user", auth.AuthenticatedUser())
	})

	t.Run("authentication with special characters in username", func(t *testing.T) {
		testSalt := []byte("salt1234567890ab")
		testIterations := 4096
		testPassword := "password123"

		// Username with special characters that need SASL encoding.
		username := "user=with,special"

		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")
		auth.StartAuthentication()

		// SCRAMClient handles username encoding automatically.
		client := NewSCRAMClientWithPassword(username, testPassword)
		clientFirstMessage, err := client.ClientFirstMessage()
		require.NoError(t, err)

		serverFirstMessage, err := auth.HandleClientFirst(clientFirstMessage, username)
		require.NoError(t, err)

		clientFinalMessage, err := client.ProcessServerFirst(serverFirstMessage)
		require.NoError(t, err)

		serverFinalMessage, err := auth.HandleClientFinal(clientFinalMessage)
		require.NoError(t, err)

		err = client.VerifyServerFinal(serverFinalMessage)
		require.NoError(t, err)

		assert.True(t, auth.IsAuthenticated())
		assert.Equal(t, username, auth.AuthenticatedUser())
	})
}

func TestScramAuthenticator_Reset(t *testing.T) {
	t.Run("reset clears state for reuse", func(t *testing.T) {
		testSalt := []byte("testsalt12345678")
		testIterations := 4096
		testPassword := "password"

		hash := createTestHash(testPassword, testSalt, testIterations)
		auth := NewScramAuthenticator(hash, "testdb")

		// Complete a successful authentication.
		auth.StartAuthentication()
		client := NewSCRAMClientWithPassword("testuser", testPassword)
		clientFirstMessage, _ := client.ClientFirstMessage()
		serverFirstMessage, _ := auth.HandleClientFirst(clientFirstMessage, "testuser")
		clientFinalMessage, _ := client.ProcessServerFirst(serverFirstMessage)
		_, _ = auth.HandleClientFinal(clientFinalMessage)

		assert.True(t, auth.IsAuthenticated())

		// Reset the authenticator.
		auth.Reset()

		// State should be cleared.
		assert.False(t, auth.IsAuthenticated())
		assert.Equal(t, "", auth.AuthenticatedUser())

		// Should be able to authenticate again with a new client.
		auth.StartAuthentication()
		client2 := NewSCRAMClientWithPassword("testuser", testPassword)
		clientFirstMessage2, err := client2.ClientFirstMessage()
		require.NoError(t, err)
		clientNonce2 := extractClientNonce(clientFirstMessage2)

		serverFirstMessage2, err := auth.HandleClientFirst(clientFirstMessage2, "testuser")
		require.NoError(t, err)
		assert.Contains(t, serverFirstMessage2, "r="+clientNonce2)
	})
}

func TestScramAuthenticatorState(t *testing.T) {
	t.Run("state transitions are enforced", func(t *testing.T) {
		hash := createTestHash("password", []byte("salt12345678"), 4096)
		auth := NewScramAuthenticator(hash, "testdb")

		// Cannot call HandleClientFirst before StartAuthentication.
		_, err := auth.HandleClientFirst("n,,n=user,r=nonce", "user")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "state")

		// Start authentication.
		auth.StartAuthentication()

		// Cannot call HandleClientFinal before HandleClientFirst.
		_, err = auth.HandleClientFinal("c=biws,r=nonce,p=proof")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "state")

		// Now proceed normally.
		_, err = auth.HandleClientFirst("n,,n=user,r=clientnonce", "user")
		require.NoError(t, err)

		// Cannot call HandleClientFirst again.
		_, err = auth.HandleClientFirst("n,,n=user,r=anothernonce", "user")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "state")
	})
}
