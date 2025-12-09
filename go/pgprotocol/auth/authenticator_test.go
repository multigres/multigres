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
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockHashProvider implements PasswordHashProvider for testing.
type mockHashProvider struct {
	hashes map[string]*ScramHash
	err    error
}

func (m *mockHashProvider) GetPasswordHash(_ context.Context, username, _ string) (*ScramHash, error) {
	if m.err != nil {
		return nil, m.err
	}
	hash, ok := m.hashes[username]
	if !ok {
		return nil, ErrUserNotFound
	}
	return hash, nil
}

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

// simulateClientFirstMessage creates a client-first-message as a client would send it.
func simulateClientFirstMessage(username, clientNonce string) string {
	return "n,,n=" + encodeSaslName(username) + ",r=" + clientNonce
}

// simulateClientFinalMessage creates a client-final-message given the server's response.
func simulateClientFinalMessage(password string, serverFirstMessage string, clientFirstMessageBare string, clientNonce string) (string, error) {
	// Parse server-first-message to get combined nonce, salt, iterations.
	combinedNonce, salt, iterations, err := parseServerFirstMessage(serverFirstMessage)
	if err != nil {
		return "", err
	}

	// Verify that combined nonce starts with our client nonce.
	if len(combinedNonce) < len(clientNonce) || combinedNonce[:len(clientNonce)] != clientNonce {
		return "", errors.New("server nonce does not start with client nonce")
	}

	// Compute the client proof.
	saltedPassword := ComputeSaltedPassword(password, salt, iterations)
	clientKey := ComputeClientKey(saltedPassword)
	storedKey := ComputeStoredKey(clientKey)

	// Build client-final-message-without-proof.
	channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,"))
	clientFinalMessageWithoutProof := "c=" + channelBinding + ",r=" + combinedNonce

	// Build AuthMessage.
	authMessage := buildAuthMessage(clientFirstMessageBare, serverFirstMessage, clientFinalMessageWithoutProof)

	// Compute proof.
	clientSignature := ComputeClientSignature(storedKey, authMessage)
	clientProof := ComputeClientProof(clientKey, clientSignature)
	proofB64 := base64.StdEncoding.EncodeToString(clientProof)

	// Build complete client-final-message.
	return clientFinalMessageWithoutProof + ",p=" + proofB64, nil
}

func TestNewScramAuthenticator(t *testing.T) {
	t.Run("creates authenticator with valid config", func(t *testing.T) {
		provider := &mockHashProvider{}
		auth := NewScramAuthenticator(provider, "testdb")
		require.NotNil(t, auth)
	})

	t.Run("panics with nil provider", func(t *testing.T) {
		assert.Panics(t, func() {
			NewScramAuthenticator(nil, "testdb")
		})
	})
}

func TestScramAuthenticator_StartAuthentication(t *testing.T) {
	t.Run("returns SASL mechanism list", func(t *testing.T) {
		provider := &mockHashProvider{}
		auth := NewScramAuthenticator(provider, "testdb")

		mechanisms := auth.StartAuthentication()

		assert.Contains(t, mechanisms, ScramSHA256Mechanism)
	})
}

func TestScramAuthenticator_HandleClientFirst(t *testing.T) {
	testSalt := []byte("testsalt12345678")
	testIterations := 4096
	testPassword := "testpassword"

	t.Run("valid client-first-message", func(t *testing.T) {
		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{
				"testuser": createTestHash(testPassword, testSalt, testIterations),
			},
		}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		clientNonce := "rOprNGfwEbeRWgbNEkqO"
		clientFirstMessage := simulateClientFirstMessage("testuser", clientNonce)

		serverFirstMessage, err := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		require.NoError(t, err)

		// Server-first-message should contain: r=<combined-nonce>, s=<salt-b64>, i=<iterations>
		assert.Contains(t, serverFirstMessage, "r="+clientNonce) // Should start with client nonce
		assert.Contains(t, serverFirstMessage, "s=")
		assert.Contains(t, serverFirstMessage, "i=")
	})

	t.Run("user not found", func(t *testing.T) {
		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{},
		}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		clientFirstMessage := simulateClientFirstMessage("unknownuser", "clientnonce")

		_, err := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrUserNotFound))
	})

	t.Run("invalid client-first-message - empty", func(t *testing.T) {
		provider := &mockHashProvider{}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		_, err := auth.HandleClientFirst(context.Background(), "")
		require.Error(t, err)
	})

	t.Run("invalid client-first-message - missing username", func(t *testing.T) {
		provider := &mockHashProvider{}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		_, err := auth.HandleClientFirst(context.Background(), "n,,r=clientnonce")
		require.Error(t, err)
	})

	t.Run("channel binding requested but not supported", func(t *testing.T) {
		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{
				"testuser": createTestHash(testPassword, testSalt, testIterations),
			},
		}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		// Client requests channel binding (p=tls-server-end-point)
		clientFirstMessage := "p=tls-server-end-point,,n=testuser,r=clientnonce"

		_, err := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel binding")
	})

	t.Run("hash provider error is propagated", func(t *testing.T) {
		expectedErr := errors.New("database connection failed")
		provider := &mockHashProvider{
			err: expectedErr,
		}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		clientFirstMessage := simulateClientFirstMessage("testuser", "clientnonce")

		_, err := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "database connection failed")
	})
}

func TestScramAuthenticator_HandleClientFinal(t *testing.T) {
	testSalt := []byte("testsalt12345678")
	testIterations := 4096
	testPassword := "correctpassword"
	clientNonce := "rOprNGfwEbeRWgbNEkqO"

	t.Run("valid proof - authentication succeeds", func(t *testing.T) {
		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{
				"testuser": createTestHash(testPassword, testSalt, testIterations),
			},
		}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		// Step 1: Client first message.
		clientFirstMessage := simulateClientFirstMessage("testuser", clientNonce)
		serverFirstMessage, err := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		require.NoError(t, err)

		// Step 2: Client final message with correct password.
		clientFirstMessageBare := "n=testuser,r=" + clientNonce
		clientFinalMessage, err := simulateClientFinalMessage(testPassword, serverFirstMessage, clientFirstMessageBare, clientNonce)
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
	})

	t.Run("invalid proof - wrong password", func(t *testing.T) {
		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{
				"testuser": createTestHash(testPassword, testSalt, testIterations),
			},
		}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		// Step 1: Client first message.
		clientFirstMessage := simulateClientFirstMessage("testuser", clientNonce)
		serverFirstMessage, err := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		require.NoError(t, err)

		// Step 2: Client final message with WRONG password.
		clientFirstMessageBare := "n=testuser,r=" + clientNonce
		clientFinalMessage, err := simulateClientFinalMessage("wrongpassword", serverFirstMessage, clientFirstMessageBare, clientNonce)
		require.NoError(t, err)

		// Step 3: Server should reject.
		_, err = auth.HandleClientFinal(clientFinalMessage)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrAuthenticationFailed))

		// Authenticator should report failure.
		assert.False(t, auth.IsAuthenticated())
	})

	t.Run("invalid client-final-message - empty", func(t *testing.T) {
		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{
				"testuser": createTestHash(testPassword, testSalt, testIterations),
			},
		}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		clientFirstMessage := simulateClientFirstMessage("testuser", clientNonce)
		_, err := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		require.NoError(t, err)

		_, err = auth.HandleClientFinal("")
		require.Error(t, err)
	})

	t.Run("nonce mismatch - server nonce tampered", func(t *testing.T) {
		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{
				"testuser": createTestHash(testPassword, testSalt, testIterations),
			},
		}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		clientFirstMessage := simulateClientFirstMessage("testuser", clientNonce)
		_, err := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		require.NoError(t, err)

		// Send a client-final-message with a different nonce (attacker trying to replay).
		tamperedFinalMessage := "c=biws,r=differentnonce,p=fakeproof"
		_, err = auth.HandleClientFinal(tamperedFinalMessage)
		require.Error(t, err)
	})

	t.Run("called without HandleClientFirst - state error", func(t *testing.T) {
		provider := &mockHashProvider{}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		// Skip HandleClientFirst, go directly to HandleClientFinal.
		_, err := auth.HandleClientFinal("c=biws,r=nonce,p=proof")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "state")
	})
}

func TestScramAuthenticator_FullExchange(t *testing.T) {
	// This test simulates a complete SCRAM exchange as would happen
	// between a PostgreSQL client and our proxy.

	t.Run("complete successful authentication", func(t *testing.T) {
		testSalt := []byte("randomsalt123456")
		testIterations := 4096
		testPassword := "pencil"
		clientNonce := "fyko+d2lbbFgONRv9qkxdawL"

		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{
				"user": createTestHash(testPassword, testSalt, testIterations),
			},
		}
		auth := NewScramAuthenticator(provider, "testdb")

		// 1. Server sends mechanism list (AuthSASL).
		mechanisms := auth.StartAuthentication()
		assert.Contains(t, mechanisms, "SCRAM-SHA-256")

		// 2. Client sends SASLInitialResponse with client-first-message.
		clientFirstMessage := simulateClientFirstMessage("user", clientNonce)
		serverFirstMessage, err := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		require.NoError(t, err)

		// 3. Verify server-first-message format.
		parsedNonce, parsedSalt, parsedIterations, err := parseServerFirstMessage(serverFirstMessage)
		require.NoError(t, err)
		assert.True(t, len(parsedNonce) > len(clientNonce)) // Combined nonce is longer
		assert.Equal(t, clientNonce, parsedNonce[:len(clientNonce)])
		assert.Equal(t, testSalt, parsedSalt)
		assert.Equal(t, testIterations, parsedIterations)

		// 4. Client computes and sends client-final-message.
		clientFirstMessageBare := "n=user,r=" + clientNonce
		clientFinalMessage, err := simulateClientFinalMessage(testPassword, serverFirstMessage, clientFirstMessageBare, clientNonce)
		require.NoError(t, err)

		// 5. Server verifies and sends server-final-message.
		serverFinalMessage, err := auth.HandleClientFinal(clientFinalMessage)
		require.NoError(t, err)

		// 6. Verify server signature (client would verify this for mutual auth).
		assert.True(t, len(serverFinalMessage) > 2)
		assert.Equal(t, "v=", serverFinalMessage[:2])

		// 7. Check final state.
		assert.True(t, auth.IsAuthenticated())
		assert.Equal(t, "user", auth.AuthenticatedUser())
	})

	t.Run("authentication with special characters in username", func(t *testing.T) {
		testSalt := []byte("salt1234567890ab")
		testIterations := 4096
		testPassword := "password123"
		clientNonce := "abcdefghijk"

		// Username with special characters that need SASL encoding.
		username := "user=with,special"

		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{
				username: createTestHash(testPassword, testSalt, testIterations),
			},
		}
		auth := NewScramAuthenticator(provider, "testdb")
		auth.StartAuthentication()

		// Client-first-message with encoded username.
		clientFirstMessage := simulateClientFirstMessage(username, clientNonce)
		serverFirstMessage, err := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		require.NoError(t, err)

		// Continue with the exchange.
		encodedUsername := encodeSaslName(username)
		clientFirstMessageBare := "n=" + encodedUsername + ",r=" + clientNonce
		clientFinalMessage, err := simulateClientFinalMessage(testPassword, serverFirstMessage, clientFirstMessageBare, clientNonce)
		require.NoError(t, err)

		_, err = auth.HandleClientFinal(clientFinalMessage)
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
		clientNonce := "clientnonce"

		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{
				"testuser": createTestHash(testPassword, testSalt, testIterations),
			},
		}
		auth := NewScramAuthenticator(provider, "testdb")

		// Complete a successful authentication.
		auth.StartAuthentication()
		clientFirstMessage := simulateClientFirstMessage("testuser", clientNonce)
		serverFirstMessage, _ := auth.HandleClientFirst(context.Background(), clientFirstMessage)
		clientFirstMessageBare := "n=testuser,r=" + clientNonce
		clientFinalMessage, _ := simulateClientFinalMessage(testPassword, serverFirstMessage, clientFirstMessageBare, clientNonce)
		_, _ = auth.HandleClientFinal(clientFinalMessage)

		assert.True(t, auth.IsAuthenticated())

		// Reset the authenticator.
		auth.Reset()

		// State should be cleared.
		assert.False(t, auth.IsAuthenticated())
		assert.Equal(t, "", auth.AuthenticatedUser())

		// Should be able to authenticate again.
		auth.StartAuthentication()
		newClientNonce := "newclientnonce"
		clientFirstMessage2 := simulateClientFirstMessage("testuser", newClientNonce)
		serverFirstMessage2, err := auth.HandleClientFirst(context.Background(), clientFirstMessage2)
		require.NoError(t, err)
		assert.Contains(t, serverFirstMessage2, "r="+newClientNonce)
	})
}

func TestScramAuthenticatorState(t *testing.T) {
	t.Run("state transitions are enforced", func(t *testing.T) {
		provider := &mockHashProvider{
			hashes: map[string]*ScramHash{
				"user": createTestHash("password", []byte("salt12345678"), 4096),
			},
		}
		auth := NewScramAuthenticator(provider, "testdb")

		// Cannot call HandleClientFirst before StartAuthentication.
		_, err := auth.HandleClientFirst(context.Background(), "n,,n=user,r=nonce")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "state")

		// Start authentication.
		auth.StartAuthentication()

		// Cannot call HandleClientFinal before HandleClientFirst.
		_, err = auth.HandleClientFinal("c=biws,r=nonce,p=proof")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "state")

		// Now proceed normally.
		_, err = auth.HandleClientFirst(context.Background(), "n,,n=user,r=clientnonce")
		require.NoError(t, err)

		// Cannot call HandleClientFirst again.
		_, err = auth.HandleClientFirst(context.Background(), "n,,n=user,r=anothernonce")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "state")
	})
}

// Note: encodeSaslName is defined in scram.go and used in tests via package access.
