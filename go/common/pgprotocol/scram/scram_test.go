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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseClientFirstMessage(t *testing.T) {
	t.Run("valid client-first-message with standard GS2 header", func(t *testing.T) {
		// Standard format: n,,n=<username>,r=<nonce>
		msg := "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"

		parsed, err := parseClientFirstMessage(msg)
		require.NoError(t, err)

		assert.Equal(t, "user", parsed.username)
		assert.Equal(t, "fyko+d2lbbFgONRv9qkxdawL", parsed.clientNonce)
		assert.Equal(t, "n=user,r=fyko+d2lbbFgONRv9qkxdawL", parsed.clientFirstMessageBare)
		assert.Equal(t, "n", parsed.gs2CbindFlag)
		assert.Equal(t, "", parsed.authzid)
	})

	t.Run("valid client-first-message with authzid", func(t *testing.T) {
		// Format with authorization identity: n,a=authzid,n=<username>,r=<nonce>
		msg := "n,a=admin,n=user,r=rOprNGfwEbeRWgbNEkqO"

		parsed, err := parseClientFirstMessage(msg)
		require.NoError(t, err)

		assert.Equal(t, "user", parsed.username)
		assert.Equal(t, "rOprNGfwEbeRWgbNEkqO", parsed.clientNonce)
		assert.Equal(t, "admin", parsed.authzid)
	})

	t.Run("username with escaped comma", func(t *testing.T) {
		// Comma in username must be escaped as =2C
		msg := "n,,n=user=2Cname,r=nonce123"

		parsed, err := parseClientFirstMessage(msg)
		require.NoError(t, err)

		assert.Equal(t, "user,name", parsed.username)
	})

	t.Run("username with escaped equals", func(t *testing.T) {
		// Equals in username must be escaped as =3D
		msg := "n,,n=user=3Dname,r=nonce123"

		parsed, err := parseClientFirstMessage(msg)
		require.NoError(t, err)

		assert.Equal(t, "user=name", parsed.username)
	})

	t.Run("missing GS2 header", func(t *testing.T) {
		msg := "n=user,r=nonce"
		_, err := parseClientFirstMessage(msg)
		assert.Error(t, err)
	})

	t.Run("invalid GS2 cbind flag", func(t *testing.T) {
		// Only 'n', 'y', or 'p' are valid
		msg := "x,,n=user,r=nonce"
		_, err := parseClientFirstMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "GS2")
	})

	t.Run("channel binding requested but not supported", func(t *testing.T) {
		// 'p' means channel binding is required
		msg := "p=tls-unique,,n=user,r=nonce"
		_, err := parseClientFirstMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "channel binding")
	})

	t.Run("missing username attribute - valid (fallback handled later)", func(t *testing.T) {
		// PostgreSQL allows empty username in SCRAM client-first-message.
		// The authenticator will use the startup message username as fallback.
		msg := "n,,r=nonce"
		parsed, err := parseClientFirstMessage(msg)
		require.NoError(t, err)
		assert.Equal(t, "", parsed.username)
		assert.Equal(t, "nonce", parsed.clientNonce)
	})

	t.Run("missing nonce", func(t *testing.T) {
		msg := "n,,n=user"
		_, err := parseClientFirstMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nonce")
	})

	t.Run("empty message", func(t *testing.T) {
		_, err := parseClientFirstMessage("")
		assert.Error(t, err)
	})

	t.Run("empty username - valid (fallback handled later)", func(t *testing.T) {
		// PostgreSQL allows empty username in SCRAM client-first-message.
		// The authenticator will use the startup message username as fallback.
		msg := "n,,n=,r=nonce"
		parsed, err := parseClientFirstMessage(msg)
		require.NoError(t, err)
		assert.Equal(t, "", parsed.username)
		assert.Equal(t, "nonce", parsed.clientNonce)
	})

	t.Run("empty nonce", func(t *testing.T) {
		msg := "n,,n=user,r="
		_, err := parseClientFirstMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nonce")
	})
}

func TestParseClientFinalMessage(t *testing.T) {
	t.Run("valid client-final-message", func(t *testing.T) {
		// Format: c=<channel-binding>,r=<nonce>,p=<proof>
		msg := "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="

		parsed, err := parseClientFinalMessage(msg)
		require.NoError(t, err)

		assert.Equal(t, "biws", parsed.channelBinding)
		assert.Equal(t, "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j", parsed.nonce)
		assert.NotEmpty(t, parsed.proof)
		assert.Equal(t, "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j", parsed.clientFinalMessageWithoutProof)
	})

	t.Run("missing channel binding", func(t *testing.T) {
		msg := "r=nonce,p=proof"
		_, err := parseClientFinalMessage(msg)
		assert.Error(t, err)
	})

	t.Run("missing nonce", func(t *testing.T) {
		msg := "c=biws,p=proof"
		_, err := parseClientFinalMessage(msg)
		assert.Error(t, err)
	})

	t.Run("missing proof", func(t *testing.T) {
		msg := "c=biws,r=nonce"
		_, err := parseClientFinalMessage(msg)
		assert.Error(t, err)
	})

	t.Run("invalid proof - not base64", func(t *testing.T) {
		msg := "c=biws,r=nonce,p=not-valid-base64!!!"
		_, err := parseClientFinalMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "proof")
	})

	t.Run("empty message", func(t *testing.T) {
		_, err := parseClientFinalMessage("")
		assert.Error(t, err)
	})
}

func TestGenerateServerFirstMessage(t *testing.T) {
	t.Run("generates valid server-first-message", func(t *testing.T) {
		clientNonce := "fyko+d2lbbFgONRv9qkxdawL"
		salt := []byte("salt")
		iterations := 4096

		msg, serverNonce, err := generateServerFirstMessage(clientNonce, salt, iterations)
		require.NoError(t, err)

		// Server nonce should start with client nonce
		assert.True(t, len(serverNonce) > len(clientNonce))
		assert.Equal(t, clientNonce, serverNonce[:len(clientNonce)])

		// Message should contain expected parts
		assert.Contains(t, msg, "r="+serverNonce)
		assert.Contains(t, msg, "s=")
		assert.Contains(t, msg, "i=4096")
	})

	t.Run("server adds to client nonce", func(t *testing.T) {
		clientNonce := "abc"
		salt := []byte("salt")

		_, serverNonce, err := generateServerFirstMessage(clientNonce, salt, 4096)
		require.NoError(t, err)

		// Combined nonce must be longer than client nonce
		assert.Greater(t, len(serverNonce), len(clientNonce))
	})

	t.Run("different calls produce different nonces", func(t *testing.T) {
		clientNonce := "test"
		salt := []byte("salt")

		_, nonce1, err := generateServerFirstMessage(clientNonce, salt, 4096)
		require.NoError(t, err)

		_, nonce2, err := generateServerFirstMessage(clientNonce, salt, 4096)
		require.NoError(t, err)

		// Server portion should be different
		assert.NotEqual(t, nonce1, nonce2)
	})

	t.Run("empty client nonce rejected", func(t *testing.T) {
		_, _, err := generateServerFirstMessage("", []byte("salt"), 4096)
		assert.Error(t, err)
	})

	t.Run("empty salt rejected", func(t *testing.T) {
		_, _, err := generateServerFirstMessage("nonce", []byte{}, 4096)
		assert.Error(t, err)
	})

	t.Run("zero iterations rejected", func(t *testing.T) {
		_, _, err := generateServerFirstMessage("nonce", []byte("salt"), 0)
		assert.Error(t, err)
	})
}

func TestGenerateServerFinalMessage(t *testing.T) {
	t.Run("generates valid server-final-message", func(t *testing.T) {
		serverSignature := []byte("serversignature")

		msg := generateServerFinalMessage(serverSignature)

		assert.True(t, len(msg) > 2)
		assert.Equal(t, "v=", msg[:2])
	})
}

func TestClientFirstMessageStruct(t *testing.T) {
	t.Run("struct fields", func(t *testing.T) {
		msg := clientFirstMessage{
			gs2CbindFlag:           "n",
			authzid:                "",
			username:               "user",
			clientNonce:            "nonce",
			clientFirstMessageBare: "n=user,r=nonce",
		}

		assert.Equal(t, "n", msg.gs2CbindFlag)
		assert.Equal(t, "", msg.authzid)
		assert.Equal(t, "user", msg.username)
		assert.Equal(t, "nonce", msg.clientNonce)
		assert.Equal(t, "n=user,r=nonce", msg.clientFirstMessageBare)
	})
}

func TestClientFinalMessageStruct(t *testing.T) {
	t.Run("struct fields", func(t *testing.T) {
		msg := clientFinalMessage{
			channelBinding:                 "biws",
			nonce:                          "combinednonce",
			proof:                          []byte("proof"),
			clientFinalMessageWithoutProof: "c=biws,r=combinednonce",
		}

		assert.Equal(t, "biws", msg.channelBinding)
		assert.Equal(t, "combinednonce", msg.nonce)
		assert.Equal(t, []byte("proof"), msg.proof)
		assert.Equal(t, "c=biws,r=combinednonce", msg.clientFinalMessageWithoutProof)
	})
}
