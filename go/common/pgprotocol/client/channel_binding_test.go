// Copyright 2026 Supabase, Inc.
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
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
)

func TestParseChannelBinding(t *testing.T) {
	cases := map[string]struct {
		want    ChannelBindingMode
		wantErr bool
	}{
		"":         {ChannelBindingPrefer, false}, // libpq default
		"prefer":   {ChannelBindingPrefer, false},
		"disable":  {ChannelBindingDisable, false},
		"require":  {ChannelBindingRequire, false},
		"PREFER":   {ChannelBindingPrefer, false}, // case-insensitive
		"  prefer": {ChannelBindingPrefer, false}, // trimmed
		"bogus":    {"", true},
	}
	for in, want := range cases {
		got, err := ParseChannelBinding(in)
		if want.wantErr {
			assert.Error(t, err, "input %q", in)
			continue
		}
		require.NoError(t, err, "input %q", in)
		assert.Equal(t, want.want, got, "input %q", in)
	}
}

func TestDecideChannelBinding(t *testing.T) {
	const (
		overTLS   = true
		plaintext = false
	)
	plusAndPlain := []string{scram.ScramSHA256PlusMechanism, scram.ScramSHA256Mechanism}
	plainOnly := []string{scram.ScramSHA256Mechanism}
	plusOnly := []string{scram.ScramSHA256PlusMechanism}

	tests := []struct {
		name       string
		mode       ChannelBindingMode
		isTLS      bool
		mechanisms []string
		wantPlus   bool
		wantErr    string
	}{
		// prefer: opportunistic upgrade over TLS, graceful fallback otherwise.
		{"prefer/tls/offered → PLUS", ChannelBindingPrefer, overTLS, plusAndPlain, true, ""},
		{"prefer/tls/not-offered → plain", ChannelBindingPrefer, overTLS, plainOnly, false, ""},
		{"prefer/no-tls/offered → plain (cannot bind without TLS)", ChannelBindingPrefer, plaintext, plusAndPlain, false, ""},
		{"prefer/no-tls/plain-only → plain", ChannelBindingPrefer, plaintext, plainOnly, false, ""},
		{"prefer/tls/plus-only-no-plain → PLUS", ChannelBindingPrefer, overTLS, plusOnly, true, ""},
		{"prefer/no-usable-mechanism → err", ChannelBindingPrefer, plaintext, plusOnly, false, "does not support SCRAM-SHA-256"},
		// empty mode is treated as prefer (libpq default).
		{"empty(=prefer)/tls/offered → PLUS", ChannelBindingMode(""), overTLS, plusAndPlain, true, ""},

		// require: must actually bind, or fail.
		{"require/tls/offered → PLUS", ChannelBindingRequire, overTLS, plusAndPlain, true, ""},
		{"require/no-tls → err", ChannelBindingRequire, plaintext, plusAndPlain, false, "not using TLS"},
		{"require/tls/not-offered → err", ChannelBindingRequire, overTLS, plainOnly, false, "did not offer SCRAM-SHA-256-PLUS"},

		// disable: never bind, even when offered over TLS.
		{"disable/tls/offered → plain", ChannelBindingDisable, overTLS, plusAndPlain, false, ""},
		{"disable/plain-only → plain", ChannelBindingDisable, overTLS, plainOnly, false, ""},
		{"disable/no-plain → err", ChannelBindingDisable, overTLS, plusOnly, false, "does not support SCRAM-SHA-256"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			usePlus, err := decideChannelBinding(tt.mode, tt.isTLS, tt.mechanisms)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantPlus, usePlus)
		})
	}
}

// TestScramClient_MechanismNameFollowsChannelBinding verifies the announced
// SASL mechanism name tracks the channel-binding state: the SASLInitialResponse
// announces SCRAM-SHA-256-PLUS with a p=tls-server-end-point gs2 header when
// channel binding is enabled, and plain SCRAM-SHA-256 with an n,, header otherwise.
func TestScramClient_MechanismNameFollowsChannelBinding(t *testing.T) {
	tests := []struct {
		name          string
		enableBinding bool
		wantMechanism string
		wantGS2Prefix string
	}{
		{"binding enabled", true, scram.ScramSHA256PlusMechanism, "p=tls-server-end-point,,"},
		{"binding disabled", false, scram.ScramSHA256Mechanism, "n,,"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientEnd, serverEnd := net.Pipe()
			t.Cleanup(func() { _ = clientEnd.Close(); _ = serverEnd.Close() })

			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			c := &Conn{ctx: ctx, cancel: cancel}
			c.resetConn(clientEnd)

			var cbHash []byte
			if tt.enableBinding {
				cbHash = make([]byte, 32) // 32-byte tls-server-end-point hash
			}
			s := newScramClient(c, "alice", "secret", cbHash)

			// net.Pipe is synchronous: send from a goroutine while we read.
			errCh := make(chan error, 1)
			go func() { errCh <- s.sendClientFirst() }()

			mechanism, clientFirst := readSASLInitialResponse(t, serverEnd)
			require.NoError(t, <-errCh)

			assert.Equal(t, tt.wantMechanism, mechanism)
			assert.True(t, len(clientFirst) >= len(tt.wantGS2Prefix) && clientFirst[:len(tt.wantGS2Prefix)] == tt.wantGS2Prefix,
				"client-first %q should start with gs2 header %q", clientFirst, tt.wantGS2Prefix)
		})
	}
}

// readSASLInitialResponse reads one PasswordMessage frame carrying a SASLInitialResponse
// and returns the mechanism name and the client-first message body.
func readSASLInitialResponse(t *testing.T, r io.Reader) (mechanism, clientFirst string) {
	t.Helper()
	header := make([]byte, 5) // 1-byte type + int32 length
	_, err := io.ReadFull(r, header)
	require.NoError(t, err)
	require.Equal(t, byte(protocol.MsgPasswordMsg), header[0])

	bodyLen := int(binary.BigEndian.Uint32(header[1:5])) - 4
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(r, body)
	require.NoError(t, err)

	// body = mechanism (null-terminated) | int32 client-first-len | client-first
	nul := -1
	for i, b := range body {
		if b == 0 {
			nul = i
			break
		}
	}
	require.GreaterOrEqual(t, nul, 0, "mechanism must be null-terminated")
	mechanism = string(body[:nul])

	rest := body[nul+1:]
	require.GreaterOrEqual(t, len(rest), 4)
	cfLen := int(binary.BigEndian.Uint32(rest[:4]))
	require.Equal(t, cfLen, len(rest)-4)
	clientFirst = string(rest[4:])
	return mechanism, clientFirst
}
