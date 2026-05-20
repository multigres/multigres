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

package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/scram"
)

// TestClassifyAuthError covers the closed mapping from SCRAM sentinels to
// the auth.attempts outcome label set. Wrapping the sentinel with %w must
// preserve the classification — sendAuthError-style helpers rewrap the
// underlying cause and the recorder must still see the specific outcome.
func TestClassifyAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil maps to success", nil, AuthOutcomeSuccess},
		{"bad password", scram.ErrAuthenticationFailed, AuthOutcomeBadPassword},
		{"user not found", scram.ErrUserNotFound, AuthOutcomeUserNotFound},
		{"login disabled", scram.ErrLoginDisabled, AuthOutcomeLoginDisabled},
		{"password expired", scram.ErrPasswordExpired, AuthOutcomePasswordExpired},
		{"sasl protocol violation", scram.ErrSASLProtocol, AuthOutcomeProtocolError},
		{"channel binding negotiation", scram.ErrChannelBindingNegotiation, AuthOutcomeProtocolError},
		{"channel binding check", scram.ErrChannelBindingCheck, AuthOutcomeProtocolError},
		{"authzid not supported", scram.ErrAuthzidNotSupported, AuthOutcomeProtocolError},
		{"wrapped bad password", fmt.Errorf("scram failed: %w", scram.ErrAuthenticationFailed), AuthOutcomeBadPassword},
		{"unknown is lookup_error", errors.New("transport closed"), AuthOutcomeLookupError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyAuthError(tc.err))
		})
	}
}

// TestIsClientAbortError separates network teardown from crypto/handshake
// failures so the tls.handshake.duration outcome label can suppress noise
// from clients that hang up mid-handshake.
func TestIsClientAbortError(t *testing.T) {
	require.False(t, isClientAbortError(nil))
	require.False(t, isClientAbortError(errors.New("opaque crypto failure")))
	require.True(t, isClientAbortError(io.EOF))
	require.True(t, isClientAbortError(io.ErrUnexpectedEOF))
	require.True(t, isClientAbortError(fmt.Errorf("wrapped: %w", net.ErrClosed)))
}
