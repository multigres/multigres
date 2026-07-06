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

package handler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsGatewayManagedVariable(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"statement_timeout", true},
		{"idle_session_timeout", true},
		// Case-insensitive: PostgreSQL lowercases unquoted identifiers, but a
		// quoted identifier preserves case and must still be recognized.
		{"STATEMENT_TIMEOUT", true},
		{"Statement_Timeout", true},
		{"IDLE_SESSION_TIMEOUT", true},
		{"application_name", false},
		{"work_mem", false},
		{"search_path", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsGatewayManagedVariable(tc.name))
		})
	}
}

// TestApplyGatewayManagedVariable_RoutesToGatewayState confirms gateway-managed
// variables are applied to gateway-local state (visible via SHOW) and are kept
// out of SessionSettings, while non-managed variables are left for the caller.
func TestResetGatewayManagedVariable(t *testing.T) {
	t.Run("session reset restores default", func(t *testing.T) {
		s := &MultigatewayConnectionState{}
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(5 * time.Second)

		assert.True(t, s.ResetGatewayManagedVariable("statement_timeout", false))
		assert.Equal(t, "30s", s.ShowStatementTimeout())
	})

	t.Run("local reset masks session until transaction end", func(t *testing.T) {
		s := &MultigatewayConnectionState{}
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(5 * time.Second)

		assert.True(t, s.ResetGatewayManagedVariable("statement_timeout", true))
		assert.Equal(t, "30s", s.ShowStatementTimeout())

		s.ResetAllLocalGUCs()
		assert.Equal(t, "5s", s.ShowStatementTimeout())
	})

	t.Run("non gateway managed variable", func(t *testing.T) {
		s := &MultigatewayConnectionState{}
		assert.False(t, s.ResetGatewayManagedVariable("work_mem", false))
	})
}

func TestApplyGatewayManagedVariable_RoutesToGatewayState(t *testing.T) {
	t.Run("statement_timeout session", func(t *testing.T) {
		s := &MultigatewayConnectionState{}
		handled, err := s.ApplyGatewayManagedVariable("statement_timeout", "5s", false)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, "5s", s.ShowStatementTimeout())
		_, exists := s.GetSessionVariable("statement_timeout")
		assert.False(t, exists)
	})

	t.Run("case-insensitive name", func(t *testing.T) {
		s := &MultigatewayConnectionState{}
		handled, err := s.ApplyGatewayManagedVariable("Statement_Timeout", "5s", false)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, "5s", s.ShowStatementTimeout())
	})

	t.Run("idle_session_timeout session", func(t *testing.T) {
		s := &MultigatewayConnectionState{}
		handled, err := s.ApplyGatewayManagedVariable("idle_session_timeout", "5s", false)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, "5s", s.ShowIdleSessionTimeout())
		assert.Equal(t, 5*time.Second, s.GetIdleSessionTimeout())
		_, exists := s.GetSessionVariable("idle_session_timeout")
		assert.False(t, exists)
	})

	t.Run("idle_session_timeout local", func(t *testing.T) {
		s := &MultigatewayConnectionState{}
		s.InitIdleSessionTimeout(30 * time.Second)
		s.SetIdleSessionTimeout(5 * time.Second)
		handled, err := s.ApplyGatewayManagedVariable("idle_session_timeout", "250ms", true)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, 250*time.Millisecond, s.GetIdleSessionTimeout())
		s.ResetAllLocalGUCs()
		assert.Equal(t, 5*time.Second, s.GetIdleSessionTimeout(), "session value restored after local reset")
		_, exists := s.GetSessionVariable("idle_session_timeout")
		assert.False(t, exists)
	})

	t.Run("invalid statement_timeout returns handled with error", func(t *testing.T) {
		s := &MultigatewayConnectionState{}
		handled, err := s.ApplyGatewayManagedVariable("statement_timeout", "not-a-duration", false)
		require.Error(t, err)
		assert.True(t, handled, "still gateway-managed even though the value is invalid")
	})

	t.Run("invalid idle_session_timeout returns handled with error", func(t *testing.T) {
		s := &MultigatewayConnectionState{}
		handled, err := s.ApplyGatewayManagedVariable("idle_session_timeout", "not-a-duration", false)
		require.Error(t, err)
		assert.True(t, handled, "still gateway-managed even though the value is invalid")
	})

	t.Run("non-managed variable is not handled", func(t *testing.T) {
		s := &MultigatewayConnectionState{}
		handled, err := s.ApplyGatewayManagedVariable("work_mem", "256MB", false)
		require.NoError(t, err)
		assert.False(t, handled)
		// Caller is responsible for SessionSettings; ApplyGatewayManagedVariable
		// must not have touched it.
		_, exists := s.GetSessionVariable("work_mem")
		assert.False(t, exists)
	})
}

func TestIdleSessionTimeoutGatewayManagedVariableLifecycle(t *testing.T) {
	s := &MultigatewayConnectionState{}
	s.InitIdleSessionTimeout(30 * time.Second)
	assert.Equal(t, 30*time.Second, s.GetIdleSessionTimeout())

	s.SetIdleSessionTimeout(5 * time.Second)
	assert.Equal(t, 5*time.Second, s.GetIdleSessionTimeout())

	s.SetLocalIdleSessionTimeout(250 * time.Millisecond)
	assert.Equal(t, 250*time.Millisecond, s.GetIdleSessionTimeout())

	s.SetLocalIdleSessionTimeoutToDefault()
	assert.Equal(t, 30*time.Second, s.GetIdleSessionTimeout(), "LOCAL TO DEFAULT masks session value with default")

	s.ResetAllLocalGUCs()
	assert.Equal(t, 5*time.Second, s.GetIdleSessionTimeout(), "transaction end restores session value")

	s.SetLocalIdleSessionTimeout(250 * time.Millisecond)
	s.ResetIdleSessionTimeout()
	assert.Equal(t, 30*time.Second, s.GetIdleSessionTimeout(), "RESET clears session and local overrides")
}
