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

	"github.com/stretchr/testify/require"
)

func TestResolveStatementTimeout(t *testing.T) {
	tests := []struct {
		name      string
		directive *time.Duration
		effective time.Duration
		want      time.Duration
	}{
		{
			name:      "no directive uses effective",
			directive: nil,
			effective: 10 * time.Second,
			want:      10 * time.Second,
		},
		{
			name:      "directive wins over effective",
			directive: new(500 * time.Millisecond),
			effective: 10 * time.Second,
			want:      500 * time.Millisecond,
		},
		{
			name:      "directive=0 disables timeout",
			directive: new(time.Duration(0)),
			effective: 10 * time.Second,
			want:      0,
		},
		{
			name:      "nil directive with zero effective means no timeout",
			directive: nil,
			want:      0,
		},
		{
			name:      "directive wins even when larger",
			directive: new(60 * time.Second),
			effective: 10 * time.Second,
			want:      60 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveStatementTimeout(tt.directive, tt.effective)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParsePostgresInterval(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		want        time.Duration
		wantErr     bool
		errContains string // when set, the error message must contain this substring
	}{
		{
			name:  "integer milliseconds",
			value: "5000",
			want:  5 * time.Second,
		},
		{
			name:  "PostgreSQL seconds unit",
			value: "30s",
			want:  30 * time.Second,
		},
		{
			name:  "PostgreSQL milliseconds unit",
			value: "200ms",
			want:  200 * time.Millisecond,
		},
		{
			name:  "PostgreSQL min unit",
			value: "1min",
			want:  time.Minute,
		},
		{
			name:  "PostgreSQL min unit with space",
			value: "1 min",
			want:  time.Minute,
		},
		{
			name:  "PostgreSQL hour unit",
			value: "1h",
			want:  time.Hour,
		},
		{
			name:  "PostgreSQL day unit",
			value: "1d",
			want:  24 * time.Hour,
		},
		{
			name:  "PostgreSQL decimal unit with space",
			value: "1.5 s",
			want:  1500 * time.Millisecond,
		},
		{
			name:  "zero",
			value: "0",
			want:  0,
		},
		{
			name:  "max boundary",
			value: "2147483647",
			want:  2147483647 * time.Millisecond,
		},
		{
			name:        "negative integer",
			value:       "-1",
			wantErr:     true,
			errContains: `-1 ms is outside the valid range for parameter "statement_timeout" (0 .. 2147483647)`,
		},
		{
			name:        "negative seconds unit",
			value:       "-5s",
			wantErr:     true,
			errContains: `-5000 ms is outside the valid range for parameter "statement_timeout" (0 .. 2147483647)`,
		},
		{
			// Rounds to -1 ms, not a misleading 0 ms (truncation would report 0).
			name:        "negative sub-millisecond duration",
			value:       "-600us",
			wantErr:     true,
			errContains: `-1 ms is outside the valid range for parameter "statement_timeout" (0 .. 2147483647)`,
		},
		{
			// Sub-millisecond magnitude rounds to 0 ms (no timeout), matching PG.
			name:  "sub-millisecond rounds to zero",
			value: "200us",
			want:  0,
		},
		{
			name:        "over max integer",
			value:       "2147483648",
			wantErr:     true,
			errContains: `2147483648 ms is outside the valid range for parameter "statement_timeout" (0 .. 2147483647)`,
		},
		{
			name:    "invalid string",
			value:   "not-a-number",
			wantErr: true,
		},
		{
			name:    "Go-only minute unit rejected",
			value:   "1m",
			wantErr: true,
		},
		{
			name:    "long seconds unit rejected",
			value:   "5 seconds",
			wantErr: true,
		},
		{
			name:    "plural minutes unit rejected",
			value:   "30 mins",
			wantErr: true,
		},
		{
			name:    "long hour unit rejected",
			value:   "1 hour",
			wantErr: true,
		},
		{
			name:    "long day unit rejected",
			value:   "2 days",
			wantErr: true,
		},
		{
			name:    "empty string",
			value:   "",
			wantErr: true,
		},
		{
			name:  "whitespace padded",
			value: "  100  ",
			want:  100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePostgresInterval("statement_timeout", tt.value)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestGatewayManagedVariable(t *testing.T) {
	t.Run("default value returned when not set", func(t *testing.T) {
		v := NewGatewayManagedVariable(30 * time.Second)
		require.Equal(t, 30*time.Second, v.GetEffective())
		require.False(t, v.IsSet())
	})

	t.Run("set overrides default", func(t *testing.T) {
		v := NewGatewayManagedVariable(30 * time.Second)
		v.Set(5 * time.Second)
		require.Equal(t, 5*time.Second, v.GetEffective())
		require.True(t, v.IsSet())
	})

	t.Run("set zero overrides non-zero default", func(t *testing.T) {
		v := NewGatewayManagedVariable(30 * time.Second)
		v.Set(0)
		require.Equal(t, time.Duration(0), v.GetEffective())
		require.True(t, v.IsSet())
	})

	t.Run("reset reverts to default", func(t *testing.T) {
		v := NewGatewayManagedVariable(30 * time.Second)
		v.Set(5 * time.Second)
		v.Reset()
		require.Equal(t, 30*time.Second, v.GetEffective())
		require.False(t, v.IsSet())
	})

	t.Run("overwrite with new value", func(t *testing.T) {
		v := NewGatewayManagedVariable(30 * time.Second)
		v.Set(5 * time.Second)
		v.Set(10 * time.Second)
		require.Equal(t, 10*time.Second, v.GetEffective())
	})

	t.Run("set local overrides session and default", func(t *testing.T) {
		v := NewGatewayManagedVariable(30 * time.Second)
		v.Set(5 * time.Second)
		v.SetLocal(100 * time.Millisecond)
		require.Equal(t, 100*time.Millisecond, v.GetEffective())
		require.True(t, v.IsLocalSet())
		require.True(t, v.IsSet())
	})

	t.Run("set local without session set still wins over default", func(t *testing.T) {
		v := NewGatewayManagedVariable(30 * time.Second)
		v.SetLocal(100 * time.Millisecond)
		require.Equal(t, 100*time.Millisecond, v.GetEffective())
		require.True(t, v.IsLocalSet())
		require.False(t, v.IsSet())
	})

	t.Run("reset local reverts to session", func(t *testing.T) {
		v := NewGatewayManagedVariable(30 * time.Second)
		v.Set(5 * time.Second)
		v.SetLocal(100 * time.Millisecond)
		v.ResetLocal()
		require.Equal(t, 5*time.Second, v.GetEffective())
		require.False(t, v.IsLocalSet())
		require.True(t, v.IsSet())
	})

	t.Run("reset local with no session reverts to default", func(t *testing.T) {
		v := NewGatewayManagedVariable(30 * time.Second)
		v.SetLocal(100 * time.Millisecond)
		v.ResetLocal()
		require.Equal(t, 30*time.Second, v.GetEffective())
		require.False(t, v.IsLocalSet())
	})

	t.Run("session set clears active local override", func(t *testing.T) {
		// PostgreSQL semantics: SET inside a transaction with a prior SET LOCAL
		// supersedes the LOCAL — effective value immediately becomes the new
		// session value (verified directly against PG 17).
		v := NewGatewayManagedVariable(30 * time.Second)
		v.SetLocal(100 * time.Millisecond)
		v.Set(5 * time.Second)
		require.Equal(t, 5*time.Second, v.GetEffective())
		require.False(t, v.IsLocalSet())
		require.True(t, v.IsSet())
	})

	t.Run("session reset clears active local override", func(t *testing.T) {
		// PostgreSQL semantics: RESET inside a transaction with a prior SET LOCAL
		// also supersedes the LOCAL — effective value becomes the default
		// (verified directly against PG 17).
		v := NewGatewayManagedVariable(30 * time.Second)
		v.Set(5 * time.Second)
		v.SetLocal(100 * time.Millisecond)
		v.Reset()
		require.Equal(t, 30*time.Second, v.GetEffective())
		require.False(t, v.IsLocalSet())
		require.False(t, v.IsSet())
	})

	t.Run("local zero is honored as override", func(t *testing.T) {
		v := NewGatewayManagedVariable(30 * time.Second)
		v.SetLocal(0)
		require.Equal(t, time.Duration(0), v.GetEffective())
		require.True(t, v.IsLocalSet())
	})

	t.Run("SetLocalToDefault masks session without destroying it", func(t *testing.T) {
		// PG semantics: SET LOCAL var TO DEFAULT installs a transaction-scoped
		// override equal to the default, masking the session value during the
		// transaction. The session value must be preserved for restoration on
		// COMMIT/ROLLBACK (when ResetLocal fires).
		v := NewGatewayManagedVariable(30 * time.Second)
		v.Set(5 * time.Second)
		v.SetLocalToDefault()
		require.Equal(t, 30*time.Second, v.GetEffective(),
			"during txn: SHOW returns default value, masking session")
		require.True(t, v.IsLocalSet())
		require.True(t, v.IsSet(), "session value must be preserved, not destroyed")

		// Simulate transaction-end ResetLocal: session value should be restored.
		v.ResetLocal()
		require.Equal(t, 5*time.Second, v.GetEffective(),
			"after txn end: session value is restored")
	})
}

func TestConnectionState_StatementTimeout(t *testing.T) {
	t.Run("returns default when not set", func(t *testing.T) {
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		require.Equal(t, 30*time.Second, s.GetStatementTimeout())
	})

	t.Run("set overrides default", func(t *testing.T) {
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(5 * time.Second)
		require.Equal(t, 5*time.Second, s.GetStatementTimeout())
	})

	t.Run("set zero disables timeout", func(t *testing.T) {
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(0)
		require.Equal(t, time.Duration(0), s.GetStatementTimeout())
	})

	t.Run("reset reverts to default", func(t *testing.T) {
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(5 * time.Second)
		s.ResetStatementTimeout()
		require.Equal(t, 30*time.Second, s.GetStatementTimeout())
	})

	t.Run("show formats using PG GUC_UNIT_MS convention", func(t *testing.T) {
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		require.Equal(t, "30s", s.ShowStatementTimeout())

		s.SetStatementTimeout(5 * time.Second)
		require.Equal(t, "5s", s.ShowStatementTimeout())

		s.SetStatementTimeout(0)
		require.Equal(t, "0", s.ShowStatementTimeout())

		s.SetStatementTimeout(500 * time.Millisecond)
		require.Equal(t, "500ms", s.ShowStatementTimeout())

		s.SetStatementTimeout(2 * time.Minute)
		require.Equal(t, "2min", s.ShowStatementTimeout())

		s.SetStatementTimeout(time.Hour)
		require.Equal(t, "1h", s.ShowStatementTimeout())

		// Non-even values stay in ms
		s.SetStatementTimeout(1500 * time.Millisecond)
		require.Equal(t, "1500ms", s.ShowStatementTimeout())
	})

	t.Run("set local overrides session and shows local value", func(t *testing.T) {
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(5 * time.Second)
		s.SetLocalStatementTimeout(40 * time.Millisecond)
		require.Equal(t, 40*time.Millisecond, s.GetStatementTimeout())
		require.Equal(t, "40ms", s.ShowStatementTimeout())
	})

	t.Run("ResetAllLocalGUCs clears local but keeps session", func(t *testing.T) {
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(5 * time.Second)
		s.SetLocalStatementTimeout(40 * time.Millisecond)

		s.ResetAllLocalGUCs()
		require.Equal(t, 5*time.Second, s.GetStatementTimeout())
	})

	t.Run("ResetAllLocalGUCs reverts to default when no session set", func(t *testing.T) {
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetLocalStatementTimeout(40 * time.Millisecond)

		s.ResetAllLocalGUCs()
		require.Equal(t, 30*time.Second, s.GetStatementTimeout())
	})

	t.Run("session set supersedes active local override", func(t *testing.T) {
		// Mirrors PG: SET inside a transaction with a prior SET LOCAL
		// supersedes the LOCAL — effective value is the new session value.
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetLocalStatementTimeout(40 * time.Millisecond)
		s.SetStatementTimeout(5 * time.Second)
		require.Equal(t, 5*time.Second, s.GetStatementTimeout())
	})

	t.Run("session reset supersedes active local override", func(t *testing.T) {
		// Mirrors PG: RESET inside a transaction with a prior SET LOCAL
		// supersedes the LOCAL — effective value is the default.
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(5 * time.Second)
		s.SetLocalStatementTimeout(40 * time.Millisecond)
		s.ResetStatementTimeout()
		require.Equal(t, 30*time.Second, s.GetStatementTimeout())
	})

	t.Run("SetLocalStatementTimeoutToDefault masks session", func(t *testing.T) {
		// Repro the reviewer-flagged bug: SET LOCAL var TO DEFAULT must NOT
		// destroy the session value. ShowStatementTimeout reflects the default
		// during the transaction; after ResetAllLocalGUCs (transaction end),
		// the original session value is restored.
		s := NewMultigatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(5 * time.Second)

		s.SetLocalStatementTimeoutToDefault()
		require.Equal(t, 30*time.Second, s.GetStatementTimeout(),
			"during txn: effective value is the default")

		s.ResetAllLocalGUCs()
		require.Equal(t, 5*time.Second, s.GetStatementTimeout(),
			"after txn end: session value is restored, not lost")
	})
}
