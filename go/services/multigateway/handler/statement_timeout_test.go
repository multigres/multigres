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

func durationPtr(d time.Duration) *time.Duration { return &d }

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
			directive: durationPtr(500 * time.Millisecond),
			effective: 10 * time.Second,
			want:      500 * time.Millisecond,
		},
		{
			name:      "directive=0 disables timeout",
			directive: durationPtr(0),
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
			directive: durationPtr(60 * time.Second),
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
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{
			name:  "integer milliseconds",
			value: "5000",
			want:  5 * time.Second,
		},
		{
			name:  "Go duration 30s",
			value: "30s",
			want:  30 * time.Second,
		},
		{
			name:  "Go duration 200ms",
			value: "200ms",
			want:  200 * time.Millisecond,
		},
		{
			name:  "Go duration 1m",
			value: "1m",
			want:  time.Minute,
		},
		{
			name:  "zero",
			value: "0",
			want:  0,
		},
		{
			name:    "negative integer",
			value:   "-1",
			wantErr: true,
		},
		{
			name:    "negative Go duration",
			value:   "-5s",
			wantErr: true,
		},
		{
			name:    "invalid string",
			value:   "not-a-number",
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
}

func TestConnectionState_StatementTimeout(t *testing.T) {
	t.Run("returns default when not set", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		require.Equal(t, 30*time.Second, s.GetStatementTimeout())
	})

	t.Run("set overrides default", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(5 * time.Second)
		require.Equal(t, 5*time.Second, s.GetStatementTimeout())
	})

	t.Run("set zero disables timeout", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(0)
		require.Equal(t, time.Duration(0), s.GetStatementTimeout())
	})

	t.Run("reset reverts to default", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitStatementTimeout(30 * time.Second)
		s.SetStatementTimeout(5 * time.Second)
		s.ResetStatementTimeout()
		require.Equal(t, 30*time.Second, s.GetStatementTimeout())
	})

	t.Run("show formats using PG GUC_UNIT_MS convention", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
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
}
