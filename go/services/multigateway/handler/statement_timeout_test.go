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
	d := func(v time.Duration) *time.Duration { return &v }

	tests := []struct {
		name       string
		directive  *time.Duration
		sessionVar *time.Duration
		flag       time.Duration
		want       time.Duration
	}{
		{
			name: "flag only",
			flag: 10 * time.Second,
			want: 10 * time.Second,
		},
		{
			name:       "session wins over flag",
			sessionVar: d(5 * time.Second),
			flag:       10 * time.Second,
			want:       5 * time.Second,
		},
		{
			name:       "session wins even when larger than flag",
			sessionVar: d(15 * time.Second),
			flag:       10 * time.Second,
			want:       15 * time.Second,
		},
		{
			name:       "directive wins over session and flag",
			directive:  d(500 * time.Millisecond),
			sessionVar: d(5 * time.Second),
			flag:       10 * time.Second,
			want:       500 * time.Millisecond,
		},
		{
			name:       "directive=0 disables timeout",
			directive:  d(0),
			sessionVar: d(5 * time.Second),
			flag:       10 * time.Second,
			want:       0,
		},
		{
			name:       "session=0 disables timeout",
			sessionVar: d(0),
			flag:       10 * time.Second,
			want:       0,
		},
		{
			name: "all zero means no timeout",
			flag: 0,
			want: 0,
		},
		{
			name:      "directive wins even when larger than flag",
			directive: d(60 * time.Second),
			flag:      10 * time.Second,
			want:      60 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveStatementTimeout(tt.directive, tt.sessionVar, tt.flag)
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
			name:  "negative integer",
			value: "-1",
			want:  -time.Millisecond,
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
			got, err := ParsePostgresInterval(tt.value)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestConnectionState_StatementTimeout(t *testing.T) {
	t.Run("not set returns nil", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		require.Nil(t, s.GetStatementTimeout())
	})

	t.Run("set and get", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.SetStatementTimeout(5 * time.Second)
		got := s.GetStatementTimeout()
		require.NotNil(t, got)
		require.Equal(t, 5*time.Second, *got)
	})

	t.Run("set zero means disabled", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.SetStatementTimeout(0)
		got := s.GetStatementTimeout()
		require.NotNil(t, got)
		require.Equal(t, time.Duration(0), *got)
	})

	t.Run("reset clears to nil", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.SetStatementTimeout(5 * time.Second)
		s.ResetStatementTimeout()
		require.Nil(t, s.GetStatementTimeout())
	})

	t.Run("overwrite with new value", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.SetStatementTimeout(5 * time.Second)
		s.SetStatementTimeout(10 * time.Second)
		got := s.GetStatementTimeout()
		require.NotNil(t, got)
		require.Equal(t, 10*time.Second, *got)
	})
}
