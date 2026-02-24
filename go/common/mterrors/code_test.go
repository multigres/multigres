// Copyright 2026 Supabase, Inc.
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

package mterrors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMTError_New_ProducesPgDiagnostic(t *testing.T) {
	diag := MT10001.New()

	require.Equal(t, byte('E'), diag.MessageType)
	require.Equal(t, "ERROR", diag.Severity)
	require.Equal(t, "MT10001", diag.Code)
	require.Equal(t, "current transaction is aborted, commands ignored until end of transaction block", diag.Message)
}

func TestMTError_New_WithArgs(t *testing.T) {
	diag := MT13004.New("bad syntax near ';'")

	require.Equal(t, byte('E'), diag.MessageType)
	require.Equal(t, "ERROR", diag.Severity)
	require.Equal(t, "MT13004", diag.Code)
	require.Equal(t, "parse failed: bad syntax near ';'", diag.Message)
}

func TestMTError_New_FatalSeverity(t *testing.T) {
	diag := MT14001.New("invalid protocol version")

	require.Equal(t, "FATAL", diag.Severity)
	require.Equal(t, "MT14001", diag.Code)
	require.Equal(t, "connection startup failed: invalid protocol version", diag.Message)
}

func TestMTError_New_MultipleArgs(t *testing.T) {
	diag := MT13002.New("PRIMARY", "REPLICA")

	require.Equal(t, "MT13002", diag.Code)
	require.Equal(t, "pooler type mismatch: topology says PRIMARY but PostgreSQL is REPLICA", diag.Message)
}

func TestMTError_PgDiagnosticExtraction(t *testing.T) {
	// Verify that wrapping an MT error preserves PgDiagnostic via RootCause.
	mtErr := MT13003.New("something went wrong")
	wrapped := Wrapf(mtErr, "handling request")

	rootErr := RootCause(wrapped)
	var diag *PgDiagnostic
	require.True(t, errors.As(rootErr, &diag))
	require.Equal(t, "MT13003", diag.Code)
	require.Equal(t, "something went wrong", diag.Message)
}

func TestIsError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		code     string
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			code:     "MT10001",
			expected: false,
		},
		{
			name:     "matching MT error",
			err:      MT10001.New(),
			code:     "MT10001",
			expected: true,
		},
		{
			name:     "non-matching code",
			err:      MT10001.New(),
			code:     "MT13001",
			expected: false,
		},
		{
			name:     "parameterized MT error",
			err:      MT13004.New("bad query"),
			code:     "MT13004",
			expected: true,
		},
		{
			name:     "wrapped PgDiagnostic",
			err:      Wrapf(MT13003.New("oops"), "context"),
			code:     "MT13003",
			expected: true,
		},
		{
			name:     "generic error",
			err:      errors.New("some error"),
			code:     "MT10001",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, IsError(tt.err, tt.code))
		})
	}
}

func TestMTError_New_NoArgsWithFormatString(t *testing.T) {
	// When called without args on a format-string error, the format string
	// should be used literally (no Sprintf).
	diag := MT13001.New()

	require.Equal(t, "MT13001", diag.Code)
	require.Equal(t, "[BUG] %s", diag.Message)
}
