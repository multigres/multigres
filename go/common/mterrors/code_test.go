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
	diag := MTD03.New()

	require.Equal(t, byte('E'), diag.MessageType)
	require.Equal(t, "ERROR", diag.Severity)
	require.Equal(t, "MTD03", diag.Code)
	require.Equal(t, "internal error", diag.Message)
	require.Equal(t, "Internal proxy error.", diag.Detail)
}

func TestMTError_New_WithArgs(t *testing.T) {
	diag := MTD02.New("PRIMARY", "REPLICA")

	require.Equal(t, byte('E'), diag.MessageType)
	require.Equal(t, "ERROR", diag.Severity)
	require.Equal(t, "MTD02", diag.Code)
	require.Equal(t, "pooler type mismatch: topology says PRIMARY but PostgreSQL is REPLICA", diag.Message)
	require.Equal(t, "The pooler type in the topology does not match the actual PostgreSQL role. This indicates the pooler is in an inconsistent state and requires intervention.", diag.Detail)
}

func TestMTError_New_FatalSeverity(t *testing.T) {
	diag := MTE01.New()

	require.Equal(t, "FATAL", diag.Severity)
	require.Equal(t, "MTE01", diag.Code)
	require.Equal(t, "connection startup failed", diag.Message)
	require.Equal(t, "Invalid startup message.", diag.Detail)
}

func TestMTError_NewWithDetail(t *testing.T) {
	diag := MTD04.NewWithDetail("bad syntax near ';'")

	require.Equal(t, byte('E'), diag.MessageType)
	require.Equal(t, "ERROR", diag.Severity)
	require.Equal(t, "MTD04", diag.Code)
	require.Equal(t, "parse failed", diag.Message)
	require.Equal(t, "bad syntax near ';'", diag.Detail)
}

func TestMTError_NewWithDetail_FatalSeverity(t *testing.T) {
	diag := MTE01.NewWithDetail("invalid protocol version")

	require.Equal(t, "FATAL", diag.Severity)
	require.Equal(t, "MTE01", diag.Code)
	require.Equal(t, "connection startup failed", diag.Message)
	require.Equal(t, "invalid protocol version", diag.Detail)
}

func TestMTError_PgDiagnosticExtraction(t *testing.T) {
	// Verify that wrapping an MT error preserves PgDiagnostic via RootCause.
	mtErr := MTD03.NewWithDetail("something went wrong")
	wrapped := Wrapf(mtErr, "handling request")

	rootErr := RootCause(wrapped)
	var diag *PgDiagnostic
	require.True(t, errors.As(rootErr, &diag))
	require.Equal(t, "MTD03", diag.Code)
	require.Equal(t, "internal error", diag.Message)
	require.Equal(t, "something went wrong", diag.Detail)
}

func TestIsErrorCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		codes    []string
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			codes:    []string{"MTD03"},
			expected: false,
		},
		{
			name:     "matching MT error",
			err:      MTD03.New(),
			codes:    []string{"MTD03"},
			expected: true,
		},
		{
			name:     "non-matching code",
			err:      MTD03.New(),
			codes:    []string{"MTD01"},
			expected: false,
		},
		{
			name:     "parameterized MT error",
			err:      MTD01.New("bad query"),
			codes:    []string{"MTD01"},
			expected: true,
		},
		{
			name:     "wrapped PgDiagnostic",
			err:      Wrapf(MTD03.NewWithDetail("oops"), "context"),
			codes:    []string{"MTD03"},
			expected: true,
		},
		{
			name:     "generic error",
			err:      errors.New("some error"),
			codes:    []string{"MTD03"},
			expected: false,
		},
		{
			name:     "NewPgError matches code",
			err:      NewPgError("ERROR", PgSSInFailedTransaction, "aborted", ""),
			codes:    []string{PgSSInFailedTransaction},
			expected: true,
		},
		{
			name:     "matches second of multiple codes",
			err:      MTD03.New(),
			codes:    []string{"MTD01", "MTD03"},
			expected: true,
		},
		{
			name:     "no match among multiple codes",
			err:      MTD03.New(),
			codes:    []string{"MTD01", "MTD02"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, IsErrorCode(tt.err, tt.codes...))
		})
	}
}

func TestMTError_New_NoArgsWithFormatString(t *testing.T) {
	// When called without args on a format-string error, the format string
	// should be used literally (no Sprintf).
	diag := MTD01.New()

	require.Equal(t, "MTD01", diag.Code)
	require.Equal(t, "[BUG] %s", diag.Message)
	require.Equal(t, "This error should not happen and is a bug. Please file an issue on GitHub: https://github.com/multigres/multigres/issues/new/choose.", diag.Detail)
}

func TestNewPgError(t *testing.T) {
	diag := NewPgError("ERROR", PgSSInFailedTransaction,
		"current transaction is aborted, commands ignored until end of transaction block", "")

	require.Equal(t, byte('E'), diag.MessageType)
	require.Equal(t, "ERROR", diag.Severity)
	require.Equal(t, "25P02", diag.Code)
	require.Equal(t, "current transaction is aborted, commands ignored until end of transaction block", diag.Message)
	require.Equal(t, "", diag.Detail)
}

func TestNewPgError_WithDetail(t *testing.T) {
	diag := NewPgError("ERROR", PgSSInternalError, "something failed", "underlying cause")

	require.Equal(t, "XX000", diag.Code)
	require.Equal(t, "something failed", diag.Message)
	require.Equal(t, "underlying cause", diag.Detail)
}
