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

package executor

import (
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
)

func TestWrapQueryError_Nil(t *testing.T) {
	assert.NoError(t, wrapQueryError(nil))
}

func TestWrapQueryError_ConnectionLossBecomes08006(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"EOF", io.EOF},
		{"unexpected EOF", io.ErrUnexpectedEOF},
		{"connection reset", fmt.Errorf("read: %w", syscall.ECONNRESET)},
		{"streaming already started", mterrors.Wrapf(io.EOF, "streaming already started, cannot retry")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := wrapQueryError(tt.err)

			var diag *mterrors.PgDiagnostic
			require.ErrorAs(t, mterrors.RootCause(err), &diag)
			assert.Equal(t, "ERROR", diag.Severity)
			assert.Equal(t, mterrors.PgSSConnectionFailure, diag.Code)
			assert.Equal(t, "connection to server lost", diag.Message)
			assert.Equal(t, tt.err.Error(), diag.Detail)
			assert.True(t, mterrors.IsConnectionError(err))

			// The diagnostic must survive the trip to the gateway.
			var got *mterrors.PgDiagnostic
			require.ErrorAs(t, mterrors.FromGRPC(mterrors.ToGRPC(err)), &got)
			assert.Equal(t, mterrors.PgSSConnectionFailure, got.Code)
		})
	}
}

func TestWrapQueryError_KeepsExistingDiagnostic(t *testing.T) {
	tests := []struct {
		name string
		diag *mterrors.PgDiagnostic
	}{
		{"unique violation", mterrors.NewPgError("ERROR", "23505", "duplicate key", "")},
		{"admin shutdown", mterrors.NewPgError("FATAL", "57P01", "terminating connection due to administrator command", "")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := wrapQueryError(tt.diag)

			var diag *mterrors.PgDiagnostic
			require.ErrorAs(t, mterrors.RootCause(err), &diag)
			assert.Same(t, tt.diag, diag)
		})
	}
}

func TestWrapQueryError_OtherErrorsHaveNoDiagnostic(t *testing.T) {
	err := wrapQueryError(errors.New("boom"))

	var diag *mterrors.PgDiagnostic
	assert.False(t, errors.As(err, &diag))
	assert.ErrorContains(t, err, "query execution failed: boom")
}
