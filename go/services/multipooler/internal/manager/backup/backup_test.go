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

package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

func TestValidateBackupType(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		want        PgBackRestType
		wantErr     bool
		errContains string
	}{
		{name: "full", input: "full", want: PgBackRestFull},
		{name: "differential", input: "differential", want: PgBackRestDiff},
		{name: "incremental", input: "incremental", want: PgBackRestIncr},
		{name: "empty is required", input: "", wantErr: true, errContains: "type is required"},
		{name: "unknown is rejected", input: "weekly", wantErr: true, errContains: "invalid backup type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateBackupType(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Empty(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
