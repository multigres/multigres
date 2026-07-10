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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRestore_ExecPaths drives Restore against a stubbed pgbackrest, covering
// the success and failure paths both with and without an explicit backup ID.
func TestRestore_ExecPaths(t *testing.T) {
	tests := []struct {
		name        string
		exitCode    int
		backupID    string
		wantErr     bool
		errContains string
	}{
		{name: "success with backup id", exitCode: 0, backupID: "20250104-100000F"},
		{name: "success without backup id", exitCode: 0, backupID: ""},
		{name: "failure on nonzero exit", exitCode: 1, backupID: "20250104-100000F", wantErr: true, errContains: "pgbackrest restore failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubPgbackrest(t, fmt.Sprintf("#!/bin/bash\nexit %d\n", tt.exitCode))
			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
			e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

			err := e.Restore(context.Background(), tt.backupID, poolerDir)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRestore_ErrorsWhenConfigPathMissing(t *testing.T) {
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
	err := e.Restore(context.Background(), "20250104-100000F", poolerDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgbackrest config not found")
}
