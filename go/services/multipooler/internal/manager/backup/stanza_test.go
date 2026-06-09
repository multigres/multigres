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

// TestStanzaCreate_ExecPaths drives StanzaCreate against a stubbed pgbackrest,
// covering the success (exit 0) and failure (non-zero) paths.
func TestStanzaCreate_ExecPaths(t *testing.T) {
	tests := []struct {
		name        string
		exitCode    int
		wantErr     bool
		errContains string
	}{
		{name: "success on exit 0", exitCode: 0},
		{name: "failure on nonzero exit", exitCode: 1, wantErr: true, errContains: "pgbackrest stanza-create failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubPgbackrest(t, fmt.Sprintf("#!/bin/bash\nexit %d\n", tt.exitCode))
			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
			e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

			err := e.StanzaCreate(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestStanzaCreate_ErrorsWhenConfigPathMissing(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "", "", "/tmp/backups")
	err := e.StanzaCreate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet generated")
}

// TestSetPgpassPath_PropagatesToCommand verifies SetPgpassPath exports
// PGPASSFILE to pgbackrest's environment. The stub echoes the variable and
// fails, so the error output reveals what the command saw.
func TestSetPgpassPath_PropagatesToCommand(t *testing.T) {
	stubPgbackrest(t, "#!/bin/bash\necho \"PGPASSFILE=$PGPASSFILE\"\nexit 1\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))
	e.SetPgpassPath("/tmp/test.pgpass")

	err := e.StanzaCreate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PGPASSFILE=/tmp/test.pgpass")
}
