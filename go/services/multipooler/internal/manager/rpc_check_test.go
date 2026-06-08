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

package manager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestRunPgBackRestCheck(t *testing.T) {
	// The real `pgbackrest check` invocation is exercised by integration
	// tests (endtoend/multipooler). This validates the config-missing guard.
	t.Run("errors when pgbackrest config is missing", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		err := pm.runPgBackRestCheck(context.Background())
		require.Error(t, err)
	})
}

// TestRunPgBackRestCheck_ExecPaths drives runPgBackRestCheck against a stubbed
// `pgbackrest` binary on PATH. Unlike verify, check relies on pgBackRest's exit
// code, so this covers the success (exit 0) and failure (non-zero) paths the
// in-process unit test cannot otherwise reach.
func TestRunPgBackRestCheck_ExecPaths(t *testing.T) {
	tests := []struct {
		name        string
		exitCode    int
		wantErr     bool
		errContains string
	}{
		{
			name:     "success on exit 0",
			exitCode: 0,
			wantErr:  false,
		},
		{
			name:        "failure on nonzero exit",
			exitCode:    1,
			wantErr:     true,
			errContains: "pgbackrest check failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binDir := t.TempDir()
			script := fmt.Sprintf("#!/bin/bash\necho 'P00   INFO: check command end: completed successfully'\nexit %d\n", tt.exitCode)
			require.NoError(t, os.WriteFile(filepath.Join(binDir, "pgbackrest"), []byte(script), 0o755))
			t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

			poolerDir := t.TempDir()
			pm := createTestManager(poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)
			pm.pgBackRestConfigPath = setupMockPgBackRestConfig(t, poolerDir)

			err := pm.runPgBackRestCheck(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}
