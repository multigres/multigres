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
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// fakeIdentity is a test Identity for the backup engine. It exposes a fixed
// pooler ID/shard.
type fakeIdentity struct {
	id       *clustermetadatapb.ID
	shardKey *clustermetadatapb.ShardKey
}

func (f *fakeIdentity) Id() *clustermetadatapb.ID             { return f.id }
func (f *fakeIdentity) ShardKey() *clustermetadatapb.ShardKey { return f.shardKey }

// newTestEngine builds a backup engine with a fake identity, optionally
// resolving a repo config from the given backup location.
func newTestEngine(t *testing.T, poolerDir, tableGroup, shard, backupLocation string) (*Engine, *fakeIdentity) {
	t.Helper()

	database := "test-database"
	if tableGroup == "" {
		tableGroup = constants.DefaultTableGroup
	}
	if shard == "" {
		shard = constants.DefaultShard
	}

	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-multipooler",
	}
	fid := &fakeIdentity{
		id: id,
		shardKey: &clustermetadatapb.ShardKey{
			TableGroup: tableGroup,
			Shard:      shard,
			Database:   database,
		},
	}

	var backupConfig *commonbackup.Config
	if backupLocation != "" {
		backupConfig, _ = commonbackup.NewConfig(utils.FilesystemBackupLocation(backupLocation))
	}

	logger := slog.Default()
	run := func(ctx context.Context, cmd *executil.Cmd, _ string) ([]byte, error) {
		return cmd.CombinedOutput()
	}
	e, err := NewEngine(logger, run, fid, Settings{})
	require.NoError(t, err)
	if backupConfig != nil {
		e.SetBackupConfig(backupConfig)
	}
	return e, fid
}

// setupMockPgBackRestConfig creates a mock pgbackrest.conf file and returns its path.
func setupMockPgBackRestConfig(t *testing.T, poolerDir string) string {
	t.Helper()

	pgbackrestDir := filepath.Join(poolerDir, "pgbackrest")
	require.NoError(t, os.MkdirAll(pgbackrestDir, 0o755), "failed to create pgbackrest directory")

	configPath := filepath.Join(pgbackrestDir, "pgbackrest.conf")
	configContent := `[global]
log-path=` + filepath.Join(pgbackrestDir, "logs") + `
spool-path=` + filepath.Join(pgbackrestDir, "spool") + `
lock-path=` + filepath.Join(pgbackrestDir, "lock") + `

[multigres]
repo1-path=/tmp/backups
pg1-path=` + filepath.Join(poolerDir, "pg_data") + `
pg1-socket-path=` + filepath.Join(poolerDir, "pg_sockets") + `
pg1-port=5432
`
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0o600), "failed to create pgbackrest.conf")
	return configPath
}

func TestFindByJobID(t *testing.T) {
	tests := []struct {
		name          string
		jobID         string
		jsonOutput    string
		wantBackupID  string
		wantError     bool
		errorContains string
	}{
		{
			name:  "Single matching backup",
			jobID: "20250104-100000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}]
			}]`,
			wantBackupID: "20250104-100000F",
			wantError:    false,
		},
		{
			name:  "Multiple backups, one match",
			jobID: "20250104-120000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}, {
					"label": "20250104-120000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-120000.000000_mp-zone1"
					}
				}]
			}]`,
			wantBackupID: "20250104-120000F",
			wantError:    false,
		},
		{
			name:  "No matching backup",
			jobID: "20250104-180000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}]
			}]`,
			wantError:     true,
			errorContains: "no backup found",
		},
		{
			name:          "No backups at all",
			jobID:         "20250104-100000.000000_mp-zone1",
			jsonOutput:    `[{"backup": []}]`,
			wantError:     true,
			errorContains: "no backups found",
		},
		{
			name:  "Duplicate matching backups",
			jobID: "20250104-100000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}, {
					"label": "20250104-100000F_20250104-110000I",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}]
			}]`,
			wantError:     true,
			errorContains: "found 2 backups",
		},
		{
			name:  "Backup without annotations",
			jobID: "20250104-100000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F"
				}]
			}]`,
			wantError:     true,
			errorContains: "no backup found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp directory for mock pgbackrest binary
			binDir := t.TempDir()

			// Create mock pgbackrest binary that returns the test JSON
			mockScript := `#!/bin/bash
if [[ "$*" == *"info"* ]]; then
    cat << 'JSONEOF'
` + tt.jsonOutput + `
JSONEOF
    exit 0
fi
exit 1
`
			pgbackrestPath := binDir + "/pgbackrest"
			err := exec.Command("sh", "-c", "cat > "+pgbackrestPath+" << 'EOF'\n"+mockScript+"\nEOF").Run()
			require.NoError(t, err)
			err = exec.Command("chmod", "+x", pgbackrestPath).Run()
			require.NoError(t, err)

			// Prepend bin dir to PATH so our mock pgbackrest is found first
			t.Setenv("PATH", binDir+":/usr/bin:/bin")

			// Use separate directory for pooler data
			poolerDir := t.TempDir()
			configPath := setupMockPgBackRestConfig(t, poolerDir)
			e, _ := newTestEngine(t, poolerDir, "test-tg", "0", poolerDir)
			e.SetConfigPath(configPath)

			backupID, err := e.FindByJobID(context.Background(), tt.jobID)

			if tt.wantError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantBackupID, backupID)
			}
		})
	}
}

func TestRequireConfigPath(t *testing.T) {
	t.Run("Success when config path is set", func(t *testing.T) {
		poolerDir := t.TempDir()
		configPath := setupMockPgBackRestConfig(t, poolerDir)
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/test-backups")
		e.SetConfigPath(configPath)

		result, err := e.requireConfigPath()
		require.NoError(t, err)
		assert.Equal(t, configPath, result)
	})

	t.Run("Error when config path is not set", func(t *testing.T) {
		poolerDir := t.TempDir()
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/test-backups")

		result, err := e.requireConfigPath()
		require.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "not yet generated")
	})

	t.Run("Repeated calls return same path", func(t *testing.T) {
		poolerDir := t.TempDir()
		configPath := setupMockPgBackRestConfig(t, poolerDir)
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")
		e.SetConfigPath(configPath)

		result1, err := e.requireConfigPath()
		require.NoError(t, err)

		result2, err := e.requireConfigPath()
		require.NoError(t, err)
		assert.Equal(t, result1, result2, "should return same path on repeated calls")
	})
}
