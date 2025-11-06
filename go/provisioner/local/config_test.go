// Copyright 2025 Supabase, Inc.
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

package local

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeneratePgBackRestConfigs(t *testing.T) {
	t.Run("creates backup directory and config files", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create a minimal config
		p := &localProvisioner{
			config: &LocalProvisionerConfig{
				RootWorkingDir: tmpDir,
				BackupRepoPath: filepath.Join(tmpDir, "data", "backups"),
				Cells: map[string]CellServicesConfig{
					"zone1": {
						Multipooler: MultipoolerConfig{
							ServiceID:  "test-service-1",
							PoolerDir:  filepath.Join(tmpDir, "pooler1"),
							PgPort:     5432,
							BackupConf: filepath.Join(tmpDir, "pooler1", "pgbackrest.conf"),
						},
					},
					"zone2": {
						Multipooler: MultipoolerConfig{
							ServiceID:  "test-service-2",
							PoolerDir:  filepath.Join(tmpDir, "pooler2"),
							PgPort:     5433,
							BackupConf: filepath.Join(tmpDir, "pooler2", "pgbackrest.conf"),
						},
					},
				},
			},
		}

		// Create pooler directories
		require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "pooler1"), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "pooler2"), 0o755))

		// Generate configs
		err := p.GeneratePgBackRestConfigs()
		require.NoError(t, err)

		// Verify backup repository directory was created
		backupDir := filepath.Join(tmpDir, "data", "backups")
		stat, err := os.Stat(backupDir)
		require.NoError(t, err)
		assert.True(t, stat.IsDir())

		// Verify config files were created for both zones
		config1 := filepath.Join(tmpDir, "pooler1", "pgbackrest.conf")
		content1, err := os.ReadFile(config1)
		require.NoError(t, err)
		assert.Contains(t, string(content1), "[global]")
		assert.Contains(t, string(content1), "[test-service-1]")
		assert.Contains(t, string(content1), "repo1-path="+backupDir)
		assert.Contains(t, string(content1), "pg1-port=5432")

		config2 := filepath.Join(tmpDir, "pooler2", "pgbackrest.conf")
		content2, err := os.ReadFile(config2)
		require.NoError(t, err)
		assert.Contains(t, string(content2), "[global]")
		assert.Contains(t, string(content2), "[test-service-2]")
		assert.Contains(t, string(content2), "repo1-path="+backupDir)
		assert.Contains(t, string(content2), "pg1-port=5433")
	})

	t.Run("uses default paths when BackupConf is empty", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create config without BackupConf specified
		p := &localProvisioner{
			config: &LocalProvisionerConfig{
				RootWorkingDir: tmpDir,
				BackupRepoPath: "", // Empty - should use default
				Cells: map[string]CellServicesConfig{
					"zone1": {
						Multipooler: MultipoolerConfig{
							ServiceID:  "test-service",
							PoolerDir:  filepath.Join(tmpDir, "pooler1"),
							PgPort:     5432,
							BackupConf: "", // Empty - should use default
						},
					},
				},
			},
		}

		// Create pooler directory
		require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "pooler1"), 0o755))

		// Generate configs
		err := p.GeneratePgBackRestConfigs()
		require.NoError(t, err)

		// Verify default backup repository was created
		defaultBackupDir := filepath.Join(tmpDir, "data", "backups")
		stat, err := os.Stat(defaultBackupDir)
		require.NoError(t, err)
		assert.True(t, stat.IsDir())

		// Verify BackupRepoPath was set to default
		assert.Equal(t, defaultBackupDir, p.config.BackupRepoPath)

		// Verify config file was created at default location
		defaultConfigPath := filepath.Join(tmpDir, "pooler1", "pgbackrest.conf")
		content, err := os.ReadFile(defaultConfigPath)
		require.NoError(t, err)
		assert.Contains(t, string(content), "[global]")
		assert.Contains(t, string(content), "[test-service]")
	})

	t.Run("handles multiple cells correctly", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create config with 3 cells
		cells := make(map[string]CellServicesConfig)
		for i := 1; i <= 3; i++ {
			cellName := filepath.Join(tmpDir, "pooler", string(rune('0'+i)))
			cells[cellName] = CellServicesConfig{
				Multipooler: MultipoolerConfig{
					ServiceID:  "service-" + string(rune('0'+i)),
					PoolerDir:  filepath.Join(tmpDir, "pooler"+string(rune('0'+i))),
					PgPort:     5432 + i - 1,
					BackupConf: filepath.Join(tmpDir, "pooler"+string(rune('0'+i)), "pgbackrest.conf"),
				},
			}
			// Create pooler directory
			require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "pooler"+string(rune('0'+i))), 0o755))
		}

		p := &localProvisioner{
			config: &LocalProvisionerConfig{
				RootWorkingDir: tmpDir,
				BackupRepoPath: filepath.Join(tmpDir, "data", "backups"),
				Cells:          cells,
			},
		}

		// Generate configs
		err := p.GeneratePgBackRestConfigs()
		require.NoError(t, err)

		// Verify all config files were created
		for i := 1; i <= 3; i++ {
			configPath := filepath.Join(tmpDir, "pooler"+string(rune('0'+i)), "pgbackrest.conf")
			_, err := os.Stat(configPath)
			require.NoError(t, err, "config file should exist for pooler %d", i)
		}
	})

	t.Run("fails when config is nil", func(t *testing.T) {
		p := &localProvisioner{
			config: nil,
		}

		err := p.GeneratePgBackRestConfigs()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "configuration not loaded")
	})

	t.Run("creates backup directory with correct permissions", func(t *testing.T) {
		tmpDir := t.TempDir()
		backupPath := filepath.Join(tmpDir, "custom-backups")

		p := &localProvisioner{
			config: &LocalProvisionerConfig{
				RootWorkingDir: tmpDir,
				BackupRepoPath: backupPath,
				Cells: map[string]CellServicesConfig{
					"zone1": {
						Multipooler: MultipoolerConfig{
							ServiceID:  "test",
							PoolerDir:  filepath.Join(tmpDir, "pooler"),
							PgPort:     5432,
							BackupConf: filepath.Join(tmpDir, "pooler", "pgbackrest.conf"),
						},
					},
				},
			},
		}

		require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "pooler"), 0o755))

		err := p.GeneratePgBackRestConfigs()
		require.NoError(t, err)

		// Verify directory exists and has correct permissions
		stat, err := os.Stat(backupPath)
		require.NoError(t, err)
		assert.True(t, stat.IsDir())
		assert.Equal(t, os.FileMode(0o755)|os.ModeDir, stat.Mode())
	})
}
