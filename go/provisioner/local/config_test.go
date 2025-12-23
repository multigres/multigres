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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/provisioner/local/ports"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
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
							Database:   "postgres",
							TableGroup: constants.DefaultTableGroup,
							PoolerDir:  filepath.Join(tmpDir, "pooler1"),
							PgPort:     5432,
							BackupConf: filepath.Join(tmpDir, "pooler1", "pgbackrest.conf"),
						},
					},
					"zone2": {
						Multipooler: MultipoolerConfig{
							ServiceID:  "test-service-2",
							Database:   "postgres",
							TableGroup: constants.DefaultTableGroup,
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

		// Verify per-pooler spool and lock directories were created
		spoolDir1 := filepath.Join(tmpDir, "data", "backups", "spool", "pooler_test-service-1")
		stat, err = os.Stat(spoolDir1)
		require.NoError(t, err)
		assert.True(t, stat.IsDir())

		spoolDir2 := filepath.Join(tmpDir, "data", "backups", "spool", "pooler_test-service-2")
		stat, err = os.Stat(spoolDir2)
		require.NoError(t, err)
		assert.True(t, stat.IsDir())

		lockDir1 := filepath.Join(tmpDir, "data", "backups", "lock", "pooler_test-service-1")
		stat, err = os.Stat(lockDir1)
		require.NoError(t, err)
		assert.True(t, stat.IsDir())

		lockDir2 := filepath.Join(tmpDir, "data", "backups", "lock", "pooler_test-service-2")
		stat, err = os.Stat(lockDir2)
		require.NoError(t, err)
		assert.True(t, stat.IsDir())

		// Verify config files were created for both zones
		config1 := filepath.Join(tmpDir, "pooler1", "pgbackrest.conf")
		content1, err := os.ReadFile(config1)
		require.NoError(t, err)
		assert.Contains(t, string(content1), "[global]")
		assert.Contains(t, string(content1), "[multigres]")      // Shared stanza for HA
		assert.NotContains(t, string(content1), "repo1-path=")   // repo1-path should not be in config
		assert.Contains(t, string(content1), "pg1-socket-path=") // Using socket connections
		assert.Contains(t, string(content1), "pg2-path=")        // Has zone2 as pg2
		assert.Contains(t, string(content1), "spool-path="+spoolDir1)
		assert.Contains(t, string(content1), "lock-path="+lockDir1)

		config2 := filepath.Join(tmpDir, "pooler2", "pgbackrest.conf")
		content2, err := os.ReadFile(config2)
		require.NoError(t, err)
		assert.Contains(t, string(content2), "[global]")
		assert.Contains(t, string(content2), "[multigres]")      // Shared stanza for HA
		assert.NotContains(t, string(content2), "repo1-path=")   // repo1-path should not be in config
		assert.Contains(t, string(content2), "pg1-socket-path=") // Using socket connections
		assert.Contains(t, string(content2), "pg2-path=")        // Has zone1 as pg2
		assert.Contains(t, string(content2), "spool-path="+spoolDir2)
		assert.Contains(t, string(content2), "lock-path="+lockDir2)
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
							Database:   "postgres",
							TableGroup: constants.DefaultTableGroup,
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

		// Verify per-pooler spool and lock directories were created
		spoolDir := filepath.Join(tmpDir, "data", "backups", "spool", "pooler_test-service")
		stat, err = os.Stat(spoolDir)
		require.NoError(t, err)
		assert.True(t, stat.IsDir())

		lockDir := filepath.Join(tmpDir, "data", "backups", "lock", "pooler_test-service")
		stat, err = os.Stat(lockDir)
		require.NoError(t, err)
		assert.True(t, stat.IsDir())

		// Verify config file was created at default location
		defaultConfigPath := filepath.Join(tmpDir, "pooler1", "pgbackrest.conf")
		content, err := os.ReadFile(defaultConfigPath)
		require.NoError(t, err)
		assert.Contains(t, string(content), "[global]")
		assert.Contains(t, string(content), "[multigres]")    // Shared stanza for HA
		assert.NotContains(t, string(content), "repo1-path=") // repo1-path should not be in config
		assert.Contains(t, string(content), "spool-path="+spoolDir)
		assert.Contains(t, string(content), "lock-path="+lockDir)
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
					Database:   "postgres",
					TableGroup: constants.DefaultTableGroup,
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
							Database:   "postgres",
							TableGroup: constants.DefaultTableGroup,
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

	t.Run("generates symmetric configs for 3-cluster HA deployment", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create 3 cells with predictable names (alphabetically sorted)
		// to ensure consistent pg2/pg3 ordering
		p := &localProvisioner{
			config: &LocalProvisionerConfig{
				RootWorkingDir: tmpDir,
				BackupRepoPath: filepath.Join(tmpDir, "data", "backups"),
				Cells: map[string]CellServicesConfig{
					"cell-a": {
						Multipooler: MultipoolerConfig{
							ServiceID:  "service-a",
							Database:   "postgres",
							TableGroup: constants.DefaultTableGroup,
							PoolerDir:  filepath.Join(tmpDir, "pooler-a"),
							PgPort:     5432,
							BackupConf: filepath.Join(tmpDir, "pooler-a", "pgbackrest.conf"),
						},
					},
					"cell-b": {
						Multipooler: MultipoolerConfig{
							ServiceID:  "service-b",
							Database:   "postgres",
							TableGroup: constants.DefaultTableGroup,
							PoolerDir:  filepath.Join(tmpDir, "pooler-b"),
							PgPort:     5433,
							BackupConf: filepath.Join(tmpDir, "pooler-b", "pgbackrest.conf"),
						},
					},
					"cell-c": {
						Multipooler: MultipoolerConfig{
							ServiceID:  "service-c",
							Database:   "postgres",
							TableGroup: constants.DefaultTableGroup,
							PoolerDir:  filepath.Join(tmpDir, "pooler-c"),
							PgPort:     5434,
							BackupConf: filepath.Join(tmpDir, "pooler-c", "pgbackrest.conf"),
						},
					},
				},
			},
		}

		// Create pooler directories
		for _, cell := range []string{"a", "b", "c"} {
			require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "pooler-"+cell), 0o755))
		}

		// Generate configs
		err := p.GeneratePgBackRestConfigs()
		require.NoError(t, err)

		// Read all config files
		configA, err := os.ReadFile(filepath.Join(tmpDir, "pooler-a", "pgbackrest.conf"))
		require.NoError(t, err)
		configB, err := os.ReadFile(filepath.Join(tmpDir, "pooler-b", "pgbackrest.conf"))
		require.NoError(t, err)
		configC, err := os.ReadFile(filepath.Join(tmpDir, "pooler-c", "pgbackrest.conf"))
		require.NoError(t, err)

		contentA := string(configA)
		contentB := string(configB)
		contentC := string(configC)

		// Verify all configs use the shared stanza name for HA
		assert.Contains(t, contentA, "[multigres]")
		assert.Contains(t, contentB, "[multigres]")
		assert.Contains(t, contentC, "[multigres]")

		// Verify each cluster treats itself as pg1
		assert.Contains(t, contentA, "pg1-path="+filepath.Join(tmpDir, "pooler-a", "pg_data"))
		assert.Contains(t, contentA, "pg1-socket-path="+filepath.Join(tmpDir, "pooler-a", "pg_sockets"))
		assert.Contains(t, contentB, "pg1-path="+filepath.Join(tmpDir, "pooler-b", "pg_data"))
		assert.Contains(t, contentB, "pg1-socket-path="+filepath.Join(tmpDir, "pooler-b", "pg_sockets"))
		assert.Contains(t, contentC, "pg1-path="+filepath.Join(tmpDir, "pooler-c", "pg_data"))
		assert.Contains(t, contentC, "pg1-socket-path="+filepath.Join(tmpDir, "pooler-c", "pg_sockets"))

		// Verify pg1-port is present even with socket-path (port is needed for socket filename)
		assert.Contains(t, contentA, "pg1-port=")
		assert.Contains(t, contentB, "pg1-port=")
		assert.Contains(t, contentC, "pg1-port=")

		// Verify cluster A has B and C as pg2 and pg3 (sorted order)
		assert.Contains(t, contentA, "pg2-path="+filepath.Join(tmpDir, "pooler-b", "pg_data"))
		assert.Contains(t, contentA, "pg2-socket-path="+filepath.Join(tmpDir, "pooler-b", "pg_sockets"))
		assert.Contains(t, contentA, "pg3-path="+filepath.Join(tmpDir, "pooler-c", "pg_data"))
		assert.Contains(t, contentA, "pg3-socket-path="+filepath.Join(tmpDir, "pooler-c", "pg_sockets"))
		assert.Contains(t, contentA, "pg2-port=")
		assert.Contains(t, contentA, "pg3-port=")

		// Verify cluster B has A and C as pg2 and pg3 (sorted order)
		assert.Contains(t, contentB, "pg2-path="+filepath.Join(tmpDir, "pooler-a", "pg_data"))
		assert.Contains(t, contentB, "pg2-socket-path="+filepath.Join(tmpDir, "pooler-a", "pg_sockets"))
		assert.Contains(t, contentB, "pg3-path="+filepath.Join(tmpDir, "pooler-c", "pg_data"))
		assert.Contains(t, contentB, "pg3-socket-path="+filepath.Join(tmpDir, "pooler-c", "pg_sockets"))
		assert.Contains(t, contentB, "pg2-port=")
		assert.Contains(t, contentB, "pg3-port=")

		// Verify cluster C has A and B as pg2 and pg3 (sorted order)
		assert.Contains(t, contentC, "pg2-path="+filepath.Join(tmpDir, "pooler-a", "pg_data"))
		assert.Contains(t, contentC, "pg2-socket-path="+filepath.Join(tmpDir, "pooler-a", "pg_sockets"))
		assert.Contains(t, contentC, "pg3-path="+filepath.Join(tmpDir, "pooler-b", "pg_data"))
		assert.Contains(t, contentC, "pg3-socket-path="+filepath.Join(tmpDir, "pooler-b", "pg_sockets"))
		assert.Contains(t, contentC, "pg2-port=")
		assert.Contains(t, contentC, "pg3-port=")

		// Verify symmetry: each cluster should be in exactly the right position
		// in other clusters' configs
		// A should NOT appear in its own additional hosts
		assert.NotContains(t, contentA, "pg2-path="+filepath.Join(tmpDir, "pooler-a", "pg_data"))
		assert.NotContains(t, contentA, "pg3-path="+filepath.Join(tmpDir, "pooler-a", "pg_data"))
		// B should NOT appear in its own additional hosts
		assert.NotContains(t, contentB, "pg2-path="+filepath.Join(tmpDir, "pooler-b", "pg_data"))
		assert.NotContains(t, contentB, "pg3-path="+filepath.Join(tmpDir, "pooler-b", "pg_data"))
		// C should NOT appear in its own additional hosts
		assert.NotContains(t, contentC, "pg2-path="+filepath.Join(tmpDir, "pooler-c", "pg_data"))
		assert.NotContains(t, contentC, "pg3-path="+filepath.Join(tmpDir, "pooler-c", "pg_data"))

		// Verify repo1-path is not in any of the configs
		assert.NotContains(t, contentA, "repo1-path=")
		assert.NotContains(t, contentB, "repo1-path=")
		assert.NotContains(t, contentC, "repo1-path=")
	})

	t.Run("uses correct defaults for local ports", func(t *testing.T) {
		tmpDir := t.TempDir()
		p := &localProvisioner{}

		// Generate default config
		config := p.DefaultConfig([]string{tmpDir})
		p.config = &LocalProvisionerConfig{}

		// Convert map config to typed config for easier assertion
		yamlData, err := yaml.Marshal(config)
		require.NoError(t, err)
		err = yaml.Unmarshal(yamlData, p.config)
		require.NoError(t, err)

		// Verify Zone 1 (Primary)
		zone1 := p.config.Cells["zone1"]
		assert.Equal(t, ports.DefaultLocalPostgresPort, zone1.Multipooler.PgPort, "Zone 1 Multipooler PG port should be 25432")
		assert.Equal(t, ports.DefaultLocalPostgresPort, zone1.Pgctld.PgPort, "Zone 1 Pgctld PG port should be 25432")
		assert.Equal(t, ports.DefaultMultigatewayPG, zone1.Multigateway.PgPort, "Zone 1 Multigateway PG port should be 15432")

		// Verify Zone 2
		zone2 := p.config.Cells["zone2"]
		assert.Equal(t, ports.DefaultLocalPostgresPort+1, zone2.Multipooler.PgPort, "Zone 2 Multipooler PG port should be 25433")
		assert.Equal(t, ports.DefaultLocalPostgresPort+1, zone2.Pgctld.PgPort, "Zone 2 Pgctld PG port should be 25433")
		assert.Equal(t, ports.DefaultMultigatewayPG+1, zone2.Multigateway.PgPort, "Zone 2 Multigateway PG port should be 15433")

		// Verify Zone 3
		zone3 := p.config.Cells["zone3"]
		assert.Equal(t, ports.DefaultLocalPostgresPort+2, zone3.Multipooler.PgPort, "Zone 3 Multipooler PG port should be 25434")
		assert.Equal(t, ports.DefaultLocalPostgresPort+2, zone3.Pgctld.PgPort, "Zone 3 Pgctld PG port should be 25434")
		assert.Equal(t, ports.DefaultMultigatewayPG+2, zone3.Multigateway.PgPort, "Zone 3 Multigateway PG port should be 15434")
	})
}
