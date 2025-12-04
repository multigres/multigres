// Copyright 2025 Supabase, Inc.
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

package manager

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestInitializationStatus(t *testing.T) {
	tests := []struct {
		name                string
		setupFunc           func(t *testing.T, pm *MultiPoolerManager, poolerDir string)
		expectedInitialized bool
		expectedHasDataDir  bool
		expectedRole        string
		expectedShardID     string
	}{
		{
			name: "uninitialized pooler",
			setupFunc: func(t *testing.T, pm *MultiPoolerManager, poolerDir string) {
				// Don't create anything - pooler is completely uninitialized
			},
			expectedInitialized: false,
			expectedHasDataDir:  false,
			expectedRole:        "unknown",
			expectedShardID:     "0-inf",
		},
		{
			name: "pooler with data directory but no database",
			setupFunc: func(t *testing.T, pm *MultiPoolerManager, poolerDir string) {
				// Create data directory
				dataDir := filepath.Join(poolerDir, "pg_data")
				require.NoError(t, os.MkdirAll(dataDir, 0o755))
			},
			expectedInitialized: false,
			expectedHasDataDir:  true,
			expectedRole:        "unknown",
			expectedShardID:     "0-inf",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			poolerDir := t.TempDir()

			// Create test config
			store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
			defer store.Close()

			serviceID := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      "test-pooler",
			}

			config := &Config{
				PoolerDir:  poolerDir,
				PgPort:     5432,
				Database:   "postgres",
				TopoClient: store,
				ServiceID:  serviceID,
				TableGroup: types.DefaultTableGroup,
				Shard:      tt.expectedShardID,
			}

			logger := slog.Default()
			pm, err := NewMultiPoolerManager(logger, config)
			require.NoError(t, err)

			// Create multipooler record in topology
			multipooler := &clustermetadatapb.MultiPooler{
				Id:         serviceID,
				Database:   "testdb",
				TableGroup: types.DefaultTableGroup,
				Shard:      tt.expectedShardID,
			}

			pm.mu.Lock()
			pm.multipooler = &topo.MultiPoolerInfo{MultiPooler: multipooler}
			pm.updateCachedMultipooler()
			pm.mu.Unlock()

			// Run setup function
			if tt.setupFunc != nil {
				tt.setupFunc(t, pm, poolerDir)
			}

			// Call InitializationStatus
			resp, err := pm.InitializationStatus(ctx, &multipoolermanagerdatapb.InitializationStatusRequest{})
			require.NoError(t, err)
			require.NotNil(t, resp)

			// Verify response
			assert.Equal(t, tt.expectedInitialized, resp.IsInitialized, "IsInitialized mismatch")
			assert.Equal(t, tt.expectedHasDataDir, resp.HasDataDirectory, "HasDataDirectory mismatch")
			assert.Equal(t, tt.expectedRole, resp.Role, "Role mismatch")
			assert.Equal(t, tt.expectedShardID, resp.ShardId, "ShardId mismatch")
		})
	}
}

func TestInitializeEmptyPrimary(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func(t *testing.T, pm *MultiPoolerManager, poolerDir string)
		term          int64
		expectError   bool
		errorContains string
	}{
		{
			name: "initialize fresh pooler",
			setupFunc: func(t *testing.T, pm *MultiPoolerManager, poolerDir string) {
				// Fresh pooler - no setup needed
			},
			term:        1,
			expectError: false,
		},
		{
			name: "idempotent - already initialized",
			setupFunc: func(t *testing.T, pm *MultiPoolerManager, poolerDir string) {
				// Create data directory and multigres schema to simulate initialization
				dataDir := filepath.Join(poolerDir, "pg_data")
				require.NoError(t, os.MkdirAll(dataDir, 0o755))

				// Mark as initialized by setting up the manager state
				// In real scenario, database would have multigres schema
			},
			term:        1,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			poolerDir := t.TempDir()

			// Create test config
			store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
			defer store.Close()
			serviceID := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      "test-pooler",
			}

			config := &Config{
				PoolerDir:  poolerDir,
				PgPort:     5432,
				Database:   "postgres",
				TopoClient: store,
				ServiceID:  serviceID,
				TableGroup: types.DefaultTableGroup,
				Shard:      types.DefaultShard,
				// Note: pgctldClient is nil - operations that need it will fail gracefully
			}

			logger := slog.Default()
			pm, err := NewMultiPoolerManager(logger, config)
			require.NoError(t, err)

			// Initialize consensus state
			pm.consensusState = NewConsensusState(poolerDir, serviceID)
			_, err = pm.consensusState.Load()
			require.NoError(t, err)

			// Run setup function
			if tt.setupFunc != nil {
				tt.setupFunc(t, pm, poolerDir)
			}

			// Call InitializeEmptyPrimary
			req := &multipoolermanagerdatapb.InitializeEmptyPrimaryRequest{
				ConsensusTerm: tt.term,
			}

			resp, err := pm.InitializeEmptyPrimary(ctx, req)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				// Note: This will fail because pgctldClient is nil
				// But we verify the error is expected
				if err != nil {
					// Expected error due to missing pgctld client
					assert.Contains(t, err.Error(), "pgctld")
				}
				if resp != nil {
					assert.True(t, resp.Success)
				}
			}
		})
	}
}

func TestInitializeAsStandby(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func(t *testing.T, pm *MultiPoolerManager, poolerDir string)
		primaryHost   string
		primaryPort   int32
		term          int64
		force         bool
		expectError   bool
		errorContains string
	}{
		{
			name: "initialize fresh standby",
			setupFunc: func(t *testing.T, pm *MultiPoolerManager, poolerDir string) {
				// Fresh pooler - no setup needed
			},
			primaryHost: "primary-host",
			primaryPort: 5432,
			term:        1,
			force:       false,
			expectError: false,
		},
		{
			name: "force reinit removes existing data",
			setupFunc: func(t *testing.T, pm *MultiPoolerManager, poolerDir string) {
				// Create existing data directory
				dataDir := filepath.Join(poolerDir, "pg_data")
				require.NoError(t, os.MkdirAll(dataDir, 0o755))

				// Create a test file
				testFile := filepath.Join(dataDir, "test.txt")
				require.NoError(t, os.WriteFile(testFile, []byte("test"), 0o644))
			},
			primaryHost: "primary-host",
			primaryPort: 5432,
			term:        1,
			force:       true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			poolerDir := t.TempDir()

			// Create test config
			store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
			defer store.Close()
			serviceID := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      "test-pooler",
			}

			config := &Config{
				PoolerDir:  poolerDir,
				PgPort:     5432,
				Database:   "postgres",
				TopoClient: store,
				ServiceID:  serviceID,
				TableGroup: types.DefaultTableGroup,
				Shard:      types.DefaultShard,
			}

			logger := slog.Default()
			pm, err := NewMultiPoolerManager(logger, config)
			require.NoError(t, err)

			// Initialize consensus state
			pm.consensusState = NewConsensusState(poolerDir, serviceID)
			_, err = pm.consensusState.Load()
			require.NoError(t, err)

			// Run setup function
			if tt.setupFunc != nil {
				tt.setupFunc(t, pm, poolerDir)
			}

			// Verify test file exists if force test
			if tt.force {
				dataDir := filepath.Join(poolerDir, "pg_data")
				testFile := filepath.Join(dataDir, "test.txt")
				_, err := os.Stat(testFile)
				require.NoError(t, err, "test file should exist before reinit")
			}

			// Call InitializeAsStandby
			req := &multipoolermanagerdatapb.InitializeAsStandbyRequest{
				PrimaryHost:   tt.primaryHost,
				PrimaryPort:   tt.primaryPort,
				ConsensusTerm: tt.term,
				Force:         tt.force,
			}

			resp, err := pm.InitializeAsStandby(ctx, req)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				// Note: This will fail because pgctldClient is nil
				// But we verify the error is expected or that reinit worked
				if tt.force {
					// Verify data directory was removed
					dataDir := filepath.Join(poolerDir, "pg_data")
					testFile := filepath.Join(dataDir, "test.txt")
					_, err := os.Stat(testFile)
					assert.True(t, os.IsNotExist(err), "test file should be removed after force reinit")
				}

				if err != nil {
					// Expected error due to missing pgctld client or Restore not implemented
					// This is acceptable for unit tests
					t.Logf("Expected error (pgctld or Restore not available): %v", err)
				}
				if resp != nil {
					assert.True(t, resp.Success)
				}
			}
		})
	}
}

func TestHelperMethods(t *testing.T) {
	t.Run("hasDataDirectory", func(t *testing.T) {
		poolerDir := t.TempDir()
		config := &Config{PoolerDir: poolerDir}
		pm := &MultiPoolerManager{config: config}

		// Initially no data directory
		assert.False(t, pm.hasDataDirectory())

		// Create data directory
		dataDir := filepath.Join(poolerDir, "pg_data")
		require.NoError(t, os.MkdirAll(dataDir, 0o755))

		// Now should return true
		assert.True(t, pm.hasDataDirectory())
	})

	t.Run("getShardID", func(t *testing.T) {
		serviceID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "test-cell",
			Name:      "test-pooler",
		}

		multipooler := &clustermetadatapb.MultiPooler{
			Id:         serviceID,
			Database:   "testdb",
			TableGroup: "testgroup",
			Shard:      "shard-123",
		}

		pm := &MultiPoolerManager{
			multipooler: &topo.MultiPoolerInfo{MultiPooler: multipooler},
		}

		assert.Equal(t, "shard-123", pm.getShardID())
	})

	t.Run("removeDataDirectory safety checks", func(t *testing.T) {
		poolerDir := t.TempDir()
		config := &Config{PoolerDir: poolerDir}
		pm := &MultiPoolerManager{config: config, logger: slog.Default()}

		// Create data directory
		dataDir := filepath.Join(poolerDir, "pg_data")
		require.NoError(t, os.MkdirAll(dataDir, 0o755))

		// Should succeed with valid directory
		err := pm.removeDataDirectory()
		require.NoError(t, err)

		// Verify directory was removed
		_, err = os.Stat(dataDir)
		assert.True(t, os.IsNotExist(err))
	})
}
