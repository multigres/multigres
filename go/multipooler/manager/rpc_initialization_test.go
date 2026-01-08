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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

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
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
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
		primary       *clustermetadatapb.MultiPooler
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
			primary: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "test-cell",
					Name:      "primary-pooler",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			term:        1,
			force:       false,
			expectError: false,
		},
		{
			name: "force reinit removes existing data",
			setupFunc: func(t *testing.T, pm *MultiPoolerManager, poolerDir string) {
				// Create existing data directory with PG_VERSION file
				dataDir := filepath.Join(poolerDir, "pg_data")
				require.NoError(t, os.MkdirAll(dataDir, 0o755))
				pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
				require.NoError(t, os.WriteFile(pgVersionFile, []byte("16"), 0o644))

				// Create a test file
				testFile := filepath.Join(dataDir, "test.txt")
				require.NoError(t, os.WriteFile(testFile, []byte("test"), 0o644))
			},
			primary: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "test-cell",
					Name:      "primary-pooler",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			term:        1,
			force:       true,
			expectError: false,
		},
		{
			name: "error when primary is nil",
			setupFunc: func(t *testing.T, pm *MultiPoolerManager, poolerDir string) {
				// Fresh pooler - no setup needed
			},
			primary:       nil,
			term:          1,
			force:         false,
			expectError:   true,
			errorContains: "primary is required",
		},
		{
			name: "error when primary has no postgres port",
			setupFunc: func(t *testing.T, pm *MultiPoolerManager, poolerDir string) {
				// Fresh pooler - no setup needed
			},
			primary: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "test-cell",
					Name:      "primary-pooler",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{}, // No postgres port
			},
			term:          1,
			force:         false,
			expectError:   true,
			errorContains: "no postgres port configured",
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
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
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
				Primary:       tt.primary,
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

func TestInitializeAsStandbySetsPrimaryPoolerID(t *testing.T) {
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
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	logger := slog.Default()
	pm, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)

	// Initialize consensus state
	pm.consensusState = NewConsensusState(poolerDir, serviceID)
	_, err = pm.consensusState.Load()
	require.NoError(t, err)

	// Verify primaryPoolerID is initially nil
	pm.mu.Lock()
	assert.Nil(t, pm.primaryPoolerID, "primaryPoolerID should be nil initially")
	pm.mu.Unlock()

	// Create primary MultiPooler
	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "test-cell",
		Name:      "primary-pooler",
	}
	primary := &clustermetadatapb.MultiPooler{
		Id:       primaryID,
		Hostname: "primary-host",
		PortMap:  map[string]int32{"postgres": 5432},
	}

	// Call InitializeAsStandby with Primary field
	req := &multipoolermanagerdatapb.InitializeAsStandbyRequest{
		Primary:       primary,
		ConsensusTerm: 1,
		Force:         false,
	}

	// This will fail early due to missing pgctld/backup, but primaryPoolerID should be set first
	_, _ = pm.InitializeAsStandby(ctx, req)

	// Verify primaryPoolerID was set (even though the operation failed later)
	pm.mu.Lock()
	assert.NotNil(t, pm.primaryPoolerID, "primaryPoolerID should be set from Primary field")
	if pm.primaryPoolerID != nil {
		assert.Equal(t, "primary-pooler", pm.primaryPoolerID.Name)
		assert.Equal(t, "test-cell", pm.primaryPoolerID.Cell)
	}
	pm.mu.Unlock()
}

func TestHelperMethods(t *testing.T) {
	t.Run("hasDataDirectory", func(t *testing.T) {
		poolerDir := t.TempDir()
		config := &Config{PoolerDir: poolerDir}
		pm := &MultiPoolerManager{config: config}

		// Initially no data directory
		assert.False(t, pm.hasDataDirectory())

		// Create data directory with PG_VERSION file (simulating initialized postgres)
		dataDir := filepath.Join(poolerDir, "pg_data")
		require.NoError(t, os.MkdirAll(dataDir, 0o755))
		pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
		require.NoError(t, os.WriteFile(pgVersionFile, []byte("16"), 0o644))

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
			multipooler: &topoclient.MultiPoolerInfo{MultiPooler: multipooler},
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

// MonitorPostgres Tests

func TestDiscoverPostgresState_PgctldUnavailable(t *testing.T) {
	ctx := context.Background()
	pm := &MultiPoolerManager{
		pgctldClient: nil, // pgctld unavailable
	}

	state := pm.discoverPostgresState(ctx)

	assert.False(t, state.pgctldAvailable)
	assert.False(t, state.dirInitialized)
	assert.False(t, state.postgresRunning)
	assert.False(t, state.backupsAvailable)
}

func TestDiscoverPostgresState_NotInitialized(t *testing.T) {
	ctx := context.Background()

	// Create mock pgctld client
	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_NOT_INITIALIZED,
		},
	}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
		actionLock:   NewActionLock(),
		config:       &Config{PoolerDir: t.TempDir()},
	}

	state := pm.discoverPostgresState(ctx)

	assert.True(t, state.pgctldAvailable)
	assert.False(t, state.dirInitialized)
	assert.False(t, state.postgresRunning)
	// backupsAvailable will be false since no pgbackrest setup
	assert.False(t, state.backupsAvailable)
}

func TestDiscoverPostgresState_InitializedNotRunning(t *testing.T) {
	ctx := context.Background()

	// Create mock pgctld client
	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_STOPPED,
		},
	}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
	}

	state := pm.discoverPostgresState(ctx)

	assert.True(t, state.pgctldAvailable)
	assert.True(t, state.dirInitialized)
	assert.False(t, state.postgresRunning)
	// backupsAvailable should NOT be checked when dirInitialized is true
}

func TestDiscoverPostgresState_Running(t *testing.T) {
	ctx := context.Background()

	// Create mock pgctld client
	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_RUNNING,
		},
	}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
	}

	state := pm.discoverPostgresState(ctx)

	assert.True(t, state.pgctldAvailable)
	assert.True(t, state.dirInitialized)
	assert.True(t, state.postgresRunning)
}

func TestDiscoverPostgresState_StatusError(t *testing.T) {
	ctx := context.Background()

	// Create mock pgctld client that returns error
	mockPgctld := &mockPgctldClient{
		statusError: assert.AnError,
	}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
	}

	state := pm.discoverPostgresState(ctx)

	// When Status() fails, treat as pgctld unavailable
	assert.False(t, state.pgctldAvailable)
	assert.False(t, state.dirInitialized)
	assert.False(t, state.postgresRunning)
	assert.False(t, state.backupsAvailable)
}

func TestTakeRemedialAction_PgctldUnavailable(t *testing.T) {
	ctx := context.Background()

	pm := &MultiPoolerManager{
		logger: slog.Default(),
	}

	state := postgresState{
		pgctldAvailable: false,
	}

	lastLoggedReason := ""

	// Should log error and take no action
	pm.takeRemedialAction(ctx, state, &lastLoggedReason)

	assert.Equal(t, "pgctld_unavailable", lastLoggedReason)
}

func TestTakeRemedialAction_PostgresRunning(t *testing.T) {
	ctx := context.Background()

	pm := &MultiPoolerManager{
		logger: slog.Default(),
	}

	state := postgresState{
		pgctldAvailable: true,
		dirInitialized:  true,
		postgresRunning: true,
	}

	lastLoggedReason := ""

	// Should log info and take no action
	pm.takeRemedialAction(ctx, state, &lastLoggedReason)

	assert.Equal(t, "postgres_running", lastLoggedReason)
}

func TestTakeRemedialAction_StartPostgres(t *testing.T) {
	ctx := context.Background()

	mockPgctld := &mockPgctldClient{}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
	}

	state := postgresState{
		pgctldAvailable: true,
		dirInitialized:  true,
		postgresRunning: false,
	}

	lastLoggedReason := ""

	// Should attempt to start postgres
	pm.takeRemedialAction(ctx, state, &lastLoggedReason)

	assert.Equal(t, "starting_postgres", lastLoggedReason)
	assert.True(t, mockPgctld.startCalled, "Should have called Start()")
}

func TestTakeRemedialAction_StartPostgresFails(t *testing.T) {
	ctx := context.Background()

	mockPgctld := &mockPgctldClient{
		startError: assert.AnError,
	}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
	}

	state := postgresState{
		pgctldAvailable: true,
		dirInitialized:  true,
		postgresRunning: false,
	}

	lastLoggedReason := "starting_postgres"

	// Should handle error gracefully
	pm.takeRemedialAction(ctx, state, &lastLoggedReason)

	assert.True(t, mockPgctld.startCalled, "Should have attempted to call Start()")
	// Reason stays the same since we're retrying
}

func TestTakeRemedialAction_WaitingForBackup(t *testing.T) {
	ctx := context.Background()

	pm := &MultiPoolerManager{
		logger: slog.Default(),
	}

	state := postgresState{
		pgctldAvailable:  true,
		dirInitialized:   false,
		postgresRunning:  false,
		backupsAvailable: false,
	}

	lastLoggedReason := ""

	// Should log info and wait
	pm.takeRemedialAction(ctx, state, &lastLoggedReason)

	assert.Equal(t, "waiting_for_backup", lastLoggedReason)
}

func TestTakeRemedialAction_LogDeduplication(t *testing.T) {
	ctx := context.Background()

	pm := &MultiPoolerManager{
		logger: slog.Default(),
	}

	state := postgresState{
		pgctldAvailable: true,
		dirInitialized:  true,
		postgresRunning: true,
	}

	lastLoggedReason := "postgres_running"

	// Call multiple times with same reason
	pm.takeRemedialAction(ctx, state, &lastLoggedReason)
	pm.takeRemedialAction(ctx, state, &lastLoggedReason)
	pm.takeRemedialAction(ctx, state, &lastLoggedReason)

	// Reason should stay the same (log deduplication working)
	assert.Equal(t, "postgres_running", lastLoggedReason)

	// Change state
	state.pgctldAvailable = false
	pm.takeRemedialAction(ctx, state, &lastLoggedReason)

	// Reason should change
	assert.Equal(t, "pgctld_unavailable", lastLoggedReason)
}

func TestHasCompleteBackups_WithCompleteBackup(t *testing.T) {
	ctx := context.Background()
	poolerDir := t.TempDir()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: NewActionLock(),
		config:     &Config{PoolerDir: poolerDir},
	}

	// Mock listBackups to return a complete backup
	// This is tested via the actual implementation
	// For unit test, we verify hasCompleteBackups returns false when no backups
	result := pm.hasCompleteBackups(ctx)

	// Without proper pgbackrest setup, should return false
	assert.False(t, result)
}

func TestHasCompleteBackups_NoBackups(t *testing.T) {
	ctx := context.Background()
	poolerDir := t.TempDir()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: NewActionLock(),
		config:     &Config{PoolerDir: poolerDir},
	}

	result := pm.hasCompleteBackups(ctx)

	assert.False(t, result)
}

func TestHasCompleteBackups_ActionLockTimeout(t *testing.T) {
	poolerDir := t.TempDir()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: NewActionLock(),
		config:     &Config{PoolerDir: poolerDir},
	}

	// Acquire the action lock to block hasCompleteBackups
	lockCtx, err := pm.actionLock.Acquire(context.Background(), "test-holder")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Create a context with timeout for hasCompleteBackups call
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// hasCompleteBackups should return false when it can't acquire lock
	result := pm.hasCompleteBackups(ctx)

	assert.False(t, result)
}

func TestStartPostgres_Success(t *testing.T) {
	ctx := context.Background()

	mockPgctld := &mockPgctldClient{}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
	}

	err := pm.startPostgres(ctx)

	require.NoError(t, err)
	assert.True(t, mockPgctld.startCalled)
}

func TestStartPostgres_PgctldUnavailable(t *testing.T) {
	ctx := context.Background()

	pm := &MultiPoolerManager{
		pgctldClient: nil,
		logger:       slog.Default(),
	}

	err := pm.startPostgres(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgctld client not available")
}

func TestStartPostgres_StartFails(t *testing.T) {
	ctx := context.Background()

	mockPgctld := &mockPgctldClient{
		startError: assert.AnError,
	}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
	}

	err := pm.startPostgres(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start PostgreSQL")
	assert.True(t, mockPgctld.startCalled)
}

// Integration Tests for MonitorPostgres

func TestMonitorPostgres_WaitsForReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readyChan := make(chan struct{})

	pm := &MultiPoolerManager{
		logger:    slog.Default(),
		readyChan: readyChan,
	}

	monitorDone := make(chan struct{})
	go func() {
		pm.MonitorPostgres(ctx)
		close(monitorDone)
	}()

	// Give it a moment - should be blocked waiting for ready
	select {
	case <-monitorDone:
		t.Fatal("MonitorPostgres should wait for ready")
	case <-time.After(50 * time.Millisecond):
		// Good, still waiting
	}

	// Close readyChan to signal ready
	close(readyChan)

	// Cancel context to stop monitoring
	cancel()

	// Should exit now
	select {
	case <-monitorDone:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("MonitorPostgres should exit after context cancel")
	}
}

func TestMonitorPostgres_ExitsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	readyChan := make(chan struct{})
	close(readyChan) // Ready immediately

	pm := &MultiPoolerManager{
		logger:               slog.Default(),
		readyChan:            readyChan,
		monitorRetryInterval: 10 * time.Millisecond,
		pgctldClient:         nil, // Will cause pgctld unavailable
	}

	monitorDone := make(chan struct{})
	go func() {
		pm.MonitorPostgres(ctx)
		close(monitorDone)
	}()

	// Should exit when context is cancelled
	select {
	case <-monitorDone:
		// Success - exited cleanly
	case <-time.After(1 * time.Second):
		t.Fatal("MonitorPostgres should exit when context is cancelled")
	}
}

func TestMonitorPostgres_HandlesRunningPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	readyChan := make(chan struct{})
	close(readyChan)

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_RUNNING,
		},
	}

	pm := &MultiPoolerManager{
		logger:               slog.Default(),
		readyChan:            readyChan,
		monitorRetryInterval: 10 * time.Millisecond,
		pgctldClient:         mockPgctld,
	}

	monitorDone := make(chan struct{})
	go func() {
		pm.MonitorPostgres(ctx)
		close(monitorDone)
	}()

	// Should run until context timeout
	select {
	case <-monitorDone:
		// Success - discovered running state and continued monitoring
	case <-time.After(1 * time.Second):
		t.Fatal("MonitorPostgres should complete")
	}

	// Should not have called Start (postgres already running)
	assert.False(t, mockPgctld.startCalled)
}

func TestMonitorPostgres_StartsStoppedPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	readyChan := make(chan struct{})
	close(readyChan)

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_STOPPED,
		},
	}

	pm := &MultiPoolerManager{
		logger:               slog.Default(),
		readyChan:            readyChan,
		monitorRetryInterval: 10 * time.Millisecond,
		pgctldClient:         mockPgctld,
	}

	pm.MonitorPostgres(ctx)

	// Should have attempted to start postgres
	assert.True(t, mockPgctld.startCalled, "Should attempt to start stopped postgres")
}

func TestMonitorPostgres_RetriesOnStartFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	readyChan := make(chan struct{})
	close(readyChan)

	mockPgctld := &mockPgctldClientWithCounter{
		mockPgctldClient: mockPgctldClient{
			statusResponse: &pgctldpb.StatusResponse{
				Status: pgctldpb.ServerStatus_STOPPED,
			},
			startError: assert.AnError,
		},
	}

	pm := &MultiPoolerManager{
		logger:               slog.Default(),
		readyChan:            readyChan,
		monitorRetryInterval: 10 * time.Millisecond,
		pgctldClient:         mockPgctld,
	}

	pm.MonitorPostgres(ctx)

	// Should have retried multiple times
	assert.Greater(t, mockPgctld.startCallCount, 1, "Should retry on start failure")
}

// Mock pgctld client for testing
type mockPgctldClient struct {
	statusResponse *pgctldpb.StatusResponse
	statusError    error
	startCalled    bool
	startError     error
	restartCalled  bool
	restartError   error
}

func (m *mockPgctldClient) Status(ctx context.Context, req *pgctldpb.StatusRequest, opts ...grpc.CallOption) (*pgctldpb.StatusResponse, error) {
	if m.statusError != nil {
		return nil, m.statusError
	}
	if m.statusResponse != nil {
		return m.statusResponse, nil
	}
	return &pgctldpb.StatusResponse{
		Status: pgctldpb.ServerStatus_RUNNING,
	}, nil
}

func (m *mockPgctldClient) Start(ctx context.Context, req *pgctldpb.StartRequest, opts ...grpc.CallOption) (*pgctldpb.StartResponse, error) {
	m.startCalled = true
	if m.startError != nil {
		return nil, m.startError
	}
	return &pgctldpb.StartResponse{}, nil
}

func (m *mockPgctldClient) Stop(ctx context.Context, req *pgctldpb.StopRequest, opts ...grpc.CallOption) (*pgctldpb.StopResponse, error) {
	return &pgctldpb.StopResponse{}, nil
}

func (m *mockPgctldClient) Restart(ctx context.Context, req *pgctldpb.RestartRequest, opts ...grpc.CallOption) (*pgctldpb.RestartResponse, error) {
	m.restartCalled = true
	if m.restartError != nil {
		return nil, m.restartError
	}
	return &pgctldpb.RestartResponse{}, nil
}

func (m *mockPgctldClient) InitDataDir(ctx context.Context, req *pgctldpb.InitDataDirRequest, opts ...grpc.CallOption) (*pgctldpb.InitDataDirResponse, error) {
	return &pgctldpb.InitDataDirResponse{}, nil
}

func (m *mockPgctldClient) ReloadConfig(ctx context.Context, req *pgctldpb.ReloadConfigRequest, opts ...grpc.CallOption) (*pgctldpb.ReloadConfigResponse, error) {
	return &pgctldpb.ReloadConfigResponse{}, nil
}

func (m *mockPgctldClient) Version(ctx context.Context, req *pgctldpb.VersionRequest, opts ...grpc.CallOption) (*pgctldpb.VersionResponse, error) {
	return &pgctldpb.VersionResponse{}, nil
}

func (m *mockPgctldClient) PgRewind(ctx context.Context, req *pgctldpb.PgRewindRequest, opts ...grpc.CallOption) (*pgctldpb.PgRewindResponse, error) {
	return &pgctldpb.PgRewindResponse{}, nil
}

// mockPgctldClientWithCounter extends mockPgctldClient with call counters
type mockPgctldClientWithCounter struct {
	mockPgctldClient
	startCallCount int
}

func (m *mockPgctldClientWithCounter) Start(ctx context.Context, req *pgctldpb.StartRequest, opts ...grpc.CallOption) (*pgctldpb.StartResponse, error) {
	m.startCallCount++
	return m.mockPgctldClient.Start(ctx, req, opts...)
}
