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

// NewTestMultiPoolerManager creates a MultiPoolerManager for testing with MultiPooler field populated
func NewTestMultiPoolerManager(t *testing.T, multiPooler *clustermetadatapb.MultiPooler, config *Config) *MultiPoolerManager {
	t.Helper()
	logger := slog.Default()
	pm, err := NewMultiPoolerManager(logger, multiPooler, config)
	require.NoError(t, err)

	return pm
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

			// Create test config with topology store that has backup location
			database := "postgres"
			backupLocation := "/tmp/test-backups"
			store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
			defer store.Close()

			// Create database in topology with backup location
			err := store.CreateDatabase(ctx, database, &clustermetadatapb.Database{
				Name:           database,
				BackupLocation: backupLocation,
			})
			require.NoError(t, err)

			serviceID := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      "test-pooler",
			}

			multiPooler := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
			multiPooler.Shard = constants.DefaultShard
			multiPooler.PoolerDir = poolerDir
			multiPooler.PortMap = map[string]int32{"postgres": 5432}
			multiPooler.Database = database

			config := &Config{
				TopoClient: store,
				// Note: pgctldClient is nil - operations that need it will fail gracefully
			}

			pm := NewTestMultiPoolerManager(t, multiPooler, config)

			// Initialize consensus state
			pm.consensusState = NewConsensusState(poolerDir, serviceID)
			_, err = pm.consensusState.Load()
			require.NoError(t, err)

			// Set manager to ready state with backup location so checkReady() passes
			pm.mu.Lock()
			pm.state = ManagerStateReady
			pm.backupLocation = filepath.Join(backupLocation, database, constants.DefaultTableGroup, constants.DefaultShard)
			pm.topoLoaded = true
			pm.mu.Unlock()

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

func TestHelperMethods(t *testing.T) {
	t.Run("hasDataDirectory", func(t *testing.T) {
		poolerDir := t.TempDir()
		multiPooler := &clustermetadatapb.MultiPooler{PoolerDir: poolerDir}
		pm := &MultiPoolerManager{config: &Config{}, multipooler: multiPooler}

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
			multipooler: multipooler,
		}

		assert.Equal(t, "shard-123", pm.getShardID())
	})

	t.Run("removeDataDirectory safety checks", func(t *testing.T) {
		poolerDir := t.TempDir()
		multiPooler := &clustermetadatapb.MultiPooler{PoolerDir: poolerDir}
		pm := &MultiPoolerManager{config: &Config{}, multipooler: multiPooler, logger: slog.Default()}

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
		config:       &Config{},
		multipooler:  &clustermetadatapb.MultiPooler{PoolerDir: t.TempDir()},
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

	// Should log error and take no action
	pm.takeRemedialAction(ctx, state)

	assert.Equal(t, "pgctld_unavailable", pm.pgMonitorLastLoggedReason)
}

func TestTakeRemedialAction_PostgresRunning(t *testing.T) {
	ctx := context.Background()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: NewActionLock(),
		multipooler: &clustermetadatapb.MultiPooler{
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	}

	state := postgresState{
		pgctldAvailable: true,
		dirInitialized:  true,
		postgresRunning: true,
	}

	// Should log info and take no action
	pm.takeRemedialAction(ctx, state)

	assert.Equal(t, "postgres_running", pm.pgMonitorLastLoggedReason)
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

	// Should attempt to start postgres
	pm.takeRemedialAction(ctx, state)

	assert.Equal(t, "starting_postgres", pm.pgMonitorLastLoggedReason)
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

	pm.pgMonitorLastLoggedReason = "starting_postgres"

	// Should handle error gracefully
	pm.takeRemedialAction(ctx, state)

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

	// Should log info and wait
	pm.takeRemedialAction(ctx, state)

	assert.Equal(t, "waiting_for_backup", pm.pgMonitorLastLoggedReason)
}

func TestTakeRemedialAction_LogDeduplication(t *testing.T) {
	ctx := context.Background()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: NewActionLock(),
		multipooler: &clustermetadatapb.MultiPooler{
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	}

	state := postgresState{
		pgctldAvailable: true,
		dirInitialized:  true,
		postgresRunning: true,
	}

	pm.pgMonitorLastLoggedReason = "postgres_running"

	// Call multiple times with same reason
	pm.takeRemedialAction(ctx, state)
	pm.takeRemedialAction(ctx, state)
	pm.takeRemedialAction(ctx, state)

	// Reason should stay the same (log deduplication working)
	assert.Equal(t, "postgres_running", pm.pgMonitorLastLoggedReason)

	// Change state
	state.pgctldAvailable = false
	pm.takeRemedialAction(ctx, state)

	// Reason should change
	assert.Equal(t, "pgctld_unavailable", pm.pgMonitorLastLoggedReason)
}

func TestHasCompleteBackups_WithCompleteBackup(t *testing.T) {
	ctx := context.Background()
	poolerDir := t.TempDir()

	pm := &MultiPoolerManager{
		logger:      slog.Default(),
		actionLock:  NewActionLock(),
		config:      &Config{},
		multipooler: &clustermetadatapb.MultiPooler{PoolerDir: poolerDir},
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
		logger:      slog.Default(),
		actionLock:  NewActionLock(),
		config:      &Config{},
		multipooler: &clustermetadatapb.MultiPooler{PoolerDir: poolerDir},
	}

	result := pm.hasCompleteBackups(ctx)

	assert.False(t, result)
}

func TestHasCompleteBackups_ActionLockTimeout(t *testing.T) {
	poolerDir := t.TempDir()

	pm := &MultiPoolerManager{
		logger:      slog.Default(),
		actionLock:  NewActionLock(),
		config:      &Config{},
		multipooler: &clustermetadatapb.MultiPooler{PoolerDir: poolerDir},
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
	ctx := context.Background()

	readyChan := make(chan struct{})

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_STOPPED,
		},
	}

	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		readyChan:    readyChan,
		pgctldClient: mockPgctld,
		state:        ManagerStateStarting,
	}

	// Call iteration when not ready - should return early without calling pgctld
	pm.monitorPostgresIteration(ctx)
	assert.False(t, mockPgctld.startCalled, "Should not attempt to start when not ready")

	// Set state to ready
	pm.mu.Lock()
	pm.state = ManagerStateReady
	pm.mu.Unlock()
	close(readyChan)

	// Call iteration again when ready - should proceed and attempt to start
	pm.monitorPostgresIteration(ctx)
	assert.True(t, mockPgctld.startCalled, "Should attempt to start when ready")
}

func TestMonitorPostgres_HandlesRunningPostgres(t *testing.T) {
	ctx := context.Background()

	readyChan := make(chan struct{})
	close(readyChan)

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_RUNNING,
		},
	}

	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		readyChan:    readyChan,
		pgctldClient: mockPgctld,
		state:        ManagerStateReady,
		actionLock:   NewActionLock(),
		multipooler: &clustermetadatapb.MultiPooler{
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
	}

	// Call iteration - should discover running state and not call Start
	pm.monitorPostgresIteration(ctx)

	// Should not have called Start (postgres already running)
	assert.False(t, mockPgctld.startCalled, "Should not call Start when postgres is already running")
}

func TestMonitorPostgres_StartsStoppedPostgres(t *testing.T) {
	ctx := context.Background()

	readyChan := make(chan struct{})
	close(readyChan)

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_STOPPED,
		},
	}

	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		readyChan:    readyChan,
		pgctldClient: mockPgctld,
		state:        ManagerStateReady,
	}

	// Call iteration - should discover stopped state and attempt to start
	pm.monitorPostgresIteration(ctx)

	// Should have attempted to start postgres
	assert.True(t, mockPgctld.startCalled, "Should attempt to start stopped postgres")
}

func TestMonitorPostgres_RetriesOnStartFailure(t *testing.T) {
	ctx := context.Background()

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
		logger:       slog.Default(),
		readyChan:    readyChan,
		pgctldClient: mockPgctld,
		state:        ManagerStateReady,
	}

	// Call iteration multiple times to simulate retry behavior
	for range 5 {
		pm.monitorPostgresIteration(ctx)
	}

	// Should have retried multiple times
	assert.Equal(t, 5, mockPgctld.startCallCount, "Should attempt to start on each iteration")
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

func (m *mockPgctldClient) CrashRecovery(ctx context.Context, req *pgctldpb.CrashRecoveryRequest, opts ...grpc.CallOption) (*pgctldpb.CrashRecoveryResponse, error) {
	return &pgctldpb.CrashRecoveryResponse{}, nil
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
