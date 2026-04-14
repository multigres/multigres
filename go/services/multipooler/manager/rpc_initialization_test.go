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
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/test/utils"
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
		expectSuccess bool // true: must succeed (nil err, Success=true); false: must fail
		errorContains string
	}{
		{
			// The marker file exists → isInitialized() returns true → early return.
			// pgctld is never called, so this succeeds without real postgres.
			name: "idempotent - already has a backup",
			setupFunc: func(t *testing.T, pm *MultiPoolerManager, poolerDir string) {
				dataDir := filepath.Join(poolerDir, "pg_data")
				markerDir := filepath.Join(dataDir, constants.MultigresMarkerDirectory)
				require.NoError(t, os.MkdirAll(markerDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16"), 0o644))
				require.NoError(t, os.WriteFile(filepath.Join(markerDir, multigresInitMarker), []byte("initialized\n"), 0o644))
			},
			term:          1,
			expectSuccess: true,
		},
		{
			// Term validation happens before any I/O, so pgctld is never called.
			name:          "rejects invalid consensus term",
			term:          2,
			expectSuccess: false,
			errorContains: "consensus term must be 1",
		},
		{
			// A fresh (uninitialized) pooler passes term and idempotency checks,
			// then reaches the first pgctld call (InitDataDir). Unit tests stop
			// here because real pgctld/postgres are not available; the full success
			// path is covered by the integration test.
			name:          "initialize fresh pooler",
			term:          1,
			expectSuccess: false,
			errorContains: "pgctld",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			poolerDir := t.TempDir()
			t.Setenv(constants.PgDataDirEnvVar, filepath.Join(poolerDir, "pg_data"))

			// Create test config with topology store that has backup location
			database := "postgres"
			backupLocation := "/tmp/test-backups"
			store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
			defer store.Close()

			// Create database in topology with backup location
			err := store.CreateDatabase(ctx, database, &clustermetadatapb.Database{
				Name:           database,
				BackupLocation: utils.FilesystemBackupLocation(backupLocation),
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

			pm := NewTestMultiPoolerManager(t, multiPooler, &Config{TopoClient: store})

			pm.consensusState = NewConsensusState(poolerDir, serviceID)
			_, err = pm.consensusState.Load()
			require.NoError(t, err)

			backupConfig, err := backup.NewConfig(utils.FilesystemBackupLocation(backupLocation))
			require.NoError(t, err)

			pm.mu.Lock()
			pm.state = ManagerStateReady
			pm.backupConfig = backupConfig
			pm.topoLoaded = true
			pm.mu.Unlock()

			if tt.setupFunc != nil {
				tt.setupFunc(t, pm, poolerDir)
			}

			resp, err := pm.InitializeEmptyPrimary(ctx, &multipoolermanagerdatapb.InitializeEmptyPrimaryRequest{
				ConsensusTerm: tt.term,
			})

			if tt.expectSuccess {
				require.NoError(t, err)
				require.NotNil(t, resp)
				assert.True(t, resp.Success)
			} else {
				require.Error(t, err)
				assert.Nil(t, resp)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			}
		})
	}
}

// TestIsInitialized verifies that isInitialized requires both the marker file and
// multigres schema to be present. The marker file (MULTIGRES_INITIALIZED) is written
// by setInitialized() only after the full bootstrap sequence completes:
//   - Primary: initdb + multigres schema + pgBackRest stanza-create + backup
//   - Replica:  restore from canonical backup + postgres started
//
// The marker file prevents false positives on crash-restart between schema creation
// and backup completion. The schema check is a safety invariant: schema existence is
// necessary (though not sufficient) for a node to be initialized.
func TestIsInitialized(t *testing.T) {
	t.Run("returns false when no data directory exists", func(t *testing.T) {
		ctx := t.Context()
		poolerDir := t.TempDir()
		pm := &MultiPoolerManager{
			config:      &Config{},
			multipooler: &clustermetadatapb.MultiPooler{PoolerDir: poolerDir},
		}

		assert.False(t, pm.isInitialized(ctx))
	})

	t.Run("returns false when data directory exists but marker file is absent", func(t *testing.T) {
		ctx := t.Context()
		poolerDir := t.TempDir()
		dataDir := filepath.Join(poolerDir, "pg_data")
		require.NoError(t, os.MkdirAll(dataDir, 0o755))
		// Simulate postgres having run initdb and created the multigres schema,
		// but the full bootstrap sequence (backup) did not complete: no marker file.
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16"), 0o644))

		pm := &MultiPoolerManager{
			config:      &Config{},
			multipooler: &clustermetadatapb.MultiPooler{PoolerDir: poolerDir},
		}

		assert.False(t, pm.isInitialized(ctx))
		// Cached state must not be poisoned.
		assert.False(t, pm.initialized)
	})

	t.Run("returns true when marker file is present but postgres is unreachable; cache not set", func(t *testing.T) {
		ctx := t.Context()
		poolerDir := t.TempDir()
		dataDir := filepath.Join(poolerDir, "pg_data")
		markerDir := filepath.Join(dataDir, constants.MultigresMarkerDirectory)
		require.NoError(t, os.MkdirAll(markerDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(markerDir, multigresInitMarker), []byte("initialized\n"), 0o644))
		t.Setenv(constants.PgDataDirEnvVar, dataDir)

		pm := &MultiPoolerManager{
			config:      &Config{},
			multipooler: &clustermetadatapb.MultiPooler{PoolerDir: poolerDir},
		}

		// Marker present, postgres unreachable → trust marker, return true.
		assert.True(t, pm.isInitialized(ctx))
		// Cache is NOT set since we could not confirm the schema; re-check on next call.
		assert.False(t, pm.initialized)
	})

	t.Run("fast path: returns true when initialized is already cached", func(t *testing.T) {
		poolerDir := t.TempDir()
		// No data directory at all, but in-memory cache is true.
		pm := &MultiPoolerManager{
			config:      &Config{},
			multipooler: &clustermetadatapb.MultiPooler{PoolerDir: poolerDir},
			initialized: true,
		}

		assert.True(t, pm.isInitialized(t.Context()))
	})
}

func TestHelperMethods(t *testing.T) {
	t.Run("hasDataDirectory", func(t *testing.T) {
		poolerDir := t.TempDir()
		dataDir := filepath.Join(poolerDir, "pg_data")
		t.Setenv(constants.PgDataDirEnvVar, dataDir)

		multiPooler := &clustermetadatapb.MultiPooler{PoolerDir: poolerDir}
		pm := &MultiPoolerManager{config: &Config{}, multipooler: multiPooler}

		// Initially no data directory
		assert.False(t, pm.hasDataDirectory())

		// Create data directory with PG_VERSION file (simulating initialized postgres)
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
		t.Setenv(constants.PgDataDirEnvVar, dataDir)

		// Should succeed with valid directory
		err := pm.removeDataDirectory()
		require.NoError(t, err)

		// Verify directory was removed
		_, err = os.Stat(dataDir)
		assert.True(t, os.IsNotExist(err))
	})
}

// TestInitializeEmptyPrimary_EventPoolerName verifies that primary.init events emitted
// by MultiPoolerManager include the pooler_name attribute from the logger (set via
// logger.With in NewMultiPoolerManager), not as an explicit struct field.
func TestInitializeEmptyPrimary_EventPoolerName(t *testing.T) {
	ctx := t.Context()
	poolerDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, filepath.Join(poolerDir, "pg_data"))

	database := "postgres"
	backupLocation := t.TempDir()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()

	err := store.CreateDatabase(ctx, database, &clustermetadatapb.Database{
		Name:           database,
		BackupLocation: utils.FilesystemBackupLocation(backupLocation),
	})
	require.NoError(t, err)

	multiPooler := topoclient.NewMultiPooler("pooler-7", "test-cell", "localhost", constants.DefaultTableGroup)
	multiPooler.Shard = constants.DefaultShard
	multiPooler.PoolerDir = poolerDir
	multiPooler.PortMap = map[string]int32{"postgres": 5432}
	multiPooler.Database = database

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	pm, err := NewMultiPoolerManager(logger, multiPooler, &Config{TopoClient: store})
	require.NoError(t, err)

	serviceID := multiPooler.Id
	pm.consensusState = NewConsensusState(poolerDir, serviceID)
	_, err = pm.consensusState.Load()
	require.NoError(t, err)

	backupConfig, err := backup.NewConfig(utils.FilesystemBackupLocation(backupLocation))
	require.NoError(t, err)

	pm.mu.Lock()
	pm.state = ManagerStateReady
	pm.backupConfig = backupConfig
	pm.topoLoaded = true
	pm.mu.Unlock()

	// InitializeEmptyPrimary will fail (no pgctld client), but both Started and Failed
	// primary.init events are emitted before the pgctld call and via the defer, respectively.
	_, _ = pm.InitializeEmptyPrimary(ctx, &multipoolermanagerdatapb.InitializeEmptyPrimaryRequest{
		ConsensusTerm: 1,
	})

	var foundEvents int
	dec := json.NewDecoder(&buf)
	for dec.More() {
		var m map[string]any
		require.NoError(t, dec.Decode(&m))
		if m["msg"] != "multigres.event" || m["event_type"] != "primary.init" {
			continue
		}
		assert.Equal(t, "pooler-7", m["pooler_name"], "primary.init event must carry pooler_name from logger")
		foundEvents++
	}
	assert.Equal(t, 2, foundEvents, "expected started and failed primary.init events")
}

// MonitorPostgres Tests

func TestDiscoverPostgresState_PgctldUnavailable(t *testing.T) {
	ctx := t.Context()
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
	ctx := t.Context()

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
	ctx := t.Context()

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
	ctx := t.Context()

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
	ctx := t.Context()

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

// TestDetermineRemedialAction tests the decision logic that maps discovered state to remedial actions.
// This is a table-driven test covering all decision paths in the monitor loop.
func TestDetermineRemedialAction(t *testing.T) {
	tests := []struct {
		name           string
		state          postgresState
		poolerType     clustermetadatapb.PoolerType
		expectedAction remedialAction
	}{
		{
			name:           "pgctld_unavailable",
			state:          postgresState{pgctldAvailable: false},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			expectedAction: remedialActionNone,
		},
		{
			name: "postgres_ready_type_matches_primary",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				isPrimary:       true,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			expectedAction: remedialActionNone,
		},
		{
			name: "postgres_ready_promote_to_primary",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				isPrimary:       true,
			},
			poolerType:     clustermetadatapb.PoolerType_REPLICA,
			expectedAction: remedialActionAdjustTypeToPrimary,
		},
		{
			name: "postgres_ready_demote_to_replica",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				isPrimary:       false,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			expectedAction: remedialActionAdjustTypeToReplica,
		},
		{
			name: "postgres_ready_type_matches_replica",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				isPrimary:       false,
			},
			poolerType:     clustermetadatapb.PoolerType_REPLICA,
			expectedAction: remedialActionNone,
		},
		{
			name: "postgres_stopped_start",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: false,
				dirInitialized:  true,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			expectedAction: remedialActionStartPostgres,
		},
		{
			name: "postgres_stopped_restore",
			state: postgresState{
				pgctldAvailable:  true,
				postgresRunning:  false,
				dirInitialized:   false,
				backupsAvailable: true,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			expectedAction: remedialActionRestoreFromBackup,
		},
		{
			name: "postgres_stopped_wait_for_backup",
			state: postgresState{
				pgctldAvailable:  true,
				postgresRunning:  false,
				dirInitialized:   false,
				backupsAvailable: false,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			expectedAction: remedialActionNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := &MultiPoolerManager{
				multipooler: &clustermetadatapb.MultiPooler{
					Type: tt.poolerType,
				},
			}

			got := pm.determineRemedialAction(tt.state)
			require.Equal(t, tt.expectedAction, got)
		})
	}
}

func TestTakeRemedialAction_PgctldUnavailable(t *testing.T) {
	ctx := t.Context()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: NewActionLock(),
	}

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Should log error and take no action
	pm.takeRemedialAction(lockCtx, remedialActionNone)

	// Note: takeRemedialAction with remedialActionNone doesn't log
	assert.Equal(t, "", pm.pgMonitorLastLoggedReason)
}

func TestTakeRemedialAction_PostgresReady(t *testing.T) {
	ctx := t.Context()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: NewActionLock(),
		multipooler: &clustermetadatapb.MultiPooler{
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	}

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Should log info and take no action (no type mismatch)
	pm.takeRemedialAction(lockCtx, remedialActionNone)

	// Note: takeRemedialAction with remedialActionNone doesn't log
	assert.Equal(t, "", pm.pgMonitorLastLoggedReason)
}

func TestTakeRemedialAction_StartPostgres(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
		actionLock:   NewActionLock(),
	}

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Should attempt to start postgres
	pm.takeRemedialAction(lockCtx, remedialActionStartPostgres)

	assert.Equal(t, "starting_postgres", pm.pgMonitorLastLoggedReason)
	assert.True(t, mockPgctld.startCalled, "Should have called Start()")
}

func TestTakeRemedialAction_StartPostgresFails(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{
		startError: assert.AnError,
	}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
		actionLock:   NewActionLock(),
	}

	pm.pgMonitorLastLoggedReason = "starting_postgres"

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Should handle error gracefully
	pm.takeRemedialAction(lockCtx, remedialActionStartPostgres)

	assert.True(t, mockPgctld.startCalled, "Should have attempted to call Start()")
	// Reason stays the same since we're retrying
}

func TestTakeRemedialAction_WaitingForBackup(t *testing.T) {
	ctx := t.Context()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: NewActionLock(),
	}

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// With no backups and uninitialized dir, action is None - doesn't do anything
	pm.takeRemedialAction(lockCtx, remedialActionNone)

	// takeRemedialAction with None action doesn't modify last logged reason
	assert.Equal(t, "", pm.pgMonitorLastLoggedReason)
}

func TestTakeRemedialAction_LogDeduplication(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{}

	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		actionLock:   NewActionLock(),
		pgctldClient: mockPgctld,
		multipooler: &clustermetadatapb.MultiPooler{
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	}

	pm.pgMonitorLastLoggedReason = "starting_postgres"

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Call multiple times with same action - reason should stay the same (log deduplication)
	pm.takeRemedialAction(lockCtx, remedialActionStartPostgres)
	assert.Equal(t, "starting_postgres", pm.pgMonitorLastLoggedReason)

	pm.takeRemedialAction(lockCtx, remedialActionStartPostgres)
	assert.Equal(t, "starting_postgres", pm.pgMonitorLastLoggedReason)

	pm.takeRemedialAction(lockCtx, remedialActionStartPostgres)
	assert.Equal(t, "starting_postgres", pm.pgMonitorLastLoggedReason)

	// Change action type - reason should change
	pm.takeRemedialAction(lockCtx, remedialActionRestoreFromBackup)
	assert.Equal(t, "restoring_from_backup", pm.pgMonitorLastLoggedReason)
}

// Note: Type adjustment action execution (AdjustTypeToPrimary, AdjustTypeToReplica) is tested in
// integration tests because it requires topoClient and full infrastructure.
// The decision logic for type adjustment is tested in TestDetermineRemedialAction above.
// The resignation signal behavior is tested below without full infrastructure.

func newRemedialActionTestManager(t *testing.T, multipooler *clustermetadatapb.MultiPooler) *MultiPoolerManager {
	t.Helper()
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))
	return &MultiPoolerManager{
		logger:       slog.Default(),
		actionLock:   NewActionLock(),
		multipooler:  multipooler,
		serviceID:    multipooler.Id,
		topoClient:   ts,
		servingState: NewStateManager(slog.Default(), multipooler),
	}
}

func TestTakeRemedialAction_ResignationSignal(t *testing.T) {
	tests := []struct {
		name           string
		action         remedialAction
		poolerType     clustermetadatapb.PoolerType
		primaryTerm    int64 // set in consensus state before action
		resignedBefore int64 // set resignedPrimaryAtTerm before action (0 = don't set)
		wantAvStatus   *clustermetadatapb.AvailabilityStatus
	}{
		{
			name:        "AdjustTypeToReplica sets resignation at primary_term",
			action:      remedialActionAdjustTypeToReplica,
			poolerType:  clustermetadatapb.PoolerType_PRIMARY,
			primaryTerm: 5,
			wantAvStatus: &clustermetadatapb.AvailabilityStatus{
				LeadershipStatus: &clustermetadatapb.LeadershipStatus{
					PrimaryTerm: 5,
					Signal:      clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
				},
			},
		},
		{
			name:         "AdjustTypeToReplica sets no resignation when primary_term is zero",
			action:       remedialActionAdjustTypeToReplica,
			poolerType:   clustermetadatapb.PoolerType_PRIMARY,
			primaryTerm:  0,
			wantAvStatus: nil,
		},
		{
			name:           "AdjustTypeToPrimary does not clear existing resignation signal",
			action:         remedialActionAdjustTypeToPrimary,
			poolerType:     clustermetadatapb.PoolerType_REPLICA,
			resignedBefore: 7,
			wantAvStatus: &clustermetadatapb.AvailabilityStatus{
				LeadershipStatus: &clustermetadatapb.LeadershipStatus{
					PrimaryTerm: 7,
					Signal:      clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			multipooler := &clustermetadatapb.MultiPooler{
				Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"},
				Type: tc.poolerType,
			}
			pm := newRemedialActionTestManager(t, multipooler)

			cs := NewConsensusState("", nil)
			cs.mu.Lock()
			cs.term = &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 1, PrimaryTerm: tc.primaryTerm}
			cs.mu.Unlock()
			pm.consensusState = cs

			if tc.resignedBefore != 0 {
				pm.setResignedPrimaryAtTerm(tc.resignedBefore)
			}

			lockCtx, err := pm.actionLock.Acquire(ctx, "test")
			require.NoError(t, err)
			defer pm.actionLock.Release(lockCtx)

			pm.takeRemedialAction(lockCtx, tc.action)

			assert.Equal(t, tc.wantAvStatus, pm.buildAvailabilityStatus())
		})
	}
}

func TestHasCompleteBackups_WithCompleteBackup(t *testing.T) {
	ctx := t.Context()
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
	ctx := t.Context()
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
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-holder")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Create a context with timeout for hasCompleteBackups call
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	// hasCompleteBackups should return false when it can't acquire lock
	result := pm.hasCompleteBackups(ctx)

	assert.False(t, result)
}

func TestStartPostgres_Success(t *testing.T) {
	ctx := t.Context()

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
	ctx := t.Context()

	pm := &MultiPoolerManager{
		pgctldClient: nil,
		logger:       slog.Default(),
	}

	err := pm.startPostgres(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgctld client not available")
}

func TestStartPostgres_StartFails(t *testing.T) {
	ctx := t.Context()

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
	ctx := t.Context()

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
		actionLock:   NewActionLock(),
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
	ctx := t.Context()

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
	ctx := t.Context()

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
		actionLock:   NewActionLock(),
	}

	// Call iteration - should discover stopped state and attempt to start
	pm.monitorPostgresIteration(ctx)

	// Should have attempted to start postgres
	assert.True(t, mockPgctld.startCalled, "Should attempt to start stopped postgres")
}

func TestMonitorPostgres_RetriesOnStartFailure(t *testing.T) {
	ctx := t.Context()

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
		actionLock:   NewActionLock(),
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

// mockPgctldClientWithCounter extends mockPgctldClient with call counters
type mockPgctldClientWithCounter struct {
	mockPgctldClient
	startCallCount int
}

func (m *mockPgctldClientWithCounter) Start(ctx context.Context, req *pgctldpb.StartRequest, opts ...grpc.CallOption) (*pgctldpb.StartResponse, error) {
	m.startCallCount++
	return m.mockPgctldClient.Start(ctx, req, opts...)
}
