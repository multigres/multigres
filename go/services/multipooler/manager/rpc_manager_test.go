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
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/executor/mock"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// setTermForTest writes the consensus term file directly for testing.
func setTermForTest(t *testing.T, poolerDir string, term *clustermetadatapb.TermRevocation) {
	t.Helper()
	cs := NewConsensusState(poolerDir, nil)
	require.NoError(t, cs.setRevocation(term), "failed to write term file")
}

// addDatabaseToTopo creates a database in the topology with a backup location
func addDatabaseToTopo(t *testing.T, ts topoclient.Store, database string) {
	t.Helper()
	ctx := context.Background()
	err := ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
		Name:           database,
		BackupLocation: utils.FilesystemBackupLocation("/var/backups/pgbackrest"),
	})
	require.NoError(t, err)
}

func TestPrimaryPosition(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	tests := []struct {
		name          string
		poolerType    clustermetadatapb.PoolerType
		expectError   bool
		expectedCode  mtrpcpb.Code
		errorContains string
	}{
		{
			name:          "REPLICA pooler returns FAILED_PRECONDITION",
			poolerType:    clustermetadatapb.PoolerType_REPLICA,
			expectError:   true,
			expectedCode:  mtrpcpb.Code_FAILED_PRECONDITION,
			errorContains: "standby mode",
		},
		{
			name:          "PRIMARY pooler passes type check",
			poolerType:    clustermetadatapb.PoolerType_PRIMARY,
			expectError:   true,
			errorContains: "failed to get current WAL LSN", // Will fail on WAL LSN query, not type check
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
			defer ts.Close()

			// Create temp directory for pooler-dir
			poolerDir := t.TempDir()
			createPgDataDir(t, poolerDir)

			// Create the database in topology with backup location
			database := "testdb"
			addDatabaseToTopo(t, ts, database)

			multipooler := &clustermetadatapb.MultiPooler{
				Id:            serviceID,
				Hostname:      "localhost",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          tt.poolerType,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   database,
					TableGroup: constants.DefaultTableGroup,
					Shard:      constants.DefaultShard,
				},
			}
			require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

			multipooler.PoolerDir = poolerDir

			config := &Config{
				TopoClient: ts,
			}
			manager, err := NewMultiPoolerManager(logger, multipooler, config)
			require.NoError(t, err)
			defer manager.Shutdown()

			// Set up mock query service for isInRecovery checks during test
			mockQueryService := mock.NewQueryService()
			isReplica := tt.poolerType == clustermetadatapb.PoolerType_REPLICA
			mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{isReplica}}))
			manager.qsc = &mockPoolerController{queryService: mockQueryService}
			manager.rules = newRuleStore(logger, mockQueryService, noopSyncStandbyManager{})

			// Mark as initialized to skip auto-restore (not testing backup functionality)
			err = manager.setInitialized()
			require.NoError(t, err)

			// Start and wait for ready
			senv := servenv.NewServEnv(viperutil.NewRegistry())
			go manager.Start(senv)
			require.Eventually(t, func() bool {
				return manager.GetState() == ManagerStateReady
			}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

			// Call PrimaryPosition
			_, err = manager.PrimaryPosition(ctx)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)

				if tt.expectedCode != 0 {
					code := mterrors.Code(err)
					assert.Equal(t, tt.expectedCode, code)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestActionLock_MutationMethodsTimeout(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerDir := t.TempDir()

	// Create the database in topology with backup location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	// Create PRIMARY multipooler for testing
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   database,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	multipooler.PoolerDir = poolerDir

	config := &Config{
		TopoClient: ts,
	}
	manager, err := NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	defer manager.Shutdown()

	// Set up mock query service for isInRecovery check during startup
	mockQueryService := mock.NewQueryService()
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))
	manager.qsc = &mockPoolerController{queryService: mockQueryService}
	manager.rules = newRuleStore(logger, mockQueryService, noopSyncStandbyManager{})

	// Start and wait for ready
	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go manager.Start(senv)
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	// Helper function to hold the lock for a duration
	holdLock := func(duration time.Duration) context.CancelFunc {
		lockCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(duration))
		lockAcquired := make(chan struct{})
		go func() {
			newCtx, err := manager.actionLock.Acquire(lockCtx, "test-lock-holder")
			if err == nil {
				// Signal that the lock was acquired
				close(lockAcquired)
				// Hold the lock for the duration or until cancelled
				<-lockCtx.Done()
				manager.actionLock.Release(newCtx)
			}
		}()
		// Wait for the lock to be acquired
		<-lockAcquired
		return cancel
	}

	tests := []struct {
		name       string
		poolerType clustermetadatapb.PoolerType
		callMethod func(context.Context) error
	}{
		{
			name:       "StartReplication times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_REPLICA,
			callMethod: func(ctx context.Context) error {
				return manager.StartReplication(ctx)
			},
		},
		{
			name:       "StopReplication times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_REPLICA,
			callMethod: func(ctx context.Context) error {
				return manager.StopReplication(ctx, multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY, true /* wait */)
			},
		},
		{
			name:       "ResetReplication times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_REPLICA,
			callMethod: func(ctx context.Context) error {
				return manager.ResetReplication(ctx)
			},
		},
		{
			name:       "ChangeType times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				return manager.ChangeType(ctx, "REPLICA")
			},
		},
		{
			name:       "UndoDemote times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				return manager.UndoDemote(ctx)
			},
		},
		{
			name:       "UpdateConsensusRule times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				return manager.UpdateConsensusRule(ctx, multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD, []*clustermetadatapb.ID{serviceID}, &clustermetadatapb.RuleNumber{}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Update the pooler type if needed for this test
			if tt.poolerType != multipooler.Type {
				updatedMultipooler, err := ts.UpdateMultiPoolerFields(ctx, serviceID, func(mp *clustermetadatapb.MultiPooler) error {
					mp.Type = tt.poolerType
					return nil
				})
				require.NoError(t, err)
				manager.mu.Lock()
				manager.multipooler = updatedMultipooler
				manager.mu.Unlock()
			}

			// Hold the lock for 2 seconds
			cancel := holdLock(2 * time.Second)
			defer cancel()

			// Try to call the method - it should timeout because lock is held
			err := tt.callMethod(utils.WithTimeout(t, 500*time.Millisecond))

			// Verify the error is a timeout/context error
			require.Error(t, err, "Method should fail when lock is held")
			assert.Contains(t, err.Error(), "failed to acquire action lock", "Error should mention lock acquisition failure")

			// Verify the underlying error is context deadline exceeded
			assert.ErrorIs(t, err, context.DeadlineExceeded, "Should be a deadline exceeded error")
		})
	}
}

// createPgDataDir creates the pg_data directory with PG_VERSION file.
// This is needed for setInitialized() to work since it writes a marker file to pg_data.
func createPgDataDir(t *testing.T, poolerDir string) {
	t.Helper()
	pgDataDir := filepath.Join(poolerDir, "pg_data")
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, "PG_VERSION"), []byte("16"), 0o644))
	t.Setenv(constants.PgDataDirEnvVar, pgDataDir)
}

func TestReplicationStatus(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	t.Run("PRIMARY_pooler_returns_primary_status", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
		t.Cleanup(cleanupPgctld)

		// Create the database in topology with backup location
		database := "testdb"
		addDatabaseToTopo(t, ts, database)

		// Create PRIMARY multipooler
		multipooler := &clustermetadatapb.MultiPooler{
			Id:            serviceID,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   database,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}
		require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

		tmpDir := t.TempDir()
		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Shutdown() })

		// Create mock query service and inject it
		mockQueryService := mock.NewQueryService()

		// Status() calls isInRecovery() to determine role
		// pg_is_in_recovery returns false (not in recovery = primary)
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
		// getPrimaryLSN()
		mockQueryService.AddQueryPattern("SELECT pg_current_wal_lsn",
			mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/12345678"}}))
		// getConnectedFollowerIDs()
		mockQueryService.AddQueryPattern("SELECT application_name",
			mock.MakeQueryResult([]string{"application_name"}, nil))
		// getSynchronousReplicationConfig()
		mockQueryService.AddQueryPattern("SHOW synchronous_standby_names",
			mock.MakeQueryResult([]string{"synchronous_standby_names"}, [][]any{{""}}))
		mockQueryService.AddQueryPattern("SHOW synchronous_commit",
			mock.MakeQueryResult([]string{"synchronous_commit"}, [][]any{{"on"}}))

		pm.qsc = &mockPoolerController{queryService: mockQueryService}
		pm.rules = newRuleStore(logger, mockQueryService, noopSyncStandbyManager{})
		pm.rules = &fakeRuleStore{}

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Call ReplicationStatus
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// Verify response structure
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.Status.PoolerType)
		assert.NotNil(t, status.Status.PrimaryStatus, "PrimaryStatus should be populated")
		assert.Nil(t, status.Status.ReplicationStatus, "ReplicationStatus should be nil for PRIMARY")
		assert.Equal(t, "0/12345678", status.Status.PrimaryStatus.Lsn)
	})

	t.Run("REPLICA_pooler_returns_replication_status", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
		t.Cleanup(cleanupPgctld)

		// Create the database in topology with backup location
		database := "testdb"
		addDatabaseToTopo(t, ts, database)

		// Create REPLICA multipooler
		multipooler := &clustermetadatapb.MultiPooler{
			Id:            serviceID,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_REPLICA,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   database,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}
		require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

		tmpDir := t.TempDir()
		createPgDataDir(t, tmpDir)

		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Shutdown() })
		// Mark as initialized to skip auto-restore (not testing backup functionality)
		err = pm.setInitialized()
		require.NoError(t, err)

		// Create mock query service and inject it
		mockQueryService := mock.NewQueryService()

		// Status() calls isInRecovery() - returns true (in recovery = standby)
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
		// getStandbyReplayLSN()
		mockQueryService.AddQueryPattern("SELECT pg_last_wal_replay_lsn",
			mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn"}, [][]any{{"0/12345600"}}))
		// queryReplicationStatus()
		mockQueryService.AddQueryPattern("pg_last_wal_receive_lsn",
			mock.MakeQueryResult(
				[]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"primary_conninfo",
					"wal_receiver_status",
					"last_msg_receive_time",
					"wal_receiver_status_interval",
					"wal_receiver_timeout",
				},
				[][]any{{"0/12345600", "0/12345678", "f", "not paused", "2025-01-01 00:00:00", "host=primary port=5432 user=repl application_name=test", "streaming", nil, nil, nil}}))

		pm.qsc = &mockPoolerController{queryService: mockQueryService}
		pm.rules = newRuleStore(logger, mockQueryService, noopSyncStandbyManager{})
		pm.rules = &fakeRuleStore{}

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Call ReplicationStatus
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// Verify response structure
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.Status.PoolerType)
		assert.Nil(t, status.Status.PrimaryStatus, "PrimaryStatus should be nil for REPLICA")
		assert.NotNil(t, status.Status.ReplicationStatus, "ReplicationStatus should be populated")
		assert.Equal(t, "0/12345600", status.Status.ReplicationStatus.LastReplayLsn)
	})

	t.Run("Mismatch_PRIMARY_topology_but_standby_postgres", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
		t.Cleanup(cleanupPgctld)

		// Create the database in topology with backup location
		database := "testdb"
		addDatabaseToTopo(t, ts, database)

		// Create PRIMARY multipooler (but PG will be in standby mode - mismatch!)
		multipooler := &clustermetadatapb.MultiPooler{
			Id:            serviceID,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   database,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}
		require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

		tmpDir := t.TempDir()
		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Shutdown() })

		// Create mock query service and inject it
		mockQueryService := mock.NewQueryService()

		// PostgreSQL is actually a standby (pg_is_in_recovery = true)
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
		// getStandbyReplayLSN()
		mockQueryService.AddQueryPattern("SELECT pg_last_wal_replay_lsn",
			mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn"}, [][]any{{"0/12345600"}}))
		// queryReplicationStatus()
		mockQueryService.AddQueryPattern("pg_last_wal_receive_lsn",
			mock.MakeQueryResult(
				[]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"primary_conninfo",
					"wal_receiver_status",
					"last_msg_receive_time",
					"wal_receiver_status_interval",
					"wal_receiver_timeout",
				},
				[][]any{{"0/12345600", "0/12345678", "f", "not paused", "2025-01-01 00:00:00", "host=primary port=5432 user=repl application_name=test", "streaming", nil, nil, nil}}))

		pm.qsc = &mockPoolerController{queryService: mockQueryService}
		pm.rules = newRuleStore(logger, mockQueryService, noopSyncStandbyManager{})
		pm.rules = &fakeRuleStore{}

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Call Status - now returns status with mismatch observable
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// PoolerType from topology says PRIMARY, but status shows standby state
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.Status.PoolerType)
		assert.Nil(t, status.Status.PrimaryStatus, "PrimaryStatus should be nil since PostgreSQL is a standby")
		assert.NotNil(t, status.Status.ReplicationStatus, "ReplicationStatus should be populated since PostgreSQL is a standby")
	})

	t.Run("Status_returns_cohort_members_from_leadership_history", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
		t.Cleanup(cleanupPgctld)

		database := "testdb"
		addDatabaseToTopo(t, ts, database)

		multipooler := &clustermetadatapb.MultiPooler{
			Id:            serviceID,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   database,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}
		require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

		tmpDir := t.TempDir()
		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Shutdown() })

		mockQueryService := mock.NewQueryService()

		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
		mockQueryService.AddQueryPattern("SELECT pg_current_wal_lsn",
			mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/1000000"}}))
		mockQueryService.AddQueryPattern("SELECT application_name",
			mock.MakeQueryResult([]string{"application_name"}, nil))
		mockQueryService.AddQueryPattern("SHOW synchronous_standby_names",
			mock.MakeQueryResult([]string{"synchronous_standby_names"}, [][]any{{""}}))
		mockQueryService.AddQueryPattern("SHOW synchronous_commit",
			mock.MakeQueryResult([]string{"synchronous_commit"}, [][]any{{"on"}}))
		pm.qsc = &mockPoolerController{queryService: mockQueryService}
		pm.rules = newRuleStore(logger, mockQueryService, noopSyncStandbyManager{})
		pm.rules = &fakeRuleStore{
			pos: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					CohortMembers: []*clustermetadatapb.ID{
						{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler-a"},
						{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler-b"},
					},
				},
				Lsn: "0/1000000",
			},
		}

		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		require.Len(t, status.Status.CohortMembers, 2)
		assert.Equal(t, "zone1", status.Status.CohortMembers[0].Cell)
		assert.Equal(t, "pooler-a", status.Status.CohortMembers[0].Name)
		assert.Equal(t, clustermetadatapb.ID_MULTIPOOLER, status.Status.CohortMembers[0].Component)
		assert.Equal(t, "zone1", status.Status.CohortMembers[1].Cell)
		assert.Equal(t, "pooler-b", status.Status.CohortMembers[1].Name)
	})

	t.Run("Mismatch_REPLICA_topology_but_primary_postgres", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
		t.Cleanup(cleanupPgctld)

		// Create the database in topology with backup location
		database := "testdb"
		addDatabaseToTopo(t, ts, database)

		// Create REPLICA multipooler (but PG will be in primary mode - mismatch!)
		multipooler := &clustermetadatapb.MultiPooler{
			Id:            serviceID,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_REPLICA,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   database,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}
		require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

		tmpDir := t.TempDir()
		createPgDataDir(t, tmpDir)

		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Shutdown() })
		// Mark as initialized to skip auto-restore (not testing backup functionality)
		err = pm.setInitialized()
		require.NoError(t, err)

		// Create mock query service and inject it
		mockQueryService := mock.NewQueryService()

		// PostgreSQL is actually a primary (pg_is_in_recovery = false)
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
		// getPrimaryLSN()
		mockQueryService.AddQueryPattern("SELECT pg_current_wal_lsn",
			mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/12345678"}}))
		// getConnectedFollowerIDs()
		mockQueryService.AddQueryPattern("SELECT application_name",
			mock.MakeQueryResult([]string{"application_name"}, nil))
		// getSynchronousReplicationConfig()
		mockQueryService.AddQueryPattern("SHOW synchronous_standby_names",
			mock.MakeQueryResult([]string{"synchronous_standby_names"}, [][]any{{""}}))
		mockQueryService.AddQueryPattern("SHOW synchronous_commit",
			mock.MakeQueryResult([]string{"synchronous_commit"}, [][]any{{"on"}}))

		pm.qsc = &mockPoolerController{queryService: mockQueryService}
		pm.rules = newRuleStore(logger, mockQueryService, noopSyncStandbyManager{})
		pm.rules = &fakeRuleStore{}

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Call Status - now returns status with mismatch observable
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// PoolerType from topology says REPLICA, but status shows primary state
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.Status.PoolerType)
		assert.NotNil(t, status.Status.PrimaryStatus, "PrimaryStatus should be populated since PostgreSQL is a primary")
		assert.Nil(t, status.Status.ReplicationStatus, "ReplicationStatus should be nil since PostgreSQL is a primary")
	})
}

func TestUpdateConsensusRule_HistoryFailurePreventsGUCUpdate(t *testing.T) {
	// This test verifies that if updateRule fails during
	// UpdateConsensusRule, the synchronous_standby_names GUC is NOT updated.

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-primary",
	}

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerDir := t.TempDir()
	createPgDataDir(t, poolerDir)

	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080, "postgres": 5432},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   database,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	multipooler.PoolerDir = poolerDir

	// Set consensus term
	setTermForTest(t, poolerDir, &clustermetadatapb.TermRevocation{
		RevokedBelowTerm: 5,
	})

	config := &Config{
		TopoClient: ts,
	}
	manager, err := NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	defer manager.Shutdown()

	// Initialize consensus state so the manager can read the term
	manager.mu.Lock()
	manager.consensusState = NewConsensusState(poolerDir, serviceID)
	manager.mu.Unlock()

	// Load the term from file
	_, err = manager.consensusState.Load()
	require.NoError(t, err, "Failed to load consensus state")

	// Set up mock query service
	mockQueryService := mock.NewQueryService()

	// Mock for startup
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))

	manager.qsc = &mockPoolerController{queryService: mockQueryService}
	// observePosition must succeed so UpdateCohortMembers reaches updateRule.
	// updateErr simulates the history write timing out (the failure we're testing).
	manager.rules = &fakeRuleStore{
		pos: &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
				CohortMembers: []*clustermetadatapb.ID{
					{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-1"},
					{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-2"},
				},
				DurabilityPolicy: testBootstrapPolicy(),
			},
		},
		updateErr: mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for sync replication"),
	}

	err = manager.setInitialized()
	require.NoError(t, err)

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go manager.Start(senv)
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond)

	// Mock getSynchronousReplicationConfig (called to get current config)
	// Returns current config with 2 standbys
	mockQueryService.AddQueryPattern("SHOW synchronous_standby_names",
		mock.MakeQueryResult([]string{"synchronous_standby_names"}, [][]any{{"FIRST 1 (zone1_replica-1, zone1_replica-2)"}}))
	mockQueryService.AddQueryPattern("SHOW synchronous_commit",
		mock.MakeQueryResult([]string{"synchronous_commit"}, [][]any{{"remote_write"}}))

	// We do NOT add expectations for ALTER SYSTEM SET synchronous_standby_names
	// If it gets called, ExpectationsWereMet() will fail

	// Call UpdateConsensusRule to add a new standby
	newStandby := &clustermetadatapb.ID{Cell: "zone1", Name: "replica-3"}

	err = manager.UpdateConsensusRule(
		ctx,
		multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD,
		[]*clustermetadatapb.ID{newStandby},
		&clustermetadatapb.RuleNumber{CoordinatorTerm: 5}, // expectedOutgoingRule
		nil, // coordinatorID
	)

	// Verify it failed
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to record replication config history")

	// CRITICAL: Verify that NO ALTER SYSTEM queries were executed
	assert.NoError(t, mockQueryService.ExpectationsWereMet(),
		"If this fails, it means SetPolicy was called despite history insert failure")
}

// TestRewindToSource_ManagerReopenedOnError is a regression test for a bug where
// RewindToSource would leave the manager closed if an error occurred after pausing.
// The fix uses the Pause()/resume() pattern with defer to guarantee the manager
// is always reopened, even on error paths.
func TestRewindToSource_ManagerReopenedOnError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerDir := t.TempDir()
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	multipooler := &clustermetadatapb.MultiPooler{
		Id:        serviceID,
		PoolerDir: poolerDir,
		Type:      clustermetadatapb.PoolerType_REPLICA,
		PortMap: map[string]int32{
			"postgres": 5432,
		},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "postgres",
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}

	// Start a mock pgctld server that will fail the Stop call
	mockPgctld := &testutil.MockPgCtldService{
		StopError: errors.New("mock error: PostgreSQL stop failed"),
	}
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, mockPgctld)
	t.Cleanup(cleanupPgctld)

	// Create mock query service to avoid hanging during Open()
	mockQueryService := mock.NewQueryService()
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))

	config := &Config{
		TopoClient: ts,
		PgctldAddr: pgctldAddr,
	}

	manager, err := NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	defer manager.Shutdown()

	// Create pg_data directory so setInitialized() can write marker file
	createPgDataDir(t, poolerDir)

	err = manager.setInitialized()
	require.NoError(t, err)

	// Assign mock pooler controller BEFORE opening to avoid race conditions
	manager.qsc = &mockPoolerController{queryService: mockQueryService}
	manager.rules = newRuleStore(logger, mockQueryService, noopSyncStandbyManager{})

	// Simulate the manager being open and ready (set internal state without starting goroutines)
	manager.mu.Lock()
	manager.isOpen = true
	manager.state = ManagerStateReady
	manager.ctx, manager.cancel = context.WithCancel(ctx)
	manager.mu.Unlock()

	// Create a source pooler
	sourceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "source-pooler",
	}
	source := &clustermetadatapb.MultiPooler{
		Id:       sourceID,
		Hostname: "source-host",
		PortMap: map[string]int32{
			"postgres": 5432,
		},
	}

	// Call RewindToSource - this should fail during the Stop call
	_, err = manager.RewindToSource(ctx, source)

	// Verify the call failed as expected
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PostgreSQL stop failed")

	// CRITICAL REGRESSION TEST: Verify the manager was reopened despite the error.
	// This is the bug we're testing for: if RewindToSource fails, the manager must
	// still be reopened so the node can continue operating.
	require.Eventually(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.isOpen
	}, 2*time.Second, 50*time.Millisecond, "REGRESSION: Manager should be reopened even when RewindToSource fails")
}

func TestSetPostgresRestartsEnabledRPC(t *testing.T) {
	ctx := t.Context()

	t.Run("disable", func(t *testing.T) {
		pm := &MultiPoolerManager{logger: slog.Default()}

		resp, err := pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, pm.postgresRestartsDisabled.Load(), "restarts should be disabled after RPC")
	})

	t.Run("enable", func(t *testing.T) {
		pm := &MultiPoolerManager{logger: slog.Default()}
		pm.postgresRestartsDisabled.Store(true)

		resp, err := pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.False(t, pm.postgresRestartsDisabled.Load(), "restarts should be enabled after RPC")
	})

	t.Run("idempotent_disable", func(t *testing.T) {
		pm := &MultiPoolerManager{logger: slog.Default()}

		_, err := pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
		require.NoError(t, err)
		_, err = pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
		require.NoError(t, err)
		assert.True(t, pm.postgresRestartsDisabled.Load())
	})

	t.Run("idempotent_enable", func(t *testing.T) {
		pm := &MultiPoolerManager{logger: slog.Default()}
		pm.postgresRestartsDisabled.Store(true)

		_, err := pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
		require.NoError(t, err)
		_, err = pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
		require.NoError(t, err)
		assert.False(t, pm.postgresRestartsDisabled.Load())
	})
}
