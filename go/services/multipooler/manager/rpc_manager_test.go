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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/executor/mock"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/prototest"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// fakeConnPoolMgr is a minimal PoolManager stub for tests that only need to
// control the superuser name visible to MultiPoolerManager (e.g. asserting the
// rendered primary_conninfo). All other methods inherit the embedded nil
// interface and will panic if called — keep usage scoped to tests that don't
// exercise the real pool.
type fakeConnPoolMgr struct {
	connpoolmanager.PoolManager
	user string
}

func (f *fakeConnPoolMgr) PgUser() string { return f.user }
func (f *fakeConnPoolMgr) Close()         {} // called from MultiPoolerManager.Shutdown

// setTermForTest writes the consensus term file directly for testing.
func setTermForTest(t *testing.T, poolerDir string, term *multipoolermanagerdatapb.ConsensusTerm) {
	t.Helper()
	cs := NewConsensusState(poolerDir, nil)
	require.NoError(t, cs.setConsensusTerm(term), "failed to write term file")
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
				Database:      database,
				Hostname:      "localhost",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          tt.poolerType,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				TableGroup:    constants.DefaultTableGroup,
				Shard:         constants.DefaultShard,
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
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
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
			name:       "SetPrimaryConnInfo times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_REPLICA,
			callMethod: func(ctx context.Context) error {
				primary := &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "zone1",
						Name:      "test-primary",
					},
					Hostname: "localhost",
					PortMap:  map[string]int32{"postgres": 5432},
				}
				return manager.SetPrimaryConnInfo(ctx, primary, false, false, nil, true)
			},
		},
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
			name:       "ConfigureSynchronousReplication times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				return manager.ConfigureSynchronousReplication(
					ctx,
					multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
					multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
					1,
					[]*clustermetadatapb.ID{serviceID},
					true,  // reloadConfig
					false, // force
				)
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
			name:       "EmergencyDemote times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				_, err := manager.EmergencyDemote(ctx, nil, 5*time.Second, true /* force */)
				return err
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
			name:       "Promote times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_REPLICA,
			callMethod: func(ctx context.Context) error {
				_, err := manager.Promote(ctx, nil, "", nil, true /* force */, "", nil, nil)
				return err
			},
		},
		{
			name:       "UpdateSynchronousStandbyList times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				return manager.UpdateSynchronousStandbyList(ctx, multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD, []*clustermetadatapb.ID{serviceID}, true, nil, true)
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

// setupPromoteTestManager creates a manager configured as a REPLICA for promotion tests.
func setupPromoteTestManager(t *testing.T, mockQueryService *mock.QueryService, rules ruleStorer) (*MultiPoolerManager, string) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })

	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
	t.Cleanup(cleanupPgctld)

	// Create the database in topology with backup location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-replica",
	}

	// Create as REPLICA (ready for promotion)
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_REPLICA,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	tmpDir := t.TempDir()

	// Create pg_data directory with PG_VERSION
	// We'll call setInitialized() later to mark as initialized
	pgDataDir := filepath.Join(tmpDir, "pg_data")
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, "PG_VERSION"), []byte("16"), 0o644))
	t.Setenv(constants.PgDataDirEnvVar, pgDataDir)

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

	// Assign mock pooler controller and rule store BEFORE starting the manager
	// to avoid race conditions.
	pm.qsc = &mockPoolerController{queryService: mockQueryService}
	pm.rules = rules

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)

	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	// Set consensus term to expected value (10) for testing via direct file write
	term := &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 10}
	setTermForTest(t, tmpDir, term)

	// Initialize consensus state so the manager can read the term
	pm.mu.Lock()
	pm.consensusState = NewConsensusState(tmpDir, serviceID)
	pm.mu.Unlock()

	// Load the term from file
	_, err = pm.consensusState.Load()
	require.NoError(t, err, "Failed to load consensus state")

	return pm, tmpDir
}

// These tests verify that the Promote method is truly idempotent and can handle partial failures.
// TestPromoteIdempotency_PostgreSQLPromotedButTopologyNotUpdated tests the critical idempotency scenario:
// PostgreSQL was promoted but topology update failed. The retry should succeed and only update topology.
func TestPromoteIdempotency_PostgreSQLPromotedButTopologyNotUpdated(t *testing.T) {
	ctx := context.Background()

	// Simulate partial completion:
	// 1. PostgreSQL is already primary (pg_promote() was called successfully)
	// 2. Topology still shows REPLICA (update failed previously)
	// 3. No sync replication config requested

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// checkPromotionState should return "f" (PG is already primary)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: Since already promoted, get current LSN (called twice - during processing and for final response)
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/ABCDEF0"}}))
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/ABCDEF0"}}))

	fakeRules := &fakeRuleStore{}
	pm, _ := setupPromoteTestManager(t, mockQueryService, fakeRules)

	// Topology is still REPLICA (this is what the guard rail checks)
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote - should detect PG is already promoted and only update topology
	resp, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/ABCDEF0", nil, false /* force */, "", nil, nil)
	require.NoError(t, err, "Should succeed - idempotent retry after partial failure")
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary, "Should not report as fully complete since topology wasn't updated")
	assert.Equal(t, int64(10), resp.ConsensusTerm)
	assert.Equal(t, "0/ABCDEF0", resp.LsnPosition)

	// Verify topology was updated
	pm.mu.Lock()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.multipooler.Type, "Topology should be updated to PRIMARY")
	pm.mu.Unlock()
	fakeRules.assertPromoteRecorded(t)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_FullyCompleteTopologyPrimary tests that Promote succeeds when everything is complete
// This is the true idempotency case - calling Promote when topology is PRIMARY and everything is consistent
func TestPromoteIdempotency_FullyCompleteTopologyPrimary(t *testing.T) {
	ctx := context.Background()

	// Simulate fully completed promotion:
	// 1. PostgreSQL is primary (not in recovery)
	// 2. Topology is PRIMARY
	// 3. No sync replication config requested (so it matches by default)

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// checkPromotionState should return "f" (PG is already primary)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: Get current LSN (since already primary)
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/FEDCBA0"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService, &fakeRuleStore{})

	// Topology is already PRIMARY
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_PRIMARY
	pm.mu.Unlock()

	// Call Promote - should succeed with WasAlreadyPrimary=true (idempotent)
	resp, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/FEDCBA0", nil, false /* force */, "", nil, nil)
	require.NoError(t, err, "Should succeed - everything is already complete")
	require.NotNil(t, resp)

	assert.True(t, resp.WasAlreadyPrimary, "Should report as already primary")
	assert.Equal(t, int64(10), resp.ConsensusTerm)
	assert.Equal(t, "0/FEDCBA0", resp.LsnPosition)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_InconsistentStateTopologyPrimaryPgNotPrimary tests error when topology is PRIMARY but PG is not
func TestPromoteIdempotency_InconsistentStateTopologyPrimaryPgNotPrimary(t *testing.T) {
	ctx := context.Background()

	// Simulate inconsistent state (should never happen):
	// 1. PostgreSQL is still in recovery (standby)
	// 2. Topology shows PRIMARY
	// This indicates a serious problem that requires manual intervention

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// Mock: checkPromotionState queries pg_is_in_recovery() - returns true (still standby!)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService, &fakeRuleStore{})

	// Topology shows PRIMARY (inconsistent!)
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_PRIMARY
	pm.mu.Unlock()

	// Call Promote without force - should fail with inconsistent state error
	_, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/FEDCBA0", nil, false, "", nil, nil)
	require.Error(t, err, "Should fail due to inconsistent state without force flag")
	assert.Contains(t, err.Error(), "inconsistent state")
	assert.Contains(t, err.Error(), "Manual intervention required")
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_InconsistentStateFixedWithForce tests that force flag fixes inconsistent state
func TestPromoteIdempotency_InconsistentStateFixedWithForce(t *testing.T) {
	ctx := context.Background()

	// Simulate inconsistent state:
	// 1. PostgreSQL is still in recovery (standby)
	// 2. Topology shows PRIMARY
	// With force=true, it should complete the missing promotion steps

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// The sequence of pg_is_in_recovery calls is:
	// 1. Promote checkPromotionState - returns "t" (consumed) - still in recovery
	// 2. waitForPromotionComplete polling - returns "f" (persistent) - promotion complete
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// After pg_promote(), waitForPromotionComplete polls until pg_is_in_recovery returns false
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: Validate expected LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}, [][]any{{"0/FEDCBA0", "t"}}))

	// Mock: pg_promote() call to fix the inconsistency
	mockQueryService.AddQueryPatternOnce("SELECT pg_promote",
		mock.MakeQueryResult(nil, nil))

	// Mock: Clear primary_conninfo after promotion
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/FEDCBA0"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService, &fakeRuleStore{})

	// Topology shows PRIMARY (inconsistent!)
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_PRIMARY
	pm.mu.Unlock()

	// Call Promote with force=true - should fix the inconsistency. Even with
	// force=true we pass a claim because the handler reads claim.GetTerm() for
	// the response; force just bypasses admission validation.
	resp, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/FEDCBA0", nil, true, "", nil, nil)
	require.NoError(t, err, "Should succeed with force flag - fixing inconsistent state")
	require.NotNil(t, resp)

	// PostgreSQL was promoted, so this is not "already primary" case
	assert.False(t, resp.WasAlreadyPrimary)
	assert.Equal(t, int64(10), resp.ConsensusTerm)
	assert.Equal(t, "0/FEDCBA0", resp.LsnPosition)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_NothingCompleteYet tests promotion from scratch
func TestPromoteIdempotency_NothingCompleteYet(t *testing.T) {
	ctx := context.Background()

	// Simulate fresh promotion - nothing done yet:
	// 1. PostgreSQL is still in recovery (standby)
	// 2. Topology is REPLICA
	// 3. No sync replication configured

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// The sequence of pg_is_in_recovery calls is:
	// 1. Promote checkPromotionState - returns "t" (consumed) - still in recovery
	// 2. waitForPromotionComplete polling - returns "f" (persistent) - promotion complete
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// After pg_promote(), waitForPromotionComplete polls until pg_is_in_recovery returns false
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: Validate expected LSN (pg_last_wal_replay_lsn + pg_is_wal_replay_paused)
	mockQueryService.AddQueryPatternOnce("SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}, [][]any{{"0/5678ABC", "t"}}))

	// Mock: pg_promote() call
	mockQueryService.AddQueryPatternOnce("SELECT pg_promote",
		mock.MakeQueryResult(nil, nil))

	// Mock: Clear primary_conninfo after promotion
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/5678ABC"}}))

	fakeRules := &fakeRuleStore{}
	pm, _ := setupPromoteTestManager(t, mockQueryService, fakeRules)

	// Topology is REPLICA
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote - should execute all steps
	resp, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/5678ABC", nil, false /* force */, "", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary)
	assert.Equal(t, int64(10), resp.ConsensusTerm)

	// Verify topology was updated
	pm.mu.Lock()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.multipooler.Type)
	pm.mu.Unlock()
	fakeRules.assertPromoteRecorded(t)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_LSNMismatchBeforePromotion tests that promotion fails if LSN doesn't match
func TestPromoteIdempotency_LSNMismatchBeforePromotion(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// PostgreSQL is still in recovery
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))

	// Mock: Check LSN - return different value than expected
	mockQueryService.AddQueryPatternOnce("SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}, [][]any{{"0/9999999", "t"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService, &fakeRuleStore{})

	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote with different expected LSN - should fail
	_, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/1111111", nil, false /* force */, "", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LSN")
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_TermMismatch tests that promotion fails with wrong term
func TestPromoteIdempotency_TermMismatch(t *testing.T) {
	ctx := context.Background()

	// Create mock - only startup expectations needed because term validation happens before test DB queries
	mockQueryService := mock.NewQueryService()

	pm, tmpDir := setupPromoteTestManager(t, mockQueryService, &fakeRuleStore{})

	// Explicitly set the term to 10 to ensure we have the expected value via direct file write
	term := &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 10}
	setTermForTest(t, tmpDir, term)

	// Call Promote with wrong term (current term is 10, passing 5)
	_, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 5, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/1234567", nil, false /* force */, "", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "term")
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_SecondCallSucceedsAfterCompletion tests that calling Promote after completion succeeds (idempotent)
func TestPromoteIdempotency_SecondCallSucceedsAfterCompletion(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// The sequence of pg_is_in_recovery calls is:
	// 1. Promote checkPromotionState - returns "t" (consumed)
	// 2. waitForPromotionComplete polling - returns "f" (consumed on first call)
	// 3. Second Promote call checkPromotionState - returns "f" (consumed on second call)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// waitForPromotionComplete returns false (promotion complete)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
	// Second Promote call returns false (already primary)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: Validate expected LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}, [][]any{{"0/AAA1111", "t"}}))

	// Mock: pg_promote() call
	mockQueryService.AddQueryPatternOnce("SELECT pg_promote",
		mock.MakeQueryResult(nil, nil))

	// Mock: Clear primary_conninfo after promotion
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	// Mock: Get current LSN (called twice - once after first promote, once in second call)
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/AAA1111"}}))
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/AAA1111"}}))

	fakeRules := &fakeRuleStore{}
	pm, _ := setupPromoteTestManager(t, mockQueryService, fakeRules)

	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// First call
	resp1, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/AAA1111", nil, false /* force */, "", nil, nil)
	require.NoError(t, err)
	assert.False(t, resp1.WasAlreadyPrimary)

	// Verify topology was updated to PRIMARY
	pm.mu.Lock()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.multipooler.Type)
	pm.mu.Unlock()
	fakeRules.assertPromoteRecorded(t)

	// Second call should SUCCEED - topology is PRIMARY and everything is consistent (idempotent)
	// The pg_is_in_recovery pattern already returns "f" (false) since the first call consumed the "t" patterns
	resp2, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/AAA1111", nil, false /* force */, "", nil, nil)
	require.NoError(t, err, "Second call should succeed - idempotent operation")
	assert.True(t, resp2.WasAlreadyPrimary, "Second call should report as already primary")
	assert.Equal(t, "0/AAA1111", resp2.LsnPosition)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_EmptyExpectedLSNSkipsValidation tests that empty expectedLSN skips validation
func TestPromoteIdempotency_EmptyExpectedLSNSkipsValidation(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// The sequence of pg_is_in_recovery calls is:
	// 1. Promote checkPromotionState - returns "t" (consumed)
	// 2. waitForPromotionComplete polling - returns "f" (consumed)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// After pg_promote(), waitForPromotionComplete polls until pg_is_in_recovery returns false
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: pg_promote() call (LSN validation skipped because expectedLSN is empty)
	mockQueryService.AddQueryPatternOnce("SELECT pg_promote",
		mock.MakeQueryResult(nil, nil))

	// Mock: Clear primary_conninfo after promotion
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/BBBBBBB"}}))

	fakeRules := &fakeRuleStore{}
	pm, _ := setupPromoteTestManager(t, mockQueryService, fakeRules)

	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote with empty expectedLSN - should skip LSN validation
	resp, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "", nil, false /* force */, "", nil, nil)
	require.NoError(t, err, "Should succeed with empty expectedLSN")
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary)
	assert.Equal(t, "0/BBBBBBB", resp.LsnPosition)
	fakeRules.assertPromoteRecorded(t)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromote_WithElectionMetadata tests that Promote accepts and uses election metadata fields
func TestPromote_WithElectionMetadata(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// checkPromotionState returns "t" (still in recovery)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// After pg_promote(), waitForPromotionComplete polls until pg_is_in_recovery returns false
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: Validate expected LSN (pg_last_wal_replay_lsn + pg_is_wal_replay_paused)
	mockQueryService.AddQueryPatternOnce("SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}, [][]any{{"0/1234567", "t"}}))

	// Mock: pg_promote() call
	mockQueryService.AddQueryPatternOnce("SELECT pg_promote",
		mock.MakeQueryResult(nil, nil))

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/1234567"}}))

	// Mock: Clear primary_conninfo after promotion
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	fakeRules := &fakeRuleStore{}
	pm, _ := setupPromoteTestManager(t, mockQueryService, fakeRules)

	// Topology is REPLICA
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote with election metadata
	reason := "dead_primary"
	coordinatorID := &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}
	cohortMembers := []*clustermetadatapb.ID{
		{Cell: "zone1", Name: "pooler-1"},
		{Cell: "zone1", Name: "pooler-2"},
		{Cell: "zone1", Name: "pooler-3"},
	}
	acceptedMembers := []*clustermetadatapb.ID{
		{Cell: "zone1", Name: "pooler-1"},
		{Cell: "zone1", Name: "pooler-3"},
	}

	resp, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: coordinatorID, CoordinatorInitiatedAt: timestamppb.Now()}, "0/1234567", nil, false /* force */, reason, cohortMembers, acceptedMembers)
	require.NoError(t, err, "Promote should succeed with election metadata")
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary)
	assert.Equal(t, int64(10), resp.ConsensusTerm)
	assert.Equal(t, "0/1234567", resp.LsnPosition)

	// Verify topology was updated
	pm.mu.Lock()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.multipooler.Type)
	pm.mu.Unlock()

	update := fakeRules.assertPromoteRecorded(t)
	assert.Equal(t, "dead_primary", update.reason)
	prototest.AssertEqual(t, coordinatorID, update.coordinatorID)
	prototest.RequireElementsMatch(t, cohortMembers, update.cohortMembers)
	prototest.RequireElementsMatch(t, acceptedMembers, update.acceptedMembers)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromote_RuleHistoryErrorFailsPromotion tests that an error in updateRule
// fails the entire Promote operation. This ensures sync replication is functioning before
// accepting the promotion - better to have no primary than one that violates durability policy.
func TestPromote_RuleHistoryErrorFailsPromotion(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// checkPromotionState returns "t" (still in recovery)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// After pg_promote(), waitForPromotionComplete polls until pg_is_in_recovery returns false
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: Validate expected LSN (pg_last_wal_replay_lsn + pg_is_wal_replay_paused)
	mockQueryService.AddQueryPatternOnce("SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}, [][]any{{"0/9876543", "t"}}))

	// Mock: pg_promote() call
	mockQueryService.AddQueryPatternOnce("SELECT pg_promote",
		mock.MakeQueryResult(nil, nil))

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/9876543"}}))

	// Mock: Clear primary_conninfo after promotion (executed before updateRule)
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	// updateRule fails (e.g., sync replication timeout)
	fakeRules := &fakeRuleStore{updateErr: mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for synchronous replication")}
	pm, _ := setupPromoteTestManager(t, mockQueryService, fakeRules)

	// Topology is REPLICA
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Verify primary_term is 0 before promotion
	term, err := pm.consensusState.GetInconsistentTerm()
	require.NoError(t, err)
	require.Equal(t, int64(0), term.GetPrimaryTerm(), "primary_term should be 0 before promotion")

	// Call Promote - should FAIL because rule history write fails
	resp, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/9876543", nil, false /* force */, "test_reason", nil, nil)
	require.Error(t, err, "Promote should fail when rule history write fails")
	require.Nil(t, resp)

	// Error message should indicate the rule history failure
	assert.Contains(t, err.Error(), "rule history")

	// CRITICAL: Verify that primary_term WAS set even though promotion failed.
	// This is intentional - we set primary_term (local state) before writing to history table
	// (committed transaction). If the order were reversed and history write succeeded but
	// primary_term write failed, we'd have a committed transaction without local state.
	// With this ordering, on retry the primary_term set is idempotent and history write will succeed.
	term, err = pm.consensusState.GetInconsistentTerm()
	require.NoError(t, err)
	assert.Equal(t, int64(10), term.GetPrimaryTerm(),
		"primary_term should be set to 10 even though promotion failed (set before history write)")

	// Note: PostgreSQL was promoted but we return error to indicate the promotion is incomplete.
	// The coordinator should handle this partial promotion state (e.g., retry or repair).
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromote_TopologyUpdateFailureDoesNotFailPromotion tests that a topo failure
// during updateTopologyAfterPromotion does not fail the overall Promote operation.
func TestPromote_TopologyUpdateFailureDoesNotFailPromotion(t *testing.T) {
	ctx := context.Background()

	mockQueryService := mock.NewQueryService()

	// checkPromotionState
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// waitForPromotionComplete
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Validate expected LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}, [][]any{{"0/ABCDEF0", "t"}}))

	// pg_promote()
	mockQueryService.AddQueryPatternOnce("SELECT pg_promote",
		mock.MakeQueryResult(nil, nil))

	// Clear primary_conninfo after promotion
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	// Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/ABCDEF0"}}))

	// Inline setup (like setupPromoteTestManager but capturing factory)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })

	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
	t.Cleanup(cleanupPgctld)

	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-replica",
	}

	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_REPLICA,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	tmpDir := t.TempDir()
	pgDataDir := filepath.Join(tmpDir, "pg_data")
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, "PG_VERSION"), []byte("16"), 0o644))
	t.Setenv(constants.PgDataDirEnvVar, pgDataDir)
	multipooler.PoolerDir = tmpDir

	config := &Config{
		TopoClient: ts,
		PgctldAddr: pgctldAddr,
	}
	pm, err := NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	t.Cleanup(func() { pm.Shutdown() })

	err = pm.setInitialized()
	require.NoError(t, err)

	fakeRules := &fakeRuleStore{}
	pm.qsc = &mockPoolerController{queryService: mockQueryService}
	pm.rules = fakeRules

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)

	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	term := &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 10}
	setTermForTest(t, tmpDir, term)

	pm.mu.Lock()
	pm.consensusState = NewConsensusState(tmpDir, serviceID)
	pm.mu.Unlock()

	_, err = pm.consensusState.Load()
	require.NoError(t, err)

	// Inject topo failure before calling Promote
	factory.SetError(errors.New("topo unavailable"))

	// Promote should succeed despite topo failure
	resp, err := pm.Promote(ctx, &consensusdatapb.CoordinatorClaim{Term: 10, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()}, "0/ABCDEF0", nil, false, "test_reason", nil, nil)
	require.NoError(t, err, "Promote should succeed even when topology update fails")
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary)
	assert.Equal(t, int64(10), resp.ConsensusTerm)
	assert.Equal(t, "0/ABCDEF0", resp.LsnPosition)

	// Local state should still be updated to PRIMARY
	pm.mu.Lock()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.multipooler.Type)
	pm.mu.Unlock()

	// Verify health streamer has primary observation with self as primary
	healthState := pm.healthStreamer.getState()
	require.NotNil(t, healthState.PrimaryObservation, "health streamer should have primary observation after Promote")
	assert.Equal(t, serviceID, healthState.PrimaryObservation.PrimaryID, "primary observation should point to self")
	assert.Equal(t, int64(10), healthState.PrimaryObservation.PrimaryTerm, "primary observation term should match consensus term")

	fakeRules.assertPromoteRecorded(t)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

func TestSetPrimaryTerm_InvariantValidation(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create pg_data directory structure
	createPgDataDir(t, tmpDir)

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	// Create consensus state and set initial term to 5
	consensusState := NewConsensusState(tmpDir, serviceID)
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		TermNumber:  5,
		PrimaryTerm: 0,
	}
	setTermForTest(t, tmpDir, initialTerm)
	_, err := consensusState.Load()
	require.NoError(t, err)

	// Create action lock and acquire it
	actionLock := NewActionLock()
	lockCtx, err := actionLock.Acquire(ctx, "test-primary-term")
	require.NoError(t, err)
	defer actionLock.Release(lockCtx)

	// Setting primary_term to match current term should succeed
	err = consensusState.SetPrimaryTerm(lockCtx, 5, false /* force */)
	require.NoError(t, err, "Should be able to set primary_term to current term value")

	term, err := consensusState.GetInconsistentTerm()
	require.NoError(t, err)
	assert.Equal(t, int64(5), term.GetPrimaryTerm(), "primary_term should be set to 5")

	// Setting primary_term to a different value should fail (invariant violation)
	err = consensusState.SetPrimaryTerm(lockCtx, 7, false /* force */)
	require.Error(t, err, "Should fail when primary_term doesn't match current term")

	// Verify primary_term was not changed
	term, err = consensusState.GetInconsistentTerm()
	require.NoError(t, err)
	assert.Equal(t, int64(5), term.GetPrimaryTerm(), "primary_term should still be 5")

	// But with force=true, setting a mismatched term should succeed
	err = consensusState.SetPrimaryTerm(lockCtx, 7, true /* force */)
	require.NoError(t, err, "Should succeed with force=true even when terms don't match")

	term, err = consensusState.GetInconsistentTerm()
	require.NoError(t, err)
	assert.Equal(t, int64(7), term.GetPrimaryTerm(), "primary_term should be updated to 7 with force")

	// Clearing primary_term to 0 should always succeed (exception to invariant)
	err = consensusState.SetPrimaryTerm(lockCtx, 0, false /* force */)
	require.NoError(t, err, "Should be able to clear primary_term to 0 regardless of current term")

	term, err = consensusState.GetInconsistentTerm()
	require.NoError(t, err)
	assert.Equal(t, int64(0), term.GetPrimaryTerm(), "primary_term should be cleared to 0")

	// Negative primary_term should fail even with force
	err = consensusState.SetPrimaryTerm(lockCtx, -1, false /* force */)
	require.Error(t, err, "Should fail with negative primary_term")
	assert.Contains(t, err.Error(), "primary_term cannot be negative")

	err = consensusState.SetPrimaryTerm(lockCtx, -1, true /* force */)
	require.Error(t, err, "Should fail with negative primary_term even with force=true")
	assert.Contains(t, err.Error(), "primary_term cannot be negative")
}

func TestSetPrimaryConnInfo_StoresPrimaryPoolerID(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-replica",
	}

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
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_REPLICA,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
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
	defer pm.Shutdown()

	// Mark as initialized to skip auto-restore (not testing backup functionality)
	err = pm.setInitialized()
	require.NoError(t, err)

	// Initialize consensus state
	pm.mu.Lock()
	pm.consensusState = NewConsensusState(tmpDir, serviceID)
	pm.mu.Unlock()

	// Set up mock query service
	mockQueryService := mock.NewQueryService()
	// REPLICA: pg_is_in_recovery returns true (in recovery) - for SetPrimaryConnInfo guardrail check
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	// SetPrimaryConnInfo executes ALTER SYSTEM SET primary_conninfo — capture the full
	// SQL so we can assert on the rendered libpq conninfo.
	var capturedConnInfoSQL string
	mockQueryService.AddQueryPatternWithCallback(
		"ALTER SYSTEM SET primary_conninfo",
		mock.MakeQueryResult(nil, nil),
		func(sql string) { capturedConnInfoSQL = sql },
	)
	// SetPrimaryConnInfo executes pg_reload_conf()
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
	pm.qsc = &mockPoolerController{queryService: mockQueryService}

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)
	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	// Inject a fake pool manager so we can verify primary_conninfo uses the
	// configured superuser name.
	const testSuperuser = "admin"
	pm.mu.Lock()
	pm.connPoolMgr = &fakeConnPoolMgr{user: testSuperuser}
	pm.mu.Unlock()

	// Set consensus term first (required for SetPrimaryConnInfo) via direct file write
	term := &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 1}
	setTermForTest(t, tmpDir, term)
	// Reload consensus state to pick up the term from file
	_, err = pm.consensusState.Load()
	require.NoError(t, err, "Failed to load consensus state")

	// Call SetPrimaryConnInfo with a specific primary MultiPooler
	testPrimaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "primary-pooler-123",
	}
	primary := &clustermetadatapb.MultiPooler{
		Id:       testPrimaryID,
		Hostname: "primary-host",
		PortMap:  map[string]int32{"postgres": 5432},
	}
	claim := &consensusdatapb.CoordinatorClaim{
		Term:                   1,
		CoordinatorId:          &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo-test"},
		CoordinatorInitiatedAt: timestamppb.Now(),
	}
	err = pm.SetPrimaryConnInfo(ctx, primary, false, false, claim, false)
	require.NoError(t, err)

	// Verify all mock expectations were met
	assert.NoError(t, mockQueryService.ExpectationsWereMet())

	// Verify the rendered primary_conninfo wires the configured superuser into
	// the user= slot. Regression guard for the bug where the database name was
	// being passed here.
	assert.Contains(t, capturedConnInfoSQL, "user="+testSuperuser,
		"primary_conninfo must contain user=%s, got: %s", testSuperuser, capturedConnInfoSQL)

	// Verify the primaryPoolerID is stored in the manager as a *clustermetadatapb.ID
	pm.mu.Lock()
	storedPrimaryPoolerID := pm.primaryPoolerID
	pm.mu.Unlock()

	require.NotNil(t, storedPrimaryPoolerID, "primaryPoolerID should be stored")
	assert.Equal(t, testPrimaryID.Component, storedPrimaryPoolerID.Component, "primaryPoolerID component should match")
	assert.Equal(t, testPrimaryID.Cell, storedPrimaryPoolerID.Cell, "primaryPoolerID cell should match")
	assert.Equal(t, testPrimaryID.Name, storedPrimaryPoolerID.Name, "primaryPoolerID name should match")
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
			Database:      database,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			TableGroup:    constants.DefaultTableGroup,
			Shard:         constants.DefaultShard,
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
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.PoolerType)
		assert.NotNil(t, status.PrimaryStatus, "PrimaryStatus should be populated")
		assert.Nil(t, status.ReplicationStatus, "ReplicationStatus should be nil for PRIMARY")
		assert.Equal(t, "0/12345678", status.PrimaryStatus.Lsn)
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
			Database:      database,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_REPLICA,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			TableGroup:    constants.DefaultTableGroup,
			Shard:         constants.DefaultShard,
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
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.PoolerType)
		assert.Nil(t, status.PrimaryStatus, "PrimaryStatus should be nil for REPLICA")
		assert.NotNil(t, status.ReplicationStatus, "ReplicationStatus should be populated")
		assert.Equal(t, "0/12345600", status.ReplicationStatus.LastReplayLsn)
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
			Database:      database,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			TableGroup:    constants.DefaultTableGroup,
			Shard:         constants.DefaultShard,
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
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.PoolerType)
		assert.Nil(t, status.PrimaryStatus, "PrimaryStatus should be nil since PostgreSQL is a standby")
		assert.NotNil(t, status.ReplicationStatus, "ReplicationStatus should be populated since PostgreSQL is a standby")
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
			Database:      database,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			TableGroup:    constants.DefaultTableGroup,
			Shard:         constants.DefaultShard,
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

		require.Len(t, status.CohortMembers, 2)
		assert.Equal(t, "zone1", status.CohortMembers[0].Cell)
		assert.Equal(t, "pooler-a", status.CohortMembers[0].Name)
		assert.Equal(t, clustermetadatapb.ID_MULTIPOOLER, status.CohortMembers[0].Component)
		assert.Equal(t, "zone1", status.CohortMembers[1].Cell)
		assert.Equal(t, "pooler-b", status.CohortMembers[1].Name)
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
			Database:      database,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_REPLICA,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			TableGroup:    constants.DefaultTableGroup,
			Shard:         constants.DefaultShard,
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
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.PoolerType)
		assert.NotNil(t, status.PrimaryStatus, "PrimaryStatus should be populated since PostgreSQL is a primary")
		assert.Nil(t, status.ReplicationStatus, "ReplicationStatus should be nil since PostgreSQL is a primary")
	})
}

func TestConfigureSynchronousReplication_HistoryFailurePreventGUCUpdates(t *testing.T) {
	// This test verifies that if updateRule fails,
	// the synchronous_commit and synchronous_standby_names GUCs are NOT updated.
	// This ensures that we only update GUCs if the rule update succeeds.

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
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080, "postgres": 5432},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	multipooler.PoolerDir = poolerDir

	// Set consensus term
	setTermForTest(t, poolerDir, &multipoolermanagerdatapb.ConsensusTerm{
		PrimaryTerm: 1,
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

	// Mock for startup: pg_is_in_recovery returns false (PRIMARY)
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))

	manager.qsc = &mockPoolerController{queryService: mockQueryService}
	manager.rules = &fakeRuleStore{updateErr: mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for sync replication")}

	// Mark as initialized
	err = manager.setInitialized()
	require.NoError(t, err)

	// Start and wait for ready
	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go manager.Start(senv)
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond)

	// We do NOT add expectations for ALTER SYSTEM SET queries
	// If they get called, ExpectationsWereMet() will fail

	// Call ConfigureSynchronousReplication
	standbyIDs := []*clustermetadatapb.ID{
		{Cell: "zone1", Name: "replica-1"},
		{Cell: "zone1", Name: "replica-2"},
	}

	err = manager.ConfigureSynchronousReplication(
		ctx,
		multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE,
		multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
		1,
		standbyIDs,
		true,  // reloadConfig
		false, // force
	)

	// Verify it failed with the expected error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to record replication config history")

	// CRITICAL: Verify that NO additional queries were executed beyond the INSERT
	// This proves that setSynchronousCommit and setSynchronousStandbyNames were NOT called
	assert.NoError(t, mockQueryService.ExpectationsWereMet(),
		"If this fails, it means GUC update queries were called despite history insert failure")
}

func TestUpdateSynchronousStandbyList_HistoryFailurePreventsGUCUpdate(t *testing.T) {
	// This test verifies that if updateRule fails during
	// UpdateSynchronousStandbyList, the synchronous_standby_names GUC is NOT updated.

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
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080, "postgres": 5432},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	multipooler.PoolerDir = poolerDir

	// Set consensus term
	setTermForTest(t, poolerDir, &multipoolermanagerdatapb.ConsensusTerm{
		PrimaryTerm: 5,
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
	manager.rules = &fakeRuleStore{updateErr: mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for sync replication")}

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

	// Call UpdateSynchronousStandbyList to add a new standby
	newStandby := &clustermetadatapb.ID{Cell: "zone1", Name: "replica-3"}

	err = manager.UpdateSynchronousStandbyList(
		ctx,
		multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
		[]*clustermetadatapb.ID{newStandby},
		true, // reloadConfig
		&consensusdatapb.CoordinatorClaim{Term: 5, CoordinatorId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: "mo"}, CoordinatorInitiatedAt: timestamppb.Now()},
		false, // force
	)

	// Verify it failed
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to record replication config history")

	// CRITICAL: Verify that NO ALTER SYSTEM queries were executed
	assert.NoError(t, mockQueryService.ExpectationsWereMet(),
		"If this fails, it means applySynchronousStandbyNames was called despite history insert failure")
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
		Id:         serviceID,
		PoolerDir:  poolerDir,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		Type:       clustermetadatapb.PoolerType_REPLICA,
		Database:   "postgres",
		PortMap: map[string]int32{
			"postgres": 5432,
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
