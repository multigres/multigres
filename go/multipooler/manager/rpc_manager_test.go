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

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/multipooler/executor/mock"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// addDatabaseToTopo creates a database in the topology with a backup location
func addDatabaseToTopo(t *testing.T, ts topoclient.Store, database string) {
	t.Helper()
	ctx := context.Background()
	err := ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
		Name:             database,
		BackupLocation:   "/var/backups/pgbackrest",
		DurabilityPolicy: "ANY_2",
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
			errorContains: "pooler type is REPLICA",
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

			config := &Config{
				TopoClient: ts,
				ServiceID:  serviceID,
				PoolerDir:  poolerDir,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			}
			manager, err := NewMultiPoolerManager(logger, config)
			require.NoError(t, err)
			defer manager.Close()

			// Set up mock query service for isInRecovery check during startup
			mockQueryService := mock.NewQueryService()
			// PRIMARY: pg_is_in_recovery returns false (not in recovery)
			// REPLICA: pg_is_in_recovery returns true (in recovery)
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

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PoolerDir:  poolerDir,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}
	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)
	defer manager.Close()

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
				return manager.SetPrimaryConnInfo(ctx, "test-primary-id", "localhost", 5432, false, false, 1, true)
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
					true,
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
			name:       "Demote times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				_, err := manager.Demote(ctx, 1, 5*time.Second, false)
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
				_, err := manager.Promote(ctx, 1, "", nil, false /* force */)
				return err
			},
		},
		{
			name:       "SetTerm times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				return manager.SetTerm(ctx, &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 5})
			},
		},
		{
			name:       "UpdateSynchronousStandbyList times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				return manager.UpdateSynchronousStandbyList(ctx, multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD, []*clustermetadatapb.ID{serviceID}, true, 0, true)
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
				manager.multipooler.MultiPooler = updatedMultipooler
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

// expectStartupQueries adds expectations for queries that happen during manager startup.
// The manager is created as a REPLICA, so pg_is_in_recovery() returns true,
// which causes the heartbeat reader to start (not writer).
// Note: Schema creation is now handled by multiorch during bootstrap initialization,
// so we no longer expect CREATE SCHEMA or CREATE TABLE queries here.
// Also note: Since we mark as initialized in these tests, we don't expect the isInitialized check.
func expectStartupQueries(m *mock.QueryService) {
	// Heartbeat startup: checks if DB is primary/replica
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
}

// createPgDataDir creates the pg_data directory with PG_VERSION file.
// This is needed for setInitialized() to work since it writes a marker file to pg_data.
func createPgDataDir(t *testing.T, poolerDir string) {
	t.Helper()
	pgDataDir := filepath.Join(poolerDir, "pg_data")
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, "PG_VERSION"), []byte("16"), 0o644))
}

// setupPromoteTestManager creates a manager configured as a REPLICA for promotion tests.
func setupPromoteTestManager(t *testing.T, mockQueryService *mock.QueryService) (*MultiPoolerManager, string) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })

	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
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

	// Create pg_data directory with PG_VERSION for SetTerm (which checks isDataDirInitialized)
	// We'll call setInitialized() later to mark as initialized
	pgDataDir := filepath.Join(tmpDir, "pg_data")
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, "PG_VERSION"), []byte("16"), 0o644))

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PgctldAddr: pgctldAddr,
		PoolerDir:  tmpDir,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}
	pm, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)
	t.Cleanup(func() { pm.Close() })

	// Mark as initialized to skip auto-restore (not testing backup functionality)
	err = pm.setInitialized()
	require.NoError(t, err)

	// Assign mock pooler controller BEFORE starting the manager to avoid race conditions
	pm.qsc = &mockPoolerController{queryService: mockQueryService}

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)

	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	// Set consensus term to expected value (10) for testing using SetTerm
	term := &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 10}
	err = pm.SetTerm(ctx, term)
	require.NoError(t, err)

	// Acquire action lock to inspect consensus state
	inspectCtx, err := pm.actionLock.Acquire(ctx, "inspect")
	require.NoError(t, err)
	currentTerm, err := pm.consensusState.GetCurrentTermNumber(inspectCtx)
	require.NoError(t, err)
	pm.actionLock.Release(inspectCtx)
	assert.Equal(t, int64(10), currentTerm, "Term should be set to 10")

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

	// Note: We don't call expectStartupQueries here because we need fine-grained control.
	// Startup heartbeat check returns "t" (we're configured as REPLICA initially)
	// But checkPromotionState should return "f" (PG is already primary)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// All subsequent calls return "f" (PostgreSQL is already primary)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: Since already promoted, get current LSN (called twice - during processing and for final response)
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/ABCDEF0"}}))
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/ABCDEF0"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Topology is still REPLICA (this is what the guard rail checks)
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote - should detect PG is already promoted and only update topology
	resp, err := pm.Promote(ctx, 10, "0/ABCDEF0", nil, false /* force */)
	require.NoError(t, err, "Should succeed - idempotent retry after partial failure")
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary, "Should not report as fully complete since topology wasn't updated")
	assert.Equal(t, int64(10), resp.ConsensusTerm)
	assert.Equal(t, "0/ABCDEF0", resp.LsnPosition)

	// Verify topology was updated
	pm.mu.Lock()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.multipooler.Type, "Topology should be updated to PRIMARY")
	pm.mu.Unlock()
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

	// Note: We don't call expectStartupQueries here because we need fine-grained control.
	// Startup heartbeat check returns "t" (we're configured as REPLICA initially)
	// But checkPromotionState should return "f" (PG is already primary)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// All subsequent calls return "f" (PostgreSQL is already primary)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: Get current LSN (since already primary)
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/FEDCBA0"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Topology is already PRIMARY
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_PRIMARY
	pm.mu.Unlock()

	// Call Promote - should succeed with WasAlreadyPrimary=true (idempotent)
	resp, err := pm.Promote(ctx, 10, "0/FEDCBA0", nil, false /* force */)
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
	expectStartupQueries(mockQueryService)

	// Mock: checkPromotionState queries pg_is_in_recovery() - returns true (still standby!)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Topology shows PRIMARY (inconsistent!)
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_PRIMARY
	pm.mu.Unlock()

	// Call Promote without force - should fail with inconsistent state error
	_, err := pm.Promote(ctx, 10, "0/FEDCBA0", nil, false)
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

	// Note: We don't call expectStartupQueries here because we need fine-grained control.
	// The sequence of pg_is_in_recovery calls is:
	// 1. Startup heartbeat check - returns "t" (consumed)
	// 2. Promote checkPromotionState - returns "t" (consumed) - still in recovery
	// 3. waitForPromotionComplete polling - returns "f" (persistent) - promotion complete
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
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

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/FEDCBA0"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Topology shows PRIMARY (inconsistent!)
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_PRIMARY
	pm.mu.Unlock()

	// Call Promote with force=true - should fix the inconsistency
	resp, err := pm.Promote(ctx, 10, "0/FEDCBA0", nil, true)
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

	// Note: We don't call expectStartupQueries here because we need fine-grained control.
	// The sequence of pg_is_in_recovery calls is:
	// 1. Startup heartbeat check - returns "t" (consumed)
	// 2. Promote checkPromotionState - returns "t" (consumed) - still in recovery
	// 3. waitForPromotionComplete polling - returns "f" (persistent) - promotion complete
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
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

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/5678ABC"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Topology is REPLICA
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote - should execute all steps
	resp, err := pm.Promote(ctx, 10, "0/5678ABC", nil, false /* force */)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary)
	assert.Equal(t, int64(10), resp.ConsensusTerm)

	// Verify topology was updated
	pm.mu.Lock()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.multipooler.Type)
	pm.mu.Unlock()
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_LSNMismatchBeforePromotion tests that promotion fails if LSN doesn't match
func TestPromoteIdempotency_LSNMismatchBeforePromotion(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()
	expectStartupQueries(mockQueryService)

	// PostgreSQL is still in recovery
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))

	// Mock: Check LSN - return different value than expected
	mockQueryService.AddQueryPatternOnce("SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}, [][]any{{"0/9999999", "t"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote with different expected LSN - should fail
	_, err := pm.Promote(ctx, 10, "0/1111111", nil, false /* force */)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LSN")
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_TermMismatch tests that promotion fails with wrong term
func TestPromoteIdempotency_TermMismatch(t *testing.T) {
	ctx := context.Background()

	// Create mock - only startup expectations needed because term validation happens before test DB queries
	mockQueryService := mock.NewQueryService()
	expectStartupQueries(mockQueryService)

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Explicitly set the term to 10 to ensure we have the expected value using SetTerm
	term := &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 10}
	err := pm.SetTerm(ctx, term)
	require.NoError(t, err)

	// Call Promote with wrong term (current term is 10, passing 5)
	_, err = pm.Promote(ctx, 5, "0/1234567", nil, false /* force */)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "term")
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromoteIdempotency_SecondCallSucceedsAfterCompletion tests that calling Promote after completion succeeds (idempotent)
func TestPromoteIdempotency_SecondCallSucceedsAfterCompletion(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// Note: We don't call expectStartupQueries here because we need fine-grained control
	// over which pg_is_in_recovery calls return "t" (in recovery) vs "f" (primary).
	// The sequence of pg_is_in_recovery calls is:
	// 1. Startup heartbeat check - returns "t" (consumed)
	// 2. Promote checkPromotionState - returns "t" (consumed)
	// 3. waitForPromotionComplete polling - returns "f" (consumed on first call)
	// 4. Second Promote call checkPromotionState - returns "f" (consumed on second call)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
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

	// Mock: Get current LSN (called twice - once after first promote, once in second call)
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/AAA1111"}}))
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/AAA1111"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// First call
	resp1, err := pm.Promote(ctx, 10, "0/AAA1111", nil, false /* force */)
	require.NoError(t, err)
	assert.False(t, resp1.WasAlreadyPrimary)

	// Verify topology was updated to PRIMARY
	pm.mu.Lock()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.multipooler.Type)
	pm.mu.Unlock()

	// Second call should SUCCEED - topology is PRIMARY and everything is consistent (idempotent)
	// The pg_is_in_recovery pattern already returns "f" (false) since the first call consumed the "t" patterns
	resp2, err := pm.Promote(ctx, 10, "0/AAA1111", nil, false /* force */)
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

	// Note: We don't call expectStartupQueries here because we need fine-grained control
	// over which pg_is_in_recovery calls return "t" (in recovery) vs "f" (primary).
	// The sequence of pg_is_in_recovery calls is:
	// 1. Startup heartbeat check - returns "t" (consumed)
	// 2. Promote checkPromotionState - returns "t" (consumed)
	// 3. waitForPromotionComplete polling - returns "f" (consumed)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// After pg_promote(), waitForPromotionComplete polls until pg_is_in_recovery returns false
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// Mock: pg_promote() call (LSN validation skipped because expectedLSN is empty)
	mockQueryService.AddQueryPatternOnce("SELECT pg_promote",
		mock.MakeQueryResult(nil, nil))

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/BBBBBBB"}}))

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote with empty expectedLSN - should skip LSN validation
	resp, err := pm.Promote(ctx, 10, "", nil, false /* force */)
	require.NoError(t, err, "Should succeed with empty expectedLSN")
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary)
	assert.Equal(t, "0/BBBBBBB", resp.LsnPosition)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
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

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
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
		config := &Config{
			TopoClient: ts,
			ServiceID:  serviceID,
			PgctldAddr: pgctldAddr,
			PoolerDir:  tmpDir,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		}
		pm, err := NewMultiPoolerManager(logger, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Close() })

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

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
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

		config := &Config{
			TopoClient: ts,
			ServiceID:  serviceID,
			PgctldAddr: pgctldAddr,
			PoolerDir:  tmpDir,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		}
		pm, err := NewMultiPoolerManager(logger, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Close() })
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
				},
				[][]any{{"0/12345600", "0/12345678", "f", "not paused", "2025-01-01 00:00:00", "host=primary port=5432 user=repl application_name=test"}}))

		pm.qsc = &mockPoolerController{queryService: mockQueryService}

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

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
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
		config := &Config{
			TopoClient: ts,
			ServiceID:  serviceID,
			PgctldAddr: pgctldAddr,
			PoolerDir:  tmpDir,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		}
		pm, err := NewMultiPoolerManager(logger, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Close() })

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
				},
				[][]any{{"0/12345600", "0/12345678", "f", "not paused", "2025-01-01 00:00:00", "host=primary port=5432 user=repl application_name=test"}}))

		pm.qsc = &mockPoolerController{queryService: mockQueryService}

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

	t.Run("Mismatch_REPLICA_topology_but_primary_postgres", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
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

		config := &Config{
			TopoClient: ts,
			ServiceID:  serviceID,
			PgctldAddr: pgctldAddr,
			PoolerDir:  tmpDir,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		}
		pm, err := NewMultiPoolerManager(logger, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Close() })
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
