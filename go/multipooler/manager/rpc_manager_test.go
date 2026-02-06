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
	"google.golang.org/protobuf/encoding/protojson"

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

// setTermForTest writes the consensus term file directly for testing.
func setTermForTest(t *testing.T, poolerDir string, term *multipoolermanagerdatapb.ConsensusTerm) {
	t.Helper()
	data, err := protojson.Marshal(term)
	require.NoError(t, err, "failed to marshal term")
	// Write to the correct path: {poolerDir}/pg_data/consensus/consensus_term.json
	consensusDir := filepath.Join(poolerDir, "pg_data", "consensus")
	require.NoError(t, os.MkdirAll(consensusDir, 0o755), "failed to create consensus dir")
	termPath := filepath.Join(consensusDir, "consensus_term.json")
	require.NoError(t, os.WriteFile(termPath, data, 0o644), "failed to write term file")
}

// addDatabaseToTopo creates a database in the topology with a backup location
func addDatabaseToTopo(t *testing.T, ts topoclient.Store, database string) {
	t.Helper()
	ctx := context.Background()
	err := ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
		Name:             database,
		BackupLocation:   utils.FilesystemBackupLocation("/var/backups/pgbackrest"),
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

			multipooler.PoolerDir = poolerDir

			config := &Config{
				TopoClient: ts,
			}
			manager, err := NewMultiPoolerManager(logger, multipooler, config)
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

	multipooler.PoolerDir = poolerDir

	config := &Config{
		TopoClient: ts,
	}
	manager, err := NewMultiPoolerManager(logger, multipooler, config)
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
				primary := &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "zone1",
						Name:      "test-primary",
					},
					Hostname: "localhost",
					PortMap:  map[string]int32{"postgres": 5432},
				}
				return manager.SetPrimaryConnInfo(ctx, primary, false, false, 1, true)
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
			name:       "EmergencyDemote times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				_, err := manager.EmergencyDemote(ctx, 1, 5*time.Second, false)
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
				_, err := manager.Promote(ctx, 1, "", nil, false /* force */, "", "", nil, nil)
				return err
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

// expectLeadershipHistoryInsert adds a mock expectation for successful leadership history insertion.
// This is required for Promote to succeed since leadership history insertion failure now fails the promotion.
func expectLeadershipHistoryInsert(m *mock.QueryService) {
	m.AddQueryPatternOnce("INSERT INTO multigres.leadership_history", mock.MakeQueryResult(nil, nil))
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

	multipooler.PoolerDir = tmpDir

	config := &Config{
		TopoClient: ts,
		PgctldAddr: pgctldAddr,
	}
	pm, err := NewMultiPoolerManager(logger, multipooler, config)
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

	// Mock: insertLeadershipHistory - required for promotion success
	expectLeadershipHistoryInsert(mockQueryService)

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Topology is still REPLICA (this is what the guard rail checks)
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote - should detect PG is already promoted and only update topology
	resp, err := pm.Promote(ctx, 10, "0/ABCDEF0", nil, false /* force */, "", "", nil, nil)
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
	resp, err := pm.Promote(ctx, 10, "0/FEDCBA0", nil, false /* force */, "", "", nil, nil)
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
	_, err := pm.Promote(ctx, 10, "0/FEDCBA0", nil, false, "", "", nil, nil)
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

	// Mock: Clear primary_conninfo after promotion
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/FEDCBA0"}}))

	// Mock: insertLeadershipHistory - required for promotion success
	expectLeadershipHistoryInsert(mockQueryService)

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Topology shows PRIMARY (inconsistent!)
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_PRIMARY
	pm.mu.Unlock()

	// Call Promote with force=true - should fix the inconsistency
	resp, err := pm.Promote(ctx, 10, "0/FEDCBA0", nil, true, "", "", nil, nil)
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

	// Mock: Clear primary_conninfo after promotion
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/5678ABC"}}))

	// Mock: insertLeadershipHistory - required for promotion success
	expectLeadershipHistoryInsert(mockQueryService)

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Topology is REPLICA
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote - should execute all steps
	resp, err := pm.Promote(ctx, 10, "0/5678ABC", nil, false /* force */, "", "", nil, nil)
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
	_, err := pm.Promote(ctx, 10, "0/1111111", nil, false /* force */, "", "", nil, nil)
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

	pm, tmpDir := setupPromoteTestManager(t, mockQueryService)

	// Explicitly set the term to 10 to ensure we have the expected value via direct file write
	term := &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 10}
	setTermForTest(t, tmpDir, term)

	// Call Promote with wrong term (current term is 10, passing 5)
	_, err := pm.Promote(ctx, 5, "0/1234567", nil, false /* force */, "", "", nil, nil)
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

	// Mock: insertLeadershipHistory - required for first promotion call success
	// Note: second call returns early (WasAlreadyPrimary) so doesn't need this
	expectLeadershipHistoryInsert(mockQueryService)

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// First call
	resp1, err := pm.Promote(ctx, 10, "0/AAA1111", nil, false /* force */, "", "", nil, nil)
	require.NoError(t, err)
	assert.False(t, resp1.WasAlreadyPrimary)

	// Verify topology was updated to PRIMARY
	pm.mu.Lock()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.multipooler.Type)
	pm.mu.Unlock()

	// Second call should SUCCEED - topology is PRIMARY and everything is consistent (idempotent)
	// The pg_is_in_recovery pattern already returns "f" (false) since the first call consumed the "t" patterns
	resp2, err := pm.Promote(ctx, 10, "0/AAA1111", nil, false /* force */, "", "", nil, nil)
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

	// Mock: Clear primary_conninfo after promotion
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	// Mock: Get final LSN
	mockQueryService.AddQueryPatternOnce("SELECT pg_current_wal_lsn",
		mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/BBBBBBB"}}))

	// Mock: insertLeadershipHistory - required for promotion success
	expectLeadershipHistoryInsert(mockQueryService)

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote with empty expectedLSN - should skip LSN validation
	resp, err := pm.Promote(ctx, 10, "", nil, false /* force */, "", "", nil, nil)
	require.NoError(t, err, "Should succeed with empty expectedLSN")
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary)
	assert.Equal(t, "0/BBBBBBB", resp.LsnPosition)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestPromote_WithElectionMetadata tests that Promote accepts and uses election metadata fields
func TestPromote_WithElectionMetadata(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// Startup heartbeat check returns "t" (we're configured as REPLICA initially)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
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

	// Mock: insertLeadershipHistory - required for promotion success
	expectLeadershipHistoryInsert(mockQueryService)

	// Mock: Clear primary_conninfo after promotion
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Topology is REPLICA
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote with election metadata
	reason := "dead_primary"
	coordinatorID := "coordinator-1"
	cohortMembers := []string{"pooler-1", "pooler-2", "pooler-3"}
	acceptedMembers := []string{"pooler-1", "pooler-3"}

	resp, err := pm.Promote(ctx, 10, "0/1234567", nil, false /* force */, reason, coordinatorID, cohortMembers, acceptedMembers)
	require.NoError(t, err, "Promote should succeed with election metadata")
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary)
	assert.Equal(t, int64(10), resp.ConsensusTerm)
	assert.Equal(t, "0/1234567", resp.LsnPosition)

	// Verify topology was updated
	pm.mu.Lock()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.multipooler.Type)
	pm.mu.Unlock()
	assert.NoError(t, mockQueryService.ExpectationsWereMet())

	// Note: We don't directly test that insertLeadershipHistory is called with the correct metadata
	// because that would require mocking the database layer. The leadership history functionality
	// will be tested by the actual implementation. This test verifies that the Promote method
	// accepts the new parameters without error and completes successfully.
}

// TestPromote_LeadershipHistoryErrorFailsPromotion tests that an error in insertLeadershipHistory
// fails the entire Promote operation. This ensures sync replication is functioning before
// accepting the promotion - better to have no primary than one that violates durability policy.
func TestPromote_LeadershipHistoryErrorFailsPromotion(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockQueryService := mock.NewQueryService()

	// Startup heartbeat check returns "t" (we're configured as REPLICA initially)
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
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

	// Mock: Clear primary_conninfo after promotion (executed before insertLeadershipHistory)
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo",
		mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf",
		mock.MakeQueryResult(nil, nil))

	// Mock: insertLeadershipHistory fails with database error (e.g., sync replication timeout)
	mockQueryService.AddQueryPatternOnceWithError("INSERT INTO multigres.leadership_history",
		mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for synchronous replication"))

	pm, _ := setupPromoteTestManager(t, mockQueryService)

	// Topology is REPLICA
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Verify primary_term is 0 before promotion
	term, err := pm.consensusState.GetInconsistentTerm()
	require.NoError(t, err)
	require.Equal(t, int64(0), term.GetPrimaryTerm(), "primary_term should be 0 before promotion")

	// Call Promote - should FAIL because leadership history insertion fails
	resp, err := pm.Promote(ctx, 10, "0/9876543", nil, false /* force */, "test_reason", "test_coordinator", nil, nil)
	require.Error(t, err, "Promote should fail when leadership history insertion fails")
	require.Nil(t, resp)

	// Error message should indicate the leadership history failure
	assert.Contains(t, err.Error(), "leadership history")

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
	// The mock expectations should still be met (all queries were executed).
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
	defer pm.Close()

	// Mark as initialized to skip auto-restore (not testing backup functionality)
	err = pm.setInitialized()
	require.NoError(t, err)

	// Initialize consensus state
	pm.mu.Lock()
	pm.consensusState = NewConsensusState(tmpDir, serviceID)
	pm.mu.Unlock()

	// Set up mock query service for isInRecovery check during startup
	mockQueryService := mock.NewQueryService()
	// REPLICA: pg_is_in_recovery returns true (in recovery) - for startup check
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	// REPLICA: pg_is_in_recovery returns true (in recovery) - for SetPrimaryConnInfo guardrail check
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	// SetPrimaryConnInfo executes ALTER SYSTEM SET primary_conninfo
	mockQueryService.AddQueryPatternOnce("ALTER SYSTEM SET primary_conninfo", mock.MakeQueryResult(nil, nil))
	// SetPrimaryConnInfo executes pg_reload_conf()
	mockQueryService.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
	pm.qsc = &mockPoolerController{queryService: mockQueryService}

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)
	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

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
	err = pm.SetPrimaryConnInfo(ctx, primary, false, false, 1, false)
	require.NoError(t, err)

	// Verify all mock expectations were met
	assert.NoError(t, mockQueryService.ExpectationsWereMet())

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
					"wal_receiver_status",
				},
				[][]any{{"0/12345600", "0/12345678", "f", "not paused", "2025-01-01 00:00:00", "host=primary port=5432 user=repl application_name=test", "streaming"}}))

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
					"wal_receiver_status",
				},
				[][]any{{"0/12345600", "0/12345678", "f", "not paused", "2025-01-01 00:00:00", "host=primary port=5432 user=repl application_name=test", "streaming"}}))

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

func TestSetMonitorRPCEnable(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	t.Run("SetMonitor_Enable_Success", func(t *testing.T) {
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
		createPgDataDir(t, tmpDir)

		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Close() })

		err = pm.setInitialized()
		require.NoError(t, err)

		mockQueryService := mock.NewQueryService()
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

		pm.qsc = &mockPoolerController{queryService: mockQueryService}

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Disable monitor first
		pm.disableMonitorInternal()
		require.False(t, pm.pgMonitor.Running(), "Monitor should be disabled")

		// Enable monitor via RPC
		resp, err := pm.SetMonitor(ctx, &multipoolermanagerdatapb.SetMonitorRequest{Enabled: true})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, pm.pgMonitor.Running(), "Monitor should be enabled")
	})

	t.Run("SetMonitor_Enable_Idempotent", func(t *testing.T) {
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
		createPgDataDir(t, tmpDir)

		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Close() })

		err = pm.setInitialized()
		require.NoError(t, err)

		mockQueryService := mock.NewQueryService()
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

		pm.qsc = &mockPoolerController{queryService: mockQueryService}

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Monitor should already be running after Start
		require.True(t, pm.pgMonitor.Running(), "Monitor should be running after Start")

		// Enable monitor again - should be idempotent (no error, monitor still running)
		resp, err := pm.SetMonitor(ctx, &multipoolermanagerdatapb.SetMonitorRequest{Enabled: true})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, pm.pgMonitor.Running(), "Monitor should still be enabled")
	})

	t.Run("SetMonitor_Enable_WhenNotOpen", func(t *testing.T) {
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
		createPgDataDir(t, tmpDir)

		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Close() })

		// Don't start the manager - isOpen should be false

		// Try to enable monitor when manager is not open
		resp, err := pm.SetMonitor(ctx, &multipoolermanagerdatapb.SetMonitorRequest{Enabled: true})
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "manager is not open")
	})
}

func TestStopPostgresForEmergencyDemote(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	t.Run("Success_StopsPostgres", func(t *testing.T) {
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
		createPgDataDir(t, tmpDir)
		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		defer pm.Close()

		err = pm.setInitialized()
		require.NoError(t, err)

		mockQueryService := mock.NewQueryService()
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
		pm.qsc = &mockPoolerController{queryService: mockQueryService}

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Create demotion state indicating postgres is running as primary (not read-only)
		state := &demotionState{
			isReadOnly: false,
		}

		// Verify monitoring is initially running
		require.True(t, pm.pgMonitor.Running(), "Monitor should be running initially")

		// Acquire action lock before calling stopPostgresForEmergencyDemote
		lockCtx, err := pm.actionLock.Acquire(ctx, "StopPostgresForEmergencyDemote")
		require.NoError(t, err)
		defer pm.actionLock.Release(lockCtx)

		// Call stopPostgresForEmergencyDemote - should succeed
		err = pm.stopPostgresForEmergencyDemote(lockCtx, state)
		require.NoError(t, err, "Should successfully stop postgres for emergency demotion")

		// Verify monitoring remains enabled after emergency demotion to allow node to detect changes and rejoin
		require.True(t, pm.pgMonitor.Running(), "Monitor should remain running after emergency demotion to allow node to rejoin")
	})

	t.Run("Error_AlreadyInStandbyMode", func(t *testing.T) {
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
		createPgDataDir(t, tmpDir)
		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		defer pm.Close()

		err = pm.setInitialized()
		require.NoError(t, err)

		mockQueryService := mock.NewQueryService()
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
		pm.qsc = &mockPoolerController{queryService: mockQueryService}

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Create demotion state indicating postgres is already in standby mode
		state := &demotionState{
			isReadOnly: true,
		}

		// Call stopPostgresForEmergencyDemote - should fail with unexpected state error
		err = pm.stopPostgresForEmergencyDemote(ctx, state)
		require.Error(t, err, "Should fail when postgres is already in standby mode")
		assert.Contains(t, err.Error(), "unexpected state")
		assert.Contains(t, err.Error(), "standby mode")

		// Verify error code
		code := mterrors.Code(err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, code)
	})

	t.Run("Error_PgctldClientNotInitialized", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

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
		createPgDataDir(t, tmpDir)
		multipooler.PoolerDir = tmpDir

		// No pgctld address - client will not be initialized
		config := &Config{
			TopoClient: ts,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		defer pm.Close()

		// Create demotion state
		state := &demotionState{
			isReadOnly: false,
		}

		// Call stopPostgresForEmergencyDemote - should fail because pgctld client is nil
		err = pm.stopPostgresForEmergencyDemote(ctx, state)
		require.Error(t, err, "Should fail when pgctld client is not initialized")
		assert.Contains(t, err.Error(), "pgctld client not initialized")

		// Verify error code
		code := mterrors.Code(err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, code)
	})
}

func TestSetMonitorRPCDisable(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	t.Run("SetMonitor_Disable_Success", func(t *testing.T) {
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
		createPgDataDir(t, tmpDir)

		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Close() })

		err = pm.setInitialized()
		require.NoError(t, err)

		mockQueryService := mock.NewQueryService()
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

		pm.qsc = &mockPoolerController{queryService: mockQueryService}

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Monitor should be running after Start
		require.True(t, pm.pgMonitor.Running(), "Monitor should be running")

		// Disable monitor via RPC
		resp, err := pm.SetMonitor(ctx, &multipoolermanagerdatapb.SetMonitorRequest{Enabled: false})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, pm.pgMonitor.Running(), "Monitor should be disabled")
	})

	t.Run("SetMonitor_Disable_Idempotent", func(t *testing.T) {
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
		createPgDataDir(t, tmpDir)

		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		pm, err := NewMultiPoolerManager(logger, multipooler, config)
		require.NoError(t, err)
		t.Cleanup(func() { pm.Close() })

		err = pm.setInitialized()
		require.NoError(t, err)

		mockQueryService := mock.NewQueryService()
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

		pm.qsc = &mockPoolerController{queryService: mockQueryService}

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Disable monitor
		pm.disableMonitorInternal()
		require.False(t, pm.pgMonitor.Running(), "Monitor should be disabled")

		// Disable monitor again via RPC - should be idempotent
		resp, err := pm.SetMonitor(ctx, &multipoolermanagerdatapb.SetMonitorRequest{Enabled: false})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, pm.pgMonitor.Running(), "Monitor should still be disabled")
	})
}
