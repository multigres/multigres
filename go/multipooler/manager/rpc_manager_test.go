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
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
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
			errorContains: "database connection failed", // Will fail on DB connection, not type check
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
				return manager.SetPrimaryConnInfo(ctx, "localhost", 5432, false, false, 1, true)
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
				_, err := manager.UndoDemote(ctx, 0)
				return err
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

// newMockDB creates a mock database for testing and registers cleanup.
func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	mockDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	t.Cleanup(func() { mockDB.Close() })
	return mockDB, mock
}

// expectStartupQueries adds expectations for queries that happen during manager startup.
// The manager is created as a REPLICA, so pg_is_in_recovery() returns true,
// which causes the heartbeat reader to start (not writer).
// Note: Schema creation is now handled by multiorch during bootstrap initialization,
// so we no longer expect CREATE SCHEMA or CREATE TABLE queries here.
// Also note: Since we mark as initialized in these tests, we don't expect the isInitialized check.
func expectStartupQueries(mock sqlmock.Sqlmock) {
	// Heartbeat startup: checks if DB is primary/replica
	mock.ExpectQuery("SELECT pg_is_in_recovery").
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
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
func setupPromoteTestManager(t *testing.T, mockDB *sql.DB) (*MultiPoolerManager, string) {
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

	// Assign mock DB BEFORE starting the manager to avoid race conditions
	pm.db = mockDB

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
	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// Mock: checkPromotionState queries pg_is_in_recovery() - returns false (already promoted)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))

	// Mock: Since already promoted, get current LSN
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()::text")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/ABCDEF0"))

	// Mock: Get final LSN (after topology update)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()::text")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/ABCDEF0"))

	pm, _ := setupPromoteTestManager(t, mockDB)

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

	require.NoError(t, mock.ExpectationsWereMet())
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
	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// Mock: checkPromotionState queries pg_is_in_recovery() - returns false (already primary)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))

	// Mock: Get current LSN (since already primary)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()::text")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/FEDCBA0"))

	pm, _ := setupPromoteTestManager(t, mockDB)

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

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPromoteIdempotency_InconsistentStateTopologyPrimaryPgNotPrimary tests error when topology is PRIMARY but PG is not
func TestPromoteIdempotency_InconsistentStateTopologyPrimaryPgNotPrimary(t *testing.T) {
	ctx := context.Background()

	// Simulate inconsistent state (should never happen):
	// 1. PostgreSQL is still in recovery (standby)
	// 2. Topology shows PRIMARY
	// This indicates a serious problem that requires manual intervention

	// Create mock and set ALL expectations BEFORE starting the manager
	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// Mock: checkPromotionState queries pg_is_in_recovery() - returns true (still standby!)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))

	pm, _ := setupPromoteTestManager(t, mockDB)

	// Topology shows PRIMARY (inconsistent!)
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_PRIMARY
	pm.mu.Unlock()

	// Call Promote without force - should fail with inconsistent state error
	_, err := pm.Promote(ctx, 10, "0/FEDCBA0", nil, false)
	require.Error(t, err, "Should fail due to inconsistent state without force flag")
	assert.Contains(t, err.Error(), "inconsistent state")
	assert.Contains(t, err.Error(), "Manual intervention required")

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPromoteIdempotency_InconsistentStateFixedWithForce tests that force flag fixes inconsistent state
func TestPromoteIdempotency_InconsistentStateFixedWithForce(t *testing.T) {
	ctx := context.Background()

	// Simulate inconsistent state:
	// 1. PostgreSQL is still in recovery (standby)
	// 2. Topology shows PRIMARY
	// With force=true, it should complete the missing promotion steps

	// Create mock and set ALL expectations BEFORE starting the manager
	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// Mock: checkPromotionState queries pg_is_in_recovery() - returns true (still standby!)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))

	// Mock: Validate expected LSN
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_last_wal_replay_lsn()::text, pg_is_wal_replay_paused()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}).AddRow("0/FEDCBA0", true))

	// Mock: pg_promote() call to fix the inconsistency
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_promote()")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Mock: Wait for promotion
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))

	// Mock: Get final LSN
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()::text")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/FEDCBA0"))

	pm, _ := setupPromoteTestManager(t, mockDB)

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

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPromoteIdempotency_NothingCompleteYet tests promotion from scratch
func TestPromoteIdempotency_NothingCompleteYet(t *testing.T) {
	ctx := context.Background()

	// Simulate fresh promotion - nothing done yet:
	// 1. PostgreSQL is still in recovery (standby)
	// 2. Topology is REPLICA
	// 3. No sync replication configured

	// Create mock and set ALL expectations BEFORE starting the manager
	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// Mock: pg_is_in_recovery() returns true (still standby)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))

	// Mock: Validate expected LSN (pg_last_wal_replay_lsn + pg_is_wal_replay_paused)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_last_wal_replay_lsn()::text, pg_is_wal_replay_paused()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}).AddRow("0/5678ABC", true))

	// Mock: pg_promote() call
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_promote()")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Mock: Wait for promotion - first check returns true (still in recovery)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))

	// Mock: Second check returns false (promotion complete)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))

	// Mock: Get final LSN
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()::text")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/5678ABC"))

	pm, _ := setupPromoteTestManager(t, mockDB)

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

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPromoteIdempotency_LSNMismatchBeforePromotion tests that promotion fails if LSN doesn't match
func TestPromoteIdempotency_LSNMismatchBeforePromotion(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// PostgreSQL is still in recovery
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))

	// Mock: Check LSN - return different value than expected
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_last_wal_replay_lsn()::text, pg_is_wal_replay_paused()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}).AddRow("0/9999999", true))

	pm, _ := setupPromoteTestManager(t, mockDB)

	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote with different expected LSN - should fail
	_, err := pm.Promote(ctx, 10, "0/1111111", nil, false /* force */)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LSN")

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPromoteIdempotency_TermMismatch tests that promotion fails with wrong term
func TestPromoteIdempotency_TermMismatch(t *testing.T) {
	ctx := context.Background()

	// Create mock - only startup expectations needed because term validation happens before test DB queries
	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	pm, _ := setupPromoteTestManager(t, mockDB)

	// Explicitly set the term to 10 to ensure we have the expected value using SetTerm
	term := &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 10}
	err := pm.SetTerm(ctx, term)
	require.NoError(t, err)

	// Call Promote with wrong term (current term is 10, passing 5)
	_, err = pm.Promote(ctx, 5, "0/1234567", nil, false /* force */)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "term")

	// No database queries should happen if term validation fails
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPromoteIdempotency_SecondCallSucceedsAfterCompletion tests that calling Promote after completion succeeds (idempotent)
func TestPromoteIdempotency_SecondCallSucceedsAfterCompletion(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// Setup for first call - complete promotion
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_last_wal_replay_lsn()::text, pg_is_wal_replay_paused()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}).AddRow("0/AAA1111", true))
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_promote()")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()::text")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/AAA1111"))

	// Setup for second call - everything already complete
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()::text")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/AAA1111"))

	pm, _ := setupPromoteTestManager(t, mockDB)

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
	resp2, err := pm.Promote(ctx, 10, "0/AAA1111", nil, false /* force */)
	require.NoError(t, err, "Second call should succeed - idempotent operation")
	assert.True(t, resp2.WasAlreadyPrimary, "Second call should report as already primary")
	assert.Equal(t, "0/AAA1111", resp2.LsnPosition)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPromoteIdempotency_EmptyExpectedLSNSkipsValidation tests that empty expectedLSN skips validation
func TestPromoteIdempotency_EmptyExpectedLSNSkipsValidation(t *testing.T) {
	ctx := context.Background()

	// Create mock and set ALL expectations BEFORE starting the manager
	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// PostgreSQL is still in recovery
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))

	// Mock: pg_promote() call (LSN validation skipped because expectedLSN is empty)
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_promote()")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Mock: Wait for promotion
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))

	// Mock: Get final LSN
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()::text")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/BBBBBBB"))

	pm, _ := setupPromoteTestManager(t, mockDB)

	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	pm.mu.Unlock()

	// Call Promote with empty expectedLSN - should skip LSN validation
	resp, err := pm.Promote(ctx, 10, "", nil, false /* force */)
	require.NoError(t, err, "Should succeed with empty expectedLSN")
	require.NotNil(t, resp)

	assert.False(t, resp.WasAlreadyPrimary)
	assert.Equal(t, "0/BBBBBBB", resp.LsnPosition)

	require.NoError(t, mock.ExpectationsWereMet())
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

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Create mock database and inject it
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		pm.db = db

		// Status() calls isPrimary() multiple times via different code paths:
		// 1. getRole() -> isPrimary() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
		// 2. getWALPosition() -> isPrimary() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
		// 3. getWALPosition() -> getPrimaryLSN() (since isPrimary=true)
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/12345678"))
		// 4. Direct isPrimary() call in Status() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
		// 5. getPrimaryStatusInternal() -> getPrimaryLSN()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/12345678"))
		// 6. getPrimaryStatusInternal() -> getConnectedFollowerIDs()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT application_name")).
			WillReturnRows(sqlmock.NewRows([]string{"application_name"}))
		// 7. getPrimaryStatusInternal() -> getSynchronousReplicationConfig()
		mock.ExpectQuery(regexp.QuoteMeta("SHOW synchronous_standby_names")).
			WillReturnRows(sqlmock.NewRows([]string{"synchronous_standby_names"}).AddRow(""))
		// 8. getSynchronousReplicationConfig() -> SHOW synchronous_commit
		mock.ExpectQuery(regexp.QuoteMeta("SHOW synchronous_commit")).
			WillReturnRows(sqlmock.NewRows([]string{"synchronous_commit"}).AddRow("on"))

		// Call ReplicationStatus
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// Verify response structure
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.PoolerType)
		assert.NotNil(t, status.PrimaryStatus, "PrimaryStatus should be populated")
		assert.Nil(t, status.ReplicationStatus, "ReplicationStatus should be nil for PRIMARY")
		assert.Equal(t, "0/12345678", status.PrimaryStatus.Lsn)

		require.NoError(t, mock.ExpectationsWereMet())
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

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Create mock database and inject it
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		pm.db = db

		// Status() calls isPrimary() multiple times via different code paths:
		// 1. getRole() -> isPrimary() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
		// 2. getWALPosition() -> isPrimary() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
		// 3. getWALPosition() -> getStandbyReplayLSN() (since isPrimary=false)
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_last_wal_replay_lsn()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_last_wal_replay_lsn"}).AddRow("0/12345600"))
		// 4. Direct isPrimary() call in Status() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
		// 5. getStandbyStatusInternal() -> queryReplicationStatus()
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT
		pg_last_wal_replay_lsn(),
		pg_last_wal_receive_lsn(),
		pg_is_wal_replay_paused(),
		pg_get_wal_replay_pause_state(),
		pg_last_xact_replay_timestamp(),
		current_setting('primary_conninfo')`)).
			WillReturnRows(sqlmock.NewRows([]string{
				"pg_last_wal_replay_lsn",
				"pg_last_wal_receive_lsn",
				"pg_is_wal_replay_paused",
				"pg_get_wal_replay_pause_state",
				"pg_last_xact_replay_timestamp",
				"primary_conninfo",
			}).AddRow("0/12345600", "0/12345678", false, "not paused", "2025-01-01 00:00:00", "host=primary port=5432 user=repl application_name=test"))

		// Call ReplicationStatus
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// Verify response structure
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.PoolerType)
		assert.Nil(t, status.PrimaryStatus, "PrimaryStatus should be nil for REPLICA")
		assert.NotNil(t, status.ReplicationStatus, "ReplicationStatus should be populated")
		assert.Equal(t, "0/12345600", status.ReplicationStatus.LastReplayLsn)

		require.NoError(t, mock.ExpectationsWereMet())
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

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Create mock database and inject it
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		pm.db = db

		// Status() calls isPrimary() multiple times via different code paths:
		// PostgreSQL is actually a standby (pg_is_in_recovery = true)
		// 1. getRole() -> isPrimary() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
		// 2. getWALPosition() -> isPrimary() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
		// 3. getWALPosition() -> getStandbyReplayLSN() (since isPrimary=false)
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_last_wal_replay_lsn()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_last_wal_replay_lsn"}).AddRow("0/12345600"))
		// 4. Direct isPrimary() call in Status() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
		// 5. getStandbyStatusInternal() -> queryReplicationStatus()
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT
		pg_last_wal_replay_lsn(),
		pg_last_wal_receive_lsn(),
		pg_is_wal_replay_paused(),
		pg_get_wal_replay_pause_state(),
		pg_last_xact_replay_timestamp(),
		current_setting('primary_conninfo')`)).
			WillReturnRows(sqlmock.NewRows([]string{
				"pg_last_wal_replay_lsn",
				"pg_last_wal_receive_lsn",
				"pg_is_wal_replay_paused",
				"pg_get_wal_replay_pause_state",
				"pg_last_xact_replay_timestamp",
				"primary_conninfo",
			}).AddRow("0/12345600", "0/12345678", false, "not paused", "2025-01-01 00:00:00", "host=primary port=5432 user=repl application_name=test"))

		// Call Status - now returns status with mismatch observable
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// PoolerType from topology says PRIMARY, but status shows standby state
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.PoolerType)
		assert.Nil(t, status.PrimaryStatus, "PrimaryStatus should be nil since PostgreSQL is a standby")
		assert.NotNil(t, status.ReplicationStatus, "ReplicationStatus should be populated since PostgreSQL is a standby")

		require.NoError(t, mock.ExpectationsWereMet())
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

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Create mock database and inject it
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		pm.db = db

		// Status() calls isPrimary() multiple times via different code paths:
		// PostgreSQL is actually a primary (pg_is_in_recovery = false)
		// 1. getRole() -> isPrimary() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
		// 2. getWALPosition() -> isPrimary() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
		// 3. getWALPosition() -> getPrimaryLSN() (since isPrimary=true)
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/12345678"))
		// 4. Direct isPrimary() call in Status() -> pg_is_in_recovery
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_is_in_recovery()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
		// 5. getPrimaryStatusInternal() -> getPrimaryLSN()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_current_wal_lsn()")).
			WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/12345678"))
		// 6. getPrimaryStatusInternal() -> getConnectedFollowerIDs()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT application_name")).
			WillReturnRows(sqlmock.NewRows([]string{"application_name"}))
		// 7. getPrimaryStatusInternal() -> getSynchronousReplicationConfig()
		mock.ExpectQuery(regexp.QuoteMeta("SHOW synchronous_standby_names")).
			WillReturnRows(sqlmock.NewRows([]string{"synchronous_standby_names"}).AddRow(""))
		// 8. getSynchronousReplicationConfig() -> SHOW synchronous_commit
		mock.ExpectQuery(regexp.QuoteMeta("SHOW synchronous_commit")).
			WillReturnRows(sqlmock.NewRows([]string{"synchronous_commit"}).AddRow("on"))

		// Call Status - now returns status with mismatch observable
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// PoolerType from topology says REPLICA, but status shows primary state
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.PoolerType)
		assert.NotNil(t, status.PrimaryStatus, "PrimaryStatus should be populated since PostgreSQL is a primary")
		assert.Nil(t, status.ReplicationStatus, "ReplicationStatus should be nil since PostgreSQL is a primary")

		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// UndoDemote Tests
// =============================================================================

// setupUndoDemoteTestManager creates a manager configured as a demoted primary
// (topology shows REPLICA) for undo demotion tests.
// Returns the manager and the mock pgctld service for configuring restart behavior.
func setupUndoDemoteTestManager(t *testing.T, mockDB *sql.DB) (*MultiPoolerManager, *testutil.MockPgCtldService) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })

	// Create mock pgctld service that we can configure
	mockPgctld := &testutil.MockPgCtldService{}
	pgctldClient, cleanupPgctld := testutil.StartTestServer(t, mockPgctld)
	t.Cleanup(cleanupPgctld)

	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-demoted-primary",
	}

	// Create as REPLICA (simulating a demoted primary)
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_REPLICA,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING_RDONLY,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	tmpDir := t.TempDir()
	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PoolerDir:  tmpDir,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}
	pm, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)
	t.Cleanup(func() { pm.Close() })

	// Inject the pgctld client directly
	pm.pgctldClient = pgctldClient

	// Assign mock DB BEFORE starting the manager
	pm.db = mockDB

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)

	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	// Create pg_data directory
	pgDataDir := tmpDir + "/pg_data"
	err = os.MkdirAll(pgDataDir, 0o755)
	require.NoError(t, err)
	err = os.WriteFile(pgDataDir+"/PG_VERSION", []byte("18\n"), 0o644)
	require.NoError(t, err)

	return pm, mockPgctld
}

// TestUndoDemote_AlreadyPrimary tests that UndoDemote returns success with WasAlreadyPrimary=true
// when PostgreSQL is already running as a primary (idempotent behavior).
func TestUndoDemote_AlreadyPrimary(t *testing.T) {
	ctx := context.Background()

	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// queryUndoDemoteState: single query returns all state
	// PostgreSQL is NOT in recovery (already primary)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT\n\t\tpg_is_in_recovery(),\n\t\t(SELECT timeline_id FROM pg_control_checkpoint()),\n\t\tCOALESCE(pg_current_wal_lsn()::text, pg_last_wal_replay_lsn()::text, '')")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "timeline_id", "lsn"}).
			AddRow(false, 1, "0/ABCDEF0"))

	pm, _ := setupUndoDemoteTestManager(t, mockDB)

	// Call UndoDemote - should detect already primary and return idempotent success
	resp, err := pm.UndoDemote(ctx, 1)
	require.NoError(t, err, "Should succeed - already primary (idempotent)")
	require.NotNil(t, resp)

	assert.True(t, resp.WasAlreadyPrimary, "Should report as already primary")
	assert.Equal(t, "0/ABCDEF0", resp.LsnPosition)
	assert.Equal(t, int32(1), resp.TimelineId)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestUndoDemote_TimelineMismatch tests that UndoDemote returns an error when the timeline
// has changed, indicating another node may have been promoted.
func TestUndoDemote_TimelineMismatch(t *testing.T) {
	ctx := context.Background()

	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// queryUndoDemoteState: PostgreSQL is in recovery (standby)
	// Timeline is 2, but we expect 1
	mock.ExpectQuery(regexp.QuoteMeta("SELECT\n\t\tpg_is_in_recovery(),\n\t\t(SELECT timeline_id FROM pg_control_checkpoint()),\n\t\tCOALESCE(pg_current_wal_lsn()::text, pg_last_wal_replay_lsn()::text, '')")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "timeline_id", "lsn"}).
			AddRow(true, 2, "0/FEDCBA0"))

	pm, _ := setupUndoDemoteTestManager(t, mockDB)

	// Call UndoDemote with expected timeline 1, but actual is 2
	resp, err := pm.UndoDemote(ctx, 1)
	require.Error(t, err, "Should fail due to timeline mismatch")
	require.Nil(t, resp)

	// Verify error code and message
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "timeline mismatch")
	assert.Contains(t, err.Error(), "expected 1")
	assert.Contains(t, err.Error(), "current 2")

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestUndoDemote_DatabaseConnectionFailed tests error handling when database connection fails.
func TestUndoDemote_DatabaseConnectionFailed(t *testing.T) {
	ctx := context.Background()

	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// queryUndoDemoteState: Database query fails
	mock.ExpectQuery(regexp.QuoteMeta("SELECT\n\t\tpg_is_in_recovery(),\n\t\t(SELECT timeline_id FROM pg_control_checkpoint()),\n\t\tCOALESCE(pg_current_wal_lsn()::text, pg_last_wal_replay_lsn()::text, '')")).
		WillReturnError(sql.ErrConnDone)

	pm, _ := setupUndoDemoteTestManager(t, mockDB)

	resp, err := pm.UndoDemote(ctx, 1)
	require.Error(t, err)
	require.Nil(t, resp)
	assert.Contains(t, err.Error(), "failed to query undo demote state")

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestUndoDemote_RestartFailed tests error handling when pgctld restart fails.
func TestUndoDemote_RestartFailed(t *testing.T) {
	ctx := context.Background()

	mockDB, mock := newMockDB(t)
	expectStartupQueries(mock)

	// Initial state: PostgreSQL is in recovery
	mock.ExpectQuery(regexp.QuoteMeta("SELECT\n\t\tpg_is_in_recovery(),\n\t\t(SELECT timeline_id FROM pg_control_checkpoint()),\n\t\tCOALESCE(pg_current_wal_lsn()::text, pg_last_wal_replay_lsn()::text, '')")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "timeline_id", "lsn"}).
			AddRow(true, 1, "0/1234567"))

	pm, mockPgctld := setupUndoDemoteTestManager(t, mockDB)

	// Configure pgctld to fail on restart
	mockPgctld.RestartError = mterrors.New(mtrpcpb.Code_INTERNAL, "mock restart failed")

	resp, err := pm.UndoDemote(ctx, 1)
	require.Error(t, err)
	require.Nil(t, resp)
	assert.Contains(t, err.Error(), "failed to restart as primary")

	require.NoError(t, mock.ExpectationsWereMet())
}
