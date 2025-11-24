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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Helper function to setup a manager with a mock database
func setupManagerWithMockDB(t *testing.T) (*MultiPoolerManager, sqlmock.Sqlmock, string) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
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
		Name:      "test-pooler",
	}
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	tmpDir := t.TempDir()
	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PgctldAddr: pgctldAddr,
		PoolerDir:  tmpDir,
	}
	pm := NewMultiPoolerManager(logger, config)
	t.Cleanup(func() { pm.Close() })

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)

	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	// Create mock database connection with ping monitoring enabled
	mockDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	t.Cleanup(func() { mockDB.Close() })

	pm.db = mockDB

	// Create the pg_data directory to simulate initialized data directory
	pgDataDir := tmpDir + "/pg_data"
	err = os.MkdirAll(pgDataDir, 0o755)
	require.NoError(t, err)
	// Create PG_VERSION file to mark it as initialized
	err = os.WriteFile(pgDataDir+"/PG_VERSION", []byte("18\n"), 0o644)
	require.NoError(t, err)

	// Initialize consensus state
	pm.mu.Lock()
	pm.consensusState = NewConsensusState(tmpDir, serviceID)
	pm.mu.Unlock()

	return pm, mock, tmpDir
}

// ============================================================================
// BeginTerm Tests
// ============================================================================

func TestBeginTerm(t *testing.T) {
	tests := []struct {
		name                                string
		initialTerm                         *multipoolermanagerdatapb.ConsensusTerm
		requestTerm                         int64
		requestCandidate                    *clustermetadatapb.ID
		setupMocks                          func(mock sqlmock.Sqlmock)
		expectedAccepted                    bool
		expectedTerm                        int64
		expectedAcceptedTermFromCoordinator string
		description                         string
	}{
		{
			name: "AlreadyAcceptedLeaderInOlderTerm",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
				AcceptedTermFromCoordinatorId: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "candidate-A",
				},
			},
			requestTerm: 10,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-B",
			},
			setupMocks: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
				recentTime := time.Now().Add(-5 * time.Second)
				mock.ExpectQuery("SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"last_msg_receipt_time"}).AddRow(recentTime))
			},
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "candidate-B",
			description:                         "Acceptance should succeed when request term is newer than current term, even if already accepted leader in older term",
		},
		{
			name: "AlreadyAcceptedLeaderInSameTerm",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
				AcceptedTermFromCoordinatorId: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "candidate-A",
				},
			},
			requestTerm: 5,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-B",
			},
			setupMocks: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
			},
			expectedAccepted:                    false,
			expectedTerm:                        5,
			expectedAcceptedTermFromCoordinator: "candidate-A",
			description:                         "Acceptance should be rejected when already accepted different candidate in same term",
		},
		{
			name: "AlreadyAcceptedSameCandidateInSameTerm",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
				AcceptedTermFromCoordinatorId: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "candidate-A",
				},
			},
			requestTerm: 5,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-A",
			},
			setupMocks: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
				recentTime := time.Now().Add(-5 * time.Second)
				mock.ExpectQuery("SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"last_msg_receipt_time"}).AddRow(recentTime))
			},
			expectedAccepted:                    true,
			expectedTerm:                        5,
			expectedAcceptedTermFromCoordinator: "candidate-A",
			description:                         "Acceptance should succeed when already accepted same candidate in same term (idempotent)",
		},
	}

	// Add tests for save failure scenarios
	saveFailureTests := []struct {
		name                   string
		initialTerm            *multipoolermanagerdatapb.ConsensusTerm
		requestTerm            int64
		requestCandidate       *clustermetadatapb.ID
		setupMocks             func(mock sqlmock.Sqlmock)
		makeFilesystemReadOnly bool
		expectedError          bool
		expectedMemoryTerm     int64
		expectedMemoryLeader   string
		description            string
	}{
		{
			name: "SaveFailureDuringAcceptance_MemoryUnchanged",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber:                    5,
				AcceptedTermFromCoordinatorId: nil, // No coordinator accepted yet
			},
			requestTerm: 5,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-B",
			},
			makeFilesystemReadOnly: true,
			setupMocks: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
				recentTime := time.Now().Add(-5 * time.Second)
				mock.ExpectQuery("SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"last_msg_receipt_time"}).AddRow(recentTime))
			},
			expectedError:        true,
			expectedMemoryTerm:   5,
			expectedMemoryLeader: "", // Should remain empty after save failure
			description:          "Save failure should leave memory unchanged with original term and leader",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pm, mock, tmpDir := setupManagerWithMockDB(t)

			// Initialize term on disk
			err := setConsensusTerm(tmpDir, tt.initialTerm)
			require.NoError(t, err)

			// Load into consensus state
			loadedTermNumber, err := pm.consensusState.Load()
			require.NoError(t, err)
			assert.Equal(t, tt.initialTerm.TermNumber, loadedTermNumber, "Loaded term number should match initial term")

			// Setup mocks
			tt.setupMocks(mock)

			// Make request
			req := &consensusdatapb.BeginTermRequest{
				Term:        tt.requestTerm,
				CandidateId: tt.requestCandidate,
				ShardId:     "shard-1",
			}

			resp, err := pm.BeginTerm(ctx, req)

			// Verify response
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, tt.expectedAccepted, resp.Accepted, tt.description)
			assert.Equal(t, tt.expectedTerm, resp.Term)

			// Verify persisted state
			persistedTerm, err := getConsensusTerm(tmpDir)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedTerm, persistedTerm.TermNumber)
			assert.Equal(t, tt.expectedAcceptedTermFromCoordinator, persistedTerm.AcceptedTermFromCoordinatorId.GetName())

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}

	// Run save failure tests
	for _, tt := range saveFailureTests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pm, mock, tmpDir := setupManagerWithMockDB(t)

			// Initialize term on disk
			err := setConsensusTerm(tmpDir, tt.initialTerm)
			require.NoError(t, err)

			// Load into consensus state
			loadedTermNumber, err := pm.consensusState.Load()
			require.NoError(t, err)
			assert.Equal(t, tt.initialTerm.TermNumber, loadedTermNumber, "Loaded term number should match initial term")

			// Make filesystem read-only to simulate save failure
			if tt.makeFilesystemReadOnly {
				pgDataDir := tmpDir + "/pg_data"
				consensusDir := pgDataDir + "/consensus"
				err := os.Chmod(consensusDir, 0o555)
				require.NoError(t, err)
				// Restore permissions after test
				t.Cleanup(func() {
					_ = os.Chmod(consensusDir, 0o755)
				})
			}

			// Setup mocks
			tt.setupMocks(mock)

			// Make request
			req := &consensusdatapb.BeginTermRequest{
				Term:        tt.requestTerm,
				CandidateId: tt.requestCandidate,
				ShardId:     "shard-1",
			}

			resp, err := pm.BeginTerm(ctx, req)

			// Verify error behavior
			if tt.expectedError {
				assert.Error(t, err, tt.description)
				assert.Nil(t, resp)

				// Acquire action lock to inspect consensus state
				inspectCtx, err := pm.actionLock.Acquire(ctx, "inspect")
				require.NoError(t, err)
				defer pm.actionLock.Release(inspectCtx)

				// CRITICAL: Verify memory is unchanged despite save failure
				memoryTerm, err := pm.consensusState.GetCurrentTermNumber(inspectCtx)
				require.NoError(t, err)
				assert.Equal(t, tt.expectedMemoryTerm, memoryTerm, "Memory term should be unchanged after save failure")
				memoryLeader, err := pm.consensusState.GetAcceptedLeader(inspectCtx)
				require.NoError(t, err)
				assert.Equal(t, tt.expectedMemoryLeader, memoryLeader, "Memory leader should be unchanged after save failure")

				// Verify disk is unchanged
				loadedTerm, loadErr := getConsensusTerm(tmpDir)
				require.NoError(t, loadErr)
				assert.Equal(t, tt.expectedMemoryTerm, loadedTerm.TermNumber, "Disk term should match initial state after save failure")
				if tt.expectedMemoryLeader != "" {
					assert.Equal(t, tt.expectedMemoryLeader, loadedTerm.AcceptedTermFromCoordinatorId.GetName(), "Disk leader should match initial state after save failure")
				}
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// ============================================================================
// CanReachPrimary Tests
// ============================================================================

func TestCanReachPrimary(t *testing.T) {
	tests := []struct {
		name                  string
		requestHost           string
		requestPort           int32
		setupMocks            func(mock sqlmock.Sqlmock)
		nilDB                 bool
		expectedReachable     bool
		expectedErrorContains string
		description           string
	}{
		{
			name:        "Success_MatchingHostPort",
			requestHost: "localhost",
			requestPort: 5432,
			setupMocks: func(mock sqlmock.Sqlmock) {
				conninfo := "host=localhost port=5432 user=replicator application_name=test-cell_standby-1"
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
						AddRow("streaming", conninfo))
			},
			expectedReachable: true,
			description:       "Should be reachable when WAL receiver is active and connected to correct host/port",
		},
		{
			name:        "NoWALReceiver",
			requestHost: "localhost",
			requestPort: 5432,
			setupMocks: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnError(sql.ErrNoRows)
			},
			expectedReachable:     false,
			expectedErrorContains: "no active WAL receiver",
			description:           "Should not be reachable when there is no WAL receiver",
		},
		{
			name:        "WALReceiverStopping",
			requestHost: "localhost",
			requestPort: 5432,
			setupMocks: func(mock sqlmock.Sqlmock) {
				conninfo := "host=localhost port=5432 user=replicator"
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
						AddRow("stopping", conninfo))
			},
			expectedReachable:     false,
			expectedErrorContains: "WAL receiver is stopping",
			description:           "Should not be reachable when WAL receiver is stopping",
		},
		{
			name:        "HostMismatch",
			requestHost: "localhost",
			requestPort: 5432,
			setupMocks: func(mock sqlmock.Sqlmock) {
				conninfo := "host=other-host port=5432 user=replicator"
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
						AddRow("streaming", conninfo))
			},
			expectedReachable:     false,
			expectedErrorContains: "expected localhost, got other-host",
			description:           "Should not be reachable when connected to different host",
		},
		{
			name:        "PortMismatch",
			requestHost: "localhost",
			requestPort: 5432,
			setupMocks: func(mock sqlmock.Sqlmock) {
				conninfo := "host=localhost port=5433 user=replicator"
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
						AddRow("streaming", conninfo))
			},
			expectedReachable:     false,
			expectedErrorContains: "expected 5432, got 5433",
			description:           "Should not be reachable when connected to different port",
		},
		{
			name:                  "NoDatabaseConnection",
			requestHost:           "localhost",
			requestPort:           5432,
			nilDB:                 true,
			expectedReachable:     false,
			expectedErrorContains: "database connection not available",
			description:           "Should not be reachable when database connection is not available",
		},
		{
			name:        "InvalidConnInfo",
			requestHost: "localhost",
			requestPort: 5432,
			setupMocks: func(mock sqlmock.Sqlmock) {
				conninfo := "invalid format without equals"
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
						AddRow("streaming", conninfo))
			},
			expectedReachable:     false,
			expectedErrorContains: "failed to parse conninfo",
			description:           "Should not be reachable when conninfo parsing fails",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pm, mock, _ := setupManagerWithMockDB(t)

			if tt.nilDB {
				pm.db = nil
			} else if tt.setupMocks != nil {
				tt.setupMocks(mock)
			}

			req := &consensusdatapb.CanReachPrimaryRequest{
				PrimaryHost: tt.requestHost,
				PrimaryPort: tt.requestPort,
			}

			resp, err := pm.CanReachPrimary(ctx, req)

			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, tt.expectedReachable, resp.Reachable, tt.description)

			if tt.expectedErrorContains != "" {
				assert.Contains(t, resp.ErrorMessage, tt.expectedErrorContains)
			} else {
				assert.Empty(t, resp.ErrorMessage)
			}

			if !tt.nilDB {
				assert.NoError(t, mock.ExpectationsWereMet())
			}
		})
	}
}

// ============================================================================
// ConsensusStatus Tests
// ============================================================================

func TestConsensusStatus(t *testing.T) {
	tests := []struct {
		name                string
		initialTerm         *multipoolermanagerdatapb.ConsensusTerm
		termInMemory        bool
		nilDB               bool
		setupMocks          func(mock sqlmock.Sqlmock)
		expectedCurrentTerm int64
		expectedIsHealthy   bool
		expectedRole        string
		expectedWALLsn      string
		description         string
	}{
		{
			name: "HealthyPrimary",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
				AcceptedTermFromCoordinatorId: &clustermetadatapb.ID{
					Cell: "zone1",
					Name: "leader-node",
				},
			},
			termInMemory: true,
			setupMocks: func(mock sqlmock.Sqlmock) {
				// Single pg_is_in_recovery check determines both role and which WAL position to query
				mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
					WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
				mock.ExpectQuery("SELECT pg_current_wal_lsn\\(\\)").
					WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/4000000"))
			},
			expectedCurrentTerm: 5,
			expectedIsHealthy:   true,
			expectedRole:        "primary",
			expectedWALLsn:      "0/4000000",
			description:         "Healthy primary should return correct status with WAL position",
		},
		{
			name: "HealthyStandby",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber:                    3,
				AcceptedTermFromCoordinatorId: nil,
			},
			termInMemory: true,
			setupMocks: func(mock sqlmock.Sqlmock) {
				// Single pg_is_in_recovery check determines both role and which WAL position to query
				mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
					WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
				// queryReplicationStatus() expects full replication status query
				mock.ExpectQuery("SELECT").
					WillReturnRows(sqlmock.NewRows([]string{
						"pg_last_wal_replay_lsn",
						"pg_last_wal_receive_lsn",
						"pg_is_wal_replay_paused",
						"pg_get_wal_replay_pause_state",
						"pg_last_xact_replay_timestamp",
						"current_setting",
					}).AddRow("0/4FFFFFF", "0/5000000", false, "not paused", nil, ""))
			},
			expectedCurrentTerm: 3,
			expectedIsHealthy:   true,
			expectedRole:        "replica",
			expectedWALLsn:      "0/5000000", // receive LSN
			description:         "Healthy standby should return correct status with receive/replay LSNs",
		},
		{
			name: "NoDatabaseConnection",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber:                    7,
				AcceptedTermFromCoordinatorId: nil,
			},
			termInMemory:        true,
			nilDB:               true,
			expectedCurrentTerm: 7,
			expectedIsHealthy:   false,
			expectedRole:        "replica",
			description:         "Should handle missing database connection gracefully",
		},
		{
			name: "DatabaseQueryFailure",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber:                    4,
				AcceptedTermFromCoordinatorId: nil,
			},
			termInMemory: true,
			setupMocks: func(mock sqlmock.Sqlmock) {
				// No database queries expected - database connection exists but no queries made
			},
			expectedCurrentTerm: 4,
			expectedIsHealthy:   true,
			expectedRole:        "replica",
			description:         "Should handle database query failure gracefully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pm, mock, tmpDir := setupManagerWithMockDB(t)

			// Initialize term on disk
			err := setConsensusTerm(tmpDir, tt.initialTerm)
			require.NoError(t, err)

			// Load term into consensus state if term should be in memory
			if tt.termInMemory {
				loadedTerm, err := pm.consensusState.Load()
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCurrentTerm, loadedTerm, "Loaded term should match expected current term")
			}

			// Handle nil DB case
			if tt.nilDB {
				pm.db = nil
			} else if tt.setupMocks != nil {
				tt.setupMocks(mock)
			}

			req := &consensusdatapb.StatusRequest{
				ShardId: "test-shard",
			}

			resp, err := pm.ConsensusStatus(ctx, req)

			// Verify response
			require.NoError(t, err, tt.description)
			require.NotNil(t, resp)
			assert.Equal(t, "test-pooler", resp.PoolerId)
			assert.Equal(t, tt.expectedCurrentTerm, resp.CurrentTerm)
			assert.Equal(t, tt.expectedIsHealthy, resp.IsHealthy, tt.description)
			assert.True(t, resp.IsEligible)
			assert.Equal(t, "zone1", resp.Cell)
			assert.Equal(t, tt.expectedRole, resp.Role)

			// Verify WAL position if expected
			require.NotNil(t, resp.WalPosition)
			if tt.expectedWALLsn != "" {
				if tt.expectedRole == "primary" {
					assert.Equal(t, tt.expectedWALLsn, resp.WalPosition.CurrentLsn)
				} else if tt.expectedRole == "replica" && tt.expectedIsHealthy {
					assert.Equal(t, tt.expectedWALLsn, resp.WalPosition.LastReceiveLsn)
				}
			}

			// Verify term was loaded if applicable
			if !tt.termInMemory && !tt.nilDB {
				// Acquire action lock to inspect consensus state
				inspectCtx, err := pm.actionLock.Acquire(ctx, "inspect")
				require.NoError(t, err)
				currentTerm, err := pm.consensusState.GetCurrentTermNumber(inspectCtx)
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCurrentTerm, currentTerm, "Term should be loaded into memory")
				pm.actionLock.Release(inspectCtx)
			}

			if !tt.nilDB {
				assert.NoError(t, mock.ExpectationsWereMet())
			}
		})
	}
}
