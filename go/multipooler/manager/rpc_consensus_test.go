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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestBeginTerm(t *testing.T) {
	tests := []struct {
		name                string
		initialTerm         int64
		initialLeader       string
		requestTerm         int64
		requestCandidate    string
		needsReplicationLag bool
		expectedAccepted    bool
		expectedTerm        int64
		expectedLeader      string
		description         string
	}{
		{
			name:                "newer_term_resets_acceptance",
			initialTerm:         5,
			initialLeader:       "candidate-A",
			requestTerm:         10,
			requestCandidate:    "candidate-B",
			needsReplicationLag: true,
			expectedAccepted:    true,
			expectedTerm:        10,
			expectedLeader:      "candidate-B",
			description:         "Vote should be accepted when request term is newer than current term, even if already voted in older term",
		},
		{
			name:                "same_term_different_candidate_rejected",
			initialTerm:         5,
			initialLeader:       "candidate-A",
			requestTerm:         5,
			requestCandidate:    "candidate-B",
			needsReplicationLag: false,
			expectedAccepted:    false,
			expectedTerm:        5,
			expectedLeader:      "candidate-A",
			description:         "Vote should be rejected when already voted for different candidate in same term",
		},
		{
			name:                "same_term_same_candidate_idempotent",
			initialTerm:         5,
			initialLeader:       "candidate-A",
			requestTerm:         5,
			requestCandidate:    "candidate-A",
			needsReplicationLag: true,
			expectedAccepted:    true,
			expectedTerm:        5,
			expectedLeader:      "candidate-A",
			description:         "Vote should be accepted when already voted for same candidate in same term (idempotent)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pm, mock, tmpDir := setupManagerWithMockDB(t)

			// Step 1: Initialize term and vote for initial leader
			initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
				CurrentTerm: tt.initialTerm,
				AcceptedLeader: &clustermetadatapb.ID{
					Cell: "zone1",
					Name: tt.initialLeader,
				},
			}
			err := SetTerm(tmpDir, initialTerm)
			require.NoError(t, err)

			// Initialize consensus state and load the term
			err = pm.InitializeConsensusState()
			require.NoError(t, err)
			err = pm.consensusState.Load()
			require.NoError(t, err)

			// Step 2: Set up mock expectations
			mock.ExpectPing()

			// Only expect replication lag check if needed
			if tt.needsReplicationLag {
				recentTime := time.Now().Add(-5 * time.Second)
				mock.ExpectQuery("SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"last_msg_receipt_time"}).AddRow(recentTime))
			}

			// Step 3: Make the BeginTerm request
			req := &consensusdatapb.BeginTermRequest{
				Term:        tt.requestTerm,
				CandidateId: tt.requestCandidate,
				ShardId:     "shard-1",
			}

			resp, err := pm.BeginTerm(ctx, req)

			// Step 4: Verify response
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, tt.expectedAccepted, resp.Accepted, tt.description)
			assert.Equal(t, tt.expectedTerm, resp.Term)

			// Wait for async persistence to complete
			waitCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			err = pm.consensusState.WaitForPersistence(waitCtx)
			require.NoError(t, err, "Failed to wait for persistence")

			// Step 5: Verify persisted state
			loadedTerm, err := GetTerm(tmpDir)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedTerm, loadedTerm.CurrentTerm)
			assert.Equal(t, tt.expectedLeader, loadedTerm.AcceptedLeader.GetName())

			// Verify all mock expectations were met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// Helper function to setup a manager with a mock database
func setupManagerWithMockDB(t *testing.T) (*MultiPoolerManager, sqlmock.Sqlmock, string) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })

	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-voter",
	}
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
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

	senv := servenv.NewServEnv()
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

	return pm, mock, tmpDir
}

// ============================================================================
// CanReachPrimary Tests
// ============================================================================

func TestCanReachPrimary(t *testing.T) {
	tests := []struct {
		name              string
		mockSetup         func(mock sqlmock.Sqlmock)
		disableDB         bool
		requestHost       string
		requestPort       int32
		expectedReachable bool
		expectedErrorMsg  string
		description       string
	}{
		{
			name: "success_matching_host_port",
			mockSetup: func(mock sqlmock.Sqlmock) {
				conninfo := "host=localhost port=5432 user=replicator application_name=test-cell_standby-1"
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
						AddRow("streaming", conninfo))
			},
			requestHost:       "localhost",
			requestPort:       5432,
			expectedReachable: true,
			expectedErrorMsg:  "",
			description:       "Should be reachable when WAL receiver is active and connected to correct host/port",
		},
		{
			name: "no_wal_receiver",
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnError(sql.ErrNoRows)
			},
			requestHost:       "localhost",
			requestPort:       5432,
			expectedReachable: false,
			expectedErrorMsg:  "no active WAL receiver",
			description:       "Should not be reachable when there is no WAL receiver",
		},
		{
			name: "wal_receiver_stopping",
			mockSetup: func(mock sqlmock.Sqlmock) {
				conninfo := "host=localhost port=5432 user=replicator"
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
						AddRow("stopping", conninfo))
			},
			requestHost:       "localhost",
			requestPort:       5432,
			expectedReachable: false,
			expectedErrorMsg:  "WAL receiver is stopping",
			description:       "Should not be reachable when WAL receiver is stopping",
		},
		{
			name: "host_mismatch",
			mockSetup: func(mock sqlmock.Sqlmock) {
				conninfo := "host=other-host port=5432 user=replicator"
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
						AddRow("streaming", conninfo))
			},
			requestHost:       "localhost",
			requestPort:       5432,
			expectedReachable: false,
			expectedErrorMsg:  "expected localhost, got other-host",
			description:       "Should not be reachable when connected to different host",
		},
		{
			name: "port_mismatch",
			mockSetup: func(mock sqlmock.Sqlmock) {
				conninfo := "host=localhost port=5433 user=replicator"
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
						AddRow("streaming", conninfo))
			},
			requestHost:       "localhost",
			requestPort:       5432,
			expectedReachable: false,
			expectedErrorMsg:  "expected 5432, got 5433",
			description:       "Should not be reachable when connected to different port",
		},
		{
			name:              "no_database_connection",
			mockSetup:         nil,
			disableDB:         true,
			requestHost:       "localhost",
			requestPort:       5432,
			expectedReachable: false,
			expectedErrorMsg:  "database connection not available",
			description:       "Should not be reachable when database connection is not available",
		},
		{
			name: "invalid_conninfo",
			mockSetup: func(mock sqlmock.Sqlmock) {
				conninfo := "invalid format without equals"
				mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
					WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
						AddRow("streaming", conninfo))
			},
			requestHost:       "localhost",
			requestPort:       5432,
			expectedReachable: false,
			expectedErrorMsg:  "failed to parse conninfo",
			description:       "Should not be reachable when conninfo parsing fails",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pm, mock, _ := setupManagerWithMockDB(t)

			// Set up mock expectations if provided
			if tt.mockSetup != nil {
				tt.mockSetup(mock)
			}

			// Disable DB connection if needed
			if tt.disableDB {
				pm.db = nil
			}

			// Make the request
			req := &consensusdatapb.CanReachPrimaryRequest{
				PrimaryHost: tt.requestHost,
				PrimaryPort: tt.requestPort,
			}

			resp, err := pm.CanReachPrimary(ctx, req)

			// Verify response
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, tt.expectedReachable, resp.Reachable, tt.description)

			if tt.expectedErrorMsg != "" {
				assert.Contains(t, resp.ErrorMessage, tt.expectedErrorMsg)
			} else {
				assert.Empty(t, resp.ErrorMessage)
			}

			// Verify mock expectations were met (only if we have mocks)
			if !tt.disableDB && tt.mockSetup != nil {
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
		initialTerm         int64
		mockSetup           func(mock sqlmock.Sqlmock)
		disableDB           bool
		disableConsensus    bool
		expectedError       bool
		expectedErrorMsg    string
		expectedCurrentTerm int64
		expectedLeaderTerm  int64
		expectedHealthy     bool
		expectedRole        string
		checkWALPosition    func(t *testing.T, walPos *consensusdatapb.WALPosition)
		description         string
	}{
		{
			name:        "healthy_primary",
			initialTerm: 5,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Mock heartbeat query
				mock.ExpectQuery("SELECT COALESCE\\(MAX\\(leader_term\\), 0\\)").
					WillReturnRows(sqlmock.NewRows([]string{"leader_term"}).AddRow(5))
				// Mock WAL position (primary)
				mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
					WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
				mock.ExpectQuery("SELECT pg_current_wal_lsn\\(\\)").
					WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/4000000"))
				// Mock role determination
				mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
					WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
			},
			expectedCurrentTerm: 5,
			expectedLeaderTerm:  5,
			expectedHealthy:     true,
			expectedRole:        "primary",
			checkWALPosition: func(t *testing.T, walPos *consensusdatapb.WALPosition) {
				assert.Equal(t, "0/4000000", walPos.CurrentLsn)
			},
			description: "Should return healthy status for primary with correct WAL position",
		},
		{
			name:        "healthy_standby",
			initialTerm: 3,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Mock heartbeat query
				mock.ExpectQuery("SELECT COALESCE\\(MAX\\(leader_term\\), 0\\)").
					WillReturnRows(sqlmock.NewRows([]string{"leader_term"}).AddRow(5))
				// Mock WAL position (standby)
				mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
					WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
				mock.ExpectQuery("SELECT pg_last_wal_receive_lsn\\(\\), pg_last_wal_replay_lsn\\(\\)").
					WillReturnRows(sqlmock.NewRows([]string{"pg_last_wal_receive_lsn", "pg_last_wal_replay_lsn"}).
						AddRow("0/5000000", "0/4FFFFFF"))
				// Mock role determination
				mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
					WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
			},
			expectedCurrentTerm: 3,
			expectedLeaderTerm:  5,
			expectedHealthy:     true,
			expectedRole:        "replica",
			checkWALPosition: func(t *testing.T, walPos *consensusdatapb.WALPosition) {
				assert.Equal(t, "0/5000000", walPos.LastReceiveLsn)
				assert.Equal(t, "0/4FFFFFF", walPos.LastReplayLsn)
			},
			description: "Should return healthy status for standby with correct WAL positions",
		},
		{
			name:                "no_database_connection",
			initialTerm:         7,
			disableDB:           true,
			expectedCurrentTerm: 7,
			expectedLeaderTerm:  0,
			expectedHealthy:     false,
			expectedRole:        "replica",
			checkWALPosition: func(t *testing.T, walPos *consensusdatapb.WALPosition) {
				assert.Empty(t, walPos.CurrentLsn)
			},
			description: "Should return unhealthy status when DB is unavailable",
		},
		{
			name:        "database_query_failure",
			initialTerm: 4,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Mock heartbeat query failure
				mock.ExpectQuery("SELECT COALESCE\\(MAX\\(leader_term\\), 0\\)").
					WillReturnError(assert.AnError)
			},
			expectedCurrentTerm: 4,
			expectedLeaderTerm:  0,
			expectedHealthy:     false,
			expectedRole:        "",
			description:         "Should return unhealthy status when query fails",
		},
		{
			name:             "service_not_enabled",
			initialTerm:      8,
			disableConsensus: true,
			expectedError:    true,
			expectedErrorMsg: "consensus service not enabled",
			description:      "Should return error when consensus service is not enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pm, mock, tmpDir := setupManagerWithMockDB(t)

			// Setup: Initialize term file
			initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
				CurrentTerm:    tt.initialTerm,
				AcceptedLeader: nil,
			}
			err := SetTerm(tmpDir, initialTerm)
			require.NoError(t, err)

			// Initialize consensus state unless disabled
			if !tt.disableConsensus {
				err = pm.InitializeConsensusState()
				require.NoError(t, err)
				err = pm.consensusState.Load()
				require.NoError(t, err)
			} else {
				pm.mu.Lock()
				pm.consensusState = nil
				pm.mu.Unlock()
			}

			// Set up mock expectations if provided
			if tt.mockSetup != nil {
				tt.mockSetup(mock)
			}

			// Disable DB connection if needed
			if tt.disableDB {
				pm.db = nil
			}

			// Make the request
			req := &consensusdatapb.StatusRequest{
				ShardId: "test-shard",
			}

			resp, err := pm.ConsensusStatus(ctx, req)

			// Verify response
			if tt.expectedError {
				require.Error(t, err, tt.description)
				assert.Nil(t, resp)
				if tt.expectedErrorMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrorMsg)
				}
			} else {
				require.NoError(t, err, tt.description)
				require.NotNil(t, resp)
				assert.Equal(t, "test-voter", resp.PoolerId)
				assert.Equal(t, tt.expectedCurrentTerm, resp.CurrentTerm)
				assert.Equal(t, tt.expectedLeaderTerm, resp.LeaderTerm)
				assert.Equal(t, tt.expectedHealthy, resp.IsHealthy)
				assert.True(t, resp.IsEligible)
				assert.Equal(t, "zone1", resp.Cell)
				if tt.expectedRole != "" {
					assert.Equal(t, tt.expectedRole, resp.Role)
				}
				require.NotNil(t, resp.WalPosition)
				if tt.checkWALPosition != nil {
					tt.checkWALPosition(t, resp.WalPosition)
				}

				// Verify mock expectations were met (only if we have mocks)
				if !tt.disableDB && !tt.disableConsensus && tt.mockSetup != nil {
					assert.NoError(t, mock.ExpectationsWereMet())
				}
			}
		})
	}
}
