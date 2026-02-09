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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/multipooler/executor/mock"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// Helper function to setup a manager with a mock database
// expectPrimaryStartupQueries adds expectations for queries that happen during PRIMARY manager startup.
// The manager is created as a PRIMARY, so it checks pg_is_in_recovery() which returns false.
// Note: Schema creation is now handled by multiorch during bootstrap initialization,
// so we no longer expect CREATE SCHEMA or CREATE TABLE queries here.
func expectPrimaryStartupQueries(m *mock.QueryService) {
	// Heartbeat startup: checks if DB is primary (consumed once so test-specific patterns take precedence)
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
}

func expectStandbyStartupQueries(m *mock.QueryService) {
	// Heartbeat startup: checks if DB is standby (consumed once so test-specific patterns take precedence)
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
}

func setupManagerWithMockDB(t *testing.T, mockQueryService *mock.QueryService) (*MultiPoolerManager, string) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
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
		Name:      "test-pooler",
	}
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

	// Assign mock pooler controller BEFORE starting the manager to avoid race conditions
	pm.qsc = &mockPoolerController{queryService: mockQueryService}

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	pm.Start(senv)

	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

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

	return pm, tmpDir
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
		action                              consensusdatapb.BeginTermAction
		setupMocks                          func(*mock.QueryService)
		expectedError                       bool
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
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
			setupMocks: func(m *mock.QueryService) {
				// executeRevoke: health check
				m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
				// executeRevoke: determine role (standby)
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				// executeRevoke: pauseReplication
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
				// executeRevoke: queryReplicationStatus (WAL position captured after pause)
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status"},
					[][]any{{"0/2000000", "0/2000000", false, "not paused", nil, "", "streaming"}}))
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
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
			setupMocks: func(m *mock.QueryService) {
				// Term not accepted - Phase 2 never runs, no queries expected
			},
			expectedAccepted:                    false,
			expectedTerm:                        5,
			expectedAcceptedTermFromCoordinator: "candidate-A",
			description:                         "Acceptance should be rejected when already accepted different candidate in same term",
		},
		{
			name:   "AlreadyAcceptedSameCandidateInSameTerm",
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
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
			setupMocks: func(m *mock.QueryService) {
				// executeRevoke: health check
				m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
				// executeRevoke: determine role (standby)
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				// executeRevoke: pauseReplication
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
				// executeRevoke: queryReplicationStatus (WAL position captured after pause)
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status"},
					[][]any{{"0/3000000", "0/3000000", false, "not paused", nil, "", "streaming"}}))
			},
			expectedAccepted:                    true,
			expectedTerm:                        5,
			expectedAcceptedTermFromCoordinator: "candidate-A",
			description:                         "Acceptance should succeed when already accepted same candidate in same term (idempotent)",
		},
		{
			name:   "PrimaryRejectTermWhenDemotionFails",
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
			},
			requestTerm: 10,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "new-candidate",
			},
			setupMocks: func(m *mock.QueryService) {
				// executeRevoke: health check
				m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
				// executeRevoke: isInRecovery check - returns false (not in recovery = primary)
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
				// executeRevoke: demoteLocked fails at checkDemotionState or another early step
				// Simulate failure by not setting up expected queries for demotion steps
			},
			expectedError:                       true,
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "new-candidate",
			description:                         "Primary should accept term even when demotion fails",
		},
		{
			name:   "PrimaryAcceptsTermAfterSuccessfulDemotion",
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
			},
			requestTerm: 10,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "new-candidate",
			},
			setupMocks: func(m *mock.QueryService) {
				// executeRevoke: health check
				m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
				// executeRevoke: isInRecovery check - returns true (in recovery = standby/demoted)
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				// executeRevoke: pauseReplication - ALTER SYSTEM RESET primary_conninfo
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				// executeRevoke: pauseReplication - pg_reload_conf
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
				// executeRevoke: queryReplicationStatus (WAL position captured after pause)
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status"},
					[][]any{{"0/4000000", "0/4000000", false, "not paused", nil, "", "streaming"}}))
			},
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "new-candidate",
			description:                         "Primary should accept term after successful demotion (idempotent case - already demoted)",
		},
		{
			name:   "StandbyAcceptsTermAndPausesReplication",
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
			},
			requestTerm: 10,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "new-candidate",
			},
			setupMocks: func(m *mock.QueryService) {
				// executeRevoke: health check
				m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
				// executeRevoke: isInRecovery check - returns true (in recovery = standby)
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				// executeRevoke: pauseReplication - ALTER SYSTEM RESET primary_conninfo
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				// executeRevoke: pauseReplication - pg_reload_conf
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
				// executeRevoke: queryReplicationStatus (WAL position captured after pause)
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status"},
					[][]any{{"0/5000000", "0/5000000", false, "not paused", nil, "", "streaming"}}))
			},
			expectedError:                       false,
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "new-candidate",
			description:                         "Standby accepts term with REVOKE action and pauses replication",
		},
		{
			name:   "StandbyPausesReplicationWhenAcceptingNewTerm",
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
			},
			requestTerm: 10,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "new-candidate",
			},
			setupMocks: func(m *mock.QueryService) {
				// executeRevoke: health check
				m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
				// executeRevoke: isInRecovery check - returns true (standby)
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				// executeRevoke: pauseReplication - ALTER SYSTEM RESET primary_conninfo
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				// executeRevoke: pauseReplication - pg_reload_conf
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
				// executeRevoke: queryReplicationStatus (WAL position captured after pause)
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status"},
					[][]any{{"0/6000000", "0/6000000", false, "not paused", nil, "", "streaming"}}))
			},
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "new-candidate",
			description:                         "Standby should pause replication when accepting new term",
		},
		{
			name: "NoAction_AcceptsTermWithoutRevoke",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
			},
			requestTerm: 10,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "new-candidate",
			},
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION,
			setupMocks: func(m *mock.QueryService) {
				// NO_ACTION: No queries should be executed
			},
			expectedError:                       false,
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "new-candidate",
			description:                         "NO_ACTION accepts term without executing revoke",
		},
		{
			name: "NoAction_AcceptsTermEvenWhenPostgresDown",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
			},
			requestTerm: 10,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "new-candidate",
			},
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION,
			setupMocks: func(m *mock.QueryService) {
				// NO_ACTION: No queries, even if postgres is down
			},
			expectedError:                       false,
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "new-candidate",
			description:                         "NO_ACTION accepts term even when postgres is unhealthy",
		},
		{
			name: "NoAction_RejectsOutdatedTerm",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 10,
			},
			requestTerm: 5,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "old-candidate",
			},
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION,
			setupMocks: func(m *mock.QueryService) {
				// NO_ACTION: No queries
			},
			expectedError:                       false,
			expectedAccepted:                    false,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "",
			description:                         "NO_ACTION still respects term acceptance rules",
		},
		{
			name:   "PostgresDown_AcceptsTermButRevokeFails",
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
			},
			requestTerm: 10,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "new-candidate",
			},
			setupMocks: func(m *mock.QueryService) {
				// executeRevoke: health check FAILS - postgres is down
				// DO NOT add SELECT 1 expectation - let it fail
			},
			expectedError:                       true, // Revoke fails because postgres is down
			expectedAccepted:                    true, // Term IS accepted (acceptance happens before revoke)
			expectedTerm:                        10,   // Term advances to 10
			expectedAcceptedTermFromCoordinator: "new-candidate",
			description:                         "Node accepts term but revoke fails when postgres is down",
		},
	}

	// Add tests for save failure scenarios
	saveFailureTests := []struct {
		name                   string
		initialTerm            *multipoolermanagerdatapb.ConsensusTerm
		requestTerm            int64
		requestCandidate       *clustermetadatapb.ID
		action                 consensusdatapb.BeginTermAction
		setupMocks             func(*mock.QueryService)
		makeFilesystemReadOnly bool
		expectedError          bool
		expectedAccepted       bool
		expectedRespTerm       int64
		checkMemoryUnchanged   bool
		expectedMemoryTerm     int64
		expectedMemoryLeader   string
		description            string
	}{
		{
			name:   "SaveFailureDuringAcceptance_MemoryUnchanged",
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
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
			setupMocks: func(m *mock.QueryService) {
			},
			expectedError:        false,
			expectedAccepted:     false,
			expectedRespTerm:     5,
			checkMemoryUnchanged: true,
			expectedMemoryTerm:   5,
			expectedMemoryLeader: "", // Should remain empty after save failure
			description:          "Save failure should leave memory unchanged with original term and leader",
		},
		{
			name:   "NoAction_SaveFailureDuringAcceptance_MemoryUnchanged",
			action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION,
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber:                    5,
				AcceptedTermFromCoordinatorId: nil,
			},
			requestTerm: 5,
			requestCandidate: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-B",
			},
			makeFilesystemReadOnly: true,
			setupMocks: func(m *mock.QueryService) {
				// NO_ACTION: No queries
			},
			expectedError:        false,
			expectedAccepted:     false,
			expectedRespTerm:     5,
			checkMemoryUnchanged: true,
			expectedMemoryTerm:   5,
			expectedMemoryLeader: "",
			description:          "NO_ACTION: Save failure should leave memory unchanged",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create mock and set ALL expectations BEFORE starting the manager
			mockQueryService := mock.NewQueryService()
			expectPrimaryStartupQueries(mockQueryService)
			tt.setupMocks(mockQueryService)

			pm, tmpDir := setupManagerWithMockDB(t, mockQueryService)

			// Initialize term on disk
			err := setConsensusTerm(tmpDir, tt.initialTerm)
			require.NoError(t, err)

			// Load into consensus state
			loadedTermNumber, err := pm.consensusState.Load()
			require.NoError(t, err)
			assert.Equal(t, tt.initialTerm.TermNumber, loadedTermNumber, "Loaded term number should match initial term")

			// Make request
			req := &consensusdatapb.BeginTermRequest{
				Term:        tt.requestTerm,
				CandidateId: tt.requestCandidate,
				ShardId:     "shard-1",
				Action:      tt.action,
			}

			resp, err := pm.BeginTerm(ctx, req)

			// Verify response
			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if resp != nil {
				assert.Equal(t, tt.expectedAccepted, resp.Accepted, tt.description)
				assert.Equal(t, tt.expectedTerm, resp.Term)
			}

			// Verify persisted state (acceptance should be persisted even if revoke fails)
			persistedTerm, err := getConsensusTerm(tmpDir)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedTerm, persistedTerm.TermNumber)
			assert.Equal(t, tt.expectedAcceptedTermFromCoordinator, persistedTerm.AcceptedTermFromCoordinatorId.GetName())
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}

	// Run save failure tests
	for _, tt := range saveFailureTests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create mock and set ALL expectations BEFORE starting the manager
			mockQueryService := mock.NewQueryService()
			expectPrimaryStartupQueries(mockQueryService)
			tt.setupMocks(mockQueryService)

			pm, tmpDir := setupManagerWithMockDB(t, mockQueryService)

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

			// Make request
			req := &consensusdatapb.BeginTermRequest{
				Term:        tt.requestTerm,
				CandidateId: tt.requestCandidate,
				ShardId:     "shard-1",
				Action:      tt.action,
			}

			resp, err := pm.BeginTerm(ctx, req)

			// Verify error behavior
			if tt.expectedError {
				assert.Error(t, err, tt.description)
				assert.Nil(t, resp)
			} else {
				assert.NoError(t, err, tt.description)
				assert.NotNil(t, resp)
				assert.Equal(t, tt.expectedAccepted, resp.Accepted, tt.description)
				assert.Equal(t, tt.expectedRespTerm, resp.Term)
			}

			if tt.checkMemoryUnchanged {
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
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

// ============================================================================
// UpdateTermAndAcceptCandidate Tests
// ============================================================================

// setActionLockHeld is a test helper that creates a context with action lock held
func setActionLockHeld(ctx context.Context) context.Context {
	lock := NewActionLock()
	newCtx, err := lock.Acquire(ctx, "test-operation")
	if err != nil {
		panic(err)
	}
	return newCtx
}

func TestUpdateTermAndAcceptCandidate(t *testing.T) {
	tests := []struct {
		name           string
		initialTerm    int64
		initialAccept  *clustermetadatapb.ID
		newTerm        int64
		candidateID    *clustermetadatapb.ID
		expectError    bool
		expectedTerm   int64
		expectedAccept string
	}{
		{
			name:        "higher term updates and accepts atomically",
			initialTerm: 5,
			newTerm:     10,
			candidateID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-a",
			},
			expectError:    false,
			expectedTerm:   10,
			expectedAccept: "candidate-a",
		},
		{
			name:        "same term accepts candidate",
			initialTerm: 5,
			newTerm:     5,
			candidateID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-b",
			},
			expectError:    false,
			expectedTerm:   5,
			expectedAccept: "candidate-b",
		},
		{
			name:        "lower term rejected",
			initialTerm: 10,
			newTerm:     5,
			candidateID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-c",
			},
			expectError: true,
		},
		{
			name:        "nil candidate ID rejected",
			initialTerm: 5,
			newTerm:     10,
			candidateID: nil,
			expectError: true,
		},
		{
			name:        "same term same candidate is idempotent",
			initialTerm: 5,
			initialAccept: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-b",
			},
			newTerm: 5,
			candidateID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-b",
			},
			expectError:    false,
			expectedTerm:   5,
			expectedAccept: "candidate-b",
		},
		{
			name:        "same term different candidate rejected",
			initialTerm: 5,
			initialAccept: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-a",
			},
			newTerm: 5,
			candidateID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-b",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			poolerDir := t.TempDir()
			serviceID := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      "test-pooler",
			}

			// Create the pg_data directory to simulate initialized data directory
			pgDataDir := poolerDir + "/pg_data"
			err := os.MkdirAll(pgDataDir, 0o755)
			require.NoError(t, err)
			// Create PG_VERSION file to mark it as initialized
			err = os.WriteFile(pgDataDir+"/PG_VERSION", []byte("18\n"), 0o644)
			require.NoError(t, err)

			cs := NewConsensusState(poolerDir, serviceID)
			_, err = cs.Load()
			require.NoError(t, err)

			// Set initial term
			ctx := context.Background()
			ctx = setActionLockHeld(ctx)
			if tt.initialTerm > 0 {
				err = cs.UpdateTermAndSave(ctx, tt.initialTerm)
				require.NoError(t, err)

				// If we have an initial accepted candidate, set it
				if tt.initialAccept != nil {
					err = cs.AcceptCandidateAndSave(ctx, tt.initialAccept)
					require.NoError(t, err)
				}
			}

			// Call UpdateTermAndAcceptCandidate
			err = cs.UpdateTermAndAcceptCandidate(ctx, tt.newTerm, tt.candidateID)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			term, err := cs.GetTerm(ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedTerm, term.TermNumber)
			assert.Equal(t, tt.expectedAccept, term.AcceptedTermFromCoordinatorId.GetName())
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
		setupMock             func(*mock.QueryService)
		nilQsc                bool
		expectedReachable     bool
		expectedErrorContains string
		description           string
	}{
		{
			name:        "Success_MatchingHostPort",
			requestHost: "localhost",
			requestPort: 5432,
			setupMock: func(m *mock.QueryService) {
				conninfo := "host=localhost port=5432 user=replicator application_name=test-cell_standby-1"
				m.AddQueryPatternOnce("SELECT status, conninfo FROM pg_stat_wal_receiver",
					mock.MakeQueryResult([]string{"status", "conninfo"}, [][]any{{"streaming", conninfo}}))
			},
			expectedReachable: true,
			description:       "Should be reachable when WAL receiver is active and connected to correct host/port",
		},
		{
			name:        "NoWALReceiver",
			requestHost: "localhost",
			requestPort: 5432,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT status, conninfo FROM pg_stat_wal_receiver",
					mock.MakeQueryResult([]string{"status", "conninfo"}, [][]any{}))
			},
			expectedReachable:     false,
			expectedErrorContains: "no active WAL receiver",
			description:           "Should not be reachable when there is no WAL receiver",
		},
		{
			name:        "WALReceiverStopping",
			requestHost: "localhost",
			requestPort: 5432,
			setupMock: func(m *mock.QueryService) {
				conninfo := "host=localhost port=5432 user=replicator"
				m.AddQueryPatternOnce("SELECT status, conninfo FROM pg_stat_wal_receiver",
					mock.MakeQueryResult([]string{"status", "conninfo"}, [][]any{{"stopping", conninfo}}))
			},
			expectedReachable:     false,
			expectedErrorContains: "WAL receiver is stopping",
			description:           "Should not be reachable when WAL receiver is stopping",
		},
		{
			name:        "HostMismatch",
			requestHost: "localhost",
			requestPort: 5432,
			setupMock: func(m *mock.QueryService) {
				conninfo := "host=other-host port=5432 user=replicator"
				m.AddQueryPatternOnce("SELECT status, conninfo FROM pg_stat_wal_receiver",
					mock.MakeQueryResult([]string{"status", "conninfo"}, [][]any{{"streaming", conninfo}}))
			},
			expectedReachable:     false,
			expectedErrorContains: "expected localhost, got other-host",
			description:           "Should not be reachable when connected to different host",
		},
		{
			name:        "PortMismatch",
			requestHost: "localhost",
			requestPort: 5432,
			setupMock: func(m *mock.QueryService) {
				conninfo := "host=localhost port=5433 user=replicator"
				m.AddQueryPatternOnce("SELECT status, conninfo FROM pg_stat_wal_receiver",
					mock.MakeQueryResult([]string{"status", "conninfo"}, [][]any{{"streaming", conninfo}}))
			},
			expectedReachable:     false,
			expectedErrorContains: "expected 5432, got 5433",
			description:           "Should not be reachable when connected to different port",
		},
		{
			name:                  "NoDatabaseConnection",
			requestHost:           "localhost",
			requestPort:           5432,
			nilQsc:                true,
			setupMock:             func(m *mock.QueryService) {},
			expectedReachable:     false,
			expectedErrorContains: "database connection not available",
			description:           "Should not be reachable when database connection is not available",
		},
		{
			name:        "InvalidConnInfo",
			requestHost: "localhost",
			requestPort: 5432,
			setupMock: func(m *mock.QueryService) {
				conninfo := "invalid format without equals"
				m.AddQueryPatternOnce("SELECT status, conninfo FROM pg_stat_wal_receiver",
					mock.MakeQueryResult([]string{"status", "conninfo"}, [][]any{{"streaming", conninfo}}))
			},
			expectedReachable:     false,
			expectedErrorContains: "failed to parse conninfo",
			description:           "Should not be reachable when conninfo parsing fails",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create mock and set ALL expectations BEFORE starting the manager
			mockQueryService := mock.NewQueryService()
			expectPrimaryStartupQueries(mockQueryService)
			tt.setupMock(mockQueryService)

			pm, _ := setupManagerWithMockDB(t, mockQueryService)

			// Handle nil qsc case
			if tt.nilQsc {
				pm.qsc = nil
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
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
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
		nilQsc              bool
		setupMock           func(*mock.QueryService)
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
			setupMock: func(m *mock.QueryService) {
				// Health check SELECT 1
				m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
				// Single pg_is_in_recovery check determines both role and which WAL position to query
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
				m.AddQueryPatternOnce("SELECT pg_current_wal_lsn", mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/4000000"}}))
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
			setupMock: func(m *mock.QueryService) {
				// Health check SELECT 1
				m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
				// Single pg_is_in_recovery check determines both role and which WAL position to query
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				// queryReplicationStatus() expects full replication status query
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{
						"pg_last_wal_replay_lsn",
						"pg_last_wal_receive_lsn",
						"pg_is_wal_replay_paused",
						"pg_get_wal_replay_pause_state",
						"pg_last_xact_replay_timestamp",
						"current_setting",
						"wal_receiver_status",
					},
					[][]any{{"0/4FFFFFF", "0/5000000", "f", "not paused", nil, "", "streaming"}}))
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
			nilQsc:              true,
			setupMock:           func(m *mock.QueryService) {},
			expectedCurrentTerm: 7,
			expectedIsHealthy:   false,
			expectedRole:        "unknown", // no database, we can't check the postgres role
			description:         "Should handle missing database connection gracefully",
		},
		{
			name: "DatabaseQueryFailure",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber:                    4,
				AcceptedTermFromCoordinatorId: nil,
			},
			termInMemory: true,
			setupMock: func(m *mock.QueryService) {
				// Health check fails
				m.AddQueryPatternOnceWithError("^SELECT 1$", errors.New("connection refused"))
			},
			expectedCurrentTerm: 4,
			expectedIsHealthy:   false,
			expectedRole:        "unknown",
			description:         "Should handle database query failure gracefully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create mock and set ALL expectations BEFORE starting the manager
			mockQueryService := mock.NewQueryService()
			// Use appropriate startup queries based on expected role
			if tt.expectedRole == "replica" {
				expectStandbyStartupQueries(mockQueryService)
			} else {
				expectPrimaryStartupQueries(mockQueryService)
			}
			tt.setupMock(mockQueryService)

			pm, tmpDir := setupManagerWithMockDB(t, mockQueryService)

			// Initialize term on disk
			err := setConsensusTerm(tmpDir, tt.initialTerm)
			require.NoError(t, err)

			// Load term into consensus state if term should be in memory
			if tt.termInMemory {
				loadedTerm, err := pm.consensusState.Load()
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCurrentTerm, loadedTerm, "Loaded term should match expected current term")
			}

			// Handle nil qsc case
			if tt.nilQsc {
				pm.qsc = nil
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
			if !tt.termInMemory && !tt.nilQsc {
				// Acquire action lock to inspect consensus state
				inspectCtx, err := pm.actionLock.Acquire(ctx, "inspect")
				require.NoError(t, err)
				currentTerm, err := pm.consensusState.GetCurrentTermNumber(inspectCtx)
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCurrentTerm, currentTerm, "Term should be loaded into memory")
				pm.actionLock.Release(inspectCtx)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

// ============================================================================
// DemoteStalePrimary Tests
// ============================================================================

func TestDemoteStalePrimary_UpdatesConsensusTerm(t *testing.T) {
	tests := []struct {
		name                       string
		initialTerm                *multipoolermanagerdatapb.ConsensusTerm
		requestTerm                int64
		force                      bool
		setupPgRewindMock          func(*testutil.MockPgCtldService)
		setupQueryMock             func(*mock.QueryService)
		expectedFinalConsensusTerm int64
		expectedPrimaryTerm        int64
		expectedError              bool
		expectedErrorContains      string
		description                string
	}{
		{
			name: "SuccessfulDemotion_UpdatesTermFromLowerToHigher",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber:  5,
				PrimaryTerm: 5, // Was primary in term 5
			},
			requestTerm: 10,
			force:       false,
			setupPgRewindMock: func(m *testutil.MockPgCtldService) {
				// pg_rewind dry-run reports no divergence (servers already aligned)
				m.PgRewindResponse = &pgctldpb.PgRewindResponse{
					Message: "No divergence detected",
					Output:  "", // Empty output = no divergence
				}
			},
			setupQueryMock: func(m *mock.QueryService) {
				// waitForDatabaseConnection after restart - health check
				m.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))

				// resetSynchronousReplication queries
				m.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				m.AddQueryPatternOnce("ALTER SYSTEM SET synchronous_standby_names = ''", mock.MakeQueryResult(nil, nil))

				// setPrimaryConnInfoLocked queries (after reopen)
				m.AddQueryPatternOnce("ALTER SYSTEM SET primary_conninfo", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("SELECT pg_reload_conf", mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
			},
			expectedFinalConsensusTerm: 10,
			expectedPrimaryTerm:        0, // Primary term cleared after demotion
			expectedError:              false,
			description:                "Successful demotion should update consensus term from 5 to 10 and clear primary_term",
		},
		{
			name: "OutdatedTerm_RejectedWithoutForce",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber:  15,
				PrimaryTerm: 15,
			},
			requestTerm: 10,
			force:       false,
			setupPgRewindMock: func(m *testutil.MockPgCtldService) {
				// Should not reach pg_rewind since term validation fails
			},
			setupQueryMock: func(m *mock.QueryService) {
				// No queries should execute
			},
			expectedFinalConsensusTerm: 15, // Term should remain unchanged
			expectedPrimaryTerm:        15, // Primary term should remain unchanged
			expectedError:              true,
			expectedErrorContains:      "consensus term too old",
			description:                "Should reject outdated term without force flag",
		},
		{
			name: "OutdatedTerm_AcceptedWithForce",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber:  15,
				PrimaryTerm: 15,
			},
			requestTerm: 10,
			force:       true,
			setupPgRewindMock: func(m *testutil.MockPgCtldService) {
				m.PgRewindResponse = &pgctldpb.PgRewindResponse{
					Message: "No divergence",
					Output:  "",
				}
			},
			setupQueryMock: func(m *mock.QueryService) {
				// waitForDatabaseConnection after restart - health check
				m.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))

				// resetSynchronousReplication queries
				m.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				m.AddQueryPatternOnce("ALTER SYSTEM SET synchronous_standby_names = ''", mock.MakeQueryResult(nil, nil))

				// setPrimaryConnInfoLocked queries (after reopen)
				m.AddQueryPatternOnce("ALTER SYSTEM SET primary_conninfo", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("SELECT pg_reload_conf", mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
			},
			expectedFinalConsensusTerm: 15, // With force, term is NOT updated when older
			expectedPrimaryTerm:        0,  // Primary term is cleared
			expectedError:              false,
			description:                "With force=true, should accept outdated term but not update it (term stays at 15)",
		},
		{
			name: "SameTerm_Idempotent",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber:  10,
				PrimaryTerm: 10,
			},
			requestTerm: 10,
			force:       false,
			setupPgRewindMock: func(m *testutil.MockPgCtldService) {
				m.PgRewindResponse = &pgctldpb.PgRewindResponse{
					Message: "No divergence",
					Output:  "",
				}
			},
			setupQueryMock: func(m *mock.QueryService) {
				// waitForDatabaseConnection after restart - health check
				m.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))

				// resetSynchronousReplication queries
				m.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				m.AddQueryPatternOnce("ALTER SYSTEM SET synchronous_standby_names = ''", mock.MakeQueryResult(nil, nil))

				// setPrimaryConnInfoLocked queries (after reopen)
				m.AddQueryPatternOnce("ALTER SYSTEM SET primary_conninfo", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("SELECT pg_reload_conf", mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
			},
			expectedFinalConsensusTerm: 10,
			expectedPrimaryTerm:        0, // Primary term cleared
			expectedError:              false,
			description:                "Idempotent: same term should succeed and clear primary_term",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
			ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
			t.Cleanup(func() { ts.Close() })

			// Start mock pgctld server
			mockPgctld := &testutil.MockPgCtldService{}
			tt.setupPgRewindMock(mockPgctld)
			pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, mockPgctld)
			t.Cleanup(cleanupPgctld)

			// Create the database in topology
			database := "testdb"
			addDatabaseToTopo(t, ts, database)

			serviceID := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "stale-primary",
			}
			multipooler := &clustermetadatapb.MultiPooler{
				Id:            serviceID,
				Database:      database,
				Hostname:      "localhost",
				PortMap:       map[string]int32{"grpc": 8080, "postgres": 5432},
				Type:          clustermetadatapb.PoolerType_PRIMARY, // Starting as PRIMARY
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				TableGroup:    constants.DefaultTableGroup,
				Shard:         constants.DefaultShard,
			}
			require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

			tmpDir := t.TempDir()
			multipooler.PoolerDir = tmpDir

			// Create pg_data directory
			pgDataDir := tmpDir + "/pg_data"
			err := os.MkdirAll(pgDataDir, 0o755)
			require.NoError(t, err)
			err = os.WriteFile(pgDataDir+"/PG_VERSION", []byte("18\n"), 0o644)
			require.NoError(t, err)

			config := &Config{
				TopoClient: ts,
				PgctldAddr: pgctldAddr,
			}
			pm, err := NewMultiPoolerManager(logger, multipooler, config)
			require.NoError(t, err)
			t.Cleanup(func() { pm.Close() })

			// Set up mock query service
			mockQueryService := mock.NewQueryService()
			expectPrimaryStartupQueries(mockQueryService)
			tt.setupQueryMock(mockQueryService)
			pm.qsc = &mockPoolerController{queryService: mockQueryService}

			senv := servenv.NewServEnv(viperutil.NewRegistry())
			pm.Start(senv)
			require.Eventually(t, func() bool {
				return pm.GetState() == ManagerStateReady
			}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

			// Initialize consensus state and set initial term
			pm.mu.Lock()
			pm.consensusState = NewConsensusState(tmpDir, serviceID)
			pm.mu.Unlock()

			err = setConsensusTerm(tmpDir, tt.initialTerm)
			require.NoError(t, err)
			_, err = pm.consensusState.Load()
			require.NoError(t, err)

			// Create source pooler (the correct primary)
			sourcePooler := &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "correct-primary",
				},
				Hostname: "correct-primary-host",
				PortMap:  map[string]int32{"postgres": 5433},
			}

			// Call DemoteStalePrimary
			resp, err := pm.DemoteStalePrimary(ctx, sourcePooler, tt.requestTerm, tt.force)

			// Verify error expectation
			if tt.expectedError {
				require.Error(t, err, tt.description)
				if tt.expectedErrorContains != "" {
					assert.Contains(t, err.Error(), tt.expectedErrorContains, tt.description)
				}
			} else {
				require.NoError(t, err, tt.description)
				require.NotNil(t, resp)
				assert.True(t, resp.Success, tt.description)
			}

			// Verify consensus term was updated correctly
			persistedTerm, err := getConsensusTerm(tmpDir)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedFinalConsensusTerm, persistedTerm.TermNumber,
				"Consensus term should be %d but got %d", tt.expectedFinalConsensusTerm, persistedTerm.TermNumber)
			assert.Equal(t, tt.expectedPrimaryTerm, persistedTerm.PrimaryTerm,
				"Primary term should be %d but got %d", tt.expectedPrimaryTerm, persistedTerm.PrimaryTerm)

			// Verify topology was updated to REPLICA (only on success)
			if !tt.expectedError {
				updatedPooler, err := ts.GetMultiPooler(ctx, serviceID)
				require.NoError(t, err)
				assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, updatedPooler.Type,
					"Pooler type should be updated to REPLICA in topology")
			}

			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}
