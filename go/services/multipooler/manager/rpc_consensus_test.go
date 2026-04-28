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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/executor/mock"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// observePositionRow builds a mock result row for the observePosition query
// where the rule names primaryAppName (e.g. "zone1_stale-primary") as the
// primary with the given coordinator term.
func observePositionRow(primaryAppName string, coordinatorTerm int64) ([]string, [][]any) {
	cols := []string{
		"coordinator_term", "leader_subterm", "leader_id", "coordinator_id", "cohort_members",
		"durability_policy_name", "durability_quorum_type", "durability_required_count",
		"current_lsn",
	}
	row := [][]any{{
		coordinatorTerm, int64(0), primaryAppName, primaryAppName, "{}",
		nil, nil, nil, "0/1",
	}}
	return cols, row
}

// expectObservePositionAsCurrentPrimary queues one-shot mocks so that the pre-
// demotion observePosition calls (one from manager startup's checkAndSetReady
// and one from DemoteStalePrimary's "already demoted" check) both report this
// pooler as the primary of the current rule with the given coordinatorTerm.
// A subsequent expectObservePositionAsStalePrimary call can install a persistent
// post-demotion mock for the final assertion.
func expectObservePositionAsCurrentPrimary(m *mock.QueryService, appName string, coordinatorTerm int64) {
	cols, row := observePositionRow(appName, coordinatorTerm)
	// Consumed by checkAndSetReady during manager startup.
	m.AddQueryPatternOnce("FROM multigres.current_rule", mock.MakeQueryResult(cols, row))
	// Consumed by DemoteStalePrimary's "already demoted" check.
	m.AddQueryPatternOnce("FROM multigres.current_rule", mock.MakeQueryResult(cols, row))
}

// expectObservePositionAsStalePrimary installs a persistent mock that
// simulates the rule-state convergence that replication will eventually
// produce after DemoteStalePrimary completes. DemoteStalePrimary itself
// does NOT rewrite the rule: pg_rewind overwrites multigres.current_rule
// only when the timelines diverge; otherwise the rule converges via
// streaming replication from the source primary. Tests use this helper
// to assert the post-convergence primary_term without actually running
// replication.
func expectObservePositionAsStalePrimary(m *mock.QueryService, primaryAppName string, coordinatorTerm int64) {
	cols, row := observePositionRow(primaryAppName, coordinatorTerm)
	m.AddQueryPattern("FROM multigres.current_rule", mock.MakeQueryResult(cols, row))
}

// expectStandbyRevokeMocks sets up mock expectations for the standby revoke path:
// receiver disconnect, wait for disconnect, and replay stabilization.
func expectStandbyRevokeMocks(m *mock.QueryService, lsn string) {
	replStatusCols := []string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"}
	replStatusRow := [][]any{{lsn, lsn, false, "not paused", nil, "", nil, nil, nil, nil}}

	// Replay state columns used by queryReplayState during stabilization polling
	replayStateCols := []string{"replay_lsn", "is_paused"}
	replayStateRow := [][]any{{lsn, false}}

	// pre-executeRevoke role check for term.begin event RevokedRole field
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// health check
	m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
	// determine role (standby)
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// pauseReplication: resetPrimaryConnInfo
	m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
	// waitForReceiverDisconnect
	m.AddQueryPatternOnce("SELECT COUNT.*pg_stat_wal_receiver", mock.MakeQueryResult([]string{"count"}, [][]any{{int64(0)}}))
	// queryReplicationStatus (from waitForReceiverDisconnect)
	m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(replStatusCols, replStatusRow))
	// waitForReplayStabilize: three consecutive polls with same replay_lsn = stable
	m.AddQueryPatternOnce("^SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult(replayStateCols, replayStateRow))
	m.AddQueryPatternOnce("^SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult(replayStateCols, replayStateRow))
	m.AddQueryPatternOnce("^SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult(replayStateCols, replayStateRow))
	// Final queryReplicationStatus after stability confirmed
	m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(replStatusCols, replStatusRow))
}

func setupManagerWithMockDB(t *testing.T, mockQueryService *mock.QueryService, rules ruleStorer) (*MultiPoolerManager, string) {
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
	t.Cleanup(func() { pm.Shutdown() })

	// Assign mock pooler controller and rule store BEFORE starting the manager
	// to avoid race conditions.
	pm.qsc = &mockPoolerController{queryService: mockQueryService}
	pm.rules = rules

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
	t.Setenv(constants.PgDataDirEnvVar, pgDataDir)

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
		expectedWalPosition                 *consensusdatapb.WALPosition // nil means don't check
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
				expectStandbyRevokeMocks(m, "0/2000000")
			},
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "candidate-B",
			expectedWalPosition:                 &consensusdatapb.WALPosition{LastReceiveLsn: "0/2000000", LastReplayLsn: "0/2000000"},
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
				expectStandbyRevokeMocks(m, "0/3000000")
			},
			expectedAccepted:                    true,
			expectedTerm:                        5,
			expectedAcceptedTermFromCoordinator: "candidate-A",
			expectedWalPosition:                 &consensusdatapb.WALPosition{LastReceiveLsn: "0/3000000", LastReplayLsn: "0/3000000"},
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
				// pre-executeRevoke role check for term.begin event RevokedRole field
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
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
				expectStandbyRevokeMocks(m, "0/4000000")
			},
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "new-candidate",
			expectedWalPosition:                 &consensusdatapb.WALPosition{LastReceiveLsn: "0/4000000", LastReplayLsn: "0/4000000"},
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
				expectStandbyRevokeMocks(m, "0/5000000")
			},
			expectedError:                       false,
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "new-candidate",
			expectedWalPosition:                 &consensusdatapb.WALPosition{LastReceiveLsn: "0/5000000", LastReplayLsn: "0/5000000"},
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
				expectStandbyRevokeMocks(m, "0/6000000")
			},
			expectedAccepted:                    true,
			expectedTerm:                        10,
			expectedAcceptedTermFromCoordinator: "new-candidate",
			expectedWalPosition:                 &consensusdatapb.WALPosition{LastReceiveLsn: "0/6000000", LastReplayLsn: "0/6000000"},
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

			tt.setupMocks(mockQueryService)

			pm, _ := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{})

			// Initialize term on disk
			err := pm.consensusState.setConsensusTerm(tt.initialTerm)
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
				if tt.expectedWalPosition != nil {
					require.NotNil(t, resp.WalPosition, "WalPosition should be set")
					assert.Equal(t, tt.expectedWalPosition.CurrentLsn, resp.WalPosition.CurrentLsn)
					assert.Equal(t, tt.expectedWalPosition.LastReceiveLsn, resp.WalPosition.LastReceiveLsn)
					assert.Equal(t, tt.expectedWalPosition.LastReplayLsn, resp.WalPosition.LastReplayLsn)
				}
			}

			// Verify persisted state (acceptance should be persisted even if revoke fails)
			persistedTerm, err := pm.consensusState.getConsensusTerm()
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

			tt.setupMocks(mockQueryService)

			pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{})

			// Initialize term on disk
			err := pm.consensusState.setConsensusTerm(tt.initialTerm)
			require.NoError(t, err)

			// Load into consensus state
			loadedTermNumber, err := pm.consensusState.Load()
			require.NoError(t, err)
			assert.Equal(t, tt.initialTerm.TermNumber, loadedTermNumber, "Loaded term number should match initial term")

			// Make filesystem read-only to simulate save failure
			if tt.makeFilesystemReadOnly {
				err := os.Chmod(tmpDir, 0o555)
				require.NoError(t, err)
				// Restore permissions after test
				t.Cleanup(func() {
					_ = os.Chmod(tmpDir, 0o755)
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
				loadedTerm, loadErr := pm.consensusState.getConsensusTerm()
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
			t.Setenv(constants.PgDataDirEnvVar, pgDataDir)

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
// ConsensusStatus Tests
// ============================================================================

func TestConsensusStatus(t *testing.T) {
	tests := []struct {
		name                string
		initialTerm         *multipoolermanagerdatapb.ConsensusTerm
		fakePos             *clustermetadatapb.PoolerPosition
		expectedCurrentTerm int64
		expectedWALLsn      string
		description         string
	}{
		{
			name: "WithTermAndPosition",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
				AcceptedTermFromCoordinatorId: &clustermetadatapb.ID{
					Cell: "zone1",
					Name: "leader-node",
				},
			},
			fakePos:             &clustermetadatapb.PoolerPosition{Lsn: "0/4000000"},
			expectedCurrentTerm: 5,
			expectedWALLsn:      "0/4000000",
			description:         "Returns term and WAL position from rule store",
		},
		{
			name: "WithTermNoPosition",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 7,
			},
			fakePos:             nil,
			expectedCurrentTerm: 7,
			expectedWALLsn:      "",
			description:         "Returns term with empty position when rule store has no position",
		},
		{
			name:                "NoTerm",
			initialTerm:         nil,
			fakePos:             nil,
			expectedCurrentTerm: 0,
			expectedWALLsn:      "",
			description:         "Returns zero term when no term has been set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			frs := &fakeRuleStore{pos: tt.fakePos}
			pm, _ := setupManagerWithMockDB(t, mock.NewQueryService(), frs)

			if tt.initialTerm != nil {
				err := pm.consensusState.setConsensusTerm(tt.initialTerm)
				require.NoError(t, err)
				_, err = pm.consensusState.Load()
				require.NoError(t, err)
			}

			req := &consensusdatapb.StatusRequest{ShardId: "test-shard"}
			resp, err := pm.ConsensusStatus(ctx, req)

			require.NoError(t, err, tt.description)
			require.NotNil(t, resp)

			cs := resp.GetConsensusStatus()
			require.NotNil(t, cs)
			assert.Equal(t, "test-pooler", cs.GetId().GetName())
			assert.Equal(t, "zone1", cs.GetId().GetCell())
			assert.Equal(t, tt.expectedCurrentTerm, cs.GetTermRevocation().GetRevokedBelowTerm())
			assert.Equal(t, tt.expectedWALLsn, cs.GetCurrentPosition().GetLsn())
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
				TermNumber: 5,
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
				// observePosition reports this pooler as the current primary, so
				// the "already demoted" check does not short-circuit.
				expectObservePositionAsCurrentPrimary(m, "zone1_stale-primary", 5)
				// Simulates the post-replication rule state (source pooler as
				// primary). DemoteStalePrimary doesn't rewrite the rule — this
				// mock stands in for the streaming convergence that would make
				// primary_term read back as 0 after DemoteStalePrimary returns.
				expectObservePositionAsStalePrimary(m, "zone1_correct-primary", 10)

				// waitForDatabaseConnection after restart - health check
				m.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))

				// resetSynchronousReplication queries
				m.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				m.AddQueryPatternOnce("ALTER SYSTEM RESET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // First pg_reload_conf call

				// setPrimaryConnInfoLocked queries
				m.AddQueryPatternOnce("ALTER SYSTEM SET primary_conninfo = 'host=correct-primary-host port=5433 user=postgres application_name=zone1_stale-primary'", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // Second pg_reload_conf call
			},
			expectedFinalConsensusTerm: 10,
			expectedPrimaryTerm:        0, // Primary term cleared after demotion
			expectedError:              false,
			description:                "Successful demotion should update consensus term from 5 to 10 and clear primary_term",
		},
		{
			name: "OutdatedTerm_RejectedWithoutForce",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 15,
			},
			requestTerm: 10,
			force:       false,
			setupPgRewindMock: func(m *testutil.MockPgCtldService) {
				// Should not reach pg_rewind since term validation fails
			},
			setupQueryMock: func(m *mock.QueryService) {
				// observePosition reports this pooler as the current primary, so
				// DemoteStalePrimary proceeds to term validation (which fails).
				expectObservePositionAsCurrentPrimary(m, "zone1_stale-primary", 15)
				// Demotion short-circuited on term validation, so no replication
				// was ever wired; the rule genuinely still names this pooler as
				// primary for the post-error assertion.
				expectObservePositionAsStalePrimary(m, "zone1_stale-primary", 15)
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
				TermNumber: 15,
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
				// observePosition reports this pooler as the current primary, so
				// the "already demoted" check does not short-circuit.
				expectObservePositionAsCurrentPrimary(m, "zone1_stale-primary", 5)
				// Simulates the post-replication rule state (source pooler as
				// primary). DemoteStalePrimary doesn't rewrite the rule — this
				// mock stands in for the streaming convergence that would make
				// primary_term read back as 0 after DemoteStalePrimary returns.
				expectObservePositionAsStalePrimary(m, "zone1_correct-primary", 10)

				// waitForDatabaseConnection after restart - health check
				m.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))

				// resetSynchronousReplication queries
				m.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				m.AddQueryPatternOnce("ALTER SYSTEM RESET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // First pg_reload_conf call

				// setPrimaryConnInfoLocked queries
				m.AddQueryPatternOnce("ALTER SYSTEM SET primary_conninfo = 'host=correct-primary-host port=5433 user=postgres application_name=zone1_stale-primary'", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // Second pg_reload_conf call
			},
			expectedFinalConsensusTerm: 15, // With force, term is NOT updated when older
			expectedPrimaryTerm:        0,  // Primary term is cleared
			expectedError:              false,
			description:                "With force=true, should accept outdated term but not update it (term stays at 15)",
		},
		{
			name: "SameTerm_Idempotent",
			initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 10,
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
				// observePosition reports this pooler as the current primary, so
				// the "already demoted" check does not short-circuit.
				expectObservePositionAsCurrentPrimary(m, "zone1_stale-primary", 5)
				// Simulates the post-replication rule state (source pooler as
				// primary). DemoteStalePrimary doesn't rewrite the rule — this
				// mock stands in for the streaming convergence that would make
				// primary_term read back as 0 after DemoteStalePrimary returns.
				expectObservePositionAsStalePrimary(m, "zone1_correct-primary", 10)

				// waitForDatabaseConnection after restart - health check
				m.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))

				// resetSynchronousReplication queries
				m.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				m.AddQueryPatternOnce("ALTER SYSTEM RESET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // First pg_reload_conf call

				// setPrimaryConnInfoLocked queries
				m.AddQueryPatternOnce("ALTER SYSTEM SET primary_conninfo = 'host=correct-primary-host port=5433 user=postgres application_name=zone1_stale-primary'", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // Second pg_reload_conf call
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
			t.Setenv(constants.PgDataDirEnvVar, pgDataDir)

			config := &Config{
				TopoClient: ts,
				PgctldAddr: pgctldAddr,
			}
			pm, err := NewMultiPoolerManager(logger, multipooler, config)
			require.NoError(t, err)
			t.Cleanup(func() { pm.Shutdown() })

			// Set up mock query service
			mockQueryService := mock.NewQueryService()

			tt.setupQueryMock(mockQueryService)
			pm.qsc = &mockPoolerController{queryService: mockQueryService}
			pm.rules = newRuleStore(logger, mockQueryService)

			senv := servenv.NewServEnv(viperutil.NewRegistry())
			pm.Start(senv)
			require.Eventually(t, func() bool {
				return pm.GetState() == ManagerStateReady
			}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

			// Initialize consensus state and set initial term
			pm.mu.Lock()
			pm.consensusState = NewConsensusState(tmpDir, serviceID)
			pm.mu.Unlock()

			err = pm.consensusState.setConsensusTerm(tt.initialTerm)
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
			persistedTerm, err := pm.consensusState.getConsensusTerm()
			require.NoError(t, err)
			assert.Equal(t, tt.expectedFinalConsensusTerm, persistedTerm.TermNumber,
				"Consensus term should be %d but got %d", tt.expectedFinalConsensusTerm, persistedTerm.TermNumber)
			cs, err := pm.getInconsistentConsensusStatus(ctx)
			require.NoError(t, err)
			primaryTerm := commonconsensus.PrimaryTerm(cs)
			assert.Equal(t, tt.expectedPrimaryTerm, primaryTerm,
				"Primary term should be %d but got %d", tt.expectedPrimaryTerm, primaryTerm)

			// Verify topology was updated to REPLICA (only on success).
			// The write is asynchronous so we poll until the publisher catches up.
			if !tt.expectedError {
				require.Eventually(t, func() bool {
					updatedPooler, err := ts.GetMultiPooler(ctx, serviceID)
					return err == nil && updatedPooler.Type == clustermetadatapb.PoolerType_REPLICA
				}, 500*time.Millisecond, 50*time.Millisecond, "Pooler type should be updated to REPLICA in topology")

				// Verify health streamer reports the new primary (source)
				healthState := pm.healthStreamer.getState()
				require.NotNil(t, healthState.PrimaryObservation,
					"health streamer should have primary observation pointing to new primary after DemoteStalePrimary")
				assert.Equal(t, sourcePooler.Id, healthState.PrimaryObservation.PrimaryID,
					"primary observation should point to the source (new primary)")
				assert.Equal(t, tt.requestTerm, healthState.PrimaryObservation.PrimaryTerm,
					"primary observation term should match the consensus term from the request")
			}

			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestAvailabilityStatus(t *testing.T) {
	t.Run("buildAvailabilityStatus returns nil when no resignation is set", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		assert.Nil(t, pm.buildAvailabilityStatus())
	})

	t.Run("resignedPrimaryAtTerm set makes buildAvailabilityStatus return the term", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		pm.resignedPrimaryAtTerm = 7
		av := pm.buildAvailabilityStatus()
		require.NotNil(t, av)
		require.NotNil(t, av.LeadershipStatus)
		assert.Equal(t, int64(7), av.LeadershipStatus.PrimaryTerm)
		assert.Equal(t, clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION, av.LeadershipStatus.Signal)
	})

	t.Run("resignedPrimaryAtTerm cleared makes buildAvailabilityStatus return nil", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		pm.resignedPrimaryAtTerm = 3
		pm.resignedPrimaryAtTerm = 0
		assert.Nil(t, pm.buildAvailabilityStatus())
	})
}
