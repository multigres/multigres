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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus/consensustest"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// recruitTS is a fixed coordinator_initiated_at timestamp used in Recruit test cases.
var recruitTS = timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

// ruleCreatedTS is a distinct timestamp used for ShardRule.CreationTime in
// Promote test cases. Kept different from recruitTS on purpose: if any code
// path reads one field but stores the other, the assertion catches it.
var ruleCreatedTS = timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))

func setupManagerWithMockDB(t *testing.T, mockQueryService *mock.QueryService, rules consensus.RuleStorer) (*MultipoolerManager, string) {
	ctx := t.Context()
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
	multipooler := &clustermetadatapb.Multipooler{
		Id:            serviceID,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		// A PRIMARY record must name itself as leader (the record invariant).
		RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   database,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}
	require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

	tmpDir := t.TempDir()
	multipooler.PoolerDir = tmpDir
	config := &Config{
		TopoClient: ts,
		PgctldAddr: pgctldAddr,
	}
	// Build through the real constructor with the mock query service and fake
	// rule store injected, so the consensus manager (promises rooted at tmpDir
	// via PoolerDir, rule store = the fake) is wired correctly from the start.
	pm, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
		withMockController(&mockPoolerController{queryService: mockQueryService}),
		withFakeRules(rules),
	)
	require.NoError(t, err)
	t.Cleanup(func() { pm.ShutdownForTest(context.Background()) })

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

	return pm, tmpDir
}

// ============================================================================
// Recruit Tests
// ============================================================================

// expectStandbyRecruitMocks sets up mock expectations for the standby Recruit path:
// saves primary_conninfo, disconnects receiver, and waits for replay to stabilize.
func expectStandbyRecruitMocks(m *mock.QueryService, lsn string, savedConnInfo string) {
	replStatusCols := []string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"}
	replStatusRow := [][]any{{lsn, lsn, false, "not paused", nil, "", nil, nil, nil, nil}}

	replayStateCols := []string{"replay_lsn", "is_paused"}
	replayStateRow := [][]any{{lsn, false}}

	// isPrimary check
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// readPrimaryConnInfo: return nil (NULL) when no conninfo saved, or the value when set
	var connInfoRow [][]any
	if savedConnInfo == "" {
		connInfoRow = [][]any{{nil}}
	} else {
		connInfoRow = [][]any{{savedConnInfo}}
	}
	m.AddQueryPatternOnce("current_setting.*primary_conninfo", mock.MakeQueryResult([]string{"current_setting"}, connInfoRow))
	// pauseReplication: resetPrimaryConnInfo
	m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
	expectReloadConfig(m)
	// waitForReceiverDisconnect
	m.AddQueryPatternOnce("SELECT COUNT.*pg_stat_wal_receiver", mock.MakeQueryResult([]string{"count", "status", "primary_conninfo"}, [][]any{{int64(0), "", ""}}))
	// queryReplicationStatus (from waitForReceiverDisconnect)
	m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(replStatusCols, replStatusRow))
	// resetRestoreCommand: this pooler is becoming a cohort member again
	m.AddQueryPatternOnce("ALTER SYSTEM RESET restore_command", mock.MakeQueryResult(nil, nil))
	expectReloadConfig(m)
	// stopRestoreCommand goes through the mock pgctld gRPC server, not the SQL mock;
	// its default response (nothing found running) requires no setup here.
	// waitForReplayStabilize: three consecutive polls with same replay_lsn = stable
	m.AddQueryPatternOnce("^SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult(replayStateCols, replayStateRow))
	m.AddQueryPatternOnce("^SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult(replayStateCols, replayStateRow))
	m.AddQueryPatternOnce("^SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult(replayStateCols, replayStateRow))
	// Final queryReplicationStatus after stability confirmed
	m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(replStatusCols, replStatusRow))
}

// makeRulePosition builds a minimal PoolerPosition with the given coordinator term,
// used to control what fakeRuleStore returns without running postgres queries.
func makeRulePosition(coordinatorTerm int64) *clustermetadatapb.PoolerPosition {
	return &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{
				CoordinatorTerm: coordinatorTerm,
			},
		}},
		Lsn: "16/B374D848",
	}
}

func TestRecruit(t *testing.T) {
	coordinatorA := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "coordinator-a",
	}
	coordinatorB := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "coordinator-b",
	}

	tests := []struct {
		name                       string
		initialRevocation          *clustermetadatapb.TermRevocation
		ruleStore                  *fakeRuleStore
		req                        *consensusdatapb.RecruitRequest
		setupMocks                 func(*mock.QueryService)
		makeFilesystemReadOnly     bool
		expectError                bool
		expectErrContains          string
		expectPersistedTerm        int64
		expectPersistedCoordinator string
	}{
		{
			name:                       "NilTermRevocation",
			initialRevocation:          &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
			ruleStore:                  &fakeRuleStore{},
			req:                        &consensusdatapb.RecruitRequest{TermRevocation: nil},
			setupMocks:                 func(m *mock.QueryService) {},
			expectError:                true,
			expectErrContains:          "term_revocation is required",
			expectPersistedTerm:        3,
			expectPersistedCoordinator: "",
		},
		{
			name:              "SanityCheckRejects_EqualCoordinatorTerm",
			initialRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
			ruleStore:         &fakeRuleStore{pos: makeRulePosition(5)},
			req: &consensusdatapb.RecruitRequest{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					AcceptedCoordinatorId:  coordinatorA,
					CoordinatorInitiatedAt: recruitTS,
					OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
				},
			},
			setupMocks:                 func(m *mock.QueryService) {},
			expectError:                true,
			expectErrContains:          "is not revoked by outgoing_rule",
			expectPersistedTerm:        3,
			expectPersistedCoordinator: "",
		},
		{
			name:              "SanityCheckRejects_HigherCoordinatorTerm",
			initialRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
			ruleStore:         &fakeRuleStore{pos: makeRulePosition(7)},
			req: &consensusdatapb.RecruitRequest{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					AcceptedCoordinatorId:  coordinatorA,
					CoordinatorInitiatedAt: recruitTS,
					OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
				},
			},
			setupMocks:                 func(m *mock.QueryService) {},
			expectError:                true,
			expectErrContains:          "is not revoked by outgoing_rule",
			expectPersistedTerm:        3,
			expectPersistedCoordinator: "",
		},
		{
			name:              "StandbySuccess_NewTerm",
			initialRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
			// Non-nil pos so getCachedConsensusStatus returns a populated ConsensusStatus.
			ruleStore: &fakeRuleStore{pos: makeRulePosition(0)},
			req: &consensusdatapb.RecruitRequest{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       7,
					AcceptedCoordinatorId:  coordinatorA,
					CoordinatorInitiatedAt: recruitTS,
					OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				},
			},
			setupMocks: func(m *mock.QueryService) {
				expectStandbyRecruitMocks(m, "0/2000000", "")
			},
			expectError:                false,
			expectPersistedTerm:        7,
			expectPersistedCoordinator: "coordinator-a",
		},
		{
			name: "StandbySuccess_Idempotent",
			initialRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       7,
				AcceptedCoordinatorId:  coordinatorA,
				CoordinatorInitiatedAt: recruitTS,
				OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			},
			ruleStore: &fakeRuleStore{pos: makeRulePosition(0)},
			req: &consensusdatapb.RecruitRequest{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       7,
					AcceptedCoordinatorId:  coordinatorA,
					CoordinatorInitiatedAt: recruitTS,
					OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				},
			},
			setupMocks: func(m *mock.QueryService) {
				expectStandbyRecruitMocks(m, "0/3000000", "")
			},
			expectError:                false,
			expectPersistedTerm:        7,
			expectPersistedCoordinator: "coordinator-a",
		},
		{
			name: "StandbyReject_DifferentCoordinator",
			initialRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:      7,
				AcceptedCoordinatorId: coordinatorA,
			},
			// WAL check passes; rejection is caught by the stored-term check.
			ruleStore: &fakeRuleStore{pos: makeRulePosition(1)},
			req: &consensusdatapb.RecruitRequest{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       7,
					AcceptedCoordinatorId:  coordinatorB,
					CoordinatorInitiatedAt: recruitTS,
					OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				},
			},
			// ValidateRevocation rejects at step 1 — postgres is never touched.
			setupMocks:                 func(m *mock.QueryService) {},
			expectError:                true,
			expectErrContains:          "already accepted term 7 from coordinator",
			expectPersistedTerm:        7,
			expectPersistedCoordinator: "coordinator-a",
		},
		{
			name:              "StandbyReject_OlderTerm",
			initialRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 10},
			// WAL check passes; rejection is caught by the stored-term check.
			ruleStore: &fakeRuleStore{pos: makeRulePosition(2)},
			req: &consensusdatapb.RecruitRequest{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					AcceptedCoordinatorId:  coordinatorA,
					CoordinatorInitiatedAt: recruitTS,
					OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
				},
			},
			// ValidateRevocation rejects at step 1 — postgres is never touched.
			setupMocks:                 func(m *mock.QueryService) {},
			expectError:                true,
			expectErrContains:          "already accepted term 10 > requested 5",
			expectPersistedTerm:        10,
			expectPersistedCoordinator: "",
		},
		{
			name:              "PersistFailure_FilesystemReadOnly",
			initialRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
			ruleStore:         &fakeRuleStore{pos: makeRulePosition(2)},
			req: &consensusdatapb.RecruitRequest{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       7,
					AcceptedCoordinatorId:  coordinatorA,
					CoordinatorInitiatedAt: recruitTS,
					OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
				},
			},
			setupMocks: func(m *mock.QueryService) {
				expectStandbyRecruitMocks(m, "0/7000000", "")
			},
			makeFilesystemReadOnly:     true,
			expectError:                true,
			expectErrContains:          "failed to persist term revocation",
			expectPersistedTerm:        3,
			expectPersistedCoordinator: "",
		},
		{
			// Primary node where demoteToStandbyLocked fails because there are no
			// postgres mocks for the demotion queries. Exercises the isPrimary=true
			// branch in Recruit and verifies that nothing is persisted on failure.
			name:              "PrimaryReject_DemotionFails",
			initialRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
			ruleStore:         &fakeRuleStore{pos: makeRulePosition(0)},
			req: &consensusdatapb.RecruitRequest{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       7,
					AcceptedCoordinatorId:  coordinatorA,
					CoordinatorInitiatedAt: recruitTS,
					OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				},
			},
			setupMocks: func(m *mock.QueryService) {
				// isPrimary returns false for pg_is_in_recovery (not in recovery = primary).
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
				// No further mocks — demoteToStandbyLocked fails on its first query.
			},
			expectError:                true,
			expectErrContains:          "failed to stop replication during recruit",
			expectPersistedTerm:        3,
			expectPersistedCoordinator: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			mockQueryService := mock.NewQueryService()
			tt.setupMocks(mockQueryService)

			pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, tt.ruleStore)

			consensustest.SeedTerm(t, tmpDir, tt.initialRevocation)
			_, err := pm.consensusMgr.Promises().Load()
			require.NoError(t, err)

			if tt.makeFilesystemReadOnly {
				require.NoError(t, os.Chmod(tmpDir, 0o555))
				t.Cleanup(func() { _ = os.Chmod(tmpDir, 0o755) })
			}

			resp, err := pm.Recruit(ctx, tt.req)

			if tt.expectError {
				require.Error(t, err)
				if tt.expectErrContains != "" {
					assert.Contains(t, err.Error(), tt.expectErrContains)
				}
				assert.Nil(t, resp)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				// ConsensusStatus should reflect the newly persisted revocation.
				require.NotNil(t, resp.ConsensusStatus)
				require.NotNil(t, resp.ConsensusStatus.TermRevocation)
				assert.Equal(t, tt.expectPersistedTerm, resp.ConsensusStatus.TermRevocation.GetRevokedBelowTerm())
			}

			// Verify persisted state matches expectations regardless of success/failure.
			persisted := pm.consensusMgr.Promises().GetInconsistentRevocation()
			assert.Equal(t, tt.expectPersistedTerm, persisted.GetRevokedBelowTerm())
			assert.Equal(t, tt.expectPersistedCoordinator, persisted.GetAcceptedCoordinatorId().GetName())

			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}

	t.Run("SuspectedDivergence_DoesNotBlockRecruit", func(t *testing.T) {
		// suspectedDivergence is intentionally not a Recruit gate: a node still
		// pending a rewind may participate so multiorch can always form
		// quorum. The actual rewind happens at SetPrimary.
		mockQueryService := mock.NewQueryService()
		pm, _ := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: makeRulePosition(0)})
		seedLockCtx, err := pm.actionLock.Acquire(t.Context(), "test-seed")
		require.NoError(t, err)
		_, err = pm.consensusMgr.SetSuspectedDivergence(seedLockCtx, true)
		require.NoError(t, err)
		pm.actionLock.Release(seedLockCtx)

		req := &consensusdatapb.RecruitRequest{
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       7,
				AcceptedCoordinatorId:  &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "coord"},
				CoordinatorInitiatedAt: recruitTS,
				OutgoingRule:           &clustermetadatapb.RuleNumber{},
			},
		}
		_, err = pm.Recruit(t.Context(), req)
		// We don't require success here (the minimal mock setup doesn't
		// satisfy the full Recruit path); we only assert that suspectedDivergence
		// is no longer the reason for rejection.
		if err != nil {
			assert.NotContains(t, err.Error(), "rewind pending",
				"suspectedDivergence should no longer block Recruit")
		}
	})

	seedFloor := func(t *testing.T, pm *MultipoolerManager, floor *clustermetadatapb.LsnPosition) {
		t.Helper()
		lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-seed-floor")
		require.NoError(t, err)
		defer pm.actionLock.Release(lockCtx)
		require.NoError(t, pm.consensusMgr.Promises().SetRecruitBlockedUntil(lockCtx, floor))
	}
	// rulePositionWithLSN builds a minimal PoolerPosition like makeRulePosition,
	// but with a caller-chosen LSN instead of makeRulePosition's fixed one, for
	// tests that need to control the LSN relative to a seeded floor.
	rulePositionWithLSN := func(coordinatorTerm int64, lsn string) *clustermetadatapb.PoolerPosition {
		pos := makeRulePosition(coordinatorTerm)
		pos.Lsn = lsn
		return pos
	}

	t.Run("PositionFloor_RejectsBelowFloor", func(t *testing.T) {
		mockQueryService := mock.NewQueryService()
		expectStandbyRecruitMocks(mockQueryService, "0/1000", "")
		pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: rulePositionWithLSN(0, "0/1000")})
		consensustest.SeedTerm(t, tmpDir, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3})
		_, err := pm.consensusMgr.Promises().Load()
		require.NoError(t, err)
		seedFloor(t, pm, &clustermetadatapb.LsnPosition{Lsn: "0/2000"})

		req := &consensusdatapb.RecruitRequest{
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       7,
				AcceptedCoordinatorId:  coordinatorA,
				CoordinatorInitiatedAt: recruitTS,
				OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			},
		}
		resp, err := pm.Recruit(t.Context(), req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "has not caught up to its recruit position floor")
		assert.Nil(t, resp)
	})

	t.Run("PositionFloor_AppliesToObserverToo", func(t *testing.T) {
		// pos names no cohort members (this pooler is an observer), but the
		// floor applies unconditionally — no cohort-member/observer exemption.
		mockQueryService := mock.NewQueryService()
		pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: rulePositionWithLSN(0, "0/1000")})
		consensustest.SeedTerm(t, tmpDir, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3})
		_, err := pm.consensusMgr.Promises().Load()
		require.NoError(t, err)
		seedFloor(t, pm, &clustermetadatapb.LsnPosition{Lsn: "0/2000"})

		req := &consensusdatapb.RecruitRequest{
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       7,
				AcceptedCoordinatorId:  coordinatorA,
				CoordinatorInitiatedAt: recruitTS,
				OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			},
		}
		resp, err := pm.Recruit(t.Context(), req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "has not caught up to its recruit position floor")
		assert.Nil(t, resp)
	})

	t.Run("PositionFloor_SucceedsAndOmitsFloorOnceCaughtUp", func(t *testing.T) {
		mockQueryService := mock.NewQueryService()
		expectStandbyRecruitMocks(mockQueryService, "0/3000", "")
		pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: rulePositionWithLSN(0, "0/3000")})
		consensustest.SeedTerm(t, tmpDir, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3})
		_, err := pm.consensusMgr.Promises().Load()
		require.NoError(t, err)
		seedFloor(t, pm, &clustermetadatapb.LsnPosition{Lsn: "0/2000"})

		req := &consensusdatapb.RecruitRequest{
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       7,
				AcceptedCoordinatorId:  coordinatorA,
				CoordinatorInitiatedAt: recruitTS,
				OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			},
		}
		resp, err := pm.Recruit(t.Context(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Not cleared in storage (no explicit clearing in this design), but
		// omitted from the built ConsensusStatus since current position
		// already clears it — see ConsensusManager.recruitPositionFloorIfOutstanding.
		assert.Nil(t, resp.ConsensusStatus.GetRecruitBlockedUntil(),
			"a satisfied floor should be omitted from ConsensusStatus")
	})

	t.Run("PositionFloor_EarlyExitSkipsStopReplication", func(t *testing.T) {
		// Zero query mocks registered: if the early, pre-stop-replication
		// floor check (using preStatus) didn't short-circuit before any SQL
		// runs, stopReplicationForRecruit would fail with "no matching query
		// pattern" instead of the expected floor-rejection error.
		mockQueryService := mock.NewQueryService()
		pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: rulePositionWithLSN(0, "0/1000")})
		consensustest.SeedTerm(t, tmpDir, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3})
		_, err := pm.consensusMgr.Promises().Load()
		require.NoError(t, err)
		seedFloor(t, pm, &clustermetadatapb.LsnPosition{Lsn: "0/2000"})

		req := &consensusdatapb.RecruitRequest{
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       7,
				AcceptedCoordinatorId:  coordinatorA,
				CoordinatorInitiatedAt: recruitTS,
				OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			},
		}
		resp, err := pm.Recruit(t.Context(), req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "has not caught up to its recruit position floor")
		assert.Nil(t, resp)
	})
}

// expectStandbyReadyMocks adds the mock responses for the standby-state
// precondition check at the start of Promote(): postgres must be in recovery
// and primary_conninfo must be empty.
func expectStandbyReadyMocks(m *mock.QueryService) {
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	m.AddQueryPatternOnce("current_setting.*primary_conninfo",
		mock.MakeQueryResult([]string{"current_setting"}, [][]any{{nil}}))
}

// expectLeaderPromoteMocks sets up the postgres query expectations for the leader
// Promote path: check recovery state, pg_promote, wait for promotion, reset conninfo,
// reload config. The WAL position is read from the rule store's cached LSN
// (via beforeStatus.GetCurrentPosition().GetLsn()), not from a separate pg_current_wal_lsn query.
func expectLeaderPromoteMocks(m *mock.QueryService) {
	expectLeaderPromoteMocksWithUnlogged(m, nil)
}

// expectLeaderPromoteMocksWithUnlogged is expectLeaderPromoteMocks plus the
// dropUnloggedTablesAfterPromotion catalog query, which returns the given fully
// qualified unlogged table names (none when nil). Callers that pass user tables
// must also register the matching DROP TABLE patterns.
func expectLeaderPromoteMocksWithUnlogged(m *mock.QueryService, unloggedTables []string) {
	expectStandbyReadyMocks(m)
	// checkPromotionState: postgres is still in recovery (standby before promotion)
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// promoteStandbyToPrimary: call pg_promote()
	m.AddQueryPatternOnce("SELECT pg_promote", mock.MakeQueryResult(nil, nil))
	// waitForPromotionComplete: poll until pg_is_in_recovery returns false
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
	// resetPrimaryConnInfo: clear conninfo + reload
	m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
	expectReloadConfig(m)
	// dropUnloggedTablesAfterPromotion: list unlogged tables to inspect.
	rows := make([][]any, len(unloggedTables))
	for i, tbl := range unloggedTables {
		rows[i] = []any{tbl}
	}
	m.AddQueryPatternOnce("relpersistence = 'u'", mock.MakeQueryResult([]string{"format"}, rows))
	// Ensure the unlogged backend_vpid sidecar table exists while the sweep runs.
	m.AddQueryPatternOnce("CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid", mock.MakeQueryResult(nil, nil))
}

func TestPromote(t *testing.T) {
	// selfID matches the service ID created by setupManagerWithMockDB
	selfID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	coordinatorA := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "coordinator-a",
	}
	coordinatorB := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "coordinator-b",
	}
	otherPooler := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "other-pooler",
	}

	// recruitedTerm simulates a node that has been recruited for term 7 by coordinatorA.
	recruitedTerm := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       7,
		AcceptedCoordinatorId:  coordinatorA,
		CoordinatorInitiatedAt: recruitTS,
		OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 0, LeaderSubterm: 1},
	}
	validProposedRule := &clustermetadatapb.ShardRule{
		RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 7},
		CohortMembers: []*clustermetadatapb.ID{selfID, otherPooler},
		CoordinatorId: coordinatorA,
		CreationTime:  ruleCreatedTS,
	}

	makeLeaderReq := func() *consensusdatapb.PromoteRequest {
		return &consensusdatapb.PromoteRequest{
			Proposal: &consensusdatapb.CoordinatorProposal{
				TermRevocation: recruitedTerm,
				ProposalLeader: &clustermetadatapb.PoolerAddress{
					Id:           selfID,
					Host:         "pg-primary.internal",
					PostgresPort: 5432,
				},
				ProposedTransition: &clustermetadatapb.RulePosition{Proposal: validProposedRule},
			},
		}
	}

	makeReplicaReq := func() *consensusdatapb.PromoteRequest {
		return &consensusdatapb.PromoteRequest{
			Proposal: &consensusdatapb.CoordinatorProposal{
				TermRevocation: recruitedTerm,
				ProposalLeader: &clustermetadatapb.PoolerAddress{
					Id:           otherPooler,
					Host:         "pg-primary.internal",
					PostgresPort: 5432,
				},
				ProposedTransition: &clustermetadatapb.RulePosition{Proposal: validProposedRule},
			},
		}
	}

	tests := []struct {
		name              string
		initialTerm       *clustermetadatapb.TermRevocation
		ruleStore         *fakeRuleStore
		req               *consensusdatapb.PromoteRequest
		setupMocks        func(*mock.QueryService)
		preRun            func(*testing.T, *MultipoolerManager)
		expectError       bool
		expectErrContains string
		postCheck         func(*testing.T, *MultipoolerManager, *fakeRuleStore)
	}{
		{
			name:              "NilProposal",
			initialTerm:       recruitedTerm,
			ruleStore:         &fakeRuleStore{},
			req:               &consensusdatapb.PromoteRequest{},
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "proposal is required",
		},
		{
			name:        "NilTermRevocation",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{},
			req: &consensusdatapb.PromoteRequest{
				Proposal: &consensusdatapb.CoordinatorProposal{
					ProposalLeader:     &clustermetadatapb.PoolerAddress{Id: selfID, PostgresPort: 5432},
					ProposedTransition: &clustermetadatapb.RulePosition{Proposal: validProposedRule},
				},
			},
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "proposal.term_revocation is required",
		},
		{
			name:        "NilProposalLeader",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{},
			req: &consensusdatapb.PromoteRequest{
				Proposal: &consensusdatapb.CoordinatorProposal{
					TermRevocation:     recruitedTerm,
					ProposedTransition: &clustermetadatapb.RulePosition{Proposal: validProposedRule},
				},
			},
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "proposal.proposal_leader is required",
		},
		{
			// proposal_leader is present but its id is missing. The handler must
			// reject before any postgres work — it can't know who to promote.
			name:        "NilProposalLeaderId",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{},
			req: &consensusdatapb.PromoteRequest{
				Proposal: &consensusdatapb.CoordinatorProposal{
					TermRevocation:     recruitedTerm,
					ProposalLeader:     &clustermetadatapb.PoolerAddress{PostgresPort: 5432},
					ProposedTransition: &clustermetadatapb.RulePosition{Proposal: validProposedRule},
				},
			},
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "proposal.proposal_leader.id is required",
		},
		{
			name:        "ZeroPostgresPort",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{},
			req: &consensusdatapb.PromoteRequest{
				Proposal: &consensusdatapb.CoordinatorProposal{
					TermRevocation:     recruitedTerm,
					ProposalLeader:     &clustermetadatapb.PoolerAddress{Id: selfID},
					ProposedTransition: &clustermetadatapb.RulePosition{Proposal: validProposedRule},
				},
			},
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "proposal.proposal_leader.postgres_port is required",
		},
		{
			name:        "NilProposedRule",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{},
			req: &consensusdatapb.PromoteRequest{
				Proposal: &consensusdatapb.CoordinatorProposal{
					TermRevocation: recruitedTerm,
					ProposalLeader: &clustermetadatapb.PoolerAddress{Id: selfID, PostgresPort: 5432},
				},
			},
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "proposal.proposed_rule is required",
		},
		{
			name:              "TermNotRecruited_StoredLower",
			initialTerm:       &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
			ruleStore:         &fakeRuleStore{pos: makeRulePosition(0)},
			req:               makeLeaderReq(),
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "must Recruit before Promote",
		},
		{
			// Stored term (10) is newer than the proposal's term (7): also rejected.
			name:              "TermNotRecruited_StoredHigher",
			initialTerm:       &clustermetadatapb.TermRevocation{RevokedBelowTerm: 10},
			ruleStore:         &fakeRuleStore{pos: makeRulePosition(0)},
			req:               makeLeaderReq(),
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "already accepted term 10 > requested 7",
		},
		{
			// The revocation must revoke ALL terms below the rule being promoted.
			// Here the node was recruited for term 7 (revocation revokes below 7)
			// but the proposal would install a rule at term 9, leaving terms [7,9)
			// non-revoked — so a stale committed rule in that range could be
			// misread as a live leader. Reject.
			name:        "RevocationDoesNotCoverRuleTerm",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{pos: makeRulePosition(0)},
			req: &consensusdatapb.PromoteRequest{
				Proposal: &consensusdatapb.CoordinatorProposal{
					TermRevocation: recruitedTerm, // revokes below 7
					ProposalLeader: &clustermetadatapb.PoolerAddress{Id: selfID, Host: "pg-primary.internal", PostgresPort: 5432},
					ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
						RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 9},
						CohortMembers: []*clustermetadatapb.ID{selfID, otherPooler},
						CoordinatorId: coordinatorA,
						CreationTime:  ruleCreatedTS,
					}},
				},
			},
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "must revoke all rules below the new term",
		},
		{
			name:        "WrongCoordinator",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{pos: makeRulePosition(0)},
			req: &consensusdatapb.PromoteRequest{
				Proposal: &consensusdatapb.CoordinatorProposal{
					TermRevocation: &clustermetadatapb.TermRevocation{
						RevokedBelowTerm:       7,
						AcceptedCoordinatorId:  coordinatorB,
						CoordinatorInitiatedAt: recruitTS,
						OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 0, LeaderSubterm: 1},
					},
					ProposalLeader:     &clustermetadatapb.PoolerAddress{Id: selfID, PostgresPort: 5432},
					ProposedTransition: &clustermetadatapb.RulePosition{Proposal: validProposedRule},
				},
			},
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "already accepted term 7 from coordinator",
		},
		{
			name:        "WALAheadOfTerm",
			initialTerm: recruitedTerm,
			// fakeRuleStore returns a position with coordinator_term == 7:
			// ValidateRevocation rejects because committed rule term >= revoked_below_term.
			ruleStore:         &fakeRuleStore{pos: makeRulePosition(7)},
			req:               makeLeaderReq(),
			setupMocks:        func(m *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "is not revoked by outgoing_rule",
		},
		{
			name:        "LeaderSuccess",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{pos: makeRulePosition(0)},
			req:         makeLeaderReq(),
			setupMocks: func(m *mock.QueryService) {
				expectLeaderPromoteMocks(m)
			},
			// Verify the promotion was recorded with the right coordinator term and WAL
			// position, and that the health streamer was updated for write traffic.
			preRun: func(t *testing.T, pm *MultipoolerManager) {
				// Pre-set so we can verify clearResignedLeaderAtTerm ran.
				lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-seed")
				require.NoError(t, err)
				require.NoError(t, pm.consensusMgr.SetResignedLeaderAtTerm(lockCtx, &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 7}}}))
				pm.actionLock.Release(lockCtx)
			},
			postCheck: func(t *testing.T, pm *MultipoolerManager, rs *fakeRuleStore) {
				update := rs.assertPromoteRecorded(t)
				assert.Equal(t, int64(7), update.GetTermNumber())
				assert.True(t, proto.Equal(coordinatorA, update.GetCoordinatorID()))
				assert.True(t, proto.Equal(selfID, update.GetLeaderID()))
				assert.Len(t, update.GetCohortMembers(), 2)
				// walPosition comes from beforeStatus.GetCurrentPosition().GetLsn(),
				// which is the rule store's cached LSN at the time of the Promote call.
				assert.Equal(t, makeRulePosition(0).Lsn, update.GetWALPosition())
				assert.Nil(t, update.GetDurabilityPolicy())
				assert.Empty(t, update.GetAcceptedMembers())

				state := pm.healthStreamer.getState()
				require.NotNil(t, state.RoutingState)
				assert.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, state.RoutingState.GetRole())
				assert.Equal(t, int64(7), state.RoutingState.GetRule().GetCoordinatorTerm())

				assert.Equal(t, int64(0), pm.consensusMgr.ResignedLeaderAtTerm(), "clearResignedLeaderAtTerm should have cleared the term")

				// ReplicationPrimary should advertise this pooler as the primary
				// at the proposalLeader's host/port. Coordinators reading this
				// pooler's health stream see (self, host, port).
				rp := pm.consensusMgr.GetReplicationPrimary()
				require.NotNil(t, rp)
				require.NotNil(t, rp.GetPrimary())
				assert.True(t, proto.Equal(selfID, rp.GetPrimary().GetId()))
				assert.Equal(t, "pg-primary.internal", rp.GetPrimary().GetHost())
				assert.Equal(t, int32(5432), rp.GetPrimary().GetPostgresPort())
			},
		},
		{
			// GUC application (syncStandby.SetPolicy inside UpdateRule) fails before
			// pg_promote is called. Promote must return an error without promoting, since
			// a misconfigured GUC would allow the primary to accept writes without the
			// required sync acknowledgment. GUC application now lives inside UpdateRule,
			// so we simulate the failure via fakeRuleStore.updateErr.
			name:        "LeaderGUCFailure",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{pos: makeRulePosition(0), updateErr: errors.New("pre-promote GUC: GUC write failed")},
			req:         makeLeaderReq(),
			setupMocks: func(m *mock.QueryService) {
				expectStandbyReadyMocks(m)
				// checkPromotionState: standby. UpdateRule then fails (no pg_promote called).
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
					mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
			},
			expectError:       true,
			expectErrContains: "promote failed: could not write rule",
		},
		{
			// postgres is already primary (not in recovery): the standby-state
			// precondition check rejects the proposal. The coordinator must
			// re-Recruit before calling Promote again.
			name:        "LeaderAlreadyPromoted",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{pos: makeRulePosition(0)},
			req:         makeLeaderReq(),
			setupMocks: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
					mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
			},
			expectError:       true,
			expectErrContains: "not in standby mode",
		},
		{
			// primary_conninfo is set (Promote may have already succeeded):
			// caught by the standby-state precondition check.
			name:        "PromoteFails_PrimaryConninfoSet",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{pos: makeRulePosition(0)},
			req:         makeLeaderReq(),
			setupMocks: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
					mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				m.AddQueryPatternOnce("current_setting.*primary_conninfo",
					mock.MakeQueryResult([]string{"current_setting"}, [][]any{{"host=primary port=5432"}}))
			},
			expectError:       true,
			expectErrContains: "primary_conninfo is set",
		},
		{
			// proposal_leader is some other pooler: Promote is leader-only, so
			// it must reject this and direct the coordinator to use
			// SetPrimary for followers. The identity check runs before any
			// postgres queries, so no mocks are needed.
			name:              "RejectedWhenNotDesignatedLeader",
			initialTerm:       recruitedTerm,
			ruleStore:         &fakeRuleStore{pos: makeRulePosition(0)},
			req:               makeReplicaReq(),
			setupMocks:        func(_ *mock.QueryService) {},
			expectError:       true,
			expectErrContains: "non-leaders should be told via SetPrimary",
		},
		{
			// DurabilityPolicy is structurally invalid (RequiredCount=0 violates
			// AT_LEAST_N's invariant). syncConfigFromProposedRule fails before
			// any postgres GUCs are written; Promote must propagate the error.
			name:        "InvalidDurabilityPolicy",
			initialTerm: recruitedTerm,
			ruleStore:   &fakeRuleStore{pos: makeRulePosition(0)},
			req: &consensusdatapb.PromoteRequest{
				Proposal: &consensusdatapb.CoordinatorProposal{
					TermRevocation: recruitedTerm,
					ProposalLeader: &clustermetadatapb.PoolerAddress{
						Id:           selfID,
						Host:         "pg-primary.internal",
						PostgresPort: 5432,
					},
					ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
						RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 7},
						CohortMembers: []*clustermetadatapb.ID{selfID, otherPooler},
						CoordinatorId: coordinatorA,
						CreationTime:  ruleCreatedTS,
						DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
							QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
							RequiredCount: 0,
						},
					}},
				},
			},
			setupMocks: func(m *mock.QueryService) {
				expectStandbyReadyMocks(m)
				// checkPromotionState: standby.
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
					mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
			},
			expectError:       true,
			expectErrContains: "durability policy has missing or invalid fields",
		},
		{
			// fakeRuleStore.updateErrAfterHook returns an error after the postgres
			// promote has already happened. Promote must propagate that error
			// rather than mark the promotion successful — the rule write is
			// the durability gate, and a failure here means the new primary
			// hasn't satisfied sync replication.
			name:        "UpdateRuleFails",
			initialTerm: recruitedTerm,
			ruleStore: &fakeRuleStore{
				pos:                makeRulePosition(0),
				updateErrAfterHook: errors.New("rule store offline"),
			},
			req: makeLeaderReq(),
			setupMocks: func(m *mock.QueryService) {
				expectLeaderPromoteMocks(m)
			},
			expectError:       true,
			expectErrContains: "promote failed: could not write rule",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			mockQueryService := mock.NewQueryService()
			tt.setupMocks(mockQueryService)

			pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, tt.ruleStore)

			consensustest.SeedTerm(t, tmpDir, tt.initialTerm)
			_, err := pm.consensusMgr.Promises().Load()
			require.NoError(t, err)

			if tt.preRun != nil {
				tt.preRun(t, pm)
			}

			resp, err := pm.Promote(ctx, tt.req)

			if tt.expectError {
				require.Error(t, err)
				if tt.expectErrContains != "" {
					assert.Contains(t, err.Error(), tt.expectErrContains)
				}
				assert.Nil(t, resp)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.NotNil(t, resp.ConsensusStatus)
				if tt.postCheck != nil {
					tt.postCheck(t, pm, tt.ruleStore)
				}
			}

			require.Eventually(t, func() bool {
				return mockQueryService.ExpectationsWereMet() == nil
			}, time.Second, time.Millisecond, "mock expectations not met after promotion")
		})
	}
}

// TestPromoteDropsUnloggedTables verifies the post-promotion unlogged-table
// sweep: user tables are dropped, backend_vpid is preserved, and a drop blocked
// by a dependency is logged without failing the promotion.
func TestPromoteDropsUnloggedTables(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"}
	coordinatorA := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "coordinator-a"}
	otherPooler := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "other-pooler"}

	recruitedTerm := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       7,
		AcceptedCoordinatorId:  coordinatorA,
		CoordinatorInitiatedAt: recruitTS,
		OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 0, LeaderSubterm: 1},
	}
	req := &consensusdatapb.PromoteRequest{
		Proposal: &consensusdatapb.CoordinatorProposal{
			TermRevocation: recruitedTerm,
			ProposalLeader: &clustermetadatapb.PoolerAddress{Id: selfID, Host: "pg-primary.internal", PostgresPort: 5432},
			ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
				RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 7},
				CohortMembers: []*clustermetadatapb.ID{selfID, otherPooler},
				CoordinatorId: coordinatorA,
				CreationTime:  ruleCreatedTS,
			}},
		},
	}

	runPromote := func(t *testing.T, setupMocks func(*mock.QueryService)) (*mock.QueryService, error) {
		m := mock.NewQueryService()
		setupMocks(m)
		pm, tmpDir := setupManagerWithMockDB(t, m, &fakeRuleStore{pos: makeRulePosition(0)})
		consensustest.SeedTerm(t, tmpDir, recruitedTerm)
		_, err := pm.consensusMgr.Promises().Load()
		require.NoError(t, err)
		_, err = pm.Promote(t.Context(), req)
		return m, err
	}

	t.Run("drops user tables and preserves backend vpid", func(t *testing.T) {
		var mu sync.Mutex
		var dropped []string
		m, err := runPromote(t, func(m *mock.QueryService) {
			expectLeaderPromoteMocksWithUnlogged(m, []string{"public.foo", "public.bar", "multigres.backend_vpid"})
			m.AddQueryPatternWithCallback("DROP TABLE ", mock.MakeQueryResult(nil, nil), func(q string) {
				mu.Lock()
				dropped = append(dropped, strings.TrimPrefix(q, "DROP TABLE "))
				mu.Unlock()
			})
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(dropped) == 2
		}, time.Second, 10*time.Millisecond, "expected user-created unlogged tables to be dropped")
		mu.Lock()
		assert.ElementsMatch(t, []string{"public.foo", "public.bar"}, dropped)
		assert.NotContains(t, dropped, "multigres.backend_vpid")
		mu.Unlock()
		assert.NoError(t, m.ExpectationsWereMet())
	})

	t.Run("dependency-blocked drop does not fail promotion", func(t *testing.T) {
		m, err := runPromote(t, func(m *mock.QueryService) {
			expectLeaderPromoteMocksWithUnlogged(m, []string{"public.has_view"})
			m.AddQueryPatternWithError(`DROP TABLE public\.has_view`,
				errors.New("cannot drop table public.has_view because other objects depend on it"))
		})
		require.NoError(t, err, "best-effort drop must not fail the promotion")
		assert.NoError(t, m.ExpectationsWereMet())
	})
}

func TestAvailabilityStatus(t *testing.T) {
	t.Run("buildAvailabilityStatus publishes cohort eligibility with no leadership status when no resignation is set", func(t *testing.T) {
		pm := newTestManager(t)
		av := pm.buildAvailabilityStatus(true)
		require.NotNil(t, av)
		assert.Nil(t, av.LeadershipStatus)
		require.NotNil(t, av.CohortEligibilityStatus)
		assert.Equal(t, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE, av.CohortEligibilityStatus.Signal)
	})

	t.Run("resignedLeaderAtTerm set adds a LeadershipStatus alongside cohort eligibility", func(t *testing.T) {
		pm := newTestManager(t, withResignedLeaderAtTerm(7))
		av := pm.buildAvailabilityStatus(true)
		require.NotNil(t, av)
		require.NotNil(t, av.LeadershipStatus)
		assert.Equal(t, int64(7), av.LeadershipStatus.LeaderTerm)
		assert.Equal(t, clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION, av.LeadershipStatus.Signal)
		require.NotNil(t, av.CohortEligibilityStatus)
		assert.Equal(t, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE, av.CohortEligibilityStatus.Signal)
	})

	t.Run("no resignation -> no LeadershipStatus but keeps cohort eligibility", func(t *testing.T) {
		pm := newTestManager(t)
		av := pm.buildAvailabilityStatus(true)
		require.NotNil(t, av)
		assert.Nil(t, av.LeadershipStatus)
		require.NotNil(t, av.CohortEligibilityStatus)
	})

	t.Run("SetCohortEligibility flips the signal", func(t *testing.T) {
		pm := newTestManager(t)
		lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
		require.NoError(t, err)
		require.NoError(t, pm.consensusMgr.SetCohortEligibility(lockCtx, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE))
		pm.actionLock.Release(lockCtx)
		av := pm.buildAvailabilityStatus(true)
		require.NotNil(t, av)
		require.NotNil(t, av.CohortEligibilityStatus)
		assert.Equal(t, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE, av.CohortEligibilityStatus.Signal)
	})

	t.Run("suspectedDivergence is published", func(t *testing.T) {
		pm := newTestManager(t)
		assert.False(t, pm.buildAvailabilityStatus(true).SuspectedDivergence, "defaults to false")
		lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-seed")
		require.NoError(t, err)
		_, err = pm.consensusMgr.SetSuspectedDivergence(lockCtx, true)
		require.NoError(t, err)
		pm.actionLock.Release(lockCtx)
		assert.True(t, pm.buildAvailabilityStatus(true).SuspectedDivergence, "reflects the in-memory flag")
	})

	t.Run("postgres ready sets LeadershipAvailability READY", func(t *testing.T) {
		pm := newTestManager(t)
		av := pm.buildAvailabilityStatus(true)
		require.NotNil(t, av.GetLeadershipAvailability())
		assert.Equal(t,
			clustermetadatapb.LeadershipAvailabilitySignal_LEADERSHIP_AVAILABILITY_SIGNAL_READY,
			av.GetLeadershipAvailability().GetSignal())
	})

	t.Run("postgres not ready sets LeadershipAvailability STARTING with reason", func(t *testing.T) {
		pm := newTestManager(t)
		av := pm.buildAvailabilityStatus(false)
		require.NotNil(t, av.GetLeadershipAvailability())
		assert.Equal(t,
			clustermetadatapb.LeadershipAvailabilitySignal_LEADERSHIP_AVAILABILITY_SIGNAL_STARTING,
			av.GetLeadershipAvailability().GetSignal())
		assert.NotEmpty(t, av.GetLeadershipAvailability().GetReason(),
			"reason should be set for debugging")
		assert.Equal(t,
			clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
			av.GetCohortEligibilityStatus().GetSignal(),
			"postgres readiness must not affect cohort eligibility signal")
	})
}

// TestSetResignedLeaderAtTerm_BroadcastsOnChange verifies that setting the
// resignation term broadcasts to subscribers on change so the coordinator
// sees the signal without waiting for the next periodic snapshot, and does
// NOT broadcast when the value is unchanged (idempotent calls are cheap).
func TestSetResignedLeaderAtTerm_BroadcastsOnChange(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test"}
	streamer := newHealthStreamer(logger, id, "tg", "0")
	pm := &MultipoolerManager{
		actionLock:   actionlock.NewActionLock(),
		consensusMgr: consensus.NewManagerForTesting(t, id, consensus.NewConsensusPromises("", id), &fakeRuleStore{}, streamer),
	}

	// Subscribe so we can observe broadcasts.
	_, ch := streamer.subscribe()
	drain := func() int {
		n := 0
		for {
			select {
			case <-ch:
				n++
			default:
				return n
			}
		}
	}
	drain() // discard the initial state delivered by subscribe.

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	require.NoError(t, pm.consensusMgr.SetResignedLeaderAtTerm(lockCtx, &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}}}))
	assert.Equal(t, 1, drain(), "first call should broadcast on change from 0 to 5")

	require.NoError(t, pm.consensusMgr.SetResignedLeaderAtTerm(lockCtx, &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}}}))
	assert.Equal(t, 0, drain(), "repeating the same value should NOT broadcast")

	require.NoError(t, pm.consensusMgr.SetResignedLeaderAtTerm(lockCtx, &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 7}}}))
	assert.Equal(t, 1, drain(), "changing to a new term should broadcast")

	require.NoError(t, pm.consensusMgr.SetResignedLeaderAtTerm(lockCtx, &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 0}}}))
	assert.Equal(t, 1, drain(), "clearing the term is also a change and should broadcast")
}
