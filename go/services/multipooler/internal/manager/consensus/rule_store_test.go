// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consensus

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMockRuleStore constructs a ruleStore wired to the given mock query service
// and a no-op SyncStandbyManager. Most tests don't exercise GUC reconciliation
// directly, so the noop is sufficient.
func newMockRuleStore(qs *mock.QueryService) *ruleStore {
	return NewRuleStore(slog.New(slog.DiscardHandler), qs, noopSyncStandbyManager{})
}

// errSyncStandbyManager is a SyncStandbyManager test double whose NeedsApply
// always returns the configured error. Used to exercise HasInconsistentGUC's
// NeedsApply error path.
type errSyncStandbyManager struct {
	noopSyncStandbyManager
	needsApplyErr error
}

func (e errSyncStandbyManager) NeedsApply(_ context.Context, _ commonconsensus.PolicyWithCohort) (bool, error) {
	return false, e.needsApplyErr
}

// makePoolerPosition builds a PoolerPosition with the given rule number.
func makePoolerPosition(coordinatorTerm, leaderSubterm int64) *clustermetadatapb.PoolerPosition {
	return &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{
				CoordinatorTerm: coordinatorTerm,
				LeaderSubterm:   leaderSubterm,
			},
		}},
	}
}

func TestQueryRuleHistory(t *testing.T) {
	t.Run("returns records ordered newest first", func(t *testing.T) {
		mockQueryService := mock.NewQueryService()
		rs := NewRuleStore(slog.New(slog.DiscardHandler), mockQueryService, noopSyncStandbyManager{})

		leaderAppName := "zone1_leader-1"
		coordID := "zone1_coordinator-1"
		walPos := "0/1234567"
		operation := "promotion"
		createdAt := "2026-03-24 09:00:17.000000+00"

		mockQueryService.AddQueryPatternOnce(
			"SELECT coordinator_term, leader_subterm, event_type",
			mock.MakeQueryResult(
				[]string{
					"coordinator_term", "leader_subterm", "event_type", "leader_id", "coordinator_id",
					"wal_position", "operation", "reason", "cohort_members", "accepted_members",
					"durability_policy_name", "durability_quorum_type", "durability_required_count",
					"decided", "created_at",
				},
				[][]any{
					{
						int64(2), int64(1), "promotion", leaderAppName, coordID, walPos, operation,
						"manual failover", "{zone1_pooler-2,zone1_pooler-3}", "{zone1_pooler-2}",
						nil, nil, nil, true, createdAt,
					},
					{
						int64(1), int64(0), "replication_config", leaderAppName, coordID, nil, nil,
						"initial bootstrap", "{zone1_pooler-1,zone1_pooler-2}", nil,
						nil, nil, nil, true, createdAt,
					},
				},
			),
		)

		records, err := rs.queryRuleHistory(t.Context(), 10)
		require.NoError(t, err)
		require.Len(t, records, 2)

		// First record: term 2, subterm 1 (newest)
		assert.Equal(t, int64(2), records[0].CoordinatorTerm)
		assert.Equal(t, int64(1), records[0].LeaderSubterm)
		assert.Equal(t, "promotion", records[0].EventType)
		require.NotNil(t, records[0].LeaderID)
		assert.Equal(t, leaderAppName, records[0].LeaderID.appName)
		require.NotNil(t, records[0].WALPosition)
		assert.Equal(t, walPos, *records[0].WALPosition)
		require.NotNil(t, records[0].Operation)
		assert.Equal(t, operation, *records[0].Operation)
		assert.Equal(t, "manual failover", records[0].Reason)
		require.Len(t, records[0].CohortMembers, 2)
		assert.Equal(t, "zone1_pooler-2", records[0].CohortMembers[0].appName)
		assert.Equal(t, "zone1_pooler-3", records[0].CohortMembers[1].appName)
		require.Len(t, records[0].AcceptedMembers, 1)
		assert.Equal(t, "zone1_pooler-2", records[0].AcceptedMembers[0].appName)
		assert.False(t, records[0].CreatedAt.IsZero())

		// Second record: term 1, subterm 0; nullable fields are nil/empty
		assert.Equal(t, int64(1), records[1].CoordinatorTerm)
		assert.Nil(t, records[1].WALPosition)
		assert.Nil(t, records[1].Operation)
		assert.Empty(t, records[1].AcceptedMembers)
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})

	t.Run("returns empty slice when no records exist", func(t *testing.T) {
		mockQueryService := mock.NewQueryService()
		rs := NewRuleStore(slog.New(slog.DiscardHandler), mockQueryService, noopSyncStandbyManager{})

		mockQueryService.AddQueryPatternOnce(
			"SELECT coordinator_term, leader_subterm, event_type",
			mock.MakeQueryResult(
				[]string{
					"coordinator_term", "leader_subterm", "event_type", "leader_id", "coordinator_id",
					"wal_position", "operation", "reason", "cohort_members", "accepted_members",
					"durability_policy_name", "durability_quorum_type", "durability_required_count",
					"decided", "created_at",
				},
				[][]any{},
			),
		)

		records, err := rs.queryRuleHistory(t.Context(), 10)
		require.NoError(t, err)
		assert.Empty(t, records)
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})

	t.Run("propagates query error", func(t *testing.T) {
		mockQueryService := mock.NewQueryService()
		rs := NewRuleStore(slog.New(slog.DiscardHandler), mockQueryService, noopSyncStandbyManager{})

		mockQueryService.AddQueryPatternOnceWithError(
			"SELECT coordinator_term, leader_subterm, event_type",
			errors.New("connection refused"),
		)

		_, err := rs.queryRuleHistory(t.Context(), 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query rule_history")
		assert.Contains(t, err.Error(), "connection refused")
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})
}

func TestCacheRuleObservation_StaleRuleIgnored(t *testing.T) {
	rs := newMockRuleStore(mock.NewQueryService())

	// Seed cache with rule number (2, 1).
	rs.cacheRuleObservation(makePoolerPosition(2, 1))
	require.NotNil(t, rs.CachedPosition())
	assert.Equal(t, int64(2), rs.CachedPosition().GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())

	// Stale observation: rule number (1, 5) is strictly less than (2, 1).
	rs.cacheRuleObservation(makePoolerPosition(1, 5))

	// Cache must still hold the newer rule.
	assert.Equal(t, int64(2), rs.CachedPosition().GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
	assert.Equal(t, int64(1), rs.CachedPosition().GetPosition().GetDecision().GetRuleNumber().GetLeaderSubterm())
}

func TestHasInconsistentGUC_FalseWhenCacheCold(t *testing.T) {
	rs := newMockRuleStore(mock.NewQueryService())
	// No position observed yet — CachedPosition returns nil, no policy to check.
	assert.False(t, rs.HasInconsistentGUC(t.Context()))
}

func TestHasInconsistentGUC_FalseWhenPolicyInvalid(t *testing.T) {
	rs := newMockRuleStore(mock.NewQueryService())
	// Pre-seed a position whose durability policy fails NewPolicyFromProto
	// (UNKNOWN quorum type). HasInconsistentGUC must swallow the error and
	// return false rather than crash or report drift.
	rs.lastPos = &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN,
				RequiredCount: 1,
			},
		}},
	}
	assert.False(t, rs.HasInconsistentGUC(t.Context()))
}

func TestHasInconsistentGUC_FalseWhenNeedsApplyErrors(t *testing.T) {
	rs := NewRuleStore(
		slog.New(slog.DiscardHandler),
		mock.NewQueryService(),
		errSyncStandbyManager{needsApplyErr: errors.New("postgres down")},
	)
	rs.lastPos = &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			DurabilityPolicy: testBootstrapPolicy(),
		}},
	}
	// NeedsApply errors → treat as "no known inconsistency" rather than reporting drift.
	assert.False(t, rs.HasInconsistentGUC(t.Context()))
}

// TestExpectedSyncStandbyPolicy covers expectedSyncStandbyPolicy directly,
// including the proposal-present branch (a leader-led rule change in
// flight): the decision and proposal's policies must be combined via
// BuildPolicyTransition, not just read off the decision alone.
func TestExpectedSyncStandbyPolicy(t *testing.T) {
	member1 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler-1"}
	member2 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler-2"}
	member3 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler-3"}

	atLeastN := func(n int32) *clustermetadatapb.DurabilityPolicy {
		return &clustermetadatapb.DurabilityPolicy{
			PolicyName:    "AT_LEAST_N",
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: n,
		}
	}
	invalidPolicy := &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN}

	t.Run("no decision durability policy errors", func(t *testing.T) {
		_, err := expectedSyncStandbyPolicy(&clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{},
		})
		require.ErrorContains(t, err, "no decision durability policy")
	})

	t.Run("invalid decision durability policy errors", func(t *testing.T) {
		_, err := expectedSyncStandbyPolicy(&clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{DurabilityPolicy: invalidPolicy},
		})
		require.ErrorContains(t, err, "invalid decision durability policy")
	})

	t.Run("no proposal returns decision policy directly", func(t *testing.T) {
		got, err := expectedSyncStandbyPolicy(&clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{
				CohortMembers:    []*clustermetadatapb.ID{member1, member2},
				DurabilityPolicy: atLeastN(2),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, 2, got.Policy.(commonconsensus.AtLeastNPolicy).N)
	})

	t.Run("proposal present with no durability policy errors", func(t *testing.T) {
		_, err := expectedSyncStandbyPolicy(&clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{
				CohortMembers:    []*clustermetadatapb.ID{member1, member2},
				DurabilityPolicy: atLeastN(2),
			},
			Proposal: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
			},
		})
		require.ErrorContains(t, err, "no proposal durability policy")
	})

	t.Run("invalid proposal durability policy errors", func(t *testing.T) {
		_, err := expectedSyncStandbyPolicy(&clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{
				CohortMembers:    []*clustermetadatapb.ID{member1, member2},
				DurabilityPolicy: atLeastN(2),
			},
			Proposal: &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
				DurabilityPolicy: invalidPolicy,
			},
		})
		require.ErrorContains(t, err, "invalid proposal durability policy")
	})

	t.Run("proposal present with same cohort and N returns that policy", func(t *testing.T) {
		got, err := expectedSyncStandbyPolicy(&clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{
				CohortMembers:    []*clustermetadatapb.ID{member1, member2},
				DurabilityPolicy: atLeastN(2),
			},
			Proposal: &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
				CohortMembers:    []*clustermetadatapb.ID{member1, member2},
				DurabilityPolicy: atLeastN(2),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, 2, got.Policy.(commonconsensus.AtLeastNPolicy).N)
	})

	t.Run("proposal raising N over the same cohort uses the stricter policy during transition", func(t *testing.T) {
		// A leader-led durability change in flight (decision still AT_LEAST_2,
		// proposal already AT_LEAST_3): the effective sync_standby_names GUC
		// must satisfy both until the proposal is decided, so Both uses the
		// larger N.
		got, err := expectedSyncStandbyPolicy(&clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{
				CohortMembers:    []*clustermetadatapb.ID{member1, member2, member3},
				DurabilityPolicy: atLeastN(2),
			},
			Proposal: &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
				CohortMembers:    []*clustermetadatapb.ID{member1, member2, member3},
				DurabilityPolicy: atLeastN(3),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, 3, got.Policy.(commonconsensus.AtLeastNPolicy).N)
	})

	t.Run("unsupported transition (both N and cohort change) errors", func(t *testing.T) {
		_, err := expectedSyncStandbyPolicy(&clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{
				CohortMembers:    []*clustermetadatapb.ID{member1, member2},
				DurabilityPolicy: atLeastN(2),
			},
			Proposal: &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
				CohortMembers:    []*clustermetadatapb.ID{member2, member3},
				DurabilityPolicy: atLeastN(1),
			},
		})
		require.ErrorContains(t, err, "computing decision->proposal transition")
	})
}

func TestAppNamesToIDs_InvalidName(t *testing.T) {
	_, err := appNamesToIDs([]string{"zone1_valid", "noseparator"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid ID "noseparator"`)
}

func TestParsePoolerIDStrings(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		ids, err := ParseReplicaIDStrings(nil)
		require.NoError(t, err)
		assert.Nil(t, ids)
	})

	t.Run("invalid name surfaces parse error", func(t *testing.T) {
		_, err := ParseReplicaIDStrings([]string{"noseparator"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid cell_name format")
	})
}

func TestScanRuleHistoryRow_LeaderIDInvalid(t *testing.T) {
	bad := "noseparator"
	rec := &ruleHistoryRecord{}
	err := scanRuleHistoryRow(rec, &bad, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse leader_id")
}

func TestScanRuleHistoryRow_CohortInvalid(t *testing.T) {
	rec := &ruleHistoryRecord{}
	err := scanRuleHistoryRow(rec, nil, []string{"noseparator"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse cohort_members")
}

func TestScanRuleHistoryRow_AcceptedInvalid(t *testing.T) {
	rec := &ruleHistoryRecord{}
	err := scanRuleHistoryRow(rec, nil, []string{"zone1_valid"}, []string{"noseparator"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse accepted_members")
}

// mkDecisionRow builds an unvalidatedRuleRow with all core columns set, as a
// real decision row always has.
func mkDecisionRow(coordinatorTerm, leaderSubterm int64, leaderIDStr *string, cohortNames []string, durabilityPolicyName, durabilityQuorumType string, durabilityRequiredCount int64) unvalidatedRuleRow {
	return unvalidatedRuleRow{
		coordinatorTerm:         &coordinatorTerm,
		leaderSubterm:           &leaderSubterm,
		leaderIDStr:             leaderIDStr,
		cohortNames:             cohortNames,
		durabilityPolicyName:    &durabilityPolicyName,
		durabilityQuorumType:    &durabilityQuorumType,
		durabilityRequiredCount: &durabilityRequiredCount,
	}
}

func TestBuildPoolerPosition_LeaderInvalid(t *testing.T) {
	bad := "noseparator"
	_, err := buildPoolerPosition(mkDecisionRow(1, 0, &bad, nil, "AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", 2), "", unvalidatedRuleRow{}, "0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse leader_id")
}

func TestBuildPoolerPosition_CoordinatorInvalid(t *testing.T) {
	_, err := buildPoolerPosition(mkDecisionRow(1, 0, nil, nil, "AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", 2), "noseparator", unvalidatedRuleRow{}, "0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse coordinator_id")
}

func TestBuildPoolerPosition_CohortInvalid(t *testing.T) {
	_, err := buildPoolerPosition(mkDecisionRow(1, 0, nil, []string{"noseparator"}, "AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", 2), "", unvalidatedRuleRow{}, "0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse cohort_members")
}

func TestBuildPoolerPosition_UnknownQuorumType(t *testing.T) {
	_, err := buildPoolerPosition(mkDecisionRow(1, 0, nil, nil, "AT_LEAST_2", "QUORUM_TYPE_BOGUS", 2), "", unvalidatedRuleRow{}, "0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown quorum_type")
}

func TestUpdateRule_ForceModeSkipsWrite(t *testing.T) {
	// Force-mode short-circuits before any postgres interaction, so this test
	// can use the mock QueryService — no real UpdateRule SQL would fire.
	rs := newMockRuleStore(mock.NewQueryService())
	coordinatorID := &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}
	update := NewRuleUpdate(1, coordinatorID, "force_event", "test", time.Now()).WithForce()

	pos, err := rs.UpdateRule(withTestActionLock(t), update)
	require.NoError(t, err)
	assert.Nil(t, pos, "force-mode UpdateRule returns nil position to signal no write happened")
}

func TestUpdateRule_RequiresCoordinatorID(t *testing.T) {
	rs := newMockRuleStore(mock.NewQueryService())
	update := NewRuleUpdate(1, nil /* coordinatorID */, "promotion", "test", time.Now())

	_, err := rs.UpdateRule(withTestActionLock(t), update)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UpdateRule requires a non-nil coordinator_id")
}

func TestUpdateRule_RequiresCreatedAt(t *testing.T) {
	rs := newMockRuleStore(mock.NewQueryService())
	coordinatorID := &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}
	update := NewRuleUpdate(1, coordinatorID, "promotion", "test", time.Time{} /* zero */)

	_, err := rs.UpdateRule(withTestActionLock(t), update)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UpdateRule requires a non-zero created_at")
}

// mockDecidedReadResult returns a mock result matching readCurrentRule's
// column list: a decided rule at term 1, subterm 0, with a leader, a
// two-member cohort, and an AT_LEAST_2 policy, and no pending proposal.
// Used to get UpdateRule past its initial read so tests can target a
// specific failure in the write path that follows.
func mockDecidedReadResult() *sqltypes.Result {
	return mock.MakeQueryResult(
		[]string{
			"decision_coordinator_term", "decision_leader_subterm", "leader_id", "coordinator_id", "cohort_members",
			"durability_policy_name", "durability_quorum_type", "durability_required_count", "created_at",
			"proposal_coordinator_term", "proposal_leader_subterm", "proposal_leader_id", "proposal_cohort_members",
			"proposal_durability_policy_name", "proposal_durability_quorum_type", "proposal_durability_required_count",
			"proposal_created_at", "current_lsn",
		},
		[][]any{
			{
				int64(1), int64(0), "zone1_leader-1", "zone1_coordinator-1", "{zone1_member-1,zone1_member-2}",
				"AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", int64(2), "2026-01-01 00:00:00+00",
				nil, nil, nil, nil, nil, nil, nil, nil,
				"0/100",
			},
		},
	)
}

// readPattern matches readCurrentRule's SELECT; proposeWritePattern and
// expectedTermPattern match the propose and finalize queries respectively —
// each names a params column unique to that query, so tests can target one
// write phase without accidentally matching another.
const (
	readPattern         = "SELECT decision_coordinator_term, decision_leader_subterm, leader_id, coordinator_id, cohort_members"
	proposeWritePattern = "AS new_subterm"
	expectedTermPattern = "AS expected_term"
)

func TestUpdateRule_ProposeWriteErrorPropagated(t *testing.T) {
	mockQueryService := mock.NewQueryService()
	rs := newMockRuleStore(mockQueryService)

	mockQueryService.AddQueryPatternOnce(readPattern, mockDecidedReadResult())
	mockQueryService.AddQueryPatternOnceWithError(proposeWritePattern, errors.New("connection reset"))

	coordinatorID := &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}
	_, err := rs.UpdateRule(withTestActionLock(t), NewRuleUpdate(1, coordinatorID, "config_change", "test", time.Now()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write rule proposal")
	assert.Contains(t, err.Error(), "connection reset")
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

func TestUpdateRule_ProposeScanErrorPropagated(t *testing.T) {
	mockQueryService := mock.NewQueryService()
	rs := newMockRuleStore(mockQueryService)

	mockQueryService.AddQueryPatternOnce(readPattern, mockDecidedReadResult())
	// The propose query succeeds but returns far fewer columns than UpdateRule
	// scans for — a malformed response, not a query error.
	mockQueryService.AddQueryPatternOnce(proposeWritePattern,
		mock.MakeQueryResult([]string{"decision_coordinator_term"}, [][]any{{int64(1)}}))

	coordinatorID := &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}
	_, err := rs.UpdateRule(withTestActionLock(t), NewRuleUpdate(1, coordinatorID, "config_change", "test", time.Now()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to scan written rule proposal")
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

func TestUpdateRule_ProposeParseErrorPropagated(t *testing.T) {
	mockQueryService := mock.NewQueryService()
	rs := newMockRuleStore(mockQueryService)

	mockQueryService.AddQueryPatternOnce(readPattern, mockDecidedReadResult())
	// The propose query succeeds and scans cleanly, but the decision it
	// returns carries a durability_quorum_type buildShardRule can't parse —
	// malformed content, not a malformed row shape.
	mockQueryService.AddQueryPatternOnce(proposeWritePattern, mock.MakeQueryResult(
		[]string{
			"decision_coordinator_term", "decision_leader_subterm", "leader_id", "coordinator_id", "cohort_members",
			"durability_policy_name", "durability_quorum_type", "durability_required_count", "created_at",
			"proposal_coordinator_term", "proposal_leader_subterm", "proposal_leader_id", "proposal_cohort_members",
			"proposal_durability_policy_name", "proposal_durability_quorum_type", "proposal_durability_required_count",
			"proposal_created_at", "current_lsn",
		},
		[][]any{
			{
				int64(1), int64(1), "zone1_leader-1", "zone1_coordinator-1", "{zone1_member-1,zone1_member-2}",
				"AT_LEAST_2", "QUORUM_TYPE_BOGUS", int64(2), "2026-01-01 00:00:00+00",
				nil, nil, nil, nil, nil, nil, nil, nil,
				"0/100",
			},
		},
	))

	coordinatorID := &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}
	_, err := rs.UpdateRule(withTestActionLock(t), NewRuleUpdate(1, coordinatorID, "config_change", "test", time.Now()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse proposal row into memory")
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

func TestUpdateRule_FinalizeErrorPropagated(t *testing.T) {
	mockQueryService := mock.NewQueryService()
	rs := newMockRuleStore(mockQueryService)

	mockQueryService.AddQueryPatternOnce(readPattern, mockDecidedReadResult())
	mockQueryService.AddQueryPatternOnce(proposeWritePattern, mock.MakeQueryResult(
		[]string{
			"decision_coordinator_term", "decision_leader_subterm", "leader_id", "coordinator_id", "cohort_members",
			"durability_policy_name", "durability_quorum_type", "durability_required_count", "created_at",
			"proposal_coordinator_term", "proposal_leader_subterm", "proposal_leader_id", "proposal_cohort_members",
			"proposal_durability_policy_name", "proposal_durability_quorum_type", "proposal_durability_required_count",
			"proposal_created_at", "current_lsn",
		},
		[][]any{
			{
				int64(1), int64(0), "zone1_leader-1", "zone1_coordinator-1", "{zone1_member-1,zone1_member-2}",
				"AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", int64(2), "2026-01-01 00:00:00+00",
				int64(1), int64(1), "zone1_leader-1", "{zone1_member-1,zone1_member-2}",
				"AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", int64(2), "2026-01-01 00:01:00+00",
				"0/100",
			},
		},
	))
	mockQueryService.AddQueryPatternOnceWithError(expectedTermPattern, errors.New("connection reset"))

	coordinatorID := &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}
	_, err := rs.UpdateRule(withTestActionLock(t), NewRuleUpdate(1, coordinatorID, "config_change", "test", time.Now()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to promote rule proposal to decision")
	assert.Contains(t, err.Error(), "connection reset")
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}
