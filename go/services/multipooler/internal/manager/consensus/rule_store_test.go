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
	"github.com/multigres/multigres/go/tools/pgutil"

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

// mockProposeWriteResult is the result of the proposal write, reporting currentLSN.
func mockProposeWriteResult(currentLSN string) *sqltypes.Result {
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
				int64(1), int64(1), "zone1_leader-1", "{zone1_member-1,zone1_member-2}",
				"AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", int64(2), "2026-01-01 00:01:00+00",
				currentLSN,
			},
		},
	)
}

// TestUpdateRule_ProposalAckedHookRunsBeforeFinalizeWithTheProposalLSN: once the
// proposal write has returned (a sync standby acked it), and before the decision is
// written, the hook receives that write's LSN and the time the write started. A failure
// to decide must not undo it.
func TestUpdateRule_ProposalAckedHookRunsBeforeFinalizeWithTheProposalLSN(t *testing.T) {
	mockQueryService := mock.NewQueryService()
	rs := newMockRuleStore(mockQueryService)

	var events []string
	var gotLSN pgutil.LSN
	var gotStartedAt time.Time
	rs.onProposalAcked = func(_ context.Context, lsn pgutil.LSN, startedAt time.Time) {
		events = append(events, "hook")
		gotLSN, gotStartedAt = lsn, startedAt
	}

	var writeServedAt time.Time
	mockQueryService.AddQueryPatternOnce(readPattern, mockDecidedReadResult())
	mockQueryService.AddQueryPatternWithCallback(proposeWritePattern, mockProposeWriteResult("0/100"),
		func(string) { writeServedAt = time.Now() })
	mockQueryService.AddQueryPatternOnceWithError(expectedTermPattern, errors.New("connection reset"))

	before := time.Now()
	coordinatorID := &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}
	_, err := rs.UpdateRule(withTestActionLock(t), NewRuleUpdate(1, coordinatorID, "config_change", "test", time.Now()))

	require.Error(t, err, "the finalize failed")
	require.Equal(t, []string{"hook"}, events, "the hook ran once, before the finalize, even though the finalize failed")
	assert.Equal(t, "0/100", gotLSN.String(), "it receives the proposal write's LSN")
	assert.False(t, gotStartedAt.Before(before), "the start time is from this update")
	assert.False(t, gotStartedAt.After(writeServedAt), "the start time is taken before the proposal write, not after it was acked")
}

// TestUpdateRule_NoProofWhenTheProposalLSNIsUnparseable: a proposal write that reports
// an LSN we can't parse still gets decided, just without recording a proof.
func TestUpdateRule_NoProofWhenTheProposalLSNIsUnparseable(t *testing.T) {
	mockQueryService := mock.NewQueryService()
	rs := newMockRuleStore(mockQueryService)

	hookCalls := 0
	rs.onProposalAcked = func(context.Context, pgutil.LSN, time.Time) { hookCalls++ }

	mockQueryService.AddQueryPatternOnce(readPattern, mockDecidedReadResult())
	mockQueryService.AddQueryPatternOnce(proposeWritePattern, mockProposeWriteResult("not-an-lsn"))
	mockQueryService.AddQueryPatternOnceWithError(expectedTermPattern, errors.New("connection reset"))

	coordinatorID := &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}
	_, err := rs.UpdateRule(withTestActionLock(t), NewRuleUpdate(1, coordinatorID, "config_change", "test", time.Now()))

	require.ErrorContains(t, err, "connection reset", "the decision was still attempted")
	assert.Zero(t, hookCalls)
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestUpdateRule_RefreshesAnOldQuorumProofBeforeDeciding: a proof from a slow proposal
// write would make the decided rule look stalled, so it is refreshed by a no-op write
// that waits for the same acks. A fast proposal is not refreshed, and a failed refresh
// leaves the older proof in place.
func TestUpdateRule_RefreshesAnOldQuorumProofBeforeDeciding(t *testing.T) {
	type outcome struct {
		lsn              string
		startedAt        time.Time
		proposalServedAt time.Time
		refreshes        int
	}
	run := func(t *testing.T, refreshAfter time.Duration, refreshErr error) outcome {
		t.Helper()
		var got outcome
		qs := mock.NewQueryService()
		rs := newMockRuleStore(qs)
		rs.proofRefreshAfter = refreshAfter
		rs.onProposalAcked = func(_ context.Context, lsn pgutil.LSN, startedAt time.Time) {
			got.lsn, got.startedAt = lsn.String(), startedAt
		}
		qs.AddQueryPatternOnce(readPattern, mockDecidedReadResult())
		qs.AddQueryPatternWithCallback(proposeWritePattern, mockProposeWriteResult("0/100"),
			func(string) { got.proposalServedAt = time.Now() })
		// Registered before the decide pattern: the refresh query also mentions expected_term.
		if refreshErr != nil {
			qs.AddQueryPatternWithError(confirmProposalQuorumPattern, refreshErr)
		} else {
			qs.AddQueryPatternWithCallback(confirmProposalQuorumPattern,
				mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/200"}}),
				func(string) { got.refreshes++ })
		}
		qs.AddQueryPatternOnceWithError(expectedTermPattern, errors.New("connection reset"))

		coordinatorID := &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}
		_, err := rs.UpdateRule(withTestActionLock(t), NewRuleUpdate(1, coordinatorID, "config_change", "test", time.Now()))
		require.ErrorContains(t, err, "connection reset", "the decision was still attempted")
		return got
	}

	t.Run("a fast proposal is not refreshed", func(t *testing.T) {
		got := run(t, time.Hour, nil)
		assert.Equal(t, "0/100", got.lsn)
		assert.Zero(t, got.refreshes)
	})

	t.Run("a slow proposal gets a newer proof from a fresh write", func(t *testing.T) {
		got := run(t, time.Nanosecond, nil)
		assert.Equal(t, "0/200", got.lsn, "the proof is the refresh write's")
		assert.Equal(t, 1, got.refreshes)
		assert.False(t, got.startedAt.Before(got.proposalServedAt), "its start time is after the proposal write")
	})

	t.Run("a failed refresh keeps the older proof", func(t *testing.T) {
		got := run(t, time.Nanosecond, errors.New("standby gone"))
		assert.Equal(t, "0/100", got.lsn)
		assert.False(t, got.startedAt.After(got.proposalServedAt), "the older proof's start time stands")
	})
}

// confirmProposalQuorumPattern matches confirmProposalQuorum's no-op write.
const confirmProposalQuorumPattern = `(?s)SET proposal_created_at = proposal_created_at.*RETURNING pg_current_wal_lsn`

func TestConfirmProposalQuorum(t *testing.T) {
	newStore := func(configure func(*mock.QueryService)) *ruleStore {
		mockQueryService := mock.NewQueryService()
		configure(mockQueryService)
		return newMockRuleStore(mockQueryService)
	}

	t.Run("returns the LSN read before the write committed", func(t *testing.T) {
		rs := newStore(func(qs *mock.QueryService) {
			qs.AddQueryPattern(confirmProposalQuorumPattern, mock.MakeQueryResult(
				[]string{"pg_current_wal_lsn"}, [][]any{{"0/ABC"}}))
		})
		lsn, err := rs.confirmProposalQuorum(t.Context(), 4, 1)
		require.NoError(t, err)
		assert.Equal(t, "0/ABC", lsn.String())
	})

	t.Run("a query error is wrapped", func(t *testing.T) {
		rs := newStore(func(qs *mock.QueryService) {
			qs.AddQueryPatternWithError(confirmProposalQuorumPattern, errors.New("connection reset"))
		})
		_, err := rs.confirmProposalQuorum(t.Context(), 4, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to confirm stuck proposal quorum")
		assert.Contains(t, err.Error(), "connection reset")
	})

	t.Run("no row means the proposal changed under us", func(t *testing.T) {
		rs := newStore(func(qs *mock.QueryService) {
			qs.AddQueryPattern(confirmProposalQuorumPattern, mock.MakeQueryResult(
				[]string{"pg_current_wal_lsn"}, [][]any{}))
		})
		_, err := rs.confirmProposalQuorum(t.Context(), 4, 1)
		assert.ErrorIs(t, err, errRuleConflict)
	})

	t.Run("a row without the LSN column is reported", func(t *testing.T) {
		rs := newStore(func(qs *mock.QueryService) {
			qs.AddQueryPattern(confirmProposalQuorumPattern, mock.MakeQueryResult(
				[]string{"pg_current_wal_lsn"}, [][]any{{}}))
		})
		_, err := rs.confirmProposalQuorum(t.Context(), 4, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read confirmed proposal LSN")
	})

	t.Run("an unparseable LSN is reported", func(t *testing.T) {
		rs := newStore(func(qs *mock.QueryService) {
			qs.AddQueryPattern(confirmProposalQuorumPattern, mock.MakeQueryResult(
				[]string{"pg_current_wal_lsn"}, [][]any{{"not-an-lsn"}}))
		})
		_, err := rs.confirmProposalQuorum(t.Context(), 4, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse confirmed proposal LSN")
	})
}
