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
		Rule: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{
				CoordinatorTerm: coordinatorTerm,
				LeaderSubterm:   leaderSubterm,
			},
		},
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
					"created_at",
				},
				[][]any{
					{
						int64(2), int64(1), "promotion", leaderAppName, coordID, walPos, operation,
						"manual failover", "{zone1_pooler-2,zone1_pooler-3}", "{zone1_pooler-2}",
						nil, nil, nil, createdAt,
					},
					{
						int64(1), int64(0), "replication_config", leaderAppName, coordID, nil, nil,
						"initial bootstrap", "{zone1_pooler-1,zone1_pooler-2}", nil,
						nil, nil, nil, createdAt,
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
					"created_at",
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
	assert.Equal(t, int64(2), rs.CachedPosition().GetRule().GetRuleNumber().GetCoordinatorTerm())

	// Stale observation: rule number (1, 5) is strictly less than (2, 1).
	rs.cacheRuleObservation(makePoolerPosition(1, 5))

	// Cache must still hold the newer rule.
	assert.Equal(t, int64(2), rs.CachedPosition().GetRule().GetRuleNumber().GetCoordinatorTerm())
	assert.Equal(t, int64(1), rs.CachedPosition().GetRule().GetRuleNumber().GetLeaderSubterm())
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
		Rule: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN,
				RequiredCount: 1,
			},
		},
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
		Rule: &clustermetadatapb.ShardRule{
			RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			DurabilityPolicy: testBootstrapPolicy(),
		},
	}
	// NeedsApply errors → treat as "no known inconsistency" rather than reporting drift.
	assert.False(t, rs.HasInconsistentGUC(t.Context()))
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

func TestBuildPoolerPosition_LeaderInvalid(t *testing.T) {
	bad := "noseparator"
	_, err := buildPoolerPosition(1, 0, &bad, "", nil, "AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", 2, time.Time{}, "0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse leader_id")
}

func TestBuildPoolerPosition_CoordinatorInvalid(t *testing.T) {
	_, err := buildPoolerPosition(1, 0, nil, "noseparator", nil, "AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", 2, time.Time{}, "0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse coordinator_id")
}

func TestBuildPoolerPosition_CohortInvalid(t *testing.T) {
	_, err := buildPoolerPosition(1, 0, nil, "", []string{"noseparator"}, "AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", 2, time.Time{}, "0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse cohort_members")
}

func TestBuildPoolerPosition_UnknownQuorumType(t *testing.T) {
	_, err := buildPoolerPosition(1, 0, nil, "", nil, "AT_LEAST_2", "QUORUM_TYPE_BOGUS", 2, time.Time{}, "0/0")
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
