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

package manager

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/multigres/multigres/go/services/multipooler/executor/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryRuleHistory(t *testing.T) {
	t.Run("returns records ordered newest first", func(t *testing.T) {
		mockQueryService := mock.NewQueryService()
		rs := newRuleStore(slog.New(slog.NewTextHandler(io.Discard, nil)), mockQueryService)

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
					"durability_async_fallback", "created_at",
				},
				[][]any{
					{
						int64(2), int64(1), "promotion", leaderAppName, coordID, walPos, operation,
						"manual failover", "{zone1_pooler-2,zone1_pooler-3}", "{zone1_pooler-2}",
						nil, nil, nil, nil, createdAt,
					},
					{
						int64(1), int64(0), "replication_config", leaderAppName, coordID, nil, nil,
						"initial bootstrap", "{zone1_pooler-1,zone1_pooler-2}", nil,
						nil, nil, nil, nil, createdAt,
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
		rs := newRuleStore(slog.New(slog.NewTextHandler(io.Discard, nil)), mockQueryService)

		mockQueryService.AddQueryPatternOnce(
			"SELECT coordinator_term, leader_subterm, event_type",
			mock.MakeQueryResult(
				[]string{
					"coordinator_term", "leader_subterm", "event_type", "leader_id", "coordinator_id",
					"wal_position", "operation", "reason", "cohort_members", "accepted_members",
					"durability_policy_name", "durability_quorum_type", "durability_required_count",
					"durability_async_fallback", "created_at",
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
		rs := newRuleStore(slog.New(slog.NewTextHandler(io.Discard, nil)), mockQueryService)

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
