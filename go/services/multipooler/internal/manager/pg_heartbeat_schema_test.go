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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/mterrors"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/heartbeat"
	"github.com/multigres/multigres/go/tools/pgutil"
	"github.com/multigres/multigres/go/tools/telemetry"
)

var (
	compatibleHeartbeatSchema   = &heartbeat.SchemaReport{}
	incompatibleHeartbeatSchema = &heartbeat.SchemaReport{Problems: []heartbeat.SchemaProblem{
		{Kind: heartbeat.ProblemMissingColumn, Column: "leader_id", Detail: "column is missing (expected text)"},
	}}
)

func TestHeartbeatSchemaBroken(t *testing.T) {
	pm := newHeartbeatProofManager(t, mock.NewQueryService())

	assert.False(t, pm.heartbeatSchemaBroken(), "unchecked is not broken: absence of a check must not suppress the proof")

	pm.heartbeatSchema.Store(compatibleHeartbeatSchema)
	assert.False(t, pm.heartbeatSchemaBroken())

	pm.heartbeatSchema.Store(incompatibleHeartbeatSchema)
	assert.True(t, pm.heartbeatSchemaBroken())
}

// TestLeaderQuorumCommitProofWithheldWhenHeartbeatTableIsBroken: the leader's quorum-commit
// proof in PrimaryStatus is what multiorch ages into LeaderQuorumWritesStalled, and only a
// working heartbeat writer refreshes it. With the heartbeat table incompatible the proof
// only ages, so it is withheld rather than driving a failover to a node with the same table.
func TestLeaderQuorumCommitProofWithheldWhenHeartbeatTableIsBroken(t *testing.T) {
	lsn, err := pgutil.ParseLSN("0/100")
	require.NoError(t, err)

	proof := func(pm *MultipoolerManager) *multipoolermanagerdatapb.PrimaryStatus {
		status := &multipoolermanagerdatapb.PrimaryStatus{}
		pm.setLeaderQuorumCommitProof(status)
		return status
	}

	t.Run("reported when the table is fine", func(t *testing.T) {
		queryService := mock.NewQueryService()
		queryService.AddQueryPattern(heartbeatWritePattern, mock.MakeQueryResult([]string{"lsn"}, [][]any{{"0/900"}}))
		pm := newHeartbeatProofManager(t, queryService)
		pm.writeHeartbeatForAckedProposal(t.Context(), lsn, time.Now())

		status := proof(pm)
		assert.Equal(t, "0/100", status.QuorumCommitLsn)
		assert.NotNil(t, status.QuorumCommitTs)
	})

	t.Run("withheld after a schema check found the table incompatible", func(t *testing.T) {
		queryService := mock.NewQueryService()
		queryService.AddQueryPattern(heartbeatWritePattern, mock.MakeQueryResult([]string{"lsn"}, [][]any{{"0/900"}}))
		pm := newHeartbeatProofManager(t, queryService)
		pm.writeHeartbeatForAckedProposal(t.Context(), lsn, time.Now())
		pm.heartbeatSchema.Store(incompatibleHeartbeatSchema)

		status := proof(pm)
		assert.Empty(t, status.QuorumCommitLsn)
		assert.Nil(t, status.QuorumCommitTs)
	})

	t.Run("withheld as soon as a write fails for a schema reason, before the next check", func(t *testing.T) {
		queryService := mock.NewQueryService()
		queryService.AddQueryPatternWithError(heartbeatWritePattern, mterrors.NewPgError("ERROR", "42703", `column "quorum_commit_lsn" does not exist`, ""))
		pm := newHeartbeatProofManager(t, queryService)
		// The proof is recorded even though its write fails, which is what would age.
		pm.writeHeartbeatForAckedProposal(t.Context(), lsn, time.Now())
		_, _, have := pm.replTracker.HeartbeatWriter().LastProven()
		require.True(t, have)

		status := proof(pm)
		assert.Empty(t, status.QuorumCommitLsn)
		assert.Nil(t, status.QuorumCommitTs)
	})

	t.Run("kept on a transient write failure, so a real stall still convicts", func(t *testing.T) {
		queryService := mock.NewQueryService()
		queryService.AddQueryPatternWithError(heartbeatWritePattern, mterrors.NewPgError("ERROR", "57014", "canceling statement due to statement timeout", ""))
		pm := newHeartbeatProofManager(t, queryService)
		pm.writeHeartbeatForAckedProposal(t.Context(), lsn, time.Now())

		status := proof(pm)
		assert.Equal(t, "0/100", status.QuorumCommitLsn)
		assert.NotNil(t, status.QuorumCommitTs)
	})
}

func TestRecordHeartbeatSchemaPublishesGauge(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))
	metrics, err := newManagerMetrics()
	require.NoError(t, err)

	pm := newHeartbeatProofManager(t, mock.NewQueryService())
	pm.metrics = metrics

	value := func() (int64, bool) {
		var rm metricdata.ResourceMetrics
		require.NoError(t, setup.MetricReader.Collect(t.Context(), &rm))
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != "mg.pooler.heartbeat.schema.compatible" {
					continue
				}
				if points := m.Data.(metricdata.Gauge[int64]).DataPoints; len(points) > 0 {
					return points[0].Value, true
				}
			}
		}
		return 0, false
	}
	_, ok := value()
	assert.False(t, ok, "nothing is observed before the first check")

	pm.recordHeartbeatSchema(t.Context(), incompatibleHeartbeatSchema)
	got, ok := value()
	require.True(t, ok)
	assert.EqualValues(t, 0, got)
	assert.Same(t, incompatibleHeartbeatSchema, pm.heartbeatSchema.Load())

	pm.recordHeartbeatSchema(t.Context(), compatibleHeartbeatSchema)
	got, ok = value()
	require.True(t, ok)
	assert.EqualValues(t, 1, got)
}

// TestMaybeRefreshHeartbeatSchema: a healthy steady state costs no queries; a schema error
// since the last check, or a standing "incompatible" verdict, triggers a re-check.
func TestMaybeRefreshHeartbeatSchema(t *testing.T) {
	queryService := mock.NewQueryService()
	pm := newHeartbeatProofManager(t, queryService)
	pm.qsc = &mockPoolerController{queryService: queryService}
	// pg_is_in_recovery() = t: a standby, so the refresh is the read-only check.
	queryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	checks := 0
	queryService.AddQueryPatternWithCallback("pg_attribute a\\s+WHERE", mock.MakeQueryResult(
		[]string{"attname", "format_type", "required"}, [][]any{
			{"shard_id", "bytea", "t"},
			{"leader_id", "text", "t"},
			{"ts", "bigint", "t"},
			{"quorum_commit_lsn", "pg_lsn", "f"},
			{"quorum_commit_ts", "timestamp with time zone", "f"},
		},
	), func(string) { checks++ })
	queryService.AddQueryPattern("SELECT EXISTS", mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))
	ctx := t.Context()

	pm.heartbeatSchema.Store(compatibleHeartbeatSchema)
	pm.maybeRefreshHeartbeatSchema(ctx)
	assert.Zero(t, checks, "compatible and no new schema errors: nothing to do")

	// A schema error from the writer since the last check.
	queryService.AddQueryPatternOnceWithError(heartbeatWritePattern, mterrors.NewPgError("ERROR", "42703", "no such column", ""))
	pm.writeHeartbeatForAckedProposal(ctx, 1, time.Now())
	pm.maybeRefreshHeartbeatSchema(ctx)
	assert.Equal(t, 1, checks)
	pm.maybeRefreshHeartbeatSchema(ctx)
	assert.Equal(t, 1, checks, "the error was accounted for by the check")

	// An incompatible verdict is re-checked until the table is repaired.
	pm.heartbeatSchema.Store(incompatibleHeartbeatSchema)
	pm.maybeRefreshHeartbeatSchema(ctx)
	assert.Equal(t, 2, checks)
	assert.True(t, pm.heartbeatSchema.Load().Compatible(), "the re-check found the table repaired")
	pm.maybeRefreshHeartbeatSchema(ctx)
	assert.Equal(t, 2, checks)
}
