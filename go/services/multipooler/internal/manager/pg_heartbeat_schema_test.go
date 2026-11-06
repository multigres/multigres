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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
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

const (
	hbColumnsPattern = "pg_attribute a\\s+WHERE"
	hbKeyPattern     = "SELECT EXISTS"
	hbAlterPattern   = "ALTER TABLE IF EXISTS multigres\\.heartbeat"
	hbRecoveryQuery  = "SELECT pg_is_in_recovery"
)

var hbColumnsCols = []string{"attname", "format_type", "required"}

func hbCurrentColumns() [][]any {
	return [][]any{
		{"shard_id", "bytea", "t"},
		{"leader_id", "text", "t"},
		{"ts", "bigint", "t"},
		{"quorum_commit_lsn", "pg_lsn", "f"},
		{"quorum_commit_ts", "timestamp with time zone", "f"},
	}
}

func hbLegacyColumns() [][]any { return hbCurrentColumns()[:3] }

func hbInRecovery(inRecovery string) *sqltypes.Result {
	return mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{inRecovery}})
}

// lockedBuffer is a log sink safe to write from the health loop and read from the test.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newHeartbeatSchemaManager returns a manager whose internal query service is the returned
// mock, logging to the returned buffer.
func newHeartbeatSchemaManager(t *testing.T) (*MultipoolerManager, *mock.QueryService, *lockedBuffer) {
	t.Helper()
	queryService := mock.NewQueryService()
	pm := newHeartbeatProofManager(t, queryService)
	pm.qsc = &mockPoolerController{queryService: queryService}
	logs := &lockedBuffer{}
	pm.logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return pm, queryService, logs
}

func TestEnsureHeartbeatSchemaFailurePaths(t *testing.T) {
	t.Run("no query service is a no-op", func(t *testing.T) {
		pm, _, _ := newHeartbeatSchemaManager(t)
		pm.qsc = nil
		assert.NotPanics(t, func() {
			pm.ensureHeartbeatSchema(t.Context())
			pm.checkHeartbeatSchema(t.Context())
		})
		assert.Nil(t, pm.heartbeatSchema.Load())
	})

	t.Run("a check that cannot run keeps the previous verdict", func(t *testing.T) {
		pm, queryService, logs := newHeartbeatSchemaManager(t)
		pm.heartbeatSchema.Store(compatibleHeartbeatSchema)
		queryService.AddQueryPatternWithError(hbColumnsPattern, errors.New("connection reset"))

		pm.ensureHeartbeatSchema(t.Context())
		pm.checkHeartbeatSchema(t.Context())

		assert.Same(t, compatibleHeartbeatSchema, pm.heartbeatSchema.Load(), "an error is not a verdict")
		assert.Contains(t, logs.String(), "failed to ensure the heartbeat table schema")
		assert.Contains(t, logs.String(), "failed to check the heartbeat table schema")
	})

	t.Run("a failing ALTER records the incompatible table and does not panic", func(t *testing.T) {
		pm, queryService, logs := newHeartbeatSchemaManager(t)
		queryService.AddQueryPattern(hbColumnsPattern, mock.MakeQueryResult(hbColumnsCols, hbLegacyColumns()))
		queryService.AddQueryPattern(hbKeyPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))
		queryService.AddQueryPatternWithError(hbAlterPattern, errors.New("cannot execute ALTER TABLE in a read-only transaction"))

		pm.ensureHeartbeatSchema(t.Context())

		report := pm.heartbeatSchema.Load()
		require.NotNil(t, report)
		assert.False(t, report.Compatible())
		assert.True(t, pm.heartbeatSchemaBroken())
		assert.Contains(t, logs.String(), "failed to ensure the heartbeat table schema")
	})

	t.Run("an upgrade is recorded as compatible", func(t *testing.T) {
		pm, queryService, _ := newHeartbeatSchemaManager(t)
		queryService.AddQueryPatternOnce(hbColumnsPattern, mock.MakeQueryResult(hbColumnsCols, hbLegacyColumns()))
		queryService.AddQueryPatternOnce(hbKeyPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))
		queryService.AddQueryPattern(hbAlterPattern, mock.MakeQueryResult(nil, nil))
		queryService.AddQueryPattern(hbColumnsPattern, mock.MakeQueryResult(hbColumnsCols, hbCurrentColumns()))
		queryService.AddQueryPattern(hbKeyPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))

		pm.ensureHeartbeatSchema(t.Context())

		assert.True(t, pm.heartbeatSchema.Load().Compatible())
		assert.False(t, pm.heartbeatSchemaBroken())
	})
}

// TestRefreshHeartbeatSchemaByRole: only a primary may alter the table; a standby only
// checks it, since the change reaches it by replication.
func TestRefreshHeartbeatSchemaByRole(t *testing.T) {
	setup := func(t *testing.T, recovery *sqltypes.Result, recoveryErr error) (*MultipoolerManager, *atomic.Int32, *atomic.Int32) {
		pm, queryService, _ := newHeartbeatSchemaManager(t)
		var altered, checked atomic.Int32
		if recoveryErr != nil {
			queryService.AddQueryPatternWithError(hbRecoveryQuery, recoveryErr)
		} else {
			queryService.AddQueryPattern(hbRecoveryQuery, recovery)
		}
		queryService.AddQueryPatternWithCallback(hbColumnsPattern, mock.MakeQueryResult(hbColumnsCols, hbLegacyColumns()),
			func(string) { checked.Add(1) })
		queryService.AddQueryPattern(hbKeyPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))
		queryService.AddQueryPatternWithCallback(hbAlterPattern, mock.MakeQueryResult(nil, nil), func(string) { altered.Add(1) })
		return pm, &altered, &checked
	}

	t.Run("a primary upgrades the table", func(t *testing.T) {
		pm, altered, _ := setup(t, hbInRecovery("f"), nil)
		pm.refreshHeartbeatSchema(t.Context())
		assert.EqualValues(t, 1, altered.Load())
		assert.NotNil(t, pm.heartbeatSchema.Load())
	})

	t.Run("a standby only checks, even for an upgradable table", func(t *testing.T) {
		pm, altered, checked := setup(t, hbInRecovery("t"), nil)
		pm.refreshHeartbeatSchema(t.Context())
		assert.EqualValues(t, 0, altered.Load(), "a standby must not run DDL")
		assert.EqualValues(t, 1, checked.Load())
		report := pm.heartbeatSchema.Load()
		require.NotNil(t, report)
		assert.False(t, report.Compatible())
		assert.True(t, report.Upgradable())
	})

	t.Run("an unknown role does nothing", func(t *testing.T) {
		pm, altered, checked := setup(t, nil, errors.New("connection refused"))
		pm.heartbeatSchema.Store(compatibleHeartbeatSchema)
		assert.NotPanics(t, func() { pm.refreshHeartbeatSchema(t.Context()) })
		assert.EqualValues(t, 0, altered.Load())
		assert.EqualValues(t, 0, checked.Load(), "no check without knowing whether the DDL is allowed")
		assert.Same(t, compatibleHeartbeatSchema, pm.heartbeatSchema.Load())
	})
}

// TestRecordHeartbeatSchemaLogsOnTransitions: the ERROR is logged when the verdict is new
// or changes, not on every re-check of an unchanged incompatible table.
func TestRecordHeartbeatSchemaLogsOnTransitions(t *testing.T) {
	pm, _, logs := newHeartbeatSchemaManager(t)
	ctx := t.Context()
	other := &heartbeat.SchemaReport{Problems: []heartbeat.SchemaProblem{{Kind: heartbeat.ProblemWrongType, Column: "ts", Detail: "x"}}}
	errorLogs := func() int { return strings.Count(logs.String(), "level=ERROR") }

	pm.recordHeartbeatSchema(ctx, compatibleHeartbeatSchema)
	assert.Equal(t, 0, errorLogs(), "compatible from the start is silent")

	pm.recordHeartbeatSchema(ctx, incompatibleHeartbeatSchema)
	assert.Equal(t, 1, errorLogs())
	pm.recordHeartbeatSchema(ctx, &heartbeat.SchemaReport{Problems: incompatibleHeartbeatSchema.Problems})
	assert.Equal(t, 1, errorLogs(), "the same problems again are not logged again")

	pm.recordHeartbeatSchema(ctx, other)
	assert.Equal(t, 2, errorLogs(), "a different problem is logged")

	assert.NotContains(t, logs.String(), "compatible again")
	pm.recordHeartbeatSchema(ctx, compatibleHeartbeatSchema)
	assert.Contains(t, logs.String(), "compatible again")
	assert.Equal(t, 2, errorLogs())
}

// TestCreateSidecarSchemaNeverFailsOnHeartbeatSchema: an incompatible heartbeat table is
// reported, not fatal, so it cannot fail the bootstrap that runs it (nor, by the same call,
// an open). This is the policy documented in pg_heartbeat_schema.go.
func TestCreateSidecarSchemaNeverFailsOnHeartbeatSchema(t *testing.T) {
	for _, tt := range []struct {
		name       string
		columns    [][]any
		checkError error
		wantOK     bool
		wantNil    bool
	}{
		{name: "compatible", columns: hbCurrentColumns(), wantOK: true},
		{name: "incompatible beyond repair", columns: hbCurrentColumns()[1:]},
		{name: "check cannot run", checkError: errors.New("connection reset"), wantNil: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pm, queryService := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
			queryService.AddQueryPatternOnce("CREATE SCHEMA multigres", mock.MakeQueryResult(nil, nil))
			queryService.AddQueryPatternOnce("CREATE TABLE multigres.heartbeat", mock.MakeQueryResult(nil, nil))
			queryService.AddQueryPatternOnce("CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid", mock.MakeQueryResult(nil, nil))
			queryService.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos", mock.MakeQueryResult(nil, nil))
			queryService.AddQueryPatternOnce("CREATE TABLE multigres.current_rule", mock.MakeQueryResult(nil, nil))
			queryService.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
			queryService.AddQueryPatternOnce("CREATE TABLE multigres.rule_history", mock.MakeQueryResult(nil, nil))
			addFreshCatalogEnsure(queryService)
			addShardRefEnsure(queryService, nil)
			addMigrationTableCreate(queryService)
			addCatalogMigrationEnsure(queryService, nil)
			if tt.checkError != nil {
				queryService.AddQueryPatternWithError(hbColumnsPattern, tt.checkError)
			} else {
				queryService.AddQueryPattern(hbColumnsPattern, mock.MakeQueryResult(hbColumnsCols, tt.columns))
				queryService.AddQueryPattern(hbKeyPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))
			}

			require.NoError(t, pm.createSidecarSchema(t.Context(), testBootstrapPolicy()))

			report := pm.heartbeatSchema.Load()
			if tt.wantNil {
				assert.Nil(t, report)
				return
			}
			require.NotNil(t, report)
			assert.Equal(t, tt.wantOK, report.Compatible())
		})
	}
}

// TestHealthLoopRefreshesHeartbeatSchema: the periodic health loop is what notices that an
// incompatible table was repaired, so it must call the refresh.
func TestHealthLoopRefreshesHeartbeatSchema(t *testing.T) {
	pm, queryService, _ := newHeartbeatSchemaManager(t)
	queryService.AddQueryPattern(hbRecoveryQuery, hbInRecovery("t"))
	queryService.AddQueryPattern(hbColumnsPattern, mock.MakeQueryResult(hbColumnsCols, hbCurrentColumns()))
	queryService.AddQueryPattern(hbKeyPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))
	pm.heartbeatSchema.Store(incompatibleHeartbeatSchema)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pm.runHealthHeartbeat(ctx, 10*time.Millisecond)
	}()
	defer func() {
		cancel()
		<-done
	}()

	require.Eventually(t, func() bool { return pm.heartbeatSchema.Load().Compatible() },
		5*time.Second, 10*time.Millisecond, "the loop re-checked the standing incompatible verdict")
}

// TestMaybeRefreshHeartbeatSchemaWithoutTracker: no heartbeat tracker yet means no errors
// to account for; an unchecked or compatible verdict costs nothing.
func TestMaybeRefreshHeartbeatSchemaWithoutTracker(t *testing.T) {
	pm, queryService, _ := newHeartbeatSchemaManager(t)
	pm.replTracker = nil
	checked := false
	queryService.AddQueryPatternWithCallback(hbColumnsPattern, mock.MakeQueryResult(hbColumnsCols, hbCurrentColumns()), func(string) { checked = true })

	assert.NotPanics(t, func() { pm.maybeRefreshHeartbeatSchema(t.Context()) })
	assert.False(t, checked, "never checked and no errors: nothing triggers a check")
	assert.False(t, pm.heartbeatSchemaBroken())
}
