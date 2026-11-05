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

package heartbeat

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// heartbeatInsert matches the Writer's INSERT.
const heartbeatInsert = "(?s)INSERT INTO multigres\\.heartbeat.*"

func pgErr(sqlState string) error {
	return mterrors.NewPgError("ERROR", sqlState, "boom", "")
}

func TestIsSchemaError(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want bool
	}{
		{"undefined column", pgErr("42703"), true},
		{"undefined table", pgErr("42P01"), true},
		{"datatype mismatch", pgErr("42804"), true},
		{"invalid text representation", pgErr("22P02"), true},
		{"not null violation", pgErr("23502"), true},
		{"on conflict without unique index", pgErr("42P10"), true},
		{"wrapped", mterrors.Wrap(pgErr("42703"), "failed to write heartbeat"), true},
		{"query canceled", pgErr("57014"), false},
		{"read-only transaction", pgErr("25006"), false},
		{"connection failure", pgErr("08006"), false},
		{"not a postgres error", errors.New("connection reset"), false},
		{"nil", nil, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsSchemaError(tt.err))
		})
	}
}

func TestWriterClassifiesSchemaErrors(t *testing.T) {
	queryService := mock.NewQueryService()
	tw := newTestWriter(t, queryService, nil)
	ctx := t.Context()

	// A transient failure is an error, but says nothing about the table.
	queryService.AddQueryPatternOnceWithError(heartbeatInsert, pgErr("57014"))
	tw.writeHeartbeat(ctx)
	assert.EqualValues(t, 1, tw.WriteErrors())
	assert.EqualValues(t, 0, tw.WriteSchemaErrors())
	assert.False(t, tw.SchemaBroken())

	// A missing column is a schema error and marks the table broken.
	queryService.AddQueryPatternOnceWithError(heartbeatInsert, pgErr("42703"))
	tw.writeHeartbeat(ctx)
	assert.EqualValues(t, 2, tw.WriteErrors(), "schema errors are write errors too")
	assert.EqualValues(t, 1, tw.WriteSchemaErrors())
	assert.True(t, tw.SchemaBroken())

	// A later transient failure leaves the verdict alone.
	queryService.AddQueryPatternOnceWithError(heartbeatInsert, pgErr("57014"))
	tw.writeHeartbeat(ctx)
	assert.True(t, tw.SchemaBroken())

	// Only a successful write clears it.
	queryService.AddQueryPattern(heartbeatInsert, mock.MakeQueryResult([]string{}, [][]any{}))
	tw.writeHeartbeat(ctx)
	assert.False(t, tw.SchemaBroken())
	assert.EqualValues(t, 1, tw.Writes())
}

func TestReaderClassifiesSchemaErrors(t *testing.T) {
	queryService := mock.NewQueryService()
	tr := newTestReader(t, queryService, nil)
	ctx := t.Context()

	queryService.AddQueryPatternOnceWithError("SELECT ts", pgErr("57014"))
	tr.readHeartbeat(ctx)
	assert.EqualValues(t, 1, tr.ReadErrors())
	assert.EqualValues(t, 0, tr.ReadSchemaErrors())

	// A core column is missing, so the lag-only fallback fails the same way.
	queryService.AddQueryPatternOnceWithError("SELECT ts", pgErr("42703"))
	queryService.AddQueryPatternOnceWithError("SELECT ts", pgErr("42703"))
	tr.readHeartbeat(ctx)
	assert.EqualValues(t, 2, tr.ReadErrors())
	assert.EqualValues(t, 1, tr.ReadSchemaErrors())
}

// heartbeatCounter returns the value of a counter data point with the given attributes.
func heartbeatCounter(t *testing.T, reader *sdkmetric.ManualReader, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	want := attribute.NewSet(attrs...)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s is not an int64 sum", name)
			for _, dp := range sum.DataPoints {
				if dp.Attributes.Equals(&want) {
					return dp.Value
				}
			}
		}
	}
	t.Fatalf("no data point %v for metric %q", attrs, name)
	return 0
}

// TestReplTrackerExportsHeartbeatCounters: the writer's and reader's success and error
// counts reach OTel, split by operation and (for errors) by whether the heartbeat table
// is the cause.
func TestReplTrackerExportsHeartbeatCounters(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	queryService := mock.NewQueryService()
	rt := NewReplTracker(queryService, slog.New(slog.DiscardHandler), []byte("0"), "pooler-1", 250)
	defer rt.Close()
	ctx := t.Context()

	queryService.AddQueryPatternOnceWithError(heartbeatInsert, pgErr("42703"))
	queryService.AddQueryPatternOnceWithError(heartbeatInsert, pgErr("57014"))
	queryService.AddQueryPattern(heartbeatInsert, mock.MakeQueryResult([]string{}, [][]any{}))
	rt.hw.writeHeartbeat(ctx)
	rt.hw.writeHeartbeat(ctx)
	rt.hw.writeHeartbeat(ctx)

	queryService.AddQueryPatternOnceWithError("SELECT ts", pgErr("42703"))
	queryService.AddQueryPatternOnceWithError("SELECT ts", pgErr("42703"))
	rt.hr.readHeartbeat(ctx)

	op := func(v string) attribute.KeyValue { return attribute.String(attrOperation, v) }
	reason := func(v string) attribute.KeyValue { return attribute.String(attrReason, v) }
	r := setup.MetricReader
	assert.EqualValues(t, 1, heartbeatCounter(t, r, "mg.pooler.heartbeat.operations", op(operationWrite)))
	assert.EqualValues(t, 0, heartbeatCounter(t, r, "mg.pooler.heartbeat.operations", op(operationRead)))
	assert.EqualValues(t, 1, heartbeatCounter(t, r, "mg.pooler.heartbeat.errors", op(operationWrite), reason(reasonSchema)))
	assert.EqualValues(t, 1, heartbeatCounter(t, r, "mg.pooler.heartbeat.errors", op(operationWrite), reason(reasonOther)))
	assert.EqualValues(t, 1, heartbeatCounter(t, r, "mg.pooler.heartbeat.errors", op(operationRead), reason(reasonSchema)))
	assert.EqualValues(t, 0, heartbeatCounter(t, r, "mg.pooler.heartbeat.errors", op(operationRead), reason(reasonOther)))

	// Closing twice must not panic, and stops reporting.
	rt.Close()
	rt.Close()
}

// schemaCatalogResult is the live-column listing for a table whose columns are all in the
// shape the current release expects.
func schemaCatalogResult() [][]any {
	return [][]any{
		{"shard_id", "bytea", "t"},
		{"leader_id", "text", "t"},
		{"ts", "bigint", "t"},
		{"quorum_commit_lsn", "pg_lsn", "f"},
		{"quorum_commit_ts", "timestamp with time zone", "f"},
	}
}

// TestEnsureSchemaDoesNotAlterACompatibleTable: ALTER TABLE takes an ACCESS EXCLUSIVE lock
// even when it changes nothing, so a table that already matches must never be altered.
// The mock fails any statement it has no pattern for, so an ALTER here fails the test.
func TestEnsureSchemaDoesNotAlterACompatibleTable(t *testing.T) {
	queryService := mock.NewQueryService()
	queryService.AddQueryPattern("pg_attribute a\\s+WHERE", mock.MakeQueryResult(
		[]string{"attname", "format_type", "required"}, schemaCatalogResult(),
	))
	queryService.AddQueryPattern("SELECT EXISTS", mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))

	report, err := EnsureSchema(t.Context(), queryService)
	require.NoError(t, err)
	assert.True(t, report.Compatible(), report.String())
}

// TestEnsureSchemaAltersALegacyTable: a table missing only the newer columns is altered,
// then re-checked.
func TestEnsureSchemaAltersALegacyTable(t *testing.T) {
	queryService := mock.NewQueryService()
	legacy := schemaCatalogResult()[:3]
	queryService.AddQueryPatternOnce("pg_attribute a\\s+WHERE", mock.MakeQueryResult(
		[]string{"attname", "format_type", "required"}, legacy,
	))
	queryService.AddQueryPatternOnce("SELECT EXISTS", mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))
	var altered string
	queryService.AddQueryPatternWithCallback("ALTER TABLE IF EXISTS multigres\\.heartbeat", mock.MakeQueryResult(nil, nil),
		func(q string) { altered = q })
	queryService.AddQueryPattern("pg_attribute a\\s+WHERE", mock.MakeQueryResult(
		[]string{"attname", "format_type", "required"}, schemaCatalogResult(),
	))
	queryService.AddQueryPattern("SELECT EXISTS", mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))

	report, err := EnsureSchema(t.Context(), queryService)
	require.NoError(t, err)
	assert.True(t, report.Compatible(), report.String())
	assert.Contains(t, altered, "ADD COLUMN IF NOT EXISTS quorum_commit_lsn")
	assert.Contains(t, altered, "ADD COLUMN IF NOT EXISTS quorum_commit_ts")
}

func TestSchemaReportVerdicts(t *testing.T) {
	var nilReport *SchemaReport
	assert.False(t, nilReport.Compatible(), "no check yet is not a compatible verdict")
	assert.False(t, nilReport.Upgradable())

	assert.True(t, (&SchemaReport{}).Compatible())
	assert.False(t, (&SchemaReport{}).Upgradable())

	newer := SchemaProblem{Kind: ProblemMissingColumn, Column: "quorum_commit_ts"}
	assert.True(t, (&SchemaReport{Problems: []SchemaProblem{newer}}).Upgradable())

	original := SchemaProblem{Kind: ProblemMissingColumn, Column: "ts"}
	assert.False(t, (&SchemaReport{Problems: []SchemaProblem{newer, original}}).Upgradable(),
		"an original column cannot be added back")
	assert.False(t, (&SchemaReport{Problems: []SchemaProblem{{Kind: ProblemMissingTable}}}).Upgradable())
}
