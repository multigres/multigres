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
	"bytes"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/switcher"
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

// legacySchemaCatalogResult is the live-column listing of the original three-column table.
func legacySchemaCatalogResult() [][]any {
	return schemaCatalogResult()[:3]
}

const (
	columnsQueryPattern = "pg_attribute a\\s+WHERE"
	keyQueryPattern     = "SELECT EXISTS"
	alterQueryPattern   = "ALTER TABLE IF EXISTS multigres\\.heartbeat"
)

var columnsResultCols = []string{"attname", "format_type", "required"}

func TestCheckSchemaNegative(t *testing.T) {
	t.Run("a failing column query is an error, not a verdict", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPatternWithError(columnsQueryPattern, errors.New("connection reset"))

		report, err := CheckSchema(t.Context(), qs)
		require.Error(t, err)
		assert.Nil(t, report, "a check that could not run must not read as compatible")
		assert.ErrorContains(t, err, "failed to read heartbeat table columns")
	})

	t.Run("a malformed row is an error", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPattern(columnsQueryPattern, mock.MakeQueryResult(columnsResultCols, [][]any{{"shard_id", "bytea", "maybe"}}))

		report, err := CheckSchema(t.Context(), qs)
		require.Error(t, err)
		assert.Nil(t, report)
		assert.ErrorContains(t, err, "nullability")
	})

	t.Run("a failing key query is an error", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPattern(columnsQueryPattern, mock.MakeQueryResult(columnsResultCols, schemaCatalogResult()))
		qs.AddQueryPatternWithError(keyQueryPattern, errors.New("boom"))

		report, err := CheckSchema(t.Context(), qs)
		require.Error(t, err)
		assert.Nil(t, report)
		assert.ErrorContains(t, err, "failed to read heartbeat table keys")
	})

	t.Run("a key query without a row is an error", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPattern(columnsQueryPattern, mock.MakeQueryResult(columnsResultCols, schemaCatalogResult()))
		qs.AddQueryPattern(keyQueryPattern, mock.MakeQueryResult([]string{"exists"}, nil))

		report, err := CheckSchema(t.Context(), qs)
		require.Error(t, err)
		assert.Nil(t, report)
	})

	t.Run("a missing table is a verdict and skips the key query", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPattern(columnsQueryPattern, mock.MakeQueryResult(columnsResultCols, nil))

		report, err := CheckSchema(t.Context(), qs)
		require.NoError(t, err)
		assert.Equal(t, []SchemaProblemKind{ProblemMissingTable}, []SchemaProblemKind{report.Problems[0].Kind})
		assert.Len(t, report.Problems, 1)
	})

	t.Run("no shard_id column skips the key query", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPattern(columnsQueryPattern, mock.MakeQueryResult(columnsResultCols, schemaCatalogResult()[1:]))
		// No key pattern: running the key query would fail the check.

		report, err := CheckSchema(t.Context(), qs)
		require.NoError(t, err)
		require.Len(t, report.Problems, 1)
		assert.Equal(t, ProblemMissingColumn, report.Problems[0].Kind)
		assert.Equal(t, "shard_id", report.Problems[0].Column)
	})
}

func TestEnsureSchemaNegative(t *testing.T) {
	legacyTable := func(qs *mock.QueryService) {
		qs.AddQueryPattern(columnsQueryPattern, mock.MakeQueryResult(columnsResultCols, legacySchemaCatalogResult()))
		qs.AddQueryPattern(keyQueryPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))
	}

	t.Run("a failing check issues no ALTER", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPatternWithError(columnsQueryPattern, errors.New("connection reset"))
		altered := false
		qs.AddQueryPatternWithCallback(alterQueryPattern, mock.MakeQueryResult(nil, nil), func(string) { altered = true })

		report, err := EnsureSchema(t.Context(), qs)
		require.Error(t, err)
		assert.Nil(t, report)
		assert.False(t, altered)
	})

	t.Run("a failing ALTER returns the pre-ALTER report and the error", func(t *testing.T) {
		qs := mock.NewQueryService()
		legacyTable(qs)
		qs.AddQueryPatternWithError(alterQueryPattern, errors.New("cannot execute ALTER TABLE in a read-only transaction"))

		report, err := EnsureSchema(t.Context(), qs)
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to add missing heartbeat columns")
		require.NotNil(t, report)
		assert.False(t, report.Compatible())
		assert.True(t, report.Upgradable())
	})

	t.Run("an ALTER that leaves the table incompatible is reported, not hidden", func(t *testing.T) {
		qs := mock.NewQueryService()
		legacyTable(qs) // still the legacy columns after the ALTER
		altered := 0
		qs.AddQueryPatternWithCallback(alterQueryPattern, mock.MakeQueryResult(nil, nil), func(string) { altered++ })

		report, err := EnsureSchema(t.Context(), qs)
		require.NoError(t, err)
		assert.Equal(t, 1, altered, "one ALTER per call, no retry loop")
		assert.False(t, report.Compatible())
	})

	t.Run("a table that needs more than the missing columns is not altered", func(t *testing.T) {
		qs := mock.NewQueryService()
		// Legacy columns plus a wrong type on ts: missing newer columns AND another problem.
		qs.AddQueryPattern(columnsQueryPattern, mock.MakeQueryResult(columnsResultCols, [][]any{
			{"shard_id", "bytea", "t"}, {"leader_id", "text", "t"}, {"ts", "text", "t"},
		}))
		qs.AddQueryPattern(keyQueryPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{"t"}}))
		altered := false
		qs.AddQueryPatternWithCallback(alterQueryPattern, mock.MakeQueryResult(nil, nil), func(string) { altered = true })

		report, err := EnsureSchema(t.Context(), qs)
		require.NoError(t, err)
		assert.False(t, altered)
		assert.False(t, report.Upgradable())
	})
}

// TestWriteProvenFailuresAreCounted: WriteProven is how a new leader writes its first proof,
// so its failures must reach WriteErrors and the schema count like a tick's do. Counting
// them only as schema errors made total-minus-schema negative.
func TestWriteProvenFailuresAreCounted(t *testing.T) {
	queryService := mock.NewQueryService()
	tw := newTestWriter(t, queryService, nil)
	ctx := t.Context()

	queryService.AddQueryPatternOnceWithError(heartbeatInsert, pgErr("42703"))
	require.Error(t, tw.WriteProven(ctx, 1, time.Now()))
	assert.EqualValues(t, 1, tw.WriteErrors())
	assert.EqualValues(t, 1, tw.WriteSchemaErrors())
	assert.True(t, tw.SchemaBroken())

	// A transient failure counts as an error but not a schema error, and leaves the
	// verdict alone.
	queryService.AddQueryPatternOnceWithError(heartbeatInsert, pgErr("57014"))
	require.Error(t, tw.WriteProven(ctx, 2, time.Now()))
	assert.EqualValues(t, 2, tw.WriteErrors())
	assert.EqualValues(t, 1, tw.WriteSchemaErrors())
	assert.True(t, tw.SchemaBroken(), "a transient failure says nothing about the table")

	// Only a successful write clears the verdict.
	queryService.AddQueryPattern(heartbeatInsert, mock.MakeQueryResult([]string{}, [][]any{}))
	require.NoError(t, tw.WriteProven(ctx, 3, time.Now()))
	assert.False(t, tw.SchemaBroken())
	assert.EqualValues(t, 2, tw.WriteErrors(), "a success is not an error")
}

// TestWriteProvenSchemaFailureKeepsErrorCountersConsistent: the counters behind the
// mg.pooler.heartbeat.errors split must satisfy schema <= total, or the 'other' series
// goes negative.
func TestWriteProvenSchemaFailureKeepsErrorCountersConsistent(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	queryService := mock.NewQueryService()
	rt := NewReplTracker(queryService, slog.New(slog.DiscardHandler), []byte("0"), "pooler-1", 250)
	defer rt.Close()

	queryService.AddQueryPatternOnceWithError(heartbeatInsert, pgErr("42703"))
	require.Error(t, rt.hw.WriteProven(t.Context(), 1, time.Now()))

	op := attribute.String(attrOperation, operationWrite)
	assert.EqualValues(t, 1, heartbeatCounter(t, setup.MetricReader, "mg.pooler.heartbeat.errors", op, attribute.String(attrReason, reasonSchema)))
	assert.EqualValues(t, 0, heartbeatCounter(t, setup.MetricReader, "mg.pooler.heartbeat.errors", op, attribute.String(attrReason, reasonOther)),
		"the other series must not go negative")
}

// failingMeterProvider returns instruments that fail to register for the listed names.
type failingMeterProvider struct {
	metricnoop.MeterProvider
	fail map[string]bool
}

func (p failingMeterProvider) Meter(name string, opts ...metric.MeterOption) metric.Meter {
	return failingMeter{Meter: p.MeterProvider.Meter(name, opts...), fail: p.fail}
}

type failingMeter struct {
	metric.Meter
	fail map[string]bool
}

func (m failingMeter) Int64ObservableCounter(name string, opts ...metric.Int64ObservableCounterOption) (metric.Int64ObservableCounter, error) {
	if m.fail[name] {
		return nil, errors.New("instrument refused")
	}
	return m.Meter.Int64ObservableCounter(name, opts...)
}

// TestRegisterMetricsInstrumentFailures: a metric that cannot be created is reported and
// skipped, never fatal, and the returned unregister is always callable.
func TestRegisterMetricsInstrumentFailures(t *testing.T) {
	const ops, errs = "mg.pooler.heartbeat.operations", "mg.pooler.heartbeat.errors"
	for _, tt := range []struct {
		name    string
		fail    []string
		wantIn  []string
		wantOut []string
	}{
		{"operations fails", []string{ops}, []string{ops}, []string{errs}},
		{"errors fails", []string{errs}, []string{errs}, []string{ops}},
		{"both fail", []string{ops, errs}, []string{ops, errs}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			previous := otel.GetMeterProvider()
			t.Cleanup(func() { otel.SetMeterProvider(previous) })
			fail := map[string]bool{}
			for _, name := range tt.fail {
				fail[name] = true
			}
			otel.SetMeterProvider(failingMeterProvider{fail: fail})

			queryService := mock.NewQueryService()
			unregister, err := registerMetrics(newTestWriter(t, queryService, nil), newTestReader(t, queryService, nil))

			require.Error(t, err)
			for _, want := range tt.wantIn {
				assert.ErrorContains(t, err, want)
			}
			for _, notWant := range tt.wantOut {
				assert.NotContains(t, err.Error(), notWant)
			}
			assert.NotContains(t, err.Error(), "%!w", "a nil error must not be wrapped")
			require.NotNil(t, unregister)
			assert.NotPanics(t, func() { unregister(); unregister() })
		})
	}
}

// TestReplTrackerCloseWithoutMetricsRegistration: a tracker that never registered metrics
// (or whose registration failed outright) still closes.
func TestReplTrackerCloseWithoutMetricsRegistration(t *testing.T) {
	queryService := mock.NewQueryService()
	hw := newTestWriter(t, queryService, nil)
	hr := newTestReader(t, queryService, nil)
	rt := &ReplTracker{hw: hw, hr: hr, sw: switcher.NewRoleSwitcher(hw, hr)}
	assert.NotPanics(t, rt.Close)
}

func TestSchemaReportEdgeCases(t *testing.T) {
	var nilReport *SchemaReport
	assert.Equal(t, "unknown", nilReport.String())
	assert.Equal(t, "compatible", (&SchemaReport{}).String())

	// A column the code does not know is never something to add.
	unknown := &SchemaReport{Problems: []SchemaProblem{{Kind: ProblemMissingColumn, Column: "bogus"}}}
	assert.False(t, unknown.Upgradable())

	// A problem with no column renders without a column name.
	assert.Equal(t, "missing_table: multigres.heartbeat does not exist",
		SchemaProblem{Kind: ProblemMissingTable, Detail: "multigres.heartbeat does not exist"}.String())
}

// TestNewReplTrackerSurvivesMetricRegistrationFailure: a metric that cannot be created is
// logged and skipped; the tracker is still usable.
func TestNewReplTrackerSurvivesMetricRegistrationFailure(t *testing.T) {
	previous := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	otel.SetMeterProvider(failingMeterProvider{fail: map[string]bool{"mg.pooler.heartbeat.errors": true}})

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	rt := NewReplTracker(mock.NewQueryService(), logger, []byte("0"), "pooler-1", 250)
	require.NotNil(t, rt)
	assert.Contains(t, logs.String(), "failed to initialise some heartbeat metrics")
	assert.NotPanics(t, rt.Close)
}
