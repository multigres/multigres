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

package handler

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/tools/telemetry"
)

func TestNewHandlerMetrics(t *testing.T) {
	m, err := NewHandlerMetrics()
	require.NoError(t, err)
	require.NotNil(t, m)
}

// setupHandlerMetrics installs an in-memory metric reader and returns a fresh
// *HandlerMetrics wired to it. The reader can be used to inspect emitted data
// points.
func setupHandlerMetrics(t *testing.T) (*HandlerMetrics, *sdkmetric.ManualReader) {
	t.Helper()

	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multigateway"))

	m, err := NewHandlerMetrics()
	require.NoError(t, err)
	return m, setup.MetricReader
}

// findMetric returns the first metric named `name` across all scope metrics
// in the collected snapshot.
func findMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Metrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, mm := range sm.Metrics {
			if mm.Name == name {
				return mm
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

// attrValue extracts a string attribute by key from an attribute.Set.
func attrValue(t *testing.T, set attribute.Set, key string) string {
	t.Helper()
	v, ok := set.Value(attribute.Key(key))
	require.True(t, ok, "attribute %q missing", key)
	return v.AsString()
}

// cacheSize counts entries in a sync.Map.
func cacheSize(m *sync.Map) int {
	n := 0
	m.Range(func(_, _ any) bool { n++; return true })
	return n
}

// TestQueryDuration_CacheHit verifies that identical dimensions reuse the
// cached MeasurementOption and distinct dimensions get separate entries.
func TestQueryDuration_CacheHit(t *testing.T) {
	m, _ := setupHandlerMetrics(t)
	ctx := t.Context()

	m.queryDuration.Record(ctx, 0.1, "db1", "SELECT", "simple", "", QueryStatusOK, "fp1")
	key := queryDurationKey{db: "db1", op: "SELECT", proto: "simple", errType: "", status: string(QueryStatusOK), fp: "fp1"}
	v1, ok := m.queryDuration.optsCache.Load(key)
	require.True(t, ok, "expected cache entry after first record")

	m.queryDuration.Record(ctx, 0.2, "db1", "SELECT", "simple", "", QueryStatusOK, "fp1")
	v2, ok := m.queryDuration.optsCache.Load(key)
	require.True(t, ok)
	assert.Equal(t, v1, v2, "cache value should be reused for identical dimensions")
	assert.Equal(t, 1, cacheSize(&m.queryDuration.optsCache))

	m.queryDuration.Record(ctx, 0.3, "db1", "SELECT", "simple", "", QueryStatusOK, "fp2")
	assert.Equal(t, 2, cacheSize(&m.queryDuration.optsCache),
		"distinct fingerprints should produce a second cache entry")
}

func TestQueryErrors_CacheHit(t *testing.T) {
	m, _ := setupHandlerMetrics(t)
	ctx := t.Context()

	m.queryErrors.Add(ctx, "42601", "client", "db1", "SELECT", "fp1")
	key := queryErrorsKey{errType: "42601", errSource: "client", db: "db1", op: "SELECT", fp: "fp1"}
	v1, ok := m.queryErrors.optsCache.Load(key)
	require.True(t, ok)

	m.queryErrors.Add(ctx, "42601", "client", "db1", "SELECT", "fp1")
	v2, ok := m.queryErrors.optsCache.Load(key)
	require.True(t, ok)
	assert.Equal(t, v1, v2)
	assert.Equal(t, 1, cacheSize(&m.queryErrors.optsCache))

	m.queryErrors.Add(ctx, "42601", "backend", "db1", "SELECT", "fp1")
	assert.Equal(t, 2, cacheSize(&m.queryErrors.optsCache),
		"distinct error.source should produce a second cache entry")
}

func TestRowsReturned_CacheHit(t *testing.T) {
	m, _ := setupHandlerMetrics(t)
	ctx := t.Context()

	m.rowsReturned.Record(ctx, 5, "db1", "SELECT", "fp1")
	key := rowsReturnedKey{db: "db1", op: "SELECT", fp: "fp1"}
	v1, ok := m.rowsReturned.optsCache.Load(key)
	require.True(t, ok)

	m.rowsReturned.Record(ctx, 10, "db1", "SELECT", "fp1")
	v2, ok := m.rowsReturned.optsCache.Load(key)
	require.True(t, ok)
	assert.Equal(t, v1, v2)
	assert.Equal(t, 1, cacheSize(&m.rowsReturned.optsCache))

	m.rowsReturned.Record(ctx, 1, "db1", "SELECT", "fp2")
	assert.Equal(t, 2, cacheSize(&m.rowsReturned.optsCache))
}

func TestTableQueries_CacheHit(t *testing.T) {
	m, _ := setupHandlerMetrics(t)
	ctx := t.Context()

	m.tableQueries.Add(ctx, "db1", "users", "SELECT")
	key := tableQueriesKey{db: "db1", table: "users", op: "SELECT"}
	v1, ok := m.tableQueries.optsCache.Load(key)
	require.True(t, ok)

	m.tableQueries.Add(ctx, "db1", "users", "SELECT")
	v2, ok := m.tableQueries.optsCache.Load(key)
	require.True(t, ok)
	assert.Equal(t, v1, v2)
	assert.Equal(t, 1, cacheSize(&m.tableQueries.optsCache))

	m.tableQueries.Add(ctx, "db1", "orders", "SELECT")
	assert.Equal(t, 2, cacheSize(&m.tableQueries.optsCache),
		"distinct table names should produce a second cache entry")
}

// TestHandlerMetrics_EmittedAttributes ensures caching does not alter the
// shape of the emitted metric attributes.
func TestHandlerMetrics_EmittedAttributes(t *testing.T) {
	m, reader := setupHandlerMetrics(t)
	ctx := t.Context()

	m.queryDuration.Record(ctx, 0.05, "ns", "SELECT", "simple", "", QueryStatusOK, "fp1")
	m.queryErrors.Add(ctx, "08006", "routing", "ns", "SELECT", "fp1")
	m.rowsReturned.Record(ctx, 42, "ns", "SELECT", "fp1")
	m.tableQueries.Add(ctx, "ns", "t1", "SELECT")

	dur := findMetric(t, reader, "mg.gateway.query.duration")
	durHist, ok := dur.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, durHist.DataPoints, 1)
	dp := durHist.DataPoints[0]
	assert.Equal(t, "ns", attrValue(t, dp.Attributes, "db.namespace"))
	assert.Equal(t, "SELECT", attrValue(t, dp.Attributes, "db.operation.name"))
	assert.Equal(t, "simple", attrValue(t, dp.Attributes, "db.query.protocol"))
	assert.Equal(t, "", attrValue(t, dp.Attributes, "error.type"))
	assert.Equal(t, "ok", attrValue(t, dp.Attributes, "status"))
	assert.Equal(t, "fp1", attrValue(t, dp.Attributes, "query.fingerprint"))

	errs := findMetric(t, reader, "mg.gateway.query.errors")
	errSum, ok := errs.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, errSum.DataPoints, 1)
	edp := errSum.DataPoints[0]
	assert.Equal(t, int64(1), edp.Value)
	assert.Equal(t, "08006", attrValue(t, edp.Attributes, "error.type"))
	assert.Equal(t, "routing", attrValue(t, edp.Attributes, "error.source"))
	assert.Equal(t, "fp1", attrValue(t, edp.Attributes, "query.fingerprint"))

	rows := findMetric(t, reader, "mg.gateway.query.rows_returned")
	rowsHist, ok := rows.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, rowsHist.DataPoints, 1)
	rdp := rowsHist.DataPoints[0]
	assert.Equal(t, "ns", attrValue(t, rdp.Attributes, "db.namespace"))
	assert.Equal(t, "SELECT", attrValue(t, rdp.Attributes, "db.operation.name"))
	assert.Equal(t, "fp1", attrValue(t, rdp.Attributes, "query.fingerprint"))

	tq := findMetric(t, reader, "mg.gateway.query.table_queries")
	tqSum, ok := tq.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, tqSum.DataPoints, 1)
	tdp := tqSum.DataPoints[0]
	assert.Equal(t, "ns", attrValue(t, tdp.Attributes, "db.namespace"))
	assert.Equal(t, "t1", attrValue(t, tdp.Attributes, "db.collection.name"))
	assert.Equal(t, "SELECT", attrValue(t, tdp.Attributes, "db.operation.name"))
}

// TestClassifyErrorSource_SkipsOnNil verifies the success-path short-circuit.
func TestClassifyErrorSource_SkipsOnNil(t *testing.T) {
	assert.Equal(t, "", classifyErrorSource(nil))

	pgErr := &mterrors.PgDiagnostic{Code: "42601"}
	assert.Equal(t, "backend", classifyErrorSource(pgErr))

	mtErr := &mterrors.PgDiagnostic{Code: "MTD01"}
	assert.Equal(t, "internal", classifyErrorSource(mtErr))

	assert.Equal(t, "client", classifyErrorSource(errors.New("opaque")))
}

// setupBenchMetrics installs an in-memory metric reader for benchmarks.
// Mirrors setupHandlerMetrics but takes testing.TB so it works with B.
func setupBenchMetrics(b *testing.B) *HandlerMetrics {
	b.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	b.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(prev)
	})

	m, err := NewHandlerMetrics()
	require.NoError(b, err)
	return m
}

// BenchmarkQueryDuration_Cached measures the cached hot path with a stable
// dimension tuple (the realistic shape: same db/op/fp tuple repeating).
func BenchmarkQueryDuration_Cached(b *testing.B) {
	m := setupBenchMetrics(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		m.queryDuration.Record(ctx, 0.001, "db1", "SELECT", "simple", "", QueryStatusOK, "fp1")
	}
}

// BenchmarkQueryDuration_Uncached replicates the pre-cache code path:
// build a fresh []attribute.KeyValue + metric.WithAttributes per call.
func BenchmarkQueryDuration_Uncached(b *testing.B) {
	m := setupBenchMetrics(b)
	ctx := context.Background()
	h := m.queryDuration.Float64Histogram
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		h.Record(ctx, 0.001, metric.WithAttributes(
			attribute.String("db.namespace", "db1"),
			attribute.String("db.operation.name", "SELECT"),
			attribute.String("db.query.protocol", "simple"),
			attribute.String("error.type", ""),
			attribute.String("status", string(QueryStatusOK)),
			attribute.String("query.fingerprint", "fp1"),
		))
	}
}

// BenchmarkRowsReturned_Cached / _Uncached: smallest key, highest call rate.
func BenchmarkRowsReturned_Cached(b *testing.B) {
	m := setupBenchMetrics(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		m.rowsReturned.Record(ctx, 1, "db1", "SELECT", "fp1")
	}
}

func BenchmarkRowsReturned_Uncached(b *testing.B) {
	m := setupBenchMetrics(b)
	ctx := context.Background()
	h := m.rowsReturned.Float64Histogram
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		h.Record(ctx, 1, metric.WithAttributes(
			attribute.String("db.namespace", "db1"),
			attribute.String("db.operation.name", "SELECT"),
			attribute.String("query.fingerprint", "fp1"),
		))
	}
}

// BenchmarkTableQueries_Cached / _Uncached: the per-table inner-loop call.
func BenchmarkTableQueries_Cached(b *testing.B) {
	m := setupBenchMetrics(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		m.tableQueries.Add(ctx, "db1", "users", "SELECT")
	}
}

func BenchmarkTableQueries_Uncached(b *testing.B) {
	m := setupBenchMetrics(b)
	ctx := context.Background()
	c := m.tableQueries.Int64Counter
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c.Add(ctx, 1, metric.WithAttributes(
			attribute.String("db.namespace", "db1"),
			attribute.String("db.collection.name", "users"),
			attribute.String("db.operation.name", "SELECT"),
		))
	}
}

// BenchmarkRecordQueryCompletion_Mixed_Cached approximates real traffic:
// 16 distinct (op, fingerprint) tuples rotating through the three hot-path
// instruments per iteration. This exercises the cache under realistic
// dimension churn rather than the best-case single-tuple shape.
func BenchmarkRecordQueryCompletion_Mixed_Cached(b *testing.B) {
	m := setupBenchMetrics(b)
	ctx := context.Background()
	ops := []string{"SELECT", "INSERT", "UPDATE", "DELETE"}
	fps := []string{"fp1", "fp2", "fp3", "fp4"}
	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for b.Loop() {
		op := ops[i&3]
		fp := fps[(i>>2)&3]
		m.queryDuration.Record(ctx, 0.001, "db1", op, "simple", "", QueryStatusOK, fp)
		m.rowsReturned.Record(ctx, 1, "db1", op, fp)
		m.tableQueries.Add(ctx, "db1", "users", op)
		i++
	}
}

func BenchmarkRecordQueryCompletion_Mixed_Uncached(b *testing.B) {
	m := setupBenchMetrics(b)
	ctx := context.Background()
	dur := m.queryDuration.Float64Histogram
	rows := m.rowsReturned.Float64Histogram
	tq := m.tableQueries.Int64Counter
	ops := []string{"SELECT", "INSERT", "UPDATE", "DELETE"}
	fps := []string{"fp1", "fp2", "fp3", "fp4"}
	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for b.Loop() {
		op := ops[i&3]
		fp := fps[(i>>2)&3]
		dur.Record(ctx, 0.001, metric.WithAttributes(
			attribute.String("db.namespace", "db1"),
			attribute.String("db.operation.name", op),
			attribute.String("db.query.protocol", "simple"),
			attribute.String("error.type", ""),
			attribute.String("status", string(QueryStatusOK)),
			attribute.String("query.fingerprint", fp),
		))
		rows.Record(ctx, 1, metric.WithAttributes(
			attribute.String("db.namespace", "db1"),
			attribute.String("db.operation.name", op),
			attribute.String("query.fingerprint", fp),
		))
		tq.Add(ctx, 1, metric.WithAttributes(
			attribute.String("db.namespace", "db1"),
			attribute.String("db.collection.name", "users"),
			attribute.String("db.operation.name", op),
		))
		i++
	}
}
