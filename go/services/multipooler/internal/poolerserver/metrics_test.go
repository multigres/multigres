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

package poolerserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// setupPoolerTelemetry installs an in-memory metric reader and rebuilds the
// stats so they bind to it. Must be called before any drainStats methods.
func setupPoolerTelemetry(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-pooler"))
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})
	return setup.MetricReader
}

func readHistogramFloat64(t *testing.T, reader *sdkmetric.ManualReader, name string) *metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "%s should be Histogram[float64], got %T", name, m.Data)
			if len(h.DataPoints) == 0 {
				return nil
			}
			return &h.DataPoints[0]
		}
	}
	return nil
}

// readCounterByAttrs returns the value of an Int64 counter data point matching
// all provided attributes, or 0 if absent.
func readCounterByAttrs(t *testing.T, reader *sdkmetric.ManualReader, name string, attrs map[string]string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s should be Sum[int64], got %T", name, m.Data)
			for _, dp := range sum.DataPoints {
				if hasAttrs(dp.Attributes, attrs) {
					return dp.Value
				}
			}
		}
	}
	return 0
}

// readCounterTotalByAttrs returns the sum of all Int64 counter data points
// matching the provided attributes.
func readCounterTotalByAttrs(t *testing.T, reader *sdkmetric.ManualReader, name string, attrs map[string]string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s should be Sum[int64], got %T", name, m.Data)
			var total int64
			for _, dp := range sum.DataPoints {
				if hasAttrs(dp.Attributes, attrs) {
					total += dp.Value
				}
			}
			return total
		}
	}
	return 0
}

func hasAttrs(set attribute.Set, attrs map[string]string) bool {
	for k, want := range attrs {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.AsString() != want {
			return false
		}
	}
	return true
}

func primaryDrainAttrs(outcome string) map[string]string {
	return map[string]string{
		"pooler_type": "primary",
		"outcome":     outcome,
	}
}

func primaryDrainMetricAttrs() map[string]string {
	return map[string]string{
		"pooler_type": "primary",
	}
}

func TestPoolerTypeLabelValues(t *testing.T) {
	require.Equal(t, "primary", poolerTypeLabel(clustermetadatapb.PoolerType_PRIMARY))
	require.Equal(t, "replica", poolerTypeLabel(clustermetadatapb.PoolerType_REPLICA))
	require.Equal(t, "unknown", poolerTypeLabel(clustermetadatapb.PoolerType_UNKNOWN))

	require.Equal(t,
		[]attribute.KeyValue{attribute.String("pooler_type", "replica")},
		drainAttributes(clustermetadatapb.PoolerType_REPLICA),
	)
}

// TestDrainMetricGracefulOutcome verifies that a drain that completes
// before the grace period elapses records outcome=graceful and no
// force-closed connections.
func TestDrainMetricGracefulOutcome(t *testing.T) {
	reader := setupPoolerTelemetry(t)

	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 5 * time.Second
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	// No in-flight connections → WaitForDrain returns immediately.
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING))

	assert.Equal(t, int64(1), readCounterByAttrs(t, reader, "mg.pooler.drain.outcome", primaryDrainAttrs("graceful")))
	assert.Equal(t, int64(0), readCounterByAttrs(t, reader, "mg.pooler.drain.outcome", primaryDrainAttrs("force_close")))
	assert.Equal(t, int64(0), readCounterTotalByAttrs(t, reader, "mg.pooler.drain.force_closed", primaryDrainMetricAttrs()))

	hist := readHistogramFloat64(t, reader, "mg.pooler.drain.duration")
	require.NotNil(t, hist)
	assert.True(t, hasAttrs(hist.Attributes, primaryDrainMetricAttrs()))
	assert.Equal(t, uint64(1), hist.Count)
}

// TestDrainMetricForceCloseOutcome verifies that a drain that times out
// records outcome=force_close and the force-closed connection count.
func TestDrainMetricForceCloseOutcome(t *testing.T) {
	reader := setupPoolerTelemetry(t)

	mock := newDrainMockPoolManager()
	mock.closeReservedCount = 4 // CloseReservedConnections will report killing 4 connections.
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 50 * time.Millisecond
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	// Simulate an in-flight connection that never returns → WaitForDrain blocks until grace period.
	mock.regularAdd(1)
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING))

	assert.Equal(t, int64(0), readCounterByAttrs(t, reader, "mg.pooler.drain.outcome", primaryDrainAttrs("graceful")))
	assert.Equal(t, int64(1), readCounterByAttrs(t, reader, "mg.pooler.drain.outcome", primaryDrainAttrs("force_close")))
	assert.Equal(t, int64(4), readCounterTotalByAttrs(t, reader, "mg.pooler.drain.force_closed", primaryDrainMetricAttrs()))

	hist := readHistogramFloat64(t, reader, "mg.pooler.drain.duration")
	require.NotNil(t, hist)
	assert.True(t, hasAttrs(hist.Attributes, primaryDrainMetricAttrs()))
	assert.Equal(t, uint64(1), hist.Count)
	assert.GreaterOrEqual(t, hist.Sum, 0.05,
		"recorded duration should reflect at least the grace period")
}

func TestDrainMetricUsesDrainedPoolerType(t *testing.T) {
	reader := setupPoolerTelemetry(t)

	mock := newDrainMockPoolManager()
	mock.closeReservedCount = 2
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 50 * time.Millisecond
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	mock.regularAdd(1)
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_NOT_SERVING))

	metricAttrs := primaryDrainMetricAttrs()
	assert.Equal(t, int64(1), readCounterByAttrs(t, reader, "mg.pooler.drain.outcome", primaryDrainAttrs("force_close")))
	assert.Equal(t, int64(2), readCounterTotalByAttrs(t, reader, "mg.pooler.drain.force_closed", metricAttrs))

	hist := readHistogramFloat64(t, reader, "mg.pooler.drain.duration")
	require.NotNil(t, hist)
	assert.True(t, hasAttrs(hist.Attributes, metricAttrs))
}

// TestDrainMetricDurationBuckets verifies the histogram uses our
// explicit boundaries rather than the OTel ms-default.
func TestDrainMetricDurationBuckets(t *testing.T) {
	reader := setupPoolerTelemetry(t)

	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 50 * time.Millisecond
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING))

	hist := readHistogramFloat64(t, reader, "mg.pooler.drain.duration")
	require.NotNil(t, hist)
	assert.Equal(t,
		[]float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		hist.Bounds,
		"drain.duration must use seconds-scale buckets")
}

// TestDrainMetricBucketDistribution feeds explicit samples and verifies
// each lands in the expected bucket. Avoids real-time flakiness by
// calling the recorder directly.
func TestDrainMetricBucketDistribution(t *testing.T) {
	reader := setupPoolerTelemetry(t)

	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	ctx := t.Context()

	// Bounds: 0.1, 0.5, 1, 2, 5, 10, 30, 60 → 9 buckets.
	// Per OTel spec, sample <= Bound[i] lands in Bucket[i].
	samples := []float64{
		0.05, // (-inf, 0.1]
		0.3,  // (0.1, 0.5]
		1.5,  // (1, 2]
		7,    // (5, 10]
		20,   // (10, 30]
		90,   // (60, +inf)
	}
	var wantSum float64
	for _, s := range samples {
		pooler.drainStats.recordDrain(ctx, s, drainOutcomeGraceful, clustermetadatapb.PoolerType_PRIMARY)
		wantSum += s
	}

	hist := readHistogramFloat64(t, reader, "mg.pooler.drain.duration")
	require.NotNil(t, hist)
	require.Equal(t, []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60}, hist.Bounds)
	assert.Equal(t,
		[]uint64{1, 1, 0, 1, 0, 1, 1, 0, 1},
		hist.BucketCounts,
		"samples should land in expected buckets")
	assert.Equal(t, uint64(len(samples)), hist.Count)
	assert.InDelta(t, wantSum, hist.Sum, 0.001)
}

// TestDrainMetricBoundaryInclusivity verifies the OTel inclusivity rule:
// a sample equal to Bound[i] belongs to Bucket[i].
func TestDrainMetricBoundaryInclusivity(t *testing.T) {
	reader := setupPoolerTelemetry(t)

	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	ctx := t.Context()

	for _, s := range []float64{0.1, 0.5, 1, 2} {
		pooler.drainStats.recordDrain(ctx, s, drainOutcomeGraceful, clustermetadatapb.PoolerType_PRIMARY)
	}

	hist := readHistogramFloat64(t, reader, "mg.pooler.drain.duration")
	require.NotNil(t, hist)
	assert.Equal(t,
		[]uint64{1, 1, 1, 1, 0, 0, 0, 0, 0},
		hist.BucketCounts,
		"a sample equal to Bound[i] must land in Bucket[i]")
}
