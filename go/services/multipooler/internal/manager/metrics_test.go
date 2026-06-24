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
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/telemetry"
)

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

func attrValue(t *testing.T, set attribute.Set, key string) string {
	t.Helper()
	v, ok := set.Value(attribute.Key(key))
	require.True(t, ok, "attribute %q missing", key)
	return v.AsString()
}

func newTestHealthStreamer(t *testing.T) (*healthStreamer, *sdkmetric.ManualReader) {
	t.Helper()
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test"}
	return newHealthStreamer(logger, id, "tg1", "0"), setup.MetricReader
}

// TestReplicationLagGauge verifies the observable gauge samples the lag atomic
// and converts nanoseconds to seconds.
func TestReplicationLagGauge(t *testing.T) {
	hs, reader := newTestHealthStreamer(t)

	hs.SetReplicationLag(2_500_000_000) // 2.5s in ns

	g := findMetric(t, reader, "mg.pooler.replication.lag")
	gauge, ok := g.Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Len(t, gauge.DataPoints, 1)
	assert.InDelta(t, 2.5, gauge.DataPoints[0].Value, 1e-9)
}

// TestServingTransitions verifies a serving-status change records a transition
// with from/to attributes, and that a no-op change does not.
func TestServingTransitions(t *testing.T) {
	hs, reader := newTestHealthStreamer(t)
	ctx := t.Context()

	// NOT_SERVING (initial) → SERVING records one transition.
	require.NoError(t, hs.OnStateChange(ctx,
		clustermetadatapb.PoolerType_PRIMARY,
		clustermetadatapb.PoolerServingStatus_SERVING))

	// SERVING → SERVING is a no-op (poolerType change only): no new transition.
	require.NoError(t, hs.OnStateChange(ctx,
		clustermetadatapb.PoolerType_REPLICA,
		clustermetadatapb.PoolerServingStatus_SERVING))

	// SERVING → NOT_SERVING records a second transition.
	require.NoError(t, hs.OnStateChange(ctx,
		clustermetadatapb.PoolerType_REPLICA,
		clustermetadatapb.PoolerServingStatus_NOT_SERVING))

	m := findMetric(t, reader, "mg.pooler.serving.transitions")
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 2, "two distinct from/to transitions expected")

	total := int64(0)
	for _, dp := range sum.DataPoints {
		total += dp.Value
		from := attrValue(t, dp.Attributes, "from")
		to := attrValue(t, dp.Attributes, "to")
		assert.NotEqual(t, from, to, "a recorded transition must change status")
	}
	assert.Equal(t, int64(2), total)
}

// TestRecordTransition_NilSafe covers the guards in recordTransition: a nil
// receiver and a zero-value healthMetrics (nil counter) must both be no-ops.
func TestRecordTransition_NilSafe(t *testing.T) {
	from := clustermetadatapb.PoolerServingStatus_NOT_SERVING
	to := clustermetadatapb.PoolerServingStatus_SERVING

	var nilM *healthMetrics
	nilM.recordTransition(t.Context(), from, to)

	(&healthMetrics{}).recordTransition(t.Context(), from, to)
}
