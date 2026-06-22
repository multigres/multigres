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

package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/tools/telemetry"
)

// newTestStream builds a per-stream recorder against the default (noop) global
// meter provider. Used by the allocation guard and nil-safety tests, which care
// about our own allocation behavior, not the SDK's.
func newTestStream(t *testing.T) *Stream {
	t.Helper()
	m, err := NewMetrics()
	require.NoError(t, err)
	return m.NewStream("test-user")
}

// setupReplStream wires a real SDK meter provider with a manual reader so tests
// can collect and assert the emitted data points.
func setupReplStream(t *testing.T) (*Stream, *sdkmetric.ManualReader) {
	t.Helper()
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := NewMetrics()
	require.NoError(t, err)
	return m.NewStream("test-user"), setup.MetricReader
}

func collectAggregation(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				return m.Data
			}
		}
	}
	return nil
}

// TestMetrics_RecordIsAllocationFree guards the per-chunk hot path against the
// classic OTel footgun of building a fresh attribute set on every Record.
func TestMetrics_RecordIsAllocationFree(t *testing.T) {
	m := newTestStream(t)
	avg := testing.AllocsPerRun(1000, func() { m.recordDownstream(4096) })
	if avg > 0 {
		t.Fatalf("recordDownstream allocates %.1f times/op, want 0", avg)
	}
	avg = testing.AllocsPerRun(1000, func() { m.recordUpstream(4096) })
	if avg > 0 {
		t.Fatalf("recordUpstream allocates %.1f times/op, want 0", avg)
	}
	avg = testing.AllocsPerRun(1000, func() { m.recordDownstreamLatency(0.5) })
	if avg > 0 {
		t.Fatalf("recordDownstreamLatency allocates %.1f times/op, want 0", avg)
	}
}

// TestMetrics_NilReceiverIsNoop verifies every recorder tolerates a nil
// *Metrics (the tunnel runs without metrics in unit tests).
func TestMetrics_NilReceiverIsNoop(t *testing.T) {
	var m *Stream
	require.NotPanics(t, func() {
		m.recordDownstream(1)
		m.recordUpstream(1)
		m.recordDownstreamLatency(1)
		m.recordUpstreamLatency(1)
		m.IncActive()
		m.DecActive()
		m.RecordDuration(1)
		m.RecordSetupLatency(1)
		m.RecordTermination(TerminationClientDisconnect)
	})
}

// TestMetrics_RecordsBytesAndChunksPerDirection asserts bytes/chunks are
// attributed to the right direction and accumulate correctly.
func TestMetrics_RecordsBytesAndChunksPerDirection(t *testing.T) {
	m, reader := setupReplStream(t)

	m.recordDownstream(100)
	m.recordDownstream(40)
	m.recordUpstream(7)

	agg := collectAggregation(t, reader, "mg.pooler.replication.bytes")
	require.NotNil(t, agg, "bytes counter not emitted")
	sum, ok := agg.(metricdata.Sum[int64])
	require.True(t, ok, "expected Sum[int64]")

	byDirection := map[string]int64{}
	for _, dp := range sum.DataPoints {
		dir, present := dp.Attributes.Value(attribute.Key(attrDirection))
		require.True(t, present, "data point missing direction attribute")
		byDirection[dir.AsString()] = dp.Value
	}
	require.Equal(t, int64(140), byDirection[directionDownstream])
	require.Equal(t, int64(7), byDirection[directionUpstream])

	chunks := collectAggregation(t, reader, "mg.pooler.replication.chunks")
	require.NotNil(t, chunks, "chunks counter not emitted")
	chunkSum := chunks.(metricdata.Sum[int64])
	byDirChunks := map[string]int64{}
	for _, dp := range chunkSum.DataPoints {
		dir, _ := dp.Attributes.Value(attribute.Key(attrDirection))
		byDirChunks[dir.AsString()] = dp.Value
	}
	require.Equal(t, int64(2), byDirChunks[directionDownstream])
	require.Equal(t, int64(1), byDirChunks[directionUpstream])
}

// TestMetrics_LifecycleInstruments exercises the lifecycle recorders and spot
// checks that representative instruments emit.
func TestMetrics_LifecycleInstruments(t *testing.T) {
	m, reader := setupReplStream(t)

	m.IncActive()
	m.RecordSetupLatency(0.01)
	m.recordDownstreamLatency(0.5)
	m.recordUpstreamLatency(0.2)
	m.RecordTermination(TerminationClientDisconnect)
	m.RecordDuration(12.5)
	m.DecActive()

	active := collectAggregation(t, reader, "mg.pooler.replication.active")
	require.NotNil(t, active, "active gauge not emitted")
	activeSum := active.(metricdata.Sum[int64])
	require.Len(t, activeSum.DataPoints, 1)
	require.Equal(t, int64(0), activeSum.DataPoints[0].Value, "active should be balanced after inc+dec")

	term := collectAggregation(t, reader, "mg.pooler.replication.terminations")
	require.NotNil(t, term, "terminations counter not emitted")
	termSum := term.(metricdata.Sum[int64])
	require.Len(t, termSum.DataPoints, 1)
	reason, present := termSum.DataPoints[0].Attributes.Value(attribute.Key(attrReason))
	require.True(t, present)
	require.Equal(t, TerminationClientDisconnect, reason.AsString())

	require.NotNil(t, collectAggregation(t, reader, "mg.pooler.replication.duration"))
	require.NotNil(t, collectAggregation(t, reader, "mg.pooler.replication.setup.latency"))
	require.NotNil(t, collectAggregation(t, reader, "mg.pooler.replication.forward.latency"))
}
