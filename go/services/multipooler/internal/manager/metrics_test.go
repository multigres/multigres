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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
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

	// DISABLED (initial) → SERVING records one transition.
	require.NoError(t, hs.OnStateChange(ctx,
		servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	// SERVING → SERVING is a no-op (role change only, primary → replica):
	// no new transition.
	require.NoError(t, hs.OnStateChange(ctx,
		servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	// SERVING → DISABLED records a second transition.
	require.NoError(t, hs.OnStateChange(ctx,
		servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))

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
	from := clustermetadatapb.PoolerServingStatus_DISABLED
	to := clustermetadatapb.PoolerServingStatus_SERVING

	var nilM *healthMetrics
	nilM.recordTransition(t.Context(), from, to)

	(&healthMetrics{}).recordTransition(t.Context(), from, to)
}

// TestRewindExecutionDurationMetric verifies pg_rewind runtime is recorded per
// phase (dry_run vs the mutating rewind) with the durations converted to seconds.
func TestRewindExecutionDurationMetric(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := newManagerMetrics()
	require.NoError(t, err)

	m.recordRewindExecutionDuration(t.Context(), rewindPhaseRewind, 2500*time.Millisecond)
	m.recordRewindExecutionDuration(t.Context(), rewindPhaseDryRun, 500*time.Millisecond)

	hist := findMetric(t, setup.MetricReader, "multipooler.rewind.execution.duration")
	h, ok := hist.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, h.DataPoints, 2, "one data point per phase")

	byPhase := map[string]float64{}
	for _, dp := range h.DataPoints {
		byPhase[attrValue(t, dp.Attributes, "phase")] = dp.Sum
	}
	assert.InDelta(t, 2.5, byPhase[string(rewindPhaseRewind)], 1e-9)
	assert.InDelta(t, 0.5, byPhase[string(rewindPhaseDryRun)], 1e-9)
}

// TestRecordRewindExecutionDuration_NilSafe covers the guards: a nil receiver and
// a zero-value managerMetrics (nil histogram) must both be no-ops.
func TestRecordRewindExecutionDuration_NilSafe(t *testing.T) {
	var nilM *managerMetrics
	nilM.recordRewindExecutionDuration(t.Context(), rewindPhaseRewind, time.Second)

	(&managerMetrics{}).recordRewindExecutionDuration(t.Context(), rewindPhaseDryRun, time.Second)
}

// TestRecordLogicalFailover verifies the logical-replication slot-management
// span's duration and count are recorded per outcome.
func TestRecordLogicalFailover(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := newManagerMetrics()
	require.NoError(t, err)

	m.recordLogicalFailover(t.Context(), logicalFailoverStatusSuccess, 1500*time.Millisecond)
	m.recordLogicalFailover(t.Context(), logicalFailoverStatusFailure, 500*time.Millisecond)

	hist := findMetric(t, setup.MetricReader, "mg.pooler.logical_failover.duration")
	h, ok := hist.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, h.DataPoints, 2, "one data point per status")
	byStatus := map[string]float64{}
	for _, dp := range h.DataPoints {
		byStatus[attrValue(t, dp.Attributes, "status")] = dp.Sum
	}
	assert.InDelta(t, 1.5, byStatus[string(logicalFailoverStatusSuccess)], 1e-9)
	assert.InDelta(t, 0.5, byStatus[string(logicalFailoverStatusFailure)], 1e-9)

	count := findMetric(t, setup.MetricReader, "mg.pooler.logical_failover.count")
	sum, ok := count.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 2, "one data point per status")
	for _, dp := range sum.DataPoints {
		assert.Equal(t, int64(1), dp.Value)
	}
}

// TestRecordLogicalFailover_NilSafe covers the guards: a nil receiver and a
// zero-value managerMetrics (nil instruments) must both be no-ops.
func TestRecordLogicalFailover_NilSafe(t *testing.T) {
	var nilM *managerMetrics
	nilM.recordLogicalFailover(t.Context(), logicalFailoverStatusSuccess, time.Second)

	(&managerMetrics{}).recordLogicalFailover(t.Context(), logicalFailoverStatusFailure, time.Second)
}

// TestRecordSlotsDropped verifies slots-dropped counts are recorded per reason
// and that a zero count is a no-op (no empty data point emitted).
func TestRecordSlotsDropped(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := newManagerMetrics()
	require.NoError(t, err)

	m.recordSlotsDropped(t.Context(), slotsDroppedReasonOrphaned, 2)
	m.recordSlotsDropped(t.Context(), slotsDroppedReasonDepartedFollower, 1)
	m.recordSlotsDropped(t.Context(), slotsDroppedReasonDepartedFollower, 0) // no-op

	sum, ok := findMetric(t, setup.MetricReader, "mg.pooler.logical_failover.slots_dropped").Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 2, "one data point per reason; the zero-count call adds none")

	byReason := map[string]int64{}
	for _, dp := range sum.DataPoints {
		byReason[attrValue(t, dp.Attributes, "reason")] = dp.Value
	}
	assert.Equal(t, int64(2), byReason[string(slotsDroppedReasonOrphaned)])
	assert.Equal(t, int64(1), byReason[string(slotsDroppedReasonDepartedFollower)])
}

// TestRecordSlotsDropped_NilSafe covers the guards: a nil receiver and a
// zero-value managerMetrics (nil counter) must both be no-ops.
func TestRecordSlotsDropped_NilSafe(t *testing.T) {
	var nilM *managerMetrics
	nilM.recordSlotsDropped(t.Context(), slotsDroppedReasonOrphaned, 1)

	(&managerMetrics{}).recordSlotsDropped(t.Context(), slotsDroppedReasonOrphaned, 1)
}

// TestFailoverSlotReadinessGauge verifies the observable gauge samples the
// readiness atomics set via setFailoverSlotReadiness, split by status label.
func TestFailoverSlotReadinessGauge(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := newManagerMetrics()
	require.NoError(t, err)

	m.setFailoverSlotReadiness(2, 3)

	g := findMetric(t, setup.MetricReader, "mg.pooler.logical_failover.slots")
	gauge, ok := g.Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Len(t, gauge.DataPoints, 2, "one data point per status")

	byStatus := map[string]int64{}
	for _, dp := range gauge.DataPoints {
		byStatus[attrValue(t, dp.Attributes, "status")] = dp.Value
	}
	assert.Equal(t, int64(2), byStatus["ready"])
	assert.Equal(t, int64(1), byStatus["unready"])
}

// TestFailoverSlotReadinessGauge_NilSafe covers the guard: a nil receiver must
// be a no-op.
func TestFailoverSlotReadinessGauge_NilSafe(t *testing.T) {
	var nilM *managerMetrics
	nilM.setFailoverSlotReadiness(2, 3)
}

// TestFailoverSlotReadinessGauge_NotYetMeasured verifies the gauge reports no
// data points before setFailoverSlotReadiness has ever been called, rather
// than a misleading ready=0/unready=0 (which would look identical to "no
// failover slots exist").
func TestFailoverSlotReadinessGauge_NotYetMeasured(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	_, err := newManagerMetrics()
	require.NoError(t, err)

	var rm metricdata.ResourceMetrics
	require.NoError(t, setup.MetricReader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, mm := range sm.Metrics {
			assert.NotEqual(t, "mg.pooler.logical_failover.slots", mm.Name,
				"gauge must report nothing until setFailoverSlotReadiness has run once")
		}
	}
}

// TestFailoverSlotReadinessGauge_ConcurrentUpdates exercises
// setFailoverSlotReadiness and the gauge callback from concurrent goroutines.
// ready and total are published as a single atomic pointer swap, so a
// concurrent reader must only ever see one of the stored (ready, total)
// pairs — never a torn combination (e.g. a newer ready with an older total)
// that would make total-ready negative. Run with -race to also catch any
// unsynchronized access.
func TestFailoverSlotReadinessGauge_ConcurrentUpdates(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := newManagerMetrics()
	require.NoError(t, err)

	pairs := [][2]int{{0, 0}, {2, 3}, {5, 5}, {0, 7}, {1, 1}}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 200 {
			p := pairs[i%len(pairs)]
			m.setFailoverSlotReadiness(p[0], p[1])
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			var rm metricdata.ResourceMetrics
			if !assert.NoError(t, setup.MetricReader.Collect(t.Context(), &rm)) {
				return
			}
			for _, sm := range rm.ScopeMetrics {
				for _, mm := range sm.Metrics {
					if mm.Name != "mg.pooler.logical_failover.slots" {
						continue
					}
					gauge, ok := mm.Data.(metricdata.Gauge[int64])
					if !assert.True(t, ok) {
						continue
					}
					for _, dp := range gauge.DataPoints {
						assert.GreaterOrEqual(t, dp.Value, int64(0),
							"a status data point must never go negative from a torn ready/total read")
					}
				}
			}
		}
	}()
	wg.Wait()
}
