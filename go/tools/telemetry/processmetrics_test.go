// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"context"
	"errors"
	"testing"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectMetrics initializes telemetry, collects one round of metrics through
// the in-memory ManualReader, and returns the scope metrics.
func collectMetrics(t *testing.T) []metricdata.ScopeMetrics {
	t.Helper()

	setup := SetupTestTelemetry(t)
	ctx := context.Background()

	require.NoError(t, setup.Telemetry.InitTelemetry(ctx, "test-service"))
	t.Cleanup(func() {
		require.NoError(t, setup.Telemetry.ShutdownTelemetry(ctx))
	})

	var rm metricdata.ResourceMetrics
	require.NoError(t, setup.MetricReader.Collect(ctx, &rm))
	return rm.ScopeMetrics
}

// metricByName returns the collected metric with the given OTel name.
func metricByName(t *testing.T, scopes []metricdata.ScopeMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, scope := range scopes {
		for _, m := range scope.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

// gaugeInt64 returns the first Int64 gauge data point value for name.
func gaugeInt64(t *testing.T, scopes []metricdata.ScopeMetrics, name string) int64 {
	t.Helper()
	m := metricByName(t, scopes, name)
	g, ok := m.Data.(metricdata.Gauge[int64])
	require.Truef(t, ok, "%s should be an Int64 gauge, got %T", name, m.Data)
	require.NotEmptyf(t, g.DataPoints, "%s should have a data point", name)
	return g.DataPoints[0].Value
}

// metricNameSet flattens every emitted metric name across all scopes into a set.
func metricNameSet(scopes []metricdata.ScopeMetrics) map[string]bool {
	names := make(map[string]bool)
	for _, scope := range scopes {
		for _, m := range scope.Metrics {
			names[m.Name] = true
		}
	}
	return names
}

// hasDataPoints reports whether a metric with the given name was emitted with at
// least one data point. Observable instruments that receive no measurement in a
// collection cycle are omitted, so this is false when every read was skipped.
func hasDataPoints(scopes []metricdata.ScopeMetrics, name string) bool {
	for _, scope := range scopes {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				return len(d.DataPoints) > 0
			case metricdata.Sum[float64]:
				return len(d.DataPoints) > 0
			default:
				return true
			}
		}
	}
	return false
}

// TestProcessMetrics_Exported verifies that every Multigres component, by virtue
// of going through InitTelemetry, exports process CPU/memory metrics (the
// kubectl-top replacement) with the right units and plausible values.
func TestProcessMetrics_Exported(t *testing.T) {
	scopes := collectMetrics(t)

	// Memory gauges: correct unit and plausible values.
	rssMetric := metricByName(t, scopes, "process.memory.usage")
	assert.Equal(t, "By", rssMetric.Unit, "process.memory.usage should be in bytes")
	vmsMetric := metricByName(t, scopes, "process.memory.virtual")
	assert.Equal(t, "By", vmsMetric.Unit, "process.memory.virtual should be in bytes")

	rss := gaugeInt64(t, scopes, "process.memory.usage")
	assert.Positive(t, rss, "process.memory.usage (RSS) should be positive")
	vms := gaugeInt64(t, scopes, "process.memory.virtual")
	assert.GreaterOrEqual(t, vms, rss, "virtual memory should be >= resident memory")

	// CPU time: a monotonic Float64 counter in seconds, split by cpu.mode.
	cpuMetric := metricByName(t, scopes, "process.cpu.time")
	assert.Equal(t, "s", cpuMetric.Unit, "process.cpu.time should be in seconds")
	sum, ok := cpuMetric.Data.(metricdata.Sum[float64])
	require.Truef(t, ok, "process.cpu.time should be a Float64 sum, got %T", cpuMetric.Data)
	assert.True(t, sum.IsMonotonic, "process.cpu.time should be a monotonic counter")

	modes := make(map[string]bool)
	for _, dp := range sum.DataPoints {
		if v, present := dp.Attributes.Value(cpuModeKey); present {
			modes[v.AsString()] = true
		}
		assert.GreaterOrEqual(t, dp.Value, 0.0, "cpu time must be non-negative")
	}
	assert.True(t, modes["user"], "process.cpu.time should have a user-mode series")
	assert.True(t, modes["system"], "process.cpu.time should have a system-mode series")
}

// TestProcessMetrics_GoRuntimeExported verifies the Go runtime collector is wired
// up and emits exactly the instrument set the generated metric catalog hard-codes
// (go/tools/metricsgen runtimeMetricSpecs). If a dependency bump changes this set,
// this fails so the catalog list can be updated in lock-step.
func TestProcessMetrics_GoRuntimeExported(t *testing.T) {
	names := metricNameSet(collectMetrics(t))

	// Mirror of go/tools/metricsgen runtimeMetricSpecs (OTel instrument names).
	wantRuntime := []string{
		"go.config.gogc",
		"go.goroutine.count",
		"go.memory.allocated",
		"go.memory.allocations",
		"go.memory.gc.goal",
		"go.memory.used",
		"go.processor.limit",
	}
	for _, name := range wantRuntime {
		assert.Truef(t, names[name], "expected Go runtime metric %q; got %v", name, names)
	}
}

// fakeProcessReader is a processStatReader whose reads can be made to fail.
type fakeProcessReader struct {
	times    *cpu.TimesStat
	mem      *process.MemoryInfoStat
	timesErr error
	memErr   error
}

func (f fakeProcessReader) TimesWithContext(context.Context) (*cpu.TimesStat, error) {
	return f.times, f.timesErr
}

func (f fakeProcessReader) MemoryInfoWithContext(context.Context) (*process.MemoryInfoStat, error) {
	return f.mem, f.memErr
}

// TestInitProcessMetrics_RuntimeStartError verifies a failure to start the Go
// runtime collector fails telemetry init (observability is a hard requirement).
func TestInitProcessMetrics_RuntimeStartError(t *testing.T) {
	orig := startRuntimeMetrics
	t.Cleanup(func() { startRuntimeMetrics = orig })
	startRuntimeMetrics = func(metric.MeterProvider) error { return errors.New("boom") }

	setup := SetupTestTelemetry(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = setup.Telemetry.ShutdownTelemetry(ctx) })

	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Go runtime metrics")
}

// TestInitProcessMetrics_ProcessOpenError verifies a failure to open the current
// process for stats fails telemetry init.
func TestInitProcessMetrics_ProcessOpenError(t *testing.T) {
	orig := newProcessReader
	t.Cleanup(func() { newProcessReader = orig })
	newProcessReader = func() (processStatReader, error) { return nil, errors.New("no such process") }

	setup := SetupTestTelemetry(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = setup.Telemetry.ShutdownTelemetry(ctx) })

	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open current process")
}

// TestProcessMetrics_ReadFailuresSkip verifies that when the per-scrape stat
// reads fail, init still succeeds and the affected process metrics are simply
// omitted rather than crashing or failing collection.
func TestProcessMetrics_ReadFailuresSkip(t *testing.T) {
	orig := newProcessReader
	t.Cleanup(func() { newProcessReader = orig })
	newProcessReader = func() (processStatReader, error) {
		return fakeProcessReader{
			timesErr: errors.New("cpu read failed"),
			memErr:   errors.New("mem read failed"),
		}, nil
	}

	scopes := collectMetrics(t)

	assert.False(t, hasDataPoints(scopes, "process.cpu.time"),
		"process.cpu.time should be omitted when the CPU read fails")
	assert.False(t, hasDataPoints(scopes, "process.memory.usage"),
		"process.memory.usage should be omitted when the memory read fails")

	// The Go runtime collector is independent and still reports.
	assert.True(t, metricNameSet(scopes)["go.memory.used"],
		"runtime metrics should be unaffected by a process read failure")
}

// TestProcessMetrics_PartialReadSuccess verifies the two reads are independent:
// a memory read failure does not suppress CPU metrics, and vice versa.
func TestProcessMetrics_PartialReadSuccess(t *testing.T) {
	orig := newProcessReader
	t.Cleanup(func() { newProcessReader = orig })
	newProcessReader = func() (processStatReader, error) {
		return fakeProcessReader{
			times:  &cpu.TimesStat{User: 1.5, System: 0.5},
			memErr: errors.New("mem read failed"),
		}, nil
	}

	scopes := collectMetrics(t)

	assert.True(t, hasDataPoints(scopes, "process.cpu.time"),
		"process.cpu.time should report even when the memory read fails")
	assert.False(t, hasDataPoints(scopes, "process.memory.usage"),
		"process.memory.usage should be omitted when the memory read fails")
}
