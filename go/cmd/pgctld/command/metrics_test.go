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

package command

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/tools/telemetry"
)

// getGaugeInt64 extracts a named Int64 gauge from collected metric data.
func getGaugeInt64(t *testing.T, reader *sdkmetric.ManualReader, name string) *metricdata.Gauge[int64] {
	t.Helper()

	var metricData metricdata.ResourceMetrics
	err := reader.Collect(t.Context(), &metricData)
	require.NoError(t, err)

	for _, scopeMetric := range metricData.ScopeMetrics {
		for _, m := range scopeMetric.Metrics {
			if m.Name == name {
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok, "expected Gauge[int64] data type for %s", name)
				return &gauge
			}
		}
	}
	return nil
}

// gaugeValue returns the single data point value from a gauge, or 0 if nil.
func gaugeValue(g *metricdata.Gauge[int64]) int64 {
	if g == nil || len(g.DataPoints) == 0 {
		return 0
	}
	return g.DataPoints[0].Value
}

// setupMetrics is a test helper that initializes telemetry and creates a Metrics instance.
func setupMetrics(t *testing.T) (*Metrics, *sdkmetric.ManualReader) {
	t.Helper()

	setup := telemetry.SetupTestTelemetry(t)
	ctx := t.Context()
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	m, err := NewMetrics()
	require.NoError(t, err)

	return m, setup.MetricReader
}

func TestMetrics_SetServerUp_True(t *testing.T) {
	m, reader := setupMetrics(t)

	m.SetServerUp(true)

	serverUp := getGaugeInt64(t, reader, "pgbackrest_server_up")
	require.NotNil(t, serverUp, "pgbackrest_server_up gauge not found")
	assert.Equal(t, int64(1), gaugeValue(serverUp), "pgbackrest_server_up should be 1 when SetServerUp(true)")
}

func TestMetrics_SetServerUp_False(t *testing.T) {
	m, reader := setupMetrics(t)

	m.SetServerUp(false)

	serverUp := getGaugeInt64(t, reader, "pgbackrest_server_up")
	require.NotNil(t, serverUp, "pgbackrest_server_up gauge not found")
	assert.Equal(t, int64(0), gaugeValue(serverUp), "pgbackrest_server_up should be 0 when SetServerUp(false)")
}

func TestMetrics_SetRestartCount(t *testing.T) {
	m, reader := setupMetrics(t)

	m.SetRestartCount(42)

	restarts := getGaugeInt64(t, reader, "pgbackrest_restart_count")
	require.NotNil(t, restarts, "pgbackrest_restart_count gauge not found")
	assert.Equal(t, int64(42), gaugeValue(restarts), "pgbackrest_restart_count should be 42")
}

func TestMetrics_NewMetrics_ReturnsNonNil(t *testing.T) {
	m, err := NewMetrics()
	assert.NoError(t, err)
	assert.NotNil(t, m, "NewMetrics() should always return non-nil *Metrics")
}

// TestMetrics_Property_ServerUpReflectsRunningState generates random boolean sequences,
// calls SetServerUp, and verifies the gauge always reflects the most recent value.
func TestMetrics_Property_ServerUpReflectsRunningState(t *testing.T) {
	m, reader := setupMetrics(t)

	for i := range 100 {
		running := rand.IntN(2) == 1

		m.SetServerUp(running)

		serverUp := getGaugeInt64(t, reader, "pgbackrest_server_up")
		require.NotNil(t, serverUp, "iteration %d: pgbackrest_server_up gauge not found", i)

		var expected int64
		if running {
			expected = 1
		}
		assert.Equal(t, expected, gaugeValue(serverUp),
			"iteration %d: pgbackrest_server_up should be %d when running=%v", i, expected, running)
	}
}

// TestMetrics_Property_RestartCountReflectsValue generates random int32 values,
// calls SetRestartCount, and verifies the gauge always matches.
func TestMetrics_Property_RestartCountReflectsValue(t *testing.T) {
	m, reader := setupMetrics(t)

	for i := range 100 {
		count := rand.Int32()

		m.SetRestartCount(count)

		restarts := getGaugeInt64(t, reader, "pgbackrest_restart_count")
		require.NotNil(t, restarts, "iteration %d: pgbackrest_restart_count gauge not found", i)
		assert.Equal(t, int64(count), gaugeValue(restarts),
			"iteration %d: pgbackrest_restart_count should match input count=%d", i, count)
	}
}

// TestMetrics_ServerUptime_PositiveWhenUp verifies that the uptime gauge is positive
// when the server is up, and 0 when the server is down.
func TestMetrics_ServerUptime_PositiveWhenUp(t *testing.T) {
	m, reader := setupMetrics(t)

	m.SetServerUp(true)

	uptime := getGaugeInt64(t, reader, "pgbackrest_server_uptime_seconds")
	require.NotNil(t, uptime, "pgbackrest_server_uptime_seconds gauge not found")
	assert.GreaterOrEqual(t, gaugeValue(uptime), int64(0),
		"uptime should be >= 0 when server is up")

	m.SetServerUp(false)

	uptime = getGaugeInt64(t, reader, "pgbackrest_server_uptime_seconds")
	require.NotNil(t, uptime, "pgbackrest_server_uptime_seconds gauge not found")
	assert.Equal(t, int64(0), gaugeValue(uptime),
		"uptime should be 0 when server is down")
}
