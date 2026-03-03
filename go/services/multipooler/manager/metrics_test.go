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
	"context"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
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

func TestMetrics_Property_RunningTrueYieldsServerUp1(t *testing.T) {
	// Fuzz Running=true with random RestartCount values and check that
	// pgbackrest_server_up always reports 1 and restart_count matches.

	setup := telemetry.SetupTestTelemetry(t)
	ctx := t.Context()
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	m, err := NewMetrics()
	require.NoError(t, err)

	for i := range 100 {
		restartCount := rand.Int32N(10000)

		m.UpdateFromPgBackRestStatus(&pgctldpb.PgBackRestStatus{
			Running:      true,
			RestartCount: restartCount,
		})

		serverUp := getGaugeInt64(t, setup.MetricReader, "pgbackrest_server_up")
		require.NotNil(t, serverUp, "iteration %d: pgbackrest_server_up gauge not found", i)
		assert.Equal(t, int64(1), gaugeValue(serverUp),
			"iteration %d: pgbackrest_server_up should be 1 when Running=true", i)

		restarts := getGaugeInt64(t, setup.MetricReader, "pgbackrest_restart_count")
		require.NotNil(t, restarts, "iteration %d: pgbackrest_restart_count gauge not found", i)
		assert.Equal(t, int64(restartCount), gaugeValue(restarts),
			"iteration %d: pgbackrest_restart_count should match input RestartCount=%d", i, restartCount)
	}
}

// TestMetrics_RunningTrue_ServerUpReports1 verifies that when PgBackRestStatus.Running
// is true, the pgbackrest_server_up gauge reports 1.
func TestMetrics_RunningTrue_ServerUpReports1(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	ctx := t.Context()
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	m, err := NewMetrics()
	require.NoError(t, err)

	m.UpdateFromPgBackRestStatus(&pgctldpb.PgBackRestStatus{
		Running:      true,
		RestartCount: 3,
	})

	serverUp := getGaugeInt64(t, setup.MetricReader, "pgbackrest_server_up")
	require.NotNil(t, serverUp, "pgbackrest_server_up gauge not found")
	assert.Equal(t, int64(1), gaugeValue(serverUp), "pgbackrest_server_up should be 1 when Running=true")
}

// TestMetrics_RunningFalse_ServerUpReports0 verifies that when PgBackRestStatus.Running
// is false, the pgbackrest_server_up gauge reports 0.
func TestMetrics_RunningFalse_ServerUpReports0(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	ctx := t.Context()
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	m, err := NewMetrics()
	require.NoError(t, err)

	m.UpdateFromPgBackRestStatus(&pgctldpb.PgBackRestStatus{
		Running:      false,
		RestartCount: 5,
	})

	serverUp := getGaugeInt64(t, setup.MetricReader, "pgbackrest_server_up")
	require.NotNil(t, serverUp, "pgbackrest_server_up gauge not found")
	assert.Equal(t, int64(0), gaugeValue(serverUp), "pgbackrest_server_up should be 0 when Running=false")
}

// TestMetrics_RestartCountPropagation verifies that pgbackrest_restart_count matches
// the RestartCount field from PgBackRestStatus.
func TestMetrics_RestartCountPropagation(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	ctx := t.Context()
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	m, err := NewMetrics()
	require.NoError(t, err)

	m.UpdateFromPgBackRestStatus(&pgctldpb.PgBackRestStatus{
		Running:      true,
		RestartCount: 42,
	})

	restarts := getGaugeInt64(t, setup.MetricReader, "pgbackrest_restart_count")
	require.NotNil(t, restarts, "pgbackrest_restart_count gauge not found")
	assert.Equal(t, int64(42), gaugeValue(restarts), "pgbackrest_restart_count should match PgBackRestStatus.RestartCount")
}

// TestMetrics_NilStatus_BothGaugesReport0 verifies that when PgBackRestStatus is nil,
// both pgbackrest_server_up and pgbackrest_restart_count report 0.
func TestMetrics_NilStatus_BothGaugesReport0(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	ctx := t.Context()
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	m, err := NewMetrics()
	require.NoError(t, err)

	// First set non-zero values to ensure nil actually resets them
	m.UpdateFromPgBackRestStatus(&pgctldpb.PgBackRestStatus{
		Running:      true,
		RestartCount: 7,
	})

	m.UpdateFromPgBackRestStatus(nil)

	serverUp := getGaugeInt64(t, setup.MetricReader, "pgbackrest_server_up")
	require.NotNil(t, serverUp, "pgbackrest_server_up gauge not found")
	assert.Equal(t, int64(0), gaugeValue(serverUp), "pgbackrest_server_up should be 0 for nil status")

	restarts := getGaugeInt64(t, setup.MetricReader, "pgbackrest_restart_count")
	require.NotNil(t, restarts, "pgbackrest_restart_count gauge not found")
	assert.Equal(t, int64(0), gaugeValue(restarts), "pgbackrest_restart_count should be 0 for nil status")
}
