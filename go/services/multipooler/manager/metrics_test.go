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

	"github.com/multigres/multigres/go/tools/telemetry"
)

// getCounterInt64 extracts a named Int64 counter (Sum) from collected metric data.
func getCounterInt64(t *testing.T, reader *sdkmetric.ManualReader, name string) *metricdata.Sum[int64] {
	t.Helper()

	var metricData metricdata.ResourceMetrics
	err := reader.Collect(t.Context(), &metricData)
	require.NoError(t, err)

	for _, scopeMetric := range metricData.ScopeMetrics {
		for _, m := range scopeMetric.Metrics {
			if m.Name == name {
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok, "expected Sum[int64] data type for %s", name)
				return &sum
			}
		}
	}
	return nil
}

// counterValue returns the single data point value from a counter Sum, or 0 if nil.
func counterValue(s *metricdata.Sum[int64]) int64 {
	if s == nil || len(s.DataPoints) == 0 {
		return 0
	}
	return s.DataPoints[0].Value
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

// TestMetrics_SingleAttemptSuccess verifies that a single attempt + success
// yields attempts=1, successes=1, failures=0.
func TestMetrics_SingleAttemptSuccess(t *testing.T) {
	m, reader := setupMetrics(t)
	ctx := t.Context()

	m.IncBackupAttempts(ctx)
	m.IncBackupSuccesses(ctx)

	attempts := getCounterInt64(t, reader, "pgbackrest_backup_attempts_total")
	require.NotNil(t, attempts, "pgbackrest_backup_attempts_total counter not found")
	assert.Equal(t, int64(1), counterValue(attempts))

	successes := getCounterInt64(t, reader, "pgbackrest_backup_successes_total")
	require.NotNil(t, successes, "pgbackrest_backup_successes_total counter not found")
	assert.Equal(t, int64(1), counterValue(successes))

	// failures counter was never incremented, so it may not appear in collected metrics;
	// counterValue returns 0 for nil, which is the expected value.
	failures := getCounterInt64(t, reader, "pgbackrest_backup_failures_total")
	assert.Equal(t, int64(0), counterValue(failures))
}

// TestMetrics_SingleAttemptFailure verifies that a single attempt + failure
// yields attempts=1, successes=0, failures=1.
func TestMetrics_SingleAttemptFailure(t *testing.T) {
	m, reader := setupMetrics(t)
	ctx := t.Context()

	m.IncBackupAttempts(ctx)
	m.IncBackupFailures(ctx)

	attempts := getCounterInt64(t, reader, "pgbackrest_backup_attempts_total")
	require.NotNil(t, attempts, "pgbackrest_backup_attempts_total counter not found")
	assert.Equal(t, int64(1), counterValue(attempts))

	// successes counter was never incremented, so it may not appear in collected metrics;
	// counterValue returns 0 for nil, which is the expected value.
	successes := getCounterInt64(t, reader, "pgbackrest_backup_successes_total")
	assert.Equal(t, int64(0), counterValue(successes))

	failures := getCounterInt64(t, reader, "pgbackrest_backup_failures_total")
	require.NotNil(t, failures, "pgbackrest_backup_failures_total counter not found")
	assert.Equal(t, int64(1), counterValue(failures))
}

// TestMetrics_NewMetrics_ReturnsNonNil verifies that NewMetrics() returns non-nil
// even with the default (noop) provider.
func TestMetrics_NewMetrics_ReturnsNonNil(t *testing.T) {
	m, err := NewMetrics()
	assert.NoError(t, err)
	assert.NotNil(t, m, "NewMetrics() should always return non-nil *Metrics")
}

// TestMetrics_NilSafe verifies that calling increment methods on a nil *Metrics
// does not panic.
func TestMetrics_NilSafe(t *testing.T) {
	var m *Metrics
	ctx := t.Context()

	assert.NotPanics(t, func() { m.IncBackupAttempts(ctx) })
	assert.NotPanics(t, func() { m.IncBackupSuccesses(ctx) })
	assert.NotPanics(t, func() { m.IncBackupFailures(ctx) })
}

// TestMetrics_Property_AttemptsEqualSuccessesPlusFailures generates 100 random
// backup outcomes and asserts the counter invariant holds.
func TestMetrics_Property_AttemptsEqualSuccessesPlusFailures(t *testing.T) {
	m, reader := setupMetrics(t)
	ctx := t.Context()

	var expectedSuccesses, expectedFailures int64

	for range 100 {
		m.IncBackupAttempts(ctx)

		if rand.IntN(2) == 1 {
			m.IncBackupSuccesses(ctx)
			expectedSuccesses++
		} else {
			m.IncBackupFailures(ctx)
			expectedFailures++
		}
	}

	attempts := getCounterInt64(t, reader, "pgbackrest_backup_attempts_total")
	require.NotNil(t, attempts, "pgbackrest_backup_attempts_total counter not found")

	successes := getCounterInt64(t, reader, "pgbackrest_backup_successes_total")
	require.NotNil(t, successes, "pgbackrest_backup_successes_total counter not found")

	failures := getCounterInt64(t, reader, "pgbackrest_backup_failures_total")
	require.NotNil(t, failures, "pgbackrest_backup_failures_total counter not found")

	assert.Equal(t, int64(100), counterValue(attempts), "should have 100 total attempts")
	assert.Equal(t, expectedSuccesses, counterValue(successes), "successes should match expected")
	assert.Equal(t, expectedFailures, counterValue(failures), "failures should match expected")
	assert.Equal(t, counterValue(attempts), counterValue(successes)+counterValue(failures),
		"attempts should equal successes + failures")
}
