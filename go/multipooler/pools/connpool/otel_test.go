// Copyright 2025 Supabase, Inc.
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

package connpool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/tools/telemetry"
)

// getConnectionCountMetric extracts the db.client.connection.count metric data.
func getConnectionCountMetric(t *testing.T, reader *sdkmetric.ManualReader) *metricdata.Sum[int64] {
	t.Helper()

	var metricData metricdata.ResourceMetrics
	err := reader.Collect(t.Context(), &metricData)
	require.NoError(t, err)

	for _, scopeMetric := range metricData.ScopeMetrics {
		for _, m := range scopeMetric.Metrics {
			if m.Name == "db.client.connection.count" {
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok, "expected Sum[int64] data type for db.client.connection.count")
				return &sum
			}
		}
	}
	return nil
}

// getStateCount extracts the count for a specific pool name and state from the metric data.
func getStateCount(sum *metricdata.Sum[int64], poolName, state string) int64 {
	if sum == nil {
		return 0
	}
	for _, dp := range sum.DataPoints {
		var dpPoolName, dpState string
		for _, attr := range dp.Attributes.ToSlice() {
			if string(attr.Key) == attrKeyPoolName {
				dpPoolName = attr.Value.AsString()
			}
			if string(attr.Key) == attrKeyState {
				dpState = attr.Value.AsString()
			}
		}
		if dpPoolName == poolName && dpState == state {
			return dp.Value
		}
	}
	return 0
}

func TestOTelConnectionCount_GetAndRecycle(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)

	ctx := t.Context()
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	meter := setup.Telemetry.GetMeterProvider().Meter("test")
	connCount, err := NewConnectionCount(meter)
	require.NoError(t, err)

	pool := NewPool[*mockConnection](ctx, &Config{
		Name:            "test-pool",
		Capacity:        2,
		MaxIdleCount:    2,
		ConnectionCount: connCount,
	})
	pool.Open(func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}, nil)
	defer pool.Close()

	// Initially, no connections exist
	sum := getConnectionCountMetric(t, setup.MetricReader)
	assert.Nil(t, sum, "should have no metrics before any connections")

	// Get a connection - should be used=1, idle=0 (new connection, never went to idle)
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn1)

	sum = getConnectionCountMetric(t, setup.MetricReader)
	require.NotNil(t, sum, "should have metrics after Get")
	assert.Equal(t, int64(1), getStateCount(sum, "test-pool", "used"), "used should be 1 after Get")
	assert.Equal(t, int64(0), getStateCount(sum, "test-pool", "idle"), "idle should be 0 (new connection never idled)")

	// Recycle the connection - should be used=0, idle=1
	conn1.Recycle()

	sum = getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(0), getStateCount(sum, "test-pool", "used"), "used should be 0 after Recycle")
	assert.Equal(t, int64(1), getStateCount(sum, "test-pool", "idle"), "idle should be 1 after Recycle")

	// Get the same connection again - should be used=1, idle=0
	conn2, err := pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	sum = getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(1), getStateCount(sum, "test-pool", "used"), "used should be 1 after second Get")
	assert.Equal(t, int64(0), getStateCount(sum, "test-pool", "idle"), "idle should be 0 after second Get")

	// Recycle again
	conn2.Recycle()

	sum = getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(0), getStateCount(sum, "test-pool", "used"), "used should be 0 after second Recycle")
	assert.Equal(t, int64(1), getStateCount(sum, "test-pool", "idle"), "idle should be 1 after second Recycle")
}

func TestOTelConnectionCount_MultipleConnections(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)

	ctx := t.Context()
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	meter := setup.Telemetry.GetMeterProvider().Meter("test")
	connCount, err := NewConnectionCount(meter)
	require.NoError(t, err)

	pool := NewPool[*mockConnection](ctx, &Config{
		Name:            "multi-pool",
		Capacity:        5,
		MaxIdleCount:    5,
		ConnectionCount: connCount,
	})
	pool.Open(func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}, nil)
	defer pool.Close()

	// Get 3 connections
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)
	conn2, err := pool.Get(ctx)
	require.NoError(t, err)
	conn3, err := pool.Get(ctx)
	require.NoError(t, err)

	sum := getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(3), getStateCount(sum, "multi-pool", "used"), "used should be 3 after 3 Gets")
	assert.Equal(t, int64(0), getStateCount(sum, "multi-pool", "idle"), "idle should be 0")

	// Return 2 connections
	conn1.Recycle()
	conn2.Recycle()

	sum = getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(1), getStateCount(sum, "multi-pool", "used"), "used should be 1 after returning 2")
	assert.Equal(t, int64(2), getStateCount(sum, "multi-pool", "idle"), "idle should be 2 after returning 2")

	// Return last connection
	conn3.Recycle()

	sum = getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(0), getStateCount(sum, "multi-pool", "used"), "used should be 0 after returning all")
	assert.Equal(t, int64(3), getStateCount(sum, "multi-pool", "idle"), "idle should be 3 after returning all")
}

func TestOTelConnectionCount_WaiterHandoff(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)

	ctx := t.Context()
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	meter := setup.Telemetry.GetMeterProvider().Meter("test")
	connCount, err := NewConnectionCount(meter)
	require.NoError(t, err)

	// Pool with capacity 1 to force waiting
	pool := NewPool[*mockConnection](ctx, &Config{
		Name:            "waiter-pool",
		Capacity:        1,
		MaxIdleCount:    1,
		ConnectionCount: connCount,
	})
	pool.Open(func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}, nil)
	defer pool.Close()

	// Get the only connection
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)

	sum := getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(1), getStateCount(sum, "waiter-pool", "used"), "used should be 1")

	// Start a goroutine that will wait for a connection
	gotConn := make(chan *Pooled[*mockConnection], 1)
	go func() {
		conn, err := pool.Get(ctx)
		if err == nil {
			gotConn <- conn
		}
	}()

	// Give the waiter time to register
	time.Sleep(50 * time.Millisecond)

	// Return the connection - should go directly to the waiter
	conn1.Recycle()

	// Wait for the waiter to get the connection
	var conn2 *Pooled[*mockConnection]
	select {
	case conn2 = <-gotConn:
	case <-time.After(time.Second):
		t.Fatal("waiter never got connection")
	}

	// Connection went directly from used -> used (waiter handoff)
	// So used should still be 1, idle should be 0
	sum = getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(1), getStateCount(sum, "waiter-pool", "used"), "used should be 1 (waiter handoff)")
	assert.Equal(t, int64(0), getStateCount(sum, "waiter-pool", "idle"), "idle should be 0 (direct handoff)")

	conn2.Recycle()

	sum = getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(0), getStateCount(sum, "waiter-pool", "used"), "used should be 0 after final recycle")
	assert.Equal(t, int64(1), getStateCount(sum, "waiter-pool", "idle"), "idle should be 1 after final recycle")
}

func TestOTelConnectionCount_SetCapacityReduction(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)

	ctx := t.Context()
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	meter := setup.Telemetry.GetMeterProvider().Meter("test")
	connCount, err := NewConnectionCount(meter)
	require.NoError(t, err)

	pool := NewPool[*mockConnection](ctx, &Config{
		Name:            "capacity-pool",
		Capacity:        5,
		MaxIdleCount:    5,
		ConnectionCount: connCount,
	})
	pool.Open(func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}, nil)
	defer pool.Close()

	// Create 3 connections and return them to idle
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)
	conn2, err := pool.Get(ctx)
	require.NoError(t, err)
	conn3, err := pool.Get(ctx)
	require.NoError(t, err)

	conn1.Recycle()
	conn2.Recycle()
	conn3.Recycle()

	sum := getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(0), getStateCount(sum, "capacity-pool", "used"), "used should be 0")
	assert.Equal(t, int64(3), getStateCount(sum, "capacity-pool", "idle"), "idle should be 3")

	// Reduce capacity to 1 - should close 2 idle connections
	err = pool.SetCapacity(ctx, 1)
	require.NoError(t, err)

	sum = getConnectionCountMetric(t, setup.MetricReader)
	assert.Equal(t, int64(0), getStateCount(sum, "capacity-pool", "used"), "used should still be 0")
	assert.Equal(t, int64(1), getStateCount(sum, "capacity-pool", "idle"), "idle should be 1 after capacity reduction")
}
