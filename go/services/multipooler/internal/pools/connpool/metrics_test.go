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

package connpool

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/tools/telemetry"
)

func TestOpenErrorReason(t *testing.T) {
	assert.Equal(t, "timeout", openErrorReason(context.DeadlineExceeded))
	assert.Equal(t, "timeout", openErrorReason(context.Canceled))
	assert.Equal(t, "error", openErrorReason(errors.New("dial refused")))
}

// findMetric returns the named metric from the collected snapshot.
func findMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) (metricdata.Metrics, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, mm := range sm.Metrics {
			if mm.Name == name {
				return mm, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func attrValue(t *testing.T, set attribute.Set, key string) string {
	t.Helper()
	v, _ := set.Value(attribute.Key(key))
	return v.AsString()
}

// TestServerConnMetrics_OpenSuccess verifies a successful connection
// establishment records opened + setup.duration tagged by pool_type.
func TestServerConnMetrics_OpenSuccess(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))
	reader := setup.MetricReader

	scm, err := NewServerConnMetrics(otel.Meter("test"))
	require.NoError(t, err)

	pool := NewPool[*mockConnection](context.Background(), &Config{
		Name:              "regular:alice",
		PoolType:          "regular",
		Capacity:          2,
		MaxIdleCount:      2,
		ServerConnMetrics: scm,
	})
	pool.Open(func(ctx context.Context, poolCtx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}, nil)
	defer pool.Close()

	conn, err := pool.Get(context.Background())
	require.NoError(t, err)
	conn.Recycle()

	opened, ok := findMetric(t, reader, "mg.pooler.server_conn.opened")
	require.True(t, ok, "opened counter not found")
	sum := opened.Data.(metricdata.Sum[int64])
	require.NotEmpty(t, sum.DataPoints)
	assert.Equal(t, "regular", attrValue(t, sum.DataPoints[0].Attributes, "pool_type"))
	assert.GreaterOrEqual(t, sum.DataPoints[0].Value, int64(1))

	dur, ok := findMetric(t, reader, "mg.pooler.server_conn.setup.duration")
	require.True(t, ok, "setup.duration histogram not found")
	hist := dur.Data.(metricdata.Histogram[float64])
	require.NotEmpty(t, hist.DataPoints)
	assert.Equal(t, "regular", attrValue(t, hist.DataPoints[0].Attributes, "pool_type"))
}

// TestServerConnMetrics_OpenError verifies a connector failure records
// open_errors tagged by pool_type and a bounded reason.
func TestServerConnMetrics_OpenError(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))
	reader := setup.MetricReader

	scm, err := NewServerConnMetrics(otel.Meter("test"))
	require.NoError(t, err)

	pool := NewPool[*mockConnection](context.Background(), &Config{
		Name:              "reserved:bob",
		PoolType:          "reserved",
		Capacity:          2,
		MaxIdleCount:      2,
		ServerConnMetrics: scm,
	})
	pool.Open(func(ctx context.Context, poolCtx context.Context) (*mockConnection, error) {
		return nil, context.DeadlineExceeded
	}, nil)
	defer pool.Close()

	_, err = pool.Get(context.Background())
	require.Error(t, err)

	errs, ok := findMetric(t, reader, "mg.pooler.server_conn.open_errors")
	require.True(t, ok, "open_errors counter not found")
	sum := errs.Data.(metricdata.Sum[int64])
	require.NotEmpty(t, sum.DataPoints)
	dp := sum.DataPoints[0]
	assert.Equal(t, "reserved", attrValue(t, dp.Attributes, "pool_type"))
	assert.Equal(t, "timeout", attrValue(t, dp.Attributes, "reason"))
	assert.GreaterOrEqual(t, dp.Value, int64(1))
}
