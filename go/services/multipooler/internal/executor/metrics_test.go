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

package executor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// setupQueryStats installs an in-memory metric reader and returns a fresh
// *queryStats wired to it.
func setupQueryStats(t *testing.T) (*queryStats, *sdkmetric.ManualReader) {
	t.Helper()

	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	return newQueryStats(), setup.MetricReader
}

// findMetric returns the first metric named `name` in the collected snapshot.
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

func TestRecordQuery_Success(t *testing.T) {
	s, reader := setupQueryStats(t)

	s.recordQuery(t.Context(), poolTypeRegular, 5*time.Millisecond, 42, nil)

	dur := findMetric(t, reader, "mg.pooler.query.duration")
	durHist, ok := dur.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, durHist.DataPoints, 1)
	dp := durHist.DataPoints[0]
	assert.Equal(t, poolTypeRegular, attrValue(t, dp.Attributes, "pool_type"))
	assert.Equal(t, "ok", attrValue(t, dp.Attributes, "status"))

	rows := findMetric(t, reader, "mg.pooler.query.rows")
	rowsHist, ok := rows.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, rowsHist.DataPoints, 1)
	assert.Equal(t, float64(42), rowsHist.DataPoints[0].Sum)

	// No error metric should be emitted on success.
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, mm := range sm.Metrics {
			assert.NotEqual(t, "mg.pooler.query.errors", mm.Name, "no error metric expected on success")
		}
	}
}

func TestRecordQuery_Error(t *testing.T) {
	s, reader := setupQueryStats(t)

	// A real PostgreSQL SQLSTATE → classified as a backend error.
	err := mterrors.NewParseError("bad syntax")
	s.recordQuery(t.Context(), poolTypeReserved, 2*time.Millisecond, 0, err)

	dur := findMetric(t, reader, "mg.pooler.query.duration")
	durHist := dur.Data.(metricdata.Histogram[float64])
	require.Len(t, durHist.DataPoints, 1)
	assert.Equal(t, "error", attrValue(t, durHist.DataPoints[0].Attributes, "status"))
	assert.Equal(t, poolTypeReserved, attrValue(t, durHist.DataPoints[0].Attributes, "pool_type"))

	errs := findMetric(t, reader, "mg.pooler.query.errors")
	errSum, ok := errs.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, errSum.DataPoints, 1)
	edp := errSum.DataPoints[0]
	assert.Equal(t, int64(1), edp.Value)
	assert.Equal(t, mterrors.PgSSSyntaxError, attrValue(t, edp.Attributes, "sqlstate"))
	assert.Equal(t, "backend", attrValue(t, edp.Attributes, "error_source"))
	assert.Equal(t, poolTypeReserved, attrValue(t, edp.Attributes, "pool_type"))
}

func TestAcquireOutcome(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"success", nil, "acquired"},
		{"pool timeout", connpool.ErrTimeout, "timeout"},
		{"pool ctx timeout", connpool.ErrCtxTimeout, "timeout"},
		{"context deadline", context.DeadlineExceeded, "timeout"},
		{"context canceled", context.Canceled, "timeout"},
		{"%w-wrapped pool timeout", fmt.Errorf("get conn: %w", connpool.ErrTimeout), "timeout"},
		{"other error", errors.New("boom"), "error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, acquireOutcome(tt.err))
		})
	}
}

func TestRecordPoolAcquire(t *testing.T) {
	s, reader := setupQueryStats(t)

	s.recordPoolAcquire(t.Context(), poolTypeRegular, 3*time.Millisecond, nil)
	s.recordPoolAcquire(t.Context(), poolTypeReserved, time.Millisecond, connpool.ErrTimeout)

	m := findMetric(t, reader, "mg.pooler.query.pool_acquire.duration")
	hist, ok := m.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, hist.DataPoints, 2)

	got := map[string]string{} // pool_type -> outcome
	for _, dp := range hist.DataPoints {
		got[attrValue(t, dp.Attributes, "pool_type")] = attrValue(t, dp.Attributes, "outcome")
	}
	assert.Equal(t, "acquired", got[poolTypeRegular])
	assert.Equal(t, "timeout", got[poolTypeReserved])
}
