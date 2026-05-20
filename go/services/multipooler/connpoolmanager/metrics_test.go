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

package connpoolmanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/tools/telemetry"
)

func setupPoolerMetrics(t *testing.T) (*Metrics, *sdkmetric.ManualReader) {
	t.Helper()

	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := NewMetrics()
	require.NoError(t, err)
	return m, setup.MetricReader
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

// TestMetrics_RecordCredentialQuery_DurationAlwaysRecorded ensures the
// histogram fires on every call (success and error) so success-path
// latency is also visible — operators triaging the admin pool need to see
// the full distribution, not only failures.
func TestMetrics_RecordCredentialQuery_DurationAlwaysRecorded(t *testing.T) {
	m, reader := setupPoolerMetrics(t)
	ctx := context.Background()

	m.RecordCredentialQuery(ctx, 5*time.Millisecond, "")
	m.RecordCredentialQuery(ctx, 7*time.Millisecond, CredentialQueryErrorUserNotFound)
	m.RecordCredentialQuery(ctx, 8*time.Millisecond, CredentialQueryErrorPoolAcquireFailed)

	agg := collectAggregation(t, reader, "mg.pooler.auth.credential_query.duration")
	require.NotNil(t, agg, "duration histogram not emitted")
	hist, ok := agg.(metricdata.Histogram[float64])
	require.True(t, ok, "expected Histogram[float64]")
	require.Len(t, hist.DataPoints, 1)
	require.Equal(t, uint64(3), hist.DataPoints[0].Count,
		"all three calls should land in the unlabeled histogram bucket")
}

// TestMetrics_RecordCredentialQuery_ErrorCounter checks that the error
// counter only fires when an error_type is supplied and that each label
// is its own bucket.
func TestMetrics_RecordCredentialQuery_ErrorCounter(t *testing.T) {
	m, reader := setupPoolerMetrics(t)
	ctx := context.Background()

	m.RecordCredentialQuery(ctx, time.Millisecond, "")
	m.RecordCredentialQuery(ctx, time.Millisecond, CredentialQueryErrorUserNotFound)
	m.RecordCredentialQuery(ctx, time.Millisecond, CredentialQueryErrorUserNotFound)
	m.RecordCredentialQuery(ctx, time.Millisecond, CredentialQueryErrorDB)

	agg := collectAggregation(t, reader, "mg.pooler.auth.credential_query.errors")
	require.NotNil(t, agg)
	sum, ok := agg.(metricdata.Sum[int64])
	require.True(t, ok)

	counts := map[string]int64{}
	for _, dp := range sum.DataPoints {
		v, _ := dp.Attributes.Value(attribute.Key("error_type"))
		counts[v.AsString()] = dp.Value
	}
	require.Equal(t, int64(2), counts[CredentialQueryErrorUserNotFound])
	require.Equal(t, int64(1), counts[CredentialQueryErrorDB])
	require.NotContains(t, counts, "", "empty error_type should not produce a counter point")
}

// TestMetrics_RecordCredentialQuery_NilReceiver ensures the gRPC handler
// can call through a nil *Metrics without crashing — metric init may
// have failed at startup, and the auth path must keep working.
func TestMetrics_RecordCredentialQuery_NilReceiver(t *testing.T) {
	var m *Metrics
	require.NotPanics(t, func() {
		m.RecordCredentialQuery(context.Background(), time.Millisecond, CredentialQueryErrorDB)
	})
}
