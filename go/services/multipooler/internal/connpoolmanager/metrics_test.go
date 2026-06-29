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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
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

func collectMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q not emitted", name)
	return metricdata.Metrics{}
}

func requireDataPointWithAttrs(t *testing.T, data metricdata.Aggregation, attrs map[string]string) {
	t.Helper()
	for _, set := range attributeSets(data) {
		if hasAttrs(set, attrs) {
			return
		}
	}
	t.Fatalf("no data point with attributes %v in %T", attrs, data)
}

func attributeSets(data metricdata.Aggregation) []attribute.Set {
	switch d := data.(type) {
	case metricdata.Gauge[int64]:
		sets := make([]attribute.Set, 0, len(d.DataPoints))
		for _, dp := range d.DataPoints {
			sets = append(sets, dp.Attributes)
		}
		return sets
	case metricdata.Sum[int64]:
		sets := make([]attribute.Set, 0, len(d.DataPoints))
		for _, dp := range d.DataPoints {
			sets = append(sets, dp.Attributes)
		}
		return sets
	case metricdata.Sum[float64]:
		sets := make([]attribute.Set, 0, len(d.DataPoints))
		for _, dp := range d.DataPoints {
			sets = append(sets, dp.Attributes)
		}
		return sets
	case metricdata.Histogram[float64]:
		sets := make([]attribute.Set, 0, len(d.DataPoints))
		for _, dp := range d.DataPoints {
			sets = append(sets, dp.Attributes)
		}
		return sets
	default:
		return nil
	}
}

func hasAttrs(set attribute.Set, attrs map[string]string) bool {
	for k, want := range attrs {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.AsString() != want {
			return false
		}
	}
	return true
}

func TestMetrics_RegisterManagerCallbacks_PoolerTypeLabel(t *testing.T) {
	m, reader := setupPoolerMetrics(t)
	m.SetPoolerType(clustermetadatapb.PoolerType_PRIMARY)

	stats := ManagerStats{
		Admin: connpool.PoolStats{Borrowed: 2, Idle: 3},
		UserPools: map[string]UserPoolStats{
			"alice": {
				Regular: connpool.PoolStats{
					Active:   4,
					Borrowed: 1,
					Idle:     1,
					Capacity: 8,
				},
				Reserved: reserved.PoolStats{
					Active: 2,
					RegularPool: connpool.PoolStats{
						Active:   2,
						Borrowed: 2,
						Idle:     1,
						Capacity: 4,
					},
				},
				Waiting:  3,
				WaitTime: 2 * time.Second,
				GetCount: 7,
			},
		},
	}

	require.NoError(t, m.RegisterManagerCallbacks(
		func() ManagerStats { return stats },
		func() int { return len(stats.UserPools) },
		func() int64 { return 12 },
		func() bool { return false },
	))

	for _, name := range []string{
		"mg.pooler.up",
		"mg.pooler.pools",
		"mg.pooler.users",
		"mg.pooler.databases",
		"mg.pooler.client.waiting_connections",
		"mg.pooler.reserved.active_connections",
		"mg.pooler.config.max_server_connections",
		"mg.pooler.client.wait_time_total",
		"mg.pooler.queries_pooled_total",
	} {
		metric := collectMetric(t, reader, name)
		requireDataPointWithAttrs(t, metric.Data, map[string]string{"pooler_type": "primary"})
	}

	serverConnections := collectMetric(t, reader, "mg.pooler.server.connections")
	requireDataPointWithAttrs(t, serverConnections.Data, map[string]string{
		"pooler_type": "primary",
		"state":       "active",
	})
	requireDataPointWithAttrs(t, serverConnections.Data, map[string]string{
		"pooler_type": "primary",
		"state":       "idle",
	})

	for _, name := range []string{
		"mg.pooler.pool.capacity",
		"mg.pooler.pool.current_connections",
	} {
		metric := collectMetric(t, reader, name)
		requireDataPointWithAttrs(t, metric.Data, map[string]string{
			"pooler_type": "primary",
			"user":        "alice",
		})
	}
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
	m.RecordCredentialQuery(ctx, time.Millisecond, CredentialQueryErrorLoginDisabled)
	m.RecordCredentialQuery(ctx, time.Millisecond, CredentialQueryErrorPasswordExpired)
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
	require.Equal(t, int64(1), counts[CredentialQueryErrorLoginDisabled])
	require.Equal(t, int64(1), counts[CredentialQueryErrorPasswordExpired])
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
