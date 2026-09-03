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

package servenv

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"

	"github.com/multigres/multigres/go/tools/telemetry"
)

func TestBuildInfoMetricAttributes(t *testing.T) {
	commitTime := time.Date(2026, time.March, 1, 12, 30, 0, 0, time.FixedZone("test", -5*60*60))
	attrs := buildInfoMetricAttributes("multipooler", "0.1.0-SNAPSHOT", buildSnapshot{
		revision:   "deadbeef",
		modified:   true,
		commitTime: commitTime,
		goVersion:  "go1.25.0",
		mainPath:   "github.com/multigres/multigres/go/cmd/multipooler",
	})

	assertAttribute := func(key string, want any) {
		t.Helper()
		value, ok := attrs.Value(attribute.Key(key))
		require.Truef(t, ok, "attribute %q not found", key)
		assert.Equal(t, want, value.AsInterface())
	}
	assertAttribute(string(semconv.ServiceNameKey), "multipooler")
	assertAttribute(string(buildVersionKey), "0.1.0-SNAPSHOT")
	assertAttribute(string(buildRevisionKey), "deadbeef")
	assertAttribute(string(buildModifiedKey), true)
	assertAttribute(string(buildCommitTimeKey), "2026-03-01T17:30:00Z")
	assertAttribute(string(buildGoVersionKey), "go1.25.0")
	assertAttribute(string(buildMainPathKey), "github.com/multigres/multigres/go/cmd/multipooler")
}

func TestBuildInfoMetricExported(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	ctx := context.Background()
	require.NoError(t, setup.Telemetry.InitTelemetry(ctx, "test-service"))
	t.Cleanup(func() {
		require.NoError(t, setup.Telemetry.ShutdownTelemetry(ctx))
	})

	require.NoError(t, registerBuildInfoMetric("test-service"))

	var rm metricdata.ResourceMetrics
	require.NoError(t, setup.MetricReader.Collect(ctx, &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "multigres.build.info" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.Truef(t, ok, "multigres.build.info should be an Int64 gauge, got %T", m.Data)
			require.Len(t, gauge.DataPoints, 1)
			assert.Equal(t, int64(1), gauge.DataPoints[0].Value)
			serviceName, ok := gauge.DataPoints[0].Attributes.Value(semconv.ServiceNameKey)
			require.True(t, ok)
			assert.Equal(t, "test-service", serviceName.AsString())
			version, ok := gauge.DataPoints[0].Attributes.Value(buildVersionKey)
			require.True(t, ok)
			assert.Equal(t, Version(), version.AsString())
			return
		}
	}
	t.Fatal("multigres.build.info metric not found")
}
