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
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/semconv/v1.37.0/dbconv"
)

func TestBoundConnectionCountPreservesLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { require.NoError(t, provider.Shutdown(t.Context())) }()
	counter, err := NewConnectionCount(provider.Meter("test"))
	require.NoError(t, err)
	a, b := counter.bind("a"), counter.bind("b")
	a.Add(t.Context(), 3, dbconv.ClientConnectionStateIdle)
	a.Add(t.Context(), -1, dbconv.ClientConnectionStateIdle)
	a.Add(t.Context(), 1, dbconv.ClientConnectionStateUsed)
	b.Add(t.Context(), 4, dbconv.ClientConnectionStateIdle)
	m, ok := findMetric(t, reader, "db.client.connection.count")
	require.True(t, ok)
	values := map[string]int64{}
	for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
		values[attrValue(t, p.Attributes, attrKeyPoolName)+"/"+attrValue(t, p.Attributes, attrKeyState)] = p.Value
	}
	require.Equal(t, map[string]int64{"a/idle": 2, "a/used": 1, "b/idle": 4}, values)
}

func BenchmarkConnectionCountAttributes(b *testing.B) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { require.NoError(b, provider.Shutdown(b.Context())) }()
	counter, err := NewConnectionCount(provider.Meter("benchmark"))
	require.NoError(b, err)
	b.Run("dynamic", func(b *testing.B) {
		// The pre-bind path: a fresh attribute set per update.
		b.ReportAllocs()
		for b.Loop() {
			counter.counter.Add(b.Context(), 1, metric.WithAttributes(
				attribute.String(attrKeyPoolName, "pool"),
				attribute.String(attrKeyState, string(dbconv.ClientConnectionStateIdle)),
			))
		}
	})
	b.Run("bound", func(b *testing.B) {
		bound := counter.bind("pool")
		b.ReportAllocs()
		for b.Loop() {
			bound.Add(b.Context(), 1, dbconv.ClientConnectionStateIdle)
		}
	})
}
