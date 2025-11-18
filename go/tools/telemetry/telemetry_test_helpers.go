// Copyright 2025 Supabase, Inc.
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

package telemetry

import (
	"context"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// testTelemetrySetup holds test telemetry infrastructure
type testTelemetrySetup struct {
	Telemetry    *Telemetry
	SpanExporter *tracetest.InMemoryExporter
	MetricReader *metric.ManualReader
}

func (t *testTelemetrySetup) ForceFlush(ctx context.Context) error {
	err := t.Telemetry.tracerProvider.ForceFlush(ctx)
	if err != nil {
		return err
	}
	return t.Telemetry.meterProvider.ForceFlush(ctx)
}

// setupRestoreDefaultGlobals saves http.DefaultClient.Transport and otel.GetTracerProvider
// to restore after the test and subtests complete.
func setupRestoreDefaultGlobals(t *testing.T) {
	t.Helper()
	originalTransport := http.DefaultClient.Transport
	originalTracerProvider := otel.GetTracerProvider()
	originalMeterProvider := otel.GetMeterProvider()
	originalTextMapPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		http.DefaultClient.Transport = originalTransport
		otel.SetTracerProvider(originalTracerProvider)
		otel.SetMeterProvider(originalMeterProvider)
		otel.SetTextMapPropagator(originalTextMapPropagator)
	})
}

// SetupTestTelemetry creates a telemetry instance with in-memory exporters for testing
func SetupTestTelemetry(t *testing.T) *testTelemetrySetup {
	t.Helper()

	// Save and restore the HTTP client transport
	setupRestoreDefaultGlobals(t)

	spanExporter := tracetest.NewInMemoryExporter()
	metricReader := metric.NewManualReader()

	// Create telemetry with test exporters - this will use them during InitTelemetry
	telemetry := NewTelemetry().WithTestExporters(spanExporter, metricReader)

	return &testTelemetrySetup{
		Telemetry:    telemetry,
		SpanExporter: spanExporter,
		MetricReader: metricReader,
	}
}
