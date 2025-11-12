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
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

func TestNewTelemetry(t *testing.T) {
	tel := NewTelemetry()
	require.NotNil(t, tel)
	assert.False(t, tel.initialized)
	assert.Nil(t, tel.tracerProvider)
	assert.Nil(t, tel.meterProvider)
}

func TestInitTelemetry_DefaultServiceName(t *testing.T) {
	setup := SetupTestTelemetry(t)
	ctx := context.Background()

	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)

	// Verify initialization completed
	assert.True(t, setup.Telemetry.initialized)
	assert.NotNil(t, setup.Telemetry.tracerProvider)
	assert.NotNil(t, setup.Telemetry.meterProvider)

	// Verify shutdown works
	err = setup.Telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestInitTelemetry_WithServiceNameEnvVar(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "custom-service-from-env")

	setup := SetupTestTelemetry(t)
	ctx := context.Background()

	err := setup.Telemetry.InitTelemetry(ctx, "default-service")
	require.NoError(t, err)

	// Note: We can't easily assert the service name was used since it's internal
	// but we verify that initialization succeeded with the env var set
	assert.True(t, setup.Telemetry.initialized)

	err = setup.Telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestInitTelemetry_Idempotency(t *testing.T) {
	setup := SetupTestTelemetry(t)
	ctx := context.Background()

	// First initialization
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)

	// Second initialization should succeed without error
	err = setup.Telemetry.InitTelemetry(ctx, "different-service")
	require.NoError(t, err)

	// Should still be initialized
	assert.True(t, setup.Telemetry.initialized)

	err = setup.Telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestGetTracerProvider(t *testing.T) {
	setup := SetupTestTelemetry(t)
	ctx := context.Background()

	// Before initialization, should return a provider (possibly noop)
	provider1 := setup.Telemetry.GetTracerProvider()
	require.NotNil(t, provider1)

	// Initialize
	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)

	// After initialization, should return the configured provider
	provider2 := setup.Telemetry.GetTracerProvider()
	require.NotNil(t, provider2)
	assert.Equal(t, setup.Telemetry.tracerProvider, provider2)

	assert.NotEqual(t, provider1, provider2)

	err = setup.Telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestGetTracer(t *testing.T) {
	setup := SetupTestTelemetry(t)
	ctx := context.Background()

	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)

	// Verify the global tracer provider was set correctly
	globalProvider := otel.GetTracerProvider()
	require.Equal(t, setup.Telemetry.GetTracerProvider(), globalProvider,
		"global tracer provider should match Telemetry's tracer provider")

	// Test using the global tracer (matches production usage where Telemetry object isn't passed around)
	tracer := otel.Tracer("test-tracer")
	require.NotNil(t, tracer)

	// Verify we can create a span with the global tracer
	_, span := tracer.Start(ctx, "test-span")
	require.NotNil(t, span)
	span.End()

	// Also verify GetTracer() method still works for cases where Telemetry is available
	tracerDirect := otel.Tracer("test-tracer-direct")
	require.NotNil(t, tracerDirect)

	err = setup.Telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestShutdownTelemetry_BeforeInit(t *testing.T) {
	setup := SetupTestTelemetry(t)
	ctx := context.Background()

	// Shutdown before initialization should not error
	err := setup.Telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestShutdownTelemetry_WithTimeout(t *testing.T) {
	setup := SetupTestTelemetry(t)
	ctx := context.Background()

	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)

	// Create some spans
	tracer := otel.Tracer("test")
	_, span := tracer.Start(ctx, "test-span")
	span.End()

	// Shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = setup.Telemetry.ShutdownTelemetry(shutdownCtx)
	require.NoError(t, err)
}

func TestWrapSlogHandler_InjectsTraceContext(t *testing.T) {
	setup := SetupTestTelemetry(t)
	ctx := context.Background()

	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, setup.Telemetry.ShutdownTelemetry(ctx))
	})

	// Create a simple in-memory handler to capture log output
	type capturedRecord struct {
		traceID string
		spanID  string
	}
	var captured *capturedRecord

	baseHandler := &testHandler{
		onHandle: func(ctx context.Context, r slog.Record) error {
			captured = &capturedRecord{}
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "trace_id" {
					captured.traceID = a.Value.String()
				}
				if a.Key == "span_id" {
					captured.spanID = a.Value.String()
				}
				return true
			})
			return nil
		},
	}

	// Wrap the handler
	wrappedHandler := setup.Telemetry.WrapSlogHandler(baseHandler)

	// Create a span context
	tracer := otel.Tracer("test")
	spanCtx, span := tracer.Start(ctx, "test-span")
	defer span.End()

	// Log with span context
	logger := slog.New(wrappedHandler)
	logger.InfoContext(spanCtx, "test message")

	// Verify trace_id and span_id were injected
	require.NotNil(t, captured)
	assert.NotEmpty(t, captured.traceID, "trace_id should be injected")
	assert.NotEmpty(t, captured.spanID, "span_id should be injected")

	// Verify the IDs match the span
	spanContext := span.SpanContext()
	assert.Equal(t, spanContext.TraceID().String(), captured.traceID)
	assert.Equal(t, spanContext.SpanID().String(), captured.spanID)
}

func TestWrapSlogHandler_NoSpanContext(t *testing.T) {
	setup := SetupTestTelemetry(t)
	ctx := context.Background()

	err := setup.Telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, setup.Telemetry.ShutdownTelemetry(ctx))
	})

	// Track whether trace_id or span_id were added
	var hasTraceID, hasSpanID bool

	baseHandler := &testHandler{
		onHandle: func(ctx context.Context, r slog.Record) error {
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "trace_id" {
					hasTraceID = true
				}
				if a.Key == "span_id" {
					hasSpanID = true
				}
				return true
			})
			return nil
		},
	}

	wrappedHandler := setup.Telemetry.WrapSlogHandler(baseHandler)

	// Log without span context
	logger := slog.New(wrappedHandler)
	logger.InfoContext(ctx, "test message without span")

	// Verify trace_id and span_id were NOT injected
	assert.False(t, hasTraceID, "trace_id should not be injected without span context")
	assert.False(t, hasSpanID, "span_id should not be injected without span context")
}

// testHandler is a simple slog.Handler for testing
type testHandler struct {
	onHandle func(context.Context, slog.Record) error
}

func (h *testHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (h *testHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.onHandle != nil {
		return h.onHandle(ctx, r)
	}
	return nil
}

func (h *testHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *testHandler) WithGroup(name string) slog.Handler {
	return h
}
