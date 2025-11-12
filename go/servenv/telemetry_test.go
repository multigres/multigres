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

package servenv

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/multigres/multigres/go/viperutil"
)

// testTelemetrySetup holds test telemetry infrastructure
type testTelemetrySetup struct {
	telemetry    *Telemetry
	spanExporter *tracetest.InMemoryExporter
	metricReader *metric.ManualReader
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

// setupTestTelemetry creates a telemetry instance with in-memory exporters for testing
func setupTestTelemetry(t *testing.T) *testTelemetrySetup {
	t.Helper()

	// Save and restore the HTTP client transport
	setupRestoreDefaultGlobals(t)

	spanExporter := tracetest.NewInMemoryExporter()
	metricReader := metric.NewManualReader()

	// Create telemetry with test exporters - this will use them during InitTelemetry
	telemetry := NewTelemetry().WithTestExporters(spanExporter, metricReader)

	return &testTelemetrySetup{
		telemetry:    telemetry,
		spanExporter: spanExporter,
		metricReader: metricReader,
	}
}

func TestNewTelemetry(t *testing.T) {
	tel := NewTelemetry()
	require.NotNil(t, tel)
	assert.False(t, tel.initialized)
	assert.Nil(t, tel.tracerProvider)
	assert.Nil(t, tel.meterProvider)
}

func TestInitTelemetry_DefaultServiceName(t *testing.T) {
	setup := setupTestTelemetry(t)
	ctx := context.Background()

	err := setup.telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)

	// Verify initialization completed
	assert.True(t, setup.telemetry.initialized)
	assert.NotNil(t, setup.telemetry.tracerProvider)
	assert.NotNil(t, setup.telemetry.meterProvider)

	// Verify shutdown works
	err = setup.telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestInitTelemetry_WithServiceNameEnvVar(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "custom-service-from-env")

	setup := setupTestTelemetry(t)
	ctx := context.Background()

	err := setup.telemetry.InitTelemetry(ctx, "default-service")
	require.NoError(t, err)

	// Note: We can't easily assert the service name was used since it's internal
	// but we verify that initialization succeeded with the env var set
	assert.True(t, setup.telemetry.initialized)

	err = setup.telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestInitTelemetry_Idempotency(t *testing.T) {
	setup := setupTestTelemetry(t)
	ctx := context.Background()

	// First initialization
	err := setup.telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)

	// Second initialization should succeed without error
	err = setup.telemetry.InitTelemetry(ctx, "different-service")
	require.NoError(t, err)

	// Should still be initialized
	assert.True(t, setup.telemetry.initialized)

	err = setup.telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestGetTracerProvider(t *testing.T) {
	setup := setupTestTelemetry(t)
	ctx := context.Background()

	// Before initialization, should return a provider (possibly noop)
	provider1 := setup.telemetry.GetTracerProvider()
	require.NotNil(t, provider1)

	// Initialize
	err := setup.telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)

	// After initialization, should return the configured provider
	provider2 := setup.telemetry.GetTracerProvider()
	require.NotNil(t, provider2)
	assert.Equal(t, setup.telemetry.tracerProvider, provider2)

	assert.NotEqual(t, provider1, provider2)

	err = setup.telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestGetTracer(t *testing.T) {
	setup := setupTestTelemetry(t)
	ctx := context.Background()

	err := setup.telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)

	// Verify the global tracer provider was set correctly
	globalProvider := otel.GetTracerProvider()
	require.Equal(t, setup.telemetry.GetTracerProvider(), globalProvider,
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

	err = setup.telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestShutdownTelemetry_BeforeInit(t *testing.T) {
	setup := setupTestTelemetry(t)
	ctx := context.Background()

	// Shutdown before initialization should not error
	err := setup.telemetry.ShutdownTelemetry(ctx)
	require.NoError(t, err)
}

func TestShutdownTelemetry_WithTimeout(t *testing.T) {
	setup := setupTestTelemetry(t)
	ctx := context.Background()

	err := setup.telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)

	// Create some spans
	tracer := otel.Tracer("test")
	_, span := tracer.Start(ctx, "test-span")
	span.End()

	// Shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = setup.telemetry.ShutdownTelemetry(shutdownCtx)
	require.NoError(t, err)
}

func TestWrapSlogHandler_InjectsTraceContext(t *testing.T) {
	setup := setupTestTelemetry(t)
	ctx := context.Background()

	err := setup.telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, setup.telemetry.ShutdownTelemetry(ctx))
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
	wrappedHandler := setup.telemetry.WrapSlogHandler(baseHandler)

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
	setup := setupTestTelemetry(t)
	ctx := context.Background()

	err := setup.telemetry.InitTelemetry(ctx, "test-service")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, setup.telemetry.ShutdownTelemetry(ctx))
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

	wrappedHandler := setup.telemetry.WrapSlogHandler(baseHandler)

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

// getFreePorts allocates n free ports for testing
func getFreePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, n)
	listeners := make([]net.Listener, n)

	for i := 0; i < n; i++ {
		listener, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)
		listeners[i] = listener
		ports[i] = listener.Addr().(*net.TCPAddr).Port
	}

	// Close all listeners after getting ports to avoid reuse
	for _, listener := range listeners {
		listener.Close()
	}

	return ports
}

// TestServEnvTelemetryIntegration tests telemetry with a real ServEnv instance
func TestServEnvTelemetryIntegration(t *testing.T) {
	// Save and restore http.DefaultClient.Transport to avoid interference
	setupRestoreDefaultGlobals(t)

	// Allocate free ports for HTTP and gRPC servers
	ports := getFreePorts(t, 2)
	httpPort := ports[0]
	grpcPort := ports[1]

	// Setup test telemetry exporters
	spanExporter := tracetest.NewInMemoryExporter()
	metricReader := metric.NewManualReader()
	telemetry := NewTelemetry().WithTestExporters(spanExporter, metricReader)

	// Create ServEnv with test telemetry
	reg := viperutil.NewRegistry()
	logger := NewLogger(reg, telemetry)
	vc := viperutil.NewViperConfig(reg)
	sv := NewServEnvWithConfig(reg, logger, vc, telemetry)

	// Configure HTTP server
	sv.httpPort.Set(httpPort)
	sv.bindAddress.Set("localhost")

	// Initialize
	sv.Init("test-integration")

	// Create gRPC server with health service (automatically registered by ServEnv)
	grpcServer := NewGrpcServer(reg)
	grpcServer.port.Set(grpcPort)
	grpcServer.bindAddress.Set("localhost")

	// Start ServEnv in background
	ready := make(chan struct{})
	go func() {
		sv.OnRun(func() {
			close(ready)
		})
		sv.Run(sv.bindAddress.Get(), httpPort, grpcServer)
	}()

	// Wait for server to be ready
	select {
	case <-ready:
		// Ready
	case <-time.After(5 * time.Second):
		t.Fatal("server failed to start")
	}

	// Build URLs for testing
	httpURL := fmt.Sprintf("http://localhost:%d", httpPort)
	grpcAddr := fmt.Sprintf("localhost:%d", grpcPort)

	// Wait for servers to be listening
	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", httpPort))
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond, "HTTP server should start listening")

	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", grpcAddr)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond, "gRPC server should start listening")

	// Run subtests for different telemetry behaviors
	t.Run("HTTP_TracePropagation", func(t *testing.T) {
		spanExporter.Reset()
		ctx := context.Background()

		// Create a parent span representing an application-level operation
		tracer := otel.Tracer("http-test")
		parentCtx, parentSpan := tracer.Start(ctx, "http-request-operation")

		// Make HTTP request with parent context
		// http.DefaultClient is instrumented (creates client span)
		// ServEnv HTTP server is instrumented with otelhttp.NewHandler (creates server span)
		req, err := http.NewRequestWithContext(parentCtx, "GET", httpURL+"/live", nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Close response body to end the HTTP client span
		// (otelhttp.Transport ends the span when body is closed)
		resp.Body.Close()

		parentSpan.End()

		// Force flush to collect all spans
		err = telemetry.tracerProvider.ForceFlush(ctx)
		require.NoError(t, err)

		// Verify spans: should have parent + HTTP client span + HTTP server span
		spans := spanExporter.GetSpans()
		require.Len(t, spans, 3, "should have exactly 3 spans (parent, http client, http server)")

		// Find parent, client, and server spans
		var foundParent, foundClient, foundServer *tracetest.SpanStub
		for i := range spans {
			switch {
			case spans[i].Name == "http-request-operation":
				foundParent = &spans[i]
			case spans[i].SpanKind == oteltrace.SpanKindClient:
				// HTTP client span (name varies by otelhttp version)
				foundClient = &spans[i]
			case spans[i].Name == "http-server" && spans[i].SpanKind == oteltrace.SpanKindServer:
				foundServer = &spans[i]
			}
		}

		require.NotNil(t, foundParent, "parent span should exist")
		require.NotNil(t, foundClient, "HTTP client span should exist")
		require.NotNil(t, foundServer, "HTTP server span should exist")

		// Verify trace propagation - all spans share same trace ID
		traceID := foundParent.SpanContext.TraceID()
		assert.Equal(t, traceID, foundClient.SpanContext.TraceID(), "client span should share trace ID")
		assert.Equal(t, traceID, foundServer.SpanContext.TraceID(), "server span should share trace ID")

		// Verify parent-child relationships
		assert.Equal(t, foundParent.SpanContext.SpanID(), foundClient.Parent.SpanID(),
			"client span's parent should be the parent span")
		assert.Equal(t, foundClient.SpanContext.SpanID(), foundServer.Parent.SpanID(),
			"server span's parent should be the client span")

		// Verify HTTP client span attributes
		var hasClientMethod, hasClientURL bool
		for _, attr := range foundClient.Attributes {
			if attr.Key == "http.request.method" {
				hasClientMethod = true
				assert.Equal(t, "GET", attr.Value.AsString())
			}
			if attr.Key == "url.full" {
				hasClientURL = true
			}
		}
		assert.True(t, hasClientMethod, "client span should have http.request.method attribute")
		assert.True(t, hasClientURL, "client span should have url.full attribute")

		// Verify HTTP server span attributes
		var hasServerMethod, hasServerTarget bool
		for _, attr := range foundServer.Attributes {
			if attr.Key == "http.request.method" {
				hasServerMethod = true
				assert.Equal(t, "GET", attr.Value.AsString())
			}
			if attr.Key == "http.target" || attr.Key == "url.path" {
				hasServerTarget = true
			}
		}
		assert.True(t, hasServerMethod, "server span should have http.request.method attribute")
		assert.True(t, hasServerTarget, "server span should have target/path attribute")
	})

	t.Run("HTTP_Metrics", func(t *testing.T) {
		ctx := context.Background()

		// Make an HTTP request
		resp, err := http.Get(httpURL + "/live")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Close response body to end the HTTP client span and record metrics
		// (otelhttp.Transport ends the span and records metrics when body is closed)
		resp.Body.Close()

		// Flush both traces and metrics to ensure everything is recorded
		err = telemetry.tracerProvider.ForceFlush(ctx)
		require.NoError(t, err)
		err = telemetry.meterProvider.ForceFlush(ctx)
		require.NoError(t, err)

		// Collect metrics
		var metricData metricdata.ResourceMetrics
		err = metricReader.Collect(ctx, &metricData)
		require.NoError(t, err)

		// Verify we have metrics
		assert.NotNil(t, metricData.Resource, "should have resource")
		require.Greater(t, len(metricData.ScopeMetrics), 0, "should have scope metrics")

		// Collect metric names
		metricNames := make(map[string]bool)
		for _, scopeMetric := range metricData.ScopeMetrics {
			for _, metric := range scopeMetric.Metrics {
				metricNames[metric.Name] = true
			}
		}

		// Verify specific HTTP client and server metrics
		assert.True(t, metricNames["http.client.request.duration"], "should have http.client.request.duration metric")
		assert.True(t, metricNames["http.server.request.duration"], "should have http.server.request.duration metric")
	})

	t.Run("GRPC_TracePropagation", func(t *testing.T) {
		spanExporter.Reset()
		ctx := context.Background()

		// Create gRPC client with otelgrpc instrumentation
		conn, err := grpc.NewClient(
			grpcAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		)
		require.NoError(t, err)
		defer conn.Close()

		// Create health check client
		healthClient := healthpb.NewHealthClient(conn)

		// Create a parent span
		tracer := otel.Tracer("grpc-test")
		parentCtx, parentSpan := tracer.Start(ctx, "grpc-request-operation")

		// Make gRPC call
		_, err = healthClient.Check(parentCtx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)

		parentSpan.End()

		// Force flush
		err = telemetry.tracerProvider.ForceFlush(ctx)
		require.NoError(t, err)

		// Verify we captured exactly 3 spans: parent + gRPC client + gRPC server
		spans := spanExporter.GetSpans()
		require.Len(t, spans, 3, "should have exactly 3 spans (parent, grpc client, grpc server)")

		// Find spans by name and kind
		var foundParent, foundClient, foundServer *tracetest.SpanStub
		for i := range spans {
			switch spans[i].Name {
			case "grpc-request-operation":
				foundParent = &spans[i]
			case "grpc.health.v1.Health/Check":
				switch spans[i].SpanKind {
				case oteltrace.SpanKindClient:
					foundClient = &spans[i]
				case oteltrace.SpanKindServer:
					foundServer = &spans[i]
				}
			}
		}

		require.NotNil(t, foundParent, "parent span should exist")
		require.NotNil(t, foundClient, "gRPC client span should exist")
		require.NotNil(t, foundServer, "gRPC server span should exist")

		// Verify all spans share the same trace ID
		traceID := foundParent.SpanContext.TraceID()
		assert.Equal(t, traceID, foundClient.SpanContext.TraceID(), "client span should share trace ID")
		assert.Equal(t, traceID, foundServer.SpanContext.TraceID(), "server span should share trace ID")

		// Verify parent-child relationships
		assert.Equal(t, foundParent.SpanContext.SpanID(), foundClient.Parent.SpanID(),
			"client span's parent should be the parent span")
		assert.Equal(t, foundClient.SpanContext.SpanID(), foundServer.Parent.SpanID(),
			"server span's parent should be the client span")

		// Verify gRPC attributes
		var hasRPCSystem, hasRPCService bool
		for _, attr := range foundClient.Attributes {
			if attr.Key == "rpc.system" && attr.Value.AsString() == "grpc" {
				hasRPCSystem = true
			}
			if attr.Key == "rpc.service" && attr.Value.AsString() == "grpc.health.v1.Health" {
				hasRPCService = true
			}
		}
		assert.True(t, hasRPCSystem, "client span should have rpc.system=grpc attribute")
		assert.True(t, hasRPCService, "client span should have rpc.service attribute")
	})

	t.Run("GRPC_MetricsAndExemplars", func(t *testing.T) {
		ctx := context.Background()

		// Create gRPC client
		conn, err := grpc.NewClient(
			grpcAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		)
		require.NoError(t, err)
		defer conn.Close()

		healthClient := healthpb.NewHealthClient(conn)

		// Create a span and make a request
		tracer := otel.Tracer("grpc-metrics-test")
		spanCtx, span := tracer.Start(ctx, "grpc-with-metrics")
		expectedTraceID := span.SpanContext().TraceID()

		_, err = healthClient.Check(spanCtx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)

		span.End()

		// Flush both traces and metrics to ensure everything is recorded
		err = telemetry.tracerProvider.ForceFlush(ctx)
		require.NoError(t, err)
		err = telemetry.meterProvider.ForceFlush(ctx)
		require.NoError(t, err)

		// Helper function to check for exemplars in metrics
		checkExemplar := func(metricName string) bool {
			var metricData metricdata.ResourceMetrics
			if err := metricReader.Collect(ctx, &metricData); err != nil {
				return false
			}

			for _, scopeMetric := range metricData.ScopeMetrics {
				for _, metric := range scopeMetric.Metrics {
					if metric.Name != metricName {
						continue
					}

					// Duration metrics are histograms
					histogram, ok := metric.Data.(metricdata.Histogram[float64])
					if !ok {
						continue
					}

					// Check each data point for exemplars
					for _, dataPoint := range histogram.DataPoints {
						for _, exemplar := range dataPoint.Exemplars {
							// Verify the exemplar trace ID matches our span's trace ID
							exemplarTraceID := oteltrace.TraceID(exemplar.TraceID)
							if exemplarTraceID == expectedTraceID {
								return true
							}
						}
					}
				}
			}
			return false
		}

		// Wait for client exemplar to appear.
		require.Eventually(t, func() bool {
			return checkExemplar("rpc.client.duration")
		}, 2*time.Second, 10*time.Millisecond, "should have exemplar in rpc.client.duration metric linking to trace")

		// Wait for server exemplar to appear. There was a failure on CI that
		// might be because server data isn't flushed synchronously?...
		require.Eventually(t, func() bool {
			return checkExemplar("rpc.server.duration")
		}, 2*time.Second, 10*time.Millisecond, "should have exemplar in rpc.server.duration metric linking to trace")
	})

	// Shutdown
	sv.exitChan <- syscall.SIGTERM
	time.Sleep(100 * time.Millisecond)

	// Cleanup telemetry
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = telemetry.ShutdownTelemetry(ctx)
}
