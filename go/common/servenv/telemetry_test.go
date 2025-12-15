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
	"net"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// spanSummary returns a human-readable summary of spans for error messages.
func spanSummary(spans []tracetest.SpanStub) string {
	if len(spans) == 0 {
		return "[]"
	}
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = fmt.Sprintf("%s(%s)", s.Name, s.SpanKind)
	}
	return fmt.Sprintf("%v", names)
}

// getFreePorts allocates n free ports for testing
func getFreePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, n)
	listeners := make([]net.Listener, n)

	for i := range n {
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
	setup := telemetry.SetupTestTelemetry(t)

	// Allocate free ports for HTTP and gRPC servers
	ports := getFreePorts(t, 2)
	httpPort := ports[0]
	grpcPort := ports[1]

	// Create ServEnv with test telemetry
	reg := viperutil.NewRegistry()
	logger := NewLogger(reg, setup.Telemetry)
	vc := viperutil.NewViperConfig(reg)
	sv := NewServEnvWithConfig(reg, logger, vc, setup.Telemetry)

	// Configure HTTP server
	sv.httpPort.Set(httpPort)
	sv.bindAddress.Set("localhost")

	// Initialize
	require.NoError(t, sv.Init("test-integration"))

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
		if err := sv.Run(sv.bindAddress.Get(), httpPort, grpcServer); err != nil {
			t.Errorf("Run() failed: %v", err)
		}
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
		setup.SpanExporter.Reset()
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
		err = setup.ForceFlush(ctx)
		require.NoError(t, err)

		// Verify spans: should have parent + HTTP client span + HTTP server span
		spans := setup.SpanExporter.GetSpans()
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
		err = setup.ForceFlush(ctx)
		require.NoError(t, err)

		// Collect metrics
		var metricData metricdata.ResourceMetrics
		err = setup.MetricReader.Collect(ctx, &metricData)
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
		setup.SpanExporter.Reset()
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

		// Wait for all 3 spans to be exported (parent + gRPC client + gRPC server).
		// The server span may still be in flight after the client call returns,
		// as the gRPC server handler continues async cleanup after sending the response.
		var spans []tracetest.SpanStub
		if !assert.Eventually(t, func() bool {
			err = setup.ForceFlush(ctx)
			require.NoError(t, err)
			spans = setup.SpanExporter.GetSpans()
			return len(spans) == 3
		}, 2*time.Second, 10*time.Millisecond) {
			t.Fatalf("expected 3 spans (parent, grpc client, grpc server), got %d: %v",
				len(spans), spanSummary(spans))
		}

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
		err = setup.ForceFlush(ctx)
		require.NoError(t, err)

		// Helper function to check for exemplars in metrics
		checkExemplar := func(metricName string) bool {
			var metricData metricdata.ResourceMetrics
			if err := setup.MetricReader.Collect(ctx, &metricData); err != nil {
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
	_ = setup.Telemetry.ShutdownTelemetry(ctx)
}
