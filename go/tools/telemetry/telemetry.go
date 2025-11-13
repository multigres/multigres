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

package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"github.com/spf13/cobra"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// TODO(dweitzman): Do we want package-specific tracing services, or is a shared
// one for all of multigres fine?
const tracingServiceName = "github.com/multigres/multigres"

var tracer = otel.Tracer(tracingServiceName)

// Tracer returns a tracer for creating spans named github.com/multigres/multigres
func Tracer() trace.Tracer {
	return tracer
}

// Telemetry holds OpenTelemetry configuration and state
type Telemetry struct {
	// State
	mu             sync.Mutex
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	initialized    bool

	// Test overrides (only used in tests)
	testSpanExporter sdktrace.SpanExporter
	testMetricReader sdkmetric.Reader
}

// NewTelemetry creates a new Telemetry instance
func NewTelemetry() *Telemetry {
	return &Telemetry{}
}

// WithTestExporters configures the telemetry instance to use test exporters instead of autoexport.
// This allows tests to capture and verify telemetry data while still going through normal initialization.
// Must be called before InitTelemetry().
func (t *Telemetry) WithTestExporters(spanExporter sdktrace.SpanExporter, metricReader sdkmetric.Reader) *Telemetry {
	t.testSpanExporter = spanExporter
	t.testMetricReader = metricReader
	return t
}

// InitTelemetry initializes OpenTelemetry providers and exporters
//
// Configuration is done via standard OpenTelemetry environment variables
func (t *Telemetry) InitTelemetry(ctx context.Context, defaultServiceName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.initialized {
		return nil
	}

	// Determine service name (env var > default)
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = defaultServiceName
	}

	// Create resource with service name and standard attributes
	// Note: We don't merge with resource.Default() to avoid schema version conflicts
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	)

	if err := t.initTracing(ctx, res); err != nil {
		return fmt.Errorf("failed to initialize tracing: %w", err)
	}

	if err := t.initMetrics(ctx, res); err != nil {
		return fmt.Errorf("failed to initialize metrics: %w", err)
	}

	// Instrument the default HTTP client for automatic tracing and metrics of outgoing HTTP requests
	// This must happen AFTER both tracing and metrics are initialized so otelhttp can capture
	// the correct TracerProvider and MeterProvider
	http.DefaultClient.Transport = otelhttp.NewTransport(http.DefaultTransport)

	// Set up trace context propagation
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	t.initialized = true

	slog.InfoContext(ctx, "OpenTelemetry initialized", "service", serviceName)

	return nil
}

// initTracing initializes the TracerProvider using autoexport
// The exporter is automatically configured based on OTEL_TRACES_EXPORTER and OTEL_EXPORTER_OTLP_PROTOCOL
func (t *Telemetry) initTracing(ctx context.Context, res *resource.Resource) error {
	var traceExporter sdktrace.SpanExporter
	var err error

	// Use test exporter if provided, otherwise use autoexport
	if t.testSpanExporter != nil {
		traceExporter = t.testSpanExporter
	} else {
		// Default to "none" if OTEL_TRACES_EXPORTER is not explicitly set
		// This prevents unwanted data export when telemetry is not explicitly configured
		if os.Getenv("OTEL_TRACES_EXPORTER") == "" {
			os.Setenv("OTEL_TRACES_EXPORTER", "none")
		}

		traceExporter, err = autoexport.NewSpanExporter(ctx)
		if err != nil {
			return fmt.Errorf("failed to create trace exporter: %w", err)
		}
	}

	// TODO(dweitzman): For "multigres cluster start" we may want different tracing settings for
	// the "multigres" command vs for the long-running services it starts. For example, maybe
	// the multigres command itself should have tracing at 100% but the services should have tracing
	// at a lower sample rate.
	//
	// Also, different commands may want to export their telemetry data in different ways. They can't
	// all use the same port for a Prometheus exporter, for example.

	// Create TracerProvider with batch span processor (or syncer for tests)
	// Batch processing reduces overhead by grouping spans before export
	// Sampler is automatically configured from OTEL_TRACES_SAMPLER (or COMMAND_OTEL_TRACES_SAMPLER)
	var providerOpts []sdktrace.TracerProviderOption
	if t.testSpanExporter != nil {
		// Use synchronous export for tests to avoid timing issues
		providerOpts = []sdktrace.TracerProviderOption{
			sdktrace.WithSyncer(traceExporter),
			sdktrace.WithResource(res),
		}
	} else {
		providerOpts = []sdktrace.TracerProviderOption{
			sdktrace.WithBatcher(traceExporter),
			sdktrace.WithResource(res),
		}
	}
	t.tracerProvider = sdktrace.NewTracerProvider(providerOpts...)

	otel.SetTracerProvider(t.tracerProvider)

	return nil
}

// initMetrics initializes the MeterProvider with dual exporters (autoexport + Prometheus)
func (t *Telemetry) initMetrics(ctx context.Context, res *resource.Resource) error {
	var metricReader sdkmetric.Reader
	var err error

	// Use test metric reader if provided, otherwise use autoexport
	if t.testMetricReader != nil {
		metricReader = t.testMetricReader
	} else {
		// Default to "none" if OTEL_METRICS_EXPORTER is not explicitly set
		// This prevents unwanted data export when telemetry is not explicitly configured
		if os.Getenv("OTEL_METRICS_EXPORTER") == "" {
			os.Setenv("OTEL_METRICS_EXPORTER", "none")
		}

		metricReader, err = autoexport.NewMetricReader(ctx)
		if err != nil {
			return fmt.Errorf("failed to create metric reader: %w", err)
		}
	}

	t.meterProvider = sdkmetric.NewMeterProvider(
		// TODO(dweitzman): Add an additional prometheus exporter that's always at /metrics for debugging
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(metricReader), // Configured via env vars or test reader
	)

	// Set global meter provider
	otel.SetMeterProvider(t.meterProvider)

	return nil
}

// WithEnvTraceparent parses the TRACEPARENT env variable and returns a context within that
// parent
func (t *Telemetry) WithEnvTraceparent(ctx context.Context) context.Context {
	traceparent := os.Getenv("TRACEPARENT")

	if traceparent == "" {
		return ctx
	}

	// Parse W3C Trace Context format: version-trace_id-span_id-flags
	// Example: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
	carrier := propagation.MapCarrier{
		"traceparent": traceparent,
	}

	propagator := otel.GetTextMapPropagator()
	return propagator.Extract(ctx, carrier)
}

func (t *Telemetry) InitForCommand(cmd *cobra.Command, defaultServiceName string, startSpan bool) (trace.Span, error) {
	if err := t.InitTelemetry(cmd.Context(), defaultServiceName); err != nil {
		return nil, fmt.Errorf("failed to initialize OpenTelemetry: %w", err)
	}

	ctx := t.WithEnvTraceparent(cmd.Context())
	var span trace.Span
	if startSpan {
		ctx, span = tracer.Start(ctx, cmd.Use)
	}
	cmd.SetContext(ctx)
	return span, nil
}

// GetTracerProvider returns the configured TracerProvider
func (t *Telemetry) GetTracerProvider() trace.TracerProvider {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.tracerProvider == nil {
		return otel.GetTracerProvider()
	}
	return t.tracerProvider
}

// ShutdownTelemetry gracefully shuts down all telemetry providers
// This ensures all pending spans and metrics are flushed before the service exits
func (t *Telemetry) ShutdownTelemetry(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.initialized {
		return nil
	}

	slog.InfoContext(ctx, "Shutting down OpenTelemetry")

	var errs []error

	// Shutdown tracer provider
	if t.tracerProvider != nil {
		if err := t.tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to shutdown tracer provider: %w", err))
		}
	}

	// Shutdown meter provider
	if t.meterProvider != nil {
		if err := t.meterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to shutdown meter provider: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during telemetry shutdown: %v", errs)
	}

	slog.InfoContext(ctx, "OpenTelemetry shutdown complete")
	return nil
}

// WrapSlogHandler wraps an slog.Handler to inject trace context
func (t *Telemetry) WrapSlogHandler(handler slog.Handler) slog.Handler {
	return &traceHandler{wrapped: handler}
}

// traceHandler wraps an slog.Handler to inject trace_id and span_id from context
type traceHandler struct {
	wrapped slog.Handler
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.wrapped.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		r.AddAttrs(
			slog.String("trace_id", span.SpanContext().TraceID().String()),
			slog.String("span_id", span.SpanContext().SpanID().String()),
		)
	}
	return h.wrapped.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{wrapped: h.wrapped.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{wrapped: h.wrapped.WithGroup(name)}
}
