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

package grpccommon

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

// startHealthServer runs a plain gRPC health server on an in-memory listener
// and returns a dialer for it.
func startHealthServer(t *testing.T) func(context.Context, string) (net.Conn, error) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return func(context.Context, string) (net.Conn, error) { return lis.Dial() }
}

// installSpanRecorder makes an in-memory exporter the global tracer provider
// for the duration of the test. otelgrpc reads the global provider when the
// stats handler is created, so this must run before NewClient.
func installSpanRecorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})
	return exporter
}

func otelHealthCheck(t *testing.T, dial func(context.Context, string) (net.Conn, error)) {
	t.Helper()
	conn, err := NewClient("passthrough:///bufnet",
		WithDialOptions(grpc.WithContextDialer(dial), grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
}

func TestNewClientOTelInstrumentationOnByDefault(t *testing.T) {
	SetOTelInstrumentationEnabled(nil)
	exporter := installSpanRecorder(t)

	otelHealthCheck(t, startHealthServer(t))

	require.Eventually(t, func() bool { return len(exporter.GetSpans()) > 0 }, 2*time.Second, 10*time.Millisecond,
		"a client span is recorded for the RPC when instrumentation is on")
	require.Equal(t, "grpc.health.v1.Health/Check", exporter.GetSpans()[0].Name)
}

func TestNewClientOTelInstrumentationOff(t *testing.T) {
	SetOTelInstrumentationEnabled(func() bool { return false })
	t.Cleanup(func() { SetOTelInstrumentationEnabled(nil) })
	exporter := installSpanRecorder(t)

	otelHealthCheck(t, startHealthServer(t))

	require.Empty(t, exporter.GetSpans(), "no stats handler, so no span for the RPC")
}

func TestSetOTelInstrumentationEnabledIsEvaluatedLazily(t *testing.T) {
	enabled := true
	SetOTelInstrumentationEnabled(func() bool { return enabled })
	t.Cleanup(func() { SetOTelInstrumentationEnabled(nil) })

	require.True(t, OTelInstrumentationEnabled())
	enabled = false
	require.False(t, OTelInstrumentationEnabled(), "the decision reflects configuration parsed after registration")

	SetOTelInstrumentationEnabled(nil)
	require.True(t, OTelInstrumentationEnabled(), "nil restores the default")
}
