// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package grpcdiag

import (
	"context"
	"io"
	"net"
	"os"
	"runtime/trace"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/multigres/multigres/go/tools/latencydiag"
)

func TestGRPCCorrelationUnaryStreamAndCancellation(t *testing.T) {
	if path := os.Getenv("MULTIGRES_LATENCY_TEST_TRACE"); path != "" {
		file, err := os.Create(path)
		require.NoError(t, err)
		defer file.Close()
		require.NoError(t, trace.Start(file))
		defer trace.Stop()
	}
	c := capture(t, 1000)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.WaitForHandlers(true), grpc.UnaryInterceptor(UnaryServer), grpc.StreamInterceptor(StreamServer))
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, h)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithChainUnaryInterceptor(UnaryClient), grpc.WithChainStreamInterceptor(StreamClient))
	require.NoError(t, err)
	defer conn.Close()
	root := latencydiag.Message(context.Background(), 77, 9)
	client := healthpb.NewHealthClient(conn)
	_, err = client.Check(root.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(root.Context())
	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	cancel()
	_, err = stream.Recv()
	require.Error(t, err)
	root.End(nil, 0)
	server.Stop() // WaitForHandlers also waits for server interceptors.
	report := c.Stop()
	var linked int
	for _, e := range report.Events {
		if e.RemoteParent != "" {
			linked++
			require.Contains(t, e.Root, "/77/9")
		}
	}
	require.Equal(t, 2, linked)
	// The asynchronous client cancellation callback may still be completing at
	// the stop boundary; this must be reported, never silently discarded.
	require.LessOrEqual(t, report.Unfinished, uint64(1))
}

func TestIncomingRequiresLocalCaptureAndMatchingID(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(metadataKey, "test|abc/1/2|abc:3"))
	_, s := incoming(ctx, "/test")
	require.Nil(t, s)
	capture(t, 10)
	_, s = incoming(ctx, "/test")
	require.NotNil(t, s)
	s.End(io.EOF, 0)
	for _, value := range []string{"other|abc/1/2|abc:3", "test|SQL_SECRET|abc:3", "test|abc/1/2|abc:3|extra"} {
		ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs(metadataKey, value))
		_, s = incoming(ctx, "/test")
		require.Nil(t, s)
	}
}

func capture(t *testing.T, maxEvents int) *latencydiag.Capture {
	t.Helper()
	c, err := latencydiag.Begin(latencydiag.Options{ID: "test", SampleEvery: 1, MaxEvents: maxEvents})
	require.NoError(t, err)
	t.Cleanup(func() { c.Stop() })
	return c
}
