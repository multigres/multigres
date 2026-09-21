// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package queryrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"

	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

type testServer struct {
	pb.UnimplementedMultipoolerServiceServer
	execute func(*pb.StreamExecuteRequest, pb.MultipoolerService_StreamExecuteServer) error
	streams atomic.Int32
	custom  func(pb.MultipoolerService_ExecuteStreamServer) error
}

func (s *testServer) ExecuteStream(stream pb.MultipoolerService_ExecuteStreamServer) error {
	s.streams.Add(1)
	if s.custom != nil {
		return s.custom(stream)
	}
	return Serve(stream, s.execute)
}

func connect(t testing.TB, impl pb.MultipoolerServiceServer) (pb.MultipoolerServiceClient, *Pool) {
	t.Helper()
	l := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterMultipoolerServiceServer(s, impl)
	go func() { _ = s.Serve(l) }()
	cc, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return l.Dial() }))
	require.NoError(t, err)
	c := pb.NewMultipoolerServiceClient(cc)
	p := NewPool(c, cc)
	t.Cleanup(func() { p.Close(); _ = cc.Close(); s.Stop(); _ = l.Close() })
	stopServer = s.Stop
	return c, p
}

// stopServer stops the most recently connected test server.
var stopServer func()

func query(t testing.TB, p *Pool, ctx context.Context, sql string) error {
	t.Helper()
	r, used, err := p.Open(ctx, &pb.StreamExecuteRequest{Query: sql})
	if err != nil {
		return err
	}
	require.True(t, used)
	require.NotNil(t, r)
	defer r.Release()
	for {
		_, err = r.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func TestReuseAndSQLStatusDetails(t *testing.T) {
	diagnostic, err := status.New(codes.InvalidArgument, "SQL diagnostic").WithDetails(wrapperspb.String("original diagnostic"))
	require.NoError(t, err)
	var calls atomic.Int32
	s := &testServer{execute: func(req *pb.StreamExecuteRequest, stream pb.MultipoolerService_StreamExecuteServer) error {
		calls.Add(1)
		if req.Query == "error" {
			return diagnostic.Err()
		}
		return stream.Send(&pb.StreamExecuteResponse{})
	}}
	_, p := connect(t, s)
	for range 4 {
		require.NoError(t, query(t, p, t.Context(), "ok"))
		err = query(t, p, t.Context(), "error")
		require.Equal(t, diagnostic.Proto(), status.Convert(err).Proto())
	}
	require.EqualValues(t, 8, calls.Load())
	require.EqualValues(t, 1, s.streams.Load())
}

func TestPerOperationContext(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	type observed struct {
		baggage string
		trace   trace.TraceID
	}
	seen := make(chan observed, 3)
	s := &testServer{execute: func(_ *pb.StreamExecuteRequest, stream pb.MultipoolerService_StreamExecuteServer) error {
		seen <- observed{baggage.FromContext(stream.Context()).Member("request").Value(), trace.SpanContextFromContext(stream.Context()).TraceID()}
		return nil
	}}
	_, p := connect(t, s)
	for i := byte(1); i <= 3; i++ {
		ctx := t.Context()
		var id trace.TraceID
		if i < 3 {
			member, err := baggage.NewMember("request", strconv.FormatUint(uint64(i), 10))
			require.NoError(t, err)
			bag, err := baggage.New(member)
			require.NoError(t, err)
			ctx = baggage.ContextWithBaggage(ctx, bag)
			id[0] = i
			ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: id, SpanID: trace.SpanID{1}}))
		}
		require.NoError(t, query(t, p, ctx, "ok"))
		got := <-seen
		require.Equal(t, id, got.trace)
		if i < 3 {
			require.Equal(t, strconv.FormatUint(uint64(i), 10), got.baggage)
		} else {
			require.Empty(t, got.baggage)
		}
	}
	require.EqualValues(t, 1, s.streams.Load())
}

func TestCancellationAndAbandonment(t *testing.T) {
	for _, mode := range []string{"cancel", "abandon", "close"} {
		t.Run(mode, func(t *testing.T) {
			started, ended := make(chan struct{}), make(chan struct{})
			s := &testServer{execute: func(req *pb.StreamExecuteRequest, stream pb.MultipoolerService_StreamExecuteServer) error {
				if req.Query == "wait" {
					close(started)
					<-stream.Context().Done()
					close(ended)
					return stream.Context().Err()
				}
				return nil
			}}
			_, p := connect(t, s)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r, used, err := p.Open(ctx, &pb.StreamExecuteRequest{Query: "wait"})
			require.NoError(t, err)
			require.True(t, used)
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not start")
			}
			switch mode {
			case "cancel":
				cancel()
				_, err = r.Recv()
				require.Error(t, err)
				r.Release()
			case "abandon":
				r.Release()
			case "close":
				p.Close()
				r.Release()
			}
			select {
			case <-ended:
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation did not reach executor")
			}
			if mode != "close" {
				require.NoError(t, query(t, p, t.Context(), "ok"))
				require.EqualValues(t, 2, s.streams.Load())
			}
		})
	}
}

func TestFallbackBeforeSendingSQL(t *testing.T) {
	s := &testServer{custom: func(pb.MultipoolerService_ExecuteStreamServer) error {
		return status.Error(codes.Unimplemented, "old server")
	}}
	_, p := connect(t, s)
	for range 3 {
		r, used, err := p.Open(t.Context(), &pb.StreamExecuteRequest{Query: "never sent"})
		require.NoError(t, err)
		require.False(t, used)
		require.Nil(t, r)
	}
	require.EqualValues(t, 1, s.streams.Load())
}

func TestMetadataAndInvalidPropagation(t *testing.T) {
	s := &testServer{execute: func(*pb.StreamExecuteRequest, pb.MultipoolerService_StreamExecuteServer) error { return nil }}
	c, p := connect(t, s)
	ctx := metadata.AppendToOutgoingContext(t.Context(), "custom-credential", "value")
	_, used, err := p.Open(ctx, &pb.StreamExecuteRequest{})
	require.NoError(t, err)
	require.False(t, used)
	require.Zero(t, s.streams.Load())
	stream, err := c.ExecuteStream(metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer test"))
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.Unimplemented, status.Code(err))
	for _, carrier := range []map[string]string{{"unknown": "x"}, {"baggage": strings.Repeat("x", 16*1024)}} {
		require.False(t, validPropagation(carrier))
		ctx, cancel := context.WithCancel(t.Context())
		stream, err := c.ExecuteStream(ctx)
		require.NoError(t, err)
		ready, err := stream.Recv()
		require.NoError(t, err)
		require.True(t, ready.Ready)
		require.NoError(t, stream.Send(&pb.ExecuteStreamRequest{Request: &pb.StreamExecuteRequest{}, Propagation: carrier}))
		_, err = stream.Recv()
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		cancel()
	}
}

func TestTransportLossNotReplayed(t *testing.T) {
	var received atomic.Int32
	s := &testServer{custom: func(stream pb.MultipoolerService_ExecuteStreamServer) error {
		if err := stream.Send(&pb.ExecuteStreamResponse{Ready: true}); err != nil {
			return err
		}
		if _, err := stream.Recv(); err != nil {
			return err
		}
		received.Add(1)
		return status.Error(codes.Unavailable, "lost after execution")
	}}
	_, p := connect(t, s)
	err := query(t, p, t.Context(), "write")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.EqualValues(t, 1, received.Load())
	require.Empty(t, p.idle)
}

func TestConcurrentExclusiveLeases(t *testing.T) {
	s := &testServer{execute: func(req *pb.StreamExecuteRequest, stream pb.MultipoolerService_StreamExecuteServer) error {
		return status.Error(codes.InvalidArgument, req.Query)
	}}
	_, p := connect(t, s)
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			for j := range 10 {
				sql := fmt.Sprintf("%d/%d", i, j)
				err := query(t, p, t.Context(), sql)
				require.Equal(t, sql, status.Convert(err).Message())
			}
		})
	}
	wg.Wait()
	require.LessOrEqual(t, len(p.idle), maxIdleStreams)
	p.Close()
	require.Empty(t, p.idle)
}

func TestRetirementAndLateCancellation(t *testing.T) {
	s := &testServer{execute: func(*pb.StreamExecuteRequest, pb.MultipoolerService_StreamExecuteServer) error { return nil }}
	_, p := connect(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, query(t, p, ctx, "first"))
	cancel()
	require.NoError(t, query(t, p, t.Context(), "second"))
	require.EqualValues(t, 1, s.streams.Load())
	p.mu.Lock()
	p.idle[0].requests = maxStreamRequests - 1
	p.mu.Unlock()
	require.NoError(t, query(t, p, t.Context(), "retire"))
	require.Empty(t, p.idle)
	require.NoError(t, query(t, p, t.Context(), "new"))
	require.EqualValues(t, 2, s.streams.Load())
}

func TestMalformedFrames(t *testing.T) {
	for _, frame := range []*pb.ExecuteStreamResponse{{}, {Ready: true}, {Response: &pb.StreamExecuteResponse{}, Completion: &pb.ExecuteStreamCompletion{}}} {
		s := &testServer{custom: func(stream pb.MultipoolerService_ExecuteStreamServer) error {
			if err := stream.Send(&pb.ExecuteStreamResponse{Ready: true}); err != nil {
				return err
			}
			if _, err := stream.Recv(); err != nil {
				return err
			}
			return stream.Send(frame)
		}}
		_, p := connect(t, s)
		require.Equal(t, codes.Internal, status.Code(query(t, p, t.Context(), "ok")))
		require.Empty(t, p.idle)
	}
}

func TestOperationDeadline(t *testing.T) {
	seen := make(chan bool, 1)
	s := &testServer{execute: func(_ *pb.StreamExecuteRequest, stream pb.MultipoolerService_StreamExecuteServer) error {
		_, ok := stream.Context().Deadline()
		seen <- ok
		return nil
	}}
	_, p := connect(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, query(t, p, ctx, "deadline"))
	require.True(t, <-seen)
	require.NoError(t, query(t, p, t.Context(), "no deadline"))
	require.False(t, <-seen)
}

func TestIdleExpirationAndRepeatedRelease(t *testing.T) {
	s := &testServer{execute: func(*pb.StreamExecuteRequest, pb.MultipoolerService_StreamExecuteServer) error { return nil }}
	_, p := connect(t, s)
	r, used, err := p.Open(t.Context(), &pb.StreamExecuteRequest{})
	require.NoError(t, err)
	require.True(t, used)
	_, err = r.Recv()
	require.ErrorIs(t, err, io.EOF)
	r.Release()
	r.Release()
	p.mu.Lock()
	require.Len(t, p.idle, 1)
	p.idle[0].timer.Reset(0)
	p.mu.Unlock()
	require.Eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.idle) == 0 }, time.Second, time.Millisecond)
	require.NoError(t, query(t, p, t.Context(), "new"))
	require.EqualValues(t, 2, s.streams.Load())
}

func TestTransportEOFCannotReportSuccess(t *testing.T) {
	s := &testServer{custom: func(stream pb.MultipoolerService_ExecuteStreamServer) error {
		if err := stream.Send(&pb.ExecuteStreamResponse{Ready: true}); err != nil {
			return err
		}
		if _, err := stream.Recv(); err != nil {
			return err
		}
		// A server ending the transport successfully is not proof that the
		// operation completed, even if it sent some rows first.
		return stream.Send(&pb.ExecuteStreamResponse{Response: &pb.StreamExecuteResponse{}})
	}}
	_, p := connect(t, s)
	require.Equal(t, codes.Unavailable, status.Code(query(t, p, t.Context(), "write")))
	require.Empty(t, p.idle)
}

func TestPoolMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	s := &testServer{execute: func(req *pb.StreamExecuteRequest, stream pb.MultipoolerService_StreamExecuteServer) error {
		if req.Query == "wait" {
			<-stream.Context().Done()
		}
		return nil
	}}
	_, p := connect(t, s)
	p.m = newPoolMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	for range 3 {
		require.NoError(t, query(t, p, t.Context(), "ok"))
	}
	ctx, cancel := context.WithCancel(t.Context())
	r, used, err := p.Open(ctx, &pb.StreamExecuteRequest{Query: "wait"})
	require.NoError(t, err)
	require.True(t, used)
	cancel()
	_, err = r.Recv()
	require.Error(t, err)
	r.Release()
	_, used, err = p.Open(metadata.AppendToOutgoingContext(t.Context(), "k", "v"), &pb.StreamExecuteRequest{})
	require.NoError(t, err)
	require.False(t, used)
	require.NoError(t, query(t, p, t.Context(), "ok"))
	p.Close()

	sums := map[string]int64{}
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					key := m.Name
					if dp.Attributes.Len() > 0 {
						key += "{" + dp.Attributes.ToSlice()[0].Value.AsString() + "}"
					}
					sums[key] += dp.Value
				}
			}
		}
	}
	require.Equal(t, map[string]int64{
		"mg.gateway.query_stream.operations{new}":             2,
		"mg.gateway.query_stream.operations{reused}":          3,
		"mg.gateway.query_stream.operations{legacy_metadata}": 1,
		"mg.gateway.query_stream.discards{cancelled}":         1,
		"mg.gateway.query_stream.discards{closed}":            1,
		"mg.gateway.query_stream.active":                      0,
		"mg.gateway.query_stream.idle":                        0,
	}, sums)
}

func TestHandshakeHonoursCallerDeadline(t *testing.T) {
	s := &testServer{custom: func(stream pb.MultipoolerService_ExecuteStreamServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	}}
	_, p := connect(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	r, used, err := p.Open(ctx, &pb.StreamExecuteRequest{Query: "never sent"})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.False(t, used)
	require.Nil(t, r)
	require.Less(t, time.Since(start), 5*time.Second)
	require.Empty(t, p.idle)
}

func TestDeadTransportDropsIdleStreams(t *testing.T) {
	s := &testServer{execute: func(*pb.StreamExecuteRequest, pb.MultipoolerService_StreamExecuteServer) error { return nil }}
	_, p := connect(t, s)
	require.NoError(t, query(t, p, t.Context(), "ok"))
	p.mu.Lock()
	require.Len(t, p.idle, 1)
	p.mu.Unlock()
	stopServer()
	// The connectivity watcher discards the stale idle stream, so the next
	// operation opens a fresh stream and fails before sending SQL (used=false)
	// rather than failing a Send on the dead transport (used=true).
	require.Eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.idle) == 0 }, 5*time.Second, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	r, used, err := p.Open(ctx, &pb.StreamExecuteRequest{Query: "write"})
	require.Error(t, err)
	require.False(t, used)
	require.Nil(t, r)
}

func TestResponseOrderAndEmptyResult(t *testing.T) {
	s := &testServer{execute: func(req *pb.StreamExecuteRequest, stream pb.MultipoolerService_StreamExecuteServer) error {
		n, _ := strconv.Atoi(req.Query)
		for i := range n {
			if err := stream.Send(&pb.StreamExecuteResponse{ReservedState: &querypb.ReservedState{ReservedConnectionId: uint64(i + 1)}}); err != nil {
				return err
			}
		}
		return nil
	}}
	_, p := connect(t, s)
	for _, n := range []int{0, 3, 0, 1} {
		r, used, err := p.Open(t.Context(), &pb.StreamExecuteRequest{Query: strconv.Itoa(n)})
		require.NoError(t, err)
		require.True(t, used)
		var got []uint64
		for {
			resp, err := r.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			got = append(got, resp.GetReservedState().GetReservedConnectionId())
		}
		r.Release()
		require.Len(t, got, n)
		for i, id := range got {
			require.EqualValues(t, i+1, id)
		}
	}
	require.EqualValues(t, 1, s.streams.Load())
}
