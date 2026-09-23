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

// Package queryrpc holds the wire contract of the reusable ExecuteStream SQL
// transport shared by multigateway (client pool, see poolergateway) and
// multipooler (server loop below): frame validation, propagation rules and the
// server-side adaptation onto the ordinary StreamExecute handler. It does not
// own database sessions or reserved connections.
package queryrpc

import (
	"context"
	"errors"
	"io"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Propagation captures only the standard tracing/baggage headers. A custom
// propagator with other fields requires the legacy RPC, not silent data loss.
func Propagation(ctx context.Context) (map[string]string, bool) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier, ValidPropagation(carrier)
}

// ValidPropagation reports whether a propagation carrier holds only the
// standard trace/baggage keys within the size the server accepts.
func ValidPropagation(carrier map[string]string) bool {
	size := 0
	for k, v := range carrier {
		switch k {
		case "traceparent", "tracestate", "baggage":
		default:
			return false
		}
		size += len(k) + len(v)
	}
	return size <= 16*1024
}

// Serve reuses the ordinary StreamExecute handler, including admission,
// reservation validation and PostgreSQL diagnostics, once per operation.
func Serve(stream pb.MultipoolerService_ExecuteStreamServer, execute func(*pb.StreamExecuteRequest, pb.MultipoolerService_StreamExecuteServer) error) error {
	// Per-call authorization headers must be revalidated by the ordinary RPC
	// interceptor. Never silently widen their lifetime through transport reuse.
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok && len(md.Get("authorization")) != 0 {
		return status.Error(codes.Unimplemented, "per-call authorization requires StreamExecute")
	}
	if err := stream.Send(&pb.ExecuteStreamResponse{Ready: true}); err != nil {
		return err
	}
	tracer := otel.Tracer("github.com/multigres/multigres/go/common/queryrpc")
	baseCtx := baggage.ContextWithoutBaggage(stream.Context())
	baseCtx = trace.ContextWithSpanContext(baseCtx, trace.SpanContext{})
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if req.GetRequest() == nil || req.TimeoutNanos < 0 || !ValidPropagation(req.GetPropagation()) {
			return status.Error(codes.InvalidArgument, "invalid execute stream request")
		}
		// The enclosing RPC has no SQL session identity. Reconstruct each
		// operation's telemetry context independently; never inherit a prior one.
		ctx := otel.GetTextMapPropagator().Extract(baseCtx, propagation.MapCarrier(req.Propagation))
		ctx, span := tracer.Start(ctx, "StreamExecute", trace.WithSpanKind(trace.SpanKindServer))
		var cancel context.CancelFunc
		if req.TimeoutNanos > 0 {
			ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutNanos))
		}
		adapter := &responseStream{ServerStream: stream, ctx: ctx, stream: stream}
		err = execute(req.Request, adapter)
		if cancel != nil {
			cancel()
		}
		span.End()
		if adapter.sendErr != nil {
			return adapter.sendErr
		}
		// status.Convert(nil) yields a nil *Status, so build the OK frame
		// explicitly: success must be an explicit completion too.
		completion := status.Convert(err).Proto()
		if completion == nil {
			completion = &statuspb.Status{Code: int32(codes.OK)}
		}
		if err := stream.Send(&pb.ExecuteStreamResponse{Completion: completion}); err != nil {
			return err
		}
	}
}

type responseStream struct {
	grpc.ServerStream
	ctx     context.Context
	stream  pb.MultipoolerService_ExecuteStreamServer
	sendErr error
}

func (s *responseStream) Context() context.Context { return s.ctx }
func (s *responseStream) Send(r *pb.StreamExecuteResponse) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sendErr = s.stream.Send(&pb.ExecuteStreamResponse{Response: r})
	return s.sendErr
}
