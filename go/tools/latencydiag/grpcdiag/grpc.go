// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package grpcdiag adapts bounded latency captures to dedicated gRPC calls.
package grpcdiag

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/multigres/multigres/go/tools/latencydiag"
)

const metadataKey = "multigres-latency-diagnostic"

func outgoing(ctx context.Context, phase string, async bool) (context.Context, *latencydiag.Span) {
	begin := latencydiag.Start
	if async {
		begin = latencydiag.StartAsync
	}
	ctx, s := begin(ctx, phase)
	if s == nil {
		return ctx, nil
	}
	// Copy and replace: retries/nested RPCs must not accumulate stale headers.
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set(metadataKey, latencydiag.ExportReference(ctx))
	return metadata.NewOutgoingContext(ctx, md), s
}

func incoming(ctx context.Context, method string) (context.Context, *latencydiag.Span) {
	if !latencydiag.Active() {
		return ctx, nil
	}
	values := metadata.ValueFromIncomingContext(ctx, metadataKey)
	if len(values) != 1 || len(values[0]) > 256 {
		return ctx, nil
	}
	return latencydiag.ImportReference(ctx, values[0], "rpc.server"+method)
}

func UnaryClient(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if !latencydiag.Selected(ctx) {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	ctx, s := outgoing(ctx, "rpc.client"+method, false)
	err := invoker(ctx, method, req, reply, cc, opts...)
	s.End(err, 0)
	return err
}

func UnaryServer(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx, s := incoming(ctx, info.FullMethod)
	response, err := handler(ctx, req)
	s.End(err, 0)
	return response, err
}

func StreamClient(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if !latencydiag.Selected(ctx) {
		return streamer(ctx, desc, cc, method, opts...)
	}
	ctx, s := outgoing(ctx, "rpc.client"+method, true)
	stream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		s.End(err, 0)
		return stream, err
	}
	if s == nil {
		return stream, nil
	}
	wrapped := &clientStream{ClientStream: stream, span: s}
	// Stop the root on cancellation even if the consumer never calls RecvMsg.
	wrapped.stop = context.AfterFunc(ctx, func() { s.End(ctx.Err(), 0) })
	return wrapped, nil
}

type clientStream struct {
	grpc.ClientStream
	span *latencydiag.Span
	stop func() bool
}

func (s *clientStream) RecvMsg(m any) error {
	_, wait := latencydiag.Start(s.span.Context(), "rpc.receive")
	err := s.ClientStream.RecvMsg(m)
	wait.End(nonEOF(err), 0)
	if err != nil {
		s.stop()
		s.span.End(nonEOF(err), 0)
	}
	return err
}

func (s *clientStream) SendMsg(m any) error {
	_, send := latencydiag.Start(s.span.Context(), "rpc.send")
	err := s.ClientStream.SendMsg(m)
	send.End(err, 0)
	if err != nil {
		s.stop()
		s.span.End(err, 0)
	}
	return err
}

func nonEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

type serverStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStream) Context() context.Context { return s.ctx }
func (s *serverStream) SendMsg(m any) error {
	_, span := latencydiag.Start(s.ctx, "rpc.send")
	err := s.ServerStream.SendMsg(m)
	span.End(err, 0)
	return err
}

func (s *serverStream) RecvMsg(m any) error {
	_, span := latencydiag.Start(s.ctx, "rpc.receive")
	err := s.ServerStream.RecvMsg(m)
	span.End(nonEOF(err), 0)
	return err
}

func StreamServer(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, s := incoming(stream.Context(), info.FullMethod)
	if s == nil {
		return handler(srv, stream)
	}
	err := handler(srv, &serverStream{ServerStream: stream, ctx: ctx})
	s.End(err, 0)
	return err
}
