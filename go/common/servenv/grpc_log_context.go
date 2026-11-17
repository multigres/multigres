// Copyright 2026 Supabase, Inc.
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

package servenv

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/multigres/multigres/go/tools/telemetry"
)

// rpcLogContext tags the context with the RPC as its task, plus the caller
// address, so that *Context log calls made while serving an RPC identify it.
func rpcLogContext(ctx context.Context, fullMethod string) context.Context {
	ctx = telemetry.WithTask(ctx, shortMethodName(fullMethod))
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		ctx = telemetry.WithClientAddress(ctx, p.Addr)
	}
	return ctx
}

// shortMethodName drops the leading slash and proto package from a gRPC full
// method name: "/pkg.Service/Method" becomes "Service/Method".
func shortMethodName(fullMethod string) string {
	m := strings.TrimPrefix(fullMethod, "/")
	service, _, ok := strings.Cut(m, "/")
	if !ok {
		return m
	}
	if dot := strings.LastIndexByte(service, '.'); dot >= 0 {
		return m[dot+1:]
	}
	return m
}

func logContextUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return handler(rpcLogContext(ctx, info.FullMethod), req)
}

func logContextStreamInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	wrapped := WrapServerStream(stream)
	wrapped.WrappedContext = rpcLogContext(wrapped.WrappedContext, info.FullMethod)
	return handler(srv, wrapped)
}
