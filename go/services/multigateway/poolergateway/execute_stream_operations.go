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

package poolergateway

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/queryrpc"
)

// executionStream adapts the shared response receiver to a typed streaming RPC.
type executionStream[T proto.Message] struct{ *streamResponses }

func (s executionStream[T]) Recv() (T, error) { return receiveTyped[T](s.streamResponses) }

func receiveTyped[T proto.Message](stream *streamResponses) (T, error) {
	var zero T
	message, err := stream.recv()
	if err != nil {
		return zero, err
	}
	response, ok := message.(T)
	if !ok {
		return zero, stream.protocolError("unexpected execute stream response type")
	}
	return response, nil
}

// callUnary keeps the dedicated RPC's contract: exactly one response on success,
// no response on error, and original status details. It consumes completion
// before returning the lease. A request is never replayed after submission.
// Unary callers retain their existing conservative retry classification even
// when a reusable-stream handshake fails before execution.
func callUnary[Request, Response proto.Message](
	ctx context.Context,
	g *grpcQueryService,
	request Request,
	legacy func(context.Context, Request, ...grpc.CallOption) (Response, error),
) (Response, error) {
	var zero Response
	if g.executeStreams == nil {
		return legacy(ctx, request)
	}
	stream, used, err := g.executeStreams.open(ctx, queryrpc.Request(request))
	if err != nil {
		return zero, err
	}
	if !used {
		return legacy(ctx, request)
	}
	defer stream.Release()

	response, err := receiveTyped[Response](stream)
	if errors.Is(err, io.EOF) {
		return zero, stream.protocolError("unary operation completed without a response")
	}
	if err != nil {
		return zero, err
	}
	_, err = stream.recv()
	if errors.Is(err, io.EOF) {
		return response, nil
	}
	if err != nil {
		return zero, err
	}
	return zero, stream.protocolError("unary operation returned multiple responses")
}
