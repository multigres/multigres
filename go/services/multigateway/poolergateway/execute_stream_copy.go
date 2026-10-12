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
	"sync"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/queryrpc"
	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// openCopy submits INITIATE exactly once, selecting the dedicated RPC only
// before submission when reuse is disabled or the peer lacks COPY support.
func (g *grpcQueryService) openCopy(ctx context.Context, req *pb.CopyBidiExecuteRequest) (pb.MultipoolerService_CopyBidiExecuteClient, error) {
	if g.executeStreams != nil {
		responses, used, err := g.executeStreams.open(ctx, queryrpc.Request(req))
		if err != nil {
			return nil, mterrors.Wrapf(mterrors.FromGRPC(err), "failed to start reusable COPY")
		}
		if used {
			return &reusableCopyStream{ClientStream: responses.lease.stream, responses: responses}, nil
		}
	}
	stream, err := g.client.CopyBidiExecute(ctx)
	if err != nil {
		return nil, mterrors.Wrapf(mterrors.FromGRPC(err), "failed to start bidirectional execute stream")
	}
	if err := stream.Send(req); err != nil {
		_ = stream.CloseSend()
		_, _ = stream.Recv()
		return nil, mterrors.Wrapf(mterrors.FromGRPC(err), "failed to send INITIATE")
	}
	return stream, nil
}

// reusableCopyStream holds an exclusive lease across READY, DATA and the final
// RESULT/ERROR. CloseSend ends only this operation's input, not the transport.
// One sender and one receiver may run concurrently, as with a gRPC bidi stream.
type reusableCopyStream struct {
	grpc.ClientStream
	responses *streamResponses
	sendMu    sync.Mutex
	closed    bool
}

func (s *reusableCopyStream) Send(req *pb.CopyBidiExecuteRequest) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.closed {
		return io.EOF
	}
	return s.responses.lease.stream.Send(&pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_CopyInput{
		CopyInput: &pb.CopyStreamInput{Input: &pb.CopyStreamInput_Request{Request: req}},
	}})
}

func (s *reusableCopyStream) CloseSend() error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.responses.lease.stream.Send(&pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_CopyInput{
		CopyInput: &pb.CopyStreamInput{Input: &pb.CopyStreamInput_End{End: true}},
	}})
}

func (s *reusableCopyStream) Recv() (*pb.CopyBidiExecuteResponse, error) {
	if s.responses.released {
		return nil, io.EOF
	}
	response, err := receiveTyped[*pb.CopyBidiExecuteResponse](s.responses)
	if err != nil {
		// Admission failures can complete without any COPY response. End input
		// even in this case so the server can advance to its next operation.
		if s.responses.done {
			if closeErr := s.CloseSend(); closeErr != nil {
				s.responses.invalid = true
			}
		}
		if errors.Is(err, io.EOF) {
			err = s.responses.protocolError("COPY completed without a terminal response")
		}
		s.Release()
		return nil, err
	}
	switch response.Phase {
	case pb.CopyBidiExecuteResponse_READY, pb.CopyBidiExecuteResponse_DATA:
		return response, nil
	case pb.CopyBidiExecuteResponse_RESULT, pb.CopyBidiExecuteResponse_ERROR:
		defer s.Release()
		closeErr := s.CloseSend()
		_, err := s.responses.recv()
		// An early COPY FROM failure closes the server transport to stop
		// further input. Its completion still acknowledges handler cleanup;
		// preserve its payload while discarding the closed transport.
		if closeErr != nil {
			s.responses.invalid = true
		}
		if !s.responses.done {
			if err == nil {
				err = s.responses.protocolError("COPY returned data after its terminal response")
			}
			return nil, err
		}
		// ERROR carries the authoritative reservation state, diagnostics and
		// notices. A handler error in completion must not hide that payload.
		if response.Phase == pb.CopyBidiExecuteResponse_ERROR {
			return response, nil
		}
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
		return response, nil
	default:
		err := s.responses.protocolError("invalid COPY response phase")
		s.Release()
		return nil, err
	}
}

func (s *reusableCopyStream) Release() {
	// A sender may be blocked on flow control. Cancel an abandoned operation
	// before taking sendMu so that sender can exit and cannot delay cleanup.
	if !s.responses.done || s.responses.invalid {
		s.responses.lease.cancel()
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.closed = true
	s.responses.Release()
}

// Dedicated RPCs retain their existing half-close lifecycle. Reusable leases
// must additionally be discarded on every path that abandons an operation.
func releaseCopyStream(stream pb.MultipoolerService_CopyBidiExecuteClient) {
	if reusable, ok := stream.(*reusableCopyStream); ok {
		reusable.Release()
	}
}
