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

package queryrpc

import (
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// copyStream presents a single COPY conversation to the existing bidi handler.
// Only this adapter reads continuation frames; Serve resumes reading operations
// after drain has consumed the explicit input boundary.
type copyStream struct {
	*operationStream
	initial       *pb.CopyBidiExecuteRequest
	ended         bool
	recvErr       error
	direction     pb.CopyBidiExecuteRequest_Direction
	ready         bool
	inputFinished bool
}

func (s *copyStream) Send(response *pb.CopyBidiExecuteResponse) error {
	if response.Phase == pb.CopyBidiExecuteResponse_READY {
		s.ready = true
	}
	return s.send(&pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_CopyBidiExecute{CopyBidiExecute: response}})
}

func (s *copyStream) Recv() (*pb.CopyBidiExecuteRequest, error) {
	if s.initial != nil {
		req := s.initial
		s.initial = nil
		return req, nil
	}
	if s.ended {
		return nil, io.EOF
	}
	if s.recvErr != nil {
		return nil, s.recvErr
	}
	frame, err := s.stream.Recv()
	if err != nil {
		s.recvErr = err
		return nil, err
	}
	input := frame.GetCopyInput()
	if input == nil || frame.TimeoutNanos != 0 || len(frame.Propagation) != 0 {
		s.recvErr = status.Error(codes.InvalidArgument, "expected COPY input frame")
		return nil, s.recvErr
	}
	if input.GetEnd() {
		s.ended = true
		return nil, io.EOF
	}
	req := input.GetRequest()
	if req == nil || (req.Phase != pb.CopyBidiExecuteRequest_DATA && req.Phase != pb.CopyBidiExecuteRequest_DONE && req.Phase != pb.CopyBidiExecuteRequest_FAIL) {
		s.recvErr = status.Error(codes.InvalidArgument, "invalid COPY input")
		return nil, s.recvErr
	}
	if req.Phase == pb.CopyBidiExecuteRequest_DONE || req.Phase == pb.CopyBidiExecuteRequest_FAIL {
		s.inputFinished = true
	}
	return req, nil
}

// A COPY FROM client can be busy sending and may not read another response
// until DONE. On an early failure after READY, close the transport to unblock
// its sender, just as the dedicated RPC does. Draining indefinitely would hide
// the failure behind arbitrarily much client input. Completion is sent first
// so a receiver can still recover the diagnostic and reservation state.
func (s *copyStream) reusable() bool {
	return !s.ready || s.direction == pb.CopyBidiExecuteRequest_TO_STDOUT || s.inputFinished || s.ended
}

func (s *copyStream) drain() error {
	for !s.ended {
		if _, err := s.Recv(); err != nil {
			if s.ended && errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
	return nil
}
