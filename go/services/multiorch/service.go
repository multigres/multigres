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

package multiorch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
)

// MultiOrchServer implements the MultiOrchService gRPC API
type MultiOrchServer struct {
	multiorchpb.UnimplementedMultiOrchServiceServer
	mo *MultiOrch
}

// NewMultiOrchServer creates a new MultiOrchServer instance
func NewMultiOrchServer(mo *MultiOrch) *MultiOrchServer {
	return &MultiOrchServer{mo: mo}
}

// DisableRecovery stops the recovery loop and waits for in-flight actions to complete
func (s *MultiOrchServer) DisableRecovery(ctx context.Context, req *multiorchpb.DisableRecoveryRequest) (*multiorchpb.DisableRecoveryResponse, error) {
	if s.mo.recoveryEngine == nil {
		return &multiorchpb.DisableRecoveryResponse{
			Success: false,
			Message: "recovery engine not initialized",
		}, nil
	}

	s.mo.recoveryEngine.DisableRecovery()
	return &multiorchpb.DisableRecoveryResponse{
		Success: true,
		Message: "recovery disabled",
	}, nil
}

// EnableRecovery resumes the recovery loop
func (s *MultiOrchServer) EnableRecovery(ctx context.Context, req *multiorchpb.EnableRecoveryRequest) (*multiorchpb.EnableRecoveryResponse, error) {
	if s.mo.recoveryEngine == nil {
		return &multiorchpb.EnableRecoveryResponse{
			Success: false,
			Message: "recovery engine not initialized",
		}, nil
	}

	s.mo.recoveryEngine.EnableRecovery()
	return &multiorchpb.EnableRecoveryResponse{
		Success: true,
		Message: "recovery enabled",
	}, nil
}

// GetRecoveryStatus returns whether recovery is currently enabled or disabled
func (s *MultiOrchServer) GetRecoveryStatus(ctx context.Context, req *multiorchpb.GetRecoveryStatusRequest) (*multiorchpb.GetRecoveryStatusResponse, error) {
	if s.mo.recoveryEngine == nil {
		return &multiorchpb.GetRecoveryStatusResponse{
			Enabled: false,
		}, nil
	}

	return &multiorchpb.GetRecoveryStatusResponse{
		Enabled: s.mo.recoveryEngine.IsRecoveryEnabled(),
	}, nil
}

// TriggerRecoveryNow immediately executes recovery cycles until no problems remain
// or the request context times out. Returns problem codes that remain unresolved.
func (s *MultiOrchServer) TriggerRecoveryNow(ctx context.Context, req *multiorchpb.TriggerRecoveryNowRequest) (*multiorchpb.TriggerRecoveryNowResponse, error) {
	if s.mo.recoveryEngine == nil {
		return nil, status.Error(codes.Unavailable, "recovery engine not initialized")
	}

	// Use request deadline minus 200ms margin for network/framework overhead
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		// No deadline specified - use reasonable default (30s)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	} else {
		// Subtract 200ms from deadline to allow time for response
		timeout := time.Until(deadline) - 200*time.Millisecond
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
	}

	remainingProblems, err := s.mo.recoveryEngine.TriggerRecoveryNow(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return nil, status.Error(codes.Internal, fmt.Sprintf("recovery trigger failed: %v", err))
	}

	// Convert DetectedProblemData to problem codes
	problemCodes := make([]string, 0, len(remainingProblems))
	for _, p := range remainingProblems {
		problemCodes = append(problemCodes, p.AnalysisType)
	}

	return &multiorchpb.TriggerRecoveryNowResponse{
		RemainingProblemCodes: problemCodes,
	}, nil
}

// RegisterWithGRPCServer registers the service with a gRPC server
func (s *MultiOrchServer) RegisterWithGRPCServer(server *grpc.Server) {
	multiorchpb.RegisterMultiOrchServiceServer(server, s)
}
