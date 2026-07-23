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

package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	commontypes "github.com/multigres/multigres/go/common/types"
	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery"
)

// MultiorchServer implements the MultiorchService gRPC service.
// It provides diagnostic information about the multiorch recovery engine,
// including detected problems and shard health status.
type MultiorchServer struct {
	multiorchpb.UnimplementedMultiorchServiceServer
	engine      *recovery.Engine
	coordinator *consensus.Coordinator
	logger      *slog.Logger
}

// NewMultiorchServer creates a new MultiorchServer instance.
func NewMultiorchServer(engine *recovery.Engine, coordinator *consensus.Coordinator, logger *slog.Logger) *MultiorchServer {
	return &MultiorchServer{
		engine:      engine,
		coordinator: coordinator,
		logger:      logger,
	}
}

// RegisterWithGRPCServer registers the MultiorchService with the provided gRPC server.
func (s *MultiorchServer) RegisterWithGRPCServer(grpcServer *grpc.Server) {
	multiorchpb.RegisterMultiorchServiceServer(grpcServer, s)
	s.logger.Info("Multiorch service registered")
}

// GetShardStatus returns diagnostic information for a specific shard.
// It includes detected problems, pooler health, and shard summary.
func (s *MultiorchServer) GetShardStatus(
	ctx context.Context,
	req *multiorchpb.ShardStatusRequest,
) (*multiorchpb.ShardStatusResponse, error) {
	// Validate that this shard is in our watch targets
	if req.ShardKey == nil {
		return nil, status.Errorf(codes.InvalidArgument, "shard_key is required")
	}
	sk := req.ShardKey
	if !s.engine.IsWatchingShard(sk.Database, sk.TableGroup, sk.Shard) {
		return nil, status.Errorf(codes.NotFound,
			"shard %s is not in watch targets for this multiorch instance", commontypes.FormatShardKey(sk))
	}

	// Get all detected problems from the engine
	allProblems := s.engine.GetDetectedProblems()

	// Filter problems for the requested shard
	skStr := commontypes.FormatShardKey(sk)
	var shardProblems []*multiorchpb.DetectedProblem
	for _, p := range allProblems {
		if commontypes.FormatShardKey(p.ShardKey) == skStr {
			shardProblems = append(shardProblems, &multiorchpb.DetectedProblem{
				Code:        string(p.Code),
				CheckName:   string(p.CheckName),
				PoolerId:    p.PoolerID,
				ShardKey:    p.ShardKey,
				Description: p.Description,
				Priority:    int32(p.Priority),
				Scope:       string(p.Scope),
				DetectedAt:  timestamppb.New(p.DetectedAt),
			})
		}
	}

	resp := &multiorchpb.ShardStatusResponse{
		Problems:      shardProblems,
		PoolerHealths: s.buildPoolerHealthList(req),
	}

	return resp, nil
}

// DisableRecovery stops the recovery loop and waits for in-flight actions to complete.
func (s *MultiorchServer) DisableRecovery(_ context.Context, _ *multiorchpb.DisableRecoveryRequest) (*multiorchpb.DisableRecoveryResponse, error) {
	s.engine.DisableRecovery()
	return &multiorchpb.DisableRecoveryResponse{
		Success: true,
		Message: "recovery disabled",
	}, nil
}

// EnableRecovery resumes the recovery loop.
func (s *MultiorchServer) EnableRecovery(_ context.Context, _ *multiorchpb.EnableRecoveryRequest) (*multiorchpb.EnableRecoveryResponse, error) {
	s.engine.EnableRecovery()
	return &multiorchpb.EnableRecoveryResponse{
		Success: true,
		Message: "recovery enabled",
	}, nil
}

// GetRecoveryStatus returns whether recovery is currently enabled or disabled.
func (s *MultiorchServer) GetRecoveryStatus(_ context.Context, _ *multiorchpb.GetRecoveryStatusRequest) (*multiorchpb.GetRecoveryStatusResponse, error) {
	return &multiorchpb.GetRecoveryStatusResponse{
		Enabled: s.engine.IsRecoveryEnabled(),
	}, nil
}

// TriggerRecoveryNow immediately executes recovery cycles until no problems remain
// or the request context times out. Returns problem codes that remain unresolved.
func (s *MultiorchServer) TriggerRecoveryNow(ctx context.Context, req *multiorchpb.TriggerRecoveryNowRequest) (*multiorchpb.TriggerRecoveryNowResponse, error) {
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	} else {
		// Subtract 200ms from deadline to allow time for response overhead.
		timeout := time.Until(deadline) - 200*time.Millisecond
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
	}

	if req.MaxCycles > 1 {
		return nil, status.Errorf(codes.InvalidArgument, "max_cycles must be 0 (unlimited) or 1 (single cycle), got %d", req.MaxCycles)
	}

	remainingProblems, err := s.engine.TriggerRecoveryNow(ctx, req.MaxCycles)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return nil, status.Error(codes.Internal, fmt.Sprintf("recovery trigger failed: %v", err))
	}

	problemCodes := make([]string, 0, len(remainingProblems))
	for _, p := range remainingProblems {
		problemCodes = append(problemCodes, p.AnalysisType)
	}

	return &multiorchpb.TriggerRecoveryNowResponse{
		RemainingProblemCodes: problemCodes,
	}, nil
}

// ApplyCertifiedRuleChange installs a new shard rule using a fully-populated
// externally certified revocation. See proto/multiorchservice.proto for the
// shape contract — multiorch is a pure executor and the caller must populate
// every identity and timing field.
func (s *MultiorchServer) ApplyCertifiedRuleChange(
	ctx context.Context,
	req *multiorchpb.ApplyCertifiedRuleChangeRequest,
) (*multiorchpb.ApplyCertifiedRuleChangeResponse, error) {
	if req.GetShardKey() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "shard_key is required")
	}
	sk := req.GetShardKey()
	if !s.engine.IsWatchingShard(sk.Database, sk.TableGroup, sk.Shard) {
		return nil, status.Errorf(codes.NotFound,
			"shard %s is not in watch targets for this multiorch instance", commontypes.FormatShardKey(sk))
	}

	if err := s.coordinator.ApplyCertifiedRuleChange(ctx, sk, req.GetProposedTransition(), req.GetCert(), req.GetReason()); err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multiorchpb.ApplyCertifiedRuleChangeResponse{}, nil
}

// buildPoolerHealthList creates pooler health snapshots for the requested shard.
func (s *MultiorchServer) buildPoolerHealthList(req *multiorchpb.ShardStatusRequest) []*multiorchpb.PoolerHealth {
	sk := req.ShardKey
	poolers := s.engine.GetPoolerHealthForShard(sk.Database, sk.TableGroup, sk.Shard)

	healthList := make([]*multiorchpb.PoolerHealth, 0, len(poolers))
	for _, p := range poolers {
		h := p.Health()

		// Get pooler type string
		poolerType := h.GetStatus().GetPoolerType().String()

		healthList = append(healthList, &multiorchpb.PoolerHealth{
			PoolerId:      h.Multipooler.Id,
			Reachable:     h.IsLastCheckValid,
			PostgresReady: h.GetStatus().GetPostgresReady(),
			PoolerType:    poolerType,
			LastCheck:     h.LastCheckAttempted,
		})
	}

	return healthList
}
