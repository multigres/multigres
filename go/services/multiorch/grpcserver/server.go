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
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	"github.com/multigres/multigres/go/services/multiorch/recovery"
)

// MultiOrchServer implements the MultiOrchService gRPC service.
// It provides diagnostic information about the multiorch recovery engine,
// including detected problems and shard health status.
type MultiOrchServer struct {
	multiorchpb.UnimplementedMultiOrchServiceServer
	engine *recovery.Engine
	logger *slog.Logger
}

// NewMultiOrchServer creates a new MultiOrchServer instance.
func NewMultiOrchServer(engine *recovery.Engine, logger *slog.Logger) *MultiOrchServer {
	return &MultiOrchServer{
		engine: engine,
		logger: logger,
	}
}

// RegisterWithGRPCServer registers the MultiOrchService with the provided gRPC server.
func (s *MultiOrchServer) RegisterWithGRPCServer(grpcServer *grpc.Server) {
	multiorchpb.RegisterMultiOrchServiceServer(grpcServer, s)
	s.logger.Info("MultiOrch service registered")
}

// GetShardStatus returns diagnostic information for a specific shard.
// It includes detected problems, pooler health, and shard summary.
func (s *MultiOrchServer) GetShardStatus(
	ctx context.Context,
	req *multiorchpb.ShardStatusRequest,
) (*multiorchpb.ShardStatusResponse, error) {
	// Validate that this shard is in our watch targets
	if !s.engine.IsWatchingShard(req.Database, req.TableGroup, req.Shard) {
		return nil, status.Errorf(codes.NotFound,
			"shard %s/%s/%s is not in watch targets for this multiorch instance",
			req.Database, req.TableGroup, req.Shard)
	}

	// Get all detected problems from the engine
	allProblems := s.engine.GetDetectedProblems()

	// Filter problems for the requested shard
	var shardProblems []*multiorchpb.DetectedProblem
	for _, p := range allProblems {
		if p.ShardKey.Database == req.Database &&
			p.ShardKey.TableGroup == req.TableGroup &&
			p.ShardKey.Shard == req.Shard {
			shardProblems = append(shardProblems, &multiorchpb.DetectedProblem{
				Code:        string(p.Code),
				CheckName:   string(p.CheckName),
				PoolerId:    p.PoolerID,
				Database:    p.ShardKey.Database,
				TableGroup:  p.ShardKey.TableGroup,
				Shard:       p.ShardKey.Shard,
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

// buildPoolerHealthList creates pooler health snapshots for the requested shard.
func (s *MultiOrchServer) buildPoolerHealthList(req *multiorchpb.ShardStatusRequest) []*multiorchpb.PoolerHealth {
	poolers := s.engine.GetPoolerHealthForShard(req.Database, req.TableGroup, req.Shard)

	healthList := make([]*multiorchpb.PoolerHealth, 0, len(poolers))
	for _, p := range poolers {
		if p == nil || p.MultiPooler == nil {
			continue
		}

		// Get pooler type string
		poolerType := p.PoolerType.String()

		healthList = append(healthList, &multiorchpb.PoolerHealth{
			PoolerId:        p.MultiPooler.Id,
			Reachable:       p.IsLastCheckValid,
			PostgresRunning: p.IsPostgresRunning,
			PoolerType:      poolerType,
			LastCheck:       p.LastCheckAttempted,
		})
	}

	return healthList
}
