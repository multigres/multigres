// Copyright 2025 Supabase, Inc.
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

// Package grpcconsensusservice implements the gRPC server for consensus operations
package grpcconsensusservice

import (
	"context"

	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multipooler/manager"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdata "github.com/multigres/multigres/go/pb/consensusdata"
	"github.com/multigres/multigres/go/servenv"
)

// consensusService is the gRPC wrapper for consensus operations
type consensusService struct {
	consensuspb.UnimplementedMultiPoolerConsensusServer
	manager *manager.MultiPoolerManager
}

func RegisterConsensusServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the manager starts
	manager.RegisterPoolerManagerServices = append(manager.RegisterPoolerManagerServices, func(pm *manager.MultiPoolerManager) {
		if grpc.CheckServiceMap("consensus", senv) {
			srv := &consensusService{
				manager: pm,
			}
			consensuspb.RegisterMultiPoolerConsensusServer(grpc.Server, srv)
		}
	})
}

// RequestVote handles vote requests during leader election
func (s *consensusService) RequestVote(ctx context.Context, req *consensusdata.RequestVoteRequest) (*consensusdata.RequestVoteResponse, error) {
	resp, err := s.manager.RequestVote(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// Status returns the current status of this node
func (s *consensusService) Status(ctx context.Context, req *consensusdata.StatusRequest) (*consensusdata.StatusResponse, error) {
	resp, err := s.manager.ConsensusStatus(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// GetLeadershipView returns leadership information from the heartbeat table
func (s *consensusService) GetLeadershipView(ctx context.Context, req *consensusdata.LeadershipViewRequest) (*consensusdata.LeadershipViewResponse, error) {
	resp, err := s.manager.GetLeadershipView(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// GetWALPosition returns the current WAL position
func (s *consensusService) GetWALPosition(ctx context.Context, req *consensusdata.GetWALPositionRequest) (*consensusdata.GetWALPositionResponse, error) {
	resp, err := s.manager.GetWALPosition(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// CanReachPrimary checks if this node can reach the primary
func (s *consensusService) CanReachPrimary(ctx context.Context, req *consensusdata.CanReachPrimaryRequest) (*consensusdata.CanReachPrimaryResponse, error) {
	resp, err := s.manager.CanReachPrimary(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}
