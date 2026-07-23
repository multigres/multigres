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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdata "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
)

// consensusService is the gRPC wrapper for consensus operations
type consensusService struct {
	consensuspb.UnimplementedMultipoolerConsensusServer
	manager *manager.MultipoolerManager
}

func RegisterConsensusServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the manager starts
	manager.RegisterPoolerManagerServices = append(manager.RegisterPoolerManagerServices, func(pm *manager.MultipoolerManager) {
		if grpc.CheckServiceMap("consensus", senv) {
			srv := &consensusService{
				manager: pm,
			}
			consensuspb.RegisterMultipoolerConsensusServer(grpc.Server, srv)
		}
	})
}

// Promote sends a role assignment to this pooler: promote to primary or point replication at the new primary.
func (s *consensusService) Promote(ctx context.Context, req *consensusdata.PromoteRequest) (*consensusdata.PromoteResponse, error) {
	resp, err := s.manager.Promote(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// Recruit stops this pooler's replication participation and records a TermRevocation.
func (s *consensusService) Recruit(ctx context.Context, req *consensusdata.RecruitRequest) (*consensusdata.RecruitResponse, error) {
	resp, err := s.manager.Recruit(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// UpdateConsensusRule applies a cohort-membership change on the primary.
func (s *consensusService) UpdateConsensusRule(ctx context.Context, req *multipoolermanagerdatapb.UpdateConsensusRuleRequest) (*multipoolermanagerdatapb.UpdateConsensusRuleResponse, error) {
	err := s.manager.UpdateConsensusRule(ctx,
		req.Operation,
		req.StandbyIds,
		req.ExpectedOutgoingRule,
		req.CoordinatorId)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.UpdateConsensusRuleResponse{}, nil
}

// SetPrimary updates this pooler's replication settings to point at the supplied
// primary, gated on a position comparison. See manager.SetPrimary for details.
func (s *consensusService) SetPrimary(ctx context.Context, req *consensusdata.SetPrimaryRequest) (*consensusdata.SetPrimaryResponse, error) {
	resp, err := s.manager.SetPrimary(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// RewindToSource performs pg_rewind to synchronize this server with a source
func (s *consensusService) RewindToSource(ctx context.Context, req *multipoolermanagerdatapb.RewindToSourceRequest) (*multipoolermanagerdatapb.RewindToSourceResponse, error) {
	resp, err := s.manager.RewindToSource(ctx, req.Source)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}
