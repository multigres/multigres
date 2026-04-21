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
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdata "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/manager"
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

// BeginTerm handles coordinator requests during leader appointments
func (s *consensusService) BeginTerm(ctx context.Context, req *consensusdata.BeginTermRequest) (*consensusdata.BeginTermResponse, error) {
	resp, err := s.manager.BeginTerm(ctx, req)
	if err != nil {
		// Return response even on error - the response may contain accepted=true
		// when the term was accepted but the action (e.g. REVOKE) failed
		return resp, mterrors.ToGRPC(err)
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

// EmergencyDemote demotes the current leader server
func (s *consensusService) EmergencyDemote(ctx context.Context, req *multipoolermanagerdatapb.EmergencyDemoteRequest) (*multipoolermanagerdatapb.EmergencyDemoteResponse, error) {
	drainTimeout := 5 * time.Second
	if req.DrainTimeout != nil {
		drainTimeout = req.DrainTimeout.AsDuration()
	}
	resp, err := s.manager.EmergencyDemote(ctx, req.ConsensusTerm, drainTimeout, req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// DemoteStalePrimary demotes a stale primary that came back after failover
func (s *consensusService) DemoteStalePrimary(ctx context.Context, req *multipoolermanagerdatapb.DemoteStalePrimaryRequest) (*multipoolermanagerdatapb.DemoteStalePrimaryResponse, error) {
	resp, err := s.manager.DemoteStalePrimary(ctx, req.Source, req.ConsensusTerm, req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// Promote promotes a replica to leader
func (s *consensusService) Promote(ctx context.Context, req *multipoolermanagerdatapb.PromoteRequest) (*multipoolermanagerdatapb.PromoteResponse, error) {
	resp, err := s.manager.Promote(ctx,
		req.ConsensusTerm,
		req.ExpectedLsn,
		req.SyncReplicationConfig,
		req.Force,
		req.Reason,
		req.CoordinatorId,
		req.CohortMembers,
		req.AcceptedMembers)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// UpdateConsensusRule updates the synchronous standby list (quorum membership)
func (s *consensusService) UpdateConsensusRule(ctx context.Context, req *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error) {
	err := s.manager.UpdateSynchronousStandbyList(ctx,
		req.Operation,
		req.StandbyIds,
		req.ReloadConfig,
		req.ConsensusTerm,
		req.Force,
		req.CoordinatorId)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{}, nil
}

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (s *consensusService) SetPrimaryConnInfo(ctx context.Context, req *multipoolermanagerdatapb.SetPrimaryConnInfoRequest) (*multipoolermanagerdatapb.SetPrimaryConnInfoResponse, error) {
	err := s.manager.SetPrimaryConnInfo(ctx,
		req.Primary,
		req.StopReplicationBefore,
		req.StartReplicationAfter,
		req.CurrentTerm,
		req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.SetPrimaryConnInfoResponse{}, nil
}

// RewindToSource performs pg_rewind to synchronize this server with a source
func (s *consensusService) RewindToSource(ctx context.Context, req *multipoolermanagerdatapb.RewindToSourceRequest) (*multipoolermanagerdatapb.RewindToSourceResponse, error) {
	return s.manager.RewindToSource(ctx, req.Source)
}
