// Copyright 2025 Supabase, Inc.
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

// Package grpcmanagerservice implements the gRPC server for MultiPoolerManager
package grpcmanagerservice

import (
	"context"

	"github.com/multigres/multigres/go/multipooler/manager"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/servenv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// managerService is the gRPC wrapper for MultiPoolerManager
type managerService struct {
	multipoolermanagerpb.UnimplementedMultiPoolerManagerServer
	manager *manager.MultiPoolerManager
}

func init() {
	// Register ourselves to be invoked when the manager starts
	// Following Vitess pattern from grpctmserver/server.go
	manager.RegisterPoolerManagerServices = append(manager.RegisterPoolerManagerServices, func(pm *manager.MultiPoolerManager) {
		if servenv.GRPCCheckServiceMap("poolermanager") {
			srv := &managerService{
				manager: pm,
			}
			multipoolermanagerpb.RegisterMultiPoolerManagerServer(servenv.GRPCServer, srv)
		}
	})
}

// WaitForLSN waits for PostgreSQL server to reach a specific LSN position
func (s *managerService) WaitForLSN(ctx context.Context, req *multipoolermanagerdata.WaitForLSNRequest) (*multipoolermanagerdata.WaitForLSNResponse, error) {
	err := s.manager.WaitForLSN(ctx, req.TargetLsn)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.WaitForLSNResponse{}, nil
}

// SetReadOnly makes the PostgreSQL instance read-only
func (s *managerService) SetReadOnly(ctx context.Context, req *multipoolermanagerdata.SetReadOnlyRequest) (*multipoolermanagerdata.SetReadOnlyResponse, error) {
	err := s.manager.SetReadOnly(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.SetReadOnlyResponse{}, nil
}

// PromoteStandby PostgreSQL standby server to primary
func (s *managerService) PromoteStandby(ctx context.Context, req *multipoolermanagerdata.PromoteStandbyRequest) (*multipoolermanagerdata.PromoteStandbyResponse, error) {
	err := s.manager.PromoteStandby(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.PromoteStandbyResponse{}, nil
}

// GetPrimaryLSN gets the current leader LSN position
func (s *managerService) GetPrimaryLSN(ctx context.Context, req *multipoolermanagerdata.GetPrimaryLSNRequest) (*multipoolermanagerdata.GetPrimaryLSNResponse, error) {
	lsn, err := s.manager.GetPrimaryLSN(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get primary LSN: %v", err)
	}
	return &multipoolermanagerdata.GetPrimaryLSNResponse{
		LeaderLsn: lsn,
	}, nil
}

// IsReadOnly checks if PostgreSQL instance is in read-only mode
func (s *managerService) IsReadOnly(ctx context.Context, req *multipoolermanagerdata.IsReadOnlyRequest) (*multipoolermanagerdata.IsReadOnlyResponse, error) {
	readOnly, err := s.manager.IsReadOnly(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.IsReadOnlyResponse{
		ReadOnly: readOnly,
	}, nil
}

// SetStandbyPrimaryConnInfo sets the primary connection info for a standby server
func (s *managerService) SetStandbyPrimaryConnInfo(ctx context.Context, req *multipoolermanagerdata.SetStandbyPrimaryConnInfoRequest) (*multipoolermanagerdata.SetStandbyPrimaryConnInfoResponse, error) {
	err := s.manager.SetStandbyPrimaryConnInfo(ctx, req.Host, req.Port)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.SetStandbyPrimaryConnInfoResponse{}, nil
}

// StartStandbyReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (s *managerService) StartStandbyReplication(ctx context.Context, req *multipoolermanagerdata.StartReplicationRequest) (*multipoolermanagerdata.StartReplicationResponse, error) {
	err := s.manager.StartStandbyReplication(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.StartReplicationResponse{}, nil
}

// StopStandbyReplication stops WAL replay on standby (calls pg_wal_replay_pause)
func (s *managerService) StopStandbyReplication(ctx context.Context, req *multipoolermanagerdata.StopStandbyReplicationRequest) (*multipoolermanagerdata.StopStandbyReplicationResponse, error) {
	err := s.manager.StopStandbyReplication(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.StopStandbyReplicationResponse{}, nil
}

// StandbyReplicationStatus gets the current replication status of the standby
func (s *managerService) StandbyReplicationStatus(ctx context.Context, req *multipoolermanagerdata.StandbyReplicationStatusRequest) (*multipoolermanagerdata.StandbyReplicationStatusResponse, error) {
	_, err := s.manager.StandbyReplicationStatus(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	// TODO: Convert map to proper response structure
	return &multipoolermanagerdata.StandbyReplicationStatusResponse{}, nil
}

// ResetStandbyReplication resets the standby's connection to its primary
func (s *managerService) ResetStandbyReplication(ctx context.Context, req *multipoolermanagerdata.ResetStandbyReplicationRequest) (*multipoolermanagerdata.ResetStandbyReplicationResponse, error) {
	err := s.manager.ResetStandbyReplication(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.ResetStandbyReplicationResponse{}, nil
}

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (s *managerService) ConfigureSynchronousReplication(ctx context.Context, req *multipoolermanagerdata.ConfigureSynchronousReplicationRequest) (*multipoolermanagerdata.ConfigureSynchronousReplicationResponse, error) {
	err := s.manager.ConfigureSynchronousReplication(ctx, req.SynchronousCommit.String())
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.ConfigureSynchronousReplicationResponse{}, nil
}

// PrimaryStatus gets the status of the leader server
func (s *managerService) PrimaryStatus(ctx context.Context, req *multipoolermanagerdata.PrimaryStatusRequest) (*multipoolermanagerdata.PrimaryStatusResponse, error) {
	_, err := s.manager.PrimaryStatus(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	// TODO: Convert map to proper response structure
	return &multipoolermanagerdata.PrimaryStatusResponse{}, nil
}

// PrimaryPosition gets the current LSN position of the leader
func (s *managerService) PrimaryPosition(ctx context.Context, req *multipoolermanagerdata.PrimaryPositionRequest) (*multipoolermanagerdata.PrimaryPositionResponse, error) {
	position, err := s.manager.PrimaryPosition(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.PrimaryPositionResponse{
		LsnPosition: position,
	}, nil
}

// StopReplicationAndGetStatus stops PostgreSQL replication and returns the status
func (s *managerService) StopReplicationAndGetStatus(ctx context.Context, req *multipoolermanagerdata.StopReplicationAndGetStatusRequest) (*multipoolermanagerdata.StopReplicationAndGetStatusResponse, error) {
	_, err := s.manager.StopReplicationAndGetStatus(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	// TODO: Convert map to proper response structure
	return &multipoolermanagerdata.StopReplicationAndGetStatusResponse{}, nil
}

// ChangeType changes the pooler type (LEADER/FOLLOWER)
func (s *managerService) ChangeType(ctx context.Context, req *multipoolermanagerdata.ChangeTypeRequest) (*multipoolermanagerdata.ChangeTypeResponse, error) {
	err := s.manager.ChangeType(ctx, req.PoolerType.String())
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.ChangeTypeResponse{}, nil
}

// GetFollowers gets the list of follower servers
func (s *managerService) GetFollowers(ctx context.Context, req *multipoolermanagerdata.GetFollowersRequest) (*multipoolermanagerdata.GetFollowersResponse, error) {
	_, err := s.manager.GetFollowers(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	// TODO: Convert []string to proper response structure
	return &multipoolermanagerdata.GetFollowersResponse{}, nil
}

// DemoteLeader demotes the current leader server
func (s *managerService) DemoteLeader(ctx context.Context, req *multipoolermanagerdata.DemoteLeaderRequest) (*multipoolermanagerdata.DemoteLeaderResponse, error) {
	err := s.manager.DemoteLeader(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.DemoteLeaderResponse{}, nil
}

// UndoDemoteLeader undoes a leader demotion
func (s *managerService) UndoDemoteLeader(ctx context.Context, req *multipoolermanagerdata.UndoDemoteLeaderRequest) (*multipoolermanagerdata.UndoDemoteLeaderResponse, error) {
	err := s.manager.UndoDemoteLeader(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.UndoDemoteLeaderResponse{}, nil
}

// PromoteFollower promotes a follower to leader
func (s *managerService) PromoteFollower(ctx context.Context, req *multipoolermanagerdata.PromoteFollowerRequest) (*multipoolermanagerdata.PromoteFollowerResponse, error) {
	err := s.manager.PromoteFollower(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "%v", err)
	}
	return &multipoolermanagerdata.PromoteFollowerResponse{}, nil
}
