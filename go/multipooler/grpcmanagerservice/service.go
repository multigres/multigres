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
		return nil, err
	}
	return &multipoolermanagerdata.WaitForLSNResponse{}, nil
}

// SetReadOnly toggles the PostgreSQL instance read-only mode
func (s *managerService) SetReadOnly(ctx context.Context, req *multipoolermanagerdata.SetReadOnlyRequest) (*multipoolermanagerdata.SetReadOnlyResponse, error) {
	err := s.manager.SetReadOnly(ctx, req.ReadOnly)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.SetReadOnlyResponse{}, nil
}

// IsReadOnly checks if PostgreSQL instance is in read-only mode
func (s *managerService) IsReadOnly(ctx context.Context, req *multipoolermanagerdata.IsReadOnlyRequest) (*multipoolermanagerdata.IsReadOnlyResponse, error) {
	readOnly, err := s.manager.IsReadOnly(ctx)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.IsReadOnlyResponse{
		ReadOnly: readOnly,
	}, nil
}

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (s *managerService) SetPrimaryConnInfo(ctx context.Context, req *multipoolermanagerdata.SetPrimaryConnInfoRequest) (*multipoolermanagerdata.SetPrimaryConnInfoResponse, error) {
	err := s.manager.SetPrimaryConnInfo(ctx, req.Host, req.Port)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.SetPrimaryConnInfoResponse{}, nil
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (s *managerService) StartReplication(ctx context.Context, req *multipoolermanagerdata.StartReplicationRequest) (*multipoolermanagerdata.StartReplicationResponse, error) {
	err := s.manager.StartReplication(ctx)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.StartReplicationResponse{}, nil
}

// StopReplication stops WAL replay on standby (calls pg_wal_replay_pause)
func (s *managerService) StopReplication(ctx context.Context, req *multipoolermanagerdata.StopReplicationRequest) (*multipoolermanagerdata.StopReplicationResponse, error) {
	err := s.manager.StopReplication(ctx)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.StopReplicationResponse{}, nil
}

// ReplicationStatus gets the current replication status of the standby
func (s *managerService) ReplicationStatus(ctx context.Context, req *multipoolermanagerdata.ReplicationStatusRequest) (*multipoolermanagerdata.ReplicationStatusResponse, error) {
	_, err := s.manager.ReplicationStatus(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: Convert map to proper response structure
	return &multipoolermanagerdata.ReplicationStatusResponse{}, nil
}

// ResetReplication resets the standby's connection to its primary
func (s *managerService) ResetReplication(ctx context.Context, req *multipoolermanagerdata.ResetReplicationRequest) (*multipoolermanagerdata.ResetReplicationResponse, error) {
	err := s.manager.ResetReplication(ctx)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.ResetReplicationResponse{}, nil
}

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (s *managerService) ConfigureSynchronousReplication(ctx context.Context, req *multipoolermanagerdata.ConfigureSynchronousReplicationRequest) (*multipoolermanagerdata.ConfigureSynchronousReplicationResponse, error) {
	err := s.manager.ConfigureSynchronousReplication(ctx, req.SynchronousCommit.String())
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.ConfigureSynchronousReplicationResponse{}, nil
}

// PrimaryStatus gets the status of the leader server
func (s *managerService) PrimaryStatus(ctx context.Context, req *multipoolermanagerdata.PrimaryStatusRequest) (*multipoolermanagerdata.PrimaryStatusResponse, error) {
	_, err := s.manager.PrimaryStatus(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: Convert map to proper response structure
	return &multipoolermanagerdata.PrimaryStatusResponse{}, nil
}

// PrimaryPosition gets the current LSN position of the leader
func (s *managerService) PrimaryPosition(ctx context.Context, req *multipoolermanagerdata.PrimaryPositionRequest) (*multipoolermanagerdata.PrimaryPositionResponse, error) {
	position, err := s.manager.PrimaryPosition(ctx)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.PrimaryPositionResponse{
		LsnPosition: position,
	}, nil
}

// StopReplicationAndGetStatus stops PostgreSQL replication and returns the status
func (s *managerService) StopReplicationAndGetStatus(ctx context.Context, req *multipoolermanagerdata.StopReplicationAndGetStatusRequest) (*multipoolermanagerdata.StopReplicationAndGetStatusResponse, error) {
	_, err := s.manager.StopReplicationAndGetStatus(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: Convert map to proper response structure
	return &multipoolermanagerdata.StopReplicationAndGetStatusResponse{}, nil
}

// ChangeType changes the pooler type (LEADER/FOLLOWER)
func (s *managerService) ChangeType(ctx context.Context, req *multipoolermanagerdata.ChangeTypeRequest) (*multipoolermanagerdata.ChangeTypeResponse, error) {
	err := s.manager.ChangeType(ctx, req.PoolerType.String())
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.ChangeTypeResponse{}, nil
}

// GetFollowers gets the list of follower servers
func (s *managerService) GetFollowers(ctx context.Context, req *multipoolermanagerdata.GetFollowersRequest) (*multipoolermanagerdata.GetFollowersResponse, error) {
	_, err := s.manager.GetFollowers(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: Convert []string to proper response structure
	return &multipoolermanagerdata.GetFollowersResponse{}, nil
}

// Demote demotes the current leader server
func (s *managerService) Demote(ctx context.Context, req *multipoolermanagerdata.DemoteRequest) (*multipoolermanagerdata.DemoteResponse, error) {
	err := s.manager.Demote(ctx)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.DemoteResponse{}, nil
}

// UndoDemote undoes a demotion
func (s *managerService) UndoDemote(ctx context.Context, req *multipoolermanagerdata.UndoDemoteRequest) (*multipoolermanagerdata.UndoDemoteResponse, error) {
	err := s.manager.UndoDemote(ctx)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.UndoDemoteResponse{}, nil
}

// Promote promotes a replica to leader (Multigres-level operation)
func (s *managerService) Promote(ctx context.Context, req *multipoolermanagerdata.PromoteRequest) (*multipoolermanagerdata.PromoteResponse, error) {
	err := s.manager.Promote(ctx)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdata.PromoteResponse{}, nil
}

// Status gets the current status of the manager
func (s *managerService) Status(ctx context.Context, req *multipoolermanagerdata.StatusRequest) (*multipoolermanagerdata.StatusResponse, error) {
	return s.manager.Status(ctx)
}
