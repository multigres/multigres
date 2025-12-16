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
	"time"

	"google.golang.org/grpc/codes"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/multipooler/manager"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

const serviceName = "MultiPoolerManager"

// managerService is the gRPC wrapper for MultiPoolerManager
type managerService struct {
	multipoolermanagerpb.UnimplementedMultiPoolerManagerServer
	manager *manager.MultiPoolerManager
}

// grpcStatusCode extracts the gRPC status code from an mterror.
func (s *managerService) grpcStatusCode(ctx context.Context, err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	code := mterrors.Code(err)
	if code == 0 {
		s.manager.Logger().WarnContext(ctx, "unable to extract error code from error", "error", err)
		return codes.Unknown
	}
	return codes.Code(code)
}

func RegisterPoolerManagerServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the manager starts
	manager.RegisterPoolerManagerServices = append(manager.RegisterPoolerManagerServices, func(pm *manager.MultiPoolerManager) {
		if grpc.CheckServiceMap("poolermanager", senv) {
			srv := &managerService{manager: pm}
			multipoolermanagerpb.RegisterMultiPoolerManagerServer(grpc.Server, srv)
		}
	})
}

// WaitForLSN waits for PostgreSQL server to reach a specific LSN position
func (s *managerService) WaitForLSN(ctx context.Context, req *multipoolermanagerdatapb.WaitForLSNRequest) (_ *multipoolermanagerdatapb.WaitForLSNResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "WaitForLSN", s.grpcStatusCode(ctx, err))
	}()

	err = s.manager.WaitForLSN(ctx, req.TargetLsn)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.WaitForLSNResponse{}, nil
}

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (s *managerService) SetPrimaryConnInfo(ctx context.Context, req *multipoolermanagerdatapb.SetPrimaryConnInfoRequest) (_ *multipoolermanagerdatapb.SetPrimaryConnInfoResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "SetPrimaryConnInfo", s.grpcStatusCode(ctx, err))
	}()

	err = s.manager.SetPrimaryConnInfo(ctx,
		req.Host,
		req.Port,
		req.StopReplicationBefore,
		req.StartReplicationAfter,
		req.CurrentTerm,
		req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.SetPrimaryConnInfoResponse{}, nil
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (s *managerService) StartReplication(ctx context.Context, req *multipoolermanagerdatapb.StartReplicationRequest) (_ *multipoolermanagerdatapb.StartReplicationResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "StartReplication", s.grpcStatusCode(ctx, err))
	}()

	err = s.manager.StartReplication(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.StartReplicationResponse{}, nil
}

// StopReplication stops replication based on the specified mode
func (s *managerService) StopReplication(ctx context.Context, req *multipoolermanagerdatapb.StopReplicationRequest) (_ *multipoolermanagerdatapb.StopReplicationResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "StopReplication", s.grpcStatusCode(ctx, err))
	}()

	err = s.manager.StopReplication(ctx, req.Mode, req.Wait)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.StopReplicationResponse{}, nil
}

// StandbyReplicationStatus gets the current replication status of the standby
func (s *managerService) StandbyReplicationStatus(ctx context.Context, req *multipoolermanagerdatapb.StandbyReplicationStatusRequest) (_ *multipoolermanagerdatapb.StandbyReplicationStatusResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "StandbyReplicationStatus", s.grpcStatusCode(ctx, err))
	}()

	status, err := s.manager.StandbyReplicationStatus(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.StandbyReplicationStatusResponse{
		Status: status,
	}, nil
}

// Status gets unified status that works for both PRIMARY and REPLICA poolers
func (s *managerService) Status(ctx context.Context, req *multipoolermanagerdatapb.StatusRequest) (_ *multipoolermanagerdatapb.StatusResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "Status", s.grpcStatusCode(ctx, err))
	}()

	status, err := s.manager.Status(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.StatusResponse{
		Status: status,
	}, nil
}

// ResetReplication resets the standby's connection to its primary
func (s *managerService) ResetReplication(ctx context.Context, req *multipoolermanagerdatapb.ResetReplicationRequest) (_ *multipoolermanagerdatapb.ResetReplicationResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "ResetReplication", s.grpcStatusCode(ctx, err))
	}()

	err = s.manager.ResetReplication(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.ResetReplicationResponse{}, nil
}

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (s *managerService) ConfigureSynchronousReplication(ctx context.Context, req *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) (_ *multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "ConfigureSynchronousReplication", s.grpcStatusCode(ctx, err))
	}()

	err = s.manager.ConfigureSynchronousReplication(ctx,
		req.SynchronousCommit,
		req.SynchronousMethod,
		req.NumSync,
		req.StandbyIds,
		req.ReloadConfig)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}, nil
}

// UpdateSynchronousStandbyList updates the synchronous standby list
func (s *managerService) UpdateSynchronousStandbyList(ctx context.Context, req *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest) (_ *multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "UpdateSynchronousStandbyList", s.grpcStatusCode(ctx, err))
	}()

	err = s.manager.UpdateSynchronousStandbyList(ctx,
		req.Operation,
		req.StandbyIds,
		req.ReloadConfig,
		req.ConsensusTerm,
		req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{}, nil
}

// PrimaryStatus gets the status of the leader server
func (s *managerService) PrimaryStatus(ctx context.Context, req *multipoolermanagerdatapb.PrimaryStatusRequest) (_ *multipoolermanagerdatapb.PrimaryStatusResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "PrimaryStatus", s.grpcStatusCode(ctx, err))
	}()

	status, err := s.manager.PrimaryStatus(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.PrimaryStatusResponse{
		Status: status,
	}, nil
}

// PrimaryPosition gets the current LSN position of the leader
func (s *managerService) PrimaryPosition(ctx context.Context, req *multipoolermanagerdatapb.PrimaryPositionRequest) (_ *multipoolermanagerdatapb.PrimaryPositionResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "PrimaryPosition", s.grpcStatusCode(ctx, err))
	}()

	position, err := s.manager.PrimaryPosition(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.PrimaryPositionResponse{
		LsnPosition: position,
	}, nil
}

// StopReplicationAndGetStatus stops PostgreSQL replication and returns the status
func (s *managerService) StopReplicationAndGetStatus(ctx context.Context, req *multipoolermanagerdatapb.StopReplicationAndGetStatusRequest) (_ *multipoolermanagerdatapb.StopReplicationAndGetStatusResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "StopReplicationAndGetStatus", s.grpcStatusCode(ctx, err))
	}()

	status, err := s.manager.StopReplicationAndGetStatus(ctx, req.Mode, req.Wait)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.StopReplicationAndGetStatusResponse{
		Status: status,
	}, nil
}

// ChangeType changes the pooler type (LEADER/FOLLOWER)
func (s *managerService) ChangeType(ctx context.Context, req *multipoolermanagerdatapb.ChangeTypeRequest) (_ *multipoolermanagerdatapb.ChangeTypeResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "ChangeType", s.grpcStatusCode(ctx, err))
	}()

	err = s.manager.ChangeType(ctx, req.PoolerType.String())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.ChangeTypeResponse{}, nil
}

// GetFollowers gets the list of follower servers
func (s *managerService) GetFollowers(ctx context.Context, req *multipoolermanagerdatapb.GetFollowersRequest) (_ *multipoolermanagerdatapb.GetFollowersResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "GetFollowers", s.grpcStatusCode(ctx, err))
	}()

	response, err := s.manager.GetFollowers(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return response, nil
}

// Demote demotes the current leader server
func (s *managerService) Demote(ctx context.Context, req *multipoolermanagerdatapb.DemoteRequest) (_ *multipoolermanagerdatapb.DemoteResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "Demote", s.grpcStatusCode(ctx, err))
	}()

	// Default drain timeout if not specified
	drainTimeout := 5 * time.Second
	if req.DrainTimeout != nil {
		drainTimeout = req.DrainTimeout.AsDuration()
	}

	resp, err := s.manager.Demote(ctx,
		req.ConsensusTerm,
		drainTimeout,
		req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// UndoDemote undoes a demotion
func (s *managerService) UndoDemote(ctx context.Context, req *multipoolermanagerdatapb.UndoDemoteRequest) (_ *multipoolermanagerdatapb.UndoDemoteResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "UndoDemote", s.grpcStatusCode(ctx, err))
	}()

	err = s.manager.UndoDemote(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.UndoDemoteResponse{}, nil
}

// Promote promotes a replica to leader (Multigres-level operation)
func (s *managerService) Promote(ctx context.Context, req *multipoolermanagerdatapb.PromoteRequest) (_ *multipoolermanagerdatapb.PromoteResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "Promote", s.grpcStatusCode(ctx, err))
	}()

	resp, err := s.manager.Promote(ctx,
		req.ConsensusTerm,
		req.ExpectedLsn,
		req.SyncReplicationConfig,
		req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// State gets the current status of the manager
func (s *managerService) State(ctx context.Context, req *multipoolermanagerdatapb.StateRequest) (_ *multipoolermanagerdatapb.StateResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "State", s.grpcStatusCode(ctx, err))
	}()

	resp, err := s.manager.State(ctx)
	return resp, err
}

// SetTerm sets the consensus term information
func (s *managerService) SetTerm(ctx context.Context, req *multipoolermanagerdatapb.SetTermRequest) (_ *multipoolermanagerdatapb.SetTermResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "SetTerm", s.grpcStatusCode(ctx, err))
	}()

	if err = s.manager.SetTerm(ctx, req.Term); err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.SetTermResponse{}, nil
}

// Backup performs a backup
func (s *managerService) Backup(ctx context.Context, req *multipoolermanagerdatapb.BackupRequest) (_ *multipoolermanagerdatapb.BackupResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "Backup", s.grpcStatusCode(ctx, err))
	}()

	backupID, err := s.manager.Backup(ctx, req.ForcePrimary, req.Type)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdatapb.BackupResponse{
		BackupId: backupID,
	}, nil
}

// RestoreFromBackup restores from a backup
func (s *managerService) RestoreFromBackup(ctx context.Context, req *multipoolermanagerdatapb.RestoreFromBackupRequest) (_ *multipoolermanagerdatapb.RestoreFromBackupResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "RestoreFromBackup", s.grpcStatusCode(ctx, err))
	}()

	err = s.manager.RestoreFromBackup(ctx, req.BackupId)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdatapb.RestoreFromBackupResponse{}, nil
}

// GetBackups retrieves backup information
func (s *managerService) GetBackups(ctx context.Context, req *multipoolermanagerdatapb.GetBackupsRequest) (_ *multipoolermanagerdatapb.GetBackupsResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "GetBackups", s.grpcStatusCode(ctx, err))
	}()

	backups, err := s.manager.GetBackups(ctx, req.Limit)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdatapb.GetBackupsResponse{
		Backups: backups,
	}, nil
}

// InitializeEmptyPrimary initializes an empty PostgreSQL instance as a primary
func (s *managerService) InitializeEmptyPrimary(ctx context.Context, req *multipoolermanagerdatapb.InitializeEmptyPrimaryRequest) (_ *multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "InitializeEmptyPrimary", s.grpcStatusCode(ctx, err))
	}()

	resp, err := s.manager.InitializeEmptyPrimary(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// InitializeAsStandby initializes an empty PostgreSQL instance as a standby
func (s *managerService) InitializeAsStandby(ctx context.Context, req *multipoolermanagerdatapb.InitializeAsStandbyRequest) (_ *multipoolermanagerdatapb.InitializeAsStandbyResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "InitializeAsStandby", s.grpcStatusCode(ctx, err))
	}()

	resp, err := s.manager.InitializeAsStandby(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// CreateDurabilityPolicy creates a new durability policy in the local database
func (s *managerService) CreateDurabilityPolicy(ctx context.Context, req *multipoolermanagerdatapb.CreateDurabilityPolicyRequest) (_ *multipoolermanagerdatapb.CreateDurabilityPolicyResponse, err error) {
	start := time.Now()
	defer func() {
		s.manager.Metrics().RPCServerDuration().Record(ctx, time.Since(start), serviceName, "CreateDurabilityPolicy", s.grpcStatusCode(ctx, err))
	}()

	resp, err := s.manager.CreateDurabilityPolicy(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}
