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
	"log/slog"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multipooler/manager"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/servenv"
)

// managerService is the gRPC wrapper for MultiPoolerManager
type managerService struct {
	multipoolermanagerpb.UnimplementedMultiPoolerManagerServer
	manager *manager.MultiPoolerManager
}

func RegisterPoolerManagerServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the manager starts
	manager.RegisterPoolerManagerServices = append(manager.RegisterPoolerManagerServices, func(pm *manager.MultiPoolerManager) {
		if grpc.CheckServiceMap("poolermanager", senv) {
			srv := &managerService{
				manager: pm,
			}
			multipoolermanagerpb.RegisterMultiPoolerManagerServer(grpc.Server, srv)
		}
	})
}

// WaitForLSN waits for PostgreSQL server to reach a specific LSN position
func (s *managerService) WaitForLSN(ctx context.Context, req *multipoolermanagerdata.WaitForLSNRequest) (*multipoolermanagerdata.WaitForLSNResponse, error) {
	err := s.manager.WaitForLSN(ctx, req.TargetLsn)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.WaitForLSNResponse{}, nil
}

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (s *managerService) SetPrimaryConnInfo(ctx context.Context, req *multipoolermanagerdata.SetPrimaryConnInfoRequest) (*multipoolermanagerdata.SetPrimaryConnInfoResponse, error) {
	err := s.manager.SetPrimaryConnInfo(ctx,
		req.Host,
		req.Port,
		req.StopReplicationBefore,
		req.StartReplicationAfter,
		req.CurrentTerm,
		req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.SetPrimaryConnInfoResponse{}, nil
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (s *managerService) StartReplication(ctx context.Context, req *multipoolermanagerdata.StartReplicationRequest) (*multipoolermanagerdata.StartReplicationResponse, error) {
	err := s.manager.StartReplication(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.StartReplicationResponse{}, nil
}

// StopReplication stops replication based on the specified mode
func (s *managerService) StopReplication(ctx context.Context, req *multipoolermanagerdata.StopReplicationRequest) (*multipoolermanagerdata.StopReplicationResponse, error) {
	err := s.manager.StopReplication(ctx, req.Mode, req.Wait)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.StopReplicationResponse{}, nil
}

// ReplicationStatus gets the current replication status of the standby
func (s *managerService) ReplicationStatus(ctx context.Context, req *multipoolermanagerdata.ReplicationStatusRequest) (*multipoolermanagerdata.ReplicationStatusResponse, error) {
	status, err := s.manager.ReplicationStatus(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.ReplicationStatusResponse{
		Status: status,
	}, nil
}

// ResetReplication resets the standby's connection to its primary
func (s *managerService) ResetReplication(ctx context.Context, req *multipoolermanagerdata.ResetReplicationRequest) (*multipoolermanagerdata.ResetReplicationResponse, error) {
	err := s.manager.ResetReplication(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.ResetReplicationResponse{}, nil
}

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (s *managerService) ConfigureSynchronousReplication(ctx context.Context, req *multipoolermanagerdata.ConfigureSynchronousReplicationRequest) (*multipoolermanagerdata.ConfigureSynchronousReplicationResponse, error) {
	err := s.manager.ConfigureSynchronousReplication(ctx,
		req.SynchronousCommit,
		req.SynchronousMethod,
		req.NumSync,
		req.StandbyIds,
		req.ReloadConfig)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.ConfigureSynchronousReplicationResponse{}, nil
}

// UpdateSynchronousStandbyList updates the synchronous standby list
func (s *managerService) UpdateSynchronousStandbyList(ctx context.Context, req *multipoolermanagerdata.UpdateSynchronousStandbyListRequest) (*multipoolermanagerdata.UpdateSynchronousStandbyListResponse, error) {
	err := s.manager.UpdateSynchronousStandbyList(ctx,
		req.Operation,
		req.StandbyIds,
		req.ReloadConfig,
		req.ConsensusTerm,
		req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.UpdateSynchronousStandbyListResponse{}, nil
}

// PrimaryStatus gets the status of the leader server
func (s *managerService) PrimaryStatus(ctx context.Context, req *multipoolermanagerdata.PrimaryStatusRequest) (*multipoolermanagerdata.PrimaryStatusResponse, error) {
	status, err := s.manager.PrimaryStatus(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.PrimaryStatusResponse{
		Status: status,
	}, nil
}

// PrimaryPosition gets the current LSN position of the leader
func (s *managerService) PrimaryPosition(ctx context.Context, req *multipoolermanagerdata.PrimaryPositionRequest) (*multipoolermanagerdata.PrimaryPositionResponse, error) {
	position, err := s.manager.PrimaryPosition(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.PrimaryPositionResponse{
		LsnPosition: position,
	}, nil
}

// StopReplicationAndGetStatus stops PostgreSQL replication and returns the status
func (s *managerService) StopReplicationAndGetStatus(ctx context.Context, req *multipoolermanagerdata.StopReplicationAndGetStatusRequest) (*multipoolermanagerdata.StopReplicationAndGetStatusResponse, error) {
	status, err := s.manager.StopReplicationAndGetStatus(ctx, req.Mode, req.Wait)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.StopReplicationAndGetStatusResponse{
		Status: status,
	}, nil
}

// ChangeType changes the pooler type (LEADER/FOLLOWER)
func (s *managerService) ChangeType(ctx context.Context, req *multipoolermanagerdata.ChangeTypeRequest) (*multipoolermanagerdata.ChangeTypeResponse, error) {
	err := s.manager.ChangeType(ctx, req.PoolerType.String())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.ChangeTypeResponse{}, nil
}

// GetFollowers gets the list of follower servers
func (s *managerService) GetFollowers(ctx context.Context, req *multipoolermanagerdata.GetFollowersRequest) (*multipoolermanagerdata.GetFollowersResponse, error) {
	response, err := s.manager.GetFollowers(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return response, nil
}

// Demote demotes the current leader server
func (s *managerService) Demote(ctx context.Context, req *multipoolermanagerdata.DemoteRequest) (*multipoolermanagerdata.DemoteResponse, error) {
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
func (s *managerService) UndoDemote(ctx context.Context, req *multipoolermanagerdata.UndoDemoteRequest) (*multipoolermanagerdata.UndoDemoteResponse, error) {
	err := s.manager.UndoDemote(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.UndoDemoteResponse{}, nil
}

// Promote promotes a replica to leader (Multigres-level operation)
func (s *managerService) Promote(ctx context.Context, req *multipoolermanagerdata.PromoteRequest) (*multipoolermanagerdata.PromoteResponse, error) {
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

// Status gets the current status of the manager
func (s *managerService) Status(ctx context.Context, req *multipoolermanagerdata.StatusRequest) (*multipoolermanagerdata.StatusResponse, error) {
	return s.manager.Status(ctx)
}

// SetTerm sets the consensus term information
func (s *managerService) SetTerm(ctx context.Context, req *multipoolermanagerdata.SetTermRequest) (*multipoolermanagerdata.SetTermResponse, error) {
	if err := s.manager.SetTerm(ctx, req.Term); err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdata.SetTermResponse{}, nil
}

// Backup performs a backup
func (s *managerService) Backup(ctx context.Context, req *multipoolermanagerdata.BackupRequest) (*multipoolermanagerdata.BackupResponse, error) {
	// Check if this is a primary pooler based on topology
	poolerType := s.manager.GetPoolerType()
	isPrimary := (poolerType == clustermetadatapb.PoolerType_PRIMARY)

	// Prevent backups from primary databases unless ForcePrimary is set
	if isPrimary && !req.ForcePrimary {
		slog.WarnContext(ctx, "Backup requested on primary database without ForcePrimary flag")
		return nil, mterrors.ToGRPC(mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"backups from primary databases are not allowed unless ForcePrimary is set"))
	}

	configPath := s.manager.GetBackupConfigPath()
	stanzaName := s.manager.GetBackupStanza()
	tableGroup := s.manager.GetTableGroup()
	shard := s.manager.GetShard()

	result, err := manager.Backup(ctx, configPath, stanzaName, manager.BackupOptions{
		ForcePrimary: req.ForcePrimary,
		Type:         req.Type,
		TableGroup:   tableGroup,
		Shard:        shard,
	})
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdata.BackupResponse{
		BackupId: result.BackupID,
	}, nil
}

// RestoreFromBackup restores from a backup
func (s *managerService) RestoreFromBackup(ctx context.Context, req *multipoolermanagerdata.RestoreFromBackupRequest) (*multipoolermanagerdata.RestoreFromBackupResponse, error) {
	slog.InfoContext(ctx, "RestoreFromBackup called", "backup_id", req.BackupId)

	pgctldClient := s.manager.GetPgCtldClient()
	configPath := s.manager.GetBackupConfigPath()
	stanzaName := s.manager.GetBackupStanza()

	// Get pg_data directory from the backup config path
	// configPath is like /path/to/pooler_dir/pgbackrest.conf, so we get the dir and append pg_data
	poolerDir := filepath.Dir(configPath)
	pgDataDir := filepath.Join(poolerDir, "pg_data")

	// Determine if we should maintain standby status after restore
	// We query PostgreSQL directly to get the current recovery status
	slog.InfoContext(ctx, "Checking recovery status before restore")
	isPrimary, err := s.manager.IsPrimary(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to check recovery status before restore", "error", err)
		return nil, mterrors.ToGRPC(mterrors.Wrap(err, "failed to check recovery status"))
	}

	asStandby := !isPrimary

	// If this is a standby, get the current primary connection info
	// so we can restore it after pgbackrest overwrites postgresql.auto.conf
	var primaryHost string
	var primaryPort int32
	if asStandby {
		replStatus, err := s.manager.ReplicationStatus(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to get replication status", "error", err)
			return nil, mterrors.ToGRPC(mterrors.Wrap(err, "failed to get replication status"))
		}
		if replStatus == nil || replStatus.PrimaryConnInfo == nil || replStatus.PrimaryConnInfo.Host == "" {
			return nil, mterrors.ToGRPC(mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "standby has no primary connection configured"))
		}
		primaryHost = replStatus.PrimaryConnInfo.Host
		primaryPort = replStatus.PrimaryConnInfo.Port
	}

	slog.InfoContext(ctx, "Restore parameters determined",
		"is_primary", isPrimary,
		"as_standby", asStandby,
		"primary_host", primaryHost,
		"primary_port", primaryPort,
		"backup_id", req.BackupId)

	_, err = manager.RestoreShardFromBackup(ctx, pgctldClient, configPath, stanzaName, pgDataDir, manager.RestoreOptions{
		BackupID:    req.BackupId,
		AsStandby:   asStandby,
		PrimaryHost: primaryHost,
		PrimaryPort: primaryPort,
	})
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdata.RestoreFromBackupResponse{}, nil
}

// GetBackups retrieves backup information
func (s *managerService) GetBackups(ctx context.Context, req *multipoolermanagerdata.GetBackupsRequest) (*multipoolermanagerdata.GetBackupsResponse, error) {
	configPath := s.manager.GetBackupConfigPath()
	stanzaName := s.manager.GetBackupStanza()

	result, err := manager.GetBackups(ctx, configPath, stanzaName, manager.GetBackupsOptions{
		Limit: req.Limit,
	})
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdata.GetBackupsResponse{
		Backups: result.Backups,
	}, nil
}
