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
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/timeouts"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/manager"
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
func (s *managerService) WaitForLSN(ctx context.Context, req *multipoolermanagerdatapb.WaitForLSNRequest) (*multipoolermanagerdatapb.WaitForLSNResponse, error) {
	err := s.manager.WaitForLSN(ctx, req.TargetLsn)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.WaitForLSNResponse{}, nil
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (s *managerService) StartReplication(ctx context.Context, req *multipoolermanagerdatapb.StartReplicationRequest) (*multipoolermanagerdatapb.StartReplicationResponse, error) {
	err := s.manager.StartReplication(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.StartReplicationResponse{}, nil
}

// StopReplication stops replication based on the specified mode
func (s *managerService) StopReplication(ctx context.Context, req *multipoolermanagerdatapb.StopReplicationRequest) (*multipoolermanagerdatapb.StopReplicationResponse, error) {
	err := s.manager.StopReplication(ctx, req.Mode, req.Wait)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.StopReplicationResponse{}, nil
}

// Status gets unified status that works for both PRIMARY and REPLICA poolers
func (s *managerService) Status(ctx context.Context, req *multipoolermanagerdatapb.StatusRequest) (*multipoolermanagerdatapb.StatusResponse, error) {
	resp, err := s.manager.Status(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// Backup performs a backup
func (s *managerService) Backup(ctx context.Context, req *multipoolermanagerdatapb.BackupRequest) (*multipoolermanagerdatapb.BackupResponse, error) {
	backupID, err := s.manager.Backup(ctx, req.ForcePrimary, req.Type, req.JobId, req.Overrides)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdatapb.BackupResponse{
		BackupId: backupID,
	}, nil
}

// RestoreFromBackup restores from a backup
func (s *managerService) RestoreFromBackup(ctx context.Context, req *multipoolermanagerdatapb.RestoreFromBackupRequest) (*multipoolermanagerdatapb.RestoreFromBackupResponse, error) {
	err := s.manager.RestoreFromBackup(ctx, req.BackupId)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdatapb.RestoreFromBackupResponse{}, nil
}

// GetBackups retrieves backup information
func (s *managerService) GetBackups(ctx context.Context, req *multipoolermanagerdatapb.GetBackupsRequest) (*multipoolermanagerdatapb.GetBackupsResponse, error) {
	backups, err := s.manager.GetBackups(ctx, req.Limit)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdatapb.GetBackupsResponse{
		Backups: backups,
	}, nil
}

// GetBackupByJobId retrieves a backup by its job_id annotation
func (s *managerService) GetBackupByJobId(ctx context.Context, req *multipoolermanagerdatapb.GetBackupByJobIdRequest) (*multipoolermanagerdatapb.GetBackupByJobIdResponse, error) {
	backup, err := s.manager.GetBackupByJobId(ctx, req.JobId)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdatapb.GetBackupByJobIdResponse{
		Backup: backup,
	}, nil
}

// ExpireBackups removes backups that exceed the configured retention policy
func (s *managerService) ExpireBackups(ctx context.Context, req *multipoolermanagerdatapb.ExpireBackupsRequest) (*multipoolermanagerdatapb.ExpireBackupsResponse, error) {
	expiredIDs, err := s.manager.ExpireBackups(ctx, req.Overrides)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdatapb.ExpireBackupsResponse{
		ExpiredBackupIds: expiredIDs,
	}, nil
}

// SetPostgresRestartsEnabled enables or disables automatic PostgreSQL restarts by the monitor
func (s *managerService) SetPostgresRestartsEnabled(ctx context.Context, req *multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest) (*multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse, error) {
	return s.manager.SetPostgresRestartsEnabled(ctx, req)
}

// ManagerHealthStream is the bidirectional health stream implementation.
//
// The orchestrator sends a start message (optionally carrying snapshot_interval
// and staleness_timeout preferences) to open the stream. The server responds
// immediately with a ManagerHealthStreamStartResponse confirming the actual
// values it will use. Subsequent messages are health snapshots.
//
// On each healthStreamer broadcast or poll request we call Status() to build a
// full snapshot and send it. Every message is a complete snapshot — there are
// no incremental updates.
//
// In addition to broadcast-driven snapshots, we also send a snapshot every
// snapshotInterval regardless of broadcasts. This ensures that postgres state
// changes — particularly process death detected by pgctld — reach the
// orchestrator promptly even when the local monitor is disabled.
//
// When the health channel is closed (buffer full), we return Unavailable to
// force the client to reconnect and receive a fresh initial snapshot.
func (s *managerService) ManagerHealthStream(
	stream multipoolermanagerpb.MultiPoolerManager_ManagerHealthStreamServer,
) error {
	ctx := stream.Context()

	// Read the start message. The first client message must be a start message.
	startMsg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "expected start message: %v", err)
	}
	req := startMsg.GetStart()
	if req == nil {
		return status.Error(codes.InvalidArgument, "first message must be a start message")
	}

	// Resolve effective timing values from the request, falling back to defaults.
	// Use AsDuration() rather than direct .Seconds access: proto fields are nil
	// when not set, and AsDuration() is nil-safe (returns 0 for nil).
	snapshotInterval := timeouts.DefaultSnapshotInterval
	if d := req.SnapshotInterval.AsDuration(); d > 0 {
		snapshotInterval = d
	}

	timeout := timeouts.DefaultHealthStreamStalenessTimeout
	if d := req.StalenessTimeout.AsDuration(); d > 0 {
		timeout = d
	}

	// Send start response so the orchestrator knows the actual values in use.
	response := &multipoolermanagerdatapb.ManagerHealthStreamResponse{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamResponse_Start{
			Start: &multipoolermanagerdatapb.ManagerHealthStreamStartResponse{
				SnapshotInterval: durationpb.New(snapshotInterval),
				StalenessTimeout: durationpb.New(timeout),
			},
		},
	}
	if err := stream.Send(response); err != nil {
		return status.Errorf(codes.Internal, "send start response: %v", err)
	}

	// Subscribe to health state changes. We use the channel as a notification
	// signal only — the actual payload sent to the orchestrator is a full
	// Status() snapshot rather than the lightweight gateway HealthState.
	_, healthChan, err := s.manager.SubscribeHealth(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to subscribe to health: %v", err)
	}

	if healthChan == nil {
		return status.Error(codes.Unavailable, "health provider not initialized")
	}

	// Send initial snapshot immediately upon connection.
	if err := s.sendManagerHealthSnapshot(ctx, stream, multipoolermanagerdatapb.SnapshotTrigger_SNAPSHOT_TRIGGER_INITIAL, timeout); err != nil {
		return err
	}

	// Goroutine: read incoming client messages and forward poll requests.
	// pollCh is buffered so bursts coalesce — only one snapshot is sent per
	// batch of poll requests received while the send loop is busy.
	pollCh := make(chan struct{}, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return // stream ended; send loop will exit via ctx.Done or healthChan close
			}
			if msg.GetPoll() != nil {
				select {
				case pollCh <- struct{}{}:
				default: // already a poll pending; coalesce
				}
			}
		}
	}()

	// Periodic ticker so we poll Status() even without a broadcast.
	// This catches postgres process death (reported by pgctld) within
	// snapshotInterval even when the local monitor is disabled.
	pollTicker := time.NewTicker(snapshotInterval)
	defer pollTicker.Stop()

	// Convenience function to send a snapshot with the given trigger, used by
	// all cases in the select below.
	sendSnapshot := func(trigger multipoolermanagerdatapb.SnapshotTrigger) error {
		return s.sendManagerHealthSnapshot(ctx, stream, trigger, timeout)
	}

	// Stream updates until the client disconnects or the context is cancelled.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-healthChan:
			if !ok {
				// Channel closed because the buffer was full. Return Unavailable
				// so the client reconnects and receives a fresh initial snapshot.
				return status.Error(codes.Unavailable, "health stream buffer full, reconnect required")
			}
			if err := sendSnapshot(multipoolermanagerdatapb.SnapshotTrigger_SNAPSHOT_TRIGGER_BROADCAST); err != nil {
				return err
			}
		case <-pollCh:
			if err := sendSnapshot(multipoolermanagerdatapb.SnapshotTrigger_SNAPSHOT_TRIGGER_POLL); err != nil {
				return err
			}
		case <-pollTicker.C:
			if err := sendSnapshot(multipoolermanagerdatapb.SnapshotTrigger_SNAPSHOT_TRIGGER_HEARTBEAT); err != nil {
				return err
			}
		}
	}
}

// sendManagerHealthSnapshot fetches the current Status and sends it as a
// ManagerHealthSnapshot on the stream.
//
// This is called both for the initial snapshot and for subsequent updates. We
// always fetch a fresh Status() to ensure we have the latest information, even
// if the health notification was coalesced or delayed.
func (s *managerService) sendManagerHealthSnapshot(
	ctx context.Context,
	stream multipoolermanagerpb.MultiPoolerManager_ManagerHealthStreamServer,
	trigger multipoolermanagerdatapb.SnapshotTrigger,
	timeout time.Duration,
) error {
	statusResp, err := s.manager.Status(ctx)
	if err != nil {
		return mterrors.ToGRPC(err)
	}

	healthSnapshot := &multipoolermanagerdatapb.ManagerHealthSnapshot{
		Status:  statusResp,
		Timeout: durationpb.New(timeout),
		Trigger: trigger,
	}

	response := &multipoolermanagerdatapb.ManagerHealthStreamResponse{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamResponse_Snapshot{
			Snapshot: healthSnapshot,
		},
	}

	return stream.Send(response)
}
