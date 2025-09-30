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

package server

import (
	"context"
	"database/sql"
	"log/slog"

	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MultiPoolerManagerServer implements the MultiPoolerManager gRPC interface
type MultiPoolerManagerServer struct {
	multipoolermanagerpb.UnimplementedMultiPoolerManagerServer
	logger *slog.Logger
	config *Config
	db     *sql.DB
}

// NewMultiPoolerManagerServer creates a new multipooler manager gRPC server
func NewMultiPoolerManagerServer(logger *slog.Logger, config *Config) *MultiPoolerManagerServer {
	return &MultiPoolerManagerServer{
		logger: logger,
		config: config,
	}
}

// connectDB establishes a connection to PostgreSQL (reuses the shared logic)
func (s *MultiPoolerManagerServer) connectDB() error {
	if s.db != nil {
		return nil // Already connected
	}

	db, err := createDBConnection(s.logger, s.config)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

// Close closes the database connection
func (s *MultiPoolerManagerServer) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// WaitForLSN waits for PostgreSQL server to reach a specific LSN position
func (s *MultiPoolerManagerServer) WaitForLSN(ctx context.Context, req *multipoolermanagerdata.WaitForLSNRequest) (*multipoolermanagerdata.WaitForLSNResponse, error) {
	s.logger.Info("WaitForLSN called", "target_lsn", req.TargetLsn)
	return nil, status.Errorf(codes.Unimplemented, "method WaitForLSN not implemented")
}

// SetReadOnly makes the PostgreSQL instance read-only
func (s *MultiPoolerManagerServer) SetReadOnly(ctx context.Context, req *multipoolermanagerdata.SetReadOnlyRequest) (*multipoolermanagerdata.SetReadOnlyResponse, error) {
	s.logger.Info("SetReadOnly called")
	return nil, status.Errorf(codes.Unimplemented, "method SetReadOnly not implemented")
}

// PromoteStandby PostgreSQL standby server to primary
func (s *MultiPoolerManagerServer) PromoteStandby(ctx context.Context, req *multipoolermanagerdata.PromoteStandbyRequest) (*multipoolermanagerdata.PromoteStandbyResponse, error) {
	s.logger.Info("PromoteStandby called")
	return nil, status.Errorf(codes.Unimplemented, "method PromoteStandby not implemented")
}

// GetPrimaryLSN gets the current leader LSN position
func (s *MultiPoolerManagerServer) GetPrimaryLSN(ctx context.Context, req *multipoolermanagerdata.GetPrimaryLSNRequest) (*multipoolermanagerdata.GetPrimaryLSNResponse, error) {
	s.logger.Info("GetPrimaryLSN called")

	// Ensure database connection
	if err := s.connectDB(); err != nil {
		s.logger.Error("Failed to connect to database", "error", err)
		return nil, status.Errorf(codes.Internal, "database connection failed: %v", err)
	}

	// Query PostgreSQL for the current LSN position
	// pg_current_wal_lsn() returns the current write-ahead log write location
	var lsn string
	err := s.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	if err != nil {
		s.logger.Error("Failed to query LSN", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to query LSN: %v", err)
	}

	s.logger.Info("GetPrimaryLSN returning", "lsn", lsn)
	return &multipoolermanagerdata.GetPrimaryLSNResponse{
		LeaderLsn: lsn,
	}, nil
}

// IsReadOnly checks if PostgreSQL instance is in read-only mode
func (s *MultiPoolerManagerServer) IsReadOnly(ctx context.Context, req *multipoolermanagerdata.IsReadOnlyRequest) (*multipoolermanagerdata.IsReadOnlyResponse, error) {
	s.logger.Info("IsReadOnly called")
	return nil, status.Errorf(codes.Unimplemented, "method IsReadOnly not implemented")
}

// SetStandbyPrimaryConnInfo sets the primary connection info for a standby server
func (s *MultiPoolerManagerServer) SetStandbyPrimaryConnInfo(ctx context.Context, req *multipoolermanagerdata.SetStandbyPrimaryConnInfoRequest) (*multipoolermanagerdata.SetStandbyPrimaryConnInfoResponse, error) {
	s.logger.Info("SetStandbyPrimaryConnInfo called", "host", req.Host, "port", req.Port)
	return nil, status.Errorf(codes.Unimplemented, "method SetStandbyPrimaryConnInfo not implemented")
}

// StartStandbyReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (s *MultiPoolerManagerServer) StartStandbyReplication(ctx context.Context, req *multipoolermanagerdata.StartReplicationRequest) (*multipoolermanagerdata.StartReplicationResponse, error) {
	s.logger.Info("StartStandbyReplication called")
	return nil, status.Errorf(codes.Unimplemented, "method StartStandbyReplication not implemented")
}

// StopStandbyReplication stops WAL replay on standby (calls pg_wal_replay_pause)
func (s *MultiPoolerManagerServer) StopStandbyReplication(ctx context.Context, req *multipoolermanagerdata.StopStandbyReplicationRequest) (*multipoolermanagerdata.StopStandbyReplicationResponse, error) {
	s.logger.Info("StopStandbyReplication called")
	return nil, status.Errorf(codes.Unimplemented, "method StopStandbyReplication not implemented")
}

// StandbyReplicationStatus gets the current replication status of the standby
func (s *MultiPoolerManagerServer) StandbyReplicationStatus(ctx context.Context, req *multipoolermanagerdata.StandbyReplicationStatusRequest) (*multipoolermanagerdata.StandbyReplicationStatusResponse, error) {
	s.logger.Info("StandbyReplicationStatus called")
	return nil, status.Errorf(codes.Unimplemented, "method StandbyReplicationStatus not implemented")
}

// ResetStandbyReplication resets the standby's connection to its primary
func (s *MultiPoolerManagerServer) ResetStandbyReplication(ctx context.Context, req *multipoolermanagerdata.ResetStandbyReplicationRequest) (*multipoolermanagerdata.ResetStandbyReplicationResponse, error) {
	s.logger.Info("ResetStandbyReplication called")
	return nil, status.Errorf(codes.Unimplemented, "method ResetStandbyReplication not implemented")
}

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (s *MultiPoolerManagerServer) ConfigureSynchronousReplication(ctx context.Context, req *multipoolermanagerdata.ConfigureSynchronousReplicationRequest) (*multipoolermanagerdata.ConfigureSynchronousReplicationResponse, error) {
	s.logger.Info("ConfigureSynchronousReplication called", "synchronous_commit", req.SynchronousCommit)
	return nil, status.Errorf(codes.Unimplemented, "method ConfigureSynchronousReplication not implemented")
}

// PrimaryStatus gets the status of the leader server
func (s *MultiPoolerManagerServer) PrimaryStatus(ctx context.Context, req *multipoolermanagerdata.PrimaryStatusRequest) (*multipoolermanagerdata.PrimaryStatusResponse, error) {
	s.logger.Info("PrimaryStatus called")
	return nil, status.Errorf(codes.Unimplemented, "method PrimaryStatus not implemented")
}

// PrimaryPosition gets the current LSN position of the leader
func (s *MultiPoolerManagerServer) PrimaryPosition(ctx context.Context, req *multipoolermanagerdata.PrimaryPositionRequest) (*multipoolermanagerdata.PrimaryPositionResponse, error) {
	s.logger.Info("PrimaryPosition called")
	return nil, status.Errorf(codes.Unimplemented, "method PrimaryPosition not implemented")
}

// StopReplicationAndGetStatus stops PostgreSQL replication and returns the status
func (s *MultiPoolerManagerServer) StopReplicationAndGetStatus(ctx context.Context, req *multipoolermanagerdata.StopReplicationAndGetStatusRequest) (*multipoolermanagerdata.StopReplicationAndGetStatusResponse, error) {
	s.logger.Info("StopReplicationAndGetStatus called")
	return nil, status.Errorf(codes.Unimplemented, "method StopReplicationAndGetStatus not implemented")
}

// ChangeType changes the pooler type (LEADER/FOLLOWER)
func (s *MultiPoolerManagerServer) ChangeType(ctx context.Context, req *multipoolermanagerdata.ChangeTypeRequest) (*multipoolermanagerdata.ChangeTypeResponse, error) {
	s.logger.Info("ChangeType called", "pooler_type", req.PoolerType)
	return nil, status.Errorf(codes.Unimplemented, "method ChangeType not implemented")
}

// GetFollowers gets the list of follower servers
func (s *MultiPoolerManagerServer) GetFollowers(ctx context.Context, req *multipoolermanagerdata.GetFollowersRequest) (*multipoolermanagerdata.GetFollowersResponse, error) {
	s.logger.Info("GetFollowers called")
	return nil, status.Errorf(codes.Unimplemented, "method GetFollowers not implemented")
}

// DemoteLeader demotes the current leader server
func (s *MultiPoolerManagerServer) DemoteLeader(ctx context.Context, req *multipoolermanagerdata.DemoteLeaderRequest) (*multipoolermanagerdata.DemoteLeaderResponse, error) {
	s.logger.Info("DemoteLeader called")
	return nil, status.Errorf(codes.Unimplemented, "method DemoteLeader not implemented")
}

// UndoDemoteLeader undoes a leader demotion
func (s *MultiPoolerManagerServer) UndoDemoteLeader(ctx context.Context, req *multipoolermanagerdata.UndoDemoteLeaderRequest) (*multipoolermanagerdata.UndoDemoteLeaderResponse, error) {
	s.logger.Info("UndoDemoteLeader called")
	return nil, status.Errorf(codes.Unimplemented, "method UndoDemoteLeader not implemented")
}

// PromoteFollower promotes a follower to leader
func (s *MultiPoolerManagerServer) PromoteFollower(ctx context.Context, req *multipoolermanagerdata.PromoteFollowerRequest) (*multipoolermanagerdata.PromoteFollowerResponse, error) {
	s.logger.Info("PromoteFollower called")
	return nil, status.Errorf(codes.Unimplemented, "method PromoteFollower not implemented")
}
