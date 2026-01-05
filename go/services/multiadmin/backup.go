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

package multiadmin

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/tools/ctxutil"
	"github.com/multigres/multigres/go/tools/telemetry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Backup starts an async backup of a specific shard
func (s *MultiAdminServer) Backup(ctx context.Context, req *multiadminpb.BackupRequest) (*multiadminpb.BackupResponse, error) {
	s.logger.DebugContext(ctx, "Backup request received",
		"database", req.Database,
		"table_group", req.TableGroup,
		"shard", req.Shard,
		"type", req.Type,
		"force_primary", req.ForcePrimary)

	// Find a pooler synchronously so we can generate a stable job ID.
	// The job ID includes the pooler name, which enables recovery after multiadmin restart.
	pooler, err := s.findPoolerForBackup(ctx, req.Database, req.TableGroup, req.Shard, req.ForcePrimary)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to find pooler: %v", err)
	}

	// Generate a job ID that will be stored in pgbackrest and can be queried after restart.
	// Format: YYYYMMDD-HHMMSS.microseconds_<pooler_name>
	jobID := backup.GenerateJobID(pooler.Id.Name)

	// Create a job to track this backup operation
	s.backupJobTracker.CreateJobWithID(jobID, multiadminpb.JobType_JOB_TYPE_BACKUP, req.Database, req.TableGroup, req.Shard)

	// Start the backup operation asynchronously with a linked root span
	go func() {
		bgCtx := ctxutil.Detach(ctx)
		bgCtx, span := ctxutil.StartLinkedSpan(bgCtx, telemetry.Tracer(), "Backup")
		defer span.End()

		if err := s.executeBackup(bgCtx, jobID, pooler, req); err != nil {
			span.RecordError(err)
			s.logger.ErrorContext(bgCtx, "Backup failed", "job_id", jobID, "error", err)
			s.backupJobTracker.FailJob(jobID, err.Error())
		}
	}()

	return &multiadminpb.BackupResponse{
		JobId: jobID,
	}, nil
}

// executeBackup performs the actual backup operation
func (s *MultiAdminServer) executeBackup(ctx context.Context, jobID string, pooler *clustermetadatapb.MultiPooler, req *multiadminpb.BackupRequest) error {
	s.backupJobTracker.UpdateJobStatus(jobID, multiadminpb.JobStatus_JOB_STATUS_RUNNING)

	s.logger.InfoContext(ctx, "Starting backup",
		"job_id", jobID,
		"database", req.Database,
		"table_group", req.TableGroup,
		"shard", req.Shard,
		"force_primary", req.ForcePrimary)

	// Call backup on the pooler using the shared rpcClient
	// The jobID was generated in Backup() and is passed to pgbackrest as an annotation
	backupReq := &multipoolermanagerdata.BackupRequest{
		Type:         req.Type,
		ForcePrimary: req.ForcePrimary,
		JobId:        jobID,
	}

	resp, err := s.rpcClient.Backup(ctx, pooler, backupReq)
	if err != nil {
		return fmt.Errorf("pooler backup failed: %w", err)
	}

	// Mark job as completed
	s.backupJobTracker.CompleteJob(jobID, resp.BackupId)
	s.logger.InfoContext(ctx, "Backup completed",
		"job_id", jobID,
		"backup_id", resp.BackupId)

	return nil
}

// findPoolerForBackup finds a pooler for backup operations.
// If forcePrimary is true, finds a PRIMARY pooler; otherwise finds a REPLICA.
func (s *MultiAdminServer) findPoolerForBackup(ctx context.Context, database, tableGroup, shard string, forcePrimary bool) (*clustermetadatapb.MultiPooler, error) {
	targetType := clustermetadatapb.PoolerType_REPLICA
	if forcePrimary {
		targetType = clustermetadatapb.PoolerType_PRIMARY
	}

	// Get all cells
	allCells, err := s.ts.GetCellNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cell names: %w", err)
	}

	// Search across all cells for a pooler of the target type
	// TODO: Pick the replica with the least replica lag, as measured by the heartbeat service
	for _, cellName := range allCells {
		opts := &topoclient.GetMultiPoolersByCellOptions{
			DatabaseShard: &topoclient.DatabaseShard{
				Database:   database,
				TableGroup: tableGroup,
				// Note: Shard is intentionally left empty to match all shards.
				// Multipoolers currently don't set Shard when registering.
			},
		}
		poolerInfos, err := s.ts.GetMultiPoolersByCell(ctx, cellName, opts)
		if err != nil {
			s.logger.DebugContext(ctx, "Failed to get poolers for cell", "cell", cellName, "error", err)
			continue
		}

		// Find a pooler of the target type
		for _, info := range poolerInfos {
			if info.MultiPooler.Type == targetType {
				return info.MultiPooler, nil
			}
		}
	}

	typeStr := "replica"
	if forcePrimary {
		typeStr = "primary"
	}
	return nil, fmt.Errorf("%s pooler not found for database=%s, table_group=%s, shard=%s", typeStr, database, tableGroup, shard)
}

// RestoreFromBackup starts an async restore of a specific shard from a backup
func (s *MultiAdminServer) RestoreFromBackup(ctx context.Context, req *multiadminpb.RestoreFromBackupRequest) (*multiadminpb.RestoreFromBackupResponse, error) {
	s.logger.DebugContext(ctx, "RestoreFromBackup request received",
		"database", req.Database,
		"table_group", req.TableGroup,
		"shard", req.Shard,
		"backup_id", req.BackupId,
		"pooler_id", req.PoolerId)

	// Validate request
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database cannot be empty")
	}
	// Note: TableGroup and Shard validation ensures the restore target is fully specified.
	// The CLI currently hardcodes these to defaults, but the API is designed to support
	// the full use case when multi-shard support is implemented.
	if req.TableGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "table_group cannot be empty")
	}
	if req.Shard == "" {
		return nil, status.Error(codes.InvalidArgument, "shard cannot be empty")
	}
	if req.PoolerId == nil {
		return nil, status.Error(codes.InvalidArgument, "pooler_id cannot be empty")
	}

	// Create job
	jobID := s.backupJobTracker.CreateJob(multiadminpb.JobType_JOB_TYPE_RESTORE, req.Database, req.TableGroup, req.Shard)
	// TODO: store job_id somewhere persistent, such as the PGDATA directory or topo,
	// so that job state is not lost after multiadmin restart

	// Start restore in background with a linked root span
	go func() {
		bgCtx := ctxutil.Detach(ctx)
		bgCtx, span := ctxutil.StartLinkedSpan(bgCtx, telemetry.Tracer(), "Restore")
		defer span.End()

		if err := s.executeRestore(bgCtx, jobID, req); err != nil {
			span.RecordError(err)
			s.logger.ErrorContext(bgCtx, "Restore failed", "job_id", jobID, "error", err)
			s.backupJobTracker.FailJob(jobID, err.Error())
		}
	}()

	return &multiadminpb.RestoreFromBackupResponse{
		JobId: jobID,
	}, nil
}

// executeRestore performs the actual restore operation
// TODO: Add job_id support for RestoreFromBackup similar to Backup():
//  1. Add JobId field to RestoreFromBackupRequest proto
//  2. Generate backupJobID = backup.GenerateJobID(pooler.Id.Name)
//  3. Store job_id as pgbackrest annotation on restore
//  4. Add GetRestoreByJobId RPC or extend GetBackupByJobId for restore tracking
func (s *MultiAdminServer) executeRestore(ctx context.Context, jobID string, req *multiadminpb.RestoreFromBackupRequest) error {
	s.backupJobTracker.UpdateJobStatus(jobID, multiadminpb.JobStatus_JOB_STATUS_RUNNING)

	// Get pooler info from topology
	poolerInfo, err := s.ts.GetMultiPooler(ctx, req.PoolerId)
	if err != nil {
		return fmt.Errorf("failed to get pooler info: %w", err)
	}

	// Call restore on the pooler
	restoreReq := &multipoolermanagerdata.RestoreFromBackupRequest{
		BackupId: req.BackupId,
	}

	_, err = s.rpcClient.RestoreFromBackup(ctx, poolerInfo.MultiPooler, restoreReq)
	if err != nil {
		return fmt.Errorf("pooler restore failed: %w", err)
	}

	// Mark job as completed
	s.backupJobTracker.CompleteJob(jobID, req.BackupId)
	s.logger.InfoContext(ctx, "Restore completed", "job_id", jobID, "backup_id", req.BackupId)

	return nil
}

// GetBackupJobStatus checks the status of a backup or restore job
func (s *MultiAdminServer) GetBackupJobStatus(ctx context.Context, req *multiadminpb.GetBackupJobStatusRequest) (*multiadminpb.GetBackupJobStatusResponse, error) {
	s.logger.DebugContext(ctx, "GetBackupJobStatus request received", "job_id", req.JobId)

	// Validate request
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id cannot be empty")
	}

	// Try the in-memory job tracker first
	jobStatus, err := s.backupJobTracker.GetJobStatus(req.JobId)
	if err == nil {
		s.logger.DebugContext(ctx, "GetBackupJobStatus completed from tracker",
			"job_id", req.JobId,
			"status", jobStatus.Status,
			"job_type", jobStatus.JobType)
		return jobStatus, nil
	}

	// Job not in tracker - try fallback to MultiPooler if shard context provided.
	// This handles the case where the multiadmin process restarted and lost in-memory job state.
	if req.Database == "" || req.TableGroup == "" {
		s.logger.DebugContext(ctx, "Job not found and no shard context for fallback", "job_id", req.JobId)
		return nil, status.Errorf(codes.NotFound, "job not found: %s", req.JobId)
	}

	return s.getBackupJobStatusFromPooler(ctx, req)
}

// getBackupJobStatusFromPooler queries a MultiPooler for backup status when job is not in memory.
func (s *MultiAdminServer) getBackupJobStatusFromPooler(ctx context.Context, req *multiadminpb.GetBackupJobStatusRequest) (*multiadminpb.GetBackupJobStatusResponse, error) {
	s.logger.DebugContext(ctx, "Falling back to pooler for job status",
		"job_id", req.JobId,
		"database", req.Database,
		"table_group", req.TableGroup,
		"shard", req.Shard)

	// Find a replica pooler - all poolers for a shard share the same pgbackrest repo
	pooler, err := s.findPoolerForBackup(ctx, req.Database, req.TableGroup, req.Shard, false)
	if err != nil {
		s.logger.DebugContext(ctx, "Failed to find pooler for fallback", "error", err)
		return nil, status.Errorf(codes.NotFound, "job not found and unable to query pooler: %s", req.JobId)
	}

	// Query the pooler for backup by job_id
	backupResp, err := s.rpcClient.GetBackupByJobId(ctx, pooler, &multipoolermanagerdata.GetBackupByJobIdRequest{
		JobId: req.JobId,
	})
	if err != nil {
		s.logger.DebugContext(ctx, "Pooler GetBackupByJobId failed", "job_id", req.JobId, "error", err)
		return nil, status.Errorf(codes.NotFound, "job not found: %s", req.JobId)
	}

	// No backup found with this job_id
	if backupResp.Backup == nil {
		s.logger.DebugContext(ctx, "Backup not found by job_id", "job_id", req.JobId)
		return nil, status.Errorf(codes.NotFound, "job not found: %s", req.JobId)
	}

	// Convert backup metadata to job status
	var jobStatus multiadminpb.JobStatus
	switch backupResp.Backup.Status {
	case multipoolermanagerdata.BackupMetadata_COMPLETE:
		jobStatus = multiadminpb.JobStatus_JOB_STATUS_COMPLETED
	case multipoolermanagerdata.BackupMetadata_INCOMPLETE:
		jobStatus = multiadminpb.JobStatus_JOB_STATUS_FAILED
	default:
		jobStatus = multiadminpb.JobStatus_JOB_STATUS_UNKNOWN
	}

	s.logger.DebugContext(ctx, "GetBackupJobStatus completed from pooler fallback",
		"job_id", req.JobId,
		"backup_id", backupResp.Backup.BackupId,
		"status", jobStatus)

	return &multiadminpb.GetBackupJobStatusResponse{
		JobId:      req.JobId,
		JobType:    multiadminpb.JobType_JOB_TYPE_BACKUP,
		Status:     jobStatus,
		Database:   req.Database,
		TableGroup: req.TableGroup,
		Shard:      req.Shard,
		BackupId:   backupResp.Backup.BackupId,
	}, nil
}

// GetBackups lists backup artifacts with optional filtering
func (s *MultiAdminServer) GetBackups(ctx context.Context, req *multiadminpb.GetBackupsRequest) (*multiadminpb.GetBackupsResponse, error) {
	s.logger.DebugContext(ctx, "GetBackups request received",
		"database", req.Database,
		"table_group", req.TableGroup,
		"shard", req.Shard,
		"limit", req.Limit)

	// Validate request
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database cannot be empty")
	}
	if req.TableGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "table_group cannot be empty")
	}

	// Find a replica pooler - all replicas for a shard share the same pgbackrest repo
	pooler, err := s.findPoolerForBackup(ctx, req.Database, req.TableGroup, req.Shard, false)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to find replica pooler: %v", err)
	}

	// Query backups from the pooler
	getBackupsReq := &multipoolermanagerdata.GetBackupsRequest{
		Limit: req.Limit,
	}

	resp, err := s.rpcClient.GetBackups(ctx, pooler, getBackupsReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get backups from pooler: %v", err)
	}

	// Convert multipoolermanagerdata.BackupMetadata to multiadminpb.BackupInfo
	backups := make([]*multiadminpb.BackupInfo, len(resp.Backups))
	for i, b := range resp.Backups {
		var backupStatus multiadminpb.BackupStatus
		switch b.Status {
		case multipoolermanagerdata.BackupMetadata_COMPLETE:
			backupStatus = multiadminpb.BackupStatus_BACKUP_STATUS_COMPLETE
		case multipoolermanagerdata.BackupMetadata_INCOMPLETE:
			backupStatus = multiadminpb.BackupStatus_BACKUP_STATUS_INCOMPLETE
		default:
			backupStatus = multiadminpb.BackupStatus_BACKUP_STATUS_UNKNOWN
		}

		backups[i] = &multiadminpb.BackupInfo{
			BackupId:             b.BackupId,
			Database:             req.Database,
			TableGroup:           b.TableGroup,
			Shard:                b.Shard,
			Type:                 b.Type,
			Status:               backupStatus,
			BackupSizeBytes:      b.BackupSizeBytes,
			MultipoolerServiceId: b.MultipoolerId,
			PoolerType:           b.PoolerType,
		}
	}

	s.logger.DebugContext(ctx, "GetBackups completed", "backup_count", len(backups))

	return &multiadminpb.GetBackupsResponse{
		Backups: backups,
	}, nil
}
