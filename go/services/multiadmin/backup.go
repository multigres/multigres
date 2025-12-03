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

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"

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

	// TODO: Implement async backup logic
	return nil, status.Error(codes.Unimplemented, "Backup operation is not yet implemented")
}

// RestoreFromBackup starts an async restore of a specific shard from a backup
func (s *MultiAdminServer) RestoreFromBackup(ctx context.Context, req *multiadminpb.RestoreFromBackupRequest) (*multiadminpb.RestoreFromBackupResponse, error) {
	s.logger.DebugContext(ctx, "RestoreFromBackup request received",
		"database", req.Database,
		"table_group", req.TableGroup,
		"shard", req.Shard,
		"backup_id", req.BackupId)

	// TODO: Implement async restore logic
	return nil, status.Error(codes.Unimplemented, "RestoreFromBackup operation is not yet implemented")
}

// GetBackupJobStatus checks the status of a backup or restore job
func (s *MultiAdminServer) GetBackupJobStatus(ctx context.Context, req *multiadminpb.GetBackupJobStatusRequest) (*multiadminpb.GetBackupJobStatusResponse, error) {
	s.logger.DebugContext(ctx, "GetBackupJobStatus request received", "job_id", req.JobId)

	// TODO: Implement job status lookup
	return nil, status.Error(codes.Unimplemented, "GetBackupJobStatus operation is not yet implemented")
}

// GetBackups lists backup artifacts with optional filtering
func (s *MultiAdminServer) GetBackups(ctx context.Context, req *multiadminpb.GetBackupsRequest) (*multiadminpb.GetBackupsResponse, error) {
	s.logger.DebugContext(ctx, "GetBackups request received",
		"database", req.Database,
		"table_group", req.TableGroup,
		"shard", req.Shard,
		"limit", req.Limit)

	// TODO: Implement backup listing with topology discovery and aggregation
	return nil, status.Error(codes.Unimplemented, "GetBackups operation is not yet implemented")
}
