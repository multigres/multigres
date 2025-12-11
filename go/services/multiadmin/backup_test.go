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
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestGetBackupJobStatus_Success(t *testing.T) {
	logger := slog.Default()
	server := NewMultiAdminServer(nil, logger)
	defer server.backupJobTracker.Stop()

	// Create a job directly in tracker
	jobID := server.backupJobTracker.CreateJob(multiadminpb.JobType_JOB_TYPE_BACKUP, "postgres", "test", "0")

	req := &multiadminpb.GetBackupJobStatusRequest{
		JobId: jobID,
	}

	resp, err := server.GetBackupJobStatus(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, jobID, resp.JobId)
	require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_PENDING, resp.Status)
}

func TestGetBackupJobStatus_NotFound(t *testing.T) {
	logger := slog.Default()
	server := NewMultiAdminServer(nil, logger)
	defer server.backupJobTracker.Stop()

	req := &multiadminpb.GetBackupJobStatusRequest{
		JobId: "non-existent-job-id",
	}

	_, err := server.GetBackupJobStatus(t.Context(), req)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestGetBackupJobStatus_EmptyJobID(t *testing.T) {
	logger := slog.Default()
	server := NewMultiAdminServer(nil, logger)
	defer server.backupJobTracker.Stop()

	req := &multiadminpb.GetBackupJobStatusRequest{
		JobId: "",
	}

	_, err := server.GetBackupJobStatus(t.Context(), req)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestRestoreFromBackup_ValidationErrors(t *testing.T) {
	logger := slog.Default()
	server := NewMultiAdminServer(nil, logger)
	defer server.backupJobTracker.Stop()

	tests := []struct {
		name    string
		req     *multiadminpb.RestoreFromBackupRequest
		wantErr codes.Code
	}{
		{
			name:    "empty database",
			req:     &multiadminpb.RestoreFromBackupRequest{Database: "", TableGroup: "test"},
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "empty table_group",
			req:     &multiadminpb.RestoreFromBackupRequest{Database: "postgres", TableGroup: ""},
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "nil pooler_id",
			req:     &multiadminpb.RestoreFromBackupRequest{Database: "postgres", TableGroup: "test", PoolerId: nil},
			wantErr: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.RestoreFromBackup(t.Context(), tt.req)
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, tt.wantErr, st.Code())
		})
	}
}

func TestGetBackups_ValidationErrors(t *testing.T) {
	logger := slog.Default()
	server := NewMultiAdminServer(nil, logger)
	defer server.backupJobTracker.Stop()

	tests := []struct {
		name    string
		req     *multiadminpb.GetBackupsRequest
		wantErr codes.Code
	}{
		{
			name:    "empty database",
			req:     &multiadminpb.GetBackupsRequest{Database: "", TableGroup: "test"},
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "empty table_group",
			req:     &multiadminpb.GetBackupsRequest{Database: "postgres", TableGroup: ""},
			wantErr: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.GetBackups(t.Context(), tt.req)
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, tt.wantErr, st.Code())
		})
	}
}

func TestGetBackupJobStatus_FallbackToPooler(t *testing.T) {
	// This test verifies that when a job is not in the in-memory tracker,
	// GetBackupJobStatus falls back to querying the MultiPooler via GetBackupByJobId.
	// This ensures resilience against multiadmin process restarts.

	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	logger := slog.Default()
	server := NewMultiAdminServer(ts, logger)
	defer server.backupJobTracker.Stop()

	// Create a replica pooler in the topology
	replicaPooler := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "replica-pooler",
		},
		Hostname:   "replica-pooler.cell1",
		PortMap:    map[string]int32{"grpc": 8081},
		Database:   "testdb",
		TableGroup: "default",
		Type:       clustermetadatapb.PoolerType_REPLICA,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, replicaPooler))

	// Create a fake RPC client with a pre-configured response
	// The FakeClient uses topo.MultiPoolerIDString which formats as "multipooler-cell-name"
	fakeClient := rpcclient.NewFakeClient()
	poolerKey := "multipooler-cell1-replica-pooler"
	fakeClient.GetBackupByJobIdResponses[poolerKey] = &multipoolermanagerdata.GetBackupByJobIdResponse{
		Backup: &multipoolermanagerdata.BackupMetadata{
			BackupId: "20251203-143045F",
			Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
			JobId:    "20251203-143045.000000_replica-pooler",
		},
	}
	server.SetRPCClient(fakeClient)

	t.Run("fallback succeeds when job not in tracker but found in pooler", func(t *testing.T) {
		// Request a job that's NOT in the tracker, but provide shard context
		req := &multiadminpb.GetBackupJobStatusRequest{
			JobId:      "20251203-143045.000000_replica-pooler",
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		}

		resp, err := server.GetBackupJobStatus(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "20251203-143045.000000_replica-pooler", resp.JobId)
		require.Equal(t, multiadminpb.JobType_JOB_TYPE_BACKUP, resp.JobType)
		require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, resp.Status)
		require.Equal(t, "20251203-143045F", resp.BackupId)

		// Verify the fake client was called
		require.Contains(t, fakeClient.CallLog, "GetBackupByJobId(multipooler-cell1-replica-pooler)")
	})

	t.Run("fallback returns not found when backup not in pooler", func(t *testing.T) {
		// Configure fake to return nil backup (not found)
		fakeClient.GetBackupByJobIdResponses[poolerKey] = &multipoolermanagerdata.GetBackupByJobIdResponse{
			Backup: nil, // Not found
		}

		req := &multiadminpb.GetBackupJobStatusRequest{
			JobId:      "unknown-job-id",
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		}

		_, err := server.GetBackupJobStatus(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("no fallback without shard context", func(t *testing.T) {
		// Without shard context (database, table_group), fallback should not happen
		req := &multiadminpb.GetBackupJobStatusRequest{
			JobId: "some-job-id",
			// No database, table_group, shard
		}

		_, err := server.GetBackupJobStatus(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("in-memory tracker takes precedence over fallback", func(t *testing.T) {
		// Create a job in the tracker
		trackerJobID := server.backupJobTracker.CreateJob(multiadminpb.JobType_JOB_TYPE_BACKUP, "testdb", "default", "0")

		req := &multiadminpb.GetBackupJobStatusRequest{
			JobId:      trackerJobID,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		}

		resp, err := server.GetBackupJobStatus(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, trackerJobID, resp.JobId)
		require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_PENDING, resp.Status)
	})
}

func TestBackup_ForcePrimary(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	logger := slog.Default()
	server := NewMultiAdminServer(ts, logger)
	defer server.backupJobTracker.Stop()

	// Create both a primary and replica pooler
	primaryPooler := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary-pooler",
		},
		Hostname:   "primary-pooler.cell1",
		PortMap:    map[string]int32{"grpc": 8081},
		Database:   "testdb",
		TableGroup: "default",
		Type:       clustermetadatapb.PoolerType_PRIMARY,
	}
	replicaPooler := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "replica-pooler",
		},
		Hostname:   "replica-pooler.cell1",
		PortMap:    map[string]int32{"grpc": 8081},
		Database:   "testdb",
		TableGroup: "default",
		Type:       clustermetadatapb.PoolerType_REPLICA,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, primaryPooler))
	require.NoError(t, ts.CreateMultiPooler(ctx, replicaPooler))

	fakeClient := rpcclient.NewFakeClient()
	fakeClient.BackupResponses["multipooler-cell1-primary-pooler"] = &multipoolermanagerdata.BackupResponse{
		BackupId: "20251203-120000F",
	}
	fakeClient.BackupResponses["multipooler-cell1-replica-pooler"] = &multipoolermanagerdata.BackupResponse{
		BackupId: "20251203-120001F",
	}
	server.SetRPCClient(fakeClient)

	t.Run("force_primary=true uses primary pooler", func(t *testing.T) {
		fakeClient.CallLog = nil // Reset call log

		req := &multiadminpb.BackupRequest{
			Database:     "testdb",
			TableGroup:   "default",
			Type:         "full",
			ForcePrimary: true,
		}

		resp, err := server.Backup(ctx, req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.JobId)

		// Wait for async backup to execute
		time.Sleep(20 * time.Millisecond)

		// Verify the primary pooler was called
		require.Contains(t, fakeClient.CallLog, "Backup(multipooler-cell1-primary-pooler)")
	})

	t.Run("force_primary=false uses replica pooler", func(t *testing.T) {
		fakeClient.CallLog = nil // Reset call log

		req := &multiadminpb.BackupRequest{
			Database:     "testdb",
			TableGroup:   "default",
			Type:         "full",
			ForcePrimary: false,
		}

		resp, err := server.Backup(ctx, req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.JobId)

		// Wait for async backup to execute
		time.Sleep(20 * time.Millisecond)

		// Verify the replica pooler was called
		require.Contains(t, fakeClient.CallLog, "Backup(multipooler-cell1-replica-pooler)")
	})

	t.Run("force_primary=false with no replicas returns error", func(t *testing.T) {
		// Create a new topology with only primary pooler
		ctx := t.Context()
		ts := memorytopo.NewServer(ctx, "cell2")
		server := NewMultiAdminServer(ts, logger)
		defer server.backupJobTracker.Stop()

		// Only create a primary pooler (no replica)
		primaryOnly := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell2",
				Name:      "primary-only",
			},
			Hostname:   "primary-only.cell2",
			PortMap:    map[string]int32{"grpc": 8081},
			Database:   "testdb",
			TableGroup: "default",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		}
		require.NoError(t, ts.CreateMultiPooler(ctx, primaryOnly))

		req := &multiadminpb.BackupRequest{
			Database:     "testdb",
			TableGroup:   "default",
			Type:         "full",
			ForcePrimary: false,
		}

		_, err := server.Backup(ctx, req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "replica pooler not found")
	})
}
