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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestGetBackupJobStatus_Success(t *testing.T) {
	logger := slog.Default()
	server := NewMultiadminServer(nil, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	server := NewMultiadminServer(nil, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	server := NewMultiadminServer(nil, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func TestGetBackups_ValidationErrors(t *testing.T) {
	logger := slog.Default()
	server := NewMultiadminServer(nil, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	// GetBackupJobStatus falls back to querying the Multipooler via GetBackupByJobId.
	// This ensures resilience against multiadmin process restarts.

	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	logger := slog.Default()
	server := NewMultiadminServer(ts, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer server.backupJobTracker.Stop()

	// Create a replica pooler in the topology
	replicaPooler := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "replica-pooler",
		},
		Hostname: "replica-pooler.cell1",
		PortMap:  map[string]int32{"grpc": 8081},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
		},
		Type: clustermetadatapb.PoolerType_REPLICA,
	}
	require.NoError(t, ts.CreateMultipooler(ctx, replicaPooler))

	// Create a fake RPC client with a pre-configured response
	// The FakeClient uses topo.ComponentIDString which formats as "multipooler-cell-name"
	fakeClient := rpcclient.NewFakeClient()
	poolerKey := topoclient.ComponentID("multipooler-cell1-replica-pooler")
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

func TestGetBackups_PropagatesLSNAndPgVersion(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	server := NewMultiadminServer(ts, slog.Default(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer server.backupJobTracker.Stop()

	replicaPooler := &clustermetadatapb.Multipooler{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica-pooler"},
		Hostname: "replica-pooler.cell1",
		PortMap:  map[string]int32{"grpc": 8081},
		ShardKey: &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default"},
		Type:     clustermetadatapb.PoolerType_REPLICA,
	}
	require.NoError(t, ts.CreateMultipooler(ctx, replicaPooler))

	startTimestamp := timestamppb.New(time.Date(2026, 1, 4, 10, 0, 0, 0, time.UTC))
	stopTimestamp := timestamppb.New(time.Date(2026, 1, 4, 10, 5, 0, 0, time.UTC))

	fakeClient := rpcclient.NewFakeClient()
	poolerKey := topoclient.ComponentID("multipooler-cell1-replica-pooler")
	fakeClient.GetBackupsResponses[poolerKey] = &multipoolermanagerdata.GetBackupsResponse{
		Backups: []*multipoolermanagerdata.BackupMetadata{{
			BackupId:       "20250104-100000F",
			TableGroup:     "default",
			Shard:          "0",
			Type:           "full",
			Status:         multipoolermanagerdata.BackupMetadata_COMPLETE,
			StartLsn:       "0/21000028",
			StopLsn:        "0/21000100",
			PgVersion:      "16.2",
			StartTimestamp: startTimestamp,
			StopTimestamp:  stopTimestamp,
			JobId:          "20250104-100000.000000_replica-pooler",
		}},
	}
	server.SetRPCClient(fakeClient)

	resp, err := server.GetBackups(ctx, &multiadminpb.GetBackupsRequest{
		Database: "testdb", TableGroup: "default", Shard: "0",
	})
	require.NoError(t, err)
	require.Len(t, resp.Backups, 1)
	require.Equal(t, "0/21000028", resp.Backups[0].StartLsn)
	require.Equal(t, "0/21000100", resp.Backups[0].StopLsn)
	require.Equal(t, "16.2", resp.Backups[0].PgVersion)
	require.True(t, startTimestamp.AsTime().Equal(resp.Backups[0].StartTimestamp.AsTime()))
	require.True(t, stopTimestamp.AsTime().Equal(resp.Backups[0].StopTimestamp.AsTime()))
	require.Equal(t, "20250104-100000.000000_replica-pooler", resp.Backups[0].JobId)
}

func TestFindPoolerForBackup_SkipsNonServingReplicas(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t, "cell1")
	defer server.Stop()

	disabled := makeRoutedPooler("cell1", "disabled-replica", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
	disabled.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	require.NoError(t, server.ts.CreateMultipooler(ctx, disabled))

	_, err := server.findPoolerForBackup(ctx, "db1", "default", "0-inf", false)
	require.ErrorContains(t, err, "serving follower pooler not found")

	serving := makeRoutedPooler("cell1", "serving-replica", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
	serving.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	require.NoError(t, server.ts.CreateMultipooler(ctx, serving))

	got, err := server.findPoolerForBackup(ctx, "db1", "default", "0-inf", false)
	require.NoError(t, err)
	require.Equal(t, serving.Id.Name, got.Id.Name)
}

func TestFindPoolerForBackup_StaticLeader(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t, "cell1")
	defer server.Stop()

	leader := makeRoutedPooler("cell1", "static-leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	leader.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	leader.LeadershipMode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
	require.NoError(t, server.ts.CreateMultipooler(ctx, leader))
	_, err := server.findPoolerForBackup(ctx, "db1", "default", "0-inf", false)
	require.ErrorContains(t, err, "is not serving", "a static leader is never replaced by a guess")

	_, err = server.ts.UpdateMultipoolerFields(ctx, leader.Id, func(p *clustermetadatapb.Multipooler) error {
		p.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
		return nil
	})
	require.NoError(t, err)
	for _, forceLeader := range []bool{false, true} {
		got, err := server.findPoolerForBackup(ctx, "db1", "default", "0-inf", forceLeader)
		require.NoError(t, err, "a static leader is chosen with forceLeader=%v", forceLeader)
		require.Equal(t, leader.Id.Name, got.Id.Name)
	}

	// A stopped consensus pooler in the same shard makes it mixed.
	replica := makeRoutedPooler("cell1", "consensus-replica", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
	replica.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	require.NoError(t, server.ts.CreateMultipooler(ctx, replica))
	_, err = server.findPoolerForBackup(ctx, "db1", "default", "0-inf", false)
	require.ErrorContains(t, err, "has a static leader (multipooler-cell1-static-leader) and 1 other pooler(s)")
}

// TestFindPoolerForBackup_StaticLeaderIgnoresShutDownPoolers: a static leader
// restarted under a new service ID leaves its old record, shut down, behind.
func TestFindPoolerForBackup_StaticLeaderIgnoresShutDownPoolers(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t, "cell1")
	defer server.Stop()

	old := makeRoutedPooler("cell1", "old-static-leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_UNKNOWN)
	old.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	old.LeadershipMode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
	old.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN}
	require.NoError(t, server.ts.CreateMultipooler(ctx, old))
	leader := makeRoutedPooler("cell1", "new-static-leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	leader.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	leader.LeadershipMode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
	require.NoError(t, server.ts.CreateMultipooler(ctx, leader))

	got, err := server.findPoolerForBackup(ctx, "db1", "default", "0-inf", false)
	require.NoError(t, err, "the shut-down record is not another pooler")
	require.Equal(t, leader.Id.Name, got.Id.Name)
}

// TestFindPoolerForBackup_StaticLeaderWithUnreadableCell fails closed: a cell
// that cannot be read could hide another pooler of the static leader's shard.
func TestFindPoolerForBackup_StaticLeaderWithUnreadableCell(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t, "cell1")
	defer server.Stop()

	leader := makeRoutedPooler("cell1", "static-leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	leader.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	leader.LeadershipMode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
	require.NoError(t, server.ts.CreateMultipooler(ctx, leader))
	// cell2 is listed in the global topology but its topology server cannot
	// be reached.
	require.NoError(t, server.ts.CreateCell(ctx, "cell2", &clustermetadatapb.Cell{ServerAddresses: []string{"unreachable"}, Root: "/cell2"}))

	_, err := server.findPoolerForBackup(ctx, "db1", "default", "0-inf", false)
	require.ErrorContains(t, err, "cannot confirm static leader multipooler-cell1-static-leader is the only pooler")
	require.ErrorContains(t, err, "[cell2]")
}

// TestFindPoolerForMigration_ReachesDrainingPrimary: a migration in progress
// holds the target primary DRAINING; the migration control plane must still
// reach it (findPoolerForMigration's requireServing=false), while the backup
// path (findPoolerForBackup, always requireServing=true) correctly does not.
func TestFindPoolerForMigration_ReachesDrainingPrimary(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t, "cell1")
	defer server.Stop()

	primary := makeRoutedPooler("cell1", "draining-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	primary.ServingStatus = clustermetadatapb.PoolerServingStatus_DRAINING
	require.NoError(t, server.ts.CreateMultipooler(ctx, primary))

	// Backup path: a non-serving leader is invisible.
	_, err := server.findPoolerForBackup(ctx, "db1", "default", "0-inf", true)
	require.ErrorContains(t, err, "serving leader pooler not found")

	// Migration path: the DRAINING primary is reached.
	got, err := server.findPoolerForMigration(ctx, &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"})
	require.NoError(t, err)
	require.Equal(t, primary.Id.Name, got.Id.Name)
}

// TestFindPoolerForMigration_StaticLeaderDraining covers the same
// requireServing=false need for a static-leader-topology shard: the migration
// control plane must reach a DRAINING static leader too.
func TestFindPoolerForMigration_StaticLeaderDraining(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t, "cell1")
	defer server.Stop()

	leader := makeRoutedPooler("cell1", "static-leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	leader.ServingStatus = clustermetadatapb.PoolerServingStatus_DRAINING
	leader.LeadershipMode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
	require.NoError(t, server.ts.CreateMultipooler(ctx, leader))

	// Backup path: a non-serving static leader is refused.
	_, err := server.findPoolerForBackup(ctx, "db1", "default", "0-inf", false)
	require.ErrorContains(t, err, "is not serving")

	// Migration path: the DRAINING static leader is reached.
	got, err := server.findPoolerForMigration(ctx, &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"})
	require.NoError(t, err)
	require.Equal(t, leader.Id.Name, got.Id.Name)
}

// TestFindPooler_ExcludesOtherShards: a target for one shard never resolves to
// another shard's primary.
func TestFindPooler_ExcludesOtherShards(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t, "cell1")
	defer server.Stop()

	wrongShard := makeRoutedPooler("cell1", "wrong-shard-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	wrongShard.ShardKey.Shard = "80-inf"
	require.NoError(t, server.ts.CreateMultipooler(ctx, wrongShard))

	// A caller asking for "0-inf" must not resolve to the "80-inf" primary,
	// even though it is the only serving primary registered.
	_, err := server.findPoolerForBackup(ctx, "db1", "default", "0-inf", true)
	require.ErrorContains(t, err, "serving leader pooler not found")

	// Once the right shard's primary is registered too, it — and only it —
	// is selected.
	rightShard := makeRoutedPooler("cell1", "right-shard-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	require.NoError(t, server.ts.CreateMultipooler(ctx, rightShard))

	got, err := server.findPoolerForBackup(ctx, "db1", "default", "0-inf", true)
	require.NoError(t, err)
	require.Equal(t, rightShard.Id.Name, got.Id.Name)
}

// TestFindPoolerForMigration_ExcludesShutDownPooler: a LIFECYCLE_SHUTDOWN pooler
// is never selected, even with requireServing=false.
func TestFindPoolerForMigration_ExcludesShutDownPooler(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t, "cell1")
	defer server.Stop()

	stale := makeRoutedPooler("cell1", "stale-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	stale.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	stale.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN}
	require.NoError(t, server.ts.CreateMultipooler(ctx, stale))

	_, err := server.findPoolerForMigration(ctx, &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"})
	require.ErrorContains(t, err, "serving leader pooler not found", "a shut-down pooler's stale record must never be selected as the leader")
}

// TestFindPoolerForMigration_StaticLeaderElsewhereDoesNotBreakWildcardSearch: a
// static-leader shard elsewhere doesn't break a wildcard search.
func TestFindPoolerForMigration_StaticLeaderElsewhereDoesNotBreakWildcardSearch(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t, "cell1")
	defer server.Stop()

	// A static-leader single-node shard ("80-inf")...
	staticLeader := makeRoutedPooler("cell1", "static-leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	staticLeader.LeadershipMode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
	staticLeader.ShardKey.Shard = "80-inf"
	require.NoError(t, server.ts.CreateMultipooler(ctx, staticLeader))

	// ...coexisting with an ordinary consensus primary on a different shard ("0-inf").
	ordinary := makeRoutedPooler("cell1", "ordinary-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	require.NoError(t, server.ts.CreateMultipooler(ctx, ordinary))

	// target=nil is the match-any-pooler wildcard used by ref-addressed migration
	// RPCs (GetMigration, DropMigration, etc.).
	_, err := server.findPoolerForMigration(ctx, nil)
	require.NoError(t, err, "a static leader on an unrelated shard must not break an unscoped wildcard search")
}

func TestBackup_ForcePrimary(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	logger := slog.Default()
	server := NewMultiadminServer(ts, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer server.backupJobTracker.Stop()

	// Create a leader (self_leadership set) and a follower (self_leadership nil).
	// findPoolerForBackup selects on self_leadership, not on the topology Type.
	primaryPooler := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary-pooler",
		},
		Hostname: "primary-pooler.cell1",
		PortMap:  map[string]int32{"grpc": 8081},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
		},
		Type: clustermetadatapb.PoolerType_PRIMARY,
		RoutingState: &clustermetadatapb.RoutingState{
			Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY,
			Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
		},
	}
	replicaPooler := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "replica-pooler",
		},
		Hostname: "replica-pooler.cell1",
		PortMap:  map[string]int32{"grpc": 8081},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
		},
		Type: clustermetadatapb.PoolerType_REPLICA,
	}
	require.NoError(t, ts.CreateMultipooler(ctx, primaryPooler))
	require.NoError(t, ts.CreateMultipooler(ctx, replicaPooler))

	fakeClient := rpcclient.NewFakeClient()
	fakeClient.BackupResponses["multipooler-cell1-primary-pooler"] = &multipoolermanagerdata.BackupResponse{
		BackupId: "20251203-120000F",
	}
	fakeClient.BackupResponses["multipooler-cell1-replica-pooler"] = &multipoolermanagerdata.BackupResponse{
		BackupId: "20251203-120001F",
	}
	server.SetRPCClient(fakeClient)

	t.Run("force_primary=true uses primary pooler", func(t *testing.T) {
		fakeClient.ResetCallLog()

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
		require.Contains(t, fakeClient.GetCallLog(), "Backup(multipooler-cell1-primary-pooler)")
	})

	t.Run("force_primary=false uses replica pooler", func(t *testing.T) {
		fakeClient.ResetCallLog()

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
		require.Contains(t, fakeClient.GetCallLog(), "Backup(multipooler-cell1-replica-pooler)")
	})

	t.Run("force_primary=false with no replicas returns error", func(t *testing.T) {
		// Create a new topology with only primary pooler
		ctx := t.Context()
		ts := memorytopo.NewServer(ctx, "cell2")
		server := NewMultiadminServer(ts, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
		defer server.backupJobTracker.Stop()

		// Only create a primary pooler (no replica)
		primaryOnly := &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell2",
				Name:      "primary-only",
			},
			Hostname: "primary-only.cell2",
			PortMap:  map[string]int32{"grpc": 8081},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
			RoutingState: &clustermetadatapb.RoutingState{
				Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY,
				Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
			},
		}
		require.NoError(t, ts.CreateMultipooler(ctx, primaryOnly))

		req := &multiadminpb.BackupRequest{
			Database:     "testdb",
			TableGroup:   "default",
			Type:         "full",
			ForcePrimary: false,
		}

		_, err := server.Backup(ctx, req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "follower pooler not found")
	})
}

func TestVerifyBackups(t *testing.T) {
	logger := slog.Default()

	t.Run("validation errors", func(t *testing.T) {
		server := NewMultiadminServer(nil, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
		defer server.backupJobTracker.Stop()

		tests := []struct {
			name    string
			req     *multiadminpb.VerifyBackupsRequest
			wantErr codes.Code
		}{
			{
				name:    "empty database",
				req:     &multiadminpb.VerifyBackupsRequest{Database: "", TableGroup: "default"},
				wantErr: codes.InvalidArgument,
			},
			{
				name:    "empty table_group",
				req:     &multiadminpb.VerifyBackupsRequest{Database: "postgres", TableGroup: ""},
				wantErr: codes.InvalidArgument,
			},
			{
				name:    "empty shard",
				req:     &multiadminpb.VerifyBackupsRequest{Database: "postgres", TableGroup: "default", Shard: ""},
				wantErr: codes.InvalidArgument,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := server.VerifyBackups(t.Context(), tt.req)
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, tt.wantErr, st.Code())
			})
		}
	})

	t.Run("no replica pooler found", func(t *testing.T) {
		ctx := t.Context()
		ts := memorytopo.NewServer(ctx, "cell1")
		server := NewMultiadminServer(ts, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
		defer server.backupJobTracker.Stop()

		_, err := server.VerifyBackups(ctx, &multiadminpb.VerifyBackupsRequest{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0-inf",
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("success forwards to replica pooler", func(t *testing.T) {
		ctx := t.Context()
		ts := memorytopo.NewServer(ctx, "cell1")
		server := NewMultiadminServer(ts, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
		defer server.backupJobTracker.Stop()

		replicaPooler := &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica-pooler",
			},
			Hostname: "replica-pooler.cell1",
			PortMap:  map[string]int32{"grpc": 8081},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		require.NoError(t, ts.CreateMultipooler(ctx, replicaPooler))

		fakeClient := rpcclient.NewFakeClient()
		server.SetRPCClient(fakeClient)

		resp, err := server.VerifyBackups(ctx, &multiadminpb.VerifyBackupsRequest{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0-inf",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Contains(t, fakeClient.GetCallLog(), "VerifyBackups(multipooler-cell1-replica-pooler)")
	})
}
