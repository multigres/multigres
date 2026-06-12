// Copyright 2026 Supabase, Inc.
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

package multiadmin

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPlannedFailoverAutoSelectStandby verifies that a planned failover
// completes successfully when no target is specified: multiadmin picks the most
// advanced standby, waits for it to catch up to the frozen LSN, and promotes it
// without needing pg_rewind on the demoted primary.
func TestPlannedFailoverAutoSelectStandby(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithMultigateway(),
		shardsetup.WithMultiadmin(),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	initialPrimary := setup.GetPrimary(t)
	require.NotNil(t, initialPrimary)
	t.Logf("Initial primary: %s", initialPrimary.Name)

	// Write some rows through the gateway so we have data to verify after failover.
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	gatewayDB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer gatewayDB.Close()

	validator, validatorCleanup, err := shardsetup.NewWriterValidator(t, gatewayDB,
		shardsetup.WithWorkerCount(2),
		shardsetup.WithWriteInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(validatorCleanup)

	validator.Start(t)
	time.Sleep(200 * time.Millisecond)

	// Perform planned failover — no target, let multiadmin choose.
	adminClient, adminConn := newAdminClient(t, setup.MultiadminGrpcPort)
	defer adminConn.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	t.Log("Calling PlannedFailover (auto-select standby)...")
	resp, err := adminClient.SwitchPrimary(ctx, &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "postgres",
			TableGroup: "default",
			Shard:      "0-inf",
		},
		Reason:         "integration test",
		CatchupTimeout: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err, "SwitchPrimary should succeed")
	require.NotNil(t, resp.GetNewLeaderId())
	require.NotNil(t, resp.GetOldLeaderId())

	t.Logf("Failover complete: old=%s/%s, new=%s/%s",
		resp.GetOldLeaderId().GetCell(), resp.GetOldLeaderId().GetName(),
		resp.GetNewLeaderId().GetCell(), resp.GetNewLeaderId().GetName())

	// The new leader must be different from the old one.
	assert.NotEqual(t, resp.GetOldLeaderId().GetName(), resp.GetNewLeaderId().GetName(),
		"new leader should be different from old leader")

	// Wait for multiorch to fully converge (old primary demoted, replication established).
	setup.RequireRecovery(t, "multiorch", 45*time.Second)

	// The cluster should now have a new primary and the old primary rejoined as standby.
	newPrimary := setup.RefreshPrimary(t)
	require.NotNil(t, newPrimary)
	assert.Equal(t, resp.GetNewLeaderId().GetName(), newPrimary.Name,
		"cluster primary should match the failover response")

	t.Logf("Cluster primary after failover: %s", newPrimary.Name)

	// Verify the old primary is now a replica.
	oldPrimaryInst := setup.GetMultipoolerInstance(initialPrimary.Name)
	require.NotNil(t, oldPrimaryInst)
	verifyIsStandby(t, oldPrimaryInst)

	// Verify writes can still flow through the gateway after failover.
	require.Eventually(t, func() bool {
		return validator.WriteNow(t.Context()) == nil
	}, 15*time.Second, 200*time.Millisecond,
		"gateway should route writes to new primary after planned failover")

	// Verify no data was lost: all successful pre-failover writes must appear
	// on the new primary.
	validator.Stop()
	verifyWriteDurability(t, setup, validator)
}

// TestPlannedFailoverNoPgRewind verifies that the old primary rejoins as
// standby without pg_rewind: because writes were quiesced before the standby
// was promoted, the old primary's WAL is not diverged from the new timeline.
func TestPlannedFailoverNoPgRewind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithMultigateway(),
		shardsetup.WithMultiadmin(),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	initialPrimary := setup.GetPrimary(t)
	require.NotNil(t, initialPrimary)

	adminClient, adminConn := newAdminClient(t, setup.MultiadminGrpcPort)
	defer adminConn.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	resp, err := adminClient.SwitchPrimary(ctx, &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "postgres",
			TableGroup: "default",
			Shard:      "0-inf",
		},
		Reason:         "no-pg_rewind integration test",
		CatchupTimeout: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err, "SwitchPrimary should succeed")

	setup.RequireRecovery(t, "multiorch", 45*time.Second)

	// The old primary should rejoin as a streaming replica. If pg_rewind had
	// been needed but failed, the old primary would stay stuck in NOT_SERVING
	// or REQUESTING_DEMOTION state instead of becoming a healthy REPLICA.
	oldPrimaryInst := setup.GetMultipoolerInstance(resp.GetOldLeaderId().GetName())
	require.NotNil(t, oldPrimaryInst)

	shardsetup.EventuallyPoolerCondition(t,
		[]*shardsetup.MultipoolerInstance{oldPrimaryInst},
		30*time.Second, 500*time.Millisecond,
		func(r shardsetup.PoolerStatusResult) (bool, string) {
			s := r.Status
			if s.PoolerType != clustermetadatapb.PoolerType_REPLICA {
				return false, fmt.Sprintf("old primary not REPLICA yet (is %v)", s.PoolerType)
			}
			if !s.PostgresReady {
				return false, "postgres not running"
			}
			if s.ReplicationStatus == nil || s.ReplicationStatus.PrimaryConnInfo == nil {
				return false, "replication not configured"
			}
			if s.ReplicationStatus.WalReceiverStatus != "streaming" {
				return false, fmt.Sprintf("not streaming (wal_receiver=%s)", s.ReplicationStatus.WalReceiverStatus)
			}
			return true, ""
		},
		"old primary %s should rejoin as streaming replica without pg_rewind", resp.GetOldLeaderId().GetName(),
	)

	t.Logf("Old primary %s rejoined as streaming replica (no pg_rewind needed)",
		resp.GetOldLeaderId().GetName())
}

// verifyIsStandby asserts that a multipooler is a healthy streaming replica.
func verifyIsStandby(t *testing.T, inst *shardsetup.MultipoolerInstance) {
	t.Helper()
	client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()

	resp, err := client.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, resp.GetStatus().GetPoolerType(),
		"demoted primary %s should be REPLICA", inst.Name)
	assert.NotNil(t, resp.GetStatus().GetReplicationStatus(),
		"demoted primary %s should have replication status", inst.Name)
}

// verifyWriteDurability checks that all writes recorded by the validator are
// present on the (new) primary's postgres.
func verifyWriteDurability(t *testing.T, setup *shardsetup.ShardSetup, validator *shardsetup.WriterValidator) {
	t.Helper()
	primaryInst := setup.GetPrimary(t)
	require.NotNil(t, primaryInst)

	poolerClient, err := shardsetup.NewMultiPoolerTestClient(
		fmt.Sprintf("localhost:%d", primaryInst.Multipooler.GrpcPort),
	)
	require.NoError(t, err)
	defer poolerClient.Close()

	err = validator.Verify(t, []*shardsetup.MultiPoolerTestClient{poolerClient})
	require.NoError(t, err, "all successful pre-failover writes should be present on the new primary")
}
