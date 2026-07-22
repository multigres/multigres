// Copyright 2026 Supabase, Inc.
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

package multiorch

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestOverRecruitedReplicaReconnect covers a spurious partial failover that does
// NOT depose the leader: a single follower is Recruit()'d at a new term, which
// bumps that follower's per-pooler revocation above the current leader's rule.
// The follower now refuses to follow the leader (it will reject a SetPrimary for
// a rule whose term is below its revocation), so it sits disconnected while the
// leader and the rest of the cohort keep serving.
//
// multiorch must notice the stranded follower and reconnect it. Because plain
// SetPrimary cannot reattach a follower whose revocation is ahead of the rule,
// the expected remedy is a no-op leader-led rule change (same leader, same
// cohort, same quorum) that lifts the rule term past the stray revocation,
// after which the follower accepts SetPrimary and rejoins as a streaming standby.
//
// NOTE: this encodes the DESIRED end state and may fail against current code if
// that reconnect path is not yet implemented — it is a regression/spec test for
// the gap, not (yet) a guarantee.
func TestOverRecruitedReplicaReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping over-recruited replica reconnect test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end over-recruited replica reconnect test (no postgres binaries)")
	}

	// Three poolers under AT_LEAST_2: a majority (2 of 3) remains after one
	// follower is over-recruited, so the leader keeps serving and there is
	// headroom to fix the stray follower without a full failover.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithDurabilityPolicy("AT_LEAST_2"),
		shardsetup.WithMultigateway(),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)

	primaryName := waitForShardReady(t, setup, 2 /*expectedStandbyCount*/, 60*time.Second)
	t.Logf("Bootstrap complete; primary=%s (3-member cohort)", primaryName)

	// Pick one follower to over-recruit — any pooler that is not the primary.
	var targetReplica string
	for name := range setup.Multipoolers {
		if name != primaryName {
			targetReplica = name
			break
		}
	}
	require.NotEmpty(t, targetReplica, "expected at least one non-primary pooler")
	t.Logf("Over-recruiting single follower: %s", targetReplica)

	// Quiet multiorch while we bump the target's term, so it does not race our
	// manual Recruit. We re-enable below and let it drive the reconnect.
	resumeRecovery := setup.DisableRecovery(t, "multiorch")

	// Open per-pooler clients: consensus for Recruit, manager for Status snapshots.
	type poolerClient struct {
		name      string
		consensus consensuspb.MultipoolerConsensusClient
		manager   multipoolermanagerpb.MultipoolerManagerClient
	}
	poolerClients := make(map[string]*poolerClient, len(setup.Multipoolers))
	for name, inst := range setup.Multipoolers {
		conn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", inst.Multipooler.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err, "dial multipooler %s", name)
		t.Cleanup(func() { conn.Close() })
		poolerClients[name] = &poolerClient{
			name:      name,
			consensus: consensuspb.NewMultipoolerConsensusClient(conn),
			manager:   multipoolermanagerpb.NewMultipoolerManagerClient(conn),
		}
	}

	// Snapshot current consensus statuses so we can build a revocation at the next
	// term (the same helper multiorch uses during AppointLeader).
	statuses := make([]*clustermetadatapb.ConsensusStatus, 0, len(poolerClients))
	// Baseline each pooler's revocation term. Healing must leave every pooler other
	// than the target exactly here — a failover or coordinator-led rule change would
	// re-recruit the cohort and bump these.
	baseRevoke := make(map[string]int64, len(poolerClients))
	for _, pc := range poolerClients {
		resp, err := pc.manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "Status on %s", pc.name)
		require.NotNil(t, resp.GetConsensusStatus(), "ConsensusStatus from %s", pc.name)
		statuses = append(statuses, resp.GetConsensusStatus())
		baseRevoke[pc.name] = resp.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm()
	}

	testCoordinatorID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}
	revocation, err := commonconsensus.NewTermRevocation(statuses, testCoordinatorID, timestamppb.Now())
	require.NoError(t, err, "build term revocation")
	t.Logf("Recruiting only %s at new term %d (leader %s stays in place)", targetReplica, revocation.GetRevokedBelowTerm(), primaryName)

	// Recruit ONLY the target follower — do not touch the leader or the other
	// follower. This strands the target ahead of the leader's rule.
	_, err = poolerClients[targetReplica].consensus.Recruit(utils.WithTimeout(t, 30*time.Second), &consensusdatapb.RecruitRequest{
		TermRevocation: revocation,
	})
	require.NoError(t, err, "Recruit on %s", targetReplica)

	// Diagnostic: capture the target's state immediately after Recruit, before
	// recovery runs — this shows whether Recruit actually stranded it (replication
	// stopped, primary_conninfo cleared, revoked above the leader's rule).
	{
		resp, err := poolerClients[targetReplica].manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "post-Recruit Status on %s", targetReplica)
		rs := resp.GetStatus().GetReplicationStatus()
		t.Logf("post-Recruit %s: pooler_type=%v wal_receiver=%q primary_conninfo_host=%q last_receive_lsn=%q revoked_below_term=%d",
			targetReplica, resp.GetStatus().GetPoolerType(), rs.GetWalReceiverStatus(), rs.GetPrimaryConnInfo().GetHost(),
			rs.GetLastReceiveLsn(), resp.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm())
	}

	// Hand control back to multiorch and require it to reconnect the stranded
	// follower and settle to a problem-free state.
	resumeRecovery()
	waitForNodeToRejoinAsStandby(t, setup, targetReplica, primaryName, revocation.GetRevokedBelowTerm(), 30*time.Second)
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioFixReplication)

	// Healing must be purely replication-level: no failover, no coordinator-led
	// rule change, and no re-recruitment of the untouched poolers. The leader stays
	// put and every OTHER pooler's revocation is exactly what it was before — a
	// failover or coordinator rule change would re-recruit the cohort and bump these.
	for _, pc := range poolerClients {
		resp, err := pc.manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "post-heal Status on %s", pc.name)
		finalRevoke := resp.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm()
		t.Logf("post-heal %s: type=%v revoked_below_term=%d (baseline %d)",
			pc.name, resp.GetStatus().GetPoolerType(), finalRevoke, baseRevoke[pc.name])
		if pc.name == targetReplica {
			continue
		}
		require.Equal(t, baseRevoke[pc.name], finalRevoke,
			"%s revocation changed: healing must not recruit other poolers or run a coordinator-led rule change", pc.name)
		if pc.name == primaryName {
			require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, resp.GetStatus().GetPoolerType(),
				"leader %s must still be primary — healing must not trigger failover", primaryName)
		}
	}
}
