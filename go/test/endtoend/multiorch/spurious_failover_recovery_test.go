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

// TestSpuriousFailoverRecovery as a regression test: after a bootstrap that
// commits a minimum-size cohort (here, 2 members under AT_LEAST_2 — the
// production scenario where one of three intended poolers had not finished
// restoring when the orch claimed), a coordinator-led rule change Recruits
// all cohort members, which demotes the outgoing primary. The
// cluster must recover from that state on its own — multiorch should
// re-recruit, promote a new leader, and clear any lingering pooler-side
// state (e.g. rewindPending) so no problems remain.
//
// A 2-member cohort with AT_LEAST_2 has zero failover headroom (2-of-2
// outgoing quorum required), so the demoted primary must still be able
// to participate in consensus progress.
// We drive only the Recruit step and let multiorch handle the rest, then
// assert recovery completes with no remaining problems.
func TestSpuriousFailoverRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping spurious failover recovery test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end spurious failover recovery test (no postgres binaries)")
	}

	// Use only two poolers so bootstrap commits a 2-member cohort, matching
	// the production MUL-505 scenario where headroom was zero.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithDurabilityPolicy("AT_LEAST_2"),
		shardsetup.WithMultigateway(),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)

	// Wait for bootstrap to complete and sync replication to be configured on
	// the single standby. The committed cohort size is 2.
	primaryName := waitForShardReady(t, setup, 1 /*expectedStandbyCount*/, 60*time.Second)
	t.Logf("Bootstrap complete; primary=%s (cohort size = 2)", primaryName)

	// Quiet multiorch while we issue the Recruit fan-out so it does not race
	// our manual rule change. We re-enable below and let multiorch drive
	// recovery from the resulting state.
	resumeRecovery := setup.DisableRecovery(t, "multiorch")

	// Open gRPC clients to each pooler — consensus for Recruit fan-out,
	// manager for Status snapshots (ConsensusStatus is delivered as part of
	// the manager Status response).
	type poolerClient struct {
		name      string
		consensus consensuspb.MultiPoolerConsensusClient
		manager   multipoolermanagerpb.MultiPoolerManagerClient
	}
	poolerClients := make([]*poolerClient, 0, len(setup.Multipoolers))
	for name, inst := range setup.Multipoolers {
		conn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", inst.Multipooler.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err, "dial multipooler %s", name)
		t.Cleanup(func() { conn.Close() })
		poolerClients = append(poolerClients, &poolerClient{
			name:      name,
			consensus: consensuspb.NewMultiPoolerConsensusClient(conn),
			manager:   multipoolermanagerpb.NewMultiPoolerManagerClient(conn),
		})
	}

	// Snapshot the current consensus statuses so we can build a fresh
	// TermRevocation at the next term.
	statuses := make([]*clustermetadatapb.ConsensusStatus, 0, len(poolerClients))
	for _, pc := range poolerClients {
		resp, err := pc.manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "Status on %s", pc.name)
		require.NotNil(t, resp.GetConsensusStatus(), "ConsensusStatus from %s", pc.name)
		statuses = append(statuses, resp.GetConsensusStatus())
	}

	// Build the new revocation using the same helper multiorch uses during
	// AppointLeader. A synthetic test coordinator ID is sufficient.
	testCoordinatorID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}
	revocation, err := commonconsensus.NewTermRevocation(statuses, testCoordinatorID, timestamppb.Now())
	require.NoError(t, err, "build term revocation")
	t.Logf("Recruiting at new term: %d", revocation.GetRevokedBelowTerm())

	// Fan out Recruit RPCs to all three poolers concurrently. The current
	// primary will emergency-demote as a side effect (rpc_consensus.go's
	// Recruit handler routes through emergencyDemoteLocked when isPrimary).
	// We deliberately do NOT issue a follow-up Promote — recovery is
	// multiorch's job once we re-enable it below.
	type recruitResult struct {
		name string
		err  error
	}
	recruitCh := make(chan recruitResult, len(poolerClients))
	for _, pc := range poolerClients {
		go func(pc *poolerClient) {
			_, callErr := pc.consensus.Recruit(utils.WithTimeout(t, 30*time.Second), &consensusdatapb.RecruitRequest{
				TermRevocation: revocation,
			})
			recruitCh <- recruitResult{name: pc.name, err: callErr}
		}(pc)
	}
	for range poolerClients {
		r := <-recruitCh
		require.NoError(t, r.err, "Recruit on %s", r.name)
	}
	t.Logf("Recruit complete at term %d — original primary %s has been emergency-demoted", revocation.GetRevokedBelowTerm(), primaryName)

	// Hand control back to multiorch and require it to bring the shard back
	// to a problem-free state. RequireRecovery loops TriggerRecoveryNow until
	// no problem codes remain, or fails the test on timeout.
	resumeRecovery()
	setup.RequireRecovery(t, "multiorch", 90*time.Second)
}
