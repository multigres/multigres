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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/ha"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// statusOf is a small helper: fetch a fresh Status for the named pooler.
func statusOf(t *testing.T, setup *shardsetup.ShardSetup, name string) *multipoolermanagerdatapb.StatusResponse {
	t.Helper()
	inst := setup.GetMultipoolerInstance(name)
	require.NotNil(t, inst)
	client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()
	resp, err := client.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	return resp
}

// TestLeaderNeverPromoted_AutoRecovers is a regression test for a staging
// incident: a recruit round selects a new leader, but its Promote RPC is
// lost (network partition, timeout, ...) while the SetPrimary RPCs to the
// other cohort members still land. rule_change.go's dispatchPromote sends
// both as independent, concurrent RPCs — Promote to the selected leader,
// SetPrimary to every other cohort member — so this is a real, reachable
// partial-failure race in the existing dispatch, not a fabricated state.
//
// Simulated here via the same public Consensus RPCs a real recruit round
// uses (Recruit, then SetPrimary), driven directly by the test rather than
// through multiorch's Coordinator: go/services/multiorch/... is off-limits to
// e2e tests (see the multiorch-isolation depguard rule), so this can't drive
// Coordinator.AppointLeader in-process the way an in-package test could.
// Recruit is called on both live standbys first (as a real recruit round
// would), since a pooler's TermRevocation must already be durably accepted
// before its SetPrimary/Promote is meaningful — only the leader's Promote is
// skipped.
//
// The follower's SetPrimary handler calls RecordTermPrimary unconditionally
// ("record what we've been told, even if we don't end up applying the
// change"), which updates its own ConsensusStatus.ReplicationPrimary and
// feeds the gossiped highest_known_rule that multiorch's shard_analysis.go
// aggregates shard-wide. So after this, the shard's gossiped position names
// the selected-but-never-promoted pooler leader — even though it was never
// actually sent pg_promote() and is a perfectly ordinary, healthy standby.
// Meanwhile the real primary is dead (killed below, matching the live
// incident's own trigger), so nothing is actually primary at all.
//
// LeaderNeedsReplacementAnalyzer's rule-support axis catches this: the
// never-promoted standby's own consensus report never confirms it as leader
// (commonconsensus.IsActiveLeader), so it's convicted regardless of how
// healthy it otherwise looks.
func TestLeaderNeverPromoted_AutoRecovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestLeaderNeverPromoted_AutoRecovers test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end leader-never-promoted test (no postgres binaries)")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	t.Logf("Test cluster ready in directory: %s", setup.TempDir)
	t.Logf("Identified primary: %s", setup.PrimaryName)
	oldPrimaryName := setup.PrimaryName

	var standbyAName, standbyBName string
	for name := range setup.Multipoolers {
		if name == oldPrimaryName {
			continue
		}
		if standbyAName == "" {
			standbyAName = name
		} else {
			standbyBName = name
		}
	}
	require.NotEmpty(t, standbyAName, "should have a standby to (never) promote")
	require.NotEmpty(t, standbyBName, "should have a second standby to tell about the (lost) promotion")
	standbyAID := setup.GetMultipoolerID(standbyAName)
	require.NotNil(t, standbyAID)

	ctx := t.Context()

	// Snapshot the current decided rule before killing the primary below —
	// needed for ProposedTransition.Decision further down (the "from" side
	// of the transition), since it's unreachable once the primary is dead.
	oldDecision := statusOf(t, setup, oldPrimaryName).GetConsensusStatus().GetCurrentPosition().GetPosition().GetDecision()

	// Kill the real primary's Postgres for real: this is what actually
	// creates the need for a failover in the first place, mirroring the live
	// incident's own trigger ("reason=LeaderResigned"). Disable restarts
	// first so the monitor doesn't bring it back before we're done.
	oldPrimaryClient := setup.NewClient(t, oldPrimaryName)
	defer oldPrimaryClient.Close()
	_, err := oldPrimaryClient.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)
	setup.KillPostgres(t, oldPrimaryName)

	// Recruit both live standbys for real: this is the same public RPC (and
	// the same TermRevocation) a genuine coordinator-led recruit round would
	// use, and it's what actually makes the subsequent SetPrimary meaningful
	// (a pooler's TermRevocation must already be durably accepted). Only the
	// designated leader's Promote is skipped below — everything up to that
	// point is the real thing, not simulated.
	var statuses []*clustermetadatapb.ConsensusStatus
	for _, name := range []string{standbyAName, standbyBName} {
		statuses = append(statuses, statusOf(t, setup, name).GetConsensusStatus())
	}
	coordinatorID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      setup.CellName,
		Name:      "test-coordinator",
	}
	revocation, err := commonconsensus.NewTermRevocation(statuses, coordinatorID, timestamppb.Now(), ha.DefaultBackoffResetDuration())
	require.NoError(t, err)
	proposedTerm := revocation.GetRevokedBelowTerm()

	for _, name := range []string{standbyAName, standbyBName} {
		client := setup.NewClient(t, name)
		_, err := client.Consensus.Recruit(ctx, &consensusdatapb.RecruitRequest{TermRevocation: revocation})
		client.Close()
		require.NoError(t, err, "Recruit should succeed on %s", name)
	}

	// Build the same CoordinatorProposal a real recruit round would build
	// once quorum is reached (standbyA as leader, standbyB following) and
	// derive the ReplicationPrimary from it exactly as dispatchPromote's
	// promote() does for a follower. Then send it via SetPrimary to
	// standbyB only — simulating the lost-Promote/landed-SetPrimary race
	// without ever contacting standbyA.
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: revocation,
		ProposalLeader: &clustermetadatapb.PoolerAddress{
			Id:           standbyAID,
			Host:         "localhost",
			PostgresPort: int32(setup.GetMultipoolerInstance(standbyAName).Pgctld.PgPort),
		},
		ProposedTransition: &clustermetadatapb.RulePosition{
			Decision: oldDecision,
			Proposal: &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: proposedTerm},
				LeaderId:         standbyAID,
				CohortMembers:    oldDecision.GetCohortMembers(),
				DurabilityPolicy: oldDecision.GetDurabilityPolicy(),
			},
		},
	}
	standbyBClient := setup.NewClient(t, standbyBName)
	defer standbyBClient.Close()
	_, err = standbyBClient.Consensus.SetPrimary(ctx, &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: commonconsensus.ReplicationPrimaryFromProposal(proposal, false),
	})
	require.NoError(t, err, "SetPrimary (simulating the landed half of the lost-Promote race) should succeed on standbyB")

	t.Logf("never-promoted (gossiped) leader: %s; other standby (received SetPrimary): %s", standbyAName, standbyBName)

	// Let a real multiorch, with ordinary automatic recovery, run against
	// this state. This is the exact live-incident condition: nothing is
	// crash-looping, nothing else needs killing — one standby is live,
	// healthy, and fully caught up, but gossiped as leader without ever
	// having been promoted, and the real primary is dead. orch needs to
	// notice no one is actually primary and recover — some standby (not
	// necessarily standbyA; whichever the fresh recruit round picks) should
	// become the real new primary.
	setup.StartMultiorchs(t.Context(), t)
	shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, utils.ScaleTimeout(30*time.Second))

	// Re-enable restarts on the old primary now that a new leader is in
	// place: it comes back as a stale primary, and RequireRecovery's
	// problem-free bar can't be met while it stays permanently down.
	_, err = oldPrimaryClient.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err)

	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioStalePrimaryDemote)

	newPrimary := setup.RefreshPrimary(t)
	require.NotNil(t, newPrimary)
	require.NotEqual(t, oldPrimaryName, newPrimary.Name, "a surviving standby should have been promoted")
}
