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

// Package multiorch contains end-to-end tests for multiorch behavior.
//
// These tests exercise how a stuck (undecided) proposal — left behind by a
// coordinator that crashed between UpdateRule's two write phases, see
// rule_store.go's two-phase write — interacts with the ways a rule can
// change:
//
//   - TestCohortChangeRejectedWithStuckProposal: an ordinary rule change
//     that also changes cohort or durability policy has no way to know
//     whether the stuck proposal reflects durable, quorum-verified work,
//     so it fails closed rather than risk silently discarding it.
//   - TestApplyCertifiedRuleChange_FromStuckProposal: an externally-certified
//     rule change (multiadmin.ApplyCertifiedRuleChange) can supersede it,
//     because the caller's cert — not quorum verification of the stored
//     decision — is what establishes safety here.
//   - TestStuckProposalAutoRecovers: ordinary automatic failover (no cert)
//     also recovers it, as long as the failover doesn't also change cohort
//     or durability policy — see buildProposalCore/validateProposal. The
//     fresh write the new leader performs can't get its own synchronous ack
//     unless the stuck proposal it supersedes was already durable, so no
//     separate quorum-verification step is needed.
package multiorch

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminserver "github.com/multigres/multigres/go/services/multiadmin"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestCohortChangeRejectedWithStuckProposal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestCohortChangeRejectedWithStuckProposal test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end stuck proposal test (short mode or no postgres binaries)")
	}

	// 3 nodes so there's a standby left after removing one from the cohort.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	t.Logf("Test cluster ready in directory: %s", setup.TempDir)
	t.Logf("Identified primary: %s", setup.PrimaryName)

	var replicaName string
	for name := range setup.Multipoolers {
		if name != setup.PrimaryName {
			replicaName = name
			break
		}
	}
	require.NotEmpty(t, replicaName, "should have a replica")

	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	ctx := context.Background()

	statusResp, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err, "Status should succeed")
	decision := statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetDecision()
	currentRule := decision.GetRuleNumber()
	require.NotNil(t, currentRule, "primary must have a current rule number")
	require.Nil(t, statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetProposal(),
		"sanity check: no proposal exists yet")

	// Seed a stuck proposal directly via raw SQL, bypassing UpdateRule entirely —
	// there is no write path that leaves one behind on demand, so this simulates
	// the crash-between-phases scenario the two-phase write is meant to survive.
	// Reuses the current decision's own leader/cohort/policy content; only the
	// rule number needs to be new to make it a distinct, observable proposal.
	proposedTerm := currentRule.GetCoordinatorTerm() + 1
	cohortNames := make([]string, len(decision.GetCohortMembers()))
	for i, m := range decision.GetCohortMembers() {
		cohortNames[i] = m.GetCell() + "_" + m.GetName()
	}
	cohortLiteral := "{" + strings.Join(cohortNames, ",") + "}"
	_, err = primaryClient.Pooler.ExecuteQuery(ctx, fmt.Sprintf(`
		UPDATE multigres.current_rule
		SET proposal_coordinator_term = %d,
		    proposal_leader_subterm = 0,
		    proposal_leader_id = '%s_%s',
		    proposal_cohort_members = '%s',
		    proposal_durability_policy_name = '%s',
		    proposal_durability_quorum_type = '%s',
		    proposal_durability_required_count = %d,
		    proposal_created_at = now()
		WHERE shard_id = '0'::bytea`,
		proposedTerm,
		decision.GetLeaderId().GetCell(), decision.GetLeaderId().GetName(),
		cohortLiteral,
		decision.GetDurabilityPolicy().GetPolicyName(),
		decision.GetDurabilityPolicy().GetQuorumType().String(),
		decision.GetDurabilityPolicy().GetRequiredCount(),
	), 0)
	require.NoError(t, err, "seeding the stuck proposal should succeed")

	// The stuck proposal must now be observable.
	statusResp, err = primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err, "Status should succeed")
	proposal := statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetProposal()
	require.NotNil(t, proposal, "the seeded proposal must be observable")
	assert.Equal(t, proposedTerm, proposal.GetRuleNumber().GetCoordinatorTerm())

	// An ordinary cohort change must refuse to proceed while that proposal is
	// undecided: it has no independent safety backing (no cert, no verified
	// quorum) to know whether overwriting it would discard durable work.
	_, err = primaryClient.Consensus.UpdateConsensusRule(ctx, &multipoolermanagerdatapb.UpdateConsensusRuleRequest{
		Operation: multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE,
		StandbyIds: []*clustermetadatapb.ID{{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "test-cell",
			Name:      replicaName,
		}},
		ExpectedOutgoingRule: currentRule,
	})
	require.Error(t, err, "cohort change must be rejected while a proposal is stuck")
	assert.ErrorContains(t, err, "undecided proposal")

	// The decision must be untouched — the rejected write must not have
	// partially applied.
	statusResp, err = primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err, "Status should succeed")
	assert.Equal(t, currentRule.GetCoordinatorTerm(),
		statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm(),
		"decision must be unchanged by the rejected cohort change")
}

func TestApplyCertifiedRuleChange_FromStuckProposal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestApplyCertifiedRuleChange_FromStuckProposal test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end stuck proposal test (short mode or no postgres binaries)")
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

	var newLeaderName string
	for name := range setup.Multipoolers {
		if name != oldPrimaryName {
			newLeaderName = name
			break
		}
	}
	require.NotEmpty(t, newLeaderName, "should have a standby to promote")

	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	ctx := t.Context()

	statusResp, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err, "Status should succeed")
	decision := statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetDecision()
	currentTerm := decision.GetRuleNumber().GetCoordinatorTerm()

	var poolerIDs []*clustermetadatapb.ID
	for _, name := range []string{"pooler-1", "pooler-2", "pooler-3"} {
		poolerIDs = append(poolerIDs, &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      setup.CellName,
			Name:      name,
		})
	}

	// Restart multiorch (NewIsolated stops the bootstrap multiorch once the
	// shard is up) and pause its automatic recovery: ApplyCertifiedRuleChange
	// needs a live multiorch to dispatch to, and disabling recovery keeps this
	// test deterministic (it's exercising the manual/CLI path specifically).
	setup.StartMultiorchs(t.Context(), t)
	defer setup.DisableRecovery(t, "multiorch")()

	// Seed a stuck proposal directly via raw SQL on the current primary,
	// bypassing UpdateRule entirely — there is no write path that leaves one
	// behind on demand, so this simulates a coordinator that crashed between
	// UpdateRule's two write phases (see rule_store.go). The seeded proposal
	// keeps the same leader/cohort/policy as the current decision; only the
	// rule number is new, since it's simulating an in-flight cohort or policy
	// change, not a leader change.
	stuckTerm := currentTerm + 1
	_, err = primaryClient.Pooler.ExecuteQuery(ctx, fmt.Sprintf(`
		UPDATE multigres.current_rule
		SET proposal_coordinator_term = %d,
		    proposal_leader_subterm = 0,
		    proposal_leader_id = leader_id,
		    proposal_cohort_members = cohort_members,
		    proposal_durability_policy_name = durability_policy_name,
		    proposal_durability_quorum_type = durability_quorum_type,
		    proposal_durability_required_count = durability_required_count,
		    proposal_created_at = now()
		WHERE shard_id = '0'::bytea`, stuckTerm), 0)
	require.NoError(t, err, "seeding the stuck proposal should succeed")

	statusResp, err = primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetProposal(),
		"the seeded proposal must be observable before we try to recover from it")

	// Kill the old primary now, matching the scenario this whole feature is
	// about: a leader that crashed leaving a stuck proposal behind, not one
	// that's still alive and needs a graceful demotion. This also sidesteps
	// pg_rewind entirely — a live old primary that kept running would need it
	// to rejoin (its WAL diverges from the new leader's timeline the moment
	// a different node is promoted), which is an orthogonal concern to what
	// this test is exercising.
	oldPrimaryInst := setup.GetMultipoolerInstance(oldPrimaryName)
	require.NotNil(t, oldPrimaryInst)
	killMultipooler(t, oldPrimaryInst)

	// Build an externally-certified rule change whose outgoing rule is the
	// stuck proposal itself (not the last marked decision): the caller
	// (a human/CLI operator, standing in for future automated propagation)
	// has independently determined it's safe to treat the stuck proposal as
	// the true baseline, and attests to that via the cert rather than
	// relying on this multipooler ever having marked it decided.
	orchProtoID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      setup.CellName,
		Name:      "multiorch",
	}
	newLeaderID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      setup.CellName,
		Name:      newLeaderName,
	}
	now := timestamppb.New(time.Now().UTC().Truncate(time.Microsecond))
	newTerm := stuckTerm + 1

	req := &multiadminpb.ApplyCertifiedRuleChangeRequest{
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "postgres",
			TableGroup: "default",
			Shard:      "0-inf",
		},
		ProposedTransition: &clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: stuckTerm},
			},
			Proposal: &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: newTerm},
				LeaderId:         newLeaderID,
				CohortMembers:    poolerIDs,
				DurabilityPolicy: decision.GetDurabilityPolicy(),
				CoordinatorId:    orchProtoID,
				CreationTime:     now,
			},
		},
		CertSource: &multiadminpb.ApplyCertifiedRuleChangeRequest_Cert{
			Cert: &clustermetadatapb.ExternallyCertifiedRevocation{
				// Permissive: this test is exercising that the stuck proposal
				// is accepted as the outgoing baseline, not re-deriving a real
				// quorum-verified WAL floor.
				FrozenLsn: "0/0",
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       newTerm,
					AcceptedCoordinatorId:  orchProtoID,
					CoordinatorInitiatedAt: now,
					OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: stuckTerm, LeaderSubterm: 0},
				},
			},
		},
		Reason: "test-recover-stuck-proposal",
	}

	logger := slog.Default()
	adminServer := adminserver.NewMultiadminServer(
		setup.TopoServer, logger, grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer adminServer.Stop()

	// The cohort still carries a recent term-acceptance record from the
	// bootstrap multiorch's own recruitment a moment ago; recruiting again
	// within that backoff window is deliberately rejected to avoid two
	// coordinators racing (see checkRecentAcceptance). Retry past it.
	var resp *multiadminpb.ApplyCertifiedRuleChangeResponse
	require.Eventually(t, func() bool {
		var applyErr error
		applyCtx := utils.WithTimeout(t, 15*time.Second)
		resp, applyErr = adminServer.ApplyCertifiedRuleChange(applyCtx, req)
		if applyErr != nil {
			t.Logf("ApplyCertifiedRuleChange not ready yet: %v", applyErr)
			return false
		}
		return true
	}, 10*time.Second, 500*time.Millisecond, "ApplyCertifiedRuleChange should succeed starting from a stuck proposal")
	require.NotNil(t, resp.InstalledRule)
	assert.Equal(t, newTerm, resp.InstalledRule.GetRuleNumber().GetCoordinatorTerm())
	assert.Equal(t, newLeaderName, resp.InstalledRule.GetLeaderId().GetName())

	// The new leader must be a decided (not stuck) rule at the new term.
	// Only 1 standby is expected: the old primary is dead (killed above,
	// simulating the crash this scenario is about) and stays out of the
	// picture — recovering a live diverged node is a separate concern
	// (pg_rewind), not what this test exercises.
	setup.PrimaryName = newLeaderName
	primaryName := waitForShardReady(t, setup, 1 /* expectedStandbyCount */, 15*time.Second)
	require.Equal(t, newLeaderName, primaryName, "the promoted node should become primary")

	newPrimaryClient := setup.NewPrimaryClient(t)
	defer newPrimaryClient.Close()
	statusResp, err = newPrimaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	newPos := statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition()
	assert.Equal(t, newTerm, newPos.GetDecision().GetRuleNumber().GetCoordinatorTerm(),
		"the stuck proposal is superseded by a fresh decided rule, exactly like any other promotion")
	assert.Nil(t, newPos.GetProposal(), "the new leader's own write is fully decided, not stuck")
}

// TestStuckProposalAutoRecovers shows that ordinary automatic failover — no
// cert, no manual CLI/admin intervention — also recovers a stuck proposal on
// its own, as long as the failover doesn't also change cohort or durability
// policy. Complements TestApplyCertifiedRuleChange_FromStuckProposal, which
// needs a cert specifically because multiadmin's flow allows changing
// cohort/policy in the same step; ordinary failover here keeps them
// unchanged, so no cert is needed at all.
func TestStuckProposalAutoRecovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestStuckProposalAutoRecovers test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end stuck proposal test (short mode or no postgres binaries)")
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

	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	ctx := t.Context()

	statusResp, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err, "Status should succeed")
	decision := statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetDecision()
	currentTerm := decision.GetRuleNumber().GetCoordinatorTerm()

	// Restart multiorch (NewIsolated stops the bootstrap multiorch once the
	// shard is up) and pause its automatic recovery while we set up the
	// stuck-proposal scenario, so it doesn't race the seeding below.
	setup.StartMultiorchs(t.Context(), t)
	resumeRecovery := setup.DisableRecovery(t, "multiorch")

	// Seed a stuck proposal directly via raw SQL on the current primary,
	// bypassing UpdateRule entirely — see the sibling tests in this file for
	// why. Same leader/cohort/policy as the current decision: the whole
	// point of this test is that ordinary failover can recover it without a
	// cert precisely because nothing but the leader is changing.
	stuckTerm := currentTerm + 1
	_, err = primaryClient.Pooler.ExecuteQuery(ctx, fmt.Sprintf(`
		UPDATE multigres.current_rule
		SET proposal_coordinator_term = %d,
		    proposal_leader_subterm = 0,
		    proposal_leader_id = leader_id,
		    proposal_cohort_members = cohort_members,
		    proposal_durability_policy_name = durability_policy_name,
		    proposal_durability_quorum_type = durability_quorum_type,
		    proposal_durability_required_count = durability_required_count,
		    proposal_created_at = now()
		WHERE shard_id = '0'::bytea`, stuckTerm), 0)
	require.NoError(t, err, "seeding the stuck proposal should succeed")

	// A real stuck proposal always has a matching decided=false rule_history
	// row, inserted by the propose step of the two-phase UpdateRule write it got stuck
	// partway through. markProposalAsDecision's finalize step (propagation)
	// depends on that row existing, so the raw-SQL seed above must mirror it
	// too, copying the decision's own leader/cohort/policy since nothing else
	// is changing here.
	_, err = primaryClient.Pooler.ExecuteQuery(ctx, fmt.Sprintf(`
		INSERT INTO multigres.rule_history
		  (coordinator_term, leader_subterm, event_type, leader_id, coordinator_id,
		   wal_position, operation, reason, cohort_members, accepted_members,
		   durability_policy_name, durability_quorum_type, durability_required_count,
		   decided, created_at)
		SELECT %d, 0, 'promotion', leader_id, coordinator_id, '0/0', 'promotion',
		       'seeded stuck proposal', cohort_members, cohort_members,
		       durability_policy_name, durability_quorum_type, durability_required_count,
		       false, now()
		FROM multigres.current_rule
		WHERE shard_id = '0'::bytea`, stuckTerm), 0)
	require.NoError(t, err, "seeding the stuck proposal's rule_history row should succeed")

	statusResp, err = primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetProposal(),
		"the seeded proposal must be observable before we try to recover from it")

	// Kill postgres (not the multipooler process) on the old primary: the
	// scenario this whole feature is about is a leader that crashed leaving
	// a stuck proposal behind, not one that's still alive. Keeping
	// multipooler running lets it report "postgres not running" to
	// multiorch's health poller, which is what actually drives automatic
	// failover here — a dead multipooler process would only be detected as
	// unreachable, a slower and different signal. Disable restarts first so
	// the monitor doesn't bring postgres back up before failover runs. The
	// two surviving standbys already replicated the same stuck proposal
	// (current_rule is an ordinary replicated table), so either is eligible
	// to propagate it.
	oldPrimaryInst := setup.GetMultipoolerInstance(oldPrimaryName)
	require.NotNil(t, oldPrimaryInst)
	oldPrimaryManagerClient, err := shardsetup.NewMultipoolerClient(oldPrimaryInst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer oldPrimaryManagerClient.Close()
	_, err = oldPrimaryManagerClient.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)

	setup.KillPostgres(t, oldPrimaryName)

	// Let multiorch auto-recover: no cert, no manual RPC, just ordinary
	// automatic failover trusting the undecided proposal as its outgoing
	// baseline (see NewTermRevocation/buildProposalCore).
	resumeRecovery()
	shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 30*time.Second)

	// Re-enable restarts on the old primary now that a new leader is in
	// place: it comes back as a stale primary, and RequireRecovery's
	// problem-free bar can't be met while it stays permanently down.
	_, err = oldPrimaryManagerClient.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err)

	setup.RequireRecovery(t, "multiorch", 20*time.Second)

	newPrimary := setup.RefreshPrimary(t)
	require.NotNil(t, newPrimary)
	require.NotEqual(t, oldPrimaryName, newPrimary.Name, "a surviving standby should have been promoted")
	setup.PrimaryName = newPrimary.Name

	newPrimaryClient := setup.NewPrimaryClient(t)
	defer newPrimaryClient.Close()
	statusResp, err = newPrimaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	newPos := statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition()
	assert.Greater(t, newPos.GetDecision().GetRuleNumber().GetCoordinatorTerm(), stuckTerm,
		"the stuck proposal is superseded by a fresh decided rule at a higher term")
	assert.Nil(t, newPos.GetProposal(), "the new leader's own write is fully decided, not stuck")
}
