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
// TestBootstrap_ViaExternalAPI verifies the manual-bootstrap path:
// multiadmin.ApplyCertifiedRuleChange installs the initial shard rule when
// multiorch's automatic recovery is disabled. This is the same RPC the
// `multigres cluster apply-rule-change` CLI invokes, and the path a
// provisioner would use to bring up a fresh shard without relying on
// multiorch's auto-bootstrap.
package multiorch

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	adminserver "github.com/multigres/multigres/go/services/multiadmin"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestBootstrap_ViaExternalAPI brings up an uninitialized 3-node cluster
// with multiorch's automatic recovery disabled, then bootstraps the shard
// by calling multiadmin.ApplyCertifiedRuleChange (the same RPC the
// `multigres cluster apply-rule-change` CLI invokes). After the RPC
// returns, the test asserts that the cohort is initialized at term 1
// with the chosen leader and synchronous replication configured.
//
// To avoid racing against multiorch's auto-bootstrap, the test relies on
// the ordering that the shardsetup helpers create: pgctld is started, but
// multipoolers are deferred (WithDeferredMultipoolerStart). Multiorch is
// started before any pooler registers — so its first recovery cycles have
// no work to do. DisableRecovery is called before the multipoolers come
// up, so no auto-bootstrap can fire after registration.
func TestBootstrap_ViaExternalAPI(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end bootstrap test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end bootstrap test (no postgres binaries)")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDeferredMultipoolerStart(),
		shardsetup.WithDurabilityPolicy("AT_LEAST_2"),
	)
	defer cleanup()

	// Start multiorch BEFORE multipoolers register, so its first recovery
	// cycles find no poolers to act on.
	setup.StartMultiorchs(t.Context(), t)

	// Belt-and-suspenders: disable recovery before any pooler registers.
	// If recovery were left enabled, the watcher would eventually discover
	// the newly-started poolers and multiorch would attempt to auto-bootstrap.
	setup.DisableRecovery(t, "multiorch")

	// Now bring the multipoolers up. They register themselves in topology
	// but stay uninitialized (no postgres data dir, no rule).
	for name, inst := range setup.Multipoolers {
		require.NoError(t, inst.Multipooler.Start(t.Context(), t), "should start multipooler %s", name)
		shardsetup.WaitForManagerReady(t, inst.Multipooler)
	}

	// Confirm uninitialized state on every pooler.
	for name, inst := range setup.Multipoolers {
		client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		require.NoError(t, err)
		status, err := client.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
		client.Close()
		require.NoError(t, err)
		require.Empty(t, status.Status.CohortMembers, "pooler %s should have no cohort before CLI bootstrap", name)
		require.NotEqual(t, multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY, status.Status.PostgresStatus,
			"pooler %s should not be primary before CLI bootstrap", name)
	}

	// Pick the leader and build the proposed-rule cohort. Use stable
	// ordering so the test is deterministic.
	var poolerIDs []*clustermetadatapb.ID
	for _, name := range []string{"pooler-1", "pooler-2", "pooler-3"} {
		inst := setup.GetMultipoolerInstance(name)
		require.NotNil(t, inst, "expected pooler %s to exist", name)
		poolerIDs = append(poolerIDs, &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      setup.CellName,
			Name:      name,
		})
	}
	leaderID := poolerIDs[0]

	// Build the bootstrap request: zero outgoing rule, frozen_lsn="0/0",
	// fully-populated identity and timing fields. Multiadmin will fill in
	// any of those left blank, but populating them explicitly here mirrors
	// what the CLI does and exercises the strict-validation path in
	// multiorch.
	orchInst := setup.MultiorchInstances["multiorch"]
	require.NotNil(t, orchInst)
	orchProtoID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      setup.CellName,
		Name:      "multiorch",
	}
	// Truncate to microsecond before sending so the round-trip through
	// postgres TIMESTAMP (microsecond-precision, rounds half-up at the
	// nanosecond boundary) is exact and assertion-friendly.
	now := timestamppb.New(time.Now().UTC().Truncate(time.Microsecond))
	durability, err := commonconsensus.ParseUserSpecifiedDurabilityPolicy("AT_LEAST_2")
	require.NoError(t, err)

	req := &multiadminpb.ApplyCertifiedRuleChangeRequest{
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "postgres",
			TableGroup: "default",
			Shard:      "0-inf",
		},
		ProposedRule: &clustermetadatapb.ShardRule{
			RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			LeaderId:         leaderID,
			CohortMembers:    poolerIDs,
			DurabilityPolicy: durability,
			CoordinatorId:    orchProtoID,
			CreationTime:     now,
		},
		CertSource: &multiadminpb.ApplyCertifiedRuleChangeRequest_Cert{
			Cert: &clustermetadatapb.ExternallyCertifiedRevocation{
				FrozenLsn: "0/0",
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       1,
					AcceptedCoordinatorId:  orchProtoID,
					CoordinatorInitiatedAt: now,
					OutgoingRule:           &clustermetadatapb.RuleNumber{},
				},
			},
		},
		Reason: "test-cli-bootstrap",
	}

	// Spin up multiadmin in-process; it will dial multiorch over gRPC.
	logger := slog.Default()
	adminServer := adminserver.NewMultiAdminServer(
		setup.TopoServer, logger, grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer adminServer.Stop()

	ctx := utils.WithTimeout(t, 15*time.Second)
	resp, err := adminServer.ApplyCertifiedRuleChange(ctx, req)
	require.NoError(t, err, "ApplyCertifiedRuleChange should succeed")
	require.NotNil(t, resp.InstalledRule)
	assert.Equal(t, int64(1), resp.InstalledRule.GetRuleNumber().GetCoordinatorTerm())
	assert.Equal(t, leaderID.Name, resp.InstalledRule.GetLeaderId().GetName())

	// Wait for every pooler in the cohort to reach the expected end state.
	setup.PrimaryName = leaderID.Name
	primaryName := waitForShardReady(t, setup, 2 /* expectedStandbyCount */, 10*time.Second)
	require.Equal(t, leaderID.Name, primaryName, "elected primary should match the CLI's chosen leader")

	t.Run("primary term, schema, and preserved timestamps", func(t *testing.T) {
		primaryClient := setup.NewPrimaryClient(t)
		defer primaryClient.Close()

		status, err := primaryClient.Manager.Status(t.Context(), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err)
		require.NotNil(t, status.ConsensusStatus.GetTermRevocation())
		assert.Equal(t, int64(1), status.ConsensusStatus.GetTermRevocation().GetRevokedBelowTerm(),
			"primary should be on term 1 after CLI bootstrap")
		assert.Equal(t, int64(1), commonconsensus.LeaderTerm(status.ConsensusStatus),
			"primary leader_term should be 1")

		// The TermRevocation's coordinator_initiated_at is what each pooler
		// persists during Recruit. It must match exactly what we sent in
		// the cert; any mismatch would mean multiorch silently rewrote the
		// caller's attestation.
		assert.Equal(t, now.AsTime().UTC(), status.ConsensusStatus.GetTermRevocation().GetCoordinatorInitiatedAt().AsTime().UTC(),
			"persisted coordinator_initiated_at should equal the value passed in by the caller")

		// The recorded rule's creation_time should also round-trip — it's
		// the field the caller set on the ShardRule, written to current_rule,
		// and reported back via observePosition in ConsensusStatus.
		// Postgres truncates to microseconds, so compare at that granularity.
		recordedRule := status.ConsensusStatus.GetCurrentPosition().GetRule()
		require.NotNil(t, recordedRule, "primary should have a recorded rule")
		require.NotNil(t, recordedRule.GetCreationTime(), "recorded rule should have a creation_time")
		wantCreation := now.AsTime().Truncate(time.Microsecond).UTC()
		assert.True(t,
			recordedRule.GetCreationTime().AsTime().UTC().Equal(wantCreation),
			"recorded rule creation_time = %v, want %v",
			recordedRule.GetCreationTime().AsTime().UTC(), wantCreation)
		assert.Equal(t, setup.CellName, recordedRule.GetCoordinatorId().GetCell(),
			"recorded rule coordinator_id cell should match the orch we picked")
		assert.Equal(t, "multiorch", recordedRule.GetCoordinatorId().GetName(),
			"recorded rule coordinator_id name should match the orch we picked")

		exists, err := primaryClient.Pooler.ExecuteQuery(t.Context(),
			"SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'multigres')", 1)
		require.NoError(t, err)
		require.Len(t, exists.Rows, 1)
		assert.Equal(t, "t", string(exists.Rows[0].Values[0]), "multigres schema should exist")
	})

	t.Run("current_rule reflects the CLI's choice", func(t *testing.T) {
		primaryClient := setup.NewPrimaryClient(t)
		defer primaryClient.Close()

		// current_rule is the single-row source of truth for the shard's
		// installed rule. After bootstrap it must reflect exactly what the
		// caller proposed: leader, coordinator, cohort, and durability policy.
		resp, err := primaryClient.Pooler.ExecuteQuery(t.Context(), `
			SELECT coordinator_term,
			       leader_subterm,
			       leader_id,
			       coordinator_id,
			       array_to_json(cohort_members)::text,
			       durability_policy_name,
			       durability_quorum_type,
			       durability_required_count
			FROM multigres.current_rule
			WHERE shard_id = '0'::bytea
		`, 8)
		require.NoError(t, err)
		require.Len(t, resp.Rows, 1, "current_rule must have exactly one row per shard")

		row := resp.Rows[0]
		assert.Equal(t, "1", string(row.Values[0]), "coordinator_term should be 1")
		assert.Equal(t, "0", string(row.Values[1]), "leader_subterm should be 0 for initial term")
		assert.Equal(t, setup.CellName+"_"+leaderID.Name, string(row.Values[2]), "leader_id should match")
		assert.Equal(t, setup.CellName+"_multiorch", string(row.Values[3]), "coordinator_id should match orch")

		var cohortMembers []string
		require.NoError(t, json.Unmarshal(row.Values[4], &cohortMembers),
			"cohort_members should be a valid JSON array")
		expectedCohort := []string{
			setup.CellName + "_pooler-1",
			setup.CellName + "_pooler-2",
			setup.CellName + "_pooler-3",
		}
		assert.ElementsMatch(t, expectedCohort, cohortMembers,
			"cohort_members should match the CLI's proposed cohort")

		assert.Equal(t, durability.GetPolicyName(), string(row.Values[5]),
			"durability_policy_name should match")
		assert.Equal(t, durability.GetQuorumType().String(), string(row.Values[6]),
			"durability_quorum_type should match")
		assert.Equal(t, "2", string(row.Values[7]),
			"durability_required_count should match")
	})
}
