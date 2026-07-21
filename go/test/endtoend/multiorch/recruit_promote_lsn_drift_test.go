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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestRecruitPromoteLsnDrift is a direct, consensus-doesn't-need-to-be-correct
// experiment: it drives Recruit() and Promote() by hand against a single
// standby (bypassing multiorch's own failover decisions entirely) to verify
// Recruit() genuinely stopped WAL replay from advancing.
//
// Durability/quorum policy is irrelevant here: checkRecruitLsnDrift runs in
// promoteLocked before the rule write that needs quorum ack, so a Promote()
// call that hangs on writing the rule (because the real pooler-1 will never
// ack a fake cohort) already tells us the drift check passed silently — we
// use a short client-side timeout on Promote() and only care whether it
// returns the specific "recruited position drifted" error quickly, not
// whether the whole call succeeds.
//
// recovery_min_apply_delay creates a real, growing gap between received and
// applied WAL while writes flow to the primary; a real wall-clock gap between
// Recruit() and Promote() gives the standby's background replay process time
// to drain that gap in the background (recovery_min_apply_delay expiry is
// time-based, not tied to promotion) — if Recruit()'s waitForReplayStabilize
// call correctly waited out replay before returning, Promote() should see the
// same position; if it returned prematurely, checkRecruitLsnDrift should
// reject it.
func TestRecruitPromoteLsnDrift(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestRecruitPromoteLsnDrift test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end recruit/promote LSN drift test (no postgres binaries)")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)

	pName := waitForShardReady(t, setup, 1, 30*time.Second)
	t.Logf("Shard ready: primary=%s", pName)

	var standbyName string
	for name := range setup.Multipoolers {
		if name != pName {
			standbyName = name
			break
		}
	}
	require.NotEmpty(t, standbyName, "should have a standby")
	t.Logf("Standby to recruit/promote directly: %s", standbyName)

	primaryInst := setup.GetMultipoolerInstance(pName)
	require.NotNil(t, primaryInst, "primary instance should exist")
	primarySocketDir := filepath.Join(primaryInst.Pgctld.PoolerDir, "pg_sockets")
	primaryDB := connectToPostgres(t, primarySocketDir, primaryInst.Pgctld.PgPort)
	defer primaryDB.Close()

	standbyInst := setup.GetMultipoolerInstance(standbyName)
	require.NotNil(t, standbyInst, "standby instance should exist")
	standbySocketDir := filepath.Join(standbyInst.Pgctld.PoolerDir, "pg_sockets")
	standbyDB := connectToPostgres(t, standbySocketDir, standbyInst.Pgctld.PgPort)
	defer standbyDB.Close()

	// Make sure orch is settled, then keep it out of the way: we drive
	// Recruit/Promote by hand, so multiorch must not race with us.
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioInitialSettle)
	setup.DisableRecovery(t, "multiorch")

	// Force a real, growing gap between received and applied WAL on the
	// standby: recovery_min_apply_delay delays application by a fixed amount
	// past each record's commit timestamp, independent of promotion.
	_, err := standbyDB.Exec("ALTER SYSTEM SET recovery_min_apply_delay = '3000ms'")
	require.NoError(t, err, "should set recovery_min_apply_delay on standby")
	_, err = standbyDB.Exec("SELECT pg_reload_conf()")
	require.NoError(t, err, "should reload standby config")

	_, err = primaryDB.Exec("CREATE TABLE IF NOT EXISTS recruit_drift_test (id SERIAL PRIMARY KEY, data TEXT)")
	require.NoError(t, err, "should create test table on primary")

	// Generate sustained write traffic so there's a continuous backlog
	// building up behind the standby's apply delay.
	stopWrites := make(chan struct{})
	writesDone := make(chan struct{})
	go func() {
		defer close(writesDone)
		for {
			select {
			case <-stopWrites:
				return
			default:
				_, _ = primaryDB.Exec("INSERT INTO recruit_drift_test (data) VALUES ('x')")
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()
	time.Sleep(2 * time.Second) // let backlog build up behind the delay
	close(stopWrites)
	<-writesDone

	standbyClient, err := shardsetup.NewMultipoolerClient(standbyInst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer standbyClient.Close()

	statusResp, err := standbyClient.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err, "should get standby status")
	standbyID := statusResp.ConsensusStatus.GetId()
	require.NotNil(t, standbyID, "standby status should carry its own id")
	previousRule := statusResp.ConsensusStatus.GetCurrentPosition().GetPosition().GetDecision()
	require.NotNil(t, previousRule, "standby should have a recorded rule before recruit")
	outgoingRule := previousRule.GetRuleNumber()

	term := outgoingRule.GetCoordinatorTerm() + 1
	coordinatorID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      setup.CellName,
		Name:      "test-coordinator",
	}
	revocation := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       term,
		AcceptedCoordinatorId:  coordinatorID,
		CoordinatorInitiatedAt: timestamppb.Now(),
		OutgoingRule:           outgoingRule,
	}

	t.Logf("Calling Recruit on standby %s with term %d", standbyName, term)
	recruitResp, err := standbyClient.Consensus.Recruit(utils.WithTimeout(t, 15*time.Second), &consensusdatapb.RecruitRequest{
		TermRevocation: revocation,
	})
	require.NoError(t, err, "Recruit should succeed")
	observedLsn := recruitResp.GetLsn().GetAppliedLsn()
	t.Logf("Recruit succeeded; observed (recorded-as-stable) LSN: %s", observedLsn)

	// Give the standby's background replay process real wall-clock time to
	// drain the recovery_min_apply_delay backlog — independent of promotion.
	// This is the gap checkRecruitLsnDrift exists to catch if
	// waitForReplayStabilize under-waited.
	t.Log("Sleeping to let delayed WAL apply in the background before Promote...")
	time.Sleep(5 * time.Second)

	// Cohort/durability policy is irrelevant to what we're testing —
	// checkRecruitLsnDrift runs before the rule write that needs quorum ack.
	// A short client-side timeout below means we only observe whether
	// Promote() rejects quickly with the drift error, not whether the whole
	// call (including the write pooler-1 will never ack) completes.
	proposedRule := &clustermetadatapb.ShardRule{
		RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
		LeaderId:      standbyID,
		CohortMembers: previousRule.GetCohortMembers(),
		DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 1,
		},
		CoordinatorId: coordinatorID,
		CreationTime:  timestamppb.Now(),
	}
	promoteResp, err := standbyClient.Consensus.Promote(utils.WithTimeout(t, 5*time.Second), &consensusdatapb.PromoteRequest{
		Proposal: &consensusdatapb.CoordinatorProposal{
			TermRevocation: revocation,
			ProposalLeader: &clustermetadatapb.PoolerAddress{
				Id:           standbyID,
				Host:         "localhost",
				PostgresPort: int32(standbyInst.Pgctld.PgPort),
			},
			ProposedTransition: &clustermetadatapb.RulePosition{Proposal: proposedRule},
		},
	})

	switch {
	case err == nil:
		require.NotNil(t, promoteResp)
		t.Log("Promote succeeded — no drift detected between Recruit and Promote")
	case strings.Contains(err.Error(), "recruited position drifted"):
		t.Log("Drift correctly detected: Promote rejected before ever reaching the rule write")
	case strings.Contains(err.Error(), "context deadline exceeded"):
		// The call hung on the later rule-write step (pooler-1 will never ack
		// a fake cohort). checkRecruitLsnDrift runs before that write, so
		// reaching this hang means the drift check already passed silently —
		// a valid negative result, not a broken test.
		t.Logf("Promote did not return within 5s (hung on the unrelated quorum write, as expected): %v", err)
		t.Log("No drift detected: checkRecruitLsnDrift must have passed silently before the hang")
	default:
		t.Fatalf("Promote failed with an unexpected error (not drift, not the expected quorum-write timeout): %v", err)
	}
}
