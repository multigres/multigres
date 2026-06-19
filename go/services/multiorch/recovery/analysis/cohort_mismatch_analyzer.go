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

package analysis

import (
	"errors"
	"fmt"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// CohortMismatchAnalyzer detects drift between the desired cohort and the
// recorded cohort on the leader.
//
// Two flavors of drift are reported:
//
//   - ProblemPoolerNotInCohort: a healthy, replicating, eligible pooler exists
//     in the shard but is absent from the leader's recorded cohort.
//   - ProblemCohortMemberIneligible: a current cohort member has self-reported
//     INELIGIBLE via AvailabilityStatus.CohortEligibilityStatus and should be
//     removed.
//
// Both surface a single ReconcileCohortAction; the action interprets the
// problem code and applies the appropriate ADD/REMOVE.
//
// TODO: this currently includes every healthy, eligible pooler in the cohort.
// In the future we'll likely want to constrain cohort size (e.g. cap at
// durability_required_count + 1) and choose the best-qualified members from
// the available poolers. That requires a fitness heuristic (LSN proximity,
// cell topology, last-known leadership history, etc.) that hasn't been
// designed yet.
type CohortMismatchAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *CohortMismatchAnalyzer) Name() types.CheckName {
	return "CohortMismatch"
}

func (a *CohortMismatchAnalyzer) ProblemCode() types.ProblemCode {
	// This analyzer can produce two problem codes; ProblemCode() returns the
	// primary one. The recovery loop uses this for routing/logging — the
	// per-problem code on each emitted Problem is what matters at execution.
	return types.ProblemPoolerNotInCohort
}

func (a *CohortMismatchAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewReconcileCohortAction()
}

func (a *CohortMismatchAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// We only act when the shard has a reachable, ready leader to receive the
	// rule update. Bootstrap and failover paths set up the cohort separately.
	if !sa.LeaderReachable || !sa.LeaderPostgresReady {
		return nil, nil
	}

	// Build cohort map keyed by serialized ID, paired with the raw
	// *clustermetadata.ID so we can emit a Problem for a missing-from-cache
	// cohort member (no PoolerAnalysis carries its ID otherwise).
	cohortIDs := make(map[topoclient.ComponentID]*clustermetadatapb.ID, len(sa.HighestShardRule.GetCohortMembers()))
	for _, id := range sa.HighestShardRule.GetCohortMembers() {
		cohortIDs[topoclient.ComponentIDString(id)] = id
	}

	// Track which cohort members we observed in the cache during the loop;
	// anything left over is missing entirely.
	seen := make(map[topoclient.ComponentID]struct{}, len(cohortIDs))

	var problems []types.Problem
	for _, pa := range sa.Analyses {
		key := topoclient.ComponentIDString(pa.PoolerID)
		// Removal candidates: current cohort members signaling INELIGIBLE.
		if _, inCohort := cohortIDs[key]; inCohort {
			seen[key] = struct{}{}
			if types.PoolerIsCohortIneligible(pa.AvailabilityStatus) {
				problems = append(problems, types.Problem{
					Code:           types.ProblemCohortMemberIneligible,
					CheckName:      "CohortMismatch",
					PoolerID:       pa.PoolerID,
					ShardKey:       pa.ShardKey,
					Description:    fmt.Sprintf("Cohort member %s self-reported INELIGIBLE", pa.PoolerID.Name),
					Priority:       types.PriorityNormal,
					Scope:          types.ScopePooler,
					DetectedAt:     time.Now(),
					RecoveryAction: a.factory.NewReconcileCohortAction(),
				})
			}
			continue
		}

		// Addition candidates: replicas not currently in the cohort that are
		// healthy enough to be added.
		if !a.isAdditionCandidate(sa, pa) {
			continue
		}
		problems = append(problems, types.Problem{
			Code:           types.ProblemPoolerNotInCohort,
			CheckName:      "CohortMismatch",
			PoolerID:       pa.PoolerID,
			ShardKey:       pa.ShardKey,
			Description:    fmt.Sprintf("Pooler %s is replicating and eligible but not in the cohort", pa.PoolerID.Name),
			Priority:       types.PriorityNormal,
			Scope:          types.ScopePooler,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewReconcileCohortAction(),
		})
	}

	// Removal candidates: cohort members no longer in the live cache. Two
	// confidence tiers — explicit cache tombstones (known SHUTDOWN) outrank
	// missing-without-tombstone (could be a transient gap). Each candidate
	// is gated by commonconsensus.IsCohortMemberRemovalSafe, which under
	// the rule's durability policy verifies that removing the member AND
	// having the current leader subsequently fail still leaves a
	// recruitable quorum. To avoid compounding risk, we propose at most
	// one REMOVE per cycle per shard; the next cycle picks up the next one
	// once the previous applies.
	var (
		tombstoneRemove *clustermetadatapb.ID
		missingRemove   *clustermetadatapb.ID
	)
	for key, id := range cohortIDs {
		if _, ok := seen[key]; ok {
			continue
		}
		isTombstone := false
		if _, ok := sa.GhostIDs[key]; ok {
			isTombstone = true
		}
		// Tombstones are KNOWN dead — they aren't contributing to the cohort
		// anyway, so the durability-policy safety check (which asks "would
		// removal + another failure leave a recruitable quorum?") doesn't
		// apply. Removing a tombstone makes the rule accurate without
		// degrading operational fault-tolerance beyond what already exists.
		//
		// For non-tombstone missing-from-cache cases the pooler may still be
		// alive somewhere; gate those on the durability policy.
		if !isTombstone && !commonconsensus.IsCohortMemberRemovalSafe(sa.HighestShardRule, id) {
			continue
		}
		if isTombstone {
			if tombstoneRemove == nil {
				tombstoneRemove = id
			}
		} else if missingRemove == nil {
			missingRemove = id
		}
	}
	if tombstoneRemove != nil {
		problems = append(problems, types.Problem{
			Code:           types.ProblemCohortMemberIneligible,
			CheckName:      "CohortMismatch",
			PoolerID:       tombstoneRemove,
			ShardKey:       sa.ShardKey,
			Description:    fmt.Sprintf("Cohort member %s is SHUTDOWN (cache tombstone); removing from cohort", tombstoneRemove.GetName()),
			Priority:       types.PriorityNormal,
			Scope:          types.ScopePooler,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewReconcileCohortAction(),
		})
	} else if missingRemove != nil {
		problems = append(problems, types.Problem{
			Code:           types.ProblemCohortMemberIneligible,
			CheckName:      "CohortMismatch",
			PoolerID:       missingRemove,
			ShardKey:       sa.ShardKey,
			Description:    fmt.Sprintf("Cohort member %s is no longer tracked by the pooler cache; removing from cohort", missingRemove.GetName()),
			Priority:       types.PriorityNormal,
			Scope:          types.ScopePooler,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewReconcileCohortAction(),
		})
	}
	return problems, nil
}

// isAdditionCandidate reports whether a non-cohort pooler is healthy enough to
// be added: it must be a non-leader REPLICA, initialized, replicating, and not
// signaling INELIGIBLE.
//
// TODO: long-term, most of these checks should fold into the pooler's
// self-reported cohort eligibility signal. The pooler is in the best position
// to know whether it can durably serve as a cohort member — it knows whether
// it has a working backup, whether it's in a drained state, whether
// replication is healthy, and so on. The analyzer should largely just trust
// the CohortEligibilityStatus signal rather than reconstruct that judgment
// from individual health fields.
//
// The IsLeader gate is also conceptually unnecessary — there's no correctness
// problem an acting primary adding itself to the cohort. This may be useful
// in some propagation scenarios.
func (a *CohortMismatchAnalyzer) isAdditionCandidate(_ *ShardAnalysis, pa *PoolerAnalysis) bool {
	if pa.NamesSelfAsLeader {
		return false
	}
	if !pa.LastCheckValid {
		return false
	}
	if !pa.IsInitialized {
		return false
	}
	// Replication must be configured and not stopped — otherwise the standby
	// can't acknowledge writes and adding it would degrade durability.
	if pa.PrimaryConnInfoHost == "" || !pa.WalReplayNotPaused {
		return false
	}
	if types.PoolerIsCohortIneligible(pa.AvailabilityStatus) {
		return false
	}
	return true
}
