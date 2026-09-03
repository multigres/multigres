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

package consensus

import (
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/ha"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// filterCohortIneligible splits a cohort into the members that may
// participate in the recruit round and the names of those advertising
// INELIGIBLE (see runFailover).
func filterCohortIneligible(cohort []*multiorchdatapb.PoolerHealthState) (eligible []*multiorchdatapb.PoolerHealthState, ineligible []string) {
	for _, p := range cohort {
		if types.PoolerIsCohortIneligible(p.GetAvailabilityStatus()) {
			ineligible = append(ineligible, p.GetMultipooler().GetId().GetName())
			continue
		}
		eligible = append(eligible, p)
	}
	return eligible, ineligible
}

// consensusStatusesOf extracts each pooler's ConsensusStatus, skipping any
// that haven't reported one.
func consensusStatusesOf(poolers []*multiorchdatapb.PoolerHealthState) []*clustermetadatapb.ConsensusStatus {
	var statuses []*clustermetadatapb.ConsensusStatus
	for _, p := range poolers {
		if cs := p.GetConsensusStatus(); cs != nil {
			statuses = append(statuses, cs)
		}
	}
	return statuses
}

// eligibleConsensusStatuses returns the ConsensusStatus of every cohort
// member that isn't INELIGIBLE — the same set runFailover builds
// NewTermRevocation's input from, so NextFailoverAttempt's decision
// computation agrees with it on which members count.
func eligibleConsensusStatuses(cohort []*multiorchdatapb.PoolerHealthState) []*clustermetadatapb.ConsensusStatus {
	eligible, _ := filterCohortIneligible(cohort)
	return consensusStatusesOf(eligible)
}

// NextFailoverAttempt returns this orchestrator's earliest permitted failover
// recruitment time for cohort and whether that time has arrived, per backoff.
// Aggressive-first: acts immediately when no revocation is observed, or when
// the observed one demonstrably targets a different, already-resolved
// problem (see backoffRelevantRevocations); otherwise defers to the
// revocation's deterministic collective backoff.
func (c *Coordinator) NextFailoverAttempt(cohort []*multiorchdatapb.PoolerHealthState, backoff ha.BackoffSchedule) (readyAt time.Time, ready bool) {
	statuses := eligibleConsensusStatuses(cohort)
	decision := commonconsensus.HighestDecidedRule(statuses)
	rev := commonconsensus.HighestRevokedBelowTermRevocation(backoffRelevantRevocations(statuses, decision))
	if rev == nil {
		return time.Time{}, true
	}
	readyAt = backoff.NextAttempt(rev, c.GetCoordinatorID())
	return readyAt, !time.Now().Before(readyAt)
}

// backoffRelevantRevocations returns every TermRevocation among statuses
// that cannot be shown to target a problem other than decision: it matches
// decision, or carries no RecruitIntent at all (e.g. an externally-supplied
// cert or an external resignation) and so can't be proven unrelated. Only a
// revocation demonstrably targeting a *different* decision — resolved
// history, e.g. a shard's original bootstrap recruitment — is excluded.
//
// More permissive than commonconsensus.NewTermRevocation's own matching on
// purpose: that caller only risks a slightly-wrong escalation count by
// guessing wrong; this caller decides whether to act at all, so the same
// ambiguity must default to caution, not a free pass.
func backoffRelevantRevocations(statuses []*clustermetadatapb.ConsensusStatus, decision *clustermetadatapb.RuleNumber) []*clustermetadatapb.TermRevocation {
	var relevant []*clustermetadatapb.TermRevocation
	for _, cs := range statuses {
		rev := cs.GetTermRevocation()
		if rev.GetRevokedBelowTerm() <= 0 {
			continue
		}
		if replaceDecision := rev.GetRecruitIntent().GetReplaceDecision(); replaceDecision != nil &&
			commonconsensus.CompareRuleNumbers(replaceDecision, decision) != 0 {
			continue
		}
		relevant = append(relevant, rev)
	}
	return relevant
}
