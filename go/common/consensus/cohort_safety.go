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
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// IsCohortMemberRemovalSafe reports whether removing memberID from the
// given rule's cohort would still leave enough redundancy for a
// subsequent failover. The rule supplies the durability policy
// (rule.GetDurabilityPolicy()) under which sufficient-recruitment is
// judged.
//
// The check models the worst-realistic case: memberID is removed from
// the cohort AND the current leader subsequently fails, AND every other
// cohort member is reachable. Under those assumptions, can the remaining
// cohort still satisfy the policy's sufficient-recruitment requirement
// (the same predicate failover evaluates when electing a new leader)?
//
// Returns false if rule or memberID is nil, the rule carries no
// durability policy, the policy fails to parse, memberID isn't an
// actual member of rule's cohort, or the simulated recruitment is
// insufficient. The leader is identified by rule.GetLeaderId(); if the
// rule has no leader the check still applies the removal but skips the
// leader-failure step (the cohort doesn't lose another voter).
func IsCohortMemberRemovalSafe(rule *clustermetadatapb.ShardRule, memberID *clustermetadatapb.ID) bool {
	if rule == nil || memberID == nil {
		return false
	}
	policy, err := NewPolicyFromProto(rule.GetDurabilityPolicy())
	if err != nil || policy == nil {
		return false
	}
	cohort := rule.GetCohortMembers()
	wantedKey := topoclient.ComponentIDString(memberID)
	found := false
	newCohort := make([]*clustermetadatapb.ID, 0, len(cohort))
	for _, id := range cohort {
		if topoclient.ComponentIDString(id) == wantedKey {
			found = true
			continue
		}
		newCohort = append(newCohort, id)
	}
	if !found {
		return false
	}
	// Simulate leader failure: recruited = new cohort minus leader.
	recruited := newCohort
	if leaderID := rule.GetLeaderId(); leaderID != nil {
		leaderKey := topoclient.ComponentIDString(leaderID)
		recruited = make([]*clustermetadatapb.ID, 0, len(newCohort))
		for _, id := range newCohort {
			if topoclient.ComponentIDString(id) == leaderKey {
				continue
			}
			recruited = append(recruited, id)
		}
	}
	return policy.CheckSufficientRecruitment(newCohort, recruited) == nil
}
