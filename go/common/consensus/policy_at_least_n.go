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
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// AtLeastNPolicy requires any N poolers from the cohort to acknowledge writes.
type AtLeastNPolicy struct {
	N    int
	Desc string
}

// CheckAchievable returns nil iff the proposed cohort has at least N poolers.
func (p AtLeastNPolicy) CheckAchievable(proposedCohort []*clustermetadatapb.ID) error {
	if len(proposedCohort) < p.N {
		return fmt.Errorf("durability not achievable: proposed cohort has %d poolers, required %d (%s)",
			len(proposedCohort), p.N, p.Desc)
	}
	return nil
}

// CheckSufficientRecruitment enforces two proposal-agnostic invariants:
//   - Revocation: fewer than N cohort poolers are absent from recruited, so no
//     N-subset of the cohort avoids our recruitment — no parallel quorum can
//     still form outside our recruited set.
//   - Majority: recruited is a strict majority of cohort, so any two
//     concurrent recruitments must share at least one pooler and, via
//     "one accept per term", cannot both complete.
//
// Candidacy (whether the recruited set satisfies the policy's quorum under
// the *proposed* leadership change) is not checked here — that is a
// proposal-specific concern handled by the leader-appointment layer via
// CheckAchievable.
func (p AtLeastNPolicy) CheckSufficientRecruitment(cohort, recruited []*clustermetadatapb.ID) error {
	if err := validateRecruitedSubset(cohort, recruited); err != nil {
		return err
	}

	// The two obligations below combine into a single binding threshold:
	//
	//   |recruited| >= max(
	//     len(cohort)/2 + 1,   // majority — any two recruitments must share a pooler
	//     len(cohort) - N + 1, // revocation — un-recruited cannot form an N-quorum
	//   )
	//
	// We check each separately so failures report the specific reason.

	// Majority: prevents two coordinators from recruiting disjoint sets at the same term.
	if err := validateMajority(cohort, recruited); err != nil {
		return err
	}

	// Revocation: if N or more cohort poolers are un-recruited, they could form a
	// quorum on their own. Example: AT_LEAST_N with N=2 and a cohort of 5 poolers.
	// After recruiting pooler-1, pooler-2, and pooler-3, we still need to recruit
	// pooler-4 OR pooler-5 — otherwise the 2 unrecruited poolers could form their
	// own 2-pooler quorum that still satisfies the durability policy.
	unrecruited := unrecruitedKeys(cohort, recruited, topoclient.ClusterIDString)
	if len(unrecruited) >= p.N {
		return fmt.Errorf("revocation not satisfied: %d cohort poolers not recruited, another possible quorum could be formed of %d (%s)",
			len(unrecruited), p.N, p.Desc)
	}
	return nil
}

// Description returns a human-readable summary of the policy.
func (p AtLeastNPolicy) Description() string {
	if p.Desc != "" {
		return p.Desc
	}
	return fmt.Sprintf("AT_LEAST_N(N=%d)", p.N)
}
