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

// CheckSufficientRecruitment enforces both candidacy and revocation:
//   - Candidacy: recruited has at least N poolers, so the new leader can form a
//     fresh quorum.
//   - Revocation: fewer than N cohort poolers are absent from recruited, so no
//     N-subset of the cohort avoids our recruitment — no parallel quorum can
//     still form outside our recruited set.
func (p AtLeastNPolicy) CheckSufficientRecruitment(cohort, recruited []*clustermetadatapb.ID) error {
	if err := validateRecruitedSubset(cohort, recruited); err != nil {
		return err
	}

	if len(recruited) < p.N {
		return fmt.Errorf("candidacy not satisfied: recruited %d poolers, required %d (%s)",
			len(recruited), p.N, p.Desc)
	}

	recruitedKeys := poolerKeysOf(recruited)
	missing := 0
	for _, pooler := range cohort {
		if _, ok := recruitedKeys[poolerKey(pooler)]; !ok {
			missing++
		}
	}
	// If we have N or more nodes missing in the recruitment, they could form a quorum on their own.
	// We can't allow that.
	// Example: AT_LEAST_N with N=2 and a cohort of 5 poolers. After recruiting
	// pooler-1, pooler-2, and pooler-3, we still need to recruit pooler-4 OR
	// pooler-5 — otherwise the 2 unrecruited poolers could form their own
	// 2-pooler quorum that still satisfies the durability policy.
	if missing >= p.N {
		return fmt.Errorf("revocation not satisfied: %d cohort poolers not recruited, another possible quorum could be formed of %d (%s)",
			missing, p.N, p.Desc)
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
