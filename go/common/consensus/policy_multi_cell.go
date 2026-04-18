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

// MultiCellPolicy requires acknowledgement from poolers spanning at least N
// distinct cells.
type MultiCellPolicy struct {
	N    int
	Desc string
}

// CheckAchievable returns nil iff the proposed cohort spans at least N
// distinct cells.
func (p MultiCellPolicy) CheckAchievable(proposedCohort []*clustermetadatapb.ID) error {
	cells := cellsOf(proposedCohort)
	if len(cells) < p.N {
		return fmt.Errorf("durability not achievable: proposed cohort spans %d cells, required %d (%s)",
			len(cells), p.N, p.Desc)
	}
	return nil
}

// CheckSufficientRecruitment enforces two proposal-agnostic invariants:
//   - Revocation: the un-recruited cohort poolers span fewer than N distinct
//     cells, so they cannot themselves form a commit quorum satisfying the
//     policy. Cell coverage by recruited is not enough when a cell holds
//     multiple poolers: a recruited pooler in a cell does not block an
//     un-recruited pooler in the same cell from participating in a separate
//     quorum elsewhere.
//   - Majority: recruited is a pooler-majority of cohort, so any two
//     concurrent recruitments must share at least one pooler. Cell-level
//     intersection is not sufficient here because two pooler-disjoint
//     recruitments can share cells when a cell has multiple poolers.
//
// Candidacy (whether recruited spans enough cells for the *proposed*
// leadership change) is not checked here — that is a proposal-specific
// concern handled by the leader-appointment layer via CheckAchievable.
func (p MultiCellPolicy) CheckSufficientRecruitment(cohort, recruited []*clustermetadatapb.ID) error {
	if err := validateRecruitedSubset(cohort, recruited); err != nil {
		return err
	}

	if err := validateMajority(cohort, recruited); err != nil {
		return err
	}

	// Revocation: if the un-recruited cohort poolers themselves span N or more cells, they could
	// form a commit quorum on their own under this policy. We can't allow that.
	// Example: MULTI_CELL_AT_LEAST_2 and a cohort of 6 poolers (2 per cell across 3 cells).
	// Recruiting one pooler from each cell covers every cohort cell, but the 3 un-recruited
	// poolers still span 3 cells — enough to form a separate 2-cell quorum on their own.
	unrecruitedCells := unrecruitedKeys(cohort, recruited, func(id *clustermetadatapb.ID) string { return id.GetCell() })
	if len(unrecruitedCells) >= p.N {
		return fmt.Errorf("revocation not satisfied: un-recruited cohort poolers span %d cells, another possible quorum could be formed spanning %d cells (%s)",
			len(unrecruitedCells), p.N, p.Desc)
	}
	return nil
}

// Description returns a human-readable summary of the policy.
func (p MultiCellPolicy) Description() string {
	if p.Desc != "" {
		return p.Desc
	}
	return fmt.Sprintf("MULTI_CELL_AT_LEAST_N(N=%d)", p.N)
}

// cellsOf returns the set of distinct cells covered by poolers.
func cellsOf(poolers []*clustermetadatapb.ID) map[string]struct{} {
	cells := make(map[string]struct{})
	for _, p := range poolers {
		cells[p.GetCell()] = struct{}{}
	}
	return cells
}
