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

// CheckSufficientRecruitment enforces both candidacy and revocation:
//   - Candidacy: recruited spans at least N distinct cells, so the new leader
//     can form a fresh quorum.
//   - Revocation: fewer than N cohort cells are left uncovered by recruited, so
//     no N-cell subset of the cohort avoids our recruitment — a prior leader
//     cannot still commit under any of its possible cell quorums.
func (p MultiCellPolicy) CheckSufficientRecruitment(cohort, recruited []*clustermetadatapb.ID) error {
	if err := validateRecruitedSubset(cohort, recruited); err != nil {
		return err
	}

	recruitedCells := cellsOf(recruited)
	if len(recruitedCells) < p.N {
		return fmt.Errorf("candidacy not satisfied: recruited poolers span %d cells, required %d (%s)",
			len(recruitedCells), p.N, p.Desc)
	}

	uncovered := 0
	seen := make(map[string]struct{})
	for _, pooler := range cohort {
		cell := pooler.GetCell()
		if _, already := seen[cell]; already {
			continue
		}
		seen[cell] = struct{}{}
		if _, covered := recruitedCells[cell]; !covered {
			uncovered++
		}
	}
	// If we have N or more cohort cells uncovered by recruited, those cells could form a quorum on
	// their own. We can't allow that.
	// Example: MULTI_CELL_AT_LEAST_2 and the cohort spans 3 cells (cell1,
	// cell2, cell3). If we only recruit poolers in cell1, cells cell2 and
	// cell3 are uncovered — together they could form a 2-cell quorum that
	// still satisfies the durability policy.
	if uncovered >= p.N {
		return fmt.Errorf("revocation not satisfied: %d cohort cells not covered by recruited, another possible quorum could span %d cells (%s)",
			uncovered, p.N, p.Desc)
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

// poolerKeysOf returns the set of pooler keys (cell + name) present in poolers.
// Name alone is insufficient because names are only unique within a cell.
func poolerKeysOf(poolers []*clustermetadatapb.ID) map[string]struct{} {
	out := make(map[string]struct{}, len(poolers))
	for _, p := range poolers {
		out[poolerKey(p)] = struct{}{}
	}
	return out
}

// poolerKey returns a cluster-unique identifier for a pooler.
func poolerKey(id *clustermetadatapb.ID) string {
	return id.GetCell() + "/" + id.GetName()
}

// validateRecruitedSubset returns an error if any recruited pooler is not a
// member of the cohort. All durability policies assume recruited ⊆ cohort so
// that candidacy counts reflect only policy-eligible poolers. This is a
// defensive invariant check; call sites should already enforce it upstream.
func validateRecruitedSubset(cohort, recruited []*clustermetadatapb.ID) error {
	cohortKeys := poolerKeysOf(cohort)
	for _, p := range recruited {
		if _, ok := cohortKeys[poolerKey(p)]; !ok {
			return fmt.Errorf("recruited pooler %s is not in cohort", poolerKey(p))
		}
	}
	return nil
}
