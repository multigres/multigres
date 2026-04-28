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
	"log/slog"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// MultiCellPolicy requires acknowledgement from poolers spanning at least N
// distinct cells.
type MultiCellPolicy struct {
	N int
}

// CheckAchievable returns nil iff the proposed cohort spans at least N
// distinct cells.
func (p MultiCellPolicy) CheckAchievable(proposedCohort []*clustermetadatapb.ID) error {
	cells := cellsOf(proposedCohort)
	if len(cells) < p.N {
		return fmt.Errorf("durability not achievable: proposed cohort spans %d cells, required %d",
			len(cells), p.N)
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
	unrecruitedCells := unrecruitedKeyCount(cohort, recruited, func(id *clustermetadatapb.ID) string { return id.GetCell() })
	if unrecruitedCells >= p.N {
		return fmt.Errorf("revocation not satisfied: un-recruited cohort poolers span %d cells, another possible quorum could be formed spanning %d cells",
			unrecruitedCells, p.N)
	}
	return nil
}

// BuildLeaderDurabilityPostgresConfig returns the Postgres-level config the
// new primary must apply to satisfy MULTI_CELL_AT_LEAST_N. Standbys in the
// primary's own cell are excluded so synchronous acknowledgement always
// crosses a cell boundary.
//
// Errors when no eligible different-cell standbys exist or when the eligible
// set is too small to satisfy num_sync.
func (p MultiCellPolicy) BuildLeaderDurabilityPostgresConfig(
	logger *slog.Logger,
	cohort []*clustermetadatapb.ID,
	leader *clustermetadatapb.ID,
) (*LeaderDurabilityPostgresConfig, error) {
	// N==1 means the primary alone satisfies durability — return an explicit
	// "no sync standbys" config so the new primary clears any stale
	// synchronous_standby_names instead of silently inheriting them.
	if p.N == 1 {
		logger.Info("Configuring leader for local-only durability",
			"policy", "MULTI_CELL_AT_LEAST_N",
			"required_count", p.N)
		return &LeaderDurabilityPostgresConfig{
			SyncCommit:     multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL,
			SyncMethod:     multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:        1,
			SyncStandbyIDs: nil,
		}, nil
	}

	// Drop cohort members in the primary's own cell so synchronous
	// acknowledgement always crosses a cell boundary. The primary itself is
	// naturally excluded (it's in its own cell).
	leaderCell := leader.GetCell()
	eligible := make([]*clustermetadatapb.ID, 0, len(cohort))
	for _, s := range cohort {
		if s.GetCell() != leaderCell {
			eligible = append(eligible, s)
		}
	}

	logger.Info("Filtered standbys for MULTI_CELL_AT_LEAST_N",
		"leader_cell", leaderCell,
		"cohort_size", len(cohort),
		"eligible_standbys", len(eligible),
		"excluded_same_cell", len(cohort)-len(eligible))

	if len(eligible) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("cannot establish synchronous replication: no eligible standbys in different cells (leader_cell=%s)",
				leaderCell))
	}

	// num_sync = required_count - 1: primary itself counts as 1 ack.
	requiredNumSync := p.N - 1
	if requiredNumSync > len(eligible) {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("cannot establish synchronous replication: insufficient different-cell standbys (required %d standbys, available %d)",
				requiredNumSync, len(eligible)))
	}

	logger.Info("Configuring synchronous replication",
		"policy", "MULTI_CELL_AT_LEAST_N",
		"required_count", p.N,
		"num_sync", requiredNumSync,
		"eligible_standbys", len(eligible))

	return &LeaderDurabilityPostgresConfig{
		SyncCommit:     multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
		SyncMethod:     multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
		NumSync:        requiredNumSync,
		SyncStandbyIDs: eligible,
	}, nil
}

// Description returns a human-readable summary of the policy.
func (p MultiCellPolicy) Description() string {
	return fmt.Sprintf("MULTI_CELL_AT_LEAST_N(N=%d)", p.N)
}

// cellsOf returns the set of distinct cells covered by poolers.
func cellsOf(poolers []*clustermetadatapb.ID) map[string]struct{} {
	return keysOf(poolers, func(id *clustermetadatapb.ID) string { return id.GetCell() })
}
