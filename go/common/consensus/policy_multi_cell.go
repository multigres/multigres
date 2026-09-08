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

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// multiCellPolicy requires acknowledgement from poolers spanning at least 2
// distinct cells.
// In future we could expand this to a configurable number of cells
type multiCellPolicy struct{}

// SatisfiedBy returns nil iff poolers spans at least 2 distinct cells.
func (p multiCellPolicy) SatisfiedBy(poolers []*clustermetadatapb.ID) error {
	// cellsOf already dedupes by cell, so a duplicate pooler entry can't
	// inflate the cell count the way it could a raw pooler count — this
	// normalization is for consistency with AtLeastNPolicy.SatisfiedBy, not
	// because it changes cellsOf's result here.
	poolers = normalizeIDs(poolers)
	cells := cellsOf(poolers)
	if len(cells) < 2 {
		return fmt.Errorf("durability not satisfied: poolers span %d cells, required %d",
			len(cells), 2)
	}
	return nil
}

// BuildSyncReplicationConfig returns the Postgres-level config the primary
// must apply to satisfy MULTI_CELL_AT_LEAST_N. Standbys in the primary's own
// cell are excluded so synchronous acknowledgement always crosses a cell boundary.
//
// Errors when no eligible different-cell standbys exist or when the eligible
// set is too small to satisfy num_sync.
func (p multiCellPolicy) BuildSyncReplicationConfig(
	cohort []*clustermetadatapb.ID,
	primary *clustermetadatapb.ID,
) (*SyncReplicationConfig, error) {
	// Drop cohort members in the primary's own cell so synchronous
	// acknowledgement always crosses a cell boundary. The primary itself is
	// naturally excluded (it's in its own cell).
	primaryCell := primary.GetCell()
	eligible := make([]*clustermetadatapb.ID, 0, len(cohort))
	for _, s := range cohort {
		if s.GetCell() != primaryCell {
			eligible = append(eligible, s)
		}
	}

	if len(eligible) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("cannot establish synchronous replication: no eligible standbys in different cells (primary_cell=%s)",
				primaryCell))
	}

	return &SyncReplicationConfig{
		SyncCommit:     multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
		SyncMethod:     multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
		NumSync:        1,
		SyncStandbyIDs: eligible,
	}, nil
}

// Description returns a human-readable summary of the policy.
func (p multiCellPolicy) Description() string {
	return "MULTI_CELL_AT_LEAST_N(N=2)"
}

// cellsOf returns the set of distinct cells covered by poolers.
func cellsOf(poolers []*clustermetadatapb.ID) map[string]struct{} {
	return keysOf(poolers, func(id *clustermetadatapb.ID) string { return id.GetCell() })
}
