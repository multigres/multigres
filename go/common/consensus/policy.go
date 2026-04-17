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
	"errors"
	"fmt"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// DurabilityPolicy captures the quorum semantics of a single durability rule.
//
// It exposes two checks a coordinator needs to safely appoint a new leader
// under the generalized-consensus model:
//
//  1. Achievability: a pre-flight feasibility gate — the proposed cohort could
//     ever satisfy this policy under ideal conditions.
//  2. Sufficient recruitment: the recruited subset of a committed cohort can
//     form a fresh quorum (candidacy) AND intersects every other quorum the
//     cohort could form (revocation).
type DurabilityPolicy interface {
	// CheckAchievable returns nil if the proposed cohort could ever satisfy
	// this policy under ideal conditions. Used as a pre-flight feasibility
	// gate before attempting recruitment.
	CheckAchievable(proposedCohort []*clustermetadatapb.ID) error

	// CheckSufficientRecruitment returns nil if recruited is sufficient to
	// safely establish a new leader. This has three conceptual obligations:
	//
	//   - Candidacy: recruited can form a fresh quorum under this policy, so
	//     the new leader has forward progress.
	//   - Revocation: recruited intersects every other quorum the cohort
	//     could form under this policy, so no parallel quorum can still
	//     commit outside our recruitment.
	//   - Competing-recruitment overlap: recruited intersects any other
	//     sufficient recruitment a competing coordinator could form,
	//     preventing two coordinators from both "successfully" recruiting
	//     disjoint sets at the same time.
	//
	// For uniform durability policies, candidacy + revocation mathematically
	// imply competing-recruitment overlap, so one combined check suffices.
	// Per-leader policies (not yet supported) would require explicit overlap
	// checks against every admissible policy.
	CheckSufficientRecruitment(cohort, recruited []*clustermetadatapb.ID) error

	// Description returns a human-readable summary of the policy.
	Description() string
}

// PolicyFromProto converts a proto DurabilityPolicy into a concrete
// DurabilityPolicy implementation.
func PolicyFromProto(policy *clustermetadatapb.DurabilityPolicy) (DurabilityPolicy, error) {
	if policy == nil {
		return nil, errors.New("durability policy is nil")
	}

	switch policy.QuorumType {
	case clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N:
		return AtLeastNPolicy{N: int(policy.RequiredCount), Desc: policy.Description}, nil
	case clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N:
		return MultiCellPolicy{N: int(policy.RequiredCount), Desc: policy.Description}, nil
	default:
		return nil, fmt.Errorf("unsupported quorum type: %v", policy.QuorumType)
	}
}
