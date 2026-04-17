// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ParseUserSpecifiedDurabilityPolicy converts a policy name string into a DurabilityPolicy message.
// TODO: generalize to support AT_LEAST_N and MULTI_CELL_AT_LEAST_N for arbitrary N by parsing the number
// from the suffix (e.g. "AT_LEAST_3", "MULTI_CELL_AT_LEAST_4") instead of enumerating each case.
func ParseUserSpecifiedDurabilityPolicy(name string) (*clustermetadatapb.DurabilityPolicy, error) {
	switch name {
	case "AT_LEAST_2":
		return topoclient.AtLeastN(2), nil
	case "MULTI_CELL_AT_LEAST_2":
		return topoclient.MultiCellAtLeastN(2), nil
	default:
		return nil, fmt.Errorf("unsupported durability policy %q (supported: AT_LEAST_2, MULTI_CELL_AT_LEAST_2)", name)
	}
}

// DurabilityPolicy captures the quorum semantics of a single durability rule.
//
// It exposes two checks a coordinator needs to safely appoint a new leader
// under the generalized-consensus model:
//
//  1. Achievability: a pre-flight feasibility gate. Checks if the proposed cohort could
//     satisfy this policy's durability conditions.
//  2. Sufficient recruitment: the recruited subset of a committed cohort can
//     form a fresh quorum (candidacy) AND intersects every other quorum the
//     cohort could form (revocation).
type DurabilityPolicy interface {
	// CheckAchievable returns nil if the proposed cohort could satisfy
	// this policy. Used as a pre-flight feasibility gate before attempting
	// recruitment.
	CheckAchievable(proposedCohort []*clustermetadatapb.ID) error

	// CheckSufficientRecruitment returns nil if recruited is sufficient to
	// safely establish a new leader. This has two obligations:
	//
	//   - Candidacy: recruited can form a fresh quorum under this policy, so
	//     the new leader has forward progress.
	//   - Revocation: recruited intersects every other quorum the cohort
	//     could form under this policy, so no parallel quorum can still
	//     commit outside our recruitment.
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

// poolerKeysOf returns the set of cluster-unique keys present in poolers.
func poolerKeysOf(poolers []*clustermetadatapb.ID) map[string]struct{} {
	out := make(map[string]struct{}, len(poolers))
	for _, p := range poolers {
		out[topoclient.ClusterIDString(p)] = struct{}{}
	}
	return out
}

// validateRecruitedSubset returns an error if any recruited pooler is not a
// member of the cohort. All durability policies assume recruited ⊆ cohort so
// that candidacy counts reflect only policy-eligible poolers. This is a
// defensive invariant check; call sites should already enforce it upstream.
func validateRecruitedSubset(cohort, recruited []*clustermetadatapb.ID) error {
	cohortKeys := poolerKeysOf(cohort)
	for _, p := range recruited {
		key := topoclient.ClusterIDString(p)
		if _, ok := cohortKeys[key]; !ok {
			return fmt.Errorf("recruited pooler %s is not in cohort", key)
		}
	}
	return nil
}
