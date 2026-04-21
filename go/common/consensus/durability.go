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
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// IsDurabilityPolicyAchievable returns true if the given number of poolers is sufficient
// to satisfy the durability policy. A nil policy is treated as achievable (no constraint).
//
// This is a conservative check: it only considers node count, not cell distribution.
// MULTI_CELL policies have additional geographic constraints that are not evaluated here.
func IsDurabilityPolicyAchievable(policy *clustermetadatapb.DurabilityPolicy, numPoolers int) bool {
	if policy == nil {
		return true
	}
	return numPoolers >= int(policy.RequiredCount)
}

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
