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

package topoclient

import (
	"fmt"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// AtLeastN returns a DurabilityPolicy that requires at least n nodes to acknowledge writes.
func AtLeastN(n int32) *clustermetadatapb.DurabilityPolicy {
	return &clustermetadatapb.DurabilityPolicy{
		PolicyName:    fmt.Sprintf("AT_LEAST_%d", n),
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
		RequiredCount: n,
		Description:   fmt.Sprintf("At least %d nodes must acknowledge", n),
	}
}

// MultiCellAtLeastN returns a DurabilityPolicy that requires acknowledgement from nodes in
// at least n distinct cells.
func MultiCellAtLeastN(n int32) *clustermetadatapb.DurabilityPolicy {
	return &clustermetadatapb.DurabilityPolicy{
		PolicyName:    fmt.Sprintf("MULTI_CELL_AT_LEAST_%d", n),
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N,
		RequiredCount: n,
		Description:   fmt.Sprintf("At least %d nodes from different cells must acknowledge", n),
	}
}
