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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// poolerIDs extracts the clustermetadata IDs from a slice of PoolerHealthState.
// Used at the boundary where poolers cross into the durability-policy layer,
// which operates on bare *clustermetadatapb.ID values.
func poolerIDs(poolers []*multiorchdatapb.PoolerHealthState) []*clustermetadatapb.ID {
	out := make([]*clustermetadatapb.ID, len(poolers))
	for i, p := range poolers {
		out[i] = p.MultiPooler.Id
	}
	return out
}
