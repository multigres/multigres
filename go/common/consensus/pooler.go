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

import clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

// IsPrimary reports whether the pooler identified by cs is the designated
// primary according to its highest committed rule. Returns false when cs, its
// ID, or the current rule is absent.
func IsPrimary(cs *clustermetadatapb.ConsensusStatus) bool {
	if cs == nil {
		return false
	}
	self := cs.GetId()
	primary := cs.GetCurrentPosition().GetRule().GetPrimaryId()
	if self == nil || primary == nil {
		return false
	}
	return self.Cell == primary.Cell && self.Name == primary.Name
}
