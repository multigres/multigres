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

// IsLeader reports whether the pooler identified by cs is the consensus-elected
// leader according to its highest committed rule. Returns false when cs, its
// ID, or the current rule is absent.
func IsLeader(cs *clustermetadatapb.ConsensusStatus) bool {
	if cs == nil {
		return false
	}
	self := cs.GetId()
	leader := cs.GetCurrentPosition().GetRule().GetLeaderId()
	if self == nil || leader == nil {
		return false
	}
	return self.Cell == leader.Cell && self.Name == leader.Name
}

// LeaderTerm returns the coordinator term of the pooler's current committed
// rule if the pooler holds the leader role (per IsLeader). Returns 0 when
// the pooler is not the leader, when the consensus status is nil/empty, or
// when the rule has no coordinator term.
func LeaderTerm(cs *clustermetadatapb.ConsensusStatus) int64 {
	if !IsLeader(cs) {
		return 0
	}
	return cs.GetCurrentPosition().GetRule().GetRuleNumber().GetCoordinatorTerm()
}
