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

// ConsensusRole is the role a pooler plays in the highest consensus rule it
// knows about: leader, follower (a cohort member that is not the leader), or
// observer (not a cohort member, or no rule is known yet). It is derived from
// ConsensusStatus and answers "what does consensus say this node is?" — distinct
// from the routing role (writability self-report) the gateway uses.
type ConsensusRole int

const (
	// ConsensusRoleObserver means the pooler is not a cohort member of the
	// highest known rule (or no rule is known yet).
	ConsensusRoleObserver ConsensusRole = iota
	// ConsensusRoleFollower means the pooler is a cohort member of the highest
	// known rule but is not its leader.
	ConsensusRoleFollower
	// ConsensusRoleLeader means the highest known rule names the pooler as leader.
	ConsensusRoleLeader
)

// String returns a human-readable name for the consensus role.
func (r ConsensusRole) String() string {
	switch r {
	case ConsensusRoleLeader:
		return "leader"
	case ConsensusRoleFollower:
		return "follower"
	case ConsensusRoleObserver:
		return "observer"
	default:
		return "unknown"
	}
}

// SelfConsensusRole reports the pooler's role in the highest rule it knows
// (HighestKnownRule over this single status): leader if that rule names self,
// follower if self is one of that rule's cohort members, observer otherwise
// (including when cs, its ID, or any known rule is absent).
//
// Callers that only need a leader/non-leader answer compare against
// ConsensusRoleLeader; keep follower and observer distinct otherwise, since
// treating a non-cohort observer as a follower is a bug.
func SelfConsensusRole(cs *clustermetadatapb.ConsensusStatus) ConsensusRole {
	rule := HighestKnownRule([]*clustermetadatapb.ConsensusStatus{cs})
	self := cs.GetId()
	if self == nil {
		return ConsensusRoleObserver
	}
	if RuleNamesLeader(rule, self) {
		return ConsensusRoleLeader
	}
	for _, member := range rule.GetCohortMembers() {
		if idsEqual(member, self) {
			return ConsensusRoleFollower
		}
	}
	return ConsensusRoleObserver
}

// IsNonRevokedCommittedLeader reports whether cs's committed position names its
// own pooler as leader and that committed rule has not been revoked. This is the
// write-safety leadership input: it stays false in the window between pg_promote
// and the new rule being committed to WAL, so a freshly-promoted pooler is not
// treated as the writable leader until its leadership is durably committed.
func IsNonRevokedCommittedLeader(cs *clustermetadatapb.ConsensusStatus) bool {
	committed := cs.GetCurrentPosition().GetRule()
	if !RuleNamesLeader(committed, cs.GetId()) {
		return false
	}
	return !IsRuleRevoked(committed, cs.GetTermRevocation())
}

// LeaderTerm returns the coordinator term of the pooler's current recorded
// rule if the pooler names itself as leader (SelfConsensusRole is leader).
// Returns 0 when it does not, when the consensus status is nil/empty, or when
// the rule has no coordinator term.
func LeaderTerm(cs *clustermetadatapb.ConsensusStatus) int64 {
	if SelfConsensusRole(cs) != ConsensusRoleLeader {
		return 0
	}
	return cs.GetCurrentPosition().GetRule().GetRuleNumber().GetCoordinatorTerm()
}
