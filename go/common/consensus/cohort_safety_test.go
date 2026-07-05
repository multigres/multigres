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
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// id returns a stable *ID for a pooler named `name`.
func poolerID(name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      name,
	}
}

// atLeastNRule builds a ShardRule with an AT_LEAST_N durability policy of
// the given N, the supplied cohort members, and leaderID (may be nil to
// model a rule with no leader).
func atLeastNRule(n int32, leaderID *clustermetadatapb.ID, cohort ...*clustermetadatapb.ID) *clustermetadatapb.ShardRule {
	return &clustermetadatapb.ShardRule{
		LeaderId: leaderID,
		DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: n,
		},
		CohortMembers: cohort,
	}
}

func TestIsCohortMemberRemovalSafe_NilRule(t *testing.T) {
	assert.False(t, IsCohortMemberRemovalSafe(nil, poolerID("a")))
}

func TestIsCohortMemberRemovalSafe_NilMember(t *testing.T) {
	rule := atLeastNRule(2, poolerID("a"), poolerID("a"), poolerID("b"), poolerID("c"))
	assert.False(t, IsCohortMemberRemovalSafe(rule, nil))
}

func TestIsCohortMemberRemovalSafe_MemberNotInCohort(t *testing.T) {
	// The caller is asking us to evaluate a removal that wouldn't happen —
	// we must not pretend the cohort can shrink to accommodate it. Returning
	// true here would let bad call sites slip through.
	rule := atLeastNRule(2, poolerID("a"), poolerID("a"), poolerID("b"), poolerID("c"))
	assert.False(t, IsCohortMemberRemovalSafe(rule, poolerID("stranger")))
}

func TestIsCohortMemberRemovalSafe_NilDurabilityPolicy(t *testing.T) {
	// Without a policy we cannot compute the safety bound, so the conservative
	// answer is "not safe."
	rule := &clustermetadatapb.ShardRule{
		LeaderId:      poolerID("a"),
		CohortMembers: []*clustermetadatapb.ID{poolerID("a"), poolerID("b"), poolerID("c")},
	}
	assert.False(t, IsCohortMemberRemovalSafe(rule, poolerID("b")))
}

func TestIsCohortMemberRemovalSafe_BadPolicy(t *testing.T) {
	// AT_LEAST_N with N=0 is rejected by NewPolicyFromProto; we must propagate
	// that as "not safe" rather than crashing or being permissive.
	rule := &clustermetadatapb.ShardRule{
		LeaderId: poolerID("a"),
		DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 0,
		},
		CohortMembers: []*clustermetadatapb.ID{poolerID("a"), poolerID("b"), poolerID("c")},
	}
	assert.False(t, IsCohortMemberRemovalSafe(rule, poolerID("b")))
}

func TestIsCohortMemberRemovalSafe_AmpleCohortRemovalOK(t *testing.T) {
	// 5-member cohort, AT_LEAST_N=2, removing one follower while the leader
	// also fails leaves 3 recruitable poolers — easily a quorum.
	//   newCohort = [a,c,d,e], simulated leader (a) failure → recruited = [c,d,e]
	//   |recruited|=3 ≥ majority(4)=3 ✓     |cohort\recruited|=1 < N=2 ✓
	rule := atLeastNRule(2, poolerID("a"),
		poolerID("a"), poolerID("b"), poolerID("c"), poolerID("d"), poolerID("e"))
	assert.True(t, IsCohortMemberRemovalSafe(rule, poolerID("b")))
}

func TestIsCohortMemberRemovalSafe_ThreeMemberCohortIsUnsafe(t *testing.T) {
	// 3-member cohort, AT_LEAST_N=2. Removing one and then losing the leader
	// leaves a single survivor — below the majority threshold, so a future
	// leader cannot be elected. This is the realistic failure mode that
	// motivated adding the safety check in the first place.
	rule := atLeastNRule(2, poolerID("a"), poolerID("a"), poolerID("b"), poolerID("c"))
	assert.False(t, IsCohortMemberRemovalSafe(rule, poolerID("b")))
}

func TestIsCohortMemberRemovalSafe_NoLeaderSkipsLeaderFailureStep(t *testing.T) {
	// Without a current leader we only simulate the removal — there's no
	// leader to take down. With cohort=[a,b,c] removing b → [a,c],
	// recruited=[a,c]: |2|≥majority(2)=2 ✓, 0 < N=2 ✓ → safe.
	// Confirms we don't accidentally remove a leader the rule never named.
	rule := atLeastNRule(2, nil /*no leader*/, poolerID("a"), poolerID("b"), poolerID("c"))
	assert.True(t, IsCohortMemberRemovalSafe(rule, poolerID("b")))
}

func TestIsCohortMemberRemovalSafe_BoundaryFiveMemberAtLeastThree(t *testing.T) {
	// Tightens the boundary: AT_LEAST_N=3 with a 5-cohort. After removing
	// one follower and losing the leader, recruited=3 out of cohort=4.
	// Majority(4)=3 ✓, but |cohort\recruited| = 1 < N=3 ✓. Safe.
	rule := atLeastNRule(3, poolerID("a"),
		poolerID("a"), poolerID("b"), poolerID("c"), poolerID("d"), poolerID("e"))
	assert.True(t, IsCohortMemberRemovalSafe(rule, poolerID("b")))

	// One fewer: 4-cohort AT_LEAST_N=3 is unsafe — leaving 2 recruited out
	// of 3 remaining cohort doesn't satisfy the revocation bound
	// (|cohort\recruited|=1 < N=3 looks OK, but majority(3)=2 ✓ and the
	// remaining 1 < N=3 ✓ — so this is actually safe). The clearer unsafe
	// case is 3-cohort N=3: every single member is required.
	tightRule := atLeastNRule(3, poolerID("a"), poolerID("a"), poolerID("b"), poolerID("c"))
	assert.False(t, IsCohortMemberRemovalSafe(tightRule, poolerID("b")),
		"removing any cohort member from an N=3 / cohort=3 setup must fail")
}
