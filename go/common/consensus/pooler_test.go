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

func TestSelfConsensusRoleLeadership(t *testing.T) {
	id := func(cell, name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{Cell: cell, Name: name}
	}
	statusWithRule := func(self, leader *clustermetadatapb.ID) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{
			Id: self,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{LeaderId: leader},
			},
		}
	}
	// ruleAt builds a ShardRule naming leader at the given coordinator term.
	ruleAt := func(leader *clustermetadatapb.ID, term int64) *clustermetadatapb.ShardRule {
		return &clustermetadatapb.ShardRule{
			LeaderId:   leader,
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
		}
	}

	tests := []struct {
		name string
		cs   *clustermetadatapb.ConsensusStatus
		want bool
	}{
		{
			name: "nil status",
			cs:   nil,
			want: false,
		},
		{
			name: "nil id",
			cs:   statusWithRule(nil, id("zone1", "pooler-1")),
			want: false,
		},
		{
			name: "nil current_position",
			cs:   &clustermetadatapb.ConsensusStatus{Id: id("zone1", "pooler-1")},
			want: false,
		},
		{
			name: "nil rule",
			cs: &clustermetadatapb.ConsensusStatus{
				Id:              id("zone1", "pooler-1"),
				CurrentPosition: &clustermetadatapb.PoolerPosition{},
			},
			want: false,
		},
		{
			name: "nil leader_id",
			cs:   statusWithRule(id("zone1", "pooler-1"), nil),
			want: false,
		},
		{
			name: "self matches leader",
			cs:   statusWithRule(id("zone1", "pooler-1"), id("zone1", "pooler-1")),
			want: true,
		},
		{
			name: "different name",
			cs:   statusWithRule(id("zone1", "pooler-1"), id("zone1", "pooler-2")),
			want: false,
		},
		{
			name: "different cell",
			cs:   statusWithRule(id("zone1", "pooler-1"), id("zone2", "pooler-1")),
			want: false,
		},
		{
			// Mid-demotion: current position still self-claims (term 5) but the
			// replication primary already names a higher-term leader. The node has
			// learned it is superseded, so it does not name itself at its highest
			// known rule.
			name: "self-claim superseded by a higher replication-primary rule",
			cs: &clustermetadatapb.ConsensusStatus{
				Id:              id("zone1", "pooler-1"),
				CurrentPosition: &clustermetadatapb.PoolerPosition{Rule: ruleAt(id("zone1", "pooler-1"), 5)},
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule: ruleAt(id("zone1", "pooler-2"), 6),
				},
			},
			want: false,
		},
		{
			// A legitimate leader at the highest known rule: a lower-term stale
			// replication primary must not unseat its self-claim.
			name: "self-claim at highest rule despite a lower replication-primary rule",
			cs: &clustermetadatapb.ConsensusStatus{
				Id:              id("zone1", "pooler-1"),
				CurrentPosition: &clustermetadatapb.PoolerPosition{Rule: ruleAt(id("zone1", "pooler-1"), 6)},
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule: ruleAt(id("zone1", "pooler-2"), 4),
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, SelfConsensusRole(tt.cs) == ConsensusRoleLeader)
		})
	}
}

func TestSelfConsensusRole(t *testing.T) {
	id := func(cell, name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{Cell: cell, Name: name}
	}
	self := id("zone1", "pooler-1")
	other := id("zone1", "pooler-2")

	tests := []struct {
		name string
		cs   *clustermetadatapb.ConsensusStatus
		want ConsensusRole
	}{
		{
			name: "nil status is observer",
			cs:   nil,
			want: ConsensusRoleObserver,
		},
		{
			name: "no known rule is observer",
			cs:   &clustermetadatapb.ConsensusStatus{Id: self},
			want: ConsensusRoleObserver,
		},
		{
			name: "rule names self as leader",
			cs: &clustermetadatapb.ConsensusStatus{
				Id: self,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{LeaderId: self, CohortMembers: []*clustermetadatapb.ID{self, other}},
				},
			},
			want: ConsensusRoleLeader,
		},
		{
			name: "self is a cohort member but not leader",
			cs: &clustermetadatapb.ConsensusStatus{
				Id: self,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{LeaderId: other, CohortMembers: []*clustermetadatapb.ID{self, other}},
				},
			},
			want: ConsensusRoleFollower,
		},
		{
			name: "self is not in the cohort",
			cs: &clustermetadatapb.ConsensusStatus{
				Id: self,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{LeaderId: other, CohortMembers: []*clustermetadatapb.ID{other}},
				},
			},
			want: ConsensusRoleObserver,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, SelfConsensusRole(tt.cs))
		})
	}
}

func TestIsNonRevokedCommittedLeader(t *testing.T) {
	id := func(cell, name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{Cell: cell, Name: name}
	}
	self := id("zone1", "pooler-1")
	other := id("zone1", "pooler-2")
	committedRule := func(leader *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{
			Id: self,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					LeaderId:   leader,
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				},
			},
		}
	}

	tests := []struct {
		name string
		cs   *clustermetadatapb.ConsensusStatus
		want bool
	}{
		{
			name: "nil status",
			cs:   nil,
			want: false,
		},
		{
			name: "committed rule names another pooler",
			cs:   committedRule(other, 5),
			want: false,
		},
		{
			name: "committed rule names self, not revoked",
			cs:   committedRule(self, 5),
			want: true,
		},
		{
			// The pg_promote→WAL-commit window: the highest known rule may name
			// self, but the *committed* position does not yet — so write-safety
			// leadership stays false until the new rule is durably committed.
			name: "self-claim only in replication primary, not yet committed",
			cs: &clustermetadatapb.ConsensusStatus{
				Id:              self,
				CurrentPosition: &clustermetadatapb.PoolerPosition{Rule: &clustermetadatapb.ShardRule{LeaderId: other, RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}}},
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule: &clustermetadatapb.ShardRule{LeaderId: self, RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}},
				},
			},
			want: false,
		},
		{
			name: "committed rule names self but is revoked",
			cs: &clustermetadatapb.ConsensusStatus{
				Id:              self,
				CurrentPosition: &clustermetadatapb.PoolerPosition{Rule: &clustermetadatapb.ShardRule{LeaderId: self, RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}}},
				TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsNonRevokedCommittedLeader(tt.cs))
		})
	}
}

func TestLeaderTerm(t *testing.T) {
	id := func(cell, name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{Cell: cell, Name: name}
	}

	tests := []struct {
		name string
		cs   *clustermetadatapb.ConsensusStatus
		want int64
	}{
		{
			name: "nil status",
			cs:   nil,
			want: 0,
		},
		{
			name: "not leader",
			cs: &clustermetadatapb.ConsensusStatus{
				Id: id("zone1", "pooler-1"),
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						LeaderId:   id("zone1", "pooler-2"),
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 7},
					},
				},
			},
			want: 0,
		},
		{
			name: "is leader with term",
			cs: &clustermetadatapb.ConsensusStatus{
				Id: id("zone1", "pooler-1"),
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						LeaderId:   id("zone1", "pooler-1"),
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 7},
					},
				},
			},
			want: 7,
		},
		{
			name: "is leader with no rule number",
			cs: &clustermetadatapb.ConsensusStatus{
				Id: id("zone1", "pooler-1"),
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						LeaderId: id("zone1", "pooler-1"),
					},
				},
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, LeaderTerm(tt.cs))
		})
	}
}
