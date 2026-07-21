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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// fakeCohortRuleStore is a minimal RuleStorer that only supports CachedPosition,
// enough to exercise IsPotentialCohortMember without a real postgres connection.
type fakeCohortRuleStore struct {
	pos *clustermetadatapb.PoolerPosition
}

func (f *fakeCohortRuleStore) ObservePosition(context.Context) (*clustermetadatapb.PoolerPosition, *clustermetadatapb.PoolerLsn, error) {
	return f.pos, nil, nil
}

func (f *fakeCohortRuleStore) UpdateRule(context.Context, *RuleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error) {
	return f.pos, nil
}

func (f *fakeCohortRuleStore) CreateRuleTables(context.Context, *clustermetadatapb.DurabilityPolicy, *clustermetadatapb.ID) error {
	return nil
}

func (f *fakeCohortRuleStore) CachedPosition() *clustermetadatapb.PoolerPosition {
	return f.pos
}

func (f *fakeCohortRuleStore) HasInconsistentGUC(context.Context) bool { return false }

func (f *fakeCohortRuleStore) ReconcileGUC(context.Context, bool) error { return nil }

func (f *fakeCohortRuleStore) ClearSyncStandby(context.Context) error { return nil }

func idAt(name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      name,
	}
}

func ruleWithCohort(term int64, members ...string) *clustermetadatapb.ShardRule {
	ids := make([]*clustermetadatapb.ID, len(members))
	for i, m := range members {
		ids[i] = idAt(m)
	}
	return &clustermetadatapb.ShardRule{
		RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
		CohortMembers: ids,
	}
}

func TestIsPotentialCohortMember(t *testing.T) {
	tests := []struct {
		name     string
		position *clustermetadatapb.PoolerPosition
		self     *clustermetadatapb.ID
		want     bool
	}{
		{
			name:     "no known position",
			position: nil,
			self:     idAt("self"),
			want:     false,
		},
		{
			name: "member of decided rule",
			position: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: ruleWithCohort(1, "self", "other")},
			},
			self: idAt("self"),
			want: true,
		},
		{
			name: "not a member of decided rule, no proposal",
			position: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: ruleWithCohort(1, "other")},
			},
			self: idAt("self"),
			want: false,
		},
		{
			name: "confirmed member of decision, excluded from a still-undecided newer proposal",
			position: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{
					Decision: ruleWithCohort(1, "self", "other"),
					Proposal: ruleWithCohort(2, "other", "third"),
				},
			},
			self: idAt("self"),
			// Still a decided member — must not read as "no longer a member"
			// just because a newer, unconfirmed proposal excludes them.
			want: true,
		},
		{
			name: "not yet a decided member, but named in an outstanding proposal",
			position: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{
					Decision: ruleWithCohort(1, "other"),
					Proposal: ruleWithCohort(2, "other", "self"),
				},
			},
			self: idAt("self"),
			// Treated as a potential member preemptively, before the proposal decides.
			want: true,
		},
		{
			name: "named in neither decision nor proposal",
			position: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{
					Decision: ruleWithCohort(1, "other"),
					Proposal: ruleWithCohort(2, "other", "third"),
				},
			},
			self: idAt("self"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			promises := NewConsensusPromises(t.TempDir(), tt.self)
			cm := NewManagerForTesting(t, tt.self, promises, &fakeCohortRuleStore{pos: tt.position}, nil)
			got := cm.IsPotentialCohortMember(tt.self)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRuleNamesCohortMember_NilRule(t *testing.T) {
	require.False(t, ruleNamesCohortMember(nil, idAt("self")))
}
