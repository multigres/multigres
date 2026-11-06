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

package analysis

import (
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// findPoolerByName returns the rider with the given name from a shard snapshot,
// or nil if not found.
func findPoolerByName(sa *snapshot.Shard, name string) *multiorchdatapb.PoolerHealthState {
	for _, pa := range sa.Poolers {
		if poolerID(pa).Name == name {
			return pa
		}
	}
	return nil
}

// analyzeOne wraps a single pooler snapshot in a shard snapshot, calls Analyze, and returns
// the first problem detected (or nil) along with any error.
func analyzeOne(analyzer Analyzer, a *multiorchdatapb.PoolerHealthState) (*types.Problem, error) {
	sa := &snapshot.Shard{
		Poolers:            []*multiorchdatapb.PoolerHealthState{a},
		AvailabilityPolicy: DefaultAvailabilityPolicy(),
	}
	problems, err := analyzer.Analyze(sa)
	if len(problems) > 0 {
		p := problems[0]
		return &p, err
	}
	return nil, err
}

// ruleCarrierName names the inert pooler newTestShard uses to publish a rule.
const ruleCarrierName = "rule-carrier"

// newTestShard publishes rule as the shard's highest position and adds
// leader to the shard's poolers. A nil rule with a leader defaults to a rule
// naming that leader. The rule rides on an inert observer pooler (never
// observed, so never evidence of anything), keeping the leader's own health
// untouched.
func newTestShard(sa *snapshot.Shard, rule *clustermetadatapb.RulePosition, leader *multiorchdatapb.PoolerHealthState) *snapshot.Shard {
	if leader != nil {
		if !slices.Contains(sa.Poolers, leader) {
			sa.Poolers = append(sa.Poolers, leader)
		}
		if rule == nil {
			rule = &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				LeaderId:   poolerID(leader),
			}}
		}
	}
	setRule(sa, rule)
	return sa
}

// editRule applies fn to a copy of the shard's published rule and republishes it.
func editRule(sa *snapshot.Shard, fn func(*clustermetadatapb.RulePosition)) {
	rule := proto.Clone(sa.HighestIntendedRule().Position()).(*clustermetadatapb.RulePosition)
	fn(rule)
	setRule(sa, rule)
}

// setRule replaces the shard's published rule; nil clears it.
func setRule(sa *snapshot.Shard, rule *clustermetadatapb.RulePosition) {
	for _, p := range sa.Poolers {
		if poolerID(p).GetName() == ruleCarrierName {
			p.ConsensusStatus = &clustermetadatapb.ConsensusStatus{
				Id:              poolerID(p),
				CurrentPosition: &clustermetadatapb.PoolerPosition{Position: rule},
			}
			return
		}
	}
	if rule == nil {
		return
	}
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test", Name: ruleCarrierName}
	// First, so it wins ties against equal rule numbers on other poolers.
	carrier := &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: id},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id:              id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{Position: rule},
		},
	}
	sa.Poolers = append([]*multiorchdatapb.PoolerHealthState{carrier}, sa.Poolers...)
}

// removePooler drops the named pooler from the shard's poolers.
func removePooler(sa *snapshot.Shard, name string) {
	kept := sa.Poolers[:0:0]
	for _, p := range sa.Poolers {
		if poolerID(p).GetName() != name {
			kept = append(kept, p)
		}
	}
	sa.Poolers = kept
}

// replacePooler swaps the named pooler's snapshot for state.
func replacePooler(sa *snapshot.Shard, id *clustermetadatapb.ID, state *multiorchdatapb.PoolerHealthState) {
	for i, p := range sa.Poolers {
		if poolerID(p).GetName() == id.GetName() {
			sa.Poolers[i] = state
			return
		}
	}
}

// setFollowers replaces the shard's follower snapshots, keeping the published
// rule and the leader.
func setFollowers(sa *snapshot.Shard, followers ...*multiorchdatapb.PoolerHealthState) {
	leader := sa.IntendedLeaderHealth()
	var next []*multiorchdatapb.PoolerHealthState
	for _, p := range sa.Poolers {
		if poolerID(p).GetName() == ruleCarrierName {
			next = append(next, p)
		}
	}
	next = append(next, followers...)
	if leader != nil {
		next = append(next, leader)
	}
	sa.Poolers = next
}

// testCapture freezes shards from a test cache the way the engine does.
type testCapture struct {
	cache *store.PoolerCache
	now   time.Time
}

func newTestCapture(cache *store.PoolerCache) *testCapture {
	return &testCapture{cache: cache, now: time.Now()}
}

func (c *testCapture) opts() store.SnapshotOptions {
	return store.SnapshotOptions{Now: c.now, AvailabilityPolicy: DefaultAvailabilityPolicy()}
}

func (c *testCapture) shard(key *clustermetadatapb.ShardRef) (*snapshot.Shard, error) {
	return store.SnapshotShard(c.cache, key, c.opts())
}

// primaryConsensusStatus builds a ConsensusStatus that names id as the leader
// in its current rule with the given coordinator term. This is the minimal
// fixture required for SelfConsensusRole to report ConsensusRoleLeader for a given pooler.
func primaryConsensusStatus(id *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: id,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{
				Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
					LeaderId:   id,
				},
			},
		},
	}
}
