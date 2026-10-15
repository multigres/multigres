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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// flattenShardAnalyses collects all rider entries across all ShardAnalysis groups.
// Used in tests to assert on individual poolers without caring about shard grouping.
func flattenShardAnalyses(shards []*ShardAnalysis) []*multiorchdatapb.PoolerHealthState {
	var analyses []*multiorchdatapb.PoolerHealthState
	for _, sa := range shards {
		analyses = append(analyses, sa.Poolers...)
	}
	return analyses
}

// findPoolerByName returns the rider with the given name from a ShardAnalysis,
// or nil if not found.
func findPoolerByName(sa *ShardAnalysis, name string) *multiorchdatapb.PoolerHealthState {
	for _, pa := range sa.Poolers {
		if poolerID(pa).Name == name {
			return pa
		}
	}
	return nil
}

// analyzeOne wraps a single rider in a ShardAnalysis, calls Analyze, and returns
// the first problem detected (or nil) along with any error.
func analyzeOne(analyzer Analyzer, a *multiorchdatapb.PoolerHealthState) (*types.Problem, error) {
	sa := &ShardAnalysis{
		Poolers: []*multiorchdatapb.PoolerHealthState{a},
		Policy:  DefaultAvailabilityPolicy(),
	}
	problems, err := analyzer.Analyze(sa)
	if len(problems) > 0 {
		p := problems[0]
		return &p, err
	}
	return nil, err
}

// ruleCarrierName names the inert pooler newShardAnalysis uses to publish a rule.
const ruleCarrierName = "rule-carrier"

// newShardAnalysis publishes rule as the shard's highest position and adds
// leader to the shard's poolers. A nil rule with a leader defaults to a rule
// naming that leader. The rule rides on an inert observer pooler (never
// observed, so never evidence of anything), keeping the leader's own health
// untouched.
func newShardAnalysis(sa *ShardAnalysis, rule *clustermetadatapb.RulePosition, leader *multiorchdatapb.PoolerHealthState) *ShardAnalysis {
	if leader != nil {
		if !slices.Contains(sa.Poolers, leader) {
			sa.Poolers = append(sa.Poolers, leader)
		}
		if rule == nil {
			rule = &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{LeaderId: poolerID(leader)}}
		}
	}
	setRule(sa, rule)
	return sa
}

// setRule replaces the shard's published rule; nil clears it.
func setRule(sa *ShardAnalysis, rule *clustermetadatapb.RulePosition) {
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
func removePooler(sa *ShardAnalysis, name string) {
	kept := sa.Poolers[:0:0]
	for _, p := range sa.Poolers {
		if poolerID(p).GetName() != name {
			kept = append(kept, p)
		}
	}
	sa.Poolers = kept
}

// replacePooler swaps the named pooler's snapshot for state.
func replacePooler(sa *ShardAnalysis, id *clustermetadatapb.ID, state *multiorchdatapb.PoolerHealthState) {
	for i, p := range sa.Poolers {
		if poolerID(p).GetName() == id.GetName() {
			sa.Poolers[i] = state
			return
		}
	}
}

// setFollowers replaces the shard's follower snapshots, keeping the published
// rule and the leader.
func setFollowers(sa *ShardAnalysis, followers ...*multiorchdatapb.PoolerHealthState) {
	leader := sa.IntendedLeader()
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
