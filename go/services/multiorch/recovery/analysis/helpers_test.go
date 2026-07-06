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
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// newRider wraps a PoolerHealthState in a cache rider for analyzer tests.
// Tests build the real PoolerHealthState (the same shape the generator feeds
// analyzers in production) rather than a digest mirror; namesSelfAsLeader and
// the other derivations then fall out of the raw consensus/status the same way
// they do at runtime (use primaryConsensusStatus for the leader case).
func newRider(h *multiorchdatapb.PoolerHealthState) *store.Pooler {
	return store.NewPooler(h, nil)
}

// flattenShardAnalyses collects all rider entries across all ShardAnalysis groups.
// Used in tests to assert on individual poolers without caring about shard grouping.
func flattenShardAnalyses(shards []*ShardAnalysis) []*store.Pooler {
	var analyses []*store.Pooler
	for _, sa := range shards {
		analyses = append(analyses, sa.Analyses...)
	}
	return analyses
}

// findPoolerByName returns the rider with the given name from a ShardAnalysis,
// or nil if not found.
func findPoolerByName(sa *ShardAnalysis, name string) *store.Pooler {
	for _, pa := range sa.Analyses {
		if poolerID(pa).Name == name {
			return pa
		}
	}
	return nil
}

// analyzeOne wraps a single rider in a ShardAnalysis, calls Analyze, and returns
// the first problem detected (or nil) along with any error.
func analyzeOne(analyzer Analyzer, a *store.Pooler) (*types.Problem, error) {
	sa := &ShardAnalysis{
		Analyses: []*store.Pooler{a},
		Policy:   DefaultAvailabilityPolicy(),
	}
	problems, err := analyzer.Analyze(sa)
	if len(problems) > 0 {
		p := problems[0]
		return &p, err
	}
	return nil, err
}
