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
