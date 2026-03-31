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
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// flattenShardAnalyses collects all ReplicationAnalysis entries across all ShardAnalysis groups.
// Used in tests to assert on individual analyses without caring about shard grouping.
func flattenShardAnalyses(shards []*ShardAnalysis) []*store.ReplicationAnalysis {
	var analyses []*store.ReplicationAnalysis
	for _, sa := range shards {
		analyses = append(analyses, sa.Analyses...)
	}
	return analyses
}

// analyzeOne wraps a single ReplicationAnalysis in a ShardAnalysis, calls Analyze,
// and returns the first problem detected (or nil) along with any error.
// Used in tests that exercise single-pooler scenarios.
func analyzeOne(analyzer Analyzer, a *store.ReplicationAnalysis) (*types.Problem, error) {
	sa := &ShardAnalysis{Analyses: []*store.ReplicationAnalysis{a}}
	problems, err := analyzer.Analyze(sa)
	if len(problems) > 0 {
		p := problems[0]
		return &p, err
	}
	return nil, err
}
