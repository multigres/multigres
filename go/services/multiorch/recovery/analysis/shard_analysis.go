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
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// ShardAnalysis groups all per-pooler analyses for a single shard.
// It is the input type for the Analyzer interface.
type ShardAnalysis struct {
	ShardKey commontypes.ShardKey
	Analyses []*store.ReplicationAnalysis
}

// analyzeAllPoolers runs fn against each pooler analysis in sa, collecting all problems.
// Errors are accumulated — the first error encountered is returned alongside any problems collected.
func analyzeAllPoolers(sa *ShardAnalysis, fn func(*store.ReplicationAnalysis) (*types.Problem, error)) ([]types.Problem, error) {
	var problems []types.Problem
	var firstErr error
	for _, poolerAnalysis := range sa.Analyses {
		p, err := fn(poolerAnalysis)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if p != nil {
			problems = append(problems, *p)
		}
	}
	return problems, firstErr
}
