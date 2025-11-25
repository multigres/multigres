// Copyright 2025 Supabase, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestShardNeedsBootstrapAnalyzer_Analyze(t *testing.T) {
	analyzer := &ShardNeedsBootstrapAnalyzer{}

	t.Run("detects uninitialized shard", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
			Database:        "db",
			TableGroup:      "tg",
			Shard:           "0",
			IsInitialized:   false,
			PrimaryPoolerID: nil, // no primary exists
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 1)
		require.Equal(t, ProblemShardNeedsBootstrap, problems[0].Code)
		require.Equal(t, ScopeShard, problems[0].Scope)
		require.Equal(t, PriorityShardBootstrap, problems[0].Priority)
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("ignores initialized pooler", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsInitialized:   true,
			PrimaryPoolerID: nil,
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 0)
	})

	t.Run("ignores uninitialized pooler if primary exists", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsInitialized:   false,
			PrimaryPoolerID: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 0)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, CheckName("ShardNeedsBootstrap"), analyzer.Name())
	})
}
