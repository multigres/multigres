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
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestCohortMismatchAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore()
	coordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone1", Name: "test-coord"}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(nil, poolerStore, rpcClient, ts, coord, slog.Default())
	analyzer := &CohortMismatchAnalyzer{factory: factory}

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	replicaA := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-a"}
	replicaB := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-b"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	// healthyReplicaPA returns a PoolerAnalysis for a healthy, replicating
	// REPLICA, optionally with a cohort eligibility signal.
	healthyReplicaPA := func(id *clustermetadatapb.ID, signal clustermetadatapb.CohortEligibilitySignal) *PoolerAnalysis {
		var av *clustermetadatapb.AvailabilityStatus
		if signal != clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_UNKNOWN {
			av = &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{Signal: signal},
			}
		}
		return &PoolerAnalysis{
			PoolerID:            id,
			ShardKey:            shardKey,
			LastCheckValid:      true,
			IsInitialized:       true,
			PrimaryConnInfoHost: "primary.example.com",
			WalReplayNotPaused:  true,
			AvailabilityStatus:  av,
		}
	}

	leaderPA := &PoolerAnalysis{
		PoolerID:       primaryID,
		ShardKey:       shardKey,
		IsLeader:       true,
		LastCheckValid: true,
		IsInitialized:  true,
	}

	healthyShard := func(standbys []*clustermetadatapb.ID, replicas ...*PoolerAnalysis) *ShardAnalysis {
		analyses := append([]*PoolerAnalysis{leaderPA}, replicas...)
		return &ShardAnalysis{
			ShardKey: shardKey,
			HighestShardRule: &clustermetadatapb.ShardRule{
				RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				LeaderId:      primaryID,
				CohortMembers: standbys,
			},
			LeaderReachable:     true,
			LeaderPostgresReady: true,
			Analyses:            analyses,
		}
	}

	t.Run("detects healthy replica missing from cohort", func(t *testing.T) {
		// Cohort = {} — replica A is replicating, eligible by default, and absent.
		sa := healthyShard(nil, healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_UNKNOWN))
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Equal(t, types.ProblemPoolerNotInCohort, problems[0].Code)
		assert.Equal(t, replicaA.Name, problems[0].PoolerID.Name)
		assert.Equal(t, types.PriorityNormal, problems[0].Priority)
		assert.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("detects cohort member signaling INELIGIBLE", func(t *testing.T) {
		sa := healthyShard(
			[]*clustermetadatapb.ID{replicaA},
			healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE),
		)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Equal(t, types.ProblemCohortMemberIneligible, problems[0].Code)
		assert.Equal(t, replicaA.Name, problems[0].PoolerID.Name)
	})

	t.Run("ignores eligible cohort member already in cohort", func(t *testing.T) {
		sa := healthyShard(
			[]*clustermetadatapb.ID{replicaA},
			healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE),
		)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores ineligible non-cohort pooler (don't add)", func(t *testing.T) {
		sa := healthyShard(nil, healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE))
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores non-replicating non-cohort pooler", func(t *testing.T) {
		pa := healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE)
		pa.PrimaryConnInfoHost = "" // not yet replicating
		sa := healthyShard(nil, pa)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores replica with stopped replication", func(t *testing.T) {
		pa := healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE)
		pa.WalReplayNotPaused = false
		sa := healthyShard(nil, pa)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores uninitialized replica", func(t *testing.T) {
		pa := healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE)
		pa.IsInitialized = false
		sa := healthyShard(nil, pa)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("does not fire when leader is unreachable", func(t *testing.T) {
		sa := healthyShard(nil, healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE))
		sa.LeaderReachable = false
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("does not fire when leader postgres is not ready", func(t *testing.T) {
		sa := healthyShard(nil, healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE))
		sa.LeaderPostgresReady = false
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("emits both add and remove problems together", func(t *testing.T) {
		sa := healthyShard(
			[]*clustermetadatapb.ID{replicaA}, // A is in cohort, B is not
			healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE),
			healthyReplicaPA(replicaB, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE),
		)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 2)
		codes := []types.ProblemCode{problems[0].Code, problems[1].Code}
		assert.Contains(t, codes, types.ProblemCohortMemberIneligible)
		assert.Contains(t, codes, types.ProblemPoolerNotInCohort)
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &CohortMismatchAnalyzer{factory: nil}
		sa := healthyShard(nil, healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE))
		problems, err := nilFactoryAnalyzer.Analyze(sa)
		require.Error(t, err)
		assert.Nil(t, problems)
		assert.Contains(t, err.Error(), "factory not initialized")
	})

	t.Run("ignores leader as addition candidate", func(t *testing.T) {
		// A leader's PoolerAnalysis happens to satisfy every other condition
		// (initialized, replicating-ish), but it must never be added to the
		// cohort as a "missing replica".
		leaderShard := healthyShard(nil)
		// Inject the leader as a non-cohort-member by mutating its state to
		// look like an addition candidate apart from IsLeader=true.
		leaderShard.Analyses[0].PrimaryConnInfoHost = "primary.example.com"
		problems, err := analyzer.Analyze(leaderShard)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores replica with stale health snapshot (LastCheckValid false)", func(t *testing.T) {
		pa := healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE)
		pa.LastCheckValid = false
		sa := healthyShard(nil, pa)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("metadata accessors return expected values", func(t *testing.T) {
		assert.Equal(t, types.CheckName("CohortMismatch"), analyzer.Name())
		assert.Equal(t, types.ProblemPoolerNotInCohort, analyzer.ProblemCode())
		require.NotNil(t, analyzer.RecoveryAction())
	})
}
