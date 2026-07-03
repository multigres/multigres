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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestCohortMismatchAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewTestCache(t)
	coordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone1", Name: "test-coord"}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(nil, poolerStore, rpcClient, ts, coord, slog.Default())
	analyzer := &CohortMismatchAnalyzer{factory: factory}

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	replicaA := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-a"}
	replicaB := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-b"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	// healthyReplicaPA returns a rider for a healthy, replicating REPLICA,
	// optionally with a cohort eligibility signal.
	healthyReplicaPA := func(id *clustermetadatapb.ID, signal clustermetadatapb.CohortEligibilitySignal) *store.Pooler {
		var av *clustermetadatapb.AvailabilityStatus
		if signal != clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_UNKNOWN {
			av = &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{Signal: signal},
			}
		}
		return newRider(&multiorchdatapb.PoolerHealthState{
			MultiPooler:        &clustermetadatapb.MultiPooler{Id: id, ShardKey: shardKey},
			IsLastCheckValid:   true,
			AvailabilityStatus: av,
			Status: &multipoolermanagerdatapb.Status{
				IsInitialized: true,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{Host: "primary.example.com"},
				},
			},
		})
	}

	// leaderRider builds a fresh, currently-serving leader: recent snapshot,
	// postgres ready, names itself leader. leaderServing(sa) reads this rider, so
	// subtests that want an unusable leader mutate sa.Leader (see below).
	leaderRider := func() *store.Pooler {
		return newRider(&multiorchdatapb.PoolerHealthState{
			MultiPooler:      &clustermetadatapb.MultiPooler{Id: primaryID, ShardKey: shardKey},
			IsLastCheckValid: true,
			LastSeen:         timestamppb.Now(),
			ConsensusStatus:  primaryConsensusStatus(primaryID, 1),
			Status:           &multipoolermanagerdatapb.Status{IsInitialized: true, PostgresReady: true},
		})
	}

	healthyShard := func(standbys []*clustermetadatapb.ID, replicas ...*store.Pooler) *ShardAnalysis {
		leader := leaderRider()
		analyses := append([]*store.Pooler{leader}, replicas...)
		return &ShardAnalysis{
			ShardKey: shardKey,
			HighestShardRule: &clustermetadatapb.ShardRule{
				RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				LeaderId:      primaryID,
				CohortMembers: standbys,
			},
			Now:      time.Now(),
			Policy:   DefaultAvailabilityPolicy(),
			Leader:   leader,
			Analyses: analyses,
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
		pa.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.Status.ReplicationStatus = nil }) // not yet replicating
		sa := healthyShard(nil, pa)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores replica with stopped replication", func(t *testing.T) {
		pa := healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE)
		pa.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.Status.ReplicationStatus.IsWalReplayPaused = true })
		sa := healthyShard(nil, pa)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores uninitialized replica", func(t *testing.T) {
		pa := healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE)
		pa.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.Status.IsInitialized = false })
		sa := healthyShard(nil, pa)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("does not fire when leader is unreachable", func(t *testing.T) {
		sa := healthyShard(nil, healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE))
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.LastSeen = nil }) // stale observation → not serving
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("does not fire when leader postgres is not ready", func(t *testing.T) {
		sa := healthyShard(nil, healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE))
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.Status.PostgresReady = false })
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
		// look like an addition candidate apart from naming itself leader.
		leaderShard.Analyses[0].Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.Status.ReplicationStatus = &multipoolermanagerdatapb.StandbyReplicationStatus{
				PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{Host: "primary.example.com"},
			}
		})
		problems, err := analyzer.Analyze(leaderShard)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores replica with stale health snapshot (LastCheckValid false)", func(t *testing.T) {
		pa := healthyReplicaPA(replicaA, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE)
		pa.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.IsLastCheckValid = false })
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

	// Below: explicit tier tests for the missing-from-cache REMOVE paths.
	// These were added after CohortMismatchAnalyzer learned to drive REMOVE
	// for cohort members the cache no longer tracks (tombstones from
	// LIFECYCLE_SHUTDOWN, and untombstoned absences). They cover the
	// confidence tiers the analyzer applies before proposing a problem.

	// missingMemberShard builds a ShardAnalysis where the cohort lists every
	// id in cohortIDs, but only the analyses argument actually shows up in
	// Analyses — i.e. ids in cohortIDs but absent from analyses are
	// "missing from the cache" from the analyzer's perspective. tombstones,
	// if any, are recorded as cache tombstones so the analyzer can tell the
	// difference between "known SHUTDOWN" and "merely missing."
	missingMemberShard := func(
		policy *clustermetadatapb.DurabilityPolicy,
		cohortIDs []*clustermetadatapb.ID,
		tombstones []*clustermetadatapb.ID,
		analyses ...*store.Pooler,
	) *ShardAnalysis {
		leader := leaderRider()
		full := append([]*store.Pooler{leader}, analyses...)
		tombSet := make(map[topoclient.ComponentID]struct{}, len(tombstones))
		for _, id := range tombstones {
			tombSet[topoclient.ComponentIDString(id)] = struct{}{}
		}
		return &ShardAnalysis{
			ShardKey: shardKey,
			HighestShardRule: &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				LeaderId:         primaryID,
				CohortMembers:    cohortIDs,
				DurabilityPolicy: policy,
			},
			Now:          time.Now(),
			Policy:       DefaultAvailabilityPolicy(),
			Leader:       leader,
			Analyses:     full,
			TombstoneIDs: tombSet,
		}
	}

	atLeastN := func(n int32) *clustermetadatapb.DurabilityPolicy {
		return &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: n,
		}
	}

	t.Run("tombstoned cohort member triggers unconditional REMOVE", func(t *testing.T) {
		// Cohort = {primary, replicaA, replicaB}. replicaA is a tombstone
		// (LIFECYCLE_SHUTDOWN observed). Even though the durability policy
		// is tight (N=2 over a 3-member cohort, so a single safety-gated
		// removal would be unsafe), tombstones bypass the safety check
		// because they're already not contributing to the policy.
		sa := missingMemberShard(
			atLeastN(2),
			[]*clustermetadatapb.ID{primaryID, replicaA, replicaB},
			[]*clustermetadatapb.ID{replicaA}, // tombstone
			// Analyses contains only the leader; replicaA and replicaB are
			// both absent from sa.Analyses.
		)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "exactly one REMOVE per cycle")
		assert.Equal(t, types.ProblemCohortMemberIneligible, problems[0].Code)
		assert.Equal(t, replicaA.Name, problems[0].PoolerID.Name,
			"the tombstone — not the merely-missing replicaB — is the chosen target")
	})

	t.Run("missing-no-tombstone REMOVE proceeds when safety check passes", func(t *testing.T) {
		// 5-member cohort under N=2: removing one missing-but-not-tombstoned
		// member and simulating leader failure still leaves 3 of 4 recruitable
		// → IsCohortMemberRemovalSafe returns true.
		extraReplica1 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-c"}
		extraReplica2 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-d"}
		sa := missingMemberShard(
			atLeastN(2),
			[]*clustermetadatapb.ID{primaryID, replicaA, replicaB, extraReplica1, extraReplica2},
			nil, // no tombstones — replicaA is just missing
			// replicaB, extraReplica1, extraReplica2 present as healthy cohort members
			healthyReplicaPA(replicaB, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE),
			healthyReplicaPA(extraReplica1, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE),
			healthyReplicaPA(extraReplica2, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE),
		)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		// Exactly one REMOVE for replicaA. (No ADDs because every visible
		// non-leader is already in cohort.)
		require.Len(t, problems, 1)
		assert.Equal(t, types.ProblemCohortMemberIneligible, problems[0].Code)
		assert.Equal(t, replicaA.Name, problems[0].PoolerID.Name)
	})

	t.Run("missing-no-tombstone REMOVE is blocked when safety check fails", func(t *testing.T) {
		// 3-member cohort under N=2: removing the missing member and losing
		// the leader leaves only 1 recruitable → IsCohortMemberRemovalSafe
		// returns false. The analyzer must NOT propose a REMOVE — this is
		// the entire point of the safety gate.
		sa := missingMemberShard(
			atLeastN(2),
			[]*clustermetadatapb.ID{primaryID, replicaA, replicaB},
			nil, // no tombstones
			healthyReplicaPA(replicaB, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE),
		)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems,
			"unsafe missing-no-tombstone removal must be suppressed")
	})

	t.Run("tombstone is preferred over a competing missing-no-tombstone candidate", func(t *testing.T) {
		// Two removal candidates in the same cycle: replicaA (tombstone) and
		// replicaB (missing-no-tombstone). The cap of one REMOVE per cycle
		// is documented behavior; tombstones outrank because they're
		// higher-confidence "this pooler is gone."
		extraReplica1 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-c"}
		extraReplica2 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-d"}
		sa := missingMemberShard(
			atLeastN(2),
			[]*clustermetadatapb.ID{primaryID, replicaA, replicaB, extraReplica1, extraReplica2},
			[]*clustermetadatapb.ID{replicaA}, // replicaA is a tombstone
			// replicaB is also missing (no analysis), but isn't a tombstone.
			// extraReplica1/2 are healthy cohort members.
			healthyReplicaPA(extraReplica1, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE),
			healthyReplicaPA(extraReplica2, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE),
		)
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Equal(t, replicaA.Name, problems[0].PoolerID.Name,
			"tombstoned member must be chosen over a merely-missing one")
	})
}
