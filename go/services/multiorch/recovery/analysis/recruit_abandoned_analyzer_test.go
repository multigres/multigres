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

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestRecruitAbandonedAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	coordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "test-coord"}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	now := time.Now()

	// ruleLedBy is the leader's committed rule at term 1 — the rule orch would
	// relay to the follower.
	ruleLedBy := func(term, subterm int64) *clustermetadatapb.RulePosition {
		return &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm},
			LeaderId:   primaryID,
		}}
	}
	highestPosition := ruleLedBy(1, 0)

	leaderHealth := func() *store.Pooler {
		return store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler:      &clustermetadatapb.Multipooler{Id: primaryID, ShardKey: shardKey, Hostname: "primary.example.com"},
			IsLastCheckValid: true,
			LastSeen:         timestamppb.New(now),
			Status:           &multipoolermanagerdatapb.Status{PostgresReady: true},
			ConsensusStatus:  &clustermetadatapb.ConsensusStatus{Id: primaryID, CurrentPosition: &clustermetadatapb.PoolerPosition{Position: ruleLedBy(1, 0)}},
		}, nil)
	}

	// strandedFollower builds a follower recruited at term 2 (revocation revokes
	// the term-1 rule) whose own WAL rule is still at term 1 — the abandoned
	// recruit. initiatedAt controls the revocation's age for the grace gate.
	strandedFollower := func(initiatedAt time.Time, ownRule *clustermetadatapb.RulePosition, initialized bool) *store.Pooler {
		return newRider(&multiorchdatapb.PoolerHealthState{
			Multipooler:      &clustermetadatapb.Multipooler{Id: replicaID, ShardKey: shardKey},
			IsLastCheckValid: true,
			LastSeen:         timestamppb.New(now),
			Status:           &multipoolermanagerdatapb.Status{IsInitialized: initialized},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:              replicaID,
				CurrentPosition: &clustermetadatapb.PoolerPosition{Position: ownRule},
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       2,
					OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					AcceptedCoordinatorId:  coordID,
					CoordinatorInitiatedAt: timestamppb.New(initiatedAt),
				},
			},
		})
	}

	// newSA builds a ShardAnalysis with the given analyses, a serving leader, and
	// the shard's committed rule at term 1.
	newSA := func(analyses ...*store.Pooler) *ShardAnalysis {
		return &ShardAnalysis{
			ShardKey:        shardKey,
			Analyses:        analyses,
			Leader:          leaderHealth(),
			HighestPosition: highestPosition,
			Now:             now,
			Policy:          DefaultAvailabilityPolicy(),
		}
	}

	// noGraceFactory treats a revocation as abandoned immediately (grace 0).
	noGraceFactory := NewRecoveryActionFactory(config.NewTestConfig(), store.NewTestCache(t), rpcClient, ts, coord, slog.Default())

	t.Run("detects a stranded over-recruited follower", func(t *testing.T) {
		analyzer := &RecruitAbandonedAnalyzer{factory: noGraceFactory}
		problems, err := analyzer.Analyze(newSA(strandedFollower(now.Add(-time.Minute), ruleLedBy(1, 0), true)))
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemReplicaRecruitAbandoned, problems[0].Code)
		require.Equal(t, types.ScopePooler, problems[0].Scope)
		require.Equal(t, types.PriorityHigh, problems[0].Priority)
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("ignores follower that already caught up to the revoked term", func(t *testing.T) {
		analyzer := &RecruitAbandonedAnalyzer{factory: noGraceFactory}
		// Own rule reached term 2: no longer self-revoked.
		problems, err := analyzer.Analyze(newSA(strandedFollower(now.Add(-time.Minute), ruleLedBy(2, 0), true)))
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores follower within the failover grace window", func(t *testing.T) {
		graceFactory := NewRecoveryActionFactory(
			config.NewTestConfig(config.WithLeaderFailoverGracePeriodBase(10*time.Second)),
			store.NewTestCache(t), rpcClient, ts, coord, slog.Default())
		analyzer := &RecruitAbandonedAnalyzer{factory: graceFactory}
		// Revocation issued just now, well inside the 10s grace: a legit failover
		// may still be completing.
		problems, err := analyzer.Analyze(newSA(strandedFollower(now, ruleLedBy(1, 0), true)))
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores uninitialized follower", func(t *testing.T) {
		analyzer := &RecruitAbandonedAnalyzer{factory: noGraceFactory}
		problems, err := analyzer.Analyze(newSA(strandedFollower(now.Add(-time.Minute), ruleLedBy(1, 0), false)))
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores when no serving leader is known", func(t *testing.T) {
		analyzer := &RecruitAbandonedAnalyzer{factory: noGraceFactory}
		sa := newSA(strandedFollower(now.Add(-time.Minute), ruleLedBy(1, 0), true))
		sa.Leader = nil
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		analyzer := &RecruitAbandonedAnalyzer{factory: nil}
		problems, err := analyzer.Analyze(newSA(strandedFollower(now.Add(-time.Minute), ruleLedBy(1, 0), true)))
		require.Error(t, err)
		require.Nil(t, problems)
	})

	// A stranded follower also looks "not replicating" (no primary_conninfo), but
	// ReplicaNotReplicating must defer it to this analyzer rather than firing a
	// SetPrimary the follower would ignore.
	t.Run("ReplicaNotReplicating skips a stranded follower", func(t *testing.T) {
		rnr := &ReplicaNotReplicatingAnalyzer{factory: noGraceFactory}
		problems, err := rnr.Analyze(newSA(strandedFollower(now.Add(-time.Minute), ruleLedBy(1, 0), true)))
		require.NoError(t, err)
		require.Empty(t, problems)
	})
}
