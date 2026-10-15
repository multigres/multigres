// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recovery

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// staleHealthAnalyzer detects a shard-wide failover problem whenever the cached
// state shows no pooler with a ready postgres. It stands in for an analyzer whose
// verdict is a guess from observed health, so a live refresh can refute it.
type staleHealthAnalyzer struct {
	action      types.RecoveryAction
	skipRefresh bool
}

func (a *staleHealthAnalyzer) Name() types.CheckName { return "MockStaleHealth" }

func (a *staleHealthAnalyzer) RecoveryAction() types.RecoveryAction { return a.action }

func (a *staleHealthAnalyzer) Analyze(sa *snapshot.Shard) ([]types.Problem, error) {
	for _, p := range sa.Poolers {
		if p.GetStatus().GetPostgresReady() {
			return nil, nil
		}
	}
	return []types.Problem{{
		Code:                  types.ProblemLeaderUnhealthy,
		CheckName:             a.Name(),
		ShardKey:              sa.ShardKey,
		Priority:              types.PriorityEmergency,
		Scope:                 types.ScopeShard,
		RecoveryAction:        a.action,
		DetectedAt:            time.Now(),
		SkipPreRecheckRefresh: a.skipRefresh,
	}}, nil
}

// newRefreshEngine builds an engine over one pooler whose cached state says
// postgres is not ready, while the fake RPC client answers a live Status with
// postgres ready: a stale health guess that only a refresh can refute.
func newRefreshEngine(t *testing.T, action types.RecoveryAction, skipRefresh bool) (*Engine, *rpcclient.FakeClient, types.Problem) {
	t.Helper()
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}

	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-p1", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY, PostgresReady: true},
	})

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))
	analysis.SetTestAnalyzers([]analysis.Analyzer{&staleHealthAnalyzer{action: action, skipRefresh: skipRefresh}})
	t.Cleanup(analysis.ResetAnalyzers)

	store.SeedCache(t, engine.poolerCache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: poolerID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_PRIMARY},
		LastSeen:    timestamppb.Now(),
		Status:      &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY, PostgresReady: false},
	}, nil))

	problems := detectProblems(t, engine)
	require.Len(t, problems, 1, "the stale cache must make the problem look real")
	return engine, fakeClient, problems[0]
}

// appointAction is a stand-in failover action. A zero refreshTimeout means it
// does not ask for a refresh before the recheck.
func appointAction(refreshTimeout time.Duration) *mockRecoveryAction {
	return &mockRecoveryAction{
		metadata: types.RecoveryMetadata{Name: "Appoint", Timeout: 30 * time.Second, RefreshBeforeRecheckTimeout: refreshTimeout},
	}
}

func TestAttemptRecovery_RefreshesToVerifyAHealthGuess(t *testing.T) {
	t.Run("a live refresh refutes a stale health guess, so the action does not run", func(t *testing.T) {
		// The problem does not set SkipPreRecheckRefresh (the zero value), so it is verified.
		action := appointAction(time.Second)
		engine, fakeClient, problem := newRefreshEngine(t, action, false)

		engine.attemptRecovery(t.Context(), problem)

		assert.Contains(t, fakeClient.GetCallLog(), "Status(multipooler-cell1-p1)")
		assert.False(t, action.executed.Load(),
			"the pooler answered ready, so the recheck no longer finds the problem")
	})

	t.Run("without a refresh the same stale guess is acted on", func(t *testing.T) {
		// The action did not ask for a refresh, so the recheck sees only the stale cache.
		action := appointAction(0)
		engine, fakeClient, problem := newRefreshEngine(t, action, false)

		engine.attemptRecovery(t.Context(), problem)

		assert.Empty(t, fakeClient.GetCallLog())
		assert.True(t, action.executed.Load())
	})

	t.Run("a problem that says a refresh cannot change it is not refreshed", func(t *testing.T) {
		// The analyzer marked the verdict as unaffected by fresher health (for example a
		// resignation), so there is nothing to verify.
		action := appointAction(time.Second)
		engine, fakeClient, problem := newRefreshEngine(t, action, true)

		engine.attemptRecovery(t.Context(), problem)

		assert.Empty(t, fakeClient.GetCallLog(), "no RPC may be sent when the problem skips the refresh")
		assert.True(t, action.executed.Load())
	})
}

// TestAttemptRecovery_RegatesAFailoverAfterTheRecheck: another orchestrator's
// revocation can arrive after the engine's gate let the failover through, while
// the refresh and recheck are still running. The gate must be applied again to
// what the recheck re-detected, so we don't recruit on top of its attempt.
func TestAttemptRecovery_RegatesAFailoverAfterTheRecheck(t *testing.T) {
	action := appointAction(0)
	engine, _, problem := newRefreshEngine(t, action, false)

	poolers := store.FindPoolersInShard(engine.poolerCache, problem.ShardKey)
	require.Len(t, poolers, 1)
	poolers[0].Mutate(func(h *multiorchdatapb.PoolerHealthState) {
		h.ConsensusStatus = &clustermetadatapb.ConsensusStatus{
			CurrentPosition: &clustermetadatapb.PoolerPosition{Position: &clustermetadatapb.RulePosition{
				Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}},
			}},
			// A recruit another orchestrator just started against the current decision.
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				CoordinatorInitiatedAt: timestamppb.Now(),
				RecruitIntent: &clustermetadatapb.RecruitIntent{
					ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
					Attempt:         1,
				},
			},
		}
	})

	engine.attemptRecovery(t.Context(), problem)

	assert.False(t, action.executed.Load(), "the gate closed after the recheck, so the action must not run")
}

// TestAttemptRecovery_RefreshRefutesAStaleLeaderGuessWithTheRealAnalyzer runs the
// real LeaderNeedsReplacementAnalyzer and the real AppointLeaderAction: the cache
// says the leader's postgres is not ready, so the analyzer reports it unhealthy, but
// the leader answers a live Status as ready, so the recheck must drop the failover.
func TestAttemptRecovery_RefreshRefutesAStaleLeaderGuessWithTheRealAnalyzer(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
	id := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name}
	}
	leaderID, follower1ID, follower2ID := id("leader"), id("follower1"), id("follower2")
	decision := &clustermetadatapb.ShardRule{
		LeaderId:      leaderID,
		RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
		CohortMembers: []*clustermetadatapb.ID{leaderID, follower1ID, follower2ID},
		CreationTime:  timestamppb.New(time.Now().Add(-time.Hour)),
		DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
		},
	}
	consensusStatus := func(self *clustermetadatapb.ID) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{
			Id: self,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Lsn:      "0/1",
				Position: &clustermetadatapb.RulePosition{Decision: decision},
			},
		}
	}

	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-leader", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY, IsInitialized: true,
			PostgresRunning: true, PostgresReady: true,
		},
		ConsensusStatus: consensusStatus(leaderID),
	})
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))

	seed := func(self *clustermetadatapb.ID, status *multipoolermanagerdatapb.Status, readyAt *timestamppb.Timestamp) {
		store.SeedCache(t, engine.poolerCache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler:           &clustermetadatapb.Multipooler{Id: self, ShardKey: shardKey},
			LastSeen:              timestamppb.Now(),
			LastPostgresReadyTime: readyAt,
			Status:                status,
			ConsensusStatus:       consensusStatus(self),
		}, nil))
	}
	// The cache is stale about the leader: running but not ready, last ready long ago.
	seed(leaderID, &multipoolermanagerdatapb.Status{
		PoolerType: clustermetadatapb.PoolerType_PRIMARY, IsInitialized: true, PostgresRunning: true, PostgresReady: false,
	}, timestamppb.New(time.Now().Add(-time.Hour)))
	seed(follower1ID, &multipoolermanagerdatapb.Status{IsInitialized: true}, nil)
	seed(follower2ID, &multipoolermanagerdatapb.Status{IsInitialized: true}, nil)

	problems := detectProblems(t, engine)
	require.Len(t, problems, 1, "the stale cache must make the leader look unhealthy")
	require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
	require.False(t, problems[0].SkipPreRecheckRefresh)

	engine.attemptRecovery(ctx, problems[0])

	for _, call := range fakeClient.GetCallLog() {
		assert.NotContains(t, call, "Recruit", "the leader is healthy on a live check, so no failover may start")
	}
	leader, ok := engine.poolerCache.GetRider("multipooler-cell1-leader")
	require.True(t, ok)
	assert.True(t, leader.Health().GetStatus().GetPostgresReady(), "the refresh must have updated the cache")
}
