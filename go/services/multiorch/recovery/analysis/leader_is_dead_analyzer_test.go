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

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestLeaderIsDeadAnalyzer_Analyze(t *testing.T) {
	// Set up factory for tests
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore()
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "cell1",
		Name:      "test-coord",
	}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	cfg := config.NewTestConfig()
	factory := NewRecoveryActionFactory(cfg, poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &LeaderIsDeadAnalyzer{factory: factory}

	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "leader-1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	// deadLeaderShardAnalysis builds a ShardAnalysis that has a dead leader and an
	// initialized replica — the base case for LeaderIsDead detection.
	deadLeaderShardAnalysis := func(overrides ...func(*ShardAnalysis)) *ShardAnalysis {
		sa := &ShardAnalysis{
			ShardKey:              shardKey,
			HighestShardRule:      &clustermetadatapb.ShardRule{LeaderId: leaderID},
			LeaderReachable:       false,
			HasInitializedReplica: true,
			Analyses: []*PoolerAnalysis{
				{
					PoolerID:          &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "follower-1"},
					ShardKey:          shardKey,
					NamesSelfAsLeader: false,
					IsInitialized:     true,
				},
			},
		}
		for _, o := range overrides {
			o(sa)
		}
		return sa
	}

	t.Run("detects dead leader (leader exists in topology but unreachable)", func(t *testing.T) {
		sa := deadLeaderShardAnalysis()

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		problem := problems[0]
		require.Equal(t, types.ProblemLeaderIsDead, problem.Code)
		require.Equal(t, types.ScopeShard, problem.Scope)
		require.Equal(t, types.PriorityEmergency, problem.Priority)
		require.Equal(t, leaderID, problem.PoolerID)
		require.NotNil(t, problem.RecoveryAction)
	})

	t.Run("ignores healthy leader (reachable)", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderReachable = true
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores when no leader exists in topology (future analysis)", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.HighestShardRule = nil
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores when no initialized replica can confirm the leader is dead", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.HasInitializedReplica = false
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores when leader pooler down but all replicas still connected to postgres", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = false
			sa.ReplicasConnectedToLeader = true
			sa.LeaderLastPostgresReadyTime = time.Now().Add(-5 * time.Second) // Responded recently
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "should not trigger failover when pooler is down but replicas are connected")
	})

	t.Run("triggers failover when leader pooler up but postgres down", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = true // Pooler is up
			// LeaderReachable remains false (postgres down)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemLeaderIsDead, problems[0].Code)
		require.Equal(t, leaderID, problems[0].PoolerID)
	})

	t.Run("triggers failover when both pooler and replicas disconnected", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = false
			sa.ReplicasConnectedToLeader = false
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemLeaderIsDead, problems[0].Code)
		require.Equal(t, leaderID, problems[0].PoolerID)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("LeaderIsDead"), analyzer.Name())
	})

	t.Run("ignores when leader pooler down but replicas connected (postgres still running, recent timestamp)", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = false                                  // Pooler is down
			sa.LeaderPostgresReady = false                                    // Unknown since pooler is down
			sa.ReplicasConnectedToLeader = true                               // But replicas are still connected to postgres
			sa.LeaderLastPostgresReadyTime = time.Now().Add(-5 * time.Second) // Responded recently (within 30s default threshold)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "should not trigger failover when pooler is down but replicas are connected and postgres responded recently")
	})

	t.Run("ignores when pooler up, postgres starting (replicas connected, recent timestamp)", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = true     // Pooler is up
			sa.LeaderPostgresReady = false      // Postgres not yet accepting connections
			sa.LeaderPostgresRunning = true     // But process exists (starting up or SIGSTOP'd)
			sa.ReplicasConnectedToLeader = true // Replicas still connected via streaming replication
			sa.LeaderLastPostgresReadyTime = time.Now().Add(-5 * time.Second)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "should not trigger failover when postgres is starting but replicas are connected and timestamp is recent")
	})

	t.Run("triggers failover when pooler up but postgres process dead (SIGKILL), replicas still connected", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = true     // Pooler is up and reachable
			sa.LeaderPostgresReady = false      // Postgres not accepting connections
			sa.LeaderPostgresRunning = false    // Process is dead (SIGKILL)
			sa.ReplicasConnectedToLeader = true // Replicas still appear connected (TCP keepalive not yet fired)
			sa.LeaderLastPostgresReadyTime = time.Now().Add(-5 * time.Second)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should trigger failover when postgres process is dead even if replicas still appear connected")
		require.Equal(t, types.ProblemLeaderIsDead, problems[0].Code)
	})

	t.Run("triggers failover when pooler reachable but postgres process alive and unresponsive beyond threshold", func(t *testing.T) {
		// The pooler is reachable and reports the postgres process is alive but not
		// accepting connections (pg_isready failing). Because we can reach the pooler,
		// we trust its direct signal: suppress only while postgres responded recently,
		// then allow failover once the response threshold lapses so a wedged postgres
		// cannot block failover forever.
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = true
			sa.LeaderPostgresRunning = true                                    // process exists
			sa.LeaderPostgresReady = false                                     // but wedged (pg_isready fails)
			sa.ReplicasConnectedToLeader = true                                // replicas still appear connected
			sa.LeaderLastPostgresReadyTime = time.Now().Add(-60 * time.Second) // beyond 30s default threshold
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should fail over when a reachable postgres has been unresponsive past the threshold")
		require.Equal(t, types.ProblemLeaderIsDead, problems[0].Code)
	})

	t.Run("suppresses failover when pooler unreachable but replicas connected, even with an expired postgres timestamp", func(t *testing.T) {
		// When the leader pooler is unreachable we cannot observe its postgres
		// directly, so the leader's own LastPostgresReadyTime is irrelevant. The
		// replicas being connected (ReplicasConnectedToLeader requires fresh WAL
		// heartbeats) is itself proof the leader's postgres is alive.
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = false
			sa.LeaderPostgresReady = false
			sa.ReplicasConnectedToLeader = true
			sa.LeaderLastPostgresReadyTime = time.Now().Add(-60 * time.Second) // Older than 30s default threshold
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "replicas streaming from the leader's postgres proves it is alive; do not fail over")
	})

	t.Run("suppresses failover when pooler unreachable but replicas connected, even with a zero postgres timestamp", func(t *testing.T) {
		// Regression: the leader's pooler died before multiorch ever recorded a
		// PostgresReady snapshot (zero timestamp), yet replicas are still streaming.
		// Leader identity is recovered from the replicas' consensus rules, and their
		// fresh streaming proves postgres is alive, so failover must be suppressed.
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = false
			sa.LeaderPostgresReady = false
			sa.ReplicasConnectedToLeader = true
			sa.LeaderLastPostgresReadyTime = time.Time{} // Zero: postgres never directly seen healthy
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "a dead pooler cannot report a postgres timestamp; trust the streaming replicas")
	})

	t.Run("suppresses LeaderIsDead while pg_promote() is running", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = true  // stream is live
			sa.LeaderPostgresRunning = true  // process is running
			sa.LeaderPostgresReady = false   // not yet accepting connections (promoting)
			sa.PromotingPrimaryID = leaderID // multipooler flagged promotion in progress
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "should suppress LeaderIsDead while pg_promote() is explicitly in progress")
	})

	t.Run("does not suppress LeaderIsDead when postgres crashes during promotion", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = true  // stream still alive (multipooler survived)
			sa.LeaderPostgresRunning = false // postgres process died during promotion
			sa.LeaderPostgresReady = false
			sa.PromotingPrimaryID = leaderID // flag still set before cleared
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should detect dead leader when postgres crashes during promotion")
		require.Equal(t, types.ProblemLeaderIsDead, problems[0].Code)
	})

	t.Run("does not suppress LeaderIsDead when multipooler unreachable during promotion", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.LeaderPoolerReachable = false // stream disconnected (stale flag)
			sa.LeaderPostgresRunning = true
			sa.LeaderPostgresReady = false
			sa.PromotingPrimaryID = leaderID // stale flag from last snapshot
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should detect dead leader when multipooler is unreachable even if promotion flag is set")
		require.Equal(t, types.ProblemLeaderIsDead, problems[0].Code)
	})
}
