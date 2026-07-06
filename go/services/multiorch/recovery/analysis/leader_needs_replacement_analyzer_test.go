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

func TestLeaderNeedsReplacementAnalyzer_Analyze(t *testing.T) {
	// Set up factory for tests
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewTestCache(t)
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "cell1",
		Name:      "test-coord",
	}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	cfg := config.NewTestConfig()
	factory := NewRecoveryActionFactory(cfg, poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &LeaderNeedsReplacementAnalyzer{factory: factory}

	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "leader-1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	// deadLeaderShardAnalysis builds a ShardAnalysis that has a dead leader and an
	// initialized replica — the base case for LeaderIsDead detection. The leader
	// rider starts not-valid (no live observation); use setLeaderLive to mark it
	// observed-live in subtests that need a reachable leader.
	deadLeaderShardAnalysis := func(overrides ...func(*ShardAnalysis)) *ShardAnalysis {
		sa := &ShardAnalysis{
			ShardKey:         shardKey,
			HighestShardRule: &clustermetadatapb.ShardRule{LeaderId: leaderID},
			Now:              time.Now(),
			Policy:           DefaultAvailabilityPolicy(),
			Leader: store.NewPooler(&multiorchdatapb.PoolerHealthState{
				Multipooler: &clustermetadatapb.Multipooler{
					Id:       leaderID,
					ShardKey: shardKey,
					Hostname: "leader-host",
					PortMap:  map[string]int32{"postgres": 5432},
				},
				IsLastCheckValid: false,
				Status:           &multipoolermanagerdatapb.Status{},
			}, nil),
			Analyses: []*store.Pooler{
				newRider(&multiorchdatapb.PoolerHealthState{
					Multipooler:      &clustermetadatapb.Multipooler{Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "follower-1"}, ShardKey: shardKey},
					IsLastCheckValid: true,
					Status:           &multipoolermanagerdatapb.Status{IsInitialized: true},
				}),
			},
		}
		for _, o := range overrides {
			o(sa)
		}
		return sa
	}

	// setLeaderLive marks the leader rider as observed-live (recent snapshot) or
	// not, replacing the old pre-baked LeaderPoolerReachable verdict.
	setLeaderLive := func(sa *ShardAnalysis, live bool) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.IsLastCheckValid = live
			if live {
				h.LastSeen = timestamppb.New(sa.Now)
			} else {
				h.LastSeen = nil
			}
		})
	}

	// setLeaderPGRunning / setLeaderLastReady / setLeaderPromoting drive the
	// leader's postgres state on its rider, replacing the removed shard-level
	// verdict fields (now derived inside LeaderNeedsReplacementAnalyzer).
	setLeaderPGRunning := func(sa *ShardAnalysis, running bool) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.Status.PostgresRunning = running })
	}
	setLeaderPGReady := func(sa *ShardAnalysis, ready bool) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.Status.PostgresReady = ready })
	}
	setLeaderLastReady := func(sa *ShardAnalysis, at time.Time) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.LastPostgresReadyTime = timestamppb.New(at) })
	}
	setLeaderPromoting := func(sa *ShardAnalysis) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.Status.PostgresStatus = multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING
		})
	}

	// setLeaderResigned marks the leader as voluntarily wanting replacement via its
	// AvailabilityStatus (cohort-eligibility INELIGIBLE), which LeaderNeedsReplacement
	// treats as a resignation.
	setLeaderResigned := func(sa *ShardAnalysis) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
				},
			}
		})
	}

	// connectReplica gives the shard's follower a fresh rider that is actively
	// streaming from the leader, so replicasStreamingFromLeader sees a live
	// connection. Replaces the old pre-baked ReplicasConnectedToLeader verdict.
	connectReplica := func(sa *ShardAnalysis) {
		sa.Analyses[0] = store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler:      &clustermetadatapb.Multipooler{Id: poolerID(sa.Analyses[0]), ShardKey: shardKey},
			IsLastCheckValid: true,
			LastSeen:         timestamppb.New(sa.Now),
			Status: &multipoolermanagerdatapb.Status{
				IsInitialized: true,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					PrimaryConnInfo:    &multipoolermanagerdatapb.PrimaryConnInfo{Host: "leader-host", Port: 5432},
					LastReceiveLsn:     "0/1",
					WalReceiverStatus:  "streaming",
					LastMsgReceiveTime: timestamppb.New(sa.Now),
				},
			},
		}, nil)
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
			setLeaderLive(sa, true)
			setLeaderPGReady(sa, true)
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
			sa.Analyses = nil // no initialized replica to witness the leader
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores when leader pooler down but all replicas still connected to postgres", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, false)
			connectReplica(sa)
			setLeaderLastReady(sa, time.Now().Add(-5*time.Second)) // Responded recently
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "should not trigger failover when pooler is down but replicas are connected")
	})

	t.Run("triggers failover when leader pooler up but postgres down", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, true) // Pooler is up
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
			setLeaderLive(sa, false)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemLeaderIsDead, problems[0].Code)
		require.Equal(t, leaderID, problems[0].PoolerID)
	})

	t.Run("resigned leader takes precedence over liveness", func(t *testing.T) {
		// A resigned leader emits ProblemLeaderResigned regardless of liveness —
		// even a dead leader is reported as resigned (voluntary), and immediately,
		// without the follower-streaming suppression the dead path applies.
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderResigned(sa)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemLeaderResigned, problems[0].Code)
		require.Equal(t, analyzer.Name(), problems[0].CheckName)
		require.Equal(t, types.ScopeShard, problems[0].Scope)
		require.Equal(t, leaderID, problems[0].PoolerID)
	})

	t.Run("resigned leader reported even when otherwise live and connected", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, true)
			setLeaderPGReady(sa, true)
			connectReplica(sa)
			setLeaderResigned(sa)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemLeaderResigned, problems[0].Code)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("LeaderNeedsReplacement"), analyzer.Name())
	})

	t.Run("ignores when leader pooler down but replicas connected (postgres still running, recent timestamp)", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, false)                               // Pooler is down
			setLeaderPGReady(sa, false)                            // Unknown since pooler is down
			connectReplica(sa)                                     // But replicas are still connected to postgres
			setLeaderLastReady(sa, time.Now().Add(-5*time.Second)) // Responded recently (within 30s default threshold)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "should not trigger failover when pooler is down but replicas are connected and postgres responded recently")
	})

	t.Run("ignores when pooler up, postgres starting (replicas connected, recent timestamp)", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, true)      // Pooler is up
			setLeaderPGReady(sa, false)  // Postgres not yet accepting connections
			setLeaderPGRunning(sa, true) // But process exists (starting up or SIGSTOP'd)
			connectReplica(sa)           // Replicas still connected via streaming replication
			setLeaderLastReady(sa, time.Now().Add(-5*time.Second))
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "should not trigger failover when postgres is starting but replicas are connected and timestamp is recent")
	})

	t.Run("triggers failover when pooler up but postgres process dead (SIGKILL), replicas still connected", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, true)       // Pooler is up and reachable
			setLeaderPGReady(sa, false)   // Postgres not accepting connections
			setLeaderPGRunning(sa, false) // Process is dead (SIGKILL)
			connectReplica(sa)            // Replicas still appear connected (TCP keepalive not yet fired)
			setLeaderLastReady(sa, time.Now().Add(-5*time.Second))
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
			setLeaderLive(sa, true)
			setLeaderPGRunning(sa, true)                            // process exists
			setLeaderPGReady(sa, false)                             // but wedged (pg_isready fails)
			connectReplica(sa)                                      // replicas still appear connected
			setLeaderLastReady(sa, time.Now().Add(-60*time.Second)) // beyond 30s default threshold
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
			setLeaderLive(sa, false)
			setLeaderPGReady(sa, false)
			connectReplica(sa)
			setLeaderLastReady(sa, time.Now().Add(-60*time.Second)) // Older than 30s default threshold
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
			setLeaderLive(sa, false)
			setLeaderPGReady(sa, false)
			connectReplica(sa)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "a dead pooler cannot report a postgres timestamp; trust the streaming replicas")
	})

	t.Run("suppresses LeaderIsDead while pg_promote() is running", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, true)      // stream is live
			setLeaderPGRunning(sa, true) // process is running
			setLeaderPGReady(sa, false)  // not yet accepting connections (promoting)
			setLeaderPromoting(sa)       // multipooler flagged promotion in progress
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "should suppress LeaderIsDead while pg_promote() is explicitly in progress")
	})

	t.Run("does not suppress LeaderIsDead when postgres crashes during promotion", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, true)       // stream still alive (multipooler survived)
			setLeaderPGRunning(sa, false) // postgres process died during promotion
			setLeaderPGReady(sa, false)
			setLeaderPromoting(sa) // flag still set before cleared
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should detect dead leader when postgres crashes during promotion")
		require.Equal(t, types.ProblemLeaderIsDead, problems[0].Code)
	})

	t.Run("does not suppress LeaderIsDead when multipooler unreachable during promotion", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, false) // stream disconnected (stale flag)
			setLeaderPGRunning(sa, true)
			setLeaderPGReady(sa, false)
			setLeaderPromoting(sa) // stale flag from last snapshot
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should detect dead leader when multipooler is unreachable even if promotion flag is set")
		require.Equal(t, types.ProblemLeaderIsDead, problems[0].Code)
	})
}
