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
	follower1ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "follower-1"}
	follower2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "follower-2"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	atLeastN := func(n int32) *clustermetadatapb.DurabilityPolicy {
		return &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: n,
		}
	}

	// freshFollower builds an initialized, freshly-observed follower rider. It has
	// no replication status, so it counts as reachable/initialized (for recruitment
	// feasibility) but is NOT streaming from the leader (does not vouch) until
	// connectReplica gives it a live stream.
	freshFollower := func(id *clustermetadatapb.ID, now time.Time) *store.Pooler {
		return newRider(&multiorchdatapb.PoolerHealthState{
			Multipooler:      &clustermetadatapb.Multipooler{Id: id, ShardKey: shardKey},
			IsLastCheckValid: true,
			LastSeen:         timestamppb.New(now),
			Status:           &multipoolermanagerdatapb.Status{IsInitialized: true},
		})
	}

	// deadLeaderShardAnalysis builds a ShardAnalysis with a dead leader and two
	// initialized followers — the base case for leader-replacement detection. The
	// leader rider starts not-valid (no live observation); use setLeaderLive to mark
	// it observed-live in subtests that need a reachable leader.
	//
	// The rule's cohort is {leader, follower1, follower2} (3 members) and the
	// durability policy is AtLeast(2). A recruitment quorum needs a strict majority
	// (2 of 3) reachable, with the unreachable remainder unable to satisfy the
	// policy. So with both followers reachable a failover is FEASIBLE; subtests that
	// need infeasibility (ShardStuck / ShardAtRisk) drop a follower from Analyses.
	//
	// The rule's CreationTime is set an hour in the past so the promotion grace
	// window never accidentally suppresses detection; promotion subtests reset it.
	deadLeaderShardAnalysis := func(overrides ...func(*ShardAnalysis)) *ShardAnalysis {
		now := time.Now()
		sa := &ShardAnalysis{
			ShardKey: shardKey,
			HighestPosition: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				LeaderId:      leaderID,
				CohortMembers: []*clustermetadatapb.ID{leaderID, follower1ID, follower2ID},
				CreationTime:  timestamppb.New(now.Add(-time.Hour)),
			}},
			BootstrapDurabilityPolicy: atLeastN(2),
			Now:                       now,
			Policy:                    DefaultAvailabilityPolicy(),
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
				freshFollower(follower1ID, now),
				freshFollower(follower2ID, now),
			},
		}
		for _, o := range overrides {
			o(sa)
		}
		return sa
	}

	// setLeaderLive marks the leader rider as observed-live (recent snapshot) or
	// not, replacing the old pre-baked LeaderPoolerReachable verdict. A live leader
	// is also marked initialized so it can participate in a recruitment quorum.
	setLeaderLive := func(sa *ShardAnalysis, live bool) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.IsLastCheckValid = live
			if live {
				h.LastSeen = timestamppb.New(sa.Now)
				h.Status.IsInitialized = true
			} else {
				h.LastSeen = nil
			}
		})
	}

	// dropFollower removes a follower from Analyses, making it unreachable for
	// recruitment. Used to shrink the reachable set below a majority.
	dropFollower := func(sa *ShardAnalysis, id *clustermetadatapb.ID) {
		kept := sa.Analyses[:0:0]
		for _, pa := range sa.Analyses {
			if poolerID(pa).Name != id.Name {
				kept = append(kept, pa)
			}
		}
		sa.Analyses = kept
	}

	// setRuleCreatedNow marks the leadership rule as freshly created, so the
	// promotion grace window is in effect.
	setRuleCreatedNow := func(sa *ShardAnalysis) {
		sa.HighestPosition.Decision.CreationTime = timestamppb.New(sa.Now)
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

	// connectReplica gives every follower in Analyses a fresh rider that is actively
	// streaming from the leader, so each one vouches for the leader (and
	// allFollowersStreaming sees a live connection). Replaces the old pre-baked
	// ReplicasConnectedToLeader verdict.
	connectReplica := func(sa *ShardAnalysis) {
		for i, pa := range sa.Analyses {
			sa.Analyses[i] = store.NewPooler(&multiorchdatapb.PoolerHealthState{
				Multipooler:      &clustermetadatapb.Multipooler{Id: poolerID(pa), ShardKey: shardKey},
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
	}

	t.Run("detects dead leader (leader exists in topology but unreachable)", func(t *testing.T) {
		sa := deadLeaderShardAnalysis()

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		problem := problems[0]
		require.Equal(t, types.ProblemLeaderUnreachableByCohort, problem.Code)
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
			sa.HighestPosition = nil
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("reports ShardStuck when the leader is dead and no cohort is reachable", func(t *testing.T) {
		// With no reachable followers there is no recruitment quorum, so the shard
		// cannot fail over — a human must intervene (ShardStuck, alert-only).
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.Analyses = nil // no reachable cohort member to recruit
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemShardStuck, problems[0].Code)
		require.Equal(t, types.ScopeShard, problems[0].Scope)
		require.Equal(t, types.PriorityEmergency, problems[0].Priority)
	})

	t.Run("reports ShardStuck when the leader is dead but only one follower is reachable", func(t *testing.T) {
		// One reachable follower is not a strict majority of the 3-member cohort, so
		// recruitment is infeasible: ShardStuck rather than an actionable failover.
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			dropFollower(sa, follower2ID)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemShardStuck, problems[0].Code)
		require.Equal(t, types.ScopeShard, problems[0].Scope)
	})

	t.Run("reports ShardStuck when a cohort member is IsLastCheckValid but its observation is stale", func(t *testing.T) {
		// follower2 still has IsLastCheckValid=true but its last observation is stale.
		// reachableCohort keys on observation freshness, not IsLastCheckValid, so
		// follower2 must NOT count toward the recruitment quorum — leaving only
		// follower1 reachable, which is below the majority of 3, so the failover is
		// infeasible. (Were IsLastCheckValid used, follower2 would count and this
		// would be an actionable LeaderUnreachableByCohort instead.)
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.Analyses[1].Mutate(func(h *multiorchdatapb.PoolerHealthState) {
				h.LastSeen = timestamppb.New(sa.Now.Add(-time.Hour))
			})
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemShardStuck, problems[0].Code)
	})

	t.Run("reports ShardAtRisk when the leader is healthy but could not be recovered if lost", func(t *testing.T) {
		// Leader is healthy, so no failover is needed. But excluding the leader,
		// only one follower is reachable — below a majority — so losing the leader
		// now would strand the shard. Warn (ShardAtRisk), non-blocking.
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, true)
			setLeaderPGReady(sa, true)
			dropFollower(sa, follower2ID)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemShardAtRisk, problems[0].Code)
		require.Equal(t, types.ScopePooler, problems[0].Scope)
		require.Equal(t, types.PriorityNormal, problems[0].Priority)
		require.Equal(t, leaderID, problems[0].PoolerID)
	})

	t.Run("does not warn ShardAtRisk for a cohort at its policy floor (2 of 2)", func(t *testing.T) {
		// A healthy 2-member cohort under AtLeast(2) inherently cannot recover from a
		// leader loss — that is the operator's chosen posture, not a degradation — so
		// no ShardAtRisk fires (it would otherwise fire forever for any minimum-size
		// cohort, e.g. the spurious-failover-recovery e2e scenario).
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.HighestPosition.Decision.CohortMembers = []*clustermetadatapb.ID{leaderID, follower1ID}
			dropFollower(sa, follower2ID)
			setLeaderLive(sa, true)
			setLeaderPGReady(sa, true)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "a cohort at its policy floor is not 'at risk' — that would fire permanently")
	})

	t.Run("recruits a leader when the rule names none but has a cohort", func(t *testing.T) {
		// A rule with cohort members but no designated leader needs one recruited.
		// Both followers are reachable, so recruitment is feasible and actionable.
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.HighestPosition.Decision.LeaderId = nil
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemLeaderUnspecified, problems[0].Code)
		require.Equal(t, types.ScopeShard, problems[0].Scope)
	})

	t.Run("ignores a rule with neither leader nor cohort (unbootstrapped)", func(t *testing.T) {
		// An empty cohort with no leader is the initial, unbootstrapped rule —
		// ShardNeedsInitialization owns that, so this analyzer does nothing.
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.HighestPosition.Decision.LeaderId = nil
			sa.HighestPosition.Decision.CohortMembers = nil
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
		require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
		require.Equal(t, leaderID, problems[0].PoolerID)
	})

	t.Run("triggers failover when both pooler and replicas disconnected", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, false)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemLeaderUnreachableByCohort, problems[0].Code)
		require.Equal(t, leaderID, problems[0].PoolerID)
	})

	t.Run("suppresses failover when a single follower still streams from an unreachable leader", func(t *testing.T) {
		// Leader pooler unreachable, only ONE of the two followers streaming. That
		// single streaming follower proves the leader is alive (you cannot stream
		// from a dead primary), so the leader vouches for itself: {follower1, leader}
		// meets AtLeast(2) and failover is suppressed. Without the leader-self-vouch
		// this would be LeaderUnreachableByCohort.
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			sa.Analyses[0] = store.NewPooler(&multiorchdatapb.PoolerHealthState{
				Multipooler:      &clustermetadatapb.Multipooler{Id: follower1ID, ShardKey: shardKey},
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
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "a single streaming follower proves the leader alive; leader self-vouch makes quorum")
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
		require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
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
		require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
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

	t.Run("suppresses failover while pg_promote() is running within the grace window", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setRuleCreatedNow(sa)        // fresh rule → within the promotion grace window
			setLeaderLive(sa, true)      // stream is live
			setLeaderPGRunning(sa, true) // process is running
			setLeaderPGReady(sa, false)  // not yet accepting connections (promoting)
			setLeaderPromoting(sa)       // multipooler flagged promotion in progress
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "should suppress failover while pg_promote() is explicitly in progress")
	})

	t.Run("does not suppress a promotion that has outlasted the grace window", func(t *testing.T) {
		// Same promoting state, but the rule is old (base fixture CreationTime is an
		// hour ago), so the grace has lapsed. A leader that claims to be promoting
		// forever but never gains followers must fail over: postgres is not ready and
		// never reported ready, so the anti-flap guard does not apply either.
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, true)
			setLeaderPGRunning(sa, true)
			setLeaderPGReady(sa, false)
			setLeaderPromoting(sa)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should fail over once the promotion grace window lapses")
		require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
	})

	t.Run("does not suppress failover when postgres crashes during promotion", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, true)       // stream still alive (multipooler survived)
			setLeaderPGRunning(sa, false) // postgres process died during promotion
			setLeaderPGReady(sa, false)
			setLeaderPromoting(sa) // flag still set before cleared
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should detect dead leader when postgres crashes during promotion")
		require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
	})

	t.Run("does not suppress failover when multipooler unreachable during promotion", func(t *testing.T) {
		sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
			setLeaderLive(sa, false) // stream disconnected (stale flag)
			setLeaderPGRunning(sa, true)
			setLeaderPGReady(sa, false)
			setLeaderPromoting(sa) // stale flag from last snapshot
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should detect dead leader when multipooler is unreachable even if promotion flag is set")
		require.Equal(t, types.ProblemLeaderUnreachableByCohort, problems[0].Code)
	})
}
