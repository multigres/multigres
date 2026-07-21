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

package recovery

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/ha"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestIsFailoverProblem(t *testing.T) {
	assert.True(t, isFailoverProblem(types.ProblemLeaderIsDead))
	assert.True(t, isFailoverProblem(types.ProblemLeaderResigned))
	assert.False(t, isFailoverProblem(types.ProblemReplicaNotReplicating))
	assert.False(t, isFailoverProblem(types.ProblemStaleLeader))
	assert.False(t, isFailoverProblem(types.ProblemPoolerNotInCohort))
}

func TestLatestObservedRevocation(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	// poolerHealth builds a rider for a shard member; revokedBelow > 0 attaches a
	// ConsensusStatus carrying an accepted revocation at that term.
	poolerHealth := func(name string, revokedBelow int64) *multiorchdatapb.PoolerHealthState {
		h := &multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
				ShardKey: shardKey,
			},
		}
		if revokedBelow > 0 {
			h.ConsensusStatus = &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: revokedBelow},
			}
		}
		return h
	}

	t.Run("nil when no revocation has been observed", func(t *testing.T) {
		cache := store.NewTestCache(t)
		store.SeedCache(t, cache, store.NewPooler(poolerHealth("p1", 0), nil))
		store.SeedCache(t, cache, store.NewPooler(poolerHealth("p2", 0), nil))
		assert.Nil(t, latestObservedRevocation(cache, shardKey))
	})

	t.Run("returns the highest revoked_below_term across the shard", func(t *testing.T) {
		cache := store.NewTestCache(t)
		store.SeedCache(t, cache, store.NewPooler(poolerHealth("p1", 3), nil))
		store.SeedCache(t, cache, store.NewPooler(poolerHealth("p2", 7), nil))
		store.SeedCache(t, cache, store.NewPooler(poolerHealth("p3", 5), nil))
		rev := latestObservedRevocation(cache, shardKey)
		require.NotNil(t, rev)
		assert.Equal(t, int64(7), rev.GetRevokedBelowTerm())
	})

	t.Run("ignores poolers in other shards", func(t *testing.T) {
		cache := store.NewTestCache(t)
		store.SeedCache(t, cache, store.NewPooler(poolerHealth("p1", 4), nil))
		otherShard := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "1"}
		assert.Nil(t, latestObservedRevocation(cache, otherShard))
	})
}

func TestNextFailoverAttempt(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	coordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "orch1"}

	newEngine := func(cache *store.PoolerCache) *Engine {
		return &Engine{
			poolerCache:        cache,
			recruitmentBackoff: ha.BackoffSchedule{Base: 10 * time.Second, Cap: time.Minute},
			coordinator:        consensus.NewCoordinator(coordID, nil, nil, slog.Default()),
		}
	}
	seedRevocation := func(cache *store.PoolerCache, name string, initiated time.Time, attempt int64) {
		store.SeedCache(t, cache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
				ShardKey: shardKey,
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					CoordinatorInitiatedAt: timestamppb.New(initiated),
					RecruitIntent:          &clustermetadatapb.RecruitIntent{Attempt: attempt},
				},
			},
		}, nil))
	}

	t.Run("acts immediately when no revocation is observed", func(t *testing.T) {
		cache := store.NewTestCache(t)
		readyAt, ready := newEngine(cache).nextFailoverAttempt(shardKey)
		assert.True(t, ready, "should act immediately with nothing to back off from")
		assert.True(t, readyAt.IsZero())
	})

	t.Run("defers while a recent revocation's backoff has not elapsed", func(t *testing.T) {
		cache := store.NewTestCache(t)
		seedRevocation(cache, "p1", time.Now(), 1) // now + base(10s) → in the future
		readyAt, ready := newEngine(cache).nextFailoverAttempt(shardKey)
		assert.False(t, ready, "should defer within the backoff window")
		assert.True(t, readyAt.After(time.Now()))
	})

	t.Run("acts once a stale revocation's backoff has elapsed", func(t *testing.T) {
		cache := store.NewTestCache(t)
		seedRevocation(cache, "p1", time.Now().Add(-time.Hour), 5) // old anchor → ready time is in the past
		_, ready := newEngine(cache).nextFailoverAttempt(shardKey)
		assert.True(t, ready, "a long-stale revocation should not keep deferring")
	})
}
