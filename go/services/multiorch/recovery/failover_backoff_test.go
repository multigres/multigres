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
	assert.True(t, types.ProblemLeaderUnspecified.IsFailoverProblem())
	assert.True(t, types.ProblemLeaderUnreachableByCohort.IsFailoverProblem())
	assert.True(t, types.ProblemLeaderUnhealthy.IsFailoverProblem())
	assert.True(t, types.ProblemLeaderResigned.IsFailoverProblem())
	assert.False(t, types.ProblemReplicaNotReplicating.IsFailoverProblem())
	assert.False(t, types.ProblemStaleLeader.IsFailoverProblem())
	assert.False(t, types.ProblemPoolerNotInCohort.IsFailoverProblem())
}

func TestConsensusStatuses(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	poolerHealth := func(name string, revokedBelow int64) *multiorchdatapb.PoolerHealthState {
		return &multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
				ShardKey: shardKey,
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: revokedBelow},
			},
		}
	}

	t.Run("returns every pooler's status in the shard", func(t *testing.T) {
		cache := store.NewTestCache(t)
		store.SeedCache(t, cache, store.NewPooler(poolerHealth("p1", 3), nil))
		store.SeedCache(t, cache, store.NewPooler(poolerHealth("p2", 7), nil))
		statuses := consensusStatuses(cache, shardKey)
		require.Len(t, statuses, 2)
	})

	t.Run("ignores poolers in other shards", func(t *testing.T) {
		cache := store.NewTestCache(t)
		store.SeedCache(t, cache, store.NewPooler(poolerHealth("p1", 4), nil))
		otherShard := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "1"}
		assert.Empty(t, consensusStatuses(cache, otherShard))
	})
}

func TestRevocationsRelevantToDecision(t *testing.T) {
	decision4 := &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}
	decision6 := &clustermetadatapb.RuleNumber{CoordinatorTerm: 6}
	statusWith := func(rev *clustermetadatapb.TermRevocation) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{TermRevocation: rev}
	}

	t.Run("excludes a revocation with no accepted term", func(t *testing.T) {
		got := revocationsRelevantToDecision([]*clustermetadatapb.ConsensusStatus{
			statusWith(&clustermetadatapb.TermRevocation{}),
		}, decision4)
		assert.Empty(t, got)
	})

	t.Run("matches a revocation targeting the same decision", func(t *testing.T) {
		matching := &clustermetadatapb.TermRevocation{
			RevokedBelowTerm: 5,
			RecruitIntent:    &clustermetadatapb.RecruitIntent{ReplaceDecision: decision4},
		}
		got := revocationsRelevantToDecision([]*clustermetadatapb.ConsensusStatus{statusWith(matching)}, decision4)
		require.Len(t, got, 1)
		assert.Same(t, matching, got[0])
	})

	t.Run("excludes a revocation demonstrably targeting a different decision", func(t *testing.T) {
		// e.g. a shard's original bootstrap revocation: resolved history once
		// the shard has moved on, so it must not gate a failure against the
		// current decision.
		different := &clustermetadatapb.TermRevocation{
			RevokedBelowTerm: 9,
			RecruitIntent:    &clustermetadatapb.RecruitIntent{ReplaceDecision: decision6},
		}
		got := revocationsRelevantToDecision([]*clustermetadatapb.ConsensusStatus{statusWith(different)}, decision4)
		assert.Empty(t, got)
	})

	t.Run("includes a revocation with no RecruitIntent at all", func(t *testing.T) {
		// We cannot tell whether this revocation (e.g. an externally-supplied
		// cert, or an external actor forcing a resignation) is for our current
		// problem or an unrelated one, so it is not excluded as a free pass —
		// unlike commonconsensus.NewTermRevocation's stricter, exact-match
		// filtering, which only needs a same-decision match to be useful.
		untargeted := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5}
		got := revocationsRelevantToDecision([]*clustermetadatapb.ConsensusStatus{statusWith(untargeted)}, decision4)
		require.Len(t, got, 1)
		assert.Same(t, untargeted, got[0])
	})
}

func TestNextFailoverAttempt(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	coordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "orch1"}

	newEngine := func(cache *store.PoolerCache) *Engine {
		return &Engine{
			poolerCache:        cache,
			recruitmentBackoff: ha.BackoffSchedule{Base: 10 * time.Second, Max: time.Minute},
			coordinator:        consensus.NewCoordinator(coordID, nil, nil, slog.Default()),
		}
	}
	// seedRevocation attaches a revocation to p1, decided at term 4. When
	// replaceDecisionTerm is non-nil, the revocation carries a RecruitIntent
	// targeting that term with the given attempt count; nil means no
	// RecruitIntent at all.
	seedRevocation := func(cache *store.PoolerCache, name string, initiated time.Time, replaceDecisionTerm *int64, attempt int64) {
		rev := &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       5,
			CoordinatorInitiatedAt: timestamppb.New(initiated),
		}
		if replaceDecisionTerm != nil {
			rev.RecruitIntent = &clustermetadatapb.RecruitIntent{
				ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: *replaceDecisionTerm},
				Attempt:         attempt,
			}
		}
		store.SeedCache(t, cache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
				ShardKey: shardKey,
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{
						Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}},
					},
				},
				TermRevocation: rev,
			},
		}, nil))
	}
	termPtr := func(term int64) *int64 { return &term }

	t.Run("acts immediately when no revocation is observed", func(t *testing.T) {
		cache := store.NewTestCache(t)
		readyAt, ready := newEngine(cache).nextFailoverAttempt(shardKey)
		assert.True(t, ready, "should act immediately with nothing to back off from")
		assert.True(t, readyAt.IsZero())
	})

	t.Run("defers while a recent revocation's backoff has not elapsed", func(t *testing.T) {
		cache := store.NewTestCache(t)
		seedRevocation(cache, "p1", time.Now(), termPtr(4), 1) // now + base(10s) → in the future
		readyAt, ready := newEngine(cache).nextFailoverAttempt(shardKey)
		assert.False(t, ready, "should defer within the backoff window")
		assert.True(t, readyAt.After(time.Now()))
	})

	t.Run("acts once a stale revocation's backoff has elapsed", func(t *testing.T) {
		cache := store.NewTestCache(t)
		seedRevocation(cache, "p1", time.Now().Add(-time.Hour), termPtr(4), 5) // old anchor → ready time is in the past
		_, ready := newEngine(cache).nextFailoverAttempt(shardKey)
		assert.True(t, ready, "a long-stale revocation should not keep deferring")
	})

	t.Run("acts immediately when the observed revocation targets a different, already-resolved problem", func(t *testing.T) {
		// e.g. a shard's original bootstrap revocation: the shard has long
		// since moved on to decision term 4 (see CurrentPosition above), so
		// this revocation (which targeted term 0) is resolved history.
		cache := store.NewTestCache(t)
		seedRevocation(cache, "p1", time.Now(), termPtr(0), 1)
		readyAt, ready := newEngine(cache).nextFailoverAttempt(shardKey)
		assert.True(t, ready, "a revocation targeting a different problem should not gate this one")
		assert.True(t, readyAt.IsZero())
	})

	t.Run("defers on a fresh revocation with no RecruitIntent (default cooldown)", func(t *testing.T) {
		// We cannot tell whether this revocation (e.g. an externally-supplied
		// cert, or an external actor forcing a resignation) is for our current
		// problem or an unrelated one, so we do not treat it as a free pass.
		cache := store.NewTestCache(t)
		seedRevocation(cache, "p1", time.Now(), nil, 0)
		readyAt, ready := newEngine(cache).nextFailoverAttempt(shardKey)
		assert.False(t, ready, "an unidentifiable revocation should get the default cooldown, not a free pass")
		assert.True(t, readyAt.After(time.Now()))
	})

	t.Run("a stale revocation for a different, resolved decision does not shadow a live retry", func(t *testing.T) {
		// p1 carries an old, abandoned revocation at a long-superseded decision
		// (term 2) that happens to have a numerically higher term (9) than the
		// real prior attempt. p2 is decided at the current decision (term 6) and
		// holds the actual live retry at it (term 5). Picking the shard's
		// globally-highest-term revocation regardless of decision would find
		// p1's stale one, wrongly conclude "different, resolved problem," and
		// act immediately instead of deferring to p2's live backoff.
		cache := store.NewTestCache(t)
		store.SeedCache(t, cache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
				ShardKey: shardKey,
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{
						Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}},
					},
				},
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       9,
					CoordinatorInitiatedAt: timestamppb.New(time.Now()),
					RecruitIntent: &clustermetadatapb.RecruitIntent{
						ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
						Attempt:         5,
					},
				},
			},
		}, nil))
		store.SeedCache(t, cache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p2"},
				ShardKey: shardKey,
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{
						Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6}},
					},
				},
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					CoordinatorInitiatedAt: timestamppb.New(time.Now()),
					RecruitIntent: &clustermetadatapb.RecruitIntent{
						ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
						Attempt:         2,
					},
				},
			},
		}, nil))
		readyAt, ready := newEngine(cache).nextFailoverAttempt(shardKey)
		assert.False(t, ready, "the live retry at the current decision must still gate, despite the stale higher-term revocation elsewhere")
		assert.True(t, readyAt.After(time.Now()))
	})
}
