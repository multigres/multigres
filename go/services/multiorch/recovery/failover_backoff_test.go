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

	t.Run("an INELIGIBLE member's higher decision does not make a live retry look resolved", func(t *testing.T) {
		// p1 is decided at term 6 and holds a live retry against it. pDeparting
		// is a leader that committed decision term 7 and then began graceful
		// shutdown (INELIGIBLE) before p1 replayed the new rule — exactly the
		// same exclusion runFailover applies before computing ReplaceDecision.
		// If this decision computation didn't apply the same exclusion,
		// pDeparting's term 7 would make p1's decision-6 revocation look like
		// it targets a different, resolved problem, and backoff would be
		// skipped.
		cache := store.NewTestCache(t)
		store.SeedCache(t, cache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
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
						Attempt:         1,
					},
				},
			},
		}, nil))
		store.SeedCache(t, cache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "pDeparting"},
				ShardKey: shardKey,
			},
			AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
				},
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{
						Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 7}},
					},
				},
			},
		}, nil))
		readyAt, ready := newEngine(cache).nextFailoverAttempt(shardKey)
		assert.False(t, ready, "p1's live retry against decision 6 must still gate, despite pDeparting's higher, ineligible decision")
		assert.True(t, readyAt.After(time.Now()))
	})
}
