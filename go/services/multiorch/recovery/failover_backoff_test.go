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
		//
		// This only covers the case with no competing live attempt. It does
		// NOT cover (and there is no test for) an untargeted revocation with
		// a higher term than a concurrently-live, decision-matched one:
		// HighestRevokedBelowTermRevocation picks purely by term, with no
		// decision-awareness, so such a revocation would still win the
		// reduction and collapse the live attempt's escalated backoff down
		// to this flat default. Known, accepted gap (narrow — requires a
		// concurrent externally-certified rule change racing an active
		// failover); not fixed because the cert path is rare and
		// human-supervised.
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
		//
		// This is safe because p1's revocation carries a RecruitIntent (just
		// targeting a stale decision), so backoffRelevantRevocations excludes
		// it by decision-mismatch before the term reduction ever runs. This
		// is a different scenario from — and does not cover — the untargeted
		// (no RecruitIntent) shadowing case noted above.
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
		// shutdown (INELIGIBLE) before p1 replayed the new rule.
		//
		// pDeparting's decision is real (term 7 genuinely committed at some
		// point) — this isn't about distrusting it. The exclusion exists
		// because this gate must agree with runFailover on which members
		// count when computing "the current decision": runFailover's
		// NewTermRevocation excludes ineligible members too, so if this
		// computation didn't apply the same exclusion, the gate would think
		// the shard moved on to 7 while the next actual attempt still
		// targets 6 (it can't reach pDeparting either) — the gate and the
		// thing it's gating would disagree. Once an eligible member reflects
		// decision 7 too (e.g. via the leader-led no-op bump that reconciles
		// a stranded straggler like pDeparting), this gate picks it up
		// naturally.
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
