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

// Package ha holds high-availability coordination helpers shared across
// services. Everything here must be deterministic: given the same inputs, every
// orchestrator computes the same result, on every call, so that independent
// orchestrators converge on the same behavior without communicating. The
// package is guarded against wall-clock reads, goroutines, and non-deterministic
// map iteration for that reason.
package ha

import (
	"fmt"
	"hash/fnv"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/retry"
)

// BackoffSchedule parameterizes the collective recruitment backoff. The delay
// grows exponentially with the recruitment attempt number and is capped at Max;
// a deterministic per-orchestrator jitter — a fraction of that delay — spreads
// orchestrators across the retry window so they do not all recruit at once.
//
// This is *not* AWS Full Jitter (uniform in [0, delay], as retry.nextDelay uses):
// the delay here is an absolute offset from a shared anchor evaluated by multiple
// orchestrators, so it needs a guaranteed floor (the exponential delay) that only
// jitter *above* preserves — otherwise the luckiest orchestrator's near-zero draw
// would recruit immediately every round and defeat the backoff. And the jitter is
// a stable hash, not an RNG, so every orchestrator agrees on the schedule.
type BackoffSchedule struct {
	// Base is the delay after the first attempt (attempt 1).
	Base time.Duration
	// Max caps the exponential growth. Zero means uncapped.
	Max time.Duration
	// JitterFraction is the width of the per-orchestrator jitter as a fraction of
	// the (capped) exponential delay — e.g. 0.25 spreads orchs across the top 25%
	// of the delay. Zero disables jitter.
	JitterFraction float64
}

// DefaultBackoffSchedule returns the built-in schedule. These are tuning knobs,
// not load-bearing constants; adjust as failover behavior is characterized.
func DefaultBackoffSchedule() BackoffSchedule {
	return BackoffSchedule{
		Base:           10 * time.Second,
		Max:            5 * time.Minute,
		JitterFraction: 0.25,
	}
}

// DefaultBackoffResetDuration returns how long recruitment for a shard must have
// been quiet before an accumulated recruit-intent attempt count is treated as
// stale and reset (consumed by consensus.NewTermRevocation). It sits well above
// DefaultBackoffSchedule().Max so it only fires when recruitment has genuinely
// paused — e.g. the cluster was scaled to zero and restarted — not during active
// churn, where retries are at most one (capped) backoff interval apart.
func DefaultBackoffResetDuration() time.Duration {
	return 30 * time.Minute
}

// NextAttempt returns the earliest time orchID may launch another recruitment,
// given the most recently observed TermRevocation for the shard. It is a pure,
// deterministic function of the revocation and the orchestrator's identity:
// every orchestrator computes the same value, and re-computing it across
// recovery-loop ticks yields the same time, so an orchestrator's readiness does
// not drift while it waits.
//
//	NextAttempt = rev.CoordinatorInitiatedAt
//	            + backoff(rev.RecruitIntent.Attempt)
//	            + jitter(orchID, rev.RecruitIntent.ReplaceDecision, Attempt)
//
// The replace_decision and attempt are folded into the jitter so the
// recruitment order reshuffles both each round (by attempt) and each failover
// episode (by replace_decision, which advances as decisions are committed) — the
// same orchestrator is not perpetually first.
//
// Callers compare the result against their own clock; NextAttempt never reads
// the wall clock itself. When no revocation has been observed yet (a fresh
// failover with nothing to back off from), callers should act immediately
// rather than call this.
func (s BackoffSchedule) NextAttempt(rev *clustermetadatapb.TermRevocation, orchID *clustermetadatapb.ID) time.Time {
	intent := rev.GetRecruitIntent()
	attempt := max(intent.GetAttempt(), 1)
	base := s.backoff(attempt)
	initiated := rev.GetCoordinatorInitiatedAt().AsTime()
	return initiated.Add(base + s.jitter(orchID, intent.GetReplaceDecision(), attempt, base))
}

// backoff returns the exponential delay for the (1-based) attempt: Base *
// 2^(attempt-1), clamped to Max. It reuses retry.ExponentialBackoff (0-based) for
// the overflow-safe magnitude; the jitter strategy differs (see the type doc).
func (s BackoffSchedule) backoff(attempt int64) time.Duration {
	return retry.ExponentialBackoff(s.Base, s.Max, int(attempt-1))
}

// jitter returns a deterministic offset in [0, JitterFraction*base) derived from
// the orchestrator identity, the decision being replaced, and the attempt number.
// Using a stable hash (not a RNG) keeps it reproducible across orchestrators and
// process restarts; every input is observable identically by every
// orchestrator, so they agree on the ordering while still being spread across the
// window. The replace_decision varies the ordering across failover episodes and
// the attempt varies it across rounds within an episode. Scaling the window to
// the delay keeps the spread meaningful as the backoff grows.
func (s BackoffSchedule) jitter(orchID *clustermetadatapb.ID, replaceDecision *clustermetadatapb.RuleNumber, attempt int64, base time.Duration) time.Duration {
	window := time.Duration(float64(base) * s.JitterFraction)
	if window <= 0 {
		return 0
	}
	h := fnv.New64a()
	// Component is an enum; cell+name identify the orchestrator within it.
	fmt.Fprintf(h, "%d/%s/%s/%d.%d/%d",
		orchID.GetComponent(), orchID.GetCell(), orchID.GetName(),
		replaceDecision.GetCoordinatorTerm(), replaceDecision.GetLeaderSubterm(), attempt)
	return time.Duration(h.Sum64() % uint64(window))
}
