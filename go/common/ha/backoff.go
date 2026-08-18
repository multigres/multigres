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
// services. Everything here must be deterministic — same inputs, same result,
// on every call — so independent callers converge without communicating.
// Guarded against wall-clock reads, goroutines, and non-deterministic map
// iteration for that reason.
package ha

import (
	"fmt"
	"hash/fnv"
	"math"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/retry"
)

// BackoffSchedule parameterizes the collective recruitment backoff. The delay
// grows exponentially with the recruitment attempt number and is capped at Max;
// a deterministic per-caller jitter — a fraction of that delay — spreads callers
// across the retry window so they do not all recruit at once.
//
// Unlike AWS-style jitter (Full/Equal/Decorrelated), the jitter here is a
// stable hash of (caller identity, replace_decision, attempt), not a fresh
// random draw: every caller must recompute the same delay from the same
// shared anchor on every recovery-loop tick, which a live RNG can't do
// consistently across callers. Jitter is added above the full exponential
// delay (a floor), not sampled below it, so the luckiest caller can't recruit
// immediately every round and defeat the backoff.
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

// NextAttempt returns the earliest time orchID may launch another
// recruitment, given the shard's most recently observed TermRevocation. Pure
// and deterministic — never reads the wall clock — so every orchestrator
// computes the same value from the same revocation:
//
//	NextAttempt = rev.CoordinatorInitiatedAt
//	            + backoff(rev.RecruitIntent.Attempt)
//	            + jitter(orchID, rev.RecruitIntent.ReplaceDecision, Attempt)
//
// replace_decision and attempt feed the jitter so recruitment order reshuffles
// each round and each failover episode — no orchestrator is perpetually first.
// When no revocation has been observed yet, callers should act immediately
// rather than call this.
func (s BackoffSchedule) NextAttempt(rev *clustermetadatapb.TermRevocation, orchID *clustermetadatapb.ID) time.Time {
	intent := rev.GetRecruitIntent()
	attempt := max(intent.GetAttempt(), 1)
	base := s.backoff(attempt)
	initiated := rev.GetCoordinatorInitiatedAt().AsTime()
	return initiated.Add(base + s.jitter(orchID, intent.GetReplaceDecision(), attempt, base))
}

// backoff returns the exponential delay for the (1-based) attempt: Base *
// 2^(attempt-1), clamped to Max. It reuses retry.ExponentialBackoffMagnitude
// (0-based) for the overflow-safe magnitude; the jitter strategy differs (see
// the type doc).
func (s BackoffSchedule) backoff(attempt int64) time.Duration {
	max := s.Max
	if max <= 0 {
		// ExponentialBackoffMagnitude clamps to maxDelay unconditionally, so a
		// literal zero would floor every delay at zero instead of leaving it
		// uncapped as Max's doc promises.
		max = math.MaxInt64
	}
	return retry.ExponentialBackoffMagnitude(s.Base, max, int(attempt-1))
}

// jitter returns a deterministic offset in [0, JitterFraction*base), hashed
// from the caller identity, replace_decision, and attempt — every caller
// observes the same inputs and agrees on the ordering, without a shared RNG.
// replace_decision reshuffles the order across failover episodes, attempt
// across rounds within one.
//
// TODO: the window is narrow at low attempt counts (e.g. 2.5s at attempt 1
// with the default schedule), so with enough concurrent callers more than one
// can still collide on the same slot. Consider a floor independent of
// JitterFraction if collisions matter at scale.
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
