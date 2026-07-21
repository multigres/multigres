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
)

// BackoffSchedule parameterizes the collective recruitment backoff. The delay
// grows exponentially with the recruitment attempt number and is capped; a
// per-orchestrator jitter spreads orchestrators across the retry window so they
// do not all recruit at the same instant.
type BackoffSchedule struct {
	// Base is the delay after the first attempt (attempt 1).
	Base time.Duration
	// Cap bounds the exponential growth. Zero means uncapped.
	Cap time.Duration
	// JitterWindow is the width of the per-orchestrator jitter added to the
	// delay. Zero disables jitter (useful in tests).
	JitterWindow time.Duration
}

// DefaultBackoffSchedule returns the built-in schedule. These are tuning knobs,
// not load-bearing constants; adjust as failover behavior is characterized.
func DefaultBackoffSchedule() BackoffSchedule {
	return BackoffSchedule{
		Base:         2 * time.Second,
		Cap:          60 * time.Second,
		JitterWindow: 2 * time.Second,
	}
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
	initiated := rev.GetCoordinatorInitiatedAt().AsTime()
	return initiated.Add(s.backoff(attempt) + s.jitter(orchID, intent.GetReplaceDecision(), attempt))
}

// backoff returns Base * 2^(attempt-1), clamped to Cap. The doubling loop stops
// once Cap is reached, so a large attempt number cannot overflow the duration.
func (s BackoffSchedule) backoff(attempt int64) time.Duration {
	d := s.Base
	for i := int64(1); i < attempt; i++ {
		if s.Cap > 0 && d >= s.Cap {
			return s.Cap
		}
		d *= 2
	}
	if s.Cap > 0 && d > s.Cap {
		return s.Cap
	}
	return d
}

// jitter returns a deterministic offset in [0, JitterWindow) derived from the
// orchestrator identity, the decision being replaced, and the attempt number.
// Using a stable hash (not a RNG) keeps it reproducible across orchestrators and
// process restarts; every input is observable identically by every
// orchestrator, so they agree on the ordering while still being spread across
// the window. The replace_decision varies the ordering across failover episodes
// and the attempt varies it across rounds within an episode.
func (s BackoffSchedule) jitter(orchID *clustermetadatapb.ID, replaceDecision *clustermetadatapb.RuleNumber, attempt int64) time.Duration {
	if s.JitterWindow <= 0 {
		return 0
	}
	h := fnv.New64a()
	// Component is an enum; cell+name identify the orchestrator within it.
	fmt.Fprintf(h, "%d/%s/%s/%d.%d/%d",
		orchID.GetComponent(), orchID.GetCell(), orchID.GetName(),
		replaceDecision.GetCoordinatorTerm(), replaceDecision.GetLeaderSubterm(), attempt)
	return time.Duration(h.Sum64() % uint64(s.JitterWindow))
}
