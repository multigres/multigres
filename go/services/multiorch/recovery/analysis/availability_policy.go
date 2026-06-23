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

import "time"

// AvailabilityPolicy is configuration that influences the orchestrator's
// decisions about when to take action and what choices to make — for example,
// how stale an observation may be before it stops counting as evidence, how
// aggressively to act on weak versus strong signals, and (in time) backoff and
// selection preferences. It exists so these knobs live in one named, injectable
// place rather than as constants scattered through the analyzers, and so
// decisions that warrant different policies can carry different values.
//
// Today it holds the observation-freshness thresholds that decide when a
// pooler's last health snapshot is too old to be trusted as a live signal. The
// set of fields will grow as more decisions become policy-driven.
//
// This is an in-process Go struct, deliberately scoped to multiorch for now.
// The longer-term direction (see the orch health/failover principles doc, P6)
// is to source it from a proto distributed to poolers, with per-shard
// overrides; when that lands, callers read the same struct, populated from the
// proto instead of from DefaultAvailabilityPolicy.
type AvailabilityPolicy struct {
	// LeaderLivenessFreshness bounds how stale the leader's most recent health
	// snapshot may be before it stops counting as a live observation. This is
	// the signal that TRIGGERS failover, so it wants confidence before firing:
	// a snapshot older than this means "we no longer have current evidence the
	// leader is alive."
	LeaderLivenessFreshness time.Duration

	// FollowerStreamFreshness bounds the same for a follower's snapshot when its
	// "still streaming from the leader" report is used to SUPPRESS failover.
	// Directionality is inverted from the trigger: a stale follower snapshot
	// must NOT keep suppressing failover, so an age beyond this drops the
	// follower from the connected set.
	FollowerStreamFreshness time.Duration
}

// DefaultAvailabilityPolicy returns the built-in policy used when no operator
// configuration is supplied. The freshness thresholds are a small multiple of
// the default health-snapshot interval, so a brief stream interruption does not
// flip a pooler's liveness while a genuinely stalled stream is caught well
// before the staleness watchdog's much longer window.
func DefaultAvailabilityPolicy() AvailabilityPolicy {
	return AvailabilityPolicy{
		LeaderLivenessFreshness: 15 * time.Second,
		FollowerStreamFreshness: 15 * time.Second,
	}
}
