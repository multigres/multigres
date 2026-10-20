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

package snapshot

import (
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// observationAge reports how long ago — on the orchestrator's clock — h's
// snapshot was recorded, measured against now. ok is false when no snapshot
// time has ever been recorded (LastSeen unset), letting callers distinguish
// "never observed" from "observed but stale".
//
// It deliberately uses LastSeen (the orchestrator-clock receipt time) rather
// than the pooler-clock pooler_captured_at, so the subtraction against now
// stays same-clock and is unaffected by skew between the hosts.
func observationAge(h *multiorchdatapb.PoolerHealthState, now time.Time) (time.Duration, bool) {
	ls := h.GetLastSeen()
	if ls == nil {
		return 0, false
	}
	return now.Sub(ls.AsTime()), true
}

// DefaultObservationFreshness is the staleness tolerance for callers that need a
// trustworthy health snapshot but are not handed an AvailabilityPolicy (e.g.
// actions checking a live pooler). The default policy's ObservationFreshness uses
// this same value, so there is one source of truth.
const DefaultObservationFreshness = 15 * time.Second

// HealthWithin returns h if it was recorded within maxAge of now, and
// ok=false otherwise (including "never observed"). Prefer this over reading h
// directly wherever a decision depends on how current the data is — it makes
// the staleness tolerance an explicit, mandatory choice at the call site
// instead of a separately-remembered observationAge check that's easy to omit.
func HealthWithin(h *multiorchdatapb.PoolerHealthState, now time.Time, maxAge time.Duration) (*multiorchdatapb.PoolerHealthState, bool) {
	age, ok := observationAge(h, now)
	if !ok || age > maxAge {
		return nil, false
	}
	return h, true
}

// DefaultLeaderWriteFreshness is the default freshness bound for
// LeaderWritesProgressing's health-report check, shared so analysis and
// actions packages (which can't import each other) don't each pick their own
// value.
const DefaultLeaderWriteFreshness = 15 * time.Second

// LeaderWritesProgressing reports whether it looks safe to attempt a
// leader-led rule write right now (a cohort reconcile, a no-op rule advance,
// etc.): a recent health report, postgres genuinely out of recovery (a
// resigning leader can still be a writable primary — recovery mode is what
// actually precludes writes), and the shard's highest intended rule fully
// decided — no outstanding proposal to race against.
//
// TODO: this is a proxy for "the leader can currently commit a synchronous
// write", not direct proof of it — the strongest signal would be a recent
// successful heartbeat write, which isn't surfaced through health today.
// Tighten this once that signal exists.
func LeaderWritesProgressing(leader *multiorchdatapb.PoolerHealthState, intendedRule *clustermetadatapb.RulePosition, now time.Time, freshness time.Duration) bool {
	if leader == nil {
		return false
	}
	age, ok := observationAge(leader, now)
	if !ok || age > freshness {
		return false
	}
	if leader.GetStatus().GetPostgresStatus() != multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY {
		return false
	}
	return commonconsensus.IsRuleDecided(intendedRule)
}
