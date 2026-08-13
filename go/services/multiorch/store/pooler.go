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

package store

import (
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Pooler is multiorch's per-pooler cache rider. It bundles the proto
// health state with the per-pooler stream-lifecycle handle.
//
// Concurrency: all access to the proto state is mediated by an internal
// mutex. Readers receive an independent clone via Health(); writers run
// their callback under the lock via Mutate, so a read-modify-write inside
// a single Mutate sees a consistent snapshot (no TOCTOU across the
// callback). Health() callers may safely modify the returned proto since
// it is independent of any future mutation. The HealthStream field is
// set once at OnLive and pointer-stable.
//
// TODO (design cleanup, follow-up PR): PoolerHealthState shouldn't be a
// proto at all — it's never serialized over any RPC (zero references
// in multiorchservice.proto or any other .proto service). The proto
// machinery is overhead with no payoff: schema rigidity, proto.Clone
// cost on every Health()/Mutate(), generated code, etc.
//
// Beyond the proto issue, the type itself amalgamates three unrelated
// concerns:
//
//	(a) a COPY of the etcd Multipooler — already authoritatively held
//	    in the cache's entry.Pooler, so this is needless duplication.
//	(b) the multipooler's reported Status (the actual pooler health
//	    reply, multipoolermanagerdata.Status — itself a real wire type).
//	(c) orch bookkeeping fields (last_check_*, stream_*, etc.) that
//	    grew organically without intent.
//
// Target shape:
//   - entry.Pooler stays the single source of truth for etcd identity.
//   - The rider becomes a small Go struct (not a proto) holding the
//     pooler's *multipoolermanagerdata.Status plus only the orch
//     bookkeeping that has a justified consumer. Every field needs a
//     real reader; if nobody reads it, it doesn't exist.
//   - Helpers (FindPoolerByID, FindPoolersInShard, FindShardMembers)
//     read identity from entry.Pooler and runtime state from the rider.
//
// Don't accrete fields on PoolerHealthState in the meantime — every new
// field makes the cleanup more invasive.
type Pooler struct {
	HealthStream *HealthStream

	// state is an immutable snapshot published by Mutate via copy-on-write
	// and read by Health via atomic load. mu serializes concurrent Mutate
	// callers (so their clone+modify+store sequences don't lose updates).
	// Readers never need the mutex — atomic.Load yields a published snapshot
	// that is by-construction never modified.
	mu    sync.Mutex
	state atomic.Pointer[multiorchdatapb.PoolerHealthState]
}

// NewPooler constructs a Pooler with the given initial health state.
// The initial state is stored as-is (no clone) since the caller is
// surrendering ownership to the rider.
func NewPooler(initial *multiorchdatapb.PoolerHealthState, hs *HealthStream) *Pooler {
	p := &Pooler{HealthStream: hs}
	if initial != nil {
		p.state.Store(initial)
	}
	return p
}

// Health returns the pooler's current health state snapshot. Returns nil
// if no state has been published yet.
//
// IMPORTANT: callers MUST NOT mutate the returned proto. The snapshot is
// shared with other readers and with future Mutate calls (which copy the
// current pointer as their starting point). Mutating it would corrupt
// state visible to other goroutines.
//
// (Why no clone-on-read: snapshots are immutable by construction —
// Mutate always allocates a new proto and atomic-publishes it — so the
// safety contract is "writers don't reach in," not "readers defensively
// copy." Read paths are hot — analyzers call Health() in tight loops —
// so cloning every read would be wasteful.)
func (p *Pooler) Health() *multiorchdatapb.PoolerHealthState {
	return p.state.Load()
}

// Mutate copy-on-writes the health state. fn receives a clone of the
// current state; mutate it freely. The clone is atomically published as
// the new snapshot when fn returns.
//
// The rider's mu serializes concurrent Mutate callers so that one's
// store doesn't overwrite another's mutations. Inside fn, no concurrent
// Mutate or reader can observe a partial update (the new pointer is
// only published at function return).
func (p *Pooler) Mutate(fn func(*multiorchdatapb.PoolerHealthState)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	curr := p.state.Load()
	var next *multiorchdatapb.PoolerHealthState
	if curr == nil {
		next = &multiorchdatapb.PoolerHealthState{}
	} else {
		next = proto.Clone(curr).(*multiorchdatapb.PoolerHealthState)
	}
	fn(next)
	p.state.Store(next)
}

// ObservationAge reports how long ago — on the orchestrator's clock — this
// pooler's most recent successful health snapshot was recorded, measured
// against now. ok is false when no snapshot time has ever been recorded
// (LastSeen unset), letting callers distinguish "never observed" from
// "observed but stale".
//
// It deliberately uses LastSeen (the orchestrator-clock receipt time) rather
// than the pooler-clock pooler_captured_at, so the subtraction against now
// stays same-clock and is unaffected by skew between the hosts.
func (p *Pooler) ObservationAge(now time.Time) (time.Duration, bool) {
	ls := p.Health().GetLastSeen()
	if ls == nil {
		return 0, false
	}
	return now.Sub(ls.AsTime()), true
}

// DefaultObservationFreshness is the default staleness tolerance for callers
// that need a trustworthy health snapshot but have no more specific policy of
// their own (e.g. actions, which can't import the analysis package's
// AvailabilityPolicy). analysis.DefaultAvailabilityPolicy's ObservationFreshness
// uses this same value, so the two packages share one source of truth.
const DefaultObservationFreshness = 15 * time.Second

// HealthWithin returns the pooler's health snapshot if it was recorded within
// maxAge of now, and ok=false otherwise (including "never observed"). Prefer
// this over Health() wherever a decision depends on how current the data is —
// it makes the staleness tolerance an explicit, mandatory choice at the call
// site instead of a separately-remembered ObservationAge/observationFresh
// check that's easy to omit.
func (p *Pooler) HealthWithin(now time.Time, maxAge time.Duration) (*multiorchdatapb.PoolerHealthState, bool) {
	age, ok := p.ObservationAge(now)
	if !ok || age > maxAge {
		return nil, false
	}
	return p.Health(), true
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
// actually precludes writes), and the shard's highest known rule fully
// decided — no outstanding proposal to race against.
//
// TODO: this is a proxy for "the leader can currently commit a synchronous
// write", not direct proof of it — the strongest signal would be a recent
// successful heartbeat write, which isn't surfaced through health today.
// Tighten this once that signal exists.
func LeaderWritesProgressing(leader *Pooler, highestKnownPosition *clustermetadatapb.RulePosition, now time.Time, freshness time.Duration) bool {
	if leader == nil {
		return false
	}
	age, ok := leader.ObservationAge(now)
	if !ok || age > freshness {
		return false
	}
	if leader.Health().GetStatus().GetPostgresStatus() != multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY {
		return false
	}
	return commonconsensus.IsRuleDecided(highestKnownPosition)
}
