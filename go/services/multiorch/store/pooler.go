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

	"google.golang.org/protobuf/proto"

	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
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
//	(a) a COPY of the etcd MultiPooler — already authoritatively held
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

// IsInitialized reports whether the pooler has been initialized. A pooler is
// considered initialized based on the IsInitialized field from the Status
// RPC (data-directory state, not LSN). The node must also be reachable for
// us to trust the value.
func (p *Pooler) IsInitialized() bool {
	h := p.Health()
	if h == nil || !h.IsLastCheckValid {
		return false
	}
	if h.MultiPooler == nil {
		return false
	}
	return h.GetStatus().GetIsInitialized()
}
