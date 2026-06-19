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
type Pooler struct {
	HealthStream *HealthStream

	mu    sync.Mutex
	state *multiorchdatapb.PoolerHealthState
}

// NewPooler constructs a Pooler with the given initial health state.
// The initial state is stored as-is (no clone) since the caller is
// surrendering ownership to the rider.
func NewPooler(initial *multiorchdatapb.PoolerHealthState, hs *HealthStream) *Pooler {
	return &Pooler{HealthStream: hs, state: initial}
}

// Health returns an independent clone of the pooler's health state.
// Callers may mutate the returned proto freely — it is not shared with
// any future Mutate. Returns nil if the rider was constructed with a
// nil initial state.
func (p *Pooler) Health() *multiorchdatapb.PoolerHealthState {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == nil {
		return nil
	}
	return proto.Clone(p.state).(*multiorchdatapb.PoolerHealthState)
}

// Mutate runs fn against the pooler's health state under the rider's
// lock. fn may read AND write fields freely; the snapshot it sees is
// consistent for the duration of the call (no concurrent Mutate or
// reader can observe a partial update). fn is invoked at most once.
//
// The state passed to fn is a clone of the current state, so subsequent
// readers (who get their own clones) are unaffected if fn returns early
// or panics before completing — only fn's final mutations are published.
func (p *Pooler) Mutate(fn func(*multiorchdatapb.PoolerHealthState)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var next *multiorchdatapb.PoolerHealthState
	if p.state == nil {
		next = &multiorchdatapb.PoolerHealthState{}
	} else {
		next = proto.Clone(p.state).(*multiorchdatapb.PoolerHealthState)
	}
	fn(next)
	p.state = next
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
