// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	multiorchdata "github.com/multigres/multigres/go/pb/multiorchdata"
)

// PoolerHealthStore is a thread-safe store for pooler health state.
// It provides clone-on-read/write semantics so callers always work with
// isolated copies, preventing concurrent mutation of shared state.
type PoolerHealthStore struct {
	proto *ProtoStore[string, *multiorchdata.PoolerHealthState]
}

// NewPoolerHealthStore creates a new store for pooler health state.
func NewPoolerHealthStore() *PoolerHealthStore {
	return &PoolerHealthStore{
		proto: NewProtoStore[string, *multiorchdata.PoolerHealthState](),
	}
}

// Get retrieves a pooler's health state by its ID string.
// Returns a deep clone safe to mutate, and false if the key does not exist.
func (s *PoolerHealthStore) Get(poolerID string) (*multiorchdata.PoolerHealthState, bool) {
	return s.proto.Get(poolerID)
}

// set stores a deep clone of the pooler health state.
// Unexported: mutations must go through PoolerStore to keep health and op-state in sync.
func (s *PoolerHealthStore) set(poolerID string, state *multiorchdata.PoolerHealthState) {
	s.proto.Set(poolerID, state)
}

// delete removes a pooler from the store. Returns true if the pooler existed.
// Unexported: mutations must go through PoolerStore to keep health and op-state in sync.
func (s *PoolerHealthStore) delete(poolerID string) bool {
	return s.proto.Delete(poolerID)
}

// Len returns the number of poolers in the store.
func (s *PoolerHealthStore) Len() int {
	return s.proto.Len()
}

// Range iterates over all poolers. Each value passed to the callback is a deep
// clone safe to mutate. Iteration stops early if the callback returns false.
func (s *PoolerHealthStore) Range(fn func(key string, value *multiorchdata.PoolerHealthState) bool) {
	s.proto.Range(fn)
}

// IsInitialized returns true if the pooler has been initialized.
// A pooler is considered initialized based on the IsInitialized field from
// the Status RPC, which is determined by the data directory state (not LSN).
// The node must also be reachable for us to trust this information.
func IsInitialized(p *multiorchdata.PoolerHealthState) bool {
	if !p.IsLastCheckValid {
		return false // unreachable nodes are considered uninitialized
	}

	if p.MultiPooler == nil {
		return false
	}

	// Use the IsInitialized field from Status RPC directly.
	// This is based on data directory state, not LSN.
	return p.IsInitialized
}
