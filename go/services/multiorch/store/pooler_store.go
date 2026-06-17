// Copyright 2025 Supabase, Inc.
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
	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// PoolerStore manages cached pooler health state and provides in-memory domain
// queries over it. It is a pure data store: it makes no RPCs and forms no
// liveness judgments. Callers that must confirm a pooler is reachable or still
// serving do so separately (see the recovery actions' leader verification).
type PoolerStore struct {
	health *poolerHealthStore
}

// NewPoolerStore creates a new PoolerStore.
func NewPoolerStore() *PoolerStore {
	return &PoolerStore{
		health: newPoolerHealthStore(),
	}
}

// Get retrieves a pooler's health state by its ID.
// Returns a deep clone safe to mutate, and false if the key does not exist.
func (s *PoolerStore) Get(poolerID topoclient.ComponentID) (*multiorchdatapb.PoolerHealthState, bool) {
	return s.health.get(poolerID)
}

// Set stores a deep clone of the pooler health state.
func (s *PoolerStore) Set(poolerID topoclient.ComponentID, state *multiorchdatapb.PoolerHealthState) {
	s.health.set(poolerID, state)
}

// Delete removes a pooler from the store. Returns true if the pooler existed.
func (s *PoolerStore) Delete(poolerID topoclient.ComponentID) bool {
	return s.health.delete(poolerID)
}

// Len returns the number of poolers in the store.
func (s *PoolerStore) Len() int {
	return s.health.len()
}

// Range iterates over all poolers. Each value passed to the callback is a deep
// clone safe to mutate. Iteration stops early if the callback returns false.
func (s *PoolerStore) Range(fn func(key topoclient.ComponentID, value *multiorchdatapb.PoolerHealthState) bool) {
	s.health.rangeHealth(fn)
}

// DoUpdate performs an atomic read-modify-write on a pooler's health state.
//
// The provided function receives the current value (or nil if not present) and
// returns the new value to store. This is useful for safely updating state
// based on the existing state without needing to do multiple Get/Set calls.
//
// Note that the function should not do any expensive or blocking calls since it
// is executed while holding the store lock.
func (s *PoolerStore) DoUpdate(key topoclient.ComponentID, fn func(*multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState) {
	s.health.doUpdate(key, fn)
}

// DoUpdateRange iterates over all poolers while holding the lock and allows
// in-place updates.
//
// Each value passed to the callback is the raw internal value (not a clone).
// Return the updated value to write it back, or nil to leave it unchanged.
// Return false to stop iteration early, consistent with Range. The callback
// must not retain the pointer after it returns, and must not perform any
// expensive or blocking operations since it runs while holding the store lock.
//
// Example:
//
//	store.DoUpdateRange(func(key topoclient.ComponentID, value *PoolerHealthState) (*PoolerHealthState, bool) {
//	    value.LastSeen = timestamppb.Now()
//	    return value, true // write and continue
//	})
func (s *PoolerStore) DoUpdateRange(fn func(key topoclient.ComponentID, value *multiorchdatapb.PoolerHealthState) (*multiorchdatapb.PoolerHealthState, bool)) {
	s.health.doUpdateRange(fn)
}

// FindPoolersInShard returns all poolers belonging to the given shard.
func (s *PoolerStore) FindPoolersInShard(shardKey *clustermetadatapb.ShardKey) []*multiorchdatapb.PoolerHealthState {
	var poolers []*multiorchdatapb.PoolerHealthState

	s.health.rangeHealth(func(_ topoclient.ComponentID, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		if proto.Equal(pooler.MultiPooler.GetShardKey(), shardKey) {
			poolers = append(poolers, pooler)
		}

		return true // continue
	})

	return poolers
}

// FindPoolerByID finds a pooler in the store by its cell and name.
func (s *PoolerStore) FindPoolerByID(id *clustermetadatapb.ID) (*multiorchdatapb.PoolerHealthState, error) {
	var found *multiorchdatapb.PoolerHealthState

	s.health.rangeHealth(func(_ topoclient.ComponentID, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		if pooler.MultiPooler.Id.Name == id.Name &&
			pooler.MultiPooler.Id.Cell == id.Cell {
			found = pooler
			return false // stop iteration
		}

		return true // continue
	})

	if found == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND,
			"pooler %s/%s not found", id.Cell, id.Name)
	}

	return found, nil
}

// ShardMembers is the result of FindShardMembers: the shard's poolers, the highest
// consensus rule known across them, and the pooler that rule names as leader.
type ShardMembers struct {
	// Poolers is every pooler the store knows about for the shard.
	Poolers []*multiorchdatapb.PoolerHealthState
	// HighestKnownRule is the highest known consensus rule across Poolers, or nil
	// if none carries a rule. HighestKnownRule.GetLeaderId() names the leader.
	HighestKnownRule *clustermetadatapb.ShardRule
	// Leader is the pooler named by HighestKnownRule, or nil when no rule is known
	// or the named pooler is not in the store (e.g. known only via a follower's
	// rule).
	Leader *multiorchdatapb.PoolerHealthState
}

// FindShardMembers identifies the shard's members, consensus rule, and leader's health.
func (s *PoolerStore) FindShardMembers(shardKey *clustermetadatapb.ShardKey) ShardMembers {
	poolers := s.FindPoolersInShard(shardKey)

	statuses := make([]*clustermetadatapb.ConsensusStatus, 0, len(poolers))
	for _, pooler := range poolers {
		if cs := pooler.GetConsensusStatus(); cs != nil {
			statuses = append(statuses, cs)
		}
	}

	rule := commonconsensus.HighestKnownRule(statuses)
	leaderID := rule.GetLeaderId()

	var leader *multiorchdatapb.PoolerHealthState
	if leaderID != nil {
		for _, pooler := range poolers {
			if proto.Equal(pooler.GetMultiPooler().GetId(), leaderID) {
				leader = pooler
				break
			}
		}
	}

	return ShardMembers{Poolers: poolers, HighestKnownRule: rule, Leader: leader}
}
