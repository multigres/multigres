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
	"context"
	"log/slog"

	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// PoolerStore manages pooler health state and provides RPC-based domain queries.
type PoolerStore struct {
	health    *poolerHealthStore
	rpcClient rpcclient.MultiPoolerClient
	logger    *slog.Logger
}

// NewPoolerStore creates a new PoolerStore.
// rpcClient and logger are used by FindHealthyPrimary; they may be nil in tests
// that do not exercise that method.
func NewPoolerStore(rpcClient rpcclient.MultiPoolerClient, logger *slog.Logger) *PoolerStore {
	return &PoolerStore{
		health:    newPoolerHealthStore(),
		rpcClient: rpcClient,
		logger:    logger,
	}
}

// Get retrieves a pooler's health state by its ID string.
// Returns a deep clone safe to mutate, and false if the key does not exist.
func (s *PoolerStore) Get(poolerID string) (*multiorchdatapb.PoolerHealthState, bool) {
	return s.health.get(poolerID)
}

// Set stores a deep clone of the pooler health state.
func (s *PoolerStore) Set(poolerID string, state *multiorchdatapb.PoolerHealthState) {
	s.health.set(poolerID, state)
}

// Delete removes a pooler from the store. Returns true if the pooler existed.
func (s *PoolerStore) Delete(poolerID string) bool {
	return s.health.delete(poolerID)
}

// Len returns the number of poolers in the store.
func (s *PoolerStore) Len() int {
	return s.health.len()
}

// Range iterates over all poolers. Each value passed to the callback is a deep
// clone safe to mutate. Iteration stops early if the callback returns false.
func (s *PoolerStore) Range(fn func(key string, value *multiorchdatapb.PoolerHealthState) bool) {
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
func (s *PoolerStore) DoUpdate(key string, fn func(*multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState) {
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
//	store.DoUpdateRange(func(key string, value *PoolerHealthState) (*PoolerHealthState, bool) {
//	    value.LastSeen = timestamppb.Now()
//	    return value, true // write and continue
//	})
func (s *PoolerStore) DoUpdateRange(fn func(key string, value *multiorchdatapb.PoolerHealthState) (*multiorchdatapb.PoolerHealthState, bool)) {
	s.health.doUpdateRange(fn)
}

// IsInitialized returns true if the pooler has been initialized.
// FindPoolersInShard returns all poolers belonging to the given shard.
func (s *PoolerStore) FindPoolersInShard(shardKey *clustermetadatapb.ShardKey) []*multiorchdatapb.PoolerHealthState {
	var poolers []*multiorchdatapb.PoolerHealthState

	s.health.rangeHealth(func(_ string, pooler *multiorchdatapb.PoolerHealthState) bool {
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

	s.health.rangeHealth(func(_ string, pooler *multiorchdatapb.PoolerHealthState) bool {
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

// FindHealthyPrimary returns the shard's consensus leader from the given poolers,
// verified to still be serving as the leader.
//
// Leadership comes purely from consensus: the highest known rule across the
// poolers' consensus statuses names the leader (commonconsensus.HighestKnownRule),
// never the PoolerType topology/health label. A follower can carry that rule via
// its replication primary, so the leader is found even when its own snapshot is
// stale. The named leader is then verified via a live Status RPC and must still
// report itself as the consensus leader — a node that has since resigned or
// dropped into recovery is rejected. Because there is exactly one highest rule,
// there is no multiple-primary ambiguity to resolve here.
func (s *PoolerStore) FindHealthyPrimary(
	ctx context.Context,
	poolers []*multiorchdatapb.PoolerHealthState,
) (*multiorchdatapb.PoolerHealthState, error) {
	leaderID := findConsensusLeader(poolers)
	if leaderID == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "no consensus leader known")
	}

	var leader *multiorchdatapb.PoolerHealthState
	for _, pooler := range poolers {
		if pooler.GetMultiPooler() != nil && proto.Equal(pooler.MultiPooler.Id, leaderID) {
			leader = pooler
			break
		}
	}
	if leader == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus leader %s is not in the pooler set", leaderID.GetName())
	}

	// Verify the leader is reachable and still reports itself as the leader in its
	// live consensus status — never the PoolerType label.
	statusResp, err := s.rpcClient.Status(ctx, leader.MultiPooler,
		&multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return nil, mterrors.Wrap(err, "consensus leader unreachable during health check")
	}
	if !commonconsensus.IsLeader(statusResp.GetConsensusStatus()) {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus leader %s no longer reports itself as the leader", leaderID.GetName())
	}
	return leader, nil
}

// findConsensusLeader returns the ID of the leader named by the highest known
// rule across the poolers' consensus status, or nil if none is known.
func findConsensusLeader(poolers []*multiorchdatapb.PoolerHealthState) *clustermetadatapb.ID {
	statuses := make([]*clustermetadatapb.ConsensusStatus, 0, len(poolers))
	for _, pooler := range poolers {
		if cs := pooler.GetConsensusStatus(); cs != nil {
			statuses = append(statuses, cs)
		}
	}
	return commonconsensus.HighestKnownRule(statuses).GetLeaderId()
}
