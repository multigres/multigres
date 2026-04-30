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

		if pooler.MultiPooler.Database == shardKey.Database &&
			pooler.MultiPooler.TableGroup == shardKey.TableGroup &&
			pooler.MultiPooler.Shard == shardKey.Shard {
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

// FindHealthyPrimary finds a healthy, initialized primary in the given pooler slice.
// It verifies health by making an RPC call to each candidate.
// Returns an error if multiple primaries are found (likely a stale primary that needs to be demoted).
//
// Candidate selection uses a union of topology type and live health-stream data because
// topology (from etcd) can be stale when etcd is unavailable after a failover. A pooler
// is considered a candidate if either:
//   - its topology type is PRIMARY (MultiPooler.Type), or
//   - its most recent health-stream snapshot reports it is running as PRIMARY (Status.PoolerType).
//
// Each candidate is then verified via Status RPC; only nodes whose live PoolerType
// is PRIMARY are accepted, so stale topology entries running as standby are skipped.
func (s *PoolerStore) FindHealthyPrimary(
	ctx context.Context,
	poolers []*multiorchdatapb.PoolerHealthState,
) (*multiorchdatapb.PoolerHealthState, error) {
	var healthyPrimary *multiorchdatapb.PoolerHealthState

	for _, pooler := range poolers {
		if pooler.MultiPooler == nil {
			continue
		}

		// Accept candidates indicated as PRIMARY by topology OR live health data.
		// Topology can be stale when etcd is unavailable; health data can lag during
		// role transitions. Using the union avoids missing the actual primary in either case.
		isTopologyPrimary := pooler.MultiPooler.Type == clustermetadatapb.PoolerType_PRIMARY
		isHealthPrimary := pooler.Status != nil && pooler.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY
		if !isTopologyPrimary && !isHealthPrimary {
			continue
		}

		// Verify via Status RPC — check the live PoolerType to skip stale candidates
		// (e.g. topology says PRIMARY but postgres is running as standby after a failover).
		statusResp, err := s.rpcClient.Status(ctx, pooler.MultiPooler,
			&multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			s.logger.WarnContext(ctx, "primary unreachable during health check",
				"pooler", pooler.MultiPooler.Id.Name,
				"error", err)
			continue
		}
		if statusResp.GetStatus().GetPoolerType() != clustermetadatapb.PoolerType_PRIMARY {
			s.logger.WarnContext(ctx, "pooler is not running as primary, skipping",
				"pooler", pooler.MultiPooler.Id.Name,
				"pooler_type", statusResp.GetStatus().GetPoolerType())
			continue
		}

		if healthyPrimary != nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"multiple primaries found: %s and %s (stale primary needs demotion)",
				healthyPrimary.MultiPooler.Id.Name, pooler.MultiPooler.Id.Name)
		}
		healthyPrimary = pooler
	}

	if healthyPrimary == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no healthy primary found")
	}

	return healthyPrimary, nil
}
