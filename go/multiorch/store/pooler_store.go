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
	commontypes "github.com/multigres/multigres/go/common/types"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// PoolerStore wraps the generic ProtoStore with pooler-specific query methods.
// It provides convenient accessors for finding poolers by shard, ID, or role,
// and can verify health via RPC calls.
type PoolerStore struct {
	store     *ProtoStore[string, *multiorchdatapb.PoolerHealthState]
	rpcClient rpcclient.MultiPoolerClient
	logger    *slog.Logger
}

// NewPoolerStore creates a new PoolerStore wrapper.
func NewPoolerStore(
	store *ProtoStore[string, *multiorchdatapb.PoolerHealthState],
	rpcClient rpcclient.MultiPoolerClient,
	logger *slog.Logger,
) *PoolerStore {
	return &PoolerStore{
		store:     store,
		rpcClient: rpcClient,
		logger:    logger,
	}
}

// Range delegates to the underlying store.
func (s *PoolerStore) Range(fn func(key string, value *multiorchdatapb.PoolerHealthState) bool) {
	s.store.Range(fn)
}

// FindPoolersInShard returns all poolers belonging to the given shard.
func (s *PoolerStore) FindPoolersInShard(shardKey commontypes.ShardKey) []*multiorchdatapb.PoolerHealthState {
	var poolers []*multiorchdatapb.PoolerHealthState

	s.store.Range(func(_ string, pooler *multiorchdatapb.PoolerHealthState) bool {
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

	s.store.Range(func(_ string, pooler *multiorchdatapb.PoolerHealthState) bool {
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
func (s *PoolerStore) FindHealthyPrimary(
	ctx context.Context,
	poolers []*multiorchdatapb.PoolerHealthState,
) (*multiorchdatapb.PoolerHealthState, error) {
	var healthyPrimary *multiorchdatapb.PoolerHealthState

	for _, pooler := range poolers {
		if pooler.MultiPooler == nil ||
			pooler.MultiPooler.Type != clustermetadatapb.PoolerType_PRIMARY {
			continue
		}

		// Verify it's actually reachable and healthy via RPC
		statusResp, err := s.rpcClient.Status(ctx, pooler.MultiPooler,
			&multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			s.logger.WarnContext(ctx, "primary unreachable during health check",
				"pooler", pooler.MultiPooler.Id.Name,
				"error", err)
			continue
		}

		if statusResp.Status != nil && statusResp.Status.IsInitialized {
			if healthyPrimary != nil {
				return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"multiple primaries found: %s and %s (stale primary needs demotion)",
					healthyPrimary.MultiPooler.Id.Name, pooler.MultiPooler.Id.Name)
			}
			healthyPrimary = pooler
		}
	}

	if healthyPrimary == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no healthy primary found")
	}

	return healthyPrimary, nil
}
