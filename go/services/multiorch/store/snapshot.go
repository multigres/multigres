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
	"cmp"
	"slices"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
)

// SnapshotOptions are the inputs to a snapshot capture besides the cache.
type SnapshotOptions struct {
	// Now is the evaluation time recorded on the snapshot.
	Now time.Time
	// AvailabilityPolicy is the availability policy in effect for the snapshot.
	AvailabilityPolicy snapshot.AvailabilityPolicy
	// BootstrapDurabilityPolicy returns a database's bootstrap durability policy.
	// May be nil, in which case snapshots carry none.
	BootstrapDurabilityPolicy func(database string) *clustermetadatapb.DurabilityPolicy
}

// SnapshotShards freezes every shard in the cache: each pooler's current health
// is read once, so everything derived from a snapshot agrees. The pooler list and
// the tombstones are read separately, so a pooler shutting down mid-capture can
// appear in both.
func SnapshotShards(cache *PoolerCache, opts SnapshotOptions) []*snapshot.Shard {
	type shardEntry struct {
		key     *clustermetadatapb.ShardRef
		poolers []*multiorchdatapb.PoolerHealthState
	}
	byKey := make(map[commontypes.ShardRefString]*shardEntry)
	for _, entry := range cache.All() {
		health := healthOf(entry.Rider)
		if health == nil {
			continue
		}
		// TODO: a pooler that keeps its ID but switches shards is not handled: between
		// the cache entry and its rider's metadata updating, its old health can be
		// filed under the new shard. Poolers normally get a new ID instead.
		key := entry.Pooler.GetShardRef()
		id := commontypes.FormatShardRef(key)
		if byKey[id] == nil {
			byKey[id] = &shardEntry{key: key}
		}
		byKey[id].poolers = append(byKey[id].poolers, health)
	}

	tombstones := tombstoneIDs(cache)
	shards := make([]*snapshot.Shard, 0, len(byKey))
	for _, e := range byKey {
		shards = append(shards, newShard(e.key, e.poolers, tombstones, opts))
	}
	return shards
}

// SnapshotShard freezes one shard. It returns NOT_FOUND if the cache holds no
// poolers for it.
func SnapshotShard(cache *PoolerCache, shardRef *clustermetadatapb.ShardRef, opts SnapshotOptions) (*snapshot.Shard, error) {
	var poolers []*multiorchdatapb.PoolerHealthState
	for _, entry := range cache.GetByShard(shardRef.GetDatabase(), shardRef.GetTableGroup(), shardRef.GetShard()) {
		if health := healthOf(entry.Rider); health != nil {
			poolers = append(poolers, health)
		}
	}
	if len(poolers) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "shard not found: %s", commontypes.FormatShardRef(shardRef))
	}
	return newShard(shardRef, poolers, tombstoneIDs(cache), opts), nil
}

// healthOf returns the rider's current health, or nil if the rider has no
// identified pooler yet.
func healthOf(p *Pooler) *multiorchdatapb.PoolerHealthState {
	if p == nil {
		return nil
	}
	health := p.Health()
	if health.GetMultipooler().GetId() == nil {
		return nil
	}
	return health
}

func tombstoneIDs(cache *PoolerCache) map[topoclient.ComponentID]struct{} {
	tombstones := cache.Tombstones()
	ids := make(map[topoclient.ComponentID]struct{}, len(tombstones))
	for _, t := range tombstones {
		ids[topoclient.ComponentIDString(t.ID)] = struct{}{}
	}
	return ids
}

func newShard(
	shardRef *clustermetadatapb.ShardRef,
	poolers []*multiorchdatapb.PoolerHealthState,
	tombstones map[topoclient.ComponentID]struct{},
	opts SnapshotOptions,
) *snapshot.Shard {
	// Stable order, so ties between equally-ranked rules resolve the same way
	// every time.
	slices.SortFunc(poolers, func(a, b *multiorchdatapb.PoolerHealthState) int {
		aID, bID := a.GetMultipooler().GetId(), b.GetMultipooler().GetId()
		return cmp.Or(
			cmp.Compare(aID.GetComponent(), bID.GetComponent()),
			cmp.Compare(aID.GetCell(), bID.GetCell()),
			cmp.Compare(aID.GetName(), bID.GetName()),
		)
	})
	s := &snapshot.Shard{
		ShardRef:           shardRef,
		Poolers:            poolers,
		Now:                opts.Now,
		AvailabilityPolicy: opts.AvailabilityPolicy,
		TombstoneIDs:       tombstones,
	}
	if opts.BootstrapDurabilityPolicy != nil {
		s.BootstrapDurabilityPolicy = opts.BootstrapDurabilityPolicy(shardRef.GetDatabase())
	}
	return s
}
