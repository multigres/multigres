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

// Package store provides multiorch's typed view of the topology pooler
// cache. The cache itself lives in topoclient/poolerwatch and is generic
// over a rider type; this package fixes the rider to *PoolerHealthState
// and supplies orch-specific helpers (FindPoolersInShard,
// FindShardMembers, etc).
package store

import (
	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// PoolerCache is the orch-side type alias for the lifecycle-aware pooler
// cache keyed by orch's PoolerHealthState rider.
type PoolerCache = poolerwatch.PoolerCache[*multiorchdatapb.PoolerHealthState]

// FindPoolerByID looks up a single pooler by its component ID. Returns
// NOT_FOUND if the pooler is not in the cache.
func FindPoolerByID(cache *PoolerCache, id *clustermetadatapb.ID) (*multiorchdatapb.PoolerHealthState, error) {
	entry, ok := cache.Get(topoclient.ComponentIDString(id))
	if !ok {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND,
			"pooler %s/%s not found", id.GetCell(), id.GetName())
	}
	return entry.Rider, nil
}

// FindPoolersInShard returns every pooler the cache holds for the given
// shard. The returned slice is empty if no poolers match.
func FindPoolersInShard(cache *PoolerCache, shardKey *clustermetadatapb.ShardKey) []*multiorchdatapb.PoolerHealthState {
	entries := cache.GetByShard(shardKey.GetDatabase(), shardKey.GetTableGroup(), shardKey.GetShard())
	out := make([]*multiorchdatapb.PoolerHealthState, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Rider)
	}
	return out
}

// ShardMembers is the result of FindShardMembers: the shard's poolers, the
// highest consensus rule known across them, and the pooler that rule names
// as leader.
type ShardMembers struct {
	// Poolers is every pooler the cache holds for the shard.
	Poolers []*multiorchdatapb.PoolerHealthState
	// HighestKnownRule is the highest known consensus rule across Poolers,
	// or nil if none carries a rule. HighestKnownRule.GetLeaderId() names
	// the leader.
	HighestKnownRule *clustermetadatapb.ShardRule
	// Leader is the pooler named by HighestKnownRule, or nil when no rule
	// is known or the named pooler is not in the cache (e.g. known only
	// via a follower's rule).
	Leader *multiorchdatapb.PoolerHealthState
}

// FindShardMembers identifies the shard's members, consensus rule, and
// leader's health.
func FindShardMembers(cache *PoolerCache, shardKey *clustermetadatapb.ShardKey) ShardMembers {
	poolers := FindPoolersInShard(cache, shardKey)

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

// IsInitialized reports whether the pooler has been initialized. A pooler is
// considered initialized based on the IsInitialized field from the Status
// RPC (data-directory state, not LSN). The node must also be reachable for
// us to trust the value.
func IsInitialized(p *multiorchdatapb.PoolerHealthState) bool {
	if !p.IsLastCheckValid {
		return false
	}
	if p.MultiPooler == nil {
		return false
	}
	return p.GetStatus().GetIsInitialized()
}
