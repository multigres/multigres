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

package recovery

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// leaderInfoPropagationInterval is deliberately much shorter than a recovery
// cycle: telling a pooler who the leader is (or that it's rewind-ready) is
// always safe (SetPrimary no-ops when the pooler is already current), so
// this doesn't need recovery's grace periods or backoff.
const leaderInfoPropagationInterval = 1 * time.Second

const setPrimaryPropagationTimeout = 5 * time.Second

// leaderInfoStaleHealthThreshold skips propagation to a pooler we haven't
// heard from recently: it's likely unreachable, and this loop's per-pooler
// SetPrimary calls run sequentially, so waiting out setPrimaryPropagationTimeout
// against a dead pooler would delay reaching the next (possibly live) one.
const leaderInfoStaleHealthThreshold = 1 * time.Minute

// runLeaderInfoPropagation is a lightweight loop, independent of the
// recovery cycle, that keeps every pooler's known leader identity and
// rewind-readiness current via SetPrimary. It exists because AppointLeaderAction
// can block on the leader's Promote RPC for up to RuleWriteTimeout, during
// which the (single-flight) recovery cycle can't do anything else — including
// re-informing a standby that the leader it's waiting on has just become
// rewind-ready. This loop runs on its own short interval so that information
// reaches standbys promptly regardless of what the recovery cycle is doing.
func (re *Engine) runLeaderInfoPropagation(ctx context.Context) {
	for _, shardKey := range distinctShardKeys(re.poolerCache) {
		re.propagateLeaderInfoForShard(ctx, shardKey)
	}
}

// distinctShardKeys returns one ShardKey per shard currently represented in
// the pooler cache. Deliberately cheaper than analysis.GenerateShardAnalyses,
// which builds a full per-pooler liveness analysis this loop doesn't need —
// it only cares which shards exist, then re-derives everything else itself
// via store.FindShardMembers.
func distinctShardKeys(cache *store.PoolerCache) []*clustermetadatapb.ShardKey {
	seen := make(map[commontypes.ShardKeyString]*clustermetadatapb.ShardKey)
	for _, entry := range cache.All() {
		key := entry.Pooler.GetShardKey()
		seen[commontypes.FormatShardKey(key)] = key
	}
	keys := make([]*clustermetadatapb.ShardKey, 0, len(seen))
	for _, key := range seen {
		keys = append(keys, key)
	}
	return keys
}

// propagateLeaderInfoForShard tells every non-leader pooler in the shard
// about the current leader and its rewind-readiness, skipping any pooler
// that already knows both.
func (re *Engine) propagateLeaderInfoForShard(ctx context.Context, shardKey *clustermetadatapb.ShardKey) {
	members := store.FindShardMembers(re.poolerCache, shardKey)
	leader := members.Leader
	if leader == nil || members.HighestKnownPosition == nil {
		return
	}
	leaderAddress := topoclient.PoolerAddressFor(leader.Health().GetMultipooler())
	leaderRewindReady := commonconsensus.ReplicationPrimaryOrNil(leader.Health().GetConsensusStatus()).GetRewindReady()

	// One goroutine per pooler: each pooler appears at most once in a shard's
	// member list, so there's never more than one in-flight SetPrimary to the
	// same pooler. Waiting for all of them keeps a slow/unreachable pooler
	// from delaying the next tick, without blocking this one on that pooler.
	var wg sync.WaitGroup
	for _, pooler := range members.Poolers {
		if pooler == leader {
			continue
		}
		wg.Add(1)
		go func(pooler *store.Pooler) {
			defer wg.Done()
			re.propagateLeaderInfoToPooler(ctx, pooler, leaderAddress, leaderRewindReady, members.HighestKnownPosition)
		}(pooler)
	}
	wg.Wait()
}

// shouldPropagateLeaderInfo reports whether a pooler needs a SetPrimary call
// to learn the current leader or its rewind-readiness, given rp (the
// pooler's own last-reported ReplicationPrimary, nil if never told anything)
// and lastSeen (its own health, nil if we've never heard from it). now is
// passed in rather than read from the clock so this stays a pure function to
// unit test directly, independent of the health cache and RPC client.
func shouldPropagateLeaderInfo(
	rp *clustermetadatapb.ReplicationPrimary,
	lastSeen *timestamppb.Timestamp,
	now time.Time,
	leaderAddress *clustermetadatapb.PoolerAddress,
	leaderRewindReady bool,
	highestKnownPosition *clustermetadatapb.RulePosition,
) bool {
	if now.Sub(lastSeen.AsTime()) > leaderInfoStaleHealthThreshold {
		return false // likely unreachable; don't let it stall the poolers behind it
	}
	positionMatches := commonconsensus.ReplicationPrimaryMatches(rp, leaderAddress, highestKnownPosition)
	// rewind_ready only ever goes false -> true within a coordinator term (see
	// RecordTermPrimary), so never tell a pooler that already believes true
	// otherwise — even if our own cached view of the leader is stale-false.
	if positionMatches && (rp.GetRewindReady() || rp.GetRewindReady() == leaderRewindReady) {
		return false // already knows everything we'd tell it
	}
	return true
}

// propagateLeaderInfoToPooler tells a single pooler about the current leader
// and its rewind-readiness via SetPrimary, unless it already knows both.
func (re *Engine) propagateLeaderInfoToPooler(
	ctx context.Context,
	pooler *store.Pooler,
	leaderAddress *clustermetadatapb.PoolerAddress,
	leaderRewindReady bool,
	highestKnownPosition *clustermetadatapb.RulePosition,
) {
	health := pooler.Health()
	rp := commonconsensus.ReplicationPrimaryOrNil(health.GetConsensusStatus())
	if !shouldPropagateLeaderInfo(rp, health.GetLastSeen(), time.Now(), leaderAddress, leaderRewindReady, highestKnownPosition) {
		return
	}

	replicationPrimary := &clustermetadatapb.ReplicationPrimary{
		Position:    highestKnownPosition,
		Primary:     leaderAddress,
		RewindReady: leaderRewindReady,
	}
	rpcCtx, cancel := context.WithTimeout(ctx, setPrimaryPropagationTimeout)
	defer cancel()
	_, err := re.rpcClient.SetPrimary(rpcCtx, health.GetMultipooler(), &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: replicationPrimary,
	})
	if err != nil {
		re.logger.WarnContext(ctx, "leader-info propagation: SetPrimary failed, will retry next tick",
			"pooler", health.GetMultipooler().GetId().GetName(), "error", err)
		return
	}

	// Optimistically cache the just-applied primary so the next reconcile
	// tick doesn't redundantly re-send it before the pooler's own streamed
	// health update reports it back to us.
	pooler.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
		h.ConsensusStatus = commonconsensus.FoldReplicationPrimary(h.ConsensusStatus, replicationPrimary)
	})
}
