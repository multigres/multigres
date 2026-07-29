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
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// leaderInfoPropagationInterval is deliberately much shorter than a recovery
// cycle: telling a pooler who the leader is (or that it's rewind-ready) is
// always safe (SetPrimary no-ops when the pooler is already current), so
// this doesn't need recovery's grace periods or backoff.
const leaderInfoPropagationInterval = 1 * time.Second

const setPrimaryPropagationTimeout = 5 * time.Second

// runLeaderInfoPropagation is a lightweight loop, independent of the
// recovery cycle, that keeps every pooler's known leader identity and
// rewind-readiness current via SetPrimary. It exists because AppointLeaderAction
// can block on the leader's Promote RPC for up to RuleWriteTimeout, during
// which the (single-flight) recovery cycle can't do anything else — including
// re-informing a standby that the leader it's waiting on has just become
// rewind-ready. This loop runs on its own short interval so that information
// reaches standbys promptly regardless of what the recovery cycle is doing.
func (re *Engine) runLeaderInfoPropagation(ctx context.Context) {
	generator := analysis.NewAnalysisGenerator(re.poolerCache, nil)
	for _, shardAnalysis := range generator.GenerateShardAnalyses() {
		re.propagateLeaderInfoForShard(ctx, shardAnalysis.ShardKey)
	}
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

	for _, pooler := range members.Poolers {
		if pooler == leader {
			continue
		}
		rp := commonconsensus.ReplicationPrimaryOrNil(pooler.Health().GetConsensusStatus())
		if commonconsensus.ReplicationPrimaryMatches(rp, leaderAddress, members.HighestKnownPosition) &&
			rp.GetRewindReady() == leaderRewindReady {
			continue // already knows everything we'd tell it
		}

		rpcCtx, cancel := context.WithTimeout(ctx, setPrimaryPropagationTimeout)
		_, err := re.rpcClient.SetPrimary(rpcCtx, pooler.Health().GetMultipooler(), &consensusdatapb.SetPrimaryRequest{
			ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
				Position:    members.HighestKnownPosition,
				Primary:     leaderAddress,
				RewindReady: leaderRewindReady,
			},
		})
		cancel()
		if err != nil {
			re.logger.DebugContext(ctx, "leader-info propagation: SetPrimary failed, will retry next tick",
				"pooler", pooler.Health().GetMultipooler().GetId().GetName(), "error", err)
		}
	}
}
