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

package actions

import (
	"context"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/services/multiorch/store"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// pollLeaderHealth confirms — via a live Status RPC issued right now, not cached
// state — that the shard's consensus leader is reachable, still names itself as
// the leader, and has postgres ready to serve, and returns it.
//
// The leader is identified from cached state by the store (sl, produced by
// PoolerStore.FindShardMembers), keeping leader identification (a store concern)
// separate from this live liveness check (an RPC concern). Leader identity comes
// purely from consensus; the named leader is then polled and rejected when:
//   - it is unreachable (Status RPC fails),
//   - it no longer names itself as the leader (resigned or dropped into recovery), or
//   - its postgres is not ready — crucially, a primary whose postgres was killed
//     while its multipooler stays alive keeps self-claiming the consensus rule
//     (Status falls back to the cached rule position), so without the postgres-ready
//     check appoint_leader would treat such a dead-primary as healthy and skip the
//     failover it was dispatched to perform.
func pollLeaderHealth(ctx context.Context, rpcClient rpcclient.MultiPoolerClient, sl store.ShardMembers) (*store.Pooler, error) {
	leader := sl.Leader
	if leader == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "no consensus leader known")
	}

	statusResp, err := rpcClient.Status(ctx, leader.Health().MultiPooler, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return nil, mterrors.Wrap(err, "consensus leader unreachable during health check")
	}
	if !commonconsensus.NamesSelfAsLeader(statusResp.GetConsensusStatus()) {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus leader %s no longer reports itself as the leader", leader.Health().GetMultiPooler().GetId().GetName())
	}
	if !statusResp.GetStatus().GetPostgresReady() {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus leader %s postgres is not ready", leader.Health().GetMultiPooler().GetId().GetName())
	}
	return leader, nil
}
