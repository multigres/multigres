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
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// pollLeaderHealth confirms — via a live Status RPC issued right now, not cached
// state — that the shard's consensus leader is reachable and still serving, and
// returns it.
//
// The leader is identified from cached state by the store (sl, produced by
// PoolerStore.FindShardMembers), keeping leader identification (a store concern)
// separate from this live liveness check (an RPC concern). Leader identity comes
// purely from consensus; the named leader is then polled and must still report
// itself as the leader, so a node that has since resigned or dropped into
// recovery — or become unreachable — is rejected.
func pollLeaderHealth(ctx context.Context, rpcClient rpcclient.MultiPoolerClient, sl store.ShardMembers) (*multiorchdatapb.PoolerHealthState, error) {
	leader := sl.Leader
	if leader == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "no consensus leader known")
	}

	statusResp, err := rpcClient.Status(ctx, leader.MultiPooler, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return nil, mterrors.Wrap(err, "consensus leader unreachable during health check")
	}
	if !commonconsensus.IsLeader(statusResp.GetConsensusStatus()) {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus leader %s no longer reports itself as the leader", leader.GetMultiPooler().GetId().GetName())
	}
	return leader, nil
}
