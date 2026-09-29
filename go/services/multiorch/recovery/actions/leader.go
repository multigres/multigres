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
// the leader, and has postgres ready to serve, and returns it along with the
// live StatusResponse the check itself just fetched (callers that need a
// fresher read of the leader than the cached store, e.g. quorum-commit
// staleness, can use it instead of issuing their own extra RPC).
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
//     failover it was dispatched to perform, or
//   - its postgres is in recovery (a STANDBY): SelfConsensusRole names a leader
//     purely from the consensus rule, so a rule-named leader whose Promote never
//     completed still self-claims leader AND answers pg_isready continuously.
//     Without this check appoint_leader would treat an in-recovery standby as an
//     existing writable primary and skip the failover — mirrors the analyzer's
//     leaderInRecovery guard so a routed failover actually promotes a real primary.
func pollLeaderHealth(ctx context.Context, rpcClient rpcclient.MultipoolerClient, sl store.ShardMembers) (*store.Pooler, *multipoolermanagerdatapb.StatusResponse, error) {
	leader := sl.Leader
	if leader == nil {
		return nil, nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "no consensus leader known")
	}

	statusResp, err := rpcClient.Status(ctx, leader.Health().Multipooler, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return nil, nil, mterrors.Wrap(err, "consensus leader unreachable during health check")
	}
	if commonconsensus.SelfConsensusRole(statusResp.GetConsensusStatus()) != commonconsensus.ConsensusRoleLeader {
		return nil, nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus leader %s no longer reports itself as the leader", leader.Health().GetMultipooler().GetId().GetName())
	}
	if !statusResp.GetStatus().GetPostgresReady() {
		return nil, nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus leader %s postgres is not ready", leader.Health().GetMultipooler().GetId().GetName())
	}
	if statusResp.GetStatus().GetPostgresStatus() == multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY {
		return nil, nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus leader %s postgres is in recovery (standby), not a writable primary", leader.Health().GetMultipooler().GetId().GetName())
	}
	return leader, statusResp, nil
}
