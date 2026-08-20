// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multiadmin

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *MultiadminServer) findShardPrimary(ctx context.Context, shardKey *clustermetadatapb.ShardKey) (*clustermetadatapb.Multipooler, bool, error) {
	allPoolers, err := s.findShardPoolers(ctx, shardKey)
	if err != nil {
		return nil, false, err
	}

	var leader *clustermetadatapb.Multipooler
	var hasStandby bool

	for _, p := range allPoolers {
		if p.GetRoutingState().GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
			leader = p
		} else {
			hasStandby = true
		}
	}

	if leader != nil {
		return leader, hasStandby, nil
	}

	return nil, false, status.Errorf(codes.NotFound, "no PRIMARY pooler found for shard %s/%s/%s",
		shardKey.GetDatabase(), shardKey.GetTableGroup(), shardKey.GetShard())
}

// SwitchPrimary performs a graceful switchover: it quiesces writes on the
// current leader, which publishes REQUESTING_DEMOTION so multiorch's
// LeaderResignedAnalyzer can elect a new primary through the normal consensus
// flow. The RPC returns as soon as the old primary has been quiesced and
// restarted as a standby — it does not wait for the new leader to appear.
func (s *MultiadminServer) SwitchPrimary(ctx context.Context, req *multiadminpb.SwitchPrimaryRequest) (*multiadminpb.SwitchPrimaryResponse, error) {
	if req.GetShardKey() == nil {
		return nil, status.Error(codes.InvalidArgument, "shard_key is required")
	}
	shardKey := req.GetShardKey()

	// 1. Find the primary pooler for this shard.
	leader, hasStandby, err := s.findShardPrimary(ctx, shardKey)
	if err != nil {
		return nil, err
	}

	if !hasStandby {
		return nil, status.Error(codes.FailedPrecondition, "no standby poolers available for promotion")
	}

	// 3. Pre-check: wait for the coordinator backoff window to clear before
	// quiescing writes. The 4-second guard in checkRecentTermAcceptance rejects
	// a new Recruit if a previous one completed too recently (e.g. bootstrap).
	// Waiting here — before writes are frozen — avoids the pause that would
	// otherwise sit while the old primary's postgres is still running and
	// generating WAL.
	const coordinatorBackoffWindow = 4 * time.Second
	if statusResp, err := s.rpcClient.Status(ctx, leader, &multipoolermanagerdatapb.StatusRequest{}); err == nil {
		if rev := statusResp.GetConsensusStatus().GetTermRevocation(); rev != nil {
			if initAt := rev.GetCoordinatorInitiatedAt(); initAt != nil {
				age := time.Since(initAt.AsTime())
				if age < coordinatorBackoffWindow {
					waitDur := coordinatorBackoffWindow - age + 500*time.Millisecond
					s.logger.InfoContext(ctx, "switch_primary: coordinator backoff active, waiting before quiescing",
						"wait", waitDur)
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(waitDur):
					}
				}
			}
		}
	}

	// 4. Quiesce writes and restart postgres as standby. ResignLeadership
	// transitions to DRAINING (rejecting new queries with MTF01 so the
	// gateway buffers and retries), drains existing write connections, stops
	// postgres cleanly (sending the shutdown-checkpoint WAL to connected
	// standbys), and publishes REQUESTING_DEMOTION for multiorch to act on.
	s.logger.InfoContext(ctx, "switch_primary: resigning current leader",
		"leader", topoclient.ClusterIDString(leader.Id),
		"reason", req.GetReason())

	resignResp, err := s.rpcClient.ResignLeadership(ctx, leader, &multipoolermanagerdatapb.ResignLeadershipRequest{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ResignLeadership failed: %v", err)
	}

	s.logger.InfoContext(ctx, "switch_primary: complete; multiorch will elect a new leader",
		"old_leader", topoclient.ClusterIDString(leader.Id),
		"flush_lsn", resignResp.GetFlushLsn())

	response := &multiadminpb.SwitchPrimaryResponse{
		OldLeaderId: proto.Clone(leader.Id).(*clustermetadatapb.ID),
	}

	return response, nil
}

// findShardPoolers returns all poolers registered for the given shard across
// all cells.
func (s *MultiadminServer) findShardPoolers(ctx context.Context, shardKey *clustermetadatapb.ShardKey) ([]*clustermetadatapb.Multipooler, error) {
	cellNames, err := s.ts.GetCellNames(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list cells: %v", err)
	}

	var poolers []*clustermetadatapb.Multipooler
	for _, cell := range cellNames {
		opts := &topoclient.GetMultipoolersByCellOptions{
			DatabaseShard: &topoclient.DatabaseShard{
				Database:   shardKey.GetDatabase(),
				TableGroup: shardKey.GetTableGroup(),
				Shard:      shardKey.GetShard(),
			},
		}
		infos, err := s.ts.GetMultipoolersByCell(ctx, cell, opts)
		if err != nil {
			s.logger.WarnContext(ctx, "failed to list poolers in cell", "cell", cell, "error", err)
			continue
		}
		for _, info := range infos {
			if info.Multipooler != nil {
				poolers = append(poolers, info.Multipooler)
			}
		}
	}

	if len(poolers) == 0 {
		return nil, status.Errorf(codes.NotFound, "no poolers found for shard %s/%s/%s",
			shardKey.GetDatabase(), shardKey.GetTableGroup(), shardKey.GetShard())
	}
	return poolers, nil
}
