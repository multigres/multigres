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

const defaultCatchupTimeout = 30 * time.Second

// SwitchPrimary performs a graceful switchover: it quiesces writes on the
// current leader (which publishes REQUESTING_DEMOTION), then polls topology
// until multiorch's LeaderResignedAnalyzer elects a new primary.
func (s *MultiAdminServer) SwitchPrimary(ctx context.Context, req *multiadminpb.SwitchPrimaryRequest) (*multiadminpb.SwitchPrimaryResponse, error) {
	if req.GetShardKey() == nil {
		return nil, status.Error(codes.InvalidArgument, "shard_key is required")
	}
	shardKey := req.GetShardKey()

	// 1. Discover all poolers for this shard.
	allPoolers, err := s.findShardPoolers(ctx, shardKey)
	if err != nil {
		return nil, err
	}

	// 2. Identify the current leader (PRIMARY).
	var leader *clustermetadatapb.MultiPooler
	for _, p := range allPoolers {
		if p.Type == clustermetadatapb.PoolerType_PRIMARY {
			leader = p
			break
		}
	}
	if leader == nil {
		return nil, status.Error(codes.FailedPrecondition, "no PRIMARY pooler found for shard")
	}

	hasStandby := false
	for _, p := range allPoolers {
		if p.Type != clustermetadatapb.PoolerType_PRIMARY {
			hasStandby = true
			break
		}
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
					s.logger.InfoContext(ctx, "SwitchPrimary: coordinator backoff active, waiting before quiescing",
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
	// transitions to NOT_SERVING (rejecting new queries with MTF01 so the
	// gateway buffers and retries), drains existing write connections, stops
	// postgres cleanly (sending the shutdown-checkpoint WAL to connected
	// standbys), and publishes REQUESTING_DEMOTION for multiorch to act on.
	s.logger.InfoContext(ctx, "SwitchPrimary: resigning current leader",
		"leader", topoclient.ClusterIDString(leader.Id),
		"reason", req.GetReason())

	resignResp, err := s.rpcClient.ResignLeadership(ctx, leader, &multipoolermanagerdatapb.ResignLeadershipRequest{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ResignLeadership failed: %v", err)
	}
	flushLSN := resignResp.GetFlushLsn()
	s.logger.InfoContext(ctx, "SwitchPrimary: writes quiesced; waiting for multiorch to elect new leader",
		"flush_lsn", flushLSN,
		"leader", topoclient.ClusterIDString(leader.Id))

	// 5. Poll topology until a new PRIMARY appears (or timeout).
	catchupTimeout := defaultCatchupTimeout
	if d := req.GetCatchupTimeout().AsDuration(); d > 0 {
		catchupTimeout = d
	}
	deadline := time.Now().Add(catchupTimeout)

	var newLeader *clustermetadatapb.MultiPooler
	for time.Now().Before(deadline) {
		poolers, err := s.findShardPoolers(ctx, shardKey)
		if err == nil {
			for _, p := range poolers {
				if p.Type == clustermetadatapb.PoolerType_PRIMARY && !proto.Equal(p.Id, leader.Id) {
					newLeader = p
					break
				}
			}
		}
		if newLeader != nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}

	if newLeader == nil {
		return nil, status.Errorf(codes.DeadlineExceeded,
			"resigned (REQUESTING_DEMOTION persists) but no new leader elected within %s; "+
				"multiorch will complete the election",
			catchupTimeout)
	}

	s.logger.InfoContext(ctx, "SwitchPrimary complete",
		"old_leader", topoclient.ClusterIDString(leader.Id),
		"new_leader", topoclient.ClusterIDString(newLeader.Id))

	return &multiadminpb.SwitchPrimaryResponse{
		NewLeaderId: proto.Clone(newLeader.Id).(*clustermetadatapb.ID),
		OldLeaderId: proto.Clone(leader.Id).(*clustermetadatapb.ID),
	}, nil
}

// findShardPoolers returns all poolers registered for the given shard across
// all cells.
func (s *MultiAdminServer) findShardPoolers(ctx context.Context, shardKey *clustermetadatapb.ShardKey) ([]*clustermetadatapb.MultiPooler, error) {
	cellNames, err := s.ts.GetCellNames(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list cells: %v", err)
	}

	var poolers []*clustermetadatapb.MultiPooler
	for _, cell := range cellNames {
		opts := &topoclient.GetMultiPoolersByCellOptions{
			DatabaseShard: &topoclient.DatabaseShard{
				Database:   shardKey.GetDatabase(),
				TableGroup: shardKey.GetTableGroup(),
			},
		}
		infos, err := s.ts.GetMultiPoolersByCell(ctx, cell, opts)
		if err != nil {
			s.logger.WarnContext(ctx, "failed to list poolers in cell", "cell", cell, "error", err)
			continue
		}
		for _, info := range infos {
			if info.MultiPooler != nil {
				poolers = append(poolers, info.MultiPooler)
			}
		}
	}

	if len(poolers) == 0 {
		return nil, status.Errorf(codes.NotFound, "no poolers found for shard %s/%s/%s",
			shardKey.GetDatabase(), shardKey.GetTableGroup(), shardKey.GetShard())
	}
	return poolers, nil
}
