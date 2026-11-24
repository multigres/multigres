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

package consensus

import (
	"context"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// FindRoutingPrimary returns the topology record of shard's routing primary:
// the pooler of the shard that advertises the PRIMARY routing role, under the
// highest consensus rule if several do (one that was deposed may not have
// learned it yet). Poolers that shut down are skipped. Every cell is searched,
// as the primary may be in any of them.
//
// It fails with UNAVAILABLE when the cells cannot be listed, when any cell
// cannot be read, or when no pooler of the shard advertises PRIMARY. A cell
// that cannot be read may hold the primary under a higher rule than any the
// others show, so the best of a partial read is not the primary: during a
// failover it can be the deposed primary, which still answers with its older
// state.
//
// TODO: with the consensus rules this can do better: a primary whose rule
// the shard's cohort confirms is current is the primary even while a cell
// cannot be read, so reaching the rest of the cells should be enough.
func FindRoutingPrimary(ctx context.Context, ts topoclient.Store, shard *clustermetadatapb.ShardRef) (*clustermetadatapb.Multipooler, error) {
	cells, err := ts.GetCellNames(ctx)
	if err != nil {
		return nil, mterrors.WithCode(mterrors.Wrap(err, "failed to list cells to find the routing primary"), mtrpcpb.Code_UNAVAILABLE)
	}
	var best *clustermetadatapb.Multipooler
	var unread []string
	for _, cell := range cells {
		infos, err := ts.GetMultipoolersByCell(ctx, cell, &topoclient.GetMultipoolersByCellOptions{
			DatabaseShard: &topoclient.DatabaseShard{Database: shard.GetDatabase(), TableGroup: shard.GetTableGroup()},
		})
		if err != nil {
			unread = append(unread, cell)
			continue
		}
		for _, info := range infos {
			p := info.Multipooler
			if p.GetShardRef().GetShard() != shard.GetShard() ||
				p.GetLifecycleStatus().GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN {
				continue
			}
			rs := p.GetRoutingState()
			if rs.GetRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
				continue
			}
			if best == nil || CompareRuleNumbers(rs.GetRule(), best.GetRoutingState().GetRule()) > 0 {
				best = p
			}
		}
	}
	if len(unread) > 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"cells %v could not be read, so the routing primary of %s/%s/%s cannot be determined",
			unread, shard.GetDatabase(), shard.GetTableGroup(), shard.GetShard())
	}
	if best == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "no routing primary of %s/%s/%s is registered",
			shard.GetDatabase(), shard.GetTableGroup(), shard.GetShard())
	}
	return best, nil
}
