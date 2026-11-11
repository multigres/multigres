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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// The routing RPCs are thin forwarders to the primary of the database's
// default tablegroup, which holds the database's catalog and validates every
// request itself. multiadmin holds no routing state.

// catalogPrimary returns the pooler that holds database's catalog: the leader
// of its default tablegroup.
func (s *MultiadminServer) catalogPrimary(ctx context.Context, database string) (*clustermetadatapb.Multipooler, error) {
	if database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}
	pooler, err := s.findPooler(ctx, database, constants.DefaultTableGroup, constants.DefaultShard, true, false)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "no catalog primary for database %q: %v", database, err)
	}
	return pooler, nil
}

// GetRoutingRules reads the sharding catalog snapshot from the database's
// catalog primary and returns its routing rules, with the ids resolved to the
// names a gateway routes by.
func (s *MultiadminServer) GetRoutingRules(ctx context.Context, req *multiadminpb.GetRoutingRulesRequest) (*multiadminpb.GetRoutingRulesResponse, error) {
	pooler, err := s.catalogPrimary(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	resp, err := s.rpcClient.GetRoutingSnapshot(ctx, pooler, &multipoolermanagerdatapb.GetRoutingSnapshotRequest{})
	if err != nil {
		return nil, err
	}
	if resp.GetSnapshot() == nil {
		return nil, status.Errorf(codes.Internal, "the catalog primary of database %q returned no snapshot", req.GetDatabase())
	}
	snap, err := shardingcatalog.New(resp.GetSnapshot())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "the catalog primary of database %q served an invalid sharding catalog snapshot: %v", req.GetDatabase(), err)
	}
	return routingRulesOf(snap), nil
}

// routingRulesOf returns snap's versions and rules, with each rule's
// tablegroups, table, shard and shard key named as the read model resolved
// them.
func routingRulesOf(snap *shardingcatalog.Snapshot) *multiadminpb.GetRoutingRulesResponse {
	out := &multiadminpb.GetRoutingRulesResponse{RoutingVersion: snap.RoutingVersion(), PlanEpoch: snap.PlanEpoch()}
	for _, r := range snap.RoutingRules() {
		rule := &multiadminpb.RoutingRule{
			RuleId:         r.ID(),
			OperationId:    r.OperationID(),
			Scope:          r.Scope(),
			FromTablegroup: r.FromTableGroup().Name(),
			ToTablegroup:   r.ToTableGroup().Name(),
			CreatedVersion: r.CreatedVersion(),
		}
		if t := r.FromTable(); t != nil {
			rule.FromTable = &multiadminpb.RoutingTableName{Schema: t.Schema(), Name: t.Name()}
		}
		if sh := r.ToShard(); sh != nil {
			rule.ToShard = sh.Name()
			rule.ToKeyRange = sh.KeyRange()
		}
		if k := r.ToShardKey(); k != nil {
			rule.ToShardKey = k.Name()
		}
		out.Rules = append(out.Rules, rule)
	}
	return out
}

// SetRoutingRules forwards to the database's catalog primary.
func (s *MultiadminServer) SetRoutingRules(ctx context.Context, req *multipoolermanagerdatapb.SetRoutingRulesRequest) (*multipoolermanagerdatapb.SetRoutingRulesResponse, error) {
	pooler, err := s.catalogPrimary(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return s.rpcClient.SetRoutingRules(ctx, pooler, req)
}

// DeleteRoutingRules forwards to the database's catalog primary.
func (s *MultiadminServer) DeleteRoutingRules(ctx context.Context, req *multipoolermanagerdatapb.DeleteRoutingRulesRequest) (*multipoolermanagerdatapb.DeleteRoutingRulesResponse, error) {
	pooler, err := s.catalogPrimary(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return s.rpcClient.DeleteRoutingRules(ctx, pooler, req)
}
