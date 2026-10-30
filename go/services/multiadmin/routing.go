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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
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
	return routingRulesOf(resp.GetSnapshot()), nil
}

// routingRulesOf returns snap's versions and rules, naming each rule's
// tablegroups, table, shard and shard key. An id the snapshot does not
// contain resolves to an empty name.
func routingRulesOf(snap *shardingcatalogpb.Snapshot) *multiadminpb.GetRoutingRulesResponse {
	tablegroups := map[int64]string{}
	for _, tg := range snap.GetTablegroups() {
		tablegroups[tg.GetTablegroupId()] = tg.GetName()
	}
	shards := map[int64]*shardingcatalogpb.Shard{}
	for _, sh := range snap.GetShards() {
		shards[sh.GetShardId()] = sh
	}
	tables := map[int64]*shardingcatalogpb.Table{}
	for _, t := range snap.GetTables() {
		tables[t.GetTableId()] = t
	}
	shardKeys := map[int64]string{}
	for _, k := range snap.GetShardKeys() {
		shardKeys[k.GetShardKeyId()] = k.GetName()
	}

	out := &multiadminpb.GetRoutingRulesResponse{RoutingVersion: snap.GetRoutingVersion(), PlanEpoch: snap.GetPlanEpoch()}
	for _, r := range snap.GetRoutingRules() {
		rule := &multiadminpb.RoutingRule{
			RuleId:         r.GetRuleId(),
			OperationId:    r.GetOperationId(),
			Scope:          r.GetScope(),
			FromTablegroup: tablegroups[r.GetFromTablegroupId()],
			ToTablegroup:   tablegroups[r.GetToTablegroupId()],
			CreatedVersion: r.GetCreatedVersion(),
		}
		if r.FromTableId != nil {
			t := tables[r.GetFromTableId()]
			rule.FromTable = &multiadminpb.RoutingTableName{Schema: t.GetSchemaName(), Name: t.GetTableName()}
		}
		if r.ToShardId != nil {
			sh := shards[r.GetToShardId()]
			rule.ToShard = sh.GetName()
			rule.ToKeyRange = sh.GetKeyRange()
		}
		if r.ToShardKeyId != nil {
			rule.ToShardKey = shardKeys[r.GetToShardKeyId()]
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

// EnsureImportTarget forwards to the database's catalog primary.
func (s *MultiadminServer) EnsureImportTarget(ctx context.Context, req *multipoolermanagerdatapb.EnsureImportTargetRequest) (*multipoolermanagerdatapb.EnsureImportTargetResponse, error) {
	pooler, err := s.catalogPrimary(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return s.rpcClient.EnsureImportTarget(ctx, pooler, req)
}
