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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
)

// newRoutingTestServer returns a server whose db1 catalog primary is "leader"
// (port 9000), next to a replica of the same shard and the primary of another
// tablegroup, so the forwarders must pick the catalog primary.
func newRoutingTestServer(t *testing.T) (*MultiadminServer, *rpcclient.FakeClient) {
	t.Helper()
	s := newTestServer(t, "cell1")
	leader := makeRoutedPooler("cell1", "leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	replica := makeRoutedPooler("cell1", "replica", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
	replica.PortMap = map[string]int32{"grpc": 9001}
	other := makeRoutedPooler("cell1", "source-leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	other.ShardRef.TableGroup = "srctg"
	other.PortMap = map[string]int32{"grpc": 9002}
	for _, p := range []*clustermetadatapb.Multipooler{leader, replica, other} {
		require.NoError(t, s.ts.CreateMultipooler(t.Context(), p))
	}
	fake := rpcclient.NewFakeClient()
	s.rpcClient = fake
	return s, fake
}

func TestMultiadminRoutingForwarders(t *testing.T) {
	ctx := t.Context()
	s, fake := newRoutingTestServer(t)
	leader := topoclient.ComponentIDString(poolerID("cell1", "leader"))

	t.Run("SetRoutingRules", func(t *testing.T) {
		fake.SetRoutingRulesResponses[leader] = &multipoolermanagerdatapb.SetRoutingRulesResponse{RoutingVersion: 6}
		resp, err := s.SetRoutingRules(ctx, &multipoolermanagerdatapb.SetRoutingRulesRequest{Database: "db1", OperationId: "op"})
		require.NoError(t, err)
		assert.Equal(t, int64(6), resp.GetRoutingVersion())
	})
	t.Run("DeleteRoutingRules", func(t *testing.T) {
		fake.DeleteRoutingRulesResponses[leader] = &multipoolermanagerdatapb.DeleteRoutingRulesResponse{RoutingVersion: 7}
		resp, err := s.DeleteRoutingRules(ctx, &multipoolermanagerdatapb.DeleteRoutingRulesRequest{Database: "db1", OperationId: "op"})
		require.NoError(t, err)
		assert.Equal(t, int64(7), resp.GetRoutingVersion())
	})
	t.Run("FenceScopes", func(t *testing.T) {
		fake.FenceScopesResponses[leader] = &multipoolermanagerdatapb.FenceScopesResponse{RoutingVersion: 8}
		resp, err := s.FenceScopes(ctx, &multipoolermanagerdatapb.FenceScopesRequest{Database: "db1", OperationId: "op"})
		require.NoError(t, err)
		assert.Equal(t, int64(8), resp.GetRoutingVersion())
	})
	t.Run("UnfenceScopes", func(t *testing.T) {
		fake.UnfenceScopesResponses[leader] = &multipoolermanagerdatapb.UnfenceScopesResponse{RoutingVersion: 9}
		resp, err := s.UnfenceScopes(ctx, &multipoolermanagerdatapb.UnfenceScopesRequest{Database: "db1", OperationId: "op"})
		require.NoError(t, err)
		assert.Equal(t, int64(9), resp.GetRoutingVersion())
	})
	assert.Equal(t, []string{
		"SetRoutingRules(" + string(leader) + ")",
		"DeleteRoutingRules(" + string(leader) + ")",
		"FenceScopes(" + string(leader) + ")",
		"UnfenceScopes(" + string(leader) + ")",
	}, fake.GetCallLog(), "every call goes to the catalog primary")

	t.Run("GetRoutingRules", func(t *testing.T) {
		fake.GetRoutingSnapshotResponses[leader] = &multipoolermanagerdatapb.GetRoutingSnapshotResponse{
			RoutingVersion: 7, PlanEpoch: 2,
			Snapshot: &shardingcatalogpb.Snapshot{
				RoutingVersion: 7, PlanEpoch: 2,
				Tablegroups: []*shardingcatalogpb.TableGroup{
					{TablegroupId: 1, Name: "default", State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE},
					{TablegroupId: 2, Name: "srctg", State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE},
				},
				Shards: []*shardingcatalogpb.Shard{
					{ShardId: 1, TablegroupId: 1, Name: "0-inf", KeyRange: &clustermetadatapb.KeyRange{}, State: shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE},
					{ShardId: 2, TablegroupId: 2, Name: "0-80", KeyRange: &clustermetadatapb.KeyRange{End: []byte{0x80}}, State: shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE},
					{ShardId: 3, TablegroupId: 2, Name: "80-inf", KeyRange: &clustermetadatapb.KeyRange{Start: []byte{0x80}}, State: shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE},
				},
				Tables:       []*shardingcatalogpb.Table{{TableId: 10, SchemaName: "public", TableName: "orders", TablegroupId: 1}},
				ShardIndices: []*shardingcatalogpb.ShardIndex{{ShardIndexId: 20, Name: "hash_xx", Type: "hash", Params: "{}"}},
				// A preparing key: the one the table moves to, not yet its
				// active primary key.
				ShardKeys: []*shardingcatalogpb.ShardKey{{
					ShardKeyId: 30, TableId: 10, ShardIndexId: 20, Name: "by_customer",
					Role: shardingcatalogpb.ShardKeyRole_SHARD_KEY_ROLE_PRIMARY, State: shardingcatalogpb.ShardKeyState_SHARD_KEY_STATE_PREPARING,
				}},
				RoutingRules: []*shardingcatalogpb.RoutingRule{
					{
						RuleId: 4, OperationId: "op", Scope: shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLE,
						FromTablegroupId: 1, FromTableId: proto.Int64(10), ToTablegroupId: 2, ToShardKeyId: proto.Int64(30), CreatedVersion: 6,
					},
					{
						RuleId: 3, OperationId: "op", Scope: shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP,
						FromTablegroupId: 1, ToTablegroupId: 2, ToShardId: proto.Int64(2), CreatedVersion: 6,
					},
				},
			},
		}
		resp, err := s.GetRoutingRules(ctx, &multiadminpb.GetRoutingRulesRequest{Database: "db1"})
		require.NoError(t, err)
		assert.Equal(t, int64(7), resp.GetRoutingVersion())
		assert.Equal(t, int64(2), resp.GetPlanEpoch())
		require.Len(t, resp.GetRules(), 2)
		assert.True(t, proto.Equal(&multiadminpb.RoutingRule{
			RuleId: 3, OperationId: "op", Scope: shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP,
			FromTablegroup: "default", ToTablegroup: "srctg", ToShard: "0-80",
			ToKeyRange: &clustermetadatapb.KeyRange{End: []byte{0x80}}, CreatedVersion: 6,
		}, resp.GetRules()[0]), "the rule's ids resolve to names: %v", resp.GetRules()[0])
		assert.True(t, proto.Equal(&multiadminpb.RoutingRule{
			RuleId: 4, OperationId: "op", Scope: shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLE,
			FromTablegroup: "default", FromTable: &multiadminpb.RoutingTableName{Schema: "public", Name: "orders"},
			ToTablegroup: "srctg", ToShardKey: "by_customer", CreatedVersion: 6,
		}, resp.GetRules()[1]), "rules come in rule_id order, and a table rule names its table and shard key: %v", resp.GetRules()[1])
		assert.Equal(t, "GetRoutingSnapshot("+string(leader)+")", fake.GetCallLog()[len(fake.GetCallLog())-1],
			"the snapshot is read from the catalog primary")
	})
	t.Run("GetRoutingRules refuses an invalid snapshot", func(t *testing.T) {
		// A rule whose target shard does not exist cannot be named.
		fake.GetRoutingSnapshotResponses[leader] = &multipoolermanagerdatapb.GetRoutingSnapshotResponse{
			Snapshot: &shardingcatalogpb.Snapshot{
				Tablegroups: []*shardingcatalogpb.TableGroup{{TablegroupId: 1, Name: "default", State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE}},
				Shards:      []*shardingcatalogpb.Shard{{ShardId: 1, TablegroupId: 1, Name: "0-inf", KeyRange: &clustermetadatapb.KeyRange{}, State: shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE}},
				RoutingRules: []*shardingcatalogpb.RoutingRule{{
					RuleId: 3, OperationId: "op", Scope: shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP,
					FromTablegroupId: 1, ToTablegroupId: 1, ToShardId: proto.Int64(99),
				}},
			},
		}
		_, err := s.GetRoutingRules(ctx, &multiadminpb.GetRoutingRulesRequest{Database: "db1"})
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "routing rule 3 refers to shard 99")
	})
}

func TestMultiadminRoutingForwarders_PoolerErrorPassesThrough(t *testing.T) {
	s, fake := newRoutingTestServer(t)
	leader := topoclient.ComponentIDString(poolerID("cell1", "leader"))
	fake.Errors[leader] = status.Error(codes.FailedPrecondition, "routing version is 5, expected 4")

	_, err := s.SetRoutingRules(t.Context(), &multipoolermanagerdatapb.SetRoutingRulesRequest{Database: "db1", OperationId: "op"})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestMultiadminRoutingForwarders_NoCatalogPrimary(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cell1")
	s.rpcClient = rpcclient.NewFakeClient()

	_, err := s.SetRoutingRules(ctx, &multipoolermanagerdatapb.SetRoutingRulesRequest{Database: "db1"})
	assert.Equal(t, codes.Unavailable, status.Code(err), "no pooler: %v", err)

	_, err = s.GetRoutingRules(ctx, &multiadminpb.GetRoutingRulesRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "no database: %v", err)
}
