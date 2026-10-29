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
	assert.Equal(t, []string{
		"SetRoutingRules(" + string(leader) + ")",
		"DeleteRoutingRules(" + string(leader) + ")",
	}, fake.GetCallLog(), "every call goes to the catalog primary")

	t.Run("GetRoutingRules", func(t *testing.T) {
		fake.GetRoutingSnapshotResponses[leader] = &multipoolermanagerdatapb.GetRoutingSnapshotResponse{
			RoutingVersion: 7, PlanEpoch: 2,
			Snapshot: &shardingcatalogpb.Snapshot{
				RoutingVersion: 7, PlanEpoch: 2,
				Tablegroups: []*shardingcatalogpb.TableGroup{{TablegroupId: 1, Name: "default"}, {TablegroupId: 2, Name: "srctg"}},
				Shards: []*shardingcatalogpb.Shard{
					{ShardId: 1, TablegroupId: 1, Name: "0-inf", KeyRange: &clustermetadatapb.KeyRange{}},
					{ShardId: 2, TablegroupId: 2, Name: "0-80", KeyRange: &clustermetadatapb.KeyRange{End: []byte{0x80}}},
				},
				RoutingRules: []*shardingcatalogpb.RoutingRule{{
					RuleId: 3, OperationId: "op", Scope: shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP,
					FromTablegroupId: 1, ToTablegroupId: 2, ToShardId: proto.Int64(2), CreatedVersion: 6,
				}},
			},
		}
		resp, err := s.GetRoutingRules(ctx, &multiadminpb.GetRoutingRulesRequest{Database: "db1"})
		require.NoError(t, err)
		assert.Equal(t, int64(7), resp.GetRoutingVersion())
		assert.Equal(t, int64(2), resp.GetPlanEpoch())
		require.Len(t, resp.GetRules(), 1)
		assert.True(t, proto.Equal(&multiadminpb.RoutingRule{
			RuleId: 3, OperationId: "op", Scope: shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP,
			FromTablegroup: "default", ToTablegroup: "srctg", ToShard: "0-80",
			ToKeyRange: &clustermetadatapb.KeyRange{End: []byte{0x80}}, CreatedVersion: 6,
		}, resp.GetRules()[0]), "the rule's ids resolve to names: %v", resp.GetRules()[0])
		assert.Equal(t, "GetRoutingSnapshot("+string(leader)+")", fake.GetCallLog()[len(fake.GetCallLog())-1],
			"the snapshot is read from the catalog primary")
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
