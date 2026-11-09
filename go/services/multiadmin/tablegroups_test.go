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

package multiadmin

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func newTableGroupsTestServer(t *testing.T) (*MultiadminServer, topoclient.Store, *rpcclient.FakeClient) {
	t.Helper()
	ts := memorytopo.NewServer(t.Context(), "cell1")
	server := NewMultiadminServer(ts, slog.New(slog.DiscardHandler), grpc.WithTransportCredentials(insecure.NewCredentials()))
	t.Cleanup(server.backupJobTracker.Stop)
	fake := rpcclient.NewFakeClient()
	server.SetRPCClient(fake)
	return server, ts, fake
}

// addCatalogPooler registers a serving pooler of testdb's catalog shard. A
// primary claims its role under coordinator term `term`.
func addCatalogPooler(t *testing.T, ts topoclient.Store, name string, role clustermetadatapb.RoutingRole, term int64) {
	t.Helper()
	mp := &clustermetadatapb.Multipooler{
		Id:            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
		Hostname:      name + ".cell1",
		PortMap:       map[string]int32{"grpc": 8081},
		ShardRef:      &clustermetadatapb.ShardRef{Database: "testdb", TableGroup: "default", Shard: "0-inf"},
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}
	if role != clustermetadatapb.RoutingRole_ROUTING_ROLE_UNKNOWN {
		mp.RoutingState = &clustermetadatapb.RoutingState{Role: role, Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: term}}
	}
	require.NoError(t, ts.CreateMultipooler(t.Context(), mp))
}

func listResponse(groups ...*multipoolermanagerdatapb.TableGroup) *multipoolermanagerdatapb.ListTableGroupsResponse {
	return &multipoolermanagerdatapb.ListTableGroupsResponse{TableGroups: groups}
}

func TestListDesiredTableGroups(t *testing.T) {
	t.Run("forwards to the catalog primary", func(t *testing.T) {
		server, ts, fake := newTableGroupsTestServer(t)
		addCatalogPooler(t, ts, "catalog-replica", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA, 1)
		addCatalogPooler(t, ts, "catalog-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, 1)
		want := []*multipoolermanagerdatapb.TableGroup{
			{TablegroupId: 7, Name: "commerce", State: multipoolermanagerdatapb.TableGroupState_TABLE_GROUP_STATE_CREATING, Shards: []string{"0-80", "80-inf"}},
		}
		fake.ListTableGroupsResponses[topoclient.ComponentID("multipooler-cell1-catalog-primary")] = &multipoolermanagerdatapb.ListTableGroupsResponse{TableGroups: want}
		// The replica would answer with nothing, which must not be what we get.
		fake.ListTableGroupsResponses[topoclient.ComponentID("multipooler-cell1-catalog-replica")] = &multipoolermanagerdatapb.ListTableGroupsResponse{}

		resp, err := server.ListDesiredTableGroups(t.Context(), &multiadminpb.ListDesiredTableGroupsRequest{Database: "testdb"})
		require.NoError(t, err)
		require.Len(t, resp.TableGroups, 1)
		assert.Equal(t, "commerce", resp.TableGroups[0].Name)
		assert.Equal(t, []string{"0-80", "80-inf"}, resp.TableGroups[0].Shards)
	})

	t.Run("two primaries: the higher rule wins", func(t *testing.T) {
		// A stale leader that has not learned of the failover still advertises
		// PRIMARY under its old term, and its catalog may lag.
		server, ts, fake := newTableGroupsTestServer(t)
		addCatalogPooler(t, ts, "stale-leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, 3)
		addCatalogPooler(t, ts, "new-leader", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, 4)
		fake.ListTableGroupsResponses[topoclient.ComponentID("multipooler-cell1-stale-leader")] = listResponse()
		fake.ListTableGroupsResponses[topoclient.ComponentID("multipooler-cell1-new-leader")] = listResponse(
			&multipoolermanagerdatapb.TableGroup{TablegroupId: 8, Name: "analytics"})

		resp, err := server.ListDesiredTableGroups(t.Context(), &multiadminpb.ListDesiredTableGroupsRequest{Database: "testdb"})
		require.NoError(t, err)
		require.Len(t, resp.TableGroups, 1)
		assert.Equal(t, "analytics", resp.TableGroups[0].Name)
	})

	t.Run("a primary that is not serving is not asked", func(t *testing.T) {
		server, ts, fake := newTableGroupsTestServer(t)
		addCatalogPooler(t, ts, "draining-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, 1)
		info, err := ts.GetMultipooler(t.Context(), &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "draining-primary"})
		require.NoError(t, err)
		info.Multipooler.ServingStatus = clustermetadatapb.PoolerServingStatus_DRAINING
		require.NoError(t, ts.UpdateMultipooler(t.Context(), info))
		fake.ListTableGroupsResponses[topoclient.ComponentID("multipooler-cell1-draining-primary")] = listResponse()

		_, err = server.ListDesiredTableGroups(t.Context(), &multiadminpb.ListDesiredTableGroupsRequest{Database: "testdb"})
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("missing database", func(t *testing.T) {
		server, _, _ := newTableGroupsTestServer(t)
		_, err := server.ListDesiredTableGroups(t.Context(), &multiadminpb.ListDesiredTableGroupsRequest{})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("no catalog primary yet", func(t *testing.T) {
		server, ts, _ := newTableGroupsTestServer(t)
		addCatalogPooler(t, ts, "catalog-replica", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA, 1)
		_, err := server.ListDesiredTableGroups(t.Context(), &multiadminpb.ListDesiredTableGroupsRequest{Database: "testdb"})
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("unknown database", func(t *testing.T) {
		server, _, _ := newTableGroupsTestServer(t)
		_, err := server.ListDesiredTableGroups(t.Context(), &multiadminpb.ListDesiredTableGroupsRequest{Database: "nope"})
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("pooler failure is unavailable, never an empty list", func(t *testing.T) {
		server, ts, fake := newTableGroupsTestServer(t)
		addCatalogPooler(t, ts, "catalog-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, 1)
		fake.Errors[topoclient.ComponentID("multipooler-cell1-catalog-primary")] = errors.New("boom")
		_, err := server.ListDesiredTableGroups(t.Context(), &multiadminpb.ListDesiredTableGroupsRequest{Database: "testdb"})
		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})
}
