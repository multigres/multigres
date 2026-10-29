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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// fakeRoutingPooler is a bufconn-served pooler query service that answers
// GetRoutingSnapshot with a canned response and records the request.
type fakeRoutingPooler struct {
	multipoolerpb.UnimplementedMultipoolerServiceServer
	got *multipoolermanagerdatapb.GetRoutingSnapshotRequest
}

func (f *fakeRoutingPooler) GetRoutingSnapshot(_ context.Context, in *multipoolermanagerdatapb.GetRoutingSnapshotRequest) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error) {
	f.got = in
	return &multipoolermanagerdatapb.GetRoutingSnapshotResponse{RoutingVersion: 7, Full: true}, nil
}

func startFakeRoutingPooler(t *testing.T) (func(context.Context, string) (*grpc.ClientConn, error), *fakeRoutingPooler, *[]string) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	fake := &fakeRoutingPooler{}
	multipoolerpb.RegisterMultipoolerServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	var dialed []string
	dialer := func(_ context.Context, target string) (*grpc.ClientConn, error) {
		dialed = append(dialed, target)
		return grpc.NewClient(
			"passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
	return dialer, fake, &dialed
}

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

	t.Run("GetRoutingSnapshot", func(t *testing.T) {
		dialer, pooler, dialed := startFakeRoutingPooler(t)
		s.migrationDialer = dialer
		req := &multipoolermanagerdatapb.GetRoutingSnapshotRequest{Database: "db1", SinceVersion: 3}
		resp, err := s.GetRoutingSnapshot(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, int64(7), resp.GetRoutingVersion())
		assert.Equal(t, "db1", pooler.got.GetDatabase())
		assert.Equal(t, int64(3), pooler.got.GetSinceVersion())
		assert.Equal(t, []string{"localhost:9000"}, *dialed, "the snapshot is read from the catalog primary")
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

	_, err = s.GetRoutingSnapshot(ctx, &multipoolermanagerdatapb.GetRoutingSnapshotRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "no database: %v", err)
}
