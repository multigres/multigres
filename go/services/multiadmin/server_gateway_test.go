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
	"errors"
	"log/slog"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multigatewaymanagerpb "github.com/multigres/multigres/go/pb/multigatewaymanager"
	multigatewaymanagerdatapb "github.com/multigres/multigres/go/pb/multigatewaymanagerdata"
)

// fakeGatewayManager implements the gateway-side MultiGatewayManagerServer with
// canned responses so the multiadmin proxy can be exercised in process.
type fakeGatewayManager struct {
	multigatewaymanagerpb.UnimplementedMultiGatewayManagerServer
	queriesResp      *multigatewaymanagerpb.GetQueryRegistryResponse
	consolidatorResp *multigatewaymanagerpb.GetConsolidatorStatsResponse
	queriesErr       error
	consolidatorErr  error
}

func (f *fakeGatewayManager) GetQueryRegistry(_ context.Context, _ *multigatewaymanagerpb.GetQueryRegistryRequest) (*multigatewaymanagerpb.GetQueryRegistryResponse, error) {
	return f.queriesResp, f.queriesErr
}

func (f *fakeGatewayManager) GetConsolidatorStats(_ context.Context, _ *multigatewaymanagerpb.GetConsolidatorStatsRequest) (*multigatewaymanagerpb.GetConsolidatorStatsResponse, error) {
	return f.consolidatorResp, f.consolidatorErr
}

// startFakeGateway boots an in-process gRPC server hosting fakeGatewayManager
// on a random localhost port and returns its dial target plus a cleanup hook.
func startFakeGateway(t *testing.T, fake *fakeGatewayManager) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	multigatewaymanagerpb.RegisterMultiGatewayManagerServer(srv, fake)

	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
	})
	return lis.Addr().String()
}

func newGatewayProxyServerForTest(t *testing.T, target string) (*MultiAdminServer, topoclient.Store) {
	t.Helper()
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	srv := NewMultiAdminServer(ts, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Always dial the in-process fake regardless of the gateway's recorded
	// hostname/port; this isolates the proxy logic from address resolution.
	srv.gatewayDialer = func(_ context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return srv, ts
}

func registerTestGateway(t *testing.T, ts topoclient.Store, name string) *clustermetadatapb.ID {
	t.Helper()
	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIGATEWAY,
		Cell:      "cell1",
		Name:      name,
	}
	gw := &clustermetadatapb.MultiGateway{
		Id:       id,
		Hostname: "127.0.0.1",
		PortMap:  map[string]int32{"grpc": 1},
	}
	require.NoError(t, ts.CreateMultiGateway(t.Context(), gw))
	return id
}

func TestMultiAdminServerGetGatewayQueriesValidation(t *testing.T) {
	srv, _ := newGatewayProxyServerForTest(t, "127.0.0.1:0")

	t.Run("nil gateway_id returns InvalidArgument", func(t *testing.T) {
		_, err := srv.GetGatewayQueries(t.Context(), &multiadminpb.GetGatewayQueriesRequest{GatewayId: nil})
		st, _ := status.FromError(err)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "gateway_id cannot be empty")
	})

	t.Run("empty cell returns InvalidArgument", func(t *testing.T) {
		_, err := srv.GetGatewayQueries(t.Context(), &multiadminpb.GetGatewayQueriesRequest{
			GatewayId: &clustermetadatapb.ID{Cell: "", Name: "g1"},
		})
		st, _ := status.FromError(err)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "gateway_id must have both cell and name")
	})

	t.Run("non-existent gateway returns NotFound", func(t *testing.T) {
		_, err := srv.GetGatewayQueries(t.Context(), &multiadminpb.GetGatewayQueriesRequest{
			GatewayId: &clustermetadatapb.ID{Cell: "cell1", Name: "missing"},
		})
		st, _ := status.FromError(err)
		assert.Equal(t, codes.NotFound, st.Code())
		assert.Contains(t, st.Message(), "gateway 'cell1/missing' not found")
	})
}

func TestMultiAdminServerGetGatewayQueriesProxy(t *testing.T) {
	fake := &fakeGatewayManager{
		queriesResp: &multigatewaymanagerpb.GetQueryRegistryResponse{
			Snapshot: &multigatewaymanagerdatapb.QueryRegistrySnapshot{
				TrackedFingerprints: 2,
				Queries: []*multigatewaymanagerdatapb.QueryStatSnapshot{
					{Fingerprint: "fp1", NormalizedSql: "SELECT 1", Calls: 10},
					{Fingerprint: "fp2", NormalizedSql: "UPDATE t SET x=$1", Calls: 3},
				},
			},
		},
	}
	target := startFakeGateway(t, fake)
	srv, ts := newGatewayProxyServerForTest(t, target)
	id := registerTestGateway(t, ts, "g1")

	resp, err := srv.GetGatewayQueries(t.Context(), &multiadminpb.GetGatewayQueriesRequest{GatewayId: id})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Snapshot)
	assert.EqualValues(t, 2, resp.Snapshot.TrackedFingerprints)
	require.Len(t, resp.Snapshot.Queries, 2)
	assert.Equal(t, "fp1", resp.Snapshot.Queries[0].Fingerprint)
	assert.Equal(t, "UPDATE t SET x=$1", resp.Snapshot.Queries[1].NormalizedSql)
}

func TestMultiAdminServerGetGatewayQueriesUpstreamError(t *testing.T) {
	fake := &fakeGatewayManager{queriesErr: errors.New("boom")}
	target := startFakeGateway(t, fake)
	srv, ts := newGatewayProxyServerForTest(t, target)
	id := registerTestGateway(t, ts, "g1")

	_, err := srv.GetGatewayQueries(t.Context(), &multiadminpb.GetGatewayQueriesRequest{GatewayId: id})
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Unavailable, st.Code())
	assert.Contains(t, st.Message(), "failed to get query registry from gateway")
}

func TestMultiAdminServerGetGatewayConsolidatorProxy(t *testing.T) {
	fake := &fakeGatewayManager{
		consolidatorResp: &multigatewaymanagerpb.GetConsolidatorStatsResponse{
			Stats: &multigatewaymanagerdatapb.ConsolidatorStats{
				UniqueStatements: 4,
				TotalReferences:  10,
				ConnectionCount:  3,
				PreparedStatements: []*multigatewaymanagerdatapb.ConsolidatorPreparedStatement{
					{Name: "s1", Query: "SELECT 1", Refs: 5},
				},
			},
		},
	}
	target := startFakeGateway(t, fake)
	srv, ts := newGatewayProxyServerForTest(t, target)
	id := registerTestGateway(t, ts, "g2")

	resp, err := srv.GetGatewayConsolidator(t.Context(), &multiadminpb.GetGatewayConsolidatorRequest{GatewayId: id})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Stats)
	assert.EqualValues(t, 4, resp.Stats.UniqueStatements)
	assert.EqualValues(t, 10, resp.Stats.TotalReferences)
	assert.EqualValues(t, 3, resp.Stats.ConnectionCount)
	require.Len(t, resp.Stats.PreparedStatements, 1)
	assert.Equal(t, "s1", resp.Stats.PreparedStatements[0].Name)
}
