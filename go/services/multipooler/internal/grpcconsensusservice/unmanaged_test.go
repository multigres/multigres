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

package grpcconsensusservice

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdata "github.com/multigres/multigres/go/pb/consensusdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
	"github.com/multigres/multigres/go/tools/viperutil"
)

func newTestManager(t *testing.T, mode clustermetadata.PoolerManagementMode, tablegroup string) *manager.MultipoolerManager {
	t.Helper()
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { _ = ts.Close() })
	pm, err := manager.NewMultipoolerManager(slog.New(slog.NewTextHandler(os.Stderr, nil)), &clustermetadata.Multipooler{
		Id:             &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler"},
		ShardRef:       &clustermetadata.ShardRef{Database: "db", TableGroup: tablegroup, Shard: "0-inf"},
		ManagementMode: mode,
	}, &manager.Config{TopoClient: ts, ConnPoolConfig: connpoolmanager.NewConfig(viperutil.NewRegistry())})
	require.NoError(t, err)
	t.Cleanup(func() { pm.ShutdownForTest(ctx) })
	return pm
}

// TestConsensusServiceIsNotRegisteredOnAnUnmanagedPooler: an unmanaged pooler never takes part
// in consensus. The service is not registered at all, so every method, any added later
// included, answers Unimplemented, and a managed pooler still registers it.
func TestConsensusServiceIsNotRegisteredOnAnUnmanagedPooler(t *testing.T) {
	service := consensuspb.MultipoolerConsensus_ServiceDesc.ServiceName
	for name, tc := range map[string]struct {
		mode       clustermetadata.PoolerManagementMode
		tablegroup string
		registered bool
	}{
		"unmanaged": {clustermetadata.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, "migrateTG", false},
		"managed":   {clustermetadata.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED, "default", true},
	} {
		t.Run(name, func(t *testing.T) {
			server := grpc.NewServer()
			registerConsensusService(server, newTestManager(t, tc.mode, tc.tablegroup))
			_, registered := server.GetServiceInfo()[service]
			require.Equal(t, tc.registered, registered)

			lis, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			go func() { _ = server.Serve(lis) }()
			t.Cleanup(server.Stop)
			conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })

			_, err = consensuspb.NewMultipoolerConsensusClient(conn).Recruit(t.Context(), &consensusdata.RecruitRequest{})
			require.Error(t, err)
			if tc.registered {
				require.NotEqual(t, codes.Unimplemented, status.Code(err), "a managed pooler serves the call: %v", err)
			} else {
				require.Equal(t, codes.Unimplemented, status.Code(err))
			}
		})
	}
}
