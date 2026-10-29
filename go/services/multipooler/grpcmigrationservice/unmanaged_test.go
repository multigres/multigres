// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcmigrationservice

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// TestMigrationServiceIsNotRegisteredOnAnUnmanagedPooler: an unmanaged pooler fronts an
// external database that multigres does not own, so it serves no migration RPCs and starts no
// migration coordinator that could create migration state in that database.
func TestMigrationServiceIsNotRegisteredOnAnUnmanagedPooler(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { _ = ts.Close() })
	pm, err := manager.NewMultipoolerManager(slog.New(slog.NewTextHandler(os.Stderr, nil)), &clustermetadata.Multipooler{
		Id:             &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler"},
		ShardRef:       &clustermetadata.ShardRef{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"},
		ManagementMode: clustermetadata.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED,
	}, &manager.Config{TopoClient: ts, ConnPoolConfig: connpoolmanager.NewConfig(viperutil.NewRegistry())})
	require.NoError(t, err)
	t.Cleanup(func() { pm.ShutdownForTest(ctx) })

	server := grpc.NewServer()
	registerMigrationService(server, pm)
	_, registered := server.GetServiceInfo()[migratorpb.Migrator_ServiceDesc.ServiceName]
	require.False(t, registered)
}
