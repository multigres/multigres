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

package poolergateway

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// unreachablePooler returns a pooler record whose address cannot be reached.
func unreachablePooler(cell, name string) *clustermetadatapb.Multipooler {
	p := topoclient.NewMultipooler(name, cell, "unreachable.invalid")
	p.PortMap["grpc"] = 15270
	return p
}

func TestLocalPooler_IsLocal(t *testing.T) {
	ownName := ""
	local := &LocalPooler{ID: func() *clustermetadatapb.ID { return unreachablePooler("zone1", ownName).Id }}

	assert.False(t, local.isLocal(unreachablePooler("zone1", "pooler-1")), "the ID is not known before flags are parsed")
	ownName = "pooler-1"
	assert.True(t, local.isLocal(unreachablePooler("zone1", "pooler-1")))
	assert.False(t, local.isLocal(unreachablePooler("zone1", "pooler-2")), "another name")
	assert.False(t, local.isLocal(unreachablePooler("zone2", "pooler-1")), "the same name in another cell")
}

// TestLocalPooler_ConnectsInMemory reaches an in-memory server through the
// local pooler's dial options although the record's address is unreachable.
func TestLocalPooler_ConnectsInMemory(t *testing.T) {
	sv := servenv.NewServEnv(viperutil.NewRegistry())
	server := servenv.NewInMemoryGrpcServer(servenv.NewGrpcServer(viperutil.NewRegistry()))
	require.NoError(t, server.Create(sv))
	require.NoError(t, server.Serve(sv))
	t.Cleanup(server.Server.Stop)

	pooler := unreachablePooler("zone1", "pooler-1")
	local := &LocalPooler{ID: func() *clustermetadatapb.ID { return pooler.Id }, DialOptions: server.DialOptions()}
	conn, err := newPoolerConnection(context.Background(), pooler, slog.New(slog.DiscardHandler),
		grpc.WithTransportCredentials(insecure.NewCredentials()), local, true, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Shutdown() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := grpc_health_v1.NewHealthClient(conn.conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
}

// TestLocalPooler_WatchRecordFindsPoolerWithoutTopology discovers the local
// pooler from its record feed with no topology store at all.
func TestLocalPooler_WatchRecordFindsPoolerWithoutTopology(t *testing.T) {
	pooler := unreachablePooler("zone1", "pooler-1")
	stopped := false
	local := &LocalPooler{
		ID: func() *clustermetadatapb.ID { return pooler.Id },
		WatchRecord: func(fn func(*clustermetadatapb.Multipooler)) func() {
			fn(pooler)
			return func() { stopped = true }
		},
	}

	gw := NewPoolerGateway(PoolerGatewayOpts{
		Ctx:         t.Context(),
		Logger:      slog.New(slog.DiscardHandler),
		DialOpt:     grpc.WithTransportCredentials(insecure.NewCredentials()),
		LocalPooler: local,
	})
	assert.Equal(t, 1, gw.PoolerCount(), "the pooler is known without the topology")

	require.NoError(t, gw.Close())
	assert.True(t, stopped, "closing the gateway stops following the record")
}
