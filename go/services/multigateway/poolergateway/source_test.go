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
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// testPooler returns a pooler record whose address cannot be reached.
func testPooler(cell, name string) *clustermetadatapb.Multipooler {
	p := topoclient.NewMultipooler(name, cell, "unreachable.invalid")
	p.PortMap["grpc"] = 15270
	return p
}

// fakeFeed is a RecordFeed driven by the test.
type fakeFeed struct {
	record *clustermetadatapb.Multipooler
	fn     func(*clustermetadatapb.Multipooler)
}

func (f *fakeFeed) WatchRecord(fn func(*clustermetadatapb.Multipooler)) func() {
	f.fn = fn
	fn(f.record)
	return func() { f.fn = nil }
}

func (f *fakeFeed) publish(record *clustermetadatapb.Multipooler) {
	if f.fn != nil {
		f.fn(record)
	}
}

func TestRemoteSource_TargetIsTopologyAddress(t *testing.T) {
	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	target, opts, err := NewRemoteSource(nil, creds).Target(testPooler("zone1", "pooler-1"))
	require.NoError(t, err)
	assert.Equal(t, "unreachable.invalid:15270", target)
	assert.Len(t, opts, 1)
}

func TestLocalSource_DeliversItsPooler(t *testing.T) {
	feed := &fakeFeed{record: testPooler("zone1", "pooler-1")}
	source := NewLocalSource(feed, servenv.NewInMemoryEndpoint())
	var snapshots, upserts []string
	source.Start(context.Background(), slog.New(slog.DiscardHandler), poolerwatch.Handlers{
		OnSnapshot: func(cell string, poolers []*clustermetadatapb.Multipooler) {
			for _, p := range poolers {
				snapshots = append(snapshots, cell+"/"+p.GetId().GetName())
			}
		},
		OnUpsert: func(p *clustermetadatapb.Multipooler) { upserts = append(upserts, p.GetId().GetName()) },
	})
	assert.Equal(t, []string{"zone1/pooler-1"}, snapshots, "the pooler arrives as a snapshot of its cell")

	feed.publish(testPooler("zone1", "pooler-1"))
	assert.Equal(t, []string{"pooler-1"}, upserts, "each change arrives as an upsert")

	source.Stop()
	feed.publish(testPooler("zone1", "pooler-1"))
	assert.Len(t, upserts, 1, "nothing arrives after Stop")
}

// TestLocalSource_ConnectsAtEndpoint reaches a server on the source's endpoint
// although the record's address is unreachable.
func TestLocalSource_ConnectsAtEndpoint(t *testing.T) {
	sv := servenv.NewServEnv(viperutil.NewRegistry())
	endpoint := servenv.NewInMemoryEndpoint()
	server := servenv.NewLocalGrpcServer(servenv.NewGrpcServer(viperutil.NewRegistry()), endpoint)
	require.NoError(t, server.Create(sv))
	require.NoError(t, server.Serve(sv))
	t.Cleanup(server.Server.Stop)

	own := testPooler("zone1", "pooler-1")
	source := NewLocalSource(&fakeFeed{record: own}, endpoint)
	target, opts, err := source.Target(own)
	require.NoError(t, err)

	conn, err := grpc.NewClient(target, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
}
