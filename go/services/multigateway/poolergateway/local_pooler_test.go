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
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
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

// quietPooler stands in for the local pooler: its health subscription sends
// nothing until ctx ends. Other methods panic through the nil embedded
// interface.
type quietPooler struct {
	queryservice.Pooler
}

func (quietPooler) SubscribeHealth(ctx context.Context) (func() (*multipoolerservice.StreamPoolerHealthResponse, error), error) {
	return func() (*multipoolerservice.StreamPoolerHealthResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil
}

// TestLocalPooler_CalledDirectly checks that the connection to a local pooler
// calls its Service and opens no gRPC connection, although the record's
// address is unreachable.
func TestLocalPooler_CalledDirectly(t *testing.T) {
	pooler := unreachablePooler("zone1", "pooler-1")
	local := &LocalPooler{ID: func() *clustermetadatapb.ID { return pooler.Id }, Service: quietPooler{}}
	conn, err := newPoolerConnection(context.Background(), pooler, slog.New(slog.DiscardHandler),
		grpc.WithTransportCredentials(insecure.NewCredentials()), local, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Shutdown() }()

	assert.Nil(t, conn.conn, "no gRPC connection to a local pooler")
	direct, ok := conn.Pooler().(*directQueryService)
	require.True(t, ok, "the local pooler is called directly, got %T", conn.Pooler())
	assert.Equal(t, local.Service, direct.local)

	client, err := conn.ManagerClient()
	require.Error(t, err, "the local pooler has no manager service to call")
	assert.Nil(t, client)
}

// silentAfterFirstPooler stands in for the local pooler: its health
// subscription sends one SERVING message advertising a short staleness
// timeout, then nothing until ctx ends.
type silentAfterFirstPooler struct {
	queryservice.Pooler
}

func (silentAfterFirstPooler) SubscribeHealth(ctx context.Context) (func() (*multipoolerservice.StreamPoolerHealthResponse, error), error) {
	sent := false
	return func() (*multipoolerservice.StreamPoolerHealthResponse, error) {
		if !sent {
			sent = true
			resp := makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)
			resp.RecommendedStalenessTimeout = durationpb.New(50 * time.Millisecond)
			return resp, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil
}

// TestLocalPooler_NoStalenessTimeout checks that a local pooler's health
// subscription does not time out when it goes quiet: the timeout looks for a
// network partition or a dead pooler process, which an in-process
// subscription cannot have. Over gRPC the same silence times the stream out
// (TestPoolerConnection_StreamHealth_StalenessTimeout).
func TestLocalPooler_NoStalenessTimeout(t *testing.T) {
	pooler := unreachablePooler("zone1", "pooler-1")
	local := &LocalPooler{ID: func() *clustermetadatapb.ID { return pooler.Id }, Service: silentAfterFirstPooler{}}
	conn, err := newPoolerConnection(t.Context(), pooler, slog.New(slog.DiscardHandler),
		grpc.WithTransportCredentials(insecure.NewCredentials()), local, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Shutdown() }()

	require.Eventually(t, func() bool {
		return conn.Health().isServing()
	}, 2*time.Second, 10*time.Millisecond)

	// Many times the advertised staleness timeout.
	time.Sleep(500 * time.Millisecond)
	h := conn.Health()
	assert.True(t, h.isServing(), "a quiet local pooler stays serving")
	assert.NoError(t, h.LastError)
	assert.False(t, conn.healthTimedOut.Load())
}

// TestLocalPooler_RecordFindsPoolerWithoutTopology discovers the local pooler
// from its record with no topology store at all.
func TestLocalPooler_RecordFindsPoolerWithoutTopology(t *testing.T) {
	pooler := unreachablePooler("zone1", "pooler-1")
	local := &LocalPooler{
		ID:      func() *clustermetadatapb.ID { return pooler.Id },
		Record:  func() *clustermetadatapb.Multipooler { return pooler },
		Service: quietPooler{},
	}

	gw := NewPoolerGateway(PoolerGatewayOpts{
		Ctx:         t.Context(),
		Logger:      slog.New(slog.DiscardHandler),
		DialOpt:     grpc.WithTransportCredentials(insecure.NewCredentials()),
		LocalPooler: local,
	})
	defer func() { _ = gw.Close() }()
	assert.Equal(t, 1, gw.PoolerCount(), "the pooler is known without the topology")
}
