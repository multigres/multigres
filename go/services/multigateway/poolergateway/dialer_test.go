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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// testPooler returns a pooler record whose address cannot be reached.
func testPooler(cell, name string) *clustermetadatapb.Multipooler {
	p := topoclient.NewMultipooler(name, cell, "unreachable.invalid")
	p.PortMap["grpc"] = 15270
	return p
}

func TestRemoteDialer_UsesTopologyAddress(t *testing.T) {
	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	target, opts, err := NewRemoteDialer(creds).Target(testPooler("zone1", "pooler-1"))
	require.NoError(t, err)
	assert.Equal(t, "unreachable.invalid:15270", target)
	assert.Len(t, opts, 1)
}

func TestLocalDialer_RejectsOtherPoolers(t *testing.T) {
	self := func() *clustermetadatapb.ID { return testPooler("zone1", "pooler-1").Id }
	dialer := NewLocalDialer(self, func(context.Context) (net.Conn, error) {
		t.Fatal("must not dial another process's pooler")
		return nil, nil
	})

	for _, other := range []*clustermetadatapb.Multipooler{
		testPooler("zone1", "pooler-2"), // another name
		testPooler("zone2", "pooler-1"), // same name, another cell
	} {
		_, _, err := dialer.Target(other)
		require.ErrorIs(t, err, ErrNotLocalPooler)
	}
}

func TestLocalDialer_ReadsOwnIDOnEveryTarget(t *testing.T) {
	ownName := ""
	self := func() *clustermetadatapb.ID { return testPooler("zone1", ownName).Id }
	dialer := NewLocalDialer(self, nil)

	// Before flags are parsed the ID is not known yet.
	_, _, err := dialer.Target(testPooler("zone1", "pooler-1"))
	require.ErrorIs(t, err, ErrNotLocalPooler)

	ownName = "pooler-1"
	target, _, err := dialer.Target(testPooler("zone1", "pooler-1"))
	require.NoError(t, err)
	assert.Equal(t, localTarget, target)
}

// TestLocalDialer_ConnectsInMemory connects to the own pooler's record, whose
// address cannot be reached, and gets an answer from an in-memory server: the
// topology address is never used.
func TestLocalDialer_ConnectsInMemory(t *testing.T) {
	sv := servenv.NewServEnv(viperutil.NewRegistry())
	server := servenv.NewInMemoryGrpcServer(servenv.NewGrpcServer(viperutil.NewRegistry()))
	require.NoError(t, server.Create(sv))
	require.NoError(t, server.Serve(sv))
	t.Cleanup(server.Server.Stop)

	own := testPooler("zone1", "pooler-1")
	dialer := NewLocalDialer(func() *clustermetadatapb.ID { return own.Id }, server.Dial)
	target, opts, err := dialer.Target(own)
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
