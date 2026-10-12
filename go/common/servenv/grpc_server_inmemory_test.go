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

package servenv

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

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// dialInMemory returns a client connection to an in-memory server.
func dialInMemory(t *testing.T, g *GrpcServer) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///in-memory",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return g.Dial(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestInMemoryGrpcServer_IsEnabledWithoutPort(t *testing.T) {
	external := NewGrpcServer(viperutil.NewRegistry())
	g := NewInMemoryGrpcServer(external)

	assert.Zero(t, g.Port())
	assert.True(t, g.IsEnabled(), "an in-memory server is enabled without a port, so services register on it")
	assert.False(t, external.IsEnabled(), "the external server has no port in this test")
}

func TestInMemoryGrpcServer_SharesExternalSettings(t *testing.T) {
	external := NewGrpcServer(viperutil.NewRegistry())
	external.keepaliveTime.Set(42 * time.Second)
	external.streamWorkers.Set(8)
	external.otelInstrumentation.Set(false)

	g := NewInMemoryGrpcServer(external)

	assert.Equal(t, 42*time.Second, g.keepaliveTime.Get())
	assert.EqualValues(t, 8, g.streamWorkers.Get())
	assert.False(t, g.OTelInstrumentationEnabled())
}

func TestInMemoryGrpcServer_IgnoresTLSAndAuth(t *testing.T) {
	reg := viperutil.NewRegistry()
	sv := NewServEnv(reg)
	// TLS files that do not exist: a server that tried to use them would fail
	// to create.
	sv.tlsCert.Set("/nonexistent/server.crt")
	sv.tlsKey.Set("/nonexistent/server.key")

	external := NewGrpcServer(reg)
	external.port.Set(12345)
	external.auth.Set("mtls")
	require.Error(t, external.Create(sv), "the external server uses the process's TLS files")

	g := NewInMemoryGrpcServer(external)
	require.NoError(t, g.Create(sv))
	require.NotNil(t, g.Server)
	assert.Nil(t, g.resolvedAuthPlugin(), "an in-memory server has no auth plugin")
}

func TestInMemoryGrpcServer_ServesThroughDial(t *testing.T) {
	sv := NewServEnv(viperutil.NewRegistry())
	g := NewInMemoryGrpcServer(NewGrpcServer(viperutil.NewRegistry()))
	require.NoError(t, g.Create(sv))
	require.NoError(t, g.Serve(sv))
	t.Cleanup(g.Server.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(dialInMemory(t, g)).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
}

func TestDial_RejectsTCPServer(t *testing.T) {
	g := NewGrpcServer(viperutil.NewRegistry())
	_, err := g.Dial(context.Background())
	require.Error(t, err)
}

// TestRun_ServesEveryGrpcServer runs a process with an external server (no
// port, so it is not served) and an in-memory server, and checks that a
// service registered by a run hook is served on the in-memory server and that
// shutdown stops it.
func TestRun_ServesEveryGrpcServer(t *testing.T) {
	reg := viperutil.NewRegistry()
	sv := NewServEnv(reg)
	sv.bindAddress.Set("localhost")
	require.NoError(t, sv.Init(ServiceIdentity{ServiceName: "test-run-servers"}))

	external := NewGrpcServer(reg)
	inMemory := NewInMemoryGrpcServer(external)

	// Register the way a component does: in a run hook, after Create and
	// before Serve.
	sv.OnRun(func() {
		multipoolerpb.RegisterMultipoolerServiceServer(inMemory.Server, &multipoolerpb.UnimplementedMultipoolerServiceServer{})
	})

	done := make(chan error, 1)
	go func() { done <- sv.Run(sv.bindAddress.Get(), 0, external, inMemory) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	health := healthpb.NewHealthClient(dialInMemory(t, inMemory))
	resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{Service: "multipoolerservice.MultipoolerService"})
	require.NoError(t, err, "the service registered by the run hook is served in memory")
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
	assert.Nil(t, external.Server, "the external server has no port, so it is not created")

	sv.InitiateShutdown()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	_, err = health.Check(stopCtx, &healthpb.HealthCheckRequest{})
	require.Error(t, err, "shutdown stops the in-memory server")
}
