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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// connect returns a client connection to the server listening on endpoint.
func connect(t *testing.T, endpoint Endpoint) *grpc.ClientConn {
	t.Helper()
	target, opts := endpoint.Target()
	conn, err := grpc.NewClient(target, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestTCPEndpoint_ReadsFlagsWhenUsed(t *testing.T) {
	g := NewGrpcServer(viperutil.NewRegistry())
	assert.False(t, g.Endpoint().Local())

	// Set after construction, as flag parsing would.
	g.bindAddress.Set("127.0.0.1")
	g.port.Set(15999)
	target, opts := g.Endpoint().Target()
	assert.Equal(t, "127.0.0.1:15999", target)
	assert.Empty(t, opts, "a network endpoint leaves transport credentials to the client")
}

func TestInMemoryEndpoint_IsLocal(t *testing.T) {
	e := NewInMemoryEndpoint()
	assert.True(t, e.Local())
	target, opts := e.Target()
	assert.Equal(t, inMemoryTarget, target)
	assert.NotEmpty(t, opts, "a client reaches the in-memory listener through the endpoint's dialer")
}

func TestNewLocalGrpcServer_RejectsNetworkEndpoint(t *testing.T) {
	external := NewGrpcServer(viperutil.NewRegistry())
	assert.Panics(t, func() { NewLocalGrpcServer(external, external.Endpoint()) })
}

func TestLocalGrpcServer_IsEnabledWithoutPort(t *testing.T) {
	external := NewGrpcServer(viperutil.NewRegistry())
	g := NewLocalGrpcServer(external, NewInMemoryEndpoint())

	assert.Zero(t, g.Port())
	assert.True(t, g.IsEnabled(), "a server on a local endpoint is enabled without a port, so services register on it")
	assert.False(t, external.IsEnabled(), "the external server has no port in this test")
}

func TestLocalGrpcServer_SharesExternalSettings(t *testing.T) {
	external := NewGrpcServer(viperutil.NewRegistry())
	external.keepaliveTime.Set(42 * time.Second)
	external.streamWorkers.Set(8)
	external.otelInstrumentation.Set(false)

	g := NewLocalGrpcServer(external, NewInMemoryEndpoint())

	assert.Equal(t, 42*time.Second, g.keepaliveTime.Get())
	assert.EqualValues(t, 8, g.streamWorkers.Get())
	assert.False(t, g.OTelInstrumentationEnabled())
}

func TestLocalGrpcServer_IgnoresTLSAndAuth(t *testing.T) {
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

	g := NewLocalGrpcServer(external, NewInMemoryEndpoint())
	require.NoError(t, g.Create(sv))
	require.NotNil(t, g.Server)
	assert.Nil(t, g.resolvedAuthPlugin(), "a server on a local endpoint has no auth plugin")
}

func TestLocalGrpcServer_ServesOnItsEndpoint(t *testing.T) {
	sv := NewServEnv(viperutil.NewRegistry())
	g := NewLocalGrpcServer(NewGrpcServer(viperutil.NewRegistry()), NewInMemoryEndpoint())
	require.NoError(t, g.Create(sv))
	require.NoError(t, g.Serve(sv))
	t.Cleanup(g.Server.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(connect(t, g.Endpoint())).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
}

// TestRun_ServesEveryGrpcServer runs a process with an external server (no
// port, so it is not served) and a server on an in-memory endpoint, and checks
// that a service registered by a run hook is served on the in-memory server
// and that shutdown stops it.
func TestRun_ServesEveryGrpcServer(t *testing.T) {
	reg := viperutil.NewRegistry()
	sv := NewServEnv(reg)
	sv.bindAddress.Set("localhost")
	require.NoError(t, sv.Init(ServiceIdentity{ServiceName: "test-run-servers"}))

	external := NewGrpcServer(reg)
	local := NewLocalGrpcServer(external, NewInMemoryEndpoint())

	// Register the way a component does: in a run hook, after Create and
	// before Serve.
	sv.OnRun(func() {
		multipoolerpb.RegisterMultipoolerServiceServer(local.Server, &multipoolerpb.UnimplementedMultipoolerServiceServer{})
	})

	done := make(chan error, 1)
	go func() { done <- sv.Run(sv.bindAddress.Get(), 0, external, local) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	health := healthpb.NewHealthClient(connect(t, local.Endpoint()))
	resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{Service: "multipoolerservice.MultipoolerService"})
	require.NoError(t, err, "the service registered by the run hook is served on the local server")
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
	require.Error(t, err, "shutdown stops the local server")
}
