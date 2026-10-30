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

// connect returns a client connection to an in-memory server.
func connect(t *testing.T, g *GrpcServer) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///in-memory", g.DialOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestNewInMemoryGrpcServer(t *testing.T) {
	external := NewGrpcServer(viperutil.NewRegistry())
	assert.Nil(t, external.DialOptions(), "a TCP server has no in-memory dial options")

	external.keepaliveTime.Set(42 * time.Second)
	external.streamWorkers.Set(8)
	external.otelInstrumentation.Set(false)
	g := NewInMemoryGrpcServer(external)

	assert.True(t, g.IsEnabled(), "enabled without a port, so services register on it")
	assert.False(t, external.IsEnabled())
	assert.NotEmpty(t, g.DialOptions())
	assert.Equal(t, 42*time.Second, g.keepaliveTime.Get(), "shares the external server's settings")
	assert.EqualValues(t, 8, g.streamWorkers.Get())
	assert.False(t, g.OTelInstrumentationEnabled())
}

func TestInMemoryGrpcServer_IgnoresTLSAndAuth(t *testing.T) {
	reg := viperutil.NewRegistry()
	sv := NewServEnv(reg)
	// Missing TLS files: a server that used them would fail to create.
	sv.tlsCert.Set("/nonexistent/server.crt")
	sv.tlsKey.Set("/nonexistent/server.key")

	external := NewGrpcServer(reg)
	external.port.Set(12345)
	external.auth.Set("mtls")
	require.Error(t, external.Create(sv), "the external server uses the process's TLS files")

	g := NewInMemoryGrpcServer(external)
	require.NoError(t, g.Create(sv))
	require.NotNil(t, g.Server)
	assert.Nil(t, g.resolvedAuthPlugin(), "the in-memory server has no auth plugin")
}

func TestInMemoryGrpcServer_Serves(t *testing.T) {
	sv := NewServEnv(viperutil.NewRegistry())
	g := NewInMemoryGrpcServer(NewGrpcServer(viperutil.NewRegistry()))
	require.NoError(t, g.Create(sv))
	require.NoError(t, g.Serve(sv))
	t.Cleanup(g.Server.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(connect(t, g)).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
}

// TestRun_ServesEveryGrpcServer checks that Run serves a service registered by
// a run hook on the in-memory server, and stops it at shutdown.
func TestRun_ServesEveryGrpcServer(t *testing.T) {
	reg := viperutil.NewRegistry()
	sv := NewServEnv(reg)
	sv.bindAddress.Set("localhost")
	require.NoError(t, sv.Init(ServiceIdentity{ServiceName: "test-run-servers"}))

	external := NewGrpcServer(reg)
	inMemory := NewInMemoryGrpcServer(external)

	// As a component does: in a run hook, after Create and before Serve.
	sv.OnRun(func() {
		multipoolerpb.RegisterMultipoolerServiceServer(inMemory.Server, &multipoolerpb.UnimplementedMultipoolerServiceServer{})
	})

	done := make(chan error, 1)
	go func() { done <- sv.Run(sv.bindAddress.Get(), 0, external, inMemory) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	health := healthpb.NewHealthClient(connect(t, inMemory))
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
