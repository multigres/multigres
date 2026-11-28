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

package servenv

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/multigres/multigres/go/tools/viperutil"
)

// newRunnableGRPCServerForTest returns a gRPC server on a free port and the ServEnv to run it with.
func newRunnableGRPCServerForTest(t *testing.T) (*GrpcServer, *ServEnv) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	reg := viperutil.NewRegistry()
	g := NewGrpcServer(reg)
	g.port.Set(port)
	g.bindAddress.Set("127.0.0.1")
	return g, NewServEnv(reg)
}

// reachable dials the gRPC port, then makes a health call, which only a serving server answers.
func reachable(t *testing.T, g *GrpcServer) error {
	t.Helper()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(g.Port()))
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return err
	}
	_ = conn.Close()

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return errors.New("not serving: " + resp.GetStatus().String())
	}
	return nil
}

func stopRun(t *testing.T, sv *ServEnv, done <-chan error) {
	t.Helper()
	sv.InitiateShutdown()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after the shutdown")
	}
}

// TestRun_ServingHooksFireOnceTheGrpcServerServes: the serving hooks are for what makes the
// process reachable to others, such as registering it in a topology. By then the gRPC port
// accepts and answers, which the run hooks, which fire before the server serves, cannot say.
func TestRun_ServingHooksFireOnceTheGrpcServerServes(t *testing.T) {
	g, sv := newRunnableGRPCServerForTest(t)

	var order []string
	var fired atomic.Int32
	reachableFromHook := make(chan error, 1)
	sv.OnRun(func() { order = append(order, "run") })
	sv.OnServing(func() {
		fired.Add(1)
		order = append(order, "serving")
		reachableFromHook <- reachable(t, g)
	})
	servingE := make(chan struct{}, 1)
	sv.OnServingE(func() error {
		servingE <- struct{}{}
		return nil
	})

	done := make(chan error, 1)
	go func() { done <- sv.Run("127.0.0.1", 0, g) }()

	select {
	case err := <-reachableFromHook:
		require.NoError(t, err, "a serving hook found the gRPC port unreachable")
	case <-time.After(10 * time.Second):
		t.Fatal("the serving hook never ran")
	}
	select {
	case <-servingE:
	case <-time.After(10 * time.Second):
		t.Fatal("the error-returning serving hook never ran")
	}
	stopRun(t, sv, done)
	require.Equal(t, []string{"run", "serving"}, order, "run hooks fire before serving hooks")
	require.EqualValues(t, 1, fired.Load(), "a serving hook fires once")
}

// TestRun_RunHooksFireBeforeTheGrpcServerServes pins what the serving hooks exist for: the gRPC
// port is not bound when the run hooks fire, so a dial from one is refused. The same dial from
// a serving hook succeeds (see above).
func TestRun_RunHooksFireBeforeTheGrpcServerServes(t *testing.T) {
	g, sv := newRunnableGRPCServerForTest(t)
	fromRunHook := make(chan error, 1)
	sv.OnRun(func() {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(g.Port())), time.Second)
		if conn != nil {
			_ = conn.Close()
		}
		fromRunHook <- err
	})
	done := make(chan error, 1)
	go func() { done <- sv.Run("127.0.0.1", 0, g) }()

	select {
	case err := <-fromRunHook:
		require.Error(t, err, "the gRPC port accepts during the run hooks: serving hooks are no longer needed")
	case <-time.After(10 * time.Second):
		t.Fatal("the run hook never ran")
	}
	stopRun(t, sv, done)
}

// TestRun_ServingHooksDoNotFireWhenTheServerCannotServe: a port in use stops startup, and the
// process was never reachable, so what would announce it does not run.
func TestRun_ServingHooksDoNotFireWhenTheServerCannotServe(t *testing.T) {
	g, sv := newRunnableGRPCServerForTest(t)
	busy, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(g.Port())))
	require.NoError(t, err)
	defer busy.Close()

	var fired atomic.Bool
	sv.OnServing(func() { fired.Store(true) })
	sv.OnServingE(func() error { fired.Store(true); return nil })
	err = sv.Run("127.0.0.1", 0, g)
	require.ErrorContains(t, err, "cannot listen on grpc port")
	require.False(t, fired.Load())
}

// TestRun_AServingHookErrorStopsRun: an error from a serving hook is returned by Run.
func TestRun_AServingHookErrorStopsRun(t *testing.T) {
	g, sv := newRunnableGRPCServerForTest(t)
	sv.OnServingE(func() error { return errors.New("cannot announce") })
	require.ErrorContains(t, sv.Run("127.0.0.1", 0, g), "serving hooks: cannot announce")
}
