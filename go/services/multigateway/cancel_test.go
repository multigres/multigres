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

package multigateway

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/pgprotocol/pid"
	"github.com/multigres/multigres/go/common/topoclient"
	multigatewayservicepb "github.com/multigres/multigres/go/pb/multigatewayservice"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

func testCancelLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestCancelManager_LocalCancel(t *testing.T) {
	ownPrefix := uint32(5)
	pid := pid.EncodePID(ownPrefix, 42)

	var canceledPID, canceledSecret uint32
	localCancelFn := func(p, s uint32) bool {
		canceledPID = p
		canceledSecret = s
		return true
	}

	cm := NewCancelManager(localCancelFn, ownPrefix, nil, testCancelLogger(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer cm.Close()

	cm.HandleCancelRequest(context.Background(), pid, 99999)

	assert.Equal(t, pid, canceledPID)
	assert.Equal(t, uint32(99999), canceledSecret)
}

func TestCancelManager_RemoteForward(t *testing.T) {
	ownPrefix := uint32(5)
	targetPrefix := uint32(10)
	pid := pid.EncodePID(targetPrefix, 42)

	localCalled := false
	localCancelFn := func(p, s uint32) bool {
		localCalled = true
		return false
	}

	// Create a mock topology store that returns a gateway with the target prefix.
	gw := topoclient.NewMultiGateway("gw-target", "zone-1", "gw-target.example.com")
	gw.PortMap["grpc"] = 15000
	gw.PidPrefix = targetPrefix
	mockTS := &mockTopoStore{
		cells: []string{"zone-1"},
		gatewaysByCell: map[string][]*topoclient.MultiGatewayInfo{
			"zone-1": {
				topoclient.NewMultiGatewayInfo(gw, nil),
			},
		},
	}

	cm := NewCancelManager(localCancelFn, ownPrefix, mockTS, testCancelLogger(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer cm.Close()

	// HandleCancelRequest should try to forward but will fail at the gRPC dial
	// since there's no real server. The important thing is it doesn't call local cancel.
	cm.HandleCancelRequest(context.Background(), pid, 99999)

	assert.False(t, localCalled, "should not call local cancel for remote prefix")
}

func TestCancelManager_GRPCHandler(t *testing.T) {
	ownPrefix := uint32(5)
	pid := pid.EncodePID(ownPrefix, 42)

	var canceledPID, canceledSecret uint32
	localCancelFn := func(p, s uint32) bool {
		canceledPID = p
		canceledSecret = s
		return true
	}

	cm := NewCancelManager(localCancelFn, ownPrefix, nil, testCancelLogger(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer cm.Close()

	resp, err := cm.CancelQuery(context.Background(), &multigatewayservicepb.CancelQueryRequest{
		ProcessId: pid,
		SecretKey: 12345,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, pid, canceledPID)
	assert.Equal(t, uint32(12345), canceledSecret)
}

type captureCancelServer struct {
	multigatewayservicepb.UnimplementedMultiGatewayServiceServer
	reqCh chan *multigatewayservicepb.CancelQueryRequest
}

func (s *captureCancelServer) CancelQuery(ctx context.Context, req *multigatewayservicepb.CancelQueryRequest) (*multigatewayservicepb.CancelQueryResponse, error) {
	select {
	case s.reqCh <- req:
	default:
	}
	return &multigatewayservicepb.CancelQueryResponse{}, nil
}

func startCancelGRPCServer(t *testing.T, opts ...grpc.ServerOption) (string, <-chan *multigatewayservicepb.CancelQueryRequest, func()) {
	t.Helper()

	reqCh := make(chan *multigatewayservicepb.CancelQueryRequest, 1)
	srv := grpc.NewServer(opts...)
	multigatewayservicepb.RegisterMultiGatewayServiceServer(srv, &captureCancelServer{reqCh: reqCh})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = srv.Serve(lis)
	}()

	cleanup := func() {
		srv.Stop()
		_ = lis.Close()
	}

	return lis.Addr().String(), reqCh, cleanup
}

func newMockTopoStoreForAddr(t *testing.T, prefix uint32, addr string) *mockTopoStore {
	t.Helper()

	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.ParseUint(portStr, 10, 16)
	require.NoError(t, err)

	gw := topoclient.NewMultiGateway("gw-target", "zone-1", host)
	gw.PortMap["grpc"] = int32(port)
	gw.PidPrefix = prefix

	return &mockTopoStore{
		cells: []string{"zone-1"},
		gatewaysByCell: map[string][]*topoclient.MultiGatewayInfo{
			"zone-1": {topoclient.NewMultiGatewayInfo(gw, nil)},
		},
	}
}

func generateTestGRPCTLSFiles(t *testing.T) (caCert, serverCert, serverKey string) {
	t.Helper()

	dir := t.TempDir()
	caCert = filepath.Join(dir, "ca.crt")
	caKey := filepath.Join(dir, "ca.key")
	serverCert = filepath.Join(dir, "server.crt")
	serverKey = filepath.Join(dir, "server.key")

	require.NoError(t, local.GenerateCA(caCert, caKey))
	require.NoError(t, local.GenerateCert(caCert, caKey, serverCert, serverKey, "localhost", []string{"localhost"}))

	return caCert, serverCert, serverKey
}

func TestCancelManager_ForwardCancel_TLS_Success(t *testing.T) {
	ownPrefix := uint32(5)
	targetPrefix := uint32(10)
	processID := pid.EncodePID(targetPrefix, 42)
	secretKey := uint32(99999)

	caCert, serverCert, serverKey := generateTestGRPCTLSFiles(t)

	serverTLS, err := grpccommon.BuildServerTLSConfig(serverCert, serverKey, "", "")
	require.NoError(t, err)
	addr, reqCh, cleanup := startCancelGRPCServer(t, grpc.Creds(credentials.NewTLS(serverTLS)))
	defer cleanup()

	clientTLS, err := grpccommon.BuildClientTLSConfig("", "", caCert, "localhost")
	require.NoError(t, err)

	cm := NewCancelManager(
		func(pid, secret uint32) bool { return false },
		ownPrefix,
		newMockTopoStoreForAddr(t, targetPrefix, addr),
		testCancelLogger(),
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
	)
	defer cm.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = cm.forwardCancel(ctx, targetPrefix, processID, secretKey)
	require.NoError(t, err)

	select {
	case req := <-reqCh:
		require.NotNil(t, req)
		assert.Equal(t, processID, req.ProcessId)
		assert.Equal(t, secretKey, req.SecretKey)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for forwarded cancel request")
	}
}

func TestCancelManager_ForwardCancel_TLS_FailureWithInsecureClient(t *testing.T) {
	ownPrefix := uint32(5)
	targetPrefix := uint32(10)
	processID := pid.EncodePID(targetPrefix, 42)

	_, serverCert, serverKey := generateTestGRPCTLSFiles(t)

	serverTLS, err := grpccommon.BuildServerTLSConfig(serverCert, serverKey, "", "")
	require.NoError(t, err)
	addr, reqCh, cleanup := startCancelGRPCServer(t, grpc.Creds(credentials.NewTLS(serverTLS)))
	defer cleanup()

	cm := NewCancelManager(
		func(pid, secret uint32) bool { return false },
		ownPrefix,
		newMockTopoStoreForAddr(t, targetPrefix, addr),
		testCancelLogger(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer cm.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = cm.forwardCancel(ctx, targetPrefix, processID, 99999)
	require.Error(t, err)

	select {
	case req := <-reqCh:
		t.Fatalf("unexpected forwarded cancel request with insecure client: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
}

// mockTopoStore implements the topology store methods needed by CancelManager.
type mockTopoStore struct {
	topoclient.Store // embed to satisfy the interface
	cells            []string
	gatewaysByCell   map[string][]*topoclient.MultiGatewayInfo
}

func (m *mockTopoStore) GetCellNames(ctx context.Context) ([]string, error) {
	return m.cells, nil
}

func (m *mockTopoStore) GetMultiGatewaysByCell(ctx context.Context, cellName string) ([]*topoclient.MultiGatewayInfo, error) {
	return m.gatewaysByCell[cellName], nil
}
