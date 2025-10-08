// Copyright 2025 Supabase, Inc.
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

package testutil

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// MockPgCtldService implements a mock version of the PgCtld gRPC service for testing
type MockPgCtldService struct {
	pb.UnimplementedPgCtldServer
	StartCalls   []*pb.StartRequest
	StopCalls    []*pb.StopRequest
	RestartCalls []*pb.RestartRequest
	ReloadCalls  []*pb.ReloadConfigRequest
	StatusCalls  []*pb.StatusRequest
	VersionCalls []*pb.VersionRequest
	InitDirCalls []*pb.InitDataDirRequest
	GetTermCalls []*pb.GetTermRequest
	SetTermCalls []*pb.SetTermRequest

	// Response configurations
	StartResponse   *pb.StartResponse
	StopResponse    *pb.StopResponse
	RestartResponse *pb.RestartResponse
	ReloadResponse  *pb.ReloadConfigResponse
	StatusResponse  *pb.StatusResponse
	VersionResponse *pb.VersionResponse
	InitDirResponse *pb.InitDataDirResponse
	GetTermResponse *pb.GetTermResponse
	SetTermResponse *pb.SetTermResponse

	// Error configurations
	StartError   error
	StopError    error
	RestartError error
	ReloadError  error
	StatusError  error
	VersionError error
	InitDirError error
	GetTermError error
	SetTermError error

	// Mutable state for consensus term
	ConsensusTerm *pb.ConsensusTerm
}

func (m *MockPgCtldService) Start(ctx context.Context, req *pb.StartRequest) (*pb.StartResponse, error) {
	m.StartCalls = append(m.StartCalls, req)
	if m.StartError != nil {
		return nil, m.StartError
	}
	if m.StartResponse != nil {
		return m.StartResponse, nil
	}
	return &pb.StartResponse{Pid: 12345, Message: "Mock server started"}, nil
}

func (m *MockPgCtldService) Stop(ctx context.Context, req *pb.StopRequest) (*pb.StopResponse, error) {
	m.StopCalls = append(m.StopCalls, req)
	if m.StopError != nil {
		return nil, m.StopError
	}
	if m.StopResponse != nil {
		return m.StopResponse, nil
	}
	return &pb.StopResponse{Message: "Mock server stopped"}, nil
}

func (m *MockPgCtldService) Restart(ctx context.Context, req *pb.RestartRequest) (*pb.RestartResponse, error) {
	m.RestartCalls = append(m.RestartCalls, req)
	if m.RestartError != nil {
		return nil, m.RestartError
	}
	if m.RestartResponse != nil {
		return m.RestartResponse, nil
	}
	return &pb.RestartResponse{Pid: 12346, Message: "Mock server restarted"}, nil
}

func (m *MockPgCtldService) ReloadConfig(ctx context.Context, req *pb.ReloadConfigRequest) (*pb.ReloadConfigResponse, error) {
	m.ReloadCalls = append(m.ReloadCalls, req)
	if m.ReloadError != nil {
		return nil, m.ReloadError
	}
	if m.ReloadResponse != nil {
		return m.ReloadResponse, nil
	}
	return &pb.ReloadConfigResponse{Message: "Mock config reloaded"}, nil
}

func (m *MockPgCtldService) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	m.StatusCalls = append(m.StatusCalls, req)
	if m.StatusError != nil {
		return nil, m.StatusError
	}
	if m.StatusResponse != nil {
		return m.StatusResponse, nil
	}
	return &pb.StatusResponse{
		Status:  pb.ServerStatus_RUNNING,
		Pid:     12345,
		Version: "PostgreSQL 15.0",
		DataDir: "/tmp/test",
		Port:    5432,
		Host:    "localhost",
		Ready:   true,
		Message: "Mock server running",
	}, nil
}

func (m *MockPgCtldService) Version(ctx context.Context, req *pb.VersionRequest) (*pb.VersionResponse, error) {
	m.VersionCalls = append(m.VersionCalls, req)
	if m.VersionError != nil {
		return nil, m.VersionError
	}
	if m.VersionResponse != nil {
		return m.VersionResponse, nil
	}
	return &pb.VersionResponse{Version: "PostgreSQL 15.0", Message: "Mock version"}, nil
}

func (m *MockPgCtldService) InitDataDir(ctx context.Context, req *pb.InitDataDirRequest) (*pb.InitDataDirResponse, error) {
	m.InitDirCalls = append(m.InitDirCalls, req)
	if m.InitDirError != nil {
		return nil, m.InitDirError
	}
	if m.InitDirResponse != nil {
		return m.InitDirResponse, nil
	}
	return &pb.InitDataDirResponse{Message: "Mock data directory initialized"}, nil
}

func (m *MockPgCtldService) GetTerm(ctx context.Context, req *pb.GetTermRequest) (*pb.GetTermResponse, error) {
	m.GetTermCalls = append(m.GetTermCalls, req)
	if m.GetTermError != nil {
		return nil, m.GetTermError
	}
	if m.GetTermResponse != nil {
		return m.GetTermResponse, nil
	}
	// Return the mutable state if set, otherwise return a default term
	if m.ConsensusTerm != nil {
		return &pb.GetTermResponse{Term: m.ConsensusTerm}, nil
	}
	return &pb.GetTermResponse{
		Term: &pb.ConsensusTerm{
			CurrentTerm: 1,
		},
	}, nil
}

func (m *MockPgCtldService) SetTerm(ctx context.Context, req *pb.SetTermRequest) (*pb.SetTermResponse, error) {
	m.SetTermCalls = append(m.SetTermCalls, req)
	if m.SetTermError != nil {
		return nil, m.SetTermError
	}
	if m.SetTermResponse != nil {
		return m.SetTermResponse, nil
	}
	// Update the mutable state
	m.ConsensusTerm = req.Term
	return &pb.SetTermResponse{}, nil
}

// TestGRPCServer provides utilities for testing gRPC services
type TestGRPCServer struct {
	server   *grpc.Server
	listener *bufconn.Listener
	address  string
}

// NewTestGRPCServer creates a new test gRPC server with bufconn
func NewTestGRPCServer(t *testing.T) *TestGRPCServer {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()

	return &TestGRPCServer{
		server:   server,
		listener: listener,
		address:  "bufnet",
	}
}

// RegisterService registers a service with the test server
func (ts *TestGRPCServer) RegisterService(service pb.PgCtldServer) {
	pb.RegisterPgCtldServer(ts.server, service)
}

// Start starts the test gRPC server
func (ts *TestGRPCServer) Start(t *testing.T) {
	t.Helper()

	go func() {
		if err := ts.server.Serve(ts.listener); err != nil {
			t.Logf("gRPC server error: %v", err)
		}
	}()
}

// Stop stops the test gRPC server
func (ts *TestGRPCServer) Stop() {
	ts.server.Stop()
	ts.listener.Close()
}

// NewClient creates a new gRPC client connected to the test server
func (ts *TestGRPCServer) NewClient(t *testing.T) pb.PgCtldClient {
	t.Helper()

	dialer := func(context.Context, string) (net.Conn, error) {
		return ts.listener.Dial()
	}

	conn, err := grpc.NewClient(
		ts.address,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create gRPC client: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	return pb.NewPgCtldClient(conn)
}

// StartTestServer starts a real gRPC server on a random port for integration testing
func StartTestServer(t *testing.T, service pb.PgCtldServer) (pb.PgCtldClient, func()) {
	t.Helper()

	// Find an available port
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	address := listener.Addr().String()

	// Create gRPC server
	server := grpc.NewServer()
	pb.RegisterPgCtldServer(server, service)

	// Start server in background
	go func() {
		if err := server.Serve(listener); err != nil {
			slog.Error("Test gRPC server failed", "error", err)
		}
	}()

	// Create client
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to connect to test server: %v", err)
	}

	client := pb.NewPgCtldClient(conn)

	// Wait for server to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Test server did not become ready in time")
		default:
			if _, err := client.Status(ctx, &pb.StatusRequest{}); err == nil {
				goto ready
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
ready:

	cleanup := func() {
		conn.Close()
		server.Stop()
	}

	return client, cleanup
}

// StartMockPgctldServer starts a mock pgctld server with consensus term support
// Returns the server address and a cleanup function
func StartMockPgctldServer(t *testing.T) (string, func()) {
	return StartMockPgctldServerWithTerm(t, 1)
}

// StartMockPgctldServerWithTerm starts a mock pgctld server with a specific initial consensus term
// Returns the server address and a cleanup function
func StartMockPgctldServerWithTerm(t *testing.T, initialTerm int64) (string, func()) {
	t.Helper()

	// Create a listener on a random port
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	// Create gRPC server with mock service
	grpcServer := grpc.NewServer()
	mockService := &MockPgCtldService{
		ConsensusTerm: &pb.ConsensusTerm{
			CurrentTerm: initialTerm,
		},
	}
	pb.RegisterPgCtldServer(grpcServer, mockService)

	// Start serving in background
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	addr := lis.Addr().String()
	t.Logf("Mock pgctld server started at %s with term %d", addr, initialTerm)

	cleanup := func() {
		grpcServer.Stop()
		lis.Close()
	}

	return addr, cleanup
}
