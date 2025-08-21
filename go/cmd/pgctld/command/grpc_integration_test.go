// Copyright 2025 The Multigres Authors.
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

package command

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// TestGRPCServerIntegration tests the gRPC server with mock PostgreSQL
func TestGRPCServerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping gRPC integration tests in short mode")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_grpc_integration_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err := os.MkdirAll(binDir, 0755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Create and start gRPC server
	grpcServer, lis := createTestGRPCServer(t, dataDir, binDir)
	defer grpcServer.Stop()

	// Connect to the gRPC server
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)

	t.Run("complete_grpc_lifecycle", func(t *testing.T) {
		ctx := context.Background()

		// Step 1: Check initial status
		statusResp, err := client.Status(ctx, &pb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, pb.ServerStatus_NOT_INITIALIZED, statusResp.GetStatus())

		// Step 2: Initialize data directory
		_, err = client.InitDataDir(ctx, &pb.InitDataDirRequest{DataDir: dataDir})
		require.NoError(t, err)

		// Step 3: Start PostgreSQL
		startResp, err := client.Start(ctx, &pb.StartRequest{})
		require.NoError(t, err)
		assert.NotEmpty(t, startResp.Message)

		// Step 4: Check status - should be running
		statusResp, err = client.Status(ctx, &pb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, pb.ServerStatus_RUNNING, statusResp.GetStatus())
		assert.NotZero(t, statusResp.GetPid())

		// Step 5: Get version
		versionResp, err := client.Version(ctx, &pb.VersionRequest{})
		require.NoError(t, err)
		assert.NotEmpty(t, versionResp.Version)

		// Step 6: Reload config
		reloadResp, err := client.ReloadConfig(ctx, &pb.ReloadConfigRequest{})
		require.NoError(t, err)
		assert.NotEmpty(t, reloadResp.Message)

		// Step 7: Restart
		restartResp, err := client.Restart(ctx, &pb.RestartRequest{})
		require.NoError(t, err)
		assert.NotEmpty(t, restartResp.Message)

		// Step 8: Check status again
		statusResp, err = client.Status(ctx, &pb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, pb.ServerStatus_RUNNING, statusResp.GetStatus())

		// Step 9: Stop PostgreSQL
		stopResp, err := client.Stop(ctx, &pb.StopRequest{Mode: "fast"})
		require.NoError(t, err)
		assert.NotEmpty(t, stopResp.Message)

		// Step 10: Final status check
		statusResp, err = client.Status(ctx, &pb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, pb.ServerStatus_STOPPED, statusResp.GetStatus())
	})
}

// TestGRPCErrorHandling tests error scenarios in gRPC
func TestGRPCErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping gRPC integration tests in short mode")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_grpc_error_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err := os.MkdirAll(binDir, 0755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Create and start gRPC server
	grpcServer, lis := createTestGRPCServer(t, dataDir, binDir)
	defer grpcServer.Stop()

	// Connect to the gRPC server
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)

	t.Run("start_already_running", func(t *testing.T) {
		ctx := context.Background()

		// Initialize and start first
		_, err := client.InitDataDir(ctx, &pb.InitDataDirRequest{DataDir: dataDir})
		require.NoError(t, err)

		_, err = client.Start(ctx, &pb.StartRequest{})
		require.NoError(t, err)

		// Try to start again - should handle gracefully
		startResp, err := client.Start(ctx, &pb.StartRequest{})
		if err != nil {
			// Error is acceptable
			t.Logf("Expected error on duplicate start: %v", err)
		} else {
			// Or success with appropriate message
			assert.Contains(t, startResp.Message, "already")
		}

		// Clean up
		_, _ = client.Stop(ctx, &pb.StopRequest{Mode: "fast"})
	})

	t.Run("stop_not_running", func(t *testing.T) {
		ctx := context.Background()

		// Try to stop when not running
		stopResp, err := client.Stop(ctx, &pb.StopRequest{Mode: "fast"})
		if err != nil {
			// Error is acceptable
			t.Logf("Expected error on stop when not running: %v", err)
		} else {
			// Or success with appropriate message
			assert.NotEmpty(t, stopResp.Message)
		}
	})

	t.Run("reload_when_not_running", func(t *testing.T) {
		ctx := context.Background()

		// Try to reload when not running
		reloadResp, err := client.ReloadConfig(ctx, &pb.ReloadConfigRequest{})
		if err != nil {
			// Error is acceptable
			t.Logf("Expected error on reload when not running: %v", err)
		} else {
			// Or success with appropriate message
			assert.NotEmpty(t, reloadResp.Message)
		}
	})
}

// TestGRPCConcurrentRequests tests handling of concurrent gRPC requests
func TestGRPCConcurrentRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping gRPC integration tests in short mode")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_grpc_concurrent_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err := os.MkdirAll(binDir, 0755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Create and start gRPC server
	grpcServer, lis := createTestGRPCServer(t, dataDir, binDir)
	defer grpcServer.Stop()

	// Connect to the gRPC server
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)
	ctx := context.Background()

	// Initialize and start PostgreSQL first
	_, err = client.InitDataDir(ctx, &pb.InitDataDirRequest{DataDir: dataDir})
	require.NoError(t, err)

	_, err = client.Start(ctx, &pb.StartRequest{})
	require.NoError(t, err)

	defer func() {
		_, _ = client.Stop(ctx, &pb.StopRequest{Mode: "fast"})
	}()

	t.Run("concurrent_status_requests", func(t *testing.T) {
		const numRequests = 10
		results := make(chan error, numRequests)

		// Send multiple concurrent status requests
		for i := 0; i < numRequests; i++ {
			go func() {
				_, err := client.Status(ctx, &pb.StatusRequest{})
				results <- err
			}()
		}

		// Collect results
		for i := 0; i < numRequests; i++ {
			err := <-results
			assert.NoError(t, err, "Concurrent status request failed")
		}
	})

	t.Run("concurrent_version_requests", func(t *testing.T) {
		const numRequests = 5
		results := make(chan error, numRequests)

		// Send multiple concurrent version requests
		for i := 0; i < numRequests; i++ {
			go func() {
				_, err := client.Version(ctx, &pb.VersionRequest{})
				results <- err
			}()
		}

		// Collect results
		for i := 0; i < numRequests; i++ {
			err := <-results
			assert.NoError(t, err, "Concurrent version request failed")
		}
	})
}

// TestGRPCWithDifferentConfigurations tests gRPC with various configurations
func TestGRPCWithDifferentConfigurations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping gRPC integration tests in short mode")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_grpc_config_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err := os.MkdirAll(binDir, 0755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	t.Run("different_stop_modes", func(t *testing.T) {
		// Create and start gRPC server
		grpcServer, lis := createTestGRPCServer(t, dataDir, binDir)
		defer grpcServer.Stop()

		// Connect to the gRPC server
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()

		client := pb.NewPgCtldClient(conn)
		ctx := context.Background()

		stopModes := []string{"smart", "fast", "immediate"}

		for _, mode := range stopModes {
			t.Run(fmt.Sprintf("stop_mode_%s", mode), func(t *testing.T) {
				// Initialize and start
				_, err := client.InitDataDir(ctx, &pb.InitDataDirRequest{DataDir: dataDir})
				require.NoError(t, err)

				_, err = client.Start(ctx, &pb.StartRequest{})
				require.NoError(t, err)

				// Stop with specific mode
				stopResp, err := client.Stop(ctx, &pb.StopRequest{Mode: mode})
				require.NoError(t, err)
				assert.NotEmpty(t, stopResp.Message)

				// Verify stopped
				statusResp, err := client.Status(ctx, &pb.StatusRequest{})
				require.NoError(t, err)
				assert.Equal(t, pb.ServerStatus_STOPPED, statusResp.GetStatus())
			})
		}
	})
}

// createTestGRPCServer creates and starts a gRPC server for testing
func createTestGRPCServer(t *testing.T, dataDir, binDir string) (*grpc.Server, net.Listener) {
	t.Helper()

	// Find a free port
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create the pgctld service with mock environment
	service := &PgCtldService{
		logger: slog.Default(),
	}

	// Set environment variables for the service
	t.Setenv("PGDATA", dataDir)
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	// Configure viper for the test
	viper.Set("data-dir", dataDir)
	viper.Set("pg-port", 5432)
	viper.Set("pg-host", "localhost")
	viper.Set("pg-user", "postgres")
	viper.Set("pg-database", "postgres")

	// Register the service
	pb.RegisterPgCtldServer(grpcServer, service)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("gRPC server error: %v", err)
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	return grpcServer, lis
}
