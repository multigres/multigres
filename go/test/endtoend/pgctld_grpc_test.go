// Copyright 2025 Supabase, Inc.
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

package endtoend

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/cmd/pgctld/command"
	"github.com/multigres/multigres/go/tools/grpccommon"

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
	err := os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Create and start gRPC server
	lis, cleanupServer := createTestGRPCServer(t, dataDir, binDir)
	defer cleanupServer()

	// Connect to the gRPC server
	conn, err := grpc.NewClient(lis.Addr().String(), grpccommon.LocalClientDialOptions()...)
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
		_, err = client.InitDataDir(ctx, &pb.InitDataDirRequest{})
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
	err := os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Create and start gRPC server
	lis, cleanupServer := createTestGRPCServer(t, dataDir, binDir)
	defer cleanupServer()

	// Connect to the gRPC server
	conn, err := grpc.NewClient(lis.Addr().String(), grpccommon.LocalClientDialOptions()...)
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)

	t.Run("start_already_running", func(t *testing.T) {
		ctx := context.Background()

		// Initialize and start first
		_, err := client.InitDataDir(ctx, &pb.InitDataDirRequest{})
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
	err := os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Create and start gRPC server
	lis, cleanupServer := createTestGRPCServer(t, dataDir, binDir)
	defer cleanupServer()

	// Connect to the gRPC server
	conn, err := grpc.NewClient(lis.Addr().String(), grpccommon.LocalClientDialOptions()...)
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)
	ctx := context.Background()

	// Initialize and start PostgreSQL first
	err = InitAndStartPostgreSQL(t, lis.Addr().String())
	require.NoError(t, err)

	defer func() {
		_, _ = client.Stop(ctx, &pb.StopRequest{Mode: "fast"})
	}()

	t.Run("concurrent_status_requests", func(t *testing.T) {
		const numRequests = 10
		results := make(chan error, numRequests)

		// Send multiple concurrent status requests
		for range numRequests {
			go func() {
				_, err := client.Status(ctx, &pb.StatusRequest{})
				results <- err
			}()
		}

		// Collect results
		for range numRequests {
			err := <-results
			assert.NoError(t, err, "Concurrent status request failed")
		}
	})

	t.Run("concurrent_version_requests", func(t *testing.T) {
		const numRequests = 5
		results := make(chan error, numRequests)

		// Send multiple concurrent version requests
		for range numRequests {
			go func() {
				_, err := client.Version(ctx, &pb.VersionRequest{})
				results <- err
			}()
		}

		// Collect results
		for range numRequests {
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
	err := os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	t.Run("different_stop_modes", func(t *testing.T) {
		// Create and start gRPC server
		lis, cleanupServer := createTestGRPCServer(t, dataDir, binDir)
		defer cleanupServer()

		// Connect to the gRPC server
		conn, err := grpc.NewClient(lis.Addr().String(), grpccommon.LocalClientDialOptions()...)
		require.NoError(t, err)
		defer conn.Close()

		client := pb.NewPgCtldClient(conn)
		ctx := context.Background()

		stopModes := []string{"smart", "fast", "immediate"}

		for _, mode := range stopModes {
			t.Run("stop_mode_"+mode, func(t *testing.T) {
				// Initialize and start
				err := InitAndStartPostgreSQL(t, lis.Addr().String())
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

// TestGRPCUninitializedDatabase tests gRPC behavior with uninitialized database
func TestGRPCUninitializedDatabase(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping gRPC integration tests in short mode")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_grpc_uninitialized_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err := os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Create and start gRPC server
	lis, cleanupServer := createTestGRPCServer(t, dataDir, binDir)
	defer cleanupServer()

	// Connect to the gRPC server
	conn, err := grpc.NewClient(lis.Addr().String(), grpccommon.LocalClientDialOptions()...)
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)

	t.Run("uninitialized_database_operations", func(t *testing.T) {
		ctx := context.Background()

		// Step 1: Check initial status - should be NOT_INITIALIZED
		statusResp, err := client.Status(ctx, &pb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, pb.ServerStatus_NOT_INITIALIZED, statusResp.GetStatus())
		assert.Equal(t, "Data directory is not initialized", statusResp.GetMessage())

		// Step 2: Try to start without initialization - should fail with proper error
		_, err = client.Start(ctx, &pb.StartRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data directory not initialized")
		assert.Contains(t, err.Error(), "Run 'pgctld init' first")

		// Step 3: Try to stop without initialization - should return appropriate message
		stopResp, err := client.Stop(ctx, &pb.StopRequest{Mode: "fast"})
		if err != nil {
			// If it errors, should be about not being initialized or not running
			assert.Contains(t, err.Error(), "not initialized")
		} else {
			// If it succeeds, should indicate it's not running
			assert.Contains(t, stopResp.GetMessage(), "not running")
		}

		// Step 4: Try restart without initialization - should fail
		_, err = client.Restart(ctx, &pb.RestartRequest{Mode: "fast"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data directory not initialized")

		// Step 5: Try reload config without initialization - should fail
		_, err = client.ReloadConfig(ctx, &pb.ReloadConfigRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not initialized")

		// Step 6: Initialize should work
		initResp, err := client.InitDataDir(ctx, &pb.InitDataDirRequest{})
		require.NoError(t, err)
		assert.Contains(t, initResp.GetMessage(), "initialized successfully")

		// Step 7: Status should now show STOPPED
		statusResp, err = client.Status(ctx, &pb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, pb.ServerStatus_STOPPED, statusResp.GetStatus())
	})
}

// TestGRPCPortableConfig tests that configs are portable across different ports
func TestGRPCPortableConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping gRPC integration tests in short mode")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_grpc_portable_config_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err := os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	t.Run("portable_config_with_different_ports", func(t *testing.T) {
		// First, create a service with port 5432 and initialize
		initialPort := 5432
		service, err := command.NewPgCtldService(
			slog.Default(),
			initialPort,
			"postgres",
			"postgres",
			30,
			dataDir,
			"localhost",
		)
		require.NoError(t, err)

		// Set environment variables for the initialization
		t.Setenv("PGDATA", dataDir)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		// Initialize the data directory with the initial port
		ctx := context.Background()
		_, err = service.InitDataDir(ctx, &pb.InitDataDirRequest{})
		require.NoError(t, err)

		// Now try to create a service with a different port (5433)
		// This should succeed because port is now passed as a CLI parameter, making configs portable
		differentPort := 5433
		service2, err := command.NewPgCtldService(
			slog.Default(),
			differentPort,
			"postgres",
			"postgres",
			30,
			dataDir,
			"localhost",
		)
		require.NoError(t, err)
		assert.NotNil(t, service2, "Should be able to create service with different port for portability")
	})
}

// createTestGRPCServer creates and starts a gRPC server for testing
// Returns the server, listener, and a cleanup function
func createTestGRPCServer(t *testing.T, dataDir, binDir string) (net.Listener, func()) {
	t.Helper()

	// Find a free port
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create the pgctld service with mock environment
	service, err := command.NewPgCtldService(
		slog.Default(),
		5432,
		"postgres",
		"postgres",
		30,
		dataDir,
		"localhost",
	)

	require.NoError(t, err)
	// Set environment variables for the service
	t.Setenv("PGDATA", dataDir)
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

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

	// Return cleanup function that stops server and cleans up global variables
	cleanup := func() {
		grpcServer.Stop()
	}

	return lis, cleanup
}
