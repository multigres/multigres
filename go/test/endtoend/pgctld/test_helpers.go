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

package pgctld

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/cmd/pgctld/command"
	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

// TestSetup holds all configuration for pgBackRest server tests
type TestSetup struct {
	TempDir        string
	DataDir        string
	CertDir        string
	PgPort         int
	PgBackRestPort int
	BinDir         string
}

// setupPgBackRestTest creates a complete test environment for pgBackRest server tests
// Returns TestSetup with all paths and ports configured
func setupPgBackRestTest(t *testing.T) *TestSetup {
	t.Helper()

	// Create temp directory
	tempDir, cleanup := testutil.TempDir(t, "pgbackrest_server_test")
	t.Cleanup(cleanup)

	dataDir := filepath.Join(tempDir, "data")
	certDir := filepath.Join(tempDir, "certs")

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err := os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Set PATH for PostgreSQL binaries
	t.Setenv("PGDATA", dataDir)
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	// Generate TLS certificates
	certPaths, err := local.GeneratePgBackRestCerts(certDir)
	require.NoError(t, err, "Failed to generate pgBackRest certificates")
	t.Logf("Generated certificates in %s", certDir)

	// Ensure cert files exist
	require.FileExists(t, certPaths.CACertFile)
	require.FileExists(t, certPaths.ServerCertFile)
	require.FileExists(t, certPaths.ServerKeyFile)

	// Allocate dynamic ports (port 0 = let OS choose)
	pgPort := 15432 // PostgreSQL needs a specific port for mock binaries

	// Find a free port for pgBackRest
	pgbackrestLis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err, "Failed to allocate pgBackRest port")
	pgbackrestPort := pgbackrestLis.Addr().(*net.TCPAddr).Port
	pgbackrestLis.Close() // Close immediately - pgctld will bind to it

	return &TestSetup{
		TempDir:        tempDir,
		DataDir:        dataDir,
		CertDir:        certDir,
		PgPort:         pgPort,
		PgBackRestPort: pgbackrestPort,
		BinDir:         binDir,
	}
}

// verifyServerRunning polls socket connection until success or timeout
// Returns true if server responds, false if timeout reached
func verifyServerRunning(t *testing.T, port int, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 3*time.Second)
		if err == nil {
			conn.Close()
			return true
		}

		select {
		case <-ticker.C:
			// Continue polling
		case <-time.After(time.Until(deadline)):
			return false
		}
	}

	return false
}

// verifyServerStopped verifies socket connection fails (server not listening)
// Returns true if port is closed, false if still accepting connections
func verifyServerStopped(t *testing.T, port int) bool {
	t.Helper()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 1*time.Second)
	if err != nil {
		// Connection failed - server is stopped
		return true
	}

	// Connection succeeded - server still running
	conn.Close()
	return false
}

// getPgBackRestPID finds the pgbackrest server process ID for a specific config path
// Returns PID or 0 if not found
func getPgBackRestPID(t *testing.T, configPath string) int {
	t.Helper()

	// Get all pgbackrest server processes
	cmd := exec.Command("pgrep", "-f", "pgbackrest server")
	output, err := cmd.Output()
	if err != nil {
		// Process not found
		return 0
	}

	pidStr := strings.TrimSpace(string(output))
	if pidStr == "" {
		return 0
	}

	// Check each PID to find one using our specific config
	for pidLine := range strings.SplitSeq(pidStr, "\n") {
		var pid int
		_, err = fmt.Sscanf(pidLine, "%d", &pid)
		if err != nil {
			continue
		}

		// Check if this process is using our config by checking environment
		envCmd := exec.Command("sh", "-c", fmt.Sprintf("ps eww -p %d | grep -o 'PGBACKREST_CONFIG=[^[:space:]]*'", pid))
		envOutput, err := envCmd.Output()
		if err != nil {
			continue
		}

		envStr := strings.TrimSpace(string(envOutput))
		if strings.Contains(envStr, configPath) {
			return pid
		}
	}

	return 0
}

// killProcess sends SIGKILL to process by PID
func killProcess(t *testing.T, pid int) {
	t.Helper()

	require.Greater(t, pid, 0, "PID must be positive")

	// Send SIGKILL
	err := syscall.Kill(pid, syscall.SIGKILL)
	require.NoError(t, err, "Failed to kill process %d", pid)

	t.Logf("Killed process with PID %d", pid)
}

// createTestGRPCServerWithPgBackRest creates and starts a gRPC server with pgBackRest support
// Returns the listener and a cleanup function
func createTestGRPCServerWithPgBackRest(t *testing.T, setup *TestSetup) (net.Listener, func()) {
	t.Helper()

	// Find a free port
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create the pgctld service with pgBackRest configuration
	service, err := command.NewPgCtldService(
		slog.Default(),
		setup.PgPort,
		"postgres",
		"postgres",
		30,
		setup.DataDir,
		"localhost",
		setup.PgBackRestPort,
		setup.CertDir,
	)
	require.NoError(t, err)

	// Start pgBackRest management (runs in background goroutine)
	service.StartPgBackRestManagement()

	// Register the service
	pgctldservice.RegisterPgCtldServer(grpcServer, service)

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("gRPC server error: %v", err)
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Return cleanup function that stops server and closes service
	cleanup := func() {
		service.Close()
		grpcServer.Stop()
	}

	return lis, cleanup
}

// createPgCtldClient creates a gRPC client for pgctld
func createPgCtldClient(t *testing.T, addr string) pgctldservice.PgCtldClient {
	t.Helper()

	conn, err := grpc.NewClient(addr, grpccommon.LocalClientDialOptions()...)
	require.NoError(t, err, "Failed to create gRPC connection")

	t.Cleanup(func() { conn.Close() })

	return pgctldservice.NewPgCtldClient(conn)
}

// initAndStartPostgreSQL initializes and starts PostgreSQL via gRPC
func initAndStartPostgreSQL(t *testing.T, client pgctldservice.PgCtldClient) {
	t.Helper()

	ctx := context.Background()

	// Initialize data directory
	_, err := client.InitDataDir(ctx, &pgctldservice.InitDataDirRequest{})
	require.NoError(t, err, "InitDataDir should succeed")

	// Start PostgreSQL
	_, err = client.Start(ctx, &pgctldservice.StartRequest{})
	require.NoError(t, err, "Start should succeed")

	// Wait for PostgreSQL to be ready
	deadline := time.Now().Add(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		resp, err := client.Status(ctx, &pgctldservice.StatusRequest{})
		if err == nil && resp.Status == pgctldservice.ServerStatus_RUNNING && resp.Ready {
			t.Log("PostgreSQL is ready")
			return
		}
		<-ticker.C
	}

	require.Fail(t, "Timeout waiting for PostgreSQL to be ready")
}

// getPgBackRestStatus gets pgBackRest status from pgctld
func getPgBackRestStatus(t *testing.T, client pgctldservice.PgCtldClient) *pgctldservice.PgBackRestStatus {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Status(ctx, &pgctldservice.StatusRequest{})
	require.NoError(t, err, "Status call should succeed")
	require.NotNil(t, resp.PgbackrestStatus, "PgbackrestStatus should not be nil")

	return resp.PgbackrestStatus
}
