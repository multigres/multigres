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
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/pgctldservice"
)

// TestPgBackRestServer_Lifecycle verifies pgBackRest TLS server starts and stops with pgctld
func TestPgBackRestServer_Lifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Check prerequisites
	if _, err := exec.LookPath("pgbackrest"); err != nil {
		t.Skip("pgbackrest binary not found")
	}

	// Setup test environment
	setup := setupPgBackRestTest(t)

	// Create and start gRPC server with pgBackRest support
	lis, cleanup := createTestGRPCServerWithPgBackRest(t, setup)

	// Create gRPC client
	client := createPgCtldClient(t, lis.Addr().String())

	// Initialize and start PostgreSQL
	initAndStartPostgreSQL(t, client)

	// Verify pgbackrest-server.conf was generated
	configPath := filepath.Join(setup.DataDir, "pgbackrest", "pgbackrest-server.conf")
	assert.FileExists(t, configPath, "pgbackrest-server.conf should be generated")

	// Verify TLS server is running via socket connection
	running := verifyServerRunning(t, setup.PgBackRestPort, 10*time.Second)
	require.True(t, running, "pgBackRest TLS server should be running")

	// Verify status via gRPC API
	status := getPgBackRestStatus(t, client)
	t.Logf("pgBackRest status: running=%v, error=%s, restart_count=%d", status.Running, status.ErrorMessage, status.RestartCount)
	assert.True(t, status.Running, "Status should report server running")
	assert.Empty(t, status.ErrorMessage, "Should have no errors")

	// Verify pgBackRest stays running when PostgreSQL stops
	ctx := context.Background()
	_, err := client.Stop(ctx, &pgctldservice.StopRequest{Mode: "fast"})
	require.NoError(t, err, "Stop should succeed")

	// pgBackRest should still be running (independent of PostgreSQL)
	stillRunning := verifyServerRunning(t, setup.PgBackRestPort, 2*time.Second)
	assert.True(t, stillRunning, "pgBackRest TLS server should remain running after PostgreSQL stops")

	// Shutdown pgctld service (this should stop pgBackRest)
	cleanup()

	// Poll for pgBackRest to stop (it should stop within a few seconds)
	deadline := time.Now().Add(5 * time.Second)
	var stopped bool
	for time.Now().Before(deadline) {
		stopped = verifyServerStopped(t, setup.PgBackRestPort)
		if stopped {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.True(t, stopped, "pgBackRest TLS server should be stopped after pgctld shutdown")
}

// TestPgBackRestServer_CrashRecovery verifies pgctld automatically restarts pgBackRest after crash
func TestPgBackRestServer_CrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Check prerequisites
	if _, err := exec.LookPath("pgbackrest"); err != nil {
		t.Skip("pgbackrest binary not found")
	}

	// Setup test environment
	setup := setupPgBackRestTest(t)

	// Create and start gRPC server with pgBackRest support
	lis, cleanup := createTestGRPCServerWithPgBackRest(t, setup)
	defer cleanup()

	// Create gRPC client
	client := createPgCtldClient(t, lis.Addr().String())

	// Initialize and start PostgreSQL
	initAndStartPostgreSQL(t, client)

	// Verify pgBackRest is running and get initial status
	status1 := getPgBackRestStatus(t, client)
	require.True(t, status1.Running, "pgBackRest should be running initially")
	require.Empty(t, status1.ErrorMessage, "Should have no errors initially")
	require.Equal(t, int32(0), status1.RestartCount, "Should have zero restarts initially")

	// Get the PID of the running pgBackRest process for this specific test
	configPath := filepath.Join(setup.DataDir, "pgbackrest", "pgbackrest-server.conf")
	pid1 := getPgBackRestPID(t, configPath)
	require.Greater(t, pid1, 0, "Should find running pgBackRest process")
	t.Logf("Initial pgBackRest PID: %d", pid1)

	// Kill the pgBackRest process to simulate a crash
	t.Log("Simulating pgBackRest crash by killing process...")
	killProcess(t, pid1)

	// Wait for pgctld to detect the crash and restart (should happen within a few seconds)
	t.Log("Waiting for pgctld to detect crash and restart pgBackRest...")
	deadline := time.Now().Add(10 * time.Second)
	var restarted bool
	var status2 *pgctldservice.PgBackRestStatus

	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)

		// Check if server is back up
		running := verifyServerRunning(t, setup.PgBackRestPort, 2*time.Second)
		if !running {
			continue
		}

		// Get status and verify restart count increased
		status2 = getPgBackRestStatus(t, client)
		if status2.Running && status2.RestartCount > 0 {
			restarted = true
			break
		}
	}

	require.True(t, restarted, "pgBackRest should be automatically restarted after crash")
	require.True(t, status2.Running, "pgBackRest should be running after restart")
	require.Greater(t, status2.RestartCount, int32(0), "Restart count should be incremented")
	t.Logf("pgBackRest successfully restarted: restart_count=%d", status2.RestartCount)

	// Verify the new process has a different PID
	pid2 := getPgBackRestPID(t, configPath)
	require.Greater(t, pid2, 0, "Should find new pgBackRest process")
	require.NotEqual(t, pid1, pid2, "New process should have different PID")
	t.Logf("New pgBackRest PID after restart: %d", pid2)

	// Verify server is still functional
	stillRunning := verifyServerRunning(t, setup.PgBackRestPort, 2*time.Second)
	assert.True(t, stillRunning, "pgBackRest server should be functional after restart")
}
