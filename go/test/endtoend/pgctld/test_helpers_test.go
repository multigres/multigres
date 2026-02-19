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
	"net"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupPgBackRestTest(t *testing.T) {
	setup := setupPgBackRestTest(t)

	// Verify temp dir exists
	assert.DirExists(t, setup.TempDir)

	// Verify cert files were created
	assert.FileExists(t, filepath.Join(setup.CertDir, "ca.crt"))
	assert.FileExists(t, filepath.Join(setup.CertDir, "pgbackrest.crt"))
	assert.FileExists(t, filepath.Join(setup.CertDir, "pgbackrest.key"))

	// Verify backup config exists
	assert.NotNil(t, setup.BackupConfig)

	// Verify ports are assigned
	assert.Greater(t, setup.PgPort, 0)
	assert.Greater(t, setup.PgBackRestPort, 0)

	// Verify bin dir exists
	assert.DirExists(t, setup.BinDir)
}

func TestVerifyServerRunning(t *testing.T) {
	// Start a dummy TCP listener
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port

	// Should connect successfully
	running := verifyServerRunning(t, port, 3*time.Second)
	assert.True(t, running, "Should detect running server")

	// Close listener
	listener.Close()

	// Should fail to connect
	running = verifyServerRunning(t, port, 3*time.Second)
	assert.False(t, running, "Should detect stopped server")
}

func TestVerifyServerStopped(t *testing.T) {
	// Start a dummy TCP listener
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port

	// Should detect server is running (not stopped)
	stopped := verifyServerStopped(t, port)
	assert.False(t, stopped, "Should detect server is still running")

	// Close listener
	listener.Close()

	// Should detect server is stopped
	stopped = verifyServerStopped(t, port)
	assert.True(t, stopped, "Should detect server is stopped")
}

func TestGetPgBackRestPID(t *testing.T) {
	// This test requires a running pgbackrest server
	// We'll skip it if pgbackrest is not available
	if _, err := exec.LookPath("pgbackrest"); err != nil {
		t.Skip("pgbackrest binary not found")
	}

	// For now, just test that the function compiles
	// Full test will be in integration test
	t.Skip("Requires full pgctld setup - tested in integration tests")
}

func TestKillProcess(t *testing.T) {
	// Start a dummy process
	cmd := exec.Command("sleep", "10")
	err := cmd.Start()
	require.NoError(t, err)

	pid := cmd.Process.Pid

	// Kill it
	killProcess(t, pid)

	// Wait for it to exit
	err = cmd.Wait()
	assert.Error(t, err, "Process should have been killed")
}
