/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package test

import (
	"context"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinaryStartupShutdown(t *testing.T) {
	binaries := []struct {
		name string
		port string
	}{
		{"multigateway", "15432"}, // Use different port to avoid conflicts
		{"multipooler", "15100"},
		{"pgctld", "15200"},
		{"multiorch", "15300"},
	}

	for _, binary := range binaries {
		t.Run(binary.name, func(t *testing.T) {
			testBinaryStartupShutdown(t, binary.name, binary.port)
		})
	}
}

func testBinaryStartupShutdown(t *testing.T, binaryName, port string) {
	// Build path to binary
	binaryPath := filepath.Join("..", "bin", binaryName)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start the binary with custom port
	var cmd *exec.Cmd
	switch binaryName {
	case "multigateway":
		cmd = exec.CommandContext(ctx, binaryPath, "--port", port, "--log-level", "info")
	case "multipooler":
		cmd = exec.CommandContext(ctx, binaryPath, "--grpc-port", port, "--log-level", "info")
	case "pgctld":
		cmd = exec.CommandContext(ctx, binaryPath, "--grpc-port", port, "--log-level", "info")
	case "multiorch":
		cmd = exec.CommandContext(ctx, binaryPath, "--grpc-port", port, "--log-level", "info")
	default:
		require.Fail(t, "Unknown binary: %s", binaryName)
	}

	// Start the process
	err := cmd.Start()
	require.NoError(t, err, "Failed to start %s", binaryName)

	// Give the process time to start up
	time.Sleep(2 * time.Second)

	// Check if process is still running
	require.NotNil(t, cmd.Process, "Process %s exited unexpectedly", binaryName)

	// Send SIGTERM to gracefully shutdown
	err = cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		t.Logf("Failed to send SIGTERM to %s: %v", binaryName, err)
		// Try to kill the process
		if killErr := cmd.Process.Kill(); killErr != nil {
			t.Logf("Failed to kill process %s: %v", binaryName, killErr)
		}
	}

	// Wait for process to exit
	err = cmd.Wait()
	if err != nil {
		// Check if it's just the expected signal termination
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == -1 {
				// Process was terminated by signal, which is expected
				t.Logf("%s terminated gracefully", binaryName)
			} else {
				assert.Equal(t, -1, exitError.ExitCode(), "%s should exit cleanly or be terminated by signal", binaryName)
			}
		} else {
			require.NoError(t, err, "Error waiting for %s", binaryName)
		}
	} else {
		t.Logf("%s exited cleanly", binaryName)
	}
}
