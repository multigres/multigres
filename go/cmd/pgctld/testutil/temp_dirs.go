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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/services/pgctld"
)

// TempDir creates a temporary directory for testing and returns a cleanup function
func TempDir(t *testing.T, prefix string) (string, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", prefix+"_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cleanup := func() {
		// Clean up any leftover PostgreSQL mock processes
		cleanupMockProcesses(t, dir)

		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Failed to remove temp dir %s: %v", dir, err)
		}
	}

	return dir, cleanup
}

// CreateDataDir creates a PostgreSQL-like data directory structure for testing
func CreateDataDir(t *testing.T, baseDir string, initialized bool) string {
	t.Helper()

	// This is the base location where multigres expects postgres data
	dataDir := pgctld.PostgresDataDir(baseDir)
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatalf("Failed to create data dir: %v", err)
	}

	if initialized {
		// Create PG_VERSION file to indicate initialized data directory
		pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
		if err := os.WriteFile(pgVersionFile, []byte("15.0\n"), 0o644); err != nil {
			t.Fatalf("Failed to create PG_VERSION file: %v", err)
		}
		// Generate a proper postgresql.conf file using the postgresconfig_gen functionality
		_, err := pgctld.GeneratePostgresServerConfig(baseDir, 5432, "postgres")
		if err != nil {
			t.Fatalf("Failed to generate PostgreSQL config: %v", err)
		}

		// Create other typical PostgreSQL files manually
		files := map[string]string{
			"pg_ident.conf": "# Test ident config\n",
		}

		for file, content := range files {
			path := filepath.Join(dataDir, file)
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatalf("Failed to create file %s: %v", file, err)
			}
		}

		// Create base directory
		baseSubDir := filepath.Join(dataDir, "base")
		if err := os.MkdirAll(baseSubDir, 0o700); err != nil {
			t.Fatalf("Failed to create base dir: %v", err)
		}
	}

	return dataDir
}

// CreatePIDFile creates a postmaster.pid file for testing with a real running process
func CreatePIDFile(t *testing.T, dataDir string, pid int) {
	t.Helper()

	// Start a background sleep process to get a real PID that will pass the isProcessRunning check
	cmd := exec.Command("sleep", "3600")
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start background sleep process: %v", err)
	}

	realPID := cmd.Process.Pid

	// Register cleanup to kill the background process when test finishes
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	pidFile := filepath.Join(dataDir, "postmaster.pid")

	content := []string{
		strconv.Itoa(realPID),
		dataDir,
		"1234567890",
		"5432",
		"/tmp",
		"localhost",
		"*",
		"ready",
	}

	pidContent := strings.Join(content, "\n") + "\n"
	if err := os.WriteFile(pidFile, []byte(pidContent), 0o644); err != nil {
		t.Fatalf("Failed to create PID file: %v", err)
	}
}

// RemovePIDFile removes the postmaster.pid file for testing
func RemovePIDFile(t *testing.T, dataDir string) {
	t.Helper()

	pidFile := filepath.Join(dataDir, "postmaster.pid")
	if err := os.Remove(pidFile); err != nil && !os.IsNotExist(err) {
		t.Fatalf("Failed to remove PID file: %v", err)
	}
}

// CreateDeadPIDFile creates a postmaster.pid file with a PID that does not exist,
// simulating a crashed PostgreSQL process
func CreateDeadPIDFile(t *testing.T, dataDir string, deadPID int) {
	t.Helper()

	pidFile := filepath.Join(dataDir, "postmaster.pid")

	content := []string{
		strconv.Itoa(deadPID),
		dataDir,
		"1234567890",
		"5432",
		"/tmp",
		"localhost",
		"*",
		"ready",
	}

	pidContent := strings.Join(content, "\n") + "\n"
	if err := os.WriteFile(pidFile, []byte(pidContent), 0o644); err != nil {
		t.Fatalf("Failed to create dead PID file: %v", err)
	}
}

// cleanupMockProcesses kills any leftover sleep processes created by mock PostgreSQL binaries
func cleanupMockProcesses(t *testing.T, tempDir string) {
	t.Helper()

	// Look for any postmaster.pid files in the temp directory and kill associated processes
	err := filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil //nolint:nilerr // Continue walking even if there's an error with one file
		}

		if info.Name() == "postmaster.pid" {
			// Read the PID from the file and kill the process
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil //nolint:nilerr // Continue if we can't read the file
			}

			lines := strings.Split(string(content), "\n")
			if len(lines) > 0 {
				pidStr := strings.TrimSpace(lines[0])
				if pid, parseErr := strconv.Atoi(pidStr); parseErr == nil {
					// Try to kill the process (ignore errors since process might already be dead)
					if process, findErr := os.FindProcess(pid); findErr == nil {
						_ = process.Kill()
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Logf("Warning: failed to walk temp directory for cleanup: %v", err)
	}
}
