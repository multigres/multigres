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

package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TempDir creates a temporary directory for testing and returns a cleanup function
func TempDir(t *testing.T, prefix string) (string, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", prefix+"_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Failed to remove temp dir %s: %v", dir, err)
		}
	}

	return dir, cleanup
}

// CreateDataDir creates a PostgreSQL-like data directory structure for testing
func CreateDataDir(t *testing.T, baseDir string, initialized bool) string {
	t.Helper()

	dataDir := filepath.Join(baseDir, "data")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatalf("Failed to create data dir: %v", err)
	}

	if initialized {
		// Create PG_VERSION file to indicate initialized data directory
		pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
		if err := os.WriteFile(pgVersionFile, []byte("15.0\n"), 0644); err != nil {
			t.Fatalf("Failed to create PG_VERSION file: %v", err)
		}

		// Create other typical PostgreSQL files
		files := []string{
			"postgresql.conf",
			"pg_hba.conf",
			"pg_ident.conf",
		}

		for _, file := range files {
			path := filepath.Join(dataDir, file)
			if err := os.WriteFile(path, []byte("# Test config\n"), 0644); err != nil {
				t.Fatalf("Failed to create file %s: %v", file, err)
			}
		}

		// Create base directory
		baseSubDir := filepath.Join(dataDir, "base")
		if err := os.MkdirAll(baseSubDir, 0700); err != nil {
			t.Fatalf("Failed to create base dir: %v", err)
		}
	}

	return dataDir
}

// CreatePIDFile creates a fake postmaster.pid file for testing
func CreatePIDFile(t *testing.T, dataDir string, pid int) {
	t.Helper()

	pidFile := filepath.Join(dataDir, "postmaster.pid")
	content := []string{
		fmt.Sprintf("%d", pid),
		"/tmp/data",
		"1234567890",
		"5432",
		"/tmp",
		"localhost",
		"*",
		"ready",
	}

	pidContent := strings.Join(content, "\n") + "\n"
	if err := os.WriteFile(pidFile, []byte(pidContent), 0644); err != nil {
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
