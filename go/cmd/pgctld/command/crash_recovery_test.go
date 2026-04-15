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

package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostgresAlreadyRunningPattern verifies the regex pattern matches the actual error
func TestPostgresAlreadyRunningPattern(t *testing.T) {
	testCases := []struct {
		name     string
		output   string
		expected bool
	}{
		{
			name:     "actual postgres error",
			output:   `FATAL:  lock file "postmaster.pid" already exists`,
			expected: true,
		},
		{
			name:     "full error with hint",
			output:   `FATAL:  lock file "postmaster.pid" already exists\nHINT:  Is another postmaster (PID 12345) running in data directory "/data"?`,
			expected: true,
		},
		{
			name:     "other lock file",
			output:   `FATAL:  lock file "other.lock" already exists`,
			expected: true,
		},
		{
			name:     "different error",
			output:   `FATAL:  database system is in recovery mode`,
			expected: false,
		},
		{
			name:     "similar but not exact",
			output:   `lock file is missing`,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := postgresAlreadyRunningPattern.MatchString(tc.output)
			if result != tc.expected {
				t.Errorf("Pattern match = %v, want %v for output: %q",
					result, tc.expected, tc.output)
			}
		})
	}
}

// TestExtractClusterState verifies parsing of pg_controldata output.
func TestExtractClusterState(t *testing.T) {
	tests := []struct {
		name     string
		output   string
		expected string
	}{
		{
			name: "in production",
			output: `pg_control version number:            1300
Database cluster state:               in production
pg_control last modified:             Thu Apr  9 22:40:51 2026`,
			expected: "in production",
		},
		{
			name: "shut down",
			output: `pg_control version number:            1300
Database cluster state:               shut down
pg_control last modified:             Thu Apr  9 22:40:51 2026`,
			expected: "shut down",
		},
		{
			name: "shut down in recovery",
			output: `pg_control version number:            1300
Database cluster state:               shut down in recovery
pg_control last modified:             Thu Apr  9 22:40:51 2026`,
			expected: "shut down in recovery",
		},
		{
			name: "in archive recovery",
			output: `pg_control version number:            1300
Database cluster state:               in archive recovery
pg_control last modified:             Thu Apr  9 22:40:51 2026`,
			expected: "in archive recovery",
		},
		{
			name:     "missing state line",
			output:   `pg_control version number:            1300`,
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractClusterState(tt.output)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// TestIsCleanClusterState verifies the clean/unclean classification.
func TestIsCleanClusterState(t *testing.T) {
	tests := []struct {
		state   string
		isClean bool
	}{
		{"shut down", true},
		{"shut down in recovery", true},
		{"in production", false},
		{"in archive recovery", false},
		{"in crash recovery", false},
		{"unknown", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			assert.Equal(t, tt.isClean, IsCleanClusterState(tt.state))
		})
	}
}

// TestRunCrashRecovery_RemovesStandbySignal verifies that runCrashRecovery removes
// standby.signal before starting single-user mode.
func TestRunCrashRecovery_RemovesStandbySignal(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("PGDATA", dataDir)

	// Create standby.signal to simulate a previously started standby node.
	standbySignalPath := filepath.Join(dataDir, "standby.signal")
	require.NoError(t, os.WriteFile(standbySignalPath, []byte(""), 0o644))

	// Set up a mock postgres binary that immediately fails (exits non-zero) so
	// runCrashRecovery returns an error, but standby.signal removal is the first step.
	binDir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(binDir, "postgres"),
		[]byte("#!/bin/bash\nexit 1\n"),
		0o755,
	))
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	// Run crash recovery — it will fail because our mock postgres exits 1,
	// but standby.signal must be removed before postgres is called.
	_ = runCrashRecovery(t.Context(), testLogger())

	assert.NoFileExists(t, standbySignalPath, "standby.signal should be removed before crash recovery")
}

// TestRunCrashRecovery_NoStandbySignal verifies that runCrashRecovery works correctly
// when standby.signal does not exist (e.g., after a primary crash).
func TestRunCrashRecovery_NoStandbySignal(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("PGDATA", dataDir)

	// No standby.signal present.
	standbySignalPath := filepath.Join(dataDir, "standby.signal")

	// Set up a mock postgres that exits successfully (simulates completed crash recovery).
	binDir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(binDir, "postgres"),
		[]byte("#!/bin/bash\nexit 0\n"),
		0o755,
	))
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	err := runCrashRecovery(t.Context(), testLogger())
	require.NoError(t, err)

	// standby.signal should still not exist.
	assert.NoFileExists(t, standbySignalPath)
}
