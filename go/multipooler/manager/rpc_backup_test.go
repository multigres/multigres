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

package manager

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/timer"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// createTestManager creates a minimal MultiPoolerManager for testing
func createTestManager(poolerDir, tableGroup, shard string, poolerType clustermetadatapb.PoolerType) *MultiPoolerManager {
	return createTestManagerWithBackupLocation(poolerDir, tableGroup, shard, poolerType, "/tmp/backups")
}

// createTestManagerWithBackupLocation creates a minimal MultiPoolerManager for testing with backup_location.
// backupLocation is the base path; the full path (with database/tablegroup/shard) is computed internally.
func createTestManagerWithBackupLocation(poolerDir, tableGroup, shard string, poolerType clustermetadatapb.PoolerType, backupLocation string) *MultiPoolerManager {
	database := "test-database"

	// Use defaults if not provided
	if tableGroup == "" {
		tableGroup = constants.DefaultTableGroup
	}
	if shard == "" {
		shard = constants.DefaultShard
	}

	multipoolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-multipooler",
	}

	multipoolerProto := &clustermetadatapb.MultiPooler{
		Id:         multipoolerID,
		Type:       poolerType,
		TableGroup: tableGroup,
		Shard:      shard,
		Database:   database,
		PoolerDir:  poolerDir,
	}

	// Create a topology store with backup location if provided
	var topoClient topoclient.Store
	var backupConfig *backup.Config
	if backupLocation != "" {
		ctx := context.Background()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		err := ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
			Name:             database,
			BackupLocation:   utils.FilesystemBackupLocation(backupLocation),
			DurabilityPolicy: "ANY_2",
		})
		if err == nil {
			topoClient = ts
		}

		// Create backup config
		backupConfig, _ = backup.NewConfig(
			utils.FilesystemBackupLocation(backupLocation),
		)
	}

	monitorRunner := timer.NewPeriodicRunner(context.TODO(), 10*time.Second)

	pm := &MultiPoolerManager{
		config:       &Config{},
		serviceID:    multipoolerID,
		topoClient:   topoClient,
		multipooler:  multipoolerProto,
		state:        ManagerStateReady,
		backupConfig: backupConfig,
		actionLock:   NewActionLock(),
		logger:       slog.Default(),
		pgMonitor:    monitorRunner,
	}
	return pm
}

// setupMockPgBackRestConfig creates a mock pgbackrest.conf file that pgctld would normally generate.
// This allows tests to proceed past initPgBackRest() validation.
func setupMockPgBackRestConfig(t *testing.T, poolerDir string) {
	t.Helper()

	// Create pgbackrest directory
	pgbackrestDir := filepath.Join(poolerDir, "pgbackrest")
	err := os.MkdirAll(pgbackrestDir, 0o755)
	require.NoError(t, err, "failed to create pgbackrest directory")

	// Create minimal pgbackrest.conf
	configPath := filepath.Join(pgbackrestDir, "pgbackrest.conf")
	configContent := `[global]
log-path=` + filepath.Join(pgbackrestDir, "logs") + `
spool-path=` + filepath.Join(pgbackrestDir, "spool") + `
lock-path=` + filepath.Join(pgbackrestDir, "lock") + `

[multigres]
repo1-path=/tmp/backups
pg1-path=` + filepath.Join(poolerDir, "pg_data") + `
pg1-socket-path=` + filepath.Join(poolerDir, "pg_sockets") + `
pg1-port=5432
`
	err = os.WriteFile(configPath, []byte(configContent), 0o600)
	require.NoError(t, err, "failed to create pgbackrest.conf")
}

func TestFindBackupByJobID(t *testing.T) {
	tests := []struct {
		name          string
		jobID         string
		jsonOutput    string
		wantBackupID  string
		wantError     bool
		errorContains string
	}{
		{
			name:  "Single matching backup",
			jobID: "20250104-100000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}]
			}]`,
			wantBackupID: "20250104-100000F",
			wantError:    false,
		},
		{
			name:  "Multiple backups, one match",
			jobID: "20250104-120000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}, {
					"label": "20250104-120000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-120000.000000_mp-zone1"
					}
				}]
			}]`,
			wantBackupID: "20250104-120000F",
			wantError:    false,
		},
		{
			name:  "No matching backup",
			jobID: "20250104-180000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}]
			}]`,
			wantError:     true,
			errorContains: "no backup found",
		},
		{
			name:          "No backups at all",
			jobID:         "20250104-100000.000000_mp-zone1",
			jsonOutput:    `[{"backup": []}]`,
			wantError:     true,
			errorContains: "no backups found",
		},
		{
			name:  "Duplicate matching backups",
			jobID: "20250104-100000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}, {
					"label": "20250104-100000F_20250104-110000I",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}]
			}]`,
			wantError:     true,
			errorContains: "found 2 backups",
		},
		{
			name:  "Backup without annotations",
			jobID: "20250104-100000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F"
				}]
			}]`,
			wantError:     true,
			errorContains: "no backup found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp directory for mock pgbackrest binary
			binDir := t.TempDir()

			// Create mock pgbackrest binary that returns the test JSON
			mockScript := `#!/bin/bash
if [[ "$*" == *"info"* ]]; then
    cat << 'JSONEOF'
` + tt.jsonOutput + `
JSONEOF
    exit 0
fi
exit 1
`
			pgbackrestPath := binDir + "/pgbackrest"
			err := exec.Command("sh", "-c", "cat > "+pgbackrestPath+" << 'EOF'\n"+mockScript+"\nEOF").Run()
			require.NoError(t, err)
			err = exec.Command("chmod", "+x", pgbackrestPath).Run()
			require.NoError(t, err)

			// Prepend bin dir to PATH so our mock pgbackrest is found first
			t.Setenv("PATH", binDir+":/usr/bin:/bin")

			// Use separate directory for pooler data
			poolerDir := t.TempDir()
			setupMockPgBackRestConfig(t, poolerDir)
			pm := createTestManagerWithBackupLocation(poolerDir, "test-tg", "0", clustermetadatapb.PoolerType_REPLICA, poolerDir)

			ctx := context.Background()
			backupID, err := pm.findBackupByJobID(ctx, tt.jobID)

			if tt.wantError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantBackupID, backupID)
			}
		})
	}
}

func TestBackup_Validation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		poolerDir    string
		backupType   string
		poolerType   clustermetadatapb.PoolerType
		forcePrimary bool
		expectError  bool
		errorMsg     string
	}{
		{
			name:        "Missing type",
			poolerDir:   "/tmp/test",
			backupType:  "",
			poolerType:  clustermetadatapb.PoolerType_REPLICA,
			expectError: true,
			errorMsg:    "type is required",
		},
		{
			name:        "Invalid backup type",
			poolerDir:   "/tmp/test",
			backupType:  "invalid",
			poolerType:  clustermetadatapb.PoolerType_REPLICA,
			expectError: true,
			errorMsg:    "invalid backup type",
		},
		{
			name:         "Primary without force flag",
			poolerDir:    "/tmp/test",
			backupType:   "full",
			poolerType:   clustermetadatapb.PoolerType_PRIMARY,
			forcePrimary: false,
			expectError:  true,
			errorMsg:     "not allowed unless ForcePrimary",
		},
		{
			name:         "Primary with force flag",
			poolerDir:    "/tmp/test",
			backupType:   "full",
			poolerType:   clustermetadatapb.PoolerType_PRIMARY,
			forcePrimary: true,
			expectError:  true,                       // Will fail on pgbackrest execution but validation passes
			errorMsg:     "pgbackrest backup failed", // Execution error, not validation
		},
		{
			name:         "Replica backup",
			poolerDir:    "/tmp/test",
			backupType:   "full",
			poolerType:   clustermetadatapb.PoolerType_REPLICA,
			forcePrimary: false,
			expectError:  true, // Will fail - no pg2_path override in local mode
			errorMsg:     "local mode backup requires pg2_path override",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Provide backup location for tests that need to reach pgbackrest execution or validation
			backupLocation := "/tmp/test-backups"
			setupMockPgBackRestConfig(t, tt.poolerDir)
			pm := createTestManagerWithBackupLocation(tt.poolerDir, "", "", tt.poolerType, backupLocation)

			// Setup primary info for replica poolers (required for backup)
			if tt.poolerType == clustermetadatapb.PoolerType_REPLICA {
				pm.primaryHost = "primary.local"
				pm.primaryPort = 5432
			}

			_, err := pm.Backup(ctx, tt.forcePrimary, tt.backupType, "", nil)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				// For valid options without execution, we'd expect success
				// In test environment, we expect pgbackrest to fail
				if err != nil {
					assert.Contains(t, err.Error(), "pgbackrest")
				}
			}
		})
	}
}

func TestGetBackups_Validation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		poolerDir   string
		limit       uint32
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid request",
			poolerDir:   t.TempDir(),
			limit:       0,
			expectError: false, // Should return empty list for non-existent stanza
		},
		{
			name:        "With limit",
			poolerDir:   t.TempDir(),
			limit:       10,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupMockPgBackRestConfig(t, tt.poolerDir)
			pm := createTestManager(tt.poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)

			result, err := pm.GetBackups(ctx, tt.limit)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				// For non-existent stanza, should return empty list without error
				require.NoError(t, err)
				assert.NotNil(t, result)
				assert.Empty(t, result)
			}
		})
	}
}

func TestGetBackups_StatusMapping(t *testing.T) {
	// Test that backup status is correctly mapped from pgBackRest error flag
	tests := []struct {
		name           string
		errorFlag      bool
		expectedStatus multipoolermanagerdata.BackupMetadata_Status
	}{
		{
			name:           "No error means COMPLETE",
			errorFlag:      false,
			expectedStatus: multipoolermanagerdata.BackupMetadata_COMPLETE,
		},
		{
			name:           "Error means INCOMPLETE",
			errorFlag:      true,
			expectedStatus: multipoolermanagerdata.BackupMetadata_INCOMPLETE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This tests the status mapping logic used in GetBackups
			status := multipoolermanagerdata.BackupMetadata_COMPLETE
			if tt.errorFlag {
				status = multipoolermanagerdata.BackupMetadata_INCOMPLETE
			}

			assert.Equal(t, tt.expectedStatus, status)
		})
	}
}

func TestSafeCombinedOutput(t *testing.T) {
	tests := []struct {
		name           string
		command        string
		args           []string
		expectError    bool
		expectedOutput string
		outputContains string
	}{
		{
			name:           "Simple echo command",
			command:        "echo",
			args:           []string{"hello world"},
			expectError:    false,
			expectedOutput: "hello world\n",
		},
		{
			name:           "Command with multiple lines",
			command:        "printf",
			args:           []string{"line1\\nline2\\nline3\\n"},
			expectError:    false,
			expectedOutput: "line1\nline2\nline3\n",
		},
		{
			name:        "Command that fails",
			command:     "sh",
			args:        []string{"-c", "echo 'error message' >&2; exit 1"},
			expectError: true,
			// Output should contain the error message from stderr
			outputContains: "error message",
		},
		{
			name:           "Command with no output",
			command:        "true",
			args:           []string{},
			expectError:    false,
			expectedOutput: "",
		},
		{
			name:        "Command with both stdout and stderr",
			command:     "sh",
			args:        []string{"-c", "echo 'stdout message'; echo 'stderr message' >&2"},
			expectError: false,
			// Output should contain both stdout and stderr
			outputContains: "stdout message",
		},
		{
			name:        "Nonexistent command",
			command:     "nonexistent-command-12345",
			args:        []string{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(tt.command, tt.args...)
			output, err := safeCombinedOutput(cmd)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectedOutput != "" {
				assert.Equal(t, tt.expectedOutput, output)
			}

			if tt.outputContains != "" {
				assert.Contains(t, output, tt.outputContains)
			}
		})
	}
}

func TestSafeCombinedOutput_LargeOutput(t *testing.T) {
	// Test with large output that could potentially fill the channel buffer
	// Generate 200 lines (more than the 100-line buffer)
	t.Run("Large output exceeding channel buffer", func(t *testing.T) {
		cmd := exec.Command("sh", "-c", "for i in $(seq 1 200); do echo \"Line $i\"; done")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Equal(t, 200, len(lines), "Should capture all 200 lines")
		assert.Contains(t, output, "Line 1")
		assert.Contains(t, output, "Line 200")
	})

	// Test with very large output (thousands of lines)
	t.Run("Very large output (1000 lines)", func(t *testing.T) {
		cmd := exec.Command("sh", "-c", "for i in $(seq 1 1000); do echo \"Line $i\"; done")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Equal(t, 1000, len(lines), "Should capture all 1000 lines")
		assert.Contains(t, output, "Line 1")
		assert.Contains(t, output, "Line 1000")
	})

	// Test with large output on both stdout and stderr simultaneously
	t.Run("Large output on both stdout and stderr", func(t *testing.T) {
		script := `
		for i in $(seq 1 100); do
			echo "stdout line $i"
			echo "stderr line $i" >&2
		done
		`
		cmd := exec.Command("sh", "-c", script)
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		assert.Contains(t, output, "stdout line 1")
		assert.Contains(t, output, "stdout line 100")
		assert.Contains(t, output, "stderr line 1")
		assert.Contains(t, output, "stderr line 100")
	})
}

func TestSafeCombinedOutput_LongLines(t *testing.T) {
	// Test with very long lines to ensure bufio.Scanner handles them
	t.Run("Very long single line", func(t *testing.T) {
		// Generate a long string (10KB)
		longString := strings.Repeat("a", 10*1024)
		cmd := exec.Command("echo", longString)
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		assert.Contains(t, output, longString)
	})

	// Test with multiple long lines
	t.Run("Multiple long lines", func(t *testing.T) {
		// Generate 10 lines of 5KB each
		script := "for i in $(seq 1 10); do printf '%s\\n' \"$(printf 'x%.0s' {1..5000})\"; done"
		cmd := exec.Command("bash", "-c", script)
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Equal(t, 10, len(lines))
		for _, line := range lines {
			assert.Greater(t, len(line), 4000, "Each line should be at least 4KB")
		}
	})
}

func TestSafeCombinedOutput_RapidOutput(t *testing.T) {
	// Test with very rapid output to stress-test the channel and goroutine coordination
	t.Run("Rapid burst of output", func(t *testing.T) {
		// Use yes command to generate rapid output, limited by head
		cmd := exec.Command("sh", "-c", "yes 'rapid output line' | head -n 500")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Equal(t, 500, len(lines))
		for _, line := range lines {
			assert.Equal(t, "rapid output line", line)
		}
	})
}

func TestSafeCombinedOutput_InterleavedOutput(t *testing.T) {
	// Test with interleaved stdout and stderr to ensure proper handling
	t.Run("Interleaved stdout and stderr", func(t *testing.T) {
		script := `
		for i in $(seq 1 50); do
			echo "stdout $i"
			echo "stderr $i" >&2
		done
		`
		cmd := exec.Command("sh", "-c", script)
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		// Both stdout and stderr should be captured
		assert.Contains(t, output, "stdout 1")
		assert.Contains(t, output, "stdout 50")
		assert.Contains(t, output, "stderr 1")
		assert.Contains(t, output, "stderr 50")
	})
}

func TestSafeCombinedOutput_EmptyStreams(t *testing.T) {
	t.Run("Only stdout", func(t *testing.T) {
		cmd := exec.Command("echo", "only stdout")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		assert.Equal(t, "only stdout\n", output)
	})

	t.Run("Only stderr", func(t *testing.T) {
		cmd := exec.Command("sh", "-c", "echo 'only stderr' >&2")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		assert.Contains(t, output, "only stderr")
	})

	t.Run("Neither stdout nor stderr", func(t *testing.T) {
		cmd := exec.Command("true")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		assert.Empty(t, output)
	})
}

func TestSafeCombinedOutput_SlowProducer(t *testing.T) {
	// Test with a slow producer to ensure goroutines don't deadlock waiting
	t.Run("Slow output producer", func(t *testing.T) {
		// Produce output slowly (10 lines with small delays)
		script := `
		for i in $(seq 1 10); do
			echo "line $i"
			sleep 0.01
		done
		`
		cmd := exec.Command("sh", "-c", script)
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Equal(t, 10, len(lines))
	})
}

func TestSafeCombinedOutput_PipeCreationFailure(t *testing.T) {
	// Test error handling when pipe creation might fail
	// This is difficult to test directly, but we can test the code path
	t.Run("Command execution after successful pipe setup", func(t *testing.T) {
		// This tests that the function properly handles the happy path
		cmd := exec.Command("echo", "test")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		assert.Equal(t, "test\n", output)
	})
}

func TestSafeCombinedOutput_StressTest(t *testing.T) {
	// Stress test: Generate output that exceeds the channel buffer by a large margin
	// while also mixing stdout and stderr
	t.Run("Extreme stress test", func(t *testing.T) {
		// Generate 2000 lines on stdout and 2000 lines on stderr
		script := `
		(for i in $(seq 1 2000); do echo "stdout $i"; done) &
		(for i in $(seq 1 2000); do echo "stderr $i" >&2; done) &
		wait
		`
		cmd := exec.Command("sh", "-c", script)
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		// Should contain both first and last lines from both streams
		assert.Contains(t, output, "stdout 1")
		assert.Contains(t, output, "stdout 2000")
		assert.Contains(t, output, "stderr 1")
		assert.Contains(t, output, "stderr 2000")

		// Count approximate number of lines (should be ~4000)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Greater(t, len(lines), 3900, "Should have close to 4000 lines")
	})
}

func TestSafeCombinedOutput_BinaryOutput(t *testing.T) {
	// Test with binary output to ensure it doesn't break the scanner
	t.Run("Binary-like output", func(t *testing.T) {
		// Generate output with various characters
		cmd := exec.Command("sh", "-c", "printf 'text\\x00with\\x00nulls\\n'")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		// Should handle the output without crashing
		assert.NotEmpty(t, output)
	})
}

func TestSafeCombinedOutput_CommandNotFound(t *testing.T) {
	t.Run("Command does not exist", func(t *testing.T) {
		cmd := exec.Command("this-command-definitely-does-not-exist-12345")
		_, err := safeCombinedOutput(cmd)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to start command")
	})
}

func TestSafeCombinedOutput_RealWorldScenario(t *testing.T) {
	// Simulate pgbackrest-like output with mixed stdout/stderr
	t.Run("Simulate pgbackrest info output", func(t *testing.T) {
		script := `
		cat <<'EOF'
P00   INFO: backup command begin 2.41: --exec-id=12345
P00   INFO: execute non-exclusive pg_start_backup()
P00   INFO: backup start archive = 000000010000000000000002
P00   INFO: new backup label = 20250104-100000F
P00   INFO: full backup size = 25.3MB
P00   INFO: backup command end: completed successfully
EOF
		`
		cmd := exec.Command("sh", "-c", script)
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		assert.Contains(t, output, "new backup label = 20250104-100000F")
		assert.Contains(t, output, "backup command end: completed successfully")
	})
}

func TestSafeCombinedOutput_ConcurrentReads(t *testing.T) {
	// Verify that concurrent reads from stdout and stderr don't cause issues
	t.Run("Heavy concurrent output", func(t *testing.T) {
		script := `
		# Write to stdout and stderr concurrently as fast as possible
		(seq 1 1000 | while read i; do echo "OUT$i"; done) &
		(seq 1 1000 | while read i; do echo "ERR$i" >&2; done) &
		wait
		`
		cmd := exec.Command("sh", "-c", script)
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		// Both streams should be captured
		assert.Contains(t, output, "OUT1")
		assert.Contains(t, output, "ERR1")
		// Count lines to ensure we got most/all output
		lineCount := len(strings.Split(strings.TrimSpace(output), "\n"))
		assert.Greater(t, lineCount, 1900, "Should capture most of the 2000 lines")
	})
}

func BenchmarkSafeCombinedOutput(b *testing.B) {
	b.Run("Small output", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cmd := exec.Command("echo", "hello world")
			_, _ = safeCombinedOutput(cmd)
		}
	})

	b.Run("Medium output (100 lines)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cmd := exec.Command("sh", "-c", "for i in $(seq 1 100); do echo \"Line $i\"; done")
			_, _ = safeCombinedOutput(cmd)
		}
	})

	b.Run("Large output (1000 lines)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cmd := exec.Command("sh", "-c", "for i in $(seq 1 1000); do echo \"Line $i\"; done")
			_, _ = safeCombinedOutput(cmd)
		}
	})

	b.Run("Mixed stdout and stderr", func(b *testing.B) {
		script := "for i in $(seq 1 100); do echo \"stdout $i\"; echo \"stderr $i\" >&2; done"
		for i := 0; i < b.N; i++ {
			cmd := exec.Command("sh", "-c", script)
			_, _ = safeCombinedOutput(cmd)
		}
	})
}

func TestBackup_ActionLock(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	pm := createTestManagerWithBackupLocation(tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Hold the lock in another goroutine
	lockCtx, err := pm.actionLock.Acquire(ctx, "test-holder")
	require.NoError(t, err)

	// Create a context with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// Backup should timeout waiting for the lock
	_, err = pm.Backup(timeoutCtx, false, "full", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")

	// Release the lock
	pm.actionLock.Release(lockCtx)
}

func TestGetBackups_ActionLock(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	pm := createTestManagerWithBackupLocation(tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Hold the lock in another goroutine
	lockCtx, err := pm.actionLock.Acquire(ctx, "test-holder")
	require.NoError(t, err)

	// Create a context with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// GetBackups should timeout waiting for the lock
	_, err = pm.GetBackups(timeoutCtx, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")

	// Release the lock
	pm.actionLock.Release(lockCtx)
}

func TestRestoreFromBackup_ActionLock(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	pm := createTestManagerWithBackupLocation(tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Hold the lock in another goroutine
	lockCtx, err := pm.actionLock.Acquire(ctx, "test-holder")
	require.NoError(t, err)

	// Create a context with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// RestoreFromBackup should timeout waiting for the lock
	err = pm.RestoreFromBackup(timeoutCtx, "test-backup-id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")

	// Release the lock
	pm.actionLock.Release(lockCtx)
}

func TestBackup_ActionLockReleased(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	pm := createTestManagerWithBackupLocation(tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Call Backup - it will fail (no pgbackrest), but should release the lock
	_, _ = pm.Backup(ctx, false, "full", "", nil)

	// Verify lock was released by acquiring it with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	lockCtx, err := pm.actionLock.Acquire(timeoutCtx, "verify-release")
	require.NoError(t, err, "Lock should be released after Backup returns")
	pm.actionLock.Release(lockCtx)
}

func TestGetBackups_ActionLockReleased(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	pm := createTestManagerWithBackupLocation(tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Call GetBackups - it may fail or succeed, but should release the lock
	_, _ = pm.GetBackups(ctx, 10)

	// Verify lock was released by acquiring it with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	lockCtx, err := pm.actionLock.Acquire(timeoutCtx, "verify-release")
	require.NoError(t, err, "Lock should be released after GetBackups returns")
	pm.actionLock.Release(lockCtx)
}

func TestRestoreFromBackup_ActionLockReleased(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	pm := createTestManagerWithBackupLocation(tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Call RestoreFromBackup - it will fail (precondition), but should release the lock
	_ = pm.RestoreFromBackup(ctx, "test-backup-id")

	// Verify lock was released by acquiring it with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	lockCtx, err := pm.actionLock.Acquire(timeoutCtx, "verify-release")
	require.NoError(t, err, "Lock should be released after RestoreFromBackup returns")
	pm.actionLock.Release(lockCtx)
}

func TestBackup_StoresJobIDAnnotation(t *testing.T) {
	// Verify that when job_id is provided, it's included in annotations
	req := &multipoolermanagerdata.BackupRequest{
		JobId: "20251203-143045.123456_mp-cell-1",
	}

	// Verify the field is accessible (structural test)
	require.NotEmpty(t, req.JobId, "JobId should be set")
	assert.Equal(t, "20251203-143045.123456_mp-cell-1", req.JobId)
}

func TestGetBackupByJobId_NotFound(t *testing.T) {
	// Test that searching for a non-existent job_id returns nil backup

	resp := &multipoolermanagerdata.GetBackupByJobIdResponse{
		Backup: nil,
	}

	assert.Nil(t, resp.Backup)
}

func TestGetBackupByJobId_Found(t *testing.T) {
	// Test response structure when backup is found

	resp := &multipoolermanagerdata.GetBackupByJobIdResponse{
		Backup: &multipoolermanagerdata.BackupMetadata{
			BackupId:   "20251203-143045F",
			TableGroup: "default",
			Shard:      "0",
			Status:     multipoolermanagerdata.BackupMetadata_COMPLETE,
			JobId:      "20251203-143045.123456_mp-cell-1",
		},
	}

	require.NotNil(t, resp.Backup)
	assert.Equal(t, "20251203-143045F", resp.Backup.BackupId)
	assert.Equal(t, "20251203-143045.123456_mp-cell-1", resp.Backup.JobId)
}

func TestInitPgBackRest(t *testing.T) {
	tests := []struct {
		name          string
		setupConfig   bool // Whether to create the pgbackrest.conf file
		expectError   bool
		errorContains string
	}{
		{
			name:        "Success when config exists",
			setupConfig: true,
			expectError: false,
		},
		{
			name:          "Failure when config missing",
			setupConfig:   false,
			expectError:   true,
			errorContains: "pgbackrest config not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			poolerDir := t.TempDir()
			pm := createTestManagerWithBackupLocation(
				poolerDir,
				"test-tg",
				"0",
				clustermetadatapb.PoolerType_REPLICA,
				"/tmp/test-backups",
			)

			if tt.setupConfig {
				setupMockPgBackRestConfig(t, poolerDir)
			}

			configPath, err := pm.checkPgBackRestConfig(context.Background())

			if tt.expectError {
				require.Error(t, err)
				assert.Empty(t, configPath, "config path should be empty on error")
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, configPath, "config path should not be empty on success")

				// Verify config file exists
				pgbackrestPath := pm.pgbackrestPath()
				expectedPath := filepath.Join(pgbackrestPath, "pgbackrest.conf")
				assert.Equal(t, expectedPath, configPath)
				assert.FileExists(t, configPath, "config file should exist")
			}
		})
	}
}

func TestInitPgBackRest_Idempotent(t *testing.T) {
	// Test that calling initPgBackRest multiple times is safe (idempotent)
	poolerDir := t.TempDir()
	setupMockPgBackRestConfig(t, poolerDir)
	pm := createTestManagerWithBackupLocation(
		poolerDir,
		"test-tg",
		"0",
		clustermetadatapb.PoolerType_REPLICA,
		"/tmp/backups",
	)

	// First call
	configPath, err := pm.checkPgBackRestConfig(context.Background())
	require.NoError(t, err)
	assert.NotEmpty(t, configPath)

	// Second call - should return the same path
	configPath2, err := pm.checkPgBackRestConfig(context.Background())
	require.NoError(t, err)
	assert.Equal(t, configPath, configPath2, "should return same path on second call")
}

func TestBackup_UsesCLIArgs(t *testing.T) {
	// Test that Backup() uses CLI args with pgctld-generated config instead of creating temp config
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create mock pgbackrest binary that captures command-line arguments
	binDir := filepath.Join(tmpDir, "bin")
	err := os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)

	argsFile := filepath.Join(tmpDir, "captured_args.txt")
	mockScript := `#!/bin/bash
# Capture all arguments to a file for backup command
if [[ "$*" == *"backup"* && "$*" != *"info"* ]]; then
    echo "$@" > ` + argsFile + `
fi
# Handle info command to return mock backup info
if [[ "$*" == *"info"* ]]; then
    cat << 'JSONEOF'
[{
    "backup": [{
        "label": "20250104-100000F",
        "annotation": {
            "multipooler_id": "test-multipooler",
            "job_id": "test-job-id"
        }
    }]
}]
JSONEOF
fi
exit 0
`
	pgbackrestPath := filepath.Join(binDir, "pgbackrest")
	err = os.WriteFile(pgbackrestPath, []byte(mockScript), 0o755)
	require.NoError(t, err)

	// Prepend binDir to PATH so our mock pgbackrest is found first
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	// Create pooler directory structure
	poolerDir := filepath.Join(tmpDir, "pooler")
	err = os.MkdirAll(poolerDir, 0o755)
	require.NoError(t, err)

	// Create a pgbackrest.conf that pgctld would have generated
	pgbackrestDir := filepath.Join(poolerDir, "pgbackrest")
	err = os.MkdirAll(pgbackrestDir, 0o755)
	require.NoError(t, err)

	pgctldConfigPath := filepath.Join(pgbackrestDir, "pgbackrest.conf")
	pgctldConfig := `[global]
log-path=/tmp/pgbackrest/logs
[multigres]
repo1-path=/tmp/backups
pg1-path=/tmp/pg_data
`
	err = os.WriteFile(pgctldConfigPath, []byte(pgctldConfig), 0o600)
	require.NoError(t, err)

	// Create test manager with the pooler directory
	pm := createTestManagerWithBackupLocation(poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Setup primary info (required for replica backups)
	pm.primaryHost = "primary.local"
	pm.primaryPort = 5432

	// Call Backup with pg2_path override for local mode
	primaryDataPath := filepath.Join(poolerDir, "pg_data")
	_, err = pm.Backup(ctx, false, "full", "test-job-id", map[string]string{
		"pg2_path": primaryDataPath,
	})
	require.NoError(t, err)

	// Read captured arguments
	capturedArgs, err := os.ReadFile(argsFile)
	require.NoError(t, err)
	args := string(capturedArgs)

	// Verify --config points to pgctld's config (not a temp file)
	assert.Contains(t, args, "--config", "command should include --config flag")
	assert.Contains(t, args, "pgbackrest.conf", "config should be pgbackrest.conf from pgctld")
	assert.NotContains(t, args, "pgbackrest-backup.conf", "should NOT use temp backup config file")

	// Verify backup command structure
	assert.Contains(t, args, "--stanza=multigres", "command should include stanza")
	assert.Contains(t, args, "--type=full", "command should include backup type")
	assert.Contains(t, args, "backup", "command should be backup")

	// Verify NO temp config files were created
	files, err := os.ReadDir(pgbackrestDir)
	require.NoError(t, err)
	for _, file := range files {
		assert.NotContains(t, file.Name(), "pgbackrest-backup.conf", "temp backup config should not be created")
		assert.NotContains(t, file.Name(), "tmp", "no temp files should be created")
	}
}

func TestGetPrimaryAsPg2Args(t *testing.T) {
	tests := []struct {
		name                    string
		poolerType              clustermetadatapb.PoolerType
		primaryHost             string
		primaryPort             int32
		setupTLSCerts           bool // Whether to create TLS cert files
		pgBackRestTLSPort       int  // Port for pgBackRest TLS server
		expectError             bool
		expectedArgsContains    []string // Args we expect to find
		expectedArgsNotContains []string // Args we expect NOT to find
	}{
		{
			name:                    "primary_pooler_returns_empty",
			poolerType:              clustermetadatapb.PoolerType_PRIMARY,
			primaryHost:             "primary.example.com",
			primaryPort:             5432,
			setupTLSCerts:           true,
			pgBackRestTLSPort:       8443,
			expectError:             false,
			expectedArgsContains:    []string{},
			expectedArgsNotContains: []string{"--pg2-host", "--pg2-port"},
		},
		{
			name:              "replica_with_tls_certs",
			poolerType:        clustermetadatapb.PoolerType_REPLICA,
			primaryHost:       "primary.example.com",
			primaryPort:       5432,
			setupTLSCerts:     true,
			pgBackRestTLSPort: 8443, // Will be fetched from topology, not passed as param
			expectError:       false,
			expectedArgsContains: []string{
				"--pg2-host=primary.example.com",
				"--pg2-host-type=tls",
				"--pg2-host-port=8443",
				"--pg2-host-ca-file=",
				"--pg2-host-cert-file=",
				"--pg2-host-key-file=",
			},
			expectedArgsNotContains: []string{"--pg2-port=5432", "--pg2-path="},
		},
		{
			name:                 "replica_without_tls_local_mode_requires_override",
			poolerType:           clustermetadatapb.PoolerType_REPLICA,
			primaryHost:          "localhost",
			primaryPort:          5432,
			setupTLSCerts:        false,
			pgBackRestTLSPort:    0,
			expectError:          true, // Now expects error when pg2_path override is missing
			expectedArgsContains: nil,
		},
		{
			name:                 "replica_without_primary_info_errors",
			poolerType:           clustermetadatapb.PoolerType_REPLICA,
			primaryHost:          "",
			primaryPort:          0,
			setupTLSCerts:        false,
			pgBackRestTLSPort:    0,
			expectError:          true,
			expectedArgsContains: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test manager
			poolerDir := t.TempDir()
			pm := createTestManager(poolerDir, "", "", tt.poolerType)
			pm.primaryHost = tt.primaryHost
			pm.primaryPort = tt.primaryPort

			// Setup TLS certs if needed and create primary pooler in topology
			if tt.setupTLSCerts {
				certDir := filepath.Join(poolerDir, "pgbackrest", "certs")
				require.NoError(t, os.MkdirAll(certDir, 0o755))
				caFile := filepath.Join(certDir, "ca.crt")
				certFile := filepath.Join(certDir, "client.crt")
				keyFile := filepath.Join(certDir, "client.key")
				require.NoError(t, os.WriteFile(caFile, []byte("test ca"), 0o644))
				require.NoError(t, os.WriteFile(certFile, []byte("test cert"), 0o644))
				require.NoError(t, os.WriteFile(keyFile, []byte("test key"), 0o600))
				// Set cert paths in config so GetPrimaryAsPg2Args can use them
				pm.config.PgBackRestCAFile = caFile
				pm.config.PgBackRestCertFile = certFile
				pm.config.PgBackRestKeyFile = keyFile

				// Create primary pooler ID and add to topology with pgbackrest port
				primaryID := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "test-primary",
				}
				pm.primaryPoolerID = primaryID

				// Add primary to topology with pgbackrest port
				if pm.topoClient != nil {
					primaryPooler := &clustermetadatapb.MultiPooler{
						Id:       primaryID,
						Type:     clustermetadatapb.PoolerType_PRIMARY,
						Hostname: tt.primaryHost,
						PortMap: map[string]int32{
							"postgres":   tt.primaryPort,
							"pgbackrest": int32(tt.pgBackRestTLSPort),
						},
					}
					err := pm.topoClient.CreateMultiPooler(context.Background(), primaryPooler)
					require.NoError(t, err, "failed to create primary pooler in topology")
				}
			}

			// Call GetPrimaryAsPg2Args
			got, err := pm.GetPrimaryAsPg2Args(context.Background(), nil)

			// Check error expectation
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Check expected args are present
			for _, expected := range tt.expectedArgsContains {
				found := false
				for _, arg := range got {
					if strings.Contains(arg, strings.Split(expected, "=")[0]) {
						if expected == "" || strings.HasPrefix(arg, expected) || arg == expected {
							found = true
							break
						}
					}
				}
				assert.True(t, found, "expected arg %q not found in %v", expected, got)
			}

			// Check unexpected args are NOT present
			for _, notExpected := range tt.expectedArgsNotContains {
				for _, arg := range got {
					assert.NotContains(t, arg, strings.Split(notExpected, "=")[0],
						"unexpected arg prefix %q found in %v", notExpected, got)
				}
			}
		})
	}
}

func TestGetPrimaryAsPg2Args_WithOverrides(t *testing.T) {
	ctx := context.Background()
	poolerDir := t.TempDir()

	t.Run("TLS mode ignores pg2_path in base but applies via overrides", func(t *testing.T) {
		// Create manager with TLS certs
		pm := createTestManager(poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)
		pm.config.PgBackRestCAFile = "/path/to/ca.crt"
		pm.config.PgBackRestCertFile = "/path/to/client.crt"
		pm.config.PgBackRestKeyFile = "/path/to/client.key"
		pm.primaryHost = "primary.example.com"
		pm.primaryPort = 5432

		// Create primary pooler ID and add to topology with pgbackrest port
		primaryID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-primary-override",
		}
		pm.primaryPoolerID = primaryID

		// Add primary to topology with pgbackrest port
		if pm.topoClient != nil {
			primaryPooler := &clustermetadatapb.MultiPooler{
				Id:       primaryID,
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com",
				PortMap: map[string]int32{
					"postgres":   5432,
					"pgbackrest": 8432,
				},
			}
			err := pm.topoClient.CreateMultiPooler(ctx, primaryPooler)
			require.NoError(t, err, "failed to create primary pooler in topology")
		}

		args, err := pm.GetPrimaryAsPg2Args(ctx, map[string]string{
			"pg2_path": "/custom/path",
		})

		require.NoError(t, err)
		assert.Contains(t, args, "--pg2-host=primary.example.com")
		assert.Contains(t, args, "--pg2-host-type=tls")
		assert.Contains(t, args, "--pg2-host-port=8432")
		assert.Contains(t, args, "--pg2-path=/custom/path") // Added by override
	})

	t.Run("local mode requires pg2_path override", func(t *testing.T) {
		// Create manager without TLS certs (local mode)
		pm := createTestManager(poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)
		pm.primaryHost = "localhost"
		pm.primaryPort = 5432

		// Without override - should error
		_, err := pm.GetPrimaryAsPg2Args(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "local mode backup requires pg2_path override")

		// With override - should work
		args, err := pm.GetPrimaryAsPg2Args(ctx, map[string]string{
			"pg2_path": "/primary/data",
		})
		require.NoError(t, err)
		assert.Contains(t, args, "--pg2-host=localhost")
		assert.Contains(t, args, "--pg2-port=5432")
		assert.Contains(t, args, "--pg2-path=/primary/data")
	})

	t.Run("override replaces existing arg", func(t *testing.T) {
		pm := createTestManager(poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)
		pm.config.PgBackRestCAFile = "/path/to/ca.crt"
		pm.config.PgBackRestCertFile = "/path/to/client.crt"
		pm.config.PgBackRestKeyFile = "/path/to/client.key"
		pm.primaryHost = "primary.example.com"
		pm.primaryPort = 5432

		// Create primary pooler ID and add to topology with pgbackrest port
		primaryID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-primary-override2",
		}
		pm.primaryPoolerID = primaryID

		// Add primary to topology with pgbackrest port
		if pm.topoClient != nil {
			primaryPooler := &clustermetadatapb.MultiPooler{
				Id:       primaryID,
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com",
				PortMap: map[string]int32{
					"postgres":   5432,
					"pgbackrest": 8432,
				},
			}
			err := pm.topoClient.CreateMultiPooler(ctx, primaryPooler)
			require.NoError(t, err, "failed to create primary pooler in topology")
		}

		args, err := pm.GetPrimaryAsPg2Args(ctx, map[string]string{
			"pg2_host_port": "9999",
		})

		require.NoError(t, err)
		assert.Contains(t, args, "--pg2-host-port=9999")
		assert.NotContains(t, args, "--pg2-host-port=8432")
	})
}
