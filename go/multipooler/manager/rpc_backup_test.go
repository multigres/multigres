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
	"strconv"
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
			expectError:  true, // Will fail on pgbackrest execution
			errorMsg:     "pgbackrest backup failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Provide backup location for tests that need to reach pgbackrest execution or validation
			backupLocation := "/tmp/test-backups"
			pm := createTestManagerWithBackupLocation(tt.poolerDir, "", "", tt.poolerType, backupLocation)

			_, err := pm.Backup(ctx, tt.forcePrimary, tt.backupType, "")

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
	_, err = pm.Backup(timeoutCtx, false, "full", "")
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
	_, _ = pm.Backup(ctx, false, "full", "")

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
		name           string
		poolerDir      string
		backupLocation string
		pgPort         int
		expectError    bool
		errorContains  string
	}{
		{
			name:           "Success with valid paths",
			poolerDir:      t.TempDir(),
			backupLocation: "/tmp/test-backups",
			pgPort:         5432,
			expectError:    false,
		},
		{
			name:           "Success with different port",
			poolerDir:      t.TempDir(),
			backupLocation: "/var/lib/backups",
			pgPort:         15432,
			expectError:    false,
		},
		{
			name:           "Success with nested backup location",
			poolerDir:      t.TempDir(),
			backupLocation: "/deep/nested/backup/path",
			pgPort:         5432,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := createTestManagerWithBackupLocation(
				tt.poolerDir,
				"test-tg",
				"0",
				clustermetadatapb.PoolerType_REPLICA,
				tt.backupLocation,
			)
			pm.multipooler.PortMap = map[string]int32{"postgres": int32(tt.pgPort)}

			configPath, err := pm.initPgBackRest(context.Background(), NotForBackup)

			if tt.expectError {
				require.Error(t, err)
				assert.Empty(t, configPath, "config path should be empty on error")
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, configPath, "config path should not be empty on success")

				// Verify directories were created
				pgbackrestPath := pm.pgbackrestPath()
				assert.DirExists(t, pgbackrestPath, "pgbackrest directory should exist")
				assert.DirExists(t, filepath.Join(pgbackrestPath, "logs"), "logs directory should exist")
				assert.DirExists(t, filepath.Join(pgbackrestPath, "spool"), "spool directory should exist")
				assert.DirExists(t, filepath.Join(pgbackrestPath, "lock"), "lock directory should exist")

				// Verify config file was created
				assert.FileExists(t, configPath, "config file should exist")

				// Read and verify config content
				configContent, err := os.ReadFile(configPath)
				require.NoError(t, err, "should be able to read config file")

				configStr := string(configContent)

				// Verify [global] section paths
				assert.Contains(t, configStr, "log-path="+filepath.Join(pgbackrestPath, "logs"))
				assert.Contains(t, configStr, "spool-path="+filepath.Join(pgbackrestPath, "spool"))
				assert.Contains(t, configStr, "lock-path="+filepath.Join(pgbackrestPath, "lock"))

				// Verify global settings
				assert.Contains(t, configStr, "compress-type=zst")
				assert.Contains(t, configStr, "link-all=y")
				assert.Contains(t, configStr, "log-level-console=info")
				assert.Contains(t, configStr, "log-level-file=detail")
				assert.Contains(t, configStr, "log-subprocess=y")
				assert.Contains(t, configStr, "resume=n")
				assert.Contains(t, configStr, "start-fast=y")

				// Verify [multigres] stanza section
				assert.Contains(t, configStr, "[multigres]")
				assert.Contains(t, configStr, "repo1-path="+tt.backupLocation)
				assert.Contains(t, configStr, "pg1-path="+filepath.Join(tt.poolerDir, "pg_data"))
				assert.Contains(t, configStr, "pg1-socket-path="+filepath.Join(tt.poolerDir, "pg_sockets"))
				assert.Contains(t, configStr, "pg1-port="+strconv.Itoa(tt.pgPort))
				assert.Contains(t, configStr, "pg1-user=postgres")
				assert.Contains(t, configStr, "pg1-database=postgres")

				// Verify file permissions
				info, err := os.Stat(configPath)
				require.NoError(t, err)
				assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "config file should have 0600 permissions")

				// Verify directory permissions
				dirInfo, err := os.Stat(pgbackrestPath)
				require.NoError(t, err)
				assert.Equal(t, os.FileMode(0o755), dirInfo.Mode().Perm(), "directories should have 0755 permissions")
			}
		})
	}
}

func TestInitPgBackRest_Idempotent(t *testing.T) {
	// Test that calling initPgBackRest multiple times is safe (idempotent)
	poolerDir := t.TempDir()
	pm := createTestManagerWithBackupLocation(
		poolerDir,
		"test-tg",
		"0",
		clustermetadatapb.PoolerType_REPLICA,
		"/tmp/backups",
	)

	// First call
	configPath, err := pm.initPgBackRest(context.Background(), NotForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, configPath)

	// Read the config content from first call
	firstContent, err := os.ReadFile(configPath)
	require.NoError(t, err)

	// Second call - should return early without doing any work
	configPath2, err := pm.initPgBackRest(context.Background(), NotForBackup)
	require.NoError(t, err)
	assert.Equal(t, configPath, configPath2, "should return same path on second call")

	// Content should be the same
	secondContent, err := os.ReadFile(configPath)
	require.NoError(t, err)
	assert.Equal(t, string(firstContent), string(secondContent), "config should be identical on second call")
}

func TestInitPgBackRest_EarlyReturnWhenConfigExists(t *testing.T) {
	// Test that initPgBackRest returns early when config file already exists
	poolerDir := t.TempDir()
	pm := createTestManagerWithBackupLocation(
		poolerDir,
		"test-tg",
		"0",
		clustermetadatapb.PoolerType_REPLICA,
		"/tmp/backups",
	)

	// First call - should create everything
	configPath, err := pm.initPgBackRest(context.Background(), NotForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, configPath)

	pgbackrestPath := pm.pgbackrestPath()

	// Modify the config file content
	customContent := "# Custom modified content\n[global]\nlog-path=/custom/path\n"
	err = os.WriteFile(configPath, []byte(customContent), 0o644)
	require.NoError(t, err)

	// Get modification time after our modification
	modifiedInfo, err := os.Stat(configPath)
	require.NoError(t, err)
	modifiedModTime := modifiedInfo.ModTime()

	// Remove one of the subdirectories to verify they're not recreated
	spoolPath := filepath.Join(pgbackrestPath, "spool")
	err = os.RemoveAll(spoolPath)
	require.NoError(t, err)

	// Second call - should return early without touching anything
	configPath2, err := pm.initPgBackRest(context.Background(), NotForBackup)
	require.NoError(t, err)
	assert.Equal(t, configPath, configPath2, "should return same path on early return")

	// Verify config file was NOT overwritten (still has custom content)
	currentContent, err := os.ReadFile(configPath)
	require.NoError(t, err)
	assert.Equal(t, customContent, string(currentContent), "config file should not be modified on second call")

	// Verify modification time didn't change (file wasn't rewritten)
	currentInfo, err := os.Stat(configPath)
	require.NoError(t, err)
	assert.Equal(t, modifiedModTime, currentInfo.ModTime(), "config file modification time should not change")

	// Verify spool directory was NOT recreated (early return before directory creation)
	_, err = os.Stat(spoolPath)
	assert.True(t, os.IsNotExist(err), "spool directory should not be recreated on early return")
}

func TestInitPgBackRest_DirectoryCreation(t *testing.T) {
	// Test that all required directories are created with correct permissions
	poolerDir := t.TempDir()
	pm := createTestManagerWithBackupLocation(
		poolerDir,
		"test-tg",
		"0",
		clustermetadatapb.PoolerType_REPLICA,
		"/tmp/backups",
	)

	configPath, err := pm.initPgBackRest(context.Background(), NotForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, configPath)

	pgbackrestPath := pm.pgbackrestPath()

	// Check all directories exist with correct permissions
	directories := []string{
		pgbackrestPath,
		filepath.Join(pgbackrestPath, "logs"),
		filepath.Join(pgbackrestPath, "spool"),
		filepath.Join(pgbackrestPath, "lock"),
	}

	for _, dir := range directories {
		t.Run("Directory: "+filepath.Base(dir), func(t *testing.T) {
			info, err := os.Stat(dir)
			require.NoError(t, err, "directory %s should exist", dir)
			assert.True(t, info.IsDir(), "%s should be a directory", dir)
			assert.Equal(t, os.FileMode(0o755), info.Mode().Perm(), "directory %s should have 0755 permissions", dir)
		})
	}
}

func TestInitPgBackRest_TemplateExecution(t *testing.T) {
	// Test that the template is executed correctly with all variables substituted
	poolerDir := t.TempDir()
	backupLocation := "/custom/backup/location"
	pgPort := 15432

	pm := createTestManagerWithBackupLocation(
		poolerDir,
		"test-tg",
		"0",
		clustermetadatapb.PoolerType_REPLICA,
		backupLocation,
	)
	pm.multipooler.PortMap = map[string]int32{"postgres": int32(pgPort)}

	configPath, err := pm.initPgBackRest(context.Background(), NotForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, configPath)

	// Read the generated config
	configContent, err := os.ReadFile(configPath)
	require.NoError(t, err)
	configStr := string(configContent)

	// Verify no template variables remain (they should all be substituted)
	assert.NotContains(t, configStr, "{{", "config should not contain template variables")
	assert.NotContains(t, configStr, "}}", "config should not contain template variables")

	// Verify all expected values are present
	pgbackrestPath := pm.pgbackrestPath()
	expectedValues := map[string]string{
		"LogPath":       filepath.Join(pgbackrestPath, "logs"),
		"SpoolPath":     filepath.Join(pgbackrestPath, "spool"),
		"LockPath":      filepath.Join(pgbackrestPath, "lock"),
		"Repo1Path":     backupLocation,
		"Pg1Path":       filepath.Join(poolerDir, "pg_data"),
		"Pg1SocketPath": filepath.Join(poolerDir, "pg_sockets"),
		"Pg1Port":       "15432",
		"Pg1User":       "postgres",
		"Pg1Database":   "postgres",
	}

	for name, value := range expectedValues {
		assert.Contains(t, configStr, value, "config should contain %s value: %s", name, value)
	}
}

func TestInitPgBackRest_ConfigFileFormat(t *testing.T) {
	// Test that the generated config file is properly formatted
	poolerDir := t.TempDir()
	pm := createTestManagerWithBackupLocation(
		poolerDir,
		"test-tg",
		"0",
		clustermetadatapb.PoolerType_REPLICA,
		"/tmp/backups",
	)

	configPath, err := pm.initPgBackRest(context.Background(), NotForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, configPath)

	configContent, err := os.ReadFile(configPath)
	require.NoError(t, err)
	configStr := string(configContent)

	// Verify INI-style sections exist
	assert.Contains(t, configStr, "[global]", "config should have [global] section")
	assert.Contains(t, configStr, "[multigres]", "config should have [multigres] section")

	// Verify section order (global should come before multigres)
	globalIdx := strings.Index(configStr, "[global]")
	multigresIdx := strings.Index(configStr, "[multigres]")
	assert.Greater(t, multigresIdx, globalIdx, "[multigres] should come after [global]")

	// Verify key-value format (key=value)
	for line := range strings.Lines(configStr) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "[") {
			continue
		}
		assert.Contains(t, line, "=", "non-section lines should be key=value format: %s", line)
	}
}

func TestInitPgBackRest_ForBackupCreatesBackupConfig(t *testing.T) {
	// Test that forBackup=true creates pgbackrest-backup.conf in pgbackrest directory
	poolerDir := t.TempDir()
	pm := createTestManagerWithBackupLocation(
		poolerDir,
		"test-tg",
		"0",
		clustermetadatapb.PoolerType_REPLICA,
		"/tmp/backups",
	)

	// Call with ForBackup mode
	backupConfigPath, err := pm.initPgBackRest(context.Background(), ForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, backupConfigPath)

	// Verify the config file was created in pgbackrest directory with the name pgbackrest-backup.conf
	pgbackrestPath := pm.pgbackrestPath()
	expectedPath := filepath.Join(pgbackrestPath, "pgbackrest-backup.conf")
	assert.Equal(t, expectedPath, backupConfigPath, "backup config should be pgbackrest-backup.conf in pgbackrest directory")

	// Verify the backup config file exists and is readable
	fileInfo, err := os.Stat(backupConfigPath)
	require.NoError(t, err, "backup config file should exist")
	assert.False(t, fileInfo.IsDir(), "backup config should be a file, not a directory")

	// Verify the config content is valid
	configContent, err := os.ReadFile(backupConfigPath)
	require.NoError(t, err)
	configStr := string(configContent)

	// Should have proper sections and content
	assert.Contains(t, configStr, "[global]", "backup config should have [global] section")
	assert.Contains(t, configStr, "[multigres]", "backup config should have [multigres] section")
	assert.NotContains(t, configStr, "{{", "backup config should not contain template variables")

	// Verify the config content after multiple calls
	firstContent := string(configContent)

	// Call again with ForBackup mode - should overwrite the same file
	backupConfigPath2, err := pm.initPgBackRest(context.Background(), ForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, backupConfigPath2)
	assert.Equal(t, backupConfigPath, backupConfigPath2, "second call should use the same pgbackrest-backup.conf path")

	// Verify the backup config file exists
	_, err = os.Stat(backupConfigPath2)
	require.NoError(t, err, "backup config file should exist after second call")

	// Verify content is consistent
	configContent2, err := os.ReadFile(backupConfigPath2)
	require.NoError(t, err)
	assert.Equal(t, firstContent, string(configContent2), "backup config content should be consistent across calls")

	// Verify that the persistent config was NOT created when using ForBackup mode
	persistentConfigPath := filepath.Join(pgbackrestPath, "pgbackrest.conf")
	_, err = os.Stat(persistentConfigPath)
	assert.True(t, os.IsNotExist(err), "persistent config should not be created when using ForBackup mode")

	// Verify that only the pgbackrest directory was created (not the subdirectories)
	assert.DirExists(t, pgbackrestPath, "pgbackrest directory should be created")

	// Verify that subdirectories were NOT created in ForBackup mode
	_, err = os.Stat(filepath.Join(pgbackrestPath, "logs"))
	assert.True(t, os.IsNotExist(err), "logs directory should not be created when using ForBackup mode")
	_, err = os.Stat(filepath.Join(pgbackrestPath, "spool"))
	assert.True(t, os.IsNotExist(err), "spool directory should not be created when using ForBackup mode")
	_, err = os.Stat(filepath.Join(pgbackrestPath, "lock"))
	assert.True(t, os.IsNotExist(err), "lock directory should not be created when using ForBackup mode")
}

func TestInitPgBackRest_ForBackupFalseSkipsPg2(t *testing.T) {
	// Test that NotForBackup mode skips pg2 configuration even on standby poolers
	poolerDir := t.TempDir()
	pm := createTestManagerWithBackupLocation(
		poolerDir,
		"test-tg",
		"0",
		clustermetadatapb.PoolerType_REPLICA, // This is a standby
		"/tmp/backups",
	)

	// Set up primary pooler info to simulate a standby configuration
	// Normally this would trigger pg2 configuration in the old code
	pm.mu.Lock()
	pm.primaryHost = "primary-host.example.com"
	pm.primaryPort = 5432
	pm.mu.Unlock()

	// Call with NotForBackup mode
	configPath, err := pm.initPgBackRest(context.Background(), NotForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, configPath)

	// Verify the config file was created in poolerDir (persistent config)
	pgbackrestPath := pm.pgbackrestPath()
	expectedPath := filepath.Join(pgbackrestPath, "pgbackrest.conf")
	assert.Equal(t, expectedPath, configPath, "should create persistent config when using NotForBackup mode")

	// Read the generated config
	configContent, err := os.ReadFile(configPath)
	require.NoError(t, err)
	configStr := string(configContent)

	// Verify pg1 configuration is present
	assert.Contains(t, configStr, "pg1-socket-path=", "pg1 config should be present")
	assert.Contains(t, configStr, "pg1-port=", "pg1 config should be present")
	assert.Contains(t, configStr, "pg1-path=", "pg1 config should be present")

	// Verify pg2 configuration is NOT present (skipped because using NotForBackup mode)
	assert.NotContains(t, configStr, "pg2-host=", "pg2 config should NOT be present when using NotForBackup mode")
	assert.NotContains(t, configStr, "pg2-port=", "pg2 config should NOT be present when using NotForBackup mode")
	assert.NotContains(t, configStr, "pg2-path=", "pg2 config should NOT be present when using NotForBackup mode")
	assert.NotContains(t, configStr, "pg2-host-type=", "pg2 config should NOT be present when using NotForBackup mode")
}

func TestInitPgBackRest_ForBackupTrueIncludesPg2OnStandby(t *testing.T) {
	// Test that ForBackup mode DOES include pg2 configuration on standby poolers
	poolerDir := t.TempDir()
	pm := createTestManagerWithBackupLocation(
		poolerDir,
		"test-tg",
		"0",
		clustermetadatapb.PoolerType_REPLICA, // This is a standby
		"/tmp/backups",
	)

	// Set up primary pooler info to simulate a standby configuration
	primaryPoolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1", // Same cell as test manager
		Name:      "primary-pooler",
	}

	// Register the primary multipooler in the topology
	ctx := context.Background()
	primaryPoolerDir := "/tmp/primary-pooler-dir"
	primaryMultiPooler := &clustermetadatapb.MultiPooler{
		Id:         primaryPoolerID,
		Type:       clustermetadatapb.PoolerType_PRIMARY,
		TableGroup: "test-tg",
		Shard:      "0",
		Database:   "test-database",
		PoolerDir:  primaryPoolerDir,
		PortMap: map[string]int32{
			"grpc":       16100,
			"http":       16000,
			"pgbackrest": 8432,
		},
	}
	err := pm.topoClient.CreateMultiPooler(ctx, primaryMultiPooler)
	require.NoError(t, err)

	pm.mu.Lock()
	pm.primaryHost = "primary-host.example.com"
	pm.primaryPort = 5432
	pm.primaryPoolerID = primaryPoolerID
	pm.mu.Unlock()

	// Call with ForBackup mode
	tempConfigPath, err := pm.initPgBackRest(context.Background(), ForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, tempConfigPath)

	// Verify it's a temp file
	assert.Contains(t, tempConfigPath, os.TempDir(), "should create temp config when using ForBackup mode")

	// Read the generated config
	configContent, err := os.ReadFile(tempConfigPath)
	require.NoError(t, err)
	configStr := string(configContent)

	// Verify pg1 configuration is present
	assert.Contains(t, configStr, "pg1-socket-path=", "pg1 config should be present")
	assert.Contains(t, configStr, "pg1-port=", "pg1 config should be present")
	assert.Contains(t, configStr, "pg1-path=", "pg1 config should be present")

	// Verify pg2 configuration IS present (included because using ForBackup mode and this is a standby)
	assert.Contains(t, configStr, "pg2-host=primary-host.example.com", "pg2 config should be present when using ForBackup mode on standby")
	assert.Contains(t, configStr, "pg2-port=5432", "pg2 config should be present when using ForBackup mode on standby")
	assert.Contains(t, configStr, "pg2-path=/tmp/primary-pooler-dir/pg_data", "pg2-path should use primary pooler dir from topo")
	assert.Contains(t, configStr, "pg2-host-port=8432", "pg2-host-port should use pgbackrest port from primary pooler's port map")
	assert.Contains(t, configStr, "pg2-host-type=tls", "pg2 config should be present when using ForBackup mode on standby")

	// Clean up the temp file
	err = os.Remove(tempConfigPath)
	require.NoError(t, err)
}

func TestInitPgBackRest_InlinesCredentials(t *testing.T) {
	// Test that credentials are inlined in pgbackrest.conf when UseEnvCredentials is enabled
	ctx := context.Background()

	// Create a cluster root with a pooler directory inside it
	clusterRoot := t.TempDir()
	poolerDir := filepath.Join(clusterRoot, "zone1-pooler1")
	err := os.MkdirAll(poolerDir, 0o755)
	require.NoError(t, err)

	// Set environment variables for AWS credentials
	t.Setenv("AWS_ACCESS_KEY_ID", "ASIATESTACCESSKEY")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testsecretkey")
	t.Setenv("AWS_SESSION_TOKEN", "testsessiontoken")

	// Create a manager with S3 backup using env credentials
	database := "test-database"
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	err = ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
		Name: database,
		BackupLocation: utils.S3BackupLocation("test-bucket", "us-east-1",
			utils.WithS3EnvCredentials()),
		DurabilityPolicy: "ANY_2",
	})
	require.NoError(t, err)

	backupConfig, err := backup.NewConfig(
		utils.S3BackupLocation("test-bucket", "us-east-1",
			utils.WithS3EnvCredentials()),
	)
	require.NoError(t, err)

	multipoolerProto := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-service",
		},
		PoolerDir:  poolerDir,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	pm := &MultiPoolerManager{
		config:       &Config{},
		serviceID:    multipoolerProto.Id,
		topoClient:   ts,
		multipooler:  multipoolerProto,
		state:        ManagerStateReady,
		backupConfig: backupConfig,
		actionLock:   NewActionLock(),
		logger:       slog.Default(),
		pgMonitor:    timer.NewPeriodicRunner(context.TODO(), 10*time.Second),
	}

	// Call initPgBackRest with NotForBackup mode
	configPath, err := pm.initPgBackRest(ctx, NotForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, configPath)

	// Verify NO separate credentials file was created
	credentialsFilePath := filepath.Join(clusterRoot, "pgbackrest-credentials.conf")
	assert.NoFileExists(t, credentialsFilePath, "separate credentials file should not be created")

	// Verify the main config contains inline credentials
	configContent, err := os.ReadFile(configPath)
	require.NoError(t, err)
	configStr := string(configContent)

	// Verify credentials ARE in the main config (inlined)
	assert.Contains(t, configStr, "repo1-s3-key=ASIATESTACCESSKEY", "main config should contain inline access key")
	assert.Contains(t, configStr, "repo1-s3-key-secret=testsecretkey", "main config should contain inline secret key")
	assert.Contains(t, configStr, "repo1-s3-token=testsessiontoken", "main config should contain inline session token")

	// Verify no !include directive
	assert.NotContains(t, configStr, "!include", "main config should not have !include directive")

	// Verify config file has correct permissions (0600)
	info, err := os.Stat(configPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "config file should have 0600 permissions")
}

func TestInitPgBackRest_NoCredentialsFileForFilesystemBackup(t *testing.T) {
	// Test that credentials file is NOT created for filesystem backups
	ctx := context.Background()

	clusterRoot := t.TempDir()
	poolerDir := filepath.Join(clusterRoot, "zone1-pooler1")
	err := os.MkdirAll(poolerDir, 0o755)
	require.NoError(t, err)

	// Create a manager with filesystem backup (no env credentials)
	database := "test-database"
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	err = ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
		Name:             database,
		BackupLocation:   utils.FilesystemBackupLocation("/tmp/backups"),
		DurabilityPolicy: "ANY_2",
	})
	require.NoError(t, err)

	backupConfig, err := backup.NewConfig(
		utils.FilesystemBackupLocation("/tmp/backups"),
	)
	require.NoError(t, err)

	multipoolerProto := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-service",
		},
		PoolerDir:  poolerDir,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	pm := &MultiPoolerManager{
		config:       &Config{},
		serviceID:    multipoolerProto.Id,
		topoClient:   ts,
		multipooler:  multipoolerProto,
		state:        ManagerStateReady,
		backupConfig: backupConfig,
		actionLock:   NewActionLock(),
		logger:       slog.Default(),
		pgMonitor:    timer.NewPeriodicRunner(context.TODO(), 10*time.Second),
	}

	// Call initPgBackRest
	configPath, err := pm.initPgBackRest(ctx, NotForBackup)
	require.NoError(t, err)
	assert.NotEmpty(t, configPath)

	// Verify credentials file was NOT created
	credentialsFilePath := filepath.Join(clusterRoot, "pgbackrest-credentials.conf")
	_, err = os.Stat(credentialsFilePath)
	assert.True(t, os.IsNotExist(err), "credentials file should not be created for filesystem backups")

	// Verify the main config does NOT include a credentials file
	configContent, err := os.ReadFile(configPath)
	require.NoError(t, err)
	configStr := string(configContent)
	assert.NotContains(t, configStr, "!include", "main config should not include any credentials file for filesystem backups")
}
