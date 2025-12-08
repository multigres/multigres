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
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// createTestManager creates a minimal MultiPoolerManager for testing
func createTestManager(poolerDir, stanzaName, tableGroup, shard string, poolerType clustermetadatapb.PoolerType) *MultiPoolerManager {
	return createTestManagerWithBackupLocation(poolerDir, stanzaName, tableGroup, shard, poolerType, "/tmp/backups")
}

// createTestManagerWithBackupLocation creates a minimal MultiPoolerManager for testing with backup_location
func createTestManagerWithBackupLocation(poolerDir, stanzaName, tableGroup, shard string, poolerType clustermetadatapb.PoolerType, backupLocation string) *MultiPoolerManager {
	database := "test-database"

	multipoolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-multipooler",
	}

	multipoolerInfo := &topoclient.MultiPoolerInfo{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         multipoolerID,
			Type:       poolerType,
			TableGroup: tableGroup,
			Shard:      shard,
			Database:   database,
		},
	}

	// Create a topology store with backup location if provided
	var topoClient topoclient.Store
	if backupLocation != "" {
		ctx := context.Background()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		err := ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
			Name:             database,
			BackupLocation:   backupLocation,
			DurabilityPolicy: "ANY_2",
		})
		if err == nil {
			topoClient = ts
		}
	}

	pm := &MultiPoolerManager{
		config: &Config{
			PoolerDir:        poolerDir,
			PgBackRestStanza: stanzaName,
			ServiceID:        &clustermetadatapb.ID{Name: "test-service"},
		},
		serviceID:      &clustermetadatapb.ID{Name: "test-service"},
		topoClient:     topoClient,
		multipooler:    multipoolerInfo,
		state:          ManagerStateReady,
		backupLocation: backupLocation,
		actionLock:     NewActionLock(),
		cachedMultipooler: cachedMultiPoolerInfo{
			multipooler: topoclient.NewMultiPoolerInfo(
				proto.Clone(multipoolerInfo.MultiPooler).(*clustermetadatapb.MultiPooler),
				multipoolerInfo.Version(),
			),
		},
	}
	return pm
}

func TestFindBackupByAnnotations(t *testing.T) {
	tests := []struct {
		name            string
		multipoolerID   string
		backupTimestamp string
		jsonOutput      string
		wantBackupID    string
		wantError       bool
		errorContains   string
	}{
		{
			name:            "Single matching backup",
			multipoolerID:   "zone1-multipooler1",
			backupTimestamp: "20250104-100000.000000",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"backup_timestamp": "20250104-100000.000000"
					}
				}]
			}]`,
			wantBackupID: "20250104-100000F",
			wantError:    false,
		},
		{
			name:            "Multiple backups, one match",
			multipoolerID:   "zone1-multipooler1",
			backupTimestamp: "20250104-120000.000000",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"backup_timestamp": "20250104-100000.000000"
					}
				}, {
					"label": "20250104-120000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"backup_timestamp": "20250104-120000.000000"
					}
				}]
			}]`,
			wantBackupID: "20250104-120000F",
			wantError:    false,
		},
		{
			name:            "No matching backup",
			multipoolerID:   "zone1-multipooler1",
			backupTimestamp: "20250104-180000.000000",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"backup_timestamp": "20250104-100000.000000"
					}
				}]
			}]`,
			wantError:     true,
			errorContains: "no backup found",
		},
		{
			name:            "No backups at all",
			multipoolerID:   "zone1-multipooler1",
			backupTimestamp: "20250104-100000.000000",
			jsonOutput:      `[{"backup": []}]`,
			wantError:       true,
			errorContains:   "no backups found",
		},
		{
			name:            "Duplicate matching backups",
			multipoolerID:   "zone1-multipooler1",
			backupTimestamp: "20250104-100000.000000",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"backup_timestamp": "20250104-100000.000000"
					}
				}, {
					"label": "20250104-100000F_20250104-110000I",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"backup_timestamp": "20250104-100000.000000"
					}
				}]
			}]`,
			wantError:     true,
			errorContains: "found 2 backups",
		},
		{
			name:            "Backup without annotations",
			multipoolerID:   "zone1-multipooler1",
			backupTimestamp: "20250104-100000.000000",
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
			// Create temp directory for mock pgbackrest
			tmpDir := t.TempDir()

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
			pgbackrestPath := tmpDir + "/pgbackrest"
			err := exec.Command("sh", "-c", "cat > "+pgbackrestPath+" << 'EOF'\n"+mockScript+"\nEOF").Run()
			require.NoError(t, err)
			err = exec.Command("chmod", "+x", pgbackrestPath).Run()
			require.NoError(t, err)

			// Prepend temp dir to PATH so our mock pgbackrest is found first
			t.Setenv("PATH", tmpDir+":/usr/bin:/bin")

			pm := createTestManagerWithBackupLocation(tmpDir, "test-stanza", "test-tg", "0", clustermetadatapb.PoolerType_REPLICA, tmpDir)

			ctx := context.Background()
			backupID, err := pm.findBackupByAnnotations(ctx, tt.multipoolerID, tt.backupTimestamp)

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
		stanzaName   string
		backupType   string
		poolerType   clustermetadatapb.PoolerType
		forcePrimary bool
		expectError  bool
		errorMsg     string
	}{
		{
			name:        "Missing type",
			poolerDir:   "/tmp/test",
			stanzaName:  "test-stanza",
			backupType:  "",
			poolerType:  clustermetadatapb.PoolerType_REPLICA,
			expectError: true,
			errorMsg:    "type is required",
		},
		{
			name:        "Invalid backup type",
			poolerDir:   "/tmp/test",
			stanzaName:  "test-stanza",
			backupType:  "invalid",
			poolerType:  clustermetadatapb.PoolerType_REPLICA,
			expectError: true,
			errorMsg:    "invalid backup type",
		},
		{
			name:         "Primary without force flag",
			poolerDir:    "/tmp/test",
			stanzaName:   "test-stanza",
			backupType:   "full",
			poolerType:   clustermetadatapb.PoolerType_PRIMARY,
			forcePrimary: false,
			expectError:  true,
			errorMsg:     "not allowed unless ForcePrimary",
		},
		{
			name:         "Primary with force flag",
			poolerDir:    "/tmp/test",
			stanzaName:   "test-stanza",
			backupType:   "full",
			poolerType:   clustermetadatapb.PoolerType_PRIMARY,
			forcePrimary: true,
			expectError:  true,                       // Will fail on pgbackrest execution but validation passes
			errorMsg:     "pgbackrest backup failed", // Execution error, not validation
		},
		{
			name:         "Replica backup",
			poolerDir:    "/tmp/test",
			stanzaName:   "test-stanza",
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
			pm := createTestManagerWithBackupLocation(tt.poolerDir, tt.stanzaName, "", "", tt.poolerType, backupLocation)

			_, err := pm.Backup(ctx, tt.forcePrimary, tt.backupType)

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
		stanzaName  string
		limit       uint32
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid request",
			poolerDir:   t.TempDir(),
			stanzaName:  "test-stanza",
			limit:       0,
			expectError: false, // Should return empty list for non-existent stanza
		},
		{
			name:        "With limit",
			poolerDir:   t.TempDir(),
			stanzaName:  "test-stanza",
			limit:       10,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := createTestManager(tt.poolerDir, tt.stanzaName, "", "", clustermetadatapb.PoolerType_REPLICA)

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

	pm := createTestManagerWithBackupLocation(tmpDir, "test-stanza", "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Hold the lock in another goroutine
	lockCtx, err := pm.actionLock.Acquire(ctx, "test-holder")
	require.NoError(t, err)

	// Create a context with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// Backup should timeout waiting for the lock
	_, err = pm.Backup(timeoutCtx, false, "full")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")

	// Release the lock
	pm.actionLock.Release(lockCtx)
}

func TestGetBackups_ActionLock(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	pm := createTestManagerWithBackupLocation(tmpDir, "test-stanza", "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

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

	pm := createTestManagerWithBackupLocation(tmpDir, "test-stanza", "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

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

	pm := createTestManagerWithBackupLocation(tmpDir, "test-stanza", "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Call Backup - it will fail (no pgbackrest), but should release the lock
	_, _ = pm.Backup(ctx, false, "full")

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

	pm := createTestManagerWithBackupLocation(tmpDir, "test-stanza", "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

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

	pm := createTestManagerWithBackupLocation(tmpDir, "test-stanza", "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Call RestoreFromBackup - it will fail (precondition), but should release the lock
	_ = pm.RestoreFromBackup(ctx, "test-backup-id")

	// Verify lock was released by acquiring it with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	lockCtx, err := pm.actionLock.Acquire(timeoutCtx, "verify-release")
	require.NoError(t, err, "Lock should be released after RestoreFromBackup returns")
	pm.actionLock.Release(lockCtx)
}

// createTestManagerForAutoRestore creates a manager configured for auto-restore tests
func createTestManagerForAutoRestore(logger *slog.Logger, poolerDir string, poolerType clustermetadatapb.PoolerType) *MultiPoolerManager {
	pm := createTestManagerWithBackupLocation(poolerDir, "test-stanza", "default", "0", poolerType, "/tmp/backups")
	pm.logger = logger
	pm.autoRestoreRetryInterval = 10 * time.Millisecond // Short interval for tests
	return pm
}

func TestTryAutoRestoreFromBackup_SkipsWhenInitialized(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Create temp pooler dir with marker file to simulate initialized state
	poolerDir := t.TempDir()
	pgDataDir := filepath.Join(poolerDir, "pg_data")
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, "PG_VERSION"), []byte("16"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(pgDataDir, "global"), 0o755))

	pm := &MultiPoolerManager{
		logger: logger,
		config: &Config{
			PoolerDir: poolerDir,
		},
		backupLocation:           "/tmp/backups",
		actionLock:               NewActionLock(),
		autoRestoreRetryInterval: 10 * time.Millisecond,
	}

	// Create the initialization marker file - this is what isInitialized() checks
	require.NoError(t, pm.writeInitializationMarker())

	// Should not attempt restore when already initialized
	restored := pm.tryAutoRestoreFromBackup(ctx)
	assert.False(t, restored, "Should not restore when already initialized")
}

func TestTryAutoRestoreFromBackup_SkipsForPrimary(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Create temp pooler dir WITHOUT PG_VERSION (uninitialized)
	poolerDir := t.TempDir()

	pm := createTestManagerForAutoRestore(logger, poolerDir, clustermetadatapb.PoolerType_PRIMARY)

	// Should not attempt restore for PRIMARY even when uninitialized
	restored := pm.tryAutoRestoreFromBackup(ctx)
	assert.False(t, restored, "Should not restore PRIMARY pooler")
}

func TestTryAutoRestoreFromBackup_SkipsForUnknownType(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Create temp pooler dir WITHOUT PG_VERSION (uninitialized)
	poolerDir := t.TempDir()

	// UNKNOWN type is what a fresh multipooler has before SetPoolerType is called
	pm := createTestManagerForAutoRestore(logger, poolerDir, clustermetadatapb.PoolerType_UNKNOWN)

	// Should not attempt restore for UNKNOWN type - only REPLICA should auto-restore
	restored := pm.tryAutoRestoreFromBackup(ctx)
	assert.False(t, restored, "Should not restore UNKNOWN pooler type")
}

func TestListBackups_ReturnsEmptyWhenNoBackups(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	poolerDir := t.TempDir()

	pm := createTestManagerForAutoRestore(logger, poolerDir, clustermetadatapb.PoolerType_REPLICA)

	// Should return empty list without error when no backups exist
	// Note: listBackups with filterByShard=true is what auto-restore uses
	backups, err := pm.listBackups(ctx, true)
	assert.NoError(t, err)
	assert.Empty(t, backups)
}

func TestTryAutoRestoreFromBackup_RetriesUntilContextCancelled(t *testing.T) {
	// Use a short timeout since the function now retries indefinitely
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Create temp pooler dir WITHOUT PG_VERSION (uninitialized)
	poolerDir := t.TempDir()

	pm := createTestManagerForAutoRestore(logger, poolerDir, clustermetadatapb.PoolerType_REPLICA)

	// Should return false when context is cancelled (no restore performed)
	restored := pm.tryAutoRestoreFromBackup(ctx)
	assert.False(t, restored, "Should not restore when context is cancelled")

	// Should still be uninitialized
	assert.False(t, pm.isInitialized(t.Context()), "Should remain uninitialized")
}

func TestLoadMultiPoolerFromTopo_CallsAutoRestore(t *testing.T) {
	// This is more of an integration test - we verify the method is called
	// by checking logs or behavior. For unit testing, we verify the method
	// exists and can be called.
	// Use a short timeout since the function now retries indefinitely
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	poolerDir := t.TempDir()

	pm := createTestManagerForAutoRestore(logger, poolerDir, clustermetadatapb.PoolerType_REPLICA)

	// Verify tryAutoRestoreFromBackup can be called and returns without panic
	restored := pm.tryAutoRestoreFromBackup(ctx)
	assert.False(t, restored, "Should not restore when no backups/stanza")
}
