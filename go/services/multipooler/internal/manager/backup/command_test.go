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

package backup

import (
	"bufio"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

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
			cmd := executil.Command(t.Context(), tt.command, tt.args...)
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
		cmd := executil.Command(t.Context(), "sh", "-c", "for i in $(seq 1 200); do echo \"Line $i\"; done")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Equal(t, 200, len(lines), "Should capture all 200 lines")
		assert.Contains(t, output, "Line 1")
		assert.Contains(t, output, "Line 200")
	})

	// Test with very large output (thousands of lines)
	t.Run("Very large output (1000 lines)", func(t *testing.T) {
		cmd := executil.Command(t.Context(), "sh", "-c", "for i in $(seq 1 1000); do echo \"Line $i\"; done")
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
		cmd := executil.Command(t.Context(), "sh", "-c", script)
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
		cmd := executil.Command(t.Context(), "echo", longString)
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		assert.Contains(t, output, longString)
	})

	// Test with multiple long lines
	t.Run("Multiple long lines", func(t *testing.T) {
		// Generate 10 lines of 5KB each
		script := "for i in $(seq 1 10); do printf '%s\\n' \"$(printf 'x%.0s' {1..5000})\"; done"
		cmd := executil.Command(t.Context(), "bash", "-c", script)
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Equal(t, 10, len(lines))
		for _, line := range lines {
			assert.Greater(t, len(line), 4000, "Each line should be at least 4KB")
		}
	})
}

// TestSafeCombinedOutput_LineExceedsScannerLimit verifies that a line
// longer than bufio.Scanner's default buffer (64 KiB) does not silently
// truncate the output: safeCombinedOutput surfaces the underlying
// bufio.ErrTooLong as a wrapped error instead of returning a clean nil.
// Without the scanner.Err() check inside the reader goroutine, this case
// previously looked like a successful command with truncated output.
func TestSafeCombinedOutput_LineExceedsScannerLimit(t *testing.T) {
	// printf with no newline emits a single token larger than the
	// default 64 KiB scanner buffer.
	overlong := strings.Repeat("a", bufio.MaxScanTokenSize+1024)
	script := "printf %s '" + overlong + "'"
	cmd := executil.Command(t.Context(), "bash", "-c", script)

	_, err := safeCombinedOutput(cmd)
	require.Error(t, err, "scanner error on overlong line must be surfaced")
	assert.True(t, errors.Is(err, bufio.ErrTooLong),
		"expected wrapped bufio.ErrTooLong, got %v", err)
}

func TestSafeCombinedOutput_RapidOutput(t *testing.T) {
	// Test with very rapid output to stress-test the channel and goroutine coordination
	t.Run("Rapid burst of output", func(t *testing.T) {
		// Use yes command to generate rapid output, limited by head
		cmd := executil.Command(t.Context(), "sh", "-c", "yes 'rapid output line' | head -n 500")
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
		cmd := executil.Command(t.Context(), "sh", "-c", script)
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
		cmd := executil.Command(t.Context(), "echo", "only stdout")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		assert.Equal(t, "only stdout\n", output)
	})

	t.Run("Only stderr", func(t *testing.T) {
		cmd := executil.Command(t.Context(), "sh", "-c", "echo 'only stderr' >&2")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		assert.Contains(t, output, "only stderr")
	})

	t.Run("Neither stdout nor stderr", func(t *testing.T) {
		cmd := executil.Command(t.Context(), "true")
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
		cmd := executil.Command(t.Context(), "sh", "-c", script)
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
		cmd := executil.Command(t.Context(), "echo", "test")
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
		cmd := executil.Command(context.Background(), "sh", "-c", script)
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
		cmd := executil.Command(context.Background(), "sh", "-c", "printf 'text\\x00with\\x00nulls\\n'")
		output, err := safeCombinedOutput(cmd)

		require.NoError(t, err)
		// Should handle the output without crashing
		assert.NotEmpty(t, output)
	})
}

func TestSafeCombinedOutput_CommandNotFound(t *testing.T) {
	t.Run("Command does not exist", func(t *testing.T) {
		cmd := executil.Command(context.Background(), "this-command-definitely-does-not-exist-12345")
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
		cmd := executil.Command(context.Background(), "sh", "-c", script)
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
		cmd := executil.Command(context.Background(), "sh", "-c", script)
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
			cmd := executil.Command(context.Background(), "echo", "hello world")
			_, _ = safeCombinedOutput(cmd)
		}
	})

	b.Run("Medium output (100 lines)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cmd := executil.Command(context.Background(), "sh", "-c", "for i in $(seq 1 100); do echo \"Line $i\"; done")
			_, _ = safeCombinedOutput(cmd)
		}
	})

	b.Run("Large output (1000 lines)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cmd := executil.Command(context.Background(), "sh", "-c", "for i in $(seq 1 1000); do echo \"Line $i\"; done")
			_, _ = safeCombinedOutput(cmd)
		}
	})

	b.Run("Mixed stdout and stderr", func(b *testing.B) {
		script := "for i in $(seq 1 100); do echo \"stdout $i\"; echo \"stderr $i\" >&2; done"
		for i := 0; i < b.N; i++ {
			cmd := executil.Command(context.Background(), "sh", "-c", script)
			_, _ = safeCombinedOutput(cmd)
		}
	})
}
