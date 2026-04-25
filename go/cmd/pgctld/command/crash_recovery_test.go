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
	"context"
	"errors"
	"testing"
	"time"

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

// lockHeldOutput is the FATAL message postgres --single emits when the
// postmaster.pid lock is still held — both during the orphan-cleanup window
// after a crash and when postgres is genuinely running.
var lockHeldOutput = []byte(`FATAL:  lock file "postmaster.pid" already exists
HINT:  Is another postmaster (PID 12345) running in data directory "/data"?`)

// TestRunCrashRecovery_RetriesDuringOrphanCleanupWindow simulates a postmaster crash
// where orphaned workers keep the lock file held for the first few attempts and then
// release it. Recovery should succeed once the lock clears.
func TestRunCrashRecovery_RetriesDuringOrphanCleanupWindow(t *testing.T) {
	const holdAttempts = 3

	calls := 0
	runner := func(ctx context.Context) ([]byte, error) {
		calls++
		if calls <= holdAttempts {
			return lockHeldOutput, errors.New("exit status 1")
		}
		return []byte("recovery complete"), nil
	}

	err := runCrashRecoveryAttempts(context.Background(), testLogger(), runner, time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, holdAttempts+1, calls,
		"runner should be retried until the lock clears, then succeed")
}

// TestRunCrashRecovery_LockNeverClearsReturnsNil locks in the historical behavior:
// if the lock is held for the full retry window, treat postgres as already running
// and return nil rather than surfacing an error.
func TestRunCrashRecovery_LockNeverClearsReturnsNil(t *testing.T) {
	calls := 0
	runner := func(ctx context.Context) ([]byte, error) {
		calls++
		return lockHeldOutput, errors.New("exit status 1")
	}

	err := runCrashRecoveryAttempts(context.Background(), testLogger(), runner, time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, crashRecoveryMaxAttempts, calls,
		"runner should be retried up to the max-attempts bound")
}

// TestRunCrashRecovery_FirstAttemptSucceeds covers the cleanly-shut-down hot path:
// no retries, no sleeps.
func TestRunCrashRecovery_FirstAttemptSucceeds(t *testing.T) {
	calls := 0
	runner := func(ctx context.Context) ([]byte, error) {
		calls++
		return []byte("recovery complete"), nil
	}

	err := runCrashRecoveryAttempts(context.Background(), testLogger(), runner, time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

// TestRunCrashRecovery_NonLockErrorReturnsImmediately verifies that non-lock
// errors fail fast and do not consume the retry budget — only the orphan-cleanup
// race should trigger retries.
func TestRunCrashRecovery_NonLockErrorReturnsImmediately(t *testing.T) {
	calls := 0
	runner := func(ctx context.Context) ([]byte, error) {
		calls++
		return []byte("FATAL:  could not access data directory"), errors.New("exit status 1")
	}

	err := runCrashRecoveryAttempts(context.Background(), testLogger(), runner, time.Millisecond)
	require.Error(t, err)
	assert.Equal(t, 1, calls, "non-lock errors must not be retried")
}

// TestRunCrashRecovery_ContextCancelledDuringBackoff verifies that a cancelled
// context aborts the retry loop without further runner invocations.
func TestRunCrashRecovery_ContextCancelledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	runner := func(ctx context.Context) ([]byte, error) {
		calls++
		cancel()
		return lockHeldOutput, errors.New("exit status 1")
	}

	err := runCrashRecoveryAttempts(ctx, testLogger(), runner, time.Hour)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls)
}
