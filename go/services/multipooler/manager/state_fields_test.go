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

package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMinimalManager() *MultiPoolerManager {
	return &MultiPoolerManager{
		state: ManagerStateReady,
	}
}

func TestState_DefaultFields(t *testing.T) {
	t.Parallel()
	pm := newMinimalManager()

	resp, err := pm.State(context.Background())
	require.NoError(t, err)

	assert.Equal(t, string(ManagerStateReady), resp.State)
	assert.False(t, resp.BackupInProgress, "backup_in_progress should default to false")
	assert.Nil(t, resp.LastIntegrityCheckPassed, "last_integrity_check_passed should be nil by default")
	assert.Empty(t, resp.LastIntegrityCheckError, "last_integrity_check_error should be empty by default")
	assert.Empty(t, resp.StanzaError, "stanza_error should be empty by default")
}

func TestState_BackupInProgress(t *testing.T) {
	t.Parallel()
	pm := newMinimalManager()

	pm.mu.Lock()
	pm.backupInProgress = true
	pm.mu.Unlock()

	resp, err := pm.State(context.Background())
	require.NoError(t, err)
	assert.True(t, resp.BackupInProgress)
}

func TestState_IntegrityCheckFields(t *testing.T) {
	t.Parallel()
	pm := newMinimalManager()

	passed := true
	pm.mu.Lock()
	pm.lastIntegrityCheckPassed = &passed
	pm.lastIntegrityCheckError = ""
	pm.mu.Unlock()

	resp, err := pm.State(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resp.LastIntegrityCheckPassed)
	assert.True(t, *resp.LastIntegrityCheckPassed)
	assert.Empty(t, resp.LastIntegrityCheckError)
}

func TestState_IntegrityCheckFailed(t *testing.T) {
	t.Parallel()
	pm := newMinimalManager()

	failed := false
	pm.mu.Lock()
	pm.lastIntegrityCheckPassed = &failed
	pm.lastIntegrityCheckError = "stanza mismatch"
	pm.mu.Unlock()

	resp, err := pm.State(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resp.LastIntegrityCheckPassed)
	assert.False(t, *resp.LastIntegrityCheckPassed)
	assert.Equal(t, "stanza mismatch", resp.LastIntegrityCheckError)
}

func TestState_StanzaError(t *testing.T) {
	t.Parallel()
	pm := newMinimalManager()

	pm.mu.Lock()
	pm.lastStanzaError = "028"
	pm.mu.Unlock()

	resp, err := pm.State(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "028", resp.StanzaError)
}

func TestState_StanzaErrorUnknown(t *testing.T) {
	t.Parallel()
	pm := newMinimalManager()

	pm.mu.Lock()
	pm.lastStanzaError = "unknown"
	pm.mu.Unlock()

	resp, err := pm.State(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "unknown", resp.StanzaError)
}

// TestStanzaErrorMapping tests the production classifyStanzaError helper
// that maps pgbackrest output to the stanza_error field value.
func TestStanzaErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		output      string
		expectError string
	}{
		{
			name:        "028 system identifier mismatch",
			output:      "ERROR: [028]: stanza 'multigres' already exists on repo1 with a different system-id",
			expectError: "028",
		},
		{
			name:        "other stanza error",
			output:      "ERROR: [056]: unable to find stanza 'multigres'",
			expectError: "unknown",
		},
		{
			name:        "generic failure without error code",
			output:      "connection refused",
			expectError: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := classifyStanzaError(tt.output)
			assert.Equal(t, tt.expectError, result)
		})
	}
}

// TestStanzaErrorClearedOnSuccess verifies that the production clearStanzaError()
// method clears a previously recorded stanza error.
func TestStanzaErrorClearedOnSuccess(t *testing.T) {
	t.Parallel()
	pm := newMinimalManager()

	// Set a previous error (simulating a prior failed stanza-create)
	pm.mu.Lock()
	pm.lastStanzaError = "028"
	pm.mu.Unlock()

	// Call the production success path
	pm.clearStanzaError()

	resp, err := pm.State(context.Background())
	require.NoError(t, err)
	assert.Empty(t, resp.StanzaError)
}

// TestIntegrityCheckStateTransitions verifies that integrity check results
// are correctly stored and surfaced through State().
func TestIntegrityCheckStateTransitions(t *testing.T) {
	t.Parallel()
	pm := newMinimalManager()

	// Initially nil
	resp, err := pm.State(context.Background())
	require.NoError(t, err)
	assert.Nil(t, resp.LastIntegrityCheckPassed)

	// After a passing check
	passed := true
	pm.mu.Lock()
	pm.lastIntegrityCheckPassed = &passed
	pm.lastIntegrityCheckError = ""
	pm.mu.Unlock()

	resp, err = pm.State(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resp.LastIntegrityCheckPassed)
	assert.True(t, *resp.LastIntegrityCheckPassed)

	// After a failing check
	failed := false
	pm.mu.Lock()
	pm.lastIntegrityCheckPassed = &failed
	pm.lastIntegrityCheckError = "WAL archive check failed"
	pm.mu.Unlock()

	resp, err = pm.State(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resp.LastIntegrityCheckPassed)
	assert.False(t, *resp.LastIntegrityCheckPassed)
	assert.Equal(t, "WAL archive check failed", resp.LastIntegrityCheckError)
}
