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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// recoveryStatePath returns the path to the recovery state file
func recoveryStatePath(poolerDir string) string {
	dataDir := postgresDataDir(poolerDir)
	return filepath.Join(dataDir, "recovery", "recovery_state.json")
}

// getRecoveryState retrieves the current recovery state from disk.
// Returns nil if the file doesn't exist (normal case - no recovery pending).
func getRecoveryState(poolerDir string) (*RecoveryAction, error) {
	statePath := recoveryStatePath(poolerDir)

	// Check if recovery state file exists
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		// No recovery action needed (normal case)
		return nil, nil
	}

	// Read the file
	data, err := os.ReadFile(statePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read recovery state file: %w", err)
	}

	// Unmarshal JSON to struct
	action := &RecoveryAction{}
	if err := json.Unmarshal(data, action); err != nil {
		return nil, fmt.Errorf("failed to unmarshal recovery state: %w", err)
	}

	return action, nil
}

// setRecoveryState saves the recovery state to disk atomically.
// Uses temp file + rename pattern to ensure atomic writes.
func setRecoveryState(poolerDir string, action *RecoveryAction) error {
	// Check if data directory is initialized
	if !isDataDirInitialized(poolerDir) {
		dataDir := postgresDataDir(poolerDir)
		return fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	statePath := recoveryStatePath(poolerDir)
	recoveryDir := filepath.Dir(statePath)

	// Ensure recovery directory exists
	if err := os.MkdirAll(recoveryDir, 0o755); err != nil {
		return fmt.Errorf("failed to create recovery directory: %w", err)
	}

	// Marshal struct to JSON
	data, err := json.MarshalIndent(action, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal recovery state: %w", err)
	}

	// Write to file atomically using a temporary file
	tmpPath := statePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write recovery state file: %w", err)
	}

	// Rename to final path (atomic operation)
	if err := os.Rename(tmpPath, statePath); err != nil {
		os.Remove(tmpPath) // Clean up temp file on error
		return fmt.Errorf("failed to rename recovery state file: %w", err)
	}

	return nil
}

// clearRecoveryState removes the recovery state file.
// Idempotent: succeeds if no recovery state exists.
func clearRecoveryState(poolerDir string) error {
	statePath := recoveryStatePath(poolerDir)

	// Check if file exists
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		// Already cleared (idempotent)
		return nil
	}

	// Remove the file
	if err := os.Remove(statePath); err != nil {
		return fmt.Errorf("failed to remove recovery state file: %w", err)
	}

	return nil
}
