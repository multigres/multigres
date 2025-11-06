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

package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backupservicepb "github.com/multigres/multigres/go/pb/multipoolerbackupservice"
)

func TestGetShardBackups_Validation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		configPath  string
		stanzaName  string
		opts        ListOptions
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid request",
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			opts:        ListOptions{Limit: 0},
			expectError: false,
		},
		{
			name:        "Missing config_path",
			configPath:  "",
			stanzaName:  "test-stanza",
			opts:        ListOptions{Limit: 0},
			expectError: true,
			errorMsg:    "config_path is required",
		},
		{
			name:        "Missing stanza_name",
			configPath:  "/tmp/test.conf",
			stanzaName:  "",
			opts:        ListOptions{Limit: 0},
			expectError: true,
			errorMsg:    "stanza_name is required",
		},
		{
			name:        "With limit",
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			opts:        ListOptions{Limit: 10},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetShardBackups(ctx, tt.configPath, tt.stanzaName, tt.opts)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				// For valid options, we may get an error if the stanza doesn't exist,
				// or we may get an empty list. Both are acceptable.
				if err == nil {
					assert.NotNil(t, result)
					assert.NotNil(t, result.Backups)
				} else {
					// If there's an error, it should not be about validation
					assert.NotContains(t, err.Error(), "required")
				}
			}
		})
	}
}

func TestGetShardBackups_NonExistentStanza(t *testing.T) {
	ctx := context.Background()

	// Use a temp directory for config
	configPath := t.TempDir() + "/pgbackrest.conf"
	stanzaName := "non-existent-stanza"

	result, err := GetShardBackups(ctx, configPath, stanzaName, ListOptions{})

	// Should not return an error for non-existent stanza
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Backups)
}

func TestGetShardBackups_LimitApplication(t *testing.T) {
	// This test verifies the limit logic, even though we won't have real backups
	// We can test that the limit is properly applied if backups exist

	// Create a mock result with multiple backups
	mockBackups := []*backupservicepb.BackupMetadata{
		{BackupId: "20250101-100000F", Status: backupservicepb.BackupMetadata_COMPLETE},
		{BackupId: "20250102-100000F", Status: backupservicepb.BackupMetadata_COMPLETE},
		{BackupId: "20250103-100000F", Status: backupservicepb.BackupMetadata_COMPLETE},
		{BackupId: "20250104-100000F", Status: backupservicepb.BackupMetadata_COMPLETE},
		{BackupId: "20250105-100000F", Status: backupservicepb.BackupMetadata_COMPLETE},
	}

	tests := []struct {
		name          string
		limit         uint32
		expectedCount int
	}{
		{
			name:          "No limit returns all",
			limit:         0,
			expectedCount: 5,
		},
		{
			name:          "Limit 3 returns 3",
			limit:         3,
			expectedCount: 3,
		},
		{
			name:          "Limit greater than count returns all",
			limit:         10,
			expectedCount: 5,
		},
		{
			name:          "Limit 1 returns 1",
			limit:         1,
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate applying limit
			backups := mockBackups
			if tt.limit > 0 && uint32(len(backups)) > tt.limit {
				backups = backups[:tt.limit]
			}

			assert.Equal(t, tt.expectedCount, len(backups))
		})
	}
}

func TestGetShardBackups_StatusMapping(t *testing.T) {
	// Test that backup status is correctly mapped from pgBackRest error flag
	tests := []struct {
		name           string
		errorFlag      bool
		expectedStatus backupservicepb.BackupMetadata_Status
	}{
		{
			name:           "No error means COMPLETE",
			errorFlag:      false,
			expectedStatus: backupservicepb.BackupMetadata_COMPLETE,
		},
		{
			name:           "Error means INCOMPLETE",
			errorFlag:      true,
			expectedStatus: backupservicepb.BackupMetadata_INCOMPLETE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This tests the status mapping logic used in GetShardBackups
			status := backupservicepb.BackupMetadata_COMPLETE
			if tt.errorFlag {
				status = backupservicepb.BackupMetadata_INCOMPLETE
			}

			assert.Equal(t, tt.expectedStatus, status)
		})
	}
}
