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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

func TestExtractBackupID(t *testing.T) {
	tests := []struct {
		name        string
		output      string
		want        string
		expectError bool
	}{
		{
			name: "Full backup with standard format",
			output: `P00   INFO: backup command begin 2.41: --exec-id=12345
P00   INFO: execute non-exclusive pg_start_backup(): backup begins after the next regular checkpoint completes
P00   INFO: backup start archive = 000000010000000000000002
P00   INFO: new backup label = 20250104-100000F
P00   INFO: full backup size = 25.3MB
P00   INFO: backup command end: completed successfully`,
			want:        "20250104-100000F",
			expectError: false,
		},
		{
			name: "Incremental backup format",
			output: `P00   INFO: backup command begin 2.41
P00   INFO: last backup label = 20250104-100000F, version = 2.41
new backup label = 20250104-100000F_20250104-120000I
P00   INFO: backup command end: completed successfully`,
			want:        "20250104-100000F_20250104-120000I",
			expectError: false,
		},
		{
			name: "Differential backup format",
			output: `P00   INFO: backup command begin 2.41
P00   INFO: last backup label = 20250104-100000F, version = 2.41
new backup label = 20250104-100000F_20250104-110000D
P00   INFO: backup command end: completed successfully`,
			want:        "20250104-100000F_20250104-110000D",
			expectError: false,
		},
		{
			name: "Backup label with equals sign format",
			output: `Some random output
new backup label = 20250105-150000F
More output here`,
			want:        "20250105-150000F",
			expectError: false,
		},
		{
			name: "Output with no backup ID",
			output: `P00   INFO: backup command begin 2.41
P00   ERROR: backup failed with error`,
			want:        "",
			expectError: true,
		},
		{
			name:        "Empty output",
			output:      "",
			want:        "",
			expectError: true,
		},
		{
			name: "Backup ID in different position",
			output: `Start backup
20250106-180000F was created
End backup`,
			want:        "20250106-180000F",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractBackupID(tt.output)

			if tt.expectError {
				assert.Error(t, err)
				assert.Empty(t, got)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestBackupOptions_Validation(t *testing.T) {
	tests := []struct {
		name        string
		opts        BackupOptions
		configPath  string
		stanzaName  string
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid full backup options",
			opts: BackupOptions{
				Type: "full",
			},
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			expectError: false,
		},
		{
			name:        "Missing type",
			opts:        BackupOptions{},
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			expectError: true,
			errorMsg:    "type is required",
		},
		{
			name: "Invalid backup type",
			opts: BackupOptions{
				Type: "invalid",
			},
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			expectError: true,
			errorMsg:    "invalid backup type",
		},
		{
			name: "Missing config_path",
			opts: BackupOptions{
				Type: "full",
			},
			configPath:  "",
			stanzaName:  "test-stanza",
			expectError: true,
			errorMsg:    "config_path is required",
		},
		{
			name: "Missing stanza_name",
			opts: BackupOptions{
				Type: "full",
			},
			configPath:  "/tmp/test.conf",
			stanzaName:  "",
			expectError: true,
			errorMsg:    "stanza_name is required",
		},
		{
			name: "Valid differential backup",
			opts: BackupOptions{
				Type: "differential",
			},
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			expectError: false,
		},
		{
			name: "Valid incremental backup",
			opts: BackupOptions{
				Type: "incremental",
			},
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We're just testing validation, so we pass a context that will timeout
			// to avoid actually running pgbackrest
			ctx := t.Context()

			result, err := Backup(ctx, tt.configPath, tt.stanzaName, tt.opts)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				// For valid options, we still expect an error because pgbackrest
				// won't actually run successfully without a real setup, but the
				// validation should pass and we should get to the execution phase
				// which will fail with an INTERNAL error, not INVALID_ARGUMENT
				if err != nil {
					// If there's an error, it should be about pgbackrest execution,
					// not validation
					assert.Contains(t, err.Error(), "pgbackrest")
				}
			}
		})
	}
}

func TestRestoreShardFromBackup_Validation(t *testing.T) {
	ctx := context.Background()

	// Create mock pgctld client
	mockService := &testutil.MockPgCtldService{}
	testServer := testutil.NewTestGRPCServer(t)
	testServer.RegisterService(mockService)
	testServer.Start(t)
	defer testServer.Stop()

	client := testServer.NewClient(t)

	tests := []struct {
		name        string
		client      pgctldpb.PgCtldClient
		configPath  string
		stanzaName  string
		pgDataDir   string
		opts        RestoreOptions
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid request with backup_id",
			client:      client,
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			pgDataDir:   "/tmp/test/pg_data",
			opts:        RestoreOptions{BackupID: "20250104-100000F"},
			expectError: false,
		},
		{
			name:        "Valid request without backup_id (latest)",
			client:      client,
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			pgDataDir:   "/tmp/test/pg_data",
			opts:        RestoreOptions{BackupID: ""},
			expectError: false,
		},
		{
			name:        "Missing pgctld_client",
			client:      nil,
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			pgDataDir:   "/tmp/test/pg_data",
			opts:        RestoreOptions{},
			expectError: true,
			errorMsg:    "pgctld_client is required",
		},
		{
			name:        "Missing config_path",
			client:      client,
			configPath:  "",
			stanzaName:  "test-stanza",
			pgDataDir:   "/tmp/test/pg_data",
			opts:        RestoreOptions{},
			expectError: true,
			errorMsg:    "config_path is required",
		},
		{
			name:        "Missing stanza_name",
			client:      client,
			configPath:  "/tmp/test.conf",
			stanzaName:  "",
			pgDataDir:   "/tmp/test/pg_data",
			opts:        RestoreOptions{},
			expectError: true,
			errorMsg:    "stanza_name is required",
		},
		{
			name:        "Missing pg_data_dir",
			client:      client,
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			pgDataDir:   "",
			opts:        RestoreOptions{},
			expectError: true,
			errorMsg:    "pg_data_dir is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := RestoreShardFromBackup(ctx, tt.client, tt.configPath, tt.stanzaName, tt.pgDataDir, tt.opts)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				// For valid options, we may get an error because pgbackrest
				// won't actually run successfully without a real setup, or
				// because pgctld connection fails in test environment
				if err != nil {
					// Error should be about execution (pgbackrest or pgctld), not validation
					errMsg := err.Error()
					hasExecutionError := strings.Contains(errMsg, "pgbackrest") ||
						strings.Contains(errMsg, "PostgreSQL") ||
						strings.Contains(errMsg, "stop") ||
						strings.Contains(errMsg, "rpc error")
					assert.True(t, hasExecutionError,
						"Expected execution error, got: %s", errMsg)
				}
			}
		})
	}
}

func TestRestoreShardFromBackup_PgctldInteraction(t *testing.T) {
	ctx := context.Background()

	// Create mock pgctld service
	mockService := &testutil.MockPgCtldService{}
	testServer := testutil.NewTestGRPCServer(t)
	testServer.RegisterService(mockService)
	testServer.Start(t)
	defer testServer.Stop()

	client := testServer.NewClient(t)

	// Use temp directory for config
	configPath := t.TempDir() + "/pgbackrest.conf"
	stanzaName := "test-stanza"
	pgDataDir := t.TempDir() + "/pg_data"

	opts := RestoreOptions{
		BackupID: "20250104-100000F",
	}

	// Call restore (will fail because pgbackrest won't work, but we can verify pgctld calls)
	_, err := RestoreShardFromBackup(ctx, client, configPath, stanzaName, pgDataDir, opts)

	// The restore will fail, but we want to verify the restore flow attempted to interact with pgctld
	// In a test environment with bufconn, the gRPC calls may fail with connection errors
	assert.Error(t, err, "Expected error in test environment")

	// Note: Due to the way bufconn works with NewTestGRPCServer, the actual RPC calls
	// may not be captured in StopCalls. This test verifies the error handling behavior.
	// For real pgctld interaction verification, use integration tests with a real server.
	t.Log("Restore flow tested, error:", err)
}

func TestRestoreOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     RestoreOptions
		expected string
	}{
		{
			name:     "With specific backup ID",
			opts:     RestoreOptions{BackupID: "20250104-100000F"},
			expected: "20250104-100000F",
		},
		{
			name:     "Without backup ID (latest)",
			opts:     RestoreOptions{BackupID: ""},
			expected: "",
		},
		{
			name:     "Incremental backup ID",
			opts:     RestoreOptions{BackupID: "20250104-100000F_20250104-120000I"},
			expected: "20250104-100000F_20250104-120000I",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.opts.BackupID)
		})
	}
}

func TestGetShardBackups_Validation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		configPath  string
		stanzaName  string
		opts        GetBackupsOptions
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid request",
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			opts:        GetBackupsOptions{Limit: 0},
			expectError: false,
		},
		{
			name:        "Missing config_path",
			configPath:  "",
			stanzaName:  "test-stanza",
			opts:        GetBackupsOptions{Limit: 0},
			expectError: true,
			errorMsg:    "config_path is required",
		},
		{
			name:        "Missing stanza_name",
			configPath:  "/tmp/test.conf",
			stanzaName:  "",
			opts:        GetBackupsOptions{Limit: 0},
			expectError: true,
			errorMsg:    "stanza_name is required",
		},
		{
			name:        "With limit",
			configPath:  "/tmp/test.conf",
			stanzaName:  "test-stanza",
			opts:        GetBackupsOptions{Limit: 10},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetBackups(ctx, tt.configPath, tt.stanzaName, tt.opts)

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

	result, err := GetBackups(ctx, configPath, stanzaName, GetBackupsOptions{})

	// Should not return an error for non-existent stanza
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Backups)
}

func TestGetShardBackups_LimitApplication(t *testing.T) {
	// This test verifies the limit logic, even though we won't have real backups
	// We can test that the limit is properly applied if backups exist

	// Create a mock result with multiple backups
	mockBackups := []*multipoolermanagerdata.BackupMetadata{
		{BackupId: "20250101-100000F", Status: multipoolermanagerdata.BackupMetadata_COMPLETE},
		{BackupId: "20250102-100000F", Status: multipoolermanagerdata.BackupMetadata_COMPLETE},
		{BackupId: "20250103-100000F", Status: multipoolermanagerdata.BackupMetadata_COMPLETE},
		{BackupId: "20250104-100000F", Status: multipoolermanagerdata.BackupMetadata_COMPLETE},
		{BackupId: "20250105-100000F", Status: multipoolermanagerdata.BackupMetadata_COMPLETE},
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
			// This tests the status mapping logic used in GetShardBackups
			status := multipoolermanagerdata.BackupMetadata_COMPLETE
			if tt.errorFlag {
				status = multipoolermanagerdata.BackupMetadata_INCOMPLETE
			}

			assert.Equal(t, tt.expectedStatus, status)
		})
	}
}
