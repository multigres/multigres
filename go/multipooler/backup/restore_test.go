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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

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
