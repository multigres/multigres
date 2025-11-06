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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

			result, err := BackupShard(ctx, tt.configPath, tt.stanzaName, tt.opts)

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
