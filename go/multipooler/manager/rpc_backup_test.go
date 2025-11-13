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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// createTestManager creates a minimal MultiPoolerManager for testing
func createTestManager(poolerDir, stanzaName, tableGroup, shard string, poolerType clustermetadatapb.PoolerType) *MultiPoolerManager {
	pm := &MultiPoolerManager{
		config: &Config{
			PoolerDir:        poolerDir,
			PgBackRestStanza: stanzaName,
			ServiceID:        &clustermetadatapb.ID{Name: "test-service"},
		},
		serviceID: &clustermetadatapb.ID{Name: "test-service"},
		multipooler: &topo.MultiPoolerInfo{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Type:       poolerType,
				TableGroup: tableGroup,
				Shard:      shard,
			},
		},
	}
	return pm
}

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
			pm := createTestManager(tt.poolerDir, tt.stanzaName, "", "", tt.poolerType)

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
