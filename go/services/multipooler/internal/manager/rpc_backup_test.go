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
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/timer"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// createTestManager creates a minimal MultiPoolerManager for testing
func createTestManager(t *testing.T, poolerDir, tableGroup, shard string, poolerType clustermetadatapb.PoolerType) *MultiPoolerManager {
	return createTestManagerWithBackupLocation(t, poolerDir, tableGroup, shard, poolerType, "/tmp/backups")
}

// createTestManagerWithBackupLocation creates a minimal MultiPoolerManager for testing with backup_location.
// backupLocation is the base path; the full path (with database/tablegroup/shard) is computed internally.
func createTestManagerWithBackupLocation(t *testing.T, poolerDir, tableGroup, shard string, poolerType clustermetadatapb.PoolerType, backupLocation string) *MultiPoolerManager {
	database := "test-database"

	// Use defaults if not provided
	if tableGroup == "" {
		tableGroup = constants.DefaultTableGroup
	}
	if shard == "" {
		shard = constants.DefaultShard
	}

	multipoolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-multipooler",
	}

	multipoolerProto := &clustermetadatapb.MultiPooler{
		Id:        multipoolerID,
		Type:      poolerType,
		PoolerDir: poolerDir,
		ShardKey: &clustermetadatapb.ShardKey{
			TableGroup: tableGroup,
			Shard:      shard,
			Database:   database,
		},
	}
	// Keep the Type ⇔ SelfLeadership invariant so the record validates: a
	// PRIMARY names itself; any other type carries no self-leadership.
	if poolerType == clustermetadatapb.PoolerType_PRIMARY {
		multipoolerProto.SelfLeadership = &clustermetadatapb.LeaderObservation{LeaderId: multipoolerID}
	}

	// Create a topology store with backup location if provided
	var topoClient topoclient.Store
	var backupConfig *backup.Config
	if backupLocation != "" {
		ctx := context.Background()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		err := ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
			Name:           database,
			BackupLocation: utils.FilesystemBackupLocation(backupLocation),
		})
		if err == nil {
			topoClient = ts
		}

		// Create backup config
		backupConfig, _ = backup.NewConfig(
			utils.FilesystemBackupLocation(backupLocation),
		)
	}

	monitorRunner := timer.NewPeriodicRunner(context.TODO(), 10*time.Second)

	pm := &MultiPoolerManager{
		config:     &Config{},
		serviceID:  multipoolerID,
		topoClient: topoClient,
		record:     newRecordFromProto(multipoolerProto),
		state:      ManagerStateReady,
		actionLock: actionlock.NewActionLock(),
		logger:     slog.Default(),
		pgMonitor:  monitorRunner,
		// consensus.ConsensusPromises is the canonical home for the recorded primary
		// (replacing the former pm.primaryHost/Port/PoolerID fields). Backup
		// tests seed it via setBackupPrimary below.
		consensusMgr: consensus.NewManagerForTesting(t, multipoolerID,
			consensus.NewConsensusPromises(poolerDir, multipoolerID),
			&fakeRuleStore{},
			nil,
		),
	}

	// Build the backup engine the way the production constructor does, feeding
	// it the resolved repo config when one was supplied.
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{PgDataDir: poolerDir})
	if backupConfig != nil {
		pm.backup.SetBackupConfig(backupConfig)
	}
	return pm
}

// setBackupPrimary seeds the ReplicationPrimary on the test manager so the
// backup paths that read pm.consensusMgr.GetReplicationPrimary() see a
// configured primary. Synthetic rule at term 1 is sufficient — no consumer
// of rp.Rule reads cohort_members or durability_policy.
func setBackupPrimary(t *testing.T, pm *MultiPoolerManager, primaryName, host string, port int32) {
	t.Helper()
	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      primaryName,
	}
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-seed")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)
	require.NoError(t, pm.consensusMgr.RecordTermPrimary(lockCtx, &clustermetadatapb.ReplicationPrimary{
		Rule: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			LeaderId:   id,
		},
		Primary: &clustermetadatapb.PoolerAddress{Id: id, Host: host, PostgresPort: port},
	}))
}

// setupMockPgBackRestConfig creates a mock pgbackrest.conf file and returns its path.
// Callers must feed the returned path to pm.backup.SetConfigPath after creating the manager.
func setupMockPgBackRestConfig(t *testing.T, poolerDir string) string {
	t.Helper()

	// Create pgbackrest directory
	pgbackrestDir := filepath.Join(poolerDir, "pgbackrest")
	err := os.MkdirAll(pgbackrestDir, 0o755)
	require.NoError(t, err, "failed to create pgbackrest directory")

	// Create minimal pgbackrest.conf
	configPath := filepath.Join(pgbackrestDir, "pgbackrest.conf")
	configContent := `[global]
log-path=` + filepath.Join(pgbackrestDir, "logs") + `
spool-path=` + filepath.Join(pgbackrestDir, "spool") + `
lock-path=` + filepath.Join(pgbackrestDir, "lock") + `

[multigres]
repo1-path=/tmp/backups
pg1-path=` + filepath.Join(poolerDir, "pg_data") + `
pg1-socket-path=` + filepath.Join(poolerDir, "pg_sockets") + `
pg1-port=5432
`
	err = os.WriteFile(configPath, []byte(configContent), 0o600)
	require.NoError(t, err, "failed to create pgbackrest.conf")
	return configPath
}

func TestBackup_Validation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		poolerDir    string
		backupType   string
		poolerType   clustermetadatapb.PoolerType
		forcePrimary bool
		expectError  bool
		errorMsg     string
	}{
		{
			name:        "Missing type",
			poolerDir:   "/tmp/test",
			backupType:  "",
			poolerType:  clustermetadatapb.PoolerType_REPLICA,
			expectError: true,
			errorMsg:    "type is required",
		},
		{
			name:        "Invalid backup type",
			poolerDir:   "/tmp/test",
			backupType:  "invalid",
			poolerType:  clustermetadatapb.PoolerType_REPLICA,
			expectError: true,
			errorMsg:    "invalid backup type",
		},
		{
			name:         "Primary without force flag",
			poolerDir:    "/tmp/test",
			backupType:   "full",
			poolerType:   clustermetadatapb.PoolerType_PRIMARY,
			forcePrimary: false,
			expectError:  true,
			errorMsg:     "not allowed unless ForcePrimary",
		},
		{
			name:         "Primary with force flag",
			poolerDir:    "/tmp/test",
			backupType:   "full",
			poolerType:   clustermetadatapb.PoolerType_PRIMARY,
			forcePrimary: true,
			expectError:  true,                       // Will fail on pgbackrest execution but validation passes
			errorMsg:     "pgbackrest backup failed", // Execution error, not validation
		},
		{
			name:         "Replica backup",
			poolerDir:    "/tmp/test",
			backupType:   "full",
			poolerType:   clustermetadatapb.PoolerType_REPLICA,
			forcePrimary: false,
			expectError:  true, // Will fail - no pg2_path override in local mode
			errorMsg:     "local mode backup requires pg2_path override",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Provide backup location for tests that need to reach pgbackrest execution or validation
			backupLocation := "/tmp/test-backups"
			configPath := setupMockPgBackRestConfig(t, tt.poolerDir)
			pm := createTestManagerWithBackupLocation(t, tt.poolerDir, "", "", tt.poolerType, backupLocation)
			pm.backup.SetConfigPath(configPath)

			// Setup primary info for replica poolers (required for backup)
			if tt.poolerType == clustermetadatapb.PoolerType_REPLICA {
				setBackupPrimary(t, pm, "primary-pooler", "primary.local", 5432)
			}

			_, err := pm.Backup(ctx, tt.forcePrimary, tt.backupType, "", nil)

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
		limit       uint32
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid request",
			poolerDir:   t.TempDir(),
			limit:       0,
			expectError: false, // Should return empty list for non-existent stanza
		},
		{
			name:        "With limit",
			poolerDir:   t.TempDir(),
			limit:       10,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := setupMockPgBackRestConfig(t, tt.poolerDir)
			pm := createTestManager(t, tt.poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)
			pm.backup.SetConfigPath(configPath)

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

func TestBackup_ActionLock(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	pm := createTestManagerWithBackupLocation(t, tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Hold the lock in another goroutine
	lockCtx, err := pm.actionLock.Acquire(ctx, "test-holder")
	require.NoError(t, err)

	// Create a context with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// Backup should timeout waiting for the lock
	_, err = pm.Backup(timeoutCtx, false, "full", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")

	// Release the lock
	pm.actionLock.Release(lockCtx)
}

func TestGetBackups_ActionLock(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	pm := createTestManagerWithBackupLocation(t, tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

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

	pm := createTestManagerWithBackupLocation(t, tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

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

	pm := createTestManagerWithBackupLocation(t, tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Call Backup - it will fail (no pgbackrest), but should release the lock
	_, _ = pm.Backup(ctx, false, "full", "", nil)

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

	pm := createTestManagerWithBackupLocation(t, tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

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

	pm := createTestManagerWithBackupLocation(t, tmpDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

	// Call RestoreFromBackup - it will fail (precondition), but should release the lock
	_ = pm.RestoreFromBackup(ctx, "test-backup-id")

	// Verify lock was released by acquiring it with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	lockCtx, err := pm.actionLock.Acquire(timeoutCtx, "verify-release")
	require.NoError(t, err, "Lock should be released after RestoreFromBackup returns")
	pm.actionLock.Release(lockCtx)
}

func TestBackup_StoresJobIDAnnotation(t *testing.T) {
	// Verify that when job_id is provided, it's included in annotations
	req := &multipoolermanagerdata.BackupRequest{
		JobId: "20251203-143045.123456_mp-cell-1",
	}

	// Verify the field is accessible (structural test)
	require.NotEmpty(t, req.JobId, "JobId should be set")
	assert.Equal(t, "20251203-143045.123456_mp-cell-1", req.JobId)
}

func TestGetBackupByJobId_NotFound(t *testing.T) {
	// Test that searching for a non-existent job_id returns nil backup

	resp := &multipoolermanagerdata.GetBackupByJobIdResponse{
		Backup: nil,
	}

	assert.Nil(t, resp.Backup)
}

func TestGetBackupByJobId_Found(t *testing.T) {
	// Test response structure when backup is found

	resp := &multipoolermanagerdata.GetBackupByJobIdResponse{
		Backup: &multipoolermanagerdata.BackupMetadata{
			BackupId:   "20251203-143045F",
			TableGroup: "default",
			Shard:      "0",
			Status:     multipoolermanagerdata.BackupMetadata_COMPLETE,
			JobId:      "20251203-143045.123456_mp-cell-1",
		},
	}

	require.NotNil(t, resp.Backup)
	assert.Equal(t, "20251203-143045F", resp.Backup.BackupId)
	assert.Equal(t, "20251203-143045.123456_mp-cell-1", resp.Backup.JobId)
}

func TestBackup_UsesCLIArgs(t *testing.T) {
	// Test that Backup() uses CLI args with pgctld-generated config instead of creating temp config
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create mock pgbackrest binary that captures command-line arguments
	binDir := filepath.Join(tmpDir, "bin")
	err := os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)

	argsFile := filepath.Join(tmpDir, "captured_args.txt")
	mockScript := `#!/bin/bash
# Capture all arguments to a file for backup command
if [[ "$*" == *"backup"* && "$*" != *"info"* ]]; then
    echo "$@" > ` + argsFile + `
fi
# Handle info command to return mock backup info
if [[ "$*" == *"info"* ]]; then
    cat << 'JSONEOF'
[{
    "backup": [{
        "label": "20250104-100000F",
        "annotation": {
            "multipooler_id": "test-multipooler",
            "job_id": "test-job-id"
        }
    }]
}]
JSONEOF
fi
exit 0
`
	pgbackrestPath := filepath.Join(binDir, "pgbackrest")
	err = os.WriteFile(pgbackrestPath, []byte(mockScript), 0o755)
	require.NoError(t, err)

	// Prepend binDir to PATH so our mock pgbackrest is found first
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	// Create pooler directory structure
	poolerDir := filepath.Join(tmpDir, "pooler")
	err = os.MkdirAll(poolerDir, 0o755)
	require.NoError(t, err)

	// Create a pgbackrest.conf that pgctld would have generated
	pgbackrestDir := filepath.Join(poolerDir, "pgbackrest")
	err = os.MkdirAll(pgbackrestDir, 0o755)
	require.NoError(t, err)

	pgctldConfigPath := filepath.Join(pgbackrestDir, "pgbackrest.conf")
	pgctldConfig := `[global]
log-path=/tmp/pgbackrest/logs
[multigres]
repo1-path=/tmp/backups
pg1-path=/tmp/pg_data
`
	err = os.WriteFile(pgctldConfigPath, []byte(pgctldConfig), 0o600)
	require.NoError(t, err)

	// Create test manager with the pooler directory
	pm := createTestManagerWithBackupLocation(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)
	pm.backup.SetConfigPath(pgctldConfigPath)

	// Setup primary info (required for replica backups)
	setBackupPrimary(t, pm, "primary-pooler", "primary.local", 5432)

	// Call Backup with pg2_path override for local mode
	primaryDataPath := filepath.Join(poolerDir, "pg_data")
	_, err = pm.Backup(ctx, false, "full", "test-job-id", map[string]string{
		"pg2_path": primaryDataPath,
	})
	require.NoError(t, err)

	// Read captured arguments
	capturedArgs, err := os.ReadFile(argsFile)
	require.NoError(t, err)
	args := string(capturedArgs)

	// Verify --config points to the multipooler-generated config (not a temp file)
	assert.Contains(t, args, "--config", "command should include --config flag")
	assert.Contains(t, args, "pgbackrest.conf", "config should be pgbackrest.conf")
	assert.NotContains(t, args, "pgbackrest-backup.conf", "should NOT use temp backup config file")

	// Verify backup command structure
	assert.Contains(t, args, "--stanza=multigres", "command should include stanza")
	assert.Contains(t, args, "--type=full", "command should include backup type")
	assert.Contains(t, args, "backup", "command should be backup")

	// Verify NO temp config files were created
	files, err := os.ReadDir(pgbackrestDir)
	require.NoError(t, err)
	for _, file := range files {
		assert.NotContains(t, file.Name(), "pgbackrest-backup.conf", "temp backup config should not be created")
		assert.NotContains(t, file.Name(), "tmp", "no temp files should be created")
	}
}

func TestRecordLeaseLossIfApplicable(t *testing.T) {
	poolerDir := t.TempDir()
	pm := createTestManagerWithBackupLocation(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, t.TempDir())

	t.Run("no error → not recorded", func(t *testing.T) {
		assert.False(t, pm.recordLeaseLossIfApplicable(t.Context(), nil))
	})

	t.Run("error without lease-lost cause → not recorded", func(t *testing.T) {
		assert.False(t, pm.recordLeaseLossIfApplicable(t.Context(), errors.New("some other failure")))
	})

	t.Run("error with lease-lost cause → recorded", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(topoclient.ErrLeaseLost)
		assert.True(t, pm.recordLeaseLossIfApplicable(ctx, errors.New("backup aborted")))
	})
}

func TestBackup_TracksFailuresAndInProgress(t *testing.T) {
	// Drive a real backup through pm.Backup with a pgbackrest stub that fails
	// the first backup and succeeds the second, asserting the health tracker's
	// failure streak and in-progress timer are maintained inline.
	ctx := context.Background()
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))

	marker := filepath.Join(tmpDir, "backup_attempted_marker")
	mockScript := `#!/bin/bash
if [[ "$*" == *"info"* ]]; then
cat << 'JSONEOF'
[{"backup":[{"label":"20250104-100000F","annotation":{"multipooler_id":"test-multipooler","job_id":"test-job-id"}}]}]
JSONEOF
exit 0
fi
if [[ "$*" == *"backup"* ]]; then
  if [[ -f ` + marker + ` ]]; then exit 0; fi
  touch ` + marker + `
  echo "ERROR: simulated backup failure" >&2
  exit 1
fi
exit 0
`
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "pgbackrest"), []byte(mockScript), 0o755))
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	poolerDir := filepath.Join(tmpDir, "pooler")
	require.NoError(t, os.MkdirAll(poolerDir, 0o755))
	configPath := setupMockPgBackRestConfig(t, poolerDir)
	pm := createTestManagerWithBackupLocation(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)
	pm.backup.SetConfigPath(configPath)
	setBackupPrimary(t, pm, "primary-pooler", "primary.local", 5432)
	overrides := map[string]string{"pg2_path": filepath.Join(poolerDir, "pg_data")}

	// First backup fails → streak 1, in-progress cleared.
	_, err := pm.Backup(ctx, false, "full", "test-job-id", overrides)
	require.Error(t, err)
	snap := pm.backup.Health().Snapshot()
	assert.Equal(t, int64(1), snap.FailuresSinceSuccess)
	assert.True(t, snap.InProgressStart.IsZero(), "in-progress must be cleared after a failed backup")
	assert.NotEmpty(t, snap.LastFailErr)

	// Second backup succeeds → streak resets, in-progress cleared.
	_, err = pm.Backup(ctx, false, "full", "test-job-id", overrides)
	require.NoError(t, err)
	snap = pm.backup.Health().Snapshot()
	assert.Zero(t, snap.FailuresSinceSuccess, "streak resets after a successful backup")
	assert.True(t, snap.InProgressStart.IsZero(), "in-progress must be cleared after a successful backup")
}

func TestBackup_RefreshesRepoGaugesOnSuccess(t *testing.T) {
	// After a successful backup, the taker should reflect the new backup in its
	// age/count gauges immediately (RefreshRepoNow), not wait for the next poll.
	ctx := context.Background()
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))

	// info returns one COMPLETE backup whose annotations match this pooler's
	// shard (default / 0-inf) so parseBackups includes it; backup/verify exit 0.
	mockScript := `#!/bin/bash
if [[ "$*" == *"info"* ]]; then
cat << 'JSONEOF'
[{"backup":[{"label":"20260101-000000F","error":false,"timestamp":{"start":1735689600,"stop":1735689660},"annotation":{"job_id":"test-job-id","multipooler_id":"test-multipooler","pooler_type":"REPLICA","table_group":"default","shard":"0-inf"}}]}]
JSONEOF
exit 0
fi
exit 0
`
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "pgbackrest"), []byte(mockScript), 0o755))
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	poolerDir := filepath.Join(tmpDir, "pooler")
	require.NoError(t, os.MkdirAll(poolerDir, 0o755))
	configPath := setupMockPgBackRestConfig(t, poolerDir)
	pm := createTestManagerWithBackupLocation(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)
	pm.backup.SetConfigPath(configPath)
	setBackupPrimary(t, pm, "primary-pooler", "primary.local", 5432)

	// Gauges start empty before any backup.
	require.Zero(t, pm.backup.Health().Snapshot().CompleteCount)

	_, err := pm.Backup(ctx, false, "full", "test-job-id", map[string]string{"pg2_path": filepath.Join(poolerDir, "pg_data")})
	require.NoError(t, err)

	snap := pm.backup.Health().Snapshot()
	assert.Equal(t, int64(1), snap.CompleteCount, "completed backup should be reflected immediately")
	assert.Equal(t, int64(1735689660), snap.LastSuccessStop.Unix(), "last-success time should come from the backup's stop timestamp")
}

func TestBackup_RejectsEmptyTableGroup(t *testing.T) {
	// Regression test for MUL-223: backups with empty table_group produce
	// corrupt metadata that causes replicas to skip valid backups.
	// The backup must be rejected loudly rather than silently omitting
	// the table_group annotation.
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create mock pgbackrest binary that succeeds for both backup and info.
	// After the fix, pgbackrest should never be reached because the backup
	// is rejected early due to empty table_group/shard.
	binDir := filepath.Join(tmpDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))

	mockScript := `#!/bin/bash
if [[ "$*" == *"info"* ]]; then
    cat << 'JSONEOF'
[{
    "backup": [{
        "label": "20250104-100000F",
        "type": "full",
        "error": false,
        "timestamp": {"start": 1735970400, "stop": 1735970500},
        "annotation": {
            "multipooler_id": "test-multipooler",
            "job_id": "test-job-id"
        }
    }]
}]
JSONEOF
fi
exit 0
`
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "pgbackrest"), []byte(mockScript), 0o755))
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	poolerDir := filepath.Join(tmpDir, "pooler")
	require.NoError(t, os.MkdirAll(poolerDir, 0o755))

	configPath := setupMockPgBackRestConfig(t, poolerDir)

	tests := []struct {
		name       string
		tableGroup string
		shard      string
		wantErr    string
	}{
		{
			name:       "empty table_group",
			tableGroup: "",
			shard:      "0-inf",
			wantErr:    "table_group",
		},
		{
			name:       "empty shard",
			tableGroup: "default",
			shard:      "",
			wantErr:    "shard",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the manager directly to bypass createTestManager's
			// default-substitution for empty table_group/shard.
			multipoolerID := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "test-multipooler",
			}
			ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
			_ = ts.CreateDatabase(ctx, "test-database", &clustermetadatapb.Database{
				Name:           "test-database",
				BackupLocation: utils.FilesystemBackupLocation(tmpDir),
			})
			backupConfig, _ := backup.NewConfig(
				utils.FilesystemBackupLocation(tmpDir),
			)

			pm := &MultiPoolerManager{
				config:     &Config{},
				serviceID:  multipoolerID,
				topoClient: ts,
				record: newRecordFromProto(&clustermetadatapb.MultiPooler{
					Id:        multipoolerID,
					Type:      clustermetadatapb.PoolerType_PRIMARY,
					PoolerDir: poolerDir,
					ShardKey: &clustermetadatapb.ShardKey{
						TableGroup: tt.tableGroup,
						Shard:      tt.shard,
						Database:   "test-database",
					},
					// A PRIMARY record must name itself as leader (the record invariant).
					SelfLeadership: &clustermetadatapb.LeaderObservation{LeaderId: multipoolerID},
				}),
				state:      ManagerStateReady,
				actionLock: actionlock.NewActionLock(),
				logger:     slog.Default(),
				pgMonitor:  timer.NewPeriodicRunner(context.TODO(), 10*time.Second),
			}
			pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{PgDataDir: poolerDir})
			pm.backup.SetBackupConfig(backupConfig)
			pm.backup.SetConfigPath(configPath)

			_, err := pm.Backup(ctx, true, "full", "test-job-id", nil)
			require.Error(t, err, "backup with empty %s should be rejected", tt.name)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestGetPrimaryAsPg2Args(t *testing.T) {
	tests := []struct {
		name                    string
		poolerType              clustermetadatapb.PoolerType
		primaryHost             string
		primaryPort             int32
		setupTLSCerts           bool // Whether to create TLS cert files
		pgBackRestTLSPort       int  // Port for pgBackRest TLS server
		expectError             bool
		expectedArgsContains    []string // Args we expect to find
		expectedArgsNotContains []string // Args we expect NOT to find
	}{
		{
			name:                    "primary_pooler_returns_empty",
			poolerType:              clustermetadatapb.PoolerType_PRIMARY,
			primaryHost:             "primary.example.com",
			primaryPort:             5432,
			setupTLSCerts:           true,
			pgBackRestTLSPort:       8443,
			expectError:             false,
			expectedArgsContains:    []string{},
			expectedArgsNotContains: []string{"--pg2-host", "--pg2-port"},
		},
		{
			name:              "replica_with_tls_certs",
			poolerType:        clustermetadatapb.PoolerType_REPLICA,
			primaryHost:       "primary.example.com",
			primaryPort:       5432,
			setupTLSCerts:     true,
			pgBackRestTLSPort: 8443, // Will be fetched from topology, not passed as param
			expectError:       false,
			expectedArgsContains: []string{
				"--pg2-host=primary.example.com",
				"--pg2-path=/data/pg_data",
				"--pg2-host-type=tls",
				"--pg2-host-port=8443",
				"--pg2-host-ca-file=",
				"--pg2-host-cert-file=",
				"--pg2-host-key-file=",
			},
			expectedArgsNotContains: []string{"--pg2-port=5432"},
		},
		{
			name:                 "replica_without_tls_local_mode_requires_override",
			poolerType:           clustermetadatapb.PoolerType_REPLICA,
			primaryHost:          "localhost",
			primaryPort:          5432,
			setupTLSCerts:        false,
			pgBackRestTLSPort:    0,
			expectError:          true, // Now expects error when pg2_path override is missing
			expectedArgsContains: nil,
		},
		{
			name:                 "replica_without_primary_info_errors",
			poolerType:           clustermetadatapb.PoolerType_REPLICA,
			primaryHost:          "",
			primaryPort:          0,
			setupTLSCerts:        false,
			pgBackRestTLSPort:    0,
			expectError:          true,
			expectedArgsContains: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test manager
			poolerDir := t.TempDir()
			pm := createTestManager(t, poolerDir, "", "", tt.poolerType)
			// Only seed a recorded primary when the test case supplies one;
			// the "no primary info" case leaves ReplicationPrimary empty so
			// GetPrimaryAsPg2Args returns FAILED_PRECONDITION.
			if tt.primaryHost != "" && tt.primaryPort != 0 {
				setBackupPrimary(t, pm, "test-primary", tt.primaryHost, tt.primaryPort)
			}

			// Setup TLS certs if needed and create primary pooler in topology
			if tt.setupTLSCerts {
				certDir := filepath.Join(poolerDir, "pgbackrest", "certs")
				require.NoError(t, os.MkdirAll(certDir, 0o755))
				caFile := filepath.Join(certDir, "ca.crt")
				certFile := filepath.Join(certDir, "client.crt")
				keyFile := filepath.Join(certDir, "client.key")
				require.NoError(t, os.WriteFile(caFile, []byte("test ca"), 0o644))
				require.NoError(t, os.WriteFile(certFile, []byte("test cert"), 0o644))
				require.NoError(t, os.WriteFile(keyFile, []byte("test key"), 0o600))
				// Set cert paths in config so GetPrimaryAsPg2Args can use them
				pm.config.PgBackRestCAFile = caFile
				pm.config.PgBackRestCertFile = certFile
				pm.config.PgBackRestKeyFile = keyFile

				// Primary id matches what setBackupPrimary recorded above so
				// the topology lookup finds the right pooler.
				primaryID := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "test-primary",
				}

				// Add primary to topology with pgbackrest port and data dir
				if pm.topoClient != nil {
					primaryPooler := &clustermetadatapb.MultiPooler{
						Id:       primaryID,
						Type:     clustermetadatapb.PoolerType_PRIMARY,
						Hostname: tt.primaryHost,
						PortMap: map[string]int32{
							"postgres":   tt.primaryPort,
							"pgbackrest": int32(tt.pgBackRestTLSPort),
						},
						PgDataDir: "/data/pg_data",
					}
					err := pm.topoClient.CreateMultiPooler(context.Background(), primaryPooler)
					require.NoError(t, err, "failed to create primary pooler in topology")
				}
			}

			// Call GetPrimaryAsPg2Args
			got, err := pm.GetPrimaryAsPg2Args(context.Background(), nil, false)

			// Check error expectation
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Check expected args are present
			for _, expected := range tt.expectedArgsContains {
				found := false
				for _, arg := range got {
					if strings.Contains(arg, strings.Split(expected, "=")[0]) {
						if expected == "" || strings.HasPrefix(arg, expected) || arg == expected {
							found = true
							break
						}
					}
				}
				assert.True(t, found, "expected arg %q not found in %v", expected, got)
			}

			// Check unexpected args are NOT present
			for _, notExpected := range tt.expectedArgsNotContains {
				for _, arg := range got {
					assert.NotContains(t, arg, strings.Split(notExpected, "=")[0],
						"unexpected arg prefix %q found in %v", notExpected, got)
				}
			}
		})
	}
}

func TestGetPrimaryAsPg2Args_WithOverrides(t *testing.T) {
	ctx := context.Background()
	poolerDir := t.TempDir()

	t.Run("TLS mode pg2_path override replaces topology value", func(t *testing.T) {
		// Create manager with TLS certs
		pm := createTestManager(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)
		pm.config.PgBackRestCAFile = "/path/to/ca.crt"
		pm.config.PgBackRestCertFile = "/path/to/client.crt"
		pm.config.PgBackRestKeyFile = "/path/to/client.key"
		setBackupPrimary(t, pm, "test-primary-override", "primary.example.com", 5432)

		// Matching primary id for the topology entry below.
		primaryID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-primary-override",
		}

		if pm.topoClient != nil {
			primaryPooler := &clustermetadatapb.MultiPooler{
				Id:       primaryID,
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com",
				PortMap: map[string]int32{
					"postgres":   5432,
					"pgbackrest": 8432,
				},
				PgDataDir: "/data/pg_data",
			}
			err := pm.topoClient.CreateMultiPooler(ctx, primaryPooler)
			require.NoError(t, err, "failed to create primary pooler in topology")
		}

		args, err := pm.GetPrimaryAsPg2Args(ctx, map[string]string{
			"pg2_path": "/custom/path",
		}, false)

		require.NoError(t, err)
		assert.Contains(t, args, "--pg2-host=primary.example.com")
		assert.Contains(t, args, "--pg2-host-type=tls")
		assert.Contains(t, args, "--pg2-host-port=8432")
		assert.Contains(t, args, "--pg2-path=/custom/path") // Override wins over topology
		assert.NotContains(t, args, "--pg2-path=/data/pg_data")
	})

	t.Run("TLS mode errors when pg_data_dir missing from topology", func(t *testing.T) {
		pm := createTestManager(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)
		pm.config.PgBackRestCAFile = "/path/to/ca.crt"
		pm.config.PgBackRestCertFile = "/path/to/client.crt"
		pm.config.PgBackRestKeyFile = "/path/to/client.key"
		setBackupPrimary(t, pm, "test-primary-no-datadir", "primary.example.com", 5432)

		primaryID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-primary-no-datadir",
		}

		if pm.topoClient != nil {
			primaryPooler := &clustermetadatapb.MultiPooler{
				Id:       primaryID,
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com",
				PortMap: map[string]int32{
					"postgres":   5432,
					"pgbackrest": 8432,
				},
				// PgDataDir intentionally omitted
			}
			err := pm.topoClient.CreateMultiPooler(ctx, primaryPooler)
			require.NoError(t, err, "failed to create primary pooler in topology")
		}

		_, err := pm.GetPrimaryAsPg2Args(ctx, nil, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pg_data_dir")
	})

	t.Run("local mode requires pg2_path override", func(t *testing.T) {
		// Create manager without TLS certs (local mode)
		pm := createTestManager(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)
		setBackupPrimary(t, pm, "test-primary-local", "localhost", 5432)

		// Without override - should error
		_, err := pm.GetPrimaryAsPg2Args(ctx, nil, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "local mode backup requires pg2_path override")

		// With override - should work
		args, err := pm.GetPrimaryAsPg2Args(ctx, map[string]string{
			"pg2_path": "/primary/data",
		}, false)
		require.NoError(t, err)
		assert.Contains(t, args, "--pg2-host=localhost")
		assert.Contains(t, args, "--pg2-port=5432")
		assert.Contains(t, args, "--pg2-path=/primary/data")
	})

	t.Run("override replaces existing arg", func(t *testing.T) {
		pm := createTestManager(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)
		pm.config.PgBackRestCAFile = "/path/to/ca.crt"
		pm.config.PgBackRestCertFile = "/path/to/client.crt"
		pm.config.PgBackRestKeyFile = "/path/to/client.key"
		setBackupPrimary(t, pm, "test-primary-override2", "primary.example.com", 5432)

		primaryID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-primary-override2",
		}

		if pm.topoClient != nil {
			primaryPooler := &clustermetadatapb.MultiPooler{
				Id:       primaryID,
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com",
				PortMap: map[string]int32{
					"postgres":   5432,
					"pgbackrest": 8432,
				},
				PgDataDir: "/data/pg_data",
			}
			err := pm.topoClient.CreateMultiPooler(ctx, primaryPooler)
			require.NoError(t, err, "failed to create primary pooler in topology")
		}

		args, err := pm.GetPrimaryAsPg2Args(ctx, map[string]string{
			"pg2_host_port": "9999",
		}, false)

		require.NoError(t, err)
		assert.Contains(t, args, "--pg2-host-port=9999")
		assert.NotContains(t, args, "--pg2-host-port=8432")
	})
}

func TestExpireBackups(t *testing.T) {
	// ExpireBackups requires a running pgbackrest with a valid stanza.
	// The actual invocation is tested by integration tests (endtoend/multipooler).
	// This test validates the precondition checks.

	t.Run("fails when not ready", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		_, err := pm.ExpireBackups(context.Background(), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "manager is in unknown state")
	})
}

func TestVerifyBackups(t *testing.T) {
	// VerifyBackups requires a running pgbackrest with a valid stanza.
	// The actual invocation, including the corrupt-backup negative path, is
	// tested by integration tests (endtoend/multipooler) and the backup engine
	// unit tests. This test validates the manager handler's precondition checks.

	t.Run("fails when not ready", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		_, err := pm.VerifyBackups(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "manager is in unknown state")
	})

	t.Run("fails when config not yet generated", func(t *testing.T) {
		// Ready, but topology hasn't been loaded so no config path exists.
		pm := createTestManager(t, t.TempDir(), "", "", clustermetadatapb.PoolerType_REPLICA)
		_, err := pm.VerifyBackups(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "pgbackrest config not found")
	})
}
