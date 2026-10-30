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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/timer"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// createTestManager creates a minimal MultipoolerManager for testing
func createTestManager(t *testing.T, poolerDir, tableGroup, shard string, poolerType clustermetadatapb.PoolerType) *MultipoolerManager {
	return createTestManagerWithBackupLocation(t, poolerDir, tableGroup, shard, poolerType, "/tmp/backups")
}

// createTestManagerWithBackupLocation creates a minimal MultipoolerManager for testing with backup_location.
// backupLocation is the base path; the full path (with database/tablegroup/shard) is computed internally.
func createTestManagerWithBackupLocation(t *testing.T, poolerDir, tableGroup, shard string, poolerType clustermetadatapb.PoolerType, backupLocation string) *MultipoolerManager {
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

	multipoolerProto := &clustermetadatapb.Multipooler{
		Id:        multipoolerID,
		Type:      poolerType,
		PoolerDir: poolerDir,
		ShardRef: &clustermetadatapb.ShardRef{
			TableGroup: tableGroup,
			Shard:      shard,
			Database:   database,
		},
	}
	// Keep the Type ⇔ SelfLeadership invariant so the record validates: a
	// PRIMARY names itself; any other type carries no self-leadership.
	if poolerType == clustermetadatapb.PoolerType_PRIMARY {
		multipoolerProto.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY}
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

	pm := &MultipoolerManager{
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
	// allowBackupOnPrimary reads the routing role off the StateManager, not the
	// consensusMgr wired above (its fakeRuleStore has no cached position, so
	// CachedConsensusStatus would always return nil).
	pm.stateManager = newTestStateManagerForPoolerType(pm, multipoolerID, poolerType)

	// Build the backup engine the way the production constructor does, feeding
	// it the resolved repo config when one was supplied.
	pm.backup = newStubEngine(pm.logger, pm.record, backupengine.Settings{PgDataDir: poolerDir})
	if backupConfig != nil {
		pm.backup.SetBackupConfig(backupConfig)
	}
	useAnsweringSidecar(t, pm)
	return pm
}

// newTestStateManagerForPoolerType builds a StateManager whose derived routing
// role matches poolerType: PRIMARY poolers get an out-of-recovery pgMode and a
// consensus snapshot naming multipoolerID as the committed leader (see
// deriveRoutingState); anything else derives REPLICA off a nil snapshot.
func newTestStateManagerForPoolerType(pm *MultipoolerManager, multipoolerID *clustermetadatapb.ID, poolerType clustermetadatapb.PoolerType) *StateManager {
	consensusStatus := func() *clustermetadatapb.ConsensusStatus { return nil }
	if poolerType == clustermetadatapb.PoolerType_PRIMARY {
		consensusStatus = func() *clustermetadatapb.ConsensusStatus {
			return &clustermetadatapb.ConsensusStatus{
				Id: multipoolerID,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
						LeaderId:   multipoolerID,
					}},
				},
			}
		}
	}
	ssm := NewStateManager(pm.logger, pm.record, consensusStatus)
	if poolerType == clustermetadatapb.PoolerType_PRIMARY {
		ssm.pgMode = pgmode.Primary
	}
	return ssm
}

// setBackupPrimary seeds the ReplicationPrimary on the test manager so the
// backup paths that read pm.consensusMgr.GetReplicationPrimary() see a
// configured primary. Synthetic rule at term 1 is sufficient — no consumer
// of rp.GetPosition().GetDecision() reads cohort_members or durability_policy.
func setBackupPrimary(t *testing.T, pm *MultipoolerManager, primaryName, host string, port int32) {
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
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			LeaderId:   id,
		}},
		Primary: &clustermetadatapb.PoolerAddress{Id: id, Host: host, PostgresPort: port},
	}))
	// A replica's backup names its primary through topology: the sidecar needs
	// the primary's pgBackRest port and data directory.
	if pm.topoClient != nil {
		putTestMultipooler(t, pm.topoClient, &clustermetadatapb.Multipooler{
			Id:        id,
			Type:      clustermetadatapb.PoolerType_PRIMARY,
			Hostname:  host,
			PortMap:   map[string]int32{"postgres": port, "pgbackrest": 8432},
			PgDataDir: "/data/pg_data",
		})
	}
}

// putTestMultipooler writes mp to topology, replacing any record already there.
func putTestMultipooler(t *testing.T, ts topoclient.Store, mp *clustermetadatapb.Multipooler) {
	t.Helper()
	if err := ts.CreateMultipooler(t.Context(), mp); err != nil {
		_, uerr := ts.UpdateMultipoolerFields(t.Context(), mp.GetId(), func(cur *clustermetadatapb.Multipooler) error {
			cur.Hostname, cur.PortMap, cur.PgDataDir, cur.Type = mp.GetHostname(), mp.GetPortMap(), mp.GetPgDataDir(), mp.GetType()
			return nil
		})
		require.NoError(t, uerr, "create failed with %v", err)
	}
}

func TestBackup_Validation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		poolerDir    string
		backupType   string
		poolerType   clustermetadatapb.PoolerType
		forcePrimary bool
		staticLeader bool
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
			name:         "Static leader without force flag",
			poolerDir:    "/tmp/test",
			backupType:   "full",
			poolerType:   clustermetadatapb.PoolerType_PRIMARY,
			staticLeader: true,
			expectError:  true,                       // Will fail on pgbackrest execution but validation passes
			errorMsg:     "pgbackrest backup failed", // Execution error, not validation
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Provide backup location for tests that need to reach pgbackrest execution or validation
			backupLocation := "/tmp/test-backups"
			pm := createTestManagerWithBackupLocation(t, tt.poolerDir, "", "", tt.poolerType, backupLocation)
			if tt.staticLeader {
				// A static leader's engine runs backups without the etcd lease.
				pm.config.StaticLeader = true
				backupConfig, err := backup.NewConfig(utils.FilesystemBackupLocation(backupLocation))
				require.NoError(t, err)
				pm.backup = newStubEngine(pm.logger, pm.record, backupengine.Settings{PgDataDir: tt.poolerDir, NoBackupLease: true})
				pm.backup.SetBackupConfig(backupConfig)
			}

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
	// An empty repository: info prints no stanzas.
	stubPgbackrest(t, "#!/bin/sh\necho '[]'\n")

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
			pm := createTestManager(t, tt.poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)

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

	// Create test manager with the pooler directory
	pm := createTestManagerWithBackupLocation(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)

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

	// The sidecar adds the stanza and the config itself, so the config is never named here.
	assert.NotContains(t, args, "--config", "the config is the sidecar's to name")

	// Verify backup command structure
	assert.Contains(t, args, "--stanza=multigres", "command should include stanza")
	assert.Contains(t, args, "--type=full", "command should include backup type")
	assert.Contains(t, args, "backup", "command should be backup")
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
	pm := createTestManagerWithBackupLocation(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)
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
	pm := createTestManagerWithBackupLocation(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)
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

			pm := &MultipoolerManager{
				config:     &Config{},
				serviceID:  multipoolerID,
				topoClient: ts,
				record: newRecordFromProto(&clustermetadatapb.Multipooler{
					Id:        multipoolerID,
					Type:      clustermetadatapb.PoolerType_PRIMARY,
					PoolerDir: poolerDir,
					ShardRef: &clustermetadatapb.ShardRef{
						TableGroup: tt.tableGroup,
						Shard:      tt.shard,
						Database:   "test-database",
					},
					// A PRIMARY record must name itself as leader (the record invariant).
					RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
				}),
				state:      ManagerStateReady,
				actionLock: actionlock.NewActionLock(),
				logger:     slog.Default(),
				pgMonitor:  timer.NewPeriodicRunner(context.TODO(), 10*time.Second),
			}
			pm.stateManager = newTestStateManagerForPoolerType(pm, multipoolerID, clustermetadatapb.PoolerType_PRIMARY)
			pm.backup = newStubEngine(pm.logger, pm.record, backupengine.Settings{PgDataDir: poolerDir})
			pm.backup.SetBackupConfig(backupConfig)

			_, err := pm.Backup(ctx, true, "full", "test-job-id", nil)
			require.Error(t, err, "backup with empty %s should be rejected", tt.name)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestExpireBackups(t *testing.T) {
	// ExpireBackups requires a running pgbackrest with a valid stanza.
	// The actual invocation is tested by integration tests (endtoend/multipooler).
	// This test validates the precondition checks.

	t.Run("fails when not ready", func(t *testing.T) {
		pm := &MultipoolerManager{}
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
		pm := &MultipoolerManager{}
		_, err := pm.VerifyBackups(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "manager is in unknown state")
	})
}

// TestBackup_CallbackFailure_ErrorNotAttributedToLeaseAcquisition verifies that
// a pgbackrest failure occurring *after* the backup lease is already held
// surfaces as-is — it must not be relabeled as a lease-acquisition failure,
// since the lease was never the problem.
func TestBackup_CallbackFailure_ErrorNotAttributedToLeaseAcquisition(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))

	mockScript := `#!/bin/bash
echo "ERROR: simulated backup failure" >&2
exit 1
`
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "pgbackrest"), []byte(mockScript), 0o755))
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	poolerDir := filepath.Join(tmpDir, "pooler")
	require.NoError(t, os.MkdirAll(poolerDir, 0o755))
	pm := createTestManagerWithBackupLocation(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)
	setBackupPrimary(t, pm, "primary-pooler", "primary.local", 5432)
	overrides := map[string]string{"pg2_path": filepath.Join(poolerDir, "pg_data")}

	_, err := pm.Backup(ctx, false, "full", "test-job-id", overrides)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "failed to acquire backup lease",
		"a pgbackrest failure during the backup must not be mislabeled as a lease-acquisition failure")
	assert.Contains(t, err.Error(), "pgbackrest backup failed")
}

// TestBackup_LeaseAcquisitionFailure_DoesNotInvokeCallback verifies that when
// the backup lease genuinely cannot be acquired — here, the post-steal
// grace-period wait is aborted by context cancellation before the callback
// ever runs — the error is correctly attributed to lease acquisition.
func TestBackup_LeaseAcquisitionFailure_DoesNotInvokeCallback(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()

	shardRef := &clustermetadatapb.ShardRef{
		Database:   "test-database",
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	// Another pooler holds the lease. WithStolenBackupLease will revoke it and
	// wait out BackupLeaseStealGracePeriod (5s) before reacquiring — Backup()
	// below is given a context expiring in 50ms, well before that, so the
	// grace-period wait aborts via ctx.Done() and the callback never runs.
	_, otherUnlock, err := ts.TryLockBackup(t.Context(), shardRef, "other-pooler-backup")
	require.NoError(t, err)
	var otherErr error
	defer otherUnlock(&otherErr)

	poolerDir := t.TempDir()
	pm := &MultipoolerManager{
		state:      ManagerStateReady,
		topoClient: ts,
		actionLock: actionlock.NewActionLock(),
		logger:     slog.Default(),
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "test-multipooler",
			},
			PoolerDir: poolerDir,
			ShardRef:  shardRef,
		}),
	}
	pm.backup = newStubEngine(pm.logger, pm.record, backupengine.Settings{})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_, err = pm.Backup(ctx, false, "full", "test-job-id", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to acquire backup lease")
	assert.True(t, errors.Is(err, context.DeadlineExceeded),
		"expected the lease-acquisition timeout to surface as context.DeadlineExceeded, got: %v", err)
}

// TestBackup_StaticLeaderTakesNoLease shows that a static leader backs up
// without the shard's etcd backup lease: another pooler holding it does not
// stop the backup, which proceeds to pgbackrest.
func TestBackup_StaticLeaderTakesNoLease(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()
	shardRef := &clustermetadatapb.ShardRef{
		Database:   "test-database",
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}
	_, otherUnlock, err := ts.TryLockBackup(t.Context(), shardRef, "other-pooler-backup")
	require.NoError(t, err)
	var otherErr error
	defer otherUnlock(&otherErr)

	poolerDir := t.TempDir()
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-multipooler"}
	pm := &MultipoolerManager{
		config:     &Config{StaticLeader: true},
		state:      ManagerStateReady,
		topoClient: ts,
		actionLock: actionlock.NewActionLock(),
		logger:     slog.Default(),
		record:     newRecordFromProto(&clustermetadatapb.Multipooler{Id: id, PoolerDir: poolerDir, ShardRef: shardRef}),
	}
	pm.stateManager = newTestStateManagerForPoolerType(pm, id, clustermetadatapb.PoolerType_PRIMARY)
	pm.backup = newStubEngine(pm.logger, pm.record, backupengine.Settings{NoBackupLease: true})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = pm.Backup(ctx, false, "full", "test-job-id", nil)
	require.Error(t, err, "no backup configuration is set, so the backup itself fails")
	assert.NotContains(t, err.Error(), "lease")
}

func TestGetPrimaryAsPg2Args(t *testing.T) {
	poolerDir := t.TempDir()
	pm := createTestManager(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA)
	setBackupPrimary(t, pm, "test-primary", "primary.example.com", 5432)

	putTestMultipooler(t, pm.topoClient, &clustermetadatapb.Multipooler{
		Id:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-primary"},
		Type:      clustermetadatapb.PoolerType_PRIMARY,
		Hostname:  "primary.example.com",
		PortMap:   map[string]int32{"postgres": 5432, "pgbackrest": 8432},
		PgDataDir: "/data/pg_data",
	})

	got, err := pm.GetPrimaryAsPg2Args(context.Background(), nil, false)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"--pg2-host=primary.example.com",
		"--pg2-path=/data/pg_data",
		"--pg2-host-port=8432",
	}, got, "the sidecar adds the host type and the TLS material itself")

	t.Run("overrides replace in place and extras come out sorted", func(t *testing.T) {
		overrides := map[string]string{"pg2_path": "/other/pgdata", "repo": "1", "pg2_host_port": "9000"}
		for range 20 {
			got, err := pm.GetPrimaryAsPg2Args(context.Background(), overrides, false)
			require.NoError(t, err)
			assert.Equal(t, []string{
				"--pg2-host=primary.example.com",
				"--pg2-path=/other/pgdata",
				"--pg2-host-port=9000",
				"--repo=1",
			}, got)
		}
	})

	t.Run("a primary pooler backs up locally", func(t *testing.T) {
		pm := createTestManager(t, t.TempDir(), "", "", clustermetadatapb.PoolerType_PRIMARY)
		got, err := pm.GetPrimaryAsPg2Args(context.Background(), nil, false)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("a forced primary backup is local even on a replica", func(t *testing.T) {
		pm := createTestManager(t, t.TempDir(), "", "", clustermetadatapb.PoolerType_REPLICA)
		got, err := pm.GetPrimaryAsPg2Args(context.Background(), nil, true)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("a replica with no primary recorded is an error", func(t *testing.T) {
		pm := createTestManager(t, t.TempDir(), "", "", clustermetadatapb.PoolerType_REPLICA)
		_, err := pm.GetPrimaryAsPg2Args(context.Background(), nil, false)
		require.ErrorContains(t, err, "primary host/port not configured")
	})

	t.Run("a primary without a pgbackrest port is an error", func(t *testing.T) {
		pm := createTestManager(t, t.TempDir(), "", "", clustermetadatapb.PoolerType_REPLICA)
		setBackupPrimary(t, pm, "no-port-primary", "primary.example.com", 5432)
		putTestMultipooler(t, pm.topoClient, &clustermetadatapb.Multipooler{
			Id:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "no-port-primary"},
			Type:      clustermetadatapb.PoolerType_PRIMARY,
			PgDataDir: "/data/pg_data",
		})
		_, err := pm.GetPrimaryAsPg2Args(context.Background(), nil, false)
		require.Error(t, err)
	})
}

// TestRestore_SidecarModeNormalizesBeforeStart checks the order of a
// restore: the restore runs through the sidecar, and the restored
// data directory's commands are rewritten for this pooler before postgres is
// started, not after.
func TestRestore_SidecarModeNormalizesBeforeStart(t *testing.T) {
	var poolerDirForRestore string
	var restoreArgs []string
	fake := &fakeSidecarServer{status: &pb.StatusResponse{}}
	sock := serveFakeSidecar(t, fake)
	fake.run = func(_ context.Context, req *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
		assert.Equal(t, pb.Command_COMMAND_RESTORE, req.GetCommand())
		restoreArgs = req.GetArgs()
		dataDir := filepath.Join(poolerDirForRestore, "pg_data")
		require.NoError(t, os.MkdirAll(dataDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16"), 0o644))
		donor := filepath.Join(t.TempDir(), "pooler-9")
		stale := "restore_command = " + quoteConf(backup.SidecarRestoreCommand("/run/other/wal.sock", filepath.Join(donor, constants.RestoreCommandPIDFile))) + "\n"
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "postgresql.auto.conf"), []byte(stale), 0o600))
		return stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{ExitCode: 0}})
	}

	pm, _, atRestart := newRestartAsStandbyTestManager(t, mock.NewQueryService(), "", func(cfg *Config, m *testutil.MockPgCtldService, dir string) {
		sidecarMode(cfg, m, dir)
		cfg.PgBackRestControlSocket = sock
		poolerDirForRestore = dir
		// The engine reads PGDATA when it is built.
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(dir, "pg_data"))
	})
	require.NoError(t, os.RemoveAll(filepath.Join(poolerDirForRestore, "pg_data")))
	cfg, err := backup.NewConfig(utils.FilesystemBackupLocation("/backups"))
	require.NoError(t, err)
	pm.backup.SetBackupConfig(cfg)

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// What happens after postgres starts is not under test, so its result is not
	// either.
	_ = pm.restoreFromBackupLocked(lockCtx, "20250104-100000F")

	assert.Equal(t, []string{"--type=standby", "--set=20250104-100000F"}, restoreArgs,
		"no stanza, config or recovery option reaches the sidecar")
	require.NotEmpty(t, *atRestart, "postgres was never started")
	want := backup.SidecarRestoreCommand(testWALSocket, filepath.Join(poolerDirForRestore, constants.RestoreCommandPIDFile))
	got, ok := autoConfValue(t, *atRestart, "restore_command")
	require.True(t, ok)
	assert.Equal(t, want, got, "the restored command must be this pooler's by the time postgres starts")
}

// asyncSidecarRestore is a sidecar whose restore, like the real one, keeps
// running after the Run call that began it has failed: it ignores the request
// context, writes PG_VERSION halfway through, and only finishes when released.
type asyncSidecarRestore struct {
	fake    *fakeSidecarServer
	dataDir string

	mu      sync.Mutex
	runs    int
	release chan struct{}
	succeed bool
	midway  chan struct{}
	// startDelay holds a restore back after its request arrived, to model a Run
	// delivered after the client has already seen an error.
	startDelay time.Duration
	// refused counts restores pgBackRest would refuse: a non-empty target.
	refused int
	// autoConfAsDir makes a successful restore leave postgresql.auto.conf
	// unreadable, so the steps after the restore fail.
	autoConfAsDir bool
}

func newAsyncSidecarRestore(t *testing.T) (*asyncSidecarRestore, string) {
	t.Helper()
	a := &asyncSidecarRestore{
		fake:    &fakeSidecarServer{status: &pb.StatusResponse{}},
		release: make(chan struct{}),
		midway:  make(chan struct{}, 8),
	}
	sock := serveFakeSidecar(t, a.fake)
	a.fake.run = func(_ context.Context, req *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
		switch req.GetCommand() {
		case pb.Command_COMMAND_INFO:
			// Like the real gate, info is refused while a restore runs.
			a.fake.mu.Lock()
			running := a.fake.status.RestoreInProgress
			a.fake.mu.Unlock()
			if running {
				return connect.NewError(connect.CodeUnavailable, errors.New("a restore is in progress"))
			}
			_ = stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_Stdout{Stdout: []byte(
				`[{"backup":[{"label":"20250104-100000F","annotation":{"table_group":"` + constants.DefaultTableGroup + `","shard":"` + constants.DefaultShard + `","job_id":"j"}}],"repo":[{"key":1,"status":{"code":0,"message":"ok"}}]}]`)}})
			return stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{ExitCode: 0}})
		case pb.Command_COMMAND_RESTORE:
		default:
			return stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{ExitCode: 0}})
		}

		a.mu.Lock()
		a.runs++
		release, succeed, delay := a.release, a.succeed, a.startDelay
		a.mu.Unlock()
		time.Sleep(delay)
		// pgBackRest refuses to restore into a directory that is not empty.
		if entries, err := os.ReadDir(a.dataDir); err == nil && len(entries) > 0 {
			a.mu.Lock()
			a.refused++
			a.mu.Unlock()
			_ = stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_Stderr{Stderr: []byte("ERROR: [040]: target path is not empty")}})
			return stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{ExitCode: 40}})
		}
		a.setStatus(true, false)
		require.NoError(t, os.MkdirAll(a.dataDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(a.dataDir, "PG_VERSION"), []byte("16"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(a.dataDir, "half"), []byte("x"), 0o644))
		a.midway <- struct{}{}
		<-release
		if succeed && a.autoConfAsDir {
			require.NoError(t, os.MkdirAll(filepath.Join(a.dataDir, "postgresql.auto.conf"), 0o755))
		} else if succeed {
			donor := filepath.Join(t.TempDir(), "pooler-9")
			stale := "restore_command = " + quoteConf(backup.SidecarRestoreCommand("/run/other/wal.sock", filepath.Join(donor, constants.RestoreCommandPIDFile))) + "\n"
			require.NoError(t, os.WriteFile(filepath.Join(a.dataDir, "postgresql.auto.conf"), []byte(stale), 0o600))
		}
		a.setStatus(false, succeed)
		code := int32(1)
		if succeed {
			code = 0
		}
		return stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{ExitCode: code}})
	}
	return a, sock
}

func (a *asyncSidecarRestore) setStatus(inProgress, ok bool) {
	a.fake.mu.Lock()
	a.fake.status.RestoreInProgress = inProgress
	a.fake.status.LastRestoreSucceeded = ok
	a.fake.mu.Unlock()
}

func (a *asyncSidecarRestore) runCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.runs
}

// finish lets the running restore end, successfully or not.
func (a *asyncSidecarRestore) finish(succeed bool) {
	a.mu.Lock()
	a.succeed = succeed
	close(a.release)
	a.release = make(chan struct{})
	a.mu.Unlock()
}

type asyncRestoreFixture struct {
	mockPgctld *testutil.MockPgCtldService
	pm         *MultipoolerManager
	a          *asyncSidecarRestore
	atRestart  *string
	restarted  func() bool
	poolerDir  string
	dataDir    string
}

func newAsyncRestoreFixture(t *testing.T) *asyncRestoreFixture {
	t.Helper()
	return newAsyncRestoreFixtureWith(t, "")
}

// newAsyncRestoreFixtureWith builds the fixture with pgctld reporting STOPPED
// (the state the monitor starts postgres from) and, when sockOverride is set,
// a sidecar client pointed there instead of at the fake.
func newAsyncRestoreFixtureWith(t *testing.T, sockOverride string) *asyncRestoreFixture {
	t.Helper()
	a, sock := newAsyncSidecarRestore(t)
	if sockOverride != "" {
		sock = sockOverride
	}
	var dir string
	var mockRef *testutil.MockPgCtldService
	pm, _, atRestart := newRestartAsStandbyTestManager(t, mock.NewQueryService(), "", func(cfg *Config, m *testutil.MockPgCtldService, d string) {
		sidecarMode(cfg, m, d)
		mockRef = m
		m.StatusResponse = &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED}
		cfg.PgBackRestControlSocket = sock
		dir = d
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(d, "pg_data"))
	})
	a.dataDir = filepath.Join(dir, "pg_data")
	require.NoError(t, os.RemoveAll(a.dataDir))
	cfg, err := backup.NewConfig(utils.FilesystemBackupLocation("/backups"))
	require.NoError(t, err)
	pm.backup.SetBackupConfig(cfg)
	return &asyncRestoreFixture{
		pm: pm, a: a, atRestart: atRestart, poolerDir: dir, dataDir: a.dataDir, mockPgctld: mockRef,
		restarted: func() bool { return *atRestart != "" },
	}
}

func (f *asyncRestoreFixture) restore(t *testing.T, timeout time.Duration) error {
	t.Helper()
	lockCtx, err := f.pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer f.pm.actionLock.Release(lockCtx)
	ctx, cancel := context.WithTimeout(lockCtx, timeout)
	defer cancel()
	return f.pm.restoreFromBackupLocked(ctx, "20250104-100000F")
}

func (f *asyncRestoreFixture) hasPGVersion() bool {
	_, err := os.Stat(filepath.Join(f.dataDir, "PG_VERSION"))
	return err == nil
}

func (f *asyncRestoreFixture) hasSentinel() bool {
	_, err := os.Stat(filepath.Join(f.poolerDir, constants.RestoreSentinelFile))
	return err == nil
}

// A Run call that dies while the sidecar's restore carries on must never lead
// to PGDATA being deleted under it, and what is left once it ends decides what
// happens next.
func TestRestore_SidecarCallFailureNeverDeletesUnderRunningRestore(t *testing.T) {
	undo := backupengine.OverrideSidecarRestoreTimingForTest(10*time.Second, 5*time.Millisecond)
	t.Cleanup(undo)

	t.Run("the restore then succeeds: success, nothing deleted, postgres starts after", func(t *testing.T) {
		f := newAsyncRestoreFixture(t)
		f.a.finish(true) // consumed below: re-arm for the real run
		f.a.mu.Lock()
		f.a.release = make(chan struct{})
		f.a.succeed = true
		f.a.mu.Unlock()

		errc := make(chan error, 1)
		go func() { errc <- f.restore(t, 300*time.Millisecond) }()
		<-f.a.midway
		time.Sleep(600 * time.Millisecond) // the call has failed; the restore has not ended
		assert.True(t, f.hasPGVersion(), "PGDATA must survive a failed call while the restore runs")
		assert.True(t, f.hasSentinel())
		assert.False(t, f.restarted(), "postgres must not start on a half-restored directory")
		select {
		case err := <-errc:
			t.Fatalf("restore returned while the sidecar was still restoring: %v", err)
		default:
		}
		f.a.finish(true)
		<-errc // later steps may fail in this harness; only the data matters
		assert.True(t, f.hasPGVersion(), "a restore that finished successfully is kept")
		assert.False(t, f.hasSentinel(), "and the marker is cleared")
		// The caller's context ended during the wait, so the steps after the restore
		// fail here; the monitor's next tick starts postgres on the kept directory,
		// normalizing the archive commands first. What matters is that postgres
		// never started while the restore ran, asserted above.
	})

	t.Run("the restore then fails: nothing is deleted by the failed call, the next attempt clears it", func(t *testing.T) {
		f := newAsyncRestoreFixture(t)
		errc := make(chan error, 1)
		go func() { errc <- f.restore(t, 300*time.Millisecond) }()
		<-f.a.midway
		time.Sleep(600 * time.Millisecond)
		assert.True(t, f.hasPGVersion(), "not deleted while it runs")
		f.a.finish(false)
		require.Error(t, <-errc)
		assert.True(t, f.hasPGVersion(), "a failed call deletes nothing: only restoreAndStartPostgres does, once the sidecar confirms")
		assert.True(t, f.hasSentinel(), "the marker keeps the leftovers untrusted")
		assert.False(t, f.pm.hasDataDirectory())
		assert.False(t, f.restarted())

		f.a.mu.Lock()
		f.a.succeed = true
		f.a.mu.Unlock()
		go func() { <-f.a.midway; f.a.finish(true) }()
		_ = f.pm.restoreAndStartPostgresForTest(t, f)
		f.a.mu.Lock()
		refused := f.a.refused
		f.a.mu.Unlock()
		assert.Zero(t, refused, "the unfinished directory was cleared, after the sidecar confirmed the restore ended, before the fresh restore")
		assert.False(t, f.hasSentinel())
	})

	t.Run("a restore that never ends is left alone", func(t *testing.T) {
		f := newAsyncRestoreFixture(t)
		undo := backupengine.OverrideSidecarRestoreTimingForTest(400*time.Millisecond, 5*time.Millisecond)
		defer undo()
		err := make(chan error, 1)
		go func() { err <- f.restore(t, 100*time.Millisecond) }()
		<-f.a.midway
		select {
		case e := <-err:
			require.ErrorIs(t, e, backupengine.ErrRestoreUnsettled)
		case <-time.After(10 * time.Second):
			t.Fatal("an unsettled restore must give up waiting")
		}
		assert.True(t, f.hasPGVersion(), "unsettled means hands off")
		assert.True(t, f.hasSentinel(), "and the next start must not trust it")
		assert.False(t, f.restarted())
		f.a.finish(false)
	})
}

// A multipooler that restarts while the sidecar is restoring sees PG_VERSION
// appear without having started the restore itself.
func TestRestore_SidecarRestartMidRestore(t *testing.T) {
	f := newAsyncRestoreFixture(t)
	require.NoError(t, os.MkdirAll(f.dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(f.dataDir, "PG_VERSION"), []byte("16"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(f.poolerDir, constants.RestoreSentinelFile), []byte("restoring\n"), 0o600))
	f.a.setStatus(true, false)

	assert.False(t, f.pm.hasDataDirectory(), "a directory the sidecar is still restoring is not initialized")
	err := f.restore(t, 2*time.Second)
	require.Error(t, err)
	assert.Zero(t, f.a.runCount(), "the monitor must not start a second restore")
	assert.True(t, f.hasPGVersion(), "nor delete under the first")
	assert.False(t, f.restarted())

	// The restore ends; without success the unfinished directory is replaced.
	f.a.setStatus(false, false)
	assert.False(t, f.pm.hasDataDirectory(), "the marker keeps an unfinished directory untrusted after it ended")
	go func() { <-f.a.midway; f.a.finish(true) }()
	f.a.mu.Lock()
	f.a.succeed = true
	f.a.mu.Unlock()
	_ = f.pm.restoreAndStartPostgresForTest(t, f)
	assert.Equal(t, 1, f.a.runCount(), "one fresh restore ran onto a cleared directory")
	assert.False(t, f.hasSentinel())
	assert.True(t, f.pm.hasDataDirectory(), "a finished restore is trusted")
}

// A sidecar that cannot be reached must not make an initialized directory look
// uninitialized, nor allow a delete.
func TestHasDataDirectory_SidecarUnreachableIsNotAnAlarm(t *testing.T) {
	f := newAsyncRestoreFixture(t)
	require.NoError(t, os.MkdirAll(f.dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(f.dataDir, "PG_VERSION"), []byte("16"), 0o644))
	f.pm.sidecar = newSidecarClient(filepath.Join(t.TempDir(), "gone"))
	assert.True(t, f.pm.hasDataDirectory())

	// But with a marker present, only a confirmed idle sidecar allows cleanup.
	require.NoError(t, os.WriteFile(filepath.Join(f.poolerDir, constants.RestoreSentinelFile), []byte("x"), 0o600))
	assert.Error(t, f.pm.prepareRestore(t.Context()))
	assert.True(t, f.hasPGVersion())
}

func (f *asyncRestoreFixture) writeMarkerAndPGVersion(t *testing.T) {
	t.Helper()
	require.NoError(t, os.MkdirAll(f.dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(f.dataDir, "PG_VERSION"), []byte("16"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(f.poolerDir, constants.RestoreSentinelFile), []byte("restoring\n"), 0o600))
}

// The monitor decides to start postgres from pgctld, which only stats PG_VERSION.
// No start may be decided while a restore is unsettled.
func TestMonitor_SidecarRestoreUnsettledNeverStartsPostgres(t *testing.T) {
	decide := func(t *testing.T, f *asyncRestoreFixture) (postgresState, remedialAction) {
		t.Helper()
		state, err := f.pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		require.True(t, state.dirInitialized, "pgctld sees PG_VERSION and says initialized")
		return state, f.pm.determineRemedialAction(t.Context(), state)
	}

	t.Run("a restart mid-restore: marker on disk and the restore running", func(t *testing.T) {
		f := newAsyncRestoreFixture(t)
		f.writeMarkerAndPGVersion(t)
		f.a.setStatus(true, false)

		state, action := decide(t, f)
		assert.True(t, state.restoreSentinelPresent)
		assert.Equal(t, remedialActionRestoreFromBackup, action, "never remedialActionStartPostgres")

		// And acting on it neither starts postgres nor sends a second restore.
		_ = f.pm.restoreAndStartPostgresForTest(t, f)
		assert.Empty(t, f.mockPgctld.StartCalls)
		assert.Empty(t, f.mockPgctld.RestartCalls)
		assert.Zero(t, f.a.runCount())
		assert.True(t, f.hasPGVersion())
	})

	t.Run("the marker alone, with the sidecar idle", func(t *testing.T) {
		f := newAsyncRestoreFixture(t)
		f.writeMarkerAndPGVersion(t)
		state, action := decide(t, f)
		assert.True(t, state.restoreSentinelPresent)
		assert.Equal(t, remedialActionRestoreFromBackup, action)
	})

	t.Run("no marker: nothing of ours is unaccounted for, even while the sidecar is busy or down", func(t *testing.T) {
		f := newAsyncRestoreFixture(t)
		require.NoError(t, os.MkdirAll(f.dataDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(f.dataDir, "PG_VERSION"), []byte("16"), 0o644))
		f.a.setStatus(true, false)
		state, action := decide(t, f)
		assert.False(t, state.restoreSentinelPresent, "the marker is authoritative")
		assert.Equal(t, remedialActionStartPostgres, action)

		down := newAsyncRestoreFixtureWith(t, filepath.Join(t.TempDir(), "gone"))
		require.NoError(t, os.MkdirAll(down.dataDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(down.dataDir, "PG_VERSION"), []byte("16"), 0o644))
		state, action = decide(t, down)
		assert.False(t, state.restoreSentinelPresent, "a down sidecar must not keep a valid PGDATA from starting")
		assert.Equal(t, remedialActionStartPostgres, action)
	})

	t.Run("a marker with the sidecar down stays unsettled and deletes nothing", func(t *testing.T) {
		f := newAsyncRestoreFixtureWith(t, filepath.Join(t.TempDir(), "gone"))
		f.writeMarkerAndPGVersion(t)
		state, action := decide(t, f)
		assert.True(t, state.restoreSentinelPresent)
		assert.Equal(t, remedialActionRestoreFromBackup, action)
		_ = f.restore(t, time.Second)
		assert.True(t, f.hasPGVersion())
		assert.True(t, f.hasSentinel())
	})

	t.Run("settled: a start is decided and taken", func(t *testing.T) {
		f := newAsyncRestoreFixture(t)
		require.NoError(t, os.MkdirAll(f.dataDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(f.dataDir, "PG_VERSION"), []byte("16"), 0o644))
		state, action := decide(t, f)
		assert.False(t, state.restoreSentinelPresent)
		assert.Equal(t, remedialActionStartPostgres, action)
	})

	t.Run("startPostgres refuses by itself", func(t *testing.T) {
		f := newAsyncRestoreFixture(t)
		f.writeMarkerAndPGVersion(t)
		lockCtx, err := f.pm.actionLock.Acquire(t.Context(), "test")
		require.NoError(t, err)
		defer f.pm.actionLock.Release(lockCtx)
		require.Error(t, f.pm.startPostgres(lockCtx))
		assert.Empty(t, f.mockPgctld.StartCalls)
	})

	t.Run("after ErrRestoreUnsettled the next tick still does not start", func(t *testing.T) {
		undo := backupengine.OverrideSidecarRestoreTimingForTest(300*time.Millisecond, 5*time.Millisecond)
		defer undo()
		f := newAsyncRestoreFixture(t)
		errc := make(chan error, 1)
		go func() { errc <- f.restore(t, 100*time.Millisecond) }()
		<-f.a.midway
		require.ErrorIs(t, <-errc, backupengine.ErrRestoreUnsettled)
		_, action := decide(t, f)
		assert.Equal(t, remedialActionRestoreFromBackup, action)
		f.a.finish(false)
	})
}

// restoreAndStartPostgresForTest runs the monitor's restore action under the lock.
func (pm *MultipoolerManager) restoreAndStartPostgresForTest(t *testing.T, _ *asyncRestoreFixture) error {
	t.Helper()
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)
	return pm.restoreAndStartPostgres(lockCtx)
}

func TestHasDataDirectory_SidecarNeverWaitsOnTheSidecar(t *testing.T) {
	f := newAsyncRestoreFixture(t)
	require.NoError(t, os.MkdirAll(f.dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(f.dataDir, "PG_VERSION"), []byte("16"), 0o644))
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	f.a.fake.statusHook = func() { <-block }

	done := make(chan bool, 1)
	go func() { done <- f.pm.hasDataDirectory() }()
	select {
	case got := <-done:
		assert.True(t, got, "no marker: initialized, without asking the sidecar")
	case <-time.After(time.Second):
		t.Fatal("hasDataDirectory (run inside the manager Status RPC) waited on the sidecar")
	}

	assert.True(t, f.pm.dataDirPresentOrUntrusted(), "present")
	require.NoError(t, os.RemoveAll(f.dataDir))
	require.NoError(t, os.WriteFile(filepath.Join(f.poolerDir, constants.RestoreSentinelFile), []byte("x"), 0o600))
	assert.True(t, f.pm.dataDirPresentOrUntrusted(), "a directory being restored counts as present for a caller that creates data")
	assert.False(t, f.pm.hasDataDirectory())
}

func TestRestore_SidecarDownReturnsPromptlyAndDeletesNothing(t *testing.T) {
	f := newAsyncRestoreFixtureWith(t, filepath.Join(t.TempDir(), "gone"))
	start := time.Now()
	err := f.restore(t, 30*time.Second)
	require.Error(t, err)
	assert.ErrorIs(t, err, backupengine.ErrRunNotDelivered)
	assert.Less(t, time.Since(start), 5*time.Second, "an undelivered Run must not hold the action lock for the settle bound")
	assert.True(t, f.hasSentinel(), "kept: the next attempt settles it")
}

func TestRemedialActionTimeout_SidecarCoversTheEngineDeadline(t *testing.T) {
	f := newAsyncRestoreFixture(t)
	got := f.pm.remedialActionTimeout(remedialActionRestoreFromBackup)
	assert.Greater(t, got, backupengine.SidecarRestoreDeadline(), "the monitor must not cap the engine's deadline, with room for the steps after it")
}

// A multipooler restarted mid-restore must wait for the restore, not count
// the wait as failed recoveries until it quarantines itself.
func TestMonitor_SidecarRestoreWaitDoesNotQuarantine(t *testing.T) {
	f := newAsyncRestoreFixture(t)
	f.writeMarkerAndPGVersion(t)
	f.a.setStatus(true, false)
	clock := newFakeClock()
	f.pm.nowFn = clock.now
	f.pm.unrecoverableTimeout = 5 * time.Minute
	f.pm.unrecoverableMinAttempts = 3

	var logs bytes.Buffer
	f.pm.logger = slog.New(slog.NewTextHandler(&logs, nil))

	withLock(t, f.pm, func(ctx context.Context) {
		for range 12 {
			state, err := f.pm.discoverPostgresState(ctx)
			require.NoError(t, err)
			action := f.pm.determineRemedialAction(ctx, state)
			require.Equal(t, remedialActionRestoreFromBackup, action)
			actionErr := f.pm.takeRemedialAction(ctx, action, state)
			assert.NoError(t, actionErr, "waiting for a running restore is not a failed recovery")
			f.pm.trackRecoveryOutcome(ctx, action, state, actionErr)
			clock.advance(2 * time.Minute) // well past the timeout and the attempts floor
		}
	})

	quarantined, reason, _ := quarantineState(f.pm)
	assert.False(t, quarantined, "waiting for a restore must not quarantine the node: %s", reason)
	assert.Zero(t, f.pm.unrecoverableFailedAttempts)
	assert.Zero(t, f.a.runCount())
	assert.True(t, f.hasPGVersion())
	assert.LessOrEqual(t, strings.Count(logs.String(), "waiting for a restore to finish"), 6, "the log cadence is bounded, not one line per tick")
	assert.GreaterOrEqual(t, strings.Count(logs.String(), "waiting for a restore to finish"), 1)

	// And a down sidecar with a young marker waits too. (ErrRestoreUnsettled is
	// driven through takeRemedialAction by TestMonitor_SidecarUnsettledRestoreIsWaitingNotFailing.)
	down := newAsyncRestoreFixtureWith(t, filepath.Join(t.TempDir(), "gone"))
	down.writeMarkerAndPGVersion(t)
	down.pm.nowFn = clock.now
	down.pm.unrecoverableTimeout = 5 * time.Minute
	down.pm.unrecoverableMinAttempts = 3
	withLock(t, down.pm, func(ctx context.Context) {
		for range 6 {
			state, err := down.pm.discoverPostgresState(ctx)
			require.NoError(t, err)
			action := down.pm.determineRemedialAction(ctx, state)
			actionErr := down.pm.takeRemedialAction(ctx, action, state)
			down.pm.trackRecoveryOutcome(ctx, action, state, actionErr)
			clock.advance(2 * time.Minute)
		}
	})
	q, _, _ := quarantineState(down.pm)
	assert.False(t, q)
}

// An abandoned marker with an idle sidecar must converge: the monitor
// clears the unfinished directory and runs exactly one fresh restore onto it.
func TestMonitor_SidecarAbandonedMarkerConvergesWithOneFreshRestore(t *testing.T) {
	f := newAsyncRestoreFixture(t)
	f.writeMarkerAndPGVersion(t)
	require.NoError(t, os.WriteFile(filepath.Join(f.dataDir, "leftover"), []byte("x"), 0o644))
	go func() { <-f.a.midway; f.a.finish(true) }()
	f.a.mu.Lock()
	f.a.succeed = true
	f.a.mu.Unlock()

	withLock(t, f.pm, func(ctx context.Context) {
		state, err := f.pm.discoverPostgresState(ctx)
		require.NoError(t, err)
		require.True(t, state.restoreSentinelPresent)
		_ = f.pm.takeRemedialAction(ctx, f.pm.determineRemedialAction(ctx, state), state)
	})

	assert.Equal(t, 1, f.a.runCount(), "without the settled check the abandoned marker wedges and no restore ever runs")
	f.a.mu.Lock()
	refused := f.a.refused
	f.a.mu.Unlock()
	assert.Zero(t, refused, "the unfinished directory is emptied first, because pgBackRest refuses a non-empty target")
	assert.NoFileExists(t, filepath.Join(f.dataDir, "leftover"))
}

// The Run is delivered after the client has already seen an error. Nothing
// may be cleared or started until the late restore has run and ended.
func TestRestore_SidecarLateDeliveredRunStaysMarked(t *testing.T) {
	undo := backupengine.OverrideSidecarRestoreTimingForTest(10*time.Second, 5*time.Millisecond)
	t.Cleanup(undo)
	f := newAsyncRestoreFixture(t)
	f.a.mu.Lock()
	f.a.startDelay = 400 * time.Millisecond
	f.a.mu.Unlock()

	// The call times out before the sidecar's handler starts anything.
	err := f.restore(t, 100*time.Millisecond)
	require.Error(t, err)
	assert.True(t, f.hasSentinel(), "the marker stays on every failure path")

	// The late restore now starts and writes PG_VERSION, then fails.
	<-f.a.midway
	assert.False(t, f.pm.hasDataDirectory(), "a half-restored directory is not initialized")
	assert.False(t, f.pm.restoreSettled(t.Context()))
	f.a.finish(false)
	require.Eventually(t, func() bool { return f.a.fakeIdle() }, 5*time.Second, 10*time.Millisecond)
	assert.True(t, f.hasPGVersion())
	assert.True(t, f.hasSentinel(), "still untrusted after the late restore failed")
	assert.False(t, f.restarted())

	f.a.mu.Lock()
	f.a.succeed = true
	f.a.mu.Unlock()
	go func() { <-f.a.midway; f.a.finish(true) }()
	_ = f.pm.restoreAndStartPostgresForTest(t, f)
	f.a.mu.Lock()
	refused := f.a.refused
	f.a.mu.Unlock()
	assert.Zero(t, refused, "the unfinished directory was cleared before the fresh restore")
	assert.False(t, f.hasSentinel())
}

func (a *asyncSidecarRestore) fakeIdle() bool {
	a.fake.mu.Lock()
	defer a.fake.mu.Unlock()
	return !a.fake.status.RestoreInProgress
}

// A failure between the restore and the post-restore steps keeps the marker,
// so the generic start path cannot run on a directory that skipped them.
func TestRestore_SidecarMarkerSurvivesUntilPostRestoreStepsComplete(t *testing.T) {
	f := newAsyncRestoreFixture(t)
	f.a.mu.Lock()
	f.a.succeed = true
	f.a.autoConfAsDir = true // the archive reconfiguration cannot read postgresql.auto.conf
	f.a.mu.Unlock()
	go func() { <-f.a.midway; f.a.finish(true) }()

	err := f.restore(t, 30*time.Second)
	require.Error(t, err, "a post-restore step failed")
	assert.True(t, f.hasPGVersion(), "the restored data is kept")
	assert.True(t, f.hasSentinel(), "but still marked: the steps that failed have not been done")
	assert.False(t, f.pm.hasDataDirectory())
	assert.False(t, f.restarted())

	withLock(t, f.pm, func(ctx context.Context) {
		state, derr := f.pm.discoverPostgresState(ctx)
		require.NoError(t, derr)
		assert.Equal(t, remedialActionRestoreFromBackup, f.pm.determineRemedialAction(ctx, state),
			"never the generic start, which would skip the archive reconfiguration")
	})
}

func TestRemedialActionTimeout_SidecarCoversFirstBackupThenRestore(t *testing.T) {
	f := newAsyncRestoreFixture(t)
	got := f.pm.remedialActionTimeout(remedialActionCreateFirstBackup)
	assert.GreaterOrEqual(t, got, backup.BackupTimeout+backupengine.SidecarRestoreDeadline(),
		"a pooler that loses the bootstrap race restores on this same context")
}

// Shutdown must end a settling restore's wait even though the restore holds
// the action lock GracefulShutdown needs.
func TestGracefulShutdown_EndsASettlingRestoreBeforeTakingTheLock(t *testing.T) {
	undo := backupengine.OverrideSidecarRestoreTimingForTest(time.Hour, 5*time.Millisecond)
	t.Cleanup(undo)
	f := newAsyncRestoreFixture(t)

	errc := make(chan error, 1)
	go func() { errc <- f.restore(t, 100*time.Millisecond) }() // holds the action lock while it settles
	<-f.a.midway
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-errc:
		t.Fatalf("the restore should still be settling: %v", err)
	default:
	}

	done := make(chan struct{})
	go func() {
		f.pm.GracefulShutdown(t.Context())
		close(done)
	}()
	select {
	case err := <-errc:
		require.ErrorIs(t, err, backupengine.ErrRestoreUnsettled)
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not end the settle wait, which holds the lock it needs")
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("GracefulShutdown never got the action lock")
	}
	assert.True(t, f.hasSentinel())
	f.a.finish(false)
}

func setMarkerTime(t *testing.T, f *asyncRestoreFixture, at time.Time) {
	t.Helper()
	require.NoError(t, os.Chtimes(filepath.Join(f.poolerDir, constants.RestoreSentinelFile), at, at))
}

func tick(t *testing.T, f *asyncRestoreFixture, ctx context.Context) error {
	t.Helper()
	state, err := f.pm.discoverPostgresState(ctx)
	require.NoError(t, err)
	action := f.pm.determineRemedialAction(ctx, state)
	actionErr := f.pm.takeRemedialAction(ctx, action, state)
	f.pm.trackRecoveryOutcome(ctx, action, state, actionErr)
	return actionErr
}

// Waiting on a marker is bounded. A sidecar that cannot be asked, or that
// keeps reporting a restore, for longer than a restore can take counts;
// nothing is ever deleted or started.
func TestMonitor_SidecarMarkerWaitIsBounded(t *testing.T) {
	setup := func(t *testing.T, sock string) (*asyncRestoreFixture, *fakeClock) {
		f := newAsyncRestoreFixtureWith(t, sock)
		f.writeMarkerAndPGVersion(t)
		clock := newFakeClock()
		f.pm.nowFn = clock.now
		f.pm.unrecoverableTimeout = 5 * time.Minute
		f.pm.unrecoverableMinAttempts = 3
		return f, clock
	}
	gone := func(t *testing.T) string { return filepath.Join(t.TempDir(), "gone") }

	t.Run("an old marker with a down sidecar counts toward quarantine", func(t *testing.T) {
		f, clock := setup(t, gone(t))
		setMarkerTime(t, f, clock.now().Add(-restoreMarkerStaleAfter()-time.Minute))
		withLock(t, f.pm, func(ctx context.Context) {
			for range 6 {
				require.Error(t, tick(t, f, ctx), "past the bound the wait is a failed recovery")
				clock.advance(2 * time.Minute)
			}
		})
		q, _, _ := quarantineState(f.pm)
		assert.True(t, q, "a node whose sidecar never returns is replaced, not waited on forever")
		assert.True(t, f.hasPGVersion(), "and still nothing is deleted")
		assert.Empty(t, f.mockPgctld.StartCalls, "or started")
		assert.Greater(t, f.pm.markerAgeSec.Load(), int64(restoreMarkerStaleAfter().Seconds()), "the age gauge shows it")
	})

	t.Run("a young marker with a down sidecar never counts", func(t *testing.T) {
		f, clock := setup(t, gone(t))
		setMarkerTime(t, f, clock.now())
		withLock(t, f.pm, func(ctx context.Context) {
			for range 6 {
				require.NoError(t, tick(t, f, ctx))
				clock.advance(2 * time.Minute) // 12 minutes, well inside the bound
			}
		})
		q, _, _ := quarantineState(f.pm)
		assert.False(t, q)
	})

	t.Run("a restore Status reports running counts only once older than every sidecar bound", func(t *testing.T) {
		f, clock := setup(t, "")
		f.a.setStatus(true, false)
		setMarkerTime(t, f, clock.now())
		withLock(t, f.pm, func(ctx context.Context) {
			for range 6 {
				require.NoError(t, tick(t, f, ctx), "a young running restore is only waited on")
				clock.advance(2 * time.Minute)
			}
		})
		q, _, _ := quarantineState(f.pm)
		assert.False(t, q)

		setMarkerTime(t, f, clock.now().Add(-restoreMarkerStaleAfter()-time.Minute))
		withLock(t, f.pm, func(ctx context.Context) {
			for range 6 {
				require.Error(t, tick(t, f, ctx), "a restore running past every bound is stuck, a failed attempt")
				clock.advance(2 * time.Minute)
			}
		})
		q, _, _ = quarantineState(f.pm)
		assert.True(t, q, "a stuck sidecar ends in quarantine rather than waiting forever")
		assert.True(t, f.hasPGVersion(), "nothing is deleted")
		assert.Empty(t, f.mockPgctld.StartCalls, "or started")
		assert.Zero(t, f.a.runCount())
	})
}

// ErrRestoreUnsettled returned by the restore reaches the monitor as waiting.
func TestMonitor_SidecarUnsettledRestoreIsWaitingNotFailing(t *testing.T) {
	undo := backupengine.OverrideSidecarRestoreTimingForTest(300*time.Millisecond, 5*time.Millisecond)
	t.Cleanup(undo)
	f := newAsyncRestoreFixture(t)
	f.mockPgctld.StatusResponse = &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_NOT_INITIALIZED}
	f.pm.unrecoverableTimeout = time.Minute
	f.pm.unrecoverableMinAttempts = 1

	lockCtx, err := f.pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer f.pm.actionLock.Release(lockCtx)
	ctx, cancel := context.WithTimeout(lockCtx, 100*time.Millisecond)
	defer cancel()

	// restoreFromBackupLocked alone returns the unsettled error...
	state := postgresState{pgctldAvailable: true}
	errc := make(chan error, 1)
	go func() { errc <- f.pm.takeRemedialAction(ctx, remedialActionRestoreFromBackup, state) }()
	<-f.a.midway
	actionErr := <-errc
	require.NoError(t, actionErr, "an unsettled restore is waiting, not a failed recovery")
	f.pm.trackRecoveryOutcome(lockCtx, remedialActionRestoreFromBackup, state, actionErr)
	assert.Zero(t, f.pm.unrecoverableFailedAttempts)
	assert.True(t, f.hasSentinel(), "the restore did start and is still marked")
	f.a.finish(false)
}

// One ERROR per marker, and WARNs on the wait cadence, not per tick or per RPC.
func TestRestoreMarkerLogs_AreDeduped(t *testing.T) {
	f := newAsyncRestoreFixture(t)
	f.writeMarkerAndPGVersion(t)
	clock := newFakeClock()
	f.pm.nowFn = clock.now
	var logs bytes.Buffer
	f.pm.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	for range 50 { // the manager Status RPC path, polled
		_ = f.pm.hasDataDirectory()
	}
	assert.Equal(t, 1, strings.Count(logs.String(), "level=ERROR"), "one ERROR per marker, not one per refresh")

	for range 13 { // 13 minutes of monitor ticks
		_ = f.pm.restoreSettled(t.Context())
		clock.advance(time.Minute)
	}
	warns := strings.Count(logs.String(), "level=WARN")
	assert.LessOrEqual(t, warns, 3, "WARN on the five-minute cadence of the wait log (0, 5 and 10 minutes)")
	assert.GreaterOrEqual(t, warns, 1)

	// A new marker is a new ERROR.
	require.NoError(t, os.Remove(filepath.Join(f.poolerDir, constants.RestoreSentinelFile)))
	assert.True(t, f.pm.hasDataDirectory())
	require.NoError(t, os.WriteFile(filepath.Join(f.poolerDir, constants.RestoreSentinelFile), []byte("x"), 0o600))
	setMarkerTime(t, f, clock.now().Add(time.Hour))
	_ = f.pm.hasDataDirectory()
	assert.Equal(t, 2, strings.Count(logs.String(), "level=ERROR"))
}
