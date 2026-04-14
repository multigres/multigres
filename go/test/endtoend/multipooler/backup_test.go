// Copyright 2025 Supabase, Inc.
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

package multipooler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/pb/pgctldservice"
	adminserver "github.com/multigres/multigres/go/services/multiadmin"
	"github.com/multigres/multigres/go/test/utils"
)

// removeDataDirectory removes the pg_data directory for a given pgctld instance
func removeDataDirectory(t *testing.T, dataDir string) {
	t.Helper()
	pgDataDir := filepath.Join(dataDir, "pg_data")
	err := os.RemoveAll(pgDataDir)
	require.NoError(t, err, "Should be able to remove pg_data directory")
	t.Logf("Removed pg_data directory: %s", pgDataDir)
}

// waitForJobCompletion polls GetBackupJobStatus until the job completes or fails
func waitForJobCompletion(t *testing.T, adminServer *adminserver.MultiAdminServer, jobID string, timeout time.Duration) *multiadminpb.GetBackupJobStatusResponse {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for job %s to complete", jobID)
			return nil
		case <-ticker.C:
			status, err := adminServer.GetBackupJobStatus(ctx, &multiadminpb.GetBackupJobStatusRequest{JobId: jobID})
			require.NoError(t, err, "GetBackupJobStatus should succeed")

			switch status.Status {
			case multiadminpb.JobStatus_JOB_STATUS_COMPLETED:
				t.Logf("Job %s completed successfully", jobID)
				return status
			case multiadminpb.JobStatus_JOB_STATUS_FAILED:
				t.Fatalf("Job %s failed: %s", jobID, status.ErrorMessage)
				return status
			default:
				t.Logf("Job %s status: %s", jobID, status.Status)
			}
		}
	}
}

func TestBackup_CreateListAndRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	backends := availableBackends
	for _, backend := range backends {
		t.Run(backend, func(t *testing.T) {
			setup := getSetupForBackend(t, backend)
			setupPoolerTest(t, setup, WithDropTables("backup_restore_test"))

			// Wait for both primary and standby managers to be ready
			waitForManagerReady(t, setup, setup.PrimaryMultipooler)
			waitForManagerReady(t, setup, setup.StandbyMultipooler)

			// Create backup client connections
			backupClient := createBackupClient(t, setup.PrimaryMultipooler.GrpcPort)
			standbyBackupClient := createBackupClient(t, setup.StandbyMultipooler.GrpcPort)

			// Connect to primary PostgreSQL database using Unix socket
			var err error
			db := connectToPostgresViaSocket(t,
				getPostgresSocketPath(setup.PrimaryPgctld.PoolerDir),
				setup.PrimaryPgctld.PgPort)
			defer db.Close()

			t.Log("Creating test table and inserting initial data...")

			// Create a test table (WithDropTables ensures it doesn't exist from previous runs)
			_, err = db.Exec(`
		CREATE TABLE backup_restore_test (
			id SERIAL PRIMARY KEY,
			data TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		)
	`)
			require.NoError(t, err, "Failed to create test table")

			// Insert initial rows (these should persist after restore)
			initialRows := []string{"row1_before_backup", "row2_before_backup", "row3_before_backup"}
			for _, data := range initialRows {
				_, err = db.Exec("INSERT INTO backup_restore_test (data) VALUES ($1)", data)
				require.NoError(t, err, "Failed to insert initial row: %s", data)
			}

			// Verify initial rows were inserted
			var countBefore int
			err = db.QueryRow("SELECT COUNT(*) FROM backup_restore_test").Scan(&countBefore)
			require.NoError(t, err)
			assert.Equal(t, len(initialRows), countBefore, "Initial row count should match")
			t.Logf("Inserted %d initial rows", countBefore)

			t.Run("CreateFullBackup", func(t *testing.T) {
				t.Log("Creating full backup...")
				fullBackupID := createAndVerifyBackup(t, backupClient, "full", true, 5*time.Minute, nil)

				t.Run("GetBackups_VerifyFullBackup", func(t *testing.T) {
					t.Log("Listing backups to verify full backup...")
					foundBackup := listAndFindBackup(t, backupClient, fullBackupID, 10)
					t.Logf("Backup verified in list: ID=%s, Status=%s, FinalLSN=%s",
						foundBackup.BackupId, foundBackup.Status, foundBackup.FinalLsn)
				})

				t.Run("RestoreAndVerify", func(t *testing.T) {
					// Insert additional rows (these should NOT persist after restore)
					additionalRows := []string{"row4_after_backup", "row5_after_backup"}
					for _, data := range additionalRows {
						_, err = db.Exec("INSERT INTO backup_restore_test (data) VALUES ($1)", data)
						require.NoError(t, err, "Failed to insert additional row: %s", data)
					}

					// Verify all rows exist before restore
					var countAfterInsert int
					err = db.QueryRow("SELECT COUNT(*) FROM backup_restore_test").Scan(&countAfterInsert)
					require.NoError(t, err)
					expectedCountBeforeRestore := len(initialRows) + len(additionalRows)
					assert.Equal(t, expectedCountBeforeRestore, countAfterInsert,
						"Count should include both initial and additional rows")

					listReq := &multipoolermanagerdata.GetBackupsRequest{
						Limit: 20,
					}

					listCtx := utils.WithShortDeadline(t)
					listResp, err := standbyBackupClient.GetBackups(listCtx, listReq)
					require.NoError(t, err, "Listing backups should succeed")
					require.NotNil(t, listResp, "List response should not be nil")

					// Find our backup in the standby's list
					foundBackup := findBackupInList(t, listResp.Backups, fullBackupID)
					assertBackupComplete(t, foundBackup, fullBackupID)
					t.Logf("Backup verified in standby's list: ID=%s, Status=%s, FinalLSN=%s",
						foundBackup.BackupId, foundBackup.Status, foundBackup.FinalLsn)

					// Update term to a higher value by doing a dummy SetPrimaryConnInfo
					higherTerm := int64(100)
					primary := &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{
							Component: clustermetadatapb.ID_MULTIPOOLER,
							Cell:      "test-cell",
							Name:      setup.PrimaryMultipooler.Name,
						},
						Hostname: "localhost",
						PortMap:  map[string]int32{"postgres": int32(setup.PrimaryPgctld.PgPort)},
					}
					updateTermReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
						Primary:               primary,
						StartReplicationAfter: false, // Don't restart replication
						StopReplicationBefore: false,
						CurrentTerm:           higherTerm,
						Force:                 false,
					}
					updateTermCtx := utils.WithTimeout(t, 30*time.Second)
					_, err = standbyBackupClient.SetPrimaryConnInfo(updateTermCtx, updateTermReq)
					require.NoError(t, err, "Should be able to update term")

					// Verify term was updated
					statusCtx := utils.WithShortDeadline(t)
					statusResp, err := standbyBackupClient.Status(statusCtx, &multipoolermanagerdata.StatusRequest{})
					require.NoError(t, err, "Should be able to get status after term update")
					require.NotNil(t, statusResp.Status.ConsensusTerm, "ConsensusTerm should not be nil")
					assert.Equal(t, higherTerm, statusResp.Status.ConsensusTerm.TermNumber, "Term should be updated to higher value")
					t.Log("Preparing standby for restore (stopping PostgreSQL and removing PGDATA)...")
					// Connect to standby's pgctld to stop PostgreSQL
					standbyPgctldConn, err := grpc.NewClient(
						fmt.Sprintf("localhost:%d", setup.StandbyPgctld.GrpcPort),
						grpc.WithTransportCredentials(insecure.NewCredentials()),
					)
					require.NoError(t, err)
					defer standbyPgctldConn.Close()
					standbyPgctldClient := pgctldservice.NewPgCtldClient(standbyPgctldConn)

					// Stop PostgreSQL on standby
					stopCtx := utils.WithTimeout(t, 2*time.Minute)
					_, err = standbyPgctldClient.Stop(stopCtx, &pgctldservice.StopRequest{Mode: "fast"})
					require.NoError(t, err, "Should be able to stop PostgreSQL on standby")

					// Remove pg_data directory
					removeDataDirectory(t, setup.StandbyPgctld.PoolerDir)

					restoreReq := &multipoolermanagerdata.RestoreFromBackupRequest{
						BackupId: fullBackupID,
					}

					restoreCtx := utils.WithTimeout(t, 1*time.Minute)

					_, err = standbyBackupClient.RestoreFromBackup(restoreCtx, restoreReq)
					require.NoError(t, err, "Restore to standby should succeed")

					// Wait for PostgreSQL to be ready after restore and verify term
					var restoredTerm int64
					require.Eventually(t, func() bool {
						statusCtx = utils.WithShortDeadline(t)
						statusResp, err = standbyBackupClient.Status(statusCtx, &multipoolermanagerdata.StatusRequest{})
						if err != nil {
							return false
						}
						if statusResp.Status.ConsensusTerm != nil {
							restoredTerm = statusResp.Status.ConsensusTerm.TermNumber
						}
						return statusResp.Status.PostgresReady
					}, 10*time.Second, 100*time.Millisecond, "PostgreSQL should be running after restore")
					t.Logf("Term after restore: %d (expected: 0)", restoredTerm)
					assert.Equal(t, int64(0), restoredTerm, "Term should be reset to 0 after restore (stale term file is deleted)")

					// Verify primary_term is 0 after restore
					statusCtx = utils.WithShortDeadline(t)
					statusResp, err = standbyBackupClient.Status(statusCtx, &multipoolermanagerdata.StatusRequest{})
					require.NoError(t, err, "Should be able to get status after restore")
					require.NotNil(t, statusResp.Status.ConsensusTerm, "ConsensusTerm should not be nil after restore")
					assert.Equal(t, int64(0), statusResp.Status.ConsensusTerm.PrimaryTerm,
						"primary_term should be 0 after restore")

					// Configure replication after restore
					setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
						Primary:               primary,
						StartReplicationAfter: true,
						StopReplicationBefore: false,
						CurrentTerm:           restoredTerm, // Term is 0 after restore; Force allows multiorch to advance it
						Force:                 true,         // Force reconfiguration after restore
					}
					setPrimaryCtx := utils.WithTimeout(t, 30*time.Second)
					_, err = standbyBackupClient.SetPrimaryConnInfo(setPrimaryCtx, setPrimaryReq)
					require.NoError(t, err, "Should be able to configure replication after restore")

					// Connect to the standby database after restore
					standbyDB := connectToPostgresViaSocket(t,
						getPostgresSocketPath(setup.StandbyPgctld.PoolerDir),
						setup.StandbyPgctld.PgPort)
					defer standbyDB.Close()

					t.Log("Verifying standby database is accessible after restore...")

					// Verify standby database is accessible and we can query data
					var countAfterRestore int
					err = standbyDB.QueryRow("SELECT COUNT(*) FROM backup_restore_test").Scan(&countAfterRestore)
					require.NoError(t, err)
					t.Logf("Row count on standby after restore: %d", countAfterRestore)
					t.Logf("✓ Found %d rows in restored standby database", countAfterRestore)

					// Insert a new row on primary after restore to test replication
					testData := "row_after_restore"
					_, err = db.Exec("INSERT INTO backup_restore_test (data) VALUES ($1)", testData)
					require.NoError(t, err, "Should be able to insert data on primary after restore")

					// Wait for replication to standby
					require.Eventually(t, func() bool {
						var newRowExists bool
						err := standbyDB.QueryRow("SELECT EXISTS(SELECT 1 FROM backup_restore_test WHERE data = $1)", testData).Scan(&newRowExists)
						return err == nil && newRowExists
					}, 10*time.Second, 100*time.Millisecond, "New row should replicate to standby after restore")

					// Check if standby.signal exists
					standbySignalPath := filepath.Join(setup.StandbyPgctld.PoolerDir, "pg_data", "standby.signal")
					_, statErr := os.Stat(standbySignalPath)
					assert.Nil(t, statErr, "standby signal should exist")

					// Verify that the standby is still acting as a replica (in recovery mode)
					var isInRecovery bool
					err = standbyDB.QueryRow("SELECT pg_is_in_recovery()").Scan(&isInRecovery)
					require.NoError(t, err, "Should be able to query recovery status")
					assert.True(t, isInRecovery, "Standby should still be in recovery mode after restore")
				})

				t.Run("GetBackups_WithoutLimit", func(t *testing.T) {
					t.Log("Listing backups without limit...")

					listReq := &multipoolermanagerdata.GetBackupsRequest{
						Limit: 0, // No limit
					}

					listCtx := utils.WithShortDeadline(t)
					listResp, err := backupClient.GetBackups(listCtx, listReq)
					require.NoError(t, err, "Listing backups without limit should succeed")
					require.NotNil(t, listResp, "List response should not be nil")

					// Should have at least our backup
					assert.NotEmpty(t, listResp.Backups, "Should have at least one backup")

					t.Logf("Listed %d backup(s) without limit", len(listResp.Backups))
				})

				t.Run("GetBackups_WithSmallLimit", func(t *testing.T) {
					t.Log("Listing backups with limit=1...")

					listReq := &multipoolermanagerdata.GetBackupsRequest{
						Limit: 1,
					}

					listCtx := utils.WithShortDeadline(t)
					listResp, err := backupClient.GetBackups(listCtx, listReq)
					require.NoError(t, err, "Listing backups with limit should succeed")
					require.NotNil(t, listResp, "List response should not be nil")

					// Should return at most 1 backup
					assert.LessOrEqual(t, len(listResp.Backups), 1,
						"Should return at most 1 backup when limit=1")

					t.Logf("Listed %d backup(s) with limit=1", len(listResp.Backups))
				})
			})

			t.Run("CreateDifferentialBackup", func(t *testing.T) {
				t.Log("Creating differential backup...")
				createAndVerifyBackup(t, backupClient, "differential", true, 5*time.Minute, nil)

				// Verify differential backup appears in list
				listReq := &multipoolermanagerdata.GetBackupsRequest{
					Limit: 10,
				}

				listCtx := utils.WithShortDeadline(t)
				listResp, err := backupClient.GetBackups(listCtx, listReq)
				require.NoError(t, err, "Listing backups should succeed")

				// Should now have at least 2 backups (full + differential)
				assert.GreaterOrEqual(t, len(listResp.Backups), 2,
					"Should have at least 2 backups (full + differential)")

				t.Logf("Verified %d total backups exist", len(listResp.Backups))
			})

			t.Run("CreateIncrementalBackup", func(t *testing.T) {
				t.Log("Creating incremental backup...")
				createAndVerifyBackup(t, backupClient, "incremental", true, 5*time.Minute, nil)

				// Verify incremental backup appears in list
				listReq := &multipoolermanagerdata.GetBackupsRequest{
					Limit: 10,
				}

				listCtx := utils.WithShortDeadline(t)
				listResp, err := backupClient.GetBackups(listCtx, listReq)
				require.NoError(t, err, "Listing backups should succeed")

				// Should now have at least 3 backups (full + differential + incremental)
				assert.GreaterOrEqual(t, len(listResp.Backups), 3,
					"Should have at least 3 backups (full + differential + incremental)")

				t.Logf("Verified %d total backups exist", len(listResp.Backups))
			})
		})
	}
}

func TestBackup_ValidationErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Wait for manager to be ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	// Create backup client connection
	backupClient := createBackupClient(t, setup.PrimaryMultipooler.GrpcPort)

	t.Run("MissingType", func(t *testing.T) {
		req := &multipoolermanagerdata.BackupRequest{
			ForcePrimary: true, // Set to true to test type validation
			Type:         "",   // Missing
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.Backup(ctx, req)

		assert.Error(t, err, "Should return error for missing type")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "type", "Error should mention type")
	})

	t.Run("InvalidType", func(t *testing.T) {
		req := &multipoolermanagerdata.BackupRequest{
			ForcePrimary: true,      // Set to true to test type validation
			Type:         "invalid", // Invalid type
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.Backup(ctx, req)

		assert.Error(t, err, "Should return error for invalid type")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "invalid", "Error should mention invalid type")
	})

	t.Run("BackupFromPrimaryWithoutForcePrimary", func(t *testing.T) {
		req := &multipoolermanagerdata.BackupRequest{
			ForcePrimary: false, // Not forced
			Type:         "full",
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.Backup(ctx, req)

		assert.Error(t, err, "Should return error for backup from primary without ForcePrimary")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "primary", "Error should mention primary database")
	})
}

func TestBackup_FromStandby(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	backends := availableBackends
	for _, backend := range backends {
		t.Run(backend, func(t *testing.T) {
			setup := getSetupForBackend(t, backend)
			setupPoolerTest(t, setup)

			// Wait for standby manager to be ready
			waitForManagerReady(t, setup, setup.StandbyMultipooler)

			// Create backup client connection to standby
			backupClient := createBackupClient(t, setup.StandbyMultipooler.GrpcPort)

			t.Run("CreateFullBackupFromStandby", func(t *testing.T) {
				t.Log("Creating full backup from standby...")
				backupID := createAndVerifyBackup(t, backupClient, "full", false, 5*time.Minute, nil)
				foundBackup := listAndFindBackup(t, backupClient, backupID, 10)

				t.Logf("Standby backup verified in list: ID=%s, Status=%s, FinalLSN=%s",
					foundBackup.BackupId, foundBackup.Status, foundBackup.FinalLsn)
			})

			t.Run("CreateIncrementalBackupFromStandby", func(t *testing.T) {
				t.Log("Creating incremental backup from standby...")
				createAndVerifyBackup(t, backupClient, "incremental", false, 5*time.Minute, nil)
			})
		})
	}
}

// TestBackup_MultiAdminAPIs tests the MultiAdmin backup/restore orchestration layer.
// This also tests the multipooler backup and restore functionality since MultiAdmin
// delegates to multipooler for actual backup operations.
func TestBackup_MultiAdminAPIs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	backends := availableBackends
	for _, backend := range backends {
		t.Run(backend, func(t *testing.T) {
			setup := getSetupForBackend(t, backend)
			setupPoolerTest(t, setup)

			// Wait for managers to be ready
			waitForManagerReady(t, setup, setup.PrimaryMultipooler)
			waitForManagerReady(t, setup, setup.StandbyMultipooler)

			// Create a MultiAdminServer for testing
			logger := slog.Default()
			adminServer := adminserver.NewMultiAdminServer(setup.TopoServer, logger)
			defer adminServer.Stop()

			t.Run("Backup_CreateAndGetStatus", func(t *testing.T) {
				t.Log("Step 1: Creating backup via MultiAdmin API...")

				// Create a backup request
				backupReq := &multiadminpb.BackupRequest{
					Database:   "postgres",
					TableGroup: "default",
					Shard:      "0-inf",
					Type:       "full",
				}

				backupResp, err := adminServer.Backup(t.Context(), backupReq)
				require.NoError(t, err, "Backup request should succeed")
				require.NotEmpty(t, backupResp.JobId, "Job ID should be returned")
				t.Logf("Backup job started with ID: %s", backupResp.JobId)

				t.Log("Step 2: Waiting for backup job to complete...")
				status := waitForJobCompletion(t, adminServer, backupResp.JobId, 5*time.Minute)
				require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, status.Status, "Job should be completed")
				require.NotEmpty(t, status.BackupId, "Backup ID should be set on completion")
				t.Logf("Backup completed with backup_id: %s", status.BackupId)

				t.Run("GetBackups_VerifyBackup", func(t *testing.T) {
					t.Log("Step 3: Listing backups via MultiAdmin API...")

					getBackupsReq := &multiadminpb.GetBackupsRequest{
						Database:   "postgres",
						TableGroup: "default",
						Shard:      "0-inf",
						Limit:      10,
					}

					getBackupsResp, err := adminServer.GetBackups(t.Context(), getBackupsReq)
					require.NoError(t, err, "GetBackups should succeed")
					require.NotEmpty(t, getBackupsResp.Backups, "Should have at least one backup")

					// Find our backup
					var foundBackup *multiadminpb.BackupInfo
					for _, backup := range getBackupsResp.Backups {
						if backup.BackupId == status.BackupId {
							foundBackup = backup
							break
						}
					}

					require.NotNil(t, foundBackup, "Our backup should be in the list")
					assert.Equal(t, "postgres", foundBackup.Database)
					assert.Equal(t, "default", foundBackup.TableGroup)
					assert.Equal(t, "0-inf", foundBackup.Shard)
					assert.Equal(t, multiadminpb.BackupStatus_BACKUP_STATUS_COMPLETE, foundBackup.Status)
					t.Logf("Backup verified in list: %s", foundBackup.BackupId)
				})
			})

			t.Run("RestoreFromBackup", func(t *testing.T) {
				t.Log("Step 1: Creating a fresh backup for restore test...")

				// Create a backup first
				backupReq := &multiadminpb.BackupRequest{
					Database:   "postgres",
					TableGroup: "default",
					Shard:      "0-inf",
					Type:       "full",
				}

				backupResp, err := adminServer.Backup(t.Context(), backupReq)
				require.NoError(t, err, "Backup request should succeed")

				backupStatus := waitForJobCompletion(t, adminServer, backupResp.JobId, 5*time.Minute)
				require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, backupStatus.Status)
				backupID := backupStatus.BackupId
				t.Logf("Backup completed with ID: %s", backupID)

				t.Log("Step 2: Stopping standby PostgreSQL...")

				// Connect to standby's pgctld to stop PostgreSQL
				standbyPgctldConn, err := grpc.NewClient(
					fmt.Sprintf("localhost:%d", setup.StandbyPgctld.GrpcPort),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				require.NoError(t, err)
				defer standbyPgctldConn.Close()
				standbyPgctldClient := pgctldservice.NewPgCtldClient(standbyPgctldConn)

				stopCtx, stopCancel := context.WithTimeout(t.Context(), 2*time.Minute)
				defer stopCancel()
				_, err = standbyPgctldClient.Stop(stopCtx, &pgctldservice.StopRequest{Mode: "fast"})
				require.NoError(t, err, "Should be able to stop PostgreSQL on standby")
				t.Log("PostgreSQL stopped on standby")

				t.Log("Step 3: Removing standby pg_data directory...")
				removeDataDirectory(t, setup.StandbyPgctld.PoolerDir)

				t.Log("Step 4: Restoring backup to standby via MultiAdmin API...")

				// Create restore request targeting the standby pooler
				standbys := setup.GetStandbys()
				require.NotEmpty(t, standbys, "Should have at least one standby")
				restoreReq := &multiadminpb.RestoreFromBackupRequest{
					Database:   "postgres",
					TableGroup: "default",
					Shard:      "0-inf",
					BackupId:   backupID,
					PoolerId:   setup.GetMultipoolerID(standbys[0].Name),
				}

				restoreResp, err := adminServer.RestoreFromBackup(t.Context(), restoreReq)
				require.NoError(t, err, "RestoreFromBackup should succeed")
				require.NotEmpty(t, restoreResp.JobId, "Restore job ID should be returned")
				t.Logf("Restore job started with ID: %s", restoreResp.JobId)

				t.Log("Step 5: Waiting for restore job to complete...")
				restoreStatus := waitForJobCompletion(t, adminServer, restoreResp.JobId, 10*time.Minute)
				require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, restoreStatus.Status, "Restore should complete")
				t.Log("Restore completed successfully")

				// Configure replication after restore
				standbyClient := createBackupClient(t, setup.StandbyMultipooler.GrpcPort)

				// Wait for PostgreSQL to be ready after restore
				require.Eventually(t, func() bool {
					statusCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					statusResp, err := standbyClient.Status(statusCtx, &multipoolermanagerdata.StatusRequest{})
					return err == nil && statusResp.Status.PostgresReady
				}, 10*time.Second, 100*time.Millisecond, "PostgreSQL should be running after restore")

				primary := &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "test-cell",
						Name:      setup.PrimaryMultipooler.Name,
					},
					Hostname: "localhost",
					PortMap:  map[string]int32{"postgres": int32(setup.PrimaryPgctld.PgPort)},
				}
				setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
					Primary:               primary,
					StartReplicationAfter: true,
					StopReplicationBefore: false,
					CurrentTerm:           1,
					Force:                 true,
				}
				setPrimaryCtx, setPrimaryCancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer setPrimaryCancel()
				_, err = standbyClient.SetPrimaryConnInfo(setPrimaryCtx, setPrimaryReq)
				require.NoError(t, err, "Should be able to configure replication after restore")
				t.Log("Replication configured after restore")

				t.Log("Step 7: Verifying standby is accessible after restore...")

				// Connect to standby and verify it's in recovery mode
				standbyDB := connectToPostgresViaSocket(t,
					getPostgresSocketPath(setup.StandbyPgctld.PoolerDir),
					setup.StandbyPgctld.PgPort)
				defer standbyDB.Close()

				var isInRecovery bool
				err = standbyDB.QueryRow("SELECT pg_is_in_recovery()").Scan(&isInRecovery)
				require.NoError(t, err, "Should be able to query standby")
				assert.True(t, isInRecovery, "Standby should be in recovery mode after restore")

				t.Logf("✓ MultiAdmin backup/restore completed successfully")
				t.Logf("✓ Standby is in recovery mode: %t", isInRecovery)
			})

			t.Run("GetBackupJobStatus_SurvivesMultiAdminRestart", func(t *testing.T) {
				// This test verifies that GetBackupJobStatus works even after MultiAdmin restarts
				// by falling back to querying the MultiPooler for backup status via GetBackupByJobId.
				//
				// We simulate a restart by creating a fresh MultiAdmin server that has no in-memory
				// job state. The new server should still be able to retrieve job status by querying
				// the MultiPooler, which has the backup metadata stored in pgbackrest.

				t.Log("Step 1: Creating backup via MultiAdmin API...")

				// Create a backup request
				backupReq := &multiadminpb.BackupRequest{
					Database:   "postgres",
					TableGroup: "default",
					Shard:      "0-inf",
					Type:       "full",
				}

				backupResp, err := adminServer.Backup(t.Context(), backupReq)
				require.NoError(t, err, "Backup request should succeed")
				require.NotEmpty(t, backupResp.JobId, "Job ID should be returned")
				originalJobID := backupResp.JobId
				t.Logf("Backup job started with ID: %s", originalJobID)

				t.Log("Step 2: Waiting for backup job to complete...")
				status := waitForJobCompletion(t, adminServer, originalJobID, 5*time.Minute)
				require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, status.Status, "Job should be completed")
				completedBackupID := status.BackupId
				t.Logf("Backup completed with backup_id: %s", completedBackupID)

				t.Log("Step 3: Verifying job status is available from in-memory tracker...")
				statusFromTracker, err := adminServer.GetBackupJobStatus(t.Context(), &multiadminpb.GetBackupJobStatusRequest{
					JobId: originalJobID,
				})
				require.NoError(t, err, "GetBackupJobStatus should succeed from tracker")
				require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, statusFromTracker.Status)
				t.Logf("Job status from tracker: %s", statusFromTracker.Status)

				t.Log("Step 4: Creating fresh MultiAdmin server (simulating restart with no in-memory state)...")
				// Note: We don't stop the original adminServer since the test infrastructure manages it.
				// Instead, we create a new server to demonstrate the fallback works without in-memory state.
				freshAdminServer := adminserver.NewMultiAdminServer(setup.TopoServer, logger)
				defer freshAdminServer.Stop()

				t.Log("Step 5: Verifying job status is NOT available without shard context...")
				_, err = freshAdminServer.GetBackupJobStatus(t.Context(), &multiadminpb.GetBackupJobStatusRequest{
					JobId: originalJobID,
					// No shard context - should return NotFound since fresh server has no tracker state
				})
				require.Error(t, err, "GetBackupJobStatus without shard context should fail on fresh server")
				t.Logf("Expected error without shard context: %v", err)

				t.Log("Step 6: Verifying job status IS available WITH shard context (fallback to pooler)...")
				statusFromPooler, err := freshAdminServer.GetBackupJobStatus(t.Context(), &multiadminpb.GetBackupJobStatusRequest{
					JobId:      originalJobID,
					Database:   "postgres",
					TableGroup: "default",
					Shard:      "0-inf",
				})
				require.NoError(t, err, "GetBackupJobStatus with shard context should succeed via pooler fallback")
				require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, statusFromPooler.Status,
					"Job status should be COMPLETED")
				require.Equal(t, completedBackupID, statusFromPooler.BackupId,
					"Backup ID should match the original")
				require.Equal(t, multiadminpb.JobType_JOB_TYPE_BACKUP, statusFromPooler.JobType,
					"Job type should be BACKUP")

				t.Logf("✓ Job status retrieved from pooler fallback after MultiAdmin restart")
				t.Logf("✓ Job ID: %s", statusFromPooler.JobId)
				t.Logf("✓ Backup ID: %s", statusFromPooler.BackupId)
				t.Logf("✓ Status: %s", statusFromPooler.Status)
			})
		})
	}
}

// overrideRetentionInConfig rewrites the pgbackrest.conf to use a custom retention-full value.
// It registers a cleanup function to restore the original config when the test completes.
func overrideRetentionInConfig(t *testing.T, poolerDir string, retentionFull string) {
	t.Helper()
	configPath := filepath.Join(poolerDir, "pgbackrest", "pgbackrest.conf")
	original, err := os.ReadFile(configPath)
	require.NoError(t, err, "Should be able to read pgbackrest.conf at %s", configPath)

	updated := strings.Replace(string(original), "repo1-retention-full=7", "repo1-retention-full="+retentionFull, 1)
	require.NotEqual(t, string(original), updated, "Should have replaced repo1-retention-full=7 in config")
	err = os.WriteFile(configPath, []byte(updated), 0o644)
	require.NoError(t, err, "Should be able to write updated pgbackrest.conf")
	t.Logf("Updated retention-full to %s in %s", retentionFull, configPath)

	t.Cleanup(func() {
		_ = os.WriteFile(configPath, original, 0o644)
	})
}

func TestExpireAuto(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	backends := availableBackends
	for _, backend := range backends {
		t.Run(backend, func(t *testing.T) {
			setup := getSetupForBackend(t, backend)
			setupPoolerTest(t, setup)

			waitForManagerReady(t, setup, setup.PrimaryMultipooler)
			waitForManagerReady(t, setup, setup.StandbyMultipooler)

			// Override retention in pgbackrest.conf to keep only 1 full backup
			overrideRetentionInConfig(t, setup.PrimaryMultipooler.PoolerDir, "1")
			overrideRetentionInConfig(t, setup.StandbyMultipooler.PoolerDir, "1")

			logger := slog.Default()
			adminServer := adminserver.NewMultiAdminServer(setup.TopoServer, logger)
			defer adminServer.Stop()

			backupReq := &multiadminpb.BackupRequest{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "0-inf",
				Type:       "full",
			}
			getBackupsReq := &multiadminpb.GetBackupsRequest{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "0-inf",
				Limit:      100,
			}

			t.Log("Step 1: Creating first full backup...")
			backupResp1, err := adminServer.Backup(t.Context(), backupReq)
			require.NoError(t, err)
			status1 := waitForJobCompletion(t, adminServer, backupResp1.JobId, 5*time.Minute)
			require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, status1.Status)
			backupID1 := status1.BackupId
			t.Logf("First backup completed: %s", backupID1)

			t.Log("Step 2: Verifying first backup exists...")
			getResp, err := adminServer.GetBackups(t.Context(), getBackupsReq)
			require.NoError(t, err)
			require.Len(t, getResp.Backups, 1, "Should have exactly one backup")

			t.Log("Step 3: Creating second full backup (should auto-expire first)...")
			backupResp2, err := adminServer.Backup(t.Context(), backupReq)
			require.NoError(t, err)
			status2 := waitForJobCompletion(t, adminServer, backupResp2.JobId, 5*time.Minute)
			require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, status2.Status)
			backupID2 := status2.BackupId
			t.Logf("Second backup completed: %s", backupID2)

			t.Log("Step 4: Verifying expire-auto removed first backup...")
			getResp, err = adminServer.GetBackups(t.Context(), getBackupsReq)
			require.NoError(t, err)
			var foundIDs []string
			for _, b := range getResp.Backups {
				foundIDs = append(foundIDs, b.BackupId)
			}
			assert.NotContains(t, foundIDs, backupID1, "First backup should have been auto-expired")
			assert.Contains(t, foundIDs, backupID2, "Second backup should still exist")
			t.Logf("After auto-expiration, remaining backups: %v", foundIDs)
		})
	}
}

func TestExpireBackups(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	backends := availableBackends
	for _, backend := range backends {
		t.Run(backend, func(t *testing.T) {
			setup := getSetupForBackend(t, backend)
			setupPoolerTest(t, setup)

			// Wait for managers to be ready
			waitForManagerReady(t, setup, setup.PrimaryMultipooler)
			waitForManagerReady(t, setup, setup.StandbyMultipooler)

			// Create a MultiAdminServer for testing
			logger := slog.Default()
			adminServer := adminserver.NewMultiAdminServer(setup.TopoServer, logger)
			defer adminServer.Stop()

			backupReq := &multiadminpb.BackupRequest{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "0-inf",
				Type:       "full",
			}
			getBackupsReq := &multiadminpb.GetBackupsRequest{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "0-inf",
				Limit:      100,
			}

			t.Log("Step 1: Creating first full backup...")
			backupResp1, err := adminServer.Backup(t.Context(), backupReq)
			require.NoError(t, err, "First backup request should succeed")
			status1 := waitForJobCompletion(t, adminServer, backupResp1.JobId, 5*time.Minute)
			require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, status1.Status)
			backupID1 := status1.BackupId
			t.Logf("First backup completed: %s", backupID1)

			t.Log("Step 2: Creating second full backup...")
			backupResp2, err := adminServer.Backup(t.Context(), backupReq)
			require.NoError(t, err, "Second backup request should succeed")
			status2 := waitForJobCompletion(t, adminServer, backupResp2.JobId, 5*time.Minute)
			require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, status2.Status)
			backupID2 := status2.BackupId
			t.Logf("Second backup completed: %s", backupID2)

			t.Log("Step 3: Verifying both backups exist...")
			getResp, err := adminServer.GetBackups(t.Context(), getBackupsReq)
			require.NoError(t, err)
			var foundIDs []string
			for _, b := range getResp.Backups {
				foundIDs = append(foundIDs, b.BackupId)
			}
			assert.Contains(t, foundIDs, backupID1, "First backup should exist before expire")
			assert.Contains(t, foundIDs, backupID2, "Second backup should exist before expire")
			t.Logf("Both backups present: %v", foundIDs)

			t.Log("Step 4: Expiring with count-based retention of 1...")
			expireResp, err := adminServer.ExpireBackups(t.Context(), &multiadminpb.ExpireBackupsRequest{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "0-inf",
				Overrides: map[string]string{
					"repo1_retention_full":      "1",
					"repo1_retention_full_type": "count",
				},
			})
			require.NoError(t, err, "ExpireBackups should succeed")
			t.Logf("Expired backup IDs: %v", expireResp.ExpiredBackupIds)

			t.Log("Step 5: Verifying first backup was expired...")
			getResp, err = adminServer.GetBackups(t.Context(), getBackupsReq)
			require.NoError(t, err)
			foundIDs = nil
			for _, b := range getResp.Backups {
				foundIDs = append(foundIDs, b.BackupId)
			}
			assert.NotContains(t, foundIDs, backupID1, "First backup should have been expired")
			assert.Contains(t, foundIDs, backupID2, "Second backup should still exist")
			assert.Contains(t, expireResp.ExpiredBackupIds, backupID1, "Expired IDs should contain first backup")
			assert.NotContains(t, expireResp.ExpiredBackupIds, backupID2, "Expired IDs should not contain second backup")
			t.Logf("After expiration, remaining backups: %v", foundIDs)
		})
	}
}

func TestExpireBackups_Differential(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	// Tests that after expiring with retention_full=1, only the latest
	// full backup and its differential survive. Older fulls and any
	// differentials (whether auto-expired by pgbackrest or explicitly
	// expired) are gone.

	backends := availableBackends
	for _, backend := range backends {
		t.Run(backend, func(t *testing.T) {
			setup := getSetupForBackend(t, backend)
			setupPoolerTest(t, setup)

			waitForManagerReady(t, setup, setup.PrimaryMultipooler)
			waitForManagerReady(t, setup, setup.StandbyMultipooler)

			logger := slog.Default()
			adminServer := adminserver.NewMultiAdminServer(setup.TopoServer, logger)
			defer adminServer.Stop()

			backupReq := func(backupType string) *multiadminpb.BackupRequest {
				return &multiadminpb.BackupRequest{
					Database:   "postgres",
					TableGroup: "default",
					Shard:      "0-inf",
					Type:       backupType,
				}
			}
			getBackupsReq := &multiadminpb.GetBackupsRequest{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "0-inf",
				Limit:      100,
			}

			t.Log("Step 1: Creating full1...")
			full1Resp, err := adminServer.Backup(t.Context(), backupReq("full"))
			require.NoError(t, err)
			full1Status := waitForJobCompletion(t, adminServer, full1Resp.JobId, 5*time.Minute)
			require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, full1Status.Status)
			t.Logf("full1: %s", full1Status.BackupId)

			t.Log("Step 2: Creating diff1 on full1...")
			diff1Resp, err := adminServer.Backup(t.Context(), backupReq("differential"))
			require.NoError(t, err)
			diff1Status := waitForJobCompletion(t, adminServer, diff1Resp.JobId, 5*time.Minute)
			require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, diff1Status.Status)
			t.Logf("diff1: %s", diff1Status.BackupId)

			t.Log("Step 3: Creating full2...")
			full2Resp, err := adminServer.Backup(t.Context(), backupReq("full"))
			require.NoError(t, err)
			full2Status := waitForJobCompletion(t, adminServer, full2Resp.JobId, 5*time.Minute)
			require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, full2Status.Status)
			full2ID := full2Status.BackupId
			t.Logf("full2: %s", full2ID)

			t.Log("Step 4: Creating diff2 on full2...")
			diff2Resp, err := adminServer.Backup(t.Context(), backupReq("differential"))
			require.NoError(t, err)
			diff2Status := waitForJobCompletion(t, adminServer, diff2Resp.JobId, 5*time.Minute)
			require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, diff2Status.Status)
			diff2ID := diff2Status.BackupId
			t.Logf("diff2: %s", diff2ID)

			t.Log("Step 5: Listing backups before explicit expire...")
			getResp, err := adminServer.GetBackups(t.Context(), getBackupsReq)
			require.NoError(t, err)
			var beforeIDs []string
			for _, b := range getResp.Backups {
				beforeIDs = append(beforeIDs, b.BackupId)
			}
			require.Contains(t, beforeIDs, full2ID, "full2 should exist")
			require.Contains(t, beforeIDs, diff2ID, "diff2 should exist")
			t.Logf("Backups before expire: %v", beforeIDs)

			t.Log("Step 6: Expiring with retention_full=1...")
			expireResp, err := adminServer.ExpireBackups(t.Context(), &multiadminpb.ExpireBackupsRequest{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "0-inf",
				Overrides: map[string]string{
					"repo1_retention_full":      "1",
					"repo1_retention_full_type": "count",
				},
			})
			require.NoError(t, err, "ExpireBackups should succeed")
			t.Logf("Expired backup IDs: %v", expireResp.ExpiredBackupIds)

			t.Log("Step 7: Verifying only full2 + diff2 remain...")
			getResp, err = adminServer.GetBackups(t.Context(), getBackupsReq)
			require.NoError(t, err)
			var afterIDs []string
			for _, b := range getResp.Backups {
				afterIDs = append(afterIDs, b.BackupId)
			}
			assert.Equal(t, []string{full2ID, diff2ID}, afterIDs,
				"Only full2 and diff2 should remain after expire")
			assert.NotContains(t, expireResp.ExpiredBackupIds, full2ID, "full2 should not be expired")
			assert.NotContains(t, expireResp.ExpiredBackupIds, diff2ID, "diff2 should not be expired")
			t.Logf("After expiration, remaining: %v, expired: %v", afterIDs, expireResp.ExpiredBackupIds)
		})
	}
}
