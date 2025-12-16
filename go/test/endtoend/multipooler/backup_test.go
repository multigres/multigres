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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/pb/pgctldservice"
)

// connectToPostgres establishes a connection to the PostgreSQL database using Unix socket
func connectToPostgres(t *testing.T, socketDir string, port int) *sql.DB {
	t.Helper()

	// Use Unix socket connection which uses trust authentication
	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable", socketDir, port)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "Failed to open database connection")

	// Test the connection
	err = db.Ping()
	require.NoError(t, err, "Failed to ping database")

	return db
}

// removeDataDirectory removes the pg_data directory for a given pgctld instance
func removeDataDirectory(t *testing.T, dataDir string) {
	t.Helper()
	pgDataDir := filepath.Join(dataDir, "pg_data")
	err := os.RemoveAll(pgDataDir)
	require.NoError(t, err, "Should be able to remove pg_data directory")
	t.Logf("Removed pg_data directory: %s", pgDataDir)
}

func TestBackup_CreateListAndRestore(t *testing.T) {
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end tests (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Wait for both primary and standby managers to be ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create backup client connections
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	backupClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyBackupClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	// Connect to primary PostgreSQL database using Unix socket
	primarySocketDir := filepath.Join(setup.PrimaryPgctld.DataDir, "pg_sockets")
	db := connectToPostgres(t, primarySocketDir, setup.PrimaryPgctld.PgPort)
	defer db.Close()

	t.Log("Step 1: Creating test table and inserting initial data...")

	// Create a test table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS backup_restore_test (
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
		t.Log("Step 2: Creating full backup...")

		req := &multipoolermanagerdata.BackupRequest{
			ForcePrimary: true, // Required for backups from primary
			Type:         "full",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.Backup(ctx, req)
		require.NoError(t, err, "Full backup should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify backup ID format
		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Backup ID should match pgbackrest format: YYYYMMDD-HHMMSSF
		// F indicates full backup, D indicates differential, I indicates incremental
		backupIDPattern := regexp.MustCompile(`^\d{8}-\d{6}F$`)
		assert.True(t, backupIDPattern.MatchString(resp.BackupId),
			"Backup ID should match format YYYYMMDD-HHMMSSF, got: %s", resp.BackupId)

		fullBackupID := resp.BackupId
		t.Logf("Full backup created successfully with ID: %s", fullBackupID)

		t.Run("GetBackups_VerifyFullBackup", func(t *testing.T) {
			t.Log("Step 3: Listing backups to verify full backup...")

			listReq := &multipoolermanagerdata.GetBackupsRequest{
				Limit: 10,
			}

			listCtx := utils.WithShortDeadline(t)
			listResp, err := backupClient.GetBackups(listCtx, listReq)
			require.NoError(t, err, "Listing backups should succeed")
			require.NotNil(t, listResp, "List response should not be nil")

			// Verify at least one backup exists
			assert.NotEmpty(t, listResp.Backups, "Should have at least one backup")

			// Find our backup in the list
			var foundBackup *multipoolermanagerdata.BackupMetadata
			for _, backup := range listResp.Backups {
				if backup.BackupId == fullBackupID {
					foundBackup = backup
					break
				}
			}

			require.NotNil(t, foundBackup, "Our backup should be in the list")

			// Verify backup metadata
			assert.Equal(t, fullBackupID, foundBackup.BackupId, "Backup ID should match")
			assert.Equal(t, multipoolermanagerdata.BackupMetadata_COMPLETE, foundBackup.Status,
				"Backup status should be COMPLETE")
			assert.NotEmpty(t, foundBackup.FinalLsn, "Backup should have final LSN")

			t.Logf("Backup verified in list: ID=%s, Status=%s, FinalLSN=%s",
				foundBackup.BackupId, foundBackup.Status, foundBackup.FinalLsn)
		})

		t.Run("RestoreAndVerify", func(t *testing.T) {
			t.Log("Step 4: Inserting additional rows after backup...")

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
			t.Logf("Row count after additional inserts: %d", countAfterInsert)

			t.Log("Step 5: Verifying backup exists in standby's list...")

			listReq := &multipoolermanagerdata.GetBackupsRequest{
				Limit: 20,
			}

			listCtx := utils.WithShortDeadline(t)
			listResp, err := standbyBackupClient.GetBackups(listCtx, listReq)
			require.NoError(t, err, "Listing backups should succeed")
			require.NotNil(t, listResp, "List response should not be nil")

			// Find our backup in the standby's list
			var foundBackup *multipoolermanagerdata.BackupMetadata
			for _, backup := range listResp.Backups {
				if backup.BackupId == fullBackupID {
					foundBackup = backup
					break
				}
			}

			require.NotNil(t, foundBackup, "Backup should be in standby's list")
			assert.Equal(t, multipoolermanagerdata.BackupMetadata_COMPLETE, foundBackup.Status,
				"Backup status should be COMPLETE")
			assert.NotEmpty(t, foundBackup.FinalLsn, "Backup should have final LSN")
			t.Logf("Backup verified in standby's list: ID=%s, Status=%s, FinalLSN=%s",
				foundBackup.BackupId, foundBackup.Status, foundBackup.FinalLsn)

			t.Log("Step 6: Preparing standby for restore (stopping PostgreSQL and removing PGDATA)...")

			// Connect to standby's pgctld to stop PostgreSQL
			standbyPgctldConn, err := grpc.NewClient(
				fmt.Sprintf("localhost:%d", setup.StandbyPgctld.GrpcPort),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			require.NoError(t, err)
			defer standbyPgctldConn.Close()
			standbyPgctldClient := pgctldservice.NewPgCtldClient(standbyPgctldConn)

			// Stop PostgreSQL on standby
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stopCancel()
			_, err = standbyPgctldClient.Stop(stopCtx, &pgctldservice.StopRequest{Mode: "fast"})
			require.NoError(t, err, "Should be able to stop PostgreSQL on standby")
			t.Log("PostgreSQL stopped on standby")

			// Remove pg_data directory
			removeDataDirectory(t, setup.StandbyPgctld.DataDir)

			t.Log("Step 7: Restoring from backup to standby...")

			restoreReq := &multipoolermanagerdata.RestoreFromBackupRequest{
				BackupId:  fullBackupID,
				AsStandby: true, // Must match current standby state
			}

			restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer restoreCancel()

			_, err = standbyBackupClient.RestoreFromBackup(restoreCtx, restoreReq)
			require.NoError(t, err, "Restore to standby should succeed")
			t.Log("Restore to standby completed successfully")

			// Wait a bit for PostgreSQL to be ready after restore
			time.Sleep(5 * time.Second)

			// Configure replication after restore
			t.Log("Configuring replication after restore...")
			setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
				Host:                  "localhost",
				Port:                  int32(setup.PrimaryPgctld.PgPort),
				StartReplicationAfter: true,
				StopReplicationBefore: false,
				CurrentTerm:           1,
				Force:                 true, // Force reconfiguration after restore
			}
			setPrimaryCtx, setPrimaryCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer setPrimaryCancel()
			_, err = standbyBackupClient.SetPrimaryConnInfo(setPrimaryCtx, setPrimaryReq)
			require.NoError(t, err, "Should be able to configure replication after restore")
			t.Log("Replication configured successfully after restore")

			// Connect to the standby database after restore
			standbySocketDir := filepath.Join(setup.StandbyPgctld.DataDir, "pg_sockets")
			standbyDB := connectToPostgres(t, standbySocketDir, setup.StandbyPgctld.PgPort)
			defer standbyDB.Close()

			t.Log("Step 8: Verifying standby database is accessible after restore...")

			// Verify standby database is accessible and we can query data
			var countAfterRestore int
			err = standbyDB.QueryRow("SELECT COUNT(*) FROM backup_restore_test").Scan(&countAfterRestore)
			require.NoError(t, err)
			t.Logf("Row count on standby after restore: %d", countAfterRestore)

			t.Logf("✓ Restore completed and standby database is accessible")
			t.Logf("✓ Found %d rows in restored standby database", countAfterRestore)

			t.Log("Step 9: Verifying replication from primary to standby still works...")

			// Insert a new row on primary after restore to test replication
			testData := "row_after_restore"
			_, err = db.Exec("INSERT INTO backup_restore_test (data) VALUES ($1)", testData)
			require.NoError(t, err, "Should be able to insert data on primary after restore")

			// Wait for replication to standby (with retry logic)
			var newRowExists bool
			maxAttempts := 10
			found := false
			for i := range maxAttempts {
				time.Sleep(1 * time.Second)
				err = standbyDB.QueryRow("SELECT EXISTS(SELECT 1 FROM backup_restore_test WHERE data = $1)", testData).Scan(&newRowExists)
				if err == nil && newRowExists {
					found = true
					t.Logf("Replication working: new row appeared on standby after %d seconds", i+1)
					break
				}
			}
			require.NoError(t, err)
			assert.True(t, found, "New row should exist on standby after restore (waited %d seconds)", maxAttempts)

			t.Log("Step 10: Verifying standby is still in recovery mode...")

			// Verify that the standby is still acting as a replica (in recovery mode)
			var isInRecovery bool
			err = standbyDB.QueryRow("SELECT pg_is_in_recovery()").Scan(&isInRecovery)
			require.NoError(t, err, "Should be able to query recovery status")
			t.Logf("pg_is_in_recovery() returned: %v", isInRecovery)

			// Check if standby.signal exists
			standbySignalPath := filepath.Join(setup.StandbyPgctld.DataDir, "pg_data", "standby.signal")
			_, statErr := os.Stat(standbySignalPath)
			t.Logf("standby.signal exists: %v (path: %s)", statErr == nil, standbySignalPath)

			assert.True(t, isInRecovery, "Standby should still be in recovery mode after restore")

			t.Logf("✓ Full backup created: %s", fullBackupID)
			t.Logf("✓ Backup verified in both primary and standby lists")
			t.Logf("✓ Restore to standby completed successfully")
			t.Logf("✓ Standby database accessible after restore")
			t.Logf("✓ Replication from primary to standby working after restore")
			t.Logf("✓ Standby still in recovery mode (pg_is_in_recovery() = %t)", isInRecovery)
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

		req := &multipoolermanagerdata.BackupRequest{
			ForcePrimary: true, // Required for backups from primary
			Type:         "differential",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.Backup(ctx, req)
		require.NoError(t, err, "Differential backup should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Differential backup ID should contain reference to full backup
		// Format: YYYYMMDD-HHMMSSF_YYYYMMDD-HHMMSSD
		assert.Contains(t, resp.BackupId, "D",
			"Differential backup ID should contain 'D'")

		t.Logf("Differential backup created successfully with ID: %s", resp.BackupId)

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

		req := &multipoolermanagerdata.BackupRequest{
			ForcePrimary: true, // Required for backups from primary
			Type:         "incremental",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.Backup(ctx, req)
		require.NoError(t, err, "Incremental backup should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Incremental backup ID should contain reference to full backup
		// Format: YYYYMMDD-HHMMSSF_YYYYMMDD-HHMMSSI
		assert.Contains(t, resp.BackupId, "I",
			"Incremental backup ID should contain 'I'")

		t.Logf("Incremental backup created successfully with ID: %s", resp.BackupId)

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
}

func TestBackup_ValidationErrors(t *testing.T) {
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end tests (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Wait for manager to be ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	// Create backup client connection
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	backupClient := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)

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
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end tests (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Wait for standby manager to be ready
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create backup client connection to standby
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	backupClient := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)

	t.Run("CreateFullBackupFromStandby", func(t *testing.T) {
		t.Log("Creating full backup from standby...")

		req := &multipoolermanagerdata.BackupRequest{
			ForcePrimary: false, // Should use standby since we're connected to standby
			Type:         "full",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.Backup(ctx, req)
		require.NoError(t, err, "Full backup from standby should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify backup ID format
		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Backup ID should match pgbackrest format: YYYYMMDD-HHMMSSF
		backupIDPattern := regexp.MustCompile(`^\d{8}-\d{6}F$`)
		assert.True(t, backupIDPattern.MatchString(resp.BackupId),
			"Backup ID should match format YYYYMMDD-HHMMSSF, got: %s", resp.BackupId)

		t.Logf("Full backup from standby created successfully with ID: %s", resp.BackupId)

		// Verify backup appears in standby's backup list
		listReq := &multipoolermanagerdata.GetBackupsRequest{
			Limit: 10,
		}

		listCtx := utils.WithShortDeadline(t)
		listResp, err := backupClient.GetBackups(listCtx, listReq)
		require.NoError(t, err, "Listing backups from standby should succeed")
		require.NotNil(t, listResp, "List response should not be nil")

		// Verify at least one backup exists
		assert.NotEmpty(t, listResp.Backups, "Should have at least one backup")

		// Find our backup in the list
		var foundBackup *multipoolermanagerdata.BackupMetadata
		for _, backup := range listResp.Backups {
			if backup.BackupId == resp.BackupId {
				foundBackup = backup
				break
			}
		}

		require.NotNil(t, foundBackup, "Standby backup should be in the list")
		assert.Equal(t, resp.BackupId, foundBackup.BackupId, "Backup ID should match")
		assert.Equal(t, multipoolermanagerdata.BackupMetadata_COMPLETE, foundBackup.Status,
			"Backup status should be COMPLETE")
		assert.NotEmpty(t, foundBackup.FinalLsn, "Backup should have final LSN")

		t.Logf("Standby backup verified in list: ID=%s, Status=%s, FinalLSN=%s",
			foundBackup.BackupId, foundBackup.Status, foundBackup.FinalLsn)
	})

	t.Run("CreateIncrementalBackupFromStandby", func(t *testing.T) {
		t.Log("Creating incremental backup from standby...")

		req := &multipoolermanagerdata.BackupRequest{
			ForcePrimary: false,
			Type:         "incremental",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.Backup(ctx, req)
		require.NoError(t, err, "Incremental backup from standby should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Incremental backup ID should contain 'I'
		assert.Contains(t, resp.BackupId, "I",
			"Incremental backup ID should contain 'I'")

		t.Logf("Incremental backup from standby created successfully with ID: %s", resp.BackupId)
	})
}
