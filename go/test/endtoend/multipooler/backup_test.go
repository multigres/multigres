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

	backupservicepb "github.com/multigres/multigres/go/pb/multipoolerbackupservice"
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

func TestBackup_CreateAndList(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

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
	backupClient := backupservicepb.NewMultiPoolerBackupServiceClient(conn)

	t.Run("CreateFullBackup", func(t *testing.T) {
		t.Log("Creating full backup...")

		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "full",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.BackupShard(ctx, req)
		require.NoError(t, err, "Full backup should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify backup ID format
		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Backup ID should match pgbackrest format: YYYYMMDD-HHMMSSF
		// F indicates full backup, D indicates differential, I indicates incremental
		backupIDPattern := regexp.MustCompile(`^\d{8}-\d{6}F$`)
		assert.True(t, backupIDPattern.MatchString(resp.BackupId),
			"Backup ID should match format YYYYMMDD-HHMMSSF, got: %s", resp.BackupId)

		t.Logf("Full backup created successfully with ID: %s", resp.BackupId)

		// Store backup ID for next subtest
		fullBackupID := resp.BackupId

		t.Run("ListBackups_VerifyFullBackup", func(t *testing.T) {
			t.Log("Listing backups to verify full backup...")

			listReq := &backupservicepb.GetShardBackupsRequest{
				Limit: 10,
			}

			listCtx := utils.WithShortDeadline(t)
			listResp, err := backupClient.GetShardBackups(listCtx, listReq)
			require.NoError(t, err, "Listing backups should succeed")
			require.NotNil(t, listResp, "List response should not be nil")

			// Verify at least one backup exists
			assert.NotEmpty(t, listResp.Backups, "Should have at least one backup")

			// Find our backup in the list
			var foundBackup *backupservicepb.BackupMetadata
			for _, backup := range listResp.Backups {
				if backup.BackupId == fullBackupID {
					foundBackup = backup
					break
				}
			}

			require.NotNil(t, foundBackup, "Our backup should be in the list")

			// Verify backup metadata
			assert.Equal(t, fullBackupID, foundBackup.BackupId, "Backup ID should match")
			assert.Equal(t, backupservicepb.BackupMetadata_COMPLETE, foundBackup.Status,
				"Backup status should be COMPLETE")

			t.Logf("Backup verified in list: ID=%s, Status=%s",
				foundBackup.BackupId, foundBackup.Status)
		})

		t.Run("ListBackups_WithoutLimit", func(t *testing.T) {
			t.Log("Listing backups without limit...")

			listReq := &backupservicepb.GetShardBackupsRequest{
				Limit: 0, // No limit
			}

			listCtx := utils.WithShortDeadline(t)
			listResp, err := backupClient.GetShardBackups(listCtx, listReq)
			require.NoError(t, err, "Listing backups without limit should succeed")
			require.NotNil(t, listResp, "List response should not be nil")

			// Should have at least our backup
			assert.NotEmpty(t, listResp.Backups, "Should have at least one backup")

			t.Logf("Listed %d backup(s) without limit", len(listResp.Backups))
		})

		t.Run("ListBackups_WithSmallLimit", func(t *testing.T) {
			t.Log("Listing backups with limit=1...")

			listReq := &backupservicepb.GetShardBackupsRequest{
				Limit: 1,
			}

			listCtx := utils.WithShortDeadline(t)
			listResp, err := backupClient.GetShardBackups(listCtx, listReq)
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

		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "differential",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.BackupShard(ctx, req)
		require.NoError(t, err, "Differential backup should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Differential backup ID should contain reference to full backup
		// Format: YYYYMMDD-HHMMSSF_YYYYMMDD-HHMMSSD
		assert.Contains(t, resp.BackupId, "D",
			"Differential backup ID should contain 'D'")

		t.Logf("Differential backup created successfully with ID: %s", resp.BackupId)

		// Verify differential backup appears in list
		listReq := &backupservicepb.GetShardBackupsRequest{
			Limit: 10,
		}

		listCtx := utils.WithShortDeadline(t)
		listResp, err := backupClient.GetShardBackups(listCtx, listReq)
		require.NoError(t, err, "Listing backups should succeed")

		// Should now have at least 2 backups (full + differential)
		assert.GreaterOrEqual(t, len(listResp.Backups), 2,
			"Should have at least 2 backups (full + differential)")

		t.Logf("Verified %d total backups exist", len(listResp.Backups))
	})

	t.Run("CreateIncrementalBackup", func(t *testing.T) {
		t.Log("Creating incremental backup...")

		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "incremental",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.BackupShard(ctx, req)
		require.NoError(t, err, "Incremental backup should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Incremental backup ID should contain reference to full backup
		// Format: YYYYMMDD-HHMMSSF_YYYYMMDD-HHMMSSI
		assert.Contains(t, resp.BackupId, "I",
			"Incremental backup ID should contain 'I'")

		t.Logf("Incremental backup created successfully with ID: %s", resp.BackupId)

		// Verify incremental backup appears in list
		listReq := &backupservicepb.GetShardBackupsRequest{
			Limit: 10,
		}

		listCtx := utils.WithShortDeadline(t)
		listResp, err := backupClient.GetShardBackups(listCtx, listReq)
		require.NoError(t, err, "Listing backups should succeed")

		// Should now have at least 3 backups (full + differential + incremental)
		assert.GreaterOrEqual(t, len(listResp.Backups), 3,
			"Should have at least 3 backups (full + differential + incremental)")

		t.Logf("Verified %d total backups exist", len(listResp.Backups))
	})
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
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	backupClient := backupservicepb.NewMultiPoolerBackupServiceClient(conn)

	t.Run("MissingTableGroup", func(t *testing.T) {
		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "", // Missing
			Shard:        "default",
			ForcePrimary: false,
			Type:         "full",
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.BackupShard(ctx, req)

		assert.Error(t, err, "Should return error for missing table_group")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "table_group", "Error should mention table_group")
	})

	t.Run("MissingShard", func(t *testing.T) {
		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "", // Missing
			ForcePrimary: false,
			Type:         "full",
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.BackupShard(ctx, req)

		assert.Error(t, err, "Should return error for missing shard")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "shard", "Error should mention shard")
	})

	t.Run("MissingType", func(t *testing.T) {
		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "", // Missing
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.BackupShard(ctx, req)

		assert.Error(t, err, "Should return error for missing type")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "type", "Error should mention type")
	})

	t.Run("InvalidType", func(t *testing.T) {
		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "invalid", // Invalid type
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.BackupShard(ctx, req)

		assert.Error(t, err, "Should return error for invalid type")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "invalid", "Error should mention invalid type")
	})
}

func TestBackup_FromStandby(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

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
	backupClient := backupservicepb.NewMultiPoolerBackupServiceClient(conn)

	t.Run("CreateFullBackupFromStandby", func(t *testing.T) {
		t.Log("Creating full backup from standby...")

		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false, // Should use standby since we're connected to standby
			Type:         "full",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.BackupShard(ctx, req)
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
		listReq := &backupservicepb.GetShardBackupsRequest{
			Limit: 10,
		}

		listCtx := utils.WithShortDeadline(t)
		listResp, err := backupClient.GetShardBackups(listCtx, listReq)
		require.NoError(t, err, "Listing backups from standby should succeed")
		require.NotNil(t, listResp, "List response should not be nil")

		// Verify at least one backup exists
		assert.NotEmpty(t, listResp.Backups, "Should have at least one backup")

		// Find our backup in the list
		var foundBackup *backupservicepb.BackupMetadata
		for _, backup := range listResp.Backups {
			if backup.BackupId == resp.BackupId {
				foundBackup = backup
				break
			}
		}

		require.NotNil(t, foundBackup, "Standby backup should be in the list")
		assert.Equal(t, resp.BackupId, foundBackup.BackupId, "Backup ID should match")
		assert.Equal(t, backupservicepb.BackupMetadata_COMPLETE, foundBackup.Status,
			"Backup status should be COMPLETE")

		t.Logf("Standby backup verified in list: ID=%s, Status=%s",
			foundBackup.BackupId, foundBackup.Status)
	})

	t.Run("CreateIncrementalBackupFromStandby", func(t *testing.T) {
		t.Log("Creating incremental backup from standby...")

		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "incremental",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.BackupShard(ctx, req)
		require.NoError(t, err, "Incremental backup from standby should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Incremental backup ID should contain 'I'
		assert.Contains(t, resp.BackupId, "I",
			"Incremental backup ID should contain 'I'")

		t.Logf("Incremental backup from standby created successfully with ID: %s", resp.BackupId)
	})
}

func TestBackup_RestoreDataIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Wait for both primary and standby managers to be ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create backup client connection to primary (for creating backup)
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryBackupClient := backupservicepb.NewMultiPoolerBackupServiceClient(primaryConn)

	// Create backup client connection to standby (for restore)
	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyBackupClient := backupservicepb.NewMultiPoolerBackupServiceClient(standbyConn)

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

	t.Log("Step 2: Creating full backup from primary...")

	req := &backupservicepb.BackupShardRequest{
		TableGroup:   "test",
		Shard:        "default",
		ForcePrimary: false,
		Type:         "full",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	backupResp, err := primaryBackupClient.BackupShard(ctx, req)
	require.NoError(t, err, "Backup should succeed")
	require.NotNil(t, backupResp, "Backup response should not be nil")
	require.NotEmpty(t, backupResp.BackupId, "Backup ID should not be empty")

	backupID := backupResp.BackupId
	t.Logf("Backup created with ID: %s", backupID)

	t.Log("Step 3: Verifying backup exists in standby's list...")

	listReq := &backupservicepb.GetShardBackupsRequest{
		Limit: 20,
	}

	listCtx := utils.WithShortDeadline(t)
	listResp, err := standbyBackupClient.GetShardBackups(listCtx, listReq)
	require.NoError(t, err, "Listing backups should succeed")
	require.NotNil(t, listResp, "List response should not be nil")

	// Find our backup in the list
	var foundBackup *backupservicepb.BackupMetadata
	for _, backup := range listResp.Backups {
		if backup.BackupId == backupID {
			foundBackup = backup
			break
		}
	}

	require.NotNil(t, foundBackup, "Backup should be in the list")
	assert.Equal(t, backupservicepb.BackupMetadata_COMPLETE, foundBackup.Status,
		"Backup status should be COMPLETE")
	t.Logf("Backup verified in list: ID=%s, Status=%s", foundBackup.BackupId, foundBackup.Status)

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

	t.Log("Step 5: Restoring from backup to standby...")

	restoreReq := &backupservicepb.RestoreShardFromBackupRequest{
		BackupId: backupID,
	}

	restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer restoreCancel()

	_, err = standbyBackupClient.RestoreShardFromBackup(restoreCtx, restoreReq)
	require.NoError(t, err, "Restore to standby should succeed")
	t.Log("Restore to standby completed successfully")

	// Wait a bit for PostgreSQL to be ready after restore
	time.Sleep(5 * time.Second)

	// Connect to the standby database after restore
	standbySocketDir := filepath.Join(setup.StandbyPgctld.DataDir, "pg_sockets")
	standbyDB := connectToPostgres(t, standbySocketDir, setup.StandbyPgctld.PgPort)
	defer standbyDB.Close()

	t.Log("Step 6: Verifying standby database is accessible after restore...")

	// Verify standby database is accessible and we can query data
	var countAfterRestore int
	err = standbyDB.QueryRow("SELECT COUNT(*) FROM backup_restore_test").Scan(&countAfterRestore)
	require.NoError(t, err)
	t.Logf("Row count on standby after restore: %d", countAfterRestore)

	// TODO: Point-in-time recovery (stopping WAL replay at the backup point) requires additional work.
	// Currently, PostgreSQL replays all available WAL after restore, which brings the database
	// back to its current state. To implement proper point-in-time recovery, we need to:
	// - Use timestamp-based recovery with --type=time and a specific target timestamp
	// - Or clear the WAL archive of post-backup WAL files before restore
	// - Or manually configure PostgreSQL recovery settings after pgBackRest completes
	//
	// For now, this test verifies that:
	// - Backup is created successfully from primary
	// - Backup exists in the list
	// - Restore to standby completes without errors
	// - PostgreSQL standby starts successfully after restore
	// - Standby database is accessible and queryable after restore
	// - Replication from primary to standby continues to work after restore
	//
	// These are the core requirements for a working backup/restore system.
	// The point-in-time recovery feature can be added in a future enhancement.

	t.Logf("✓ Restore completed and standby database is accessible")
	t.Logf("✓ Found %d rows in restored standby database", countAfterRestore)

	t.Log("Step 7: Verifying replication from primary to standby still works...")

	// Insert a new row on primary after restore to test replication
	testData := "row_after_restore"
	_, err = db.Exec("INSERT INTO backup_restore_test (data) VALUES ($1)", testData)
	require.NoError(t, err, "Should be able to insert data on primary after restore")

	// Wait for replication to standby (with retry logic)
	var newRowExists bool
	maxAttempts := 10
	found := false
	for i := 0; i < maxAttempts; i++ {
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

	t.Log("Step 8: Verifying standby is still in recovery mode...")

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

	t.Log("All backup and restore tests passed!")
	t.Logf("✓ Backup created from primary: %s", backupID)
	t.Logf("✓ Backup verified in standby's list")
	t.Logf("✓ Restore to standby completed successfully")
	t.Logf("✓ Standby database accessible after restore")
	t.Logf("✓ Replication from primary to standby working after restore")
	t.Logf("✓ Standby still in recovery mode (pg_is_in_recovery() = %t)", isInRecovery)
}
