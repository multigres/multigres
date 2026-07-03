// Copyright 2026 Supabase, Inc.
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

// This file contains test helpers for backup_test.go to reduce duplication.
// These helpers follow Go testing best practices:
// - All helpers call t.Helper() for proper failure attribution
// - Helpers fail fast with require for critical assertions
// - Cleanup is automatic via t.Cleanup() where applicable

package multipooler

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// Backup ID format patterns
var (
	fullBackupIDPattern = regexp.MustCompile(`^\d{8}-\d{6}F$`)
)

// createBackupClient creates a gRPC client for backup operations.
// The connection is automatically closed via t.Cleanup.
func createBackupClient(t *testing.T, grpcPort int) multipoolermanagerpb.MultiPoolerManagerClient {
	t.Helper()

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to create gRPC connection")
	t.Cleanup(func() { conn.Close() })

	return multipoolermanagerpb.NewMultiPoolerManagerClient(conn)
}

// createConsensusClient creates a gRPC client for consensus operations.
// The connection is automatically closed via t.Cleanup.
func createConsensusClient(t *testing.T, grpcPort int) consensuspb.MultiPoolerConsensusClient {
	t.Helper()

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to create gRPC connection")
	t.Cleanup(func() { conn.Close() })

	return consensuspb.NewMultiPoolerConsensusClient(conn)
}

// assertBackupIDFormat verifies the backup ID matches the expected format.
func assertBackupIDFormat(t *testing.T, backupID string, backupType string) {
	t.Helper()

	assert.NotEmpty(t, backupID, "Backup ID should not be empty")

	switch backupType {
	case "full":
		assert.True(t, fullBackupIDPattern.MatchString(backupID),
			"Full backup ID should match format YYYYMMDD-HHMMSSF, got: %s", backupID)
	case "differential":
		assert.Contains(t, backupID, "D",
			"Differential backup ID should contain 'D', got: %s", backupID)
	case "incremental":
		assert.Contains(t, backupID, "I",
			"Incremental backup ID should contain 'I', got: %s", backupID)
	default:
		t.Fatalf("Unknown backup type: %s", backupType)
	}
}

// findBackupInList searches for a backup by ID in the list of backups.
// Fails the test if the backup is not found.
func findBackupInList(t *testing.T, backups []*multipoolermanagerdata.BackupMetadata, backupID string) *multipoolermanagerdata.BackupMetadata {
	t.Helper()

	for _, backup := range backups {
		if backup.BackupId == backupID {
			return backup
		}
	}

	require.Failf(t, "Backup not found", "Backup ID %s not found in list of %d backups", backupID, len(backups))
	return nil // unreachable
}

// assertBackupComplete verifies a backup has completed successfully.
func assertBackupComplete(t *testing.T, backup *multipoolermanagerdata.BackupMetadata, expectedID string) {
	t.Helper()

	assert.Equal(t, expectedID, backup.BackupId, "Backup ID should match")
	assert.Equal(t, multipoolermanagerdata.BackupMetadata_COMPLETE, backup.Status,
		"Backup status should be COMPLETE")
	assert.NotEmpty(t, backup.StopLsn, "Backup should have stop LSN")
	assert.NotEmpty(t, backup.StartLsn, "Backup should have start LSN")
	// pg_version capture is best-effort in production (a failed server_version
	// read never fails the backup); this assertion assumes the read succeeds,
	// which it always does in the controlled endtoend environment.
	assert.NotEmpty(t, backup.PgVersion, "Backup should have pg_version")
}

// connectToPostgresViaSocket establishes a connection to PostgreSQL using Unix socket.
// The connection is automatically closed via defer in the caller.
func connectToPostgresViaSocket(t *testing.T, socketDir string, port int) *sql.DB {
	t.Helper()

	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable password=%s",
		socketDir, port, shardsetup.TestPostgresPassword)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "Failed to open database connection")

	err = db.Ping()
	require.NoError(t, err, "Failed to ping database")

	return db
}

// getPostgresSocketPath returns the path to the PostgreSQL Unix socket directory.
func getPostgresSocketPath(pgctldDataDir string) string {
	return filepath.Join(pgctldDataDir, "pg_sockets")
}

// createAndVerifyBackup creates a backup and verifies it was created successfully.
// Returns the backup ID.
func createAndVerifyBackup(t *testing.T, client multipoolermanagerpb.MultiPoolerManagerClient, backupType string, forcePrimary bool, timeout time.Duration, overrides map[string]string) string {
	t.Helper()

	req := &multipoolermanagerdata.BackupRequest{
		ForcePrimary: forcePrimary,
		Type:         backupType,
		Overrides:    overrides,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := client.Backup(ctx, req)
	require.NoError(t, err, "%s backup should succeed", backupType)
	require.NotNil(t, resp, "Response should not be nil")

	assertBackupIDFormat(t, resp.BackupId, backupType)

	t.Logf("%s backup created successfully with ID: %s", backupType, resp.BackupId)
	return resp.BackupId
}

// corruptBackupDataFile overwrites a backup data file in the filesystem
// pgBackRest repo so that `pgbackrest verify` detects a checksum mismatch. It
// registers a t.Cleanup that restores the original bytes, leaving the shared
// stanza healthy for subsequent tests (verify is read-only, so a byte-for-byte
// restore fully reverts the corruption).
//
// Only valid for the filesystem backend — the S3 backend keeps blobs inside
// s3mock rather than on a path we can edit. The stanza name is the fixed
// "multigres" used by MultiPoolerManager.stanzaName().
func corruptBackupDataFile(t *testing.T, tempDir string) {
	t.Helper()

	backupRoot := filepath.Join(tempDir, "backup-repo", "backup", "multigres")

	// Pick the largest non-manifest file under a backup set. Manifests carry
	// their own copy/checksum and corrupting them yields a different error
	// class; a data file (bundled small files or pg_data/*) is checksum-verified
	// against the manifest, so corrupting it is the cleanest "invalid backup".
	var target string
	var targetSize int64
	err := filepath.WalkDir(backupRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), "backup.manifest") {
			return nil
		}
		// Track the largest data file seen. A failed Info() (or a zero-length
		// file) simply isn't a candidate; keep walking either way. Size() starts
		// the comparison at 0, so empty files are naturally excluded.
		if info, statErr := d.Info(); statErr == nil && info.Size() > targetSize {
			target = path
			targetSize = info.Size()
		}
		return nil
	})
	require.NoError(t, err, "walk backup repo at %s", backupRoot)
	require.NotEmpty(t, target, "no backup data file found under %s to corrupt", backupRoot)

	original, err := os.ReadFile(target)
	require.NoError(t, err, "read backup file %s", target)
	t.Cleanup(func() {
		if err := os.WriteFile(target, original, 0o600); err != nil {
			t.Logf("warning: failed to restore corrupted backup file %s: %v", target, err)
		}
	})

	// Flip every byte so the stored checksum can never match, keeping the file
	// length unchanged so the failure is a checksum mismatch rather than a
	// truncation/size error.
	corrupted := make([]byte, len(original))
	for i, b := range original {
		corrupted[i] = b ^ 0xFF
	}
	require.NoError(t, os.WriteFile(target, corrupted, 0o600), "corrupt backup file %s", target)
	t.Logf("Corrupted backup data file for verify test: %s (%d bytes)", target, targetSize)
}

// listAndFindBackup lists backups and finds a specific backup by ID.
// Returns the found backup metadata.
func listAndFindBackup(t *testing.T, client multipoolermanagerpb.MultiPoolerManagerClient, backupID string, limit uint32) *multipoolermanagerdata.BackupMetadata {
	t.Helper()

	listReq := &multipoolermanagerdata.GetBackupsRequest{
		Limit: limit,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	listResp, err := client.GetBackups(ctx, listReq)
	require.NoError(t, err, "Listing backups should succeed")
	require.NotNil(t, listResp, "List response should not be nil")
	assert.NotEmpty(t, listResp.Backups, "Should have at least one backup")

	foundBackup := findBackupInList(t, listResp.Backups, backupID)
	assertBackupComplete(t, foundBackup, backupID)

	return foundBackup
}
