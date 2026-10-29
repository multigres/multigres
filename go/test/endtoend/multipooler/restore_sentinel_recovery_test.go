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

package multipooler

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestRestoreSentinelCrashRecovery verifies that a multipooler starting up in
// the on-disk state an interrupted restore leaves behind recovers: it removes
// the partial data directory, restores from the backup again, and starts
// postgres as a standby, instead of trying to start postgres on the partial
// directory forever.
//
// One pooler self-bootstraps the first backup (no multiorch needed). A second
// pooler then joins with a planted crash state: the restore sentinel plus a
// pg_data that already contains PG_VERSION, which hasDataDirectory() would
// otherwise read as initialized. The sentinel lives in pooler_dir rather than
// PGDATA, so it outlives the partial data directory being removed.
func TestRestoreSentinelCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end restore test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end restore test (no postgres binaries)")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(1),
		shardsetup.WithDeferredMultipoolerStart(),
	)
	defer cleanup()

	require.Len(t, setup.Multipoolers, 1)
	var bootstrapper *shardsetup.MultipoolerInstance
	for _, v := range setup.Multipoolers {
		bootstrapper = v
	}
	require.NoError(t, bootstrapper.Multipooler.Start(t.Context(), t))

	// Wait for the first backup: it is what the joining pooler restores from.
	backupClient := createBackupClient(t, bootstrapper.Multipooler.GrpcPort)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := backupClient.GetBackups(ctx, &multipoolermanagerdatapb.GetBackupsRequest{Limit: 10})
		if err != nil {
			return false
		}
		for _, b := range resp.Backups {
			if b.Status == multipoolermanagerdatapb.BackupMetadata_COMPLETE {
				return true
			}
		}
		return false
	}, 90*time.Second, 2*time.Second, "first backup should be created")

	joiner := setup.CreateMultipoolerInstance(t, "pooler-2",
		utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t))
	require.NoError(t, joiner.Pgctld.Start(t.Context(), t))

	// Plant the state a crash mid-restore leaves behind, before the joining
	// multipooler's monitor first ticks.
	poolerDir := joiner.Pgctld.PoolerDir
	pgDataDir := filepath.Join(poolerDir, "pg_data")
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, "PG_VERSION"), []byte("17\n"), 0o644))
	partialFile := filepath.Join(pgDataDir, "partial_marker")
	require.NoError(t, os.WriteFile(partialFile, []byte("left by an interrupted restore\n"), 0o644))
	sentinelPath := filepath.Join(poolerDir, constants.RestoreSentinelFile)
	require.NoError(t, os.WriteFile(sentinelPath, []byte("simulated interrupted restore\n"), 0o644))

	require.NoError(t, joiner.Multipooler.Start(t.Context(), t))

	require.Eventually(t, func() bool {
		data, err := os.ReadFile(joiner.Multipooler.LogFile)
		return err == nil && strings.Contains(string(data), "MonitorPostgres: successfully restored from backup")
	}, 90*time.Second, 2*time.Second, "the joining pooler should restore from the backup")

	data, err := os.ReadFile(joiner.Multipooler.LogFile)
	require.NoError(t, err)
	assert.Contains(t, string(data), "restore sentinel from an interrupted restore detected",
		"the restore should go through the sentinel path")
	assert.NotContains(t, string(data), "PostgreSQL initialized but not running",
		"postgres must not be started on the partial data directory")
	assert.NoFileExists(t, partialFile, "the partial data directory should be removed")
	assert.NoFileExists(t, sentinelPath, "the sentinel should be removed once the restore completes")

	db := connectToPostgresViaSocket(t, getPostgresSocketPath(poolerDir), joiner.Pgctld.PgPort)
	defer db.Close()
	var inRecovery bool
	require.NoError(t, db.QueryRow("SELECT pg_is_in_recovery()").Scan(&inRecovery))
	assert.True(t, inRecovery, "the restored pooler should run postgres as a standby")
}
