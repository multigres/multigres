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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestBootstrapSentinelCrashRecovery verifies that a multipooler starting up
// with a bootstrap sentinel + empty pg_data directory — the on-disk state a
// prior crashed first-backup attempt would leave behind — recovers correctly:
// it removes the stale directory, runs the first-backup flow, and ends up with
// a completed backup.
//
// The sentinel lives in pooler_dir rather than PGDATA so pgBackRest never
// captures it in backups.
func TestBootstrapSentinelCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end bootstrap test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end bootstrap test (no postgres binaries)")
	}

	// Single pooler with pgctld started but multipooler not yet running, so
	// we can stage the crashed state under pooler_dir before the monitor ticks.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(1),
		shardsetup.WithDeferredMultipoolerStart(),
	)
	defer cleanup()

	require.Len(t, setup.Multipoolers, 1)
	var inst *shardsetup.MultipoolerInstance
	for _, v := range setup.Multipoolers {
		inst = v
	}
	poolerDir := inst.Pgctld.PoolerDir

	// Plant the simulated post-crash state: empty pg_data + sentinel file.
	pgDataDir := filepath.Join(poolerDir, "pg_data")
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	sentinelPath := filepath.Join(poolerDir, constants.BootstrapSentinelFile)
	require.NoError(t, os.WriteFile(sentinelPath, []byte("simulated prior bootstrap attempt\n"), 0o644))
	t.Logf("planted sentinel + empty pg_data at %s", poolerDir)

	// Start the multipooler. Its first monitor tick should detect the sentinel,
	// remove the empty pg_data, run a fresh initdb, and complete the first backup.
	require.NoError(t, inst.Multipooler.Start(t.Context(), t))
	shardsetup.WaitForManagerReady(t, inst.Multipooler)

	// Poll GetBackups until at least one COMPLETE backup appears.
	backupClient := createBackupClient(t, inst.Multipooler.GrpcPort)
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
	}, 90*time.Second, 1*time.Second, "expected a COMPLETE backup to appear via GetBackups")

	// The happy path removes the sentinel after the final data-directory cleanup.
	assert.Eventually(t, func() bool {
		_, err := os.Stat(sentinelPath)
		return os.IsNotExist(err)
	}, 10*time.Second, 200*time.Millisecond,
		"sentinel should be removed after successful first backup")
}
