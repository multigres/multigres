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
// in the on-disk state a crashed first-backup attempt could leave behind
// recovers correctly: it removes any stale directory, runs the first-backup
// flow, and ends up with a completed backup.
//
// Two prior-crash states are covered:
//
//   - "data dir populated": simulates a crash AFTER initdb ran — pg_data has
//     PG_VERSION so hasDataDirectory() would otherwise refuse the flow. The
//     sentinel triggers cleanup first.
//   - "data dir absent": simulates a crash BEFORE initdb ran, or between the
//     success-path removeDataDirectory and removeBootstrapSentinel. The
//     sentinel is present but pg_data does not exist.
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

	tests := []struct {
		name          string
		plantDataDir  bool // when true, create pg_data with PG_VERSION
		sentinelBytes []byte
	}{
		{
			name:          "data dir populated from prior crash",
			plantDataDir:  true,
			sentinelBytes: []byte("simulated prior bootstrap attempt\n"),
		},
		{
			name:          "data dir absent at crash time",
			plantDataDir:  false,
			sentinelBytes: []byte("simulated pre-initdb crash\n"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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

			pgDataDir := filepath.Join(poolerDir, "pg_data")
			if tc.plantDataDir {
				require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, "PG_VERSION"), []byte("16\n"), 0o644))
			}
			sentinelPath := filepath.Join(poolerDir, constants.BootstrapSentinelFile)
			require.NoError(t, os.WriteFile(sentinelPath, tc.sentinelBytes, 0o644))
			t.Logf("planted sentinel (plantDataDir=%v) at %s", tc.plantDataDir, poolerDir)

			// Start the multipooler. Its first monitor tick should detect the sentinel,
			// remove any stale pg_data, run a fresh initdb, and complete the first backup.
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
			}, 10*time.Second, 1*time.Second, "expected a COMPLETE backup to appear via GetBackups")

			// The happy path removes the sentinel after the final data-directory cleanup.
			assert.Eventually(t, func() bool {
				_, err := os.Stat(sentinelPath)
				return os.IsNotExist(err)
			}, 10*time.Second, 200*time.Millisecond,
				"sentinel should be removed after successful first backup")
		})
	}
}
