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

package queryserving

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestMultiGateway_PostgresCrashRecovery tests that SQL queries through multigateway recover
// after postgres crashes on the primary and is auto-restarted by the monitor.
func TestMultiGateway_PostgresCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PostgresCrashRecovery test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping cluster lifecycle tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t, shardsetup.WithEnabledMonitor())

	// Connect to multigateway
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)

	// Step 1: Verify baseline query works through multigateway
	t.Run("baseline query succeeds", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 10*time.Second)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err, "baseline query should succeed before crash")
		require.Equal(t, 1, result)
		t.Log("Baseline query through multigateway succeeded")
	})

	// Step 2: Kill postgres on primary and wait for auto-restart
	t.Run("kill postgres and wait for auto-restart", func(t *testing.T) {
		t.Logf("Killing postgres on primary node %s", setup.PrimaryName)
		setup.KillPostgres(t, setup.PrimaryName)

		// Wait for monitor to detect the crash and restart postgres
		primaryClient := setup.NewPrimaryClient(t)
		defer primaryClient.Close()

		t.Log("Waiting for postgres to be auto-restarted by monitor...")
		require.Eventually(t, func() bool {
			ctx := utils.WithShortDeadline(t)
			status, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			if err != nil {
				t.Logf("Status check failed: %v", err)
				return false
			}
			return status.Status.PostgresRunning
		}, 30*time.Second, 500*time.Millisecond, "Postgres should be auto-restarted by monitor")

		t.Log("Postgres auto-restarted successfully")
	})

	// Step 3: Verify queries through multigateway recover after postgres restart.
	t.Run("query succeeds after postgres crash recovery", func(t *testing.T) {
		// Open a fresh connection to multigateway (the old connection pool in
		// database/sql would also have stale connections, so use a fresh one)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		// Allow some time for connection pools to be re-established.
		// Use Eventually with a generous timeout.
		var result int
		require.Eventually(t, func() bool {
			ctx := utils.WithShortDeadline(t)
			err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
			if err != nil {
				t.Logf("Query after restart failed (may still be recovering): %v", err)
				return false
			}
			return result == 1
		}, 30*time.Second, 1*time.Second,
			"Query through multigateway should succeed after postgres crash recovery on primary")

		t.Log("Query through multigateway succeeded after postgres crash recovery")
	})
}
