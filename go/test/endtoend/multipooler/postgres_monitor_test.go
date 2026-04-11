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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestPostgresMonitorControl tests that the postgres monitor automatically restarts
// postgres when it goes down.
func TestPostgresMonitorControl(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup, WithEnabledMonitor())

	// Wait for primary manager to be ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	// Create client to primary multipooler
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	t.Run("1. verify postgres is initially running", func(t *testing.T) {
		ctx := utils.WithShortDeadline(t)
		status, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "Should get status from primary")
		require.True(t, status.Status.PostgresReady, "Postgres should be running initially")
		t.Logf("Postgres is running initially")
	})

	t.Run("2. kill postgres and verify auto-restart", func(t *testing.T) {
		// Kill postgres process
		t.Logf("Killing postgres on primary node %s", setup.PrimaryName)
		setup.KillPostgres(t, setup.PrimaryName)

		// Wait a moment for monitoring to detect the failure
		time.Sleep(2 * time.Second)

		// Verify postgres was automatically restarted by monitoring
		t.Logf("Waiting for postgres to be automatically restarted...")
		require.Eventually(t, func() bool {
			ctx := utils.WithShortDeadline(t)
			status, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			if err != nil {
				t.Logf("Status check failed: %v", err)
				return false
			}
			return status.Status.PostgresReady
		}, 30*time.Second, 500*time.Millisecond, "Postgres should be automatically restarted by monitoring")

		t.Logf("Postgres was successfully auto-restarted")
	})
}
