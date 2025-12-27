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

package multiorch

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// connectToPostgres establishes a connection to PostgreSQL using Unix socket
func connectToPostgres(t *testing.T, socketDir string, port int) *sql.DB {
	t.Helper()

	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable", socketDir, port)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "Failed to open database connection")

	err = db.Ping()
	require.NoError(t, err, "Failed to ping database")

	return db
}

// waitForShardPrimary polls multipooler nodes until at least one is initialized as primary.
// Returns the name of the elected primary.
func waitForShardPrimary(t *testing.T, setup *shardsetup.ShardSetup, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	checkInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		for name, inst := range setup.Multipoolers {
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			cancel()
			client.Close()

			if err != nil {
				continue
			}

			if status.Status.IsInitialized && status.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				t.Logf("Shard bootstrapped: primary is %s (pooler_type=%s)", name, status.Status.PoolerType)
				return name
			}
		}
		t.Logf("Waiting for shard bootstrap... (sleeping %v)", checkInterval)
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: shard did not bootstrap within %v", timeout)
	return ""
}

// waitForStandbysInitialized polls multipooler nodes until expected count of standbys are initialized.
func waitForStandbysInitialized(t *testing.T, setup *shardsetup.ShardSetup, expectedCount int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	checkInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		standbyCount := 0
		for name, inst := range setup.Multipoolers {
			if name == setup.PrimaryName {
				continue
			}

			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			cancel()
			client.Close()

			if err != nil {
				continue
			}

			if status.Status.IsInitialized && status.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA && status.Status.PostgresRunning {
				standbyCount++
			}
		}
		if standbyCount >= expectedCount {
			t.Logf("All %d standbys initialized successfully", standbyCount)
			return
		}
		t.Logf("Waiting for standbys to initialize... (have %d/%d, sleeping %v)", standbyCount, expectedCount, checkInterval)
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: standbys did not initialize within %v", timeout)
}

// waitForSyncReplicationConfigured polls the primary until synchronous_standby_names is configured.
// This is needed because configureSynchronousReplication runs after standbys are initialized.
func waitForSyncReplicationConfigured(t *testing.T, setup *shardsetup.ShardSetup, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	checkInterval := 500 * time.Millisecond

	for time.Now().Before(deadline) {
		primaryClient := setup.NewPrimaryClient(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		syncStandbyNames, err := shardsetup.QueryStringValue(ctx, primaryClient.Pooler, "SHOW synchronous_standby_names")
		cancel()
		primaryClient.Close()

		if err != nil {
			t.Logf("Waiting for sync replication config... (error: %v)", err)
			time.Sleep(checkInterval)
			continue
		}

		if syncStandbyNames != "" {
			t.Logf("Sync replication configured: synchronous_standby_names=%s", syncStandbyNames)
			return
		}
		t.Logf("Waiting for sync replication config... (synchronous_standby_names is empty)")
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: sync replication was not configured within %v", timeout)
}
