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
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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

// waitForShardReady polls until one node is an initialized primary, expectedStandbyCount nodes
// are initialized replicating standbys, and sync replication is configured on the primary.
// Returns the name of the elected primary.
func waitForShardReady(t *testing.T, setup *shardsetup.ShardSetup, expectedStandbyCount int, timeout time.Duration) string {
	t.Helper()

	var poolers []*shardsetup.MultipoolerInstance
	for _, inst := range setup.Multipoolers {
		poolers = append(poolers, inst)
	}

	primaryName := shardsetup.EventuallyPoolersCondition(t, poolers, timeout, 2*time.Second,
		func(statuses []shardsetup.PoolerStatusResult) (string, bool, string) {
			var primary string
			var syncStandbyNames map[string]bool
			replicaNames := make(map[string]bool)
			for _, r := range statuses {
				if r.Err != nil || r.Status == nil {
					continue
				}
				if r.Status.IsInitialized && r.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
					primary = r.Name
					if r.Status.PrimaryStatus != nil && r.Status.PrimaryStatus.SyncReplicationConfig != nil {
						syncStandbyNames = make(map[string]bool)
						for _, id := range r.Status.PrimaryStatus.SyncReplicationConfig.StandbyIds {
							syncStandbyNames[id.Name] = true
						}
					}
				} else if r.Status.IsInitialized && r.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA && r.Status.PostgresRunning {
					replicaNames[r.Name] = true
				}
			}
			if primary == "" {
				return "", false, "no primary elected yet"
			}
			if len(replicaNames) != expectedStandbyCount {
				return "", false, fmt.Sprintf("%d/%d standbys initialized", len(replicaNames), expectedStandbyCount)
			}
			for name := range replicaNames {
				if !syncStandbyNames[name] {
					return "", false, fmt.Sprintf("replica %s not yet in primary sync config", name)
				}
			}
			return primary, true, ""
		},
		"shard did not become ready within %v", timeout,
	)
	t.Logf("Shard ready: primary=%s, %d standbys initialized with sync replication", primaryName, expectedStandbyCount)
	return primaryName
}
