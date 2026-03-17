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

// waitForShardPrimary polls multipooler nodes until at least one is initialized as primary.
// Returns the name of the elected primary.
func waitForShardPrimary(t *testing.T, setup *shardsetup.ShardSetup, timeout time.Duration) string {
	t.Helper()

	var poolers []*shardsetup.MultipoolerInstance
	for _, inst := range setup.Multipoolers {
		poolers = append(poolers, inst)
	}

	primaryName := shardsetup.EventuallyPoolersCondition(t, poolers, timeout, 2*time.Second,
		func(statuses []shardsetup.PoolerStatusResult) (string, bool, string) {
			for _, r := range statuses {
				if r.Err != nil || r.Status == nil {
					continue
				}
				if r.Status.IsInitialized && r.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
					return r.Name, true, ""
				}
			}
			return "", false, "no primary elected yet"
		},
		"shard did not bootstrap within %v", timeout,
	)
	t.Logf("Shard bootstrapped: primary is %s", primaryName)
	return primaryName
}

// waitForStandbysInitialized polls multipooler nodes until expected count of standbys are initialized
// and synchronous replication is configured on the primary.
func waitForStandbysInitialized(t *testing.T, setup *shardsetup.ShardSetup, expectedCount int, timeout time.Duration) {
	t.Helper()

	var poolers []*shardsetup.MultipoolerInstance
	for _, inst := range setup.Multipoolers {
		poolers = append(poolers, inst)
	}

	shardsetup.EventuallyPoolersCondition(t, poolers, timeout, 2*time.Second,
		func(statuses []shardsetup.PoolerStatusResult) (any, bool, string) {
			standbyCount := 0
			syncConfigured := false
			for _, r := range statuses {
				if r.Err != nil || r.Status == nil {
					continue
				}
				if r.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
					if r.Status.PrimaryStatus != nil && r.Status.PrimaryStatus.SyncReplicationConfig != nil {
						syncConfigured = len(r.Status.PrimaryStatus.SyncReplicationConfig.StandbyIds) >= expectedCount
					}
				} else if r.Status.IsInitialized && r.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA && r.Status.PostgresRunning {
					standbyCount++
				}
			}
			if standbyCount < expectedCount {
				return nil, false, fmt.Sprintf("only %d/%d standbys initialized", standbyCount, expectedCount)
			}
			if !syncConfigured {
				return nil, false, "sync replication not yet configured on primary"
			}
			return nil, true, ""
		},
		"standbys did not initialize within %v", timeout,
	)
	t.Logf("All %d standbys initialized successfully", expectedCount)
}
