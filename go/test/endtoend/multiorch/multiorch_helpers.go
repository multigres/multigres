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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
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
			var primaryStatus *shardsetup.PoolerStatusResult
			replicaStatuses := make(map[string]*shardsetup.PoolerStatusResult)
			for i, r := range statuses {
				if r.Err != nil || r.Status == nil {
					continue
				}
				if r.Status.IsInitialized && r.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
					primaryStatus = &statuses[i]
				} else if r.Status.IsInitialized && r.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA && r.Status.PostgresRunning {
					replicaStatuses[r.Name] = &statuses[i]
				}
			}
			if primaryStatus == nil {
				return "", false, "no primary elected yet"
			}
			if len(replicaStatuses) != expectedStandbyCount {
				return "", false, fmt.Sprintf("%d/%d standbys initialized", len(replicaStatuses), expectedStandbyCount)
			}
			// Build lookup sets from the primary's sync config.
			syncStandbyNames := make(map[string]bool) // short name (e.g. "pooler-2") -> true
			syncAppNames := make(map[string]bool)     // application name (e.g. "test-cell_pooler-2") -> true
			if syncConfig := primaryStatus.Status.GetPrimaryStatus().GetSyncReplicationConfig(); syncConfig != nil {
				for _, id := range syncConfig.StandbyIds {
					syncStandbyNames[id.Name] = true
				}
				for _, appName := range syncConfig.StandbyApplicationNames {
					syncAppNames[appName] = true
				}
			}
			for name, r := range replicaStatuses {
				if !syncStandbyNames[name] {
					return "", false, fmt.Sprintf("replica %s not yet in primary sync config", name)
				}
				connInfo := r.Status.GetReplicationStatus().GetPrimaryConnInfo()
				if connInfo == nil {
					return "", false, fmt.Sprintf("replica %s primary_conn_info not yet configured", name)
				}
				if !syncAppNames[connInfo.ApplicationName] {
					return "", false, fmt.Sprintf("replica %s primary_conn_info application_name %q not in primary sync config", name, connInfo.ApplicationName)
				}
			}
			return primaryStatus.Name, true, ""
		},
		"shard did not become ready within %v", timeout,
	)
	t.Logf("Shard ready: primary=%s, %d standbys initialized with sync replication", primaryName, expectedStandbyCount)
	return primaryName
}
