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

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
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

// waitForReplicationBroken polls until the instance's primary_conninfo host is empty,
// indicating replication is no longer configured or streaming.
func waitForReplicationBroken(t *testing.T, inst *shardsetup.MultipoolerInstance, timeout time.Duration) {
	t.Helper()
	shardsetup.EventuallyPoolerCondition(t,
		[]*shardsetup.MultipoolerInstance{inst},
		timeout, 500*time.Millisecond,
		func(_ string, s *multipoolermanagerdatapb.Status) (bool, string) {
			if s.GetReplicationStatus().GetPrimaryConnInfo().GetHost() != "" {
				return false, "primary_conninfo host still set: " + s.GetReplicationStatus().GetPrimaryConnInfo().GetHost()
			}
			return true, ""
		},
		"replication should be broken (primary_conninfo cleared) within %v", timeout,
	)
}

// assertNoShardProblemsForPooler polls multiorch.GetShardStatus throughout a
// hold window and fails the test if any DetectedProblem references poolerID.
// Use this to prove multiorch did NOT act on a pooler (e.g., the draining-replica
// analyzer skip, where absence-of-problem is the thing being asserted).
func assertNoShardProblemsForPooler(
	t *testing.T,
	moClient *shardsetup.MultiOrchClient,
	poolerID *clustermetadatapb.ID,
	database, tableGroup, shard string,
	hold time.Duration,
	pollInterval time.Duration,
) {
	t.Helper()

	poolerIDStr := topoclient.MultiPoolerIDString(poolerID)
	deadline := time.Now().Add(hold)
	ticks := 0
	for time.Now().Before(deadline) {
		ticks++
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := moClient.GetShardStatus(ctx, &multiorchpb.ShardStatusRequest{
			Database:   database,
			TableGroup: tableGroup,
			Shard:      shard,
		})
		cancel()
		require.NoError(t, err, "GetShardStatus failed at tick %d", ticks)
		for _, p := range resp.Problems {
			if p.PoolerId != nil && topoclient.MultiPoolerIDString(p.PoolerId) == poolerIDStr {
				t.Fatalf("unexpected problem for pooler %s at tick %d: code=%s description=%s",
					poolerIDStr, ticks, p.Code, p.Description)
			}
		}
		time.Sleep(pollInterval)
	}
	t.Logf("assertNoShardProblemsForPooler: %s remained problem-free over %d ticks (%v)", poolerIDStr, ticks, hold)
}

// assertNoNewPrimaryElected polls every pooler's Status RPC over a hold window
// and fails if any pooler other than allowedPrimary reports PoolerType=PRIMARY.
// Use when proving that multiorch did NOT elect a new primary (e.g., the
// degraded-cluster test where no eligible candidate exists). The allowedPrimary
// is typically the old primary whose topology record cannot be cleared without
// a successor.
func assertNoNewPrimaryElected(
	t *testing.T,
	setup *shardsetup.ShardSetup,
	allowedPrimary string,
	hold time.Duration,
	pollInterval time.Duration,
) {
	t.Helper()

	poolers := make([]*shardsetup.MultipoolerInstance, 0, len(setup.Multipoolers))
	for _, inst := range setup.Multipoolers {
		poolers = append(poolers, inst)
	}

	deadline := time.Now().Add(hold)
	ticks := 0
	for time.Now().Before(deadline) {
		ticks++
		statuses := shardsetup.FetchPoolerStatuses(t, poolers)
		for _, r := range statuses {
			if r.Err != nil || r.Status == nil || r.Name == allowedPrimary {
				continue
			}
			if r.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				t.Fatalf("unexpected promotion at tick %d: pooler %s reports PoolerType=PRIMARY",
					ticks, r.Name)
			}
		}
		time.Sleep(pollInterval)
	}
	t.Logf("assertNoNewPrimaryElected: no pooler other than %s became PRIMARY over %d ticks (%v)",
		allowedPrimary, ticks, hold)
}

// waitForNewPrimary waits for a new primary (different from oldPrimaryName) to be elected.
func waitForNewPrimary(t *testing.T, setup *shardsetup.ShardSetup, oldPrimaryName string, timeout time.Duration) string {
	t.Helper()

	var poolers []*shardsetup.MultipoolerInstance
	for _, inst := range setup.Multipoolers {
		poolers = append(poolers, inst)
	}

	return shardsetup.EventuallyPoolersCondition(t, poolers, timeout, 2*time.Second,
		func(statuses []shardsetup.PoolerStatusResult) (string, bool, string) {
			for _, r := range statuses {
				if r.Name == oldPrimaryName || r.Err != nil || r.Status == nil {
					continue
				}
				if r.Status.IsInitialized && r.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
					return r.Name, true, ""
				}
			}
			return "", false, fmt.Sprintf("no new primary elected yet (old primary: %s)", oldPrimaryName)
		},
		"new primary not elected within %v", timeout,
	)
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
				} else if r.Status.IsInitialized && r.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA && r.Status.PostgresReady {
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
