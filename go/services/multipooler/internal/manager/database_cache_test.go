// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const databaseRecordPath = "databases/testdb/Database"

// loadDatabaseRecord runs the manager's database-record load and returns the
// state it settles in.
func loadDatabaseRecord(t *testing.T, ts topoclient.Store, poolerDir string, staticLeader bool) ManagerState {
	t.Helper()
	multipooler := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-service"},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
		PoolerDir: poolerDir,
	}
	pm, err := NewMultipoolerManagerWithTimeout(slog.New(slog.DiscardHandler), multipooler,
		&Config{TopoClient: ts, StaticLeader: staticLeader}, time.Second)
	require.NoError(t, err)
	defer pm.ShutdownForTest(t.Context())

	go pm.loadShardConfigFromGlobalTopo()
	require.Eventually(t, func() bool {
		state := pm.GetState()
		return state == ManagerStateReady || state == ManagerStateError
	}, 5*time.Second, 50*time.Millisecond)
	return pm.GetState()
}

func TestStaticLeader_StartsFromItsCopyOfTheDatabaseRecord(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()
	addDatabaseToTopo(t, ts, "testdb")
	poolerDir := filepath.Join(t.TempDir(), "pooler")

	require.Equal(t, ManagerStateReady, loadDatabaseRecord(t, ts, poolerDir, true))
	require.FileExists(t, filepath.Join(poolerDir, databaseCacheFile), "the record read from the topology is kept")

	// The topology becomes unreachable and the pgbackrest conf is gone: the
	// restarted static leader is ready from its copy and rewrites the conf.
	factory.AddOperationError(memorytopo.Get, databaseRecordPath, assert.AnError)
	require.NoError(t, os.RemoveAll(filepath.Join(poolerDir, "pgbackrest")))
	assert.Equal(t, ManagerStateReady, loadDatabaseRecord(t, ts, poolerDir, true))
	assert.FileExists(t, filepath.Join(poolerDir, "pgbackrest", "pgbackrest.conf"))
}

func TestStaticLeader_DoesNotServeADeletedDatabaseFromItsCopy(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()
	addDatabaseToTopo(t, ts, "testdb")
	poolerDir := filepath.Join(t.TempDir(), "pooler")
	require.Equal(t, ManagerStateReady, loadDatabaseRecord(t, ts, poolerDir, true))
	require.FileExists(t, filepath.Join(poolerDir, databaseCacheFile))

	// The topology is reachable and answers that the database does not exist.
	factory.AddOperationError(memorytopo.Get, databaseRecordPath, topoclient.NewError(topoclient.NoNode, databaseRecordPath))
	assert.Equal(t, ManagerStateError, loadDatabaseRecord(t, ts, poolerDir, true), "times out instead of using the copy")
}

func TestCoordinatedPooler_NeedsTheTopologyForTheDatabaseRecord(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()
	addDatabaseToTopo(t, ts, "testdb")
	poolerDir := filepath.Join(t.TempDir(), "pooler")

	require.Equal(t, ManagerStateReady, loadDatabaseRecord(t, ts, poolerDir, false))
	assert.NoFileExists(t, filepath.Join(poolerDir, databaseCacheFile), "only a static leader keeps a copy")

	factory.AddOperationError(memorytopo.Get, databaseRecordPath, assert.AnError)
	assert.Equal(t, ManagerStateError, loadDatabaseRecord(t, ts, poolerDir, false), "times out without the topology")
}
