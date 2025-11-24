// Copyright 2019 The Vitess Authors.
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

package endtoend

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/etcdtopo"
	"github.com/multigres/multigres/go/clustermetadata/topo/test"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/stretchr/testify/require"
)

// Use the global port allocator for consistent port allocation across all tests

var leaseTTL int

func TestEtcd2Topo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping topology etcd integration test in short mode")
	}
	// Start a single etcd in the background.
	clientAddr, _ := etcdtopo.StartEtcd(t)

	testIndex := 0
	newServer := func() topo.Store {
		// Each test will use its own subdirectories.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("etcd2", path.Join(testRoot, topo.GlobalCell), []string{clientAddr})
		require.NoError(t, err, "OpenServer() failed")

		// Create the CellInfo.
		err = ts.CreateCell(context.Background(), test.LocalCellName, &clustermetadatapb.Cell{
			ServerAddresses: []string{clientAddr},
			Root:            path.Join(testRoot, test.LocalCellName),
		})
		require.NoError(t, err, "CreateCellInfo() failed")

		return ts
	}

	// Run the TopoServerTestSuite tests.
	ctx := t.Context()
	test.TopoServerTestSuite(t, ctx, func() topo.Store {
		return newServer()
	})

	// Run etcd-specific tests.
	ts := newServer()
	testDatabaseLock(t, ts)
	ts.Close()
}

// testDatabaseLock tests etcd-specific heartbeat (TTL).
// Note TTL granularity is in seconds, even though the API uses time.Duration.
// So we have to wait a long time in these tests.
func testDatabaseLock(t *testing.T, ts topo.Store) {
	ctx := context.Background()
	databasePath := path.Join(topo.DatabasesPath, "test_database")
	err := ts.CreateDatabase(ctx, "test_database", &clustermetadatapb.Database{})
	require.NoError(t, err, "CreateKeyspace")

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err, "ConnForCell failed")

	// Long TTL, unlock before lease runs out.
	leaseTTL = 1000
	lockDescriptor, err := conn.Lock(ctx, databasePath, "ttl")
	require.NoError(t, err, "Lock failed")
	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock failed")

	// Short TTL, make sure it doesn't expire.
	leaseTTL = 1
	lockDescriptor, err = conn.Lock(ctx, databasePath, "short ttl")
	require.NoError(t, err, "Lock failed")
	time.Sleep(2 * time.Second)
	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock failed")
}
