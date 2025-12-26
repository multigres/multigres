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

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/etcdtopo"
	"github.com/multigres/multigres/go/common/topoclient/test"
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
	newServer := func() topoclient.Store {
		// Each test will use its own subdirectories.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topoclient.OpenServer(topoclient.DefaultTopoImplementation, path.Join(testRoot, topoclient.GlobalCell), []string{clientAddr}, topoclient.NewDefaultTopoConfig())
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
	test.TopoServerTestSuite(t, ctx, func() topoclient.Store {
		return newServer()
	})

	// Run etcd-specific tests.
	ts := newServer()
	testDatabaseLock(t, ts)
	testLockNameWithTTL(t, ts)
	testTryLockName(t, ts)
	ts.Close()
}

// testDatabaseLock tests etcd-specific heartbeat (TTL).
// Note TTL granularity is in seconds, even though the API uses time.Duration.
// So we have to wait a long time in these tests.
func testDatabaseLock(t *testing.T, ts topoclient.Store) {
	ctx := context.Background()
	databasePath := path.Join(topoclient.DatabasesPath, "test_database")
	err := ts.CreateDatabase(ctx, "test_database", &clustermetadatapb.Database{})
	require.NoError(t, err, "CreateKeyspace")

	conn, err := ts.ConnForCell(ctx, topoclient.GlobalCell)
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

// testLockNameWithTTL tests etcd-specific behavior of LockNameWithTTL.
func testLockNameWithTTL(t *testing.T, ts topoclient.Store) {
	ctx := context.Background()
	conn, err := ts.ConnForCell(ctx, topoclient.GlobalCell)
	require.NoError(t, err, "ConnForCell failed")

	// LockNameWithTTL should work on a non-existent path
	lockPath := "test_lock_name_with_ttl"
	customTTL := 1 * time.Hour
	lockDescriptor, err := conn.LockNameWithTTL(ctx, lockPath, "test", customTTL)
	require.NoError(t, err, "LockNameWithTTL failed")

	// Should not be able to acquire the same lock again
	ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = conn.LockNameWithTTL(ctx2, lockPath, "again", customTTL)
	require.Error(t, err, "LockNameWithTTL should fail when lock is held")

	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock failed")

	// After unlock, should be able to acquire again
	lockDescriptor, err = conn.LockNameWithTTL(ctx, lockPath, "reacquire", customTTL)
	require.NoError(t, err, "LockNameWithTTL should succeed after unlock")
	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock failed")
}

// testTryLockName tests etcd-specific behavior of TryLockName.
func testTryLockName(t *testing.T, ts topoclient.Store) {
	ctx := context.Background()
	conn, err := ts.ConnForCell(ctx, topoclient.GlobalCell)
	require.NoError(t, err, "ConnForCell failed")

	// TryLockName should work on a non-existent path
	lockPath := "test_try_lock_name"
	lockDescriptor, err := conn.TryLockName(ctx, lockPath, "test")
	require.NoError(t, err, "TryLockName failed")

	// TryLockName should fail fast when lock is held (not block)
	start := time.Now()
	_, err = conn.TryLockName(ctx, lockPath, "again")
	elapsed := time.Since(start)
	require.Error(t, err, "TryLockName should fail when lock is held")
	require.Less(t, elapsed, 100*time.Millisecond, "TryLockName should fail fast without blocking")

	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock failed")

	// After unlock, should be able to acquire again
	lockDescriptor, err = conn.TryLockName(ctx, lockPath, "reacquire")
	require.NoError(t, err, "TryLockName should succeed after unlock")
	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock failed")
}
