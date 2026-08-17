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
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

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
	clientAddr, _, _ := etcdtopo.StartEtcd(t)

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
	testEphemeralLeaseRenewal(t, ts, clientAddr)
	ts.Close()

	// The expiry test closes its server itself, so it gets its own.
	testEphemeralExpiresOnClose(t, newServer(), clientAddr)
}

// testEphemeralLeaseRenewal verifies that ephemeral files carry a lease, that
// files sharing a connection share it, and that after the lease ends the
// connection grants a fresh one on the next write rather than failing
// forever. Re-writing after a lease loss is the owner's job (toporeg's
// re-assertion loop); this covers the half the connection owns.
func testEphemeralLeaseRenewal(t *testing.T, ts topoclient.Store, clientAddr string) {
	restore := etcdtopo.SetLeaseTTLForTest(2)
	defer restore()

	ctx := context.Background()
	conn, err := ts.ConnForCell(ctx, topoclient.GlobalCell)
	require.NoError(t, err, "ConnForCell failed")

	require.NoError(t, conn.PutEphemeral(ctx, "ephemeral/gw-a", []byte("a")))
	require.NoError(t, conn.PutEphemeral(ctx, "ephemeral/gw-b", []byte("b")))

	// Raw client to inspect and revoke the lease out-of-band.
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{clientAddr}, DialTimeout: 5 * time.Second})
	require.NoError(t, err, "raw etcd client failed")
	defer cli.Close()

	fullKeyA, leaseID := findEphemeralKey(t, cli, "ephemeral/gw-a")
	require.NotZero(t, leaseID, "ephemeral file must carry a lease")
	_, leaseB := findEphemeralKey(t, cli, "ephemeral/gw-b")
	require.Equal(t, leaseID, leaseB, "files on one connection should share its lease")

	// Revoking out-of-band is what an expiry after a long outage looks like:
	// etcd deletes every key bound to the lease.
	_, err = cli.Revoke(ctx, clientv3.LeaseID(leaseID))
	require.NoError(t, err, "lease revoke failed")
	require.Eventually(t, func() bool {
		resp, err := cli.Get(ctx, fullKeyA)
		return err == nil && len(resp.Kvs) == 0
	}, 10*time.Second, 100*time.Millisecond, "revoking the lease should delete its files")

	// The owner re-asserting: the write must succeed on a fresh lease
	// instead of failing against the dead one.
	require.Eventually(t, func() bool {
		return conn.PutEphemeral(ctx, "ephemeral/gw-a", []byte("a")) == nil
	}, 10*time.Second, 100*time.Millisecond, "write after lease loss should grant a fresh lease")

	resp, err := cli.Get(ctx, fullKeyA)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.NotZero(t, resp.Kvs[0].Lease, "re-written file must carry a lease")
	require.NotEqual(t, leaseID, resp.Kvs[0].Lease, "re-written file must be on a fresh lease")
}

// testEphemeralExpiresOnClose verifies the core liveness promise: when the
// process stops renewing (here: the store is closed), etcd deletes the
// ephemeral files on its own within one TTL. This is what prevents a
// SIGKILLed component from leaking a permanent registration.
func testEphemeralExpiresOnClose(t *testing.T, ts topoclient.Store, clientAddr string) {
	restore := etcdtopo.SetLeaseTTLForTest(2)
	defer restore()

	ctx := context.Background()
	conn, err := ts.ConnForCell(ctx, topoclient.GlobalCell)
	require.NoError(t, err, "ConnForCell failed")
	require.NoError(t, conn.PutEphemeral(ctx, "ephemeral/gw-dead", []byte("dead")))

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{clientAddr}, DialTimeout: 5 * time.Second})
	require.NoError(t, err, "raw etcd client failed")
	defer cli.Close()

	fullKey, leaseID := findEphemeralKey(t, cli, "ephemeral/gw-dead")
	require.NotZero(t, leaseID, "ephemeral file must carry a lease")

	// "Kill" the process: renewals stop with the store.
	require.NoError(t, ts.Close())

	require.Eventually(t, func() bool {
		resp, err := cli.Get(ctx, fullKey)
		return err == nil && len(resp.Kvs) == 0
	}, 15*time.Second, 200*time.Millisecond, "ephemeral file should expire after its owner stops renewing")
}

// findEphemeralKey locates the full etcd key ending in suffix and returns it
// with its lease ID. The full key depends on the per-test root, so it is
// discovered by scanning rather than computed.
func findEphemeralKey(t *testing.T, cli *clientv3.Client, suffix string) (string, int64) {
	t.Helper()
	resp, err := cli.Get(context.Background(), "/", clientv3.WithPrefix())
	require.NoError(t, err, "raw etcd scan failed")
	for _, kv := range resp.Kvs {
		if strings.HasSuffix(string(kv.Key), suffix) {
			return string(kv.Key), kv.Lease
		}
	}
	t.Fatalf("no etcd key found with suffix %q", suffix)
	return "", 0
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
