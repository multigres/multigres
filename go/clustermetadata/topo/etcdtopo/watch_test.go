// Copyright 2024 The Vitess Authors.
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

package etcdtopo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/pathutil"
)

// TestMain sets the path before running tests
func TestMain(m *testing.M) {
	// Set the PATH so etcd can be found
	pathutil.PrependPath("../../../../bin")

	os.Exit(m.Run())
}

// TestWatchTopoVersion tests how the topo.Version values work within the etcd2topo
// Watch implementation. Today, those logical versions are based on the key's
// ModRevision value, which is a monotonically increasing int64 value. See
// https://github.com/vitessio/vitess/pull/15847 for additional details and the
// current reasoning behing using ModRevision. This can be changed in the future
// but should be done so intentionally, thus this test ensures we don't change the
// behavior accidentally/uinintentionally.
func TestWatchTopoVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration tests in short mode")
	}
	ctx := utils.LeakCheckContext(t)
	etcdServerAddr, _ := StartEtcd(t, 0)
	root := "/vitess/test"
	name := "testkey"
	path := path.Join(root, name)
	value := "testval"
	// We use these two variables to ensure that we receive all of the changes in
	// our watch.
	changesMade := atomic.Int64{} // This is accessed across goroutines
	changesSeen := int64(0)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdServerAddr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	serverRunningCh := make(chan struct{})
	server := &Server{
		cli:     client,
		root:    root,
		running: serverRunningCh,
	}
	defer server.Close()

	// Create the key as the vitess topo server requires that it exist before you
	// can watch it (the lower level etcd watch does not require this).
	_, err = client.Put(ctx, path, fmt.Sprintf("%s-%d", value, changesMade.Load()))
	require.NoError(t, err)
	changesMade.Add(1)

	var data <-chan *topo.WatchData
	_, data, err = server.Watch(ctx, name)
	require.NoError(t, err, "Server.Watch() error = %v", err)

	// Coordinate between the goroutines on the delete so that we don't miss
	// N changes when restarting the watch.
	token := make(chan struct{})
	defer close(token)

	// Run a goroutine that updates the key we're watching.
	go func() {
		cur := changesMade.Load() + 1
		batchSize := int64(10)
		for i := cur; i <= cur+batchSize; i++ {
			_, err := client.Put(ctx, path, fmt.Sprintf("%s-%d", value, i))
			require.NoError(t, err)
			changesMade.Add(1)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		// Delete the key to ensure that our version continues to be monotonically
		// increasing.
		_, err := client.Delete(ctx, path)
		require.NoError(t, err)
		changesMade.Add(1)
		// Let the main goroutine process the delete and restart the watch before
		// we make more changes.
		token <- struct{}{}
		cur = changesMade.Load() + 1
		for i := cur; i <= cur+batchSize; i++ {
			_, err := client.Put(ctx, path, fmt.Sprintf("%s-%d", value, i))
			require.NoError(t, err)
			changesMade.Add(1)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	// When using ModRevision as the logical key version, the Revision is initially
	// 1 as we're at the first change of the keyspace (it has been created). This
	// means that the first time we receive a change in the watch, we should expect
	// the key's topo.Version to be 2 as it's the second change to the keyspace.
	// We start with 1 as we increment this every time we receive a change in the
	// watch.
	expectedVersion := int64(1)

	// Consider the test done when we've been watching the key for 10 seconds. We
	// should receive all of the changes made within 1 second but we allow for a lot
	// of extra time to prevent flakiness when the host is very slow for any reason.
	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		select {
		case <-watchCtx.Done():
			require.Equal(t, changesMade.Load(), changesSeen, "expected %d changes, got %d", changesMade.Load(), changesSeen)
			return // Success, we're done
		case <-ctx.Done():
			require.FailNow(t, "test context cancelled")
		case <-serverRunningCh:
			require.FailNow(t, "topo server is no longer running")
		case wd := <-data:
			changesSeen++
			expectedVersion++
			if wd.Err != nil {
				if errors.Is(wd.Err, &topo.TopoError{Code: topo.NoNode}) {
					// This was our delete. We'll restart the watch.
					// Note that the lower level etcd watch doesn't treat delete as
					// any special kind of change/event, it's another change to the
					// key, but our topo server Watch treats this as an implicit end
					// of the watch and it terminates it.
					// We create the key again as the vitess topo server requires
					// that it exist before watching it.
					_, err = client.Put(ctx, path, fmt.Sprintf("%s-%d", value, changesMade.Load()))
					require.NoError(t, err)
					changesMade.Add(1)
					_, data, err = server.Watch(ctx, name)
					require.NoError(t, err, "Server.Watch() error = %v", err)
					<-token // Tell the goroutine making changes to continue
					continue
				}
				require.FailNow(t, "unexpected error in watch data", "error: %v", wd.Err)
			}
			gotVersion := int64(wd.Version.(EtcdVersion))
			require.Equal(t, expectedVersion, gotVersion, "expected version %d, got %d", expectedVersion, gotVersion)
		}
	}
}

// TestWatchRecursiveReconnection tests that WatchRecursive automatically reconnects
// when the etcd connection is lost and continues to receive updates on the same watch.
func TestWatchRecursiveReconnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration tests in short mode")
	}
	ctx := utils.LeakCheckContext(t)

	// Use a persistent data directory so etcd maintains state across restarts
	dataDir := t.TempDir()
	port := utils.GetNextEtcd2Port()

	// Start first etcd server with persistent data
	etcdServerAddr, etcdServer := StartEtcdWithOptions(t, EtcdOptions{
		Port:    port,
		DataDir: dataDir,
	})
	root := "/vitess/test"

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdServerAddr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	serverRunningCh := make(chan struct{})
	server := &Server{
		cli:     client,
		root:    root,
		running: serverRunningCh,
	}
	defer server.Close()

	// Create initial pooler data
	poolerPath := path.Join(root, "poolers/pooler1/Pooler")
	_, err = client.Put(ctx, poolerPath, "initial-value")
	require.NoError(t, err)

	// Start watching
	initial, changes, err := server.WatchRecursive(ctx, "poolers")
	require.NoError(t, err)
	require.Len(t, initial, 1)
	require.Equal(t, []byte("initial-value"), initial[0].Contents)

	// Helper to receive an update with expected value, tracking all updates received
	var allUpdates []string
	receiveUpdate := func(expectedValue string, timeout time.Duration) bool {
		deadline := time.After(timeout)
		for {
			select {
			case wd := <-changes:
				require.NoError(t, wd.Err)
				value := string(wd.Contents)
				allUpdates = append(allUpdates, value)
				if value == expectedValue {
					return true
				}
			case <-deadline:
				return false
			}
		}
	}

	// Update the value and wait for watch to receive it
	_, err = client.Put(ctx, poolerPath, "before-disconnect")
	require.NoError(t, err)
	require.True(t, receiveUpdate("before-disconnect", 5*time.Second))

	// Simulate etcd crash
	require.NoError(t, etcdServer.Process.Kill())
	_ = etcdServer.Wait() // Ignore error - process was killed

	// Restart etcd with SAME data directory (simulates cluster recovery)
	_, newEtcdServer := StartEtcdWithOptions(t, EtcdOptions{
		Port:    port,
		DataDir: dataDir,
	})
	_ = newEtcdServer // Will be cleaned up by t.Cleanup

	// Track updates received after reconnection
	updatesAfterReconnect := len(allUpdates)

	// Make a change after reconnection - use Eventually to wait for etcd to be ready
	require.Eventually(t, func() bool {
		_, err := client.Put(ctx, poolerPath, "after-reconnect")
		return err == nil
	}, 10*time.Second, 100*time.Millisecond, "etcd should become available after restart")
	require.True(t, receiveUpdate("after-reconnect", 30*time.Second),
		"watch should reconnect and receive update")

	// Verify no duplicates after reconnection: we should only see "after-reconnect"
	newUpdates := allUpdates[updatesAfterReconnect:]
	require.Equal(t, []string{"after-reconnect"}, newUpdates,
		"should not receive duplicate updates after reconnection")

	// Verify watch continues to work
	_, err = client.Put(ctx, poolerPath, "continued-watching")
	require.NoError(t, err)
	require.True(t, receiveUpdate("continued-watching", 5*time.Second))
}

// TestWatchRecursiveCompaction tests what happens when etcd compacts revisions
// that a watch depends on. When a watch's revision is compacted, etcd cancels the watch.
// In production, this is rare because etcd keeps hours of history and watches process
// events quickly. This could occur if a watch is very slow or compaction is aggressive.
func TestWatchRecursiveCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration tests in short mode")
	}
	ctx := utils.LeakCheckContext(t)

	etcdServerAddr, _ := StartEtcd(t, 0)
	root := "/vitess/test"

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdServerAddr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	serverRunningCh := make(chan struct{})
	server := &Server{
		cli:     client,
		root:    root,
		running: serverRunningCh,
	}
	defer server.Close()

	// Create initial data and start watching
	poolerPath := path.Join(root, "poolers/pooler1/Pooler")
	_, err = client.Put(ctx, poolerPath, "initial-value")
	require.NoError(t, err)

	initial, changes, err := server.WatchRecursive(ctx, "poolers")
	require.NoError(t, err)
	require.Len(t, initial, 1)

	// Make several updates to advance the revision
	var currentRevision int64
	for i := 1; i <= 5; i++ {
		putResp, err := client.Put(ctx, poolerPath, fmt.Sprintf("value-%d", i))
		require.NoError(t, err)
		currentRevision = putResp.Header.Revision
	}

	// Compact history aggressively (removes revisions the watch may depend on)
	_, err = client.Compact(ctx, currentRevision-1)
	require.NoError(t, err)

	// The watch should receive a compaction error and then close the channel
	select {
	case wd, ok := <-changes:
		if !ok {
			t.Fatal("watch channel closed without sending error")
		}
		if wd.Err != nil {
			require.Contains(t, wd.Err.Error(), "required revision has been compacted")
			// Verify the channel is closed after the error
			_, stillOpen := <-changes
			require.False(t, stillOpen, "watch channel should be closed after compaction error")
			return // Expected behavior
		}
		// Watch may have caught up before compaction - also acceptable
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch response after compaction")
	}
}
