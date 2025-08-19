// Copyright 2025 The Multigres Authors.
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

package test

import (
	"context"
	"errors"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// checkTryLock checks if we can lock / unlock as expected. It's using a keyspace
// as the lock target.
func checkTryLock(t *testing.T, ctx context.Context, ts topo.Store) {
	if err := ts.CreateCellLocation(ctx, "test_cell", &clustermetadatapb.CellLocation{}); err != nil {
		require.Fail(t, "CreateCellLocation fail", err.Error())
	}

	conn, err := ts.ConnForCell(context.Background(), topo.GlobalTopo)
	if err != nil {
		require.Fail(t, "ConnForCell(global) failed", err.Error())
	}

	t.Log("===      checkTryLockTimeout")
	checkTryLockTimeout(ctx, t, conn)

	t.Log("===      checkTryLockUnblocks")
	checkTryLockUnblocks(ctx, t, conn)
}

// checkTryLockTimeout test the fail-fast nature of TryLock
func checkTryLockTimeout(ctx context.Context, t *testing.T, conn topo.Conn) {
	cellLocationPath := path.Join(topo.CellsPath, "test_cell")
	lockDescriptor, err := conn.TryLock(ctx, cellLocationPath, "")
	if err != nil {
		require.Fail(t, "TryLock failed", err.Error())
	}

	// We have the lock, list the cell location directory.
	// It should not contain anything, except Ephemeral files.
	entries, err := conn.ListDir(ctx, cellLocationPath, true /*full*/)
	if err != nil {
		require.Fail(t, "ListDir failed: %v", err.Error())
	}
	for _, e := range entries {
		if e.Name == "Database" {
			continue
		}
		if e.Ephemeral {
			t.Logf("skipping ephemeral node %v in %v", e, cellLocationPath)
			continue
		}
		// Non-ephemeral entries better have only ephemeral children.
		p := path.Join(cellLocationPath, e.Name)
		entries, err := conn.ListDir(ctx, p, true /*full*/)
		if err != nil {
			require.Fail(t, "ListDir failed", err.Error())
		}
		for _, e := range entries {
			if e.Ephemeral {
				t.Logf("skipping ephemeral node %v in %v", e, p)
			} else {
				require.Fail(t, "non-ephemeral DirEntry")
			}
		}
	}

	// We should not be able to take the lock again. It should throw `NodeExists` error.
	fastCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if _, err := conn.TryLock(fastCtx, cellLocationPath, "again"); !errors.Is(err, &topo.TopoError{Code: topo.NodeExists}) {
		require.Fail(t, "TryLock failed", err.Error())
	}
	cancel()

	// test we can interrupt taking the lock
	interruptCtx, cancel := context.WithCancel(ctx)
	finished := make(chan struct{})

	// go routine to cancel the context.
	go func() {
		<-finished
		cancel()
	}()

	waitUntil := time.Now().Add(10 * time.Second)
	var firstTime = true
	// after attempting the `TryLock` and getting an error `NodeExists`, we will cancel the context deliberately
	// and expect `context canceled` error in next iteration of `for` loop.
	for {
		if time.Now().After(waitUntil) {
			t.Fatalf("Unlock(test_database) timed out")
		}
		// we expect context to fail with `context canceled` error
		if interruptCtx.Err() != nil {
			require.ErrorContains(t, interruptCtx.Err(), "context canceled")
			break
		}
		if _, err := conn.TryLock(interruptCtx, cellLocationPath, "interrupted"); !errors.Is(err, &topo.TopoError{Code: topo.NodeExists}) {
			require.Fail(t, "TryLock failed", err.Error())
		}
		if firstTime {
			close(finished)
			firstTime = false
		}
		time.Sleep(1 * time.Second)
	}

	if err := lockDescriptor.Check(ctx); err != nil {
		t.Errorf("Check(): %v", err)
	}

	if err := lockDescriptor.Unlock(ctx); err != nil {
		require.Fail(t, "Unlock failed", err.Error())
	}

	// test we can't unlock again
	if err := lockDescriptor.Unlock(ctx); err == nil {
		require.Fail(t, "Unlock succeeded but should not have")
	}
}

// unlike 'checkLockUnblocks', checkTryLockUnblocks will not block on other client but instead
// keep retrying until it gets the lock.
func checkTryLockUnblocks(ctx context.Context, t *testing.T, conn topo.Conn) {
	cellLocationPath := path.Join(topo.CellsPath, "test_cell")
	unblock := make(chan struct{})
	finished := make(chan struct{})

	duration := 10 * time.Second
	waitUntil := time.Now().Add(duration)
	// TryLock will keep getting NodeExists until lockDescriptor2 unlock itself.
	// It will not wait but immediately return with NodeExists error.
	go func() {
		<-unblock
		for time.Now().Before(waitUntil) {
			lockDescriptor, err := conn.TryLock(ctx, cellLocationPath, "unblocks")
			if err != nil {
				if !errors.Is(err, &topo.TopoError{Code: topo.NodeExists}) {
					require.Fail(t, "expected node exists during trylock", err.Error())
				}
				time.Sleep(1 * time.Second)
			} else {
				if err = lockDescriptor.Unlock(ctx); err != nil {
					require.Fail(t, "Unlock(test_database) failed", err.Error())
				}
				close(finished)
				break
			}
		}
	}()

	// Lock the keyspace.
	lockDescriptor2, err := conn.TryLock(ctx, cellLocationPath, "")
	if err != nil {
		require.Fail(t, "Lock(test_database) failed", err.Error())
	}

	// unblock the go routine so it starts waiting
	close(unblock)

	if err = lockDescriptor2.Unlock(ctx); err != nil {
		require.Fail(t, "Unlock(test_database) failed", err.Error())
	}

	timeout := time.After(2 * duration)
	select {
	case <-finished:
	case <-timeout:
		require.Fail(t, "Unlock(test_database) timed out")
	}
}
