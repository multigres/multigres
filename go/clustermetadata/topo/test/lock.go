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

	"github.com/multigres/multigres/go/clustermetadata/topo"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// timeUntilLockIsTaken is the time to wait until a lock is taken.
// We haven't found a better simpler way to guarantee a routine is stuck
// waiting for a topo lock than sleeping that amount.
var timeUntilLockIsTaken = 10 * time.Millisecond

// checkLock checks we can lock / unlock as expected. It's using a database
// as the lock target.
func checkLock(t *testing.T, ctx context.Context, ts topo.Store) {
	if err := ts.CreateDatabase(ctx, "test_database", &clustermetadatapb.Database{}); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}

	conn, err := ts.ConnForCell(context.Background(), topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell(global) failed: %v", err)
	}

	t.Log("===      checkLockTimeout")
	checkLockTimeout(ctx, t, conn)

	t.Log("===      checkLockUnblocks")
	checkLockUnblocks(ctx, t, conn)
}

func checkLockTimeout(ctx context.Context, t *testing.T, conn topo.Conn) {
	databasePath := path.Join(topo.DatabasesPath, "test_database")
	lockDescriptor, err := conn.Lock(ctx, databasePath, "")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// We have the lock, list the database directory.
	// It should not contain anything, except Ephemeral files.
	entries, err := conn.ListDir(ctx, databasePath, true /*full*/)
	if err != nil {
		t.Fatalf("Listdir(%v) failed: %v", databasePath, err)
	}
	for _, e := range entries {
		if e.Name == "Database" {
			continue
		}
		if e.Ephemeral {
			t.Logf("skipping ephemeral node %v in %v", e, databasePath)
			continue
		}
		// Non-ephemeral entries better have only ephemeral children.
		p := path.Join(databasePath, e.Name)
		entries, err := conn.ListDir(ctx, p, true /*full*/)
		if err != nil {
			t.Fatalf("Listdir(%v) failed: %v", p, err)
		}
		for _, e := range entries {
			if e.Ephemeral {
				t.Logf("skipping ephemeral node %v in %v", e, p)
			} else {
				t.Errorf("Entry in %v has non-ephemeral DirEntry: %v", p, e)
			}
		}
	}

	// test we can't take the lock again
	fastCtx, cancel := context.WithTimeout(ctx, timeUntilLockIsTaken)
	if _, err := conn.Lock(fastCtx, databasePath, "again"); !errors.Is(err, &topo.TopoError{Code: topo.Timeout}) {
		t.Fatalf("Lock(again): %v", err)
	}
	cancel()

	// test we can interrupt taking the lock
	interruptCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(timeUntilLockIsTaken)
		cancel()
	}()
	if _, err := conn.Lock(interruptCtx, databasePath, "interrupted"); !errors.Is(err, &topo.TopoError{Code: topo.Interrupted}) {
		t.Fatalf("Lock(interrupted): %v", err)
	}

	if err := lockDescriptor.Check(ctx); err != nil {
		t.Errorf("Check(): %v", err)
	}

	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock(): %v", err)
	}

	// test we can't unlock again
	if err := lockDescriptor.Unlock(ctx); err == nil {
		t.Fatalf("Unlock(again) worked")
	}
}

// checkLockUnblocks makes sure that a routine waiting on a lock
// is unblocked when another routine frees the lock
func checkLockUnblocks(ctx context.Context, t *testing.T, conn topo.Conn) {
	databasePath := path.Join(topo.DatabasesPath, "test_database")
	unblock := make(chan struct{})
	finished := make(chan struct{})

	// As soon as we're unblocked, we try to lock the database.
	go func() {
		<-unblock
		lockDescriptor, err := conn.Lock(ctx, databasePath, "unblocks")
		if err != nil {
			t.Errorf("Lock(test_database) failed: %v", err)
		}
		if err = lockDescriptor.Unlock(ctx); err != nil {
			t.Errorf("Unlock(test_database): %v", err)
		}
		close(finished)
	}()

	// Lock the database.
	lockDescriptor2, err := conn.Lock(ctx, databasePath, "")
	if err != nil {
		t.Fatalf("Lock(test_database) failed: %v", err)
	}

	// unblock the go routine so it starts waiting
	close(unblock)

	// sleep for a while so we're sure the go routine is blocking
	time.Sleep(timeUntilLockIsTaken)

	if err = lockDescriptor2.Unlock(ctx); err != nil {
		t.Fatalf("Unlock(test_database): %v", err)
	}

	timeout := time.After(10 * time.Second)
	select {
	case <-finished:
	case <-timeout:
		t.Fatalf("Unlock(test_database) timed out")
	}
}

// checkLockName checks if we can lock / unlock using LockName as expected.
// LockName doesn't require the path to exist and has a static 24-hour TTL.
func checkLockName(t *testing.T, ctx context.Context, ts topo.Store) {
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell(global) failed: %v", err)
	}

	// Use a non-existent path since LockName doesn't require it to exist
	lockPath := "test_lock_name_path"
	lockDescriptor, err := conn.LockName(ctx, lockPath, "")
	if err != nil {
		t.Fatalf("LockName: %v", err)
	}

	// We should not be able to take the same named lock again
	fastCtx, cancel := context.WithTimeout(ctx, timeUntilLockIsTaken)
	if _, err := conn.LockName(fastCtx, lockPath, "again"); !errors.Is(err, &topo.TopoError{Code: topo.Timeout}) {
		t.Fatalf("LockName(again): %v", err)
	}
	cancel()

	// test we can interrupt taking the lock
	interruptCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(timeUntilLockIsTaken)
		cancel()
	}()
	if _, err := conn.LockName(interruptCtx, lockPath, "interrupted"); !errors.Is(err, &topo.TopoError{Code: topo.Interrupted}) {
		t.Fatalf("LockName(interrupted): %v", err)
	}

	if err := lockDescriptor.Check(ctx); err != nil {
		t.Errorf("Check(): %v", err)
	}

	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock(): %v", err)
	}

	// test we can't unlock again
	if err := lockDescriptor.Unlock(ctx); err == nil {
		t.Fatalf("Unlock(again) worked")
	}
}

// checkTryLock checks if we can lock / unlock as expected. It's using a database
// as the lock target.
func checkTryLock(t *testing.T, ctx context.Context, ts topo.Store) {
	if err := ts.CreateDatabase(ctx, "test_database", &clustermetadatapb.Database{}); err != nil {
		require.Fail(t, "CreateDatabase fail", err.Error())
	}

	conn, err := ts.ConnForCell(context.Background(), topo.GlobalCell)
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
	databasePath := path.Join(topo.DatabasesPath, "test_database")
	lockDescriptor, err := conn.TryLock(ctx, databasePath, "")
	if err != nil {
		require.Fail(t, "TryLock failed", err.Error())
	}

	// We have the lock, list the cell location directory.
	// It should not contain anything, except Ephemeral files.
	entries, err := conn.ListDir(ctx, databasePath, true /*full*/)
	if err != nil {
		require.Fail(t, "ListDir failed: %v", err.Error())
	}
	for _, e := range entries {
		if e.Name == "Database" {
			continue
		}
		if e.Ephemeral {
			t.Logf("skipping ephemeral node %v in %v", e, databasePath)
			continue
		}
		// Non-ephemeral entries better have only ephemeral children.
		p := path.Join(databasePath, e.Name)
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
	if _, err := conn.TryLock(fastCtx, databasePath, "again"); !errors.Is(err, &topo.TopoError{Code: topo.NodeExists}) {
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
		if _, err := conn.TryLock(interruptCtx, databasePath, "interrupted"); !errors.Is(err, &topo.TopoError{Code: topo.NodeExists}) {
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
	cellLocationPath := path.Join(topo.DatabasesPath, "test_database")
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

	// Lock the database.
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
