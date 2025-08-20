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

	"github.com/stretchr/testify/assert"
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
	err := ts.CreateDatabase(ctx, "test_database", &clustermetadatapb.Database{})
	require.NoError(t, err, "CreateDatabase failed")

	conn, err := ts.ConnForCell(context.Background(), topo.GlobalCell)
	require.NoError(t, err, "ConnForCell(global) failed")

	t.Log("===      checkLockTimeout")
	checkLockTimeout(ctx, t, conn)

	t.Log("===      checkLockUnblocks")
	checkLockUnblocks(ctx, t, conn)
}

func checkLockTimeout(ctx context.Context, t *testing.T, conn topo.Conn) {
	databasePath := path.Join(topo.DatabasesPath, "test_database")
	lockDescriptor, err := conn.Lock(ctx, databasePath, "")
	require.NoError(t, err, "Lock failed")

	// We have the lock, list the database directory.
	// It should not contain anything, except Ephemeral files.
	entries, err := conn.ListDir(ctx, databasePath, true /*full*/)
	require.NoError(t, err, "ListDir(%v) failed", databasePath)
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
		require.NoError(t, err, "ListDir(%v) failed", p)
		for _, e := range entries {
			if e.Ephemeral {
				t.Logf("skipping ephemeral node %v in %v", e, p)
			} else {
				assert.Fail(t, "Entry in %v has non-ephemeral DirEntry: %v", p, e)
			}
		}
	}

	// test we can't take the lock again
	fastCtx, cancel := context.WithTimeout(ctx, timeUntilLockIsTaken)
	_, err = conn.Lock(fastCtx, databasePath, "again")
	assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.Timeout}), "Lock(again) should return Timeout error, got: %v", err)
	cancel()

	// test we can interrupt taking the lock
	interruptCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(timeUntilLockIsTaken)
		cancel()
	}()
	_, err = conn.Lock(interruptCtx, databasePath, "interrupted")
	assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.Interrupted}), "Lock(interrupted) should return Interrupted error, got: %v", err)

	err = lockDescriptor.Check(ctx)
	assert.NoError(t, err, "Check() failed")

	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock() failed")

	// test we can't unlock again
	err = lockDescriptor.Unlock(ctx)
	assert.Error(t, err, "Unlock(again) should fail")
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
		require.NoError(t, err, "Lock(test_database) failed")
		err = lockDescriptor.Unlock(ctx)
		require.NoError(t, err, "Unlock(test_database) failed")
		close(finished)
	}()

	// Lock the database.
	lockDescriptor2, err := conn.Lock(ctx, databasePath, "")
	require.NoError(t, err, "Lock(test_database) failed")

	// unblock the go routine so it starts waiting
	close(unblock)

	// sleep for a while so we're sure the go routine is blocking
	time.Sleep(timeUntilLockIsTaken)

	err = lockDescriptor2.Unlock(ctx)
	require.NoError(t, err, "Unlock(test_database) failed")

	timeout := time.After(10 * time.Second)
	select {
	case <-finished:
	case <-timeout:
		require.Fail(t, "Unlock(test_database) timed out")
	}
}

// checkLockName checks if we can lock / unlock using LockName as expected.
// LockName doesn't require the path to exist and has a static 24-hour TTL.
func checkLockName(t *testing.T, ctx context.Context, ts topo.Store) {
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err, "ConnForCell(global) failed")

	// Use a non-existent path since LockName doesn't require it to exist
	lockPath := "test_lock_name_path"
	lockDescriptor, err := conn.LockName(ctx, lockPath, "")
	require.NoError(t, err, "LockName failed")

	// We should not be able to take the same named lock again
	fastCtx, cancel := context.WithTimeout(ctx, timeUntilLockIsTaken)
	_, err = conn.LockName(fastCtx, lockPath, "again")
	assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.Timeout}), "LockName(again) should return Timeout error, got: %v", err)
	cancel()

	// test we can interrupt taking the lock
	interruptCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(timeUntilLockIsTaken)
		cancel()
	}()
	_, err = conn.LockName(interruptCtx, lockPath, "interrupted")
	assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.Interrupted}), "LockName(interrupted) should return Interrupted error, got: %v", err)

	err = lockDescriptor.Check(ctx)
	assert.NoError(t, err, "Check() failed")

	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock() failed")

	// test we can't unlock again
	err = lockDescriptor.Unlock(ctx)
	assert.Error(t, err, "Unlock(again) should fail")
}

// checkTryLock checks if we can lock / unlock as expected. It's using a database
// as the lock target.
func checkTryLock(t *testing.T, ctx context.Context, ts topo.Store) {
	err := ts.CreateDatabase(ctx, "test_database", &clustermetadatapb.Database{})
	require.NoError(t, err, "CreateDatabase failed")

	conn, err := ts.ConnForCell(context.Background(), topo.GlobalCell)
	require.NoError(t, err, "ConnForCell(global) failed")

	t.Log("===      checkTryLockTimeout")
	checkTryLockTimeout(ctx, t, conn)

	t.Log("===      checkTryLockUnblocks")
	checkTryLockUnblocks(ctx, t, conn)
}

// checkTryLockTimeout test the fail-fast nature of TryLock
func checkTryLockTimeout(ctx context.Context, t *testing.T, conn topo.Conn) {
	databasePath := path.Join(topo.DatabasesPath, "test_database")
	lockDescriptor, err := conn.TryLock(ctx, databasePath, "")
	require.NoError(t, err, "TryLock failed")

	// We have the lock, list the cell location directory.
	// It should not contain anything, except Ephemeral files.
	entries, err := conn.ListDir(ctx, databasePath, true /*full*/)
	require.NoError(t, err, "ListDir failed")
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
		require.NoError(t, err, "ListDir failed")
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
	_, err = conn.TryLock(fastCtx, databasePath, "again")
	assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.NodeExists}), "TryLock should return NodeExists error, got: %v", err)
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
			require.Fail(t, "Unlock(test_database) timed out")
		}
		// we expect context to fail with `context canceled` error
		if interruptCtx.Err() != nil {
			require.ErrorContains(t, interruptCtx.Err(), "context canceled")
			break
		}
		_, err := conn.TryLock(interruptCtx, databasePath, "interrupted")
		assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.NodeExists}), "TryLock should return NodeExists error, got: %v", err)
		if firstTime {
			close(finished)
			firstTime = false
		}
		time.Sleep(1 * time.Second)
	}

	err = lockDescriptor.Check(ctx)
	assert.NoError(t, err, "Check() failed")

	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock failed")

	// test we can't unlock again
	if err := lockDescriptor.Unlock(ctx); err == nil {
		require.Fail(t, "Unlock succeeded but should not have")
	}
}

// unlike 'checkLockUnblocks', checkTryLockUnblocks will not block on other client but instead
// keep retrying until it gets the lock.
func checkTryLockUnblocks(ctx context.Context, t *testing.T, conn topo.Conn) {
	cellPath := path.Join(topo.DatabasesPath, "test_database")
	unblock := make(chan struct{})
	finished := make(chan struct{})

	duration := 10 * time.Second
	waitUntil := time.Now().Add(duration)
	// TryLock will keep getting NodeExists until lockDescriptor2 unlock itself.
	// It will not wait but immediately return with NodeExists error.
	go func() {
		<-unblock
		for time.Now().Before(waitUntil) {
			lockDescriptor, err := conn.TryLock(ctx, cellPath, "unblocks")
			if err != nil {
				assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.NodeExists}), "expected node exists during trylock, got: %v", err)
				time.Sleep(1 * time.Second)
			} else {
				if err = lockDescriptor.Unlock(ctx); err != nil {
					require.NoError(t, err, "Unlock(test_database) failed")
				}
				close(finished)
				break
			}
		}
	}()

	// Lock the database.
	lockDescriptor2, err := conn.TryLock(ctx, cellPath, "")
	if err != nil {
		require.NoError(t, err, "Lock(test_database) failed")
	}

	// unblock the go routine so it starts waiting
	close(unblock)

	if err = lockDescriptor2.Unlock(ctx); err != nil {
		require.NoError(t, err, "Unlock(test_database) failed")
	}

	timeout := time.After(2 * duration)
	select {
	case <-finished:
	case <-timeout:
		require.Fail(t, "Unlock(test_database) timed out")
	}
}
