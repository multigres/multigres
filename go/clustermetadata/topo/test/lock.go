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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// timeUntilLockIsTaken is the time to wait until a lock is taken.
// We haven't found a better simpler way to guarantee a routine is stuck
// waiting for a topo lock than sleeping that amount.
var timeUntilLockIsTaken = 10 * time.Millisecond

// checkLock checks we can lock / unlock as expected. It's using a database
// as the lock target.
func checkLock(t *testing.T, ctx context.Context, ts topo.Store) {
	if err := ts.CreateCellLocation(ctx, "test_cell", &clustermetadatapb.CellLocation{}); err != nil {
		t.Fatalf("CreateCellLocation: %v", err)
	}

	conn, err := ts.ConnForCell(context.Background(), topo.GlobalTopo)
	if err != nil {
		t.Fatalf("ConnForCell(global) failed: %v", err)
	}

	t.Log("===      checkLockTimeout")
	checkLockTimeout(ctx, t, conn)

	t.Log("===      checkLockUnblocks")
	checkLockUnblocks(ctx, t, conn)
}

func checkLockTimeout(ctx context.Context, t *testing.T, conn topo.Conn) {
	cellLocationPath := path.Join(topo.CellsPath, "test_cell")
	lockDescriptor, err := conn.Lock(ctx, cellLocationPath, "")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// We have the lock, list the database directory.
	// It should not contain anything, except Ephemeral files.
	entries, err := conn.ListDir(ctx, cellLocationPath, true /*full*/)
	if err != nil {
		t.Fatalf("Listdir(%v) failed: %v", cellLocationPath, err)
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
	if _, err := conn.Lock(fastCtx, cellLocationPath, "again"); !errors.Is(err, &topo.TopoError{Code: topo.Timeout}) {
		t.Fatalf("Lock(again): %v", err)
	}
	cancel()

	// test we can interrupt taking the lock
	interruptCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(timeUntilLockIsTaken)
		cancel()
	}()
	if _, err := conn.Lock(interruptCtx, cellLocationPath, "interrupted"); !errors.Is(err, &topo.TopoError{Code: topo.Interrupted}) {
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
	cellLocationPath := path.Join(topo.CellsPath, "test_cell")
	unblock := make(chan struct{})
	finished := make(chan struct{})

	// As soon as we're unblocked, we try to lock the database.
	go func() {
		<-unblock
		lockDescriptor, err := conn.Lock(ctx, cellLocationPath, "unblocks")
		if err != nil {
			t.Errorf("Lock(test_cell) failed: %v", err)
		}
		if err = lockDescriptor.Unlock(ctx); err != nil {
			t.Errorf("Unlock(test_database): %v", err)
		}
		close(finished)
	}()

	// Lock the database.
	lockDescriptor2, err := conn.Lock(ctx, cellLocationPath, "")
	if err != nil {
		t.Fatalf("Lock(test_cell) failed: %v", err)
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
