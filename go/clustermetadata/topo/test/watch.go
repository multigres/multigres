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
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/clustermetadata/topo"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// waitForInitialValue waits for the initial value of
// databases/test_database/Database to appear, and match the
// provided database.
func waitForInitialValue(t *testing.T, conn topo.Conn, database *clustermetadatapb.Database) (changes <-chan *topo.WatchData, cancel context.CancelFunc) {
	var current *topo.WatchData
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	var err error
	for {
		current, changes, err = conn.Watch(ctx, "databases/test_database/Database")
		if errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
			// hasn't appeared yet
			if time.Since(start) > 10*time.Second {
				cancel()
				t.Fatalf("time out waiting for file to appear")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			cancel()
			t.Fatalf("watch failed: %v", err)
		}
		// we got a valid result
		break
	}
	got := &clustermetadatapb.Database{}
	if err := proto.Unmarshal(current.Contents, got); err != nil {
		cancel()
		t.Fatalf("cannot proto-unmarshal data: %v", err)
	}
	if !proto.Equal(got, database) {
		cancel()
		t.Fatalf("got bad data: %v expected: %v", got, database)
	}

	return changes, cancel
}

// waitForInitialValueRecursive waits for the initial value of
// databases/test_database. Any files that appear inside that directory
// will be watched. In this case will be waiting for the database to appear.
func waitForInitialValueRecursive(t *testing.T, conn topo.Conn, database *clustermetadatapb.Database) (changes <-chan *topo.WatchDataRecursive, cancel context.CancelFunc, err error) {
	var current []*topo.WatchDataRecursive
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	for {
		current, changes, err = conn.WatchRecursive(ctx, "databases/test_database")
		if errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
			// hasn't appeared yet
			if time.Since(start) > 10*time.Second {
				cancel()
				t.Fatalf("time out waiting for file to appear")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if errors.Is(err, &topo.TopoError{Code: topo.NoImplementation}) {
			// If this is not supported, skip the test
			cancel()
			return nil, nil, err
		}
		if err != nil {
			cancel()
			t.Fatalf("watch failed: %v", err)
		}
		// we got a valid result
		break
	}
	got := &clustermetadatapb.Database{}
	if err := proto.Unmarshal(current[0].Contents, got); err != nil {
		cancel()
		t.Fatalf("cannot proto-unmarshal data: %v", err)
	}
	if !proto.Equal(got, database) {
		cancel()
		t.Fatalf("got bad data: %v expected: %v", got, database)
	}

	return changes, cancel, nil
}

// checkWatch runs the tests on the Watch part of the Conn API.
// We use a Database object.
func checkWatch(t *testing.T, ctx context.Context, ts topo.Store) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}

	// start watching something that doesn't exist -> error
	current, changes, err := conn.Watch(ctx, "databases/test_database/Database")
	if !errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
		t.Errorf("watch on missing node didn't return ErrNoNode: %v %v", current, changes)
	}

	// create some data
	database := &clustermetadatapb.Database{
		Name: "test_database",
	}
	if err := ts.UpdateDatabaseFields(ctx, "test_database", func(db *clustermetadatapb.Database) error {
		db.Name = "test_database"
		return nil
	}); err != nil {
		t.Fatalf("UpdateDatabaseFields(1): %v", err)
	}

	// start watching again, it should work
	changes, secondCancel := waitForInitialValue(t, conn, database)
	defer secondCancel()

	// change the data
	database.Name = "test_database_new"
	if err := ts.UpdateDatabaseFields(ctx, "test_database", func(db *clustermetadatapb.Database) error {
		db.Name = database.Name
		return nil
	}); err != nil {
		t.Fatalf("UpdateDatabaseFields(2): %v", err)
	}

	// Make sure we get the watch data, maybe not as first notice,
	// but eventually. The API specifies it is possible to get duplicate
	// notifications.
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if wd.Err != nil {
			t.Fatalf("watch interrupted: %v", wd.Err)
		}
		got := &clustermetadatapb.Database{}
		if err := proto.Unmarshal(wd.Contents, got); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}

		if got.Name == "test_database" {
			// extra first value, still good
			continue
		}
		if got.Name == "test_database_new" {
			// watch worked, good
			break
		}
		t.Fatalf("got unknown Database: %v", got)
	}

	// remove the database
	if err := ts.DeleteDatabase(ctx, "test_database", false); err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}

	// Make sure we get the ErrNoNode notification eventually.
	// The API specifies it is possible to get duplicate
	// notifications.
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if errors.Is(wd.Err, &topo.TopoError{Code: topo.NoNode}) {
			// good
			break
		}
		if wd.Err != nil {
			t.Fatalf("bad error returned for deletion: %v", wd.Err)
		}
		// we got something, better be the right value
		got := &clustermetadatapb.Database{}
		if err := proto.Unmarshal(wd.Contents, got); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}
		if got.Name == "test_database_new" {
			// good value
			continue
		}
		t.Fatalf("got unknown Database waiting for deletion: %v", got)
	}

	// now the channel should be closed
	if wd, ok := <-changes; ok {
		t.Fatalf("got unexpected event after error: %v", wd)
	}
}

// checkWatchInterrupt tests we can interrupt a watch.
func checkWatchInterrupt(t *testing.T, ctx context.Context, ts topo.Store) {
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}

	// create some data
	database := &clustermetadatapb.Database{
		Name: "test_database",
	}
	if err := ts.UpdateDatabaseFields(ctx, "test_database", func(db *clustermetadatapb.Database) error {
		db.Name = database.Name
		return nil
	}); err != nil {
		t.Fatalf("UpdateDatabaseFields(1): %v", err)
	}

	// Start watching, it should work.
	changes, cancel := waitForInitialValue(t, conn, database)

	// Now cancel the watch.
	cancel()

	// Make sure we get the topo.ErrInterrupted notification eventually.
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if errors.Is(wd.Err, &topo.TopoError{Code: topo.Interrupted}) {
			// good
			break
		}
		if wd.Err != nil {
			t.Fatalf("bad error returned for cancellation: %v", wd.Err)
		}
		// we got something, better be the right value
		got := &clustermetadatapb.Database{}
		if err := proto.Unmarshal(wd.Contents, got); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}
		if got.Name == "test_database" {
			// good value
			continue
		}
		t.Fatalf("got unknown Database waiting for deletion: %v", got)
	}

	// Now the channel should be closed.
	if wd, ok := <-changes; ok {
		t.Fatalf("got unexpected event after error: %v", wd)
	}

	// And calling cancel() again should just work.
	cancel()
}

// checkWatchRecursive tests we can setup a recursive watch
func checkWatchRecursive(t *testing.T, ctx context.Context, ts topo.Store) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}

	// create some data
	database := &clustermetadatapb.Database{
		Name: "test_database",
	}
	if err := ts.UpdateDatabaseFields(ctx, "test_database", func(db *clustermetadatapb.Database) error {
		db.Name = database.Name
		return nil
	}); err != nil {
		t.Fatalf("UpdateDatabaseFields(1): %v", err)
	}

	// start watching again, it should work
	changes, secondCancel, err := waitForInitialValueRecursive(t, conn, database)
	if errors.Is(err, &topo.TopoError{Code: topo.NoImplementation}) {
		// Skip the rest if there's no implementation
		t.Logf("%T does not support WatchRecursive()", conn)
		return
	}
	defer secondCancel()

	// change the data
	database.Name = "test_database_new"
	if err := ts.UpdateDatabaseFields(ctx, "test_database", func(db *clustermetadatapb.Database) error {
		db.Name = "test_database_new"
		return nil
	}); err != nil {
		t.Fatalf("UpdateDatabaseFields(2): %v", err)
	}

	// Make sure we get the watch data, maybe not as first notice,
	// but eventually. The API specifies it is possible to get duplicate
	// notifications.
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if wd.Err != nil {
			t.Fatalf("watch interrupted: %v", wd.Err)
		}
		got := &clustermetadatapb.Database{}
		if err := proto.Unmarshal(wd.Contents, got); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}

		if got.Name == "test_database" {
			// extra first value, still good
			continue
		}
		if got.Name == "test_database_new" {
			// watch worked, good
			break
		}
		t.Fatalf("got unknown Database: %v", got)
	}

	// remove the database
	if err := ts.DeleteDatabase(ctx, "test_database", false); err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}

	// Make sure we get the ErrNoNode notification eventually.
	// The API specifies it is possible to get duplicate
	// notifications.
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}

		if errors.Is(wd.Err, &topo.TopoError{Code: topo.NoNode}) {
			// good
			break
		}
		if wd.Err != nil {
			t.Fatalf("bad error returned for deletion: %v", wd.Err)
		}
		// we got something, better be the right value
		got := &clustermetadatapb.Database{}
		if err := proto.Unmarshal(wd.Contents, got); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}
		if got.Name == "test_database_new" {
			// good value
			continue
		}
		t.Fatalf("got unknown Database waiting for deletion: %v", got)
	}

	// We now have to stop watching. This doesn't automatically
	// happen for recursive watches on a single file since others
	// can still be seen.
	secondCancel()

	// Make sure we get the topo.ErrInterrupted notification eventually.
	for {
		wd, ok := <-changes
		if !ok {
			t.Fatalf("watch channel unexpectedly closed")
		}
		if errors.Is(wd.Err, &topo.TopoError{Code: topo.Interrupted}) {
			// good
			break
		}
		if wd.Err != nil {
			t.Fatalf("bad error returned for cancellation: %v", wd.Err)
		}
		// we got something, better be the right value
		got := &clustermetadatapb.Database{}
		if err := proto.Unmarshal(wd.Contents, got); err != nil {
			t.Fatalf("cannot proto-unmarshal data: %v", err)
		}
		if got.Name == "test_database" {
			// good value
			continue
		}
		t.Fatalf("got unknown Database waiting for deletion: %v", got)
	}

	// Now the channel should be closed.
	if wd, ok := <-changes; ok {
		t.Fatalf("got unexpected event after error: %v", wd)
	}

	// And calling cancel() again should just work.
	secondCancel()
}
