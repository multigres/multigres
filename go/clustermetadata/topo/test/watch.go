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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				require.Fail(t, "time out waiting for file to appear")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			cancel()
			require.NoError(t, err, "watch failed")
		}
		// we got a valid result
		break
	}
	got := &clustermetadatapb.Database{}
	err = proto.Unmarshal(current.Contents, got)
	if err != nil {
		cancel()
		require.NoError(t, err, "cannot proto-unmarshal data")
	}
	if !proto.Equal(got, database) {
		cancel()
		require.Equal(t, database, got, "got bad data")
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
				require.Fail(t, "time out waiting for file to appear")
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
			require.NoError(t, err, "watch failed")
		}
		// we got a valid result
		break
	}
	got := &clustermetadatapb.Database{}
	err = proto.Unmarshal(current[0].Contents, got)
	if err != nil {
		cancel()
		require.NoError(t, err, "cannot proto-unmarshal data")
	}
	if !proto.Equal(got, database) {
		cancel()
		require.Equal(t, database, got, "got bad data")
	}

	return changes, cancel, nil
}

// checkWatch runs the tests on the Watch part of the Conn API.
// We use a Database object.
func checkWatch(t *testing.T, ctx context.Context, ts topo.Store) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err, "ConnForCell(test) failed")

	// start watching something that doesn't exist -> error
	current, changes, err := conn.Watch(ctx, "databases/test_database/Database")
	if !errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
		assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}), "watch on missing node should return ErrNoNode, got: %v %v", current, changes)
	}

	// create some data
	database := &clustermetadatapb.Database{
		Name: "test_database",
	}
	err = ts.UpdateDatabaseFields(ctx, "test_database", func(db *clustermetadatapb.Database) error {
		db.Name = "test_database"
		return nil
	})
	require.NoError(t, err, "UpdateDatabaseFields(1) failed")

	// start watching again, it should work
	changes, secondCancel := waitForInitialValue(t, conn, database)
	defer secondCancel()

	// change the data
	database.Name = "test_database_new"
	err = ts.UpdateDatabaseFields(ctx, "test_database", func(db *clustermetadatapb.Database) error {
		db.Name = database.Name
		return nil
	})
	require.NoError(t, err, "UpdateDatabaseFields(2) failed")

	// Make sure we get the watch data, maybe not as first notice,
	// but eventually. The API specifies it is possible to get duplicate
	// notifications.
	for {
		wd, ok := <-changes
		if !ok {
			require.Fail(t, "watch channel unexpectedly closed")
		}
		if wd.Err != nil {
			require.NoError(t, wd.Err, "watch interrupted")
		}
		got := &clustermetadatapb.Database{}
		err := proto.Unmarshal(wd.Contents, got)
		require.NoError(t, err, "cannot proto-unmarshal data")

		if got.Name == "test_database" {
			// extra first value, still good
			continue
		}
		if got.Name == "test_database_new" {
			// watch worked, good
			break
		}
		assert.Contains(t, []string{"test_database", "test_database_new"}, got.Name, "got unknown Database: %v", got)
	}

	// remove the database
	err = ts.DeleteDatabase(ctx, "test_database", false)
	require.NoError(t, err, "DeleteDatabase failed")

	// Make sure we get the ErrNoNode notification eventually.
	// The API specifies it is possible to get duplicate
	// notifications.
	for {
		wd, ok := <-changes
		if !ok {
			require.Fail(t, "watch channel unexpectedly closed")
		}
		if errors.Is(wd.Err, &topo.TopoError{Code: topo.NoNode}) {
			// good
			break
		}
		if wd.Err != nil {
			require.NoError(t, wd.Err, "unexpected error returned for deletion")
		}
		// we got something, better be the right value
		got := &clustermetadatapb.Database{}
		err := proto.Unmarshal(wd.Contents, got)
		require.NoError(t, err, "cannot proto-unmarshal data")
		if got.Name == "test_database_new" {
			// good value
			continue
		}
		require.Equal(t, "test_database_new", got.Name, "got unknown Database waiting for deletion: %v", got)
	}

	// now the channel should be closed
	if wd, ok := <-changes; ok {
		require.Fail(t, "got unexpected event after error: %v", wd)
	}
}

// checkWatchInterrupt tests we can interrupt a watch.
func checkWatchInterrupt(t *testing.T, ctx context.Context, ts topo.Store) {
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err, "ConnForCell(test) failed")

	// create some data
	database := &clustermetadatapb.Database{
		Name: "test_database",
	}
	if err := ts.UpdateDatabaseFields(ctx, "test_database", func(db *clustermetadatapb.Database) error {
		db.Name = database.Name
		return nil
	}); err != nil {
		require.NoError(t, err, "UpdateDatabaseFields(1) failed")
	}

	// Start watching, it should work.
	changes, cancel := waitForInitialValue(t, conn, database)

	// Now cancel the watch.
	cancel()

	// Make sure we get the topo.ErrInterrupted notification eventually.
	for {
		wd, ok := <-changes
		if !ok {
			require.Fail(t, "watch channel unexpectedly closed")
		}
		if errors.Is(wd.Err, &topo.TopoError{Code: topo.Interrupted}) {
			// good
			break
		}
		if wd.Err != nil {
			require.NoError(t, wd.Err, "unexpected error returned for cancellation")
		}
		// we got something, better be the right value
		got := &clustermetadatapb.Database{}
		err := proto.Unmarshal(wd.Contents, got)
		require.NoError(t, err, "cannot proto-unmarshal data")
		if got.Name == "test_database" {
			// good value
			continue
		}
		require.Equal(t, "test_database_new", got.Name, "got unknown Database waiting for deletion: %v", got)
	}

	// Now the channel should be closed.
	if wd, ok := <-changes; ok {
		require.Fail(t, "got unexpected event after error: %v", wd)
	}

	// And calling cancel() again should just work.
	cancel()
}

// checkWatchRecursive tests we can setup a recursive watch
func checkWatchRecursive(t *testing.T, ctx context.Context, ts topo.Store) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err, "ConnForCell(test) failed")

	// create some data
	database := &clustermetadatapb.Database{
		Name: "test_database",
	}
	if err := ts.UpdateDatabaseFields(ctx, "test_database", func(db *clustermetadatapb.Database) error {
		db.Name = database.Name
		return nil
	}); err != nil {
		require.NoError(t, err, "UpdateDatabaseFields(1) failed")
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
	err = ts.UpdateDatabaseFields(ctx, "test_database", func(db *clustermetadatapb.Database) error {
		db.Name = "test_database_new"
		return nil
	})
	require.NoError(t, err, "UpdateDatabaseFields(2) failed")

	// Make sure we get the watch data, maybe not as first notice,
	// but eventually. The API specifies it is possible to get duplicate
	// notifications.
	for {
		wd, ok := <-changes
		if !ok {
			require.Fail(t, "watch channel unexpectedly closed")
		}
		if wd.Err != nil {
			require.NoError(t, wd.Err, "watch interrupted")
		}
		got := &clustermetadatapb.Database{}
		err := proto.Unmarshal(wd.Contents, got)
		require.NoError(t, err, "cannot proto-unmarshal data")

		if got.Name == "test_database" {
			// extra first value, still good
			continue
		}
		if got.Name == "test_database_new" {
			// watch worked, good
			break
		}
		assert.Contains(t, []string{"test_database", "test_database_new"}, got.Name, "got unknown Database: %v", got)
	}

	// remove the database
	err = ts.DeleteDatabase(ctx, "test_database", false)
	require.NoError(t, err, "DeleteDatabase failed")

	// Make sure we get the ErrNoNode notification eventually.
	// The API specifies it is possible to get duplicate
	// notifications.
	for {
		wd, ok := <-changes
		if !ok {
			require.Fail(t, "watch channel unexpectedly closed")
		}

		if errors.Is(wd.Err, &topo.TopoError{Code: topo.NoNode}) {
			// good
			break
		}
		if wd.Err != nil {
			require.NoError(t, wd.Err, "unexpected error returned for deletion")
		}
		// we got something, better be the right value
		got := &clustermetadatapb.Database{}
		err := proto.Unmarshal(wd.Contents, got)
		require.NoError(t, err, "cannot proto-unmarshal data")
		if got.Name == "test_database_new" {
			// good value
			continue
		}
		require.Equal(t, "test_database_new", got.Name, "got unknown Database waiting for deletion: %v", got)
	}

	// We now have to stop watching. This doesn't automatically
	// happen for recursive watches on a single file since others
	// can still be seen.
	secondCancel()

	// Make sure we get the topo.ErrInterrupted notification eventually.
	for {
		wd, ok := <-changes
		if !ok {
			require.Fail(t, "watch channel unexpectedly closed")
		}
		if errors.Is(wd.Err, &topo.TopoError{Code: topo.Interrupted}) {
			// good
			break
		}
		if wd.Err != nil {
			require.NoError(t, wd.Err, "unexpected error returned for cancellation")
		}
		// we got something, better be the right value
		got := &clustermetadatapb.Database{}
		err := proto.Unmarshal(wd.Contents, got)
		require.NoError(t, err, "cannot proto-unmarshal data")
		if got.Name == "test_database" {
			// good value
			continue
		}
		require.Equal(t, "test_database_new", got.Name, "got unknown Database waiting for deletion: %v", got)
	}

	// Now the channel should be closed.
	if wd, ok := <-changes; ok {
		require.Fail(t, "got unexpected event after error: %v", wd)
	}

	// And calling cancel() again should just work.
	secondCancel()
}
