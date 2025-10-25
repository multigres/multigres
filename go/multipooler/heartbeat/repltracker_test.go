// Copyright 2025 Supabase, Inc.
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

package heartbeat

import (
	"log/slog"
	"testing"
	"time"

	"github.com/multigres/multigres/go/fakepgdb"

	"github.com/stretchr/testify/assert"
)

func TestReplTrackerMakePrimary(t *testing.T) {
	db := fakepgdb.New(t)
	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	db.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", &fakepgdb.ExpectedResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	})
	db.AddQueryPattern("SELECT pg_backend_pid\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_backend_pid"},
		Rows:    [][]interface{}{{int64(12345)}},
	})
	db.AddQueryPattern("SELECT pg_current_wal_lsn\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_current_wal_lsn"},
		Rows:    [][]interface{}{{"0/1A2B3C4D"}},
	})

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	rt := NewReplTracker(sqlDB, logger, shardID, poolerID, 250)
	defer rt.Close()

	assert.False(t, rt.IsPrimary())
	assert.False(t, rt.hw.IsOpen())

	rt.MakePrimary()
	assert.True(t, rt.IsPrimary())
	assert.True(t, rt.hw.IsOpen())

	// Wait for some heartbeats to be written
	time.Sleep(1 * time.Second)

	assert.Greater(t, rt.Writes(), int64(0))
	assert.EqualValues(t, 0, rt.WriteErrors())
}

func TestReplTrackerMakeNonPrimary(t *testing.T) {
	db := fakepgdb.New(t)
	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	db.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", &fakepgdb.ExpectedResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	})
	db.AddQueryPattern("SELECT pg_backend_pid\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_backend_pid"},
		Rows:    [][]interface{}{{int64(12345)}},
	})
	db.AddQueryPattern("SELECT pg_current_wal_lsn\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_current_wal_lsn"},
		Rows:    [][]interface{}{{"0/1A2B3C4D"}},
	})
	db.AddQuery("SELECT ts FROM multigres.heartbeat WHERE shard_id = $1", &fakepgdb.ExpectedResult{
		Columns: []string{"ts"},
		Rows: [][]interface{}{
			{time.Now().Add(-5 * time.Second)},
		},
	})

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	rt := NewReplTracker(sqlDB, logger, shardID, poolerID, 250)
	defer rt.Close()

	rt.MakePrimary()
	assert.True(t, rt.IsPrimary())
	assert.True(t, rt.hw.IsOpen())

	// Wait for some heartbeats
	time.Sleep(1 * time.Second)
	assert.Greater(t, rt.Writes(), int64(0))

	rt.MakeNonPrimary()
	assert.False(t, rt.IsPrimary())
	assert.False(t, rt.hw.IsOpen())

	// Capture writes count immediately after stopping to avoid race
	lastWrites := rt.Writes()

	// Wait and verify no more writes happen
	time.Sleep(1 * time.Second)
	assert.EqualValues(t, lastWrites, rt.Writes())
}

func TestReplTrackerEnableHeartbeat(t *testing.T) {
	db := fakepgdb.New(t)
	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	db.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", &fakepgdb.ExpectedResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	})
	db.AddQueryPattern("SELECT pg_backend_pid\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_backend_pid"},
		Rows:    [][]interface{}{{int64(12345)}},
	})
	db.AddQueryPattern("SELECT pg_current_wal_lsn\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_current_wal_lsn"},
		Rows:    [][]interface{}{{"0/1A2B3C4D"}},
	})

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	rt := NewReplTracker(sqlDB, logger, shardID, poolerID, 250)
	defer rt.Close()

	rt.hw.Open()
	defer rt.hw.Close()

	// Manually enable writes
	rt.EnableHeartbeat(true)

	// Wait for heartbeats
	time.Sleep(1 * time.Second)
	assert.Greater(t, rt.Writes(), int64(0))

	// Disable writes
	rt.EnableHeartbeat(false)

	// Capture writes count immediately after stopping to avoid race
	lastWrites := rt.Writes()

	// Wait and verify no more writes
	time.Sleep(1 * time.Second)
	assert.EqualValues(t, lastWrites, rt.Writes())

	// Re-enable writes
	rt.EnableHeartbeat(true)

	// Wait and verify writes resume
	time.Sleep(1 * time.Second)
	assert.Greater(t, rt.Writes(), lastWrites)
}

func TestReplTrackerMakePrimaryAndNonPrimary(t *testing.T) {
	db := fakepgdb.New(t)
	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	// Setup queries for both writer and reader
	db.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", &fakepgdb.ExpectedResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	})
	db.AddQueryPattern("SELECT pg_backend_pid\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_backend_pid"},
		Rows:    [][]interface{}{{int64(12345)}},
	})
	db.AddQueryPattern("SELECT pg_current_wal_lsn\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_current_wal_lsn"},
		Rows:    [][]interface{}{{"0/1A2B3C4D"}},
	})
	db.AddQuery("SELECT ts FROM multigres.heartbeat WHERE shard_id = $1", &fakepgdb.ExpectedResult{
		Columns: []string{"ts"},
		Rows: [][]interface{}{
			{time.Now().Add(-5 * time.Second)},
		},
	})

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	rt := NewReplTracker(sqlDB, logger, shardID, poolerID, 250)
	defer rt.Close()

	// Use shorter intervals for testing
	rt.hr.interval = 250 * time.Millisecond
	rt.hr.ticks.SetInterval(250 * time.Millisecond)

	// Start as primary
	rt.MakePrimary()
	assert.True(t, rt.IsPrimary())
	assert.True(t, rt.hw.IsOpen())
	assert.False(t, rt.hr.IsOpen())

	// Wait for some writes
	time.Sleep(1 * time.Second)
	assert.Greater(t, rt.Writes(), int64(0))
	assert.EqualValues(t, 0, rt.hr.Reads())

	// Switch to non-primary
	rt.MakeNonPrimary()
	assert.False(t, rt.IsPrimary())
	assert.False(t, rt.hw.IsOpen())
	assert.True(t, rt.hr.IsOpen())

	// Wait for some reads
	time.Sleep(1 * time.Second)
	assert.Greater(t, rt.hr.Reads(), int64(0))
}
