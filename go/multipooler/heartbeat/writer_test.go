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
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/fakepgdb"
)

func TestWriteHeartbeat(t *testing.T) {
	db := fakepgdb.New(t)
	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	now := time.Now()
	tw := newTestWriter(t, db, &now)

	// Add expected heartbeat query (must match with whitespace)
	db.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", &fakepgdb.ExpectedResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	})
	db.AddQueryPattern("SELECT pg_backend_pid\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_backend_pid"},
		Rows:    [][]interface{}{{int64(12345)}},
	})

	// Write a single heartbeat
	tw.writeHeartbeat()
	lastWrites := tw.Writes()
	assert.EqualValues(t, 1, lastWrites)
	assert.EqualValues(t, 0, tw.WriteErrors())
}

// TestWriteHeartbeatOpen tests that the heartbeat writer writes heartbeats when the writer is open.
func TestWriteHeartbeatOpen(t *testing.T) {
	db := fakepgdb.New(t)
	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	tw := newTestWriter(t, db, nil)

	// Add expected heartbeat query pattern
	db.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", &fakepgdb.ExpectedResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	})
	db.AddQueryPattern("SELECT pg_backend_pid\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_backend_pid"},
		Rows:    [][]interface{}{{int64(12345)}},
	})

	// Test initial write before opening
	tw.writeHeartbeat()
	lastWrites := tw.Writes()
	assert.EqualValues(t, 1, lastWrites)
	assert.EqualValues(t, 0, tw.WriteErrors())

	t.Run("closed, no heartbeats", func(t *testing.T) {
		time.Sleep(3 * time.Second)
		assert.EqualValues(t, 1, tw.Writes())
	})

	tw.Open()
	defer tw.Close()

	t.Run("open, heartbeats", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				assert.EqualValues(t, 0, tw.WriteErrors())
				currentWrites := tw.Writes()
				assert.Greater(t, currentWrites, lastWrites)
				lastWrites = currentWrites
			}
		}
	})
}

// TestWriteHeartbeatError tests that write errors are logged but don't crash the writer.
func TestWriteHeartbeatError(t *testing.T) {
	db := fakepgdb.New(t)
	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	tw := newTestWriter(t, db, nil)

	// Don't add any expected queries - this will cause an error
	tw.writeHeartbeat()
	assert.EqualValues(t, 0, tw.Writes())
	assert.EqualValues(t, 1, tw.WriteErrors())
}

// TestCloseWhileStuckWriting tests that Close shouldn't get stuck even if the heartbeat writer is stuck.
func TestCloseWhileStuckWriting(t *testing.T) {
	db := fakepgdb.New(t)
	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	tw := newTestWriter(t, db, nil)

	killWg := sync.WaitGroup{}
	killWg.Add(1)
	startedWaitWg := sync.WaitGroup{}
	startedWaitWg.Add(1)

	// Insert a query pattern that causes the insert to block indefinitely until it has been killed
	db.AddQueryPatternWithCallback("\\s*INSERT INTO multigres\\.heartbeat.*", &fakepgdb.ExpectedResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	}, func(s string) {
		startedWaitWg.Done()
		killWg.Wait()
	})

	db.AddQueryPattern("SELECT pg_backend_pid\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_backend_pid"},
		Rows:    [][]interface{}{{int64(12345)}},
	})

	// When we receive a kill query, we want to finish running the wait group to unblock the insert query
	db.AddQueryPatternWithCallback("SELECT pg_terminate_backend.*", &fakepgdb.ExpectedResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	}, func(s string) {
		killWg.Done()
	})

	// Open the writer and enable writes
	tw.Open()

	// Wait until the write has blocked
	startedWaitWg.Wait()

	// Even if the write is blocked, we should be able to close without waiting indefinitely
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		tw.Close()
		cancel()
	}()

	select {
	case <-ctx.Done():
		// Success - close completed
	case <-time.After(10 * time.Second):
		t.Fatalf("Timed out waiting for heartbeat writer to close")
	}
}

// TestOpenClose tests the basic open/close lifecycle.
func TestOpenClose(t *testing.T) {
	db := fakepgdb.New(t)
	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	tw := newTestWriter(t, db, nil)

	db.AddQueryPattern("\\s*INSERT INTO multigres\\.heartbeat.*", &fakepgdb.ExpectedResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	})
	db.AddQueryPattern("SELECT pg_backend_pid\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_backend_pid"},
		Rows:    [][]interface{}{{int64(12345)}},
	})

	assert.False(t, tw.IsOpen())

	tw.Open()
	assert.True(t, tw.IsOpen())

	// Open should be idempotent
	tw.Open()
	assert.True(t, tw.IsOpen())

	tw.Close()
	assert.False(t, tw.IsOpen())

	// Close should be idempotent
	tw.Close()
	assert.False(t, tw.IsOpen())
}

// TestMultipleWriters tests that multiple writers can run concurrently.
func TestMultipleWriters(t *testing.T) {
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

	tw1 := newTestWriter(t, db, nil)
	tw2 := newTestWriter(t, db, nil)

	tw1.Open()
	tw2.Open()

	defer tw1.Close()
	defer tw2.Close()

	// Let them write for a bit
	time.Sleep(3 * time.Second)

	// Both should have written heartbeats
	assert.Greater(t, tw1.Writes(), int64(0))
	assert.Greater(t, tw2.Writes(), int64(0))
	assert.EqualValues(t, 0, tw1.WriteErrors())
	assert.EqualValues(t, 0, tw2.WriteErrors())
}

// newTestWriter creates a new heartbeat writer for testing.
func newTestWriter(t *testing.T, db *fakepgdb.DB, frozenTime *time.Time) *Writer {
	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	sqlDB := db.OpenDB()
	t.Cleanup(func() { sqlDB.Close() })

	// Add pg_current_wal_lsn mock for all writer tests
	db.AddQueryPattern("SELECT pg_current_wal_lsn\\(\\)", &fakepgdb.ExpectedResult{
		Columns: []string{"pg_current_wal_lsn"},
		Rows:    [][]interface{}{{"0/1A2B3C4D"}},
	})

	// Use 250ms interval for tests to oversample our 1s test ticker
	tw := NewWriter(sqlDB, logger, shardID, poolerID, 250)

	if frozenTime != nil {
		tw.now = func() time.Time {
			return *frozenTime
		}
	}

	return tw
}

// TestWriterOpen tests various scenarios when opening the heartbeat writer
func TestWriterOpen(t *testing.T) {
	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	tests := []struct {
		name           string
		initialTerm    int64
		dbTerm         *int64 // nil means error
		dbError        error
		expectedTerm   int64
		expectedIsOpen bool
	}{
		{
			name:           "TermFromDatabase",
			initialTerm:    0,
			dbTerm:         ptr(int64(10)),
			dbError:        nil,
			expectedTerm:   10,
			expectedIsOpen: true,
		},
		{
			name:           "EqualTerms",
			initialTerm:    5,
			dbTerm:         ptr(int64(5)),
			dbError:        nil,
			expectedTerm:   5,
			expectedIsOpen: true,
		},
		{
			name:           "NoRow",
			initialTerm:    0,
			dbTerm:         nil,
			dbError:        sql.ErrNoRows,
			expectedTerm:   0,
			expectedIsOpen: true,
		},
		{
			name:           "QueryError",
			initialTerm:    0,
			dbTerm:         nil,
			dbError:        assert.AnError,
			expectedTerm:   0,
			expectedIsOpen: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock database
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Create writer
			w := NewWriter(db, logger, shardID, poolerID, 1000)
			if tt.initialTerm != 0 {
				w.SetLeaderTerm(tt.initialTerm)
			}

			// Setup mock expectation
			if tt.dbError != nil {
				mock.ExpectQuery("SELECT leader_term FROM multigres.heartbeat WHERE shard_id = \\$1").
					WithArgs(shardID).
					WillReturnError(tt.dbError)
			} else if tt.dbTerm != nil {
				rows := sqlmock.NewRows([]string{"leader_term"}).AddRow(*tt.dbTerm)
				mock.ExpectQuery("SELECT leader_term FROM multigres.heartbeat WHERE shard_id = \\$1").
					WithArgs(shardID).
					WillReturnRows(rows)
			}

			// Call Open()
			w.Open()

			// Verify state
			assert.Equal(t, tt.expectedIsOpen, w.IsOpen())
			assert.Equal(t, tt.expectedTerm, w.GetLeaderTerm())

			// Verify all expectations met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// ptr is a helper to get pointer to int64
func ptr(i int64) *int64 {
	return &i
}

// TestWriterOpen_AlreadyOpen tests that Open() is idempotent
func TestWriterOpen_AlreadyOpen(t *testing.T) {
	// Create mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	w := NewWriter(db, logger, shardID, poolerID, 1000)

	// Setup expectation for first Open()
	rows := sqlmock.NewRows([]string{"leader_term"}).AddRow(int64(5))
	mock.ExpectQuery("SELECT leader_term FROM multigres.heartbeat WHERE shard_id = \\$1").
		WithArgs(shardID).
		WillReturnRows(rows)

	// First Open() should query database
	w.Open()
	assert.True(t, w.IsOpen())

	// Second Open() should return immediately without querying (no more expectations)
	w.Open()
	assert.True(t, w.IsOpen())

	// Verify all expectations met (only one query)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestWriterClose_ResetsLeaderTerm tests that Close() resets leader_term to 0
func TestWriterClose_ResetsLeaderTerm(t *testing.T) {
	// Create mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	w := NewWriter(db, logger, shardID, poolerID, 1000)

	// Setup expectation: database returns term = 5
	rows := sqlmock.NewRows([]string{"leader_term"}).AddRow(int64(5))
	mock.ExpectQuery("SELECT leader_term FROM multigres.heartbeat WHERE shard_id = \\$1").
		WithArgs(shardID).
		WillReturnRows(rows)

	// Open should initialize term from database
	w.Open()
	assert.Equal(t, int64(5), w.GetLeaderTerm())

	// Close should reset term to 0
	w.Close()
	assert.False(t, w.IsOpen())
	assert.Equal(t, int64(0), w.GetLeaderTerm())

	// Verify all expectations met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestWriterReopenAfterClose tests that reopening after close initializes from database
func TestWriterReopenAfterClose(t *testing.T) {
	// Create mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := slog.Default()
	shardID := []byte("test-shard")
	poolerID := "test-pooler"

	w := NewWriter(db, logger, shardID, poolerID, 1000)

	// First Open: database returns term = 5
	rows1 := sqlmock.NewRows([]string{"leader_term"}).AddRow(int64(5))
	mock.ExpectQuery("SELECT leader_term FROM multigres.heartbeat WHERE shard_id = \\$1").
		WithArgs(shardID).
		WillReturnRows(rows1)

	w.Open()
	assert.Equal(t, int64(5), w.GetLeaderTerm())

	// Close (resets term to 0)
	w.Close()
	assert.Equal(t, int64(0), w.GetLeaderTerm())

	// Reopen: database now returns term = 10
	rows2 := sqlmock.NewRows([]string{"leader_term"}).AddRow(int64(10))
	mock.ExpectQuery("SELECT leader_term FROM multigres.heartbeat WHERE shard_id = \\$1").
		WithArgs(shardID).
		WillReturnRows(rows2)

	w.Open()
	assert.Equal(t, int64(10), w.GetLeaderTerm())

	// Verify all expectations met
	assert.NoError(t, mock.ExpectationsWereMet())
}
