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

package dbconn

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pools/connpool"
)

// getTestDB returns a test database connection if PG_TEST_DSN is set.
// Tests that require a database should call this and skip if it returns nil.
func getTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("Skipping test that requires database (set PG_TEST_DSN to enable)")
		return nil
	}

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err, "failed to open test database")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	require.NoError(t, err, "failed to ping test database")

	return db
}

func TestNewDBConn(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer sqlConn.Close()

	dbConn := NewDBConn(sqlConn)
	require.NotNil(t, dbConn)

	// Should start with clean state
	state := dbConn.State()
	assert.NotNil(t, state)
	assert.True(t, state.IsClean())

	// Should not be closed
	assert.False(t, dbConn.IsClosed())
}

func TestDBConn_State(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer sqlConn.Close()

	dbConn := NewDBConn(sqlConn)

	// Initial state should be clean
	state := dbConn.State()
	assert.True(t, state.IsClean())

	// Apply a new state with settings
	newState := connpool.NewConnectionState(map[string]string{
		"timezone": "UTC",
	})
	err = dbConn.ApplyState(ctx, newState)
	require.NoError(t, err)

	// State should now match
	currentState := dbConn.State()
	assert.True(t, currentState.Equals(newState))
	assert.False(t, currentState.IsClean())
}

func TestDBConn_ApplyState(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer sqlConn.Close()

	dbConn := NewDBConn(sqlConn)

	tests := []struct {
		name     string
		state    *connpool.ConnectionState
		wantErr  bool
		validate func(t *testing.T, conn *DBConn)
	}{
		{
			name: "apply single setting",
			state: connpool.NewConnectionState(map[string]string{
				"timezone": "America/New_York",
			}),
			wantErr: false,
			validate: func(t *testing.T, conn *DBConn) {
				state := conn.State()
				assert.Equal(t, "America/New_York", state.Settings["timezone"])
			},
		},
		{
			name: "apply multiple settings",
			state: connpool.NewConnectionState(map[string]string{
				"timezone":    "UTC",
				"search_path": "public,pg_catalog",
			}),
			wantErr: false,
			validate: func(t *testing.T, conn *DBConn) {
				state := conn.State()
				assert.Equal(t, "UTC", state.Settings["timezone"])
				assert.Equal(t, "public,pg_catalog", state.Settings["search_path"])
			},
		},
		{
			name:    "apply nil state (clean)",
			state:   nil,
			wantErr: false,
			validate: func(t *testing.T, conn *DBConn) {
				state := conn.State()
				assert.True(t, state.IsClean())
			},
		},
		{
			name:    "apply empty state",
			state:   connpool.NewEmptyConnectionState(),
			wantErr: false,
			validate: func(t *testing.T, conn *DBConn) {
				state := conn.State()
				assert.True(t, state.IsClean())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dbConn.ApplyState(ctx, tt.state)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.validate != nil {
					tt.validate(t, dbConn)
				}
			}
		})
	}
}

func TestDBConn_ResetState(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer sqlConn.Close()

	dbConn := NewDBConn(sqlConn)

	// Apply some state
	state := connpool.NewConnectionState(map[string]string{
		"timezone": "America/Los_Angeles",
	})
	err = dbConn.ApplyState(ctx, state)
	require.NoError(t, err)

	// Verify state is applied
	currentState := dbConn.State()
	assert.False(t, currentState.IsClean())

	// Reset state
	err = dbConn.ResetState(ctx)
	require.NoError(t, err)

	// Verify state is now clean
	currentState = dbConn.State()
	assert.True(t, currentState.IsClean())
}

func TestDBConn_ResetStateWhenClean(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer sqlConn.Close()

	dbConn := NewDBConn(sqlConn)

	// State is already clean
	assert.True(t, dbConn.State().IsClean())

	// Reset should be a no-op
	err = dbConn.ResetState(ctx)
	require.NoError(t, err)

	// Should still be clean
	assert.True(t, dbConn.State().IsClean())
}

func TestDBConn_Close(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)

	dbConn := NewDBConn(sqlConn)

	// Should not be closed initially
	assert.False(t, dbConn.IsClosed())

	// Close the connection
	err = dbConn.Close()
	require.NoError(t, err)

	// Should be marked as closed
	assert.True(t, dbConn.IsClosed())

	// Closing again should be idempotent
	err = dbConn.Close()
	require.NoError(t, err)
	assert.True(t, dbConn.IsClosed())
}

func TestDBConn_OperationsOnClosedConnection(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)

	dbConn := NewDBConn(sqlConn)

	// Close the connection
	err = dbConn.Close()
	require.NoError(t, err)

	// Operations on closed connection should fail
	state := connpool.NewConnectionState(map[string]string{
		"timezone": "UTC",
	})
	err = dbConn.ApplyState(ctx, state)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed connection")

	err = dbConn.ResetState(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed connection")

	err = dbConn.Exec(ctx, "SELECT 1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed connection")

	_, err = dbConn.Query(ctx, "SELECT 1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed connection")
}

func TestDBConn_Exec(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer sqlConn.Close()

	dbConn := NewDBConn(sqlConn)

	// Execute a simple statement
	err = dbConn.Exec(ctx, "SELECT 1")
	assert.NoError(t, err)
}

func TestDBConn_Query(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer sqlConn.Close()

	dbConn := NewDBConn(sqlConn)

	// Execute a query
	rows, err := dbConn.Query(ctx, "SELECT 1 as num, 'test' as str")
	require.NoError(t, err)
	defer rows.Close()

	// Verify we got a row
	require.True(t, rows.Next())

	var num int
	var str string
	err = rows.Scan(&num, &str)
	require.NoError(t, err)
	assert.Equal(t, 1, num)
	assert.Equal(t, "test", str)

	// Should be no more rows
	assert.False(t, rows.Next())
}

func TestDBConn_ImplementsConnectionInterface(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer sqlConn.Close()

	dbConn := NewDBConn(sqlConn)

	// Verify that PooledDBConn implements connpool.Connection
	var _ connpool.Connection = dbConn

	// Test all interface methods
	state := dbConn.State()
	assert.NotNil(t, state)

	isClosed := dbConn.IsClosed()
	assert.False(t, isClosed)

	err = dbConn.ApplyState(ctx, connpool.NewEmptyConnectionState())
	assert.NoError(t, err)

	err = dbConn.ResetState(ctx)
	assert.NoError(t, err)

	err = dbConn.Close()
	assert.NoError(t, err)
}

func TestDBConn_ConcurrentStateAccess(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer sqlConn.Close()

	dbConn := NewDBConn(sqlConn)

	// Concurrent reads should be safe
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				state := dbConn.State()
				_ = state.IsClean()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestDBConn_StateCloning(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer sqlConn.Close()

	dbConn := NewDBConn(sqlConn)

	// Apply a state
	originalState := connpool.NewConnectionState(map[string]string{
		"timezone": "UTC",
	})
	err = dbConn.ApplyState(ctx, originalState)
	require.NoError(t, err)

	// Get the current state
	currentState := dbConn.State()

	// Modify the original state
	originalState.ApplySetting("search_path", "public")

	// Current state should not be affected (it was cloned)
	assert.NotContains(t, currentState.Settings, "search_path")
	assert.Equal(t, "UTC", currentState.Settings["timezone"])
}
