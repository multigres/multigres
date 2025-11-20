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
	"fmt"
	"sync/atomic"

	"github.com/multigres/multigres/go/pools/connpool"
)

// DBConn wraps a *sql.Conn and implements the connpool.Connection interface.
// It tracks the connection's current state (settings, prepared statements, portals)
// and provides methods to apply, reset, and query the connection state.
type DBConn struct {
	// conn is the underlying database connection.
	conn *sql.Conn

	// currentState tracks the cumulative state of this connection.
	// ConnectionState has its own internal mutex for thread safety.
	currentState atomic.Pointer[connpool.ConnectionState]

	// closed tracks whether this connection has been closed.
	closed atomic.Bool
}

// NewDBConn creates a new DBConn wrapping the given sql.Conn.
// The connection starts with a clean (empty) state.
func NewDBConn(conn *sql.Conn) *DBConn {
	dbc := &DBConn{
		conn: conn,
	}
	dbc.currentState.Store(connpool.NewEmptyConnectionState())
	return dbc
}

// State returns the current state of this connection.
// This method is safe for concurrent access.
func (d *DBConn) State() *connpool.ConnectionState {
	return d.currentState.Load()
}

// IsClosed returns true if this connection has been closed.
func (d *DBConn) IsClosed() bool {
	return d.closed.Load()
}

// Close closes the underlying database connection and marks it as closed.
func (d *DBConn) Close() error {
	if !d.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Clean up the state
	if state := d.currentState.Load(); state != nil {
		state.Close()
		d.currentState.Store(nil)
	}

	// Close the underlying connection
	return d.conn.Close()
}

// ApplyState applies the given state to this connection by executing the necessary SQL commands.
// This includes:
// - Setting session variables (SET commands)
// - Creating prepared statements (PREPARE commands) - future
// - Creating portals (DECLARE CURSOR commands) - future
//
// After successful application, the connection's current state is updated to match.
func (d *DBConn) ApplyState(ctx context.Context, state *connpool.ConnectionState) error {
	if d.closed.Load() {
		return fmt.Errorf("cannot apply state to closed connection")
	}

	if state == nil {
		state = connpool.NewEmptyConnectionState()
	}

	// Generate SQL commands to apply the state
	sqlCommands := state.GenerateApplySQL()

	// Execute each SQL command
	for _, sqlCmd := range sqlCommands {
		_, err := d.conn.ExecContext(ctx, sqlCmd)
		if err != nil {
			return fmt.Errorf("failed to apply state SQL %q: %w", sqlCmd, err)
		}
	}

	// Update the current state
	d.currentState.Store(state.Clone())

	return nil
}

// ResetState resets the connection to a clean state by executing reset SQL commands.
// This includes:
// - RESET ALL - reset all session variables
// - DEALLOCATE ALL - deallocate all prepared statements
// - CLOSE ALL - close all portals/cursors
//
// After successful reset, the connection's current state becomes empty/clean.
func (d *DBConn) ResetState(ctx context.Context) error {
	if d.closed.Load() {
		return fmt.Errorf("cannot reset state on closed connection")
	}

	currentState := d.currentState.Load()
	if currentState == nil || currentState.IsClean() {
		// Already clean, nothing to do
		return nil
	}

	// Generate SQL commands to reset the state
	sqlCommands := currentState.GenerateResetSQL()

	// Execute each SQL command
	for _, sqlCmd := range sqlCommands {
		_, err := d.conn.ExecContext(ctx, sqlCmd)
		if err != nil {
			return fmt.Errorf("failed to reset state SQL %q: %w", sqlCmd, err)
		}
	}

	// Update to clean state
	d.currentState.Store(connpool.NewEmptyConnectionState())

	return nil
}

// Exec executes a query without returning any rows.
// This is a convenience method for executing statements on the underlying connection.
func (d *DBConn) Exec(ctx context.Context, query string, args ...interface{}) error {
	if d.closed.Load() {
		return fmt.Errorf("cannot execute on closed connection")
	}

	_, err := d.conn.ExecContext(ctx, query, args...)
	return err
}

// Query executes a query that returns rows.
// This is a convenience method for querying the underlying connection.
func (d *DBConn) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if d.closed.Load() {
		return nil, fmt.Errorf("cannot query on closed connection")
	}

	return d.conn.QueryContext(ctx, query, args...)
}

// Conn returns the underlying *sql.Conn.
// This should be used carefully as direct access bypasses state tracking.
func (d *DBConn) Conn() *sql.Conn {
	return d.conn
}
