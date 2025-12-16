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

// Package admin provides administrative connection management for kill operations.
package admin

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/pgprotocol/client"
)

// Conn wraps a client.Conn for administrative operations.
// It implements connpool.Connection with no settings support.
//
// AdminConn provides the ability to terminate other backend connections using
// pg_terminate_backend() and pg_cancel_backend().
type Conn struct {
	// conn is the underlying PostgreSQL connection.
	conn *client.Conn
}

// NewConn creates a new AdminConn wrapping the given client connection.
func NewConn(conn *client.Conn) *Conn {
	return &Conn{
		conn: conn,
	}
}

// --- connpool.Connection interface ---

// Settings returns nil because admin connections don't use settings-based routing.
func (c *Conn) Settings() *connstate.Settings {
	return nil
}

// IsClosed returns true if the connection has been closed.
func (c *Conn) IsClosed() bool {
	return c.conn.IsClosed()
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// ApplySettings panics because admin connections don't support settings.
// This should never be called - admin connections are always "clean".
func (c *Conn) ApplySettings(_ context.Context, _ *connstate.Settings) error {
	panic("admin connections do not support ApplySettings")
}

// ResetSettings is a no-op because admin connections don't have settings.
func (c *Conn) ResetSettings(_ context.Context) error {
	return nil
}

// --- Admin operations ---

// TerminateBackend terminates a backend process using pg_terminate_backend().
// Returns true if the backend was terminated, false if it was not found or
// the caller lacks permission.
//
// If the context is cancelled while waiting for the query, the connection is
// closed to abort the operation, and the context error is returned.
func (c *Conn) TerminateBackend(ctx context.Context, processID uint32) (bool, error) {
	sql := fmt.Sprintf("SELECT pg_terminate_backend(%d)", processID)
	return c.execBackendFunc(ctx, sql, "terminate", processID)
}

// CancelBackend cancels the current query on a backend process using pg_cancel_backend().
// This sends SIGINT to the backend, canceling the current query but keeping the connection.
// Returns true if the signal was sent, false if the backend was not found or
// the caller lacks permission.
//
// If the context is cancelled while waiting for the query, the connection is
// closed to abort the operation, and the context error is returned.
func (c *Conn) CancelBackend(ctx context.Context, processID uint32) (bool, error) {
	sql := fmt.Sprintf("SELECT pg_cancel_backend(%d)", processID)
	return c.execBackendFunc(ctx, sql, "cancel", processID)
}

// queryResult holds the result of a query execution.
type queryResult struct {
	success bool
	err     error
}

// execBackendFunc executes a pg_*_backend() function with context cancellation support.
// If the context is cancelled while the query is running, the connection is closed
// to abort the operation.
func (c *Conn) execBackendFunc(ctx context.Context, sql, operation string, processID uint32) (bool, error) {
	// Run query in goroutine so we can respect context cancellation.
	ch := make(chan queryResult, 1)
	go func() {
		results, err := c.conn.Query(ctx, sql)
		if err != nil {
			ch <- queryResult{err: fmt.Errorf("failed to %s backend %d: %w", operation, processID, err)}
			return
		}

		// pg_*_backend returns a boolean indicating success.
		success := false
		if len(results) > 0 && len(results[0].Rows) > 0 && len(results[0].Rows[0].Values) > 0 {
			// The result is 't' for true, 'f' for false.
			val := string(results[0].Rows[0].Values[0])
			success = val == "t"
		}
		ch <- queryResult{success: success}
	}()

	select {
	case <-ctx.Done():
		// Context cancelled - close the connection to abort the query.
		// This ensures we don't leave a hung query on the server.
		c.conn.Close()
		return false, context.Cause(ctx)
	case result := <-ch:
		return result.success, result.err
	}
}

// Ensure Conn implements connpool.Connection.
var _ connpool.Connection = (*Conn)(nil)
