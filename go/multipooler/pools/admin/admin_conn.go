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
	"errors"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
)

// maxQueryAttempts is the maximum number of attempts for retrying queries.
const maxQueryAttempts = 3

// retryBackoff is the delay between retry attempts. This gives PostgreSQL
// time to finish starting up when the connection error is due to a restart.
const retryBackoff = 100 * time.Millisecond

// DefaultCancelTimeout is the default timeout for cancel/terminate operations.
// This is used when cancelling a backend query due to context cancellation.
const DefaultCancelTimeout = 5 * time.Second

// ErrUserNotFound is returned when a requested PostgreSQL role doesn't exist in pg_authid.
var ErrUserNotFound = errors.New("user not found in pg_authid")

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

// GetRolPassword retrieves the role password hash from pg_authid for a given username.
// Returns the SCRAM-SHA-256 password hash, or empty string if the user has no password set.
// Returns an error if the user doesn't exist or if the query fails.
//
// This is used during authentication to retrieve password hashes for SCRAM-SHA-256 verification.
func (c *Conn) GetRolPassword(ctx context.Context, username string) (string, error) {
	// Query pg_authid for the user's password hash.
	sql := fmt.Sprintf("SELECT rolpassword FROM pg_catalog.pg_authid WHERE rolname = %s LIMIT 1",
		ast.QuoteStringLiteral(username))

	results, err := c.queryWithRetry(ctx, sql)
	if err != nil {
		return "", fmt.Errorf("failed to query role password: %w", err)
	}

	// Check if user exists
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return "", fmt.Errorf("%w: %q", ErrUserNotFound, username)
	}

	// Extract the password hash (may be NULL/empty if no password set)
	var scramHash string
	if len(results[0].Rows[0].Values) > 0 && !results[0].Rows[0].Values[0].IsNull() {
		scramHash = string(results[0].Rows[0].Values[0])
	}

	return scramHash, nil
}

// queryWithRetry executes a query with automatic retry on connection error.
// If the first attempt fails with a connection error, it reconnects and retries
// up to maxQueryAttempts total. This handles stale connections that occur when
// PostgreSQL restarts while the pool holds old socket FDs.
func (c *Conn) queryWithRetry(ctx context.Context, sql string) ([]*sqltypes.Result, error) {
	for attempt := 1; attempt <= maxQueryAttempts; attempt++ {
		results, err := c.conn.Query(ctx, sql)
		switch {
		case err == nil:
			return results, nil
		case !mterrors.IsConnectionError(err):
			return nil, err
		case attempt == maxQueryAttempts:
			c.conn.Close()
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, err
		}
		// Brief backoff before reconnecting to give PostgreSQL time to
		// finish starting up if the error is due to a restart.
		select {
		case <-time.After(retryBackoff):
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
		if reconnectErr := c.conn.Reconnect(ctx); reconnectErr != nil {
			c.conn.Close()
			return nil, reconnectErr
		}
	}
	panic("unreachable")
}

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
// to abort the operation. The underlying query uses queryWithRetry for automatic
// reconnection on stale connections.
func (c *Conn) execBackendFunc(ctx context.Context, sql, operation string, processID uint32) (bool, error) {
	// Run query in goroutine so we can respect context cancellation.
	ch := make(chan queryResult, 1)
	go func() {
		results, err := c.queryWithRetry(ctx, sql)
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
