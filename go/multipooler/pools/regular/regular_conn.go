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

// Package regular provides regular connection management with session state.
package regular

import (
	"context"
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/admin"
	"github.com/multigres/multigres/go/pb/query"
)

// maxQueryAttempts is the maximum number of attempts for retrying queries.
// On connection error, the connection is reconnected and the query retried.
const maxQueryAttempts = 3

// Conn wraps a client.Conn with session state management.
// It implements the connpool.Connection interface for settings-based pool routing.
//
// Key features:
//   - Manages ConnectionState (Settings, PreparedStatements, Portals)
//   - Holds reference to AdminPool for self-kill capability
//   - Delegates query execution to the underlying client.Conn
type Conn struct {
	// conn is the underlying PostgreSQL connection.
	conn *client.Conn

	// adminPool is used for kill operations.
	// This allows the connection to terminate itself if needed.
	adminPool *admin.Pool
}

// NewConn creates a new regular connection wrapping the given client connection.
// The connection's state is stored in conn.state as *connstate.ConnectionState.
func NewConn(conn *client.Conn, adminPool *admin.Pool) *Conn {
	// Initialize connection state if not already set.
	if conn.GetConnectionState() == nil {
		conn.SetConnectionState(connstate.NewConnectionState())
	}

	return &Conn{
		conn:      conn,
		adminPool: adminPool,
	}
}

// --- connpool.Connection interface ---

// Settings returns the current settings applied to this connection.
// Returns nil if the connection has no settings applied (clean connection).
func (c *Conn) Settings() *connstate.Settings {
	state := c.State()
	if state == nil {
		return nil
	}
	return state.GetSettings()
}

// IsClosed returns true if the connection has been closed.
func (c *Conn) IsClosed() bool {
	return c.conn.IsClosed()
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	// Clean up state.
	if state := c.State(); state != nil {
		state.Close()
	}

	return c.conn.Close()
}

// ApplySettings applies the given settings to the connection.
// This executes SET commands for each variable in the settings.
func (c *Conn) ApplySettings(ctx context.Context, settings *connstate.Settings) error {
	if settings == nil || settings.IsEmpty() {
		return nil
	}

	// Generate and execute the SET commands.
	sql := settings.ApplyQuery()
	if sql == "" {
		return nil
	}

	_, err := c.Query(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to apply settings: %w", err)
	}

	// Update state.
	c.State().SetSettings(settings)
	return nil
}

// ResetSettings resets the connection to a clean state.
// This executes RESET ALL to clear all session variables.
func (c *Conn) ResetSettings(ctx context.Context) error {
	state := c.State()
	if state == nil {
		return nil
	}

	settings := state.GetSettings()
	if settings == nil || settings.IsEmpty() {
		return nil
	}

	// Execute RESET ALL.
	_, err := c.Query(ctx, "RESET ALL")
	if err != nil {
		return fmt.Errorf("failed to reset settings: %w", err)
	}

	// Update state.
	state.SetSettings(nil)
	return nil
}

// --- State management ---

// State returns the connection's state.
// This is stored in the underlying client.Conn.state field.
func (c *Conn) State() *connstate.ConnectionState {
	state := c.conn.GetConnectionState()
	if state == nil {
		c.conn.SetConnectionState(connstate.NewConnectionState())
		state = c.conn.GetConnectionState()
	}
	return state.(*connstate.ConnectionState)
}

// --- Query execution ---

// Query executes a simple query and returns all results.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) Query(ctx context.Context, sql string) ([]*sqltypes.Result, error) {
	return execWithContextCancel(c, ctx, func() ([]*sqltypes.Result, error) {
		return c.conn.Query(ctx, sql)
	})
}

// QueryStreaming executes a query with streaming results via callback.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) QueryStreaming(ctx context.Context, sql string, callback func(context.Context, *sqltypes.Result) error) error {
	// Use a struct{} as the value type since we only care about the error.
	_, err := execWithContextCancel(c, ctx, func() (struct{}, error) {
		return struct{}{}, c.conn.QueryStreaming(ctx, sql, callback)
	})
	return err
}

// --- Extended query protocol ---

// Parse sends a Parse message to prepare a statement.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) Parse(ctx context.Context, name, queryStr string, paramTypes []uint32) error {
	_, err := execWithContextCancel(c, ctx, func() (struct{}, error) {
		return struct{}{}, c.conn.Parse(ctx, name, queryStr, paramTypes)
	})
	return err
}

// BindAndExecute binds parameters and executes atomically.
// Returns true if the execution completed (CommandComplete), false if suspended (PortalSuspended).
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) BindAndExecute(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (completed bool, err error) {
	return execWithContextCancel(c, ctx, func() (bool, error) {
		return c.conn.BindAndExecute(ctx, stmtName, params, paramFormats, resultFormats, maxRows, callback)
	})
}

// BindAndDescribe binds parameters and describes the resulting portal.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) BindAndDescribe(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16) (*query.StatementDescription, error) {
	return execWithContextCancel(c, ctx, func() (*query.StatementDescription, error) {
		return c.conn.BindAndDescribe(ctx, stmtName, params, paramFormats, resultFormats)
	})
}

// DescribePrepared describes a prepared statement.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) DescribePrepared(ctx context.Context, name string) (*query.StatementDescription, error) {
	return execWithContextCancel(c, ctx, func() (*query.StatementDescription, error) {
		return c.conn.DescribePrepared(ctx, name)
	})
}

// CloseStatement closes a prepared statement.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) CloseStatement(ctx context.Context, name string) error {
	_, err := execWithContextCancel(c, ctx, func() (struct{}, error) {
		return struct{}{}, c.conn.CloseStatement(ctx, name)
	})
	return err
}

// ClosePortal closes a portal.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) ClosePortal(ctx context.Context, name string) error {
	_, err := execWithContextCancel(c, ctx, func() (struct{}, error) {
		return struct{}{}, c.conn.ClosePortal(ctx, name)
	})
	return err
}

// Sync sends a Sync message to synchronize the extended query protocol.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) Sync(ctx context.Context) error {
	_, err := execWithContextCancel(c, ctx, func() (struct{}, error) {
		return struct{}{}, c.conn.Sync(ctx)
	})
	return err
}

// PrepareAndExecute is a convenience method that prepares and executes in one round trip.
// name is the statement/portal name (use "" for unnamed, which is cleared after Sync).
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) PrepareAndExecute(ctx context.Context, name, queryStr string, params [][]byte, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	_, err := execWithContextCancel(c, ctx, func() (struct{}, error) {
		return struct{}{}, c.conn.PrepareAndExecute(ctx, name, queryStr, params, callback)
	})
	return err
}

// QueryArgs executes a parameterized query using the extended query protocol.
// This is a convenience method that accepts Go values as arguments and converts
// them to the appropriate text format for PostgreSQL.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) QueryArgs(ctx context.Context, queryStr string, args ...any) ([]*sqltypes.Result, error) {
	return execWithContextCancel(c, ctx, func() ([]*sqltypes.Result, error) {
		return c.conn.QueryArgs(ctx, queryStr, args...)
	})
}

// Execute continues execution of a previously bound portal.
// This is used to fetch more rows from a portal that was executed with maxRows > 0
// and returned PortalSuspended.
// Returns true if the portal completed (CommandComplete), false if suspended (PortalSuspended).
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) Execute(ctx context.Context, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (completed bool, err error) {
	return execWithContextCancel(c, ctx, func() (bool, error) {
		return c.conn.Execute(ctx, portalName, maxRows, callback)
	})
}

// --- Transaction status ---

// TxnStatus returns the current transaction status.
// Returns one of: 'I' (idle), 'T' (in transaction), 'E' (error).
func (c *Conn) TxnStatus() byte {
	return c.conn.TxnStatus()
}

// IsIdle returns true if the connection is idle (not in a transaction).
func (c *Conn) IsIdle() bool {
	return c.conn.TxnStatus() == protocol.TxnStatusIdle
}

// IsInTransaction returns true if the connection is in a transaction.
func (c *Conn) IsInTransaction() bool {
	status := c.conn.TxnStatus()
	return status == protocol.TxnStatusInBlock || status == protocol.TxnStatusFailed
}

// --- Backend info ---

// ProcessID returns the backend process ID.
func (c *Conn) ProcessID() uint32 {
	return c.conn.ProcessID()
}

// SecretKey returns the backend secret key for query cancellation.
func (c *Conn) SecretKey() uint32 {
	return c.conn.SecretKey()
}

// --- Kill capability ---

// Kill terminates this connection's backend process using pg_terminate_backend().
// Requires adminPool to be set; returns error if adminPool is nil.
func (c *Conn) Kill(ctx context.Context) error {
	if c.adminPool == nil {
		return errors.New("cannot kill connection: admin pool not configured")
	}
	_, err := c.adminPool.TerminateBackend(ctx, c.ProcessID())
	return err
}

// --- Underlying connection access ---

// RawConn returns the underlying client.Conn.
// Use with caution - prefer the wrapped methods.
func (c *Conn) RawConn() *client.Conn {
	return c.conn
}

// --- Reconnect ---

// Reconnect closes the underlying connection and establishes a fresh one,
// preserving the same *Conn identity. After reconnecting the socket and
// completing the PostgreSQL startup handshake, any previously applied
// session settings are re-applied. Prepared statements are cleared since
// they don't survive a PostgreSQL session reset.
func (c *Conn) Reconnect(ctx context.Context) error {
	// Save settings before reconnecting. The PostgreSQL session will be
	// brand new, so we need to re-apply them after startup.
	settings := c.State().GetSettings()

	// Reconnect the underlying socket in-place.
	if err := c.conn.Reconnect(ctx); err != nil {
		return err
	}

	// Reset connection state (new session = clean slate).
	// Prepared statements don't survive reconnection.
	c.conn.SetConnectionState(connstate.NewConnectionState())

	// Re-apply settings on the fresh connection via SET commands.
	// We use execOnce (not execWithContextCancel) so that a failure here
	// doesn't close the connection—the caller's retry loop can attempt
	// another reconnect instead.
	if settings != nil && !settings.IsEmpty() {
		sql := settings.ApplyQuery()
		if sql != "" {
			_, err := execOnce(c, ctx, func() ([]*sqltypes.Result, error) {
				return c.conn.Query(ctx, sql)
			})
			if err != nil {
				return fmt.Errorf("failed to re-apply settings after reconnect: %w", err)
			}
			c.State().SetSettings(settings)
		}
	}

	return nil
}

// --- Stateless query retry ---
//
// QueryWithRetry, QueryStreamingWithRetry and QueryArgsWithRetry execute queries
// with automatic reconnection on connection errors.
// On connection error the underlying socket is reconnected in-place and the
// query is retried, up to maxQueryAttempts total attempts.
//
// These methods are for stateless pool queries only. Stateful operations
// (transactions, reserved connections, extended query protocol) must use the
// non-retrying Query/QueryStreaming methods directly, because server-side
// state would be lost on reconnection.

// QueryWithRetry executes a simple query with automatic retry on connection error.
func (c *Conn) QueryWithRetry(ctx context.Context, sql string) ([]*sqltypes.Result, error) {
	return retryOnConnectionError(c, ctx, func() ([]*sqltypes.Result, error) {
		return c.conn.Query(ctx, sql)
	})
}

// QueryStreamingWithRetry executes a streaming query with automatic retry on
// connection error. The callback must not have produced observable side effects
// before the retry; if streaming has already started sending results, the error
// is returned without retry.
func (c *Conn) QueryStreamingWithRetry(ctx context.Context, sql string, callback func(context.Context, *sqltypes.Result) error) error {
	_, err := retryOnConnectionError(c, ctx, func() (struct{}, error) {
		return struct{}{}, c.conn.QueryStreaming(ctx, sql, callback)
	})
	return err
}

// QueryArgsWithRetry executes a parameterized query (via the extended query
// protocol) with automatic retry on connection error, like QueryWithRetry.
// Safe to retry because QueryArgs uses PrepareAndExecute which is a single
// atomic round trip with an unnamed statement—no multi-step state to lose.
func (c *Conn) QueryArgsWithRetry(ctx context.Context, sql string, args ...any) ([]*sqltypes.Result, error) {
	return retryOnConnectionError(c, ctx, func() ([]*sqltypes.Result, error) {
		return c.conn.QueryArgs(ctx, sql, args...)
	})
}

// retryOnConnectionError executes op with automatic retry on connection error.
// On connection error the underlying socket is reconnected in-place and op is
// retried, up to maxQueryAttempts total. The connection is closed after
// exhausting all attempts or if reconnection fails.
func retryOnConnectionError[T any](c *Conn, ctx context.Context, op func() (T, error)) (T, error) {
	for attempt := 1; attempt <= maxQueryAttempts; attempt++ {
		val, err := execOnce(c, ctx, op)
		switch {
		case err == nil:
			return val, nil
		case !mterrors.IsConnectionError(err):
			var zero T
			return zero, err
		case attempt == maxQueryAttempts:
			c.conn.Close()
			var zero T
			return zero, err
		}
		if ctx.Err() != nil {
			var zero T
			return zero, context.Cause(ctx)
		}
		if reconnectErr := c.Reconnect(ctx); reconnectErr != nil {
			c.conn.Close()
			var zero T
			return zero, reconnectErr
		}
	}
	panic("unreachable")
}

// --- Context-aware execution helpers ---

// handleContextCancellation cancels the backend query if adminPool is available.
// This is called when the context is cancelled while a query is in progress.
func (c *Conn) handleContextCancellation() {
	if c.adminPool == nil {
		return
	}
	// Use the connection's context with a timeout for the cancel operation.
	// If the connection is closed, there's no need to cancel the query.
	cancelCtx, cancel := context.WithTimeout(c.conn.Context(), admin.DefaultCancelTimeout)
	defer cancel()
	_, _ = c.adminPool.CancelBackend(cancelCtx, c.ProcessID())
}

// execOnce executes an operation with context cancellation support.
// Unlike execWithContextCancel, it does NOT close the connection on error,
// allowing the caller (retry loop) to reconnect and retry.
func execOnce[T any](c *Conn, ctx context.Context, op func() (T, error)) (T, error) {
	type result struct {
		val T
		err error
	}

	ch := make(chan result, 1)
	go func() {
		val, err := op()
		ch <- result{val: val, err: err}
	}()

	select {
	case <-ctx.Done():
		// Context cancelled - cancel the backend query.
		c.handleContextCancellation()
		// Wait for the operation to complete (it should return quickly after cancel).
		<-ch
		var zero T
		return zero, context.Cause(ctx)
	case res := <-ch:
		return res.val, res.err
	}
}

// execWithContextCancel executes an operation with context cancellation support.
// If the context is cancelled while the operation is in progress, the backend
// query is cancelled via adminPool. If a connection error occurs, the connection
// is closed so the pool can replace it.
//
// This is used by the non-retrying methods (Query, QueryStreaming, etc.) where
// the pool handles replacement of broken connections.
func execWithContextCancel[T any](c *Conn, ctx context.Context, op func() (T, error)) (T, error) {
	type result struct {
		val T
		err error
	}

	ch := make(chan result, 1)
	go func() {
		val, err := op()
		ch <- result{val: val, err: err}
	}()

	select {
	case <-ctx.Done():
		// Context cancelled - cancel the backend query.
		c.handleContextCancellation()
		// Wait for the operation to complete (it should return quickly after cancel).
		res := <-ch
		// If the operation had a connection error, close the connection.
		if mterrors.IsConnectionError(res.err) {
			c.conn.Close()
		}
		var zero T
		return zero, context.Cause(ctx)
	case res := <-ch:
		// Operation completed - check for connection errors.
		if mterrors.IsConnectionError(res.err) {
			c.conn.Close()
		}
		return res.val, res.err
	}
}

// --- COPY FROM STDIN operations ---

// InitiateCopyFromStdin sends a COPY FROM STDIN command and reads the CopyInResponse.
// Returns the COPY format and column formats.
func (c *Conn) InitiateCopyFromStdin(ctx context.Context, copyQuery string) (format int16, columnFormats []int16, err error) {
	return c.conn.InitiateCopyFromStdin(ctx, copyQuery)
}

// WriteCopyData writes a CopyData message to PostgreSQL.
func (c *Conn) WriteCopyData(data []byte) error {
	return c.conn.WriteCopyData(data)
}

// WriteCopyDone sends a CopyDone message to signal completion of COPY data.
func (c *Conn) WriteCopyDone() error {
	return c.conn.WriteCopyDone()
}

// ReadCopyDoneResponse reads the CommandComplete and ReadyForQuery after CopyDone.
// Returns the command tag and rows affected.
func (c *Conn) ReadCopyDoneResponse(ctx context.Context) (string, uint64, error) {
	return c.conn.ReadCopyDoneResponse(ctx)
}

// WriteCopyFail sends a CopyFail message to abort the COPY operation.
func (c *Conn) WriteCopyFail(errorMsg string) error {
	return c.conn.WriteCopyFail(errorMsg)
}
