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
	"sort"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/admin"
)

// errStreamingAlreadyStarted is a sentinel used internally to signal that a
// streaming query's callback was already invoked, so retrying would duplicate
// rows. retryOnConnectionError treats it as a non-connection error and stops.
var errStreamingAlreadyStarted = errors.New("streaming callback already invoked, cannot retry")

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

// ApplySettings transitions the connection to the desired settings state.
// It diffs current tracked settings against desired: executes individual RESET
// commands for removed variables, then SET SESSION commands for all desired
// variables. This is safe inside transactions (individual RESETs don't destroy
// SET LOCAL settings, unlike RESET ALL).
func (c *Conn) ApplySettings(ctx context.Context, desired *connstate.Settings) error {
	current := c.State().GetSettings()

	// If desired is nil/empty, reset all current settings to reach a clean state.
	if desired == nil || desired.IsEmpty() {
		if current == nil || current.IsEmpty() {
			return nil
		}
		return c.ResetAllSettings(ctx)
	}

	// Build SQL: RESET removed variables, then SET desired variables.
	var b strings.Builder

	appendStmt := func(sql string) {
		if b.Len() > 0 {
			b.WriteString("; ")
		}
		b.WriteString(sql)
	}

	// RESET variables present in current but absent from desired. Always restore
	// the authenticated identity before applying ordinary desired GUCs: the
	// current role may not have permission to replay a setting that was accepted
	// earlier while the session was privileged. ApplyQuery restores the desired
	// session authorization and role after all ordinary GUCs.
	if current != nil {
		if _, had := current.Vars["role"]; had {
			appendStmt("RESET ROLE")
		}
		if _, had := current.Vars["session_authorization"]; had {
			appendStmt("RESET SESSION AUTHORIZATION")
		}

		var resetKeys []string
		for name := range current.Vars {
			switch connstate.CanonicalGUCName(name) {
			case "role", "session_authorization":
				continue
			}
			if _, ok := desired.Vars[name]; !ok {
				resetKeys = append(resetKeys, name)
			}
		}
		sort.Strings(resetKeys)
		for _, name := range resetKeys {
			appendStmt("RESET " + ast.QuoteQualifiedIdentifier(name))
		}
	}

	// SET all desired variables.
	applySQL := desired.ApplyQuery()
	if applySQL != "" {
		appendStmt(applySQL)
	}

	if b.Len() == 0 {
		return nil
	}

	_, err := c.Query(ctx, b.String())
	if err != nil {
		return fmt.Errorf("failed to apply settings: %w", err)
	}

	// Update tracked state.
	c.State().SetSettings(desired)
	return nil
}

// ResetAllSettings resets the connection to a clean state.
//
// Executes RESET ROLE, RESET SESSION AUTHORIZATION, then RESET ALL.
// These explicit resets must come first because PostgreSQL marks
// "role" and "session_authorization" with GUC_NO_RESET_ALL
// (src/backend/utils/misc/guc_tables.c), meaning RESET ALL
// intentionally skips them.
//
// Without the explicit resets, a pooled connection that had SET ROLE
// or SET SESSION AUTHORIZATION applied will retain those values after
// RESET ALL. If that role was subsequently dropped (e.g. by test
// cleanup), the next query on the connection fails with
// "role NNNNN was concurrently dropped".
//
// This matches what DISCARD ALL does internally
// (src/backend/commands/discard.c), but works inside transactions
// where DISCARD ALL cannot be used.
func (c *Conn) ResetAllSettings(ctx context.Context) error {
	state := c.State()
	if state == nil {
		return nil
	}

	settings := state.GetSettings()
	if settings == nil || settings.IsEmpty() {
		return nil
	}

	// Use the settings' ResetQuery which includes RESET ROLE and
	// RESET SESSION AUTHORIZATION before RESET ALL (GUC_NO_RESET_ALL).
	_, err := c.Query(ctx, settings.ResetQuery())
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

// SetPassthroughRow toggles opaque row passthrough on the underlying
// connection: when true, query responses keep raw DataRow frames instead of
// parsing them into columns. See client.Conn.SetPassthroughRow.
//
// Nil-safe: a reserved connection released during a failover can expose a nil
// underlying connection (and reservedConnAPI mocks return a nil *Conn), so a
// reset of the flag on such a connection is a harmless no-op rather than a
// panic.
func (c *Conn) SetPassthroughRow(v bool) {
	if c == nil || c.conn == nil {
		return
	}
	c.conn.SetPassthroughRow(v)
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
func (c *Conn) BindAndExecute(ctx context.Context, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (completed bool, err error) {
	return execWithContextCancel(c, ctx, func() (bool, error) {
		return c.conn.BindAndExecute(ctx, portalName, stmtName, params, paramFormats, resultFormats, maxRows, callback)
	})
}

// BindAndDescribe binds parameters and describes the resulting portal.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) BindAndDescribe(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16) (*query.StatementDescription, error) {
	return execWithContextCancel(c, ctx, func() (*query.StatementDescription, error) {
		return c.conn.BindAndDescribe(ctx, stmtName, params, paramFormats, resultFormats)
	})
}

// BindDescribeAndExecute fuses Bind+Describe(P)+Execute+Sync into a single
// backend round trip. The portal RowDescription rides back through the
// streaming callback's first Fields-bearing chunk, mirroring how the
// standalone Describe path delivers it.
// If the context is cancelled, the backend query is cancelled via adminPool.
func (c *Conn) BindDescribeAndExecute(ctx context.Context, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (bool, error) {
	return execWithContextCancel(c, ctx, func() (bool, error) {
		return c.conn.BindDescribeAndExecute(ctx, portalName, stmtName, params, paramFormats, resultFormats, maxRows, callback)
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
func (c *Conn) TxnStatus() protocol.TransactionStatus {
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
// query is retried, up to constants.MaxConnPoolRetryAttempts total attempts.
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
// connection error. If the callback has already been invoked (i.e., streaming
// has started delivering results), the error is returned without retry to
// avoid sending duplicate rows to the caller.
func (c *Conn) QueryStreamingWithRetry(ctx context.Context, sql string, callback func(context.Context, *sqltypes.Result) error) error {
	var callbackInvoked bool
	var streamErr error
	wrappedCallback := func(ctx context.Context, result *sqltypes.Result) error {
		callbackInvoked = true
		return callback(ctx, result)
	}
	_, err := retryOnConnectionError(c, ctx, func() (struct{}, error) {
		if callbackInvoked {
			// Callback was already called in a previous attempt — retrying
			// would replay the query and send duplicate rows. Return the
			// sentinel to stop the retry loop; we swap it for the real
			// error below.
			return struct{}{}, errStreamingAlreadyStarted
		}
		streamErr = c.conn.QueryStreaming(ctx, sql, wrappedCallback)
		return struct{}{}, streamErr
	})
	// Replace the internal sentinel with the actual PostgreSQL error so
	// callers can inspect it via errors.As / errors.Is.
	if errors.Is(err, errStreamingAlreadyStarted) {
		return mterrors.Wrapf(streamErr, "streaming already started, cannot retry")
	}
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
// retried, up to constants.MaxConnPoolRetryAttempts total. The connection is closed after
// exhausting all attempts or if reconnection fails.
func retryOnConnectionError[T any](c *Conn, ctx context.Context, op func() (T, error)) (T, error) {
	for attempt := 1; attempt <= constants.MaxConnPoolRetryAttempts; attempt++ {
		val, err := execOnce(c, ctx, op)
		switch {
		case err == nil:
			return val, nil
		case !mterrors.IsConnectionError(err):
			if mterrors.IsConnectionDead(err) {
				c.conn.Close()
			}
			var zero T
			return zero, err
		case attempt == constants.MaxConnPoolRetryAttempts:
			c.conn.Close()
			var zero T
			return zero, err
		}
		if ctx.Err() != nil {
			var zero T
			return zero, context.Cause(ctx)
		}
		// Brief backoff before reconnecting to give PostgreSQL time to
		// finish starting up if the error is due to a restart.
		backoffTimer := time.NewTimer(constants.ConnPoolRetryBackoff)
		select {
		case <-backoffTimer.C:
		case <-ctx.Done():
			backoffTimer.Stop()
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
// If cancellation fails (e.g. admin pool unavailable, PostgreSQL unreachable),
// the connection is force-closed to unblock the goroutine that is mid-read/write.
//
// ForceClose is used instead of Close because the op goroutine may be holding
// bufmu and writing to the buffered writer; Close would race by also writing
// a Terminate message to the same writer without the lock.
func (c *Conn) handleContextCancellation() {
	if c.adminPool == nil {
		c.conn.ForceClose()
		return
	}
	// Use the connection's context with a timeout for the cancel operation.
	// If the connection is closed, there's no need to cancel the query.
	cancelCtx, cancel := context.WithTimeout(c.conn.Context(), admin.DefaultCancelTimeout)
	defer cancel()
	ok, err := c.adminPool.CancelBackend(cancelCtx, c.ProcessID())
	if err != nil || !ok {
		c.conn.ForceClose()
	}
}

// postCancelDrainGrace bounds how long we wait for an in-flight operation to
// unwind after handleContextCancellation has requested a backend cancel.
// pg_cancel_backend only reports that the cancel signal was *delivered*, not
// that the statement actually stopped (PostgreSQL acts on it at
// CHECK_FOR_INTERRUPTS points, and cancel requests can be lost or race). Without
// an upper bound, a hung statement that never honors the cancel would block the
// caller forever, ignoring the already-expired context deadline.
const postCancelDrainGrace = 2 * time.Second

// drainOpAfterCancel waits for the op goroutine (publishing on ch) to return
// after a backend cancel has been requested. Because pg_cancel_backend is
// best-effort, the statement may never abort on its own; if it does not drain
// within postCancelDrainGrace, the connection is force-closed so the op unwinds
// promptly on the broken socket. This guarantees the call returns near the
// context deadline rather than hanging while holding the connection (which would
// also block subsequent attempts that need it).
func drainOpAfterCancel[T any](c *Conn, ch <-chan T) T {
	timer := time.NewTimer(postCancelDrainGrace)
	defer timer.Stop()
	select {
	case res := <-ch:
		return res
	case <-timer.C:
		c.conn.ForceClose()
		return <-ch
	}
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
		// Context cancelled - cancel the backend query, then wait (bounded) for
		// the operation to drain. drainOpAfterCancel force-closes the connection
		// if the cancel does not take effect within the grace period, so this
		// never blocks past the deadline.
		c.handleContextCancellation()
		drainOpAfterCancel(c, ch)
		var zero T
		return zero, context.Cause(ctx)
	case res := <-ch:
		return res.val, res.err
	}
}

// execWithContextCancel executes an operation with context cancellation support.
// If the context is cancelled while the operation is in progress, the backend
// query is cancelled via adminPool. If the session is dead, the connection is
// closed so the pool can replace it.
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
		// Context cancelled - cancel the backend query, then wait (bounded) for
		// the operation to drain. drainOpAfterCancel force-closes the connection
		// if the cancel does not take effect within the grace period, so this
		// never blocks past the deadline.
		c.handleContextCancellation()
		res := drainOpAfterCancel(c, ch)
		// If the operation killed the session, close the connection.
		if mterrors.IsConnectionDead(res.err) {
			c.conn.Close()
		}
		var zero T
		return zero, context.Cause(ctx)
	case res := <-ch:
		// Operation completed - discard dead sessions.
		if mterrors.IsConnectionDead(res.err) {
			c.conn.Close()
		}
		return res.val, res.err
	}
}

// --- COPY FROM STDIN operations ---

// InitiateCopyFromStdin sends a COPY FROM STDIN command and reads the CopyInResponse.
// Returns the COPY format, column formats, and any NoticeResponse diagnostics
// received before the CopyInResponse.
func (c *Conn) InitiateCopyFromStdin(ctx context.Context, copyQuery string) (format int16, columnFormats []int16, notices []*mterrors.PgDiagnostic, err error) {
	return c.conn.InitiateCopyFromStdin(ctx, copyQuery)
}

// InitiateCopyToStdout sends a COPY TO STDOUT command and reads the CopyOutResponse.
// Returns the COPY format, column formats, and any NoticeResponse diagnostics
// received before the CopyOutResponse.
func (c *Conn) InitiateCopyToStdout(ctx context.Context, copyQuery string) (format int16, columnFormats []int16, notices []*mterrors.PgDiagnostic, err error) {
	return c.conn.InitiateCopyToStdout(ctx, copyQuery)
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
// Returns the command tag, rows affected, and any NoticeResponse diagnostics
// received between CopyDone and ReadyForQuery (e.g. trigger output).
func (c *Conn) ReadCopyDoneResponse(ctx context.Context) (string, uint64, []*mterrors.PgDiagnostic, error) {
	return c.conn.ReadCopyDoneResponse(ctx)
}

// ReadCopyOutMessage reads the next message in a COPY TO STDOUT response
// stream: a CopyData chunk, a NoticeResponse diagnostic, or CopyDone.
func (c *Conn) ReadCopyOutMessage(ctx context.Context) (client.CopyOutMessage, error) {
	return c.conn.ReadCopyOutMessage(ctx)
}

// FinishCopyToStdout consumes the trailing CommandComplete + ReadyForQuery
// that PG sends after CopyDone in a COPY TO STDOUT flow.
func (c *Conn) FinishCopyToStdout(ctx context.Context) (string, uint64, []*mterrors.PgDiagnostic, error) {
	return c.conn.FinishCopyToStdout(ctx)
}

// ReadCopyFailResponse reads the expected ErrorResponse + ReadyForQuery
// sequence after sending CopyFail, leaving the connection in a clean state.
// Returns any NoticeResponse diagnostics seen before the trailing RFQ.
func (c *Conn) ReadCopyFailResponse(ctx context.Context) ([]*mterrors.PgDiagnostic, error) {
	return c.conn.ReadCopyFailResponse(ctx)
}

// WriteCopyFail sends a CopyFail message to abort the COPY operation.
func (c *Conn) WriteCopyFail(errorMsg string) error {
	return c.conn.WriteCopyFail(errorMsg)
}
