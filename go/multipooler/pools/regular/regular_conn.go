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
	"fmt"

	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/admin"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/client"
	"github.com/multigres/multigres/go/pgprotocol/protocol"
)

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

	_, err := c.conn.Query(ctx, sql)
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
	_, err := c.conn.Query(ctx, "RESET ALL")
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
		return nil
	}
	return state.(*connstate.ConnectionState)
}

// --- Query execution ---

// Query executes a simple query and returns all results.
func (c *Conn) Query(ctx context.Context, sql string) ([]*query.QueryResult, error) {
	return c.conn.Query(ctx, sql)
}

// QueryStreaming executes a query with streaming results via callback.
func (c *Conn) QueryStreaming(ctx context.Context, sql string, callback func(context.Context, *query.QueryResult) error) error {
	return c.conn.QueryStreaming(ctx, sql, callback)
}

// --- Extended query protocol ---

// Parse sends a Parse message to prepare a statement.
func (c *Conn) Parse(ctx context.Context, name, queryStr string, paramTypes []uint32) error {
	return c.conn.Parse(ctx, name, queryStr, paramTypes)
}

// BindAndExecute binds parameters and executes atomically.
// Returns true if the execution completed (CommandComplete), false if suspended (PortalSuspended).
func (c *Conn) BindAndExecute(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16, maxRows int32, callback func(ctx context.Context, result *query.QueryResult) error) (completed bool, err error) {
	return c.conn.BindAndExecute(ctx, stmtName, params, paramFormats, resultFormats, maxRows, callback)
}

// BindAndDescribe binds parameters and describes the resulting portal.
func (c *Conn) BindAndDescribe(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16) (*query.StatementDescription, error) {
	return c.conn.BindAndDescribe(ctx, stmtName, params, paramFormats, resultFormats)
}

// DescribePrepared describes a prepared statement.
func (c *Conn) DescribePrepared(ctx context.Context, name string) (*query.StatementDescription, error) {
	return c.conn.DescribePrepared(ctx, name)
}

// CloseStatement closes a prepared statement.
func (c *Conn) CloseStatement(ctx context.Context, name string) error {
	return c.conn.CloseStatement(ctx, name)
}

// ClosePortal closes a portal.
func (c *Conn) ClosePortal(ctx context.Context, name string) error {
	return c.conn.ClosePortal(ctx, name)
}

// Sync sends a Sync message to synchronize the extended query protocol.
func (c *Conn) Sync(ctx context.Context) error {
	return c.conn.Sync(ctx)
}

// PrepareAndExecute is a convenience method that prepares and executes in one round trip.
// name is the statement/portal name (use "" for unnamed, which is cleared after Sync).
func (c *Conn) PrepareAndExecute(ctx context.Context, name, queryStr string, params [][]byte, callback func(ctx context.Context, result *query.QueryResult) error) error {
	return c.conn.PrepareAndExecute(ctx, name, queryStr, params, callback)
}

// QueryArgs executes a parameterized query using the extended query protocol.
// This is a convenience method that accepts Go values as arguments and converts
// them to the appropriate text format for PostgreSQL.
func (c *Conn) QueryArgs(ctx context.Context, queryStr string, args ...any) ([]*query.QueryResult, error) {
	return c.conn.QueryArgs(ctx, queryStr, args...)
}

// Execute continues execution of a previously bound portal.
// This is used to fetch more rows from a portal that was executed with maxRows > 0
// and returned PortalSuspended.
// Returns true if the portal completed (CommandComplete), false if suspended (PortalSuspended).
func (c *Conn) Execute(ctx context.Context, portalName string, maxRows int32, callback func(ctx context.Context, result *query.QueryResult) error) (completed bool, err error) {
	return c.conn.Execute(ctx, portalName, maxRows, callback)
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
		return fmt.Errorf("cannot kill connection: admin pool not configured")
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
