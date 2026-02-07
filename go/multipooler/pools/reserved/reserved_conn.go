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

package reserved

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/regular"
	"github.com/multigres/multigres/go/pb/query"
)

// Conn wraps a regular connection with transaction/reservation state.
// It provides a unique ID for client-side tracking across requests.
//
// Key features:
//   - Unique ConnID for resuming sessions across requests
//   - Transaction state tracking (BEGIN, COMMIT, ROLLBACK)
//   - Portal reservation for partial Execute operations
//   - Timeout enforcement for reserved connections
type Conn struct {
	// pooled is the underlying pooled regular connection.
	pooled regular.PooledConn

	// ConnID is the unique identifier for this reservation.
	// Clients use this to resume their session across requests.
	ConnID int64

	// pool is a back-reference to the owning pool.
	// Used for Release operations.
	pool *Pool

	// reservedProps tracks why the connection is reserved.
	reservedProps *ReservationProperties

	// inactivityTimeout is the maximum duration the connection can be inactive
	// (no client activity) before expiring. A value of 0 means no timeout.
	inactivityTimeout time.Duration

	// expiryNanos stores the expiry time as Unix nanoseconds for atomic access.
	expiryNanos atomic.Int64

	// released indicates whether this connection has been released.
	released atomic.Bool
}

// newConn creates a new reserved connection.
func newConn(pooled regular.PooledConn, connID int64, pool *Pool) *Conn {
	return &Conn{
		pooled: pooled,
		ConnID: connID,
		pool:   pool,
	}
}

// Conn returns the underlying regular connection.
func (c *Conn) Conn() *regular.Conn {
	return c.pooled.Conn
}

// State returns the connection's state.
func (c *Conn) State() *connstate.ConnectionState {
	return c.pooled.Conn.State()
}

// --- Transaction lifecycle ---

// Begin starts a transaction on this connection.
func (c *Conn) Begin(ctx context.Context) error {
	if c.IsInTransaction() {
		return errors.New("transaction already in progress")
	}

	_, err := c.pooled.Conn.Query(ctx, "BEGIN")
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	c.reservedProps = NewReservationProperties(ReservationTransaction)
	return nil
}

// Commit commits the current transaction.
func (c *Conn) Commit(ctx context.Context) error {
	if !c.IsInTransaction() {
		return errors.New("no active transaction")
	}

	_, err := c.pooled.Conn.Query(ctx, "COMMIT")
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	c.reservedProps = nil
	return nil
}

// Rollback rolls back the current transaction.
func (c *Conn) Rollback(ctx context.Context) error {
	if !c.IsInTransaction() {
		// No active transaction, but that's okay for rollback.
		return nil
	}

	_, err := c.pooled.Conn.Query(ctx, "ROLLBACK")
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	c.reservedProps = nil
	return nil
}

// IsInTransaction returns true if there's an active transaction.
func (c *Conn) IsInTransaction() bool {
	return c.reservedProps != nil && c.reservedProps.IsForTransaction()
}

// --- Portal reservations ---

// ReserveForPortal marks the connection as reserved for a portal.
// This is used when Execute returns suspended (portal not fully consumed).
// Multiple portals can be reserved on the same connection.
func (c *Conn) ReserveForPortal(portalName string) {
	if c.reservedProps == nil || !c.reservedProps.IsForPortal() {
		c.reservedProps = NewReservationProperties(ReservationPortal)
	}
	c.reservedProps.AddPortal(portalName)
}

// ReleasePortal removes a specific portal from the reservation.
// If no portals remain and the connection was reserved only for portals,
// the reservation is cleared entirely.
// Returns true if the connection should be released (no more reservations).
func (c *Conn) ReleasePortal(portalName string) bool {
	if c.reservedProps == nil || !c.reservedProps.IsForPortal() {
		return false
	}
	c.reservedProps.RemovePortal(portalName)
	if !c.reservedProps.HasPortals() {
		c.reservedProps = nil
		return true
	}
	return false
}

// ReleaseAllPortals clears all portal reservations.
// Does not affect transaction reservations.
func (c *Conn) ReleaseAllPortals() {
	if c.reservedProps != nil && c.reservedProps.IsForPortal() {
		c.reservedProps = nil
	}
}

// IsReservedForPortal returns true if reserved for any portal.
func (c *Conn) IsReservedForPortal() bool {
	return c.reservedProps != nil && c.reservedProps.IsForPortal()
}

// HasPortal returns true if the specified portal is reserved on this connection.
func (c *Conn) HasPortal(portalName string) bool {
	return c.reservedProps != nil && c.reservedProps.HasPortal(portalName)
}

// ReservedProps returns the reservation properties.
func (c *Conn) ReservedProps() *ReservationProperties {
	return c.reservedProps
}

// --- Timeout ---

// SetInactivityTimeout sets the inactivity timeout and resets the expiry time.
func (c *Conn) SetInactivityTimeout(timeout time.Duration) {
	c.inactivityTimeout = timeout
	c.ResetExpiryTime()
}

// ResetExpiryTime resets the expiry time based on the inactivity timeout.
// Called when the connection is accessed to extend its lifetime.
func (c *Conn) ResetExpiryTime() {
	if c.inactivityTimeout > 0 {
		c.expiryNanos.Store(time.Now().Add(c.inactivityTimeout).UnixNano())
	}
}

// IsTimedOut returns true if the connection has exceeded its inactivity timeout.
func (c *Conn) IsTimedOut() bool {
	if c.inactivityTimeout <= 0 {
		return false
	}
	return time.Now().UnixNano() > c.expiryNanos.Load()
}

// InactivityTimeout returns the inactivity timeout duration.
func (c *Conn) InactivityTimeout() time.Duration {
	return c.inactivityTimeout
}

// --- Lifecycle ---

// Release returns this connection to the pool.
// The reason indicates why the connection is being released.
func (c *Conn) Release(reason ReleaseReason) {
	if !c.released.CompareAndSwap(false, true) {
		return // Already released.
	}

	if c.pool != nil {
		c.pool.release(c, reason)
	}
}

// IsReleased returns true if the connection has been released.
func (c *Conn) IsReleased() bool {
	return c.released.Load()
}

// Kill cancels the current operation on this connection.
func (c *Conn) Kill(ctx context.Context) error {
	return c.pooled.Conn.Kill(ctx)
}

// Close closes the underlying connection without returning to pool.
// Unlike Release(), Close() does not recycle the connection - it permanently
// removes it from the pool and closes the socket.
// The pool slot is returned to the underlying connpool via Taint().
func (c *Conn) Close() error {
	if !c.released.CompareAndSwap(false, true) {
		return nil // Already released/closed.
	}

	// Remove from active map if we have a pool reference.
	if c.pool != nil {
		c.pool.mu.Lock()
		delete(c.pool.active, c.ConnID)
		c.pool.mu.Unlock()
	}

	// Taint the connection to return the pool slot to the underlying connpool.
	// This calls pool.put(nil) which decrements the active count.
	c.pooled.Taint()

	// Close the underlying socket.
	c.pooled.Close()
	return nil
}

// IsClosed returns true if the underlying connection is closed.
func (c *Conn) IsClosed() bool {
	return c.pooled.Conn.IsClosed()
}

// --- Backend info ---

// ProcessID returns the backend process ID.
func (c *Conn) ProcessID() uint32 {
	return c.pooled.Conn.ProcessID()
}

// SecretKey returns the backend secret key.
func (c *Conn) SecretKey() uint32 {
	return c.pooled.Conn.SecretKey()
}

// --- Query execution ---

// Query executes a simple query and returns all results.
func (c *Conn) Query(ctx context.Context, sql string) ([]*sqltypes.Result, error) {
	return c.pooled.Conn.Query(ctx, sql)
}

// QueryStreaming executes a query with streaming results via callback.
func (c *Conn) QueryStreaming(ctx context.Context, sql string, callback func(context.Context, *sqltypes.Result) error) error {
	return c.pooled.Conn.QueryStreaming(ctx, sql, callback)
}

// --- Extended query protocol ---

// Parse sends a Parse message to prepare a statement.
func (c *Conn) Parse(ctx context.Context, name, queryStr string, paramTypes []uint32) error {
	return c.pooled.Conn.Parse(ctx, name, queryStr, paramTypes)
}

// BindAndExecute binds parameters and executes atomically.
// Returns true if the execution completed (CommandComplete), false if suspended (PortalSuspended).
func (c *Conn) BindAndExecute(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (completed bool, err error) {
	return c.pooled.Conn.BindAndExecute(ctx, stmtName, params, paramFormats, resultFormats, maxRows, callback)
}

// BindAndDescribe binds parameters and describes the resulting portal.
func (c *Conn) BindAndDescribe(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16) (*query.StatementDescription, error) {
	return c.pooled.Conn.BindAndDescribe(ctx, stmtName, params, paramFormats, resultFormats)
}

// DescribePrepared describes a prepared statement.
func (c *Conn) DescribePrepared(ctx context.Context, name string) (*query.StatementDescription, error) {
	return c.pooled.Conn.DescribePrepared(ctx, name)
}

// CloseStatement closes a prepared statement.
func (c *Conn) CloseStatement(ctx context.Context, name string) error {
	return c.pooled.Conn.CloseStatement(ctx, name)
}

// ClosePortal closes a portal.
func (c *Conn) ClosePortal(ctx context.Context, name string) error {
	return c.pooled.Conn.ClosePortal(ctx, name)
}

// Sync sends a Sync message to synchronize the extended query protocol.
func (c *Conn) Sync(ctx context.Context) error {
	return c.pooled.Conn.Sync(ctx)
}

// PrepareAndExecute is a convenience method that prepares and executes in one round trip.
// name is the statement/portal name (use "" for unnamed, which is cleared after Sync).
func (c *Conn) PrepareAndExecute(ctx context.Context, name, queryStr string, params [][]byte, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	return c.pooled.Conn.PrepareAndExecute(ctx, name, queryStr, params, callback)
}

// QueryArgs executes a parameterized query using the extended query protocol.
// This is a convenience method that accepts Go values as arguments and converts
// them to the appropriate text format for PostgreSQL.
func (c *Conn) QueryArgs(ctx context.Context, queryStr string, args ...any) ([]*sqltypes.Result, error) {
	return c.pooled.Conn.QueryArgs(ctx, queryStr, args...)
}

// Execute continues execution of a previously bound portal.
// This is used to fetch more rows from a portal that was executed with maxRows > 0
// and returned PortalSuspended.
// Returns true if the portal completed (CommandComplete), false if suspended (PortalSuspended).
func (c *Conn) Execute(ctx context.Context, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (completed bool, err error) {
	return c.pooled.Conn.Execute(ctx, portalName, maxRows, callback)
}
