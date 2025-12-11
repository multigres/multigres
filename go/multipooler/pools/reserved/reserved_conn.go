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
	"fmt"
	"sync/atomic"
	"time"

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

	// idleTimeout is the maximum idle duration before the connection expires.
	// A value of 0 means no timeout.
	idleTimeout time.Duration

	// expiryTime is when this connection will expire if not accessed.
	expiryTime time.Time

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
		return fmt.Errorf("transaction already in progress")
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
		return fmt.Errorf("no active transaction")
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
func (c *Conn) ReserveForPortal(portalName string) {
	c.reservedProps = NewReservationProperties(ReservationPortal)
	c.reservedProps.PortalName = portalName
}

// ReleasePortalReservation clears the portal reservation.
func (c *Conn) ReleasePortalReservation() {
	if c.reservedProps != nil && c.reservedProps.IsForPortal() {
		c.reservedProps = nil
	}
}

// IsReservedForPortal returns true if reserved for a portal.
func (c *Conn) IsReservedForPortal() bool {
	return c.reservedProps != nil && c.reservedProps.IsForPortal()
}

// ReservedProps returns the reservation properties.
func (c *Conn) ReservedProps() *ReservationProperties {
	return c.reservedProps
}

// --- Timeout ---

// SetIdleTimeout sets the idle timeout and resets the expiry time.
func (c *Conn) SetIdleTimeout(timeout time.Duration) {
	c.idleTimeout = timeout
	c.ResetExpiryTime()
}

// ResetExpiryTime resets the expiry time based on the idle timeout.
// Called when the connection is accessed to extend its lifetime.
func (c *Conn) ResetExpiryTime() {
	if c.idleTimeout > 0 {
		c.expiryTime = time.Now().Add(c.idleTimeout)
	}
}

// IsTimedOut returns true if the connection has exceeded its idle timeout.
func (c *Conn) IsTimedOut() bool {
	if c.idleTimeout <= 0 {
		return false
	}
	return time.Now().After(c.expiryTime)
}

// IdleTimeout returns the idle timeout duration.
func (c *Conn) IdleTimeout() time.Duration {
	return c.idleTimeout
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
func (c *Conn) Close() error {
	if !c.released.CompareAndSwap(false, true) {
		return nil // Already released/closed.
	}

	// Close the underlying connection.
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

// --- User/Role ---

// SetRole sets the role for RLS.
func (c *Conn) SetRole(ctx context.Context, user string) error {
	return c.pooled.Conn.SetRole(ctx, user)
}

// ResetRole resets the role.
func (c *Conn) ResetRole(ctx context.Context) error {
	return c.pooled.Conn.ResetRole(ctx)
}

// CurrentUser returns the current role.
func (c *Conn) CurrentUser() string {
	return c.pooled.Conn.CurrentUser()
}

// --- Query execution ---

// Query executes a simple query and returns all results.
func (c *Conn) Query(ctx context.Context, sql string) ([]*query.QueryResult, error) {
	return c.pooled.Conn.Query(ctx, sql)
}

// QueryStreaming executes a query with streaming results via callback.
func (c *Conn) QueryStreaming(ctx context.Context, sql string, callback func(context.Context, *query.QueryResult) error) error {
	return c.pooled.Conn.QueryStreaming(ctx, sql, callback)
}

// --- Extended query protocol ---

// Parse sends a Parse message to prepare a statement.
func (c *Conn) Parse(ctx context.Context, name, queryStr string, paramTypes []uint32) error {
	return c.pooled.Conn.Parse(ctx, name, queryStr, paramTypes)
}

// BindAndExecute binds parameters and executes atomically.
func (c *Conn) BindAndExecute(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16, maxRows int32, callback func(ctx context.Context, result *query.QueryResult) error) error {
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
func (c *Conn) PrepareAndExecute(ctx context.Context, queryStr string, params [][]byte, callback func(ctx context.Context, result *query.QueryResult) error) error {
	return c.pooled.Conn.PrepareAndExecute(ctx, queryStr, params, callback)
}
