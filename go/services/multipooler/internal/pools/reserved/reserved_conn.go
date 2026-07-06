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

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
)

// ReleaseCleanup runs just before a clean reserved backend is returned to the
// regular pool. Returning false means the backend must not be recycled as clean
// and should be tainted/closed instead.
type ReleaseCleanup func(*regular.Conn) bool

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

	// connID is the unique identifier for this reservation.
	// Clients use this to resume their session across requests.
	// Exposed via the ConnID() accessor.
	connID int64

	// pool is a back-reference to the owning pool.
	// Used for Release operations.
	pool *Pool

	// reservedProps tracks why the connection is reserved.
	reservedProps *ReservationProperties

	// txnSnapshot captures the session state (GUCs, role) at the moment this
	// connection opened its current transaction. PostgreSQL reverts session
	// SET / SET ROLE issued inside a transaction on ROLLBACK; restoring this
	// snapshot on rollback keeps the pool's cached connstate in lock-step with
	// the backend, so a recycled connection is never reused with stale settings.
	// nil when not in a transaction. Accessed only from the transaction-control
	// methods, which the gateway serializes per reserved connection.
	txnSnapshot *connstate.TxnSnapshot

	// sessionStateUntrusted is set when PostgreSQL may have reverted backend
	// session state without the pooler's connstate cache observing the exact new
	// value (e.g. successful ROLLBACK TO SAVEPOINT). While set, release
	// finalization syncs connstate to the gateway's authoritative session
	// settings instead of trusting the stale cache.
	sessionStateUntrusted bool

	// releaseCleanups run before clean release recycles the backend.
	releaseCleanups []ReleaseCleanup

	// inactivityTimeout is the maximum duration the connection can be inactive
	// (no client activity) before expiring. A value of 0 means no timeout.
	inactivityTimeout time.Duration

	// expiryNanos stores the expiry time as Unix nanoseconds for atomic access.
	expiryNanos atomic.Int64

	// released indicates whether this connection has been released.
	released atomic.Bool

	// txnStartTime is when the current transaction began. Set by the begin
	// paths (BeginWithQuery / SnapshotTxnState), cleared when the transaction is
	// concluded by Commit/Rollback. Used to compute mg.pooler.txn.duration.
	// Zero when no transaction is active.
	txnStartTime time.Time
}

// recordTxnOutcome reports a concluded transaction to the pool's metrics with
// its lifetime, then clears the start time. No-op when the connection has no
// owning pool (e.g. unit-test fixtures).
func (c *Conn) recordTxnOutcome(ctx context.Context, outcome string) {
	if c.pool == nil {
		return
	}
	var d time.Duration
	if !c.txnStartTime.IsZero() {
		d = time.Since(c.txnStartTime)
	}
	c.pool.txnMetrics.record(ctx, outcome, d)
	c.txnStartTime = time.Time{}
}

// newConn creates a new reserved connection.
func newConn(pooled regular.PooledConn, connID int64, pool *Pool, releaseCleanups []ReleaseCleanup) *Conn {
	return &Conn{
		pooled:          pooled,
		connID:          connID,
		pool:            pool,
		releaseCleanups: append([]ReleaseCleanup(nil), releaseCleanups...),
	}
}

// ConnID returns the unique identifier for this reservation.
func (c *Conn) ConnID() int64 {
	return c.connID
}

// Conn returns the underlying regular connection.
func (c *Conn) Conn() *regular.Conn {
	return c.pooled.Conn
}

// TxnStatus returns the underlying PG protocol transaction status from the
// most recent ReadyForQuery message.
func (c *Conn) TxnStatus() protocol.TransactionStatus {
	return c.pooled.Conn.TxnStatus()
}

// State returns the connection's state.
func (c *Conn) State() *connstate.ConnectionState {
	return c.pooled.Conn.State()
}

// --- Transaction lifecycle ---

// Begin starts a transaction on this connection with a plain "BEGIN".
func (c *Conn) Begin(ctx context.Context) error {
	return c.BeginWithQuery(ctx, "BEGIN")
}

// BeginWithQuery starts a transaction using the provided query string.
// This allows preserving transaction options like isolation level and access mode
// (e.g., "BEGIN ISOLATION LEVEL SERIALIZABLE" or "START TRANSACTION READ ONLY").
func (c *Conn) BeginWithQuery(ctx context.Context, beginQuery string) error {
	if c.IsInTransaction() {
		return errors.New("transaction already in progress")
	}

	_, err := c.pooled.Conn.Query(ctx, beginQuery)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Snapshot the committed session-state baseline so a ROLLBACK can revert the
	// pool's cached connstate in lock-step with PostgreSQL.
	c.txnSnapshot = c.pooled.Conn.State().SnapshotForTxn()
	c.txnStartTime = time.Now()

	c.AddReservationReason(protoutil.ReasonTransaction)
	return nil
}

// SnapshotTxnState captures the current session-state baseline as the
// transaction snapshot. Transaction-start paths that run BEGIN outside
// BeginWithQuery must call this so a later ROLLBACK can still revert the pool's
// cached connstate in lock-step with PostgreSQL. In particular, acquisition
// paths that need the first backend write to be retryable (COPY initiation and
// transaction starts on fresh reserved connections) run BEGIN on the raw
// *regular.Conn inside a connection-acquisition validate callback (the
// *reserved.Conn wrapper doesn't exist yet), then add the transaction reason
// manually; they call this immediately afterwards, before any client statement
// runs in the transaction, so the captured baseline is the pre-transaction
// state.
func (c *Conn) SnapshotTxnState() {
	c.txnSnapshot = c.pooled.Conn.State().SnapshotForTxn()
	c.txnStartTime = time.Now()
}

// Commit commits the current transaction.
func (c *Conn) Commit(ctx context.Context) error {
	_, err := c.CommitResult(ctx)
	return err
}

// CommitResult commits the current transaction and returns PostgreSQL's
// CommandComplete result, including any NoticeResponse diagnostics emitted while
// committing deferred work (for example deferred constraint triggers). Callers
// that forward a COMMIT result to the client should use this variant so notice
// ordering matches the backend.
func (c *Conn) CommitResult(ctx context.Context) (*sqltypes.Result, error) {
	if !c.IsInTransaction() {
		return nil, errors.New("no active transaction")
	}

	results, err := c.pooled.Conn.Query(ctx, "COMMIT")
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Committed: mid-transaction session changes are now durable and already
	// reflected in connstate; drop the snapshot.
	c.txnSnapshot = nil

	c.recordTxnOutcome(ctx, txnOutcomeCommit)
	c.RemoveReservationReason(protoutil.ReasonTransaction)
	return firstTxnResult(results, "COMMIT"), nil
}

// CommitAndChain commits the current transaction and immediately starts the
// next transaction on the same backend, inheriting PostgreSQL's transaction
// characteristics (isolation level, read-only flag, deferrable flag). The
// transaction reservation remains active.
func (c *Conn) CommitAndChain(ctx context.Context) error {
	_, err := c.CommitAndChainResult(ctx)
	return err
}

// CommitAndChainResult commits the current transaction, starts the next one,
// and returns PostgreSQL's CommandComplete result including notices.
func (c *Conn) CommitAndChainResult(ctx context.Context) (*sqltypes.Result, error) {
	if !c.IsInTransaction() {
		return nil, errors.New("no active transaction")
	}

	results, err := c.pooled.Conn.Query(ctx, "COMMIT AND CHAIN")
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction and chain: %w", err)
	}

	// The committed changes are durable. PostgreSQL is already inside the chained
	// transaction; capture that transaction's rollback baseline from the current
	// committed connstate.
	c.txnSnapshot = c.pooled.Conn.State().SnapshotForTxn()
	c.AddReservationReason(protoutil.ReasonTransaction)
	return firstTxnResult(results, "COMMIT"), nil
}

// Rollback rolls back the current transaction.
func (c *Conn) Rollback(ctx context.Context) error {
	_, err := c.RollbackResult(ctx)
	return err
}

// RollbackResult rolls back the current transaction and returns PostgreSQL's
// CommandComplete result, including notices. A rollback with no active
// transaction remains a no-op and returns a synthetic ROLLBACK tag.
func (c *Conn) RollbackResult(ctx context.Context) (*sqltypes.Result, error) {
	if !c.IsInTransaction() {
		// No active transaction, but that's okay for rollback.
		return &sqltypes.Result{CommandTag: "ROLLBACK"}, nil
	}

	results, err := c.pooled.Conn.Query(ctx, "ROLLBACK")
	if err != nil {
		return nil, fmt.Errorf("failed to rollback transaction: %w", err)
	}

	// PostgreSQL just reverted any SET / SET ROLE issued inside this transaction
	// to the pre-transaction baseline. Revert the pool's cached connstate to the
	// same baseline so the recycled connection is bucketed and reused correctly.
	if c.txnSnapshot != nil {
		c.pooled.Conn.State().RestoreFromTxn(c.txnSnapshot)
		c.txnSnapshot = nil
	}
	c.ClearSessionStateUntrusted()

	c.recordTxnOutcome(ctx, txnOutcomeRollback)
	c.RemoveReservationReason(protoutil.ReasonTransaction)
	return firstTxnResult(results, "ROLLBACK"), nil
}

func firstTxnResult(results []*sqltypes.Result, fallbackTag string) *sqltypes.Result {
	if len(results) == 0 || results[0] == nil {
		return &sqltypes.Result{CommandTag: fallbackTag}
	}
	if results[0].CommandTag == "" {
		clone := *results[0]
		clone.CommandTag = fallbackTag
		return &clone
	}
	return results[0]
}

// RollbackAndChain rolls back the current transaction and immediately starts
// the next transaction on the same backend, inheriting PostgreSQL's transaction
// characteristics. The transaction reservation remains active.
func (c *Conn) RollbackAndChain(ctx context.Context) error {
	_, err := c.RollbackAndChainResult(ctx)
	return err
}

// RollbackAndChainResult rolls back the current transaction, starts the next
// one, and returns PostgreSQL's CommandComplete result including notices.
func (c *Conn) RollbackAndChainResult(ctx context.Context) (*sqltypes.Result, error) {
	if !c.IsInTransaction() {
		return nil, errors.New("no active transaction")
	}

	results, err := c.pooled.Conn.Query(ctx, "ROLLBACK AND CHAIN")
	if err != nil {
		return nil, fmt.Errorf("failed to rollback transaction and chain: %w", err)
	}

	if c.txnSnapshot != nil {
		c.pooled.Conn.State().RestoreFromTxn(c.txnSnapshot)
	}
	c.ClearSessionStateUntrusted()
	c.txnSnapshot = c.pooled.Conn.State().SnapshotForTxn()
	c.AddReservationReason(protoutil.ReasonTransaction)
	return firstTxnResult(results, "ROLLBACK"), nil
}

// IsInTransaction returns true if there's an active transaction.
func (c *Conn) IsInTransaction() bool {
	return c.reservedProps != nil && c.reservedProps.IsForTransaction()
}

// --- Portal reservations ---

// ReserveForPortal marks the connection as reserved for a portal.
// This is used when Execute returns suspended (portal not fully consumed).
// Multiple portals can be reserved on the same connection.
// Preserves any existing reservation reasons (e.g., transaction).
func (c *Conn) ReserveForPortal(portalName string) {
	c.AddReservationReason(protoutil.ReasonPortal)
	c.reservedProps.AddPortal(portalName)
}

// ReleasePortal removes a specific portal from the reservation.
// If no portals remain, the portal reason is removed from the bitmask.
// Returns true if all reservation reasons are gone (connection should be released).
func (c *Conn) ReleasePortal(portalName string) bool {
	if c.reservedProps == nil {
		return false
	}
	c.reservedProps.RemovePortal(portalName)
	if !c.reservedProps.HasPortals() {
		return c.RemoveReservationReason(protoutil.ReasonPortal)
	}
	return false
}

// ReleaseAllPortals clears all portal reservations.
// Removes the portal reason from the bitmask but preserves other reasons.
func (c *Conn) ReleaseAllPortals() {
	if c.reservedProps == nil {
		return
	}
	c.reservedProps.Portals = nil
	c.RemoveReservationReason(protoutil.ReasonPortal)
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

// --- Reason management ---

// AddReservationReason adds a reason to the reservation bitmask.
// Creates reservedProps if needed (sets StartTime to now).
func (c *Conn) AddReservationReason(reason uint32) {
	if c.reservedProps == nil {
		c.reservedProps = NewReservationProperties(reason)
	} else {
		c.reservedProps.AddReason(reason)
	}
}

// RemoveReservationReason removes a reason from the reservation bitmask.
// If all reasons are removed, clears reservedProps.
// Returns true if all reservation reasons are gone (connection should be released).
func (c *Conn) RemoveReservationReason(reason uint32) bool {
	if c.reservedProps == nil {
		return true
	}
	c.reservedProps.RemoveReason(reason)
	if c.reservedProps.IsEmpty() {
		c.reservedProps = nil
		return true
	}
	return false
}

// RemainingReasons returns the current reasons bitmask, or 0 if not reserved.
func (c *Conn) RemainingReasons() uint32 {
	if c.reservedProps == nil {
		return 0
	}
	return c.reservedProps.Reasons
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

// --- Session-state reconciliation metadata ---

// MarkSessionStateUntrusted records that connstate may not match the backend's
// real session state, so the next reconciliation must be forced.
func (c *Conn) MarkSessionStateUntrusted() {
	c.sessionStateUntrusted = true
}

// SessionStateUntrusted returns true if forced reconciliation is required.
func (c *Conn) SessionStateUntrusted() bool {
	return c.sessionStateUntrusted
}

// ClearSessionStateUntrusted marks connstate as trusted again after a full
// rollback snapshot restore or successful forced reconciliation.
func (c *Conn) ClearSessionStateUntrusted() {
	c.sessionStateUntrusted = false
}

// --- Lifecycle ---

// Release releases this connection back to the pool. gatewaySessionSettings is
// the gateway's authoritative session settings at release time; it is used to
// sync connstate in-memory when the connection is marked untrusted. Pass nil
// for dirty releases or when gateway settings are unavailable.
func (c *Conn) Release(reason ReleaseReason, gatewaySessionSettings map[string]string) {
	if !c.released.CompareAndSwap(false, true) {
		return // Already released.
	}

	if c.pool != nil {
		c.pool.release(c, reason, gatewaySessionSettings, c.releaseCleanups)
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
func (c *Conn) BindAndExecute(ctx context.Context, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (completed bool, err error) {
	return c.pooled.Conn.BindAndExecute(ctx, portalName, stmtName, params, paramFormats, resultFormats, maxRows, callback)
}

// BindAndDescribe binds parameters and describes the resulting portal.
func (c *Conn) BindAndDescribe(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16) (*query.StatementDescription, error) {
	return c.pooled.Conn.BindAndDescribe(ctx, stmtName, params, paramFormats, resultFormats)
}

// BindDescribeAndExecute fuses Bind+Describe(P)+Execute+Sync into a single
// backend round trip.
func (c *Conn) BindDescribeAndExecute(ctx context.Context, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (bool, error) {
	return c.pooled.Conn.BindDescribeAndExecute(ctx, portalName, stmtName, params, paramFormats, resultFormats, maxRows, callback)
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

// --- LISTEN/NOTIFY operations ---

// SendQuery writes a simple query message without reading the response.
// Used for LISTEN/UNLISTEN commands in the split read/write pattern.
func (c *Conn) SendQuery(sql string) error {
	return c.pooled.Conn.RawConn().SendQuery(sql)
}

// ReadRawMessage reads the next raw PostgreSQL protocol message.
// Returns the message type byte and body.
func (c *Conn) ReadRawMessage() (byte, []byte, error) {
	return c.pooled.Conn.RawConn().ReadRawMessage()
}

// ParseNotification parses a NotificationResponse message body.
func (c *Conn) ParseNotification(body []byte) (*sqltypes.Notification, error) {
	return c.pooled.Conn.RawConn().ParseNotification(body)
}
