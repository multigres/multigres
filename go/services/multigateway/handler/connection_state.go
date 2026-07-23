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

package handler

import (
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/pgsettings"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// pendingListenAction records a single LISTEN/UNLISTEN operation within a transaction.
type pendingListenAction struct {
	actionType int    // pendingListen, pendingUnlisten, or pendingUnlistenAll
	channel    string // empty for pendingUnlistenAll
}

const (
	pendingListen      = iota // LISTEN channel
	pendingUnlisten           // UNLISTEN channel
	pendingUnlistenAll        // UNLISTEN *

	maxPendingNotifications = 256 // matches server.Conn async notification buffer
)

// MultigatewayConnectionState keeps track of the information specific
// to each connection.
// MultigatewayConnectionState holds all per-connection gateway state. A
// MultigatewayConnectionState is created once when a client connects and
// destroyed when the connection ends — its lifetime exactly matches the
// PostgreSQL wire-protocol session.
//
// Concurrency: the PG wire protocol is strictly serial per connection (one
// request, one response, no overlap), so every method on this type is
// effectively called from a single goroutine in production. The `mu`
// mutex is defensive — it guards against future async helpers (cancellation
// goroutines, background ticker callbacks, fan-out fan-in primitives) that
// might touch state concurrently. Callers must not rely on the mutex for
// cross-statement atomicity; that's the protocol's job.
type MultigatewayConnectionState struct {
	mu sync.Mutex
	// NOTE: We are not storing the map of Prepared Statements even though
	// that is also connection level information. We do not require storing
	// it because we have prepared statement consolidation in Multigateway
	// and that has all the required information. We can choose to store the information
	// here too but it would only lead to duplicate information storage and additional
	// code burden to keep them in sync.

	// Portals stores the list of portals created on this connection.
	// Map is keyed by the name of the portal.
	Portals map[string]*preparedstatement.PortalInfo

	// ShardStates is the information per shard that needs to be maintained.
	// It keeps track of any reserved connections on each Shard currently open.
	ShardStates []*ShardState

	// StartupParams stores startup parameters from the client's StartupMessage.
	// These are forwarded to the backend for every query.
	StartupParams map[string]string

	// SessionSettings stores session variables set via SET commands.
	// These settings are propagated to multipooler to ensure the correct
	// pooled connection (with matching settings) is reused.
	// Map keys are variable names, values are the string representation.
	SessionSettings map[string]string

	// PendingBeginQuery stores the original BEGIN/START TRANSACTION statement text
	// (e.g., "BEGIN ISOLATION LEVEL SERIALIZABLE") when a transaction is started
	// with deferred execution. This is consumed when creating the first reserved
	// connection so the multipooler can use the exact statement instead of plain "BEGIN".
	//
	// Unlike the single-query reservation signals (which ride on the
	// engine.PlanExecInfo passed to IExecute), this spans statements: it is
	// set at the deferred BEGIN and consumed by a *later* statement's reservation,
	// so it genuinely belongs to connection state.
	PendingBeginQuery string

	// ActiveTransactionBeginQuery stores the BEGIN/START statement that describes
	// the current transaction's characteristics. PendingBeginQuery is consumed when
	// the first backend reservation starts the transaction; this copy survives that
	// consumption so COMMIT/ROLLBACK AND CHAIN can start the next transaction with
	// the same isolation/read-only/deferrable options even when no backend was ever
	// reserved for the old transaction.
	ActiveTransactionBeginQuery string

	// PendingMarkSessionStateUntrusted is set by the TransactionPrimitive after a
	// successful ROLLBACK TO SAVEPOINT. PostgreSQL may have reverted session GUCs
	// on the backend without the pooler observing the exact reverted values, so
	// ScatterConn forwards it as ReservationOptions.MarkSessionStateUntrusted,
	// asking the multipooler to force reconciliation before the next reserved
	// user SQL or at release. One-shot: cleared after it is consumed.
	//
	// Like PendingBeginQuery (and unlike the single-query reservation signals
	// that ride on engine.PlanExecInfo), this spans statements: it is set on the
	// ROLLBACK TO and consumed by a *later* statement's reservation, so it
	// genuinely belongs to connection state.
	PendingMarkSessionStateUntrusted bool

	// OpenHoldCursors tracks the names of currently-open `DECLARE ... WITH HOLD`
	// cursors on this gateway session. Used to compute `CLOSE ALL` membership
	// and as a single-source-of-truth refcount so the gateway can answer
	// "do we still have any HOLD cursor open" without round-tripping to the
	// multipooler. Membership mirrors the per-conn `reservedProps.Portals`
	// set on the multipooler side for HOLD entries.
	OpenHoldCursors map[string]bool

	// TxnStartTime records when the current transaction began (set at BEGIN,
	// read at COMMIT/ROLLBACK to compute transaction duration). Zero value
	// means no active transaction is being timed.
	TxnStartTime time.Time

	// ListenChannels tracks active LISTEN channels for this connection.
	ListenChannels map[string]bool

	// pendingActions tracks LISTEN/UNLISTEN operations inside transactions,
	// preserving the order they were issued. Processed at COMMIT by CommitPendingListens.
	pendingActions []pendingListenAction

	// NotifCh receives notifications from the PubSubListener.
	NotifCh chan *sqltypes.Notification

	// AsyncNotifCh is the channel for the server.Conn async notification pusher.
	AsyncNotifCh chan<- *sqltypes.Notification

	// PendingNotifications holds notifications received while this session is
	// inside a transaction. PostgreSQL delivers LISTEN notifications only between
	// transactions, so these are drained after COMMIT/ROLLBACK.
	PendingNotifications []*sqltypes.Notification

	// notificationTxnOpen keeps notification delivery buffered while a transaction
	// is active, and after COMMIT/ROLLBACK until PendingNotifications have been
	// flushed. notificationTxnEnded marks that final drain point.
	notificationTxnOpen  bool
	notificationTxnEnded bool

	// SubSync coordinates LISTEN/NOTIFY subscriptions with the notification manager.
	// Set by the handler at connection initialization; called by engine primitives
	// to apply subscription changes before reporting success to the client.
	SubSync SubscriptionSync

	// statementTimeout is managed entirely by the gateway and is NOT forwarded
	// to PostgreSQL. The default is initialized from startup params (if
	// present) or the --statement-timeout flag. Parsed at SET time to avoid
	// repeated parsing on every query.
	statementTimeout GatewayManagedVariable[time.Duration]

	// idleSessionTimeout is managed entirely by the gateway and is NOT forwarded
	// to PostgreSQL. It applies to the client-facing gateway session while idle
	// outside a transaction; forwarding it to pooled PostgreSQL backends would
	// make backend sockets die independently of the client session.
	idleSessionTimeout GatewayManagedVariable[time.Duration]

	// savepoints is the stack of per-savepoint snapshots driving GUC revert
	// semantics on ROLLBACK / ROLLBACK TO. Each frame captures SessionSettings
	// and OpenHoldCursors; gateway-managed variables maintain parallel
	// snapshot stacks of equal depth (lockstep invariant). Index 0, when
	// present, is the BEGIN-level frame (name=""); indices 1+ correspond to
	// user SAVEPOINTs.
	//
	// MAINTENANCE CONTRACT: every gateway-managed variable must be returned by
	// gatewayManagedVariablesLocked so all transaction/savepoint lifecycle
	// methods keep its snapshot stack in lockstep with savepoints. The
	// gmvLifecycle interface (Snapshot/RestoreFromDepth/PopFrom/ClearSnapshots
	// and ResetLocal) is the complete set of operations those methods need, so
	// adding a GMV requires only extending gatewayManagedVariablesLocked — no
	// per-variable wiring in the lifecycle methods themselves. Every non-GMV
	// snapshot field stored on savepointFrame (currently `openHoldCursors`)
	// must still be wired into pushFrameLocked, ReleaseSavepoint,
	// RollbackToSavepoint, BeginTransaction, CommitTransaction, and
	// RollbackTransaction.
	savepoints []savepointFrame

	// targetReplica is true when this connection arrived on the replica-reads
	// listener port. Set once at connection initialization, never changed.
	targetReplica bool
}

// savepointFrame snapshots the SessionSettings map at the moment a savepoint
// (or BEGIN-level frame) was pushed. Gateway-managed variable snapshots live
// on each variable directly; the depth on this stack and on each variable's
// stack are always identical.
type savepointFrame struct {
	name            string
	sessionSettings map[string]string
	// openHoldCursors snapshots the names of `DECLARE … WITH HOLD`
	// cursors that were open at the moment the savepoint was pushed.
	// Used so that `ROLLBACK TO <name>` can compute the set of cursors
	// declared in the rolled-back sub-transaction and unpin them on the
	// multipooler — PG closes those cursors server-side on
	// ROLLBACK TO, and our reservation bookkeeping must follow.
	openHoldCursors map[string]bool
}

type ShardState struct {
	// Target stores the information about the shard
	Target *query.Target

	// ReservedState holds the authoritative reservation state from the multipooler,
	// including the pooler ID, reserved connection ID, and reservation reasons bitmask.
	ReservedState *query.ReservedState
}

// NewMultigatewayConnectionState creates a new MultigatewayConnectionState.
func NewMultigatewayConnectionState() *MultigatewayConnectionState {
	return &MultigatewayConnectionState{
		mu:              sync.Mutex{},
		Portals:         make(map[string]*preparedstatement.PortalInfo),
		OpenHoldCursors: make(map[string]bool),
	}
}

// AddOpenHoldCursor records a `DECLARE ... WITH HOLD` cursor as currently open
// on this gateway session. Idempotent.
func (m *MultigatewayConnectionState) AddOpenHoldCursor(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.OpenHoldCursors == nil {
		m.OpenHoldCursors = make(map[string]bool)
	}
	m.OpenHoldCursors[name] = true
}

// RemoveOpenHoldCursor drops the named HOLD cursor from the open set.
// Returns true if the entry existed.
func (m *MultigatewayConnectionState) RemoveOpenHoldCursor(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.OpenHoldCursors[name]; !ok {
		return false
	}
	delete(m.OpenHoldCursors, name)
	return true
}

// HasOpenHoldCursor reports whether the named HOLD cursor is open.
func (m *MultigatewayConnectionState) HasOpenHoldCursor(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.OpenHoldCursors[name]
}

// OpenHoldCursorNames returns a snapshot of the open HOLD cursor names.
// Used to materialise the target list for `CLOSE ALL`.
func (m *MultigatewayConnectionState) OpenHoldCursorNames() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.OpenHoldCursors) == 0 {
		return nil
	}
	names := make([]string, 0, len(m.OpenHoldCursors))
	for name := range m.OpenHoldCursors {
		names = append(names, name)
	}
	return names
}

// HasAnyOpenHoldCursor reports whether the session is holding at least one
// `DECLARE ... WITH HOLD` cursor open. Used by ScatterConn to keep
// ReasonPortal applied on follow-up queries while any HOLD cursor remains.
func (m *MultigatewayConnectionState) HasAnyOpenHoldCursor() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.OpenHoldCursors) > 0
}

// ClearOpenHoldCursors drops every tracked HOLD cursor. Called at ROLLBACK,
// when PostgreSQL closes all open cursors regardless of WITH HOLD.
func (m *MultigatewayConnectionState) ClearOpenHoldCursors() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.OpenHoldCursors = make(map[string]bool)
}

// HoldCursorsDeclaredInTxn returns the names of HOLD cursors that were
// declared after the BEGIN-level frame was pushed — i.e., the cursors
// PostgreSQL would close at ROLLBACK of the outer transaction. Cursors
// that existed before BEGIN (autocommit DECLAREs prior to the explicit
// block) are *not* included: PG keeps them across ROLLBACK and the
// multipooler must not unpin them.
//
// Returns nil if no BEGIN-level frame is present (no active txn) or the
// set is empty. State is not mutated.
func (m *MultigatewayConnectionState) HoldCursorsDeclaredInTxn() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.savepoints) == 0 || m.savepoints[0].name != "" {
		// No explicit transaction in progress — every open HOLD cursor
		// pre-dates this code path. Caller must not rely on the result
		// to drive a ROLLBACK release.
		return nil
	}
	snapshot := m.savepoints[0].openHoldCursors
	var inTxn []string
	for cur := range m.OpenHoldCursors {
		if !snapshot[cur] {
			inTxn = append(inTxn, cur)
		}
	}
	return inTxn
}

// RestoreOpenHoldCursorsToBeginSnapshot restores the OpenHoldCursors set
// to the snapshot captured by BeginTransaction at the depth-0 frame.
// Cursors declared after BEGIN are dropped (PG closed them at ROLLBACK);
// cursors that existed before BEGIN are kept (PG preserves them).
//
// Falls back to clearing the set entirely when there is no BEGIN-level
// frame on the stack — matches the previous ClearOpenHoldCursors behavior
// for the no-txn path.
func (m *MultigatewayConnectionState) RestoreOpenHoldCursorsToBeginSnapshot() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.savepoints) == 0 || m.savepoints[0].name != "" {
		m.OpenHoldCursors = make(map[string]bool)
		return
	}
	snapshot := m.savepoints[0].openHoldCursors
	restored := make(map[string]bool, len(snapshot))
	for cur := range snapshot {
		restored[cur] = true
	}
	m.OpenHoldCursors = restored
}

// StorePortalInfo stores the portal information.
func (m *MultigatewayConnectionState) StorePortalInfo(portal *query.Portal, psi *preparedstatement.PreparedStatementInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Portals[portal.Name] = preparedstatement.NewPortalInfo(psi, portal)
}

// GetPortalInfo gets the portal information for a previously stored portal.
func (m *MultigatewayConnectionState) GetPortalInfo(portalName string) *preparedstatement.PortalInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Portals[portalName]
}

// DeletePortalInfo deletes the portal information
func (m *MultigatewayConnectionState) DeletePortalInfo(portalName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.Portals, portalName)
}

// NewShardState creates a new shard state.
func NewShardState(target *query.Target) *ShardState {
	return &ShardState{
		Target: target,
	}
}

// GetMatchingShardState gets the shardState (if any) that matches the target specified.
func (m *MultigatewayConnectionState) GetMatchingShardState(target *query.Target) *ShardState {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ss := range m.ShardStates {
		if protoutil.TargetEquals(ss.Target, target) {
			return ss
		}
	}
	return nil
}

// SetReservedConnection stores the authoritative reservation state from the multipooler.
// The reasons in rs.ReservationReasons are set exactly as provided (not OR'd).
// Creates a new entry if none exists for the target.
func (m *MultigatewayConnectionState) SetReservedConnection(target *query.Target, rs *query.ReservedState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ss := range m.ShardStates {
		if protoutil.TargetEquals(ss.Target, target) {
			ss.ReservedState = rs
			return
		}
	}
	ss := NewShardState(target)
	ss.ReservedState = rs
	m.ShardStates = append(m.ShardStates, ss)
}

// ClearReservedConnection removes a reserved connection for a given target.
// This should be called when a reserved connection is released (e.g., after COPY completes).
func (m *MultigatewayConnectionState) ClearReservedConnection(target *query.Target) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, ss := range m.ShardStates {
		if protoutil.TargetEquals(ss.Target, target) {
			// Remove by swapping with last element and truncating
			lastIdx := len(m.ShardStates) - 1
			if i != lastIdx {
				m.ShardStates[i] = m.ShardStates[lastIdx]
			}
			m.ShardStates = m.ShardStates[:lastIdx]
			return
		}
	}
}

// HasReservedConnection reports whether any shard currently has backend affinity.
func (m *MultigatewayConnectionState) HasReservedConnection() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ss := range m.ShardStates {
		if ss.ReservedState != nil {
			return true
		}
	}
	return false
}

// ClearAllReservedConnections removes all reserved connection entries.
// Called after COMMIT or ROLLBACK to clean up stale shard state.
func (m *MultigatewayConnectionState) ClearAllReservedConnections() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ShardStates = nil
}

// SetSessionVariable sets a session variable (from SET command).
// The variable name is canonicalized using PostgreSQL's GUC-name comparison
// rules before storage, so later SETs for the same GUC in different ASCII case
// overwrite the previous value like PostgreSQL would.
func (m *MultigatewayConnectionState) SetSessionVariable(name, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.SessionSettings == nil {
		m.SessionSettings = make(map[string]string)
	}
	m.SessionSettings[pgsettings.CanonicalGUCName(name)] = value
}

// ResetSessionVariable removes a session variable (from RESET command).
func (m *MultigatewayConnectionState) ResetSessionVariable(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.SessionSettings, pgsettings.CanonicalGUCName(name))
}

// ResetAllSessionVariables clears all session variables (from RESET ALL command).
func (m *MultigatewayConnectionState) ResetAllSessionVariables() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SessionSettings = nil
}

// gatewayManagedVariablesLocked returns every gateway-managed variable for the
// transaction/savepoint lifecycle methods to iterate. It returns a fixed-size
// array (not a slice) so the result stays on the caller's stack — these are hot
// transaction-boundary paths and a slice literal would escape to the heap on
// every call. Adding a GMV means bumping the array size and adding the element
// here; the compiler flags a size mismatch if you forget one.
func (m *MultigatewayConnectionState) gatewayManagedVariablesLocked() [2]gmvLifecycle {
	return [2]gmvLifecycle{
		&m.statementTimeout,
		&m.idleSessionTimeout,
	}
}

// SetStatementTimeout sets the session-level statement timeout override.
func (m *MultigatewayConnectionState) SetStatementTimeout(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout.Set(d)
}

// ResetStatementTimeout clears both the session-level override and any
// active transaction-local override, reverting to the default (from startup
// params or flag). Matches PostgreSQL: RESET inside a transaction with a
// prior SET LOCAL supersedes the LOCAL — effective value becomes the default.
func (m *MultigatewayConnectionState) ResetStatementTimeout() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout.Reset()
}

// SetLocalStatementTimeout stores a transaction-local statement timeout
// override (from SET LOCAL statement_timeout). Cleared on COMMIT/ROLLBACK
// via ResetAllLocalGUCs.
func (m *MultigatewayConnectionState) SetLocalStatementTimeout(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout.SetLocal(d)
}

// SetLocalStatementTimeoutToDefault sets the transaction-local override to
// the server default (from SET LOCAL statement_timeout TO DEFAULT). This
// masks any session-level override for the duration of the transaction
// without destroying it; the session value is restored on COMMIT/ROLLBACK
// via ResetAllLocalGUCs.
func (m *MultigatewayConnectionState) SetLocalStatementTimeoutToDefault() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout.SetLocalToDefault()
}

// SetIdleSessionTimeout sets the session-level idle-session timeout override.
func (m *MultigatewayConnectionState) SetIdleSessionTimeout(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idleSessionTimeout.Set(d)
}

// ResetIdleSessionTimeout clears both the session-level override and any active
// transaction-local override, reverting to the default (0 unless set at startup).
func (m *MultigatewayConnectionState) ResetIdleSessionTimeout() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idleSessionTimeout.Reset()
}

// SetLocalIdleSessionTimeout stores a transaction-local idle-session timeout
// override, cleared on COMMIT/ROLLBACK. While the transaction is active the
// protocol layer does not enforce idle_session_timeout; idle-in-transaction
// semantics are governed by idle_in_transaction_session_timeout, which is not a
// gateway-managed variable here.
func (m *MultigatewayConnectionState) SetLocalIdleSessionTimeout(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idleSessionTimeout.SetLocal(d)
}

// SetLocalIdleSessionTimeoutToDefault sets a transaction-local override to the
// startup/default idle_session_timeout value without destroying any session
// override, matching SET LOCAL ... TO DEFAULT GUC semantics.
func (m *MultigatewayConnectionState) SetLocalIdleSessionTimeoutToDefault() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idleSessionTimeout.SetLocalToDefault()
}

// ResetAllLocalGUCs clears all transaction-local overrides for gateway-managed
// variables. Called at transaction end (COMMIT/ROLLBACK) so the next statement
// observes the session-level (or default) value.
func (m *MultigatewayConnectionState) ResetAllLocalGUCs() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, gmv := range m.gatewayManagedVariablesLocked() {
		gmv.ResetLocal()
	}
}

// ResetGatewayManagedVariables reverts every gateway-managed variable to its
// startup/default value (session and transaction-local overrides both
// cleared). Called by RESET ALL, which must cover GMVs in addition to the
// SessionSettings map since they live outside it.
func (m *MultigatewayConnectionState) ResetGatewayManagedVariables() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, gmv := range m.gatewayManagedVariablesLocked() {
		gmv.Reset()
	}
}

// snapshotSessionSettingsLocked returns a copy of SessionSettings (or nil if
// empty) for storing on a savepoint frame. Caller must hold m.mu.
func (m *MultigatewayConnectionState) snapshotSessionSettingsLocked() map[string]string {
	if len(m.SessionSettings) == 0 {
		return nil
	}
	cp := make(map[string]string, len(m.SessionSettings))
	maps.Copy(cp, m.SessionSettings)
	return cp
}

// findSavepointLocked returns the index of the most recent savepoint with the
// given name (case-sensitive match — matching PostgreSQL's GUC behavior on
// case-folded identifiers, which the parser already canonicalizes). Returns
// -1 if not found. Caller must hold m.mu.
func (m *MultigatewayConnectionState) findSavepointLocked(name string) int {
	for i, v := range slices.Backward(m.savepoints) {
		if v.name == name {
			return i
		}
	}
	return -1
}

// pushFrameLocked snapshots SessionSettings and every gateway-managed variable
// onto their respective stacks under the given savepoint name. Caller must hold m.mu.
func (m *MultigatewayConnectionState) pushFrameLocked(name string) {
	m.savepoints = append(m.savepoints, savepointFrame{
		name:            name,
		sessionSettings: m.snapshotSessionSettingsLocked(),
		openHoldCursors: m.snapshotOpenHoldCursorsLocked(),
	})
	for _, gmv := range m.gatewayManagedVariablesLocked() {
		gmv.Snapshot()
	}
}

// snapshotOpenHoldCursorsLocked returns a copy of the current OpenHoldCursors
// set. Caller must hold m.mu. Used by pushFrameLocked / BeginTransaction so a
// later ROLLBACK TO can compute the cursors declared after the savepoint.
func (m *MultigatewayConnectionState) snapshotOpenHoldCursorsLocked() map[string]bool {
	if len(m.OpenHoldCursors) == 0 {
		return nil
	}
	out := make(map[string]bool, len(m.OpenHoldCursors))
	for name := range m.OpenHoldCursors {
		out[name] = true
	}
	return out
}

// BeginTransaction pushes a BEGIN-level snapshot frame so that a subsequent
// ROLLBACK can revert SET / RESET commands issued inside the transaction.
// A genuine nested BEGIN (depth-0 frame already present with name=="") is a
// no-op, mirroring PostgreSQL's "transaction already in progress" warning.
// Any other pre-existing frames are stale (e.g. a SAVEPOINT that somehow ran
// outside a txn block); discard them and start a fresh BEGIN-level frame so
// rollback semantics match the new transaction, not leftover state.
func (m *MultigatewayConnectionState) BeginTransaction() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notificationTxnOpen = true
	m.notificationTxnEnded = false
	if len(m.savepoints) > 0 && m.savepoints[0].name == "" {
		return
	}
	m.savepoints = m.savepoints[:0]
	for _, gmv := range m.gatewayManagedVariablesLocked() {
		gmv.ClearSnapshots()
	}
	m.pushFrameLocked("")
}

// PushSavepoint snapshots state under the given savepoint name. Called after
// the backend has accepted the SAVEPOINT command.
func (m *MultigatewayConnectionState) PushSavepoint(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pushFrameLocked(name)
}

// ReleaseSavepoint drops the named savepoint frame and any frames above it,
// keeping the current (in-memory) values. PostgreSQL's RELEASE merges any
// state changes from the released sub-transaction into the parent scope.
// If `name` is not on the stack, this is a no-op (the backend would have
// already rejected the RELEASE).
func (m *MultigatewayConnectionState) ReleaseSavepoint(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.findSavepointLocked(name)
	if idx < 0 {
		return
	}
	m.savepoints = m.savepoints[:idx]
	for _, gmv := range m.gatewayManagedVariablesLocked() {
		gmv.PopFrom(idx)
	}
}

// HoldCursorsDeclaredAfterSavepoint returns the names of `DECLARE … WITH HOLD`
// cursors that were declared after the named savepoint was pushed (i.e.,
// would be closed by ROLLBACK TO). The state is not mutated. Returns nil if
// the savepoint isn't on the stack or no qualifying cursors exist.
//
// Used by executeRollbackToSavepoint to enqueue release_portal_names ahead of
// forwarding the ROLLBACK TO statement to the multipooler — PG closes those
// cursors server-side and the reservation pin set must follow suit.
func (m *MultigatewayConnectionState) HoldCursorsDeclaredAfterSavepoint(name string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.findSavepointLocked(name)
	if idx < 0 {
		return nil
	}
	snapshot := m.savepoints[idx].openHoldCursors
	var lost []string
	for cur := range m.OpenHoldCursors {
		if !snapshot[cur] {
			lost = append(lost, cur)
		}
	}
	return lost
}

// RollbackToSavepoint restores SessionSettings and every gateway-managed
// variable from the snapshot under `name`, popping any intermediate frames.
// The named frame stays on the stack so a subsequent ROLLBACK TO `name` can
// be issued again — matching PostgreSQL's behavior of leaving the savepoint
// active after rollback.
//
// OpenHoldCursors is restored to the *intersection* of the snapshot with
// the current open set. This drops:
//   - cursors declared inside the sub-transaction (snapshot doesn't have
//     them — they're closed by ROLLBACK TO), and
//   - cursors that were explicitly CLOSE'd inside the sub-transaction
//     (current set doesn't have them — CLOSE is not transactional in
//     PostgreSQL, so they stay closed after ROLLBACK TO).
//
// The names lost to the first case are returned ahead of time by
// HoldCursorsDeclaredAfterSavepoint so the caller can enqueue
// release_portal_names; the multipooler-side portal pin for the second
// case was already dropped by the original CLOSE.
func (m *MultigatewayConnectionState) RollbackToSavepoint(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.findSavepointLocked(name)
	if idx < 0 {
		return
	}
	m.SessionSettings = nil
	if m.savepoints[idx].sessionSettings != nil {
		m.SessionSettings = make(map[string]string, len(m.savepoints[idx].sessionSettings))
		maps.Copy(m.SessionSettings, m.savepoints[idx].sessionSettings)
	}
	snapshot := m.savepoints[idx].openHoldCursors
	surviving := make(map[string]bool, len(snapshot))
	for cur := range snapshot {
		if m.OpenHoldCursors[cur] {
			surviving[cur] = true
		}
	}
	m.OpenHoldCursors = surviving
	m.savepoints = m.savepoints[:idx+1]
	for _, gmv := range m.gatewayManagedVariablesLocked() {
		gmv.RestoreFromDepth(idx)
	}
}

// CommitTransaction drops all savepoint frames (current values become
// persistent session state) and clears any SET LOCAL overrides on
// gateway-managed variables — SET LOCAL doesn't survive transaction boundaries.
func (m *MultigatewayConnectionState) CommitTransaction() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markNotificationTransactionEndedLocked()
	m.savepoints = nil
	for _, gmv := range m.gatewayManagedVariablesLocked() {
		gmv.ClearSnapshots()
		gmv.ResetLocal()
	}
}

// RollbackTransaction reverts all SET / RESET commands issued inside the
// transaction by restoring SessionSettings and every gateway-managed variable
// from the BEGIN-level (depth 0) snapshot. If no transaction is in progress
// (savepoints empty), this falls back to clearing local overrides only.
func (m *MultigatewayConnectionState) RollbackTransaction() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markNotificationTransactionEndedLocked()
	if len(m.savepoints) == 0 {
		for _, gmv := range m.gatewayManagedVariablesLocked() {
			gmv.ResetLocal()
		}
		return
	}
	m.SessionSettings = nil
	if m.savepoints[0].sessionSettings != nil {
		m.SessionSettings = make(map[string]string, len(m.savepoints[0].sessionSettings))
		maps.Copy(m.SessionSettings, m.savepoints[0].sessionSettings)
	}
	for _, gmv := range m.gatewayManagedVariablesLocked() {
		gmv.RestoreFromDepth(0)
		gmv.ClearSnapshots()
		gmv.ResetLocal()
	}
	m.savepoints = nil
}

// SavepointDepth returns the current size of the savepoint stack. Exposed for
// tests; in production code, transaction state should be queried via
// conn.IsInTransaction() / conn.TxnStatus().
func (m *MultigatewayConnectionState) SavepointDepth() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.savepoints)
}

func (m *MultigatewayConnectionState) markNotificationTransactionEndedLocked() {
	if m.notificationTxnOpen {
		m.notificationTxnEnded = true
	}
}

// SendOrBufferNotification buffers notif while a transaction is active or just
// ended but not drained yet. Otherwise it sends notif to asyncCh while holding
// the same lock used by FlushReadyNotifications, preserving FIFO order across
// the transaction boundary. It returns true when asyncCh was full and notif was
// dropped.
func (m *MultigatewayConnectionState) SendOrBufferNotification(notif *sqltypes.Notification, asyncCh chan<- *sqltypes.Notification) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.notificationTxnOpen {
		if len(m.PendingNotifications) >= maxPendingNotifications {
			return true
		}
		m.PendingNotifications = append(m.PendingNotifications, notif)
		return false
	}
	if asyncCh == nil {
		return true
	}
	select {
	case asyncCh <- notif:
		return false
	default:
		return true
	}
}

// FlushReadyNotifications drains notifications buffered for a completed
// transaction. It sends them while holding m.mu so a concurrent forwarder cannot
// enqueue newer notifications first. Dropped notifications are returned for
// logging/metrics outside the lock.
func (m *MultigatewayConnectionState) FlushReadyNotifications(asyncCh chan<- *sqltypes.Notification) []*sqltypes.Notification {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.notificationTxnEnded {
		return nil
	}
	pending := m.PendingNotifications
	m.PendingNotifications = nil
	m.notificationTxnOpen = false
	m.notificationTxnEnded = false
	if asyncCh == nil {
		return nil
	}
	var dropped []*sqltypes.Notification
	for _, notif := range pending {
		select {
		case asyncCh <- notif:
		default:
			dropped = append(dropped, notif)
		}
	}
	return dropped
}

// DrainPendingNotifications clears notifications buffered for this connection.
func (m *MultigatewayConnectionState) DrainPendingNotifications() []*sqltypes.Notification {
	m.mu.Lock()
	defer m.mu.Unlock()
	pending := m.PendingNotifications
	m.PendingNotifications = nil
	m.notificationTxnOpen = false
	m.notificationTxnEnded = false
	return pending
}

// GetStatementTimeout returns the effective statement timeout:
// the session override if set, otherwise the default.
func (m *MultigatewayConnectionState) GetStatementTimeout() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.statementTimeout.GetEffective()
}

// ShowStatementTimeout returns the effective statement timeout formatted
// using PostgreSQL's GUC_UNIT_MS display convention for SHOW output.
func (m *MultigatewayConnectionState) ShowStatementTimeout() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return formatDurationPg(m.statementTimeout.GetEffective())
}

// InitStatementTimeout sets the default for the statement timeout variable.
// Called once during connection initialization with the value from startup params
// (if present) or the --statement-timeout flag.
func (m *MultigatewayConnectionState) InitStatementTimeout(defaultValue time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout = NewGatewayManagedVariable(defaultValue)
}

// GetIdleSessionTimeout returns the effective idle_session_timeout. A zero
// duration disables idle-session termination, matching PostgreSQL's default.
func (m *MultigatewayConnectionState) GetIdleSessionTimeout() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.idleSessionTimeout.GetEffective()
}

// ShowIdleSessionTimeout returns the effective idle_session_timeout formatted
// using PostgreSQL's GUC_UNIT_MS display convention for SHOW output.
func (m *MultigatewayConnectionState) ShowIdleSessionTimeout() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return formatDurationPg(m.idleSessionTimeout.GetEffective())
}

// InitIdleSessionTimeout sets the default for idle_session_timeout. Called once
// during connection initialization with the value from startup params, or 0.
func (m *MultigatewayConnectionState) InitIdleSessionTimeout(defaultValue time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idleSessionTimeout = NewGatewayManagedVariable(defaultValue)
}

// TargetReplica returns true if this connection targets a replica.
// Set once at connection initialization based on which port the connection arrived on.
func (m *MultigatewayConnectionState) TargetReplica() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.targetReplica
}

// SetTargetReplica sets whether this connection targets a replica. Exposed
// for tests in other multigateway packages (e.g. scatterconn) that need to
// exercise TargetReplica()-dependent routing without a real connection
// arriving on the replica-reads listener. Production code sets this once,
// directly on the unexported field, when the state is created — see
// MultigatewayHandler.getConnectionState (handler.go:437).
func (m *MultigatewayConnectionState) SetTargetReplica(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.targetReplica = v
}

// GetSessionSettings returns a merged view of startup parameters and session settings.
// Session settings (from SET commands) take precedence over startup params for the same key.
// When a variable is RESET (deleted from SessionSettings), the startup param value becomes visible again.
// Returns nil if neither startup params nor session settings exist.
// The copy prevents external mutation of the internal state.
func (m *MultigatewayConnectionState) GetSessionSettings() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.StartupParams) == 0 && len(m.SessionSettings) == 0 {
		return nil
	}
	// Start with startup params, then overlay session settings. Canonicalizing
	// keys here preserves PostgreSQL's ASCII-case-insensitive GUC semantics and
	// guarantees SET/session values win over same-GUC startup params regardless
	// of spelling (for example TimeZone vs timezone).
	merged := make(map[string]string, len(m.StartupParams)+len(m.SessionSettings))
	for k, v := range m.StartupParams {
		merged[pgsettings.CanonicalGUCName(k)] = v
	}
	for k, v := range m.SessionSettings {
		merged[pgsettings.CanonicalGUCName(k)] = v
	}
	return merged
}

// GetSessionVariable returns the value of a specific session variable.
// Returns (value, true) if exists, ("", false) if not.
func (m *MultigatewayConnectionState) GetSessionVariable(name string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, exists := m.SessionSettings[pgsettings.CanonicalGUCName(name)]
	return value, exists
}

// HasTempTableReservation returns true if any shard state has a reserved
// connection with the temp table reason set.
func (m *MultigatewayConnectionState) HasTempTableReservation() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ss := range m.ShardStates {
		if ss.ReservedState != nil && protoutil.HasTempTableReason(ss.ReservedState.GetReservationReasons()) {
			return true
		}
	}
	return false
}

// GetStartupParams returns a copy of the startup parameters.
// Returns nil if no startup params were set.
func (m *MultigatewayConnectionState) GetStartupParams() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.StartupParams) == 0 {
		return nil
	}
	params := make(map[string]string, len(m.StartupParams))
	maps.Copy(params, m.StartupParams)
	return params
}

// --- LISTEN/NOTIFY state tracking ---

// IsListening returns true if the channel is actively listened.
func (m *MultigatewayConnectionState) IsListening(channel string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ListenChannels[channel]
}

// AddListenChannel registers a channel as actively listened.
func (m *MultigatewayConnectionState) AddListenChannel(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ListenChannels == nil {
		m.ListenChannels = make(map[string]bool)
	}
	m.ListenChannels[channel] = true
}

// RemoveListenChannel removes a channel from active listeners.
func (m *MultigatewayConnectionState) RemoveListenChannel(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.ListenChannels, channel)
}

// ClearListenChannels removes all listen channels (UNLISTEN *).
func (m *MultigatewayConnectionState) ClearListenChannels() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ListenChannels = nil
}

// GetListenChannels returns a copy of the active listen channels.
func (m *MultigatewayConnectionState) GetListenChannels() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.ListenChannels) == 0 {
		return nil
	}
	channels := make(map[string]bool, len(m.ListenChannels))
	maps.Copy(channels, m.ListenChannels)
	return channels
}

// AddPendingListen adds a LISTEN to the pending actions list (applied at COMMIT).
func (m *MultigatewayConnectionState) AddPendingListen(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingActions = append(m.pendingActions, pendingListenAction{actionType: pendingListen, channel: channel})
}

// AddPendingUnlisten adds an UNLISTEN to the pending actions list.
func (m *MultigatewayConnectionState) AddPendingUnlisten(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingActions = append(m.pendingActions, pendingListenAction{actionType: pendingUnlisten, channel: channel})
}

// AddPendingUnlistenAll adds an UNLISTEN * to the pending actions list.
func (m *MultigatewayConnectionState) AddPendingUnlistenAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingActions = append(m.pendingActions, pendingListenAction{actionType: pendingUnlistenAll})
}

// CommitPendingListens applies pending listen/unlisten actions on transaction commit.
// Actions are processed in the order they were issued (matching PostgreSQL's
// AtCommit_Notify behavior), then a net diff is computed against the pre-transaction
// state to produce the subscribe/unsubscribe lists for the notification manager.
func (m *MultigatewayConnectionState) CommitPendingListens() (subscribes []string, unsubscribes []string, unsubscribeAll bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Snapshot starting state for net diff computation.
	startChannels := make(map[string]bool, len(m.ListenChannels))
	maps.Copy(startChannels, m.ListenChannels)

	// Process actions in order, updating ListenChannels to the final state.
	for _, action := range m.pendingActions {
		switch action.actionType {
		case pendingListen:
			if m.ListenChannels == nil {
				m.ListenChannels = make(map[string]bool)
			}
			m.ListenChannels[action.channel] = true
		case pendingUnlisten:
			delete(m.ListenChannels, action.channel)
		case pendingUnlistenAll:
			unsubscribeAll = true
			m.ListenChannels = nil
		}
	}
	m.pendingActions = nil

	// Compute net diff between starting and final state.
	if unsubscribeAll {
		// UNLISTEN * clears everything — only report channels in the final state
		// as new subscribes (they were added after UNLISTEN *).
		for ch := range m.ListenChannels {
			subscribes = append(subscribes, ch)
		}
	} else {
		for ch := range m.ListenChannels {
			if !startChannels[ch] {
				subscribes = append(subscribes, ch)
			}
		}
		for ch := range startChannels {
			if !m.ListenChannels[ch] {
				unsubscribes = append(unsubscribes, ch)
			}
		}
	}

	return subscribes, unsubscribes, unsubscribeAll
}

// DiscardPendingListens discards pending listen/unlisten state on rollback.
func (m *MultigatewayConnectionState) DiscardPendingListens() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingActions = nil
}

// HasPendingListens returns true if there are pending listen/unlisten changes.
func (m *MultigatewayConnectionState) HasPendingListens() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pendingActions) > 0
}
