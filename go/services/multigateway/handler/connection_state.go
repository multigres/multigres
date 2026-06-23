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
	"strings"
	"sync"
	"time"

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
)

// MultiGatewayConnectionState keeps track of the information specific
// to each connection.
// MultiGatewayConnectionState holds all per-connection gateway state. A
// MultiGatewayConnectionState is created once when a client connects and
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
type MultiGatewayConnectionState struct {
	mu sync.Mutex
	// NOTE: We are not storing the map of Prepared Statements even though
	// that is also connection level information. We do not require storing
	// it because we have prepared statement consolidation in MultiGateway
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

	// SubSync coordinates LISTEN/NOTIFY subscriptions with the notification manager.
	// Set by the handler at connection initialization; called by engine primitives
	// to apply subscription changes before reporting success to the client.
	SubSync SubscriptionSync

	// statementTimeout is managed entirely by the gateway and is NOT forwarded
	// to PostgreSQL. The default is initialized from startup params (if
	// present) or the --statement-timeout flag. Parsed at SET time to avoid
	// repeated parsing on every query.
	statementTimeout GatewayManagedVariable[time.Duration]

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

// NewMultiGatewayConnectionState creates a new MultiGatewayConnectionState.
func NewMultiGatewayConnectionState() *MultiGatewayConnectionState {
	return &MultiGatewayConnectionState{
		mu:              sync.Mutex{},
		Portals:         make(map[string]*preparedstatement.PortalInfo),
		OpenHoldCursors: make(map[string]bool),
	}
}

// AddOpenHoldCursor records a `DECLARE ... WITH HOLD` cursor as currently open
// on this gateway session. Idempotent.
func (m *MultiGatewayConnectionState) AddOpenHoldCursor(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.OpenHoldCursors == nil {
		m.OpenHoldCursors = make(map[string]bool)
	}
	m.OpenHoldCursors[name] = true
}

// RemoveOpenHoldCursor drops the named HOLD cursor from the open set.
// Returns true if the entry existed.
func (m *MultiGatewayConnectionState) RemoveOpenHoldCursor(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.OpenHoldCursors[name]; !ok {
		return false
	}
	delete(m.OpenHoldCursors, name)
	return true
}

// HasOpenHoldCursor reports whether the named HOLD cursor is open.
func (m *MultiGatewayConnectionState) HasOpenHoldCursor(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.OpenHoldCursors[name]
}

// OpenHoldCursorNames returns a snapshot of the open HOLD cursor names.
// Used to materialise the target list for `CLOSE ALL`.
func (m *MultiGatewayConnectionState) OpenHoldCursorNames() []string {
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
func (m *MultiGatewayConnectionState) HasAnyOpenHoldCursor() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.OpenHoldCursors) > 0
}

// ClearOpenHoldCursors drops every tracked HOLD cursor. Called at ROLLBACK,
// when PostgreSQL closes all open cursors regardless of WITH HOLD.
func (m *MultiGatewayConnectionState) ClearOpenHoldCursors() {
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
func (m *MultiGatewayConnectionState) HoldCursorsDeclaredInTxn() []string {
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
func (m *MultiGatewayConnectionState) RestoreOpenHoldCursorsToBeginSnapshot() {
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
func (m *MultiGatewayConnectionState) StorePortalInfo(portal *query.Portal, psi *preparedstatement.PreparedStatementInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Portals[portal.Name] = preparedstatement.NewPortalInfo(psi, portal)
}

// GetPortalInfo gets the portal information for a previously stored portal.
func (m *MultiGatewayConnectionState) GetPortalInfo(portalName string) *preparedstatement.PortalInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Portals[portalName]
}

// DeletePortalInfo deletes the portal information
func (m *MultiGatewayConnectionState) DeletePortalInfo(portalName string) {
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
func (m *MultiGatewayConnectionState) GetMatchingShardState(target *query.Target) *ShardState {
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
func (m *MultiGatewayConnectionState) SetReservedConnection(target *query.Target, rs *query.ReservedState) {
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
func (m *MultiGatewayConnectionState) ClearReservedConnection(target *query.Target) {
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

// ClearAllReservedConnections removes all reserved connection entries.
// Called after COMMIT or ROLLBACK to clean up stale shard state.
func (m *MultiGatewayConnectionState) ClearAllReservedConnections() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ShardStates = nil
}

// SetSessionVariable sets a session variable (from SET command).
// The variable name and value are stored to be propagated to multipooler.
func (m *MultiGatewayConnectionState) SetSessionVariable(name, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.SessionSettings == nil {
		m.SessionSettings = make(map[string]string)
	}
	m.SessionSettings[name] = value
}

// ResetSessionVariable removes a session variable (from RESET command).
func (m *MultiGatewayConnectionState) ResetSessionVariable(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.SessionSettings, name)
}

// ResetAllSessionVariables clears all session variables (from RESET ALL command).
func (m *MultiGatewayConnectionState) ResetAllSessionVariables() {
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
func (m *MultiGatewayConnectionState) gatewayManagedVariablesLocked() [1]gmvLifecycle {
	return [1]gmvLifecycle{
		&m.statementTimeout,
	}
}

// SetStatementTimeout sets the session-level statement timeout override.
func (m *MultiGatewayConnectionState) SetStatementTimeout(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout.Set(d)
}

// ResetStatementTimeout clears both the session-level override and any
// active transaction-local override, reverting to the default (from startup
// params or flag). Matches PostgreSQL: RESET inside a transaction with a
// prior SET LOCAL supersedes the LOCAL — effective value becomes the default.
func (m *MultiGatewayConnectionState) ResetStatementTimeout() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout.Reset()
}

// SetLocalStatementTimeout stores a transaction-local statement timeout
// override (from SET LOCAL statement_timeout). Cleared on COMMIT/ROLLBACK
// via ResetAllLocalGUCs.
func (m *MultiGatewayConnectionState) SetLocalStatementTimeout(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout.SetLocal(d)
}

// SetLocalStatementTimeoutToDefault sets the transaction-local override to
// the server default (from SET LOCAL statement_timeout TO DEFAULT). This
// masks any session-level override for the duration of the transaction
// without destroying it; the session value is restored on COMMIT/ROLLBACK
// via ResetAllLocalGUCs.
func (m *MultiGatewayConnectionState) SetLocalStatementTimeoutToDefault() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout.SetLocalToDefault()
}

// gatewayManagedVariableNames is the canonical set of session variables the
// gateway manages itself: SET / SHOW / RESET are handled locally and the value
// is never written to SessionSettings (so it is not replayed to backends on
// pool rotation). This is the single source of truth consulted by the planner
// (routing decisions) and the engine (set_config execution). Names compare
// case-insensitively.
var gatewayManagedVariableNames = map[string]struct{}{
	"statement_timeout": {},
}

// IsGatewayManagedVariable reports whether name (case-insensitive) is a session
// variable managed entirely by the gateway and not forwarded to PostgreSQL.
func IsGatewayManagedVariable(name string) bool {
	_, ok := gatewayManagedVariableNames[strings.ToLower(name)]
	return ok
}

// ApplyGatewayManagedVariable applies a SET / set_config(...) of a
// gateway-managed variable to gateway-local state instead of the
// SessionSettings map. Routing here (rather than SetSessionVariable) is what
// keeps SHOW consistent and keeps the variable out of GetSessionSettings, so it
// is never replayed to a backend on pool rotation.
//
// Returns (handled, err): handled is false when name is not gateway-managed and
// the caller must fall back to SessionSettings. err is non-nil when value is
// invalid for the variable (e.g. an unparsable statement_timeout), mirroring
// PostgreSQL's set-time validation.
//
// isLocal selects the transaction-local override (SET LOCAL / set_config(...,
// true)) over the session-level override.
func (m *MultiGatewayConnectionState) ApplyGatewayManagedVariable(name, value string, isLocal bool) (bool, error) {
	switch strings.ToLower(name) {
	case "statement_timeout":
		d, err := ParsePostgresInterval("statement_timeout", value)
		if err != nil {
			return true, err
		}
		if isLocal {
			m.SetLocalStatementTimeout(d)
		} else {
			m.SetStatementTimeout(d)
		}
		return true, nil
	default:
		return false, nil
	}
}

// ResetAllLocalGUCs clears all transaction-local overrides for gateway-managed
// variables. Called at transaction end (COMMIT/ROLLBACK) so the next statement
// observes the session-level (or default) value.
func (m *MultiGatewayConnectionState) ResetAllLocalGUCs() {
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
func (m *MultiGatewayConnectionState) ResetGatewayManagedVariables() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, gmv := range m.gatewayManagedVariablesLocked() {
		gmv.Reset()
	}
}

// snapshotSessionSettingsLocked returns a copy of SessionSettings (or nil if
// empty) for storing on a savepoint frame. Caller must hold m.mu.
func (m *MultiGatewayConnectionState) snapshotSessionSettingsLocked() map[string]string {
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
func (m *MultiGatewayConnectionState) findSavepointLocked(name string) int {
	for i, v := range slices.Backward(m.savepoints) {
		if v.name == name {
			return i
		}
	}
	return -1
}

// pushFrameLocked snapshots SessionSettings and every gateway-managed variable
// onto their respective stacks under the given savepoint name. Caller must hold m.mu.
func (m *MultiGatewayConnectionState) pushFrameLocked(name string) {
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
func (m *MultiGatewayConnectionState) snapshotOpenHoldCursorsLocked() map[string]bool {
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
func (m *MultiGatewayConnectionState) BeginTransaction() {
	m.mu.Lock()
	defer m.mu.Unlock()
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
func (m *MultiGatewayConnectionState) PushSavepoint(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pushFrameLocked(name)
}

// ReleaseSavepoint drops the named savepoint frame and any frames above it,
// keeping the current (in-memory) values. PostgreSQL's RELEASE merges any
// state changes from the released sub-transaction into the parent scope.
// If `name` is not on the stack, this is a no-op (the backend would have
// already rejected the RELEASE).
func (m *MultiGatewayConnectionState) ReleaseSavepoint(name string) {
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
func (m *MultiGatewayConnectionState) HoldCursorsDeclaredAfterSavepoint(name string) []string {
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
func (m *MultiGatewayConnectionState) RollbackToSavepoint(name string) {
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
func (m *MultiGatewayConnectionState) CommitTransaction() {
	m.mu.Lock()
	defer m.mu.Unlock()
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
func (m *MultiGatewayConnectionState) RollbackTransaction() {
	m.mu.Lock()
	defer m.mu.Unlock()
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
func (m *MultiGatewayConnectionState) SavepointDepth() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.savepoints)
}

// GetStatementTimeout returns the effective statement timeout:
// the session override if set, otherwise the default.
func (m *MultiGatewayConnectionState) GetStatementTimeout() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.statementTimeout.GetEffective()
}

// ShowStatementTimeout returns the effective statement timeout formatted
// using PostgreSQL's GUC_UNIT_MS display convention for SHOW output.
func (m *MultiGatewayConnectionState) ShowStatementTimeout() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return formatDurationPg(m.statementTimeout.GetEffective())
}

// InitStatementTimeout sets the default for the statement timeout variable.
// Called once during connection initialization with the value from startup params
// (if present) or the --statement-timeout flag.
func (m *MultiGatewayConnectionState) InitStatementTimeout(defaultValue time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout = NewGatewayManagedVariable(defaultValue)
}

// TargetReplica returns true if this connection targets a replica.
// Set once at connection initialization based on which port the connection arrived on.
func (m *MultiGatewayConnectionState) TargetReplica() bool {
	return m.targetReplica
}

// GetSessionSettings returns a merged view of startup parameters and session settings.
// Session settings (from SET commands) take precedence over startup params for the same key.
// When a variable is RESET (deleted from SessionSettings), the startup param value becomes visible again.
// Returns nil if neither startup params nor session settings exist.
// The copy prevents external mutation of the internal state.
func (m *MultiGatewayConnectionState) GetSessionSettings() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.StartupParams) == 0 && len(m.SessionSettings) == 0 {
		return nil
	}
	// Start with startup params, then overlay session settings (which take precedence)
	merged := make(map[string]string, len(m.StartupParams)+len(m.SessionSettings))
	maps.Copy(merged, m.StartupParams)
	maps.Copy(merged, m.SessionSettings)
	return merged
}

// GetSessionVariable returns the value of a specific session variable.
// Returns (value, true) if exists, ("", false) if not.
func (m *MultiGatewayConnectionState) GetSessionVariable(name string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, exists := m.SessionSettings[name]
	return value, exists
}

// HasTempTableReservation returns true if any shard state has a reserved
// connection with the temp table reason set.
func (m *MultiGatewayConnectionState) HasTempTableReservation() bool {
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
func (m *MultiGatewayConnectionState) GetStartupParams() map[string]string {
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
func (m *MultiGatewayConnectionState) IsListening(channel string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ListenChannels[channel]
}

// AddListenChannel registers a channel as actively listened.
func (m *MultiGatewayConnectionState) AddListenChannel(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ListenChannels == nil {
		m.ListenChannels = make(map[string]bool)
	}
	m.ListenChannels[channel] = true
}

// RemoveListenChannel removes a channel from active listeners.
func (m *MultiGatewayConnectionState) RemoveListenChannel(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.ListenChannels, channel)
}

// ClearListenChannels removes all listen channels (UNLISTEN *).
func (m *MultiGatewayConnectionState) ClearListenChannels() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ListenChannels = nil
}

// GetListenChannels returns a copy of the active listen channels.
func (m *MultiGatewayConnectionState) GetListenChannels() map[string]bool {
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
func (m *MultiGatewayConnectionState) AddPendingListen(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingActions = append(m.pendingActions, pendingListenAction{actionType: pendingListen, channel: channel})
}

// AddPendingUnlisten adds an UNLISTEN to the pending actions list.
func (m *MultiGatewayConnectionState) AddPendingUnlisten(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingActions = append(m.pendingActions, pendingListenAction{actionType: pendingUnlisten, channel: channel})
}

// AddPendingUnlistenAll adds an UNLISTEN * to the pending actions list.
func (m *MultiGatewayConnectionState) AddPendingUnlistenAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingActions = append(m.pendingActions, pendingListenAction{actionType: pendingUnlistenAll})
}

// CommitPendingListens applies pending listen/unlisten actions on transaction commit.
// Actions are processed in the order they were issued (matching PostgreSQL's
// AtCommit_Notify behavior), then a net diff is computed against the pre-transaction
// state to produce the subscribe/unsubscribe lists for the notification manager.
func (m *MultiGatewayConnectionState) CommitPendingListens() (subscribes []string, unsubscribes []string, unsubscribeAll bool) {
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
func (m *MultiGatewayConnectionState) DiscardPendingListens() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingActions = nil
}

// HasPendingListens returns true if there are pending listen/unlisten changes.
func (m *MultiGatewayConnectionState) HasPendingListens() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pendingActions) > 0
}
