// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package executor implements query execution for multipooler.
// It provides the QueryService interface implementation that executes queries
// against PostgreSQL using per-user connection pools.
package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
)

// pendingDefaultsMarker is the subset of *reserved.Conn that
// noteConnectionDefaultsChange needs in order to defer a defaults-invalidation
// bump to COMMIT. Declared as an interface so the decision logic can be unit
// tested with a lightweight double instead of a live reserved connection.
type pendingDefaultsMarker interface {
	MarkPendingDefaultsInvalidation()
}

// Executor implements the QueryService interface for executing queries against PostgreSQL.
// It uses the connpoolmanager for per-user connection pool management and consolidates
// prepared statements across connections to avoid redundant parsing.
type Executor struct {
	logger             *slog.Logger
	poolManager        connpoolmanager.PoolManager
	poolerConsolidator *preparedstatement.PoolerConsolidator
	poolerID           *clustermetadatapb.ID

	// vpidStampEnabled toggles the multigres_vpid:<id> stamping on PostgreSQL
	// backends and the matching application_name filter in
	// sessionSettingsForPool. Both must move together: stamping without
	// filtering lets ApplySettings wipe the stamp via RESET application_name;
	// filtering without stamping silently swallows client-set application_name.
	vpidStampEnabled bool
}

// sessionSettingsForPool returns a copy of settings safe to apply to a pooled
// (regular or reserved) PostgreSQL connection.
//
// When vpid stamping is enabled, it excludes application_name. The pool's
// connstate cache must never track a client-supplied application_name: when
// SetApplicationName is later called out-of-band on the same connection (to
// stamp `multigres_vpid:<id>` for lock-detection mapping), connstate is
// unaware of the new value, and a subsequent ApplySettings diff between
// connstate.current (still holding the client's app_name) and the desired
// settings on the next query emits a RESET application_name that wipes the
// stamp before the query runs. Filtering here prevents that ABA on every code
// path that pushes SessionSettings into the pool.
//
// When stamping is disabled, settings pass through unchanged so client-set
// application_name reaches the backend normally.
func (e *Executor) sessionSettingsForPool(settings map[string]string) map[string]string {
	if !e.vpidStampEnabled {
		return settings
	}
	if settings == nil {
		return nil
	}
	out := make(map[string]string, len(settings))
	for k, v := range settings {
		if strings.EqualFold(k, "application_name") {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (e *Executor) sessionSettingsFromOptions(options *query.ExecuteOptions) map[string]string {
	if options == nil {
		return nil
	}
	return e.sessionSettingsForPool(options.SessionSettings)
}

func (e *Executor) applyReservedSessionSettingsIfNeeded(ctx context.Context, conn *reserved.Conn, options *query.ExecuteOptions) error {
	if options == nil || options.SessionSettings == nil {
		return nil
	}
	return e.poolManager.ApplySettingsToConn(ctx, conn.Conn(), e.sessionSettingsForPool(options.SessionSettings))
}

func (e *Executor) releaseReservedConn(conn *reserved.Conn, reason reserved.ReleaseReason, options *query.ExecuteOptions) {
	conn.Release(reason, e.sessionSettingsFromOptions(options))
}

// stampVpidOnReserved tags a reserved connection's PostgreSQL backend with
// `multigres_vpid:<client_connection_id>` in application_name so
// lock-detection functions (e.g. an override of
// pg_isolation_test_session_is_blocked) can map a multigateway virtual PID
// back to the real backend PID via pg_stat_activity. Best-effort: a SET
// failure does not block the actual query — only lock detection through the
// proxy depends on the tag. No-op when vpid stamping is disabled.
func (e *Executor) stampVpidOnReserved(ctx context.Context, conn *reserved.Conn, options *query.ExecuteOptions) {
	if !e.vpidStampEnabled || options == nil || options.ClientConnectionId == 0 {
		return
	}
	_ = conn.SetApplicationName(ctx, fmt.Sprintf("multigres_vpid:%d", options.ClientConnectionId))
}

// stampVpidOnRegular tags a pooled regular connection with the same
// `multigres_vpid:<id>` marker. Pooled connections are shared across clients,
// so the next checkout will overwrite this stamp; for the duration of the
// current query the backend is correctly attributed to its client vpid.
// No-op when vpid stamping is disabled.
func (e *Executor) stampVpidOnRegular(ctx context.Context, conn *regular.Conn, options *query.ExecuteOptions) {
	if !e.vpidStampEnabled || options == nil || options.ClientConnectionId == 0 {
		return
	}
	_ = conn.SetApplicationName(ctx, fmt.Sprintf("multigres_vpid:%d", options.ClientConnectionId))
}

// NewExecutor creates a new Executor instance.
// vpidStampEnabled controls whether multigres_vpid:<id> is stamped on
// PostgreSQL backends and whether application_name is filtered from pool
// SessionSettings.
func NewExecutor(logger *slog.Logger, poolManager connpoolmanager.PoolManager, poolerID *clustermetadatapb.ID, vpidStampEnabled bool) *Executor {
	return &Executor{
		logger:             logger,
		poolManager:        poolManager,
		poolerConsolidator: preparedstatement.NewPoolerConsolidator(),
		poolerID:           poolerID,
		vpidStampEnabled:   vpidStampEnabled,
	}
}

// buildReservedState constructs a ReservedState from the current state of a reserved connection.
func (e *Executor) buildReservedState(reservedConn *reserved.Conn) *query.ReservedState {
	return e.buildReservedStateFromAPI(reservedConn)
}

// buildReservedStateFromAPI constructs a ReservedState from any value satisfying
// reservedConnAPI. Used by streamExecuteOnReservedConn so the helper can be exercised
// in unit tests with a mock conn instead of a real *reserved.Conn.
func (e *Executor) buildReservedStateFromAPI(rc reservedConnAPI) *query.ReservedState {
	return &query.ReservedState{
		ReservedConnectionId: uint64(rc.ConnID()),
		PoolerId:             e.poolerID,
		ReservationReasons:   rc.RemainingReasons(),
		BackendProcessId:     rc.ProcessID(),
	}
}

// noteConnectionDefaultsChange handles a statement that the multigateway flagged
// (options.InvalidatesConnectionDefaults) as changing per-database/role GUC
// defaults. Call it only after the statement executed successfully.
//
// The change is durable when the connection is no longer inside a transaction
// block: for an autocommit statement (regular pooled connection, rc == nil) or a
// non-transactional reserved connection the post-execution status is idle, so we
// bump the pools' defaults generation immediately. When the connection is still
// in a transaction block the change is durable only at COMMIT, so we mark the
// reserved connection pending and let ConcludeTransaction bump on a successful
// COMMIT (and discard the mark on ROLLBACK). This mirrors PostgreSQL's
// transactional-DDL semantics for ALTER DATABASE/ROLE ... SET.
//
// rc is the *reserved.Conn the statement ran on, or nil for an autocommit
// statement on a regular pooled connection. It is taken as an interface so the
// decision logic is unit-testable without a live backend connection. Pass a
// literal nil (not a typed-nil *reserved.Conn) for the regular-connection path.
func (e *Executor) noteConnectionDefaultsChange(options *query.ExecuteOptions, txnStatus protocol.TransactionStatus, rc pendingDefaultsMarker) {
	if options == nil || !options.GetInvalidatesConnectionDefaults() {
		return
	}
	if rc != nil && txnStatus != protocol.TxnStatusIdle {
		// In a transaction block (or a failed block whose COMMIT will roll back):
		// defer to COMMIT.
		rc.MarkPendingDefaultsInvalidation()
		return
	}
	e.poolManager.InvalidateConnectionDefaults()
}

// ExecuteQuery implements queryservice.QueryService.
// It executes a query using a pooled connection for the specified user.
// If ReservedConnectionId is set in options, uses that reserved connection instead.
// Returns ReservedState with the authoritative reservation state from the multipooler.
func (e *Executor) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	if target == nil {
		target = &query.Target{}
	}

	user := e.getUserFromOptions(options)
	e.logger.DebugContext(ctx, "executing query",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"user", user,
		"query", sql)

	// Check if we should use an existing reserved connection
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			// Connection destroyed — return zero state so gateway clears its tracking
			return nil, nil, mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
		}

		// Existing reserved connections bypass the pool's normal settings checkout
		// path. Apply deferred gateway session settings before user SQL when they
		// differ from connstate.
		if err := e.applyReservedSessionSettingsIfNeeded(ctx, reservedConn, options); err != nil {
			return nil, e.buildReservedState(reservedConn), fmt.Errorf("failed to apply session settings: %w", err)
		}

		// Stamp multigres_vpid:<id> AFTER ApplySettingsToConn. When the
		// filtered desired settings collapse to nil (e.g. the only client
		// setting was application_name), ApplySettings issues RESET ALL on
		// the reserved conn — which would wipe a stamp set earlier in this
		// function. Restamping after the reset ensures the tag is in place
		// for the actual query.
		e.stampVpidOnReserved(ctx, reservedConn, options)

		results, err := reservedConn.Query(ctx, sql)
		if err != nil {
			// Query failed but connection still exists — return current state
			return nil, e.buildReservedState(reservedConn), wrapQueryError(err)
		}
		e.noteConnectionDefaultsChange(options, reservedConn.TxnStatus(), reservedConn)

		if len(results) == 0 {
			return &sqltypes.Result{}, e.buildReservedState(reservedConn), nil
		}
		return results[0], e.buildReservedState(reservedConn), nil
	}

	// Get session settings from options
	var settings map[string]string
	if options != nil {
		settings = e.sessionSettingsForPool(options.SessionSettings)
	}

	// Get a connection from the pool for this user
	clientKey, serverKey := scramKeysFromOptions(options)
	conn, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user, clientKey, serverKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get connection for user %s: %w", user, err)
	}
	defer conn.Recycle()

	// Stamp multigres_vpid on this pooled regular conn too — the next
	// client to draw from the pool will overwrite it, but for the duration
	// of the current query the backend is correctly tagged.
	e.stampVpidOnRegular(ctx, conn.Conn, options)

	// Execute the query - the regular.Conn.QueryWithRetry returns []*sqltypes.Result
	// with proper field info, rows, and command tags already populated.
	// Uses retry variant since this is a stateless pool query.
	results, err := conn.Conn.QueryWithRetry(ctx, sql)
	if err != nil {
		return nil, nil, wrapQueryError(err)
	}
	e.noteConnectionDefaultsChange(options, conn.Conn.TxnStatus(), nil)

	// Return first result (simple query returns single result)
	if len(results) == 0 {
		return &sqltypes.Result{}, nil, nil
	}
	return results[0], nil, nil
}

// StreamExecute executes a query and streams results back via callback.
// This implements the queryservice.QueryService interface.
//
// Handles three cases based on the request:
//   - options.ReservedConnectionId > 0: use existing reserved connection
//   - reservationOptions has non-zero reasons && ReservedConnectionId == 0: create new reserved connection
//   - Neither: use regular pooled connection
//
// When reservationOptions is set on an existing reserved connection, the reasons are
// OR'd into the reservation (e.g., adding ReasonTransaction to a temp-table-reserved conn).
//
// If options.PreparedStatement is set, the statement is parsed on the chosen
// backend connection via ensurePreparedWithName() before `sql` runs. This is
// used for wrapped EXECUTE forms (EXPLAIN EXECUTE, CREATE TABLE ... AS EXECUTE)
// that reference a gateway-managed prepared statement by its canonical name.
//
// Returns ReservedState with the authoritative reservation state from the multipooler.
func (e *Executor) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	if target == nil {
		target = &query.Target{}
	}

	user := e.getUserFromOptions(options)
	reasons := protoutil.GetReasons(reservationOptions)
	e.logger.DebugContext(ctx, "stream executing query",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"user", user,
		"query", sql)

	preparedStmt := options.GetPreparedStatement()

	// Case 1: Use an existing reserved connection
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			// Connection destroyed — return zero state so gateway clears its tracking
			return nil, mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
		}

		if err := e.applyReservedSessionSettingsIfNeeded(ctx, reservedConn, options); err != nil {
			return e.buildReservedState(reservedConn), fmt.Errorf("failed to prepare reserved connection: %w", err)
		}

		// Stamp multigres_vpid:<id> AFTER ApplySettingsToConn so a RESET
		// ALL emitted by the empty-desired-settings path doesn't wipe it
		// (see the matching ordering in ExecuteQuery).
		e.stampVpidOnReserved(ctx, reservedConn, options)

		// If the query references a gateway-managed prepared statement
		// (wrapped EXECUTE forms), ensure it is parsed on this backend
		// connection before running the query. We do this before delegating
		// to streamExecuteOnReservedConn because that helper operates over
		// the reservedConnAPI interface which does not expose the underlying
		// *regular.Conn needed by ensurePreparedWithName.
		if preparedStmt != nil {
			if err := e.ensurePreparedWithName(ctx, reservedConn.Conn(), preparedStmt); err != nil {
				return e.buildReservedState(reservedConn), fmt.Errorf("failed to ensure prepared statement on reserved connection: %w", err)
			}
		}

		rs, err := e.streamExecuteOnReservedConn(ctx, reservedConn, sql, reservationOptions, e.sessionSettingsFromOptions(options), callback)
		// Only inspect the reserved connection when the statement is flagged and the
		// connection is still held: streamExecuteOnReservedConn may have released it
		// (e.g. a CLOSE that drained the last reservation reason), after which it can
		// be re-borrowed by another session and must not be touched here.
		if err == nil && options.GetInvalidatesConnectionDefaults() && !reservedConn.IsReleased() {
			e.noteConnectionDefaultsChange(options, reservedConn.TxnStatus(), reservedConn)
		}
		return rs, err
	}

	// Case 2: Create a new reserved connection
	if reasons != 0 {
		return e.reserveAndStreamExecute(ctx, sql, options, reservationOptions, callback)
	}

	// Case 3: Use regular pooled connection
	var settings map[string]string
	if options != nil {
		settings = e.sessionSettingsForPool(options.SessionSettings)
	}

	// Get a connection from the pool for this user
	clientKey, serverKey := scramKeysFromOptions(options)
	conn, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user, clientKey, serverKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection for user %s: %w", user, err)
	}
	defer conn.Recycle()

	// Stamp multigres_vpid on this pooled regular conn for lock-detection.
	e.stampVpidOnRegular(ctx, conn.Conn, options)

	// When a PreparedStatement is provided we cannot use the retry-on-connection-error
	// variant of QueryStreaming: reconnect wipes per-connection prepared-statement state
	// (regular_conn.go Reconnect: "Prepared statements don't survive reconnection"),
	// so after a silent reconnect the subsequent query would fail with "prepared
	// statement does not exist". Skip the retry for this rare path; the caller can
	// reissue the query at the application level on transient failures.
	if preparedStmt != nil {
		if err := e.ensurePreparedWithName(ctx, conn.Conn, preparedStmt); err != nil {
			return nil, fmt.Errorf("failed to ensure prepared statement: %w", err)
		}
		if err := conn.Conn.QueryStreaming(ctx, sql, callback); err != nil {
			return nil, wrapQueryError(err)
		}
		e.noteConnectionDefaultsChange(options, conn.Conn.TxnStatus(), nil)
		return nil, nil
	}

	// Use streaming query execution with retry since this is a stateless pool query.
	if err := conn.Conn.QueryStreamingWithRetry(ctx, sql, callback); err != nil {
		return nil, wrapQueryError(err)
	}
	e.noteConnectionDefaultsChange(options, conn.Conn.TxnStatus(), nil)

	return nil, nil
}

// reserveAndStreamExecute creates a new reserved connection and executes a query.
// Based on reservationOptions.Reasons, it may execute setup commands (e.g., BEGIN for transactions).
func (e *Executor) reserveAndStreamExecute(
	ctx context.Context,
	sql string,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	user := e.getUserFromOptions(options)
	var settings map[string]string
	if options != nil {
		settings = e.sessionSettingsForPool(options.SessionSettings)
	}

	// Get the reasons bitmask and determine if we need to execute BEGIN
	reasons := reservationOptions.GetReasons()
	beginTx := protoutil.RequiresBegin(reasons)

	e.logger.DebugContext(ctx, "reserve stream execute",
		"user", user,
		"reasons", protoutil.ReasonsString(reasons),
		"begin_tx", beginTx,
		"query", sql)

	// If the query references a gateway-managed prepared statement (wrapped
	// EXECUTE forms like CREATE TEMP TABLE ... AS EXECUTE), parse it during
	// reserved-connection acquisition. Doing the Parse via the validate
	// callback lets the reserved pool transparently swap a stale (silently
	// closed) socket for a fresh one before we register the connection — the
	// failure mode that flaked TestWrappedPreparedStatementExecution.
	//
	// Parse is a session-level operation in PostgreSQL, so running it before
	// BEGIN is safe; the prepared statement persists into the transaction.
	var reservedOpts []reserved.ReservedConnOption
	if preparedStmt := options.GetPreparedStatement(); preparedStmt != nil {
		validate := func(ctx context.Context, conn *regular.Conn) error {
			return e.ensurePreparedWithName(ctx, conn, preparedStmt)
		}
		reservedOpts = append(reservedOpts, reserved.WithValidate(validate))
	}

	// Create a reserved connection
	clientKey, serverKey := scramKeysFromOptions(options)
	reservedConn, err := e.poolManager.NewReservedConn(ctx, settings, user, clientKey, serverKey, reservedOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create reserved connection: %w", err)
	}

	// Stamp multigres_vpid on the freshly reserved backend so subsequent
	// lock-detection probes can map vpid → real pid. Done before BEGIN so
	// the value is in place for the entire transaction lifecycle.
	e.stampVpidOnReserved(ctx, reservedConn, options)

	// Apply all reservation reasons to the reserved connection.
	// BeginWithQuery below adds ReasonTransaction internally, but non-transaction
	// reasons (e.g., temp_table) must be added explicitly so that buildReservedState
	// returns the correct bitmask and DiscardTempTables can find the shard.
	if nonBeginReasons := reasons &^ protoutil.ReasonTransaction; nonBeginReasons != 0 {
		reservedConn.AddReservationReason(nonBeginReasons)
	}

	// Register pin-portal entries from the gateway. Each name is a cursor
	// declared with WITH HOLD; ReserveForPortal adds the name to the
	// per-conn portal set and ORs ReasonPortal into the reservation
	// bitmask (idempotent with the AddReservationReason call above).
	// This path always releases the connection on QueryStreaming error
	// (Release(ReleaseError) below), so a failed DECLARE never leaks a
	// pin from the new-reservation branch — even without the explicit
	// rollback dance that streamExecuteOnReservedConn performs for the
	// existing-reservation case.
	for _, name := range reservationOptions.GetPinPortalNames() {
		reservedConn.ReserveForPortal(name)
	}

	// If this is a transaction reservation, execute BEGIN first.
	// The BEGIN result is not sent to the callback — it's an internal setup detail.
	// The caller (multigateway) handles sending synthetic BEGIN results to the client.
	// Use the original BEGIN query if provided to preserve isolation level and access mode.
	if beginTx {
		beginQuery := "BEGIN"
		if reservationOptions.GetBeginQuery() != "" {
			beginQuery = reservationOptions.GetBeginQuery()
		}
		if err := reservedConn.BeginWithQuery(ctx, beginQuery); err != nil {
			reservedConn.Release(reserved.ReleaseError, nil)
			return nil, err
		}
	}

	// Execute the actual query and stream results to the callback as they arrive,
	// matching the non-reserved StreamExecute path. This avoids buffering the entire
	// result set in memory for large queries inside transactions.
	if err := reservedConn.QueryStreaming(ctx, sql, callback); err != nil {
		if beginTx {
			_ = reservedConn.Rollback(ctx)
		}
		reservedConn.Release(reserved.ReleaseError, nil)
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	if reservationOptions.GetMarkSessionStateUntrusted() {
		reservedConn.MarkSessionStateUntrusted()
	}
	e.noteConnectionDefaultsChange(options, reservedConn.TxnStatus(), reservedConn)

	// If the gateway flagged this statement as touching an advisory lock,
	// re-probe pg_locks: it may have been a pg_try_advisory_lock that didn't
	// acquire, in which case unpin immediately so the gateway doesn't keep an
	// empty reservation. Gated on the recheck signal so the probe stays off the
	// per-statement hot path.
	if reservationOptions.GetRecheckAdvisoryLocks() && e.maybeUnpinSessionAdvisoryLock(ctx, reservedConn, e.sessionSettingsFromOptions(options)) {
		return nil, nil
	}

	reservedState := e.buildReservedState(reservedConn)

	e.logger.DebugContext(ctx, "reserve stream execute completed",
		"reserved_conn_id", reservedState.ReservedConnectionId)

	return reservedState, nil
}

// streamExecuteOnReservedConn executes a query on an existing reserved
// connection. It optionally promotes the reservation by adding new reasons
// (e.g., starting a transaction on a temp-table-reserved connection) before
// running the query.
//
// Defined over reservedConnAPI rather than *reserved.Conn so that unit tests
// can substitute a mock and exercise this path without a live PG connection.
func (e *Executor) streamExecuteOnReservedConn(
	ctx context.Context,
	rc reservedConnAPI,
	sql string,
	reservationOptions *query.ReservationOptions,
	gatewaySessionSettings map[string]string,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	reasons := protoutil.GetReasons(reservationOptions)

	// If the caller is adding reservation reasons (e.g., promoting a temp-table
	// reservation to also hold a transaction), apply them now.
	if reasons != 0 {
		// If the new reasons include transaction and the connection is not
		// already in a transaction, execute BEGIN before the query.
		if protoutil.RequiresBegin(reasons) && !rc.IsInTransaction() {
			beginQuery := "BEGIN"
			if reservationOptions.GetBeginQuery() != "" {
				beginQuery = reservationOptions.GetBeginQuery()
			}
			if err := rc.BeginWithQuery(ctx, beginQuery); err != nil {
				return e.buildReservedStateFromAPI(rc), fmt.Errorf("failed to begin transaction on reserved connection: %w", err)
			}
		}
		// Add all requested non-transaction reasons to the reservation
		// (BeginWithQuery already added ReasonTransaction internally).
		if nonBeginReasons := reasons &^ protoutil.ReasonTransaction; nonBeginReasons != 0 {
			rc.AddReservationReason(nonBeginReasons)
		}
	}

	// Register pin-portal entries for DECLARE … WITH HOLD before running
	// the DECLARE itself so the bitmask is consistent during the round
	// trip. The gateway only records the cursor in OpenHoldCursors on
	// DECLARE success, so we mirror that here: if PG rejects the
	// DECLARE (outside-of-transaction, syntax error, table missing,
	// duplicate name, etc.) we roll back every pin we just added so
	// the multipooler-side bitmask matches what the gateway thinks is
	// open. Without this, a failed DECLARE outside an explicit
	// transaction block would leak ReasonPortal on the reserved
	// connection until session disconnect.
	pinNames := reservationOptions.GetPinPortalNames()
	for _, name := range pinNames {
		rc.ReserveForPortal(name)
	}

	if err := rc.QueryStreaming(ctx, sql, callback); err != nil {
		// Roll back every pin we registered for this DECLARE — PG
		// rejected the statement, so the gateway will never call
		// AddOpenHoldCursor and any matching CLOSE will not arrive.
		// ReleasePortal returns true iff the call drained the *last*
		// reservation reason on the connection (the bool propagates
		// IsEmpty(), not "ReasonPortal cleared"), so a single true
		// is sufficient to know the conn should be released.
		shouldRelease := false
		for _, name := range pinNames {
			if rc.ReleasePortal(name) {
				shouldRelease = true
			}
		}
		if shouldRelease {
			rc.Release(reserved.ReleaseError, nil)
			return nil, wrapQueryError(err)
		}
		return e.buildReservedStateFromAPI(rc), wrapQueryError(err)
	}

	if reservationOptions.GetMarkSessionStateUntrusted() {
		rc.MarkSessionStateUntrusted()
	}

	// Apply portal releases after the query succeeds. CLOSE forwards the
	// statement to PG first; only on success do we unpin so the
	// gateway's HOLD-cursor bookkeeping matches the server side.
	// ReleasePortal returns true iff this call drained the last
	// reservation reason on the connection — when that happens, return
	// the backend to the pool and surface a zero ReservedState so the
	// gateway clears its shard tracking.
	shouldRelease := false
	for _, name := range reservationOptions.GetReleasePortalNames() {
		if rc.ReleasePortal(name) {
			shouldRelease = true
		}
	}
	if shouldRelease {
		rc.Release(reserved.ReleasePortalComplete, gatewaySessionSettings)
		return nil, nil
	}

	// If the gateway flagged this statement as touching an advisory lock (e.g.
	// pg_advisory_unlock), re-probe pg_locks and unpin if none remain. Gated on
	// the recheck signal so the probe runs only on advisory-touching statements,
	// not after every query on a pinned connection.
	if reservationOptions.GetRecheckAdvisoryLocks() && e.maybeUnpinSessionAdvisoryLock(ctx, rc, gatewaySessionSettings) {
		return nil, nil
	}

	return e.buildReservedStateFromAPI(rc), nil
}

// maybeUnpinSessionAdvisoryLock checks, after a statement on a connection
// reserved for session-level advisory locks, whether the session still holds
// any advisory lock. If none remain it clears ReasonSessionAdvisoryLock and,
// when no other reason keeps the connection reserved, releases the backend to
// the pool. Returns true if the connection was released.
//
// PostgreSQL is the source of truth for the (reference-counted) lock state, so
// this is robust against pg_try_advisory_lock calls that failed, keys locked
// and unlocked an equal number of times, pg_advisory_unlock_all(), and unlock
// calls buried in functions or dynamic SQL — cases gateway-side counting could
// never get right. The cost is one extra round trip per statement while the
// session holds an advisory lock, which is a rare and already-pinned state.
func (e *Executor) maybeUnpinSessionAdvisoryLock(ctx context.Context, rc reservedConnAPI, gatewaySessionSettings map[string]string) bool {
	// Only meaningful for advisory-lock reservations, and only outside a
	// transaction: inside one ReasonTransaction keeps the backend pinned anyway,
	// and transaction-level advisory locks would pollute the probe.
	if !protoutil.HasSessionAdvisoryLockReason(rc.RemainingReasons()) || rc.IsInTransaction() {
		return false
	}

	results, err := rc.Query(ctx, constants.PgLocksAdvisoryProbeSQL)
	if err != nil {
		// Err on the side of staying pinned: handing back a connection that may
		// still hold the client's locks would leak them to the next session.
		// The connection is reclaimed when the session ends regardless.
		e.logger.WarnContext(ctx, "advisory-lock probe failed; keeping connection pinned",
			"reserved_conn_id", rc.ConnID(), "error", err)
		return false
	}

	// SELECT EXISTS always returns exactly one row, so an empty result is
	// unexpected — treat it like a probe failure and stay pinned rather than
	// fall through with held=false and risk releasing a backend that may still
	// hold the client's locks.
	if len(results) == 0 {
		e.logger.WarnContext(ctx, "advisory-lock probe returned no rows; keeping connection pinned",
			"reserved_conn_id", rc.ConnID())
		return false
	}

	var held bool
	if scanErr := ScanSingleRow(results[0], &held); scanErr != nil {
		e.logger.WarnContext(ctx, "advisory-lock probe returned unexpected result; keeping connection pinned",
			"reserved_conn_id", rc.ConnID(), "error", scanErr)
		return false
	}
	if held {
		return false
	}

	// No advisory locks remain. Drop the reason; release the backend if nothing
	// else keeps it reserved.
	if rc.RemoveReservationReason(protoutil.ReasonSessionAdvisoryLock) {
		rc.Release(reserved.ReleaseAdvisoryUnlock, gatewaySessionSettings)
		e.logger.DebugContext(ctx, "released advisory-lock reservation; no locks remain",
			"reserved_conn_id", rc.ConnID())
		return true
	}
	return false
}

// Close closes the executor and releases resources.
// Note: The poolManager is managed by the caller (QueryPoolerServer), not closed here.
func (e *Executor) Close() error {
	return nil
}

// PortalStreamExecute executes a portal (bound prepared statement) and streams results back via callback.
// A reserved connection is used when any of these hold: ReservedConnectionId is
// already set, MaxRows > 0 (the portal may be suspended and need resumption), or
// reservationOptions carries reasons (the caller wants this portal to reserve a
// backend — e.g. it opens a transaction or temp table). Otherwise a regular
// connection is used for better pool efficiency.
//
// portalOptions carries portal-only knobs (e.g. include_describe). Nil leaves
// every field at the proto default.
//
// reservationOptions mirrors StreamExecute: when it carries reasons and no
// ReservedConnectionId is set, a fresh backend is reserved with those reasons
// (running BeginQuery first if ReasonTransaction is set) before the portal runs;
// when a reserved connection already exists, the reasons are OR'd onto it.
func (e *Executor) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	portalOptions *multipoolerpb.PortalExecuteOptions,
	reservationOptions *query.ReservationOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	if target == nil {
		target = &query.Target{}
	}
	if preparedStatement == nil {
		return nil, errors.New("prepared statement is required")
	}
	if portal == nil {
		return nil, errors.New("portal is required")
	}

	user := e.getUserFromOptions(options)
	var settings map[string]string
	if options != nil {
		settings = e.sessionSettingsForPool(options.SessionSettings)
	}

	maxRows := int32(0)
	if options != nil && options.MaxRows > 0 {
		maxRows = int32(options.MaxRows)
	}

	e.logger.DebugContext(ctx, "portal stream execute",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"user", user,
		"statement", preparedStatement.Name,
		"portal", portal.Name,
		"max_rows", maxRows)

	// Convert formats from int32 to int16
	paramFormats := int32ToInt16Slice(portal.ParamFormats)
	resultFormats := int32ToInt16Slice(portal.ResultFormats)

	includeDescribe := portalOptions.GetIncludeDescribe()
	reasons := protoutil.GetReasons(reservationOptions)

	// Use reserved connection if:
	// 1. ReservedConnectionId is already set (e.g., from transaction or previous portal)
	// 2. MaxRows > 0 (portal may be suspended and need resumption)
	// 3. reservationOptions carries reasons (caller wants this portal to reserve
	//    a backend, e.g. it opens a transaction or temp table)
	if (options != nil && options.ReservedConnectionId > 0) || maxRows > 0 || reasons != 0 {
		return e.portalExecuteWithReserved(ctx, preparedStatement, portal, options, reservationOptions, settings, user, maxRows, includeDescribe, paramFormats, resultFormats, callback)
	}

	// Use regular connection for non-suspended execution with no existing reservation
	clientKey, serverKey := scramKeysFromOptions(options)
	return e.portalExecuteWithRegular(ctx, preparedStatement, portal, settings, user, includeDescribe, clientKey, serverKey, paramFormats, resultFormats, options, callback)
}

// portalExecuteWithReserved executes a portal using a reserved connection.
//
// reservationOptions lets a portal reserve-and-run atomically (mirroring
// reserveAndStreamExecute on the simple path): when the connection is freshly
// reserved here, BEGIN runs first if ReasonTransaction is set and the remaining
// reasons are applied; when an existing reservation is promoted, the new reasons
// are OR'd onto it. nil/zero reservationOptions preserves the prior behavior of
// just running the portal on the (existing or new) reserved connection.
func (e *Executor) portalExecuteWithReserved(
	ctx context.Context,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
	settings map[string]string,
	user string,
	maxRows int32,
	includeDescribe bool,
	paramFormats, resultFormats []int16,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	var reservedConn *reserved.Conn
	var err error

	// Track whether this call created the reservation. A freshly reserved
	// backend is owned by this call, so reservation-setup failures (e.g. BEGIN)
	// release it; an existing reservation is owned by the session and must
	// survive a single failed portal — the gateway decides its fate.
	newlyReserved := false

	// Check if we should use an existing reserved connection
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ = e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			return nil, mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
		}
	} else {
		// Create a new reserved connection. Wire ensurePrepared as the
		// validate hook so that the Parse — the first user-issued write
		// on this freshly acquired socket — triggers a transparent
		// retry on a fresh socket if the pooled conn has been silently
		// closed by PostgreSQL. The post-acquire ensurePrepared call
		// below is a no-op for the new-conn path because connState
		// dedupes by canonical name.
		//
		// Parse is a session-level operation in PostgreSQL, so running it via
		// the validate hook (before any BEGIN below) is safe; the prepared
		// statement persists into the transaction.
		clientKey, serverKey := scramKeysFromOptions(options)
		reservedConn, err = e.poolManager.NewReservedConn(ctx, settings, user, clientKey, serverKey, reserved.WithValidate(func(ctx context.Context, conn *regular.Conn) error {
			_, err := e.ensurePrepared(ctx, conn, preparedStatement)
			return err
		}))
		if err != nil {
			return nil, fmt.Errorf("failed to create reserved connection for user %s: %w", user, err)
		}
		newlyReserved = true
	}

	if err := e.applyReservedSessionSettingsIfNeeded(ctx, reservedConn, options); err != nil {
		if newlyReserved {
			reservedConn.Release(reserved.ReleaseError, nil)
			return nil, err
		}
		return e.buildReservedState(reservedConn), fmt.Errorf("failed to prepare reserved connection: %w", err)
	}

	// Stamp multigres_vpid:<id> on the (possibly fresh, possibly resumed)
	// reserved conn. Done unconditionally — both branches above can return
	// a backend without the tag (a freshly created reservation, or an
	// existing one whose tag was wiped by a prior settings reconciliation).
	e.stampVpidOnReserved(ctx, reservedConn, options)

	// Apply reservation reasons requested for this portal, mirroring
	// reserveAndStreamExecute / streamExecuteOnReservedConn: run BEGIN when a
	// transaction reason is added (and the connection isn't already in one),
	// then OR the remaining reasons onto the connection. Done before
	// Bind/Execute so the portal runs inside the transaction.
	if reasons := protoutil.GetReasons(reservationOptions); reasons != 0 {
		if protoutil.RequiresBegin(reasons) && !reservedConn.IsInTransaction() {
			beginQuery := "BEGIN"
			if reservationOptions.GetBeginQuery() != "" {
				beginQuery = reservationOptions.GetBeginQuery()
			}
			if err := reservedConn.BeginWithQuery(ctx, beginQuery); err != nil {
				if newlyReserved {
					reservedConn.Release(reserved.ReleaseError, nil)
					return nil, err
				}
				return e.buildReservedState(reservedConn), fmt.Errorf("failed to begin transaction on reserved connection: %w", err)
			}
		}
		// OR the requested reasons onto the connection. AddReservationReason is
		// idempotent, so re-adding ReasonTransaction (already set by
		// BeginWithQuery above) is harmless.
		reservedConn.AddReservationReason(reasons)
	}

	// Ensure the statement is prepared on this connection (with consolidation).
	// For the new-conn branch this is a no-op because the validate hook above
	// already parsed it; for the existing-conn branch this is the only call.
	canonicalName, err := e.ensurePrepared(ctx, reservedConn.Conn(), preparedStatement)
	if err != nil {
		reservedConn.Release(reserved.ReleaseError, nil)
		return nil, err
	}

	// Bind and execute using the portal's own name and the canonical statement name.
	// When the protocol layer folded Describe('P') into Execute, fuse the
	// backend round trip too so the portal description rides on the Execute
	// response.
	params := sqltypes.ParamsFromProto(portal.ParamLengths, portal.ParamValues)
	var completed bool
	if includeDescribe {
		completed, err = reservedConn.BindDescribeAndExecute(ctx, portal.Name, canonicalName, params, paramFormats, resultFormats, maxRows, callback)
	} else {
		completed, err = reservedConn.BindAndExecute(ctx, portal.Name, canonicalName, params, paramFormats, resultFormats, maxRows, callback)
	}
	if err != nil {
		reservedConn.Release(reserved.ReleaseError, nil)
		return nil, wrapQueryError(err)
	}

	if reservationOptions.GetMarkSessionStateUntrusted() {
		reservedConn.MarkSessionStateUntrusted()
	}
	e.noteConnectionDefaultsChange(options, reservedConn.TxnStatus(), reservedConn)

	// If portal is suspended (not completed), keep the reserved connection for
	// continuation. Do NOT probe advisory locks here: the extended-protocol
	// portal is mid-Execute, so issuing a simple probe query would corrupt the
	// protocol state on this backend.
	if !completed {
		reservedConn.ReserveForPortal(portal.Name)
		return e.buildReservedState(reservedConn), nil
	}

	// Portal completed — release this portal's reservation. ReleasePortal returns
	// true only when all reservation reasons are gone.
	if reservedConn.ReleasePortal(portal.Name) {
		e.releaseReservedConn(reservedConn, reserved.ReleasePortalComplete, options)
		return nil, nil
	}

	// The connection stays reserved for other reasons. If the gateway flagged
	// this portal as touching an advisory lock (acquire that may have failed, or
	// a release over the extended protocol), re-probe pg_locks and unpin if none
	// remain. Gated on the recheck signal to keep the probe off the hot path.
	if reservationOptions.GetRecheckAdvisoryLocks() && e.maybeUnpinSessionAdvisoryLock(ctx, reservedConn, e.sessionSettingsFromOptions(options)) {
		return nil, nil
	}

	return e.buildReservedState(reservedConn), nil
}

// portalExecuteWithRegular executes a portal using a regular pooled connection.
// clientKey and serverKey are the SCRAM passthrough keys forwarded by the
// caller's session; see connpoolmanager.GetRegularConn.
func (e *Executor) portalExecuteWithRegular(
	ctx context.Context,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	settings map[string]string,
	user string,
	includeDescribe bool,
	clientKey, serverKey []byte,
	paramFormats, resultFormats []int16,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	conn, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user, clientKey, serverKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection for user %s: %w", user, err)
	}
	defer conn.Recycle()

	// Stamp multigres_vpid on this pooled regular conn for lock-detection.
	e.stampVpidOnRegular(ctx, conn.Conn, options)

	// Ensure the statement is prepared on this connection (with consolidation)
	canonicalName, err := e.ensurePrepared(ctx, conn.Conn, preparedStatement)
	if err != nil {
		return nil, err
	}

	// Bind and execute with maxRows=0 (fetch all) using the portal's own name and canonical statement name.
	// When the protocol layer folded Describe('P') into Execute, fuse the
	// backend round trip too so the portal description rides on the Execute
	// response.
	params := sqltypes.ParamsFromProto(portal.ParamLengths, portal.ParamValues)
	if includeDescribe {
		_, err = conn.Conn.BindDescribeAndExecute(ctx, portal.Name, canonicalName, params, paramFormats, resultFormats, 0, callback)
	} else {
		_, err = conn.Conn.BindAndExecute(ctx, portal.Name, canonicalName, params, paramFormats, resultFormats, 0, callback)
	}
	if err != nil {
		return nil, wrapQueryError(err)
	}
	e.noteConnectionDefaultsChange(options, conn.Conn.TxnStatus(), nil)

	// No reserved connection for regular execution
	return nil, nil
}

// Describe returns metadata about a prepared statement or portal.
func (e *Executor) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	if target == nil {
		target = &query.Target{}
	}

	if preparedStatement == nil {
		return nil, errors.New("no prepared statement provided")
	}

	user := e.getUserFromOptions(options)
	var settings map[string]string
	if options != nil {
		settings = e.sessionSettingsForPool(options.SessionSettings)
	}

	e.logger.DebugContext(ctx, "describe",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"user", user,
		"has_statement", preparedStatement != nil,
		"has_portal", portal != nil,
		"reserved_connection_id", options.GetReservedConnectionId())

	// Acquire the connection: reserved (transactional) or regular (pooled).
	var conn *regular.Conn
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			return nil, mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
		}

		if err := e.applyReservedSessionSettingsIfNeeded(ctx, reservedConn, options); err != nil {
			return nil, fmt.Errorf("failed to prepare reserved connection: %w", err)
		}

		conn = reservedConn.Conn()
	} else {
		clientKey, serverKey := scramKeysFromOptions(options)
		pooled, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user, clientKey, serverKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get connection for user %s: %w", user, err)
		}
		defer pooled.Recycle()

		conn = pooled.Conn
	}

	// Ensure the statement is prepared on this connection
	canonicalName, err := e.ensurePrepared(ctx, conn, preparedStatement)
	if err != nil {
		return nil, err
	}

	if portal != nil {
		paramFormats := int32ToInt16Slice(portal.ParamFormats)
		resultFormats := int32ToInt16Slice(portal.ResultFormats)
		params := sqltypes.ParamsFromProto(portal.ParamLengths, portal.ParamValues)
		desc, err := conn.BindAndDescribe(ctx, canonicalName, params, paramFormats, resultFormats)
		if err != nil {
			return nil, fmt.Errorf("failed to describe portal: %w", err)
		}
		return desc, nil
	}

	// Describe prepared using canonical name
	desc, err := conn.DescribePrepared(ctx, canonicalName)
	if err != nil {
		return nil, fmt.Errorf("failed to describe prepared statement: %w", err)
	}
	return desc, nil
}

// ensurePreparedWithName ensures that a prepared statement named `name` with
// the given body exists on the backend connection, Parsing it if necessary.
// Unlike ensurePrepared (which derives its own canonical name via the pooler
// consolidator), this variant uses the caller-supplied name directly. It is
// used by the wrapped-EXECUTE StreamExecute path where the gateway has
// already rewritten the SQL to reference a specific name, and the backend
// must have a prepared statement under exactly that name.
//
// The gateway's prepared-statement consolidator assigns globally unique
// monotonic names ("stmt0", "stmt1", ...) that are deduplicated by
// (query, paramTypes), so using them as backend-session prepared statement
// names is safe across multiple gateway client sessions sharing a pool
// connection.
func (e *Executor) ensurePreparedWithName(ctx context.Context, conn *regular.Conn, stmt *query.PreparedStatement) error {
	if stmt == nil || stmt.Name == "" {
		return errors.New("ensurePreparedWithName requires a non-empty statement name")
	}
	connState := conn.State()
	existing := connState.GetPreparedStatement(stmt.Name)
	if existing != nil && existing.Query == stmt.Query {
		return nil
	}
	// If the name is already taken with a different query (e.g. from a
	// different gateway instance that reused the same canonical name space),
	// close the stale statement first — PostgreSQL rejects Parse for an
	// already-existing prepared statement name.
	if existing != nil {
		if err := conn.CloseStatement(ctx, stmt.Name); err != nil {
			return fmt.Errorf("failed to close stale prepared statement %q: %w", stmt.Name, err)
		}
		connState.DeletePreparedStatement(stmt.Name)
	}
	if err := conn.Parse(ctx, stmt.Name, stmt.Query, stmt.ParamTypes); err != nil {
		return fmt.Errorf("failed to parse statement %q: %w", stmt.Name, err)
	}
	connState.StorePreparedStatement(&query.PreparedStatement{
		Name:       stmt.Name,
		Query:      stmt.Query,
		ParamTypes: stmt.ParamTypes,
	})
	return nil
}

// ensurePrepared ensures the prepared statement is available on the connection.
// It uses the PoolerConsolidator to get a canonical name by (query, paramTypes),
// then checks the connection state to avoid redundant parsing.
// Returns the canonical statement name to use.
func (e *Executor) ensurePrepared(ctx context.Context, conn *regular.Conn, stmt *query.PreparedStatement) (string, error) {
	canonicalName := e.poolerConsolidator.CanonicalName(stmt.Query, stmt.ParamTypes)

	// Check if this connection already has the statement prepared
	connState := conn.State()
	existing := connState.GetPreparedStatement(canonicalName)
	if existing != nil && existing.Query == stmt.Query {
		// Statement already prepared on this connection, reuse it
		return canonicalName, nil
	}

	// Parse the statement on this connection
	if err := conn.Parse(ctx, canonicalName, stmt.Query, stmt.ParamTypes); err != nil {
		return "", fmt.Errorf("failed to parse statement: %w", err)
	}

	// Store in connection state for future reuse
	connState.StorePreparedStatement(&query.PreparedStatement{
		Name:       canonicalName,
		Query:      stmt.Query,
		ParamTypes: stmt.ParamTypes,
	})

	return canonicalName, nil
}

// CopyReady initiates a COPY FROM STDIN operation and returns format information.
// Uses an existing reserved connection if ReservedConnectionId is set in options,
// otherwise creates a new reserved connection (COPY requires connection affinity).
func (e *Executor) CopyReady(
	ctx context.Context,
	target *query.Target,
	copyQuery string,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
) (int16, []int16, *query.ReservedState, error) {
	user := e.getUserFromOptions(options)
	var settings map[string]string
	if options != nil {
		settings = e.sessionSettingsForPool(options.SessionSettings)
	}

	e.logger.DebugContext(ctx, "initiating COPY FROM STDIN",
		"query", copyQuery,
		"user", user)

	var (
		reservedConn  *reserved.Conn
		err           error
		format        int16
		columnFormats []int16
	)

	// Check if we should use an existing reserved connection
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ = e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			return 0, nil, nil, mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
		}

		if err := e.applyReservedSessionSettingsIfNeeded(ctx, reservedConn, options); err != nil {
			return 0, nil, e.buildReservedState(reservedConn), fmt.Errorf("failed to prepare reserved connection: %w", err)
		}

		// Existing reserved conns are actively held (never idle in the
		// regular pool), so PostgreSQL's idle timeout cannot have closed
		// the socket between uses. InitiateCopyFromStdin runs directly
		// here without a stale-socket retry hop.
		// Stamp the vpid before entering COPY mode: once
		// InitiateCopyFromStdin succeeds the backend rejects SET until
		// CopyDone/CopyFail, so this is the only window to tag the
		// backend for lock-detection during long-running COPYs.
		e.stampVpidOnReserved(ctx, reservedConn, options)
		// InitiateCopyFromStdin may return NoticeResponse diagnostics that
		// arrive before CopyInResponse (e.g. BEFORE STATEMENT triggers).
		// They are uncommon and harmless to drop here; the gateway's main
		// regression-test concern is trigger NOTICE output during the data
		// phase, which surfaces via ReadCopyDoneResponse in CopyFinalize.
		format, columnFormats, _, err = reservedConn.Conn().InitiateCopyFromStdin(ctx, copyQuery)
		if err != nil {
			// InitiateCopyFromStdin distinguishes two failure modes:
			//   - Connection-level error (broken socket): conn is dead, release it.
			//   - PG ErrorResponse + drained ReadyForQuery (e.g., "column does
			//     not exist", "conflicting options"): conn is back in a clean
			//     'I'/'T'/'E' state and is safe to reuse. The reserved conn may
			//     still be holding other reasons (transaction, temp table), so
			//     destroying it here would orphan that state and force every
			//     subsequent statement to fail with "reserved connection not
			//     found". Return the current state alongside the error so the
			//     gateway can keep tracking the conn.
			// Surface PG errors un-wrapped so the gateway can re-emit a
			// verbatim ErrorResponse — see ReadCopyDoneResponse error path
			// in CopyFinalize for the same rationale.
			if mterrors.IsConnectionError(err) {
				reservedConn.Release(reserved.ReleaseError, nil)
				return 0, nil, nil, err
			}
			return 0, nil, e.buildReservedState(reservedConn), err
		}
	} else {
		// New reserved conn — wire BEGIN-if-needed and InitiateCopyFromStdin
		// through the validate hook so that the first writes on this
		// freshly acquired socket can be transparently retried on a fresh
		// socket if the pooled conn was silently closed by PostgreSQL.
		// Capture format / columnFormats via closure for use after acquisition.
		requiresBegin := protoutil.RequiresBegin(protoutil.GetReasons(reservationOptions))
		beginQuery := "BEGIN"
		if reservationOptions != nil && reservationOptions.BeginQuery != "" {
			beginQuery = reservationOptions.BeginQuery
		}

		validate := func(ctx context.Context, conn *regular.Conn) error {
			if requiresBegin {
				if _, err := conn.Query(ctx, beginQuery); err != nil {
					return fmt.Errorf("failed to begin transaction for COPY: %w", err)
				}
			}
			// Stamp vpid before InitiateCopyFromStdin: SET is rejected
			// once the backend enters COPY mode, and the *regular.Conn
			// is the same underlying socket that NewReservedConn will
			// promote to a *reserved.Conn, so the tag carries through.
			e.stampVpidOnRegular(ctx, conn, options)
			var initErr error
			format, columnFormats, _, initErr = conn.InitiateCopyFromStdin(ctx, copyQuery)
			// Surface PG errors un-wrapped — see comment above.
			return initErr
		}

		clientKey, serverKey := scramKeysFromOptions(options)
		reservedConn, err = e.poolManager.NewReservedConn(ctx, settings, user, clientKey, serverKey, reserved.WithValidate(validate))
		if err != nil {
			return 0, nil, nil, fmt.Errorf("failed to create reserved connection for COPY: %w", err)
		}

		// validate ran BEGIN via the raw *regular.Conn, which bypassed
		// reserved.Conn.BeginWithQuery's bookkeeping. Mark the
		// reservation reason explicitly so the txn shows up in
		// RemainingReasons / RequiresBegin checks.
		if requiresBegin {
			reservedConn.AddReservationReason(protoutil.ReasonTransaction)
			// BeginWithQuery's snapshot was bypassed too; capture it now (before any
			// client statement) so a ROLLBACK can revert the pool's cached connstate.
			reservedConn.SnapshotTxnState()
		}
	}

	connID := reservedConn.ConnID()

	// Mark the connection as reserved for COPY. If the connection is also
	// reserved for a transaction, this adds the COPY reason alongside it.
	reservedConn.AddReservationReason(protoutil.ReasonCopy)

	e.logger.DebugContext(ctx, "COPY INITIATE successful",
		"conn_id", connID,
		"format", format,
		"num_columns", len(columnFormats))

	return format, columnFormats, e.buildReservedState(reservedConn), nil
}

// CopySendData sends a chunk of data for an active COPY operation.
func (e *Executor) CopySendData(
	ctx context.Context,
	target *query.Target,
	data []byte,
	options *query.ExecuteOptions,
) error {
	if options == nil || options.ReservedConnectionId == 0 {
		return errors.New("options.ReservedConnectionId is required for CopySendData")
	}

	user := e.getUserFromOptions(options)

	// Get the reserved connection
	reservedConn, ok := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
	if !ok || reservedConn == nil {
		return mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
	}

	e.logger.DebugContext(ctx, "sending COPY data",
		"data_size", len(data),
		"conn_id", options.ReservedConnectionId)

	// Get the pooled connection for COPY operations
	conn := reservedConn.Conn()

	// Write CopyData to PostgreSQL
	if err := conn.WriteCopyData(data); err != nil {
		e.logger.ErrorContext(ctx, "failed to write COPY data",
			"error", err,
			"data_size", len(data))
		return fmt.Errorf("failed to write COPY data: %w", err)
	}

	e.logger.DebugContext(ctx, "COPY DATA sent successfully",
		"data_size", len(data))

	return nil
}

// CopyFinalize completes a COPY operation, sending final data and returning the result.
// Returns ReservedState with the authoritative reservation state from the multipooler.
func (e *Executor) CopyFinalize(
	ctx context.Context,
	target *query.Target,
	finalData []byte,
	options *query.ExecuteOptions,
) (*sqltypes.Result, *query.ReservedState, error) {
	if options == nil || options.ReservedConnectionId == 0 {
		return nil, nil, errors.New("options.ReservedConnectionId is required for CopyFinalize")
	}

	user := e.getUserFromOptions(options)

	// Get the reserved connection
	reservedConn, ok := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
	if !ok || reservedConn == nil {
		return nil, nil, mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
	}

	e.logger.DebugContext(ctx, "finalizing COPY",
		"final_data_size", len(finalData),
		"conn_id", options.ReservedConnectionId)

	// Get the pooled connection for COPY operations
	conn := reservedConn.Conn()

	// Send any remaining data first
	if len(finalData) > 0 {
		if err := conn.WriteCopyData(finalData); err != nil {
			e.logger.ErrorContext(ctx, "failed to write final COPY data", "error", err)
			// Connection is in bad state - close it instead of recycling
			reservedConn.Release(reserved.ReleaseError, nil)
			return nil, nil, fmt.Errorf("failed to write final COPY data: %w", err)
		}
		e.logger.DebugContext(ctx, "sent final COPY data", "size", len(finalData))
	}

	// Send CopyDone to signal completion
	if err := conn.WriteCopyDone(); err != nil {
		e.logger.ErrorContext(ctx, "failed to write CopyDone", "error", err)
		// Connection is in bad state - close it instead of recycling
		reservedConn.Release(reserved.ReleaseError, nil)
		return nil, nil, fmt.Errorf("failed to write CopyDone: %w", err)
	}

	// Read CommandComplete response from PostgreSQL. Notices from triggers /
	// progress reporting that fired during the data phase arrive between
	// CopyDone and CommandComplete; capture them so the gateway can forward
	// them to the client as NoticeResponse frames.
	commandTag, rowsAffected, notices, err := conn.ReadCopyDoneResponse(ctx)
	if err != nil {
		e.logger.ErrorContext(ctx, "COPY operation failed", "error", err)
		// For a PG ErrorResponse (e.g., constraint violation, type mismatch,
		// missing column), ReadCopyDoneResponse drains the trailing
		// ReadyForQuery so the socket is back in a clean state. The reserved
		// conn may still be holding other reasons (transaction, temp table),
		// so we mirror CopyAbort here: remove the COPY reason and release
		// only if no other reasons remain. A connection-level failure (broken
		// socket) still falls through to Release(ReleaseError).
		//
		// We deliberately do NOT wrap the PG error with "COPY operation
		// failed:" — the gateway round-trips a structured PgDiagnostic over
		// the bidi stream's error_diagnostic field and re-emits a verbatim
		// ErrorResponse to the client. Adding a Go-style prefix here would
		// cause regression-test fixtures comparing ERROR / CONTEXT lines to
		// diverge from upstream PostgreSQL output.
		if !mterrors.IsConnectionError(err) {
			if reservedConn.RemoveReservationReason(protoutil.ReasonCopy) {
				e.releaseReservedConn(reservedConn, reserved.ReleasePortalComplete, options)
				return nil, nil, err
			}
			return nil, e.buildReservedState(reservedConn), err
		}
		reservedConn.Release(reserved.ReleaseError, nil)
		return nil, nil, err
	}

	e.logger.DebugContext(ctx, "COPY DONE successful",
		"rows_affected", rowsAffected,
		"command_tag", commandTag)

	// Build result. Attach any NoticeResponse diagnostics captured during
	// CopyDone → CommandComplete; the gateway serializes them into the
	// CopyBidiExecuteResponse.notices proto field and re-emits them as
	// NoticeResponse frames to the client before CommandComplete.
	result := &sqltypes.Result{
		CommandTag:   commandTag,
		RowsAffected: rowsAffected,
		Notices:      notices,
	}

	// Remove the COPY reason. If other reasons remain (e.g., transaction),
	// keep the connection reserved. Otherwise, release it back to the pool.
	if reservedConn.RemoveReservationReason(protoutil.ReasonCopy) {
		e.releaseReservedConn(reservedConn, reserved.ReleasePortalComplete, options)
		return result, nil, nil
	}

	return result, e.buildReservedState(reservedConn), nil
}

// CopyAbort aborts a COPY operation.
// Returns ReservedState with the authoritative reservation state from the multipooler.
func (e *Executor) CopyAbort(
	ctx context.Context,
	target *query.Target,
	errorMsg string,
	options *query.ExecuteOptions,
) (*query.ReservedState, error) {
	if options == nil || options.ReservedConnectionId == 0 {
		// Already cleaned up or never initiated
		return nil, nil
	}

	user := e.getUserFromOptions(options)

	// Get the reserved connection
	reservedConn, ok := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
	if !ok || reservedConn == nil {
		// Already cleaned up — return zero state
		e.logger.DebugContext(ctx, "COPY connection already cleaned up",
			"conn_id", options.ReservedConnectionId)
		return nil, nil
	}

	// If the conn is no longer in COPY mode (CopyReady never added the reason,
	// or CopyFinalize already removed it), the backend is back at RFQ and a
	// CopyFail here would be a protocol violation. This happens on the
	// gateway-side deferred abort that fires after CopyFinalize already
	// completed its own cleanup. Just return the current state.
	if !protoutil.HasCopyReason(reservedConn.RemainingReasons()) {
		e.logger.DebugContext(ctx, "CopyAbort: no COPY reason on conn, nothing to abort",
			"conn_id", options.ReservedConnectionId)
		return e.buildReservedState(reservedConn), nil
	}

	e.logger.DebugContext(ctx, "aborting COPY",
		"error", errorMsg,
		"conn_id", options.ReservedConnectionId)

	// Get the pooled connection for COPY operations
	conn := reservedConn.Conn()

	// Send CopyFail to abort the operation
	writeFailed := false
	if err := conn.WriteCopyFail(errorMsg); err != nil {
		e.logger.ErrorContext(ctx, "failed to write CopyFail", "error", err)
		writeFailed = true
		// Continue to try reading response
	}

	// Read ErrorResponse + ReadyForQuery from PostgreSQL.
	// After CopyFail, PostgreSQL responds with ErrorResponse then ReadyForQuery.
	// ReadCopyFailResponse drains both, leaving the connection in a clean state.
	// Any notices that arrived between CopyFail and ReadyForQuery are
	// discarded here — the client has already initiated an abort, so the
	// abort outcome is what matters, not pre-abort trigger output.
	_, readErr := conn.ReadCopyFailResponse(ctx)
	if readErr != nil {
		e.logger.ErrorContext(ctx, "failed to read response after CopyFail", "error", readErr)
	}

	e.logger.DebugContext(ctx, "COPY FAIL completed")

	if writeFailed || readErr != nil {
		// Connection is in a bad protocol state — release it.
		// We intentionally return nil error: abort is best-effort cleanup and
		// the caller needs a zero ReservedState to know the connection is gone.
		reservedConn.Release(reserved.ReleaseError, nil)
		return nil, nil //nolint:nilerr // intentional: abort is best-effort
	}

	// Clean abort — remove the COPY reason. If other reasons remain
	// (e.g., transaction), keep the connection reserved.
	if reservedConn.RemoveReservationReason(protoutil.ReasonCopy) {
		e.releaseReservedConn(reservedConn, reserved.ReleasePortalComplete, options)
		return nil, nil
	}

	return e.buildReservedState(reservedConn), nil
}

// CopyOutReady initiates a COPY ... TO STDOUT operation and returns format
// information plus any pre-CopyOutResponse notices. The caller (the
// multipooler bidi gRPC handler) then calls CopyOutStream to pump CopyData
// chunks back to the gateway, ending with the trailing CommandComplete +
// ReadyForQuery.
//
// Structure mirrors CopyReady (FROM STDIN) so the reservation lifecycle is
// identical: reuse an existing reserved conn when ReservedConnectionId is
// set, or create a new one (with optional BEGIN-if-needed). The reservation
// reason ReasonCopy is added on success; it must be removed by
// CopyOutStream / CopyAbort.
func (e *Executor) CopyOutReady(
	ctx context.Context,
	target *query.Target,
	copyQuery string,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error) {
	user := e.getUserFromOptions(options)
	var settings map[string]string
	if options != nil {
		settings = e.sessionSettingsForPool(options.SessionSettings)
	}

	e.logger.DebugContext(ctx, "initiating COPY TO STDOUT",
		"query", copyQuery,
		"user", user)

	var (
		reservedConn  *reserved.Conn
		err           error
		format        int16
		columnFormats []int16
		notices       []*mterrors.PgDiagnostic
	)

	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ = e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			return 0, nil, nil, nil, mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
		}
		if err := e.applyReservedSessionSettingsIfNeeded(ctx, reservedConn, options); err != nil {
			return 0, nil, nil, e.buildReservedState(reservedConn), fmt.Errorf("failed to prepare reserved connection: %w", err)
		}
		e.stampVpidOnReserved(ctx, reservedConn, options)
		format, columnFormats, notices, err = reservedConn.Conn().InitiateCopyToStdout(ctx, copyQuery)
		if err != nil {
			// Same dual failure handling as CopyReady (FROM STDIN): a PG
			// ErrorResponse leaves the conn at RFQ and reusable if other
			// reservation reasons remain; a connection-level error means
			// the socket is dead. Surface the PG error un-wrapped so the
			// gateway can re-emit the verbatim ErrorResponse.
			if mterrors.IsConnectionError(err) {
				reservedConn.Release(reserved.ReleaseError, nil)
				return 0, nil, notices, nil, err
			}
			return 0, nil, notices, e.buildReservedState(reservedConn), err
		}
	} else {
		requiresBegin := protoutil.RequiresBegin(protoutil.GetReasons(reservationOptions))
		beginQuery := "BEGIN"
		if reservationOptions != nil && reservationOptions.BeginQuery != "" {
			beginQuery = reservationOptions.BeginQuery
		}

		validate := func(ctx context.Context, conn *regular.Conn) error {
			if requiresBegin {
				if _, err := conn.Query(ctx, beginQuery); err != nil {
					return fmt.Errorf("failed to begin transaction for COPY: %w", err)
				}
			}
			e.stampVpidOnRegular(ctx, conn, options)
			var initErr error
			format, columnFormats, notices, initErr = conn.InitiateCopyToStdout(ctx, copyQuery)
			// Surface PG errors un-wrapped — see CopyOutReady comment above.
			return initErr
		}

		clientKey, serverKey := scramKeysFromOptions(options)
		reservedConn, err = e.poolManager.NewReservedConn(ctx, settings, user, clientKey, serverKey, reserved.WithValidate(validate))
		if err != nil {
			return 0, nil, notices, nil, err
		}

		if requiresBegin {
			reservedConn.AddReservationReason(protoutil.ReasonTransaction)
			// BeginWithQuery's snapshot was bypassed too; capture it now (before any
			// client statement) so a ROLLBACK can revert the pool's cached connstate.
			reservedConn.SnapshotTxnState()
		}
	}

	reservedConn.AddReservationReason(protoutil.ReasonCopy)

	e.logger.DebugContext(ctx, "COPY OUT INITIATE successful",
		"conn_id", reservedConn.ConnID(),
		"format", format,
		"num_columns", len(columnFormats))

	return format, columnFormats, notices, e.buildReservedState(reservedConn), nil
}

// CopyOutStream pumps the COPY ... TO STDOUT response stream back to the
// caller via onMessage callbacks. PG drives the stream: a series of
// CopyData chunks interleaved with NoticeResponse diagnostics, terminated
// by CopyDone followed by CommandComplete + ReadyForQuery. The callback
// receives one CopyOutMessage per CopyData / NoticeResponse seen; a
// CopyData chunk has Data set, a notice has Notice set. CopyDone is
// consumed internally and signals end-of-stream.
//
// After the stream ends, the trailing CommandComplete + ReadyForQuery is
// drained via FinishCopyToStdout and the resulting (*sqltypes.Result with
// CommandTag + RowsAffected + post-data Notices) is returned. On a PG
// ErrorResponse anywhere in the stream, the connection is left in a clean
// state by the underlying client helpers and the ReasonCopy reservation
// reason is dropped; the surviving ReservedState (or nil) is returned for
// the gateway to apply.
func (e *Executor) CopyOutStream(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
	onMessage func(client.CopyOutMessage) error,
) (*sqltypes.Result, *query.ReservedState, error) {
	if options == nil || options.ReservedConnectionId == 0 {
		return nil, nil, errors.New("options.ReservedConnectionId is required for CopyOutStream")
	}

	user := e.getUserFromOptions(options)
	reservedConn, ok := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
	if !ok || reservedConn == nil {
		return nil, nil, mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
	}

	conn := reservedConn.Conn()

	// abortCopyOut releases the reserved conn with ReleaseError, which
	// destroys the backend socket. This is the correct cleanup during a
	// COPY ... TO STDOUT abort because PG is in copy-out mode and is NOT
	// reading from the frontend — writing CopyFail (the FROM STDIN abort
	// message) would only buffer in the kernel and a subsequent
	// ReadCopyFailResponse would receive CopyData where ErrorResponse is
	// expected, fail, and fall through to the same ReleaseError. Skipping
	// the wasted round-trip avoids that I/O. Destroying the socket makes
	// PG see EOF and abort the COPY on its own — the protocol-correct
	// way for a frontend to cancel an in-progress COPY OUT (there is no
	// in-band cancel; pg_cancel_backend on a separate session is the only
	// other PG-blessed option).
	abortCopyOut := func(reason string) {
		e.logger.DebugContext(ctx, "aborting COPY TO STDOUT via socket destroy",
			"conn_id", options.ReservedConnectionId,
			"reason", reason)
		reservedConn.Release(reserved.ReleaseError, nil)
	}

	// Pump CopyData / NoticeResponse to the gateway until CopyDone.
	for {
		if err := ctx.Err(); err != nil {
			abortCopyOut("stream canceled: " + err.Error())
			return nil, nil, err
		}

		msg, err := conn.ReadCopyOutMessage(ctx)
		if err != nil {
			// PG ErrorResponse path: the helper already drained RFQ.
			// Mirror CopyFinalize's release semantics.
			if !mterrors.IsConnectionError(err) {
				if reservedConn.RemoveReservationReason(protoutil.ReasonCopy) {
					e.releaseReservedConn(reservedConn, reserved.ReleasePortalComplete, options)
					return nil, nil, err
				}
				return nil, e.buildReservedState(reservedConn), err
			}
			reservedConn.Release(reserved.ReleaseError, nil)
			return nil, nil, err
		}

		if msg.Done {
			break
		}

		if cbErr := onMessage(msg); cbErr != nil {
			abortCopyOut("callback failed: " + cbErr.Error())
			return nil, nil, cbErr
		}
	}

	commandTag, rowsAffected, notices, err := conn.FinishCopyToStdout(ctx)
	if err != nil {
		if !mterrors.IsConnectionError(err) {
			if reservedConn.RemoveReservationReason(protoutil.ReasonCopy) {
				e.releaseReservedConn(reservedConn, reserved.ReleasePortalComplete, options)
				return nil, nil, err
			}
			return nil, e.buildReservedState(reservedConn), err
		}
		reservedConn.Release(reserved.ReleaseError, nil)
		return nil, nil, err
	}

	result := &sqltypes.Result{
		CommandTag:   commandTag,
		RowsAffected: rowsAffected,
		Notices:      notices,
	}

	if reservedConn.RemoveReservationReason(protoutil.ReasonCopy) {
		e.releaseReservedConn(reservedConn, reserved.ReleasePortalComplete, options)
		return result, nil, nil
	}
	return result, e.buildReservedState(reservedConn), nil
}

// getUserFromOptions extracts the user from ExecuteOptions.
// Returns "postgres" as default if no user is specified.
func (e *Executor) getUserFromOptions(options *query.ExecuteOptions) string {
	if options != nil && options.User != "" {
		return options.User
	}
	// Default to postgres superuser if no user specified
	return "postgres"
}

// scramKeysFromOptions returns the SCRAM passthrough keys carried on the
// request, or (nil, nil) if no keys were forwarded. The connpoolmanager
// consults the passthrough flag before consuming them, so it is always safe
// to pass these through.
func scramKeysFromOptions(options *query.ExecuteOptions) (clientKey, serverKey []byte) {
	if options == nil {
		return nil, nil
	}
	auth := options.GetUserAuth()
	if auth == nil {
		return nil, nil
	}
	return auth.GetClientKey(), auth.GetServerKey()
}

// int32ToInt16Slice converts a slice of int32 to int16.
func int32ToInt16Slice(in []int32) []int16 {
	if in == nil {
		return nil
	}
	out := make([]int16, len(in))
	for i, v := range in {
		out[i] = int16(v)
	}
	return out
}

// wrapQueryError wraps query execution errors with context.
// PostgreSQL errors (*mterrors.PgDiagnostic) are wrapped like any other error;
// the display boundary (writeError) extracts the underlying diagnostic via errors.As.
func wrapQueryError(err error) error {
	if err == nil {
		return nil
	}
	return mterrors.Wrapf(err, "query execution failed")
}

// ConcludeTransaction concludes a transaction on a reserved connection.
// The connection may remain reserved if there are other reasons to keep it (e.g., temp tables).
//
// On ROLLBACK, the caller controls which portal pins are dropped via
// releasePortalNames + releaseAllPortals. PostgreSQL closes only the
// cursors created inside the rolled-back transaction block; cursors
// declared outside the explicit block (under autocommit, before BEGIN)
// survive the ROLLBACK. The gateway computes the per-txn diff and
// passes the inside-txn cursor names so the multipooler unpins exactly
// those, matching PG. When releaseAllPortals is true (or no diff is
// supplied — e.g. by an older gateway that doesn't yet compute it), the
// historical "drop every pin" semantics are used.
//
// Returns ReservedState with the authoritative reservation state.
func (e *Executor) ConcludeTransaction(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
	conclusion multipoolerpb.TransactionConclusion,
	releasePortalNames []string,
	releaseAllPortals bool,
) (*sqltypes.Result, *query.ReservedState, error) {
	if options == nil || options.ReservedConnectionId == 0 {
		return nil, nil, errors.New("reserved_connection_id is required")
	}

	user := e.getUserFromOptions(options)

	e.logger.DebugContext(ctx, "conclude transaction",
		"user", user,
		"reserved_conn_id", options.ReservedConnectionId,
		"conclusion", conclusion.String())

	// Get the reserved connection
	reservedConn, ok := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
	if !ok {
		// Connection destroyed — return zero state so gateway clears its tracking
		return nil, nil, mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
	}

	// Execute COMMIT or ROLLBACK using the reserved connection's methods,
	// which handle both the SQL execution and reason removal.
	var commandTag string
	var releaseReason reserved.ReleaseReason
	switch conclusion {
	case multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT:
		commandTag = "COMMIT"
		releaseReason = reserved.ReleaseCommit
		// Capture the pending defaults-invalidation before COMMIT (Commit clears
		// it). A defaults-changing statement (ALTER DATABASE/ROLE ... SET, or an
		// allowlisted CREATE EXTENSION) that ran inside this transaction becomes
		// durable only now, so the generation bump is owed on a successful COMMIT.
		pendingDefaultsInvalidation := reservedConn.PendingDefaultsInvalidation()
		if err := reservedConn.Commit(ctx); err != nil {
			reservedConn.Release(reserved.ReleaseError, nil)
			return nil, nil, err
		}
		if pendingDefaultsInvalidation {
			e.poolManager.InvalidateConnectionDefaults()
		}
	case multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK:
		commandTag = "ROLLBACK"
		releaseReason = reserved.ReleaseRollback
		if err := reservedConn.Rollback(ctx); err != nil {
			reservedConn.Release(reserved.ReleaseError, nil)
			return nil, nil, err
		}
		// PostgreSQL closes the cursors created inside this transaction
		// block at ROLLBACK; cursors declared outside the block (under
		// autocommit, before BEGIN) survive. Honor the caller's diff so
		// the multipooler pin set tracks PG exactly. When the diff isn't
		// supplied (releaseAllPortals==true), fall back to the historical
		// "drop every pin" behavior — back-compat for callers that
		// haven't been updated yet.
		if releaseAllPortals {
			reservedConn.ReleaseAllPortals()
		} else {
			for _, name := range releasePortalNames {
				reservedConn.ReleasePortal(name)
			}
		}
	default:
		return nil, nil, fmt.Errorf("invalid transaction conclusion: %v", conclusion)
	}

	result := &sqltypes.Result{CommandTag: commandTag}

	// Commit/Rollback already removed the transaction reason.
	// If other reasons remain (e.g., temp tables, portals), the connection stays reserved.
	remainingReasons := reservedConn.RemainingReasons()
	shouldRelease := remainingReasons == 0

	if shouldRelease {
		e.releaseReservedConn(reservedConn, releaseReason, options)
		e.logger.DebugContext(ctx, "transaction concluded",
			"reserved_conn_id", options.ReservedConnectionId,
			"command_tag", commandTag,
			"released", true)
		return result, nil, nil
	}

	e.logger.DebugContext(ctx, "transaction concluded",
		"reserved_conn_id", options.ReservedConnectionId,
		"command_tag", commandTag,
		"released", false,
		"remaining_reasons", protoutil.ReasonsString(remainingReasons))

	return result, e.buildReservedState(reservedConn), nil
}

// DiscardTempTables sends DISCARD TEMP on a reserved connection and removes the temp table reason.
// The connection may remain reserved if there are other reasons to keep it (e.g., transaction).
// Returns ReservedState with the authoritative reservation state.
func (e *Executor) DiscardTempTables(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
) (*sqltypes.Result, *query.ReservedState, error) {
	if options == nil || options.ReservedConnectionId == 0 {
		return nil, nil, errors.New("reserved_connection_id is required")
	}

	user := e.getUserFromOptions(options)

	e.logger.DebugContext(ctx, "discard temp tables",
		"user", user,
		"reserved_conn_id", options.ReservedConnectionId)

	// Get the reserved connection
	reservedConn, ok := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
	if !ok {
		// Connection destroyed — return zero state so gateway clears its tracking
		return nil, nil, mterrors.NewReservedConnectionTerminated(options.ReservedConnectionId)
	}

	// Send DISCARD TEMP to PostgreSQL to drop all temp tables on this backend.
	if _, err := reservedConn.Query(ctx, "DISCARD TEMP"); err != nil {
		reservedConn.Release(reserved.ReleaseError, nil)
		return nil, nil, fmt.Errorf("DISCARD TEMP failed: %w", err)
	}

	// Remove the temp table reason
	reservedConn.RemoveReservationReason(protoutil.ReasonTempTable)

	result := &sqltypes.Result{CommandTag: "DISCARD"}

	// If no other reasons remain, release the connection
	remainingReasons := reservedConn.RemainingReasons()
	if remainingReasons == 0 {
		e.releaseReservedConn(reservedConn, reserved.ReleaseCommit, options)
		e.logger.DebugContext(ctx, "discard temp tables completed, connection released",
			"reserved_conn_id", options.ReservedConnectionId)
		return result, nil, nil
	}

	e.logger.DebugContext(ctx, "discard temp tables completed, connection still reserved",
		"reserved_conn_id", options.ReservedConnectionId,
		"remaining_reasons", protoutil.ReasonsString(remainingReasons))

	return result, e.buildReservedState(reservedConn), nil
}

// ReleaseReservedConnection forcefully releases a reserved connection regardless of reason.
// Used during client disconnect cleanup. Handles transaction rollback, COPY abort,
// and portal release internally. If any cleanup step fails, the connection is
// tainted and closed so the pool creates a fresh one.
func (e *Executor) ReleaseReservedConnection(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
) error {
	if options == nil || options.ReservedConnectionId == 0 {
		return nil // Nothing to release
	}

	user := e.getUserFromOptions(options)

	reservedConn, ok := e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
	if !ok || reservedConn == nil {
		// Already cleaned up or timed out
		return nil
	}

	e.logger.DebugContext(ctx, "releasing reserved connection",
		"user", user,
		"reserved_conn_id", options.ReservedConnectionId,
		"reasons", protoutil.ReasonsString(reservedConn.RemainingReasons()))

	cleanupFailed := false

	// Step 1: If there's a transaction, rollback.
	if reservedConn.IsInTransaction() {
		if err := reservedConn.Rollback(ctx); err != nil {
			e.logger.ErrorContext(ctx, "rollback failed during release",
				"reserved_conn_id", options.ReservedConnectionId, "error", err)
			cleanupFailed = true
		}
	}

	// Step 2: If there's a COPY reason, send CopyFail and read the response.
	// After CopyFail PG sends ErrorResponse + ReadyForQuery (not
	// CommandComplete), so use ReadCopyFailResponse — it expects that
	// shape, leaves the conn in a clean RFQ state, and reports cleanup
	// success without flipping cleanupFailed. ReadCopyDoneResponse would
	// happen to drain the same bytes but treat the ErrorResponse as a
	// failure, falsely marking the conn unrecyclable.
	if !cleanupFailed && protoutil.HasCopyReason(reservedConn.RemainingReasons()) {
		conn := reservedConn.Conn()
		if err := conn.WriteCopyFail("connection closing"); err != nil {
			e.logger.ErrorContext(ctx, "CopyFail write failed during release",
				"reserved_conn_id", options.ReservedConnectionId, "error", err)
			cleanupFailed = true
		} else if _, err := conn.ReadCopyFailResponse(ctx); err != nil {
			e.logger.DebugContext(ctx, "error reading response after CopyFail",
				"reserved_conn_id", options.ReservedConnectionId, "error", err)
			cleanupFailed = true
		}
	}

	// Step 3: If there are temp tables, discard them so the backend is clean
	// when returned to the pool.
	if !cleanupFailed && protoutil.HasTempTableReason(reservedConn.RemainingReasons()) {
		if _, err := reservedConn.Conn().Query(ctx, "DISCARD TEMP"); err != nil {
			e.logger.ErrorContext(ctx, "DISCARD TEMP failed during release",
				"reserved_conn_id", options.ReservedConnectionId, "error", err)
			cleanupFailed = true
		}
	}

	// Step 3b: If the session holds session-level advisory locks, release them
	// before the backend returns to the pool. Rolling back (Step 1) only drops
	// transaction-level locks; session-level advisory locks would otherwise leak
	// to whichever client next reuses this pooled backend. pg_advisory_unlock_all
	// is the narrow, targeted fix — unlike DISCARD ALL it leaves prepared
	// statements and other backend state intact, so the multipooler's
	// per-connection prepared-statement tracking stays in sync.
	if !cleanupFailed && protoutil.HasSessionAdvisoryLockReason(reservedConn.RemainingReasons()) {
		if _, err := reservedConn.Conn().Query(ctx, "SELECT pg_advisory_unlock_all()"); err != nil {
			e.logger.ErrorContext(ctx, "pg_advisory_unlock_all failed during release",
				"reserved_conn_id", options.ReservedConnectionId, "error", err)
			cleanupFailed = true
		} else {
			reservedConn.RemoveReservationReason(protoutil.ReasonSessionAdvisoryLock)
		}
	}

	// Step 4: Release all portals (in-memory only, always succeeds).
	reservedConn.ReleaseAllPortals()

	// Step 5: Release or close the connection. The clean path forwards the
	// gateway's authoritative session settings so an untrusted connstate cache
	// (e.g. a ROLLBACK TO SAVEPOINT whose untrusted flag is still sticky under a
	// surviving session reason at teardown) is synced to the truth rather than
	// wrongly cleared — clearing it would leak the backend's real session GUCs to
	// the next client that reuses this pooled backend.
	if cleanupFailed {
		reservedConn.Release(reserved.ReleaseError, nil)
	} else {
		e.releaseReservedConn(reservedConn, reserved.ReleaseRollback, options)
	}

	e.logger.DebugContext(ctx, "reserved connection released",
		"reserved_conn_id", options.ReservedConnectionId,
		"cleanup_failed", cleanupFailed)

	return nil
}

// Ensure Executor implements queryservice.QueryService
var _ queryservice.QueryService = (*Executor)(nil)
