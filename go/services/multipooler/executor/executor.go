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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/pools/reserved"
)

// Executor implements the QueryService interface for executing queries against PostgreSQL.
// It uses the connpoolmanager for per-user connection pool management and consolidates
// prepared statements across connections to avoid redundant parsing.
type Executor struct {
	logger             *slog.Logger
	poolManager        connpoolmanager.PoolManager
	poolerConsolidator *preparedstatement.PoolerConsolidator
	poolerID           *clustermetadatapb.ID
}

// NewExecutor creates a new Executor instance.
func NewExecutor(logger *slog.Logger, poolManager connpoolmanager.PoolManager, poolerID *clustermetadatapb.ID) *Executor {
	return &Executor{
		logger:             logger,
		poolManager:        poolManager,
		poolerConsolidator: preparedstatement.NewPoolerConsolidator(),
		poolerID:           poolerID,
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
	}
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
			return nil, nil, fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
		}

		// Apply settings if they changed (e.g., SET inside a transaction).
		// Reserved connections bypass the pool's normal ApplySettings mechanism,
		// so we must explicitly apply settings changes here.
		if options.SessionSettings != nil {
			if err := e.poolManager.ApplySettingsToConn(ctx, reservedConn.Conn(), options.SessionSettings); err != nil {
				return nil, e.buildReservedState(reservedConn), fmt.Errorf("failed to apply settings to reserved connection: %w", err)
			}
		}

		results, err := reservedConn.Query(ctx, sql)
		if err != nil {
			// Query failed but connection still exists — return current state
			return nil, e.buildReservedState(reservedConn), wrapQueryError(err)
		}

		if len(results) == 0 {
			return &sqltypes.Result{}, e.buildReservedState(reservedConn), nil
		}
		return results[0], e.buildReservedState(reservedConn), nil
	}

	// Get session settings from options
	var settings map[string]string
	if options != nil {
		settings = options.SessionSettings
	}

	// Get a connection from the pool for this user
	conn, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get connection for user %s: %w", user, err)
	}
	defer conn.Recycle()

	// Execute the query - the regular.Conn.QueryWithRetry returns []*sqltypes.Result
	// with proper field info, rows, and command tags already populated.
	// Uses retry variant since this is a stateless pool query.
	results, err := conn.Conn.QueryWithRetry(ctx, sql)
	if err != nil {
		return nil, nil, wrapQueryError(err)
	}

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
			return nil, fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
		}

		// Apply settings if they changed (e.g., SET inside a transaction).
		// Reserved connections bypass the pool's normal ApplySettings mechanism,
		// so we must explicitly apply settings changes here. This step depends
		// on the underlying *regular.Conn so it stays out of the helper.
		if options.SessionSettings != nil {
			if err := e.poolManager.ApplySettingsToConn(ctx, reservedConn.Conn(), options.SessionSettings); err != nil {
				return e.buildReservedState(reservedConn), fmt.Errorf("failed to apply settings to reserved connection: %w", err)
			}
		}

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

		return e.streamExecuteOnReservedConn(ctx, reservedConn, sql, reservationOptions, callback)
	}

	// Case 2: Create a new reserved connection
	if reasons != 0 {
		return e.reserveAndStreamExecute(ctx, sql, options, reservationOptions, callback)
	}

	// Case 3: Use regular pooled connection
	var settings map[string]string
	if options != nil {
		settings = options.SessionSettings
	}

	// Get a connection from the pool for this user
	conn, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection for user %s: %w", user, err)
	}
	defer conn.Recycle()

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
		return nil, nil
	}

	// Use streaming query execution with retry since this is a stateless pool query.
	if err := conn.Conn.QueryStreamingWithRetry(ctx, sql, callback); err != nil {
		return nil, wrapQueryError(err)
	}

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
		settings = options.SessionSettings
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
	reservedConn, err := e.poolManager.NewReservedConn(ctx, settings, user, reservedOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create reserved connection: %w", err)
	}

	// Apply all reservation reasons to the reserved connection.
	// BeginWithQuery below adds ReasonTransaction internally, but non-transaction
	// reasons (e.g., temp_table) must be added explicitly so that buildReservedState
	// returns the correct bitmask and DiscardTempTables can find the shard.
	if nonBeginReasons := reasons &^ protoutil.ReasonTransaction; nonBeginReasons != 0 {
		reservedConn.AddReservationReason(nonBeginReasons)
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
			reservedConn.Release(reserved.ReleaseError)
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
		reservedConn.Release(reserved.ReleaseError)
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	reservedState := e.buildReservedState(reservedConn)

	e.logger.DebugContext(ctx, "reserve stream execute completed",
		"reserved_conn_id", reservedState.ReservedConnectionId)

	return reservedState, nil
}

// streamExecuteOnReservedConn executes a query on an existing reserved
// connection. It optionally promotes the reservation by adding new reasons
// (e.g., starting a transaction on a temp-table-reserved connection) before
// running the query, and reconciles the in-memory transaction tracking against
// PG's actual transaction status afterwards.
//
// Defined over reservedConnAPI rather than *reserved.Conn so that unit tests
// can substitute a mock and exercise this path without a live PG connection.
func (e *Executor) streamExecuteOnReservedConn(
	ctx context.Context,
	rc reservedConnAPI,
	sql string,
	reservationOptions *query.ReservationOptions,
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

	if err := rc.QueryStreaming(ctx, sql, callback); err != nil {
		// Query failed but connection still exists — return current state
		return e.buildReservedStateFromAPI(rc), wrapQueryError(err)
	}

	// Sync our in-memory transaction tracking with PG's actual state. The SQL
	// itself may contain transaction control statements (e.g., a simple-query
	// payload like "BEGIN; SELECT 1; COMMIT;") that change PG's transaction
	// status without going through ReservationOptions, so we reconcile against
	// the ReadyForQuery transaction indicator after the query completes.
	if rc.TxnStatus() == protocol.TxnStatusInBlock && !rc.IsInTransaction() {
		rc.AddReservationReason(protoutil.ReasonTransaction)
	} else if rc.TxnStatus() == protocol.TxnStatusIdle && rc.IsInTransaction() {
		// Transaction ended via inline COMMIT/ROLLBACK — remove the reason.
		rc.RemoveReservationReason(protoutil.ReasonTransaction)
	}

	return e.buildReservedStateFromAPI(rc), nil
}

// Close closes the executor and releases resources.
// Note: The poolManager is managed by the caller (QueryPoolerServer), not closed here.
func (e *Executor) Close() error {
	return nil
}

// PortalStreamExecute executes a portal (bound prepared statement) and streams results back via callback.
// If MaxRows > 0, a reserved connection is used since the portal may be suspended and need resumption.
// Otherwise, a regular connection is used for better pool efficiency.
func (e *Executor) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
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
		settings = options.SessionSettings
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

	// Use reserved connection if:
	// 1. ReservedConnectionId is already set (e.g., from transaction or previous portal)
	// 2. MaxRows > 0 (portal may be suspended and need resumption)
	if (options != nil && options.ReservedConnectionId > 0) || maxRows > 0 {
		return e.portalExecuteWithReserved(ctx, preparedStatement, portal, options, settings, user, maxRows, paramFormats, resultFormats, callback)
	}

	// Use regular connection for non-suspended execution with no existing reservation
	return e.portalExecuteWithRegular(ctx, preparedStatement, portal, settings, user, paramFormats, resultFormats, callback)
}

// portalExecuteWithReserved executes a portal using a reserved connection.
func (e *Executor) portalExecuteWithReserved(
	ctx context.Context,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	settings map[string]string,
	user string,
	maxRows int32,
	paramFormats, resultFormats []int16,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	var reservedConn *reserved.Conn
	var err error

	// Check if we should use an existing reserved connection
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ = e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			return nil, fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
		}
	} else {
		// Create a new reserved connection
		reservedConn, err = e.poolManager.NewReservedConn(ctx, settings, user)
		if err != nil {
			return nil, fmt.Errorf("failed to create reserved connection for user %s: %w", user, err)
		}
	}

	// Ensure the statement is prepared on this connection (with consolidation)
	canonicalName, err := e.ensurePrepared(ctx, reservedConn.Conn(), preparedStatement)
	if err != nil {
		reservedConn.Release(reserved.ReleaseError)
		return nil, err
	}

	// Bind and execute using the portal's own name and the canonical statement name
	params := sqltypes.ParamsFromProto(portal.ParamLengths, portal.ParamValues)
	completed, err := reservedConn.BindAndExecute(ctx, portal.Name, canonicalName, params, paramFormats, resultFormats, maxRows, callback)
	if err != nil {
		reservedConn.Release(reserved.ReleaseError)
		return nil, wrapQueryError(err)
	}

	// If portal is suspended (not completed), keep the reserved connection for continuation
	if !completed {
		reservedConn.ReserveForPortal(portal.Name)
	} else {
		// Portal completed, release this portal's reservation.
		// ReleasePortal returns true only when all reservation reasons are gone.
		shouldRelease := reservedConn.ReleasePortal(portal.Name)
		if shouldRelease {
			reservedConn.Release(reserved.ReleasePortalComplete)
			return nil, nil
		}
	}

	return e.buildReservedState(reservedConn), nil
}

// portalExecuteWithRegular executes a portal using a regular pooled connection.
func (e *Executor) portalExecuteWithRegular(
	ctx context.Context,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	settings map[string]string,
	user string,
	paramFormats, resultFormats []int16,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	conn, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection for user %s: %w", user, err)
	}
	defer conn.Recycle()

	// Ensure the statement is prepared on this connection (with consolidation)
	canonicalName, err := e.ensurePrepared(ctx, conn.Conn, preparedStatement)
	if err != nil {
		return nil, err
	}

	// Bind and execute with maxRows=0 (fetch all) using the portal's own name and canonical statement name
	params := sqltypes.ParamsFromProto(portal.ParamLengths, portal.ParamValues)
	_, err = conn.Conn.BindAndExecute(ctx, portal.Name, canonicalName, params, paramFormats, resultFormats, 0, callback)
	if err != nil {
		return nil, wrapQueryError(err)
	}

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
		settings = options.SessionSettings
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
			return nil, fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
		}

		if options.SessionSettings != nil {
			if err := e.poolManager.ApplySettingsToConn(ctx, reservedConn.Conn(), options.SessionSettings); err != nil {
				return nil, fmt.Errorf("failed to apply settings to reserved connection: %w", err)
			}
		}

		conn = reservedConn.Conn()
	} else {
		pooled, err := e.poolManager.GetRegularConnWithSettings(ctx, settings, user)
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
		settings = options.SessionSettings
	}

	e.logger.DebugContext(ctx, "initiating COPY FROM STDIN",
		"query", copyQuery,
		"user", user)

	var reservedConn *reserved.Conn
	var err error

	// Check if we should use an existing reserved connection
	if options != nil && options.ReservedConnectionId > 0 {
		reservedConn, _ = e.poolManager.GetReservedConn(int64(options.ReservedConnectionId), user)
		if reservedConn == nil {
			return 0, nil, nil, fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
		}
	} else {
		// Create a new reserved connection (COPY requires connection affinity)
		reservedConn, err = e.poolManager.NewReservedConn(ctx, settings, user)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("failed to create reserved connection for COPY: %w", err)
		}

		// If this is a transaction reservation, execute BEGIN before COPY.
		// This handles the deferred-BEGIN case where the client sent BEGIN
		// but no query has been executed yet to create a reserved connection.
		if protoutil.RequiresBegin(protoutil.GetReasons(reservationOptions)) {
			beginQuery := "BEGIN"
			if reservationOptions != nil && reservationOptions.BeginQuery != "" {
				beginQuery = reservationOptions.BeginQuery
			}
			if err := reservedConn.BeginWithQuery(ctx, beginQuery); err != nil {
				reservedConn.Release(reserved.ReleaseError)
				return 0, nil, nil, fmt.Errorf("failed to begin transaction for COPY: %w", err)
			}
		}
	}

	connID := reservedConn.ConnID()

	// Get the pooled connection for COPY operations
	conn := reservedConn.Conn()

	// Send COPY command and read CopyInResponse
	format, columnFormats, err := conn.InitiateCopyFromStdin(ctx, copyQuery)
	if err != nil {
		// Connection is in bad state after failed COPY initiation - close it instead of recycling
		reservedConn.Release(reserved.ReleaseError)
		return 0, nil, nil, fmt.Errorf("failed to initiate COPY FROM STDIN: %w", err)
	}

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
		return fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
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
		return nil, nil, fmt.Errorf("reserved connection %d not found for user %s", options.ReservedConnectionId, user)
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
			reservedConn.Release(reserved.ReleaseError)
			return nil, nil, fmt.Errorf("failed to write final COPY data: %w", err)
		}
		e.logger.DebugContext(ctx, "sent final COPY data", "size", len(finalData))
	}

	// Send CopyDone to signal completion
	if err := conn.WriteCopyDone(); err != nil {
		e.logger.ErrorContext(ctx, "failed to write CopyDone", "error", err)
		// Connection is in bad state - close it instead of recycling
		reservedConn.Release(reserved.ReleaseError)
		return nil, nil, fmt.Errorf("failed to write CopyDone: %w", err)
	}

	// Read CommandComplete response from PostgreSQL
	commandTag, rowsAffected, err := conn.ReadCopyDoneResponse(ctx)
	if err != nil {
		e.logger.ErrorContext(ctx, "COPY operation failed", "error", err)
		// Connection might be in bad state - close it instead of recycling
		reservedConn.Release(reserved.ReleaseError)
		return nil, nil, fmt.Errorf("COPY operation failed: %w", err)
	}

	e.logger.DebugContext(ctx, "COPY DONE successful",
		"rows_affected", rowsAffected,
		"command_tag", commandTag)

	// Build result
	result := &sqltypes.Result{
		CommandTag:   commandTag,
		RowsAffected: rowsAffected,
	}

	// Remove the COPY reason. If other reasons remain (e.g., transaction),
	// keep the connection reserved. Otherwise, release it back to the pool.
	if reservedConn.RemoveReservationReason(protoutil.ReasonCopy) {
		reservedConn.Release(reserved.ReleasePortalComplete)
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
	readErr := conn.ReadCopyFailResponse(ctx)
	if readErr != nil {
		e.logger.ErrorContext(ctx, "failed to read response after CopyFail", "error", readErr)
	}

	e.logger.DebugContext(ctx, "COPY FAIL completed")

	if writeFailed || readErr != nil {
		// Connection is in a bad protocol state — release it.
		// We intentionally return nil error: abort is best-effort cleanup and
		// the caller needs a zero ReservedState to know the connection is gone.
		reservedConn.Release(reserved.ReleaseError)
		return nil, nil //nolint:nilerr // intentional: abort is best-effort
	}

	// Clean abort — remove the COPY reason. If other reasons remain
	// (e.g., transaction), keep the connection reserved.
	if reservedConn.RemoveReservationReason(protoutil.ReasonCopy) {
		reservedConn.Release(reserved.ReleasePortalComplete)
		return nil, nil
	}

	return e.buildReservedState(reservedConn), nil
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
// Returns ReservedState with the authoritative reservation state.
func (e *Executor) ConcludeTransaction(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
	conclusion multipoolerpb.TransactionConclusion,
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
		return nil, nil, fmt.Errorf("reserved connection %d not found", options.ReservedConnectionId)
	}

	// Execute COMMIT or ROLLBACK using the reserved connection's methods,
	// which handle both the SQL execution and reason removal.
	var commandTag string
	var releaseReason reserved.ReleaseReason
	switch conclusion {
	case multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT:
		commandTag = "COMMIT"
		releaseReason = reserved.ReleaseCommit
		if err := reservedConn.Commit(ctx); err != nil {
			reservedConn.Release(reserved.ReleaseError)
			return nil, nil, err
		}
	case multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK:
		commandTag = "ROLLBACK"
		releaseReason = reserved.ReleaseRollback
		if err := reservedConn.Rollback(ctx); err != nil {
			reservedConn.Release(reserved.ReleaseError)
			return nil, nil, err
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
		reservedConn.Release(releaseReason)
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
		return nil, nil, fmt.Errorf("reserved connection %d not found", options.ReservedConnectionId)
	}

	// Send DISCARD TEMP to PostgreSQL to drop all temp tables on this backend.
	if _, err := reservedConn.Query(ctx, "DISCARD TEMP"); err != nil {
		reservedConn.Release(reserved.ReleaseError)
		return nil, nil, fmt.Errorf("DISCARD TEMP failed: %w", err)
	}

	// Remove the temp table reason
	reservedConn.RemoveReservationReason(protoutil.ReasonTempTable)

	result := &sqltypes.Result{CommandTag: "DISCARD"}

	// If no other reasons remain, release the connection
	remainingReasons := reservedConn.RemainingReasons()
	if remainingReasons == 0 {
		reservedConn.Release(reserved.ReleaseCommit)
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
	if !cleanupFailed && protoutil.HasCopyReason(reservedConn.RemainingReasons()) {
		conn := reservedConn.Conn()
		if err := conn.WriteCopyFail("connection closing"); err != nil {
			e.logger.ErrorContext(ctx, "CopyFail write failed during release",
				"reserved_conn_id", options.ReservedConnectionId, "error", err)
			cleanupFailed = true
		} else if _, _, err := conn.ReadCopyDoneResponse(ctx); err != nil {
			e.logger.DebugContext(ctx, "error reading response after CopyFail (expected)",
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

	// Step 4: Release all portals (in-memory only, always succeeds).
	reservedConn.ReleaseAllPortals()

	// Step 5: Release or close the connection.
	if cleanupFailed {
		reservedConn.Release(reserved.ReleaseError)
	} else {
		reservedConn.Release(reserved.ReleaseRollback)
	}

	e.logger.DebugContext(ctx, "reserved connection released",
		"reserved_conn_id", options.ReservedConnectionId,
		"cleanup_failed", cleanupFailed)

	return nil
}

// Ensure Executor implements queryservice.QueryService
var _ queryservice.QueryService = (*Executor)(nil)
