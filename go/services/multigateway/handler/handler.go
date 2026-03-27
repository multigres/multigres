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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// ExecuteResult carries plan metadata back from the executor to the handler
// for metrics and observability.
type ExecuteResult struct {
	// TablesUsed contains deduplicated, schema-qualified table names
	// referenced by the executed query.
	TablesUsed []string

	// PlanType is the name of the root primitive (e.g. "Route", "Transaction").
	PlanType string

	// PlanTime is how long query planning took.
	PlanTime time.Duration
}

// Executor defines the interface for query execution.
type Executor interface {
	// StreamExecute is used to run the provided query in streaming mode.
	StreamExecute(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, queryStr string, astStmt ast.Stmt, callback func(ctx context.Context, result *sqltypes.Result) error) (*ExecuteResult, error)

	// PortalStreamExecute is used to execute a Portal that was previously created.
	PortalStreamExecute(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, portalInfo *preparedstatement.PortalInfo, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (*ExecuteResult, error)

	// Describe returns metadata about a prepared statement or portal.
	// The options should contain PreparedStatement or Portal information and the reserved connection ID.
	Describe(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, portalInfo *preparedstatement.PortalInfo, preparedStatementInfo *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error)

	// ReleaseAll releases all reserved connections, regardless of reservation reason.
	// For transaction-reserved connections, a ROLLBACK is sent first.
	// For COPY-reserved connections, the COPY is aborted.
	// Any remaining reserved connections are force-cleared.
	// Used for connection cleanup when a client disconnects.
	ReleaseAll(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState) error
}

// MultiGatewayHandler implements the pgprotocol Handler interface for multigateway.
// It routes PostgreSQL protocol queries to the appropriate multipooler instances.
type MultiGatewayHandler struct {
	executor         Executor
	logger           *slog.Logger
	psc              *preparedstatement.Consolidator
	statementTimeout time.Duration
	metrics          *HandlerMetrics
	slowThreshold    time.Duration
	notifMgr         NotificationManager
}

// NewMultiGatewayHandler creates a new PostgreSQL protocol handler.
func NewMultiGatewayHandler(executor Executor, logger *slog.Logger, statementTimeout time.Duration) *MultiGatewayHandler {
	metrics, err := NewHandlerMetrics()
	if err != nil {
		logger.Warn("failed to initialise some handler metrics", "error", err)
	}
	return &MultiGatewayHandler{
		executor:         executor,
		logger:           logger.With("component", "multigateway_handler"),
		psc:              preparedstatement.NewConsolidator(),
		statementTimeout: statementTimeout,
		metrics:          metrics,
		slowThreshold:    constants.DefaultSlowQueryThreshold,
		notifMgr:         DefaultNotificationManager(),
	}
}

// Consolidator returns the prepared statement consolidator.
func (h *MultiGatewayHandler) Consolidator() *preparedstatement.Consolidator {
	return h.psc
}

// errAbortedTransaction is the error returned when queries are executed in an aborted transaction.
// PostgreSQL returns SQLSTATE 25P02 (in_failed_sql_transaction) for this condition.
var errAbortedTransaction = mterrors.NewPgError("ERROR", mterrors.PgSSInFailedTransaction,
	"current transaction is aborted, commands ignored until end of transaction block", "")

// statementTimeoutCtx resolves the effective statement timeout and returns a
// context with that deadline applied. The returned cancel function must always
// be called (use defer). The caller (conn.go's queryContextError) is
// responsible for mapping DeadlineExceeded to the appropriate PostgreSQL error.
func (h *MultiGatewayHandler) statementTimeoutCtx(ctx context.Context, state *MultiGatewayConnectionState, query ast.Stmt) (context.Context, context.CancelFunc) {
	timeout := ResolveStatementTimeout(
		ParseStatementTimeoutDirective(query),
		state.GetStatementTimeout(),
	)
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

// HandleQuery processes a simple query protocol message ('Q').
// Routes the query to an appropriate multipooler instance and streams results back.
func (h *MultiGatewayHandler) HandleQuery(ctx context.Context, conn *server.Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	queryStart := time.Now()
	h.logger.DebugContext(ctx, "handling query", "query", queryStr, "user", conn.User(), "database", conn.Database())

	parseStart := time.Now()
	asts, err := parser.ParseSQL(queryStr)
	parseDuration := time.Since(parseStart)
	if err != nil {
		h.recordQueryCompletion(ctx, conn, "UNKNOWN", "simple", parseDuration, 0, time.Since(queryStart), 0, nil, err)
		return err
	}

	// Handle empty query (e.g., just a semicolon or whitespace).
	// Call callback with nil to signal empty query response.
	if len(asts) == 0 {
		return callback(ctx, nil)
	}

	// TODO: For multi-statement batches, this only captures the first statement's
	// operation name. Consider recording per-statement metrics or using "MULTI" as
	// the operation name when len(asts) > 1.
	operationName := asts[0].StatementType()
	ctx, span := startQuerySpan(ctx, operationName, "simple", conn.Database(), conn.User())
	defer span.End()

	st := h.getConnectionState(conn)

	// If the transaction is in an aborted state, reject all queries unless the
	// first statement is ROLLBACK. This matches PostgreSQL's behavior: after an
	// error in a transaction block, commands are rejected until ROLLBACK.
	// Multi-statement batches starting with ROLLBACK are allowed (e.g.,
	// "ROLLBACK; SELECT 1;") — ROLLBACK clears the aborted state and the
	// remaining statements execute normally.
	if conn.TxnStatus() == protocol.TxnStatusFailed {
		if !h.startsWithRollback(asts) {
			h.recordQueryCompletion(ctx, conn, operationName, "simple", parseDuration, 0, time.Since(queryStart), 0, nil, errAbortedTransaction)
			recordSpanError(span, errAbortedTransaction, mterrors.ExtractSQLSTATE(errAbortedTransaction))
			return errAbortedTransaction
		}
	}

	// Row-counting callback wraps the original to track rows streamed to the client.
	var rowCount int64
	countingCallback := func(ctx context.Context, result *sqltypes.Result) error {
		if result != nil {
			rowCount += int64(len(result.Rows))
		}
		return callback(ctx, result)
	}

	execStart := time.Now()

	// For multi-statement batches, use implicit transaction handling.
	// This handles cases where transactions start/end mid-batch and ensures
	// proper auto-rollback for implicit transaction segments on failure.
	// Per-table metrics are emitted per-statement inside executeWithImplicitTransaction.
	// For multi-statement batches, per-table metrics, span attributes, and query log
	// enrichment are handled per-statement inside executeWithImplicitTransaction.
	// The batch-level recordQueryCompletion gets nil result (no plan type for a batch).
	var result *ExecuteResult
	if len(asts) > 1 {
		h.logger.DebugContext(ctx, "executing multi-statement batch with implicit transaction handling",
			"statement_count", len(asts),
			"already_in_transaction", conn.IsInTransaction())
		err = h.executeWithImplicitTransaction(ctx, conn, st, queryStr, asts, countingCallback)
	} else {
		// Single statement - execute with timeout enforcement
		ctx, cancel := h.statementTimeoutCtx(ctx, st, asts[0])
		defer cancel()

		result, err = h.executor.StreamExecute(ctx, conn, st, queryStr, asts[0], countingCallback)
		if err != nil {
			// If we're in an active transaction and the query failed,
			// transition to aborted state. The client must ROLLBACK to recover.
			if conn.TxnStatus() == protocol.TxnStatusInBlock {
				conn.SetTxnStatus(protocol.TxnStatusFailed)
			}
		}

		// Post-execution: sync LISTEN/NOTIFY subscriptions.
		if err == nil {
			h.handleListenStateChanges(ctx, conn, st, asts[0])
		}
	}

	execDuration := time.Since(execStart)
	totalDuration := time.Since(queryStart)
	h.recordQueryCompletion(ctx, conn, operationName, "simple", parseDuration, execDuration, totalDuration, rowCount, result, err)
	if err != nil {
		recordSpanError(span, err, mterrors.ExtractSQLSTATE(err))
	}

	// Flush pending notifications to client after query completes.
	h.flushNotifications(conn, st)

	return err
}

// startsWithRollback returns true if the first statement is ROLLBACK,
// allowing the client to exit an aborted transaction. Multi-statement
// batches are permitted as long as the first statement is ROLLBACK
// (matching PostgreSQL behavior).
func (h *MultiGatewayHandler) startsWithRollback(asts []ast.Stmt) bool {
	return len(asts) > 0 && ast.IsRollbackStatement(asts[0])
}

// getConnectionState retrieves and typecasts the connection state for this handler.
// Initializes a new state if it doesn't exist.
func (h *MultiGatewayHandler) getConnectionState(conn *server.Conn) *MultiGatewayConnectionState {
	state := conn.GetConnectionState()
	if state == nil {
		newState := NewMultiGatewayConnectionState()
		newState.StartupParams = conn.GetStartupParams()

		// Initialize gateway-managed variables. Startup params take precedence
		// over the flag default (matching PostgreSQL's GUC priority).
		stDefault := h.statementTimeout
		if v, ok := newState.StartupParams["statement_timeout"]; ok {
			if d, err := ParsePostgresInterval("statement_timeout", v); err == nil {
				stDefault = d
			} else {
				h.logger.Warn("ignoring invalid statement_timeout startup parameter, using flag default",
					"value", v, "default", h.statementTimeout, "error", err)
			}
			delete(newState.StartupParams, "statement_timeout")
		}
		newState.InitStatementTimeout(stDefault)

		conn.SetConnectionState(newState)
		return newState
	}
	return state.(*MultiGatewayConnectionState)
}

// HandleParse processes a Parse message ('P') for the extended query protocol.
// Creates and stores a prepared statement.
func (h *MultiGatewayHandler) HandleParse(ctx context.Context, conn *server.Conn, name, queryStr string, paramTypes []uint32) error {
	h.logger.DebugContext(ctx, "parse", "name", name, "query", queryStr, "param_count", len(paramTypes))

	// Basic validation: query must not be empty.
	if queryStr == "" {
		return errors.New("query string cannot be empty")
	}

	_, err := h.psc.AddPreparedStatement(conn.ConnectionID(), name, queryStr, paramTypes)
	return err
}

// HandleBind processes a Bind message ('B') for the extended query protocol.
// Creates and stores a portal for the specified prepared statement with bound parameters.
func (h *MultiGatewayHandler) HandleBind(ctx context.Context, conn *server.Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	h.logger.DebugContext(ctx, "bind", "portal", portalName, "statement", stmtName, "param_count", len(params))

	// Get the prepared statement to verify it exists.
	psi := h.psc.GetPreparedStatementInfo(conn.ConnectionID(), stmtName)
	if psi == nil {
		return mterrors.NewInvalidPreparedStatementError(stmtName)
	}

	// Get the connection state.
	state := h.getConnectionState(conn)

	// Create portal using protoutil helper.
	portal := protoutil.NewPortal(portalName, psi.Name, params, paramFormats, resultFormats)
	state.StorePortalInfo(portal, psi)

	return nil
}

// HandleExecute processes an Execute message ('E') for the extended query protocol.
// Executes the specified portal's query with bound parameters and streams results via callback.
func (h *MultiGatewayHandler) HandleExecute(ctx context.Context, conn *server.Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	queryStart := time.Now()
	h.logger.DebugContext(ctx, "execute", "portal", portalName, "max_rows", maxRows)

	// Get the connection state.
	state := h.getConnectionState(conn)

	// Get the portal.
	portalInfo := state.GetPortalInfo(portalName)
	if portalInfo == nil {
		portalErr := mterrors.NewInvalidPortalError(portalName)
		// Record before span creation since we don't have an operation name yet.
		h.recordQueryCompletion(ctx, conn, "UNKNOWN", "extended", 0, 0, time.Since(queryStart), 0, nil, portalErr)
		return portalErr
	}

	operationName := "UNKNOWN"
	if stmt := portalInfo.AstStmt(); stmt != nil {
		operationName = stmt.StatementType()
	}
	ctx, span := startQuerySpan(ctx, operationName, "extended", conn.Database(), conn.User())
	defer span.End()

	// Reject queries in aborted transaction state, except ROLLBACK which is the
	// only statement that can recover from an aborted transaction.
	if conn.TxnStatus() == protocol.TxnStatusFailed {
		if !ast.IsRollbackStatement(portalInfo.AstStmt()) {
			h.recordQueryCompletion(ctx, conn, operationName, "extended", 0, 0, time.Since(queryStart), 0, nil, errAbortedTransaction)
			recordSpanError(span, errAbortedTransaction, mterrors.ExtractSQLSTATE(errAbortedTransaction))
			return errAbortedTransaction
		}
	}

	// Row-counting callback.
	var rowCount int64
	countingCallback := func(ctx context.Context, result *sqltypes.Result) error {
		if result != nil {
			rowCount += int64(len(result.Rows))
		}
		return callback(ctx, result)
	}

	// Use the original query string for directive parsing (extended protocol preserves comments).
	astStmt := portalInfo.PreparedStatementInfo.AstStmt()
	ctx, cancel := h.statementTimeoutCtx(ctx, state, astStmt)
	defer cancel()

	execStart := time.Now()
	result, err := h.executor.PortalStreamExecute(ctx, conn, state, portalInfo, maxRows, countingCallback)
	if err != nil {
		if conn.TxnStatus() == protocol.TxnStatusInBlock {
			conn.SetTxnStatus(protocol.TxnStatusFailed)
		}
	}

	// Post-execution: sync LISTEN/NOTIFY subscriptions for extended protocol.
	if err == nil && astStmt != nil {
		h.handleListenStateChanges(ctx, conn, state, astStmt)
	}

	execDuration := time.Since(execStart)
	totalDuration := time.Since(queryStart)
	h.recordQueryCompletion(ctx, conn, operationName, "extended", 0, execDuration, totalDuration, rowCount, result, err)
	if err != nil {
		recordSpanError(span, err, mterrors.ExtractSQLSTATE(err))
	}

	// Deliver any pending notifications after query completes.
	h.flushNotifications(conn, state)

	return err
}

// HandleDescribe processes a Describe message ('D').
// Describes either a prepared statement ('S') or a portal ('P').
func (h *MultiGatewayHandler) HandleDescribe(ctx context.Context, conn *server.Conn, typ byte, name string) (*query.StatementDescription, error) {
	h.logger.DebugContext(ctx, "describe", "type", string(typ), "name", name)

	// Get the connection state.
	state := h.getConnectionState(conn)

	switch typ {
	case 'S': // Describe prepared statement
		stmt := h.psc.GetPreparedStatementInfo(conn.ConnectionID(), name)
		if stmt == nil {
			return nil, mterrors.NewInvalidPreparedStatementError(name)
		}

		// Call executor to get description from multipooler
		return h.executor.Describe(ctx, conn, state, nil, stmt)

	case 'P': // Describe portal
		portalInfo := state.GetPortalInfo(name)
		if portalInfo == nil {
			return nil, mterrors.NewInvalidPortalError(name)
		}

		// Call executor to get description from multipooler
		return h.executor.Describe(ctx, conn, state, portalInfo, nil)

	default:
		return nil, fmt.Errorf("invalid describe type: %c (expected 'S' or 'P')", typ)
	}
}

// HandleClose processes a Close message ('C').
// Closes either a prepared statement ('S') or a portal ('P').
func (h *MultiGatewayHandler) HandleClose(ctx context.Context, conn *server.Conn, typ byte, name string) error {
	h.logger.DebugContext(ctx, "close", "type", string(typ), "name", name)

	switch typ {
	case 'S': // Close prepared statement (extended protocol — silent on nonexistent)
		h.psc.RemovePreparedStatement(conn.ConnectionID(), name)
		return nil

	case 'P': // Close portal
		state := h.getConnectionState(conn)
		state.DeletePortalInfo(name)
		return nil

	case 'D': // Deallocate prepared statement (simple protocol — errors on nonexistent)
		if h.psc.GetPreparedStatementInfo(conn.ConnectionID(), name) == nil {
			return mterrors.NewInvalidPreparedStatementError(name)
		}
		h.psc.RemovePreparedStatement(conn.ConnectionID(), name)
		return nil

	case 'A': // Deallocate all prepared statements (simple protocol)
		h.psc.RemoveConnection(conn.ConnectionID())
		return nil

	default:
		return fmt.Errorf("invalid close type: %c (expected 'S', 'P', 'D', or 'A')", typ)
	}
}

// HandleSync processes a Sync message ('S').
func (h *MultiGatewayHandler) HandleSync(ctx context.Context, conn *server.Conn) error {
	h.logger.DebugContext(ctx, "sync")
	return nil
}

// ConnectionClosed is called when a client connection is closed.
// It releases all reserved connections (rolling back transactions, aborting COPYs)
// and removes prepared statement state.
func (h *MultiGatewayHandler) ConnectionClosed(conn *server.Conn) {
	// Release reserved connections if connection state exists.
	connState := conn.GetConnectionState()
	if connState != nil {
		state, ok := connState.(*MultiGatewayConnectionState)
		if ok && state != nil {
			// Unsubscribe from all LISTEN channels on disconnect.
			if state.NotifCh != nil {
				h.notifMgr.UnsubscribeAll(state.NotifCh)
				state.NotifCh = nil
				state.ClearListenChannels()
			}
			if state.AsyncNotifCh != nil {
				conn.StopAsyncNotifications()
				state.AsyncNotifCh = nil
			}
		}
		if ok && state != nil && len(state.ShardStates) > 0 {
			// Release all reserved connections regardless of reason (transaction, COPY, portal).
			// Add a timeout to bound cleanup duration — conn.Context() is still valid here
			// (cancelled after ConnectionClosed returns) but we don't want cleanup to hang.
			ctx, cancel := context.WithTimeout(conn.Context(), 5*time.Second)
			defer cancel()
			h.logger.DebugContext(ctx, "releasing reserved connections on client disconnect",
				"connection_id", conn.ConnectionID(),
				"shard_states", len(state.ShardStates))
			if err := h.executor.ReleaseAll(ctx, conn, state); err != nil {
				h.logger.ErrorContext(ctx, "failed to release connections on client disconnect",
					"connection_id", conn.ConnectionID(),
					"error", err)
			}
		}
	}

	// Always clean up prepared statements for this connection.
	h.psc.RemoveConnection(conn.ConnectionID())
}

// recordQueryCompletion records all three metrics and emits a structured
// query log entry. Centralises the instrumentation logic shared by
// HandleQuery and HandleExecute.
func (h *MultiGatewayHandler) recordQueryCompletion(
	ctx context.Context,
	conn *server.Conn,
	operationName string,
	queryProtocol string,
	parseDuration time.Duration,
	execDuration time.Duration,
	totalDuration time.Duration,
	rowCount int64,
	result *ExecuteResult,
	err error,
) {
	dbNamespace := conn.Database()

	status := QueryStatusOK
	errorType := ""
	if err != nil {
		status = QueryStatusError
		errorType = mterrors.ExtractSQLSTATE(err)
		h.metrics.queryErrors.Add(ctx, errorType, mterrors.ClassifyErrorSource(err), dbNamespace, operationName)
	}

	h.metrics.queryDuration.Record(ctx, totalDuration.Seconds(), dbNamespace, operationName, queryProtocol, errorType, status)
	h.metrics.rowsReturned.Record(ctx, float64(rowCount), dbNamespace, operationName)

	var tablesUsed []string
	var planType string
	var planTime time.Duration
	if result != nil {
		tablesUsed = result.TablesUsed
		planType = result.PlanType
		planTime = result.PlanTime
	}

	for _, table := range tablesUsed {
		h.metrics.tableQueries.Add(ctx, dbNamespace, table, operationName)
	}

	// Enrich the active span with plan metadata.
	setSpanPlanAttributes(ctx, planType, tablesUsed)

	emitQueryLog(ctx, h.logger, queryLogEntry{
		User:          conn.User(),
		Database:      dbNamespace,
		OperationName: operationName,
		Protocol:      queryProtocol,
		TotalDuration: totalDuration,
		ParseDuration: parseDuration,
		PlanDuration:  planTime,
		ExecDuration:  execDuration,
		RowCount:      rowCount,
		PlanType:      planType,
		TablesUsed:    tablesUsed,
		Error:         err,
		SQLSTATE:      errorType,
		ErrorSource:   mterrors.ClassifyErrorSource(err),
	}, h.slowThreshold)
}

// Ensure MultiGatewayHandler implements server.Handler interface.
var _ server.Handler = (*MultiGatewayHandler)(nil)

// SetNotificationManager sets the notification manager for LISTEN/NOTIFY support.
func (h *MultiGatewayHandler) SetNotificationManager(mgr NotificationManager) {
	h.notifMgr = mgr
}

// ensureNotifCh creates the notification channel for a connection if needed.
func (h *MultiGatewayHandler) ensureNotifCh(state *MultiGatewayConnectionState) chan *sqltypes.Notification {
	if state.NotifCh == nil {
		state.NotifCh = make(chan *sqltypes.Notification, 256)
	}
	return state.NotifCh
}

// syncListenSubscriptions subscribes/unsubscribes based on state changes.
// It also enables async notification delivery on the server.Conn when the
// first LISTEN is registered, and manages the notification forwarding goroutine.
func (h *MultiGatewayHandler) syncListenSubscriptions(
	ctx context.Context,
	conn *server.Conn,
	state *MultiGatewayConnectionState,
	subscribes []string,
	unsubscribes []string,
	unsubscribeAll bool,
) {
	notifCh := h.ensureNotifCh(state)

	if unsubscribeAll {
		h.notifMgr.UnsubscribeAll(notifCh)
	}

	for _, ch := range unsubscribes {
		h.notifMgr.Unsubscribe(ch, notifCh)
	}

	for _, ch := range subscribes {
		h.notifMgr.Subscribe(ch, notifCh)
	}

	// Start async notification pusher if we have listen channels and no pusher yet.
	listenCount := len(state.GetListenChannels())
	if listenCount > 0 && state.AsyncNotifCh == nil {
		asyncCh := conn.EnableAsyncNotifications(ctx)
		state.AsyncNotifCh = asyncCh
		// Start forwarding goroutine: notifCh -> asyncCh
		go h.forwardNotifications(ctx, notifCh, asyncCh)
	}

	// Stop async pusher if no more listen channels.
	if listenCount == 0 && state.AsyncNotifCh != nil {
		conn.StopAsyncNotifications()
		state.AsyncNotifCh = nil
	}
}

// forwardNotifications reads from the handler notifCh and forwards to the
// server.Conn async pusher channel.
func (h *MultiGatewayHandler) forwardNotifications(
	ctx context.Context,
	notifCh chan *sqltypes.Notification,
	asyncCh chan<- *sqltypes.Notification,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case notif, ok := <-notifCh:
			if !ok {
				return
			}
			select {
			case asyncCh <- notif:
			default:
				h.logger.WarnContext(ctx, "async notification channel full, dropping notification",
					"channel", notif.Channel)
			}
		}
	}
}

// flushNotifications delivers any pending notifications to the client.
// Called after each query completes (after ReadyForQuery would be sent).
func (h *MultiGatewayHandler) flushNotifications(conn *server.Conn, state *MultiGatewayConnectionState) {
	if state.NotifCh == nil {
		return
	}
	for {
		select {
		case notif := <-state.NotifCh:
			if notif == nil {
				return
			}
			if err := conn.WriteNotificationResponse(notif.PID, notif.Channel, notif.Payload); err != nil {
				h.logger.Error("failed to write notification", "error", err,
					"channel", notif.Channel)
			}
		default:
			// No more pending notifications
			return
		}
	}
}

// handleListenStateChanges syncs PubSubListener subscriptions after statement execution.
func (h *MultiGatewayHandler) handleListenStateChanges(
	ctx context.Context,
	conn *server.Conn,
	state *MultiGatewayConnectionState,
	stmt ast.Stmt,
) {
	// Use the connection-scoped context for subscription management.
	// The caller's ctx may be query-scoped (with statement timeout) and would
	// cancel the long-lived forwardNotifications goroutine after the query completes.
	connCtx := conn.Context()

	switch stmt.NodeTag() {
	case ast.T_ListenStmt:
		listenStmt := stmt.(*ast.ListenStmt)
		if !conn.IsInTransaction() {
			// Autocommit LISTEN — subscribe immediately.
			// Truncate to NAMEDATALEN-1 (63 chars) to match PG behavior.
			ch := listenStmt.Conditionname
			if len(ch) > 63 {
				ch = ch[:63]
			}
			h.syncListenSubscriptions(connCtx, conn, state, []string{ch}, nil, false)
		}

	case ast.T_UnlistenStmt:
		unlistenStmt := stmt.(*ast.UnlistenStmt)
		if !conn.IsInTransaction() {
			if unlistenStmt.Conditionname == "*" || unlistenStmt.Conditionname == "" {
				h.syncListenSubscriptions(connCtx, conn, state, nil, nil, true)
			} else {
				ch := unlistenStmt.Conditionname
				if len(ch) > 63 {
					ch = ch[:63]
				}
				h.syncListenSubscriptions(connCtx, conn, state, nil, []string{ch}, false)
			}
		}

	case ast.T_TransactionStmt:
		txnStmt := stmt.(*ast.TransactionStmt)
		switch txnStmt.Kind {
		case ast.TRANS_STMT_COMMIT:
			if state.HasPendingListens() {
				subs, unsubs, unsubAll := state.CommitPendingListens()
				h.syncListenSubscriptions(connCtx, conn, state, subs, unsubs, unsubAll)
			}
		case ast.TRANS_STMT_ROLLBACK:
			state.DiscardPendingListens()
		}
	}
}
