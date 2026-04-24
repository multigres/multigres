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
	"github.com/multigres/multigres/go/services/multigateway/handler/queryregistry"
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

	// CacheHit indicates whether the plan was served from the plan cache.
	CacheHit bool

	// NormalizedSQL is the query with literals replaced by $N placeholders.
	// Empty for non-cacheable statements (utility, DDL, transactions) that
	// don't go through normalization.
	NormalizedSQL string

	// Fingerprint is a stable 16-hex-char hash of NormalizedSQL, used to
	// aggregate per-query-shape metrics. Empty if NormalizedSQL is empty.
	Fingerprint string
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
	onNotifDropped   func(ctx context.Context) // called when async notification delivery drops
	queryRegistry    *queryregistry.Registry
	// targetReplica is set to true for the replica-port listener. When true,
	// new connection states target replicas so the planner routes queries there.
	targetReplica bool
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

// SetTargetReplica configures whether connections accepted by this handler
// target replicas. Must be called before connections are accepted.
func (h *MultiGatewayHandler) SetTargetReplica(target bool) {
	h.targetReplica = target
}

// SetQueryRegistry attaches a per-query-shape registry to the handler.
// When set, the handler emits a `query.fingerprint` label on query metrics
// and records aggregate stats for queries in the registry's tracked set.
// A nil registry disables per-query tracking (all other metrics still work).
func (h *MultiGatewayHandler) SetQueryRegistry(r *queryregistry.Registry) {
	h.queryRegistry = r
}

// QueryRegistry returns the attached query registry, or nil if none is set.
// Exposed so the /debug/queries HTTP handler can enumerate tracked fingerprints.
func (h *MultiGatewayHandler) QueryRegistry() *queryregistry.Registry {
	return h.queryRegistry
}

// Consolidator returns the prepared statement consolidator.
func (h *MultiGatewayHandler) Consolidator() *preparedstatement.Consolidator {
	return h.psc
}

// GetPreparedStatementInfo returns metadata for a SQL-level prepared
// statement registered under the given user-visible name on connID.
func (h *MultiGatewayHandler) GetPreparedStatementInfo(connID uint32, name string) *preparedstatement.PreparedStatementInfo {
	return h.psc.GetPreparedStatementInfo(connID, name)
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
	// first statement can recover from the aborted state. PostgreSQL allows
	// ROLLBACK, ROLLBACK TO SAVEPOINT, and COMMIT (converted to ROLLBACK) in
	// this state. Multi-statement batches are permitted as long as the first
	// statement is one of these (e.g., "ROLLBACK TO sp1; SELECT 1;").
	if conn.TxnStatus() == protocol.TxnStatusFailed {
		if !h.startsWithAbortRecovery(asts) {
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
	// Exception: sessions with temp table reservations iterate and plan each
	// statement individually so that DISCARD TEMP and other special statements
	// get proper primitives. The PG backend handles auto-commit natively on
	// the reserved connection.
	// Per-table metrics are emitted per-statement inside executeWithImplicitTransaction.
	// For multi-statement batches, per-table metrics, span attributes, and query log
	// enrichment are handled per-statement inside executeWithImplicitTransaction.
	// The batch-level recordQueryCompletion gets nil result (no plan type for a batch).
	var result *ExecuteResult
	if len(asts) > 1 {
		if st.HasTempTableReservation() {
			h.logger.DebugContext(ctx, "executing multi-statement batch on temp-table-reserved session",
				"statement_count", len(asts))
			var allTablesUsed []string
			for _, stmt := range asts {
				stmtCtx, cancel := h.statementTimeoutCtx(ctx, st, stmt)
				var stmtResult *ExecuteResult
				stmtResult, err = h.executor.StreamExecute(stmtCtx, conn, st, stmt.SqlString(), stmt, countingCallback)
				cancel()
				if stmtResult != nil {
					allTablesUsed = append(allTablesUsed, stmtResult.TablesUsed...)
				}
				if err != nil {
					break
				}
			}
			if len(allTablesUsed) > 0 {
				result = &ExecuteResult{TablesUsed: allTablesUsed}
			}
		} else {
			h.logger.DebugContext(ctx, "executing multi-statement batch with implicit transaction handling",
				"statement_count", len(asts),
				"already_in_transaction", conn.IsInTransaction())
			err = h.executeWithImplicitTransaction(ctx, conn, st, queryStr, asts, countingCallback)
		}
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

// startsWithAbortRecovery returns true if the first statement can recover
// from an aborted transaction. PostgreSQL allows ROLLBACK, ROLLBACK TO
// SAVEPOINT, and COMMIT in aborted state.
func (h *MultiGatewayHandler) startsWithAbortRecovery(asts []ast.Stmt) bool {
	return len(asts) > 0 && ast.IsAllowedInAbortedTransaction(asts[0])
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
		newState.targetReplica = h.targetReplica

		newState.SubSync = &handlerSubSync{
			notifMgr:       h.notifMgr,
			logger:         h.logger,
			onNotifDropped: h.onNotifDropped,
		}

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

	// Reject queries in aborted transaction state, except statements that can
	// recover: ROLLBACK, ROLLBACK TO SAVEPOINT, and COMMIT (converted to ROLLBACK).
	if conn.TxnStatus() == protocol.TxnStatusFailed {
		if !ast.IsAllowedInAbortedTransaction(portalInfo.AstStmt()) {
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

	var (
		tablesUsed    []string
		planType      string
		planTime      time.Duration
		normalizedSQL string
		fingerprint   string
	)
	if result != nil {
		tablesUsed = result.TablesUsed
		planType = result.PlanType
		planTime = result.PlanTime
		normalizedSQL = result.NormalizedSQL
		fingerprint = result.Fingerprint
	}

	// Resolve the fingerprint label: either the fingerprint itself (if tracked),
	// __other__ (tracked query space exists but this fingerprint didn't make the
	// cut), or __utility__ (non-cacheable statement with no fingerprint).
	fingerprintLabel := queryregistry.UtilityLabel
	if fingerprint != "" {
		fingerprintLabel = h.queryRegistry.Labelize(fingerprint)
	}

	status := QueryStatusOK
	errorType := ""
	if err != nil {
		status = QueryStatusError
		errorType = mterrors.ExtractSQLSTATE(err)
		h.metrics.queryErrors.Add(ctx, errorType, mterrors.ClassifyErrorSource(err), dbNamespace, operationName, fingerprintLabel)
	}

	h.metrics.queryDuration.Record(ctx, totalDuration.Seconds(), dbNamespace, operationName, queryProtocol, errorType, status, fingerprintLabel)
	h.metrics.rowsReturned.Record(ctx, float64(rowCount), dbNamespace, operationName, fingerprintLabel)

	// Feed the registry so popular fingerprints stay tracked, their stats roll
	// up for /debug/queries, and newly-popular queries get promoted.
	if fingerprint != "" {
		h.queryRegistry.Record(fingerprint, normalizedSQL, totalDuration, int(rowCount), err != nil)
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
// The optional onDropped callback is invoked when a notification is dropped due to
// a full async delivery channel (for metrics recording).
func (h *MultiGatewayHandler) SetNotificationManager(mgr NotificationManager, onDropped func(ctx context.Context)) {
	h.notifMgr = mgr
	h.onNotifDropped = onDropped
}

// flushNotifications delivers any pending notifications to the client.
// Called after each query completes (before ReadyForQuery is sent).
//
// Notifications flow through a pipeline: NotifCh → forwardNotifications → asyncCh.
// This method drains the asyncCh (the server.Conn's internal notification channel)
// with proper bufMu locking, avoiding races with the async pusher goroutine.
func (h *MultiGatewayHandler) flushNotifications(conn *server.Conn, state *MultiGatewayConnectionState) {
	if state.AsyncNotifCh == nil {
		return
	}
	if err := conn.FlushPendingNotifications(); err != nil {
		h.logger.Error("failed to flush notifications", "error", err)
	}
}
