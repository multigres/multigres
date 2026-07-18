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
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/callerid"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/replparser"
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
	StreamExecute(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState, queryStr string, astStmt ast.Stmt, callback func(ctx context.Context, result *sqltypes.Result) error) (*ExecuteResult, error)

	// PortalStreamExecute is used to execute a Portal that was previously created.
	// includeDescribe asks the execution layer to fold a portal Describe('P')
	// into the same backend round trip as Execute (libpq pipelines the two);
	// when true, RowDescription rides back on the streaming callback's
	// first Fields-bearing chunk.
	PortalStreamExecute(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState, portalInfo *preparedstatement.PortalInfo, maxRows int32, includeDescribe bool, callback func(ctx context.Context, result *sqltypes.Result) error) (*ExecuteResult, error)

	// Describe returns metadata about a prepared statement or portal.
	// The options should contain PreparedStatement or Portal information and the reserved connection ID.
	Describe(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState, portalInfo *preparedstatement.PortalInfo, preparedStatementInfo *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error)

	// EagerParseInTransaction sends a backend Parse for PREPARE/Parse issued inside
	// an explicit transaction, so PostgreSQL acquires relation locks at prepare time.
	EagerParseInTransaction(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState, queryStr string, paramTypes []uint32) error

	// ReleaseAll releases all reserved connections, regardless of reservation reason.
	// For transaction-reserved connections, a ROLLBACK is sent first.
	// For COPY-reserved connections, the COPY is aborted.
	// Any remaining reserved connections are force-cleared.
	// Used for connection cleanup when a client disconnects.
	ReleaseAll(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState) error
}

// MultigatewayHandler implements the pgprotocol Handler interface for multigateway.
// It routes PostgreSQL protocol queries to the appropriate multipooler instances.
type MultigatewayHandler struct {
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

	// normalQueryLogSampleRate controls 1/N sampling for normal-path query
	// logs. 0 disables sampling (handler level alone governs emission); 1
	// emits every normal query; N>1 emits every Nth. Normal queries always
	// log at DEBUG, so the default INFO-level handler drops them entirely.
	normalQueryLogSampleRate uint64
	// normalQueryLogSamplingCursor is the 1/N modulo cursor. It is only
	// touched when sampleRate > 1; emission counts are exposed via the
	// queryLogEmits OTel counter on h.metrics.
	normalQueryLogSamplingCursor atomic.Uint64
}

// NewMultigatewayHandler creates a new PostgreSQL protocol handler.
func NewMultigatewayHandler(executor Executor, logger *slog.Logger, statementTimeout time.Duration) *MultigatewayHandler {
	metrics, err := NewHandlerMetrics()
	if err != nil {
		logger.Warn("failed to initialise some handler metrics", "error", err)
	}
	return &MultigatewayHandler{
		executor:         executor,
		logger:           logger.With("component", "multigateway_handler"),
		psc:              preparedstatement.NewConsolidator(),
		statementTimeout: statementTimeout,
		metrics:          metrics,
		slowThreshold:    constants.DefaultSlowQueryThreshold,
		notifMgr:         DefaultNotificationManager(),
	}
}

// SetNormalQueryLogSampleRate sets the 1/N sampling rate for normal-path
// query logs. 0 disables sampling (handler level alone governs); 1 emits
// every query; N>1 emits every Nth. Normal queries always log at DEBUG.
// Must be called before connections are accepted.
func (h *MultigatewayHandler) SetNormalQueryLogSampleRate(rate uint64) {
	h.normalQueryLogSampleRate = rate
}

// SetTargetReplica configures whether connections accepted by this handler
// target replicas. Must be called before connections are accepted.
func (h *MultigatewayHandler) SetTargetReplica(target bool) {
	h.targetReplica = target
}

// SetQueryRegistry attaches a per-query-shape registry to the handler.
// When set, the handler emits a `query.fingerprint` label on query metrics
// and records aggregate stats for queries in the registry's tracked set.
// A nil registry disables per-query tracking (all other metrics still work).
func (h *MultigatewayHandler) SetQueryRegistry(r *queryregistry.Registry) {
	h.queryRegistry = r
}

// QueryRegistry returns the attached query registry, or nil if none is set.
// Exposed so the /debug/queries HTTP handler can enumerate tracked fingerprints.
func (h *MultigatewayHandler) QueryRegistry() *queryregistry.Registry {
	return h.queryRegistry
}

// Consolidator returns the prepared statement consolidator.
func (h *MultigatewayHandler) Consolidator() *preparedstatement.Consolidator {
	return h.psc
}

// GetPreparedStatementInfo returns metadata for a SQL-level prepared
// statement registered under the given user-visible name on connID.
func (h *MultigatewayHandler) GetPreparedStatementInfo(connID uint32, name string) *preparedstatement.PreparedStatementInfo {
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
func (h *MultigatewayHandler) statementTimeoutCtx(ctx context.Context, state *MultigatewayConnectionState, query ast.Stmt) (context.Context, context.CancelFunc) {
	timeout := ResolveStatementTimeout(
		ParseStatementTimeoutDirective(query),
		state.GetStatementTimeout(),
	)
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

// callerContext enriches ctx with the client's identity so it reaches the
// multipooler: a typed CallerID for the queryservice request field and
// OpenTelemetry baggage for observability propagation. The identity is the
// authenticated database user and the client's application_name.
//
// application_name is read from the merged session view rather than the startup
// handshake, so a mid-session SET application_name is reflected, matching what
// the client sees via SHOW application_name and pg_stat_activity.
func (h *MultigatewayHandler) callerContext(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState) context.Context {
	appName := state.GetSessionSettings()["application_name"]
	return callerid.NewContext(ctx, callerid.New(conn.User(), appName))
}

// standardConformingStringsFor returns the session's effective
// standard_conforming_strings. It defaults to true — the PostgreSQL default and
// the value a fresh backend session uses — when the client has not changed it or
// stored an unparsable value.
//
// The value is read from the merged session view, not just SessionSettings, so a
// value supplied in the startup handshake is honored even without a later SET.
// standard_conforming_strings is a USERSET GUC a client may pass in the startup
// packet; reading only SET overrides would miss that and mis-lex the query.
func standardConformingStringsFor(st *MultigatewayConnectionState) bool {
	if v, ok := st.GetSessionSettings()["standard_conforming_strings"]; ok {
		if b, parsed := sqltypes.ParseBool(v); parsed {
			return b
		}
	}
	return true
}

// HandleQuery processes a simple query protocol message ('Q').
// Routes the query to an appropriate multipooler instance and streams results back.
func (h *MultigatewayHandler) HandleQuery(ctx context.Context, conn *server.Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	queryStart := time.Now()
	h.logger.DebugContext(ctx, "handling query", "query", queryStr, "user", conn.User(), "database", conn.Database())
	st := h.getConnectionState(conn)
	ctx = h.callerContext(ctx, conn, st)

	if conn.ReplicationMode() == server.ReplicationLogical && replparser.IsReplicationCommand(queryStr) {
		if handled, err := h.handleReplicationCommand(ctx, conn, queryStr, queryStart); handled {
			return err
		}
		// SHOW falls through to the regular SQL path below.
	}

	parseStart := time.Now()
	// Lex with the session's standard_conforming_strings so the client's string
	// literals are tokenized the way its session would. With scs=off an ordinary
	// '...' literal processes backslash escapes; parsing with the default scs=on
	// would mis-lex the query (e.g. 'a\\b\'cd') and reconstruct malformed SQL.
	asts, err := parser.ParseSQLWithStandardConformingStrings(queryStr, standardConformingStringsFor(st))
	parseDuration := time.Since(parseStart)
	if err != nil {
		// ParseSQL only does syntactic parsing, so any error here is a parse-stage
		// error. Surface it as the diagnostic PostgreSQL would send (the parser
		// stays mterrors-free): 42601 unless the parser named a SQLSTATE of its
		// own, carrying the cursor position for the ErrorResponse "P" field.
		var se *parser.ParseSyntaxError
		if errors.As(err, &se) {
			diag := mterrors.NewParseErrorAt(se.Message, se.CursorPosition, se.SQLState)
			diag.Hint = se.Hint
			err = diag
		} else {
			err = mterrors.NewParseError(err.Error())
		}
		h.recordQueryCompletion(ctx, conn, "UNKNOWN", "simple", parseDuration, 0, time.Since(queryStart), 0, nil, err)
		return err
	}

	// Handle empty query (e.g., just a semicolon or whitespace).
	// Call callback with nil to signal empty query response.
	if len(asts) == 0 {
		return callback(ctx, nil)
	}

	// Fold gateway-provided functions (e.g. multigres.version()) into constants
	// in the statements we just parsed — reusing the parse rather than re-parsing.
	// The batch execution paths re-render each statement via SqlString, so the
	// rewritten AST flows through; only the single-statement path below sends
	// queryStr, so refresh it from the rewritten AST when a fold happened.
	if containsGatewayFunction(queryStr) && foldGatewayFunctionsInStatements(asts) {
		queryStr = renderStatements(asts)
	}

	// TODO: For multi-statement batches, this only captures the first statement's
	// operation name. Consider recording per-statement metrics or using "MULTI" as
	// the operation name when len(asts) > 1.
	operationName := asts[0].StatementType()
	ctx, span := startQuerySpan(ctx, operationName, "simple", conn.Database(), conn.User())
	defer span.End()

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
func (h *MultigatewayHandler) startsWithAbortRecovery(asts []ast.Stmt) bool {
	return len(asts) > 0 && ast.IsAllowedInAbortedTransaction(asts[0])
}

// IdleSessionTimeout exposes the effective idle_session_timeout to the pgwire
// protocol loop. The loop enforces it only while waiting for the next client
// message in an idle transaction state.
func (h *MultigatewayHandler) IdleSessionTimeout(conn *server.Conn) time.Duration {
	return h.getConnectionState(conn).GetIdleSessionTimeout()
}

// getConnectionState retrieves and typecasts the connection state for this handler.
// Initializes a new state if it doesn't exist.
func (h *MultigatewayHandler) getConnectionState(conn *server.Conn) *MultigatewayConnectionState {
	state := conn.GetConnectionState()
	if state == nil {
		newState := NewMultigatewayConnectionState()
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

		idleDefault := time.Duration(0)
		if v, ok := newState.StartupParams["idle_session_timeout"]; ok {
			if d, err := ParsePostgresInterval("idle_session_timeout", v); err == nil {
				idleDefault = d
			} else {
				h.logger.Warn("ignoring invalid idle_session_timeout startup parameter, using default",
					"value", v, "default", idleDefault, "error", err)
			}
			delete(newState.StartupParams, "idle_session_timeout")
		}
		newState.InitIdleSessionTimeout(idleDefault)

		// Safety net: strip every registered gateway-managed variable from the
		// startup params so it is never forwarded to the backend (where it would
		// set the real GUC on the pooled connection and leak across clients). The
		// two handled above are already deleted; this covers any GMV added to the
		// registry without a typed init here — its value won't be applied to
		// gateway state (SHOW would show the default until a typed init is added),
		// but it can never leak to the backend.
		for key := range newState.StartupParams {
			if IsGatewayManagedVariable(key) {
				delete(newState.StartupParams, key)
			}
		}
		newState.targetReplica = h.targetReplica

		newState.SubSync = &handlerSubSync{
			notifMgr:       h.notifMgr,
			logger:         h.logger,
			onNotifDropped: h.onNotifDropped,
		}

		conn.SetConnectionState(newState)
		return newState
	}
	return state.(*MultigatewayConnectionState)
}

// HandleParse processes a Parse message ('P') for the extended query protocol.
// Creates and stores a prepared statement.
func (h *MultigatewayHandler) HandleParse(ctx context.Context, conn *server.Conn, name, queryStr string, paramTypes []uint32) error {
	h.logger.DebugContext(ctx, "parse", "name", name, "query", queryStr, "param_count", len(paramTypes))

	// Fold gateway-provided functions (e.g. multigres.version()) into constants
	// before storing or eagerly parsing the prepared statement, so the folded
	// text is what Describe and Execute later forward to the backend. A no-op for
	// queries that don't use one.
	queryStr = foldGatewayFunctions(queryStr)

	// PostgreSQL sends Parse/PREPARE to the backend immediately inside an explicit
	// transaction. That parse takes transaction-scoped AccessShareLocks and raises
	// relation/semantic errors at prepare time. Preserve the lazy path outside a
	// transaction, where those locks would be released before the next statement.
	if conn.TxnStatus() == protocol.TxnStatusInBlock {
		if err := h.executor.EagerParseInTransaction(ctx, conn, h.getConnectionState(conn), queryStr, paramTypes); err != nil {
			if conn.TxnStatus() == protocol.TxnStatusInBlock {
				conn.SetTxnStatus(protocol.TxnStatusFailed)
			}
			return err
		}
	}

	// An empty or comment-only query string parses to zero statements;
	// AddPreparedStatement produces an empty PreparedStatementInfo for it, which
	// the gateway answers with EmptyQueryResponse — matching PostgreSQL.
	_, err := h.psc.AddPreparedStatement(conn.ConnectionID(), name, queryStr, paramTypes)
	return err
}

// HandleBind processes a Bind message ('B') for the extended query protocol.
// Creates and stores a portal for the specified prepared statement with bound parameters.
func (h *MultigatewayHandler) HandleBind(ctx context.Context, conn *server.Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	h.logger.DebugContext(ctx, "bind", "portal", portalName, "statement", stmtName, "param_count", len(params))

	// Get the prepared statement to verify it exists.
	psi := h.psc.GetPreparedStatementInfo(conn.ConnectionID(), stmtName)
	if psi == nil {
		return mterrors.NewInvalidPreparedStatementError(stmtName)
	}

	// For an empty / comment-only statement the gateway knows the parameter
	// count authoritatively (there is no query body in which postgres could
	// infer extra parameters), and the backend never sees the Bind because
	// Execute is short-circuited. Validate the supplied parameter count here so
	// a mismatch surfaces the same 08P01 protocol_violation PostgreSQL raises,
	// rather than silently succeeding. Non-empty statements are validated by the
	// backend, which may infer parameters beyond those declared at Parse.
	if psi.IsEmpty() {
		if declared := len(psi.GetParamTypes()); len(params) != declared {
			return mterrors.NewPgError("ERROR", mterrors.PgSSProtocolViolation,
				fmt.Sprintf("bind message supplies %d parameters, but prepared statement %q requires %d",
					len(params), stmtName, declared), "")
		}
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
func (h *MultigatewayHandler) HandleExecute(ctx context.Context, conn *server.Conn, portalName string, maxRows int32, includeDescribe bool, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	queryStart := time.Now()
	h.logger.DebugContext(ctx, "execute", "portal", portalName, "max_rows", maxRows)

	// Get the connection state.
	state := h.getConnectionState(conn)
	ctx = h.callerContext(ctx, conn, state)

	// Get the portal.
	portalInfo := state.GetPortalInfo(portalName)
	if portalInfo == nil {
		portalErr := mterrors.NewInvalidPortalError(portalName)
		// Record before span creation since we don't have an operation name yet.
		h.recordQueryCompletion(ctx, conn, "UNKNOWN", "extended", 0, 0, time.Since(queryStart), 0, nil, portalErr)
		return portalErr
	}

	// Empty / comment-only prepared statement: answer Execute with
	// EmptyQueryResponse, mirroring the simple-query path. Short-circuit here so
	// the nil AST never reaches the planner (resolvePortalPlan/isCacheable would
	// dereference it). This also bypasses the aborted-transaction gate, matching
	// PostgreSQL, which returns EmptyQueryResponse for an empty query regardless
	// of transaction state.
	if portalInfo.IsEmpty() {
		err := callback(ctx, nil)
		h.recordQueryCompletion(ctx, conn, "EMPTY", "extended", 0, 0, time.Since(queryStart), 0, nil, err)
		return err
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
	result, err := h.executor.PortalStreamExecute(ctx, conn, state, portalInfo, maxRows, includeDescribe, countingCallback)
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
func (h *MultigatewayHandler) HandleDescribe(ctx context.Context, conn *server.Conn, typ byte, name string) (*query.StatementDescription, error) {
	h.logger.DebugContext(ctx, "describe", "type", string(typ), "name", name)

	// Get the connection state.
	state := h.getConnectionState(conn)
	ctx = h.callerContext(ctx, conn, state)

	switch typ {
	case 'S': // Describe prepared statement
		stmt := h.psc.GetPreparedStatementInfo(conn.ConnectionID(), name)
		if stmt == nil {
			return nil, mterrors.NewInvalidPreparedStatementError(name)
		}
		if stmt.IsEmpty() {
			// Empty statement: ParameterDescription(0) + NoData, no backend call.
			return &query.StatementDescription{}, nil
		}

		// Call executor to get description from multipooler
		return h.executor.Describe(ctx, conn, state, nil, stmt)

	case 'P': // Describe portal
		portalInfo := state.GetPortalInfo(name)
		if portalInfo == nil {
			return nil, mterrors.NewInvalidPortalError(name)
		}
		if portalInfo.IsEmpty() {
			// Empty portal: NoData, no backend call.
			return &query.StatementDescription{}, nil
		}

		// Call executor to get description from multipooler
		return h.executor.Describe(ctx, conn, state, portalInfo, nil)

	default:
		return nil, fmt.Errorf("invalid describe type: %c (expected 'S' or 'P')", typ)
	}
}

// HandleClose processes a Close message ('C').
// Closes either a prepared statement ('S') or a portal ('P').
func (h *MultigatewayHandler) HandleClose(ctx context.Context, conn *server.Conn, typ byte, name string) error {
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
func (h *MultigatewayHandler) HandleSync(ctx context.Context, conn *server.Conn) error {
	h.logger.DebugContext(ctx, "sync")
	return nil
}

// ConnectionClosed is called when a client connection is closed.
// It releases all reserved connections (rolling back transactions, aborting COPYs)
// and removes prepared statement state.
func (h *MultigatewayHandler) ConnectionClosed(conn *server.Conn) {
	// Release reserved connections if connection state exists.
	connState := conn.GetConnectionState()
	if connState != nil {
		state, ok := connState.(*MultigatewayConnectionState)
		if ok && state != nil {
			// Unsubscribe from all LISTEN channels on disconnect.
			if state.NotifCh != nil {
				h.notifMgr.UnsubscribeAll(state.NotifCh)
				state.NotifCh = nil
				state.ClearListenChannels()
			}
			if subSync, ok := state.SubSync.(*handlerSubSync); ok {
				subSync.stopForwarding()
			}
			state.DrainPendingNotifications()
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
			ctx = h.callerContext(ctx, conn, state)
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

// classifyErrorSource calls mterrors.ClassifyErrorSource only when err is
// non-nil. The success path is overwhelmingly common, so short-circuiting
// here avoids a per-query function call + interface check.
func classifyErrorSource(err error) string {
	if err == nil {
		return ""
	}
	return mterrors.ClassifyErrorSource(err)
}

// recordQueryCompletion records all three metrics and emits a structured
// query log entry. Centralises the instrumentation logic shared by
// HandleQuery and HandleExecute.
func (h *MultigatewayHandler) recordQueryCompletion(
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
	errorSource := ""
	if err != nil {
		status = QueryStatusError
		errorType = mterrors.ExtractSQLSTATE(err)
		errorSource = classifyErrorSource(err)
		h.metrics.queryErrors.Add(ctx, errorType, errorSource, dbNamespace, operationName, fingerprintLabel)
	}

	h.metrics.queryDuration.Record(ctx, totalDuration.Seconds(), dbNamespace, operationName, queryProtocol, errorType, status, fingerprintLabel)
	h.metrics.rowsReturned.Record(ctx, float64(rowCount), dbNamespace, operationName, fingerprintLabel)

	// Phase-latency breakdown. Each phase is recorded only when it actually ran
	// for this call: parse is 0 on the extended-protocol path (parsing happened
	// earlier), exec is 0 when an error short-circuited before execution, and
	// plan is 0 for non-planned/utility statements. Recording those zeros would
	// pollute the histograms, so guard on > 0.
	if parseDuration > 0 {
		h.metrics.parseDuration.Record(ctx, parseDuration.Seconds(), dbNamespace, operationName)
	}
	if planTime > 0 {
		h.metrics.planDuration.Record(ctx, planTime.Seconds(), dbNamespace, operationName, planType)
	}
	if execDuration > 0 {
		h.metrics.execDuration.Record(ctx, execDuration.Seconds(), dbNamespace, operationName)
	}

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
		ErrorSource:   errorSource,
	}, h.slowThreshold, h.normalQueryLogSampleRate, &h.normalQueryLogSamplingCursor, h.metrics.queryLogEmits)
}

// Ensure MultigatewayHandler implements server.Handler interface.
var _ server.Handler = (*MultigatewayHandler)(nil)

// SetNotificationManager sets the notification manager for LISTEN/NOTIFY support.
// The optional onDropped callback is invoked when a notification is dropped due to
// a full async delivery channel (for metrics recording).
func (h *MultigatewayHandler) SetNotificationManager(mgr NotificationManager, onDropped func(ctx context.Context)) {
	h.notifMgr = mgr
	h.onNotifDropped = onDropped
}

// flushNotifications delivers any pending notifications to the client.
// Called after each query completes (before ReadyForQuery is sent).
//
// Notifications flow through a pipeline: NotifCh → forwardNotifications → asyncCh.
// This method drains the asyncCh (the server.Conn's internal notification channel)
// with proper bufMu locking, avoiding races with the async pusher goroutine.
func (h *MultigatewayHandler) flushNotifications(conn *server.Conn, state *MultigatewayConnectionState) {
	for _, notif := range state.FlushReadyNotifications(state.AsyncNotifCh) {
		h.logger.Warn("async notification channel full, dropping buffered notification",
			"channel", notif.Channel)
		if h.onNotifDropped != nil {
			h.onNotifDropped(conn.Context())
		}
	}
	if state.AsyncNotifCh == nil {
		return
	}
	if err := conn.FlushPendingNotifications(); err != nil {
		h.logger.Error("failed to flush notifications", "error", err)
	}
}
