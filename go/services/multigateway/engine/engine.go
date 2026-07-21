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

// Package engine contains the query execution primitives for multigateway.
// Primitives are the building blocks of query plans and handle routing,
// execution coordination, and result aggregation.
package engine

import (
	"context"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// PlanExecInfo carries the per-query connection-reservation signals a
// routing primitive derives at execution time and hands to IExecute, which
// folds them into the multipooler ReservationOptions. These signals are scoped
// to a single StreamExecute / PortalStreamExecute call, so they ride on the
// call rather than on per-connection state (they used to live as one-shot
// state.Pending* fields, which leaked single-query intent onto the connection).
// The zero value means "no special reservation". Primitives are plan-cached and
// shared across concurrent connections, so the intent must be a per-call value,
// never a field on the primitive.
type PlanExecInfo struct {
	// TempTable requests a reserved connection with ReasonTempTable. Set by
	// TempTableRoute for CREATE TEMP / SELECT INTO TEMP.
	TempTable bool

	// AdvisoryLock requests a reserved connection with ReasonSessionAdvisoryLock,
	// pinning the backend for the lock's lifetime. Set by AdvisoryLockRoute when
	// the statement acquires a session-level advisory lock.
	AdvisoryLock bool

	// RecheckAdvisoryLocks asks the multipooler to re-probe pg_locks after the
	// statement and unpin if none remain. Set by AdvisoryLockRoute for any
	// advisory-touching statement (acquire or release); keeps the probe off the
	// per-statement hot path.
	RecheckAdvisoryLocks bool

	// PinPortals lists cursor names to pin on the reserved backend's portal set
	// (ReasonPortal). Set by HoldCursorRoute for DECLARE ... WITH HOLD.
	PinPortals []string

	// ReleasePortals lists cursor names to unpin from the reserved backend's
	// portal set. Set by CloseCursorRoute (CLOSE / CLOSE ALL) and by
	// TransactionPrimitive when ROLLBACK TO drops cursors declared after a
	// savepoint.
	ReleasePortals []string

	// HasPostQuerySessionSettings indicates that PostQuerySessionSettings is an
	// authoritative snapshot of the backend session settings after this statement
	// succeeds. It is used by Route-first SELECT set_config(...) plans so the
	// multipooler can recycle/bookkeep the backend under the settings PostgreSQL
	// just applied, while the gateway's live state is still mutated only after the
	// route reports success. The bool is separate from the map so an intentional
	// empty post-state can be represented.
	HasPostQuerySessionSettings bool

	// PostQuerySessionSettings is the backend session-settings snapshot to record
	// after successful statement execution. It must not be applied before running
	// the statement.
	PostQuerySessionSettings map[string]string

	// Exchange is a per-execution channel for handing runtime-computed data from
	// one primitive in a Sequence to a later sibling (e.g. ValidateSetting →
	// ApplySessionState). Sequence creates one per execution and threads the same
	// pointer to every child, so a producer child's writes are visible to a
	// consumer child that received a copy of this PlanExecInfo. nil outside a
	// Sequence.
	Exchange *SequenceExchange
}

// SequenceExchange is the per-execution scratchpad threaded through
// PlanExecInfo.Exchange. It lets an earlier primitive in a Sequence hand
// runtime-computed data to a later sibling without abusing connection state
// (session-lifetime) or stashing per-execution values on the cached plan.
type SequenceExchange struct {
	// ReportedSettings holds GUC_REPORT values a validating primitive captured
	// from set_config's canonical return, keyed by PostgreSQL's ParameterStatus
	// display name, for a trailing ApplySessionState to emit. nil until written.
	ReportedSettings map[string]string
}

// AddReportedSetting records a canonical GUC value under its ParameterStatus
// display name for a later sibling to emit.
func (e *SequenceExchange) AddReportedSetting(displayName, value string) {
	if e.ReportedSettings == nil {
		e.ReportedSettings = make(map[string]string)
	}
	e.ReportedSettings[displayName] = value
}

// IExecute is the execution interface that provides access to execution
// resources like ScatterConn. It's passed to primitives during execution,
// allowing them to execute queries without directly depending on concrete types.
//
// This interface helps testing Planner while allowing the underlying execution framework
// to be mocked. This interface is implemented by ScatterCon in production code.
type IExecute interface {
	// StreamExecute executes a query on the specified tablegroup and streams results.
	// This is the main execution method that primitives call to actually run queries.
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   conn: Database connection
	//   tableGroup: Target tablegroup for the query
	//   shard: Target shard (empty string for unsharded or any shard)
	//   sql: SQL query to execute
	//   executeSQLPreparedStatement: Optional SQL-level EXECUTE wrapper. When set,
	//     the multipooler resolves the prepared statement through its pooler-level
	//     consolidator and substitutes the resulting backend name into the wrapper
	//     before executing the query. Pass nil for queries that do not reference a
	//     gateway-managed prepared statement through SQL EXECUTE.
	//   state: Connection state containing session information and reserved connections
	//   info: Per-query reservation intent (temp-table / advisory-lock / portal
	//     pin-release signals) the calling primitive derived; folded into the
	//     multipooler ReservationOptions. Pass the zero value for plain routing.
	//   keepStructured: When true, opt out of opaque row passthrough so the
	//     multipooler returns structured Rows. A static plan-build-time property
	//     the calling primitive carries (see Route.KeepStructured); pass false
	//     for the default streaming path.
	//   callback: Function called for each result chunk
	// TODO: When we support sharded query serving, this method will need to take in
	// Routing parameters instead and figure out which all shards to send queries to.
	StreamExecute(
		ctx context.Context,
		conn *server.Conn,
		tableGroup string,
		shard string,
		sql string,
		executeSQLPreparedStatement *query.ExecuteSqlPreparedStatement,
		state *handler.MultigatewayConnectionState,
		info PlanExecInfo,
		keepStructured bool,
		callback func(context.Context, *sqltypes.Result) error,
	) error

	// PortalStreamExecute executes a portal (bound prepared statement) and streams results.
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   tableGroup: Target tablegroup for the query
	//   shard: Target shard (empty string for unsharded or any shard)
	//   conn: Database connection
	//   state: Connection state containing session information and reserved connections
	//   portalInfo: Portal information including bound parameters
	//   maxRows: Maximum number of rows to return (0 for unlimited)
	//   includeDescribe: when true, asks the multipooler to fold a portal
	//     Describe('P') into the same backend round trip as Execute (libpq
	//     pipelines the two). The portal RowDescription rides back through
	//     the streaming callback's Fields on the first chunk. When false,
	//     the Execute uses Bind+Execute+Sync as before.
	//   info: Per-query reservation intent, as in StreamExecute. Portal-path
	//     statements carry temp-table / advisory-lock signals (cursor pin/release
	//     only flow through StreamExecute); pass the zero value for plain routing.
	//   keepStructured: as in StreamExecute; pass false for the default path.
	//   callback: Function called for each result chunk
	PortalStreamExecute(
		ctx context.Context,
		tableGroup string,
		shard string,
		conn *server.Conn,
		state *handler.MultigatewayConnectionState,
		portalInfo *preparedstatement.PortalInfo,
		maxRows int32,
		includeDescribe bool,
		info PlanExecInfo,
		keepStructured bool,
		callback func(context.Context, *sqltypes.Result) error,
	) error

	// Describe returns metadata about a prepared statement or portal.
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   tableGroup: Target tablegroup for the query
	//   shard: Target shard (empty string for unsharded or any shard)
	//   conn: Database connection
	//   state: Connection state containing session information and reserved connections
	//   portalInfo: Portal information (nil if describing a prepared statement)
	//   preparedStatementInfo: Prepared statement information (nil if describing a portal)
	Describe(
		ctx context.Context,
		tableGroup string,
		shard string,
		conn *server.Conn,
		state *handler.MultigatewayConnectionState,
		portalInfo *preparedstatement.PortalInfo,
		preparedStatementInfo *preparedstatement.PreparedStatementInfo,
	) (*query.StatementDescription, error)

	// ConcludeTransaction concludes a transaction on reserved connections with COMMIT or ROLLBACK.
	// Iterates over all shard states, calling ConcludeTransaction on each reserved connection.
	// Returns the result of the COMMIT/ROLLBACK and clears shard state entries where the
	// connection was fully released (remainingReasons == 0).
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   conn: Client connection (for user/session info)
	//   state: Connection state containing reserved connections to conclude
	//   conclusion: COMMIT or ROLLBACK
	//   releasePortalNames: HOLD-cursor names to unpin on ROLLBACK — typically the
	//     cursors declared inside the rolled-back transaction block. Empty (and
	//     releaseAllPortals false) means "preserve every pin".
	//   releaseAllPortals: when true on ROLLBACK, drops every pin on the
	//     reserved connection (historical behavior). When false, only the
	//     names listed in releasePortalNames are released. Ignored on COMMIT.
	//   chain: when true, execute COMMIT/ROLLBACK AND CHAIN and keep the
	//     transaction reservation active on the same backend.
	//   callback: Function called with the result of the COMMIT/ROLLBACK
	ConcludeTransaction(
		ctx context.Context,
		conn *server.Conn,
		state *handler.MultigatewayConnectionState,
		conclusion multipoolerpb.TransactionConclusion,
		releasePortalNames []string,
		releaseAllPortals bool,
		chain bool,
		callback func(context.Context, *sqltypes.Result) error,
	) error

	// DiscardTempTables sends DISCARD TEMP on reserved connections with the temp table
	// reason set. Iterates over shard states and calls DiscardTempTables on each.
	// Clears shard state entries where the connection was fully released (remainingReasons == 0)
	// and keeps entries where the connection is still reserved for other reasons.
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   conn: Client connection (for user/session info)
	//   state: Connection state containing reserved connections to discard temp tables on
	//   callback: Function called with the result of the DISCARD command
	DiscardTempTables(
		ctx context.Context,
		conn *server.Conn,
		state *handler.MultigatewayConnectionState,
		callback func(context.Context, *sqltypes.Result) error,
	) error

	// ReleaseAllReservedConnections forcefully releases ALL reserved connections,
	// regardless of reservation reason. Iterates all shard states and calls
	// ReleaseReservedConnection on the multipooler for each one, then clears
	// local shard state. Used during client disconnect cleanup.
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   conn: Client connection (for user/session info)
	//   state: Connection state containing all reserved connections to release
	ReleaseAllReservedConnections(
		ctx context.Context,
		conn *server.Conn,
		state *handler.MultigatewayConnectionState,
	) error

	// --- COPY FROM STDIN methods (called by CopyStatement primitive) ---
	// These methods follow the same pattern as StreamExecute: they take tableGroup/shard
	// and manage reserved connection state internally via state.ShardStates.

	// CopyInitiate initiates a COPY FROM STDIN operation using bidirectional streaming.
	// Stores reserved connection info in state.ShardStates for the given tableGroup/shard.
	// Returns: format, columnFormats, error
	CopyInitiate(
		ctx context.Context,
		conn *server.Conn,
		tableGroup string,
		shard string,
		queryStr string,
		state *handler.MultigatewayConnectionState,
		callback func(ctx context.Context, result *sqltypes.Result) error,
	) (format int16, columnFormats []int16, err error)

	// CopySendData sends a chunk of COPY data via bidirectional stream.
	// Looks up reserved connection from state.ShardStates based on tableGroup/shard.
	CopySendData(
		ctx context.Context,
		conn *server.Conn,
		tableGroup string,
		shard string,
		state *handler.MultigatewayConnectionState,
		data []byte,
	) error

	// CopyFinalize sends the final chunk and CopyDone via bidirectional stream.
	// Looks up reserved connection from state.ShardStates based on tableGroup/shard.
	CopyFinalize(
		ctx context.Context,
		conn *server.Conn,
		tableGroup string,
		shard string,
		state *handler.MultigatewayConnectionState,
		finalData []byte,
		callback func(ctx context.Context, result *sqltypes.Result) error,
	) error

	// CopyAbort aborts the COPY operation via bidirectional stream.
	// Looks up reserved connection from state.ShardStates based on tableGroup/shard.
	CopyAbort(
		ctx context.Context,
		conn *server.Conn,
		tableGroup string,
		shard string,
		state *handler.MultigatewayConnectionState,
	) error

	// CopyOutInitiate initiates a COPY ... TO STDOUT operation. Returns
	// format and column formats from CopyOutResponse plus any NoticeResponse
	// diagnostics received before CopyOutResponse. Stores the reserved
	// connection state in state.ShardStates so CopyOutStream can find it.
	CopyOutInitiate(
		ctx context.Context,
		conn *server.Conn,
		tableGroup string,
		shard string,
		queryStr string,
		state *handler.MultigatewayConnectionState,
	) (format int16, columnFormats []int16, notices []*mterrors.PgDiagnostic, err error)

	// CopyOutStream drives the COPY ... TO STDOUT data stream, invoking
	// onMessage for each CopyData chunk / NoticeResponse pumped by the
	// multipooler. Returns the final Result with CommandTag, RowsAffected,
	// and any trailing notices in result.Notices.
	CopyOutStream(
		ctx context.Context,
		conn *server.Conn,
		tableGroup string,
		shard string,
		state *handler.MultigatewayConnectionState,
		onMessage func(pgClient.CopyOutMessage) error,
	) (*sqltypes.Result, error)
}

// Primitive is the building block of the query execution plan.
// Each primitive represents an operation in the query execution tree
// (e.g., route to tablegroup, join, aggregate, etc.).
//
// Primitives receive an IExecute interface during execution, which provides
// access to execution resources without tight coupling.
type Primitive interface {
	// StreamExecute executes the primitive and streams results via callback.
	// The IExecute interface provides access to execution resources.
	// bindVars contains literal values extracted during query normalization;
	// it is nil for non-cached execution paths. Primitives that need it
	// (e.g., Route) use bindVars to reconstruct the final SQL.
	//
	// info carries the plan's PlanExecInfo (planner-computed reservation
	// directives). Routing primitives forward it to IExecute; auxiliary
	// primitives (e.g. ResolveTrackSetConfig's set_config apply) and composite
	// primitives that wrap a non-routing step pass the zero value on their own
	// IExecute calls. Cursor/rollback primitives augment it with the
	// runtime-computed portal release set.
	StreamExecute(
		ctx context.Context,
		exec IExecute,
		conn *server.Conn,
		state *handler.MultigatewayConnectionState,
		bindVars []*ast.A_Const,
		info PlanExecInfo,
		callback func(context.Context, *sqltypes.Result) error,
	) error

	// PortalStreamExecute executes the primitive on the extended query
	// protocol path, where parameter values arrive as wire-format Bind
	// values inside portalInfo rather than as ast.A_Const literals.
	//
	// Primitives that forward the user's SQL to the backend (Route) reissue
	// the portal so PG receives the original query text plus binds. Primitives
	// whose effects are local to the gateway (ApplySessionState, transaction
	// management, LISTEN, etc.) do not consume binds and may simply delegate
	// to StreamExecute with nil bindVars. Composite primitives (Sequence)
	// dispatch to the right method on each child.
	//
	// Centralizing the dispatch on the primitive — rather than having the
	// executor introspect plan shapes — keeps the executor generic and lets
	// each primitive own the question "how do I run under the portal path".
	PortalStreamExecute(
		ctx context.Context,
		exec IExecute,
		conn *server.Conn,
		state *handler.MultigatewayConnectionState,
		portalInfo *preparedstatement.PortalInfo,
		maxRows int32,
		includeDescribe bool,
		info PlanExecInfo,
		callback func(context.Context, *sqltypes.Result) error,
	) error

	// GetTableGroup returns the target tablegroup for this primitive.
	// Returns empty string if primitive doesn't target a specific tablegroup.
	GetTableGroup() string

	// GetQuery returns the SQL query to be executed.
	GetQuery() string

	// String returns a description of the primitive for logging/debugging.
	String() string
}
