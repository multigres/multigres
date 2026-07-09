// Copyright 2026 Supabase, Inc.
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

package engine

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// GatewaySessionState handles SET/RESET for variables managed entirely by the
// gateway (not forwarded to PostgreSQL). Unlike ApplySessionState (which follows
// a Route), this primitive is used standalone and sends its own CommandComplete.
//
// The variable's behavior (parse, apply, reset, show) lives in the handler
// registry (gatewayManagedVariables); this primitive just carries the raw value
// and routes to the connection state's generic gateway-managed methods, so a new
// gateway-managed variable needs no change here.
type GatewaySessionState struct {
	sql         string // Original SQL for debugging
	variable    string // Variable name (e.g., "statement_timeout")
	value       string // Raw SET value (empty for RESET / SET TO DEFAULT)
	isReset     bool   // true for RESET, SET ... TO DEFAULT, or SET LOCAL ... TO DEFAULT
	isLocal     bool   // true if the source statement included LOCAL
	isResetStmt bool   // true only for the literal `RESET var` statement; controls CommandTag

	// The (isReset, isLocal) flags map to the four user-visible variants:
	//
	//   isReset isLocal  user statement                       primitive action
	//   ------- -------  -----------------------------------  --------------------------------
	//   false   false    SET var = value                      session-level override
	//   false   true     SET LOCAL var = value                transaction-local override
	//   true    false    RESET var | SET var TO DEFAULT       clear session (and any local)
	//   true    true     SET LOCAL var TO DEFAULT             transaction-local revert to
	//                                                         default; session preserved
	//
	// isResetStmt is an orthogonal flag used only to derive the wire-protocol
	// CommandTag. PostgreSQL returns "RESET" only for the literal `RESET var`
	// statement; `SET [LOCAL] var TO DEFAULT` returns "SET" even though it
	// shares semantics with RESET. So isResetStmt is true only when isReset is
	// true AND the source was VAR_RESET (never VAR_SET_DEFAULT).
}

// NewGatewayManagedVariableSet creates a primitive that applies
// `SET [LOCAL] <variable> = value` for a gateway-managed variable. value is the
// raw string; the handler registry parses and applies it at execute time. When
// isLocal is true, the change is a transaction-local override (SET LOCAL), cleared
// on COMMIT/ROLLBACK.
func NewGatewayManagedVariableSet(sql, variable, value string, isLocal bool) *GatewaySessionState {
	return &GatewaySessionState{
		sql:      sql,
		variable: variable,
		value:    value,
		isLocal:  isLocal,
	}
}

// NewGatewaySessionStateReset creates a primitive that RESETs a
// gateway-managed variable. When isLocal is true, the primitive performs
// the SET LOCAL ... TO DEFAULT semantic (transaction-scoped override to
// the server default that masks any session value). When isLocal is false,
// it performs the session-level RESET (or SET ... TO DEFAULT) which clears
// the session override entirely. isResetStmt must be true only for the
// literal `RESET var` statement (VAR_RESET); for `SET [LOCAL] var TO
// DEFAULT` (VAR_SET_DEFAULT) it must be false so the CommandTag is "SET",
// matching PostgreSQL.
func NewGatewaySessionStateReset(sql string, variable string, isLocal bool, isResetStmt bool) *GatewaySessionState {
	return &GatewaySessionState{
		sql:         sql,
		variable:    variable,
		isReset:     true,
		isLocal:     isLocal,
		isResetStmt: isResetStmt,
	}
}

// StreamExecute applies the state mutation and sends the CommandComplete.
func (g *GatewaySessionState) StreamExecute(
	ctx context.Context,
	_ IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// PostgreSQL returns "RESET" only for the literal `RESET var` statement.
	// `SET [LOCAL] var TO DEFAULT` and all other SET forms return "SET", even
	// though VAR_SET_DEFAULT shares semantics with RESET. Use isResetStmt
	// (set by the planner from stmt.Kind == VAR_RESET) so the tag matches PG.
	commandTag := "SET"
	if g.isResetStmt {
		commandTag = "RESET"
	}

	// SET LOCAL outside a transaction block is a no-op in PostgreSQL — it emits
	// a WARNING with SQLSTATE 25P01 (no_active_sql_transaction) and the value is
	// discarded immediately by the implicit-autocommit boundary. We mirror that
	// here: skip the state mutation and surface the WARNING as a NoticeResponse.
	// Without this guard, isLocalSet would persist for the lifetime of the
	// connection because no COMMIT/ROLLBACK ever fires to clear it.
	if g.isLocal && !conn.IsInTransaction() {
		warning := mterrors.NewPgNotice("WARNING", mterrors.PgSSNoActiveTransaction,
			"SET LOCAL can only be used in transaction blocks", "")
		return callback(ctx, &sqltypes.Result{
			CommandTag: commandTag,
			Notices:    []*mterrors.PgDiagnostic{warning},
		})
	}

	// The (isReset, isLocal) combination selects the state mutation; the handler
	// registry knows how to perform it for each gateway-managed variable.
	//   - SET LOCAL var TO DEFAULT: transaction-scoped override equal to the server
	//     default (SHOW returns default during the txn; the session value is
	//     preserved and restored when ResetAllLocalGUCs fires at txn end).
	//   - RESET / SET var TO DEFAULT: clear both the session and any local override
	//     (matches PG: a RESET inside a txn with a prior SET LOCAL supersedes it).
	//   - SET [LOCAL] var = value: session-level or transaction-local override.
	var err error
	switch {
	case g.isReset && g.isLocal:
		err = state.SetGatewayManagedLocalToDefault(g.variable)
	case g.isReset:
		err = state.ResetGatewayManaged(g.variable)
	default:
		err = state.SetGatewayManaged(g.variable, g.value, g.isLocal)
	}
	if err != nil {
		return err
	}

	return callback(ctx, &sqltypes.Result{CommandTag: commandTag})
}

// PortalStreamExecute satisfies the Primitive interface for the
// extended-protocol path. Gateway-managed SET/RESET targets carry no
// parameter binds — the variable name and value are baked into the plan
// at planning time. Delegate to StreamExecute.
func (g *GatewaySessionState) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return g.StreamExecute(ctx, exec, conn, state, nil, PlanExecInfo{}, callback)
}

// GetTableGroup returns empty string as this primitive doesn't target a tablegroup.
func (g *GatewaySessionState) GetTableGroup() string {
	return ""
}

// GetQuery returns empty string as this primitive doesn't execute a query.
func (g *GatewaySessionState) GetQuery() string {
	return ""
}

// String returns a description for logging/debugging.
func (g *GatewaySessionState) String() string {
	return fmt.Sprintf("GatewaySessionState(%s)", g.sql)
}

// Ensure GatewaySessionState implements Primitive interface.
var _ Primitive = (*GatewaySessionState)(nil)
