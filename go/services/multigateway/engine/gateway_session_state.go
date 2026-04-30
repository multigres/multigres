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
	"time"

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
// Values are parsed at plan time and stored in typed fields so execution is a
// simple assignment with no parsing overhead per query.
type GatewaySessionState struct {
	sql      string // Original SQL for debugging
	variable string // Variable name (e.g., "statement_timeout")
	isReset  bool   // true for RESET, SET ... TO DEFAULT, or SET LOCAL ... TO DEFAULT
	isLocal  bool   // true if the source statement included LOCAL

	// Typed fields for each gateway-managed variable.
	// Only the field matching `variable` is used.
	statementTimeout time.Duration

	// The (isReset, isLocal) flags map to the four user-visible variants:
	//
	//   isReset isLocal  user statement                       primitive action
	//   ------- -------  -----------------------------------  --------------------------------
	//   false   false    SET var = value                      session-level override
	//   false   true     SET LOCAL var = value                transaction-local override
	//   true    false    RESET var | SET var TO DEFAULT       clear session (and any local)
	//   true    true     SET LOCAL var TO DEFAULT             transaction-local revert to
	//                                                         default; session preserved
}

// NewStatementTimeoutSet creates a primitive that SETs `statement_timeout`.
// The value is pre-parsed and stored in the appropriate typed field.
// When isLocal is true, the value is treated as a transaction-local override
// (SET LOCAL), cleared on COMMIT/ROLLBACK.
func NewStatementTimeoutSet(sql string, statementTimeout time.Duration, isLocal bool) *GatewaySessionState {
	return &GatewaySessionState{
		sql:              sql,
		variable:         "statement_timeout",
		statementTimeout: statementTimeout,
		isLocal:          isLocal,
	}
}

// NewGatewaySessionStateReset creates a primitive that RESETs a
// gateway-managed variable. When isLocal is true, the primitive performs
// the SET LOCAL ... TO DEFAULT semantic (transaction-scoped override to
// the server default that masks any session value). When isLocal is false,
// it performs the session-level RESET (or SET ... TO DEFAULT) which clears
// the session override entirely.
func NewGatewaySessionStateReset(sql string, variable string, isLocal bool) *GatewaySessionState {
	return &GatewaySessionState{
		sql:      sql,
		variable: variable,
		isReset:  true,
		isLocal:  isLocal,
	}
}

// StreamExecute applies the state mutation and sends the CommandComplete.
func (g *GatewaySessionState) StreamExecute(
	ctx context.Context,
	_ IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	commandTag := "SET"
	if g.isReset {
		commandTag = "RESET"
	}

	// SET LOCAL outside a transaction block is a no-op in PostgreSQL — it emits
	// a WARNING with SQLSTATE 25P01 (no_active_sql_transaction) and the value is
	// discarded immediately by the implicit-autocommit boundary. We mirror that
	// here: skip the state mutation and surface the WARNING as a NoticeResponse.
	// Without this guard, isLocalSet would persist for the lifetime of the
	// connection because no COMMIT/ROLLBACK ever fires to clear it.
	if g.isLocal && !conn.IsInTransaction() {
		warning := mterrors.NewPgNotice("WARNING", "25P01",
			"SET LOCAL can only be used in transaction blocks", "")
		return callback(ctx, &sqltypes.Result{
			CommandTag: commandTag,
			Notices:    []*mterrors.PgDiagnostic{warning},
		})
	}

	switch g.variable {
	case "statement_timeout":
		switch {
		case g.isReset && g.isLocal:
			// SET LOCAL var TO DEFAULT: install a transaction-scoped override
			// equal to the server default so SHOW returns default during the
			// transaction, but the session-level value (if any) is preserved
			// and will be restored when ResetAllLocalGUCs fires at txn end.
			state.SetLocalStatementTimeoutToDefault()
		case g.isReset:
			// RESET (or SET ... TO DEFAULT, non-LOCAL): clear both the
			// session-level override and any active transaction-local override.
			// Matches PostgreSQL: RESET inside a transaction with a prior
			// SET LOCAL supersedes the LOCAL — effective value becomes the
			// default (verified against PG 17).
			state.ResetStatementTimeout()
		case g.isLocal:
			state.SetLocalStatementTimeout(g.statementTimeout)
		default:
			state.SetStatementTimeout(g.statementTimeout)
		}
	default:
		// Unreachable: the planner validates the variable name before creating
		// this primitive. If we get here, there's a code bug (new variable added
		// to isGatewayManagedVariable but not here).
		panic(fmt.Sprintf("BUG: unhandled gateway-managed variable %q in GatewaySessionState", g.variable))
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
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return g.StreamExecute(ctx, exec, conn, state, nil, callback)
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
