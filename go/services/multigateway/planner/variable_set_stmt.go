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

package planner

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// planVariableSetStmt plans SET/RESET commands.
//
//   - SET var = value (non gateway-managed, non-LOCAL) is planned as
//     Sequence[ValidateSetting, ApplySessionState]: ValidateSetting runs
//     set_config(name, value, is_local := true) on a backend so an invalid
//     name/value errors at SET time (matching PostgreSQL) without persisting on
//     the pooled backend, and ApplySessionState records it for pool-rotation
//     replay only if validation succeeded.
//   - RESET / RESET ALL update local tracking only (no backend round-trip).
//   - Gateway-managed variables, SET LOCAL, and SET TRANSACTION / FROM CURRENT
//     are handled by their dedicated paths below.
func (p *Planner) planVariableSetStmt(
	sql string,
	stmt *ast.VariableSetStmt,
	conn *server.Conn,
) (*engine.Plan, error) {
	// Transaction-only variables are backend state, not replayable session GUCs.
	// In particular, RESET transaction_isolation/read_only/deferrable must reach
	// PostgreSQL so it can raise "parameter ... cannot be reset", and SET
	// TRANSACTION SNAPSHOT must use PostgreSQL's transaction-snapshot machinery
	// instead of set_config validation (where it looks like an unrecognized GUC).
	if isTransactionOnlyVariable(stmt.Name) {
		p.logger.Debug("transaction-only variable detected, passing through",
			"kind", stmt.Kind, "variable", stmt.Name)
		return p.planDefault(sql, stmt, conn, PlanOptions{})
	}

	// Role/session authorization are replayable session state, but they are not
	// ordinary GUCs for validation/replay purposes. Outside an explicit
	// transaction, keep RESET/SET-TO-DEFAULT on the local tracking path (so
	// pooled backends don't get mutated behind connstate) and let ApplySettings
	// replay them as SET SESSION AUTHORIZATION / SET ROLE in PostgreSQL-compatible
	// order before the next query.
	//
	// Inside an explicit transaction, a backend is already pinned for the
	// transaction's duration and won't be replayed onto until the *next*
	// transaction. `SET LOCAL ROLE`/`SET LOCAL SESSION AUTHORIZATION` passes
	// straight through to that pinned backend untracked (see the IsLocal branch
	// below) specifically because "the backend is authoritative" for LOCAL
	// variables — so gateway-only tracking has nothing to clear when a RESET
	// follows a LOCAL set, and the pinned backend's real role would otherwise
	// stay changed for the rest of the transaction. Route the real RESET to that
	// same backend (mirroring the route-then-track SET plan below) so it takes
	// effect immediately, then track locally for pool-rotation replay after the
	// transaction ends.
	if isRoleAuthVariable(stmt.Name) && !stmt.IsLocal {
		if stmt.Kind == ast.VAR_SET_DEFAULT || stmt.Kind == ast.VAR_RESET {
			if conn != nil && conn.IsInTransaction() {
				route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt)
				track := engine.NewApplySessionStateSilent(sql, stmt)
				plan := engine.NewPlan(sql, engine.NewSequence([]engine.Primitive{route, track}))
				p.logger.Debug("created route-then-track role/session authorization reset plan inside transaction",
					"kind", stmt.Kind, "variable", stmt.Name, "plan", plan.String())
				return plan, nil
			}

			plan := engine.NewPlan(sql, engine.NewApplySessionState(sql, stmt))
			p.logger.Debug("created role/session authorization reset plan",
				"kind", stmt.Kind, "variable", stmt.Name, "plan", plan.String())
			return plan, nil
		}
	}

	// Gateway-managed variables are handled locally without routing to PostgreSQL,
	// regardless of whether SET or SET LOCAL is used. This check must come before
	// the IsLocal pass-through so SET LOCAL on a gateway-managed variable updates
	// the gateway state instead of the (uninvolved) backend, keeping subsequent
	// SHOW consistent with PostgreSQL semantics. The check also runs before the
	// Kind filter because VAR_SET_DEFAULT needs to be intercepted (treated as RESET).
	if isGatewayManagedVariable(stmt.Name) {
		value := ""
		if stmt.Kind == ast.VAR_SET_VALUE {
			value = extractVariableValue(stmt.Args)
		}
		primitive, err := p.planGatewayManagedVariable(sql, stmt, value)
		if err != nil {
			return nil, err
		}
		plan := engine.NewPlan(sql, primitive)
		p.logger.Debug("created gateway-managed plan", "plan", plan.String())
		return plan, nil
	}

	// Non-gateway-managed SET LOCAL passes through to PostgreSQL unchanged —
	// the backend is authoritative for those variables.
	if stmt.IsLocal {
		p.logger.Debug("SET LOCAL detected, passing through",
			"variable", stmt.Name)
		return p.planDefault(sql, stmt, conn, PlanOptions{})
	}

	// SET var TO DEFAULT is equivalent to RESET var in PostgreSQL
	// (PG's ExecSetVariableStmt falls through from VAR_SET_DEFAULT to VAR_RESET).
	// Normalize before the switch so it shares the same tracking path.
	if stmt.Kind == ast.VAR_SET_DEFAULT {
		p.logger.Debug("SET TO DEFAULT treated as RESET",
			"variable", stmt.Name)
		stmt = &ast.VariableSetStmt{
			Kind: ast.VAR_RESET,
			Name: stmt.Name,
		}
	}

	switch stmt.Kind {
	case ast.VAR_SET_VALUE:
		// Inside an explicit transaction, route the real SET to PostgreSQL and
		// silently track it after success. Validating via SELECT set_config(...)
		// would assign a SERIALIZABLE snapshot before the user's first real query;
		// plain SET does not.
		if conn != nil && conn.IsInTransaction() {
			route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt)
			track := engine.NewApplySessionStateSilent(sql, stmt)
			plan := engine.NewPlan(sql, engine.NewSequence([]engine.Primitive{route, track}))
			p.logger.Debug("created route-then-track SET plan inside transaction",
				"variable", stmt.Name, "plan", plan.String())
			return plan, nil
		}

		// Validate the value against PostgreSQL, then track it locally. The
		// ValidateSetting step runs set_config(name, value, is_local := true),
		// which validates the value (an invalid name or out-of-range value
		// errors at SET time, matching PostgreSQL) but reverts immediately, so
		// no state is left on the pooled backend — multipooler stays the sole
		// authority on backend session GUCs. The Sequence stops on the first
		// child's error, so a rejected SET never reaches the tracker. On success
		// the trailing ApplySessionState records the setting for pool-rotation
		// replay and emits CommandComplete("SET").
		value := extractVariableValue(stmt.Args)
		validate := engine.NewValidateSetting(p.defaultTableGroup, constants.DefaultShard, stmt.Name, value, sql)
		track := engine.NewApplySessionState(sql, stmt)
		plan := engine.NewPlan(sql, engine.NewSequence([]engine.Primitive{validate, track}))
		p.logger.Debug("created validate-then-track SET plan",
			"variable", stmt.Name, "plan", plan.String())
		return plan, nil

	case ast.VAR_RESET, ast.VAR_RESET_ALL:
		// RESET clears local tracking; the merged settings the pool applies on
		// the next query then fall back to the startup/default value. No
		// PostgreSQL round-trip is needed.
		plan := engine.NewPlan(sql, engine.NewApplySessionState(sql, stmt))
		p.logger.Debug("created RESET plan",
			"kind", stmt.Kind, "variable", stmt.Name, "plan", plan.String())
		return plan, nil

	case ast.VAR_SET_MULTI, ast.VAR_SET_CURRENT:
		// VAR_SET_MULTI: SET TRANSACTION / SET SESSION CHARACTERISTICS — transaction-scoped,
		//   must be executed directly on the backend, no session tracking needed.
		// VAR_SET_CURRENT: SET var FROM CURRENT — reads current PG value, needs backend execution.
		p.logger.Debug("passing through to PostgreSQL",
			"kind", stmt.Kind, "variable", stmt.Name)
		return p.planDefault(sql, stmt, conn, PlanOptions{})

	default:
		return nil, mterrors.NewFeatureNotSupported(fmt.Sprintf("SET kind %d is not yet supported", stmt.Kind))
	}
}

// isTransactionOnlyVariable reports variables whose SET/RESET forms must be
// executed by PostgreSQL against the current transaction. They are not durable
// session settings and must not enter MultigatewayConnectionState.SessionSettings.
func isTransactionOnlyVariable(name string) bool {
	switch strings.ToLower(name) {
	case "transaction_isolation", "transaction_read_only", "transaction_deferrable", "transaction_snapshot":
		return true
	default:
		return false
	}
}

func isRoleAuthVariable(name string) bool {
	switch strings.ToLower(name) {
	case "role", "session_authorization":
		return true
	default:
		return false
	}
}

// isGatewayManagedVariable returns true for session variables that are managed
// entirely by the gateway and should NOT be forwarded to PostgreSQL.
// These variables control gateway-level behavior (e.g., timeouts) and sending
// them to PostgreSQL would be redundant or counterproductive for connection
// pooling. It delegates to handler.IsGatewayManagedVariable so the planner and
// engine share a single source of truth for the managed-variable set.
func isGatewayManagedVariable(name string) bool {
	return handler.IsGatewayManagedVariable(name)
}

// planGatewayManagedVariable creates a GatewaySessionState primitive for a
// gateway-managed variable. All parsing and validation happens here at plan
// time so the primitive's execute path is a simple assignment.
func (p *Planner) planGatewayManagedVariable(
	sql string,
	stmt *ast.VariableSetStmt,
	value string,
) (engine.Primitive, error) {
	name := strings.ToLower(stmt.Name)

	switch stmt.Kind {
	case ast.VAR_SET_VALUE:
		// Validate the value now so an invalid SET errors at plan time (matching
		// PostgreSQL). The handler registry is the single source of truth for how
		// each gateway-managed variable parses/applies its value; the primitive
		// carries the raw string and applies it via the registry at execute time.
		if _, err := handler.GatewayManagedCanonicalValue(name, value); err != nil {
			return nil, err
		}
		p.logger.Debug("planning SET gateway-managed variable",
			"variable", name, "value", value, "is_local", stmt.IsLocal)
		return engine.NewGatewayManagedVariableSet(sql, name, value, stmt.IsLocal), nil

	case ast.VAR_RESET, ast.VAR_SET_DEFAULT:
		// RESET and SET ... TO DEFAULT revert to the flag default.
		// SET LOCAL var TO DEFAULT (IsLocal && VAR_SET_DEFAULT) is distinct: it
		// installs a transaction-scoped override equal to the default, masking
		// (not destroying) the session value. The primitive branches on isLocal.
		// RESET itself has no LOCAL form in the grammar, so stmt.IsLocal is
		// always false for VAR_RESET.
		// Note: VAR_RESET_ALL is not listed here because RESET ALL has stmt.Name=""
		// which never passes isGatewayManagedVariable. RESET ALL is handled by
		// ApplySessionState (after routing to PostgreSQL) which resets both
		// PostgreSQL session settings and gateway-managed variables.
		// isResetStmt is set only for VAR_RESET so the wire CommandTag is
		// "RESET" for `RESET var` and "SET" for `SET [LOCAL] var TO DEFAULT`,
		// matching PostgreSQL.
		isResetStmt := stmt.Kind == ast.VAR_RESET
		p.logger.Debug("planning RESET gateway-managed variable",
			"variable", name, "is_local", stmt.IsLocal, "is_reset_stmt", isResetStmt)
		return engine.NewGatewaySessionStateReset(sql, name, stmt.IsLocal, isResetStmt), nil

	default:
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSSyntaxError,
			fmt.Sprintf("unsupported operation for parameter %q", name), "")
	}
}

// extractVariableValue converts AST NodeList arguments to a string value.
// Handles: single values, multiple values, integers, strings, etc.
func extractVariableValue(args *ast.NodeList) string {
	if args == nil || args.Len() == 0 {
		return ""
	}

	// Handle multiple args (e.g., search_path = 'schema1', 'schema2')
	var values []string
	for _, arg := range args.Items {
		switch v := arg.(type) {
		case *ast.A_Const:
			// A_Const wraps the actual value - unwrap it
			values = append(values, extractConstValue(v))
		case *ast.String:
			// Direct String literal - SVal is already unquoted
			values = append(values, v.SVal)
		case *ast.Integer:
			// Direct Integer literal
			values = append(values, strconv.Itoa(v.IVal))
		default:
			// For complex types, use SqlString() as fallback
			values = append(values, arg.SqlString())
		}
	}

	// Join multiple values with ", " (PostgreSQL format)
	return strings.Join(values, ", ")
}

// extractConstValue extracts string value from A_Const node.
func extractConstValue(aConst *ast.A_Const) string {
	if aConst == nil || aConst.Val == nil {
		return ""
	}

	switch val := aConst.Val.(type) {
	case *ast.String:
		return val.SVal
	case *ast.Integer:
		return strconv.Itoa(val.IVal)
	case *ast.Float:
		return val.FVal
	default:
		return aConst.SqlString()
	}
}
