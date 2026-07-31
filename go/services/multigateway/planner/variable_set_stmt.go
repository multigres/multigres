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
// The gateway is the sole authority on logical session GUC state; a pooled
// backend's session state may only change in lockstep with the gateway map.
// Two shapes per statement kind, split on whether the session is pinned to a
// backend (inside an explicit transaction, including its deferred-BEGIN first
// statement, or holding a reserved connection):
//
//   - Pinned SET/RESET/RESET ALL route the real statement to the pinned
//     backend and silently track it after success (Sequence[Route,
//     ApplySessionStateSilent]) — PostgreSQL applies the change for real and
//     the gateway records the same change, keeping map and backend in
//     lockstep. Routing (rather than a SELECT set_config probe) also avoids
//     assigning a REPEATABLE READ/SERIALIZABLE snapshot before the
//     transaction's first real query.
//   - Unpinned SET is planned as Sequence[ValidateSetting, ApplySessionState]:
//     the probe runs set_config(name, value, is_local := true) so an invalid
//     name/value errors at SET time (matching PostgreSQL) while reverting at
//     statement end — no state is left on the pooled backend; persistence
//     lives in the gateway map and is replayed at checkout.
//   - Unpinned RESET is Sequence[ValidateSettingReset, ApplySessionState]:
//     the probe (set_config(name, NULL, is_local := true)) errors on unknown
//     names like a real RESET and reverts, then the tracker drops the map
//     entry. Unpinned RESET ALL is a pure map edit — it cannot fail.
//   - SET SESSION CHARACTERISTICS AS TRANSACTION <mode> is translated to the
//     default_transaction_* GUC it sets and re-planned through the paths
//     above, so it is tracked like any other session GUC.
//   - SET var FROM CURRENT is rejected: its effective value is only knowable
//     by mutating a backend, which would diverge from the gateway map.
//   - Gateway-managed variables, SET LOCAL, and SET TRANSACTION are handled
//     by their dedicated paths below.
func (p *Planner) planVariableSetStmt(
	sql string,
	stmt *ast.VariableSetStmt,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
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

	pinned := sessionPinned(conn, state, p.defaultTableGroup, constants.DefaultShard)

	switch stmt.Kind {
	case ast.VAR_SET_VALUE:
		// Pinned: route the real SET to the session's backend and silently track
		// it after success — the backend genuinely carries the value (surviving
		// COMMIT, reverting on ROLLBACK exactly like the gateway map, whose
		// savepoint frames revert in lockstep). Routing rather than probing also
		// avoids assigning a REPEATABLE READ/SERIALIZABLE snapshot before the
		// transaction's first real query.
		if pinned {
			route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt)
			track := engine.NewApplySessionStateSilent(sql, stmt)
			plan := engine.NewPlan(sql, engine.NewSequence([]engine.Primitive{route, track}))
			p.logger.Debug("created route-then-track SET plan on pinned session",
				"variable", stmt.Name, "plan", plan.String())
			return plan, nil
		}

		// Unpinned: validate the value against PostgreSQL, then track it locally.
		// The ValidateSetting step runs set_config(name, value, is_local := true),
		// which validates the value (an invalid name or out-of-range value
		// errors at SET time, matching PostgreSQL) but reverts immediately, so
		// no state is left on the pooled backend — the gateway stays the sole
		// authority on session GUCs. The Sequence stops on the first
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
		// Pinned: route the real RESET to the session's backend, track after
		// success — same lockstep argument as pinned SET.
		if pinned {
			route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt)
			track := engine.NewApplySessionStateSilent(sql, stmt)
			plan := engine.NewPlan(sql, engine.NewSequence([]engine.Primitive{route, track}))
			p.logger.Debug("created route-then-track RESET plan on pinned session",
				"kind", stmt.Kind, "variable", stmt.Name, "plan", plan.String())
			return plan, nil
		}

		// Unpinned RESET: validate the name with a statement-local reset probe
		// (set_config(name, NULL, true) errors on unknown names like a real
		// RESET and reverts instantly), then drop the map entry and emit
		// CommandComplete("RESET"). No backend session state is touched.
		if stmt.Kind == ast.VAR_RESET {
			validate := engine.NewValidateSettingReset(p.defaultTableGroup, constants.DefaultShard, stmt.Name, sql)
			track := engine.NewApplySessionState(sql, stmt)
			plan := engine.NewPlan(sql, engine.NewSequence([]engine.Primitive{validate, track}))
			p.logger.Debug("created validate-then-track RESET plan",
				"variable", stmt.Name, "plan", plan.String())
			return plan, nil
		}

		// Unpinned RESET ALL cannot fail: it is a pure gateway map edit.
		plan := engine.NewPlan(sql, engine.NewApplySessionState(sql, stmt))
		p.logger.Debug("created gateway-only RESET ALL plan", "plan", plan.String())
		return plan, nil

	case ast.VAR_SET_MULTI:
		// SET TRANSACTION is transaction-scoped: it leaves no session state
		// behind, so it passes through to the backend untracked.
		if stmt.Name == "TRANSACTION" {
			p.logger.Debug("passing SET TRANSACTION through to PostgreSQL")
			return p.planDefault(sql, stmt, conn, PlanOptions{})
		}
		// SET SESSION CHARACTERISTICS AS TRANSACTION <mode> sets a session-level
		// default_transaction_* GUC. Translate it to the equivalent SET and
		// re-plan so it is tracked like any other session GUC.
		translated, err := translateSessionCharacteristics(stmt)
		if err != nil {
			return nil, err
		}
		return p.planVariableSetStmt(sql, translated, conn, state)

	case ast.VAR_SET_CURRENT:
		// SET var FROM CURRENT resolves its value inside the backend; the
		// gateway cannot learn the resulting session state without mutating a
		// pooled backend behind its own bookkeeping. Reject fail-closed.
		return nil, mterrors.NewFeatureNotSupported("SET ... FROM CURRENT is not supported: the resulting session state cannot be tracked by the connection pooler")

	default:
		return nil, mterrors.NewFeatureNotSupported(fmt.Sprintf("SET kind %d is not yet supported", stmt.Kind))
	}
}

// sessionPinned reports whether a statement routed to tableGroup/shard will
// execute on a session-affine backend: inside an explicit transaction
// (including its deferred-BEGIN first statement, whose reservation is created
// on the routed target) or on a session already holding a reserved connection
// FOR THAT TARGET (temp tables, cursors, advisory locks). Pinned statements
// may mutate that backend's session state for real, because the backend stays
// with the logical session and moves in lockstep with the gateway map;
// unpinned statements must never leave session state on a pooled backend.
//
// The target scoping is load-bearing: ScatterConn reuses a reservation only
// when the shard state matches the statement's target, so a session-wide
// check would let a statement planned as pinned fall through to a pooled
// connection on its own target and mutate it untracked.
//
// Callers must pass exactly the tablegroup/shard they hand to the Route they
// build, so predicate and routing cannot drift apart.
//
// Known gap, not closed here: with a reservation on a DIFFERENT shard, a
// tracked map change has no propagation path onto that pinned backend — the
// per-statement re-apply that used to carry it is gone. Multi-shard session
// settings need their own design; today every plan routes to the default
// tablegroup/shard, so the situation is unreachable.
func sessionPinned(conn *server.Conn, state *handler.MultigatewayConnectionState, tableGroup, shard string) bool {
	if conn != nil && conn.IsInTransaction() {
		return true
	}
	return state != nil && state.HasReservedConnectionFor(tableGroup, shard)
}

// translateSessionCharacteristics maps SET SESSION CHARACTERISTICS AS
// TRANSACTION <mode> onto the default_transaction_* session GUC it sets.
// PostgreSQL's grammar produces DefElems named transaction_isolation,
// transaction_read_only, and transaction_deferrable; the session form sets the
// corresponding default_* GUC. Multi-mode lists are rejected (rare, and each
// mode would need its own tracked plan).
func translateSessionCharacteristics(stmt *ast.VariableSetStmt) (*ast.VariableSetStmt, error) {
	if stmt.Args == nil || stmt.Args.Len() != 1 {
		return nil, mterrors.NewFeatureNotSupported("SET SESSION CHARACTERISTICS with multiple transaction modes is not supported")
	}
	def, ok := stmt.Args.Items[0].(*ast.DefElem)
	if !ok {
		return nil, mterrors.NewFeatureNotSupported("unsupported SET SESSION CHARACTERISTICS form")
	}

	var value string
	switch arg := def.Arg.(type) {
	case *ast.String:
		value = arg.SVal
	case *ast.Boolean:
		if arg.BoolVal {
			value = "on"
		} else {
			value = "off"
		}
	default:
		return nil, mterrors.NewFeatureNotSupported("unsupported SET SESSION CHARACTERISTICS form")
	}

	return ast.NewVariableSetStmt(
		ast.VAR_SET_VALUE,
		"default_"+def.Defname,
		ast.NewNodeList(ast.NewA_Const(ast.NewString(value), 0)),
		false,
	), nil
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
