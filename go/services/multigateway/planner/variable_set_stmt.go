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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// planVariableSetStmt plans SET/RESET commands.
// Creates an ApplySessionState that handles SET and RESET as local state updates
// with synthetic responses. PG validation is deferred to the next query when
// the pool applies settings to the backend connection.
func (p *Planner) planVariableSetStmt(
	sql string,
	stmt *ast.VariableSetStmt,
	conn *server.Conn,
) (*engine.Plan, error) {
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
		return p.planDefault(sql, stmt, conn)
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
	case ast.VAR_SET_VALUE, ast.VAR_RESET, ast.VAR_RESET_ALL:
		// These are tracked locally

	case ast.VAR_SET_MULTI, ast.VAR_SET_CURRENT:
		// VAR_SET_MULTI: SET TRANSACTION / SET SESSION CHARACTERISTICS — transaction-scoped,
		//   must be executed directly on the backend, no session tracking needed.
		// VAR_SET_CURRENT: SET var FROM CURRENT — reads current PG value, needs backend execution.
		p.logger.Debug("passing through to PostgreSQL",
			"kind", stmt.Kind, "variable", stmt.Name)
		return p.planDefault(sql, stmt, conn)

	default:
		return nil, mterrors.NewFeatureNotSupported(fmt.Sprintf("SET kind %d is not yet supported", stmt.Kind))
	}

	p.logger.Debug("planning SET/RESET command",
		"kind", stmt.Kind,
		"variable", stmt.Name)

	primitive := engine.NewApplySessionState(sql, stmt)

	plan := engine.NewPlan(sql, primitive)
	p.logger.Debug("created SET/RESET plan", "plan", plan.String())
	return plan, nil
}

// isGatewayManagedVariable returns true for session variables that are managed
// entirely by the gateway and should NOT be forwarded to PostgreSQL.
// These variables control gateway-level behavior (e.g., timeouts) and sending
// them to PostgreSQL would be redundant or counterproductive for connection pooling.
func isGatewayManagedVariable(name string) bool {
	switch strings.ToLower(name) {
	case "statement_timeout":
		return true
	default:
		return false
	}
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
		switch name {
		case "statement_timeout":
			d, err := handler.ParsePostgresInterval(name, value)
			if err != nil {
				return nil, err
			}
			p.logger.Debug("planning SET statement_timeout (gateway-managed)",
				"value", value, "parsed", d, "is_local", stmt.IsLocal)
			return engine.NewStatementTimeoutSet(sql, d, stmt.IsLocal), nil
		default:
			return nil, mterrors.NewUnrecognizedParameter(name)
		}

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
		p.logger.Debug("planning RESET gateway-managed variable",
			"variable", name, "is_local", stmt.IsLocal)
		return engine.NewGatewaySessionStateReset(sql, name, stmt.IsLocal), nil

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
