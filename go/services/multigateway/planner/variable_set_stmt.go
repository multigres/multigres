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
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// planVariableSetStmt plans SET/RESET commands.
// Creates a sequence that executes on PostgreSQL first, then updates local state.
func (p *Planner) planVariableSetStmt(
	sql string,
	stmt *ast.VariableSetStmt,
	conn *server.Conn,
) (*engine.Plan, error) {
	// Just pass through to PostgreSQL
	if stmt.IsLocal {
		p.logger.Debug("SET LOCAL detected, passing through",
			"variable", stmt.Name)
		return p.planDefault(sql, conn)
	}

	// Only track VAR_SET_VALUE, VAR_RESET, VAR_RESET_ALL
	// Other kinds (DEFAULT, CURRENT, MULTI) are passed through
	switch stmt.Kind {
	case ast.VAR_SET_VALUE, ast.VAR_RESET, ast.VAR_RESET_ALL:
		// These are tracked locally
	default:
		// VAR_SET_DEFAULT, VAR_SET_CURRENT, VAR_SET_MULTI - pass through
		return p.planDefault(sql, conn)
	}

	// Extract value for SET commands
	value := ""
	if stmt.Kind == ast.VAR_SET_VALUE {
		value = extractVariableValue(stmt.Args)
	}

	// SET/RESET command: Execute on PostgreSQL, then update local state
	p.logger.Debug("planning SET/RESET command",
		"kind", stmt.Kind,
		"variable", stmt.Name,
		"value", value)

	// 1. Route: Send to PostgreSQL for validation and execution
	route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql)

	// 2. ApplySessionState: Update local tracking after successful execution
	applyState := engine.NewApplySessionState(stmt, value)

	// 3. Compose in sequence (pessimistic: state update only if remote succeeds)
	seq := engine.NewSequence([]engine.Primitive{route, applyState})

	plan := engine.NewPlan(sql, seq)
	p.logger.Debug("created SET/RESET plan", "plan", plan.String())
	return plan, nil
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
