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
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// SessionStateRoute handles SET/RESET commands with correct pool state tracking.
//
// For SET commands:
//   - Updates local SessionSettings BEFORE routing to PostgreSQL
//   - Routes to PG for validation (rejects invalid variables/values)
//   - Rolls back local state if PG rejects the command
//   - The pool sees correct settings when acquiring the connection, so the
//     connection's tracked state stays in sync with PG-side GUC values
//
// For RESET/RESET ALL commands:
//   - Updates local SessionSettings (removes the variable)
//   - Does NOT route to PG — returns a synthetic "RESET" response
//   - The pool handles actual GUC restoration: on the next query, the merged
//     SessionSettings (which falls back to StartupParams) drives the pool to
//     apply the correct SET commands to the connection
//
// Why RESET is not routed: PostgreSQL's RESET restores the server-level default,
// which may differ from the client's startup parameter value. For example, the
// Go driver sends extra_float_digits=2 at startup, but RESET extra_float_digits
// sets PG to 1 (the server default). This creates a state mismatch: the pool
// tracks the connection as having the startup value, but PG has the server default.
// By not routing RESET and letting the pool handle it via ApplySettings, the
// actual PG-side state always matches the pool's tracked state.
type SessionStateRoute struct {
	// Route is the underlying route for SET commands (nil for RESET-only).
	Route *Route

	// VariableStmt is the SET/RESET statement from the AST.
	VariableStmt *ast.VariableSetStmt

	// SQL is the original SQL string.
	SQL string
}

// NewSessionStateRoute creates a new SessionStateRoute primitive.
func NewSessionStateRoute(route *Route, sql string, stmt *ast.VariableSetStmt) *SessionStateRoute {
	return &SessionStateRoute{
		Route:        route,
		VariableStmt: stmt,
		SQL:          sql,
	}
}

// StreamExecute handles the SET/RESET command.
func (s *SessionStateRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch s.VariableStmt.Kind {
	case ast.VAR_SET_VALUE:
		return s.executeSet(ctx, exec, conn, state, callback)
	case ast.VAR_RESET, ast.VAR_RESET_ALL:
		return s.executeReset(ctx, state, callback)
	default:
		return mterrors.NewFeatureNotSupported(fmt.Sprintf("SET/RESET kind %d is not supported", s.VariableStmt.Kind))
	}
}

// executeSet handles SET commands: apply state, route to PG, rollback on error.
func (s *SessionStateRoute) executeSet(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// Extract value from AST args.
	// TODO: multi-value settings (e.g. SET search_path = public, pg_catalog) are joined
	// into a single comma-separated string. ApplyQuery wraps the whole thing in one pair
	// of quotes, which PG rejects. Fix requires changing ApplyQuery to not re-quote and
	// storing SQL-ready representations instead. Tracked as a separate issue.
	value := extractVariableValue(s.VariableStmt.Args)

	// Save previous value for rollback
	prevValue, prevExists := state.GetSessionVariable(s.VariableStmt.Name)

	// Apply state optimistically so pool sees correct settings
	state.SetSessionVariable(s.VariableStmt.Name, value)

	// Roll back local state if PG rejects or StreamExecute panics
	var err error
	defer func() {
		if err != nil {
			if prevExists {
				state.SetSessionVariable(s.VariableStmt.Name, prevValue)
			} else {
				state.ResetSessionVariable(s.VariableStmt.Name)
			}
		}
	}()

	// Route to PG for validation
	err = s.Route.StreamExecute(ctx, exec, conn, state, callback)
	return err
}

// executeReset handles RESET/RESET ALL: update state, return synthetic response.
func (s *SessionStateRoute) executeReset(
	ctx context.Context,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch s.VariableStmt.Kind {
	case ast.VAR_RESET:
		state.ResetSessionVariable(s.VariableStmt.Name)
	case ast.VAR_RESET_ALL:
		state.ResetAllSessionVariables()
	default:
		return mterrors.NewFeatureNotSupported(fmt.Sprintf("RESET kind %d is not supported", s.VariableStmt.Kind))
	}

	// Return synthetic CommandComplete
	return callback(ctx, &sqltypes.Result{
		CommandTag: "RESET",
	})
}

// GetTableGroup returns the target tablegroup from the underlying Route.
func (s *SessionStateRoute) GetTableGroup() string {
	if s.Route != nil {
		return s.Route.GetTableGroup()
	}
	return ""
}

// GetQuery returns the original SQL string.
func (s *SessionStateRoute) GetQuery() string {
	return s.SQL
}

// String returns a string representation for debugging.
func (s *SessionStateRoute) String() string {
	return fmt.Sprintf("SessionStateRoute(%s)", s.VariableStmt.SqlString())
}

// extractVariableValue converts AST NodeList arguments to a string value.
func extractVariableValue(args *ast.NodeList) string {
	if args == nil || args.Len() == 0 {
		return ""
	}

	var values []string
	for _, arg := range args.Items {
		switch v := arg.(type) {
		case *ast.A_Const:
			values = append(values, extractConstValue(v))
		case *ast.String:
			values = append(values, v.SVal)
		case *ast.Integer:
			values = append(values, strconv.Itoa(v.IVal))
		default:
			values = append(values, arg.SqlString())
		}
	}

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

// Ensure SessionStateRoute implements Primitive interface.
var _ Primitive = (*SessionStateRoute)(nil)
