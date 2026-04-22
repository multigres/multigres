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

// ApplySessionState handles SET/RESET commands by updating local session state only.
//
// Neither SET nor RESET is routed to PostgreSQL. Both are applied locally and
// return a synthetic CommandComplete response. The pool propagates these settings
// to the backend connection on the next query via ApplySettings.
//
// Behaviour deviation from PostgreSQL:
// SET commands are NOT validated against PostgreSQL. This means a client can SET
// an invalid variable name or value without receiving an immediate error. The error
// will surface on the next query when the pool tries to apply the setting to a
// backend connection. The client must RESET the bad variable to recover. This
// trade-off was chosen intentionally to keep the SET/RESET path simple. It may
// be revisited in the future if stricter validation is needed.
//
// For RESET/RESET ALL:
// The variable is removed from SessionSettings. On the next query, the merged
// settings (SessionSettings overlaid on StartupParams) will fall back to the
// startup parameter value, and the pool will apply the correct SET commands.
type ApplySessionState struct {
	// VariableStmt is the SET/RESET statement from the AST.
	VariableStmt *ast.VariableSetStmt

	// Query is the original SQL string.
	Query string

	// SilentTracking, when true, updates SessionSettings but does NOT invoke
	// the callback. Used inside a Sequence where a sibling primitive (like
	// Route) owns the client-facing result — if both called back, the client
	// would see a stray CommandComplete before the real row data. This is
	// the shape a `SELECT set_config(...)` plan takes: silent tracking step
	// first, then a Route that sends the query to PG and streams the result.
	SilentTracking bool
}

// NewApplySessionState creates a new ApplySessionState primitive.
func NewApplySessionState(sql string, stmt *ast.VariableSetStmt) *ApplySessionState {
	return &ApplySessionState{
		VariableStmt: stmt,
		Query:        sql,
	}
}

// NewApplySessionStateSilent creates an ApplySessionState that updates the
// tracker without emitting anything to the client. Intended for use inside a
// Sequence where a Route primitive owns the client-facing response — see
// planner.planSelectStmt for the `SELECT set_config(...), * FROM t` case.
func NewApplySessionStateSilent(sql string, stmt *ast.VariableSetStmt) *ApplySessionState {
	return &ApplySessionState{
		VariableStmt:   stmt,
		Query:          sql,
		SilentTracking: true,
	}
}

// StreamExecute handles the SET/RESET command.
func (s *ApplySessionState) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch s.VariableStmt.Kind {
	case ast.VAR_SET_VALUE:
		return s.executeSet(ctx, state, callback)
	case ast.VAR_RESET, ast.VAR_RESET_ALL:
		return s.executeReset(ctx, state, callback)
	default:
		return mterrors.NewFeatureNotSupported(fmt.Sprintf("SET/RESET kind %d is not supported", s.VariableStmt.Kind))
	}
}

// executeSet handles SET commands: update local state and return a synthetic
// response. The value is NOT validated against PostgreSQL — see the
// ApplySessionState doc comment.
//
// Two modes:
//   - SilentTracking: update state, no callback (a sibling primitive in a
//     Sequence will respond — used for SELECT set_config(...) plans).
//   - default: update state and emit CommandComplete "SET" (real SET stmt).
func (s *ApplySessionState) executeSet(
	ctx context.Context,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	value := extractVariableValue(s.VariableStmt.Args)
	state.SetSessionVariable(s.VariableStmt.Name, value)

	if s.SilentTracking {
		return nil
	}

	return callback(ctx, &sqltypes.Result{
		CommandTag: "SET",
	})
}

// executeReset handles RESET/RESET ALL: update state, return synthetic response.
func (s *ApplySessionState) executeReset(
	ctx context.Context,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch s.VariableStmt.Kind {
	case ast.VAR_RESET:
		// RESET variable
		state.ResetSessionVariable(s.VariableStmt.Name)

	case ast.VAR_RESET_ALL:
		state.ResetAllSessionVariables()
		// Also reset gateway-managed variables that live outside SessionSettings.
		state.ResetStatementTimeout()
	default:
		return mterrors.NewFeatureNotSupported(fmt.Sprintf("RESET kind %d is not supported", s.VariableStmt.Kind))
	}

	// Return synthetic CommandComplete
	return callback(ctx, &sqltypes.Result{
		CommandTag: "RESET",
	})
}

// GetTableGroup returns empty string — SET/RESET are local-only and don't target a tablegroup.
func (s *ApplySessionState) GetTableGroup() string {
	return ""
}

// GetQuery returns the original SQL string.
func (s *ApplySessionState) GetQuery() string {
	return s.Query
}

// String returns a string representation for debugging.
func (s *ApplySessionState) String() string {
	return fmt.Sprintf("ApplySessionState(%s)", s.VariableStmt.SqlString())
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

// Ensure ApplySessionState implements Primitive interface.
var _ Primitive = (*ApplySessionState)(nil)
