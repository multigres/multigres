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

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// ApplySessionState updates local session state after a SET/RESET command
// executes successfully on PostgreSQL.
//
// This primitive does NOT execute queries - it only updates the local state
// tracking. It should be composed after a Route primitive in a Sequence.
type ApplySessionState struct {
	VariableStmt *ast.VariableSetStmt // The SET/RESET statement from AST
	Value        string               // Extracted value (for SET commands)
}

// NewApplySessionState creates a new ApplySessionState primitive.
func NewApplySessionState(stmt *ast.VariableSetStmt, value string) *ApplySessionState {
	return &ApplySessionState{
		VariableStmt: stmt,
		Value:        value,
	}
}

// StreamExecute updates the local session state based on the command type.
func (a *ApplySessionState) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// Update local session state based on the command type
	// Uses AST enums directly - no wrapper types needed
	switch a.VariableStmt.Kind {
	case ast.VAR_SET_VALUE:
		// SET variable = value
		state.SetSessionVariable(a.VariableStmt.Name, a.Value)

	case ast.VAR_RESET:
		// RESET variable
		state.ResetSessionVariable(a.VariableStmt.Name)

	case ast.VAR_RESET_ALL:
		// RESET ALL
		state.ResetAllSessionVariables()

		// VAR_SET_DEFAULT, VAR_SET_CURRENT, VAR_SET_MULTI are not tracked locally
		// They are passed through to PostgreSQL only
	}

	// This primitive doesn't produce results - the previous Route primitive
	// already streamed results to the callback. Just return success.
	return nil
}

// GetTableGroup returns empty string as this primitive doesn't target a tablegroup.
func (a *ApplySessionState) GetTableGroup() string {
	return "" // Doesn't target a tablegroup
}

// GetQuery returns empty string as this primitive doesn't execute a query.
func (a *ApplySessionState) GetQuery() string {
	return "" // Doesn't execute a query
}

// String returns a string representation for debugging.
// Reuses AST's existing SqlString() method instead of reimplementing.
func (a *ApplySessionState) String() string {
	return fmt.Sprintf("ApplySessionState(%s)", a.VariableStmt.SqlString())
}

// Ensure ApplySessionState implements Primitive interface.
var _ Primitive = (*ApplySessionState)(nil)
