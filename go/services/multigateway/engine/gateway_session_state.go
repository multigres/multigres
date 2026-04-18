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

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
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
	isReset  bool   // true for RESET, false for SET

	// Typed fields for each gateway-managed variable.
	// Only the field matching `variable` is used.
	statementTimeout time.Duration
}

// NewStatementTimeoutSet creates a primitive that SETs `statement_timeout`.
// The value is pre-parsed and stored in the appropriate typed field.
func NewStatementTimeoutSet(sql string, statementTimeout time.Duration) *GatewaySessionState {
	return &GatewaySessionState{
		sql:              sql,
		variable:         "statement_timeout",
		statementTimeout: statementTimeout,
	}
}

// NewGatewaySessionStateReset creates a primitive that RESETs `a gateway-managed variable`.
func NewGatewaySessionStateReset(sql string, variable string) *GatewaySessionState {
	return &GatewaySessionState{
		sql:      sql,
		variable: variable,
		isReset:  true,
	}
}

// StreamExecute applies the state mutation and sends the CommandComplete.
func (g *GatewaySessionState) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	commandTag := "SET"
	if g.isReset {
		commandTag = "RESET"
	}
	switch g.variable {
	case "statement_timeout":
		if g.isReset {
			state.ResetStatementTimeout()
		} else {
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
