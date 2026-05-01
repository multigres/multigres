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

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// GatewayShowVariable handles SHOW for variables managed entirely by the gateway.
// It reads the effective value from connection state (which already accounts for
// the session override vs default priority) and returns a single-row result
// matching PostgreSQL's SHOW output format.
type GatewayShowVariable struct {
	sql      string // Original SQL for debugging
	variable string // Variable name (e.g., "statement_timeout")
}

// NewGatewayShowVariable creates a primitive that returns the current effective
// value of a gateway-managed variable.
func NewGatewayShowVariable(sql string, variable string) *GatewayShowVariable {
	return &GatewayShowVariable{
		sql:      sql,
		variable: variable,
	}
}

// StreamExecute reads the variable's current value and returns it as a single-row result.
func (g *GatewayShowVariable) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	var value string
	switch g.variable {
	case "statement_timeout":
		value = state.ShowStatementTimeout()
	default:
		// Unreachable: the planner validates the variable name before creating
		// this primitive. If we get here, there's a code bug (new variable added
		// to isGatewayManagedVariable but not here).
		panic(fmt.Sprintf("BUG: unhandled gateway-managed variable %q in GatewayShowVariable", g.variable))
	}

	return callback(ctx, &sqltypes.Result{
		Fields: []*query.Field{
			{
				Name:        g.variable,
				Type:        "text",
				DataTypeOid: 25, // text OID
			},
		},
		Rows: []*sqltypes.Row{
			sqltypes.MakeRow([][]byte{[]byte(value)}),
		},
		CommandTag: "SHOW",
	})
}

// PortalStreamExecute satisfies the Primitive interface for the
// extended-protocol path. SHOW on a gateway-managed variable carries no
// parameter binds; the value is read from gateway state. Delegate.
func (g *GatewayShowVariable) PortalStreamExecute(
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
func (g *GatewayShowVariable) GetTableGroup() string {
	return ""
}

// GetQuery returns empty string as this primitive doesn't execute a query.
func (g *GatewayShowVariable) GetQuery() string {
	return ""
}

// String returns a description for logging/debugging.
func (g *GatewayShowVariable) String() string {
	return fmt.Sprintf("GatewayShowVariable(%s)", g.sql)
}

// Ensure GatewayShowVariable implements Primitive interface.
var _ Primitive = (*GatewayShowVariable)(nil)
