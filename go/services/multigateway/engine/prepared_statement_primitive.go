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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// preparedStmtKind identifies which prepared statement operation to perform.
type preparedStmtKind int

const (
	preparedStmtPrepare       preparedStmtKind = iota // PREPARE name AS query
	preparedStmtExecute                               // EXECUTE name [(params)]
	preparedStmtDeallocate                            // DEALLOCATE name
	preparedStmtDeallocateAll                         // DEALLOCATE ALL
)

// PreparedStatementPrimitive handles PREPARE, EXECUTE, and DEALLOCATE statements
// from the simple query protocol by delegating to the existing extended query
// protocol handler methods (HandleParse, HandleBind, HandleClose) via conn.Handler().
//
// Key behaviors:
//   - PREPARE: Calls HandleParse to register in the consolidator.
//   - EXECUTE: Calls HandleBind to create a portal, then PortalStreamExecute.
//   - DEALLOCATE: Calls HandleClose to remove from the consolidator.
//   - DEALLOCATE ALL: Uses the consolidator directly (no extended protocol equivalent).
type PreparedStatementPrimitive struct {
	kind       preparedStmtKind
	tableGroup string

	// stmtName is the prepared statement name (used by all kinds).
	stmtName string

	// innerQuery is the SQL body of the PREPARE statement.
	innerQuery string

	// params holds the converted EXECUTE parameters (text-format byte arrays).
	params [][]byte
}

// NewPreparePrimitive creates a primitive for PREPARE name AS query.
func NewPreparePrimitive(tableGroup, stmtName, innerQuery string) *PreparedStatementPrimitive {
	return &PreparedStatementPrimitive{
		kind:       preparedStmtPrepare,
		tableGroup: tableGroup,
		stmtName:   stmtName,
		innerQuery: innerQuery,
	}
}

// NewExecutePrimitive creates a primitive for EXECUTE name [(params)].
func NewExecutePrimitive(tableGroup, stmtName string, params [][]byte) *PreparedStatementPrimitive {
	return &PreparedStatementPrimitive{
		kind:       preparedStmtExecute,
		tableGroup: tableGroup,
		stmtName:   stmtName,
		params:     params,
	}
}

// NewDeallocatePrimitive creates a primitive for DEALLOCATE name.
func NewDeallocatePrimitive(tableGroup, stmtName string) *PreparedStatementPrimitive {
	return &PreparedStatementPrimitive{
		kind:       preparedStmtDeallocate,
		tableGroup: tableGroup,
		stmtName:   stmtName,
	}
}

// NewDeallocateAllPrimitive creates a primitive for DEALLOCATE ALL.
func NewDeallocateAllPrimitive(tableGroup string) *PreparedStatementPrimitive {
	return &PreparedStatementPrimitive{
		kind:       preparedStmtDeallocateAll,
		tableGroup: tableGroup,
	}
}

func (p *PreparedStatementPrimitive) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch p.kind {
	case preparedStmtPrepare:
		return p.executePrepare(ctx, conn, callback)
	case preparedStmtExecute:
		return p.executeExecute(ctx, exec, conn, state, callback)
	case preparedStmtDeallocate:
		return p.executeDeallocate(ctx, conn, callback)
	case preparedStmtDeallocateAll:
		return p.executeDeallocateAll(ctx, conn, callback)
	default:
		return fmt.Errorf("unknown prepared statement primitive kind: %d", p.kind)
	}
}

// executePrepare delegates to HandleParse to register the statement in the consolidator.
func (p *PreparedStatementPrimitive) executePrepare(
	ctx context.Context,
	conn *server.Conn,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if err := conn.Handler().HandleParse(ctx, conn, p.stmtName, p.innerQuery, nil); err != nil {
		return err
	}
	return callback(ctx, &sqltypes.Result{CommandTag: "PREPARE"})
}

// executeExecute delegates to HandleBind to create a portal, then executes it
// via PortalStreamExecute. We avoid calling HandleExecute directly because it
// would double-count metrics and spans (HandleQuery already records those).
func (p *PreparedStatementPrimitive) executeExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// HandleBind looks up the prepared statement, creates a portal, and stores it in state.
	if err := conn.Handler().HandleBind(ctx, conn, "", p.stmtName, p.params, nil, nil); err != nil {
		return err
	}

	// Retrieve the portal that HandleBind just created.
	portalInfo := state.GetPortalInfo("")
	if portalInfo == nil {
		return fmt.Errorf("internal error: portal not found after bind for statement \"%s\"", p.stmtName)
	}
	defer state.DeletePortalInfo("")

	return exec.PortalStreamExecute(ctx, p.tableGroup, constants.DefaultShard, conn, state, portalInfo, 0, callback)
}

// executeDeallocate delegates to HandleClose for named statements.
// DEALLOCATE ALL uses the consolidator directly since there is no
// single-statement extended protocol equivalent.
func (p *PreparedStatementPrimitive) executeDeallocate(
	ctx context.Context,
	conn *server.Conn,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if err := conn.Handler().HandleClose(ctx, conn, 'S', p.stmtName); err != nil {
		return err
	}
	return callback(ctx, &sqltypes.Result{CommandTag: "DEALLOCATE"})
}

func (p *PreparedStatementPrimitive) executeDeallocateAll(
	ctx context.Context,
	conn *server.Conn,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if err := conn.Handler().HandleCloseAll(ctx, conn); err != nil {
		return err
	}
	return callback(ctx, &sqltypes.Result{CommandTag: "DEALLOCATE ALL"})
}

func (p *PreparedStatementPrimitive) GetTableGroup() string { return p.tableGroup }
func (p *PreparedStatementPrimitive) GetQuery() string      { return p.innerQuery }
func (p *PreparedStatementPrimitive) String() string {
	switch p.kind {
	case preparedStmtPrepare:
		return fmt.Sprintf("Prepare(%s)", p.stmtName)
	case preparedStmtExecute:
		return fmt.Sprintf("Execute(%s)", p.stmtName)
	case preparedStmtDeallocate:
		return fmt.Sprintf("Deallocate(%s)", p.stmtName)
	case preparedStmtDeallocateAll:
		return "Deallocate(ALL)"
	default:
		return "PreparedStatement(unknown)"
	}
}

var _ Primitive = (*PreparedStatementPrimitive)(nil)

// ExtractExecuteParams converts EXECUTE statement parameters from AST expression
// nodes to byte arrays for portal creation. Only literal constant values (A_Const)
// are supported.
func ExtractExecuteParams(stmt *ast.ExecuteStmt) ([][]byte, error) {
	if stmt.Params == nil || stmt.Params.Len() == 0 {
		return nil, nil
	}

	params := make([][]byte, 0, stmt.Params.Len())
	for i, param := range stmt.Params.Items {
		constNode, ok := param.(*ast.A_Const)
		if !ok {
			return nil, fmt.Errorf("parameter %d: unsupported expression type %T; only literal values are supported", i+1, param)
		}

		if constNode.Isnull || constNode.Val == nil {
			params = append(params, nil)
			continue
		}

		val, err := constToBytes(constNode.Val)
		if err != nil {
			return nil, fmt.Errorf("parameter %d: %w", i+1, err)
		}
		params = append(params, val)
	}

	return params, nil
}

// ExtractInnerQuery extracts the SQL string of the inner query from a PrepareStmt.
func ExtractInnerQuery(stmt *ast.PrepareStmt) string {
	if stmt.Query == nil {
		return ""
	}
	return stmt.Query.SqlString()
}

// constToBytes converts an AST constant value to its text-format byte representation.
func constToBytes(val ast.Value) ([]byte, error) {
	switch v := val.(type) {
	case *ast.Integer:
		return []byte(strconv.Itoa(v.IVal)), nil
	case *ast.Float:
		return []byte(v.FVal), nil
	case *ast.String:
		return []byte(v.SVal), nil
	case *ast.Boolean:
		if v.BoolVal {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	default:
		return nil, fmt.Errorf("unsupported constant type %T", v)
	}
}
