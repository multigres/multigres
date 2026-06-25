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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
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

// PreparedStatementPrimitive handles SQL PREPARE, EXECUTE, and DEALLOCATE
// through gateway-managed prepared-statement consolidation.
//
// Key behaviors:
//   - PREPARE: Calls HandleParse to register in the consolidator.
//   - EXECUTE: Rewrites the prepared-statement name to the canonical name and
//     runs SQL EXECUTE verbatim so PostgreSQL evaluates argument expressions.
//   - DEALLOCATE: Calls HandleClose to remove the user-facing mapping.
//   - DEALLOCATE ALL: Clears all user-facing mappings for this connection.
type PreparedStatementPrimitive struct {
	kind       preparedStmtKind
	tableGroup string

	// stmtName is the prepared statement name (used by all kinds).
	stmtName string

	// innerQuery is the SQL body of the PREPARE statement.
	innerQuery string

	// paramTypes holds the parameter type OIDs for PREPARE (from SQL type names).
	paramTypes []uint32

	// executeStmt is the parsed EXECUTE statement. For SQL EXECUTE we preserve
	// the argument expressions verbatim and rewrite only the prepared-statement
	// name to the gateway canonical name, then let PostgreSQL evaluate/cast the
	// argument expressions itself.
	executeStmt *ast.ExecuteStmt
}

// NewPreparePrimitive creates a primitive for PREPARE name AS query.
func NewPreparePrimitive(tableGroup, stmtName, innerQuery string, paramTypes []uint32) *PreparedStatementPrimitive {
	return &PreparedStatementPrimitive{
		kind:       preparedStmtPrepare,
		tableGroup: tableGroup,
		stmtName:   stmtName,
		innerQuery: innerQuery,
		paramTypes: paramTypes,
	}
}

// NewExecutePrimitive creates a primitive for EXECUTE name [(params)].
func NewExecutePrimitive(tableGroup string, stmt *ast.ExecuteStmt) *PreparedStatementPrimitive {
	return &PreparedStatementPrimitive{
		kind:        preparedStmtExecute,
		tableGroup:  tableGroup,
		stmtName:    stmt.Name,
		executeStmt: stmt,
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
	_ []*ast.A_Const,
	_ PlanExecInfo,
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
//
// Unlike the extended Parse message, SQL-level PREPARE must reject a name that is
// already in use on this session. HandleParse silently replaces existing entries
// (to tolerate Parse retries after a failed Describe), so we check for the name
// here before delegating.
func (p *PreparedStatementPrimitive) executePrepare(
	ctx context.Context,
	conn *server.Conn,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if conn.Handler().GetPreparedStatementInfo(conn.ConnectionID(), p.stmtName) != nil {
		return mterrors.NewDuplicatePreparedStatementError(p.stmtName)
	}
	if err := conn.Handler().HandleParse(ctx, conn, p.stmtName, p.innerQuery, p.paramTypes); err != nil {
		return err
	}
	return callback(ctx, &sqltypes.Result{CommandTag: "PREPARE"})
}

// executeExecute rewrites the SQL-level EXECUTE to reference the gateway
// canonical prepared-statement name, attaches the prepared-statement metadata,
// and sends the rewritten SQL through StreamExecute. This preserves the
// consolidation path while letting PostgreSQL evaluate EXECUTE arguments
// verbatim, including casts, arrays, functions, and other expressions.
func (p *PreparedStatementPrimitive) executeExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	psi := conn.Handler().GetPreparedStatementInfo(conn.ConnectionID(), p.stmtName)
	if psi == nil {
		return mterrors.NewInvalidPreparedStatementError(p.stmtName)
	}
	if p.executeStmt == nil {
		return fmt.Errorf("internal error: execute statement AST missing for statement \"%s\"", p.stmtName)
	}

	rewrittenSQL := p.executeSQLWithName(psi.Name)
	return exec.StreamExecute(ctx, conn, p.tableGroup, constants.DefaultShard, rewrittenSQL, psi.PreparedStatement, state, PlanExecInfo{}, callback)
}

func (p *PreparedStatementPrimitive) executeSQLWithName(name string) string {
	originalName := p.executeStmt.Name
	p.executeStmt.Name = name
	defer func() { p.executeStmt.Name = originalName }()
	return p.executeStmt.SqlString()
}

// executeDeallocate uses HandleClose with typ 'D' which errors on nonexistent
// statements, matching PostgreSQL's DEALLOCATE behavior.
func (p *PreparedStatementPrimitive) executeDeallocate(
	ctx context.Context,
	conn *server.Conn,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if err := conn.Handler().HandleClose(ctx, conn, 'D', p.stmtName); err != nil {
		return err
	}
	return callback(ctx, &sqltypes.Result{CommandTag: "DEALLOCATE"})
}

func (p *PreparedStatementPrimitive) executeDeallocateAll(
	ctx context.Context,
	conn *server.Conn,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if err := conn.Handler().HandleClose(ctx, conn, 'A', ""); err != nil {
		return err
	}
	return callback(ctx, &sqltypes.Result{CommandTag: "DEALLOCATE ALL"})
}

// PortalStreamExecute satisfies the Primitive interface for the
// extended-protocol path. PREPARE/EXECUTE/DEALLOCATE are gateway-managed
// identically on both protocols, so the portal binds carry no extra meaning
// here — the EXECUTE form already runs its own internal portal-style flow,
// reusing HandleBind / PortalStreamExecute on the backend. Delegate.
func (p *PreparedStatementPrimitive) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return p.StreamExecute(ctx, exec, conn, state, nil, PlanExecInfo{}, callback)
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

// ExtractParamTypeOids resolves the Argtypes from a PREPARE statement to OIDs.
// Returns nil if there are no argument types. Unrecognized type names are passed
// as 0 (unspecified), letting the backend infer them.
func ExtractParamTypeOids(stmt *ast.PrepareStmt) []uint32 {
	if stmt.Argtypes == nil || stmt.Argtypes.Len() == 0 {
		return nil
	}
	oids := make([]uint32, 0, stmt.Argtypes.Len())
	for _, item := range stmt.Argtypes.Items {
		tn, ok := item.(*ast.TypeName)
		if !ok || tn.Names == nil || tn.Names.Len() == 0 {
			oids = append(oids, 0)
			continue
		}
		// Use the last name component (e.g., "pg_catalog"."int4" → "int4").
		lastItem := tn.Names.Items[tn.Names.Len()-1]
		name := ""
		if s, ok := lastItem.(*ast.String); ok {
			name = s.SVal
		}
		oids = append(oids, uint32(ast.TypeNameToOid(name)))
	}
	return oids
}

// ExtractInnerQuery extracts the SQL string of the inner query from a PrepareStmt.
func ExtractInnerQuery(stmt *ast.PrepareStmt) string {
	if stmt.Query == nil {
		return ""
	}
	return stmt.Query.SqlString()
}
