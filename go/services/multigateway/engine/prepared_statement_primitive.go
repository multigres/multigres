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

// SQLPreparedSetConfig describes a top-level set_config(...) call inside a
// SQL-level PREPARE body. SQL EXECUTE resolves prepared-body $N references from
// the EXECUTE argument list, then tracks the resulting session state after the
// backend accepts the EXECUTE.
type SQLPreparedSetConfig struct {
	Name  string
	Value string

	ValueParam *ast.ParamRef

	IsLocalLiteralTrue bool
}

// PreparedStatementPrimitive handles SQL PREPARE, EXECUTE, and DEALLOCATE
// through gateway-managed prepared-statement consolidation.
//
// Key behaviors:
//   - PREPARE: Calls HandleParse to register in the consolidator.
//   - EXECUTE: Sends a SQL EXECUTE prefix/suffix template plus prepared
//     statement metadata so the multipooler can resolve a pooler-consolidated
//     backend name and PostgreSQL can evaluate argument expressions.
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

	// setConfigs are visible top-level set_config(...) calls found in the
	// prepared statement body. They are applied by PostgreSQL as part of EXECUTE;
	// the gateway mirrors session-scoped effects only after EXECUTE succeeds.
	setConfigs []SQLPreparedSetConfig
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
func NewExecutePrimitive(tableGroup string, stmt *ast.ExecuteStmt, setConfigs []SQLPreparedSetConfig) *PreparedStatementPrimitive {
	return &PreparedStatementPrimitive{
		kind:        preparedStmtExecute,
		tableGroup:  tableGroup,
		stmtName:    stmt.Name,
		executeStmt: stmt,
		setConfigs:  setConfigs,
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
	state *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch p.kind {
	case preparedStmtPrepare:
		return p.executePrepare(ctx, conn, callback)
	case preparedStmtExecute:
		return p.executeExecute(ctx, exec, conn, state, nil, info, callback)
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

// executeExecute sends the SQL-level EXECUTE wrapper as prefix/suffix plus the
// prepared-statement metadata. The multipooler resolves the backend statement
// name through its pooler-level consolidator (ppstmt*) and materializes the SQL
// before sending it to PostgreSQL, which evaluates EXECUTE arguments verbatim,
// including casts, arrays, functions, and other expressions.
func (p *PreparedStatementPrimitive) executeExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	psi := conn.Handler().GetPreparedStatementInfo(conn.ConnectionID(), p.stmtName)
	if psi == nil {
		return mterrors.NewInvalidPreparedStatementError(p.stmtName)
	}
	if p.executeStmt == nil {
		return fmt.Errorf("internal error: execute statement AST missing for statement \"%s\"", p.stmtName)
	}

	executeSQLPreparedStatement, err := BuildExecuteSQLPreparedStatement(p.executeStmt, p.executeStmt, psi.PreparedStatement)
	if err != nil {
		return err
	}

	trackActions, callInfo, err := p.prepareSetConfigTracking(conn, state, portalInfo, info)
	if err != nil {
		return err
	}
	if err := exec.StreamExecute(ctx, conn, p.tableGroup, constants.DefaultShard, p.executeStmt.SqlString(), executeSQLPreparedStatement, state, callInfo, callback); err != nil {
		return err
	}
	for _, action := range trackActions {
		action()
	}
	return nil
}

func (p *PreparedStatementPrimitive) prepareSetConfigTracking(
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	info PlanExecInfo,
) ([]func(), PlanExecInfo, error) {
	if len(p.setConfigs) == 0 {
		return nil, info, nil
	}

	var actions []func()
	for _, sc := range p.setConfigs {
		resolved, err := p.resolvePreparedSetConfig(sc, portalInfo)
		if err != nil {
			return nil, info, err
		}
		if !resolved.shouldTrack {
			continue
		}
		action, preview, err := prepareTrackedSetActionWithBackendPreview(conn, state, resolved.name, resolved.value, resolved.isLocal)
		if err != nil {
			return nil, info, err
		}
		actions = append(actions, action)
		if preview != nil {
			if !info.HasPostQuerySessionSettings {
				info.PostQuerySessionSettings = state.GetSessionSettings()
				info.HasPostQuerySessionSettings = true
			}
			info.PostQuerySessionSettings = preview(info.PostQuerySessionSettings)
		}
	}
	return actions, info, nil
}

func (p *PreparedStatementPrimitive) resolvePreparedSetConfig(sc SQLPreparedSetConfig, portalInfo *preparedstatement.PortalInfo) (resolvedSetConfig, error) {
	isLocal := sc.IsLocalLiteralTrue
	if isLocal && !handler.IsGatewayManagedVariable(sc.Name) {
		return resolvedSetConfig{shouldTrack: false}, nil
	}

	value := sc.Value
	if sc.ValueParam != nil {
		v, err := p.resolveExecuteArgAsText(sc.ValueParam, portalInfo, "set_config value argument")
		if err != nil {
			return resolvedSetConfig{}, err
		}
		value = v
	}
	return resolvedSetConfig{name: sc.Name, value: value, isLocal: isLocal, shouldTrack: true}, nil
}

func (p *PreparedStatementPrimitive) resolveExecuteArgAsText(pr *ast.ParamRef, portalInfo *preparedstatement.PortalInfo, callSite string) (string, error) {
	arg, err := p.executeArg(pr, callSite)
	if err != nil {
		return "", err
	}
	return executeArgAsText(arg, portalInfo, callSite)
}

func (p *PreparedStatementPrimitive) executeArg(pr *ast.ParamRef, callSite string) (ast.Node, error) {
	if p.executeStmt == nil || p.executeStmt.Params == nil || pr.Number <= 0 || pr.Number > p.executeStmt.Params.Len() {
		return nil, mterrors.NewFeatureNotSupported(fmt.Sprintf("%s references prepared parameter $%d but EXECUTE supplies %d argument(s)", callSite, pr.Number, executeArgCount(p.executeStmt)))
	}
	return p.executeStmt.Params.Items[pr.Number-1], nil
}

func executeArgCount(stmt *ast.ExecuteStmt) int {
	if stmt == nil || stmt.Params == nil {
		return 0
	}
	return stmt.Params.Len()
}

func executeArgAsText(arg ast.Node, portalInfo *preparedstatement.PortalInfo, callSite string) (string, error) {
	switch v := unwrapTypeCastNode(arg).(type) {
	case *ast.ParamRef:
		if portalInfo == nil {
			return "", mterrors.NewFeatureNotSupported(callSite + " must be a literal constant or a bound text parameter")
		}
		return preparedstatement.DecodeBindAsText(portalInfo, v, callSite)
	case *ast.A_Const:
		if v.Isnull {
			return "", mterrors.NewFeatureNotSupported(callSite + " cannot be NULL")
		}
		return extractConstValue(v), nil
	case *ast.String:
		return v.SVal, nil
	case *ast.Integer:
		return strconv.Itoa(v.IVal), nil
	default:
		return "", mterrors.NewFeatureNotSupported(callSite + " must be a literal constant or a bound text parameter")
	}
}

func unwrapTypeCastNode(n ast.Node) ast.Node {
	for {
		tc, ok := n.(*ast.TypeCast)
		if !ok {
			return n
		}
		n = tc.Arg
	}
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
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch p.kind {
	case preparedStmtExecute:
		return p.executeExecute(ctx, exec, conn, state, portalInfo, info, callback)
	default:
		return p.StreamExecute(ctx, exec, conn, state, nil, info, callback)
	}
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
