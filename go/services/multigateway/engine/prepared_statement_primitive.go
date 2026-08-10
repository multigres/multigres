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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/pgsettings"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
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

	// ValueIsNull marks a literal NULL value in the PREPARE body. set_config is
	// not STRICT, so PostgreSQL resets the parameter and the gateway must track
	// a REMOVAL — without this flag the zero-valued Value ("") is
	// indistinguishable from set_config(name, '', false) and the tracker writes
	// an empty string the backend never had, which the next pool replay pushes
	// back as SET name = '' (rejected for most GUCs, silently empty for
	// search_path). Mirrors planner.setConfigCall.ValueIsNull; the direct
	// SELECT set_config(...) path carries the same information as a VAR_RESET
	// synthetic instead.
	ValueIsNull bool
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

	// innerQuery is the SQL body of PREPARE. sourceSQL is retained only for
	// translating SQL EXECUTE diagnostics back to the client's command text.
	innerQuery string
	sourceSQL  string

	// innerQueryOffset is the byte offset of innerQuery within the client's
	// PREPARE text. The backend validates the body alone, so its diagnostic
	// positions are body-relative; this shifts them back onto the statement the
	// client actually sent (see translatePrepareBodyPosition).
	innerQueryOffset int

	// paramTypes holds the parameter type OIDs for PREPARE. Entries not known
	// statically are resolved through PostgreSQL from paramTypeNames.
	paramTypes     []uint32
	paramTypeNames []PrepareParamType

	// executeStmt is the parsed EXECUTE statement. For SQL EXECUTE we preserve
	// the argument expressions verbatim and rewrite only the prepared-statement
	// name to the gateway canonical name, then let PostgreSQL evaluate/cast the
	// argument expressions itself.
	executeStmt *ast.ExecuteStmt

	// setConfigs are visible top-level set_config(...) calls found in the
	// prepared statement body. They are applied by PostgreSQL as part of EXECUTE;
	// the gateway mirrors session-scoped effects only after EXECUTE succeeds.
	setConfigs []SQLPreparedSetConfig

	// bodyOverride, when set, replaces the prepared statement's registered body
	// for the query sent to the backend. The planner sets it for an EXECUTE on an
	// UNPINNED session whose body carries a session-persisting set_config(...,
	// false): the override is the same body with each such call's is_local flipped
	// to true, so the pooled backend reverts it instead of persisting it (the
	// gateway map + pool-rotation replay carry the value, mirroring an unpinned
	// SET). On a pinned session the override is nil and the body runs verbatim, so
	// the reserved backend genuinely carries the change. setConfigs (tracking) is
	// unaffected — the value is still recorded either way.
	bodyOverride *query.PreparedStatement
}

// PrepareParamType retains the SQL spelling and source location of an explicit
// PREPARE parameter type that PostgreSQL must resolve.
type PrepareParamType struct {
	Name     string
	Location int
}

// NewPreparePrimitive creates a primitive for PREPARE name AS query.
// innerQueryOffset is innerQuery's byte offset within the client's PREPARE text.
func NewPreparePrimitive(tableGroup, stmtName, innerQuery string, innerQueryOffset int, paramTypes []uint32, paramTypeNames []PrepareParamType) *PreparedStatementPrimitive {
	return &PreparedStatementPrimitive{
		kind:             preparedStmtPrepare,
		innerQueryOffset: innerQueryOffset,
		tableGroup:       tableGroup,
		stmtName:         stmtName,
		innerQuery:       innerQuery,
		paramTypes:       paramTypes,
		paramTypeNames:   paramTypeNames,
	}
}

// NewExecutePrimitive creates a primitive for EXECUTE name [(params)].
// bodyOverride is nil for the verbatim (pinned) case; the planner passes a
// rewritten body for the unpinned persisting-set_config case (see bodyOverride).
func NewExecutePrimitive(tableGroup, sourceSQL string, stmt *ast.ExecuteStmt, setConfigs []SQLPreparedSetConfig, bodyOverride *query.PreparedStatement) *PreparedStatementPrimitive {
	return &PreparedStatementPrimitive{
		kind:         preparedStmtExecute,
		tableGroup:   tableGroup,
		stmtName:     stmt.Name,
		sourceSQL:    sourceSQL,
		executeStmt:  stmt,
		setConfigs:   setConfigs,
		bodyOverride: bodyOverride,
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
		return p.executePrepare(ctx, exec, conn, state, callback)
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
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if conn.Handler().GetPreparedStatementInfo(conn.ConnectionID(), p.stmtName) != nil {
		return mterrors.NewDuplicatePreparedStatementError(p.stmtName)
	}

	paramTypes, err := p.resolveParamTypes(ctx, exec, conn, state)
	if err != nil {
		return err
	}
	if err := conn.Handler().HandleParse(ctx, conn, p.stmtName, p.innerQuery, paramTypes); err != nil {
		// Inside an explicit transaction HandleParse eagerly parses the body on
		// the backend, so its diagnostic positions are body-relative too.
		return translatePrepareBodyPosition(err, p.innerQueryOffset)
	}

	psi := conn.Handler().GetPreparedStatementInfo(conn.ConnectionID(), p.stmtName)
	if psi == nil {
		return fmt.Errorf("internal error: PREPARE %q was not registered", p.stmtName)
	}
	if _, err := exec.Describe(ctx, p.tableGroup, constants.DefaultShard, conn, state, nil, psi); err != nil {
		_ = conn.Handler().HandleClose(ctx, conn, 'S', p.stmtName)
		return translatePrepareBodyPosition(err, p.innerQueryOffset)
	}
	if marker, ok := conn.Handler().(server.PreparedStatementAliasProvider); ok {
		marker.MarkSQLPreparedStatementAlias(conn.ConnectionID(), p.stmtName)
	}
	return callback(ctx, &sqltypes.Result{CommandTag: "PREPARE"})
}

// translatePrepareBodyPosition shifts a diagnostic position reported against the
// PREPARE body onto the client's full statement text. PostgreSQL validates only
// the body (the gateway registers that alone), so its 1-based position counts
// from the body's first byte, while the client — and psql's LINE/caret echo —
// sees `PREPARE name (types) AS <body>`. Without the shift the caret lands short
// by exactly the prefix width.
func translatePrepareBodyPosition(err error, bodyOffset int) error {
	if bodyOffset <= 0 {
		return err
	}
	var diagnostic *mterrors.PgDiagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Position <= 0 {
		return err
	}
	// Copy: the diagnostic may be shared with other error wrappers.
	translated := *diagnostic
	translated.Position += int32(bodyOffset)
	return &translated
}

func (p *PreparedStatementPrimitive) resolveParamTypes(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
) ([]uint32, error) {
	resolved := append([]uint32(nil), p.paramTypes...)
	for i, paramType := range p.paramTypeNames {
		if resolved[i] != 0 {
			continue
		}

		var oidText string
		sql := "SELECT " + ast.QuoteStringLiteral(paramType.Name) + "::regtype::oid"
		err := exec.StreamExecute(ctx, conn, p.tableGroup, constants.DefaultShard, sql, nil, state, PlanExecInfo{}, true,
			func(_ context.Context, result *sqltypes.Result) error {
				for _, row := range result.StructuredRows() {
					if len(row.Values) > 0 && !row.Values[0].IsNull() {
						oidText = string(row.Values[0])
					}
				}
				return nil
			})
		if err != nil {
			var diagnostic *mterrors.PgDiagnostic
			if errors.As(err, &diagnostic) {
				copy := *diagnostic
				copy.Position = int32(paramType.Location + 1)
				return nil, &copy
			}
			return nil, err
		}
		oid, err := strconv.ParseUint(oidText, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("could not resolve PREPARE parameter type %q", paramType.Name)
		}
		resolved[i] = uint32(oid)
	}
	return resolved, nil
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

	// On an unpinned session with a persisting set_config in the body, the
	// planner supplies a rewritten body (is_local flipped to true) so the pooled
	// backend reverts it; otherwise the registered body runs verbatim.
	body := psi.PreparedStatement
	if p.bodyOverride != nil {
		body = p.bodyOverride
	}
	executeSQLPreparedStatement, err := BuildExecuteSQLPreparedStatement(p.executeStmt, p.executeStmt, body)
	if err != nil {
		return err
	}

	trackActions, callInfo, err := p.prepareSetConfigTracking(conn, state, portalInfo, info)
	if err != nil {
		return err
	}
	if err := exec.StreamExecute(ctx, conn, p.tableGroup, constants.DefaultShard, p.executeStmt.SqlString(), executeSQLPreparedStatement, state, callInfo, false, callback); err != nil {
		return TranslateSQLPreparedStatementError(err, p.stmtName, p.sourceSQL, p.executeStmt)
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
		if resolved.isReset {
			// EXECUTE arg was NULL: PostgreSQL cleared the parameter, so drop
			// the tracked entry rather than writing a value.
			name := resolved.name
			actions = append(actions, func() { resetTrackedSessionVariable(state, name) })
			continue
		}
		action, err := prepareTrackedSetAction(conn, state, resolved.name, resolved.value, resolved.isLocal)
		if err != nil {
			return nil, info, err
		}
		actions = append(actions, action)
	}
	return actions, info, nil
}

func (p *PreparedStatementPrimitive) resolvePreparedSetConfig(sc SQLPreparedSetConfig, portalInfo *preparedstatement.PortalInfo) (resolvedSetConfig, error) {
	isLocal := sc.IsLocalLiteralTrue

	// Resolve the value once, from whichever source carries it: a literal NULL
	// the planner recorded at PREPARE time, an EXECUTE argument (which may
	// itself be NULL), or a plain literal. Both NULL sources must converge here
	// — gating only on ValueParam would leave a literal NULL looking like an
	// explicit empty string and track a value PostgreSQL never set.
	value := sc.Value
	valueIsNull := sc.ValueIsNull
	if sc.ValueParam != nil {
		v, isNull, err := p.resolveExecuteArgAsTextOrNull(sc.ValueParam, portalInfo, "set_config value argument")
		if err != nil {
			return resolvedSetConfig{}, err
		}
		value, valueIsNull = v, isNull
	}

	// search_path is value-restricted (see pgsettings.RejectTempSchemaSearchPath):
	// the EXECUTE argument is the one place a prepared set_config value first
	// becomes known, so it is vetted here — before the untracked early return
	// below, mirroring resolveSetConfig on the wire-protocol path. The name is
	// always a literal in a PREPARE body (bound names are rejected at PREPARE
	// time by validateSQLPreparedSetConfigs). A NULL value resets search_path to
	// its server/admin-configured default, which can never carry a
	// client-injected pg_temp — nothing to vet.
	if strings.EqualFold(sc.Name, "search_path") && !valueIsNull {
		if err := pgsettings.RejectTempSchemaSearchPath(value); err != nil {
			return resolvedSetConfig{}, err
		}
	}

	if isLocal && !handler.IsGatewayManagedVariable(sc.Name) {
		return resolvedSetConfig{shouldTrack: false}, nil
	}

	if valueIsNull {
		// set_config(name, NULL, false) resets the parameter (it is not STRICT),
		// so track a removal. Gateway-managed variables stay fail-closed — no
		// per-variable reset primitive exists for them. (Unreachable in practice:
		// validateSQLPreparedSetConfigs rejects a GMV in a PREPARE body outright.)
		if handler.IsGatewayManagedVariable(sc.Name) {
			return resolvedSetConfig{}, mterrors.NewFeatureNotSupported(fmt.Sprintf(
				"set_config(%q, NULL, ...) is not supported under connection pooling; use RESET %s", sc.Name, sc.Name))
		}
		return resolvedSetConfig{name: sc.Name, isLocal: isLocal, shouldTrack: true, isReset: true}, nil
	}

	return resolvedSetConfig{name: sc.Name, value: value, isLocal: isLocal, shouldTrack: true}, nil
}

func (p *PreparedStatementPrimitive) resolveExecuteArgAsTextOrNull(pr *ast.ParamRef, portalInfo *preparedstatement.PortalInfo, callSite string) (string, bool, error) {
	arg, err := p.executeArg(pr, callSite)
	if err != nil {
		return "", false, err
	}
	return executeArgAsTextOrNull(arg, portalInfo, callSite)
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

// executeArgAsTextOrNull renders a SQL EXECUTE argument as text, reporting a
// NULL argument via the flag rather than an error so callers can mirror
// PostgreSQL's NULL semantics (for set_config, a reset to the default).
func executeArgAsTextOrNull(arg ast.Node, portalInfo *preparedstatement.PortalInfo, callSite string) (string, bool, error) {
	switch v := unwrapTypeCastNode(arg).(type) {
	case *ast.ParamRef:
		if portalInfo == nil {
			return "", false, mterrors.NewFeatureNotSupported(callSite + " must be a literal constant or a bound text parameter")
		}
		return preparedstatement.DecodeBindAsTextOrNull(portalInfo, v, callSite)
	case *ast.A_Const:
		if v.Isnull {
			return "", true, nil
		}
		return extractConstValue(v), false, nil
	case *ast.String:
		return v.SVal, false, nil
	case *ast.Integer:
		return strconv.Itoa(v.IVal), false, nil
	default:
		return "", false, mterrors.NewFeatureNotSupported(callSite + " must be a literal constant or a bound text parameter")
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

// ExtractParamTypes resolves built-in PREPARE parameter types statically and
// retains every SQL spelling for backend resolution of qualified, domain, and
// user-defined types.
func ExtractParamTypes(stmt *ast.PrepareStmt) ([]uint32, []PrepareParamType) {
	if stmt.Argtypes == nil || stmt.Argtypes.Len() == 0 {
		return nil, nil
	}
	oids := make([]uint32, 0, stmt.Argtypes.Len())
	types := make([]PrepareParamType, 0, stmt.Argtypes.Len())
	for _, item := range stmt.Argtypes.Items {
		tn, ok := item.(*ast.TypeName)
		if !ok || tn.Names == nil || tn.Names.Len() == 0 {
			oids = append(oids, 0)
			types = append(types, PrepareParamType{})
			continue
		}
		// The static OID fast path applies only to unqualified names and
		// explicit pg_catalog qualification. Any other schema can shadow a
		// builtin name (CREATE DOMAIN s.int4 AS text), so a qualified type is
		// left as 0 for backend regtype resolution.
		oid := ast.Oid(0)
		if tn.Names.Len() == 1 || qualifierIsPgCatalog(tn.Names) {
			lastItem := tn.Names.Items[tn.Names.Len()-1]
			name := ""
			if s, ok := lastItem.(*ast.String); ok {
				name = s.SVal
			}
			oid = ast.TypeNameToOid(name)
			if tn.ArrayBounds != nil && tn.ArrayBounds.Len() > 0 {
				oid = ast.ArrayTypeOid(oid)
			}
		}
		oids = append(oids, uint32(oid))
		types = append(types, PrepareParamType{Name: tn.SqlString(), Location: tn.Location()})
	}
	return oids, types
}

// ExtractParamTypeOids is retained for callers that only need static OIDs.
func ExtractParamTypeOids(stmt *ast.PrepareStmt) []uint32 {
	oids, _ := ExtractParamTypes(stmt)
	return oids
}

func qualifierIsPgCatalog(names *ast.NodeList) bool {
	if names.Len() != 2 {
		return false
	}
	s, ok := names.Items[0].(*ast.String)
	return ok && s.SVal == "pg_catalog"
}

// ExtractInnerQuery extracts the SQL string of the inner query from a PrepareStmt.
func ExtractInnerQuery(stmt *ast.PrepareStmt) string {
	if stmt.Query == nil {
		return ""
	}
	return stmt.Query.SqlString()
}
