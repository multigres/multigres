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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// Cluster views are relations the gateway serves from its own state:
// `multigres.multigateways` (the gateways registered in the topology) and
// `multigres.multipoolers` (the poolers this gateway tracks, with the role and
// health it observes on their health streams). PostgreSQL has no such
// relations, so each reference is folded into a subquery over the current
// snapshot — like multigres.version() is folded into a literal, except the
// data changes, so the fold runs at execution time rather than parse time.
// A parse-time fold would freeze the rows into the plan cache and into
// prepared statements.
//
// The subquery is `SELECT * FROM json_to_recordset(<snapshot>) AS _(col type,
// ...)`: the statement text stays the same for every snapshot, and the rows
// travel as one JSON value — a literal in the simple protocol, an extra bind
// parameter in the extended protocol. Folding literally into VALUES would
// change the text on every heartbeat and leave a new prepared statement on
// the backend per execution.
//
// Folding means PostgreSQL does the rest: WHERE, JOIN, ORDER BY, aggregates,
// EXPLAIN and the extended protocol all work unchanged. Only references
// qualified with the `multigres` schema are recognized: the gateway does not
// know a session's search_path, so an unqualified name goes to the backend as
// written (and fails there).

// ClusterViewColumn is one output column of a cluster view.
type ClusterViewColumn struct {
	Name string
	// Type is the PostgreSQL type of the column.
	Type string
}

// ClusterView describes a cluster view's name and columns.
type ClusterView struct {
	Name    string
	Columns []ClusterViewColumn

	templateOnce sync.Once
	template     *ast.SelectStmt
}

// ClusterViews lists the views, keyed by relation name.
var ClusterViews = map[string]*ClusterView{
	constants.MultigresMultigatewaysView: {
		Name: constants.MultigresMultigatewaysView,
		Columns: []ClusterViewColumn{
			{"name", "text"},
			{"cell", "text"},
			{"pid_prefix", "integer"},
			{"is_self", "boolean"},
		},
	},
	constants.MultigresMultipoolersView: {
		Name: constants.MultigresMultipoolersView,
		Columns: []ClusterViewColumn{
			{"name", "text"},
			{"cell", "text"},
			{"database", "text"},
			{"table_group", "text"},
			{"shard", "text"},
			{"routing_role", "text"},
			{"leadership", "text"},
			{"serving_status", "text"},
			{"lifecycle", "text"},
			{"lifecycle_reason", "text"},
			{"lifecycle_updated", "timestamptz"},
			{"replication_lag", "interval"},
			{"connected", "boolean"},
			{"health_last_response", "timestamptz"},
			{"discovery_state", "text"},
			{"last_change", "timestamptz"},
		},
	},
}

// ClusterViewSource provides a view's rows at execution time. Each row holds
// one Go value per column, in column order: string, bool, int/int32/int64/
// uint32, time.Time, time.Duration, or nil for NULL.
type ClusterViewSource interface {
	ClusterViewRows(ctx context.Context, view string) ([][]any, error)
}

// MayReferenceClusterView is the cheap pre-check, as for multigres.version():
// a statement that never names the schema can't reference a view. Callers
// gate the AST walks below on it.
func MayReferenceClusterView(sql string) bool {
	return strings.Contains(strings.ToLower(sql), constants.MultigresSchema)
}

// clusterViewOf returns the view rv names, or nil. Names are compared exactly:
// the parser has already lowercased unquoted identifiers, and a quoted
// mixed-case name is a different relation to PostgreSQL.
func clusterViewOf(rv *ast.RangeVar) *ClusterView {
	if rv == nil || rv.SchemaName != constants.MultigresSchema {
		return nil
	}
	return ClusterViews[rv.RelName]
}

// IsClusterViewRelation reports whether rv names a cluster view.
func IsClusterViewRelation(rv *ast.RangeVar) bool {
	return clusterViewOf(rv) != nil
}

// fromPositions tracks, during one AST walk, the lists in which a RangeVar is
// a FROM item: SELECT ... FROM, UPDATE ... FROM, DELETE ... USING, either
// side of a JOIN, and MERGE ... USING. A RangeVar anywhere else (the target
// of a DML statement, DDL, TRUNCATE, LOCK, GRANT, ...) is not a place a
// subquery can stand in.
type fromPositions map[*ast.NodeList]bool

// note records the FROM list of n, if it has one. The walker visits a
// statement before its FROM items, so the list is known by the time they
// are reached.
func (f fromPositions) note(n ast.Node) {
	switch s := n.(type) {
	case *ast.SelectStmt:
		f[s.FromClause] = true
	case *ast.UpdateStmt:
		f[s.FromClause] = true
	case *ast.DeleteStmt:
		f[s.UsingClause] = true
	}
}

// view returns the cluster view the cursor's node references from a FROM
// position, or nil.
func (f fromPositions) view(cursor *ast.Cursor) (*ast.RangeVar, *ClusterView) {
	rv, ok := cursor.Node().(*ast.RangeVar)
	if !ok {
		return nil, nil
	}
	switch p := cursor.Parent().(type) {
	case *ast.JoinExpr:
	case *ast.MergeStmt:
		if p.SourceRelation != ast.Node(rv) {
			return nil, nil
		}
	case *ast.NodeList:
		if !f[p] {
			return nil, nil
		}
	default:
		return nil, nil
	}
	view := clusterViewOf(rv)
	if view == nil {
		return nil, nil
	}
	return rv, view
}

// ContainsClusterView reports whether stmt references a cluster view in a
// FROM position.
func ContainsClusterView(stmt ast.Stmt) bool {
	found := false
	from := fromPositions{}
	ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		from.note(cursor.Node())
		if _, view := from.view(cursor); view != nil {
			found = true
		}
		return !found
	}, nil)
	return found
}

// snapshotNode supplies the JSON snapshot expression for the i-th view
// reference folded (0-based): a literal, or a bind parameter.
type snapshotNode func(i int, view *ClusterView) (ast.Node, error)

// foldClusterViews replaces every FROM-position cluster-view reference in
// stmt with its subquery, taking each one's snapshot expression from data. It
// mutates stmt; callers fold a clone when the original must survive.
func foldClusterViews(stmt ast.Stmt, data snapshotNode) error {
	var err error
	from := fromPositions{}
	i := 0
	ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		if err != nil {
			return false
		}
		from.note(cursor.Node())
		rv, view := from.view(cursor)
		if view == nil {
			return true
		}
		var snapshot, sub ast.Node
		if snapshot, err = data(i, view); err != nil {
			return false
		}
		i++
		if sub, err = view.subselect(rv.Alias, snapshot); err != nil {
			return false
		}
		cursor.Replace(sub)
		return false
	}, nil)
	return err
}

// jsonLiteral renders a snapshot as a string literal.
func jsonLiteral(rows [][]any, view *ClusterView) (ast.Node, error) {
	text, err := view.encode(rows)
	if err != nil {
		return nil, err
	}
	return ast.NewA_Const(ast.NewString(text), 0), nil
}

// ClusterViewDescribeSQL renders stmt with every cluster view folded over an
// empty snapshot literal: valid backend SQL with the view's exact column
// names and types and no extra bind parameter, so the backend can Describe it
// (and prepare it, inside an explicit transaction). stmt is not modified.
func ClusterViewDescribeSQL(stmt ast.Stmt) (string, error) {
	folded := ast.CloneStmt(stmt)
	err := foldClusterViews(folded, func(_ int, view *ClusterView) (ast.Node, error) {
		return jsonLiteral(nil, view)
	})
	if err != nil {
		return "", err
	}
	return folded.SqlString(), nil
}

// subselectTemplate returns the view's subquery with `$1` standing for the
// snapshot: `SELECT * FROM json_to_recordset($1) AS _(col type, ...)`. Parsed
// once; callers clone it.
func (v *ClusterView) subselectTemplate() *ast.SelectStmt {
	v.templateOnce.Do(func() {
		defs := make([]string, len(v.Columns))
		for i, c := range v.Columns {
			defs[i] = ast.QuoteIdentifier(c.Name) + " " + c.Type
		}
		sql := "SELECT * FROM json_to_recordset($1) AS _(" + strings.Join(defs, ", ") + ")"
		stmts, err := parser.ParseSQL(sql)
		if err != nil || len(stmts) != 1 {
			panic(fmt.Sprintf("cluster view %s: template does not parse: %v", v.Name, err))
		}
		v.template = stmts[0].(*ast.SelectStmt)
	})
	return v.template
}

// subselect builds the subquery standing in for one reference, over the
// given snapshot expression. The inner SELECT names and types every column;
// the user's alias (table and column names) goes on the outer subquery, so a
// partial column alias list behaves as it would against a real relation —
// the remaining columns keep their names. Without an alias the view's own
// name labels the subquery.
func (v *ClusterView) subselect(alias *ast.Alias, snapshot ast.Node) (*ast.RangeSubselect, error) {
	sel := ast.CloneRefOfSelectStmt(v.subselectTemplate())
	replaced := false
	ast.Rewrite(sel, func(cursor *ast.Cursor) bool {
		if pr, ok := cursor.Node().(*ast.ParamRef); ok && pr.Number == 1 {
			cursor.Replace(snapshot)
			replaced = true
			return false
		}
		return true
	}, nil)
	if !replaced {
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
			fmt.Sprintf("internal error building multigres.%s: snapshot placeholder missing", v.Name), "")
	}
	aliasName := v.Name
	var colNames *ast.NodeList
	if alias != nil {
		if alias.AliasName != "" {
			aliasName = alias.AliasName
		}
		colNames = alias.ColNames
	}
	return ast.NewRangeSubselect(false, sel, ast.NewAlias(aliasName, colNames)), nil
}

// encode renders rows as the JSON array of objects json_to_recordset reads.
// Durations become interval input text; times marshal as RFC 3339.
func (v *ClusterView) encode(rows [][]any) (string, error) {
	objs := make([]map[string]any, len(rows))
	for r, row := range rows {
		if len(row) != len(v.Columns) {
			return "", mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
				fmt.Sprintf("internal error building multigres.%s: row has %d values for %d columns", v.Name, len(row), len(v.Columns)), "")
		}
		obj := make(map[string]any, len(row))
		for i, c := range v.Columns {
			val := row[i]
			if d, ok := val.(time.Duration); ok {
				val = fmt.Sprintf("%d microseconds", d.Microseconds())
			}
			obj[c.Name] = val
		}
		objs[r] = obj
	}
	b, err := json.Marshal(objs)
	if err != nil {
		return "", mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
			"internal error encoding multigres."+v.Name, err.Error())
	}
	return string(b), nil
}

// ClusterViewRoute routes a statement that references cluster views. At each
// execution it folds the current snapshot into a copy of the route's AST, so
// the plan (and the plan cache entry) carries no data and every execution
// sees fresh rows.
type ClusterViewRoute struct {
	route  *Route
	source ClusterViewSource
}

// NewClusterViewRoute wraps route, whose NormalizedAST references at least one
// cluster view, with the source its rows come from.
func NewClusterViewRoute(route *Route, source ClusterViewSource) *ClusterViewRoute {
	return &ClusterViewRoute{route: route, source: source}
}

// fold returns a copy of the wrapped route with the cluster views folded
// into its AST and query text, each over the expression data returns.
func (r *ClusterViewRoute) fold(data snapshotNode) (*Route, error) {
	if r.source == nil {
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
			"internal error: cluster views have no source on this gateway", "")
	}
	folded := ast.CloneStmt(r.route.NormalizedAST)
	if err := foldClusterViews(folded, data); err != nil {
		return nil, err
	}
	route := *r.route
	route.NormalizedAST = folded
	route.Query = folded.SqlString()
	return &route, nil
}

// StreamExecute folds each view's snapshot in as a JSON literal and routes
// the statement (simple protocol, where nothing is prepared).
func (r *ClusterViewRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	route, err := r.fold(func(_ int, view *ClusterView) (ast.Node, error) {
		rows, err := r.source.ClusterViewRows(ctx, view.Name)
		if err != nil {
			return nil, err
		}
		return jsonLiteral(rows, view)
	})
	if err != nil {
		return err
	}
	return route.StreamExecute(ctx, exec, conn, state, bindVars, info, callback)
}

// PortalStreamExecute serves the extended protocol. Each view's snapshot is
// appended to the portal as one extra text bind parameter, numbered past the
// client's, and the portal is forwarded with the folded text as its prepared
// statement — the way Route forwards any rewritten query. The folded text is
// the same for every snapshot, so the backend prepares it once per statement;
// it binds the client's parameters and honours the result formats the client
// asked for (drivers request binary results for integer, boolean and
// timestamp columns, which a simple-protocol execution could not satisfy).
func (r *ClusterViewRoute) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	psi := portalInfo.PreparedStatementInfo
	params := sqltypes.ParamsFromProto(portalInfo.Portal.GetParamLengths(), portalInfo.Portal.GetParamValues())
	base := len(params)
	ast.Rewrite(r.route.NormalizedAST, func(cursor *ast.Cursor) bool {
		if pr, ok := cursor.Node().(*ast.ParamRef); ok {
			base = max(base, pr.Number)
		}
		return true
	}, nil)

	var snapshots [][]byte
	route, err := r.fold(func(i int, view *ClusterView) (ast.Node, error) {
		rows, err := r.source.ClusterViewRows(ctx, view.Name)
		if err != nil {
			return nil, err
		}
		text, err := view.encode(rows)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, []byte(text))
		return ast.NewParamRef(base+i+1, 0), nil
	})
	if err != nil {
		return err
	}

	// Append the snapshots in text format. The client's parameters are
	// forwarded as sent: if they don't match what the statement needs, the
	// backend reports the mismatch as it would without the fold. Format codes
	// follow the Bind message rules: none means all text; one applies to
	// every parameter and must be expanded before a text parameter is
	// appended.
	formats := portalInfo.Portal.GetParamFormats()
	if len(formats) == 1 && len(params) != 1 {
		code := formats[0]
		formats = make([]int32, len(params))
		for i := range formats {
			formats[i] = code
		}
	}
	if len(formats) > 0 {
		for range snapshots {
			formats = append(formats, 0)
		}
	}
	params = append(params, snapshots...)
	lengths, values := sqltypes.ParamsToProto(params)

	foldedPSI, err := preparedstatement.NewPreparedStatementInfo(&query.PreparedStatement{
		Name:       psi.GetName(),
		Query:      route.Query,
		ParamTypes: psi.GetParamTypes(),
	})
	if err != nil {
		return err
	}
	foldedPSI.ForceReparse = state.ConsumeReparsePending(psi.GetName())
	portal := &query.Portal{
		Name:                  portalInfo.Portal.GetName(),
		PreparedStatementName: portalInfo.Portal.GetPreparedStatementName(),
		ParamLengths:          lengths,
		ParamValues:           values,
		ParamFormats:          formats,
		ResultFormats:         portalInfo.Portal.GetResultFormats(),
	}
	return exec.PortalStreamExecute(ctx, route.TableGroup, route.Shard, conn, state,
		preparedstatement.NewPortalInfo(foldedPSI, portal), maxRows, includeDescribe, info, route.KeepStructured,
		captureReportedSettings(info, callback))
}

// GetTableGroup returns the wrapped route's tablegroup.
func (r *ClusterViewRoute) GetTableGroup() string { return r.route.GetTableGroup() }

// GetQuery returns the wrapped route's (unfolded) SQL.
func (r *ClusterViewRoute) GetQuery() string { return r.route.GetQuery() }

// String returns a description for debugging.
func (r *ClusterViewRoute) String() string {
	return fmt.Sprintf("ClusterViewRoute(%s)", r.route.GetQuery())
}

var _ Primitive = (*ClusterViewRoute)(nil)
