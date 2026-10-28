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
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

func parseOneStmt(t *testing.T, sql string) ast.Stmt {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	return stmts[0]
}

// fixedSource serves canned rows per view.
type fixedSource struct {
	rows map[string][][]any
	err  error
}

func (s fixedSource) ClusterViewRows(_ context.Context, view string) ([][]any, error) {
	return s.rows[view], s.err
}

// capturingExec records the SQL the primitive routed.
type capturingExec struct {
	mockIExecute
	sqls []string
}

func (c *capturingExec) StreamExecute(ctx context.Context, conn *server.Conn, tableGroup, shard, sql string,
	ps *query.PreparedStatement, state *handler.MultigatewayConnectionState, info PlanExecInfo,
	keepStructured bool, callback func(context.Context, *sqltypes.Result) error,
) error {
	c.sqls = append(c.sqls, sql)
	return c.mockIExecute.StreamExecute(ctx, conn, tableGroup, shard, sql, ps, state, info, keepStructured, callback)
}

const gatewaysColDefs = `_ (name TEXT, cell TEXT, pid_prefix INT, is_self BOOLEAN)`

func TestContainsClusterView(t *testing.T) {
	for sql, want := range map[string]bool{
		"SELECT * FROM multigres.multipoolers":                                                 true,
		"SELECT * FROM MULTIGRES.MultiGateways g":                                              true, // unquoted: the parser lowercases
		"SELECT * FROM t JOIN multigres.multipoolers p ON p.name = t.n":                        true,
		"SELECT (SELECT count(*) FROM multigres.multigateways)":                                true,
		"EXPLAIN SELECT * FROM multigres.multipoolers":                                         true,
		"INSERT INTO t SELECT name FROM multigres.multipoolers":                                true,
		"UPDATE t SET x = 1 FROM multigres.multipoolers p WHERE p.name = t.n":                  true,
		"DELETE FROM t USING multigres.multigateways g WHERE g.name = t.n":                     true,
		"WITH p AS (SELECT * FROM multigres.multipoolers) SELECT * FROM p":                     true,
		"MERGE INTO t USING multigres.multipoolers p ON t.n = p.name WHEN MATCHED THEN DELETE": true,
		"SELECT * FROM multipoolers":                                                           false, // unqualified: search_path unknown
		"SELECT * FROM other.multipoolers":                                                     false,
		"SELECT * FROM multigres.backend_vpid":                                                 false, // a real sidecar table
		`SELECT * FROM multigres."MultiPoolers"`:                                               false, // quoted: a different relation
		"INSERT INTO multigres.multipoolers VALUES (1)":                                        false, // not a FROM position
		"CREATE TABLE multigres.multipoolers (a int)":                                          false,
		"TRUNCATE multigres.multipoolers":                                                      false,
		"LOCK TABLE multigres.multipoolers IN ACCESS SHARE MODE":                               false,
		"GRANT SELECT ON multigres.multipoolers TO app":                                        false,
	} {
		assert.Equal(t, want, ContainsClusterView(parseOneStmt(t, sql)), sql)
	}
}

func TestFoldClusterViews(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 500, time.UTC)
	gw := [][]any{{"gw1", "zone1", uint32(7), true}}
	literal := func(rows [][]any) snapshotNode {
		return func(_ int, view *ClusterView) (ast.Node, error) { return jsonLiteral(rows, view) }
	}

	t.Run("a reference becomes a typed json_to_recordset subquery over the snapshot", func(t *testing.T) {
		stmt := parseOneStmt(t, "SELECT name, is_self FROM multigres.multigateways WHERE pid_prefix = $1")
		require.NoError(t, foldClusterViews(stmt, literal(gw)))
		assert.Equal(t, `SELECT name, is_self FROM (SELECT * FROM json_to_recordset('[{"cell":"zone1","is_self":true,"name":"gw1","pid_prefix":7}]') AS `+gatewaysColDefs+`) AS multigateways WHERE pid_prefix = $1`, stmt.SqlString())
	})

	t.Run("the statement text does not depend on the snapshot", func(t *testing.T) {
		a := parseOneStmt(t, "SELECT * FROM multigres.multigateways")
		b := parseOneStmt(t, "SELECT * FROM multigres.multigateways")
		param := func(_ int, _ *ClusterView) (ast.Node, error) { return ast.NewParamRef(1, 0), nil }
		require.NoError(t, foldClusterViews(a, param))
		require.NoError(t, foldClusterViews(b, param))
		assert.Equal(t, a.SqlString(), b.SqlString())
		assert.Equal(t, "SELECT * FROM (SELECT * FROM json_to_recordset($1) AS "+gatewaysColDefs+") AS multigateways", a.SqlString())
	})

	t.Run("the user's alias and a partial column alias list are kept on the outer subquery", func(t *testing.T) {
		stmt := parseOneStmt(t, "SELECT g.n, g.cell FROM multigres.multigateways AS g(n)")
		require.NoError(t, foldClusterViews(stmt, literal(nil)))
		assert.Equal(t, "SELECT g.n, g.cell FROM (SELECT * FROM json_to_recordset('[]') AS "+gatewaysColDefs+") AS g(n)", stmt.SqlString())
	})

	t.Run("both views fold, each with its own snapshot, including inside a join", func(t *testing.T) {
		stmt := parseOneStmt(t, "SELECT p.name FROM multigres.multipoolers p JOIN multigres.multigateways g ON g.cell = p.cell")
		var seen []string
		require.NoError(t, foldClusterViews(stmt, func(i int, view *ClusterView) (ast.Node, error) {
			seen = append(seen, view.Name)
			return ast.NewParamRef(i+1, 0), nil
		}))
		assert.Equal(t, []string{"multipoolers", "multigateways"}, seen)
		sql := stmt.SqlString()
		assert.NotContains(t, sql, "multigres.")
		assert.Contains(t, sql, "json_to_recordset($1) AS _ (name TEXT, cell TEXT, database TEXT")
		assert.Contains(t, sql, "json_to_recordset($2) AS "+gatewaysColDefs+") AS g")
	})

	t.Run("time and duration values encode as timestamptz and interval input", func(t *testing.T) {
		row := []any{"p1", "zone1", "db", "default", "0", "primary", "leader", "serving", "active", "", now, 1500 * time.Microsecond, true, nil, "live", now}
		text, err := ClusterViews["multipoolers"].encode([][]any{row})
		require.NoError(t, err)
		assert.Contains(t, text, `"lifecycle_updated":"2026-10-21T12:00:00.0000005Z"`)
		assert.Contains(t, text, `"replication_lag":"1500 microseconds"`)
		assert.Contains(t, text, `"health_last_response":null`)
		assert.Contains(t, text, `"connected":true`)
	})

	t.Run("a row with the wrong width is an internal error", func(t *testing.T) {
		_, err := ClusterViews["multigateways"].encode([][]any{{"only-name"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "4 columns")
	})

	t.Run("ClusterViewDescribeSQL folds an empty literal and leaves the statement untouched", func(t *testing.T) {
		stmt := parseOneStmt(t, "SELECT name FROM multigres.multipoolers WHERE cell = $1")
		before := stmt.SqlString()
		stub, err := ClusterViewDescribeSQL(stmt)
		require.NoError(t, err)
		assert.Contains(t, stub, "json_to_recordset('[]') AS _ (name TEXT")
		assert.Contains(t, stub, ") AS multipoolers WHERE cell = $1")
		assert.NotContains(t, stub, "$2", "the describe stub adds no bind parameter")
		assert.Equal(t, before, stmt.SqlString())
	})

	t.Run("each view's template parses and round-trips", func(t *testing.T) {
		for name, view := range ClusterViews {
			sql := view.subselectTemplate().SqlString()
			_, err := parser.ParseSQL(sql)
			require.NoError(t, err, name)
			for _, c := range view.Columns {
				assert.Contains(t, sql, c.Name+" ", name)
			}
		}
	})
}

func TestClusterViewRoute_SimpleFoldsFreshRowsPerExecution(t *testing.T) {
	stmt := parseOneStmt(t, "SELECT name FROM multigres.multigateways")
	src := &fixedSource{rows: map[string][][]any{}}
	route := NewClusterViewRoute(NewRoute("default", "0", stmt.SqlString(), stmt), src)
	exec := &capturingExec{}
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := handler.NewMultigatewayConnectionState()

	src.rows["multigateways"] = [][]any{{"gw1", "z", nil, true}}
	require.NoError(t, route.StreamExecute(context.Background(), exec, testConn.Conn, state, nil, PlanExecInfo{}, nil))
	src.rows["multigateways"] = [][]any{{"gw2", "z", nil, false}}
	require.NoError(t, route.StreamExecute(context.Background(), exec, testConn.Conn, state, nil, PlanExecInfo{}, nil))

	require.Len(t, exec.sqls, 2)
	assert.Contains(t, exec.sqls[0], `"name":"gw1"`)
	assert.Contains(t, exec.sqls[1], `"name":"gw2"`)
	assert.NotContains(t, exec.sqls[1], "gw1", "each execution folds the current snapshot, not the plan's")
	assert.Equal(t, "SELECT name FROM multigres.multigateways", route.GetQuery(), "the plan keeps the unfolded query")
}

func TestClusterViewRoute_PortalAppendsSnapshotParams(t *testing.T) {
	stmt := parseOneStmt(t, "SELECT count(*) FROM multigres.multigateways g, multigres.multipoolers p WHERE g.is_self = $1")
	src := &fixedSource{rows: map[string][][]any{"multigateways": {{"gw1", "z", nil, true}}}}
	route := NewClusterViewRoute(NewRoute("default", "0", stmt.SqlString(), stmt), src)
	exec := &capturingExec{}
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := handler.NewMultigatewayConnectionState()

	psi, err := preparedstatement.NewPreparedStatementInfo(&query.PreparedStatement{Name: "s1", Query: stmt.SqlString(), ParamTypes: []uint32{16}})
	require.NoError(t, err)
	lengths, values := sqltypes.ParamsToProto([][]byte{[]byte("t")})
	portal := preparedstatement.NewPortalInfo(psi, &query.Portal{
		Name: "", PreparedStatementName: "s1",
		ParamLengths: lengths, ParamValues: values,
		ParamFormats:  []int32{1}, // one code applies to every client parameter
		ResultFormats: []int32{1},
	})

	run := func() *preparedstatement.PortalInfo {
		require.NoError(t, route.PortalStreamExecute(context.Background(), exec, testConn.Conn, state, portal, 0, false, PlanExecInfo{}, nil))
		require.NotNil(t, exec.lastPortalInfo)
		return exec.lastPortalInfo
	}

	first := run()
	require.Empty(t, exec.sqls, "the portal is forwarded, not run as a simple execution")
	forwarded := first.PreparedStatementInfo.GetQuery()
	assert.Equal(t, "SELECT COUNT(*) FROM (SELECT * FROM json_to_recordset($2) AS "+gatewaysColDefs+") AS g, (SELECT * FROM json_to_recordset($3) AS _ (name TEXT, cell TEXT, database TEXT, table_group TEXT, shard TEXT, routing_role TEXT, leadership TEXT, serving_status TEXT, lifecycle TEXT, lifecycle_reason TEXT, lifecycle_updated TIMESTAMPTZ, replication_lag INTERVAL, connected BOOLEAN, health_last_response TIMESTAMPTZ, discovery_state TEXT, last_change TIMESTAMPTZ)) AS p WHERE g.is_self = $1", forwarded)
	assert.Equal(t, "s1", first.PreparedStatementInfo.GetName())
	assert.Equal(t, []uint32{16}, first.PreparedStatementInfo.GetParamTypes(), "the client's declared types are kept; the snapshot slots are inferred")

	params := sqltypes.ParamsFromProto(first.Portal.GetParamLengths(), first.Portal.GetParamValues())
	require.Len(t, params, 3)
	assert.Equal(t, "t", string(params[0]))
	assert.Contains(t, string(params[1]), `"name":"gw1"`)
	assert.Equal(t, "[]", string(params[2]), "a view with no rows is an empty array")
	assert.Equal(t, []int32{1, 0, 0}, first.Portal.GetParamFormats(), "the single client code is expanded, snapshots are text")
	assert.Equal(t, []int32{1}, first.Portal.GetResultFormats(), "the client's result formats are honoured")

	// A changed snapshot changes only the bind, never the statement text.
	src.rows["multigateways"] = [][]any{{"gw2", "z", nil, false}}
	second := run()
	assert.Equal(t, forwarded, second.PreparedStatementInfo.GetQuery())
	params = sqltypes.ParamsFromProto(second.Portal.GetParamLengths(), second.Portal.GetParamValues())
	assert.Contains(t, string(params[1]), `"name":"gw2"`)
}

func TestClusterViewRoute_PortalBindEdgeCases(t *testing.T) {
	src := &fixedSource{rows: map[string][][]any{}}
	exec := &capturingExec{}
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := handler.NewMultigatewayConnectionState()
	run := func(t *testing.T, sql string, clientParams [][]byte, formats []int32) *preparedstatement.PortalInfo {
		t.Helper()
		stmt := parseOneStmt(t, sql)
		route := NewClusterViewRoute(NewRoute("default", "0", sql, stmt), src)
		psi, err := preparedstatement.NewPreparedStatementInfo(&query.PreparedStatement{Name: "s", Query: sql})
		require.NoError(t, err)
		lengths, values := sqltypes.ParamsToProto(clientParams)
		portal := preparedstatement.NewPortalInfo(psi, &query.Portal{ParamLengths: lengths, ParamValues: values, ParamFormats: formats})
		require.NoError(t, route.PortalStreamExecute(context.Background(), exec, testConn.Conn, state, portal, 0, false, PlanExecInfo{}, nil))
		return exec.lastPortalInfo
	}

	t.Run("a lone format code with no client parameters is dropped before the text snapshot is appended", func(t *testing.T) {
		out := run(t, "SELECT * FROM multigres.multipoolers", nil, []int32{1})
		assert.Empty(t, out.Portal.GetParamFormats(), "no codes means all text, which the snapshot is")
		assert.Len(t, sqltypes.ParamsFromProto(out.Portal.GetParamLengths(), out.Portal.GetParamValues()), 1)
	})

	t.Run("no format codes stay no format codes (all text)", func(t *testing.T) {
		out := run(t, "SELECT * FROM multigres.multipoolers WHERE cell = $1", [][]byte{[]byte("z")}, nil)
		assert.Empty(t, out.Portal.GetParamFormats())
		assert.Contains(t, out.PreparedStatementInfo.GetQuery(), "json_to_recordset($2)")
	})

	t.Run("missing client parameters are not padded, so the backend reports the mismatch", func(t *testing.T) {
		out := run(t, "SELECT * FROM multigres.multipoolers WHERE cell = $1", nil, nil)
		assert.Len(t, sqltypes.ParamsFromProto(out.Portal.GetParamLengths(), out.Portal.GetParamValues()), 1, "only the snapshot was appended")
		assert.Contains(t, out.PreparedStatementInfo.GetQuery(), "json_to_recordset($2)", "the snapshot slot still follows the statement's highest parameter")
	})
}

func TestClusterViewRoute_NoSourceIsAnError(t *testing.T) {
	stmt := parseOneStmt(t, "SELECT name FROM multigres.multigateways")
	route := NewClusterViewRoute(NewRoute("default", "0", stmt.SqlString(), stmt), nil)
	testConn := server.NewTestConn(&bytes.Buffer{})
	err := route.StreamExecute(context.Background(), &capturingExec{}, testConn.Conn, handler.NewMultigatewayConnectionState(), nil, PlanExecInfo{}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no source")
}
