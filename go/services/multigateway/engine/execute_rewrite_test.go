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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// parseBody parses a prepared-statement body to a single AST statement.
func parseBody(t *testing.T, sql string) ast.Stmt {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	return stmts[0]
}

// parseExecuteArgs parses "EXECUTE p (...)" and returns the argument list.
func parseExecuteArgs(t *testing.T, sql string) *ast.NodeList {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	es, ok := stmts[0].(*ast.ExecuteStmt)
	require.True(t, ok, "expected ExecuteStmt, got %T", stmts[0])
	return es.Params
}

// typeNames builds a []*ast.TypeName from simple builtin type names; an empty
// string yields a nil entry (unknown type).
func typeNames(names ...string) []*ast.TypeName {
	out := make([]*ast.TypeName, len(names))
	for i, n := range names {
		if n == "" {
			continue
		}
		out[i] = ast.NewTypeName([]string{n})
	}
	return out
}

func TestRewritePreparedBody(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		execArgs   string
		paramTypes []*ast.TypeName
		want       string
	}{
		{
			name:       "repeated param with type",
			body:       "SELECT $1, $2, $1",
			execArgs:   "EXECUTE p (5, 10)",
			paramTypes: typeNames("int8", "int8"),
			want:       "SELECT CAST(5 AS BIGINT), CAST(10 AS BIGINT), CAST(5 AS BIGINT)",
		},
		{
			name:       "no parameters",
			body:       "SELECT 1",
			execArgs:   "EXECUTE p",
			paramTypes: nil,
			want:       "SELECT 1",
		},
		{
			name:       "string literal arg with text type",
			body:       "SELECT $1",
			execArgs:   "EXECUTE p ('hi')",
			paramTypes: typeNames("text"),
			want:       "SELECT CAST('hi' AS TEXT)",
		},
		{
			name:       "unknown type wraps in parens to keep precedence",
			body:       "SELECT $1 * 2",
			execArgs:   "EXECUTE p (1 + 2)",
			paramTypes: typeNames(""),
			want:       "SELECT (1 + 2) * 2",
		},
		{
			name:       "insert body",
			body:       "INSERT INTO t VALUES ($1, $2)",
			execArgs:   "EXECUTE p (1, 2)",
			paramTypes: typeNames("int4", "int4"),
			want:       "INSERT INTO t VALUES (CAST(1 AS INT), CAST(2 AS INT))",
		},
		{
			name:       "param unused by body still requires matching arg count",
			body:       "SELECT $1",
			execArgs:   "EXECUTE p (7, 8)",
			paramTypes: typeNames("int4", "int4"),
			want:       "SELECT CAST(7 AS INT)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := parseBody(t, tt.body)
			args := parseExecuteArgs(t, tt.execArgs)

			got, err := RewritePreparedBody("p", body, args, tt.paramTypes)
			require.NoError(t, err)
			require.Equal(t, tt.want, got.SqlString())

			// The registered body AST must not be mutated by the rewrite.
			require.Equal(t, tt.body, body.SqlString(), "prepared body AST was mutated")
		})
	}
}

func TestParamTypeNamesFromOids(t *testing.T) {
	// int8, text, uuid, unknown/user-defined (0), inet.
	names := ParamTypeNamesFromOids([]uint32{20, 25, 2950, 0, 869})
	require.Len(t, names, 5)

	body := parseBody(t, "SELECT $1, $2, $3, $4, $5")
	args := parseExecuteArgs(t, "EXECUTE p (1, 'a', 'b', 'c', 'd')")

	got, err := RewritePreparedBody("p", body, args, names)
	require.NoError(t, err)
	// Known OIDs cast; the unrecognized OID (0) substitutes bare (parenthesized).
	require.Equal(t,
		"SELECT CAST(1 AS BIGINT), CAST('a' AS TEXT), CAST('b' AS UUID), ('c'), CAST('d' AS inet)",
		got.SqlString())
}

func TestRewritePreparedBodyWrongArgCount(t *testing.T) {
	body := parseBody(t, "SELECT $1, $2")
	args := parseExecuteArgs(t, "EXECUTE p (5)")

	_, err := RewritePreparedBody("p", body, args, typeNames("int4", "int4"))
	require.ErrorContains(t, err, "wrong number of parameters for prepared statement \"p\"")
}

func TestRewritePreparedBodyNilBody(t *testing.T) {
	args := parseExecuteArgs(t, "EXECUTE p (5)")
	_, err := RewritePreparedBody("p", nil, args, typeNames("int4"))
	require.ErrorContains(t, err, "no body to execute")
}

// A body $N outside the supplied argument range (which PostgreSQL would have
// rejected at PREPARE) is caught defensively rather than emitting invalid SQL.
func TestRewritePreparedBodyParamOutOfRange(t *testing.T) {
	body := parseBody(t, "SELECT $3")
	args := parseExecuteArgs(t, "EXECUTE p (5)")
	_, err := RewritePreparedBody("p", body, args, typeNames("int4"))
	require.ErrorContains(t, err, "wrong number of parameters")
}

func TestArgsContainParamRef(t *testing.T) {
	tests := []struct {
		name string
		args string
		want bool
	}{
		{"literals only", "EXECUTE p (5, 'hi')", false},
		{"single param ref", "EXECUTE p ($1)", true},
		{"param ref nested in expression", "EXECUTE p (1 + $2)", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, argsContainParamRef(parseExecuteArgs(t, tt.args)))
		})
	}
}

// TestResolveExecuteArgs covers the *outer* parameter layer: EXECUTE arguments
// that are themselves $N references bound by an extended-protocol Bind. See the
// resolveExecuteArgs doc comment for the two-scope model.
func TestResolveExecuteArgs(t *testing.T) {
	t.Run("simple-protocol literals pass through (fast path)", func(t *testing.T) {
		args := parseExecuteArgs(t, "EXECUTE p (5, 'hi')")
		got, err := resolveExecuteArgs(args, nil)
		require.NoError(t, err)
		require.Same(t, args, got, "literal args should be returned unchanged")
	})

	t.Run("no arguments passes through", func(t *testing.T) {
		args := parseExecuteArgs(t, "EXECUTE p")
		got, err := resolveExecuteArgs(args, nil)
		require.NoError(t, err)
		require.Equal(t, args, got)
	})

	t.Run("outer param ref resolved from its bound value", func(t *testing.T) {
		args := parseExecuteArgs(t, "EXECUTE p ($1)")
		got, err := resolveExecuteArgs(args, makePortal(t, "21"))
		require.NoError(t, err)
		require.Equal(t, "'21'", got.SqlString())
	})

	t.Run("mixed literal and outer refs, in order", func(t *testing.T) {
		args := parseExecuteArgs(t, "EXECUTE p ($1, 5, $2)")
		got, err := resolveExecuteArgs(args, makePortal(t, "aa", "bb"))
		require.NoError(t, err)
		require.Equal(t, "'aa', 5, 'bb'", got.SqlString())
	})

	t.Run("param ref with no portal is rejected", func(t *testing.T) {
		args := parseExecuteArgs(t, "EXECUTE p ($1)")
		_, err := resolveExecuteArgs(args, nil)
		require.ErrorContains(t, err, "has no bound parameters")
	})
}

// TestMaterializeExecute exercises the full standalone-EXECUTE pipeline
// (resolve outer args -> substitute into body -> deparse to SQL string).
func TestMaterializeExecute(t *testing.T) {
	t.Run("simple protocol: literal args cast into body", func(t *testing.T) {
		body := parseBody(t, "SELECT $1 * 2")
		args := parseExecuteArgs(t, "EXECUTE p (21)")
		got, err := materializeExecute("p", body, args, []uint32{20}, nil) // int8
		require.NoError(t, err)
		require.Equal(t, "SELECT CAST(21 AS BIGINT) * 2", got)
	})

	t.Run("extended protocol: outer bind resolved, then substituted", func(t *testing.T) {
		body := parseBody(t, "SELECT $1 * 2")
		args := parseExecuteArgs(t, "EXECUTE p ($1)")
		got, err := materializeExecute("p", body, args, []uint32{20}, makePortal(t, "21"))
		require.NoError(t, err)
		require.Equal(t, "SELECT CAST('21' AS BIGINT) * 2", got)
	})
}

// TestMaterializeWrappedExecute exercises EXECUTE nested inside a wrapper
// (EXPLAIN EXECUTE / CREATE TABLE AS EXECUTE): the substituted body is spliced
// into a clone of the wrapper, leaving the caller's AST untouched.
func TestMaterializeWrappedExecute(t *testing.T) {
	t.Run("explain execute", func(t *testing.T) {
		wrapper, exec := parseWrapped(t, "EXPLAIN EXECUTE p (5)")
		got, err := MaterializeWrappedExecute(wrapper, exec, makePSI(t, "SELECT $1", 20), nil)
		require.NoError(t, err)
		require.Equal(t, "EXPLAIN SELECT CAST(5 AS BIGINT)", got.SqlString())
		require.Contains(t, wrapper.SqlString(), "EXECUTE", "wrapper AST must not be mutated")
	})

	t.Run("create table as execute", func(t *testing.T) {
		wrapper, exec := parseWrapped(t, "CREATE TABLE t AS EXECUTE p (5)")
		got, err := MaterializeWrappedExecute(wrapper, exec, makePSI(t, "SELECT $1", 20), nil)
		require.NoError(t, err)
		require.Equal(t, "CREATE TABLE t AS SELECT CAST(5 AS BIGINT)", got.SqlString())
	})

	t.Run("wrapper without a nested EXECUTE is rejected", func(t *testing.T) {
		noExec := parseBody(t, "SELECT 1")
		_, exec := parseWrapped(t, "EXPLAIN EXECUTE p (5)") // borrow an ExecuteStmt node
		_, err := MaterializeWrappedExecute(noExec, exec, makePSI(t, "SELECT $1", 20), nil)
		require.ErrorContains(t, err, "could not locate nested EXECUTE")
	})
}

// makePortal builds a PortalInfo whose bound parameters are the given values in
// text wire format. The outer statement declares no parameter types, which
// DecodeBindAsText treats as text — enough to exercise resolveExecuteArgs.
func makePortal(t *testing.T, textValues ...string) *preparedstatement.PortalInfo {
	t.Helper()
	params := make([][]byte, len(textValues))
	for i, v := range textValues {
		params[i] = []byte(v)
	}
	lengths, values := sqltypes.ParamsToProto(params)
	psi, err := preparedstatement.NewPreparedStatementInfo(&querypb.PreparedStatement{Query: "SELECT 1"})
	require.NoError(t, err)
	return preparedstatement.NewPortalInfo(psi, &querypb.Portal{
		ParamLengths: lengths,
		ParamValues:  values,
	})
}

// makePSI builds a PreparedStatementInfo for a prepared body with the given
// resolved parameter type OIDs (surfaced via ResolvedParamTypeOids' proto fallback).
func makePSI(t *testing.T, body string, paramOids ...uint32) *preparedstatement.PreparedStatementInfo {
	t.Helper()
	psi, err := preparedstatement.NewPreparedStatementInfo(&querypb.PreparedStatement{
		Query:      body,
		ParamTypes: paramOids,
	})
	require.NoError(t, err)
	return psi
}

// parseWrapped parses a wrapper statement (EXPLAIN EXECUTE / CREATE TABLE AS
// EXECUTE) and returns both the wrapper and the ExecuteStmt nested inside it.
func parseWrapped(t *testing.T, sql string) (ast.Stmt, *ast.ExecuteStmt) {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	var exec *ast.ExecuteStmt
	ast.Rewrite(stmts[0], func(c *ast.Cursor) bool {
		if es, ok := c.Node().(*ast.ExecuteStmt); ok {
			exec = es
			return false
		}
		return true
	}, nil)
	require.NotNil(t, exec, "no ExecuteStmt found in %q", sql)
	return stmts[0], exec
}
