// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plpgsqlast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Compile-time guarantees that the nodes satisfy the right interfaces.
var (
	_ Node = (*PLpgSQL_function)(nil)
	_ Node = (*PLpgSQL_expr)(nil)
	_ Node = (*PLpgSQL_exception_block)(nil)
	_ Stmt = (*PLpgSQL_stmt_block)(nil)
	_ Stmt = (*PLpgSQL_stmt_if)(nil)
	_ Stmt = (*PLpgSQL_stmt_loop)(nil)
	_ Stmt = (*PLpgSQL_stmt_while)(nil)
	_ Stmt = (*PLpgSQL_stmt_exit)(nil)
	_ Stmt = (*PLpgSQL_stmt_fori)(nil)
	_ Stmt = (*PLpgSQL_stmt_fors)(nil)
	_ Stmt = (*PLpgSQL_stmt_foreach_a)(nil)
	_ Stmt = (*PLpgSQL_stmt_case)(nil)
	_ Stmt = (*PLpgSQL_stmt_execsql)(nil)
	_ Stmt = (*PLpgSQL_stmt_perform)(nil)
	_ Stmt = (*PLpgSQL_stmt_call)(nil)
	_ Stmt = (*PLpgSQL_stmt_return)(nil)
	_ Stmt = (*PLpgSQL_stmt_return_next)(nil)
	_ Stmt = (*PLpgSQL_stmt_return_query)(nil)
	_ Stmt = (*PLpgSQL_stmt_dynexecute)(nil)
	_ Stmt = (*PLpgSQL_stmt_dynfors)(nil)
	_ Stmt = (*PLpgSQL_stmt_open)(nil)
	_ Stmt = (*PLpgSQL_stmt_fetch)(nil)
	_ Stmt = (*PLpgSQL_stmt_close)(nil)
	// PLpgSQL_if_elsif and PLpgSQL_case_when are helper nodes (like PG's
	// structs), not statements.
	_ Node = (*PLpgSQL_if_elsif)(nil)
	_ Node = (*PLpgSQL_case_when)(nil)
)

func TestNodeTags(t *testing.T) {
	assert.Equal(t, T_PLpgSQL_function, NewPLpgSQL_function().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_block, NewPLpgSQL_stmt_block().NodeTag())
	assert.Equal(t, T_PLpgSQL_expr, NewPLpgSQL_expr("SELECT 1").NodeTag())
	assert.Equal(t, T_PLpgSQL_exception_block, NewPLpgSQL_exception_block().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_if, NewPLpgSQL_stmt_if().NodeTag())
	assert.Equal(t, T_PLpgSQL_if_elsif, NewPLpgSQL_if_elsif().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_loop, NewPLpgSQL_stmt_loop().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_while, NewPLpgSQL_stmt_while().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_exit, NewPLpgSQL_stmt_exit(true).NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_fori, NewPLpgSQL_stmt_fori().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_fors, NewPLpgSQL_stmt_fors().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_foreach_a, NewPLpgSQL_stmt_foreach_a().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_case, NewPLpgSQL_stmt_case().NodeTag())
	assert.Equal(t, T_PLpgSQL_case_when, NewPLpgSQL_case_when().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_execsql, NewPLpgSQL_stmt_execsql().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_perform, NewPLpgSQL_stmt_perform().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_call, NewPLpgSQL_stmt_call(true).NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_return, NewPLpgSQL_stmt_return().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_return_next, NewPLpgSQL_stmt_return_next().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_return_query, NewPLpgSQL_stmt_return_query().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_dynexecute, NewPLpgSQL_stmt_dynexecute().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_dynfors, NewPLpgSQL_stmt_dynfors().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_open, NewPLpgSQL_stmt_open().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_fetch, NewPLpgSQL_stmt_fetch(false).NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_close, NewPLpgSQL_stmt_close().NodeTag())
	assert.Equal(t, T_PLpgSQL_alias, NewPLpgSQL_alias("x").NodeTag())
}

// A cursor declaration is a PLpgSQL_var with CursorExplicitExpr set; its deparse
// switches to the cursor form. PLpgSQL_alias round-trips its ALIAS FOR text.
func TestCursorDeclAndAliasDeparse(t *testing.T) {
	c := NewPLpgSQL_var("c")
	c.CursorOptions = CURSOR_OPT_FAST_PLAN | CURSOR_OPT_SCROLL
	c.CursorArgs = []*PLpgSQL_var{{Refname: "a", DataType: NewPLpgSQL_type("int")}}
	c.CursorExplicitExpr = NewPLpgSQL_expr("SELECT a")
	assert.Equal(t, "c SCROLL CURSOR (a int) FOR SELECT a;", c.SqlString())

	a := NewPLpgSQL_alias("x")
	a.Target = "y"
	assert.Equal(t, "x ALIAS FOR y;", a.SqlString())
	var _ Datum = a // alias is a datum
}

// The FETCH direction deparse canonicalizes PG's (Direction, HowMany, Expr) model.
func TestFetchDirectionDeparse(t *testing.T) {
	fetch := func(dir FetchDirection, howMany int64, expr string) *PLpgSQL_stmt_fetch {
		f := NewPLpgSQL_stmt_fetch(false)
		f.Curvar = "c"
		f.Direction = dir
		f.HowMany = howMany
		if expr != "" {
			f.Expr = NewPLpgSQL_expr(expr)
		}
		return f
	}
	// FORWARD one row is the default: no direction clause.
	assert.Equal(t, "FETCH c INTO x", withTarget(fetch(FETCH_FORWARD, 1, ""), "x").SqlString())
	assert.Equal(t, "FETCH BACKWARD FROM c INTO x", withTarget(fetch(FETCH_BACKWARD, 1, ""), "x").SqlString())
	assert.Equal(t, "FETCH ABSOLUTE 3 FROM c INTO x", withTarget(fetch(FETCH_ABSOLUTE, 1, "3"), "x").SqlString())
	assert.Equal(t, "FETCH LAST FROM c INTO x", withTarget(fetch(FETCH_ABSOLUTE, -1, ""), "x").SqlString())

	all := fetch(FETCH_FORWARD, FETCH_ALL, "")
	all.IsMove = true
	assert.Equal(t, "MOVE ALL FROM c", all.SqlString())
}

func withTarget(f *PLpgSQL_stmt_fetch, target string) *PLpgSQL_stmt_fetch {
	f.Target = target
	return f
}

// The dynexecute deparse re-emits INTO/USING in their recorded source order.
func TestDynExecuteUsingFirstDeparse(t *testing.T) {
	mk := func(usingFirst bool) *PLpgSQL_stmt_dynexecute {
		s := NewPLpgSQL_stmt_dynexecute()
		s.Query = NewPLpgSQL_expr("q")
		s.Into = true
		s.Target = "x"
		s.Params = []*PLpgSQL_expr{NewPLpgSQL_expr("a")}
		s.UsingFirst = usingFirst
		return s
	}
	assert.Equal(t, "EXECUTE q INTO x USING a", mk(false).SqlString())
	assert.Equal(t, "EXECUTE q USING a INTO x", mk(true).SqlString())
}

func TestExprSqlStringIsVerbatim(t *testing.T) {
	e := NewPLpgSQL_expr("a + 1")
	assert.Equal(t, "a + 1", e.SqlString())
	assert.Nil(t, e.Parsed) // not parsed until the read_sql_construct boundary
	// A fresh expr defaults to the full-statement parse mode (PG's parseMode).
	assert.Equal(t, RAW_PARSE_DEFAULT, e.ParseMode)

	e.ParseMode = RAW_PARSE_PLPGSQL_EXPR
	assert.Equal(t, RAW_PARSE_PLPGSQL_EXPR, e.ParseMode)
}

func TestBlockSqlString(t *testing.T) {
	b := NewPLpgSQL_stmt_block()
	assert.Equal(t, "BEGIN\nEND", b.SqlString())

	b.Label = "outer"
	assert.Equal(t, "<<outer>> BEGIN\nEND outer", b.SqlString())
}

// The function body is a single top-level block, mirroring PG.
func TestFunctionActionWiring(t *testing.T) {
	fn := NewPLpgSQL_function()
	assert.Nil(t, fn.Action)
	assert.Empty(t, fn.SqlString())

	fn.Action = NewPLpgSQL_stmt_block()
	assert.Equal(t, "BEGIN\nEND", fn.SqlString())
}
