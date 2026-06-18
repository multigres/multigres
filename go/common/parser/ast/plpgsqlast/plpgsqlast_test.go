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
)

func TestNodeTags(t *testing.T) {
	assert.Equal(t, T_PLpgSQL_function, NewPLpgSQL_function().NodeTag())
	assert.Equal(t, T_PLpgSQL_stmt_block, NewPLpgSQL_stmt_block().NodeTag())
	assert.Equal(t, T_PLpgSQL_expr, NewPLpgSQL_expr("SELECT 1").NodeTag())
	assert.Equal(t, T_PLpgSQL_exception_block, NewPLpgSQL_exception_block().NodeTag())
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
