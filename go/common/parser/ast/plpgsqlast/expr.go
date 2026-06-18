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

import "github.com/multigres/multigres/go/common/parser/ast"

// RawParseMode selects how an embedded SQL fragment is parsed. It mirrors PG's
// RawParseMode (postgres/src/include/parser/parser.h), which PG threads into
// raw_parser(). Our SQL parser does not yet take a mode, so for now this is
// metadata recording what kind of fragment a PLpgSQL_expr holds; the
// read_sql_construct chunk uses it to decide how to produce PLpgSQL_expr.Parsed.
// Values and order match PG's enum exactly.
type RawParseMode int

const (
	// RAW_PARSE_DEFAULT parses a semicolon-separated list of SQL statements.
	RAW_PARSE_DEFAULT RawParseMode = iota
	// RAW_PARSE_TYPE_NAME parses a single type name.
	RAW_PARSE_TYPE_NAME
	// RAW_PARSE_PLPGSQL_EXPR parses a single PL/pgSQL expression.
	RAW_PARSE_PLPGSQL_EXPR
	// RAW_PARSE_PLPGSQL_ASSIGN1 parses a PL/pgSQL assignment with 1 target.
	RAW_PARSE_PLPGSQL_ASSIGN1
	// RAW_PARSE_PLPGSQL_ASSIGN2 parses a PL/pgSQL assignment with 2 targets.
	RAW_PARSE_PLPGSQL_ASSIGN2
	// RAW_PARSE_PLPGSQL_ASSIGN3 parses a PL/pgSQL assignment with 3 targets.
	RAW_PARSE_PLPGSQL_ASSIGN3
)

// PLpgSQL_expr is an embedded SQL fragment inside a PL/pgSQL body — the source
// of an assignment, a query in PERFORM/RETURN, an IF/WHILE condition, a
// stmt_execsql, etc. Ported from postgres/src/pl/plpgsql/src/plpgsql.h
// (PLpgSQL_expr).
//
// It is the boundary between the two AST hierarchies and the node the Tier-1
// walker exists to reach: Parsed is handed to the SQL-side analyzeStatement.
// PG keeps a compiled SPI plan here; for static analysis we keep the parsed SQL
// tree instead and drop all the execution/caching fields.
type PLpgSQL_expr struct {
	BaseNode
	// Query is the verbatim SQL text from the function body (PG's expr->query).
	Query string `json:"query,omitempty"`
	// ParseMode records how Query should be parsed, mirroring PG's
	// PLpgSQL_expr.parseMode. Set by read_sql_construct; defaults to
	// RAW_PARSE_DEFAULT (a full statement).
	ParseMode RawParseMode `json:"parse_mode,omitempty"`
	// Parsed is the SQL AST that Query parses to. PG has no parse-time
	// equivalent: it stores only the text and parses lazily at execution via
	// SPI (its `plan` field), validating syntax at compile time and discarding
	// the tree. We parse eagerly instead, because the gateway analyzes the body
	// statically and never executes it. Filled in by the read_sql_construct
	// boundary; nil until then. How a bare-expression fragment (ParseMode
	// RAW_PARSE_PLPGSQL_EXPR) becomes an ast.Stmt is decided in that chunk.
	Parsed ast.Stmt `json:"-"`
}

func (e *PLpgSQL_expr) String() string {
	return "PLpgSQL_expr"
}

// SqlString returns the verbatim fragment text.
func (e *PLpgSQL_expr) SqlString() string {
	return e.Query
}

func NewPLpgSQL_expr(query string) *PLpgSQL_expr {
	return &PLpgSQL_expr{
		BaseNode: BaseNode{Tag: T_PLpgSQL_expr, Loc: -1},
		Query:    query,
	}
}
