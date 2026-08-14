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

// Package plpgsql parses PL/pgSQL function bodies into the AST defined in
// go/common/parser/ast. The parser is a goyacc port of
// postgres/src/pl/plpgsql/src/pl_gram.y and is used by the multigateway
// planner for Tier 1 session-state-leak detection (DO blocks, CREATE FUNCTION
// … LANGUAGE plpgsql).
package plpgsql

import (
	"github.com/multigres/multigres/go/common/parser/ast/plpgsqlast"
)

// ParsePLpgSQL parses an already-extracted PL/pgSQL function body (with
// dollar-quote or quoted-string delimiters stripped by the caller) and
// returns its AST root.
//
// A body must be a block (BEGIN … END), matching PG; empty input is a parse
// error. The remaining statement families (DECLARE, control flow, embedded
// SQL, dynamic EXECUTE, …) are added incrementally as the grammar is ported.
func ParsePLpgSQL(body string) (*plpgsqlast.PLpgSQL_function, error) {
	lex := newLexer(body)
	plpgsqlNewParser().Parse(lex)

	if lex.err != nil {
		return nil, lex.err
	}
	return lex.result, nil
}
