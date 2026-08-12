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

// ParseSeed carries names to pre-declare in an outer (function-scope) namespace
// before a body is parsed. A body is parsed in isolation — without its
// surrounding CREATE FUNCTION signature — so names that live in the signature
// rather than the body's own DECLARE section (parameters, and a trigger
// function's implicit NEW/OLD/TG_* variables) would not resolve, and an
// assignment to one (`param := …`, `new.field := …`) would be misparsed as an
// embedded SQL statement.
//
// PG has the whole signature at compile time, so its do_compile (pl_comp.c)
// builds these as datums in the function-scope namespace before parsing the
// body: each parameter is registered under "$N" and, if named, under its name;
// a DML-trigger function adds NEW/OLD as records and tg_name/tg_when/tg_level/
// tg_op/tg_relid/tg_relname/tg_table_name/tg_table_schema/tg_nargs/tg_argv, and
// an event-trigger function adds tg_event/tg_tag. ParseSeed lets the caller,
// which does have the signature, hand those same names in.
//
// Scalars are seeded as plain variables; Records (NEW/OLD) as record variables
// so a `rec.field` assignment target resolves. Names should be lowercased for
// unquoted identifiers, matching the scanner's lookup.
type ParseSeed struct {
	Scalars []string // e.g. parameter names, "$1".."$N", TG_* trigger variables
	Records []string // e.g. NEW / OLD in a trigger function
}

// ParsePLpgSQL parses an already-extracted PL/pgSQL function body (with
// dollar-quote or quoted-string delimiters stripped by the caller) and
// returns its AST root.
//
// A body must be a block (BEGIN … END), matching PG; empty input is a parse
// error.
func ParsePLpgSQL(body string) (*plpgsqlast.PLpgSQL_function, error) {
	return ParsePLpgSQLSeeded(body, nil)
}

// ParsePLpgSQLSeeded is ParsePLpgSQL with a set of function-scope names
// pre-declared (see ParseSeed). A nil or empty seed behaves exactly like
// ParsePLpgSQL — the body parses in isolation.
func ParsePLpgSQLSeeded(body string, seed *ParseSeed) (*plpgsqlast.PLpgSQL_function, error) {
	lex := newLexer(body)
	lex.seed(seed)
	plpgsqlNewParser().Parse(lex)

	if lex.err != nil {
		return nil, lex.err
	}
	return lex.result, nil
}
