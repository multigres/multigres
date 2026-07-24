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

// This file holds the public parse entry points. They previously lived in the
// Go tail of postgres.y and were emitted into the generated postgres.go; keeping
// them here separates hand-written API from generated parser tables.

package parser

import (
	"sync"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// parserPool reuses goyacc parser instances across ParseSQL calls to avoid
// reallocating the (large) parser state on every parse.
var parserPool = sync.Pool{
	New: func() any { return yyNewParser() },
}

// ParseSQL parses SQL input and returns the AST, using the default parse options
// (standard_conforming_strings on, matching a fresh PostgreSQL session).
func ParseSQL(input string) ([]ast.Stmt, error) {
	return parseWithLexer(NewLexer(input))
}

// ParseSQLWithStandardConformingStrings parses SQL with an explicit
// standard_conforming_strings setting. Callers that track the session GUC (e.g.
// the multigateway) must lex string literals the way the client's session
// would: with scs=false, an ordinary '...' literal processes backslash escapes,
// so parsing with the default scs=true would mis-lex the query. Behaves like
// ParseSQL in every other respect.
func ParseSQLWithStandardConformingStrings(input string, standardConformingStrings bool) ([]ast.Stmt, error) {
	opts := DefaultParseOptions()
	opts.StandardConformingStrings = standardConformingStrings
	return parseWithLexer(NewLexerWithOptions(input, opts))
}

// parseWithLexer runs a pooled goyacc parser against the given lexer and returns
// the parse tree, or the structured first error (which carries the cursor
// position for the ErrorResponse "P" field).
func parseWithLexer(lexer *Lexer) ([]ast.Stmt, error) {
	parser := parserPool.Get().(yyParser)
	parser.Parse(lexer)
	parserPool.Put(parser)

	if lexer.HasErrors() {
		// Return the structured error so PostgreSQL-serving callers can read the
		// position for the ErrorResponse "P" field. Error() still yields the same
		// message string, so plain error consumers are unaffected.
		return nil, lexer.FirstError()
	}

	return lexer.GetParseTree(), nil
}
