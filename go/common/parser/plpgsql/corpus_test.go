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

package plpgsql

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
)

// pgCorpusCasesFile holds PL/pgSQL bodies extracted from PostgreSQL's plpgsql
// regression corpus. Following the SQL parser's testdata/postgres/*.json
// convention, the extracted cases are committed but the raw PostgreSQL .sql
// files are not — regenerate with TestGeneratePGCorpusCases.
const pgCorpusCasesFile = "pg_corpus_cases.json"

// TestPGCorpus is the Phase 1 acceptance gate: every PL/pgSQL body PostgreSQL's
// own plpgsql regression tests use must parse through ParsePLpgSQL, except the
// bodies PostgreSQL itself rejects (its negative tests), which carry an expected
// "error" substring and must fail the same way. Bodies that parse must also
// round-trip: their deparse re-parses to the same deparse.
func TestPGCorpus(t *testing.T) {
	cases := readCases(t, filepath.Join("testdata", pgCorpusCasesFile))
	require.NotEmpty(t, cases)
	for i := range cases {
		c := &cases[i]
		fn, err := ParsePLpgSQL(c.Body)
		if c.Error != "" {
			assert.ErrorContainsf(t, err, c.Error, "case: %s", c.Comment)
			continue
		}
		if !assert.NoErrorf(t, err, "case: %s\n--body--\n%s", c.Comment, c.Body) {
			continue
		}
		got := fn.SqlString()
		if fn2, rerr := ParsePLpgSQL(got); assert.NoErrorf(t, rerr, "re-parse failed, case: %s\ndeparse:\n%s", c.Comment, got) {
			assert.Equalf(t, got, fn2.SqlString(), "deparse not stable, case: %s", c.Comment)
		}
	}
}

// TestGeneratePGCorpusCases regenerates testdata/pg_corpus_cases.json from a
// local PostgreSQL checkout. It is skipped unless PLPGSQL_CORPUS_SRC points at
// PG's plpgsql SQL directory:
//
//	PLPGSQL_CORPUS_SRC=~/postgres/src/pl/plpgsql/src/sql \
//	  go test ./go/common/parser/plpgsql/ -run TestGeneratePGCorpusCases
//
// Every PL/pgSQL body found in a CREATE FUNCTION / PROCEDURE … LANGUAGE plpgsql
// or DO block becomes a case: bodies PostgreSQL itself rejects (its negative
// tests) get an "error" substring; the rest are expected to parse.
func TestGeneratePGCorpusCases(t *testing.T) {
	srcDir := os.Getenv("PLPGSQL_CORPUS_SRC")
	if srcDir == "" {
		t.Skip("set PLPGSQL_CORPUS_SRC to a PostgreSQL src/pl/plpgsql/src/sql dir to regenerate")
	}
	files, err := filepath.Glob(filepath.Join(srcDir, "*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no .sql files in %s: %v", srcDir, err)
	}
	sort.Strings(files)

	var cases []parseCase
	seen := map[string]bool{}
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		base := filepath.Base(f)
		n := 0
		for _, stmtText := range splitStatements(stripBackslashLines(string(src))) {
			parsed, perr := parser.ParseSQL(stmtText)
			if perr != nil {
				continue
			}
			for _, st := range parsed {
				body, ok := plpgsqlBody(st)
				if !ok || seen[body] {
					continue
				}
				seen[body] = true
				n++
				c := parseCase{Comment: fmt.Sprintf("%s #%d", base, n), Body: body}
				if _, berr := ParsePLpgSQL(body); berr != nil {
					c.Error = strings.TrimPrefix(berr.Error(), "plpgsql parse error: ")
				}
				cases = append(cases, c)
			}
		}
	}

	path := filepath.Join("testdata", "pg_corpus_cases.json")
	writeCases(t, path, cases)
	t.Logf("wrote %d corpus cases to %s", len(cases), path)
}

// --- extraction helpers (used by the generator above) -----------------------

// stripBackslashLines blanks psql meta-command lines (\c, \echo, \gset, …),
// which are not SQL and would stop the lexer. Newlines are kept so byte offsets
// (used for statement slicing) stay aligned with the source.
func stripBackslashLines(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for line := range strings.SplitSeq(s, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), `\`) {
			b.WriteString(line)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// splitStatements splits SQL into top-level statements at ';'. Dollar-quoted and
// single-quoted bodies are single lexer tokens, so their internal ';' never leak
// out as separators — the reason we tokenize rather than split on the byte.
func splitStatements(sql string) []string {
	lex := parser.NewLexer(sql)
	var stmts []string
	start := 0
	for {
		tok := lex.NextToken()
		if tok == nil || tok.Type == parser.EOF || tok.Type == parser.INVALID {
			break
		}
		if tok.Type == int(';') {
			if s := strings.TrimSpace(sql[start:tok.Position]); s != "" {
				stmts = append(stmts, s)
			}
			start = tok.Position + 1
		}
	}
	if s := strings.TrimSpace(sql[start:]); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// plpgsqlBody extracts a PL/pgSQL body from a parsed CREATE FUNCTION / CREATE
// PROCEDURE … LANGUAGE plpgsql or DO statement, returning "" and false for
// anything else.
func plpgsqlBody(stmt ast.Stmt) (string, bool) {
	switch s := stmt.(type) {
	case *ast.CreateFunctionStmt:
		return bodyFromOptions(s.Options, false)
	case *ast.DoStmt:
		// DO defaults to LANGUAGE plpgsql when none is given.
		return bodyFromOptions(s.Args, true)
	}
	return "", false
}

// bodyFromOptions pulls the `as` body and `language` out of a function/DO option
// list, returning the body only when the language is plpgsql (or absent and the
// caller says plpgsql is the default).
func bodyFromOptions(opts *ast.NodeList, defaultPLpgSQL bool) (string, bool) {
	if opts == nil {
		return "", false
	}
	var body, lang string
	haveBody := false
	for _, item := range opts.Items {
		de, ok := item.(*ast.DefElem)
		if !ok {
			continue
		}
		switch de.Defname {
		case "language":
			if str, ok := de.Arg.(*ast.String); ok {
				lang = strings.ToLower(str.SVal)
			}
		case "as":
			// A plpgsql body is a single string, either bare or as a one-element
			// list (a C function's `as 'objfile', 'symbol'` has two, and is not
			// plpgsql).
			if str, ok := de.Arg.(*ast.String); ok {
				body, haveBody = str.SVal, true
			} else if list, ok := de.Arg.(*ast.NodeList); ok && len(list.Items) == 1 {
				if str, ok := list.Items[0].(*ast.String); ok {
					body, haveBody = str.SVal, true
				}
			}
		}
	}
	if !haveBody {
		return "", false
	}
	return body, lang == "plpgsql" || (lang == "" && defaultPLpgSQL)
}
