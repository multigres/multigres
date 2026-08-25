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

package planner

import (
	"encoding/json"
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

// proceduralCorpusFile holds every DO / CREATE FUNCTION … LANGUAGE plpgsql
// statement extracted from PostgreSQL's regression corpus, each paired with the
// gateway body-analysis outcome we expect: accepted, or rejected with a message.
//
// It exists so the Tier-1 accept/reject decision can be exercised at unit-test
// speed instead of through the full pgregress e2e run. To see what a policy
// change does, edit the analyzer and run TestProceduralCorpus: every statement
// whose outcome flipped fails, so the diff is exactly the set that changed.
// Following the SQL parser's corpus convention (corpus_test.go), the extracted
// cases are committed but the raw PostgreSQL .sql files are not — regenerate
// with TestGenerateProceduralCorpus after a deliberate policy change.
const proceduralCorpusFile = "procedural_corpus_cases.json"

// proceduralCase is one corpus statement and its expected analysis outcome.
type proceduralCase struct {
	Comment string `json:"comment"`
	Stmt    string `json:"stmt"`
	// Reject is the expected rejection message. Empty means the statement is
	// expected to be accepted (analyzeStatement returns no error).
	Reject string `json:"reject,omitempty"`
}

// TestProceduralCorpus replays the committed corpus: every statement must parse
// and its analyzeStatement outcome must match the recorded accept/reject.
func TestProceduralCorpus(t *testing.T) {
	cases := readProceduralCases(t, filepath.Join("testdata", proceduralCorpusFile))
	require.NotEmpty(t, cases)
	for i := range cases {
		c := &cases[i]
		stmts, err := parser.ParseSQL(c.Stmt)
		if !assert.NoErrorf(t, err, "parse failed, case: %s", c.Comment) || !assert.Lenf(t, stmts, 1, "case: %s", c.Comment) {
			continue
		}
		_, aerr := analyzeStatement(stmts[0], false)
		if c.Reject == "" {
			assert.NoErrorf(t, aerr, "expected accept, case: %s\n--stmt--\n%s", c.Comment, c.Stmt)
		} else {
			assert.ErrorContainsf(t, aerr, c.Reject, "expected reject, case: %s\n--stmt--\n%s", c.Comment, c.Stmt)
		}
	}
}

// TestGenerateProceduralCorpus regenerates testdata/procedural_corpus_cases.json
// from a local PostgreSQL checkout. It is skipped unless
// PLPGSQL_ANALYZER_CORPUS_SRC names one or more (space-separated) directories of
// .sql files:
//
//	PLPGSQL_ANALYZER_CORPUS_SRC="$HOME/postgres/src/test/regress/sql $HOME/postgres/src/pl/plpgsql/src/sql" \
//	  go test ./go/services/multigateway/planner/ -run TestGenerateProceduralCorpus
//
// Every DO block and CREATE FUNCTION/PROCEDURE … LANGUAGE plpgsql becomes a
// case, labelled with the current analyzeStatement outcome.
func TestGenerateProceduralCorpus(t *testing.T) {
	srcDirs := strings.Fields(os.Getenv("PLPGSQL_ANALYZER_CORPUS_SRC"))
	if len(srcDirs) == 0 {
		t.Skip("set PLPGSQL_ANALYZER_CORPUS_SRC to one or more dirs of PostgreSQL .sql files to regenerate")
	}
	var files []string
	for _, d := range srcDirs {
		matches, err := filepath.Glob(filepath.Join(d, "*.sql"))
		require.NoError(t, err)
		files = append(files, matches...)
	}
	require.NotEmpty(t, files, "no .sql files found under %v", srcDirs)
	sort.Strings(files)

	cases := extractProceduralCases(t, files)
	path := filepath.Join("testdata", proceduralCorpusFile)
	writeProceduralCases(t, path, cases)
	t.Logf("wrote %d procedural cases to %s", len(cases), path)
}

// extractProceduralCases pulls every unique DO / CREATE FUNCTION … plpgsql
// statement out of the given .sql files (in order) and records the current
// analyzeStatement outcome for each.
func extractProceduralCases(t *testing.T, files []string) []proceduralCase {
	var cases []proceduralCase
	seen := map[string]bool{}
	for _, f := range files {
		src, err := os.ReadFile(f)
		require.NoError(t, err)
		base := filepath.Base(f)
		n := 0
		for _, stmtText := range splitCorpusStatements(stripCorpusBackslashLines(string(src))) {
			stmts, perr := parser.ParseSQL(stmtText)
			if perr != nil || len(stmts) != 1 {
				continue
			}
			if !isPlpgsqlProceduralStmt(stmts[0]) || seen[stmtText] {
				continue
			}
			seen[stmtText] = true
			n++
			c := proceduralCase{Comment: fmt.Sprintf("%s #%d", base, n), Stmt: stmtText}
			if _, aerr := analyzeStatement(stmts[0], false); aerr != nil {
				c.Reject = aerr.Error()
			}
			cases = append(cases, c)
		}
	}
	return cases
}

// isPlpgsqlProceduralStmt reports whether a statement carries a PL/pgSQL body the
// Tier-1 analyzer inspects: a DO block (defaults to LANGUAGE plpgsql) or a
// CREATE FUNCTION/PROCEDURE declared LANGUAGE plpgsql.
func isPlpgsqlProceduralStmt(stmt ast.Stmt) bool {
	switch s := stmt.(type) {
	case *ast.DoStmt:
		return true
	case *ast.CreateFunctionStmt:
		return functionLanguageIsPlpgsql(s)
	}
	return false
}

func functionLanguageIsPlpgsql(s *ast.CreateFunctionStmt) bool {
	if s.Options == nil {
		return false
	}
	for _, item := range s.Options.Items {
		de, ok := item.(*ast.DefElem)
		if !ok || de.Defname != "language" {
			continue
		}
		if str, ok := de.Arg.(*ast.String); ok {
			return strings.EqualFold(str.SVal, "plpgsql")
		}
	}
	return false
}

// stripCorpusBackslashLines blanks psql meta-command lines (\c, \echo, \gset, …),
// which are not SQL and would stop the lexer. Newlines are kept so byte offsets
// (used for statement slicing) stay aligned with the source. Mirrors the SQL
// parser corpus helper of the same shape.
func stripCorpusBackslashLines(s string) string {
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

// splitCorpusStatements splits SQL into top-level statements at ';'. Dollar-quoted
// and single-quoted bodies are single lexer tokens, so their internal ';' never
// leak out as separators.
func splitCorpusStatements(sql string) []string {
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

func readProceduralCases(t *testing.T, path string) []proceduralCase {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoErrorf(t, err, "reading %s", path)
	var cases []proceduralCase
	require.NoErrorf(t, json.Unmarshal(data, &cases), "parsing %s", path)
	return cases
}

func writeProceduralCases(t *testing.T, path string, cases []proceduralCase) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	require.NoError(t, enc.Encode(cases))
}
