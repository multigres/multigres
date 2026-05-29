// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2025, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
package parser

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// ParseTest represents a single test case for the parser
type ParseTest struct {
	Comment  string `json:"comment,omitempty"`
	Query    string `json:"query,omitempty"`
	Expected string `json:"expected,omitempty"` // If empty, defaults to Query
	Error    string `json:"error,omitempty"`
}

// parseTestSuite is the test suite for parser tests
type parseTestSuite struct {
	suite.Suite
	outputDir string
}

// SetupSuite prepares the test output directory
func (s *parseTestSuite) SetupSuite() {
	dir := getTestExpectationDir()
	err := os.RemoveAll(dir)
	require.NoError(s.T(), err)
	err = os.Mkdir(dir, 0o755)
	require.NoError(s.T(), err)
	s.outputDir = dir
}

// TestParseTestSuite runs the parser test suite
func TestParseTestSuite(t *testing.T) {
	suite.Run(t, new(parseTestSuite))
}

var expectedDir = "testdata/expected"

func getTestExpectationDir() string {
	return filepath.Clean(expectedDir)
}

// testFile runs tests from a JSON test file
func (s *parseTestSuite) testFile(filename string) {
	s.T().Run(filename, func(t *testing.T) {
		failed := false
		var expected []ParseTest

		for _, tcase := range readJSONTests(filename) {
			testName := tcase.Comment
			if testName == "" {
				testName = tcase.Query
			}
			if tcase.Query == "" {
				continue
			}

			// Use Expected if provided, otherwise default to Query
			expectedQuery := tcase.Expected
			if expectedQuery == "" {
				expectedQuery = tcase.Query
			}

			current := ParseTest{
				Comment:  tcase.Comment,
				Query:    tcase.Query,
				Expected: expectedQuery,
			}

			// Parse both the query and and parse the output again.
			parsedOutput, err := getParserOutput(tcase.Query)
			var secondErr error
			var secondParsedOutput string
			if parsedOutput != "" {
				secondParsedOutput, secondErr = getParserOutput(parsedOutput)
			}

			t.Run(testName, func(t *testing.T) {
				defer func() {
					// Use the actual output to store the files.
					if current.Query != parsedOutput {
						current.Expected = parsedOutput
					} else {
						current.Expected = ""
					}
					if err != nil {
						current.Error = err.Error()
					}
					expected = append(expected, current)
					if t.Failed() {
						failed = true
					}
				}()

				// Check if we expect an error
				if tcase.Error != "" {
					require.ErrorContains(t, err, tcase.Error)
				} else {
					// We expect a successful parse
					require.NoError(t, err)
					require.EqualValues(t, expectedQuery, parsedOutput)
					require.NoError(t, secondErr, tcase.Query)
					require.EqualValues(t, expectedQuery, secondParsedOutput, tcase.Query)
				}
			})
		}

		// Write updated test file if there were failures
		if s.outputDir != "" && failed {
			name := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
			name = filepath.Join(s.outputDir, name+".json")
			file, err := os.Create(name)
			require.NoError(t, err)
			defer file.Close()

			enc := json.NewEncoder(file)
			enc.SetIndent("", "  ")
			// Keep SQL readable: by default the encoder HTML-escapes <, >, and &
			// to <, >, &, which clutters the SQL test data.
			enc.SetEscapeHTML(false)
			err = enc.Encode(expected)
			require.NoError(t, err)

			t.Logf("Updated test expectations written to: %s", name)
		}
	})
}

// readJSONTests reads test cases from a JSON file
func readJSONTests(filename string) []ParseTest {
	var output []ParseTest
	filepath := locateFile(filename)

	// Check if file exists
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		// Return empty slice if file doesn't exist
		return output
	}

	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	dec.DisallowUnknownFields()
	err = dec.Decode(&output)
	if err != nil {
		panic(err)
	}
	return output
}

// locateFile returns the full path to a test data file
func locateFile(name string) string {
	return filepath.Join("testdata", name)
}

// getParserOutput parses a query and returns the JSON representation of the AST
func getParserOutput(query string) (string, error) {
	// Parse the query using the actual parser
	asts, err := ParseSQL(query)
	// Check for parse errors
	if err != nil {
		return "", err
	}
	if len(asts) == 0 {
		return "", errors.New("no ASTs generated")
	}

	var sqls string
	for _, ast := range asts {
		if sqls != "" {
			sqls = sqls + "; "
		}
		sqls = sqls + ast.SqlString()
	}

	// Deparse query.
	return sqls, nil
}

// TestParsing is used test the parsing support.
func (s *parseTestSuite) TestParsing() {
	s.testFile("select_cases.json")
	s.testFile("misc_cases.json")
	s.testFile("ddl_cases.json")
	s.testFile("dml_cases.json")
	s.testFile("set_cases.json")
}

// TestOne is for testing a single case during development
func (s *parseTestSuite) TestOne() {
	// This can be used to test a single case file during development
	s.testFile("onecase.json")
}

// boolOptionSpellings maps the non-canonical boolean spellings PostgreSQL
// accepts as option values to the form the deparser emits. It only rewrites the
// spelling (never removes the value), so a dropped option still fails the check.
var boolOptionSpellings = map[string]string{
	"on":  "true",
	"off": "false",
	"yes": "true",
	"no":  "false",
}

// canonicalizeForRoundtrip rewrites stmt in place so that an AST and its
// deparse→re-parse counterpart differ only when the deparser actually lost or
// changed information (the "real bug" cases), not when it merely re-spelled
// something in an equivalent canonical form. It also zeroes source locations.
//
// Each rule below is deliberately narrow and applied to *both* sides of the
// comparison. Crucially, the rules only collapse node classes where the
// deparser is loss-free (boolean option spellings and bare flags, the CROSS
// JOIN spelling, the implicit pg_catalog type schema, the default IN parameter
// mode, and order-independent ALTER DEFAULT PRIVILEGES options). They never
// touch a clause whose absence would indicate information loss, so genuine
// deparser bugs still surface.
func canonicalizeForRoundtrip(stmt ast.Stmt) ast.Stmt {
	result := ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *ast.DefElem:
			// A bare boolean flag (VACUUM FULL) and its explicit "= true" form
			// are equivalent; the deparser renders both as just the name. Only
			// a *Boolean arg is collapsed: the grammar produces *Boolean solely
			// for genuine boolean flags, whereas arbitrary option values (an FDW
			// OPTIONS string, a reloption) are *String/*Integer and are left
			// untouched so a dropped or changed value still fails the check.
			if b, ok := n.Arg.(*ast.Boolean); ok && b.BoolVal {
				n.Arg = nil
			}
		case *ast.String:
			// Boolean option values are deparsed in canonical spelling
			// (on->true, off->false). Only an option value is touched, never a
			// data literal, and only the spelling changes, so a dropped option
			// value still fails the check.
			if _, ok := cursor.Parent().(*ast.DefElem); ok {
				if v, isBool := boolOptionSpellings[strings.ToLower(n.SVal)]; isBool {
					n.SVal = v
				}
			}
		case *ast.ExplainStmt:
			// EXPLAIN options are a fixed set of boolean flags; a bare flag
			// (Arg=nil) and its "= true" form are equivalent and the deparser
			// materializes the bare form as "<flag> true". Scope the collapse to
			// these statements so arbitrary option values elsewhere (FDW OPTIONS,
			// reloptions) are never touched.
			collapseBoolFlagOptions(n.Options)
		case *ast.VacuumStmt:
			// VACUUM/ANALYZE options are likewise boolean flags (FULL, VERBOSE,
			// SKIP_LOCKED, ...); same bare/"true" equivalence as EXPLAIN.
			collapseBoolFlagOptions(n.Options)
		case *ast.JoinExpr:
			// CROSS JOIN is deparsed as INNER JOIN ON TRUE.
			if n.Jointype == ast.JOIN_INNER && !n.IsNatural &&
				n.UsingClause == nil && isTrueConst(n.Quals) {
				n.Quals = nil
			}
		case *ast.TypeName:
			// pg_catalog is the implicit schema for built-in types, so
			// `pg_catalog.int4` and `int4` are the same type. The deparser drops
			// the explicit namespace, so strip a leading pg_catalog qualifier.
			stripLeadingPgCatalog(n.Names)
		case *ast.FuncCall:
			// The grammar qualifies SQL-standard built-ins (substring, extract,
			// overlay, ...) as pg_catalog.<fn>; the deparser renders them plain.
			// pg_catalog is their implicit schema, so strip it the same way.
			stripLeadingPgCatalog(n.Funcname)
			// Funcformat records whether the call used special SQL syntax
			// (COERCE_SQL_SYNTAX, e.g. `collation for (x)`, `TRIM(...)`) or a
			// plain call; it is display metadata that flips when the deparser
			// emits the plain-call form, so normalize it.
			n.Funcformat = ast.COERCE_EXPLICIT_CALL
		case *ast.FunctionParameter:
			// IN is the default parameter mode and PostgreSQL omits it when
			// printing, so a parameter parsed as IN deparses without a mode and
			// re-parses as DEFAULT. The two are documented as equivalent.
			if n.Mode == ast.FUNC_PARAM_IN {
				n.Mode = ast.FUNC_PARAM_DEFAULT
			}
		case *ast.SelectStmt:
			// `LIMIT NULL`, `LIMIT ALL`, and no LIMIT clause are all equivalent (no
			// row limit). `LIMIT NULL` parses to a NULL-constant LimitCount
			// (Isnull=true); the deparser renders it as `LIMIT ALL`, which re-parses
			// to an A_Const wrapping a Null node (Isnull=false). Collapse either
			// NULL-constant form to nil so the spellings compare equal; a real count
			// is left untouched.
			if c, ok := n.LimitCount.(*ast.A_Const); ok {
				if _, isNull := c.Val.(*ast.Null); c.Isnull || isNull {
					n.LimitCount = nil
				}
			}
		case *ast.AlterDefaultPrivilegesStmt:
			// The IN SCHEMA and FOR ROLE options are order-independent but held
			// in an ordered list; the deparser emits them in a canonical order.
			// Sort by defname so the comparison ignores the original order.
			if n.Options != nil {
				sortDefElemsByName(n.Options.Items)
			}
		}
		if n := cursor.Node(); n != nil {
			n.SetLocation(0)
		}
		return true
	}, nil)
	return result.(ast.Stmt)
}

// sortDefElemsByName stably sorts a slice of DefElem nodes by their defname.
// Items that are not DefElems are ordered by an empty key.
func sortDefElemsByName(items []ast.Node) {
	sort.SliceStable(items, func(i, j int) bool {
		return defElemName(items[i]) < defElemName(items[j])
	})
}

func defElemName(n ast.Node) string {
	if de, ok := n.(*ast.DefElem); ok {
		return de.Defname
	}
	return ""
}

// collapseBoolFlagOptions normalizes a boolean-flag option list (EXPLAIN /
// VACUUM / ANALYZE) so a bare flag and its explicit "= true" form compare equal.
// True-valued args (absent, Boolean(true), or a true-ish string) become absent;
// false and other values are left untouched.
func collapseBoolFlagOptions(options *ast.NodeList) {
	if options == nil {
		return
	}
	for _, item := range options.Items {
		if de, ok := item.(*ast.DefElem); ok && isTrueFlagArg(de.Arg) {
			de.Arg = nil
		}
	}
}

// isTrueFlagArg reports whether a DefElem arg means "true" for a boolean flag:
// absent, Boolean(true), or a true-ish string spelling.
func isTrueFlagArg(n ast.Node) bool {
	switch v := n.(type) {
	case nil:
		return true
	case *ast.Boolean:
		return v.BoolVal
	case *ast.String:
		switch strings.ToLower(v.SVal) {
		case "true", "on", "yes":
			return true
		}
	}
	return false
}

// stripLeadingPgCatalog removes a leading "pg_catalog" qualifier from a
// qualified name (a list of String nodes). pg_catalog is the implicit schema for
// built-in types and functions, so `pg_catalog.x` and `x` are the same object.
func stripLeadingPgCatalog(names *ast.NodeList) {
	if names == nil || len(names.Items) <= 1 {
		return
	}
	if s, ok := names.Items[0].(*ast.String); ok && s.SVal == "pg_catalog" {
		names.Items = names.Items[1:]
	}
}

// isTrueConst reports whether n is the boolean constant TRUE.
func isTrueConst(n ast.Node) bool {
	c, ok := n.(*ast.A_Const)
	if !ok {
		return false
	}
	b, ok := c.Val.(*ast.Boolean)
	return ok && b.BoolVal
}

// canonicalizeStmts applies canonicalizeForRoundtrip to each statement in place.
// It must be called only after the statements have been deparsed, since
// canonicalization mutates the tree (see canonicalizeForRoundtrip).
func canonicalizeStmts(stmts []ast.Stmt) {
	for i := range stmts {
		stmts[i] = canonicalizeForRoundtrip(stmts[i])
	}
}

// deparseStmts renders a slice of statements back to SQL, mirroring how
// getParserOutput joins multiple statements.
func deparseStmts(stmts []ast.Stmt) string {
	var sqls string
	for _, stmt := range stmts {
		if sqls != "" {
			sqls += "; "
		}
		sqls += stmt.SqlString()
	}
	return sqls
}

// roundtripFile runs AST round-trip checks for a single test file. For every
// query that parses successfully it parses the query, deparses the resulting
// AST, and parses the deparsed SQL again. The AST from the first parse must be
// structurally identical to the AST from the second parse. This catches deparser
// bugs where the emitted SQL is syntactically valid but does not reconstruct the
// original tree.
//
// The pristine first parse is deparsed *before* canonicalization, because
// canonicalization mutates the tree (e.g. collapses a boolean arg) in ways that
// would change deparse output. Both trees are canonicalized only afterwards, for
// the comparison.
func (s *parseTestSuite) roundtripFile(filename string) {
	s.T().Run(filename, func(t *testing.T) {
		for _, tcase := range readJSONTests(filename) {
			if tcase.Query == "" {
				continue
			}

			firstStmts, err := ParseSQL(tcase.Query)
			if err != nil {
				// Queries that fail to parse are exercised by the parse tests;
				// there is nothing to round-trip here.
				continue
			}

			testName := tcase.Comment
			if testName == "" {
				testName = tcase.Query
			}

			t.Run(testName, func(t *testing.T) {
				deparsed := deparseStmts(firstStmts)
				secondStmts, err := ParseSQL(deparsed)
				require.NoError(t, err,
					"re-parsing the deparsed query failed\nquery:    %s\ndeparsed: %s",
					tcase.Query, deparsed)

				canonicalizeStmts(firstStmts)
				canonicalizeStmts(secondStmts)
				require.Equal(t, firstStmts, secondStmts,
					"AST changed after round-trip\nquery:    %s\ndeparsed: %s",
					tcase.Query, deparsed)
			})
		}
	})
}

// TestParseDeparseRoundtrip verifies that parsing a query, deparsing it, and
// parsing the result again yields a structurally identical AST. It covers both
// the curated case files and the full PostgreSQL regression corpus.
// canonicalizeForRoundtrip collapses the deparser's loss-free spellings so that
// only genuine deparser bugs fail here.
func (s *parseTestSuite) TestParseDeparseRoundtrip() {
	s.roundtripFile("select_cases.json")
	s.roundtripFile("misc_cases.json")
	s.roundtripFile("ddl_cases.json")
	s.roundtripFile("dml_cases.json")
	s.roundtripFile("set_cases.json")
	// deparse_cases.json is the curated deparse test corpus imported from
	// pganalyze/libpg_query (test/deparse_tests.c). It targets deparser
	// edge cases across statement types.
	s.roundtripFile("deparse_cases.json")

	postgresDir := "testdata/postgres"
	files, err := os.ReadDir(postgresDir)
	require.NoError(s.T(), err, "failed to read postgres test directory")
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			s.roundtripFile(filepath.Join("postgres", file.Name()))
		}
	}
}

// identifierMangleSuffix is appended to every user identifier in a parsed tree
// by the identifier-rewrite round-trip test. It deliberately contains a space, a
// double quote, and an uppercase letter so that the resulting identifier *must*
// be double-quoted (and its embedded quote doubled) to deparse to valid SQL. Any
// deparse path that emits an identifier without going through QuoteIdentifier
// will therefore produce SQL that fails to re-parse or re-parses to a different
// tree, surfacing the quoting bug.
const identifierMangleSuffix = ` "Mixed`

// simpleIdentifier matches a bare, lower-case SQL identifier that needs no
// quoting today: it is the shape the deparser is expected to start quoting once
// mangled. Restricting mangling to this shape is what lets the rewriter visit
// *String nodes indiscriminately — operator tokens ("+", "~~"), dollar-quoted
// function bodies, multi-word option values, and identifiers that already require
// quoting all fail the match and are left untouched, so they never masquerade as
// quoting failures.
var simpleIdentifier = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// mangleIdent appends identifierMangleSuffix to name, but only when name is a
// simple lower-case identifier (see simpleIdentifier). Anything else — an empty
// string (an absent qualifier), an operator, a body, an already-quoted-requiring
// name — is returned unchanged.
func mangleIdent(name string) string {
	if !simpleIdentifier.MatchString(name) {
		return name
	}
	return name + identifierMangleSuffix
}

// isBuiltinTypeNameList reports whether names denotes a built-in type keyword
// (using the deparser's own ast.IsBuiltInType). It matches both the bare form
// ([interval]) and the [pg_catalog, <type>] form the grammar produces for some
// keywords, by testing the final name component. Built-in types are SQL keywords
// rendered with bespoke syntax, not user identifiers, so the rewrite leaves them
// alone.
func isBuiltinTypeNameList(names *ast.NodeList) bool {
	if names == nil || len(names.Items) == 0 {
		return false
	}
	last, ok := names.Items[len(names.Items)-1].(*ast.String)
	if !ok {
		return false
	}
	return ast.IsBuiltInType(last.SVal)
}

// isPgCatalogQualified reports whether a qualified name (function or type) is
// explicitly rooted at pg_catalog. The parser synthesizes such names for system
// objects the deparser reconstructs with special syntax — the similar_to_escape
// helper behind SIMILAR TO, the implicit `pg_catalog.record` return type of
// RETURNS TABLE, and so on. Those names are never emitted as plain identifiers,
// so mangling them produces spurious diffs rather than quoting failures.
func isPgCatalogQualified(names *ast.NodeList) bool {
	if names == nil || len(names.Items) < 2 {
		return false
	}
	first, ok := names.Items[0].(*ast.String)
	return ok && first.SVal == "pg_catalog"
}

// collectNonIdentifierStrings records the *String nodes that the parser uses for
// things other than user identifiers but which still look like simple identifiers
// (so the shape guard in mangleIdent would not catch them): option values, and
// built-in / pg_catalog type keywords. These sit in fixed-keyword grammar
// positions, so mangling them produces SQL that fails to parse for reasons that
// have nothing to do with identifier quoting. Operator tokens, function bodies,
// and already-complex names are handled by mangleIdent's shape guard instead.
//
// Note we do not need to special-case functions here: this test only checks that
// the deparsed SQL re-parses, and mangling a function name simply pushes the
// deparser onto its plain-call path (e.g. EXTRACT(x FROM y) becomes a quoted
// "extract"(...) call), which is valid SQL.
func collectNonIdentifierStrings(stmt ast.Stmt, skip map[*ast.String]bool) {
	add := func(list *ast.NodeList) {
		if list == nil {
			return
		}
		for _, item := range list.Items {
			if s, ok := item.(*ast.String); ok {
				skip[s] = true
			}
		}
	}
	ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *ast.DefElem:
			// A DefElem.Arg holds an option *value*, which is almost always a
			// fixed-vocabulary keyword or enum rather than a user identifier:
			// VOLATILE / SECURITY DEFINER on a function, LANGUAGE sql, STORAGE
			// NONE / ALIGNMENT 8 on a type, COPY FORMAT csv, reloptions, FDW
			// OPTIONS, and so on. The deparser emits these verbatim in grammar
			// positions that do not accept a quoted identifier, so mangling one
			// (e.g. VOLATILE -> "VOLATILE ""Mixed") produces SQL that fails to
			// parse for reasons that have nothing to do with identifier quoting —
			// removing this skip adds ~1400 such false positives. (A handful of
			// option values genuinely are identifiers, e.g. RECEIVE = funcname;
			// we give up stress-testing those rather than special-case every
			// option name. The option name itself is a Go string, not a *String.)
			if s, ok := n.Arg.(*ast.String); ok {
				skip[s] = true
			}
		case *ast.TypeName:
			// Built-in type keywords (int, varchar, interval, ...) and
			// pg_catalog-qualified types are rendered with bespoke syntax, not as
			// plain identifiers.
			if isBuiltinTypeNameList(n.Names) || isPgCatalogQualified(n.Names) {
				add(n.Names)
			}
		case *ast.VariableSetStmt:
			// SET XML OPTION { DOCUMENT | CONTENT } stores its value as a bare
			// keyword String that the deparser emits verbatim (uppercased), not as
			// a quotable identifier — so mangling it would be a false positive.
			if n.Name == "xmloption" {
				add(n.Args)
			}
		}
		return true
	}, nil)
}

// rewriteIdentifiers rewrites stmt in place so that every user-supplied
// identifier requires double-quoting, and returns it.
//
// Identifiers reach the AST two ways, and both are handled here: as *ast.String
// nodes (column references, function and type names, alias and CTE column lists,
// collation names, ...) and as plain Go string fields on structural nodes
// (RangeVar, ResTarget, ColumnDef, ...). The *String nodes are mangled generically
// — that is the parser's identifier representation — except the literals and
// special tokens gathered by collectNonIdentifierStrings. The Go string fields
// have no node to visit, so each is listed explicitly.
func rewriteIdentifiers(stmt ast.Stmt) ast.Stmt {
	skip := map[*ast.String]bool{}
	collectNonIdentifierStrings(stmt, skip)

	result := ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *ast.String:
			// A *String directly under an A_Const is a string literal, not an
			// identifier. Everything else is an identifier unless skipped above.
			if _, isLiteral := cursor.Parent().(*ast.A_Const); isLiteral {
				return true
			}
			if !skip[n] {
				n.SVal = mangleIdent(n.SVal)
			}
		case *ast.RangeVar:
			n.CatalogName = mangleIdent(n.CatalogName)
			n.SchemaName = mangleIdent(n.SchemaName)
			n.RelName = mangleIdent(n.RelName)
		case *ast.Alias:
			// ColNames is a *String list, mangled by the *String case.
			n.AliasName = mangleIdent(n.AliasName)
		case *ast.ResTarget:
			// A SELECT-list output name or UPDATE target column.
			n.Name = mangleIdent(n.Name)
		case *ast.ColumnDef:
			n.Colname = mangleIdent(n.Colname)
		case *ast.AlterTableCmd:
			// The column/constraint/trigger the command acts on. Some subcommands
			// (e.g. ALTER ATTRIBUTE) also carry a redundant ColumnDef whose Colname
			// mirrors this, so both must be mangled to stay consistent.
			n.Name = mangleIdent(n.Name)
		case *ast.CommonTableExpr:
			// Aliascolnames is a *String list, mangled by the *String case.
			n.Ctename = mangleIdent(n.Ctename)
		}
		return true
	}, nil)
	return result.(ast.Stmt)
}

// TestParseIdentifierRewriteRoundtrip stress-tests identifier quoting in the
// deparser. For every query that parses, it rewrites every identifier in the AST
// to a form that requires double-quoting (see identifierMangleSuffix), deparses
// the rewritten tree, and parses the result again. The only requirement is that
// the deparsed SQL parses.
//
// A failure means some deparse path emitted an identifier without the quoting (or
// quote-escaping) its mangled form requires, so the result is no longer valid SQL.
// The test intentionally does not compare the re-parsed AST against the rewritten
// one — an equivalent re-spelling is fine here; AST fidelity is the separate
// concern of TestParseDeparseRoundtrip. This catches quoting bugs that the plain
// round-trip test (which never sees an identifier needing quotes) cannot.
//
// The rewriter captures identifiers generically (every simple-identifier *String
// node plus the structural Go-string fields), so this guards every deparse path
// reached by the corpus against forgetting to quote an identifier. A new failure
// means a deparse path emits an identifier (or qualified name part) raw instead of
// through QuoteIdentifier.
func (s *parseTestSuite) TestParseIdentifierRewriteRoundtrip() {
	s.identifierRewriteFile("select_cases.json")
	s.identifierRewriteFile("misc_cases.json")
	s.identifierRewriteFile("ddl_cases.json")
	s.identifierRewriteFile("dml_cases.json")
	s.identifierRewriteFile("set_cases.json")

	postgresDir := "testdata/postgres"
	files, err := os.ReadDir(postgresDir)
	require.NoError(s.T(), err, "failed to read postgres test directory")
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			s.identifierRewriteFile(filepath.Join("postgres", file.Name()))
		}
	}
}

// identifierRewriteFile runs the identifier-rewrite round-trip for one test file.
func (s *parseTestSuite) identifierRewriteFile(filename string) {
	s.T().Run(filename, func(t *testing.T) {
		for _, tcase := range readJSONTests(filename) {
			if tcase.Query == "" {
				continue
			}

			stmts, err := ParseSQL(tcase.Query)
			if err != nil {
				// Queries that fail to parse are exercised by the parse tests;
				// there is nothing to round-trip here.
				continue
			}

			testName := tcase.Comment
			if testName == "" {
				testName = tcase.Query
			}

			t.Run(testName, func(t *testing.T) {
				rewritten := make([]ast.Stmt, len(stmts))
				for i, stmt := range stmts {
					rewritten[i] = rewriteIdentifiers(stmt)
				}

				// We only assert that the deparsed SQL parses. We deliberately do
				// not compare the re-parsed AST to the rewritten one: this test is
				// about quoting identifiers in the right places, and the deparser is
				// free to choose an equivalent spelling that re-parses to a slightly
				// different (but valid) tree. AST round-trip fidelity is the job of
				// TestParseDeparseRoundtrip. A parse error here means an identifier
				// was emitted without the quoting its mangled form requires.
				deparsed := deparseStmts(rewritten)
				_, err := ParseSQL(deparsed)
				require.NoError(t, err,
					"re-parsing the identifier-mangled query failed — an identifier was deparsed without proper quoting\nquery:    %s\ndeparsed: %s",
					tcase.Query, deparsed)
			})
		}
	})
}

// TestPostgresTestsParsing runs tests from all PostgreSQL test files
// The tests have been ported over from ./src/test/regress/sql in the postgres code.
func (s *parseTestSuite) TestPostgresTestsParsing() {
	// Read all JSON files from the postgres directory
	postgresDir := "testdata/postgres"
	files, err := os.ReadDir(postgresDir)
	if err != nil {
		s.T().Fatalf("Failed to read postgres test directory: %v", err)
	}

	// Test each JSON file
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			s.testFile(filepath.Join("postgres", file.Name()))
		}
	}
}
