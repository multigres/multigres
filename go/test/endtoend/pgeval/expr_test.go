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

package pgeval

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/analyze"
	"github.com/multigres/multigres/go/common/pgeval/exec"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// expressionCase is one expression compared against the server. params are
// the parameter types declared to Parse, 0 meaning "deduce", exactly as an
// extended-protocol client would send them.
type expressionCase struct {
	sql    string
	params []pgoid.Oid
}

// expressionCorpus generates the expressions to compare: every binary and
// prefix operator, cast, function and SQL construct in the ported families
// over a fixed operand set that mixes widths, unknown literals, booleans,
// NULL and oversize integers, plus hand-written parameter-deduction cases.
func expressionCorpus() []expressionCase {
	operands := []string{
		"1", "2::int2", "3::int8", "'4'", "true", "'f'", "NULL", "2147483648", "-5", "0",
		"int2 '7'", "'0x10'::int4", "(1 + 2)", "'1'::int8 * 2",
	}
	binary := []string{"+", "-", "*", "/", "%", "=", "<>", "<", "<=", ">", ">=", "&", "|", "#", "<<", ">>", "AND", "OR"}
	prefix := []string{"-", "+", "~", "NOT"}
	unary := []string{"abs", "int2", "int4", "int8", "bool", "text", "oid", "pg_catalog.int4", "int4inc", "int8um", "gcd"}
	binaryFuncs := []string{"gcd", "lcm", "int4pl", "int4larger", "int8smaller", "mod", "int48pl", "concat"}
	unaryTemplates := []string{
		"(%s) IS NULL", "(%s) IS TRUE", "(%s) IS NOT FALSE", "CAST(%s AS int8)", "(%s)::int2",
		"(%s)::bool", "(%s)::text", "(%s)::oid", "(%s)::int4::int8::int2",
	}
	pairTemplates := []string{
		"CASE WHEN %s THEN %s END", "CASE %s WHEN %s THEN 1 ELSE 2 END", "COALESCE(%s, %s)", "GREATEST(%s, %s)",
		"LEAST(%s, %s)", "NULLIF(%s, %s)", "%s IS DISTINCT FROM %s", "%s IS NOT DISTINCT FROM %s",
		"%s IN (%s, 1)", "%s NOT IN (%s)", "%s BETWEEN %s AND 10", "%s NOT BETWEEN SYMMETRIC %s AND 10",
		"ARRAY[%s, %s]", "(%s) = ANY (ARRAY[%s])",
	}
	var cases []expressionCase
	add := func(sql string) { cases = append(cases, expressionCase{sql: sql}) }
	for _, l := range operands {
		for _, op := range prefix {
			add(op + " " + l)
		}
		for _, fn := range unary {
			add(fn + "(" + l + ")")
		}
		for _, tmpl := range unaryTemplates {
			add(fmt.Sprintf(tmpl, l))
		}
		for _, r := range operands {
			for _, op := range binary {
				add(l + " " + op + " " + r)
			}
			for _, fn := range binaryFuncs {
				add(fn + "(" + l + ", " + r + ")")
			}
			for _, tmpl := range pairTemplates {
				add(fmt.Sprintf(tmpl, l, r))
			}
		}
	}
	// Error paths that the generated corpus does not reach.
	for _, sql := range []string{
		"abs()", "abs(1, 2)", "nosuch(1)", "abs(*)", "pi(*)", "abs(DISTINCT 1)", "abs(1 ORDER BY 1)",
		"1::nosuchtype", "'abc'::int4", "'99999999999'::int4", "ARRAY[]", "ARRAY[1, true]", "ARRAY[[1], [2, 3]]::int4[]",
		"CASE WHEN 1 THEN 1 END", "COALESCE(1, true)", "1 OPERATOR(pg_catalog.+) 2", "1 OPERATOR(pg_catalog.<>) '2'",
		"int4(1::int8)", "int8('5')", "text(true)", "oid(1)", "- - 1", "NOT NOT true", "1 / 0", "2147483647 + 1",
		"(-2147483648)::int4 / -1", "1::int2 << 100", "gcd(-2147483648, -2147483648)", "abs(-2147483648::int4)",
		"CASE WHEN true THEN 1 ELSE 2::int8 END", "CASE 1 WHEN '1' THEN true END", "COALESCE('1', 2::int2)",
		"1 IN (1, 2::int8, '3')", "'1' IN (1, 2)", "NULL IS DISTINCT FROM NULL", "NULLIF(1, 2::int8)",
		"1 BETWEEN '0' AND 2::int8", "ARRAY[[1, 2], [3, 4]]", "ARRAY[1, '2']::int8[]", "ARRAY[]::int4[]",
		"concat(1, true, 2::int8)", "int4smaller(1, 2::int2)", "json_extract_path('{}'::json, 'a')",
		"1 = ANY ('{1,2}')", "1 = ALL (ARRAY[1, 2])", "true = ANY (ARRAY[true])",
		"array_append(ARRAY[1], 2)", "array_append(ARRAY[1], '2')", "array_append(ARRAY[1], true)",
		"array_cat(ARRAY[1], ARRAY[2::int8])", "array_position(ARRAY[1, 2], 2::int8)", "array_ndims(ARRAY[[1], [2]])",
		"array_fill(1::int8, ARRAY[2])", "array_fill('1', ARRAY[2])", "array_lower(ARRAY[1], 1)", "array_ndims(1)",
		"array_remove(ARRAY[1::int8], 1)", "ARRAY[1]::int8[]", "ARRAY[[1, 2]]::int2[]", "ARRAY[1] = ARRAY[1::int8]",
		// Defaults installed by system_functions.sql: the server accepts the
		// short calls, the engine must decline them rather than report
		// "function does not exist".
		"random_normal()", "random_normal(0)", "random_normal(0, 1)", "make_interval(1)", "make_interval()",
		"pg_terminate_backend(1)", "jsonb_set('{}', '{a}', '1')", "parse_ident('a.b')",
		// Column naming (FigureColname) across node kinds the corpus does not
		// otherwise produce at the top level.
		"true", "'x'", "1::integer", "CAST(1 AS int)", "(1)", "CASE WHEN true THEN 1 ELSE abs(2) END",
		"CASE WHEN true THEN 1 ELSE 2::int8 END", "ROW(1, 2)", "current_date", "current_user", "(SELECT 1)",
		"(SELECT 1 AS n)", "EXISTS (SELECT 1)", "ARRAY(SELECT 1)", "abs(1)::int8", "int4 '1'", "pg_catalog.abs(1)::int8",
		// Scalar subqueries whose name the transform stage would have
		// produced: set operations (leftmost leaf), VALUES (column1), the
		// "?column?" fallback winning over an outer cast, and WITH.
		"(SELECT 1)::int8", "(SELECT abs(1) UNION SELECT 2 LIMIT 1)", "(SELECT 1 AS n UNION ALL SELECT 2)",
		"((SELECT abs(1) UNION SELECT 2) INTERSECT SELECT 3)", "(SELECT 1 EXCEPT SELECT abs(2))",
		"(SELECT abs(1) UNION SELECT 2)::int8", "(VALUES (1))", "(VALUES (1), (2) LIMIT 1)", "(VALUES (abs(1)))",
		"(VALUES (1) UNION SELECT abs(2))", "(SELECT abs(1) UNION VALUES (2))", "(WITH c AS (SELECT 1) SELECT abs(2))",
		"(WITH c AS (SELECT 1) SELECT 2 UNION SELECT 3)",
	} {
		add(sql)
	}
	// Parameter deduction. A declared 0 is PostgreSQL's "let the server
	// deduce", so each of these exercises variable_coerce_param_hook.
	i2, i4, i8, b := pgoid.INT2OID, pgoid.INT4OID, pgoid.INT8OID, pgoid.BOOLOID
	cases = append(cases, []expressionCase{
		{"$1 + 1", []pgoid.Oid{0}},
		{"$1 + 1", []pgoid.Oid{i8}},
		{"$1 + 1", []pgoid.Oid{i2}},
		{"1 + $1", []pgoid.Oid{0}},
		{"$1 = true", []pgoid.Oid{0}},
		{"abs($1)", []pgoid.Oid{0}},
		{"abs($1)", []pgoid.Oid{i2}},
		{"$1::int8 + $1", []pgoid.Oid{0}},
		{"$1 IS NULL", []pgoid.Oid{0}},
		{"$1 IS NULL", []pgoid.Oid{i4}},
		{"CASE WHEN $1 THEN 1 END", []pgoid.Oid{0}},
		{"COALESCE($1, 1)", []pgoid.Oid{0}},
		{"$1 BETWEEN 1 AND $2", []pgoid.Oid{0, 0}},
		{"1 IN ($1, 2)", []pgoid.Oid{0}},
		{"$1 IN (1, 2)", []pgoid.Oid{0}},
		{"$1 + $2", []pgoid.Oid{0, 0}},
		{"$1 AND $2", []pgoid.Oid{0, 0}},
		{"NOT $1", []pgoid.Oid{0}},
		{"$1::int4", []pgoid.Oid{0}},
		{"$1::int4", []pgoid.Oid{i8}},
		{"-$1", []pgoid.Oid{0}},
		{"-$1", []pgoid.Oid{i8}},
		{"$1 = ANY ($2)", []pgoid.Oid{i4, 0}},
		{"concat($1, $2)", []pgoid.Oid{0, i4}},
		{"int4($1)", []pgoid.Oid{0}},
		{"$3", []pgoid.Oid{0}},
		{"$1 + 1", []pgoid.Oid{0, 0}},
		{"$1 + $1::int8", []pgoid.Oid{0}},
		{"$1 > 1 OR $1 < $2", []pgoid.Oid{0, i8}},
		{"NULLIF($1, 1)", []pgoid.Oid{0}},
		{"GREATEST($1, 1::int2)", []pgoid.Oid{0}},
		{"$1 IS DISTINCT FROM 1", []pgoid.Oid{0}},
		{"$1::bool AND $2", []pgoid.Oid{0, b}},
		{"gcd($1, $2)", []pgoid.Oid{i8, 0}},
		{"ARRAY[$1, 1]", []pgoid.Oid{0}},
		{"$1 = $2", []pgoid.Oid{i4, i8}},
		{"$2 + $1", []pgoid.Oid{i2, 0}},
		{"$1 IS NULL AND $1 = 1", []pgoid.Oid{0}},
		{"$1 = 1 AND $1 IS NULL", []pgoid.Oid{0}},
		{"$1 + 1 AND $1::int8 > 0", []pgoid.Oid{0}},
		{"CASE $1 WHEN 1 THEN $1 END", []pgoid.Oid{0}},
		{"-$1 + 1", []pgoid.Oid{i4}},
		{"- 2147483648 + $1", []pgoid.Oid{0}},
	}...)
	return cases
}

// compareExpressions analyzes each corpus expression locally and prepares
// it on the server with the same parameter declarations. The result type,
// deduced parameter types, and any error (SQLSTATE, message, hint) must
// agree. A parameter-free expression that both sides accept and that the
// engine can evaluate is also executed and the output text compared. A
// construct the engine reports as unsupported (SQLSTATE 0A000) is counted
// and skipped: the harness checks what is claimed, not what is missing.
// The one exception is a catalog lookup miss, which the server must share
// (see compareExpression).
func compareExpressions(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	cases := expressionCorpus()
	compared, unsupported, valuesCompared, failures := 0, 0, 0, 0
	skipped := map[string]int{}
	for _, tc := range cases {
		outcome := compareExpression(t, ctx, conn, tc)
		switch outcome.status {
		case "skip":
			unsupported++
			skipped[outcome.reason]++
		case "fail":
			failures++
			t.Errorf("%s (params %v): %s", tc.sql, tc.params, outcome.reason)
			if failures >= 40 {
				t.Fatalf("stopping after %d mismatches", failures)
			}
		default:
			compared++
			if outcome.valueCompared {
				valuesCompared++
			}
		}
	}
	require.NotZero(t, compared)
	t.Logf("Compared %d expressions (%d with values) against PostgreSQL; %d used constructs the engine does not support yet",
		compared, valuesCompared, unsupported)
	reasons := slices.Sorted(maps.Keys(skipped))
	for _, reason := range reasons {
		t.Logf("  skipped %4d: %s", skipped[reason], reason)
	}
}

type expressionOutcome struct {
	status        string // "", "skip" or "fail"
	reason        string
	valueCompared bool
}

func fail(format string, args ...any) expressionOutcome {
	return expressionOutcome{status: "fail", reason: fmt.Sprintf(format, args...)}
}

func compareExpression(t *testing.T, ctx context.Context, conn *pgx.Conn, tc expressionCase) expressionOutcome {
	t.Helper()
	query := "SELECT " + tc.sql
	params := make([]uint32, len(tc.params))
	for i, p := range tc.params {
		params[i] = uint32(p)
	}

	// The server's view: Parse with the declared parameter types.
	var pgErr *pgconn.PgError
	sd, err := conn.PgConn().Prepare(ctx, "", query, params)
	if err != nil && !errors.As(err, &pgErr) {
		return fail("prepare: %v", err)
	}

	// The local view.
	stmts, err := parser.ParseSQL(query)
	if err != nil {
		return fail("local parse: %v", err)
	}
	raw := stmts[0].(*ast.SelectStmt).TargetList.Items[0].(*ast.ResTarget).Val
	// The output column name is figured from the raw tree (parse_target.c), so
	// compare it before analysis consumes the tree. The server names every
	// column it accepts, whether or not the engine can evaluate it.
	if pgErr == nil && len(sd.Fields) == 1 {
		if got, want := analyze.FigureColname(raw), sd.Fields[0].Name; got != want {
			return fail("column name mismatch: server %q, local %q", want, got)
		}
	}
	ps := analyze.NewParseState()
	ps.SetVariableParameters(tc.params)
	tree, localErr := ps.TransformExpr(raw)
	if localErr == nil {
		localErr = ps.CheckVariableParameters(tree)
	}
	if localErr == nil {
		// exec_parse_message's check that every declared parameter got a
		// type, including ones the expression never referenced.
		for i, typ := range ps.ParamTypes() {
			if typ == pgoid.InvalidOid || typ == pgoid.UNKNOWNOID {
				localErr = mterrors.NewPgError("ERROR", mterrors.PgSSIndeterminateDatatype,
					fmt.Sprintf("could not determine data type of parameter $%d", i+1), "")
				break
			}
		}
	}
	if localErr == nil && analyze.ExprType(tree) == pgoid.UNKNOWNOID {
		// An unknown-type result column is resolved to text by the
		// statement-level analysis (parse_target.c), which calls textin;
		// that is not ported yet.
		return expressionOutcome{status: "skip", reason: "unknown-type result column"}
	}

	var localDiag *mterrors.PgDiagnostic
	if localErr != nil && !errors.As(localErr, &localDiag) {
		return fail("local analysis failed with a non-PostgreSQL error: %v", localErr)
	}
	if localDiag != nil && localDiag.Code == mterrors.PgSSFeatureNotSupported {
		if strings.Contains(localDiag.Message, ": no builtin ") {
			// The engine declines a name the builtin catalog does not
			// resolve because a user object might. The corpus references
			// none, so the server must have missed too: a resolution here
			// would mean the local catalog or lookup is wrong.
			switch {
			case pgErr == nil:
				return fail("the builtin catalog found no match but the server resolved it to %s",
					analyze.FormatTypeBe(pgoid.Oid(sd.Fields[0].DataTypeOID)))
			case pgErr.Code != mterrors.PgSSUndefinedFunction && pgErr.Code != mterrors.PgSSUndefinedObject:
				return fail("the builtin catalog found no match but the server failed differently (%s: %s)",
					pgErr.Code, pgErr.Message)
			}
		}
		return expressionOutcome{status: "skip", reason: skipReason(localDiag.Message)}
	}
	if pgErr != nil || localDiag != nil {
		if pgErr == nil {
			return fail("local analysis failed but the server accepted it: %s", localDiag.FullError())
		}
		if localDiag == nil {
			return fail("the server rejected it (%s: %s) but local analysis resolved it to %s",
				pgErr.Code, pgErr.Message, analyze.FormatTypeBe(analyze.ExprType(tree)))
		}
		if pgErr.Code != localDiag.Code || pgErr.Message != localDiag.Message || pgErr.Hint != localDiag.Hint {
			return fail("error mismatch:\n  server: %s %q hint %q\n  local:  %s %q hint %q",
				pgErr.Code, pgErr.Message, pgErr.Hint, localDiag.Code, localDiag.Message, localDiag.Hint)
		}
		return expressionOutcome{}
	}

	// Both accepted: types must agree.
	if len(sd.Fields) != 1 {
		return fail("server returned %d result columns", len(sd.Fields))
	}
	if got, want := analyze.ExprType(tree), pgoid.Oid(sd.Fields[0].DataTypeOID); got != want {
		return fail("result type mismatch: server %s, local %s", analyze.FormatTypeBe(want), analyze.FormatTypeBe(got))
	}
	localParams := ps.ParamTypes()
	if len(localParams) != len(sd.ParamOIDs) {
		return fail("parameter count mismatch: server %v, local %v", sd.ParamOIDs, localParams)
	}
	for i := range localParams {
		if uint32(localParams[i]) != sd.ParamOIDs[i] {
			return fail("parameter $%d type mismatch: server %s, local %s", i+1,
				analyze.FormatTypeBe(pgoid.Oid(sd.ParamOIDs[i])), analyze.FormatTypeBe(localParams[i]))
		}
	}
	if len(tc.params) > 0 {
		return expressionOutcome{}
	}

	// Values: evaluate the analyzed tree with the ported builtins and
	// compare the text output with the server's.
	local, evalErr := evaluate(tree)
	if errors.Is(evalErr, errNotEvaluable) {
		return expressionOutcome{}
	}
	results := conn.PgConn().ExecParams(ctx, query, nil, nil, nil, []int16{0}).Read()
	if results.Err != nil && !errors.As(results.Err, &pgErr) {
		return fail("execute: %v", results.Err)
	}
	if results.Err != nil || evalErr != nil {
		var evalDiag *mterrors.PgDiagnostic
		if evalErr != nil && !errors.As(evalErr, &evalDiag) {
			return fail("local evaluation failed with a non-PostgreSQL error: %v", evalErr)
		}
		switch {
		case pgErr == nil:
			return fail("local evaluation failed but the server returned a value: %s", evalDiag.FullError())
		case evalDiag == nil:
			return fail("the server failed at execution (%s: %s) but local evaluation succeeded", pgErr.Code, pgErr.Message)
		case pgErr.Code != evalDiag.Code || pgErr.Message != evalDiag.Message:
			return fail("execution error mismatch:\n  server: %s %q\n  local:  %s %q",
				pgErr.Code, pgErr.Message, evalDiag.Code, evalDiag.Message)
		}
		return expressionOutcome{valueCompared: true}
	}
	if len(results.Rows) != 1 || len(results.Rows[0]) != 1 {
		return fail("server returned %d rows", len(results.Rows))
	}
	reference := results.Rows[0][0]
	if local.IsNull != (reference == nil) {
		return fail("NULL mismatch: server %q, local null=%v", reference, local.IsNull)
	}
	if !local.IsNull {
		text, err := outputText(analyze.ExprType(tree), local.Value)
		if errors.Is(err, errNotEvaluable) {
			return expressionOutcome{}
		}
		if err != nil {
			return fail("local output function: %v", err)
		}
		if text != string(reference) {
			return fail("value mismatch: server %q, local %q", reference, text)
		}
	}
	return expressionOutcome{valueCompared: true}
}

// skipReason reduces an unsupported-construct message to its construct, so
// the summary groups e.g. every unported input function together.
func skipReason(message string) string {
	if i := strings.Index(message, " (OID "); i > 0 {
		return message[:i] + " is not yet implemented"
	}
	if i := strings.Index(message, ": "); i > 0 {
		return message[:i]
	}
	return message
}

// errNotEvaluable marks a tree the engine cannot run yet: a construct
// outside the evaluator's scope or a builtin without a ported body.
var errNotEvaluable = errors.New("not evaluable by the gateway engine yet")

// evaluate runs an analyzed tree through the gateway evaluator.
func evaluate(tree ast.Node) (datum.NullableDatum, error) {
	state, err := exec.Prepare(tree)
	if err != nil {
		return datum.NullableDatum{}, notEvaluableOr(err)
	}
	result, err := state.Eval(nil)
	return result, notEvaluableOr(err)
}

// notEvaluableOr maps feature_not_supported to errNotEvaluable and keeps
// every other error for the comparison with the server.
func notEvaluableOr(err error) error {
	var diag *mterrors.PgDiagnostic
	if errors.As(err, &diag) && diag.Code == mterrors.PgSSFeatureNotSupported {
		return errNotEvaluable
	}
	return err
}

// outputText renders a value through its type's output function, or
// errNotEvaluable when that function is not ported.
func outputText(typ pgoid.Oid, value datum.Datum) (text string, err error) {
	flinfo, err := fmgr.FmgrInfoFor(pgcatalog.TypeByOid(typ).Output)
	if err != nil {
		return "", err
	}
	err = pgerror.Recover(func() {
		text = string(datum.DatumGetBytes(fmgr.FunctionCall1Coll(flinfo, pgoid.InvalidOid, value)))
	})
	if err != nil {
		return "", notEvaluableOr(err)
	}
	return strings.Clone(text), nil
}
