// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2026, Supabase, Inc
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

// Package analyze ports PostgreSQL's expression analysis (postgres
// src/backend/parser, REL_17_6): the step that turns the grammar's raw
// expression nodes into typed expressions whose every operator, function
// and cast is resolved to a catalog OID. It is the boundary between the
// parser (go/common/parser) and the evaluation engine (fmgr, funcs).
//
// # What it does
//
// [ParseState.TransformExpr] is transformExpr (parse_expr.c:120). Given a
// raw expression it returns the analyzed tree, built from the same node
// types PostgreSQL uses for the purpose: Const, Param, OpExpr, FuncExpr,
// DistinctExpr, NullIfExpr, ScalarArrayOpExpr, BoolExpr, RelabelType,
// CoerceViaIO, ArrayCoerceExpr, ArrayExpr, CaseExpr, CoalesceExpr,
// MinMaxExpr, NullTest and BooleanTest. A Const carries its value as the
// engine's GC-safe datum.Datum; an unknown-type literal is a cstring
// payload until a coercion runs the target type's input function on it,
// exactly as upstream.
//
// Overload resolution is the upstream algorithm in full: the exact-match
// shortcut, the type-coercion-request interpretation of typename(x), the
// candidate shortlist by implicit coercibility (parse_func.c
// func_match_argtypes), the exact/preferred/unknown-category heuristics
// (func_select_candidate), polymorphic signature resolution
// (parse_coerce.c enforce_generic_type_consistency), the pg_cast-driven
// coercion pathways, and the common-type selection used by CASE, COALESCE,
// GREATEST/LEAST, ARRAY and IN. Error messages, hints and SQLSTATEs are
// the upstream ones, so a statement the engine cannot accept fails the way
// PostgreSQL would fail it.
//
// # What it does not do yet
//
// The engine evaluates scalar expressions against the builtin catalog, and
// the analysis is scoped the same way. Constructs that need more than that
// raise feature_not_supported (SQLSTATE 0A000) rather than a wrong answer,
// so a caller can route the statement to PostgreSQL instead:
//
//   - column references, subqueries, row constructors and anything else
//     that needs a relation context;
//   - aggregate, window and set-returning calls;
//   - COLLATE and collation assignment (parse_collate.c);
//   - type modifiers such as numeric(10,2) (typmodin functions);
//   - named arguments and parameter defaults (proargnames and
//     proargdefaults are not in the compiled-in catalog);
//   - objects outside pg_catalog, including user-defined types, functions
//     and operators that a search_path could make visible.
//
// Errors carry no position: the Go grammar does not record source offsets
// for expression nodes.
package analyze

import (
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// ParseState is the per-statement state of analysis - parse_node.h:180.
// Upstream's struct carries the range table, namespace and many flags;
// only what expression analysis needs exists here, which today is the
// parameter-type state of parse_param.c. A ParseState is not safe for
// concurrent use.
type ParseState struct {
	paramTypes []Oid
	paramMode  paramMode
}

// NewParseState returns a state with no parameter types declared: a $n
// reference is then "there is no parameter $n", as in upstream without a
// paramref hook. See [ParseState.SetFixedParameters] and
// [ParseState.SetVariableParameters].
func NewParseState() *ParseState {
	return &ParseState{}
}

// TransformExpr analyzes a raw expression into a typed one - parse_expr.c:120.
// A PostgreSQL-style error (including feature_not_supported for a
// construct the gateway cannot analyze) is returned as a *mterrors.PgDiagnostic.
// As upstream, the raw tree is not preserved: some nodes are transformed in
// place and others are referenced from the result.
func (p *ParseState) TransformExpr(expr ast.Node) (result ast.Node, err error) {
	err = recoverErr(func() { result = p.transformExprRecurse(expr) })
	return result, err
}

// ExprType returns the result type of an analyzed expression - nodeFuncs.c:42.
func ExprType(expr ast.Node) Oid {
	return exprType(expr)
}

// ExprTypmod returns the result typmod of an analyzed expression, or -1 -
// nodeFuncs.c:298.
func ExprTypmod(expr ast.Node) int32 {
	return exprTypmod(expr)
}

// Walk visits node and, while fn keeps returning true, every sub-expression
// of an analyzed tree, in depth-first order - the analogue of
// expression_tree_walker (nodeFuncs.c:1991) over the node kinds analysis
// produces. (ast.WalkNodes knows the grammar's nodes, not these.)
func Walk(node ast.Node, fn func(ast.Node) bool) {
	walkExpr(node, fn)
}

// FormatTypeBe renders a type OID the way PostgreSQL's error messages do,
// e.g. "integer" for int4 - format_type.c:343.
func FormatTypeBe(typeOid Oid) string {
	return formatTypeBe(typeOid)
}

// recoverErr runs fn and converts an ereport into the returned error.
func recoverErr(fn func()) error {
	return pgerror.Recover(fn)
}

// transformExprRecurse dispatches on the raw node kind - parse_expr.c:138.
func (p *ParseState) transformExprRecurse(expr ast.Node) ast.Node {
	switch n := expr.(type) {
	case nil:
		return nil
	case *ast.ParenExpr:
		// The Go grammar keeps parentheses as a node for deparsing; the
		// upstream grammar drops them, so analysis sees through them.
		return p.transformExprRecurse(n.Expr)
	case *ast.ParamRef:
		return p.transformParamRef(n)
	case *ast.A_Const:
		return p.makeAConst(n)
	case *ast.ArrayExpr:
		// The Go grammar produces ArrayExpr directly for ARRAY[...]; it is
		// upstream's A_ArrayExpr until analyzed.
		return p.transformArrayExpr(n, pgoid.InvalidOid, pgoid.InvalidOid, -1)
	case *ast.A_ArrayExpr:
		return p.transformArrayExpr(&ast.ArrayExpr{Elements: n.Elements}, pgoid.InvalidOid, pgoid.InvalidOid, -1)
	case *ast.TypeCast:
		return p.transformTypeCast(n)
	case *ast.A_Expr:
		switch n.Kind {
		case ast.AEXPR_OP:
			return p.transformAExprOp(n)
		case ast.AEXPR_OP_ANY:
			return p.transformAExprOpAny(n)
		case ast.AEXPR_OP_ALL:
			return p.transformAExprOpAll(n)
		case ast.AEXPR_DISTINCT, ast.AEXPR_NOT_DISTINCT:
			return p.transformAExprDistinct(n)
		case ast.AEXPR_NULLIF:
			return p.transformAExprNullIf(n)
		case ast.AEXPR_IN:
			return p.transformAExprIn(n)
		case ast.AEXPR_LIKE, ast.AEXPR_ILIKE, ast.AEXPR_SIMILAR:
			// we can transform these just like AEXPR_OP
			return p.transformAExprOp(n)
		case ast.AEXPR_BETWEEN, ast.AEXPR_NOT_BETWEEN, ast.AEXPR_BETWEEN_SYM, ast.AEXPR_NOT_BETWEEN_SYM:
			return p.transformAExprBetween(n)
		}
		pgerror.Elogf("unrecognized A_Expr kind: %d", int(n.Kind))
	case *ast.BoolExpr:
		return p.transformBoolExpr(n)
	case *ast.FuncCall:
		return p.transformFuncCall(n)
	case *ast.NamedArgExpr:
		n.Arg = expression(p.transformExprRecurse(n.Arg))
		return n
	case *ast.CaseExpr:
		return p.transformCaseExpr(n)
	case *ast.CoalesceExpr:
		return p.transformCoalesceExpr(n)
	case *ast.MinMaxExpr:
		return p.transformMinMaxExpr(n)
	case *ast.NullTest:
		n.Arg = expression(p.transformExprRecurse(n.Arg))
		// the argument can be any type, so don't coerce it
		n.Argisrow = typeIsRowtype(exprType(n.Arg))
		return n
	case *ast.BooleanTest:
		return p.transformBooleanTest(n)
	case *ast.SetToDefault:
		// In all places where DEFAULT is legal, the caller should have
		// processed it rather than passing it to transformExpr().
		ereport(mterrors.PgSSSyntaxError, "DEFAULT is not allowed in this context")
	case *ast.CaseTestExpr:
		// CaseTestExpr doesn't require any processing; it is only injected
		// into parse trees in a fully-formed state.
		return n
	}
	// Everything else needs a relation, query or session context the
	// gateway's scalar engine does not have (ColumnRef, SubLink, RowExpr,
	// CollateClause, SQLValueFunction, the JSON and XML constructors, ...).
	unsupported("%s expressions are not supported by the gateway evaluation engine", nodeTypeName(expr))
	return nil
}

// transformParamRef turns $n into a Param - parse_expr.c:886.
func (p *ParseState) transformParamRef(pref *ast.ParamRef) ast.Node {
	// The core parser knows nothing about Params. If a hook is supplied,
	// call it. If not, or if the hook returns NULL, throw a generic error.
	result := p.paramrefHook(pref)
	if result == nil {
		ereport(mterrors.PgSSUndefinedParameter, "there is no parameter $%d", pref.Number)
	}
	return result
}

// stripParens looks through the Go grammar's parenthesis nodes where
// upstream inspects a raw node's kind directly.
func stripParens(n ast.Node) ast.Node {
	for {
		paren, ok := n.(*ast.ParenExpr)
		if !ok {
			return n
		}
		n = paren.Expr
	}
}

// exprIsNullConstant tests whether a raw expression is a plain NULL
// constant - parse_expr.c:910.
func exprIsNullConstant(arg ast.Node) bool {
	con, ok := stripParens(arg).(*ast.A_Const)
	return ok && con.Isnull
}

// transformAExprOp analyzes an ordinary operator application -
// parse_expr.c:923. The transform_null_equals special case is off (its
// default); row comparisons are rejected when the RowExpr is reached.
func (p *ParseState) transformAExprOp(a *ast.A_Expr) ast.Node {
	lexpr := p.transformExprRecurse(a.Lexpr)
	rexpr := p.transformExprRecurse(a.Rexpr)
	return p.makeOp(a.Name, lexpr, rexpr, a.Location())
}

// transformAExprOpAny analyzes "x op ANY (array)" - parse_expr.c:1004.
func (p *ParseState) transformAExprOpAny(a *ast.A_Expr) ast.Node {
	lexpr := p.transformExprRecurse(a.Lexpr)
	rexpr := p.transformExprRecurse(a.Rexpr)
	return p.makeScalarArrayOp(a.Name, true, lexpr, rexpr, a.Location())
}

// transformAExprOpAll analyzes "x op ALL (array)" - parse_expr.c:1018.
func (p *ParseState) transformAExprOpAll(a *ast.A_Expr) ast.Node {
	lexpr := p.transformExprRecurse(a.Lexpr)
	rexpr := p.transformExprRecurse(a.Rexpr)
	return p.makeScalarArrayOp(a.Name, false, lexpr, rexpr, a.Location())
}

// transformAExprDistinct analyzes IS [NOT] DISTINCT FROM - parse_expr.c:1032.
func (p *ParseState) transformAExprDistinct(a *ast.A_Expr) ast.Node {
	// If either input is an undecorated NULL literal, transform to a
	// NullTest on the other input. That's simpler to process than a full
	// DistinctExpr, and it avoids needing to require that the datatype have
	// an = operator.
	if exprIsNullConstant(a.Rexpr) {
		return p.makeNulltestFromDistinct(a, a.Lexpr)
	}
	if exprIsNullConstant(a.Lexpr) {
		return p.makeNulltestFromDistinct(a, a.Rexpr)
	}

	lexpr := p.transformExprRecurse(a.Lexpr)
	rexpr := p.transformExprRecurse(a.Rexpr)

	// Ordinary scalar operator (ROW() op ROW() is rejected above as a
	// RowExpr).
	var result ast.Node = p.makeDistinctOp(a.Name, lexpr, rexpr, a.Location())

	// If it's NOT DISTINCT, we first build a DistinctExpr and then stick a
	// NOT on top.
	if a.Kind == ast.AEXPR_NOT_DISTINCT {
		result = makeBoolExpr(ast.NOT_EXPR, ast.NewNodeList(result), a.Location())
	}
	return result
}

// transformAExprNullIf analyzes NULLIF(a, b) - parse_expr.c:1083.
func (p *ParseState) transformAExprNullIf(a *ast.A_Expr) ast.Node {
	lexpr := p.transformExprRecurse(a.Lexpr)
	rexpr := p.transformExprRecurse(a.Rexpr)

	op := p.makeOp(a.Name, lexpr, rexpr, a.Location())

	// The comparison operator itself should yield boolean ...
	if op.Opresulttype != pgoid.BOOLOID {
		ereport(mterrors.PgSSDatatypeMismatch, "%s requires = operator to yield boolean", "NULLIF")
	}
	if op.Opretset {
		ereport(mterrors.PgSSDatatypeMismatch, "%s must not return a set", "NULLIF")
	}

	// ... but the NullIfExpr will yield the first operand's type. We rely
	// on NullIfExpr and OpExpr being the same struct.
	result := ast.NewNullIfExpr(op.Opno, op.Opfuncid, exprType(op.Args.Items[0]), op.Args)
	result.Opretset = op.Opretset
	result.SetLocation(op.Location())
	return result
}

// transformAExprIn analyzes "x IN (list)" and "x NOT IN (list)" -
// parse_expr.c:1126.
func (p *ParseState) transformAExprIn(a *ast.A_Expr) ast.Node {
	// If the operator is <>, combine with AND not OR.
	useOr := ast.StrVal(a.Name.Items[0]) != "<>"

	// We try to generate a ScalarArrayOpExpr from IN/NOT IN, but this is
	// only possible if there is a suitable array type available. If not, we
	// fall back to a boolean condition tree with multiple copies of the
	// lefthand expression. Also, any IN-list items that contain Vars are
	// handled as separate boolean conditions, because that gives the
	// planner more scope for optimization on such clauses. (No Var can
	// occur here, so every item is a non-Var.)
	//
	// First step: transform all the inputs.
	lexpr := p.transformExprRecurse(a.Lexpr)
	var rexprs []ast.Node
	for _, item := range a.Rexpr.(*ast.NodeList).Items {
		rexprs = append(rexprs, p.transformExprRecurse(item))
	}
	rnonvars := rexprs

	var result ast.Node
	// ScalarArrayOpExpr is only going to be useful if there's more than one
	// non-Var righthand item.
	if len(rnonvars) > 1 {
		// Try to select a common type for the array elements. Note that
		// since the LHS' type is first in the list, it will be preferred
		// when there is doubt (eg, when all the RHS items are unknown
		// literals).
		allexprs := append([]ast.Node{lexpr}, rnonvars...)
		scalarType, _ := p.selectCommonType(allexprs, "")

		// We have to verify that the selected type actually works.
		if scalarType != pgoid.InvalidOid && !verifyCommonType(scalarType, allexprs) {
			scalarType = pgoid.InvalidOid
		}

		// Do we have an array type to use? Aside from the case where there
		// isn't one, we don't risk using ScalarArrayOpExpr when the common
		// type is RECORD, because the RowExpr comparison logic below can
		// cope with some cases of non-identical row types.
		arrayType := pgoid.InvalidOid
		if scalarType != pgoid.InvalidOid && scalarType != pgoid.RECORDOID {
			arrayType = getArrayType(scalarType)
		}
		if arrayType != pgoid.InvalidOid {
			// OK: coerce all the right-hand non-Var inputs to the common
			// type and build an ArrayExpr for them.
			aexprs := ast.NewNodeList()
			for _, rexpr := range rnonvars {
				aexprs.Append(p.coerceToCommonType(rexpr, scalarType, "IN"))
			}
			newa := ast.NewArrayExpr(arrayType, scalarType, aexprs)
			// array_collid will be set by parse_collate.c
			newa.Multidims = false
			newa.SetLocation(-1)

			result = p.makeScalarArrayOp(a.Name, useOr, lexpr, newa, a.Location())

			// Consider only the Vars (if any) in the loop below
			rexprs = nil
		}
	}

	// Must do it the hard way, ie, with a boolean expression tree.
	for _, rexpr := range rexprs {
		// Ordinary scalar operator
		cmp := p.makeOp(a.Name, copyObject(lexpr), rexpr, a.Location())
		cmpNode := p.coerceToBoolean(cmp, "IN")
		if result == nil {
			result = cmpNode
		} else {
			boolop := ast.AND_EXPR
			if useOr {
				boolop = ast.OR_EXPR
			}
			result = makeBoolExpr(boolop, ast.NewNodeList(result, cmpNode), a.Location())
		}
	}
	return result
}

// transformAExprBetween rewrites BETWEEN and its variants into comparisons
// and analyzes the result - parse_expr.c:1284. Multiply-referenced
// subexpressions are copied, as upstream does (and as upstream notes, that
// means a volatile subexpression is evaluated more than once).
func (p *ParseState) transformAExprBetween(a *ast.A_Expr) ast.Node {
	// Deconstruct A_Expr into three subexprs.
	aexpr := a.Lexpr
	args := a.Rexpr.(*ast.NodeList)
	if args.Len() != 2 {
		pgerror.Elogf("BETWEEN has %d bounds", args.Len())
	}
	bexpr, cexpr := args.Items[0], args.Items[1]
	loc := a.Location()

	// Build the equivalent comparison expression. Ideally we would not use
	// hard-wired operators here but instead use opclasses. However, mixed
	// data types and other issues make this difficult.
	cmp := func(op string, l, r ast.Node) ast.Node {
		return makeSimpleAExpr(ast.AEXPR_OP, op, l, r, loc)
	}
	var result ast.Node
	switch a.Kind {
	case ast.AEXPR_BETWEEN:
		result = makeBoolExpr(ast.AND_EXPR, ast.NewNodeList(
			cmp(">=", aexpr, bexpr),
			cmp("<=", copyObject(aexpr), cexpr)), loc)
	case ast.AEXPR_NOT_BETWEEN:
		result = makeBoolExpr(ast.OR_EXPR, ast.NewNodeList(
			cmp("<", aexpr, bexpr),
			cmp(">", copyObject(aexpr), cexpr)), loc)
	case ast.AEXPR_BETWEEN_SYM:
		sub1 := makeBoolExpr(ast.AND_EXPR, ast.NewNodeList(
			cmp(">=", aexpr, bexpr),
			cmp("<=", copyObject(aexpr), cexpr)), loc)
		sub2 := makeBoolExpr(ast.AND_EXPR, ast.NewNodeList(
			cmp(">=", copyObject(aexpr), copyObject(cexpr)),
			cmp("<=", copyObject(aexpr), copyObject(bexpr))), loc)
		result = makeBoolExpr(ast.OR_EXPR, ast.NewNodeList(sub1, sub2), loc)
	case ast.AEXPR_NOT_BETWEEN_SYM:
		sub1 := makeBoolExpr(ast.OR_EXPR, ast.NewNodeList(
			cmp("<", aexpr, bexpr),
			cmp(">", copyObject(aexpr), cexpr)), loc)
		sub2 := makeBoolExpr(ast.OR_EXPR, ast.NewNodeList(
			cmp("<", copyObject(aexpr), copyObject(cexpr)),
			cmp(">", copyObject(aexpr), copyObject(bexpr))), loc)
		result = makeBoolExpr(ast.AND_EXPR, ast.NewNodeList(sub1, sub2), loc)
	default:
		pgerror.Elogf("unrecognized A_Expr kind: %d", int(a.Kind))
	}
	return p.transformExprRecurse(result)
}

// transformBoolExpr analyzes AND, OR and NOT - parse_expr.c:1403.
func (p *ParseState) transformBoolExpr(a *ast.BoolExpr) ast.Node {
	var opname string
	switch a.Boolop {
	case ast.AND_EXPR:
		opname = "AND"
	case ast.OR_EXPR:
		opname = "OR"
	case ast.NOT_EXPR:
		opname = "NOT"
	default:
		pgerror.Elogf("unrecognized boolop: %d", int(a.Boolop))
	}
	args := ast.NewNodeList()
	for _, arg := range a.Args.Items {
		arg = p.transformExprRecurse(arg)
		arg = p.coerceToBoolean(arg, opname)
		args.Append(arg)
	}
	return makeBoolExpr(a.Boolop, args, a.Location())
}

// transformFuncCall analyzes a function call - parse_expr.c:1439.
func (p *ParseState) transformFuncCall(fn *ast.FuncCall) ast.Node {
	// Transform the list of arguments ...
	var targs []ast.Node
	if fn.Args != nil {
		for _, arg := range fn.Args.Items {
			targs = append(targs, p.transformExprRecurse(arg))
		}
	}

	// When WITHIN GROUP is used, we treat its ORDER BY expressions as
	// additional arguments to the function, for purposes of function lookup
	// and argument type coercion. So, transform each such expression and
	// add them to the targs list. ParseFuncOrColumn can tell what's what by
	// reference to the length of agg_order.
	if fn.AggWithinGroup {
		for _, item := range fn.AggOrder.Items {
			targs = append(targs, p.transformExprRecurse(item.(*ast.SortBy).Node))
		}
	}

	// ... and hand off to parseFuncOrColumn
	return p.parseFuncOrColumn(fn.Funcname, targs, fn, fn.Location())
}

// transformCaseExpr analyzes a CASE expression - parse_expr.c:1632.
func (p *ParseState) transformCaseExpr(c *ast.CaseExpr) ast.Node {
	// transform the test expression, if any
	arg := p.transformExprRecurse(c.Arg)

	// generate placeholder for test expression
	var placeholder *ast.CaseTestExpr
	if arg != nil {
		// If test expression is an untyped literal, force it to text. We
		// have to do something now because we won't be able to do this
		// coercion on the placeholder.
		if exprType(arg) == pgoid.UNKNOWNOID {
			arg = p.coerceToCommonType(arg, pgoid.TEXTOID, "CASE")
		}
		// Upstream runs collation assignment on the test expression here
		// so that the placeholder carries its collation; exprCollation
		// reports what the catalog assigned at construction.
		placeholder = ast.NewCaseTestExpr(exprType(arg), int(exprTypmod(arg)), exprCollation(arg), -1)
	}

	// transform the list of arguments
	newargs := ast.NewNodeList()
	var resultexprs []ast.Node
	for _, item := range c.Args.Items {
		w := item.(*ast.CaseWhen)
		warg := w.Expr
		if placeholder != nil {
			// shorthand form was specified, so expand...
			warg = makeSimpleAExpr(ast.AEXPR_OP, "=", placeholder, warg, w.Location())
		}
		expr := p.transformExprRecurse(warg)
		expr = p.coerceToBoolean(expr, "CASE/WHEN")
		result := p.transformExprRecurse(w.Result)
		neww := ast.NewCaseWhen(expr, result)
		neww.SetLocation(w.Location())
		newargs.Append(neww)
		resultexprs = append(resultexprs, result)
	}

	// transform the default clause
	defresult := c.Defresult
	if defresult == nil {
		defresult = ast.NewA_ConstNull(-1)
	}
	newdefresult := p.transformExprRecurse(defresult)

	// Note: default result is considered the most significant type in
	// determining preferred type. This is how the code worked before, but
	// it seems a little bogus to me --- tgl
	resultexprs = append([]ast.Node{newdefresult}, resultexprs...)

	ptype, _ := p.selectCommonType(resultexprs, "CASE")
	if ptype == pgoid.InvalidOid {
		pgerror.Elogf("could not select CASE result type")
	}
	// casecollid will be set by parse_collate.c

	// Convert default result clause, if necessary.
	newdefresult = p.coerceToCommonType(newdefresult, ptype, "CASE/ELSE")

	// Convert when-clause results, if necessary.
	for _, item := range newargs.Items {
		w := item.(*ast.CaseWhen)
		w.Result = p.coerceToCommonType(w.Result, ptype, "CASE/WHEN")
	}

	// (A set-returning function in a subexpression is rejected where it is
	// resolved, so the upstream SRF check here has nothing to catch.)
	newc := ast.NewCaseExpr(ptype, arg, newargs, newdefresult)
	newc.SetLocation(c.Location())
	return newc
}

// transformArrayExpr analyzes ARRAY[...] - parse_expr.c:2015. When the
// caller knows the target array type (an enclosing cast), arrayType and
// elementType say so and typmod is the target typmod; otherwise both are 0
// and the element type is selected from the elements.
func (p *ParseState) transformArrayExpr(a *ast.ArrayExpr, arrayType, elementType Oid, typmod int32) ast.Node {
	multidims := false
	var newelems []ast.Node
	if a.Elements != nil {
		for _, e := range a.Elements.Items {
			var newe ast.Node
			if sub, ok := stripParens(e).(*ast.ArrayExpr); ok {
				// If an element is itself an A_ArrayExpr, recurse directly
				// so that we can pass down any target type we were given.
				newe = p.transformArrayExpr(sub, arrayType, elementType, typmod)
				// we certainly have an array here
				if arrayType != pgoid.InvalidOid && arrayType != exprType(newe) {
					pgerror.Elogf("nested ARRAY element did not resolve to %s", formatTypeBe(arrayType))
				}
				multidims = true
			} else {
				newe = p.transformExprRecurse(e)
				// Check for sub-array expressions, if we haven't already
				// found one.
				if !multidims {
					if newetype := exprType(newe); newetype != pgoid.INT2VECTOROID &&
						newetype != pgoid.OIDVECTOROID && typeIsArray(newetype) {
						multidims = true
					}
				}
			}
			newelems = append(newelems, newe)
		}
	}

	// Select a target type for the elements.
	//
	// If we haven't been given a target array type, we must try to deduce
	// a common type based on the types of the individual elements present.
	var coerceType Oid
	var coerceHard bool
	if arrayType != pgoid.InvalidOid {
		// Caller must ensure arrayType matches elementType.
		if elementType == pgoid.InvalidOid {
			pgerror.Elogf("array type %s given without its element type", formatTypeBe(arrayType))
		}
		coerceType = elementType
		if multidims {
			coerceType = arrayType
		}
		coerceHard = true
	} else {
		// Can't handle an empty array without a target type.
		if len(newelems) == 0 {
			ereportHint(mterrors.PgSSIndeterminateDatatype,
				"Explicitly cast to the desired type, for example ARRAY[]::integer[].",
				"cannot determine type of empty array")
		}
		// Select a common type for the elements.
		coerceType, _ = p.selectCommonType(newelems, "ARRAY")
		if multidims {
			arrayType = coerceType
			elementType = getElementType(arrayType)
			if elementType == pgoid.InvalidOid {
				ereport(mterrors.PgSSUndefinedObject, "could not find element type for data type %s",
					formatTypeBe(arrayType))
			}
		} else {
			elementType = coerceType
			arrayType = getArrayType(elementType)
			if arrayType == pgoid.InvalidOid {
				ereport(mterrors.PgSSUndefinedObject, "could not find array type for data type %s",
					formatTypeBe(elementType))
			}
		}
		coerceHard = false
	}

	// Coerce elements to target type.
	//
	// If the array has been explicitly cast, then the elements are in turn
	// explicitly coerced.
	//
	// If the array's type was merely derived from the common type of its
	// elements, then the elements are implicitly coerced to the common
	// type. This is consistent with other uses of selectCommonType().
	newcoercedelems := ast.NewNodeList()
	for _, e := range newelems {
		var newe ast.Node
		if coerceHard {
			newe = p.coerceToTargetType(e, exprType(e), coerceType, typmod,
				ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, -1)
			if newe == nil {
				ereport(mterrors.PgSSCannotCoerce, "cannot cast type %s to %s",
					formatTypeBe(exprType(e)), formatTypeBe(coerceType))
			}
		} else {
			newe = p.coerceToCommonType(e, coerceType, "ARRAY")
		}
		newcoercedelems.Append(newe)
	}

	newa := ast.NewArrayExpr(arrayType, elementType, newcoercedelems)
	// array_collid will be set by parse_collate.c
	newa.Multidims = multidims
	newa.SetLocation(a.Location())
	return newa
}

// transformCoalesceExpr analyzes COALESCE - parse_expr.c:2214.
func (p *ParseState) transformCoalesceExpr(c *ast.CoalesceExpr) ast.Node {
	var newargs []ast.Node
	for _, e := range c.Args.Items {
		newargs = append(newargs, p.transformExprRecurse(e))
	}

	coalescetype, _ := p.selectCommonType(newargs, "COALESCE")
	// coalescecollid will be set by parse_collate.c

	// Convert arguments if necessary.
	newcoercedargs := ast.NewNodeList()
	for _, e := range newargs {
		newcoercedargs.Append(p.coerceToCommonType(e, coalescetype, "COALESCE"))
	}

	newc := ast.NewCoalesceExpr(coalescetype, newcoercedargs)
	newc.SetLocation(c.Location())
	return newc
}

// transformMinMaxExpr analyzes GREATEST and LEAST - parse_expr.c:2263.
func (p *ParseState) transformMinMaxExpr(m *ast.MinMaxExpr) ast.Node {
	funcname := "LEAST"
	if m.Op == ast.IS_GREATEST {
		funcname = "GREATEST"
	}
	var newargs []ast.Node
	for _, e := range m.Args.Items {
		newargs = append(newargs, p.transformExprRecurse(e))
	}

	minmaxtype, _ := p.selectCommonType(newargs, funcname)
	// minmaxcollid and inputcollid will be set by parse_collate.c

	// Convert arguments if necessary.
	newcoercedargs := ast.NewNodeList()
	for _, e := range newargs {
		newcoercedargs.Append(p.coerceToCommonType(e, minmaxtype, funcname))
	}

	return ast.NewMinMaxExpr(minmaxtype, pgoid.InvalidOid, pgoid.InvalidOid, m.Op, newcoercedargs, m.Location())
}

// transformBooleanTest analyzes IS [NOT] TRUE/FALSE/UNKNOWN - parse_expr.c:2528.
func (p *ParseState) transformBooleanTest(b *ast.BooleanTest) ast.Node {
	var clausename string
	switch b.Booltesttype {
	case ast.IS_TRUE:
		clausename = "IS TRUE"
	case ast.IS_NOT_TRUE:
		clausename = "IS NOT TRUE"
	case ast.IS_FALSE:
		clausename = "IS FALSE"
	case ast.IS_NOT_FALSE:
		clausename = "IS NOT FALSE"
	case ast.IS_UNKNOWN:
		clausename = "IS UNKNOWN"
	case ast.IS_NOT_UNKNOWN:
		clausename = "IS NOT UNKNOWN"
	default:
		pgerror.Elogf("unrecognized booltesttype: %d", int(b.Booltesttype))
	}
	arg := p.transformExprRecurse(b.Arg)
	b.Arg = expression(p.coerceToBoolean(arg, clausename))
	return b
}

// transformTypeCast analyzes CAST(x AS type) and x::type - parse_expr.c:2692.
func (p *ParseState) transformTypeCast(tc *ast.TypeCast) ast.Node {
	// Look up the type name first.
	targetType, targetTypmod := typenameTypeIdAndMod(tc.TypeName)

	// If the subject of the typecast is an ARRAY[] construct and the target
	// type is an array type, we invoke transformArrayExpr() directly so
	// that we can pass down the type information. This avoids some cases
	// where transformArrayExpr() might not infer the correct type.
	// Otherwise, just transform the argument normally.
	var expr ast.Node
	if arr, ok := stripParens(tc.Arg).(*ast.ArrayExpr); ok {
		// If target is a domain over array, work with the base array type
		// here. Below, we'll cast the array type to the domain. In the
		// usual case that the target is not a domain, the remaining steps
		// will be a no-op.
		targetBaseTypmod := targetTypmod
		targetBaseType := getBaseTypeAndTypmod(targetType, &targetBaseTypmod)
		if elementType := getElementType(targetBaseType); elementType != pgoid.InvalidOid {
			expr = p.transformArrayExpr(arr, targetBaseType, elementType, targetBaseTypmod)
		} else {
			expr = p.transformExprRecurse(tc.Arg)
		}
	} else {
		expr = p.transformExprRecurse(tc.Arg)
	}

	inputType := exprType(expr)
	if inputType == pgoid.InvalidOid {
		return expr // do nothing if NULL input
	}

	// Location of the coercion is preferentially the location of the :: or
	// CAST symbol, but if there is none then use the location of the type
	// name (this can happen in TypeName 'string' syntax, for instance).
	location := tc.Location()
	if location < 0 {
		location = tc.TypeName.Location()
	}

	result := p.coerceToTargetType(expr, inputType, targetType, targetTypmod,
		ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, location)
	if result == nil {
		ereport(mterrors.PgSSCannotCoerce, "cannot cast type %s to %s",
			formatTypeBe(inputType), formatTypeBe(targetType))
	}
	return result
}

// makeDistinctOp builds the DistinctExpr for IS DISTINCT FROM -
// parse_expr.c:3062.
func (p *ParseState) makeDistinctOp(opname *ast.NodeList, ltree, rtree ast.Node, location int) *ast.DistinctExpr {
	op := p.makeOp(opname, ltree, rtree, location)
	if op.Opresulttype != pgoid.BOOLOID {
		ereport(mterrors.PgSSDatatypeMismatch, "%s requires = operator to yield boolean", "IS DISTINCT FROM")
	}
	if op.Opretset {
		ereport(mterrors.PgSSDatatypeMismatch, "%s must not return a set", "IS DISTINCT FROM")
	}
	// We rely on DistinctExpr and OpExpr being same struct.
	result := ast.NewDistinctExpr(op.Opno, op.Opfuncid, op.Opresulttype, op.Args)
	result.Opretset = op.Opretset
	result.SetLocation(op.Location())
	return result
}

// makeNulltestFromDistinct produces a NullTest from an IS [NOT] DISTINCT
// FROM NULL construct - parse_expr.c:3097. arg is the untransformed other
// argument.
func (p *ParseState) makeNulltestFromDistinct(distincta *ast.A_Expr, arg ast.Node) ast.Node {
	nulltesttype := ast.IS_NOT_NULL
	if distincta.Kind == ast.AEXPR_NOT_DISTINCT {
		nulltesttype = ast.IS_NULL
	}
	// the argument can be any type, so don't coerce it
	nt := ast.NewNullTest(expression(p.transformExprRecurse(arg)), nulltesttype)
	// argisrow = false is correct whether or not arg is composite
	nt.Argisrow = false
	nt.SetLocation(distincta.Location())
	return nt
}
