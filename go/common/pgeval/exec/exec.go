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

// Package exec ports PostgreSQL's expression executor (postgres
// src/backend/executor/execExpr.c and execExprInterp.c, REL_17_6) for the
// typed trees that package analyze produces.
//
// [Prepare] is ExecInitExpr: it walks the analyzed tree once, resolves
// every function to its fmgr entry point, preloads constant arguments and
// allocates the call frames, so that [ExprState.Eval] does no catalog work.
// Upstream compiles the tree into a linear program of ExprEvalSteps joined
// by jumps; the Go form keeps a tree of steps and evaluates it
// recursively. The results and the evaluation order are the same: left to
// right, with AND, OR, CASE and COALESCE stopping at the first argument
// that decides them.
//
// The scope is the one analyze accepts: Const, Param, FuncExpr, OpExpr,
// DistinctExpr, NullIfExpr, BoolExpr, NullTest, BooleanTest, CaseExpr,
// CoalesceExpr, RelabelType and CoerceViaIO. Arrays (ArrayExpr,
// ArrayCoerceExpr, ScalarArrayOpExpr), GREATEST/LEAST (MinMaxExpr) and
// domains (CoerceToDomain) analyze but cannot run yet: there is no array
// value representation, and the comparison and constraint lookups they
// need are not in the compiled-in catalog. Prepare reports those, and any
// builtin whose body is not ported, with SQLSTATE 0A000, so that a caller
// can decide before evaluating anything whether to run the expression here
// or hand the statement to PostgreSQL, and that decision does not depend
// on parameter values.
//
// Errors raised while evaluating are PostgreSQL's own: the function bodies
// ereport the SQLSTATE and message upstream uses, and Eval returns them as
// a *mterrors.PgDiagnostic. A Go panic that is not an ereport is not
// recovered.
package exec

import (
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/analyze"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// funcMaxArgs is FUNC_MAX_ARGS - pg_config_manual.h:43.
const funcMaxArgs = 100

// ExternParam is the value of one $n parameter - ParamExternData
// (params.h:92). Type is the parameter's type as analysis declared or
// deduced it; as upstream, an entry without a valid type counts as no
// value supplied.
type ExternParam struct {
	Value  datum.Datum
	IsNull bool
	Type   pgoid.Oid
}

// ExprState is a prepared expression - ExprState (execnodes.h:60). It owns
// the call frames its function steps reuse between evaluations, so one
// state must not be evaluated concurrently.
type ExprState struct {
	root   step
	params []ExternParam
}

// Prepare readies an analyzed expression for evaluation - ExecInitExpr
// (execExpr.c:135). It fails with feature_not_supported for a construct the
// engine cannot run and with PostgreSQL's error for a malformed tree.
func Prepare(expr ast.Node) (*ExprState, error) {
	state := &ExprState{}
	if err := pgerror.Recover(func() { state.root = state.initExpr(expr, nil) }); err != nil {
		return nil, err
	}
	return state, nil
}

// Eval evaluates the expression; params[i] is the value of $(i+1). A
// PostgreSQL error is returned as a *mterrors.PgDiagnostic.
func (s *ExprState) Eval(params []ExternParam) (result datum.NullableDatum, err error) {
	s.params = params
	err = pgerror.Recover(func() { result = s.root.eval(s) })
	s.params = nil
	return result, err
}

// step evaluates one node: an ExprEvalStep (execExpr.h:650) whose operands
// are steps instead of jump targets.
type step interface {
	eval(s *ExprState) datum.NullableDatum
}

func boolDatum(b bool) datum.NullableDatum {
	return datum.NullableDatum{Value: datum.BoolGetDatum(b)}
}

func unsupported(what string) {
	pgerror.Ereportf(mterrors.PgSSFeatureNotSupported, "%s are not supported by the gateway evaluation engine", what)
}

// initExpr compiles one node - ExecInitExprRec (execExpr.c:894). caseval is
// where the innermost enclosing CASE stores its test value, for
// CaseTestExpr to read - ExprState.innermost_caseval.
func (s *ExprState) initExpr(node ast.Node, caseval *datum.NullableDatum) step {
	switch n := node.(type) {
	case *ast.Const:
		return constStep{datum.NullableDatum{Value: n.Constvalue, IsNull: n.Constisnull}}
	case *ast.Param:
		if n.Paramkind != ast.PARAM_EXTERN {
			// PARAM_EXEC values are produced by a plan's subplans; there
			// is no plan here.
			pgerror.Elogf("unrecognized paramkind: %d", n.Paramkind)
		}
		return &paramStep{id: n.Paramid, typ: n.Paramtype}
	case *ast.FuncExpr:
		return s.initFunc(n.Args, n.Funcid, n.Inputcollid, caseval)
	case *ast.OpExpr:
		return s.initFunc(n.Args, n.Opfuncid, n.Inputcollid, caseval)
	case *ast.DistinctExpr:
		return &distinctStep{s.initFunc(n.Args, n.Opfuncid, n.Inputcollid, caseval)}
	case *ast.NullIfExpr:
		return &nullIfStep{s.initFunc(n.Args, n.Opfuncid, n.Inputcollid, caseval)}
	case *ast.BoolExpr:
		switch n.Boolop {
		case ast.AND_EXPR, ast.OR_EXPR, ast.NOT_EXPR:
		default:
			pgerror.Elogf("unrecognized boolop: %d", n.Boolop)
		}
		return &boolStep{op: n.Boolop, args: s.initList(n.Args, caseval)}
	case *ast.RelabelType:
		// relabel doesn't need to do anything at runtime
		return s.initExpr(n.Arg, caseval)
	case *ast.CoerceViaIO:
		return s.initCoerceViaIO(n, caseval)
	case *ast.CaseExpr:
		return s.initCase(n, caseval)
	case *ast.CaseTestExpr:
		if caseval == nil {
			// Upstream then reads a value the caller stored in the
			// ExprContext (caseValue_datum); nothing here does that.
			pgerror.Elogf("CaseTestExpr outside a CASE expression")
		}
		return caseTestStep{caseval}
	case *ast.CoalesceExpr:
		return &coalesceStep{s.initList(n.Args, caseval)}
	case *ast.NullTest:
		if n.Argisrow {
			unsupported("row-valued IS NULL tests")
		}
		switch n.Nulltesttype {
		case ast.IS_NULL, ast.IS_NOT_NULL:
		default:
			pgerror.Elogf("unrecognized nulltesttype: %d", n.Nulltesttype)
		}
		return &nullTestStep{arg: s.initExpr(n.Arg, caseval), wantNull: n.Nulltesttype == ast.IS_NULL}
	case *ast.BooleanTest:
		switch n.Booltesttype {
		case ast.IS_TRUE, ast.IS_NOT_TRUE, ast.IS_FALSE, ast.IS_NOT_FALSE, ast.IS_UNKNOWN, ast.IS_NOT_UNKNOWN:
		default:
			pgerror.Elogf("unrecognized booltesttype: %d", n.Booltesttype)
		}
		return &boolTestStep{arg: s.initExpr(n.Arg, caseval), kind: n.Booltesttype}
	case *ast.ArrayExpr, *ast.ArrayCoerceExpr, *ast.ScalarArrayOpExpr:
		unsupported("array values")
	case *ast.MinMaxExpr:
		unsupported("GREATEST and LEAST")
	case *ast.CoerceToDomain:
		unsupported("domain types")
	}
	pgerror.Elogf("unrecognized node type: %T", node)
	return nil // unreachable: Elogf never returns
}

func (s *ExprState) initList(list *ast.NodeList, caseval *datum.NullableDatum) []step {
	if list == nil {
		return nil
	}
	steps := make([]step, len(list.Items))
	for i, item := range list.Items {
		steps[i] = s.initExpr(item, caseval)
	}
	return steps
}

// lookupFunc resolves a function OID - fmgr_info (fmgr.c:127).
func lookupFunc(funcid pgoid.Oid) *fmgr.FmgrInfo {
	flinfo, err := fmgr.FmgrInfoFor(funcid)
	if err != nil {
		pgerror.Elogf("cache lookup failed for function %d", funcid)
	}
	return flinfo
}

// requireImplemented reports a builtin without a ported body at prepare
// time. fmgr would raise the same error when the call runs, but a dead
// branch (CASE WHEN false THEN f(x) ...) would then evaluate fine, and
// whether a statement can run here must not depend on its values.
func requireImplemented(funcid pgoid.Oid) {
	if proc := pgcatalog.ProcByOid(funcid); proc != nil && !fmgr.IsBuiltinRegistered(proc.Src) {
		pgerror.Ereportf(mterrors.PgSSFeatureNotSupported,
			"function %s (OID %d) is not yet implemented in the gateway evaluation engine", proc.Name, proc.Oid)
	}
}

func typeByOid(oid pgoid.Oid) *pgcatalog.Type {
	t := pgcatalog.TypeByOid(oid)
	if t == nil {
		pgerror.Elogf("cache lookup failed for type %d", oid)
	}
	return t
}

// constStep - EEOP_CONST (execExprInterp.c:711).
type constStep struct{ value datum.NullableDatum }

func (c constStep) eval(*ExprState) datum.NullableDatum { return c.value }

// paramStep reads a $n value - EEOP_PARAM_EXTERN, ExecEvalParamExtern
// (execExprInterp.c:2532).
type paramStep struct {
	id  int
	typ pgoid.Oid
}

func (p *paramStep) eval(s *ExprState) datum.NullableDatum {
	if p.id > 0 && p.id <= len(s.params) {
		prm := s.params[p.id-1]
		if prm.Type != pgoid.InvalidOid {
			// safety check in case hook did something unexpected
			if prm.Type != p.typ {
				pgerror.Ereportf(mterrors.PgSSDatatypeMismatch,
					"type of parameter %d (%s) does not match that when preparing the plan (%s)",
					p.id, analyze.FormatTypeBe(prm.Type), analyze.FormatTypeBe(p.typ))
			}
			return datum.NullableDatum{Value: prm.Value, IsNull: prm.IsNull}
		}
	}
	pgerror.Ereportf(mterrors.PgSSUndefinedObject, "no value found for parameter %d", p.id)
	return datum.NullableDatum{} // unreachable
}

// funcStep calls a function or operator - ExecInitFunc (execExpr.c:2628)
// and EEOP_FUNCEXPR / EEOP_FUNCEXPR_STRICT (execExprInterp.c:734). The call
// frame is allocated once and the arguments are evaluated straight into it.
type funcStep struct {
	fcinfo fmgr.FunctionCallInfo
	args   []step // nil where the argument is a Const preloaded into fcinfo
}

func (s *ExprState) initFunc(args *ast.NodeList, funcid, inputcollid pgoid.Oid, caseval *datum.NullableDatum) *funcStep {
	var items []ast.Node
	if args != nil {
		items = args.Items
	}
	// Safety check on nargs. Under normal circumstances this should never
	// fail, as parser should check sooner.
	if len(items) > funcMaxArgs {
		pgerror.Ereportf(mterrors.PgSSTooManyArguments, "cannot pass more than %d arguments to a function", funcMaxArgs)
	}
	flinfo := lookupFunc(funcid)
	// We only support non-set functions here
	if flinfo.FnRetset {
		pgerror.Ereportf(mterrors.PgSSFeatureNotSupported, "set-valued function called in context that cannot accept a set")
	}
	requireImplemented(funcid)
	f := &funcStep{fcinfo: fmgr.NewFunctionCallInfo(flinfo, len(items), inputcollid), args: make([]step, len(items))}
	for i, arg := range items {
		if con, ok := arg.(*ast.Const); ok {
			// Don't evaluate const arguments every round; especially
			// interesting for constants in comparisons.
			f.fcinfo.Args[i] = datum.NullableDatum{Value: con.Constvalue, IsNull: con.Constisnull}
			continue
		}
		f.args[i] = s.initExpr(arg, caseval)
	}
	return f
}

// evalArgs evaluates the non-constant arguments into the call frame.
func (f *funcStep) evalArgs(s *ExprState) {
	for i, arg := range f.args {
		if arg != nil {
			f.fcinfo.Args[i] = arg.eval(s)
		}
	}
}

// invoke calls the body with the frame as it is, whatever the strictness -
// FunctionCallInvoke (fmgr.h:172).
func (f *funcStep) invoke() datum.NullableDatum {
	f.fcinfo.IsNull = false
	value := f.fcinfo.Flinfo.FnAddr(f.fcinfo)
	return datum.NullableDatum{Value: value, IsNull: f.fcinfo.IsNull}
}

func (f *funcStep) eval(s *ExprState) datum.NullableDatum {
	f.evalArgs(s)
	// CallFunction is the strict check of EEOP_FUNCEXPR_STRICT followed by
	// the body call of EEOP_FUNCEXPR.
	value := fmgr.CallFunction(f.fcinfo)
	return datum.NullableDatum{Value: value, IsNull: f.fcinfo.IsNull}
}

// distinctStep - EEOP_DISTINCT (execExprInterp.c:1223). IS DISTINCT FROM
// decides on NULL arguments itself and otherwise applies the type's
// equality operator, whatever its strictness, inverting the result.
type distinctStep struct{ *funcStep }

func (d *distinctStep) eval(s *ExprState) datum.NullableDatum {
	d.evalArgs(s)
	args := d.fcinfo.Args
	switch {
	case args[0].IsNull && args[1].IsNull:
		// Both NULL? Then is not distinct...
		return boolDatum(false)
	case args[0].IsNull || args[1].IsNull:
		// Only one is NULL? Then is distinct...
		return boolDatum(true)
	}
	eq := d.invoke()
	// Must invert result of "="; safe to do even if null
	return datum.NullableDatum{Value: datum.BoolGetDatum(!datum.DatumGetBool(eq.Value)), IsNull: eq.IsNull}
}

// nullIfStep - EEOP_NULLIF (execExprInterp.c:1292).
type nullIfStep struct{ *funcStep }

func (n *nullIfStep) eval(s *ExprState) datum.NullableDatum {
	n.evalArgs(s)
	args := n.fcinfo.Args
	saveArg0 := args[0]
	// if either argument is NULL they can't be equal
	if !args[0].IsNull && !args[1].IsNull {
		// (Upstream also makes a varlena first argument read-only here;
		// there are no expanded objects in this engine.)
		if eq := n.invoke(); !eq.IsNull && datum.DatumGetBool(eq.Value) {
			// if the arguments are equal return null
			return datum.NullableDatum{IsNull: true}
		}
	}
	// Arguments aren't equal, so return the first one
	return saveArg0
}

// boolStep - EEOP_BOOL_AND_STEP*, EEOP_BOOL_OR_STEP* and EEOP_BOOL_NOT_STEP
// (execExprInterp.c:798).
type boolStep struct {
	op   ast.BoolExprType
	args []step
}

func (b *boolStep) eval(s *ExprState) datum.NullableDatum {
	if b.op == ast.NOT_EXPR {
		// It's safe to do this even on a nominally null value, so we
		// ignore resnull; that means that NULL in produces NULL out.
		v := b.args[0].eval(s)
		return datum.NullableDatum{Value: datum.BoolGetDatum(!datum.DatumGetBool(v.Value)), IsNull: v.IsNull}
	}
	// If any of its clauses is FALSE, an AND's result is FALSE regardless
	// of the states of the rest of the clauses, so we can stop evaluating
	// and return FALSE immediately. If none are FALSE and one or more is
	// NULL, we return NULL; otherwise we return TRUE. OR is the mirror
	// image with TRUE.
	decisive := b.op == ast.OR_EXPR
	anynull := false
	for _, arg := range b.args {
		v := arg.eval(s)
		switch {
		case v.IsNull:
			anynull = true
		case datum.DatumGetBool(v.Value) == decisive:
			return boolDatum(decisive)
		}
	}
	if anynull {
		return datum.NullableDatum{IsNull: true}
	}
	return boolDatum(!decisive)
}

// nullTestStep - EEOP_NULLTEST_ISNULL / EEOP_NULLTEST_ISNOTNULL
// (execExprInterp.c:990).
type nullTestStep struct {
	arg      step
	wantNull bool
}

func (n *nullTestStep) eval(s *ExprState) datum.NullableDatum {
	return boolDatum(n.arg.eval(s).IsNull == n.wantNull)
}

// boolTestStep - EEOP_BOOLTEST_IS_* (execExprInterp.c:1024); IS [NOT]
// UNKNOWN is the scalar null test.
type boolTestStep struct {
	arg  step
	kind ast.BoolTestType
}

func (b *boolTestStep) eval(s *ExprState) datum.NullableDatum {
	v := b.arg.eval(s)
	switch b.kind {
	case ast.IS_TRUE:
		if v.IsNull {
			return boolDatum(false)
		}
		return v // input value is the correct output as well
	case ast.IS_NOT_TRUE:
		if v.IsNull {
			return boolDatum(true)
		}
		return boolDatum(!datum.DatumGetBool(v.Value))
	case ast.IS_FALSE:
		if v.IsNull {
			return boolDatum(false)
		}
		return boolDatum(!datum.DatumGetBool(v.Value))
	case ast.IS_NOT_FALSE:
		if v.IsNull {
			return boolDatum(true)
		}
		return v
	case ast.IS_UNKNOWN:
		return boolDatum(v.IsNull)
	default: // IS_NOT_UNKNOWN, checked at prepare
		return boolDatum(!v.IsNull)
	}
}

// caseStep - the CaseExpr program of ExecInitExprRec (execExpr.c:1730): the
// test expression, if any, is evaluated once into val, each WHEN
// condition runs with val as the innermost CASE value, and the first true
// condition selects the result.
type caseStep struct {
	arg   step // nil without a test expression
	val   datum.NullableDatum
	whens []caseWhen
	def   step
}

type caseWhen struct{ cond, result step }

func (s *ExprState) initCase(n *ast.CaseExpr, caseval *datum.NullableDatum) step {
	c := &caseStep{}
	// Make testexpr result available to CaseTestExpr nodes within the
	// condition only; the results see the enclosing CASE's value, if any.
	var inner *datum.NullableDatum
	if n.Arg != nil {
		c.arg = s.initExpr(n.Arg, caseval)
		inner = &c.val
	}
	for _, item := range n.Args.Items {
		w, ok := item.(*ast.CaseWhen)
		if !ok {
			pgerror.Elogf("unrecognized node type: %T", item)
		}
		c.whens = append(c.whens, caseWhen{cond: s.initExpr(w.Expr, inner), result: s.initExpr(w.Result, caseval)})
	}
	if n.Defresult == nil {
		// transformCaseExpr always adds a default
		pgerror.Elogf("CASE expression without a default result")
	}
	c.def = s.initExpr(n.Defresult, caseval)
	return c
}

func (c *caseStep) eval(s *ExprState) datum.NullableDatum {
	if c.arg != nil {
		c.val = c.arg.eval(s)
	}
	for _, w := range c.whens {
		if cond := w.cond.eval(s); !cond.IsNull && datum.DatumGetBool(cond.Value) {
			return w.result.eval(s)
		}
	}
	return c.def.eval(s)
}

// caseTestStep - EEOP_CASE_TESTVAL (execExprInterp.c:1096).
type caseTestStep struct{ val *datum.NullableDatum }

func (c caseTestStep) eval(*ExprState) datum.NullableDatum { return *c.val }

// coalesceStep - the CoalesceExpr program (execExpr.c:2136): the first
// non-null argument, or NULL.
type coalesceStep struct{ args []step }

func (c *coalesceStep) eval(s *ExprState) datum.NullableDatum {
	for _, arg := range c.args {
		if v := arg.eval(s); !v.IsNull {
			return v
		}
	}
	return datum.NullableDatum{IsNull: true}
}

// ioCoerceStep runs a value through the source type's output function and
// the result type's input function - EEOP_IOCOERCE (execExprInterp.c:1153),
// set up by the CoerceViaIO case of ExecInitExprRec (execExpr.c:1572).
type ioCoerceStep struct {
	arg     step
	out, in fmgr.FunctionCallInfo
}

func (s *ExprState) initCoerceViaIO(n *ast.CoerceViaIO, caseval *datum.NullableDatum) step {
	c := &ioCoerceStep{arg: s.initExpr(n.Arg, caseval)}

	// lookup the source type's output function - getTypeOutputInfo
	// (lsyscache.c:2956)
	src := typeByOid(analyze.ExprType(n.Arg))
	if src.Output == pgoid.InvalidOid {
		pgerror.Ereportf(mterrors.PgSSUndefinedFunction, "no output function available for type %s", analyze.FormatTypeBe(src.Oid))
	}
	requireImplemented(src.Output)
	c.out = fmgr.NewFunctionCallInfo(lookupFunc(src.Output), 1, pgoid.InvalidOid)

	// lookup the result type's input function - getTypeInputInfo
	// (lsyscache.c:2919)
	dst := typeByOid(n.Resulttype)
	if dst.Input == pgoid.InvalidOid {
		pgerror.Ereportf(mterrors.PgSSUndefinedFunction, "no input function available for type %s", analyze.FormatTypeBe(dst.Oid))
	}
	requireImplemented(dst.Input)
	c.in = fmgr.NewFunctionCallInfo(lookupFunc(dst.Input), 3, pgoid.InvalidOid)

	// We can preload the second and third arguments for the input
	// function, since they're constants: the typioparam (the element type
	// for an array type, else the type itself - getTypeIOParam) and -1 for
	// the typmod.
	ioparam := dst.Oid
	if dst.Elem != pgoid.InvalidOid {
		ioparam = dst.Elem
	}
	c.in.Args[1] = datum.NullableDatum{Value: datum.ObjectIdGetDatum(ioparam)}
	c.in.Args[2] = datum.NullableDatum{Value: datum.Int32GetDatum(-1)}
	return c
}

func (c *ioCoerceStep) eval(s *ExprState) datum.NullableDatum {
	v := c.arg.eval(s)
	// call output function (similar to OutputFunctionCall)
	var str datum.Datum
	if !v.IsNull {
		// output functions are not called on nulls
		c.out.Args[0] = datum.NullableDatum{Value: v.Value}
		c.out.IsNull = false
		str = c.out.Flinfo.FnAddr(c.out)
	}
	// call input function (similar to InputFunctionCall): a strict one is
	// skipped on NULL, a non-strict one sees the NULL cstring.
	if !c.in.Flinfo.FnStrict || !v.IsNull {
		c.in.Args[0] = datum.NullableDatum{Value: str, IsNull: v.IsNull}
		c.in.IsNull = false
		v.Value = c.in.Flinfo.FnAddr(c.in)
	}
	return v
}
