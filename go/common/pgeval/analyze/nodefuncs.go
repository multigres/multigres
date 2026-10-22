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

package analyze

import (
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// This file ports the expression-inspection helpers of nodes/nodeFuncs.c
// for the node kinds analysis produces. A node kind that is not listed is an
// internal error, as upstream's "unrecognized node type" is.

// exprType returns the type OID of an analyzed expression - nodeFuncs.c:42.
func exprType(expr ast.Node) Oid {
	switch n := expr.(type) {
	case nil:
		return pgoid.InvalidOid
	case *ast.Const:
		return n.Consttype
	case *ast.Param:
		return n.Paramtype
	case *ast.FuncExpr:
		return n.Funcresulttype
	case *ast.NamedArgExpr:
		return exprType(n.Arg)
	case *ast.OpExpr:
		return n.Opresulttype
	case *ast.DistinctExpr:
		return n.Opresulttype
	case *ast.NullIfExpr:
		return n.Opresulttype
	case *ast.ScalarArrayOpExpr, *ast.BoolExpr, *ast.NullTest, *ast.BooleanTest:
		return pgoid.BOOLOID
	case *ast.RelabelType:
		return n.Resulttype
	case *ast.CoerceViaIO:
		return n.Resulttype
	case *ast.ArrayCoerceExpr:
		return n.Resulttype
	case *ast.CollateExpr:
		return exprType(n.Arg)
	case *ast.CaseExpr:
		return n.Casetype
	case *ast.CaseTestExpr:
		return n.TypeId
	case *ast.ArrayExpr:
		return n.ArrayTypeid
	case *ast.CoalesceExpr:
		return n.Coalescetype
	case *ast.MinMaxExpr:
		return n.MinMaxType
	case *ast.CoerceToDomain:
		return n.Resulttype
	}
	pgerror.Elogf("unrecognized node type: %s", nodeTypeName(expr))
	return pgoid.InvalidOid
}

// exprTypmod returns the typmod of an analyzed expression, or -1 -
// nodeFuncs.c:298.
func exprTypmod(expr ast.Node) int32 {
	switch n := expr.(type) {
	case nil:
		return -1
	case *ast.Const:
		return n.Consttypmod
	case *ast.Param:
		return n.Paramtypmod
	case *ast.FuncExpr:
		// Be smart about length-coercion functions...
		if typmod, ok := exprIsLengthCoercion(expr); ok {
			return typmod
		}
	case *ast.NamedArgExpr:
		return exprTypmod(n.Arg)
	case *ast.NullIfExpr:
		// Result is either first argument or NULL, so we can report first
		// argument's typmod if known.
		return exprTypmod(n.Args.Items[0])
	case *ast.RelabelType:
		return n.Resulttypmod
	case *ast.ArrayCoerceExpr:
		return n.Resulttypmod
	case *ast.CollateExpr:
		return exprTypmod(n.Arg)
	case *ast.CaseExpr:
		// If all the alternatives agree on type/typmod, return that typmod,
		// else use -1.
		if n.Defresult == nil || exprType(n.Defresult) != n.Casetype {
			return -1
		}
		typmod := exprTypmod(n.Defresult)
		if typmod < 0 {
			return -1 // no point in trying harder
		}
		for _, arg := range n.Args.Items {
			w := arg.(*ast.CaseWhen)
			if exprType(w.Result) != n.Casetype || exprTypmod(w.Result) != typmod {
				return -1
			}
		}
		return typmod
	case *ast.CaseTestExpr:
		return int32(n.TypeMod)
	case *ast.ArrayExpr:
		// If all the elements agree on type/typmod, return that typmod,
		// else use -1.
		if n.Elements == nil || len(n.Elements.Items) == 0 {
			return -1
		}
		typmod := exprTypmod(n.Elements.Items[0])
		if typmod < 0 {
			return -1
		}
		commontype := n.ElementTypeid
		if n.Multidims {
			commontype = n.ArrayTypeid
		}
		for _, e := range n.Elements.Items {
			if exprType(e) != commontype || exprTypmod(e) != typmod {
				return -1
			}
		}
		return typmod
	case *ast.CoalesceExpr:
		// If all the alternatives agree on type/typmod, return that typmod,
		// else use -1.
		first := n.Args.Items[0]
		if exprType(first) != n.Coalescetype {
			return -1
		}
		typmod := exprTypmod(first)
		if typmod < 0 {
			return -1
		}
		for _, e := range n.Args.Items[1:] {
			if exprType(e) != n.Coalescetype || exprTypmod(e) != typmod {
				return -1
			}
		}
		return typmod
	case *ast.MinMaxExpr:
		// If all the alternatives agree on type/typmod, return that typmod,
		// else use -1.
		first := n.Args.Items[0]
		if exprType(first) != n.MinMaxType {
			return -1
		}
		typmod := exprTypmod(first)
		if typmod < 0 {
			return -1
		}
		for _, e := range n.Args.Items[1:] {
			if exprType(e) != n.MinMaxType || exprTypmod(e) != typmod {
				return -1
			}
		}
		return typmod
	case *ast.CoerceToDomain:
		return n.Resulttypmod
	}
	return -1
}

// exprIsLengthCoercion reports whether expr is a length-coercion call and,
// if so, the typmod it coerces to - nodeFuncs.c:531.
func exprIsLengthCoercion(expr ast.Node) (int32, bool) {
	switch n := expr.(type) {
	case *ast.FuncExpr:
		// If it didn't come from a coercion context, reject.
		if n.Funcformat != ast.COERCE_EXPLICIT_CAST && n.Funcformat != ast.COERCE_IMPLICIT_CAST {
			return -1, false
		}
		// If it's not a two-argument or three-argument function with the
		// second argument being an int4 constant, it can't have been created
		// from a length coercion (it must be a type coercion, instead).
		nargs := len(n.Args.Items)
		if nargs < 2 || nargs > 3 {
			return -1, false
		}
		second, ok := n.Args.Items[1].(*ast.Const)
		if !ok || second.Consttype != pgoid.INT4OID || second.Constisnull {
			return -1, false
		}
		return datum.DatumGetInt32(second.Constvalue), true
	case *ast.ArrayCoerceExpr:
		if n.Resulttypmod < 0 {
			return -1, false
		}
		return n.Resulttypmod, true
	}
	return -1, false
}

// exprCollation returns the collation of an analyzed expression -
// nodeFuncs.c:816. parse_collate.c is not ported, so only the nodes that
// carry a collation from the catalog at construction (Const, Param, and the
// nodes that copy one) report a non-zero value; the others report 0 just as
// upstream does before assign_expr_collations runs.
func exprCollation(expr ast.Node) Oid {
	switch n := expr.(type) {
	case *ast.Const:
		return n.Constcollid
	case *ast.Param:
		return n.Paramcollid
	case *ast.FuncExpr:
		return n.Funccollid
	case *ast.NamedArgExpr:
		return exprCollation(n.Arg)
	case *ast.OpExpr:
		return n.Opcollid
	case *ast.DistinctExpr:
		return n.Opcollid
	case *ast.NullIfExpr:
		return n.Opcollid
	case *ast.RelabelType:
		return n.Resultcollid
	case *ast.CoerceViaIO:
		return n.Resultcollid
	case *ast.ArrayCoerceExpr:
		return n.Resultcollid
	case *ast.CollateExpr:
		return n.CollOid
	case *ast.CaseExpr:
		return n.Casecollid
	case *ast.CaseTestExpr:
		return n.Collation
	case *ast.ArrayExpr:
		return n.ArrayCollid
	case *ast.CoalesceExpr:
		return n.Coalescecollid
	case *ast.MinMaxExpr:
		return n.MinMaxCollid
	case *ast.CoerceToDomain:
		return n.Resultcollid
	}
	return pgoid.InvalidOid
}

// exprLocation is the source offset of an expression - nodeFuncs.c:1380.
// The Go grammar records no expression offsets (see errors.go), so this only
// keeps the upstream call sites intact.
func exprLocation(expr ast.Node) int {
	if expr == nil {
		return -1
	}
	return expr.Location()
}

// expressionReturnsSet reports whether the expression contains a
// set-returning function or operator call - nodeFuncs.c:758.
func expressionReturnsSet(clause ast.Node) bool {
	found := false
	walkExpr(clause, func(n ast.Node) bool {
		switch e := n.(type) {
		case *ast.FuncExpr:
			found = found || e.Funcretset
		case *ast.OpExpr:
			found = found || e.Opretset
		}
		return !found
	})
	return found
}

// walkExpr visits node and, while fn keeps returning true, every
// sub-expression of the node kinds analysis produces. It is the analogue of
// expression_tree_walker (nodeFuncs.c:1991) over that node set.
func walkExpr(node ast.Node, fn func(ast.Node) bool) bool {
	if node == nil {
		return true
	}
	if !fn(node) {
		return false
	}
	list := func(l *ast.NodeList) bool {
		if l == nil {
			return true
		}
		for _, item := range l.Items {
			if !walkExpr(item, fn) {
				return false
			}
		}
		return true
	}
	switch n := node.(type) {
	case *ast.Const, *ast.Param, *ast.CaseTestExpr:
		return true
	case *ast.FuncExpr:
		return list(n.Args)
	case *ast.NamedArgExpr:
		return walkExpr(n.Arg, fn)
	case *ast.OpExpr:
		return list(n.Args)
	case *ast.DistinctExpr:
		return list(n.Args)
	case *ast.NullIfExpr:
		return list(n.Args)
	case *ast.ScalarArrayOpExpr:
		return list(n.Args)
	case *ast.BoolExpr:
		return list(n.Args)
	case *ast.RelabelType:
		return walkExpr(n.Arg, fn)
	case *ast.CoerceViaIO:
		return walkExpr(n.Arg, fn)
	case *ast.ArrayCoerceExpr:
		return walkExpr(n.Arg, fn) && walkExpr(n.Elemexpr, fn)
	case *ast.CollateExpr:
		return walkExpr(n.Arg, fn)
	case *ast.CaseExpr:
		if !walkExpr(n.Arg, fn) {
			return false
		}
		for _, w := range n.Args.Items {
			when := w.(*ast.CaseWhen)
			if !walkExpr(when.Expr, fn) || !walkExpr(when.Result, fn) {
				return false
			}
		}
		return walkExpr(n.Defresult, fn)
	case *ast.ArrayExpr:
		return list(n.Elements)
	case *ast.CoalesceExpr:
		return list(n.Args)
	case *ast.MinMaxExpr:
		return list(n.Args)
	case *ast.NullTest:
		return walkExpr(n.Arg, fn)
	case *ast.BooleanTest:
		return walkExpr(n.Arg, fn)
	case *ast.CoerceToDomain:
		return walkExpr(n.Arg, fn)
	}
	pgerror.Elogf("unrecognized node type: %s", nodeTypeName(node))
	return false
}

// applyRelabelType relabels arg to the given type, folding stacked
// relabels and modifying a Const in place - nodeFuncs.c:633.
func applyRelabelType(arg ast.Node, rtype Oid, rtypmod int32, rcollid Oid,
	rformat ast.CoercionForm, rlocation int, overwriteOK bool,
) ast.Node {
	// If we find stacked RelabelTypes (eg, from foo::int::oid) we can discard
	// all but the top one, and must do so to ensure that semantically
	// equivalent expressions are equal().
	for {
		r, ok := arg.(*ast.RelabelType)
		if !ok {
			break
		}
		arg = r.Arg
	}

	if con, ok := arg.(*ast.Const); ok {
		// Modify the Const directly to preserve const-flatness.
		if !overwriteOK {
			con = ast.CloneRefOfConst(con)
		}
		con.Consttype = rtype
		con.Consttypmod = rtypmod
		con.Constcollid = rcollid
		// We keep the Const's original location.
		return con
	}
	if exprType(arg) == rtype && exprTypmod(arg) == rtypmod && exprCollation(arg) == rcollid {
		// Sometimes we find a nest of relabels that net out to nothing.
		return arg
	}
	// Nope, gotta have a RelabelType.
	r := makeRelabelType(expression(arg), rtype, rtypmod, rcollid, rformat)
	r.SetLocation(rlocation)
	return r
}

// expression narrows an analyzed node to the Expression interface that the
// coercion nodes' argument fields require. Every node analysis produces
// implements it; anything else is an internal error.
func expression(n ast.Node) ast.Expression {
	e, ok := n.(ast.Expression)
	if !ok {
		pgerror.Elogf("unrecognized node type: %s", nodeTypeName(n))
	}
	return e
}

// nodeTypeName names a node kind for internal errors, e.g. "ColumnRef".
func nodeTypeName(n ast.Node) string {
	if n == nil {
		return "nil"
	}
	return n.NodeTag().String()[2:]
}
