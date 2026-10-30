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
	"math"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// This file ports make_const (parse_node.c:347), the node constructors from
// makefuncs.c that analysis uses, and the input-function call that turns a
// literal's text into a Datum (parse_type.c:654 stringTypeDatum, fmgr.c:1534
// InputFunctionCall).

// makeConst builds a Const node - makefuncs.c:343. The gateway never sees a
// toasted value, so the detoast step is moot.
func makeConst(consttype Oid, consttypmod int32, constcollid Oid, constlen int,
	constvalue datum.Datum, constisnull, constbyval bool,
) *ast.Const {
	c := ast.NewConst(consttype, constvalue, constisnull)
	c.Consttypmod = consttypmod
	c.Constcollid = constcollid
	c.Constlen = constlen
	c.Constbyval = constbyval
	c.SetLocation(-1) // "unknown"
	return c
}

// makeAConst converts a literal from the grammar into a typed Const -
// parse_node.c:347 (make_const).
//
// For integers and floats we produce int4, int8, or numeric depending on
// the value of the number.
func (p *ParseState) makeAConst(aconst *ast.A_Const) *ast.Const {
	if aconst.Isnull {
		// return a null const
		con := makeConst(pgoid.UNKNOWNOID, -1, pgoid.InvalidOid, -2, datum.Datum{}, true, false)
		con.SetLocation(aconst.Location())
		return con
	}

	var (
		val       datum.Datum
		typeid    Oid
		typelen   int
		typebyval bool
	)
	switch v := aconst.Val.(type) {
	case *ast.Integer:
		// The grammar only produces an Integer for values that fit in int32
		// (anything wider is a Float); a wider IVal is a grammar bug.
		if v.IVal < math.MinInt32 || v.IVal > math.MaxInt32 {
			pgerror.Elogf("integer literal %d out of range", v.IVal)
		}
		val = datum.Int32GetDatum(int32(v.IVal))
		typeid = pgoid.INT4OID
		typelen = 4
		typebyval = true
	case *ast.Float:
		// could be an oversize integer as well as a float ...
		if val64, ok := pgStrtoint64Safe(v.FVal); ok {
			// It might actually fit in int32. Probably only INT_MIN can
			// occur, but we'll code the test generally just to be sure.
			if val32 := int32(val64); val64 == int64(val32) {
				val = datum.Int32GetDatum(val32)
				typeid = pgoid.INT4OID
				typelen = 4
				typebyval = true
			} else {
				val = datum.Int64GetDatum(val64)
				typeid = pgoid.INT8OID
				typelen = 8
				typebyval = true // int8 and float8 alike
			}
		} else {
			s := v.FVal
			val = stringTypeDatum(typeidType(pgoid.NUMERICOID), &s, -1)
			typeid = pgoid.NUMERICOID
			typelen = -1 // variable len
			typebyval = false
		}
	case *ast.Boolean:
		val = datum.BoolGetDatum(v.BoolVal)
		typeid = pgoid.BOOLOID
		typelen = 1
		typebyval = true
	case *ast.String:
		// We assume here that UNKNOWN's internal representation is the same
		// as CSTRING.
		val = datum.BytesGetDatum([]byte(v.SVal))
		typeid = pgoid.UNKNOWNOID // will be coerced later
		typelen = -2              // cstring-style varwidth type
		typebyval = false
	case *ast.BitString:
		s := v.BSVal
		val = stringTypeDatum(typeidType(pgoid.BITOID), &s, -1)
		typeid = pgoid.BITOID
		typelen = -1
		typebyval = false
	default:
		pgerror.Elogf("unrecognized node type: %s", nodeTypeName(aconst.Val))
	}

	con := makeConst(typeid,
		-1,               // typmod -1 is OK for all cases
		pgoid.InvalidOid, // all cases are uncollatable types
		typelen, val, false, typebyval)
	con.SetLocation(aconst.Location())
	return con
}

// pgStrtoint64Safe parses an int8 literal, reporting failure instead of
// raising - numutils.c:728 (pg_strtoint64_safe). int8in is that function
// behind the fmgr convention, so a recovered input error is the soft error.
func pgStrtoint64Safe(s string) (int64, bool) {
	var val datum.Datum
	if err := pgerror.Recover(func() {
		val = stringTypeDatum(typeidType(pgoid.INT8OID), &s, -1)
	}); err != nil {
		return 0, false
	}
	return datum.DatumGetInt64(val), true
}

// stringTypeDatum converts a literal's text to a Datum of the given type
// through the type's input function - parse_type.c:654, calling
// OidInputFunctionCall (fmgr.c:1680) and InputFunctionCall (fmgr.c:1534).
// A nil str is the NULL input upstream passes for a NULL literal.
func stringTypeDatum(tp *pgcatalog.Type, str *string, atttypmod int32) datum.Datum {
	flinfo, err := fmgr.FmgrInfoFor(tp.Input)
	if err != nil {
		pgerror.Elogf("cache lookup failed for function %d", tp.Input)
	}
	if str == nil && flinfo.FnStrict {
		return datum.Datum{} // just return null result
	}
	fcinfo := fmgr.NewFunctionCallInfo(flinfo, 3, pgoid.InvalidOid)
	if str == nil {
		fcinfo.Args[0].IsNull = true
	} else {
		fcinfo.Args[0].Value = datum.BytesGetDatum([]byte(*str))
	}
	fcinfo.Args[1].Value = datum.ObjectIdGetDatum(getTypeIOParam(tp))
	fcinfo.Args[2].Value = datum.Int32GetDatum(atttypmod)

	result := flinfo.FnAddr(fcinfo)

	// Should get null result if and only if str is NULL.
	if str == nil {
		if !fcinfo.IsNull {
			pgerror.Elogf("input function %d returned non-NULL", flinfo.FnOid)
		}
	} else if fcinfo.IsNull {
		pgerror.Elogf("input function %d returned NULL", flinfo.FnOid)
	}
	return result
}

// makeRelabelType builds a RelabelType node - makefuncs.c:430.
func makeRelabelType(arg ast.Expression, rtype Oid, rtypmod int32, rcollid Oid,
	rformat ast.CoercionForm,
) *ast.RelabelType {
	r := ast.NewRelabelType(arg, rtype, rtypmod, rformat)
	r.Resultcollid = rcollid
	r.SetLocation(-1)
	return r
}

// makeFuncExpr builds a FuncExpr node - makefuncs.c:573.
func makeFuncExpr(funcid, rettype Oid, args *ast.NodeList, funccollid, inputcollid Oid,
	fformat ast.CoercionForm,
) *ast.FuncExpr {
	f := ast.NewFuncExpr(funcid, rettype, args)
	f.Funcretset = false   // only allowed case here
	f.Funcvariadic = false // only allowed case here
	f.Funcformat = fformat
	f.Funccollid = funccollid
	f.Inputcollid = inputcollid
	f.SetLocation(-1)
	return f
}

// makeBoolExpr builds a BoolExpr node - makefuncs.c:372.
func makeBoolExpr(boolop ast.BoolExprType, args *ast.NodeList, location int) *ast.BoolExpr {
	b := ast.NewBoolExpr(boolop, args)
	b.SetLocation(location)
	return b
}

// makeSimpleAExpr builds an A_Expr with an unqualified operator name -
// makefuncs.c:48.
func makeSimpleAExpr(kind ast.A_Expr_Kind, name string, lexpr, rexpr ast.Node, location int) *ast.A_Expr {
	return ast.NewA_Expr(kind, ast.NewNodeList(ast.NewString(name)), lexpr, rexpr, location)
}

// copyObject deep-copies a raw or analyzed node - copyfuncs.c copyObjectImpl.
func copyObject(n ast.Node) ast.Node {
	return ast.CloneNode(n)
}

// checkSRFCallPlacement is the gateway's stand-in for
// parse_func.c:2511: set-returning calls are rejected outright rather than
// placed, because the engine evaluates scalar expressions only.
func checkSRFCallPlacement() {
	unsupported("set-returning functions are not supported by the gateway evaluation engine")
}
