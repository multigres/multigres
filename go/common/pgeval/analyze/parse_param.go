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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/pgoid"
)

// This file ports parse_param.c: how $n references become Param nodes.
//
// Fixed parameters have every type declared up front (an extended-protocol
// Parse with all OIDs given, or a prepared statement whose types are known).
// Variable parameters may be undeclared (OID 0): such a $n starts out
// UNKNOWN and takes the first type a coercion asks of it, exactly as
// variable_coerce_param_hook does, so "$1 + 1" deduces int4 for $1.

type paramMode int

const (
	paramsNone paramMode = iota
	paramsFixed
	paramsVariable
)

// SetFixedParameters declares the parameter types for the expressions this
// state analyzes - parse_param.c:67 (setup_parse_fixed_parameters). A $n
// beyond the slice, or whose declared type is 0, is an error.
func (p *ParseState) SetFixedParameters(paramTypes []Oid) {
	p.paramTypes = append([]Oid(nil), paramTypes...)
	p.paramMode = paramsFixed
}

// SetVariableParameters declares parameter types that analysis may extend
// and complete - parse_param.c:83 (setup_parse_variable_parameters). An
// entry of 0 means "deduce from use"; [ParseState.ParamTypes] returns the
// result after analysis.
func (p *ParseState) SetVariableParameters(paramTypes []Oid) {
	p.paramTypes = append([]Oid(nil), paramTypes...)
	p.paramMode = paramsVariable
}

// ParamTypes returns the parameter types known so far: the declared ones,
// plus, in variable mode, the ones deduced during analysis (UNKNOWNOID for a
// parameter referenced but not yet resolved).
func (p *ParseState) ParamTypes() []Oid {
	return append([]Oid(nil), p.paramTypes...)
}

// paramrefHook transforms a ParamRef into a Param - parse_param.c:99
// (fixed_paramref_hook) and :131 (variable_paramref_hook).
func (p *ParseState) paramrefHook(pref *ast.ParamRef) ast.Node {
	paramno := pref.Number
	var paramtype Oid
	switch p.paramMode {
	case paramsFixed:
		// Check parameter number is valid.
		if paramno <= 0 || paramno > len(p.paramTypes) || p.paramTypes[paramno-1] == pgoid.InvalidOid {
			ereport(mterrors.PgSSUndefinedParameter, "there is no parameter $%d", paramno)
		}
		paramtype = p.paramTypes[paramno-1]
	case paramsVariable:
		// Check parameter number is in range.
		if paramno <= 0 || paramno > math.MaxInt32/4 {
			ereport(mterrors.PgSSUndefinedParameter, "there is no parameter $%d", paramno)
		}
		if paramno > len(p.paramTypes) {
			// Need to enlarge param array.
			p.paramTypes = append(p.paramTypes, make([]Oid, paramno-len(p.paramTypes))...)
		}
		// If not seen before, initialize to UNKNOWN type.
		if p.paramTypes[paramno-1] == pgoid.InvalidOid {
			p.paramTypes[paramno-1] = pgoid.UNKNOWNOID
		}
		paramtype = p.paramTypes[paramno-1]
	default:
		// The core parser knows nothing about Params - parse_expr.c:886.
		return nil
	}

	param := ast.NewParam(ast.PARAM_EXTERN, paramno, paramtype)
	param.Paramtypmod = -1
	param.Paramcollid = getTypcollation(paramtype)
	param.SetLocation(pref.Location())
	return param
}

// coerceParamHook coerces a still-UNKNOWN Param to the type a context asks
// for, in variable mode - parse_param.c:186 (variable_coerce_param_hook).
// It returns nil to signal that normal coercion should proceed.
func (p *ParseState) coerceParamHook(param *ast.Param, targetTypeId Oid, _ int32, location int) ast.Node {
	if p.paramMode != paramsVariable || param.Paramkind != ast.PARAM_EXTERN || param.Paramtype != pgoid.UNKNOWNOID {
		// Else signal to proceed with normal coercion.
		return nil
	}
	// Input is a Param of previously undetermined type, and we want to
	// update our knowledge of the Param's type.
	paramno := param.Paramid
	if paramno <= 0 || paramno > len(p.paramTypes) { // shouldn't happen, but...
		ereport(mterrors.PgSSUndefinedParameter, "there is no parameter $%d", paramno)
	}
	switch p.paramTypes[paramno-1] {
	case pgoid.UNKNOWNOID:
		// We've successfully resolved the type.
		p.paramTypes[paramno-1] = targetTypeId
	case targetTypeId:
		// We previously resolved the type, and it matches.
	default:
		// Oops
		ereportDetail(mterrors.PgSSAmbiguousParameter,
			formatTypeBe(p.paramTypes[paramno-1])+" versus "+formatTypeBe(targetTypeId),
			"inconsistent types deduced for parameter $%d", paramno)
	}

	param.Paramtype = targetTypeId

	// Note: it is tempting here to set the Param's paramtypmod to
	// targetTypeMod, but that is probably unwise because we have no
	// infrastructure that enforces that the value delivered for a Param will
	// match any particular typmod. Leaving it -1 ensures that a run-time
	// length check/coercion will occur if needed.
	param.Paramtypmod = -1

	// This module always sets a Param's collation to be the default for its
	// datatype.
	param.Paramcollid = getTypcollation(param.Paramtype)

	// Use the leftmost of the param's and coercion's locations.
	if location >= 0 && (param.Location() < 0 || location < param.Location()) {
		param.SetLocation(location)
	}
	return param
}

// CheckVariableParameters verifies, after analysis in variable mode, that
// every Param in the tree agrees with the parameter type finally deduced -
// parse_param.c:268 (check_variable_parameters). Some Params might still
// be UNKNOWN, if there wasn't anything to force their coercion, and yet
// other instances seen later might have gotten coerced: "$1 IS NULL AND
// $1 = 1" leaves the first $1 unknown while the second deduces int4.
//
// As upstream notes, this intentionally does not check that every
// parameter got a non-UNKNOWN type; the Parse-message handler does that
// over [ParseState.ParamTypes] (postgres.c exec_parse_message), and the
// target-list rule that resolves a bare "$1" to text is statement-level
// analysis. Both belong to the caller.
func (p *ParseState) CheckVariableParameters(expr ast.Node) error {
	// If numParams is zero then no Params were generated, so no work.
	if len(p.paramTypes) == 0 {
		return nil
	}
	return recoverErr(func() {
		walkExpr(expr, func(n ast.Node) bool {
			param, ok := n.(*ast.Param)
			if !ok || param.Paramkind != ast.PARAM_EXTERN {
				return true
			}
			paramno := param.Paramid
			if paramno <= 0 || paramno > len(p.paramTypes) { // shouldn't happen, but...
				ereport(mterrors.PgSSUndefinedParameter, "there is no parameter $%d", paramno)
			}
			if param.Paramtype != p.paramTypes[paramno-1] {
				ereport(mterrors.PgSSAmbiguousParameter,
					"could not determine data type of parameter $%d", paramno)
			}
			return true
		})
	})
}
