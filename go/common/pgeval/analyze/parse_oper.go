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
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
)

// This file ports parse_oper.c (operator resolution and OpExpr construction)
// together with the operator-name lookups of catalog/namespace.c that it
// drives. The operator lookaside cache is not ported: the compiled-in
// catalog lookups it short-circuits are already map lookups.

// funcCandidate is one entry of the candidate list a name lookup returns -
// namespace.h:28 (struct _FuncCandidateList). The list is a slice rather
// than a linked list; the selection heuristics never depend on its order,
// only on which candidates survive. Upstream's pathpos (search-path
// position) and argnumbers (named-call reordering) have no counterpart:
// there is one namespace, and named calls are not resolved.
type funcCandidate struct {
	oid          Oid   // the function or operator's OID
	nominalnargs int   // either pronargs or length(proallargtypes)
	nargs        int   // number of arg types returned
	nvargs       int   // number of args to become variadic array
	ndargs       int   // number of defaulted args
	args         []Oid // arg types
}

// funcDetailCode classifies the outcome of a lookup - parse_func.h:22.
type funcDetailCode int

const (
	funcDetailNotFound   funcDetailCode = iota // no matching function
	funcDetailMultiple                         // too many matching functions
	funcDetailNormal                           // found a matching regular function
	funcDetailProcedure                        // found a matching procedure
	funcDetailAggregate                        // found a matching aggregate function
	funcDetailWindowFunc                       // found a matching window function
	funcDetailCoercion                         // it's a type coercion request
)

// opernameGetOprid finds the operator with exactly the given name and
// operand types, or 0 - namespace.c:1785.
func opernameGetOprid(names *ast.NodeList, oprleft, oprright Oid) Oid {
	schemaname, opername := deconstructQualifiedName(names)
	requireBuiltinSchema(schemaname)
	for _, op := range pgcatalog.OperatorsByName(opername) {
		if op.Left == oprleft && op.Right == oprright {
			return op.Oid
		}
	}
	return pgoid.InvalidOid
}

// opernameGetCandidates lists the operators of the given name and kind
// ('b' infix, 'l' prefix, 0 for either) - namespace.c:1888. Within the
// single builtin namespace no two share an argument list, so there is no
// search-path de-duplication to do.
func opernameGetCandidates(names *ast.NodeList, oprkind byte) []*funcCandidate {
	schemaname, opername := deconstructQualifiedName(names)
	requireBuiltinSchema(schemaname)
	var result []*funcCandidate
	for _, op := range pgcatalog.OperatorsByName(opername) {
		// Ignore operators of wrong kind, if specific kind requested.
		if oprkind != 0 && op.Kind != oprkind {
			continue
		}
		result = append(result, &funcCandidate{
			oid:          op.Oid,
			nominalnargs: 2,
			nargs:        2,
			args:         []Oid{op.Left, op.Right},
		})
	}
	return result
}

// binaryOperExact checks for an "exact" match to the specified operand
// types - parse_oper.c:333. If one operand is an unknown literal, assume it
// should be taken to be the same type as the other operand for this
// purpose. Also, consider the possibility that the other operand is a
// domain type that needs to be reduced to its base type to find an "exact"
// match.
func binaryOperExact(opname *ast.NodeList, arg1, arg2 Oid) Oid {
	wasUnknown := false
	// Unspecified type for one of the arguments? then use the other.
	if arg1 == pgoid.UNKNOWNOID && arg2 != pgoid.InvalidOid {
		arg1 = arg2
		wasUnknown = true
	} else if arg2 == pgoid.UNKNOWNOID && arg1 != pgoid.InvalidOid {
		arg2 = arg1
		wasUnknown = true
	}
	if result := opernameGetOprid(opname, arg1, arg2); result != pgoid.InvalidOid {
		return result
	}
	if wasUnknown {
		// arg1 and arg2 are the same here, need only look at arg1
		if basetype := getBaseType(arg1); basetype != arg1 {
			if result := opernameGetOprid(opname, basetype, basetype); result != pgoid.InvalidOid {
				return result
			}
		}
	}
	return pgoid.InvalidOid
}

// operSelectCandidate resolves among candidate operators none of which
// matches the input types exactly - parse_oper.c:378. Incompatible
// candidates are not yet pruned away.
func operSelectCandidate(inputTypeids []Oid, candidates []*funcCandidate) (funcDetailCode, Oid) {
	// Delete any candidates that cannot actually accept the given input
	// types, whether directly or by coercion.
	candidates = funcMatchArgtypes(inputTypeids, candidates)

	// Done if no candidate or only one candidate survives.
	if len(candidates) == 0 {
		return funcDetailNotFound, pgoid.InvalidOid
	}
	if len(candidates) == 1 {
		return funcDetailNormal, candidates[0].oid
	}

	// Use the same heuristics as for ambiguous functions to resolve the
	// conflict.
	if best := funcSelectCandidate(inputTypeids, candidates); best != nil {
		return funcDetailNormal, best.oid
	}
	return funcDetailMultiple, pgoid.InvalidOid // failed to select a best candidate
}

// oper searches for a binary operator by name and operand types -
// parse_oper.c:435. The returned operator is only promised to be
// coercion-compatible with the input types. With noError the result is nil
// when nothing matches; otherwise that is an error.
func oper(opname *ast.NodeList, ltypeId, rtypeId Oid, noError bool, location int) *pgcatalog.Operator {
	fdresult := funcDetailNotFound

	// First try for an "exact" match.
	operOid := binaryOperExact(opname, ltypeId, rtypeId)
	if operOid == pgoid.InvalidOid {
		// Otherwise, search for the most suitable candidate: binary
		// operators of the given name.
		if clist := opernameGetCandidates(opname, pgcatalog.OperatorKindInfix); len(clist) > 0 {
			// Unspecified type for one of the arguments? then use the other
			// (XXX this is probably dead code?)
			if rtypeId == pgoid.InvalidOid {
				rtypeId = ltypeId
			} else if ltypeId == pgoid.InvalidOid {
				ltypeId = rtypeId
			}
			fdresult, operOid = operSelectCandidate([]Oid{ltypeId, rtypeId}, clist)
		}
	}

	if operOid != pgoid.InvalidOid {
		if op := pgcatalog.OperatorByOid(operOid); op != nil {
			return op
		}
	}
	if !noError {
		opError(opname, ltypeId, rtypeId, fdresult, location)
	}
	return nil
}

// leftOper searches for a prefix operator by name and operand type -
// parse_oper.c:549.
func leftOper(op *ast.NodeList, arg Oid, noError bool, location int) *pgcatalog.Operator {
	fdresult := funcDetailNotFound

	// First try for an "exact" match.
	operOid := opernameGetOprid(op, pgoid.InvalidOid, arg)
	if operOid == pgoid.InvalidOid {
		// Otherwise, search for the most suitable candidate: prefix
		// operators of the given name.
		if clist := opernameGetCandidates(op, pgcatalog.OperatorKindPrefix); len(clist) > 0 {
			// The returned list has args in the form (0, oprright). Move the
			// useful data into args[0] to keep operSelectCandidate simple.
			for _, c := range clist {
				c.args[0] = c.args[1]
			}
			// We must run operSelectCandidate even if only one candidate,
			// otherwise we may falsely return a non-type-compatible
			// operator.
			fdresult, operOid = operSelectCandidate([]Oid{arg}, clist)
		}
	}

	if operOid != pgoid.InvalidOid {
		if o := pgcatalog.OperatorByOid(operOid); o != nil {
			return o
		}
	}
	if !noError {
		opError(op, pgoid.InvalidOid, arg, fdresult, location)
	}
	return nil
}

// opSignatureString renders an operator with its argument types, e.g.
// "integer + integer", for error messages - parse_oper.c:620.
func opSignatureString(op *ast.NodeList, arg1, arg2 Oid) string {
	s := ""
	if arg1 != pgoid.InvalidOid {
		s = formatTypeBe(arg1) + " "
	}
	return s + nameListToString(op) + " " + formatTypeBe(arg2)
}

// opError complains about an unresolvable operator - parse_oper.c:644.
func opError(op *ast.NodeList, arg1, arg2 Oid, fdresult funcDetailCode, _ int) {
	if fdresult == funcDetailMultiple {
		ereportHint(mterrors.PgSSAmbiguousFunction,
			"Could not choose a best candidate operator. You might need to add explicit type casts.",
			"operator is not unique: %s", opSignatureString(op, arg1, arg2))
	}
	hint := "No operator matches the given name and argument types. You might need to add explicit type casts."
	if arg1 == pgoid.InvalidOid || arg2 == pgoid.InvalidOid {
		hint = "No operator matches the given name and argument type. You might need to add an explicit type cast."
	}
	ereportHint(mterrors.PgSSUndefinedFunction, hint,
		"operator does not exist: %s", opSignatureString(op, arg1, arg2))
}

// makeOp builds the OpExpr for an operator application, resolving the
// operator and coercing its operands - parse_oper.c:703. A nil ltree means
// a prefix operator.
func (p *ParseState) makeOp(opname *ast.NodeList, ltree, rtree ast.Node, location int) *ast.OpExpr {
	// Check it's not a postfix operator.
	if rtree == nil {
		ereport(mterrors.PgSSSyntaxError, "postfix operators are not supported")
	}

	// Select the operator.
	var (
		op               *pgcatalog.Operator
		ltypeId, rtypeId Oid
	)
	if ltree == nil {
		// prefix operator
		rtypeId = exprType(rtree)
		ltypeId = pgoid.InvalidOid
		op = leftOper(opname, rtypeId, false, location)
	} else {
		// otherwise, binary operator
		ltypeId = exprType(ltree)
		rtypeId = exprType(rtree)
		op = oper(opname, ltypeId, rtypeId, false, location)
	}

	// Check it's not a shell.
	if op.Code == pgoid.InvalidOid {
		ereport(mterrors.PgSSUndefinedFunction, "operator is only a shell: %s",
			opSignatureString(opname, op.Left, op.Right))
	}

	// Do typecasting and build the expression tree.
	var (
		args                             []ast.Node
		actualArgTypes, declaredArgTypes []Oid
	)
	if ltree == nil {
		// prefix operator
		args = []ast.Node{rtree}
		actualArgTypes = []Oid{rtypeId}
		declaredArgTypes = []Oid{op.Right}
	} else {
		// otherwise, binary operator
		args = []ast.Node{ltree, rtree}
		actualArgTypes = []Oid{ltypeId, rtypeId}
		declaredArgTypes = []Oid{op.Left, op.Right}
	}

	// enforce consistency with polymorphic argument and return types,
	// possibly adjusting return type or declaredArgTypes (which will be used
	// as the cast destination by makeFnArguments)
	rettype := enforceGenericTypeConsistency(actualArgTypes, declaredArgTypes, op.Result, false)

	// perform the necessary typecasting of arguments
	p.makeFnArguments(args, actualArgTypes, declaredArgTypes)

	// and build the expression node
	result := ast.NewOpExpr(op.Oid, op.Code, rettype, ast.NewNodeList(args...))
	result.Opretset = getFuncRetset(op.Code)
	// opcollid and inputcollid will be set by parse_collate.c
	result.SetLocation(location)

	// if it returns a set, check that's OK
	if result.Opretset {
		checkSRFCallPlacement()
	}
	return result
}

// makeScalarArrayOp builds the ScalarArrayOpExpr for "scalar op ANY/ALL
// (array)" - parse_oper.c:811.
func (p *ParseState) makeScalarArrayOp(opname *ast.NodeList, useOr bool, ltree, rtree ast.Node, location int) *ast.ScalarArrayOpExpr {
	ltypeId := exprType(ltree)
	atypeId := exprType(rtree)

	// The right-hand input of the operator will be the element type of the
	// array. However, if we currently have just an untyped literal on the
	// right, stay with that and hope we can resolve the operator.
	var rtypeId Oid
	if atypeId == pgoid.UNKNOWNOID {
		rtypeId = pgoid.UNKNOWNOID
	} else {
		rtypeId = getBaseElementType(atypeId)
		if rtypeId == pgoid.InvalidOid {
			ereport(mterrors.PgSSWrongObjectType, "op ANY/ALL (array) requires array on right side")
		}
	}

	// Now resolve the operator.
	op := oper(opname, ltypeId, rtypeId, false, location)

	// Check it's not a shell.
	if op.Code == pgoid.InvalidOid {
		ereport(mterrors.PgSSUndefinedFunction, "operator is only a shell: %s",
			opSignatureString(opname, op.Left, op.Right))
	}

	args := []ast.Node{ltree, rtree}
	actualArgTypes := []Oid{ltypeId, rtypeId}
	declaredArgTypes := []Oid{op.Left, op.Right}

	// enforce consistency with polymorphic argument and return types,
	// possibly adjusting return type or declaredArgTypes (which will be used
	// as the cast destination by makeFnArguments)
	rettype := enforceGenericTypeConsistency(actualArgTypes, declaredArgTypes, op.Result, false)

	// Check that operator result is boolean.
	if rettype != pgoid.BOOLOID {
		ereport(mterrors.PgSSWrongObjectType, "op ANY/ALL (array) requires operator to yield boolean")
	}
	if getFuncRetset(op.Code) {
		ereport(mterrors.PgSSWrongObjectType, "op ANY/ALL (array) requires operator not to return a set")
	}

	// Now switch back to the array type on the right, arranging for any
	// needed cast to be applied. Beware of polymorphic operators here;
	// enforceGenericTypeConsistency may or may not have replaced a
	// polymorphic type with a real one.
	var resAtypeId Oid
	if isPolymorphicType(declaredArgTypes[1]) {
		// assume the actual array type is OK
		resAtypeId = atypeId
	} else {
		resAtypeId = getArrayType(declaredArgTypes[1])
		if resAtypeId == pgoid.InvalidOid {
			ereport(mterrors.PgSSUndefinedObject, "could not find array type for data type %s",
				formatTypeBe(declaredArgTypes[1]))
		}
	}
	actualArgTypes[1] = atypeId
	declaredArgTypes[1] = resAtypeId

	// perform the necessary typecasting of arguments
	p.makeFnArguments(args, actualArgTypes, declaredArgTypes)

	// and build the expression node
	result := ast.NewScalarArrayOpExpr(op.Oid, useOr, args[0], args[1])
	result.Opfuncid = op.Code
	result.Hashfuncid = pgoid.InvalidOid
	result.Negfuncid = pgoid.InvalidOid
	// inputcollid will be set by parse_collate.c
	result.SetLocation(location)
	return result
}
