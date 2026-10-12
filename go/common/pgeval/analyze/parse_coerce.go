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
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// This file ports parse_coerce.c: deciding whether and how a value of one
// type becomes a value of another, and building the nodes that do it.

// coercionPathType classifies how a coercion is performed - parse_coerce.h:24.
type coercionPathType int

const (
	coercionPathNone        coercionPathType = iota // failed to find any coercion pathway
	coercionPathFunc                                // apply the specified coercion function
	coercionPathRelabelType                         // binary-compatible cast, no function
	coercionPathArrayCoerce                         // need an ArrayCoerceExpr node
	coercionPathCoerceViaIO                         // need a CoerceViaIO node
)

// coerceToTargetType converts expr to the target type and typmod, or returns
// nil if no coercion is possible - parse_coerce.c:78. This is the
// general-purpose entry point; the component steps are for cases where the
// conversion is expected to succeed.
func (p *ParseState) coerceToTargetType(expr ast.Node, exprtype, targettype Oid, targettypmod int32,
	ccontext ast.CoercionContext, cformat ast.CoercionForm, location int,
) ast.Node {
	if !canCoerceType([]Oid{exprtype}, []Oid{targettype}, ccontext) {
		return nil
	}
	// Upstream strips a top-level CollateExpr here and puts it back after.
	// COLLATE is not analyzed yet (parse_collate.c is not ported), so no
	// CollateExpr can reach this point.
	result := p.coerceType(expr, exprtype, targettype, targettypmod, ccontext, cformat, location)

	// If the target is a fixed-length type, it may need a length coercion as
	// well as a type coercion. If we find ourselves adding both, force the
	// inner coercion node to implicit display form.
	_, isConst := result.(*ast.Const)
	return coerceTypeTypmod(result, targettype, targettypmod, ccontext, cformat, location,
		result != expr && !isConst)
}

// coerceType converts an expression to a different type; the caller has
// already determined that the coercion is possible - parse_coerce.c:157.
// No typmod coercion is performed here; see coerceTypeTypmod.
func (p *ParseState) coerceType(node ast.Node, inputTypeId, targetTypeId Oid, targetTypeMod int32,
	ccontext ast.CoercionContext, cformat ast.CoercionForm, location int,
) ast.Node {
	if targetTypeId == inputTypeId || node == nil {
		// no conversion needed
		return node
	}
	switch targetTypeId {
	case pgoid.ANYOID, pgoid.ANYELEMENTOID, pgoid.ANYNONARRAYOID,
		pgoid.ANYCOMPATIBLEOID, pgoid.ANYCOMPATIBLENONARRAYOID:
		// Assume can_coerce_type verified that implicit coercion is okay.
		// By returning the unmodified node we are saying that it's OK to
		// treat an UNKNOWN constant as a valid input for a function accepting
		// one of these pseudotypes. We do NOT want a RelabelType here: the
		// exposed type of the function argument must be its actual type, not
		// the polymorphic pseudotype.
		return node
	case pgoid.ANYARRAYOID, pgoid.ANYENUMOID, pgoid.ANYRANGEOID, pgoid.ANYMULTIRANGEOID,
		pgoid.ANYCOMPATIBLEARRAYOID, pgoid.ANYCOMPATIBLERANGEOID, pgoid.ANYCOMPATIBLEMULTIRANGEOID:
		// These cases are unlike the ones above because the exposed type of
		// the argument must be an actual array, enum, range, or multirange
		// type. In particular the argument must *not* be an UNKNOWN constant.
		// If it is, we just fall through; below, we'll call the pseudotype's
		// input function, which will produce an error. Also, if what we have
		// is a domain over array, enum, range, or multirange, we have to
		// relabel it to its base type.
		if inputTypeId != pgoid.UNKNOWNOID {
			if baseTypeId := getBaseType(inputTypeId); baseTypeId != inputTypeId {
				r := makeRelabelType(expression(node), baseTypeId, -1, pgoid.InvalidOid, cformat)
				r.SetLocation(location)
				return r
			}
			// Not a domain type, so return it as-is.
			return node
		}
	}
	if con, ok := node.(*ast.Const); ok && inputTypeId == pgoid.UNKNOWNOID {
		// Input is a string constant with previously undetermined type.
		// Apply the target type's typinput function to it to produce a
		// constant of the target type.
		//
		// NOTE: this case cannot be folded together with the other
		// constant-input case, since the typinput function does not
		// necessarily behave the same as a type conversion function. For
		// example, int4's typinput function will reject "1.2", whereas
		// float-to-int type conversion will round to integer.

		// If the target type is a domain, we want to call its base type's
		// input routine, not domain_in(). The needed check will be applied
		// properly inside coerceToDomain.
		baseTypeMod := targetTypeMod
		baseTypeId := getBaseTypeAndTypmod(targetTypeId, &baseTypeMod)

		// For most types we pass typmod -1 to the input routine, because
		// existing input routines follow implicit-coercion semantics for
		// length checks, which is not always what we want here. Any length
		// constraint will be applied later by our caller. An exception
		// however is the INTERVAL type, for which we *must* pass the typmod
		// or it won't be able to obey the bizarre SQL-spec input rules.
		inputTypeMod := int32(-1)
		if baseTypeId == pgoid.INTERVALOID {
			inputTypeMod = baseTypeMod
		}
		baseType := typeidType(baseTypeId)

		// We assume here that UNKNOWN's internal representation is the same
		// as CSTRING.
		var str *string
		if !con.Constisnull {
			s := string(datum.DatumGetBytes(con.Constvalue))
			str = &s
		}
		newcon := makeConst(baseTypeId, inputTypeMod, baseType.Collation, int(baseType.Len),
			stringTypeDatum(baseType, str, inputTypeMod), con.Constisnull, baseType.ByVal)
		// We use the original literal's location regardless of the position
		// of the coercion.
		newcon.SetLocation(con.Location())

		var result ast.Node = newcon
		// If target is a domain, apply constraints.
		if baseTypeId != targetTypeId {
			result = coerceToDomain(result, baseTypeId, baseTypeMod, targetTypeId,
				ccontext, cformat, location, false)
		}
		return result
	}
	if param, ok := node.(*ast.Param); ok {
		// Allow the coerce-param hook to decide what happens. It can return
		// a transformed node (very possibly the same Param node), or nil to
		// indicate we should proceed with normal coercion.
		if result := p.coerceParamHook(param, targetTypeId, targetTypeMod, location); result != nil {
			return result
		}
	}
	pathtype, funcId := findCoercionPathway(targetTypeId, inputTypeId, ccontext)
	if pathtype != coercionPathNone {
		if pathtype != coercionPathRelabelType {
			// Generate an expression tree representing run-time application
			// of the conversion function. If we are dealing with a domain
			// target type, the conversion function will yield the base type,
			// and we need to extract the correct typmod to use from the
			// domain's typtypmod.
			baseTypeMod := targetTypeMod
			baseTypeId := getBaseTypeAndTypmod(targetTypeId, &baseTypeMod)
			result := buildCoercionExpression(node, pathtype, funcId, baseTypeId, baseTypeMod,
				ccontext, cformat, location)
			// If domain, coerce to the domain type and relabel with domain
			// type ID, hiding the previous coercion node.
			if targetTypeId != baseTypeId {
				result = coerceToDomain(result, baseTypeId, baseTypeMod, targetTypeId,
					ccontext, cformat, location, true)
			}
			return result
		}
		// We don't need to do a physical conversion, but we do need to
		// attach a RelabelType node so that the expression will be seen to
		// have the intended type when inspected by higher-level code.
		//
		// Also, domains may have value restrictions beyond the base type
		// that must be accounted for. If the destination is a domain then
		// we won't need a RelabelType node.
		result := coerceToDomain(node, pgoid.InvalidOid, -1, targetTypeId, ccontext, cformat, location, false)
		if result == node {
			r := makeRelabelType(expression(result), targetTypeId, -1, pgoid.InvalidOid, cformat)
			r.SetLocation(location)
			result = r
		}
		return result
	}
	if inputTypeId == pgoid.RECORDOID && isComplex(targetTypeId) {
		// coerce_record_to_complex works on RowExprs and whole-row Vars,
		// neither of which analysis produces yet.
		unsupported("coercion of a record to a composite type is not supported by the gateway evaluation engine")
	}
	if targetTypeId == pgoid.RECORDOID && isComplex(inputTypeId) {
		// Coerce a specific complex type to RECORD. NB: we do NOT want a
		// RelabelType here.
		return node
	}
	if targetTypeId == pgoid.RECORDARRAYOID && isComplexArray(inputTypeId) {
		// Coerce a specific complex array type to record[].
		return node
	}
	if typeInheritsFrom(inputTypeId, targetTypeId) || typeIsOfTypedTable(inputTypeId, targetTypeId) {
		// Upstream builds a ConvertRowtypeExpr; no builtin type inherits.
		unsupported("row type conversion is not supported by the gateway evaluation engine")
	}
	// If we get here, caller blew it.
	pgerror.Elogf("failed to find conversion function from %s to %s",
		formatTypeBe(inputTypeId), formatTypeBe(targetTypeId))
	return nil
}

// canCoerceType reports whether each input type can be coerced to the
// corresponding target type in the given context - parse_coerce.c:556.
func canCoerceType(inputTypeids, targetTypeids []Oid, ccontext ast.CoercionContext) bool {
	haveGenerics := false
	// run through argument list...
	for i := range inputTypeids {
		inputTypeId, targetTypeId := inputTypeids[i], targetTypeids[i]
		// no problem if same type
		if inputTypeId == targetTypeId {
			continue
		}
		// accept if target is ANY
		if targetTypeId == pgoid.ANYOID {
			continue
		}
		// accept if target is polymorphic, for now
		if isPolymorphicType(targetTypeId) {
			haveGenerics = true // do more checking later
			continue
		}
		// If input is an untyped string constant, assume we can convert it
		// to anything.
		if inputTypeId == pgoid.UNKNOWNOID {
			continue
		}
		// If pg_cast shows that we can coerce, accept. This test now covers
		// both binary-compatible and coercion-function cases.
		if pathtype, _ := findCoercionPathway(targetTypeId, inputTypeId, ccontext); pathtype != coercionPathNone {
			continue
		}
		// If input is RECORD and target is a composite type, assume we can
		// coerce (may need tighter checking here).
		if inputTypeId == pgoid.RECORDOID && isComplex(targetTypeId) {
			continue
		}
		// If input is a composite type and target is RECORD, accept.
		if targetTypeId == pgoid.RECORDOID && isComplex(inputTypeId) {
			continue
		}
		// If input is a composite array type and target is record[], accept.
		if targetTypeId == pgoid.RECORDARRAYOID && isComplexArray(inputTypeId) {
			continue
		}
		// If input is a class type that inherits from target, accept.
		if typeInheritsFrom(inputTypeId, targetTypeId) || typeIsOfTypedTable(inputTypeId, targetTypeId) {
			continue
		}
		// Else, cannot coerce at this argument position.
		return false
	}
	// If we found any generic argument types, cross-check them.
	if haveGenerics && !checkGenericTypeConsistency(inputTypeids, targetTypeids) {
		return false
	}
	return true
}

// isComplexArray reports whether typid is an array of composite -
// parse_coerce.c:3368.
func isComplexArray(typid Oid) bool {
	elemtype := getElementType(typid)
	return elemtype != pgoid.InvalidOid && isComplex(elemtype)
}

// coerceToDomain wraps arg in a CoerceToDomain if typeId is a domain, else
// returns it as-is - parse_coerce.c:676. With no domains in the catalog the
// wrapping branch is unreachable, but the upstream shape stays so that
// domain support slots in here.
func coerceToDomain(arg ast.Node, baseTypeId Oid, baseTypeMod int32, typeId Oid,
	ccontext ast.CoercionContext, cformat ast.CoercionForm, location int, hideInputCoercion bool,
) ast.Node {
	// Get the base type if it hasn't been supplied.
	if baseTypeId == pgoid.InvalidOid {
		baseTypeId = getBaseTypeAndTypmod(typeId, &baseTypeMod)
	}
	// If it isn't a domain, return the node as it was passed in.
	if baseTypeId == typeId {
		return arg
	}
	// Suppress display of nested coercion steps.
	if hideInputCoercion {
		hideCoercionNode(arg)
	}
	// If the domain applies a typmod to its base type, build the
	// appropriate coercion step.
	arg = coerceTypeTypmod(arg, baseTypeId, baseTypeMod, ccontext, ast.COERCE_IMPLICIT_CAST, location, false)
	// Now build the domain coercion node. This represents run-time checking
	// of any constraints currently attached to the domain.
	result := ast.NewCoerceToDomain(expression(arg), typeId, -1, cformat)
	result.SetLocation(location)
	return result
}

// coerceTypeTypmod forces a value to a particular typmod, if meaningful and
// possible - parse_coerce.c:753. The caller must already have ensured that
// the value is of the correct type.
func coerceTypeTypmod(node ast.Node, targetTypeId Oid, targetTypMod int32,
	ccontext ast.CoercionContext, cformat ast.CoercionForm, location int, hideInputCoercion bool,
) ast.Node {
	// Skip coercion if already done.
	if targetTypMod == exprTypmod(node) {
		return node
	}
	// Suppress display of nested coercion steps.
	if hideInputCoercion {
		hideCoercionNode(node)
	}
	// A negative typmod means that no actual coercion is needed, but we
	// still want a RelabelType to ensure that the expression exposes the
	// intended typmod.
	pathtype := coercionPathNone
	var funcId Oid
	if targetTypMod >= 0 {
		pathtype, funcId = findTypmodCoercionFunction(targetTypeId)
	}
	if pathtype != coercionPathNone {
		return buildCoercionExpression(node, pathtype, funcId, targetTypeId, targetTypMod, ccontext, cformat, location)
	}
	// We don't need to perform any actual coercion step, but we should apply
	// a RelabelType to ensure that the expression exposes the intended
	// typmod.
	return applyRelabelType(node, targetTypeId, targetTypMod, exprCollation(node), cformat, location, false)
}

// hideCoercionNode marks a coercion node as IMPLICIT so that it is not
// displayed; semantics are unchanged - parse_coerce.c:811.
func hideCoercionNode(node ast.Node) {
	switch n := node.(type) {
	case *ast.FuncExpr:
		n.Funcformat = ast.COERCE_IMPLICIT_CAST
	case *ast.RelabelType:
		n.Relabelformat = ast.COERCE_IMPLICIT_CAST
	case *ast.CoerceViaIO:
		n.Coerceformat = ast.COERCE_IMPLICIT_CAST
	case *ast.ArrayCoerceExpr:
		n.Coerceformat = ast.COERCE_IMPLICIT_CAST
	case *ast.CoerceToDomain:
		n.Coercionformat = ast.COERCE_IMPLICIT_CAST
	default:
		pgerror.Elogf("unsupported node type: %s", nodeTypeName(node))
	}
}

// buildCoercionExpression constructs the node applying a pg_cast entry, for
// both type coercion and length coercion - parse_coerce.c:839.
func buildCoercionExpression(node ast.Node, pathtype coercionPathType, funcId, targetTypeId Oid,
	targetTypMod int32, ccontext ast.CoercionContext, cformat ast.CoercionForm, location int,
) ast.Node {
	nargs := 0
	if funcId != pgoid.InvalidOid {
		flinfo, err := fmgr.FmgrInfoFor(funcId)
		if err != nil {
			pgerror.Elogf("cache lookup failed for function %d", funcId)
		}
		// These checks essentially verify that the function is a legal
		// coercion function. We can't make the seemingly obvious tests on
		// prorettype and proargtypes[0], even in the COERCION_PATH_FUNC case,
		// because of various binary-compatibility cases.
		if flinfo.FnRetset {
			pgerror.Elogf("coercion function %d returns a set", funcId)
		}
		nargs = int(flinfo.FnNargs)
		if nargs < 1 || nargs > 3 {
			pgerror.Elogf("coercion function %d takes %d arguments", funcId, nargs)
		}
	}

	switch pathtype {
	case coercionPathFunc:
		// We build an ordinary FuncExpr with special arguments.
		if funcId == pgoid.InvalidOid {
			pgerror.Elogf("coercion function expected")
		}
		args := ast.NewNodeList(node)
		if nargs >= 2 {
			// Pass target typmod as an int4 constant.
			args.Append(makeConst(pgoid.INT4OID, -1, pgoid.InvalidOid, 4,
				datum.Int32GetDatum(targetTypMod), false, true))
		}
		if nargs == 3 {
			// Pass it a boolean isExplicit parameter, too.
			args.Append(makeConst(pgoid.BOOLOID, -1, pgoid.InvalidOid, 1,
				datum.BoolGetDatum(ccontext == ast.COERCION_EXPLICIT), false, true))
		}
		fexpr := makeFuncExpr(funcId, targetTypeId, args, pgoid.InvalidOid, pgoid.InvalidOid, cformat)
		fexpr.SetLocation(location)
		return fexpr
	case coercionPathArrayCoerce:
		// We need to build an ArrayCoerceExpr. Look through any domain over
		// the source array type. Note we don't expect that the target type
		// is a domain; it must be a plain array.
		sourceBaseTypeMod := exprTypmod(node)
		sourceBaseTypeId := getBaseTypeAndTypmod(exprType(node), &sourceBaseTypeMod)

		// Set up a CaseTestExpr representing one element of the source
		// array. This is an abuse of CaseTestExpr, but it's OK as long as
		// there can't be any CaseExpr or ArrayCoerceExpr within the completed
		// elemexpr.
		ctest := ast.NewCaseTestExpr(getElementType(sourceBaseTypeId), int(sourceBaseTypeMod), pgoid.InvalidOid, -1)
		if ctest.TypeId == pgoid.InvalidOid {
			pgerror.Elogf("source type %s is not an array", formatTypeBe(sourceBaseTypeId))
		}
		// And coerce it to the target element type.
		targetElementType := getElementType(targetTypeId)
		if targetElementType == pgoid.InvalidOid {
			pgerror.Elogf("target type %s is not an array", formatTypeBe(targetTypeId))
		}
		// Upstream passes no ParseState here: no parameter hook applies to
		// the placeholder.
		var noParams ParseState
		elemexpr := noParams.coerceToTargetType(ctest, ctest.TypeId, targetElementType, targetTypMod,
			ccontext, cformat, location)
		if elemexpr == nil { // shouldn't happen
			pgerror.Elogf("failed to coerce array element type as expected")
		}
		acoerce := ast.NewArrayCoerceExpr(expression(node), expression(elemexpr), targetTypeId, cformat)
		// Label the output as having a particular element typmod only if we
		// ended up with a per-element expression that is labeled that way.
		acoerce.Resulttypmod = exprTypmod(elemexpr)
		acoerce.SetLocation(location)
		return acoerce
	case coercionPathCoerceViaIO:
		// We need to build a CoerceViaIO node.
		if funcId != pgoid.InvalidOid {
			pgerror.Elogf("unexpected coercion function %d for I/O coercion", funcId)
		}
		iocoerce := ast.NewCoerceViaIO(expression(node), targetTypeId, cformat)
		iocoerce.SetLocation(location)
		return iocoerce
	}
	pgerror.Elogf("unsupported pathtype %d in build_coercion_expression", int(pathtype))
	return nil
}

// coerceToBoolean coerces the argument of a boolean construct (WHERE, AND,
// CASE/WHEN, ...) to bool and checks that it is not a set - parse_coerce.c:1161.
func (p *ParseState) coerceToBoolean(node ast.Node, constructName string) ast.Node {
	inputTypeId := exprType(node)
	if inputTypeId != pgoid.BOOLOID {
		newnode := p.coerceToTargetType(node, inputTypeId, pgoid.BOOLOID, -1,
			ast.COERCION_ASSIGNMENT, ast.COERCE_IMPLICIT_CAST, -1)
		if newnode == nil {
			ereport(mterrors.PgSSDatatypeMismatch, "argument of %s must be type %s, not type %s",
				constructName, "boolean", formatTypeBe(inputTypeId))
		}
		node = newnode
	}
	if expressionReturnsSet(node) {
		ereport(mterrors.PgSSDatatypeMismatch, "argument of %s must not return a set", constructName)
	}
	return node
}

// selectCommonType determines the common supertype of a non-empty list of
// expressions, for CASE, COALESCE, ARRAY and similar - parse_coerce.c:1344.
// Earlier items are preferred if there is doubt. With an empty context the
// routine returns InvalidOid rather than raising on failure. The second
// result is the expression the type was taken from.
//
// "Failure" just means that there were inputs of different type categories;
// the caller must still check that every input is coercible to the chosen
// type (see verifyCommonType).
func (p *ParseState) selectCommonType(exprs []ast.Node, context string) (Oid, ast.Node) {
	pexpr := exprs[0]
	ptype := exprType(pexpr)
	lc := 1

	// If all input types are valid and exactly the same, just pick that
	// type. This is the only way that we will resolve the result as being a
	// domain type; otherwise domains are smashed to their base types for
	// comparison.
	if ptype != pgoid.UNKNOWNOID {
		for ; lc < len(exprs); lc++ {
			if exprType(exprs[lc]) != ptype {
				break
			}
		}
		if lc == len(exprs) { // got to the end of the list?
			return ptype, pexpr
		}
	}

	// Nope, so set up for the full algorithm. Note that at this point, lc
	// points to the first list item with type different from pexpr's; we
	// need not re-examine any items the previous loop advanced over.
	ptype = getBaseType(ptype)
	pcategory, pispreferred := getTypeCategoryPreferred(ptype)

	for ; lc < len(exprs); lc++ {
		nexpr := exprs[lc]
		ntype := getBaseType(exprType(nexpr))

		// move on to next one if no new information...
		if ntype != pgoid.UNKNOWNOID && ntype != ptype {
			ncategory, nispreferred := getTypeCategoryPreferred(ntype)
			switch {
			case ptype == pgoid.UNKNOWNOID:
				// so far, only unknowns so take anything...
				pexpr, ptype, pcategory, pispreferred = nexpr, ntype, ncategory, nispreferred
			case ncategory != pcategory:
				// both types in different categories? then not much hope...
				if context == "" {
					return pgoid.InvalidOid, nil
				}
				ereport(mterrors.PgSSDatatypeMismatch, "%s types %s and %s cannot be matched",
					context, formatTypeBe(ptype), formatTypeBe(ntype))
			case !pispreferred &&
				canCoerceType([]Oid{ptype}, []Oid{ntype}, ast.COERCION_IMPLICIT) &&
				!canCoerceType([]Oid{ntype}, []Oid{ptype}, ast.COERCION_IMPLICIT):
				// take new type if can coerce to it implicitly but not the
				// other way; but if we have a preferred type, stay on it.
				pexpr, ptype, pcategory, pispreferred = nexpr, ntype, ncategory, nispreferred
			}
		}
	}

	// If all the inputs were UNKNOWN type --- ie, unknown-type literals ---
	// then resolve as type TEXT. We need to coerce the unknown literals while
	// they are still literals, so a decision has to be made now.
	if ptype == pgoid.UNKNOWNOID {
		ptype = pgoid.TEXTOID
	}
	return ptype, pexpr
}

// selectCommonTypeFromOids is selectCommonType over type OIDs rather than
// expressions - parse_coerce.c:1480.
func selectCommonTypeFromOids(typeids []Oid, noerror bool) Oid {
	ptype := typeids[0]
	i := 1

	// If all input types are valid and exactly the same, pick that type.
	if ptype != pgoid.UNKNOWNOID {
		for ; i < len(typeids); i++ {
			if typeids[i] != ptype {
				break
			}
		}
		if i == len(typeids) {
			return ptype
		}
	}

	// Nope, so set up for the full algorithm. Note that at this point, we
	// can skip array entries before "i"; they are all equal to ptype.
	ptype = getBaseType(ptype)
	pcategory, pispreferred := getTypeCategoryPreferred(ptype)

	for ; i < len(typeids); i++ {
		ntype := getBaseType(typeids[i])

		// move on to next one if no new information...
		if ntype != pgoid.UNKNOWNOID && ntype != ptype {
			ncategory, nispreferred := getTypeCategoryPreferred(ntype)
			switch {
			case ptype == pgoid.UNKNOWNOID:
				// so far, only unknowns so take anything...
				ptype, pcategory, pispreferred = ntype, ncategory, nispreferred
			case ncategory != pcategory:
				// both types in different categories? then not much hope...
				if noerror {
					return pgoid.InvalidOid
				}
				ereport(mterrors.PgSSDatatypeMismatch, "argument types %s and %s cannot be matched",
					formatTypeBe(ptype), formatTypeBe(ntype))
			case !pispreferred &&
				canCoerceType([]Oid{ptype}, []Oid{ntype}, ast.COERCION_IMPLICIT) &&
				!canCoerceType([]Oid{ntype}, []Oid{ptype}, ast.COERCION_IMPLICIT):
				// take new type if can coerce to it implicitly but not the
				// other way; but if we have a preferred type, stay on it.
				ptype, pcategory, pispreferred = ntype, ncategory, nispreferred
			}
		}
	}

	// Like selectCommonType, choose TEXT if all inputs were UNKNOWN.
	if ptype == pgoid.UNKNOWNOID {
		ptype = pgoid.TEXTOID
	}
	return ptype
}

// coerceToCommonType coerces an expression to the type selectCommonType
// chose - parse_coerce.c:1574.
func (p *ParseState) coerceToCommonType(node ast.Node, targetTypeId Oid, context string) ast.Node {
	inputTypeId := exprType(node)
	if inputTypeId == targetTypeId {
		return node // no work
	}
	if canCoerceType([]Oid{inputTypeId}, []Oid{targetTypeId}, ast.COERCION_IMPLICIT) {
		return p.coerceType(node, inputTypeId, targetTypeId, -1,
			ast.COERCION_IMPLICIT, ast.COERCE_IMPLICIT_CAST, -1)
	}
	ereport(mterrors.PgSSCannotCoerce, "%s could not convert type %s to %s",
		context, formatTypeBe(inputTypeId), formatTypeBe(targetTypeId))
	return nil
}

// verifyCommonType reports whether every expression can be implicitly
// coerced to the proposed common type - parse_coerce.c:1608.
func verifyCommonType(commonType Oid, exprs []ast.Node) bool {
	for _, e := range exprs {
		if !canCoerceType([]Oid{exprType(e)}, []Oid{commonType}, ast.COERCION_IMPLICIT) {
			return false
		}
	}
	return true
}

// verifyCommonTypeFromOids is verifyCommonType over type OIDs - parse_coerce.c:1628.
func verifyCommonTypeFromOids(commonType Oid, typeids []Oid) bool {
	for _, t := range typeids {
		if !canCoerceType([]Oid{t}, []Oid{commonType}, ast.COERCION_IMPLICIT) {
			return false
		}
	}
	return true
}

// checkGenericTypeConsistency reports whether actual argument types are
// potentially compatible with a polymorphic signature - parse_coerce.c:1739.
// The rules are listed with enforceGenericTypeConsistency, which applies
// them for real; this is the no-error pre-check used while matching
// candidates. UNKNOWN inputs for polymorphic arguments are assumed okay.
func checkGenericTypeConsistency(actualArgTypes, declaredArgTypes []Oid) bool {
	var (
		elemTypeid, arrayTypeid, rangeTypeid, multirangeTypeid        Oid
		anycompatibleRangeTypeid, anycompatibleRangeTypelem           Oid
		anycompatibleMultirangeTypeid, anycompatibleMultirangeTypelem Oid
		rangeTypelem                                                  Oid
		haveAnynonarray, haveAnyenum, haveAnycompatibleNonarray       bool
		anycompatibleActualTypes                                      []Oid
	)

	// Loop through the arguments to see if we have any that are polymorphic.
	// If so, require the actual types to be consistent.
	for j := range actualArgTypes {
		declType, actualType := declaredArgTypes[j], actualArgTypes[j]
		switch declType {
		case pgoid.ANYELEMENTOID, pgoid.ANYNONARRAYOID, pgoid.ANYENUMOID:
			switch declType {
			case pgoid.ANYNONARRAYOID:
				haveAnynonarray = true
			case pgoid.ANYENUMOID:
				haveAnyenum = true
			}
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			if elemTypeid != pgoid.InvalidOid && actualType != elemTypeid {
				return false
			}
			elemTypeid = actualType
		case pgoid.ANYARRAYOID:
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			actualType = getBaseType(actualType) // flatten domains
			if arrayTypeid != pgoid.InvalidOid && actualType != arrayTypeid {
				return false
			}
			arrayTypeid = actualType
		case pgoid.ANYRANGEOID:
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			actualType = getBaseType(actualType) // flatten domains
			if rangeTypeid != pgoid.InvalidOid && actualType != rangeTypeid {
				return false
			}
			rangeTypeid = actualType
		case pgoid.ANYMULTIRANGEOID:
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			actualType = getBaseType(actualType) // flatten domains
			if multirangeTypeid != pgoid.InvalidOid && actualType != multirangeTypeid {
				return false
			}
			multirangeTypeid = actualType
		case pgoid.ANYCOMPATIBLEOID, pgoid.ANYCOMPATIBLENONARRAYOID:
			if declType == pgoid.ANYCOMPATIBLENONARRAYOID {
				haveAnycompatibleNonarray = true
			}
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			// collect the actual types of non-unknown COMPATIBLE args
			anycompatibleActualTypes = append(anycompatibleActualTypes, actualType)
		case pgoid.ANYCOMPATIBLEARRAYOID:
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			actualType = getBaseType(actualType) // flatten domains
			elemType := getElementType(actualType)
			if elemType == pgoid.InvalidOid {
				return false // not an array
			}
			// collect the element type for common-supertype choice
			anycompatibleActualTypes = append(anycompatibleActualTypes, elemType)
		case pgoid.ANYCOMPATIBLERANGEOID:
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			actualType = getBaseType(actualType) // flatten domains
			if anycompatibleRangeTypeid != pgoid.InvalidOid {
				// All ANYCOMPATIBLERANGE arguments must be the same type.
				if anycompatibleRangeTypeid != actualType {
					return false
				}
			} else {
				anycompatibleRangeTypeid = actualType
				anycompatibleRangeTypelem = getRangeSubtype(actualType)
				if anycompatibleRangeTypelem == pgoid.InvalidOid {
					return false // not a range type
				}
				// collect the subtype for common-supertype choice
				anycompatibleActualTypes = append(anycompatibleActualTypes, anycompatibleRangeTypelem)
			}
		case pgoid.ANYCOMPATIBLEMULTIRANGEOID:
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			actualType = getBaseType(actualType) // flatten domains
			if anycompatibleMultirangeTypeid != pgoid.InvalidOid {
				// All ANYCOMPATIBLEMULTIRANGE arguments must be the same type.
				if anycompatibleMultirangeTypeid != actualType {
					return false
				}
			} else {
				anycompatibleMultirangeTypeid = actualType
				anycompatibleMultirangeTypelem = getMultirangeRange(actualType)
				if anycompatibleMultirangeTypelem == pgoid.InvalidOid {
					return false // not a multirange type
				}
				// we'll consider the subtype below
			}
		}
	}

	// Get the element type based on the array type, if we have one.
	if arrayTypeid != pgoid.InvalidOid {
		if arrayTypeid == pgoid.ANYARRAYOID {
			// Special case for matching ANYARRAY input to an ANYARRAY
			// argument: allow it for now. enforceGenericTypeConsistency
			// will complain later if it's wrong.
		} else {
			arrayTypelem := getElementType(arrayTypeid)
			if arrayTypelem == pgoid.InvalidOid {
				return false // should be an array, but isn't
			}
			if elemTypeid == pgoid.InvalidOid {
				// if we don't have an element type yet, use the one we just
				// got
				elemTypeid = arrayTypelem
			} else if arrayTypelem != elemTypeid {
				// otherwise, they better match
				return false
			}
		}
	}

	// Deduce range type from multirange type, or check that they agree.
	if multirangeTypeid != pgoid.InvalidOid {
		multirangeTypelem := getMultirangeRange(multirangeTypeid)
		if multirangeTypelem == pgoid.InvalidOid {
			return false // should be a multirange, but isn't
		}
		if rangeTypeid == pgoid.InvalidOid {
			// If we don't have a range type yet, use the one we just got.
			rangeTypeid = multirangeTypelem
			rangeTypelem = getRangeSubtype(multirangeTypelem)
			if rangeTypelem == pgoid.InvalidOid {
				return false // should be a range, but isn't
			}
		} else if multirangeTypelem != rangeTypeid {
			// otherwise, they better match
			return false
		}
	}

	// Get the element type based on the range type, if we have one.
	if rangeTypeid != pgoid.InvalidOid {
		rangeTypelem = getRangeSubtype(rangeTypeid)
		if rangeTypelem == pgoid.InvalidOid {
			return false // should be a range, but isn't
		}
		if elemTypeid == pgoid.InvalidOid {
			// if we don't have an element type yet, use the one we just got
			elemTypeid = rangeTypelem
		} else if rangeTypelem != elemTypeid {
			// otherwise, they better match
			return false
		}
	}

	if haveAnynonarray {
		// require the element type to not be an array or domain over array
		if typeIsArrayDomain(elemTypeid) {
			return false
		}
	}
	if haveAnyenum {
		// require the element type to be an enum
		if !typeIsEnum(elemTypeid) {
			return false
		}
	}

	// Deduce range type from multirange type, or check that they agree.
	if anycompatibleMultirangeTypeid != pgoid.InvalidOid {
		if anycompatibleRangeTypeid != pgoid.InvalidOid {
			if anycompatibleMultirangeTypelem != anycompatibleRangeTypeid {
				return false
			}
		} else {
			anycompatibleRangeTypeid = anycompatibleMultirangeTypelem
			anycompatibleRangeTypelem = getRangeSubtype(anycompatibleRangeTypeid)
			if anycompatibleRangeTypelem == pgoid.InvalidOid {
				return false // not a range type
			}
			// collect the subtype for common-supertype choice
			anycompatibleActualTypes = append(anycompatibleActualTypes, anycompatibleRangeTypelem)
		}
	}

	// Check matching of ANYCOMPATIBLE-family arguments, if any.
	if len(anycompatibleActualTypes) > 0 {
		anycompatibleTypeid := selectCommonTypeFromOids(anycompatibleActualTypes, true)
		if anycompatibleTypeid == pgoid.InvalidOid {
			return false // there's definitely no common supertype
		}
		// We have to verify that the selected type actually works.
		if !verifyCommonTypeFromOids(anycompatibleTypeid, anycompatibleActualTypes) {
			return false
		}
		if haveAnycompatibleNonarray {
			// require the anycompatible type to not be an array or domain
			// over array
			if typeIsArrayDomain(anycompatibleTypeid) {
				return false
			}
		}
		// the anycompatible type must exactly match the range element type
		if anycompatibleRangeTypelem != pgoid.InvalidOid && anycompatibleRangeTypelem != anycompatibleTypeid {
			return false
		}
	}

	// Looks valid
	return true
}

// notAllAlike raises the "arguments declared ... are not all alike" error.
func notAllAlike(declared string, a, b Oid) {
	ereportDetail(mterrors.PgSSDatatypeMismatch, formatTypeBe(a)+" versus "+formatTypeBe(b),
		"arguments declared \"%s\" are not all alike", declared)
}

// enforceGenericTypeConsistency resolves a polymorphic signature against the
// actual argument types, rewriting declaredArgTypes in place for the
// positions that need an actual cast target, and returns the actual result
// type - parse_coerce.c:2133.
//
// The argument consistency rules are:
//
//  1. All arguments declared ANYELEMENT must have the same datatype.
//  2. All arguments declared ANYARRAY must have the same datatype, which
//     must be a varlena array type.
//  3. All arguments declared ANYRANGE must be the same range type.
//     Similarly, all arguments declared ANYMULTIRANGE must be the same
//     multirange type; and if both of these appear, the ANYRANGE type must
//     be the element type of the ANYMULTIRANGE type.
//  4. If there are arguments of more than one of these polymorphic types,
//     the array element type and/or range subtype must be the same as each
//     other and the same as the ANYELEMENT type.
//  5. ANYENUM is treated the same as ANYELEMENT except that if it is used
//     (alone or in combination with plain ANYELEMENT), we add the extra
//     condition that the ANYELEMENT type must be an enum.
//  6. ANYNONARRAY is treated the same as ANYELEMENT except that if it is
//     used, we add the extra condition that the ANYELEMENT type must not be
//     an array.
//  7. All arguments declared ANYCOMPATIBLE must be implicitly castable to a
//     common supertype (chosen as per selectCommonType's rules).
//     ANYCOMPATIBLENONARRAY works like ANYCOMPATIBLE but also requires the
//     common supertype to not be an array. If there are ANYCOMPATIBLEARRAY
//     or ANYCOMPATIBLERANGE or ANYCOMPATIBLEMULTIRANGE arguments, their
//     element types or subtypes are included while making the choice of
//     common supertype.
//  8. The resolved type of ANYCOMPATIBLEARRAY arguments will be the array
//     type over the common supertype.
//  9. All ANYCOMPATIBLERANGE arguments must be the exact same range type
//     (after domain flattening), and that range's subtype must exactly
//     match the common supertype chosen by rule 7.
//  10. All ANYCOMPATIBLEMULTIRANGE arguments must be the exact same
//     multirange type (after domain flattening); its range type must match
//     ANYCOMPATIBLERANGE if that also appears, else its subtype must match
//     the common supertype chosen by rule 7.
//
// If we have UNKNOWN input (ie, an untyped literal) for any polymorphic
// argument, we deduce the type from the other inputs and coerce the
// unknown to that; with allowPoly false, failing to deduce one is an error.
func enforceGenericTypeConsistency(actualArgTypes []Oid, declaredArgTypes []Oid, rettype Oid, allowPoly bool) Oid {
	var (
		havePolyAnycompatible, havePolyUnknowns                       bool
		elemTypeid, arrayTypeid, rangeTypeid, multirangeTypeid        Oid
		anycompatibleTypeid, anycompatibleArrayTypeid                 Oid
		anycompatibleRangeTypeid, anycompatibleRangeTypelem           Oid
		anycompatibleMultirangeTypeid, anycompatibleMultirangeTypelem Oid
		nPolyArgs                                                     int // this counts all family-1 arguments
		anycompatibleActualTypes                                      []Oid
	)
	haveAnynonarray := rettype == pgoid.ANYNONARRAYOID
	haveAnyenum := rettype == pgoid.ANYENUMOID
	haveAnymultirange := rettype == pgoid.ANYMULTIRANGEOID
	haveAnycompatibleNonarray := rettype == pgoid.ANYCOMPATIBLENONARRAYOID
	haveAnycompatibleArray := rettype == pgoid.ANYCOMPATIBLEARRAYOID
	haveAnycompatibleRange := rettype == pgoid.ANYCOMPATIBLERANGEOID
	haveAnycompatibleMultirange := rettype == pgoid.ANYCOMPATIBLEMULTIRANGEOID

	// Loop through the arguments to see if we have any that are polymorphic.
	// If so, require the actual types to be consistent.
	for j := range actualArgTypes {
		declType, actualType := declaredArgTypes[j], actualArgTypes[j]
		switch declType {
		case pgoid.ANYELEMENTOID, pgoid.ANYNONARRAYOID, pgoid.ANYENUMOID:
			nPolyArgs++
			switch declType {
			case pgoid.ANYNONARRAYOID:
				haveAnynonarray = true
			case pgoid.ANYENUMOID:
				haveAnyenum = true
			}
			if actualType == pgoid.UNKNOWNOID {
				havePolyUnknowns = true
				continue
			}
			if allowPoly && declType == actualType {
				continue // no new information here
			}
			if elemTypeid != pgoid.InvalidOid && actualType != elemTypeid {
				notAllAlike("anyelement", elemTypeid, actualType)
			}
			elemTypeid = actualType
		case pgoid.ANYARRAYOID:
			nPolyArgs++
			if actualType == pgoid.UNKNOWNOID {
				havePolyUnknowns = true
				continue
			}
			if allowPoly && declType == actualType {
				continue // no new information here
			}
			actualType = getBaseType(actualType) // flatten domains
			if arrayTypeid != pgoid.InvalidOid && actualType != arrayTypeid {
				notAllAlike("anyarray", arrayTypeid, actualType)
			}
			arrayTypeid = actualType
		case pgoid.ANYRANGEOID:
			nPolyArgs++
			if actualType == pgoid.UNKNOWNOID {
				havePolyUnknowns = true
				continue
			}
			if allowPoly && declType == actualType {
				continue // no new information here
			}
			actualType = getBaseType(actualType) // flatten domains
			if rangeTypeid != pgoid.InvalidOid && actualType != rangeTypeid {
				notAllAlike("anyrange", rangeTypeid, actualType)
			}
			rangeTypeid = actualType
		case pgoid.ANYMULTIRANGEOID:
			nPolyArgs++
			haveAnymultirange = true
			if actualType == pgoid.UNKNOWNOID {
				havePolyUnknowns = true
				continue
			}
			if allowPoly && declType == actualType {
				continue // no new information here
			}
			actualType = getBaseType(actualType) // flatten domains
			if multirangeTypeid != pgoid.InvalidOid && actualType != multirangeTypeid {
				notAllAlike("anymultirange", multirangeTypeid, actualType)
			}
			multirangeTypeid = actualType
		case pgoid.ANYCOMPATIBLEOID, pgoid.ANYCOMPATIBLENONARRAYOID:
			havePolyAnycompatible = true
			if declType == pgoid.ANYCOMPATIBLENONARRAYOID {
				haveAnycompatibleNonarray = true
			}
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			if allowPoly && declType == actualType {
				continue // no new information here
			}
			anycompatibleActualTypes = append(anycompatibleActualTypes, actualType)
		case pgoid.ANYCOMPATIBLEARRAYOID:
			havePolyAnycompatible = true
			haveAnycompatibleArray = true
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			if allowPoly && declType == actualType {
				continue // no new information here
			}
			actualType = getBaseType(actualType) // flatten domains
			anycompatibleElemType := getElementType(actualType)
			if anycompatibleElemType == pgoid.InvalidOid {
				ereport(mterrors.PgSSDatatypeMismatch, "argument declared %s is not an array but type %s",
					"anycompatiblearray", formatTypeBe(actualType))
			}
			anycompatibleActualTypes = append(anycompatibleActualTypes, anycompatibleElemType)
		case pgoid.ANYCOMPATIBLERANGEOID:
			havePolyAnycompatible = true
			haveAnycompatibleRange = true
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			if allowPoly && declType == actualType {
				continue // no new information here
			}
			actualType = getBaseType(actualType) // flatten domains
			if anycompatibleRangeTypeid != pgoid.InvalidOid {
				if anycompatibleRangeTypeid != actualType {
					notAllAlike("anycompatiblerange", anycompatibleRangeTypeid, actualType)
				}
			} else {
				anycompatibleRangeTypeid = actualType
				anycompatibleRangeTypelem = getRangeSubtype(actualType)
				if anycompatibleRangeTypelem == pgoid.InvalidOid {
					ereport(mterrors.PgSSDatatypeMismatch, "argument declared %s is not a range type but type %s",
						"anycompatiblerange", formatTypeBe(actualType))
				}
				anycompatibleActualTypes = append(anycompatibleActualTypes, anycompatibleRangeTypelem)
			}
		case pgoid.ANYCOMPATIBLEMULTIRANGEOID:
			havePolyAnycompatible = true
			haveAnycompatibleMultirange = true
			if actualType == pgoid.UNKNOWNOID {
				continue
			}
			if allowPoly && declType == actualType {
				continue // no new information here
			}
			actualType = getBaseType(actualType) // flatten domains
			if anycompatibleMultirangeTypeid != pgoid.InvalidOid {
				if anycompatibleMultirangeTypeid != actualType {
					notAllAlike("anycompatiblemultirange", anycompatibleMultirangeTypeid, actualType)
				}
			} else {
				anycompatibleMultirangeTypeid = actualType
				anycompatibleMultirangeTypelem = getMultirangeRange(actualType)
				if anycompatibleMultirangeTypelem == pgoid.InvalidOid {
					ereport(mterrors.PgSSDatatypeMismatch, "argument declared %s is not a multirange type but type %s",
						"anycompatiblemultirange", formatTypeBe(actualType))
				}
			}
		}
	}

	// Fast Track: if none of the arguments are polymorphic, return the
	// unmodified rettype. Not our responsibility to resolve it if it's
	// polymorphic.
	if nPolyArgs == 0 && !havePolyAnycompatible {
		return rettype
	}

	// Check matching of family-1 polymorphic arguments, if any.
	if nPolyArgs > 0 {
		// Get the element type based on the array type, if we have one.
		if arrayTypeid != pgoid.InvalidOid {
			var arrayTypelem Oid
			if arrayTypeid == pgoid.ANYARRAYOID {
				// Special case for matching ANYARRAY input to an ANYARRAY
				// argument: allow it iff no other polymorphic arguments or
				// results are present (otherwise there'd be no way to
				// identify the element type).
				if nPolyArgs != 1 || (rettype != pgoid.ANYARRAYOID && isPolymorphicTypeFamily1(rettype)) {
					ereport(mterrors.PgSSDatatypeMismatch, "cannot determine element type of \"anyarray\" argument")
				}
				arrayTypelem = pgoid.ANYELEMENTOID
			} else {
				arrayTypelem = getElementType(arrayTypeid)
				if arrayTypelem == pgoid.InvalidOid {
					ereport(mterrors.PgSSDatatypeMismatch, "argument declared %s is not an array but type %s",
						"anyarray", formatTypeBe(arrayTypeid))
				}
			}
			if elemTypeid == pgoid.InvalidOid {
				// if we don't have an element type yet, use the one we just
				// got
				elemTypeid = arrayTypelem
			} else if arrayTypelem != elemTypeid {
				// otherwise, they better match
				ereportDetail(mterrors.PgSSDatatypeMismatch,
					formatTypeBe(arrayTypeid)+" versus "+formatTypeBe(elemTypeid),
					"argument declared %s is not consistent with argument declared %s",
					"anyarray", "anyelement")
			}
		}

		// Deduce range type from multirange type, or check that they agree.
		if multirangeTypeid != pgoid.InvalidOid {
			multirangeTypelem := getMultirangeRange(multirangeTypeid)
			if multirangeTypelem == pgoid.InvalidOid {
				ereport(mterrors.PgSSDatatypeMismatch, "argument declared %s is not a multirange type but type %s",
					"anymultirange", formatTypeBe(multirangeTypeid))
			}
			if rangeTypeid == pgoid.InvalidOid {
				// if we don't have a range type yet, use the one we just got
				rangeTypeid = multirangeTypelem
			} else if multirangeTypelem != rangeTypeid {
				// otherwise, they better match
				ereportDetail(mterrors.PgSSDatatypeMismatch,
					formatTypeBe(multirangeTypeid)+" versus "+formatTypeBe(rangeTypeid),
					"argument declared %s is not consistent with argument declared %s",
					"anymultirange", "anyrange")
			}
		} else if haveAnymultirange && rangeTypeid != pgoid.InvalidOid {
			multirangeTypeid = getRangeMultirange(rangeTypeid)
			// We'll complain below if that didn't work.
		}

		// Get the element type based on the range type, if we have one.
		if rangeTypeid != pgoid.InvalidOid {
			rangeTypelem := getRangeSubtype(rangeTypeid)
			if rangeTypelem == pgoid.InvalidOid {
				ereport(mterrors.PgSSDatatypeMismatch, "argument declared %s is not a range type but type %s",
					"anyrange", formatTypeBe(rangeTypeid))
			}
			if elemTypeid == pgoid.InvalidOid {
				// if we don't have an element type yet, use the one we just
				// got
				elemTypeid = rangeTypelem
			} else if rangeTypelem != elemTypeid {
				// otherwise, they better match
				ereportDetail(mterrors.PgSSDatatypeMismatch,
					formatTypeBe(rangeTypeid)+" versus "+formatTypeBe(elemTypeid),
					"argument declared %s is not consistent with argument declared %s",
					"anyrange", "anyelement")
			}
		}

		if elemTypeid == pgoid.InvalidOid {
			if allowPoly {
				elemTypeid = pgoid.ANYELEMENTOID
				arrayTypeid = pgoid.ANYARRAYOID
				rangeTypeid = pgoid.ANYRANGEOID
				multirangeTypeid = pgoid.ANYMULTIRANGEOID
			} else {
				// Only way to get here is if all the family-1 polymorphic
				// arguments have UNKNOWN inputs.
				ereport(mterrors.PgSSDatatypeMismatch,
					"could not determine polymorphic type because input has type %s", "unknown")
			}
		}

		if haveAnynonarray && elemTypeid != pgoid.ANYELEMENTOID {
			// require the element type to not be an array or domain over
			// array
			if typeIsArrayDomain(elemTypeid) {
				ereport(mterrors.PgSSDatatypeMismatch, "type matched to anynonarray is an array type: %s",
					formatTypeBe(elemTypeid))
			}
		}
		if haveAnyenum && elemTypeid != pgoid.ANYELEMENTOID {
			// require the element type to be an enum
			if !typeIsEnum(elemTypeid) {
				ereport(mterrors.PgSSDatatypeMismatch, "type matched to anyenum is not an enum type: %s",
					formatTypeBe(elemTypeid))
			}
		}
	}

	// Check matching of family-2 polymorphic arguments, if any.
	if havePolyAnycompatible {
		// Deduce range type from multirange type, or check that they agree.
		if anycompatibleMultirangeTypeid != pgoid.InvalidOid {
			if anycompatibleRangeTypeid != pgoid.InvalidOid {
				if anycompatibleMultirangeTypelem != anycompatibleRangeTypeid {
					ereportDetail(mterrors.PgSSDatatypeMismatch,
						formatTypeBe(anycompatibleMultirangeTypeid)+" versus "+formatTypeBe(anycompatibleRangeTypeid),
						"argument declared %s is not consistent with argument declared %s",
						"anycompatiblemultirange", "anycompatiblerange")
				}
			} else {
				anycompatibleRangeTypeid = anycompatibleMultirangeTypelem
				anycompatibleRangeTypelem = getRangeSubtype(anycompatibleRangeTypeid)
				if anycompatibleRangeTypelem == pgoid.InvalidOid {
					ereport(mterrors.PgSSDatatypeMismatch, "argument declared %s is not a multirange type but type %s",
						"anycompatiblemultirange", formatTypeBe(anycompatibleMultirangeTypeid))
				}
				// this enables element type matching check below
				haveAnycompatibleRange = true
				// collect the subtype for common-supertype choice
				anycompatibleActualTypes = append(anycompatibleActualTypes, anycompatibleRangeTypelem)
			}
		} else if haveAnycompatibleMultirange && anycompatibleRangeTypeid != pgoid.InvalidOid {
			anycompatibleMultirangeTypeid = getRangeMultirange(anycompatibleRangeTypeid)
			// We'll complain below if that didn't work.
		}

		if len(anycompatibleActualTypes) > 0 {
			anycompatibleTypeid = selectCommonTypeFromOids(anycompatibleActualTypes, false)

			// We have to verify that the selected type actually works.
			if !verifyCommonTypeFromOids(anycompatibleTypeid, anycompatibleActualTypes) {
				ereport(mterrors.PgSSDatatypeMismatch, "arguments of anycompatible family cannot be cast to a common type")
			}

			if haveAnycompatibleArray {
				anycompatibleArrayTypeid = getArrayType(anycompatibleTypeid)
				if anycompatibleArrayTypeid == pgoid.InvalidOid {
					ereport(mterrors.PgSSUndefinedObject, "could not find array type for data type %s",
						formatTypeBe(anycompatibleTypeid))
				}
			}

			if haveAnycompatibleRange {
				// we can't infer a range type from the others
				if anycompatibleRangeTypeid == pgoid.InvalidOid {
					ereport(mterrors.PgSSDatatypeMismatch,
						"could not determine polymorphic type %s because input has type %s",
						"anycompatiblerange", "unknown")
				}
				// the anycompatible type must exactly match the range
				// element type
				if anycompatibleRangeTypelem != anycompatibleTypeid {
					ereport(mterrors.PgSSDatatypeMismatch, "anycompatiblerange type %s does not match anycompatible type %s",
						formatTypeBe(anycompatibleRangeTypeid), formatTypeBe(anycompatibleTypeid))
				}
			}

			if haveAnycompatibleMultirange {
				// we can't infer a multirange type from the others
				if anycompatibleMultirangeTypeid == pgoid.InvalidOid {
					ereport(mterrors.PgSSDatatypeMismatch,
						"could not determine polymorphic type %s because input has type %s",
						"anycompatiblemultirange", "unknown")
				}
				// the anycompatible type must exactly match the multirange
				// element type
				if anycompatibleRangeTypelem != anycompatibleTypeid {
					ereport(mterrors.PgSSDatatypeMismatch, "anycompatiblemultirange type %s does not match anycompatible type %s",
						formatTypeBe(anycompatibleMultirangeTypeid), formatTypeBe(anycompatibleTypeid))
				}
			}

			if haveAnycompatibleNonarray {
				// require the element type to not be an array or domain
				// over array
				if typeIsArrayDomain(anycompatibleTypeid) {
					ereport(mterrors.PgSSDatatypeMismatch, "type matched to anycompatiblenonarray is an array type: %s",
						formatTypeBe(anycompatibleTypeid))
				}
			}
		} else {
			if allowPoly {
				anycompatibleTypeid = pgoid.ANYCOMPATIBLEOID
				anycompatibleArrayTypeid = pgoid.ANYCOMPATIBLEARRAYOID
				anycompatibleRangeTypeid = pgoid.ANYCOMPATIBLERANGEOID
				anycompatibleMultirangeTypeid = pgoid.ANYCOMPATIBLEMULTIRANGEOID
			} else {
				// Only way to get here is if all the family-2 polymorphic
				// arguments have UNKNOWN inputs. Resolve to TEXT as
				// selectCommonType would do. That doesn't license us to
				// use TEXTRANGE or TEXTMULTIRANGE, though.
				anycompatibleTypeid = pgoid.TEXTOID
				anycompatibleArrayTypeid = pgoid.TEXTARRAYOID
				if haveAnycompatibleRange {
					ereport(mterrors.PgSSDatatypeMismatch,
						"could not determine polymorphic type %s because input has type %s",
						"anycompatiblerange", "unknown")
				}
				if haveAnycompatibleMultirange {
					ereport(mterrors.PgSSDatatypeMismatch,
						"could not determine polymorphic type %s because input has type %s",
						"anycompatiblemultirange", "unknown")
				}
			}
		}

		// replace family-2 polymorphic types by selected types
		for j := range declaredArgTypes {
			switch declaredArgTypes[j] {
			case pgoid.ANYCOMPATIBLEOID, pgoid.ANYCOMPATIBLENONARRAYOID:
				declaredArgTypes[j] = anycompatibleTypeid
			case pgoid.ANYCOMPATIBLEARRAYOID:
				declaredArgTypes[j] = anycompatibleArrayTypeid
			case pgoid.ANYCOMPATIBLERANGEOID:
				declaredArgTypes[j] = anycompatibleRangeTypeid
			case pgoid.ANYCOMPATIBLEMULTIRANGEOID:
				declaredArgTypes[j] = anycompatibleMultirangeTypeid
			}
		}
	}

	// If we had any UNKNOWN inputs for family-1 polymorphic arguments,
	// re-scan to assign correct types to them.
	//
	// Note: we don't have to consider unknown inputs that were matched to
	// family-2 polymorphic arguments, because we forcibly updated their
	// declaredArgTypes[] positions just above.
	if havePolyUnknowns {
		for j := range actualArgTypes {
			if actualArgTypes[j] != pgoid.UNKNOWNOID {
				continue
			}
			switch declaredArgTypes[j] {
			case pgoid.ANYELEMENTOID, pgoid.ANYNONARRAYOID, pgoid.ANYENUMOID:
				declaredArgTypes[j] = elemTypeid
			case pgoid.ANYARRAYOID:
				if arrayTypeid == pgoid.InvalidOid {
					arrayTypeid = getArrayType(elemTypeid)
					if arrayTypeid == pgoid.InvalidOid {
						ereport(mterrors.PgSSUndefinedObject, "could not find array type for data type %s",
							formatTypeBe(elemTypeid))
					}
				}
				declaredArgTypes[j] = arrayTypeid
			case pgoid.ANYRANGEOID:
				if rangeTypeid == pgoid.InvalidOid {
					ereport(mterrors.PgSSDatatypeMismatch,
						"could not determine polymorphic type %s because input has type %s",
						"anyrange", "unknown")
				}
				declaredArgTypes[j] = rangeTypeid
			case pgoid.ANYMULTIRANGEOID:
				if multirangeTypeid == pgoid.InvalidOid {
					ereport(mterrors.PgSSDatatypeMismatch,
						"could not determine polymorphic type %s because input has type %s",
						"anymultirange", "unknown")
				}
				declaredArgTypes[j] = multirangeTypeid
			}
		}
	}

	// Identify the collection type to return based on the resolved element
	// type.
	switch rettype {
	case pgoid.ANYELEMENTOID, pgoid.ANYNONARRAYOID, pgoid.ANYENUMOID:
		return elemTypeid
	case pgoid.ANYARRAYOID:
		if arrayTypeid == pgoid.InvalidOid {
			arrayTypeid = getArrayType(elemTypeid)
			if arrayTypeid == pgoid.InvalidOid {
				ereport(mterrors.PgSSUndefinedObject, "could not find array type for data type %s",
					formatTypeBe(elemTypeid))
			}
		}
		return arrayTypeid
	case pgoid.ANYRANGEOID:
		if rangeTypeid == pgoid.InvalidOid {
			ereport(mterrors.PgSSDatatypeMismatch,
				"could not determine polymorphic type %s because input has type %s", "anyrange", "unknown")
		}
		return rangeTypeid
	case pgoid.ANYMULTIRANGEOID:
		if multirangeTypeid == pgoid.InvalidOid {
			ereport(mterrors.PgSSDatatypeMismatch,
				"could not determine polymorphic type %s because input has type %s", "anymultirange", "unknown")
		}
		return multirangeTypeid
	case pgoid.ANYCOMPATIBLEOID, pgoid.ANYCOMPATIBLENONARRAYOID:
		if anycompatibleTypeid == pgoid.InvalidOid {
			ereport(mterrors.PgSSDatatypeMismatch, "could not identify anycompatible type")
		}
		return anycompatibleTypeid
	case pgoid.ANYCOMPATIBLEARRAYOID:
		if anycompatibleArrayTypeid == pgoid.InvalidOid {
			ereport(mterrors.PgSSDatatypeMismatch, "could not identify anycompatiblearray type")
		}
		return anycompatibleArrayTypeid
	case pgoid.ANYCOMPATIBLERANGEOID:
		if anycompatibleRangeTypeid == pgoid.InvalidOid {
			ereport(mterrors.PgSSDatatypeMismatch, "could not identify anycompatiblerange type")
		}
		return anycompatibleRangeTypeid
	case pgoid.ANYCOMPATIBLEMULTIRANGEOID:
		if anycompatibleMultirangeTypeid == pgoid.InvalidOid {
			ereport(mterrors.PgSSDatatypeMismatch, "could not identify anycompatiblemultirange type")
		}
		return anycompatibleMultirangeTypeid
	}
	// we don't have a special case for non-polymorphic return types.
	return rettype
}

// typeCategory returns the category of a type - parse_coerce.c:2978.
func typeCategory(typ Oid) byte {
	category, _ := getTypeCategoryPreferred(typ)
	if category == typcategoryInvalid {
		pgerror.Elogf("type %d has no category", typ)
	}
	return category
}

// isPreferredType reports whether typ is a preferred type of the given
// category, or of any category if category is invalid - parse_coerce.c:2997.
func isPreferredType(category byte, typ Oid) bool {
	typcategory, typispreferred := getTypeCategoryPreferred(typ)
	if category == typcategory || category == typcategoryInvalid {
		return typispreferred
	}
	return false
}

// findCoercionPathway looks for a coercion pathway between two scalar types
// in the given context - parse_coerce.c:3155. The function OID is set only
// for coercionPathFunc.
func findCoercionPathway(targetTypeId, sourceTypeId Oid, ccontext ast.CoercionContext) (coercionPathType, Oid) {
	result := coercionPathNone
	var funcid Oid

	// Perhaps the types are domains; if so, look at their base types.
	if sourceTypeId != pgoid.InvalidOid {
		sourceTypeId = getBaseType(sourceTypeId)
	}
	if targetTypeId != pgoid.InvalidOid {
		targetTypeId = getBaseType(targetTypeId)
	}
	// Domains are always coercible to and from their base type.
	if sourceTypeId == targetTypeId {
		return coercionPathRelabelType, pgoid.InvalidOid
	}

	// Look in pg_cast.
	if cast := pgcatalog.LookupCast(sourceTypeId, targetTypeId); cast != nil {
		var castcontext ast.CoercionContext
		switch cast.Context {
		case pgcatalog.CastContextImplicit:
			castcontext = ast.COERCION_IMPLICIT
		case pgcatalog.CastContextAssignment:
			castcontext = ast.COERCION_ASSIGNMENT
		case pgcatalog.CastContextExplicit:
			castcontext = ast.COERCION_EXPLICIT
		default:
			pgerror.Elogf("unrecognized castcontext: %d", int(cast.Context))
		}
		// Rely on ordering of enum for correct behavior here.
		if ccontext >= castcontext {
			switch cast.Method {
			case pgcatalog.CastMethodFunction:
				result = coercionPathFunc
				funcid = cast.Func
			case pgcatalog.CastMethodInOut:
				result = coercionPathCoerceViaIO
			case pgcatalog.CastMethodBinary:
				result = coercionPathRelabelType
			default:
				pgerror.Elogf("unrecognized castmethod: %d", int(cast.Method))
			}
		}
	} else {
		// If there's no pg_cast entry, perhaps we are dealing with a pair of
		// array types. If so, and if their element types have a conversion
		// pathway, report that we can coerce with an ArrayCoerceExpr.
		//
		// Hack: disallow coercions to oidvector and int2vector, which
		// otherwise tend to capture coercions that should go to "real" array
		// types. We want those types to be considered "real" arrays for many
		// purposes, but not this one.
		if targetTypeId != pgoid.OIDVECTOROID && targetTypeId != pgoid.INT2VECTOROID {
			if targetElem := getElementType(targetTypeId); targetElem != pgoid.InvalidOid {
				if sourceElem := getElementType(sourceTypeId); sourceElem != pgoid.InvalidOid {
					if elempathtype, _ := findCoercionPathway(targetElem, sourceElem, ccontext); elempathtype != coercionPathNone {
						result = coercionPathArrayCoerce
					}
				}
			}
		}

		// If we still haven't found a possibility, consider automatic
		// casting using I/O functions. We allow assignment casts to string
		// types and explicit casts from string types to be handled this way.
		// (The CoerceViaIO mechanism is a lot more general than that, but
		// this is all we want to allow in the absence of a pg_cast entry.)
		if result == coercionPathNone {
			if ccontext >= ast.COERCION_ASSIGNMENT && typeCategory(targetTypeId) == typcategoryString {
				result = coercionPathCoerceViaIO
			} else if ccontext >= ast.COERCION_EXPLICIT && typeCategory(sourceTypeId) == typcategoryString {
				result = coercionPathCoerceViaIO
			}
		}
	}

	// When parsing PL/pgSQL assignments, allow an I/O cast to be used
	// whenever no normal coercion is available.
	if result == coercionPathNone && ccontext == ast.COERCION_PLPGSQL {
		result = coercionPathCoerceViaIO
	}
	return result, funcid
}

// findTypmodCoercionFunction reports whether the type needs a length
// coercion (a pg_cast from itself to itself) and which function applies it -
// parse_coerce.c:3318. For a true array type the element type's function is
// used through an ArrayCoerceExpr.
func findTypmodCoercionFunction(typeId Oid) (coercionPathType, Oid) {
	result := coercionPathFunc
	// Check for a "true" array type.
	if elem := getElementType(typeId); elem != pgoid.InvalidOid {
		// Yes, switch our attention to the element type.
		typeId = elem
		result = coercionPathArrayCoerce
	}
	// Look in pg_cast.
	var funcid Oid
	if cast := pgcatalog.LookupCast(typeId, typeId); cast != nil {
		funcid = cast.Func
	}
	if funcid == pgoid.InvalidOid {
		result = coercionPathNone
	}
	return result, funcid
}
