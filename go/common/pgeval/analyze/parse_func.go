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
	"slices"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// This file ports parse_func.c (function-call resolution and the shared
// candidate-selection heuristics) together with FuncnameGetCandidates from
// catalog/namespace.c.
//
// Two pg_proc facets are out of reach here. Named-argument notation needs
// proargnames, and a call that relies on parameter defaults needs the
// proargdefaults expressions; neither is in the compiled-in catalog. Such
// calls are recognized and reported as unsupported rather than resolved to
// the wrong overload.

// funcnameGetCandidates lists the functions of the given name that could
// accept nargs positional arguments, expanding a variadic parameter and
// counting defaults as upstream does - namespace.c:1192.
func funcnameGetCandidates(names *ast.NodeList, nargs int, expandVariadic, expandDefaults bool) []*funcCandidate {
	schemaname, funcname := deconstructQualifiedName(names)
	requireBuiltinSchema(schemaname)

	var resultList []*funcCandidate
	anySpecial := false
	for _, proc := range pgcatalog.ProcsByName(funcname) {
		pronargs := len(proc.ArgTypes)

		// Call uses positional notation.
		//
		// Check if function is variadic, and get variadic element type if
		// so. If expandVariadic is false, we should just ignore
		// variadic-ness.
		vaElemType := pgoid.InvalidOid
		variadic := false
		if pronargs <= nargs && expandVariadic {
			vaElemType = proc.Variadic
			variadic = vaElemType != pgoid.InvalidOid
			anySpecial = anySpecial || variadic
		}

		// Check if function can match by using parameter defaults.
		useDefaults := false
		if pronargs > nargs && expandDefaults {
			// Ignore if not enough default expressions.
			if nargs+int(proc.NArgDefaults) < pronargs {
				continue
			}
			useDefaults = true
			anySpecial = true
		}

		// Ignore if it doesn't match requested argument count.
		if nargs >= 0 && pronargs != nargs && !variadic && !useDefaults {
			continue
		}

		// We must compute the effective argument list so that we can easily
		// compare it to earlier results.
		effectiveNargs := max(pronargs, nargs)
		newResult := &funcCandidate{
			oid:          proc.Oid,
			nominalnargs: pronargs,
			nargs:        effectiveNargs,
			args:         make([]Oid, effectiveNargs),
		}
		copy(newResult.args, proc.ArgTypes)
		if variadic {
			newResult.nvargs = effectiveNargs - pronargs + 1
			// Expand variadic argument into N copies of element type.
			for j := pronargs - 1; j < effectiveNargs; j++ {
				newResult.args[j] = vaElemType
			}
		}
		if useDefaults {
			newResult.ndargs = pronargs - nargs
		}

		// Does it have the same arguments as something we already accepted?
		// If so, decide what to do to avoid returning duplicate argument
		// lists. We can skip this check if no special (variadic or defaults)
		// match has been made, since then the unique index on pg_proc
		// guarantees all the matches have different argument lists.
		// (Defaulted arguments are ignored in deciding what is a match.)
		if anySpecial {
			cmpNargs := newResult.nargs - newResult.ndargs
			prev := slices.IndexFunc(resultList, func(c *funcCandidate) bool {
				return cmpNargs == c.nargs-c.ndargs && slices.Equal(newResult.args[:cmpNargs], c.args[:cmpNargs])
			})
			if prev >= 0 {
				// We have a match with a previous result. Decide which one
				// to keep, or mark it ambiguous if we can't decide. The
				// logic here is preference > 0 means prefer the old result,
				// preference < 0 means prefer the new, preference = 0 means
				// ambiguous. (Every builtin is at the same search-path
				// position, so only the variadic rule applies.)
				prevResult := resultList[prev]
				preference := 0
				switch {
				case variadic && prevResult.nvargs == 0:
					// With variadic functions we could have, for example,
					// both foo(numeric) and foo(variadic numeric[]) in the
					// same namespace; if so we prefer the non-variadic
					// match on efficiency grounds.
					preference = 1
				case !variadic && prevResult.nvargs > 0:
					preference = -1
				}
				switch {
				case preference > 0:
					// keep previous result
					continue
				case preference < 0:
					// remove previous result from the list
					resultList = slices.Delete(resultList, prev, prev+1)
					// fall through to add newResult to list
				default:
					// mark old result as ambiguous, discard new
					prevResult.oid = pgoid.InvalidOid
					continue
				}
			}
		}

		// Okay to add it to result list.
		resultList = append(resultList, newResult)
	}
	return resultList
}

// funcMatchArgtypes shortlists the candidates that actually accept the
// input datatypes, exactly or by implicit coercion - parse_func.c:923.
// canCoerceType assumes that UNKNOWN inputs are coercible to anything, so
// candidates will not be eliminated on that basis.
func funcMatchArgtypes(inputTypeids []Oid, rawCandidates []*funcCandidate) []*funcCandidate {
	var candidates []*funcCandidate
	for _, c := range rawCandidates {
		if canCoerceType(inputTypeids, c.args, ast.COERCION_IMPLICIT) {
			candidates = append(candidates, c)
		}
	}
	return candidates
}

// funcSelectCandidate picks the best of several candidates none of which
// matches the input types exactly, or returns nil if the conflict cannot
// be resolved - parse_func.c:1008. The caller has already pruned any
// candidates that aren't actually coercion-compatible with the input
// types. The same heuristics resolve ambiguous operator references.
func funcSelectCandidate(inputTypeids []Oid, candidates []*funcCandidate) *funcCandidate {
	nargs := len(inputTypeids)
	// protect local fixed-size arrays
	if nargs > funcMaxArgs {
		tooManyArguments()
	}

	// If any input types are domains, reduce them to their base types. This
	// ensures that we will consider functions on the base type to be "exact
	// matches" in the exact-match heuristic; it also makes it possible to do
	// something useful with the type-category heuristics. Note that this
	// makes it difficult, but not impossible, to use functions declared to
	// take a domain as an input datatype. Such a function will be selected
	// over the base-type function only if it is an exact match at all
	// argument positions, and so was already chosen by our caller.
	//
	// While we're at it, count the number of unknown-type arguments for use
	// later.
	inputBaseTypeids := make([]Oid, nargs)
	nunknowns := 0
	for i, t := range inputTypeids {
		if t != pgoid.UNKNOWNOID {
			inputBaseTypeids[i] = getBaseType(t)
		} else {
			// no need to call getBaseType on UNKNOWNOID
			inputBaseTypeids[i] = pgoid.UNKNOWNOID
			nunknowns++
		}
	}

	// keepBest retains the candidates scoring highest under score; all are
	// kept when none scores.
	keepBest := func(score func(args []Oid) int) {
		var kept []*funcCandidate
		nbestMatch := 0
		for _, c := range candidates {
			nmatch := score(c.args)
			// take this one as the best choice so far?
			if nmatch > nbestMatch || kept == nil {
				nbestMatch = nmatch
				kept = []*funcCandidate{c}
			} else if nmatch == nbestMatch {
				// no worse than the last choice, so keep this one too?
				kept = append(kept, c)
			}
			// otherwise, don't bother keeping this one...
		}
		candidates = kept
	}

	// Run through all candidates and keep those with the most matches on
	// exact types. Keep all candidates if none match.
	keepBest(func(args []Oid) int {
		nmatch := 0
		for i := range nargs {
			if inputBaseTypeids[i] != pgoid.UNKNOWNOID && args[i] == inputBaseTypeids[i] {
				nmatch++
			}
		}
		return nmatch
	})
	if len(candidates) == 1 {
		return candidates[0]
	}

	// Still too many candidates? Now look for candidates which have either
	// exact matches or preferred types at the args that will require
	// coercion. (Restriction added in 7.4: preferred type must be of same
	// category as input type; give no preference to cross-category
	// conversions to preferred types.) Keep all candidates if none match.
	slotCategory := make([]byte, nargs)
	for i := range nargs { // avoid multiple lookups
		slotCategory[i] = typeCategory(inputBaseTypeids[i])
	}
	keepBest(func(args []Oid) int {
		nmatch := 0
		for i := range nargs {
			if inputBaseTypeids[i] != pgoid.UNKNOWNOID {
				if args[i] == inputBaseTypeids[i] || isPreferredType(slotCategory[i], args[i]) {
					nmatch++
				}
			}
		}
		return nmatch
	})
	if len(candidates) == 1 {
		return candidates[0]
	}

	// Still too many candidates? Try assigning types for the unknown
	// inputs.
	//
	// If there are no unknown inputs, we have no more heuristics that
	// apply, and must fail.
	if nunknowns == 0 {
		return nil // failed to select a best candidate
	}

	// The next step examines each unknown argument position to see if we
	// can determine a "type category" for it. If any candidate has an input
	// datatype of STRING category, use STRING category (this bias towards
	// STRING is appropriate since unknown-type literals look like strings).
	// Otherwise, if all the candidates agree on the type category of this
	// argument position, use that category. Otherwise, fail because we
	// cannot determine a category.
	//
	// If we are able to determine a type category, also notice whether any
	// of the candidates takes a preferred datatype within the category.
	//
	// Having completed this examination, remove candidates that accept the
	// wrong category at any unknown position. Also, if at least one
	// candidate accepted a preferred type at a position, remove candidates
	// that accept non-preferred types. If just one candidate remains,
	// return that one. However, if this rule turns out to reject all
	// candidates, keep them all instead.
	slotHasPreferredType := make([]bool, nargs)
	resolvedUnknowns := false
	for i := range nargs {
		if inputBaseTypeids[i] != pgoid.UNKNOWNOID {
			continue
		}
		resolvedUnknowns = true // assume we can do it
		slotCategory[i] = typcategoryInvalid
		slotHasPreferredType[i] = false
		haveConflict := false
		for _, c := range candidates {
			currentCategory, currentIsPreferred := getTypeCategoryPreferred(c.args[i])
			switch {
			case slotCategory[i] == typcategoryInvalid:
				// first candidate
				slotCategory[i] = currentCategory
				slotHasPreferredType[i] = currentIsPreferred
			case currentCategory == slotCategory[i]:
				// more candidates in same category
				slotHasPreferredType[i] = slotHasPreferredType[i] || currentIsPreferred
			case currentCategory == typcategoryString:
				// category conflict! STRING always wins if available
				slotCategory[i] = currentCategory
				slotHasPreferredType[i] = currentIsPreferred
			default:
				// Remember conflict, but keep going (might find STRING)
				haveConflict = true
			}
		}
		if haveConflict && slotCategory[i] != typcategoryString {
			// Failed to resolve category conflict at this position
			resolvedUnknowns = false
			break
		}
	}

	if resolvedUnknowns {
		// Strip non-matching candidates
		var kept []*funcCandidate
		for _, c := range candidates {
			keepit := true
			for i := range nargs {
				if inputBaseTypeids[i] != pgoid.UNKNOWNOID {
					continue
				}
				currentCategory, currentIsPreferred := getTypeCategoryPreferred(c.args[i])
				if currentCategory != slotCategory[i] || (slotHasPreferredType[i] && !currentIsPreferred) {
					keepit = false
					break
				}
			}
			if keepit {
				kept = append(kept, c)
			}
		}
		// if we found any matches, restrict our attention to those
		if len(kept) > 0 {
			candidates = kept
		}
		if len(kept) == 1 {
			return candidates[0]
		}
	}

	// Last gasp: if there are both known- and unknown-type inputs, and all
	// the known types are the same, assume the unknown inputs are also that
	// type, and see if that gives us a unique match. If so, use that match.
	//
	// NOTE: for a binary operator with one unknown and one non-unknown
	// input, we already tried this heuristic in binaryOperExact. However,
	// that code only finds exact matches, whereas here we will handle
	// matches that involve coercion, polymorphic type resolution, etc.
	if nunknowns < nargs {
		knownType := pgoid.UNKNOWNOID
		for i := range nargs {
			if inputBaseTypeids[i] == pgoid.UNKNOWNOID {
				continue
			}
			if knownType == pgoid.UNKNOWNOID { // first known arg?
				knownType = inputBaseTypeids[i]
			} else if knownType != inputBaseTypeids[i] {
				// oops, not all match
				knownType = pgoid.UNKNOWNOID
				break
			}
		}
		if knownType != pgoid.UNKNOWNOID {
			// okay, just one known type, apply the heuristic
			for i := range nargs {
				inputBaseTypeids[i] = knownType
			}
			var last *funcCandidate
			ncandidates := 0
			for _, c := range candidates {
				if canCoerceType(inputBaseTypeids, c.args, ast.COERCION_IMPLICIT) {
					ncandidates++
					if ncandidates > 1 {
						break // not unique, give up
					}
					last = c
				}
			}
			if ncandidates == 1 {
				// successfully identified a unique match
				return last
			}
		}
	}

	return nil // failed to select a best candidate
}

// tooManyArguments raises the FUNC_MAX_ARGS error - parse_func.c:1021.
func tooManyArguments() {
	ereport(mterrors.PgSSTooManyArguments, "cannot pass more than %d arguments to a function", funcMaxArgs)
}

// funcDetail is what funcGetDetail reports about the function it found -
// the output arguments of parse_func.c:1395.
type funcDetail struct {
	funcid      Oid
	rettype     Oid
	retset      bool
	nvargs      int
	vatype      Oid
	trueTypeids []Oid
}

// funcGetDetail finds the named function for the given argument types,
// trying an exact match first, then a type-coercion interpretation, then
// the ambiguous-function resolution rules - parse_func.c:1395. fargs are the
// transformed argument expressions, consulted only for the coercion check.
func funcGetDetail(funcname *ast.NodeList, fargs []ast.Node, argtypes []Oid,
	expandVariadic, expandDefaults bool,
) (funcDetailCode, funcDetail) {
	nargs := len(argtypes)
	var detail funcDetail

	// Get list of possible candidates from namespace search.
	rawCandidates := funcnameGetCandidates(funcname, nargs, expandVariadic, expandDefaults)

	// Quickly check if there is an exact match to the input datatypes
	// (there can be only one).
	var bestCandidate *funcCandidate
	for _, c := range rawCandidates {
		if slices.Equal(argtypes, c.args[:nargs]) {
			bestCandidate = c
			break
		}
	}

	if bestCandidate == nil {
		// If we didn't find an exact match, next consider the possibility
		// that this is really a type-coercion request: a single-argument
		// function call where the function name is a type name. If so, and
		// if the coercion path is RELABELTYPE or COERCEVIAIO, then go ahead
		// and treat the "function call" as a coercion.
		//
		// This interpretation needs to be given higher priority than
		// interpretations involving a type coercion followed by a function
		// call, otherwise we can produce surprising results. For example, we
		// want "text(varchar)" to be interpreted as a simple coercion, not
		// as "text(name(varchar))" which the code below this point is
		// entirely capable of selecting.
		//
		// We also treat a coercion of a previously-unknown-type literal
		// constant to a specific type this way.
		//
		// The reason we reject COERCION_PATH_FUNC here is that we expect the
		// cast implementation function to be named after the target type.
		// Thus the function will be found by normal lookup if appropriate.
		//
		// The reason we reject COERCION_PATH_ARRAYCOERCE is mainly that you
		// can't write "foo[] (something)" as a function call.
		//
		// We also reject the specific case of COERCEVIAIO for a composite
		// source type and a string-category target type. This is a case
		// that findCoercionPathway allows by default, but experience has
		// shown that it's too commonly invoked by mistake. So, again, insist
		// that people use cast syntax if they want to do that.
		//
		// NB: it's important that this code does not exceed what coerceType
		// can do, because the caller will try to apply coerceType if we
		// return funcDetailCoercion. If we return that result for something
		// coerceType can't handle, we'll cause infinite recursion between
		// this module and coerceType!
		if nargs == 1 && len(fargs) == 1 {
			if targetType := funcNameAsType(funcname); targetType != pgoid.InvalidOid {
				sourceType := argtypes[0]
				_, isConst := fargs[0].(*ast.Const)
				iscoercion := false
				if sourceType == pgoid.UNKNOWNOID && isConst {
					// always treat typename('literal') as coercion
					iscoercion = true
				} else {
					cpathtype, _ := findCoercionPathway(targetType, sourceType, ast.COERCION_EXPLICIT)
					switch cpathtype {
					case coercionPathRelabelType:
						iscoercion = true
					case coercionPathCoerceViaIO:
						iscoercion = !((sourceType == pgoid.RECORDOID || isComplex(sourceType)) &&
							typeCategory(targetType) == typcategoryString)
					}
				}
				if iscoercion {
					// Treat it as a type coercion.
					detail.rettype = targetType
					detail.trueTypeids = argtypes
					return funcDetailCoercion, detail
				}
			}
		}

		// didn't find an exact match, so now try to match up candidates...
		if len(rawCandidates) > 0 {
			currentCandidates := funcMatchArgtypes(argtypes, rawCandidates)
			switch {
			case len(currentCandidates) == 1:
				// one match only? then run with it...
				bestCandidate = currentCandidates[0]
			case len(currentCandidates) > 1:
				// multiple candidates? then better decide or throw an
				// error...
				bestCandidate = funcSelectCandidate(argtypes, currentCandidates)
				// If we were able to choose a best candidate, we're done.
				// Otherwise, ambiguous function call.
				if bestCandidate == nil {
					return funcDetailMultiple, detail
				}
			}
		}
	}

	if bestCandidate == nil {
		return funcDetailNotFound, detail
	}

	// If processing named args or expanding variadics or defaults, the
	// "best candidate" might represent multiple equivalently good
	// functions; treat this case as ambiguous.
	if bestCandidate.oid == pgoid.InvalidOid {
		return funcDetailMultiple, detail
	}

	detail.funcid = bestCandidate.oid
	detail.nvargs = bestCandidate.nvargs
	detail.trueTypeids = bestCandidate.args

	proc := pgcatalog.ProcByOid(bestCandidate.oid)
	if proc == nil { // should not happen
		pgerror.Elogf("cache lookup failed for function %d", bestCandidate.oid)
	}
	detail.rettype = proc.RetType
	detail.retset = proc.RetSet
	detail.vatype = proc.Variadic

	// A call that relies on defaulted parameters needs their default
	// expressions (pg_proc.proargdefaults), which the catalog does not
	// carry.
	if bestCandidate.ndargs > 0 {
		unsupported("function %s requires default arguments, which are not supported by the gateway evaluation engine",
			funcSignatureString(funcname, argtypes))
	}

	switch proc.Kind {
	case pgcatalog.ProcKindAggregate:
		return funcDetailAggregate, detail
	case pgcatalog.ProcKindFunction:
		return funcDetailNormal, detail
	case pgcatalog.ProcKindProcedure:
		return funcDetailProcedure, detail
	case pgcatalog.ProcKindWindow:
		return funcDetailWindowFunc, detail
	}
	pgerror.Elogf("unrecognized prokind: %c", proc.Kind)
	return funcDetailNormal, detail
}

// makeFnArguments adds the casts that take the actual argument expressions
// to the function's declared input types - parse_func.c:1825. The caller
// should already have verified that casting is allowed. The slice is
// modified in place.
func (p *ParseState) makeFnArguments(fargs []ast.Node, actualArgTypes, declaredArgTypes []Oid) {
	for i, node := range fargs {
		// types don't match? then force coercion using a function call...
		if actualArgTypes[i] != declaredArgTypes[i] {
			fargs[i] = p.coerceType(node, actualArgTypes[i], declaredArgTypes[i], -1,
				ast.COERCION_IMPLICIT, ast.COERCE_IMPLICIT_CAST, -1)
		}
	}
}

// funcNameAsType returns the OID of the type a function name matches, or 0
// - parse_func.c:1881. Shell types and complex types are ignored.
func funcNameAsType(funcname *ast.NodeList) Oid {
	typeName := &ast.TypeName{Names: funcname}
	t, _ := lookupTypeName(typeName)
	if t == nil || isComplex(t.Oid) {
		return pgoid.InvalidOid
	}
	return t.Oid
}

// funcSignatureString renders "name(type1, type2)" for error messages -
// parse_func.c:2030.
func funcSignatureString(funcname *ast.NodeList, argtypes []Oid) string {
	parts := make([]string, len(argtypes))
	for i, t := range argtypes {
		parts[i] = formatTypeBe(t)
	}
	return nameListToString(funcname) + "(" + strings.Join(parts, ", ") + ")"
}

// parseFuncOrColumn resolves a function call, coerces its arguments and
// builds the FuncExpr - parse_func.c:90, for the function-notation case.
// Column-projection notation (which needs a relation context), aggregates
// and window functions are outside what the gateway evaluates; their
// upstream error checks against ordinary calls are kept.
func (p *ParseState) parseFuncOrColumn(funcname *ast.NodeList, fargs []ast.Node, fn *ast.FuncCall, location int) ast.Node {
	// Most of the rest of the parser just assumes that functions do not
	// have more than FUNC_MAX_ARGS parameters. We have to test here to
	// protect against array overruns, etc.
	if len(fargs) > funcMaxArgs {
		tooManyArguments()
	}

	// Extract arg type info in preparation for function lookup.
	//
	// If any arguments are Param markers of type VOID, we discard them from
	// the parameter list. This is a hack to allow the JDBC driver to not
	// have to distinguish "input" and "output" parameter symbols while
	// parsing function-call constructs.
	actualArgTypes := make([]Oid, 0, len(fargs))
	kept := fargs[:0:0]
	for _, arg := range fargs {
		argtype := exprType(arg)
		if _, isParam := arg.(*ast.Param); isParam && argtype == pgoid.VOIDOID && !fn.AggWithinGroup {
			continue
		}
		kept = append(kept, arg)
		actualArgTypes = append(actualArgTypes, argtype)
	}
	fargs = kept
	nargs := len(fargs)

	// Check for named arguments; if there are any, build a list of names.
	//
	// We allow mixed notation (some named and some not), but only with all
	// the named parameters after all the unnamed ones.
	var argnames []string
	for _, arg := range fargs {
		if na, ok := arg.(*ast.NamedArgExpr); ok {
			// Reject duplicate arg names.
			if slices.Contains(argnames, na.Name) {
				ereport(mterrors.PgSSSyntaxError, "argument name \"%s\" used more than once", na.Name)
			}
			argnames = append(argnames, na.Name)
		} else if argnames != nil {
			ereport(mterrors.PgSSSyntaxError, "positional argument cannot follow named argument")
		}
	}
	if argnames != nil {
		// Matching names to parameters needs pg_proc.proargnames.
		unsupported("named function arguments are not supported by the gateway evaluation engine")
	}

	// funcGetDetail looks up the function in the catalogs, does
	// disambiguation for polymorphic functions, and returns the funcid and
	// type and set or singleton status of the function's return value. It
	// also returns the true argument types to the function.
	//
	// Note: for a variadic function call, the reported "true" types aren't
	// really what is in pg_proc: a variadic argument is replaced by a
	// suitable number of copies of its element type. We'll fix up the
	// variadic case below.
	fdresult, detail := funcGetDetail(funcname, fargs, actualArgTypes, !fn.FuncVariadic, true)
	funcid, rettype, retset, nvargs, vatype := detail.funcid, detail.rettype, detail.retset, detail.nvargs, detail.vatype
	declaredArgTypes := detail.trueTypeids
	funcVariadic := fn.FuncVariadic

	// Check for various wrong-kind-of-routine cases. If not a CALL, reject
	// procedures.
	if fdresult == funcDetailProcedure {
		ereportHint(mterrors.PgSSWrongObjectType, "To call a procedure, use CALL.",
			"%s is a procedure", funcSignatureString(funcname, actualArgTypes))
	}

	if fdresult == funcDetailNormal || fdresult == funcDetailCoercion {
		// In these cases, complain if there was anything indicating it
		// must be an aggregate or window function.
		name := nameListToString(funcname)
		switch {
		case fn.AggStar:
			ereport(mterrors.PgSSWrongObjectType, "%s(*) specified, but %s is not an aggregate function", name, name)
		case fn.AggDistinct:
			ereport(mterrors.PgSSWrongObjectType, "DISTINCT specified, but %s is not an aggregate function", name)
		case fn.AggWithinGroup:
			ereport(mterrors.PgSSWrongObjectType, "WITHIN GROUP specified, but %s is not an aggregate function", name)
		case fn.AggOrder != nil && fn.AggOrder.Len() > 0:
			ereport(mterrors.PgSSWrongObjectType, "ORDER BY specified, but %s is not an aggregate function", name)
		case fn.AggFilter != nil:
			ereport(mterrors.PgSSWrongObjectType, "FILTER specified, but %s is not an aggregate function", name)
		case fn.Over != nil:
			ereport(mterrors.PgSSWrongObjectType,
				"OVER specified, but %s is not a window function nor an aggregate function", name)
		}
	}

	// So far so good, so do some fdresult-type-specific processing.
	switch fdresult {
	case funcDetailNormal:
		// Nothing special to do for this case.
	case funcDetailAggregate, funcDetailWindowFunc:
		// Aggregate and window calls are evaluated over rows, which the
		// gateway's scalar engine does not do.
		unsupported("aggregate and window functions are not supported by the gateway evaluation engine: %s",
			funcSignatureString(funcname, actualArgTypes))
	case funcDetailCoercion:
		// We interpreted it as a type coercion. coerceType can handle these
		// cases, so why duplicate code...
		return p.coerceType(fargs[0], actualArgTypes[0], rettype, -1,
			ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CALL, location)
	case funcDetailMultiple:
		// We found multiple possible functional matches.
		ereportHint(mterrors.PgSSAmbiguousFunction,
			"Could not choose a best candidate function. You might need to add explicit type casts.",
			"function %s is not unique", funcSignatureString(funcname, actualArgTypes))
	default:
		// Not found as a function. Upstream reports "function does not
		// exist" (42883) here, with a hint about ORDER BY placement when
		// that looks like the cause. The builtin catalog cannot rule out a
		// user function of this name, so the miss is not authoritative.
		notInBuiltinCatalog("function matches %s", funcSignatureString(funcname, actualArgTypes))
	}

	// enforce consistency with polymorphic argument and return types,
	// possibly adjusting return type or declaredArgTypes (which will be used
	// as the cast destination by makeFnArguments)
	rettype = enforceGenericTypeConsistency(actualArgTypes, declaredArgTypes, rettype, false)

	// perform the necessary typecasting of arguments
	p.makeFnArguments(fargs, actualArgTypes, declaredArgTypes)

	// If the function isn't actually variadic, forget any VARIADIC
	// decoration on the call. (Perhaps we should throw an error instead,
	// but historically we've allowed people to write that.)
	if vatype == pgoid.InvalidOid {
		funcVariadic = false
	}

	// If it's a variadic function call, transform the last nvargs
	// arguments into an array --- unless it's an "any" variadic.
	if nvargs > 0 && vatype != pgoid.ANYOID {
		nonVarArgs := nargs - nvargs
		vargs := ast.NewNodeList(fargs[nonVarArgs:]...)
		fargs = fargs[:nonVarArgs]

		// assume all the variadic arguments were coerced to the same type
		elementType := exprType(vargs.Items[0])
		arrayType := getArrayType(elementType)
		if arrayType == pgoid.InvalidOid {
			ereport(mterrors.PgSSUndefinedObject, "could not find array type for data type %s",
				formatTypeBe(elementType))
		}
		newa := ast.NewArrayExpr(arrayType, elementType, vargs)
		// array_collid will be set by parse_collate.c
		newa.Multidims = false
		newa.SetLocation(exprLocation(vargs.Items[0]))
		fargs = append(fargs, newa)

		// We could not have had VARIADIC marking before ... but now, it's a
		// VARIADIC call.
		funcVariadic = true
	}

	// If an "any" variadic is called with explicit VARIADIC marking, insist
	// that the variadic parameter be of some array type.
	if nargs > 0 && vatype == pgoid.ANYOID && funcVariadic {
		if getBaseElementType(actualArgTypes[nargs-1]) == pgoid.InvalidOid {
			ereport(mterrors.PgSSDatatypeMismatch, "VARIADIC argument must be an array")
		}
	}

	// if it returns a set, check that's OK
	if retset {
		checkSRFCallPlacement()
	}

	// build the appropriate output structure
	funcexpr := ast.NewFuncExpr(funcid, rettype, ast.NewNodeList(fargs...))
	funcexpr.Funcretset = retset
	funcexpr.Funcvariadic = funcVariadic
	funcexpr.Funcformat = fn.Funcformat
	// funccollid and inputcollid will be set by parse_collate.c
	funcexpr.SetLocation(location)
	return funcexpr
}
