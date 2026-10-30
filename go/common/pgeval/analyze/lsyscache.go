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
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// Oid is PostgreSQL's object identifier, the same alias the parser, catalog
// and engine share.
type Oid = pgoid.Oid

// funcMaxArgs is FUNC_MAX_ARGS - pg_config_manual.h:43.
const funcMaxArgs = 100

// Type categories (pg_type.typcategory) - pg_type.h:284-300.
const (
	typcategoryInvalid   byte = 0
	typcategoryArray     byte = 'A'
	typcategoryComposite byte = 'C'
	typcategoryRange     byte = 'R'
	typcategoryString    byte = 'S'
)

// This file is the slice of lsyscache.c the parser needs, served from the
// compiled-in builtin catalog instead of the syscache. Two pg_type columns
// the catalog does not carry are approximated, each noted at its accessor:
// typtype (domain/enum/range/composite classification) and typsubscript
// (true-array detection).

// typeidType returns the pg_type row for a type OID - parse_type.c:578.
func typeidType(id Oid) *pgcatalog.Type {
	t := pgcatalog.TypeByOid(id)
	if t == nil {
		pgerror.Elogf("cache lookup failed for type %d", id)
	}
	return t
}

// getBaseType reduces a domain to its base type - lsyscache.c:2524. The
// builtin catalog has no domain rows (pg_type.typtype 'd' only arises from
// CREATE DOMAIN), and user-defined types are not visible here yet, so every
// resolvable type is its own base type. Callers keep the upstream call sites
// so that domain support later slots in here alone.
func getBaseType(typid Oid) Oid {
	return typid
}

// getBaseTypeAndTypmod is getBaseType also reducing the typmod - lsyscache.c:2544.
func getBaseTypeAndTypmod(typid Oid, _ *int32) Oid {
	return typid
}

// getTypeCategoryPreferred returns typcategory and typispreferred - lsyscache.c:2763.
func getTypeCategoryPreferred(typid Oid) (byte, bool) {
	t := typeidType(typid)
	return t.Category, t.Preferred
}

// getTypcollation returns the type's default collation - lsyscache.c:3106.
func getTypcollation(typid Oid) Oid {
	return typeidType(typid).Collation
}

// getElementType returns the element type of a true array type, else 0 -
// lsyscache.c:2898. Upstream tests typsubscript == array_subscript_handler;
// the catalog does not carry typsubscript, but every pg_type row of category
// 'A' (the generated _foo arrays, int2vector and oidvector) uses that
// handler and no other row does, so the category is an exact substitute.
func getElementType(typid Oid) Oid {
	t := pgcatalog.TypeByOid(typid)
	if t == nil || t.Category != typcategoryArray {
		return pgoid.InvalidOid
	}
	return t.Elem
}

// getArrayType returns the array type over typid, or 0 - lsyscache.c:2932.
func getArrayType(typid Oid) Oid {
	t := pgcatalog.TypeByOid(typid)
	if t == nil {
		return pgoid.InvalidOid
	}
	return t.Array
}

// getBaseElementType is getElementType looking through domains - lsyscache.c:2955.
func getBaseElementType(typid Oid) Oid {
	return getElementType(getBaseType(typid))
}

// typeIsArray is type_is_array - lsyscache.h:208.
func typeIsArray(typid Oid) bool {
	return getElementType(typid) != pgoid.InvalidOid
}

// typeIsArrayDomain is type_is_array_domain - lsyscache.h:210.
func typeIsArrayDomain(typid Oid) bool {
	return getBaseElementType(typid) != pgoid.InvalidOid
}

// isComplex is ISCOMPLEX: typrelid != 0 - parse_type.h:67. The catalog does
// not carry typrelid; the builtin rows with a relation are exactly the
// composite-category ones.
func isComplex(typid Oid) bool {
	t := pgcatalog.TypeByOid(typid)
	return t != nil && t.Category == typcategoryComposite
}

// typeIsRowtype is type_is_rowtype: RECORD or a composite - lsyscache.c:2635.
func typeIsRowtype(typid Oid) bool {
	return typid == pgoid.RECORDOID || isComplex(typid)
}

// typeIsEnum is type_is_enum - lsyscache.c:2676. No builtin type is an enum
// (typtype 'e' only arises from CREATE TYPE ... AS ENUM).
func typeIsEnum(Oid) bool {
	return false
}

// getRangeSubtype is get_range_subtype - lsyscache.c:3461. It needs pg_range,
// which the catalog does not carry. A non-range input returns 0 exactly as
// upstream; an actual range or multirange type (category 'R') cannot be
// resolved and is reported as unsupported rather than silently rejected.
func getRangeSubtype(rangeOid Oid) Oid {
	if t := pgcatalog.TypeByOid(rangeOid); t != nil && t.Category == typcategoryRange {
		unsupported("range types are not supported by the gateway evaluation engine")
	}
	return pgoid.InvalidOid
}

// getMultirangeRange is get_multirange_range - lsyscache.c:3535; see getRangeSubtype.
func getMultirangeRange(multirangeOid Oid) Oid {
	return getRangeSubtype(multirangeOid)
}

// getRangeMultirange is get_range_multirange - lsyscache.c:3512; see getRangeSubtype.
func getRangeMultirange(rangeOid Oid) Oid {
	return getRangeSubtype(rangeOid)
}

// typeInheritsFrom is typeInheritsFrom - pg_inherits.c:201, and
// typeIsOfTypedTable is parse_coerce.c:3382. Neither pg_inherits nor
// pg_class is visible here, and no builtin type takes part in inheritance.
func typeInheritsFrom(Oid, Oid) bool {
	return false
}

func typeIsOfTypedTable(Oid, Oid) bool {
	return false
}

// getFuncRetset is get_func_retset - lsyscache.c:1760, through the fmgr
// lookup so that tracked user functions resolve the same way as builtins.
func getFuncRetset(funcid Oid) bool {
	info, err := fmgr.FmgrInfoFor(funcid)
	if err != nil {
		pgerror.Elogf("cache lookup failed for function %d", funcid)
	}
	return info.FnRetset
}

// getTypeIOParam is the typioparam passed to a type's input function -
// lsyscache.c:2399.
func getTypeIOParam(t *pgcatalog.Type) Oid {
	if t.Elem != pgoid.InvalidOid {
		return t.Elem
	}
	return t.Oid
}

// isPolymorphicTypeFamily1 is IsPolymorphicTypeFamily1 - pg_type.h:318.
func isPolymorphicTypeFamily1(typid Oid) bool {
	switch typid {
	case pgoid.ANYELEMENTOID, pgoid.ANYARRAYOID, pgoid.ANYNONARRAYOID,
		pgoid.ANYENUMOID, pgoid.ANYRANGEOID, pgoid.ANYMULTIRANGEOID:
		return true
	}
	return false
}

// isPolymorphicTypeFamily2 is IsPolymorphicTypeFamily2 - pg_type.h:326.
func isPolymorphicTypeFamily2(typid Oid) bool {
	switch typid {
	case pgoid.ANYCOMPATIBLEOID, pgoid.ANYCOMPATIBLEARRAYOID, pgoid.ANYCOMPATIBLENONARRAYOID,
		pgoid.ANYCOMPATIBLERANGEOID, pgoid.ANYCOMPATIBLEMULTIRANGEOID:
		return true
	}
	return false
}

// isPolymorphicType is IsPolymorphicType - pg_type.h:313.
func isPolymorphicType(typid Oid) bool {
	return isPolymorphicTypeFamily1(typid) || isPolymorphicTypeFamily2(typid)
}
