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
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
)

// This file ports parse_type.c (type-name lookup) and format_type.c (the
// type names used in error messages).
//
// Name resolution sees only the builtin catalog, which is pg_catalog. An
// unqualified name is looked up there; "pg_catalog.x" is the same lookup; a
// name qualified with any other schema is reported as unsupported, since
// the gateway can tell neither whether that schema exists nor what it
// holds. search_path shadowing by user objects is likewise invisible here;
// schema tracking owns that later.

// deconstructQualifiedName splits a possibly-qualified name list into
// schema (or "") and object name - namespace.c:3281.
func deconstructQualifiedName(names *ast.NodeList) (schemaname, objname string) {
	switch names.Len() {
	case 1:
		return "", ast.StrVal(names.Items[0])
	case 2:
		return ast.StrVal(names.Items[0]), ast.StrVal(names.Items[1])
	case 3:
		// Upstream accepts the three-part form only when the catalog name is
		// the current database, which is not known here.
		ereport(mterrors.PgSSFeatureNotSupported,
			"cross-database references are not implemented: %s", nameListToString(names))
	default:
		ereport(mterrors.PgSSSyntaxError,
			"improper qualified name (too many dotted names): %s", nameListToString(names))
	}
	return "", ""
}

// requireBuiltinSchema rejects a schema qualifier the gateway cannot
// resolve; "" (unqualified) and pg_catalog both mean the builtin catalog.
func requireBuiltinSchema(schemaname string) {
	if schemaname != "" && schemaname != "pg_catalog" {
		unsupported("objects outside pg_catalog are not supported by the gateway evaluation engine: schema %q", schemaname)
	}
}

// nameListToString joins a qualified name with dots - namespace.c:3371.
func nameListToString(names *ast.NodeList) string {
	parts := make([]string, 0, names.Len())
	for _, item := range names.Items {
		if _, ok := item.(*ast.A_Star); ok {
			parts = append(parts, "*")
		} else {
			parts = append(parts, ast.StrVal(item))
		}
	}
	return strings.Join(parts, ".")
}

// lookupTypeName finds the pg_type row named by a TypeName, or nil, and the
// typmod the TypeName specifies - parse_type.c:73 (LookupTypeNameExtended).
func lookupTypeName(typeName *ast.TypeName) (*pgcatalog.Type, int32) {
	var typoid Oid
	switch {
	case typeName.Names == nil || typeName.Names.Len() == 0:
		// We have the OID already if it's an internally generated TypeName.
		typoid = typeName.TypeOid
	case typeName.PctType:
		// %TYPE needs the referenced relation's columns.
		unsupported("%TYPE references are not supported by the gateway evaluation engine")
	default:
		schemaname, typname := deconstructQualifiedName(typeName.Names)
		requireBuiltinSchema(schemaname)
		if t := pgcatalog.TypeByName(typname); t != nil {
			typoid = t.Oid
		}
		// If an array reference, return the array type instead.
		if typeName.ArrayBounds != nil && typeName.ArrayBounds.Len() > 0 {
			typoid = getArrayType(typoid)
		}
	}
	if typoid == pgoid.InvalidOid {
		return nil, -1
	}
	t := typeidType(typoid)
	return t, typenameTypeMod(typeName, t)
}

// typenameType looks a TypeName up, reporting an error if it names no type -
// parse_type.c:264. Upstream's error is "type does not exist" (42704); the
// builtin catalog cannot rule out a user type of this name, so the miss is
// not authoritative.
func typenameType(typeName *ast.TypeName) (*pgcatalog.Type, int32) {
	t, typmod := lookupTypeName(typeName)
	if t == nil {
		notInBuiltinCatalog("type %q", typeNameToString(typeName))
	}
	return t, typmod
}

// typenameTypeIdAndMod returns a TypeName's type OID and typmod - parse_type.c:310.
func typenameTypeIdAndMod(typeName *ast.TypeName) (Oid, int32) {
	t, typmod := typenameType(typeName)
	return t.Oid, typmod
}

// typenameTypeMod computes the typmod a TypeName specifies - parse_type.c:332.
// A type modifier is interpreted by the type's typmodin function, none of
// which are ported; a TypeName carrying one is unsupported for now. The Go
// grammar never pre-specifies a typmod (TypeName.Typemod), so a TypeName
// without modifiers always means -1.
func typenameTypeMod(typeName *ast.TypeName, _ *pgcatalog.Type) int32 {
	if typeName.Typmods == nil || typeName.Typmods.Len() == 0 {
		return -1
	}
	unsupported("type modifiers are not supported by the gateway evaluation engine: %s", typeNameToString(typeName))
	return -1
}

// typeNameToString renders a TypeName for error messages - parse_type.c:478.
func typeNameToString(typeName *ast.TypeName) string {
	var sb strings.Builder
	if typeName.Names != nil && typeName.Names.Len() > 0 {
		sb.WriteString(nameListToString(typeName.Names))
	} else {
		sb.WriteString(formatTypeBe(typeName.TypeOid))
	}
	if typeName.PctType {
		sb.WriteString("%TYPE")
	}
	if typeName.ArrayBounds != nil && typeName.ArrayBounds.Len() > 0 {
		sb.WriteString("[]")
	}
	return sb.String()
}

// formatTypeBe renders a type OID the way error messages show it -
// format_type.c:343, format_type_extended with no flags.
func formatTypeBe(typeOid Oid) string {
	t := typeidType(typeOid)

	// Check if it's a "true" array type. Pseudo-array types such as "name"
	// shouldn't get deconstructed. Also check the toast property, and don't
	// deconstruct "plain storage" array types --- this is because we don't
	// want to show oidvector as oid[]. (oidvector and int2vector are the
	// plain-storage arrays; the catalog does not carry typstorage.)
	isArray := false
	if getElementType(typeOid) != pgoid.InvalidOid &&
		typeOid != pgoid.OIDVECTOROID && typeOid != pgoid.INT2VECTOROID {
		t = typeidType(t.Elem)
		typeOid = t.Oid
		isArray = true
	}

	// See if we want to special-case the output for certain built-in types.
	// These correspond to special productions in gram.y, to ensure that the
	// type name will be taken as a system type, not a user type of the same
	// name. The typmod-bearing cases are the no-typmod spellings, since
	// format_type_be passes no typmod.
	var buf string
	switch typeOid {
	case pgoid.BITOID:
		buf = "bit"
	case pgoid.BOOLOID:
		buf = "boolean"
	case pgoid.BPCHAROID:
		buf = "character"
	case pgoid.FLOAT4OID:
		buf = "real"
	case pgoid.FLOAT8OID:
		buf = "double precision"
	case pgoid.INT2OID:
		buf = "smallint"
	case pgoid.INT4OID:
		buf = "integer"
	case pgoid.INT8OID:
		buf = "bigint"
	case pgoid.NUMERICOID:
		buf = "numeric"
	case pgoid.INTERVALOID:
		buf = "interval"
	case pgoid.TIMEOID:
		buf = "time without time zone"
	case pgoid.TIMETZOID:
		buf = "time with time zone"
	case pgoid.TIMESTAMPOID:
		buf = "timestamp without time zone"
	case pgoid.TIMESTAMPTZOID:
		buf = "timestamp with time zone"
	case pgoid.VARBITOID:
		buf = "bit varying"
	case pgoid.VARCHAROID:
		buf = "character varying"
	case pgoid.JSONOID:
		buf = "json"
	default:
		// Default handling: report the name as it appears in the catalog,
		// double-quoted if it's not a standard identifier or if it matches
		// any keyword. Builtin types are always visible, so never qualified.
		buf = quoteIdentifier(t.Name)
	}
	if isArray {
		buf += "[]"
	}
	return buf
}

// quoteIdentifier double-quotes an identifier when needed - ruleutils.c:12483.
// Lowercase ASCII letters, digits and underscores (not starting with a digit)
// pass unquoted unless the name is a keyword of any category but unreserved.
func quoteIdentifier(ident string) string {
	safe := len(ident) > 0 && (ident[0] >= 'a' && ident[0] <= 'z' || ident[0] == '_')
	for i := 0; safe && i < len(ident); i++ {
		c := ident[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			safe = false
		}
	}
	if safe {
		// Check for keyword. We quote keywords except for unreserved ones.
		if cat, ok := ast.LookupKeywordCategory(ident); ok && cat != ast.UnreservedKeyword {
			safe = false
		}
	}
	if safe {
		return ident
	}
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
