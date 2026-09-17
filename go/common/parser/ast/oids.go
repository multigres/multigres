// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2025, Supabase, Inc
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
package ast

import (
	"strings"

	"github.com/multigres/multigres/go/common/parser/pgoid"
)

// The Oid type and the builtin type OID constants live in pgoid (generated
// from pg_type.dat); the type alias in expressions.go and the re-exports in
// oids_generated.go keep existing ast call sites working. This file keeps
// only the parser-side helpers.

// InvalidOid represents an invalid object identifier.
const InvalidOid = pgoid.InvalidOid

// TypeNameToOid resolves a simple (unqualified) PostgreSQL type name to its OID.
// Handles common aliases like "int" → INT4OID, "boolean" → BOOLOID, etc.
// Returns InvalidOid for unrecognized type names.
func TypeNameToOid(name string) Oid {
	switch strings.ToLower(name) {
	// Boolean
	case "bool", "boolean":
		return BOOLOID

	// Character types
	case "char", "bpchar":
		return BPCHAROID
	case "varchar", "character varying":
		return VARCHAROID
	case "text":
		return TEXTOID
	case "name":
		return NAMEOID

	// Integer types
	case "int2", "smallint":
		return INT2OID
	case "int", "int4", "integer":
		return INT4OID
	case "int8", "bigint":
		return INT8OID

	// Floating point
	case "float4", "real":
		return FLOAT4OID
	case "float", "float8", "double precision":
		return FLOAT8OID

	// Numeric
	case "numeric", "decimal":
		return NUMERICOID

	// Date/time
	case "date":
		return DATEOID
	case "time", "time without time zone":
		return TIMEOID
	case "timetz", "time with time zone":
		return TIMETZOID
	case "timestamp", "timestamp without time zone":
		return TIMESTAMPOID
	case "timestamptz", "timestamp with time zone":
		return TIMESTAMPTZOID
	case "interval":
		return INTERVALOID

	// Binary
	case "bytea":
		return BYTEAOID

	// JSON
	case "json":
		return JSONOID
	case "jsonb":
		return JSONBOID

	// XML
	case "xml":
		return XMLOID

	// UUID
	case "uuid":
		return UUIDOID

	// Network
	case "inet":
		return INETOID
	case "cidr":
		return CIDROID
	case "macaddr":
		return MACADDROID
	case "macaddr8":
		return MACADDR8OID

	// Bit string
	case "bit":
		return BITOID
	case "varbit", "bit varying":
		return VARBITOID

	// Money
	case "money":
		return MONEYOID

	// System
	case "oid":
		return OIDOID

	default:
		return InvalidOid
	}
}

// ArrayTypeOid returns the built-in array OID for an element OID.
func ArrayTypeOid(oid Oid) Oid {
	switch oid {
	case BOOLOID:
		return BOOLARRAYOID
	case BPCHAROID:
		return BPCHARARRAYOID
	case BYTEAOID:
		return BYTEAARRAYOID
	case CHAROID:
		return CHARARRAYOID
	case DATEOID:
		return DATEARRAYOID
	case FLOAT4OID:
		return FLOAT4ARRAYOID
	case FLOAT8OID:
		return FLOAT8ARRAYOID
	case INT2OID:
		return INT2ARRAYOID
	case INT4OID:
		return INT4ARRAYOID
	case INT8OID:
		return INT8ARRAYOID
	case JSONBOID:
		return JSONBARRAYOID
	case JSONOID:
		return JSONARRAYOID
	case NAMEOID:
		return NAMEARRAYOID
	case TEXTOID:
		return TEXTARRAYOID
	case TIMEOID:
		return TIMEARRAYOID
	case TIMESTAMPOID:
		return TIMESTAMPARRAYOID
	case TIMESTAMPTZOID:
		return TIMESTAMPTZARRAYOID
	case VARCHAROID:
		return VARCHARARRAYOID
	default:
		return InvalidOid
	}
}
