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

// Package pgoid is the leaf home of PostgreSQL's object identifier type and
// the builtin type OID constants — the analogue of
// postgres/src/include/postgres_ext.h (the Oid type itself) and the type OID
// macros of the generated pg_type_d.h. It has no dependencies, so both the
// parser AST and the catalog/eval packages can sit on it without pulling in
// each other.
//
// The constants in oids_generated.go are generated from the vendored
// pg_type.dat (PostgreSQL REL_17_6); regenerate with "make pgcatalog".
package pgoid

// Oid is PostgreSQL's object identifier type -
// postgres/src/include/postgres_ext.h:31.
type Oid uint32

// InvalidOid represents an invalid object identifier -
// postgres/src/include/postgres_ext.h:36.
const InvalidOid = Oid(0)

// String returns the uppercased PostgreSQL type name for builtin type OIDs
// (e.g. "BOOL", "_INT4"), or an empty string if the OID is not a builtin
// type.
func (o Oid) String() string {
	return typeNames[o]
}
