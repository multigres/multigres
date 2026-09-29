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
package pgoid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestKnownOids pins values against PostgreSQL's generated pg_type_d.h
// (REL_17_6); the full 189-symbol list was diffed 1:1 against that header
// when this generation was introduced.
func TestKnownOids(t *testing.T) {
	assert.Equal(t, Oid(0), InvalidOid)
	assert.Equal(t, Oid(16), BOOLOID)
	assert.Equal(t, Oid(25), TEXTOID)
	assert.Equal(t, Oid(1700), NUMERICOID)
	assert.Equal(t, Oid(1000), BOOLARRAYOID)
	assert.Equal(t, Oid(210), PG_TYPEARRAYOID, "bootstrap rowtype arrays get symbols even though their scalars do not")
	assert.Equal(t, Oid(1263), CSTRINGARRAYOID)
	assert.Equal(t, Oid(6155), DATEMULTIRANGEARRAYOID)

	// Compatibility aliases, mirroring pg_type_d.h:131-132.
	assert.Equal(t, MONEYOID, CASHOID)
	assert.Equal(t, PG_LSNOID, LSNOID)
}

func TestString(t *testing.T) {
	assert.Equal(t, "BOOL", BOOLOID.String())
	assert.Equal(t, "_INT4", INT4ARRAYOID.String())
	assert.Equal(t, "TIMESTAMPTZ", TIMESTAMPTZOID.String())
	assert.Equal(t, "PG_LSN", PG_LSNOID.String())
	assert.Equal(t, "", Oid(999999).String(), "unknown OIDs render empty")
	assert.Equal(t, "", InvalidOid.String())
}
