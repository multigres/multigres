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

package fmgr

import "github.com/multigres/multigres/go/common/pgeval/datum"

// The methods below are the Go form of PostgreSQL's PG_GETARG_* / PG_ARGISNULL
// / PG_GET_COLLATION / PG_RETURN_NULL macro families (fmgr.h:198-369). Go has
// no macros, so they are methods on FunctionCallInfo; each reads args[n].value and
// applies the matching datum accessor, exactly as the C macro expands. Ported
// function bodies use these so they read line-for-line against the C:
// PG_GETARG_INT32(0) becomes fcinfo.GetArgInt32(0).
//
// A getter reads only the value; it never consults nullness. Strict functions
// never see a NULL argument (CallFunction short-circuits first), and a
// non-strict function checks ArgIsNull explicitly before calling a getter.
//
// A normal return needs no helper — a body returns datum.Int32GetDatum(x)
// directly, exactly as PG_RETURN_INT32(x) expands to return Int32GetDatum(x).
// Only the NULL result needs [FunctionCallInfoBaseData.ReturnNull], which sets the
// out-of-band IsNull flag.

// Arg returns the raw datum of argument n - PG_GETARG_DATUM(n) (fmgr.h:268).
func (fcinfo *FunctionCallInfoBaseData) Arg(n int) datum.Datum { return fcinfo.Args[n].Value }

// ArgIsNull reports whether argument n is NULL - PG_ARGISNULL(n) (fmgr.h:209).
func (fcinfo *FunctionCallInfoBaseData) ArgIsNull(n int) bool { return fcinfo.Args[n].IsNull }

// Collation returns the collation the function should use -
// PG_GET_COLLATION() (fmgr.h:198).
func (fcinfo *FunctionCallInfoBaseData) Collation() Oid { return fcinfo.Fncollation }

// GetArgBool reads argument n as a bool - PG_GETARG_BOOL(n).
func (fcinfo *FunctionCallInfoBaseData) GetArgBool(n int) bool {
	return datum.DatumGetBool(fcinfo.Args[n].Value)
}

// GetArgChar reads argument n as a "char" - PG_GETARG_CHAR(n).
func (fcinfo *FunctionCallInfoBaseData) GetArgChar(n int) datum.Char {
	return datum.DatumGetChar(fcinfo.Args[n].Value)
}

// GetArgInt16 reads argument n as an int16 - PG_GETARG_INT16(n).
func (fcinfo *FunctionCallInfoBaseData) GetArgInt16(n int) int16 {
	return datum.DatumGetInt16(fcinfo.Args[n].Value)
}

// GetArgInt32 reads argument n as an int32 - PG_GETARG_INT32(n).
func (fcinfo *FunctionCallInfoBaseData) GetArgInt32(n int) int32 {
	return datum.DatumGetInt32(fcinfo.Args[n].Value)
}

// GetArgInt64 reads argument n as an int64 - PG_GETARG_INT64(n).
func (fcinfo *FunctionCallInfoBaseData) GetArgInt64(n int) int64 {
	return datum.DatumGetInt64(fcinfo.Args[n].Value)
}

// GetArgUInt32 reads argument n as a uint32 - PG_GETARG_UINT32(n).
func (fcinfo *FunctionCallInfoBaseData) GetArgUInt32(n int) uint32 {
	return datum.DatumGetUInt32(fcinfo.Args[n].Value)
}

// GetArgOid reads argument n as an Oid - PG_GETARG_OID(n).
func (fcinfo *FunctionCallInfoBaseData) GetArgOid(n int) Oid {
	return datum.DatumGetObjectId(fcinfo.Args[n].Value)
}

// GetArgFloat4 reads argument n as a float32 - PG_GETARG_FLOAT4(n).
func (fcinfo *FunctionCallInfoBaseData) GetArgFloat4(n int) float32 {
	return datum.DatumGetFloat4(fcinfo.Args[n].Value)
}

// GetArgFloat8 reads argument n as a float64 - PG_GETARG_FLOAT8(n).
func (fcinfo *FunctionCallInfoBaseData) GetArgFloat8(n int) float64 {
	return datum.DatumGetFloat8(fcinfo.Args[n].Value)
}

// GetArgBytes reads argument n as its by-reference payload bytes -
// PG_GETARG_* for a varlena, minus detoasting (the gateway never sees a
// toasted value; see the datum package).
func (fcinfo *FunctionCallInfoBaseData) GetArgBytes(n int) []byte {
	return datum.DatumGetBytes(fcinfo.Args[n].Value)
}

// ReturnNull marks the result NULL and returns a zero datum -
// PG_RETURN_NULL() (fmgr.h:345). Nullness travels out-of-band in IsNull, so
// the returned datum itself is just a zero value.
func (fcinfo *FunctionCallInfoBaseData) ReturnNull() datum.Datum {
	fcinfo.IsNull = true
	return datum.Datum{}
}
