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

package funcs

import (
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
	"github.com/multigres/multigres/go/common/sqltypes"
)

func init() {
	fmgr.RegisterBuiltin("boolin", boolin)
	fmgr.RegisterBuiltin("boolout", boolout)
	fmgr.RegisterBuiltin("booltext", booltext)
	fmgr.RegisterBuiltin("booleq", booleq)
	fmgr.RegisterBuiltin("boolne", boolne)
	fmgr.RegisterBuiltin("boollt", boollt)
	fmgr.RegisterBuiltin("boolle", boolle)
	fmgr.RegisterBuiltin("boolgt", boolgt)
	fmgr.RegisterBuiltin("boolge", boolge)
	fmgr.RegisterBuiltin("int4_bool", int4bool)
	fmgr.RegisterBuiltin("bool_int4", boolint4)
	fmgr.RegisterBuiltin("booland_statefunc", boolandStatefunc)
	fmgr.RegisterBuiltin("boolor_statefunc", boolorStatefunc)
}

// boolin - bool.c:126, REL_17_6. The shared parser also serves Bind decoding
// and gateway settings; preserve the original spelling in error diagnostics.
func boolin(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	s := cstringArg(fcinfo)
	if value, ok := sqltypes.ParseBool(s); ok {
		return datum.BoolGetDatum(value)
	}
	pgerror.Ereportf(mterrors.PgSSInvalidTextRepresentation,
		"invalid input syntax for type boolean: \"%s\"", s)
	return datum.Datum{} // unreachable
}

// boolout - bool.c:157. Unlike the bool-to-text cast, output uses t/f.
func boolout(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	if fcinfo.GetArgBool(0) {
		return datum.BytesGetDatum([]byte("t"))
	}
	return datum.BytesGetDatum([]byte("f"))
}

// booltext - bool.c:204.
func booltext(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	if fcinfo.GetArgBool(0) {
		return datum.BytesGetDatum([]byte("true"))
	}
	return datum.BytesGetDatum([]byte("false"))
}

// Boolean comparisons - bool.c:223-275; false sorts before true.
func booleq(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(fcinfo.GetArgBool(0) == fcinfo.GetArgBool(1))
}

func boolne(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(fcinfo.GetArgBool(0) != fcinfo.GetArgBool(1))
}

func boollt(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(!fcinfo.GetArgBool(0) && fcinfo.GetArgBool(1))
}

func boolle(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(!fcinfo.GetArgBool(0) || fcinfo.GetArgBool(1))
}

func boolgt(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(fcinfo.GetArgBool(0) && !fcinfo.GetArgBool(1))
}

func boolge(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(fcinfo.GetArgBool(0) || !fcinfo.GetArgBool(1))
}

// int4_bool/bool_int4 - int.c:362-379. PostgreSQL has no direct bool/int2 or
// bool/int8 cast functions; those conversions must go through int4.
func int4bool(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(fcinfo.GetArgInt32(0) != 0)
}

func boolint4(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	if fcinfo.GetArgBool(0) {
		return datum.Int32GetDatum(1)
	}
	return datum.Int32GetDatum(0)
}

// Plain aggregate state functions - bool.c:287,299. These are strict scalar
// helpers, not SQL AND/OR: expression-level short-circuiting and three-valued
// logic belong to the evaluator, and aggregate execution is still unsupported.
func boolandStatefunc(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(fcinfo.GetArgBool(0) && fcinfo.GetArgBool(1))
}

func boolorStatefunc(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(fcinfo.GetArgBool(0) || fcinfo.GetArgBool(1))
}
