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
	"cmp"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

func init() {
	fmgr.RegisterBuiltin("btint2cmp", btint2cmp)
	fmgr.RegisterBuiltin("btint4cmp", integerCompare[int32, int32])
	fmgr.RegisterBuiltin("btint8cmp", integerCompare[int64, int64])
	fmgr.RegisterBuiltin("btint24cmp", integerCompare[int16, int32])
	fmgr.RegisterBuiltin("btint42cmp", integerCompare[int32, int16])
	fmgr.RegisterBuiltin("btint28cmp", integerCompare[int16, int64])
	fmgr.RegisterBuiltin("btint82cmp", integerCompare[int64, int16])
	fmgr.RegisterBuiltin("btint48cmp", integerCompare[int32, int64])
	fmgr.RegisterBuiltin("btint84cmp", integerCompare[int64, int32])
	fmgr.RegisterBuiltin("btboolcmp", btboolcmp)

	fmgr.RegisterBuiltin("in_range_int2_int2", integerInRange[int16, int16])
	fmgr.RegisterBuiltin("in_range_int2_int4", integerInRange[int16, int32])
	fmgr.RegisterBuiltin("in_range_int2_int8", integerInRange[int16, int64])
	fmgr.RegisterBuiltin("in_range_int4_int2", integerInRange[int32, int16])
	fmgr.RegisterBuiltin("in_range_int4_int4", integerInRange[int32, int32])
	fmgr.RegisterBuiltin("in_range_int4_int8", integerInRange[int32, int64])
	fmgr.RegisterBuiltin("in_range_int8_int8", integerInRange[int64, int64])
}

// nbtcompare.c:82, REL_17_6. Unlike the wider comparators, PG returns the
// actual difference for int2, not just its sign. Widen before subtracting.
func btint2cmp(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int32GetDatum(int32(fcinfo.GetArgInt16(0)) - int32(fcinfo.GetArgInt16(1)))
}

// nbtcompare.c:109-257. Mixed-width inputs retain their declared signedness.
func integerCompare[L, R integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int32GetDatum(int32(cmp.Compare(int64(integerArg[L](fcinfo, 0)), int64(integerArg[R](fcinfo, 1)))))
}

// nbtcompare.c:73. false sorts before true.
func btboolcmp(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	if fcinfo.GetArgBool(0) == fcinfo.GetArgBool(1) {
		return datum.Int32GetDatum(0)
	}
	if fcinfo.GetArgBool(0) {
		return datum.Int32GetDatum(1)
	}
	return datum.Int32GetDatum(-1)
}

// int.c:623-759, int8.c:401-432. These are scalar window-frame support
// functions, not window execution. Widening all arithmetic to int64 preserves
// the mathematical comparison: a sum beyond an int2/int4 bound is already
// beyond every possible val of that type. Only int64 overflow needs handling.
func integerInRange[V, O integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	val := int64(integerArg[V](fcinfo, 0))
	base := int64(integerArg[V](fcinfo, 1))
	offset := int64(integerArg[O](fcinfo, 2))
	sub, less := fcinfo.GetArgBool(3), fcinfo.GetArgBool(4)
	if offset < 0 {
		pgerror.Ereportf(mterrors.PgSSInvalidPrecedingOrFollowingSize,
			"invalid preceding or following size in window function")
	}
	if sub {
		offset = -offset // Nonnegative input means this cannot overflow.
	}
	sum := base + offset
	if (offset > 0 && sum < base) || (offset < 0 && sum > base) {
		// An overflowing addition is above every val; an overflowing
		// subtraction is below every val. This is not an arithmetic error.
		return datum.BoolGetDatum(less != sub)
	}
	if less {
		return datum.BoolGetDatum(val <= sum)
	}
	return datum.BoolGetDatum(val >= sum)
}
