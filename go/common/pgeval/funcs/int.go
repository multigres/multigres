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

// Package funcs implements PostgreSQL builtins for the evaluation engine.
// Import it for its init-time registrations, then resolve functions by OID
// through fmgr.FmgrInfoFor. The strict scalar bodies assume correctly typed,
// non-NULL arguments; fmgr.CallFunction applies catalog strictness before entry.
//
// The integer operations port postgres/src/backend/utils/adt/int.c and int8.c
// at REL_17_6. Type parameters share the identical bodies across integer widths:
// L and R are the declared input types, O is PostgreSQL's result type. Arithmetic
// is performed at O's width, not at a Go int's platform-dependent width.
package funcs

import (
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

type integer interface {
	int16 | int32 | int64
}

func init() {
	registerIntegerPair[int16, int16, int16]("int2")
	registerIntegerPair[int32, int32, int32]("int4")
	registerIntegerPair[int64, int64, int64]("int8")
	registerIntegerPair[int16, int32, int32]("int24")
	registerIntegerPair[int32, int16, int32]("int42")
	registerIntegerPair[int16, int64, int64]("int28")
	registerIntegerPair[int64, int16, int64]("int82")
	registerIntegerPair[int32, int64, int64]("int48")
	registerIntegerPair[int64, int32, int64]("int84")
	registerInteger[int16]("int2")
	registerInteger[int32]("int4")
	registerInteger[int64]("int8")

	fmgr.RegisterBuiltin("i2toi4", integerCast[int16, int32])
	fmgr.RegisterBuiltin("i4toi2", integerCast[int32, int16])
	fmgr.RegisterBuiltin("int28", integerCast[int16, int64])
	fmgr.RegisterBuiltin("int82", integerCast[int64, int16])
	fmgr.RegisterBuiltin("int48", integerCast[int32, int64])
	fmgr.RegisterBuiltin("int84", integerCast[int64, int32])
	fmgr.RegisterBuiltin("int4inc", integerInc[int32])
	fmgr.RegisterBuiltin("int8inc", integerInc[int64])
	fmgr.RegisterBuiltin("int8dec", integerDec[int64])
	fmgr.RegisterBuiltin("int4gcd", integerGCD[int32])
	fmgr.RegisterBuiltin("int8gcd", integerGCD[int64])
	fmgr.RegisterBuiltin("int4lcm", integerLCM[int32])
	fmgr.RegisterBuiltin("int8lcm", integerLCM[int64])
}

func registerIntegerPair[L, R, O integer](prefix string) {
	fmgr.RegisterBuiltin(prefix+"eq", integerEq[L, R])
	fmgr.RegisterBuiltin(prefix+"ne", integerNe[L, R])
	fmgr.RegisterBuiltin(prefix+"lt", integerLt[L, R])
	fmgr.RegisterBuiltin(prefix+"le", integerLe[L, R])
	fmgr.RegisterBuiltin(prefix+"gt", integerGt[L, R])
	fmgr.RegisterBuiltin(prefix+"ge", integerGe[L, R])
	fmgr.RegisterBuiltin(prefix+"pl", integerAdd[L, R, O])
	fmgr.RegisterBuiltin(prefix+"mi", integerSub[L, R, O])
	fmgr.RegisterBuiltin(prefix+"mul", integerMul[L, R, O])
	fmgr.RegisterBuiltin(prefix+"div", integerDiv[L, R, O])
}

func registerInteger[T integer](prefix string) {
	fmgr.RegisterBuiltin(prefix+"um", integerNegate[T])
	fmgr.RegisterBuiltin(prefix+"up", integerIdentity[T])
	fmgr.RegisterBuiltin(prefix+"abs", integerAbs[T])
	fmgr.RegisterBuiltin(prefix+"mod", integerMod[T])
	fmgr.RegisterBuiltin(prefix+"larger", integerLarger[T])
	fmgr.RegisterBuiltin(prefix+"smaller", integerSmaller[T])
	fmgr.RegisterBuiltin(prefix+"and", integerAnd[T])
	fmgr.RegisterBuiltin(prefix+"or", integerOr[T])
	fmgr.RegisterBuiltin(prefix+"xor", integerXor[T])
	fmgr.RegisterBuiltin(prefix+"not", integerNot[T])
	fmgr.RegisterBuiltin(prefix+"shl", integerShiftLeft[T])
	fmgr.RegisterBuiltin(prefix+"shr", integerShiftRight[T])
	fmgr.RegisterBuiltin(prefix+"in", integerIn[T])
	fmgr.RegisterBuiltin(prefix+"out", integerOut[T])
}

// Datum's signed integer constructors all sign-extend into the same uint64
// slot. Truncating to T here is equivalent to the respective PG_GETARG_INT*;
// returning Int64GetDatum(int64(value)) likewise matches each PG_RETURN_INT*.
func integerArg[T integer](fcinfo fmgr.FunctionCallInfo, n int) T {
	return T(datum.DatumGetInt64(fcinfo.Arg(n)))
}

func integerType[T integer]() (name string, bits int) {
	switch any(T(0)).(type) {
	case int16:
		return "smallint", 16
	case int32:
		return "integer", 32
	default:
		return "bigint", 64
	}
}

func integerOverflow[T integer]() {
	name, _ := integerType[T]()
	pgerror.Ereportf(mterrors.PgSSNumericValueOutOfRange, "%s out of range", name)
}

// Comparisons: int.c:396-609, int8.c:113-393. Widen only after reading each
// argument at its own width, so mixed-width negative values retain their sign.
func integerEq[L, R integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(int64(integerArg[L](fcinfo, 0)) == int64(integerArg[R](fcinfo, 1)))
}

func integerNe[L, R integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(int64(integerArg[L](fcinfo, 0)) != int64(integerArg[R](fcinfo, 1)))
}

func integerLt[L, R integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(int64(integerArg[L](fcinfo, 0)) < int64(integerArg[R](fcinfo, 1)))
}

func integerLe[L, R integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(int64(integerArg[L](fcinfo, 0)) <= int64(integerArg[R](fcinfo, 1)))
}

func integerGt[L, R integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(int64(integerArg[L](fcinfo, 0)) > int64(integerArg[R](fcinfo, 1)))
}

func integerGe[L, R integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BoolGetDatum(int64(integerArg[L](fcinfo, 0)) >= int64(integerArg[R](fcinfo, 1)))
}

// checkedAdd/Sub/Mul implement common/int.h's pg_*_s{16,32,64}_overflow.
// Go defines signed overflow as wrapping, unlike C, so signs and the inverse
// operation can detect overflow without a wider type (including for int64).
func checkedAdd[T integer](a, b T) T {
	result := a + b
	if (a < 0) == (b < 0) && (result < 0) != (a < 0) {
		integerOverflow[T]()
	}
	return result
}

func checkedSub[T integer](a, b T) T {
	result := a - b
	if (a < 0) != (b < 0) && (result < 0) != (a < 0) {
		integerOverflow[T]()
	}
	return result
}

func checkedMul[T integer](a, b T) T {
	result := a * b
	// MinInt / -1 wraps in Go too; don't let that hide (-1) * MinInt.
	if a != 0 && (result/a != b || a == -1 && b < 0 && result < 0) {
		integerOverflow[T]()
	}
	return result
}

// Arithmetic: int.c:791-1127, int8.c:462-1170. Mixed-width functions promote
// to the larger declared type; int2 / int4 cannot overflow merely because its
// dividend is the minimum int2 value.
func integerAdd[L, R, O integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	result := checkedAdd(O(integerArg[L](fcinfo, 0)), O(integerArg[R](fcinfo, 1)))
	return datum.Int64GetDatum(int64(result))
}

func integerSub[L, R, O integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	result := checkedSub(O(integerArg[L](fcinfo, 0)), O(integerArg[R](fcinfo, 1)))
	return datum.Int64GetDatum(int64(result))
}

func integerMul[L, R, O integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	result := checkedMul(O(integerArg[L](fcinfo, 0)), O(integerArg[R](fcinfo, 1)))
	return datum.Int64GetDatum(int64(result))
}

func integerDiv[L, R, O integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	a, b := O(integerArg[L](fcinfo, 0)), O(integerArg[R](fcinfo, 1))
	if b == 0 {
		pgerror.Ereportf(mterrors.PgSSDivisionByZero, "division by zero")
	}
	if b == -1 && a < 0 && -a == a {
		integerOverflow[O]()
	}
	return datum.Int64GetDatum(int64(a / b))
}

// Unary operators, modulo, abs: int.c:771-789,886-910,1130-1219;
// int8.c:440-460,546-591. Unlike division, MinInt % -1 is valid and returns 0.
func integerNegate[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	a := integerArg[T](fcinfo, 0)
	if a < 0 && -a == a {
		integerOverflow[T]()
	}
	return datum.Int64GetDatum(int64(-a))
}

func integerIdentity[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(integerArg[T](fcinfo, 0)))
}

func integerAbs[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	a := integerArg[T](fcinfo, 0)
	if a < 0 {
		return integerNegate[T](fcinfo)
	}
	return datum.Int64GetDatum(int64(a))
}

func integerMod[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	a, b := integerArg[T](fcinfo, 0), integerArg[T](fcinfo, 1)
	if b == 0 {
		pgerror.Ereportf(mterrors.PgSSDivisionByZero, "division by zero")
	}
	return datum.Int64GetDatum(int64(a % b))
}

// Increment/decrement: int.c:872, int8.c:719-794. Our int8 is always by-value,
// so PG's by-reference aggregate-state optimization does not apply.
func integerInc[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(checkedAdd(integerArg[T](fcinfo, 0), 1)))
}

func integerDec[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(checkedSub(integerArg[T](fcinfo, 0), 1)))
}

// Casts: int.c:342-358, int8.c:1241-1279. Narrowing must error, not truncate.
func integerCast[From, To integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	arg := integerArg[From](fcinfo, 0)
	result := To(arg)
	if int64(result) != int64(arg) {
		integerOverflow[To]()
	}
	return datum.Int64GetDatum(int64(result))
}

// Min/max: int.c:1346-1380, int8.c:866-889.
func integerLarger[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(max(integerArg[T](fcinfo, 0), integerArg[T](fcinfo, 1))))
}

func integerSmaller[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(min(integerArg[T](fcinfo, 0), integerArg[T](fcinfo, 1))))
}

// Bitwise operators: int.c:1393-1500, int8.c:1184-1235.
func integerAnd[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(integerArg[T](fcinfo, 0) & integerArg[T](fcinfo, 1)))
}

func integerOr[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(integerArg[T](fcinfo, 0) | integerArg[T](fcinfo, 1)))
}

func integerXor[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(integerArg[T](fcinfo, 0) ^ integerArg[T](fcinfo, 1)))
}

func integerNot[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(^integerArg[T](fcinfo, 0)))
}

// PG 17 uses native C shifts; int2 is promoted to int32 before shifting.
// Masks match PG on amd64/arm64; validate native shifts before adding another
// architecture, since C leaves negative/oversized counts undefined.
func integerShiftCount[T integer](fcinfo fmgr.FunctionCallInfo) uint32 {
	_, width := integerType[T]()
	return uint32(fcinfo.GetArgInt32(1)) & uint32(max(32, width)-1)
}

func integerShiftLeft[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(T(int64(integerArg[T](fcinfo, 0)) << integerShiftCount[T](fcinfo))))
}

func integerShiftRight[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(T(int64(integerArg[T](fcinfo, 0)) >> integerShiftCount[T](fcinfo))))
}

// gcd/lcm: int.c:1233-1343, int8.c:611-716. Work in negative space to retain
// MinInt's magnitude until the final representability check, as upstream does.
func integerGCDValue[T integer](a, b T) T {
	if a > 0 {
		a = -a
	}
	if b > 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 && -a == a {
		integerOverflow[T]()
	}
	return -a
}

func integerGCD[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(integerGCDValue(integerArg[T](fcinfo, 0), integerArg[T](fcinfo, 1))))
}

func integerLCM[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	a, b := integerArg[T](fcinfo, 0), integerArg[T](fcinfo, 1)
	if a == 0 || b == 0 {
		return datum.Int64GetDatum(0)
	}
	result := checkedMul(a/integerGCDValue(a, b), b)
	if result < 0 {
		if -result == result {
			integerOverflow[T]()
		}
		result = -result
	}
	return datum.Int64GetDatum(int64(result))
}
