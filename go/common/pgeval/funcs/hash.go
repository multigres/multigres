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
	"runtime"

	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pghash"
)

func init() {
	fmgr.RegisterBuiltin("hashint2", integerHash[int16])
	fmgr.RegisterBuiltin("hashint4", integerHash[int32])
	fmgr.RegisterBuiltin("hashint8", integerHash[int64])
	fmgr.RegisterBuiltin("hashint2extended", integerHashExtended[int16])
	fmgr.RegisterBuiltin("hashint4extended", integerHashExtended[int32])
	fmgr.RegisterBuiltin("hashint8extended", integerHashExtended[int64])
	// PG uses plain C char for this shared char/boolean support routine.
	// Of our linux/darwin × amd64/arm64 targets, Linux ARM64 defaults to
	// unsigned char; the others default to signed char. Bool's 0/1 values
	// hash identically everywhere. Non-default C compiler flags are outside
	// this compatibility target; validate the ABI when adding architectures.
	if runtime.GOOS == "linux" && runtime.GOARCH == "arm64" {
		fmgr.RegisterBuiltin("hashchar", integerHash[uint8])
		fmgr.RegisterBuiltin("hashcharextended", integerHashExtended[uint8])
	} else {
		fmgr.RegisterBuiltin("hashchar", integerHash[int8])
		fmgr.RegisterBuiltin("hashcharextended", integerHashExtended[int8])
	}
}

type hashInteger interface {
	int8 | uint8 | integer
}

// integerHashKey is the key PG hashes for an integer argument of any width;
// see pghash.Int8Key, which owns the fold shared with shard indexes.
func integerHashKey[T hashInteger](fcinfo fmgr.FunctionCallInfo) uint32 {
	return pghash.Int8Key(int64(T(fcinfo.GetArgInt64(0))))
}

func integerHash[T hashInteger](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int32GetDatum(int32(pghash.Uint32Extended(integerHashKey[T](fcinfo), 0)))
}

func integerHashExtended[T hashInteger](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(pghash.Uint32Extended(integerHashKey[T](fcinfo), uint64(fcinfo.GetArgInt64(1)))))
}
