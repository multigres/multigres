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

package fmgr_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// Builtin OIDs referenced from the vendored REL_17_6 catalog.
const (
	oidInt4pl = 177  // int4pl(int4,int4), strict, prosrc "int4pl"
	oidAvg    = 2100 // avg(int8), aggregate, prosrc "aggregate_dummy"
)

// testAdd is a stand-in strict int4pl: it reads two int32 args and returns
// their sum. These tests exercise the calling machinery without importing
// funcs, so its real builtin registrations do not populate this test registry.
func testAdd(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	a := fcinfo.GetArgInt32(0)
	b := fcinfo.GetArgInt32(1)
	return datum.Int32GetDatum(a + b)
}

// testReturnsNull sets the out-of-band NULL flag, like PG_RETURN_NULL.
func testReturnsNull(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return fcinfo.ReturnNull()
}

func strictInfo(fn fmgr.PGFunction) *fmgr.FmgrInfo {
	return &fmgr.FmgrInfo{FnAddr: fn, FnNargs: 2, FnStrict: true}
}

func TestCallFunctionStrictShortCircuit(t *testing.T) {
	called := false
	fn := func(fcinfo fmgr.FunctionCallInfo) datum.Datum {
		called = true
		return datum.Int32GetDatum(0)
	}
	fcinfo := fmgr.NewFunctionCallInfo(strictInfo(fn), 2, pgoid.InvalidOid)
	fcinfo.Args[0] = datum.NullableDatum{Value: datum.Int32GetDatum(5)}
	fcinfo.Args[1] = datum.NullableDatum{IsNull: true}

	result := fmgr.CallFunction(fcinfo)
	assert.False(t, called, "strict function must not be called when an arg is NULL")
	assert.True(t, fcinfo.IsNull, "result must be NULL")
	assert.Equal(t, datum.Datum{}, result)
}

func TestCallFunctionStrictAllNonNull(t *testing.T) {
	fcinfo := fmgr.NewFunctionCallInfo(strictInfo(testAdd), 2, pgoid.InvalidOid)
	fcinfo.Args[0] = datum.NullableDatum{Value: datum.Int32GetDatum(2)}
	fcinfo.Args[1] = datum.NullableDatum{Value: datum.Int32GetDatum(3)}

	result := fmgr.CallFunction(fcinfo)
	assert.False(t, fcinfo.IsNull)
	assert.Equal(t, int32(5), datum.DatumGetInt32(result))
}

// TestCallFunctionNonStrictSeesNull proves a non-strict function IS invoked
// with a NULL argument (no short-circuit), and can produce a NULL result.
func TestCallFunctionNonStrictSeesNull(t *testing.T) {
	info := &fmgr.FmgrInfo{FnAddr: testReturnsNull, FnNargs: 1, FnStrict: false}
	fcinfo := fmgr.NewFunctionCallInfo(info, 1, pgoid.InvalidOid)
	fcinfo.Args[0] = datum.NullableDatum{IsNull: true}

	result := fmgr.CallFunction(fcinfo)
	assert.True(t, fcinfo.IsNull)
	assert.Equal(t, datum.Datum{}, result)
}

// TestCallFunctionPropagatesEreport shows an ereport panic from a body flows
// through CallFunction unmodified and is caught only at the Recover boundary.
func TestCallFunctionPropagatesEreport(t *testing.T) {
	fn := func(fmgr.FunctionCallInfo) datum.Datum {
		pgerror.Ereportf("22012", "division by zero")
		return datum.Datum{}
	}
	fcinfo := fmgr.NewFunctionCallInfo(&fmgr.FmgrInfo{FnAddr: fn, FnNargs: 0}, 0, pgoid.InvalidOid)

	err := pgerror.Recover(func() { fmgr.CallFunction(fcinfo) })
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, "22012", diag.Code)
}

func TestFunctionCall2Coll(t *testing.T) {
	info := &fmgr.FmgrInfo{FnAddr: testAdd, FnOid: oidInt4pl, FnNargs: 2, FnStrict: true}
	result := fmgr.FunctionCall2Coll(info, pgoid.InvalidOid,
		datum.Int32GetDatum(40), datum.Int32GetDatum(2))
	assert.Equal(t, int32(42), datum.DatumGetInt32(result))
}

// TestFunctionCall2CollNullResult verifies the "returned NULL" internal-error
// guard fires when a non-null-args convenience caller gets a NULL back.
func TestFunctionCall2CollNullResult(t *testing.T) {
	info := &fmgr.FmgrInfo{FnAddr: testReturnsNull, FnOid: oidInt4pl, FnNargs: 2}
	err := pgerror.Recover(func() {
		fmgr.FunctionCall2Coll(info, pgoid.InvalidOid,
			datum.Int32GetDatum(1), datum.Int32GetDatum(2))
	})
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, mterrors.PgSSInternalError, diag.Code)
	assert.Contains(t, diag.Message, "returned NULL")
}

func TestDirectFunctionCall2Coll(t *testing.T) {
	result := fmgr.DirectFunctionCall2Coll(testAdd, pgoid.InvalidOid,
		datum.Int32GetDatum(7), datum.Int32GetDatum(8))
	assert.Equal(t, int32(15), datum.DatumGetInt32(result))
}

func TestAccessors(t *testing.T) {
	fcinfo := &fmgr.FunctionCallInfoBaseData{
		Fncollation: 100,
		Nargs:       3,
		Args: []datum.NullableDatum{
			{Value: datum.Int32GetDatum(-7)},
			{IsNull: true},
			{Value: datum.BoolGetDatum(true)},
		},
	}
	assert.Equal(t, int32(-7), fcinfo.GetArgInt32(0))
	assert.False(t, fcinfo.ArgIsNull(0))
	assert.True(t, fcinfo.ArgIsNull(1))
	assert.True(t, fcinfo.GetArgBool(2))
	assert.Equal(t, datum.Oid(100), fcinfo.Collation())

	// ReturnNull sets the out-of-band flag.
	got := fcinfo.ReturnNull()
	assert.True(t, fcinfo.IsNull)
	assert.Equal(t, datum.Datum{}, got)
}

// TestFmgrInfoForStub resolves a real builtin whose body is not registered: the
// metadata comes from the catalog, and calling the stub raises
// feature_not_supported naming the function.
func TestFmgrInfoForStub(t *testing.T) {
	info, err := fmgr.FmgrInfoFor(oidInt4pl)
	require.NoError(t, err)
	assert.Equal(t, datum.Oid(oidInt4pl), info.FnOid)
	assert.Equal(t, int16(2), info.FnNargs, "int4pl takes two args")
	assert.True(t, info.FnStrict, "int4pl is strict")

	fcinfo := fmgr.NewFunctionCallInfo(info, 2, pgoid.InvalidOid)
	fcinfo.Args[0] = datum.NullableDatum{Value: datum.Int32GetDatum(1)}
	fcinfo.Args[1] = datum.NullableDatum{Value: datum.Int32GetDatum(2)}
	callErr := pgerror.Recover(func() { fmgr.CallFunction(fcinfo) })
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(callErr, &diag))
	assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
	assert.Contains(t, diag.Message, "int4pl")
	assert.Contains(t, diag.Message, "not yet implemented")
}

// TestFmgrInfoForAggregateStub verifies an aggregate resolves to the distinct
// "called as a scalar function" error, not a generic not-implemented one.
func TestFmgrInfoForAggregateStub(t *testing.T) {
	info, err := fmgr.FmgrInfoFor(oidAvg)
	require.NoError(t, err)
	fcinfo := fmgr.NewFunctionCallInfo(info, int(info.FnNargs), pgoid.InvalidOid)
	callErr := pgerror.Recover(func() { fmgr.CallFunction(fcinfo) })
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(callErr, &diag))
	assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
	assert.Contains(t, diag.Message, "aggregate function avg")
	assert.Contains(t, diag.Message, "called as a scalar")
}

func TestFmgrInfoForUndefined(t *testing.T) {
	_, err := fmgr.FmgrInfoFor(999999999)
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, mterrors.PgSSUndefinedFunction, diag.Code)
}
