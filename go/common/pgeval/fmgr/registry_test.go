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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

type testProcResolver map[Oid]*pgcatalog.Proc

func (r testProcResolver) ProcByOid(oid Oid) *pgcatalog.Proc { return r[oid] }

// This test uses the internal package so its global registrations can be
// restored without adding a production unregister API. It must not run in parallel.
func TestRegisterBuiltin(t *testing.T) {
	oldBuiltins, oldResolver := builtins, procResolver
	builtins = map[string]PGFunction{}
	t.Cleanup(func() { builtins, procResolver = oldBuiltins, oldResolver })

	const oidInt4pl = 177
	add := func(fcinfo FunctionCallInfo) datum.Datum {
		return datum.Int32GetDatum(fcinfo.GetArgInt32(0) + fcinfo.GetArgInt32(1))
	}
	require.False(t, IsBuiltinRegistered("int4pl"))
	RegisterBuiltin("int4pl", add)
	assert.True(t, IsBuiltinRegistered("int4pl"))
	assert.Contains(t, RegisteredBuiltins(), "int4pl")

	// Duplicate registration is a startup bug and must panic.
	assert.Panics(t, func() { RegisterBuiltin("int4pl", add) })
	assert.Panics(t, func() { RegisterBuiltin("", add) })

	// Now FmgrInfoFor(177) resolves to the registered impl, not a stub.
	info, err := FmgrInfoFor(oidInt4pl)
	require.NoError(t, err)
	fcinfo := NewFunctionCallInfo(info, 2, pgoid.InvalidOid)
	fcinfo.Args[0] = datum.NullableDatum{Value: datum.Int32GetDatum(20)}
	fcinfo.Args[1] = datum.NullableDatum{Value: datum.Int32GetDatum(22)}
	assert.Equal(t, int32(42), datum.DatumGetInt32(CallFunction(fcinfo)))

	t.Run("user-defined function cannot reuse builtin implementation", func(t *testing.T) {
		const oidUser = 90000
		require.Nil(t, pgcatalog.ProcByOid(oidUser))
		proc := *pgcatalog.ProcByOid(oidInt4pl)
		proc.Oid = oidUser
		proc.Name = "user_add"
		proc.Strict = false
		SetProcResolver(testProcResolver{oidUser: &proc})

		info, err := FmgrInfoFor(oidUser)
		require.NoError(t, err)
		assert.Equal(t, Oid(oidUser), info.FnOid)
		assert.Equal(t, int16(2), info.FnNargs)
		assert.False(t, info.FnStrict, "user-defined metadata must still be available")

		callErr := pgerror.Recover(func() {
			FunctionCall2Coll(info, pgoid.InvalidOid,
				datum.Int32GetDatum(20), datum.Int32GetDatum(22))
		})
		var diag *mterrors.PgDiagnostic
		require.ErrorAs(t, callErr, &diag)
		assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
		assert.Contains(t, diag.Message, proc.Name)
	})
}
