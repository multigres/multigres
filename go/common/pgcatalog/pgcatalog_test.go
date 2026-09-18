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
package pgcatalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
)

func TestKnownRows(t *testing.T) {
	boolT := TypeByName("bool")
	require.NotNil(t, boolT)
	assert.Equal(t, &Type{
		Oid: ast.BOOLOID, Name: "bool", Len: 1, ByVal: true, Category: 'B',
		Preferred: true, Input: 1242, Output: 1243, Receive: 2436, Send: 2437,
		Array: 1000,
	}, boolT)
	assert.Same(t, boolT, TypeByOid(ast.BOOLOID))

	boolArr := TypeByName("_bool")
	require.NotNil(t, boolArr)
	assert.Equal(t, &Type{
		Oid: 1000, Name: "_bool", Len: -1, Category: 'A',
		Input: 750, Output: 751, Receive: 2400, Send: 2401, Elem: ast.BOOLOID,
	}, boolArr)

	text := TypeByName("text")
	require.NotNil(t, text)
	assert.Equal(t, int16(-1), text.Len, "text is a varlena")
	assert.False(t, text.ByVal)
	assert.Equal(t, Oid(100), text.Collation, "text uses the default collation")

	int4pl := ProcByOid(177)
	require.NotNil(t, int4pl)
	assert.Equal(t, &Proc{
		Oid: 177, Name: "int4pl", ArgTypes: []Oid{ast.INT4OID, ast.INT4OID},
		RetType: ast.INT4OID, Strict: true, Volatile: VolatilityImmutable,
		Kind: ProcKindFunction, Src: "int4pl",
	}, int4pl)

	// int2(int8) is an overload of "int2" whose prosrc differs from its
	// proname — proves both name collection and Src fidelity.
	overloads := ProcsByName("int2")
	var int2FromInt8 *Proc
	for _, p := range overloads {
		if len(p.ArgTypes) == 1 && p.ArgTypes[0] == ast.INT8OID {
			int2FromInt8 = p
		}
	}
	require.NotNil(t, int2FromInt8)
	assert.Equal(t, Oid(714), int2FromInt8.Oid)
	assert.Equal(t, "int82", int2FromInt8.Src)

	avg := ProcByOid(2100)
	require.NotNil(t, avg)
	assert.Equal(t, ProcKindAggregate, avg.Kind)
	assert.False(t, avg.Strict)
	assert.Equal(t, "aggregate_dummy", avg.Src)

	rowNumber := ProcByOid(3100)
	require.NotNil(t, rowNumber)
	assert.Equal(t, ProcKindWindow, rowNumber.Kind)
	assert.Empty(t, rowNumber.ArgTypes)

	plus := OperatorByOid(551)
	require.NotNil(t, plus)
	assert.Equal(t, &Operator{
		Oid: 551, Name: "+", Kind: OperatorKindInfix,
		Left: ast.INT4OID, Right: ast.INT4OID, Result: ast.INT4OID,
		Commutator: 551, Code: 177,
	}, plus, "int4 + int4 is its own commutator and is implemented by int4pl")

	neg := OperatorByOid(484)
	require.NotNil(t, neg)
	assert.Equal(t, OperatorKindPrefix, neg.Kind)
	assert.Zero(t, neg.Left, "prefix operators have no left operand")

	int8ToInt2 := LookupCast(ast.INT8OID, ast.INT2OID)
	require.NotNil(t, int8ToInt2)
	assert.Equal(t, Oid(714), int8ToInt2.Func)
	assert.Equal(t, CastContextAssignment, int8ToInt2.Context)
	assert.Equal(t, CastMethodFunction, int8ToInt2.Method)

	int4ToOid := LookupCast(ast.INT4OID, ast.OIDOID)
	require.NotNil(t, int4ToOid)
	assert.Zero(t, int4ToOid.Func)
	assert.Equal(t, CastMethodBinary, int4ToOid.Method)
}

func TestRowCounts(t *testing.T) {
	assert.Len(t, Types, 193, "112 base types + 81 synthesized array types (REL_17_6)")
	assert.Len(t, Procs, 3314)
	assert.Len(t, Operators, 799)
	assert.Len(t, Casts, 229)
}

// binaryCoercible is a lightweight version of opr_sanity's binary_coercible:
// the types are identical, or a binary-method cast leads from src to dst.
func binaryCoercible(src, dst Oid) bool {
	if src == dst {
		return true
	}
	c := LookupCast(src, dst)
	return c != nil && c.Method == CastMethodBinary
}

// TestCrossReferenceIntegrity verifies that every OID reference in the
// generated tables resolves within them, and that the pair-wise invariants
// PostgreSQL's own opr_sanity regression test enforces hold here too.
func TestCrossReferenceIntegrity(t *testing.T) {
	for i := range Types {
		typ := &Types[i]
		assert.NotNil(t, ProcByOid(typ.Input), "%s typinput", typ.Name)
		assert.NotNil(t, ProcByOid(typ.Output), "%s typoutput", typ.Name)
		if typ.Receive != 0 {
			assert.NotNil(t, ProcByOid(typ.Receive), "%s typreceive", typ.Name)
		}
		if typ.Send != 0 {
			assert.NotNil(t, ProcByOid(typ.Send), "%s typsend", typ.Name)
		}
		if typ.Elem != 0 {
			assert.NotNil(t, TypeByOid(typ.Elem), "%s typelem", typ.Name)
		}
		if typ.Array != 0 {
			arr := TypeByOid(typ.Array)
			require.NotNil(t, arr, "%s typarray", typ.Name)
			assert.Equal(t, typ.Oid, arr.Elem, "array type %s must point back to %s", arr.Name, typ.Name)
		}
	}

	for i := range Procs {
		p := &Procs[i]
		assert.NotNil(t, TypeByOid(p.RetType), "%s prorettype", p.Name)
		for _, arg := range p.ArgTypes {
			assert.NotNil(t, TypeByOid(arg), "%s proargtypes", p.Name)
		}
		assert.Contains(t, []byte{VolatilityImmutable, VolatilityStable, VolatilityVolatile}, p.Volatile, p.Name)
		assert.Contains(t, []byte{ProcKindFunction, ProcKindProcedure, ProcKindAggregate, ProcKindWindow}, p.Kind, p.Name)
	}

	for i := range Operators {
		o := &Operators[i]
		if o.Kind == OperatorKindPrefix {
			assert.Zero(t, o.Left, "prefix operator %d", o.Oid)
		} else {
			assert.NotNil(t, TypeByOid(o.Left), "operator %d oprleft", o.Oid)
		}
		assert.NotNil(t, TypeByOid(o.Right), "operator %d oprright", o.Oid)
		assert.NotNil(t, TypeByOid(o.Result), "operator %d oprresult", o.Oid)
		code := ProcByOid(o.Code)
		require.NotNil(t, code, "operator %d oprcode", o.Oid)
		assert.Equal(t, o.Result, code.RetType, "operator %d result must match its function", o.Oid)
		if o.Commutator != 0 {
			com := OperatorByOid(o.Commutator)
			require.NotNil(t, com, "operator %d oprcom", o.Oid)
			assert.Equal(t, o.Oid, com.Commutator, "commutators must be mutual: %d <-> %d", o.Oid, com.Oid)
		}
		if o.Negator != 0 {
			negator := OperatorByOid(o.Negator)
			require.NotNil(t, negator, "operator %d oprnegate", o.Oid)
			assert.Equal(t, o.Oid, negator.Negator, "negators must be mutual: %d <-> %d", o.Oid, negator.Oid)
		}
	}

	for i := range Casts {
		c := &Casts[i]
		assert.NotNil(t, TypeByOid(c.Source), "cast source %d", c.Source)
		assert.NotNil(t, TypeByOid(c.Target), "cast target %d", c.Target)
		if c.Method == CastMethodFunction {
			fn := ProcByOid(c.Func)
			require.NotNil(t, fn, "cast %d->%d castfunc", c.Source, c.Target)
			// opr_sanity's rule: the function's result must be the target
			// type or binary-coercible to it (e.g. casts to varchar/bpchar
			// use text-returning functions).
			assert.True(t, binaryCoercible(fn.RetType, c.Target),
				"cast %d->%d function returns %d, which is not coercible to the target", c.Source, c.Target, fn.RetType)
		} else {
			assert.Zero(t, c.Func, "non-function cast %d->%d must have no castfunc", c.Source, c.Target)
		}
	}
}
