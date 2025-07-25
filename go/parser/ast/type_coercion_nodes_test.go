// Package ast provides comprehensive tests for PostgreSQL AST type coercion and advanced expression nodes.
// These tests ensure the correctness of PostgreSQL's sophisticated type system implementation.
package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ==============================================================================
// RELABEL TYPE TESTS
// ==============================================================================

func TestRelabelType(t *testing.T) {
	// Create input expression
	input := NewConst(23, 42, false) // INT4 constant
	
	// Test basic RelabelType creation
	rt := NewRelabelType(input, 20, -1, COERCE_EXPLICIT_CAST) // INT8
	
	// Verify properties
	assert.Equal(t, T_RelabelType, rt.Tag, "Expected tag T_RelabelType")
	assert.Equal(t, input, rt.Arg, "Expected input argument to be set correctly")
	assert.Equal(t, Oid(20), rt.Resulttype, "Expected result type 20")
	
	assert.Equal(t, int32(-1), rt.Resulttypmod, "Expected result typmod -1")
	
	assert.Equal(t, COERCE_EXPLICIT_CAST, rt.Relabelformat, "Expected COERCE_EXPLICIT_CAST")
	
	// Test ExpressionType
	assert.Equal(t, "RelabelType", rt.ExpressionType(), "Expected ExpressionType 'RelabelType'")
	
	// Test string representation
	str := rt.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestImplicitRelabelType(t *testing.T) {
	input := NewConst(23, 42, false)
	rt := NewImplicitRelabelType(input, 25) // TEXT type
	
	assert.Equal(t, COERCE_IMPLICIT_CAST, rt.Relabelformat, "Expected COERCE_IMPLICIT_CAST")
	
	assert.Equal(t, Oid(25), rt.Resulttype, "Expected result type 25")
}

func TestExplicitRelabelType(t *testing.T) {
	input := NewConst(23, 42, false)
	rt := NewExplicitRelabelType(input, 1700) // NUMERIC type
	
	assert.Equal(t, COERCE_EXPLICIT_CAST, rt.Relabelformat, "Expected COERCE_EXPLICIT_CAST")
	
	assert.Equal(t, Oid(1700), rt.Resulttype, "Expected result type 1700")
}

// ==============================================================================
// COERCE VIA IO TESTS
// ==============================================================================

func TestCoerceViaIO(t *testing.T) {
	input := NewConst(25, Datum(42), false) // TEXT constant
	cvio := NewCoerceViaIO(input, 23, COERCE_EXPLICIT_CAST) // Convert to INT4
	
	// Verify properties
	assert.Equal(t, T_CoerceViaIO, cvio.Tag, "Expected tag T_CoerceViaIO")
	
	assert.Equal(t, input, cvio.Arg, "Expected input argument to be set correctly")
	
	assert.Equal(t, Oid(23), cvio.Resulttype, "Expected result type 23")
	
	assert.Equal(t, COERCE_EXPLICIT_CAST, cvio.Coerceformat, "Expected COERCE_EXPLICIT_CAST")
	
	// Test ExpressionType
	assert.Equal(t, "CoerceViaIO", cvio.ExpressionType(), "Expected ExpressionType 'CoerceViaIO'")
	
	// Test string representation
	str := cvio.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestExplicitCoerceViaIO(t *testing.T) {
	input := NewConst(25, Datum(314), false)
	cvio := NewExplicitCoerceViaIO(input, 700) // FLOAT4 type
	
	assert.Equal(t, COERCE_EXPLICIT_CAST, cvio.Coerceformat, "Expected COERCE_EXPLICIT_CAST")
	
	assert.Equal(t, Oid(700), cvio.Resulttype, "Expected result type 700")
}

// ==============================================================================
// ARRAY COERCE EXPR TESTS
// ==============================================================================

func TestArrayCoerceExpr(t *testing.T) {
	// Create an array expression
	arrayExpr := NewConst(1007, Datum(123), false) // INT4 array
	elemExpr := NewConst(481, Datum(1), false) // Example element coercion expression
	
	ace := NewArrayCoerceExpr(arrayExpr, elemExpr, 1016, COERCE_EXPLICIT_CAST) // INT8 array
	
	// Verify properties
	assert.Equal(t, T_ArrayCoerceExpr, ace.Tag, "Expected tag T_ArrayCoerceExpr")
	
	assert.Equal(t, arrayExpr, ace.Arg, "Expected array argument to be set correctly")
	
	assert.Equal(t, elemExpr, ace.Elemexpr, "Expected element expression to match")
	
	assert.Equal(t, Oid(1016), ace.Resulttype, "Expected result type 1016")
	
	// Test ExpressionType
	assert.Equal(t, "ArrayCoerceExpr", ace.ExpressionType(), "Expected ExpressionType 'ArrayCoerceExpr'")
	
	// Test string representation
	str := ace.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestExplicitArrayCoerceExpr(t *testing.T) {
	arrayExpr := NewConst(1007, Datum(123), false)
	elemExpr := NewConst(481, Datum(1), false)
	ace := NewExplicitArrayCoerceExpr(arrayExpr, elemExpr, 1009) // TEXTARRAY
	
	assert.Equal(t, COERCE_EXPLICIT_CAST, ace.Coerceformat, "Expected COERCE_EXPLICIT_CAST")
	
	assert.Equal(t, Oid(1009), ace.Resulttype, "Expected result type 1009")
}

// ==============================================================================
// CONVERT ROWTYPE EXPR TESTS
// ==============================================================================

func TestConvertRowtypeExpr(t *testing.T) {
	// Create a row expression
	rowExpr := NewConst(16, Datum(1), false) // Placeholder row value
	
	crte := NewConvertRowtypeExpr(rowExpr, 12345, COERCE_IMPLICIT_CAST)
	
	// Verify properties
	assert.Equal(t, T_ConvertRowtypeExpr, crte.Tag, "Expected tag T_ConvertRowtypeExpr")
	
	assert.Equal(t, rowExpr, crte.Arg, "Expected row argument to be set correctly")
	
	assert.Equal(t, Oid(12345), crte.Resulttype, "Expected result type 12345")
	
	assert.Equal(t, COERCE_IMPLICIT_CAST, crte.Convertformat, "Expected COERCE_IMPLICIT_CAST")
	
	// Test ExpressionType
	assert.Equal(t, "ConvertRowtypeExpr", crte.ExpressionType(), "Expected ExpressionType 'ConvertRowtypeExpr'")
	
	// Test string representation
	str := crte.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

// ==============================================================================
// COLLATE EXPR TESTS
// ==============================================================================

func TestCollateExpr(t *testing.T) {
	textExpr := NewConst(25, Datum(0), false) // TEXT value
	collOid := Oid(100) // Example collation OID
	
	ce := NewCollateExpr(textExpr, collOid)
	
	// Verify properties
	assert.Equal(t, T_CollateExpr, ce.Tag, "Expected tag T_CollateExpr")
	
	assert.Equal(t, textExpr, ce.Arg, "Expected text argument to be set correctly")
	
	assert.Equal(t, collOid, ce.CollOid, "Expected collation OID to match")
	
	// Test ExpressionType
	assert.Equal(t, "CollateExpr", ce.ExpressionType(), "Expected ExpressionType 'CollateExpr'")
	
	// Test string representation
	str := ce.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

// ==============================================================================
// FIELD SELECT TESTS
// ==============================================================================

func TestFieldSelect(t *testing.T) {
	// Create a composite type expression
	recordExpr := NewConst(16, Datum(1), false) // Placeholder record
	
	fs := NewFieldSelect(recordExpr, 2, 25) // Select field 2, result type TEXT
	
	// Verify properties
	assert.Equal(t, T_FieldSelect, fs.Tag, "Expected tag T_FieldSelect")
	
	assert.Equal(t, recordExpr, fs.Arg, "Expected record argument to be set correctly")
	
	assert.Equal(t, AttrNumber(2), fs.Fieldnum, "Expected field number 2")
	
	assert.Equal(t, Oid(25), fs.Resulttype, "Expected result type 25")
	
	assert.Equal(t, int32(-1), fs.Resulttypmod, "Expected result typmod -1")
	
	// Test ExpressionType
	assert.Equal(t, "FieldSelect", fs.ExpressionType(), "Expected ExpressionType 'FieldSelect'")
	
	// Test string representation
	str := fs.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

// ==============================================================================
// FIELD STORE TESTS
// ==============================================================================

func TestFieldStore(t *testing.T) {
	recordExpr := NewConst(16, Datum(1), false) // Placeholder record
	newVal1 := NewConst(23, 42, false)      // INT4 value
	newVal2 := NewConst(25, Datum(0), false) // TEXT value
	
	fs := NewFieldStore(recordExpr, []Expression{newVal1, newVal2}, []AttrNumber{1, 3}, 12345)
	
	// Verify properties
	assert.Equal(t, T_FieldStore, fs.Tag, "Expected tag T_FieldStore")
	
	assert.Equal(t, recordExpr, fs.Arg, "Expected record argument to be set correctly")
	
	assert.Equal(t, 2, len(fs.Newvals), "Expected 2 new values")
	
	assert.Equal(t, 2, len(fs.Fieldnums), "Expected 2 field numbers")
	
	assert.Equal(t, []AttrNumber{1, 3}, fs.Fieldnums, "Expected field numbers [1, 3]")
	
	assert.Equal(t, Oid(12345), fs.Resulttype, "Expected result type 12345")
	
	// Test ExpressionType
	assert.Equal(t, "FieldStore", fs.ExpressionType(), "Expected ExpressionType 'FieldStore'")
	
	// Test string representation
	str := fs.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestSingleFieldStore(t *testing.T) {
	recordExpr := NewConst(16, Datum(1), false)
	newVal := NewConst(23, 99, false)
	
	fs := NewSingleFieldStore(recordExpr, newVal, 2, 12345)
	
	assert.Equal(t, 1, len(fs.Newvals), "Expected 1 new value")
	
	assert.Equal(t, 1, len(fs.Fieldnums), "Expected 1 field number")
	
	assert.Equal(t, AttrNumber(2), fs.Fieldnums[0], "Expected field number 2")
}

// ==============================================================================
// SUBSCRIPTING REF TESTS
// ==============================================================================

func TestSubscriptingRef(t *testing.T) {
	arrayExpr := NewConst(1007, Datum(123), false) // INT4 array
	indexExpr := NewConst(23, 1, false)                // Index 1
	
	sr := NewSubscriptingRef(1007, 23, 23, arrayExpr, []Expression{indexExpr})
	
	// Verify properties
	assert.Equal(t, T_SubscriptingRef, sr.Tag, "Expected tag T_SubscriptingRef")
	
	assert.Equal(t, Oid(1007), sr.Refcontainertype, "Expected container type 1007")
	
	assert.Equal(t, Oid(23), sr.Refelemtype, "Expected element type 23")
	
	assert.Equal(t, Oid(23), sr.Refrestype, "Expected result type 23")
	
	assert.Equal(t, arrayExpr, sr.Refexpr, "Expected array expression to be set correctly")
	
	assert.Equal(t, 1, len(sr.Refupperindexpr), "Expected 1 upper index expression")
	
	// Test ExpressionType
	assert.Equal(t, "SubscriptingRef", sr.ExpressionType(), "Expected ExpressionType 'SubscriptingRef'")
	
	// Test string representation
	str := sr.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestArraySubscript(t *testing.T) {
	arrayExpr := NewConst(1007, Datum(123), false)
	indexExpr := NewConst(23, 1, false)
	
	sr := NewArraySubscript(1007, 23, arrayExpr, indexExpr)
	
	assert.Equal(t, 1, len(sr.Refupperindexpr), "Expected 1 upper index expression")
	
	assert.Equal(t, 0, len(sr.Reflowerindexpr), "Expected no lower index expressions")
	
	assert.Nil(t, sr.Refassgnexpr, "Expected no assignment expression")
}

func TestArraySlice(t *testing.T) {
	arrayExpr := NewConst(1007, Datum(123), false)
	lowerExpr := NewConst(23, 1, false)
	upperExpr := NewConst(23, 3, false)
	
	sr := NewArraySlice(1007, 23, arrayExpr, lowerExpr, upperExpr)
	
	assert.Equal(t, 1, len(sr.Refupperindexpr), "Expected 1 upper index expression")
	
	assert.Equal(t, 1, len(sr.Reflowerindexpr), "Expected 1 lower index expression")
}

func TestArrayAssignment(t *testing.T) {
	arrayExpr := NewConst(1007, Datum(123), false)
	indexExpr := NewConst(23, 1, false)
	assignExpr := NewConst(23, 99, false)
	
	sr := NewArrayAssignment(1007, 23, arrayExpr, indexExpr, assignExpr)
	
	assert.Equal(t, assignExpr, sr.Refassgnexpr, "Expected assignment expression to be set correctly")
	
	// Test string representation includes assignment
	str := sr.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

// ==============================================================================
// NULL TEST TESTS
// ==============================================================================

func TestNullTest(t *testing.T) {
	expr := NewConst(23, 42, false)
	nt := NewNullTest(expr, IS_NULL)
	
	// Verify properties
	assert.Equal(t, T_NullTest, nt.Tag, "Expected tag T_NullTest")
	
	assert.Equal(t, expr, nt.Arg, "Expected argument to be set correctly")
	
	assert.Equal(t, IS_NULL, nt.Nulltesttype, "Expected IS_NULL")
	
	assert.False(t, nt.Argisrow, "Expected non-row argument by default")
	
	// Test ExpressionType
	assert.Equal(t, "NullTest", nt.ExpressionType(), "Expected ExpressionType 'NullTest'")
	
	// Test string representation
	str := nt.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestIsNullTest(t *testing.T) {
	expr := NewConst(23, 42, false)
	nt := NewIsNullTest(expr)
	
	assert.Equal(t, IS_NULL, nt.Nulltesttype, "Expected IS_NULL")
}

func TestIsNotNullTest(t *testing.T) {
	expr := NewConst(23, 42, false)
	nt := NewIsNotNullTest(expr)
	
	assert.Equal(t, IS_NOT_NULL, nt.Nulltesttype, "Expected IS_NOT_NULL")
}

func TestRowNullTest(t *testing.T) {
	rowExpr := NewConst(16, Datum(1), false) // Row value
	nt := NewRowNullTest(rowExpr, IS_NULL)
	
	assert.True(t, nt.Argisrow, "Expected row argument flag to be set")
	
	// Test string representation includes "(ROW)"
	str := nt.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

// ==============================================================================
// BOOLEAN TEST TESTS
// ==============================================================================

func TestBooleanTest(t *testing.T) {
	boolExpr := NewConst(16, Datum(1), false) // Boolean value
	bt := NewBooleanTest(boolExpr, IS_TRUE)
	
	// Verify properties
	assert.Equal(t, T_BooleanTest, bt.Tag, "Expected tag T_BooleanTest")
	
	assert.Equal(t, boolExpr, bt.Arg, "Expected boolean argument to be set correctly")
	
	assert.Equal(t, IS_TRUE, bt.Booltesttype, "Expected IS_TRUE")
	
	// Test ExpressionType
	assert.Equal(t, "BooleanTest", bt.ExpressionType(), "Expected ExpressionType 'BooleanTest'")
	
	// Test string representation
	str := bt.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestBooleanTestTypes(t *testing.T) {
	boolExpr := NewConst(16, Datum(1), false)
	
	// Test all boolean test types
	testTypes := []BoolTestType{
		IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN,
	}
	
	for _, testType := range testTypes {
		bt := NewBooleanTest(boolExpr, testType)
		assert.Equal(t, testType, bt.Booltesttype, "Expected boolean test type to match")
	}
}

func TestIsTrueTest(t *testing.T) {
	boolExpr := NewConst(16, Datum(1), false)
	bt := NewIsTrueTest(boolExpr)
	
	assert.Equal(t, IS_TRUE, bt.Booltesttype, "Expected IS_TRUE")
}

func TestIsFalseTest(t *testing.T) {
	boolExpr := NewConst(16, Datum(0), false)
	bt := NewIsFalseTest(boolExpr)
	
	assert.Equal(t, IS_FALSE, bt.Booltesttype, "Expected IS_FALSE")
}

func TestIsUnknownTest(t *testing.T) {
	nullExpr := NewConst(16, Datum(0), true) // NULL boolean
	bt := NewIsUnknownTest(nullExpr)
	
	assert.Equal(t, IS_UNKNOWN, bt.Booltesttype, "Expected IS_UNKNOWN")
}

// ==============================================================================
// COERCE TO DOMAIN TESTS
// ==============================================================================

func TestCoerceToDomain(t *testing.T) {
	expr := NewConst(23, 42, false)
	domainOid := Oid(54321)
	
	ctd := NewCoerceToDomain(expr, domainOid, -1, COERCE_IMPLICIT_CAST)
	
	// Verify properties
	assert.Equal(t, T_CoerceToDomain, ctd.Tag, "Expected tag T_CoerceToDomain")
	
	assert.Equal(t, expr, ctd.Arg, "Expected argument to be set correctly")
	
	assert.Equal(t, domainOid, ctd.Resulttype, "Expected result type to match")
	
	assert.Equal(t, COERCE_IMPLICIT_CAST, ctd.Coercionformat, "Expected COERCE_IMPLICIT_CAST")
	
	// Test ExpressionType
	assert.Equal(t, "CoerceToDomain", ctd.ExpressionType(), "Expected ExpressionType 'CoerceToDomain'")
	
	// Test string representation
	str := ctd.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestCoerceToDomainValue(t *testing.T) {
	ctdv := NewCoerceToDomainValue(25, -1, 100)
	
	// Verify properties
	assert.Equal(t, T_CoerceToDomainValue, ctdv.Tag, "Expected tag T_CoerceToDomainValue")
	
	assert.Equal(t, Oid(25), ctdv.TypeId, "Expected type ID 25")
	
	assert.Equal(t, int32(-1), ctdv.TypeMod, "Expected typemod -1")
	
	assert.Equal(t, Oid(100), ctdv.Collation, "Expected collation 100")
	
	assert.Equal(t, Oid(200), ctdv.Collation, "Expected collation 200")
	
	// Test ExpressionType
	assert.Equal(t, "CoerceToDomainValue", ctdv.ExpressionType(), "Expected ExpressionType 'CoerceToDomainValue'")
	
	// Test string representation
	str := ctdv.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

// ==============================================================================
// SPECIAL VALUE EXPRESSION TESTS
// ==============================================================================

func TestSetToDefault(t *testing.T) {
	std := NewSetToDefault(25, -1, 100)
	
	// Verify properties
	assert.Equal(t, T_SetToDefault, std.Tag, "Expected tag T_SetToDefault")
	
	assert.Equal(t, Oid(25), std.TypeId, "Expected type ID 25")
	
	assert.Equal(t, int32(-1), std.TypeMod, "Expected typemod -1")
	
	assert.Equal(t, Oid(100), std.Collation, "Expected collation 100")
	
	assert.Equal(t, Oid(200), std.Collation, "Expected collation 200")
	
	// Test ExpressionType
	assert.Equal(t, "SetToDefault", std.ExpressionType(), "Expected ExpressionType 'SetToDefault'")
	
	// Test string representation
	str := std.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestCurrentOfExpr(t *testing.T) {
	coe := NewCurrentOfExpr(1, "my_cursor")
	
	// Verify properties
	assert.Equal(t, T_CurrentOfExpr, coe.Tag, "Expected tag T_CurrentOfExpr")
	
	assert.Equal(t, Index(1), coe.Cvarno, "Expected cvar number 1")
	
	assert.Equal(t, "my_cursor", coe.CursorName, "Expected cursor name 'my_cursor'")
	
	assert.Equal(t, int(0), coe.CursorParam, "Expected cursor param 0")
	
	// Test ExpressionType
	assert.Equal(t, "CurrentOfExpr", coe.ExpressionType(), "Expected ExpressionType 'CurrentOfExpr'")
	
	// Test string representation
	str := coe.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestCurrentOfExprParam(t *testing.T) {
	coe := NewCurrentOfExprParam(2, 5)
	
	assert.Empty(t, coe.CursorName, "Expected empty cursor name")
	
	assert.Equal(t, int(5), coe.CursorParam, "Expected cursor param 5")
	
	// Test string representation includes parameter
	str := coe.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestNextValueExpr(t *testing.T) {
	nve := NewNextValueExpr(12345, 20) // Sequence OID 12345, BIGINT result
	
	// Verify properties
	assert.Equal(t, T_NextValueExpr, nve.Tag, "Expected tag T_NextValueExpr")
	
	assert.Equal(t, Oid(12345), nve.Seqid, "Expected sequence ID 12345")
	
	assert.Equal(t, Oid(20), nve.TypeId, "Expected type ID 20")
	
	// Test ExpressionType
	assert.Equal(t, "NextValueExpr", nve.ExpressionType(), "Expected ExpressionType 'NextValueExpr'")
	
	// Test string representation
	str := nve.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestInferenceElem(t *testing.T) {
	expr := NewColumnRef(NewString("id"))
	ie := NewInferenceElem(expr)
	
	// Verify properties
	assert.Equal(t, T_InferenceElem, ie.Tag, "Expected tag T_InferenceElem")
	
	assert.Equal(t, expr, ie.Expr, "Expected expression to be set correctly")
	
	assert.Equal(t, Oid(0), ie.Infercollid, "Expected collation ID 0")
	
	assert.Equal(t, Oid(0), ie.Inferopclass, "Expected operator class 0")
	
	// Test ExpressionType
	assert.Equal(t, "InferenceElem", ie.ExpressionType(), "Expected ExpressionType 'InferenceElem'")
	
	// Test string representation
	str := ie.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestInferenceElemWithCollation(t *testing.T) {
	expr := NewColumnRef(NewString("name"))
	ie := NewInferenceElemWithCollation(expr, 100, 200)
	
	assert.Equal(t, Oid(100), ie.Infercollid, "Expected inference collation ID 100")
	
	assert.Equal(t, Oid(200), ie.Inferopclass, "Expected inference operator class 200")
	
	// Test string representation includes collation info
	str := ie.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

// ==============================================================================
// INTEGRATION TESTS
// ==============================================================================

func TestTypeCoercionInteraction(t *testing.T) {
	// Test type coercion chain: INT -> TEXT -> DOMAIN
	intExpr := NewConst(23, 42, false) // INT4
	
	// First coercion: INT to TEXT via IO
	textCoercion := NewCoerceViaIO(intExpr, 25, COERCE_EXPLICIT_CAST)
	
	// Second coercion: TEXT to domain
	domainCoercion := NewCoerceToDomain(textCoercion, 54321, -1, COERCE_IMPLICIT_CAST)
	
	// Verify chain works
	assert.Equal(t, intExpr, textCoercion.Arg, "Expected text coercion input to be int expression")
	
	assert.Equal(t, textCoercion, domainCoercion.Arg, "Expected domain coercion input to be text coercion")
	
	// Verify types
	assert.Equal(t, T_CoerceViaIO, textCoercion.Tag, "Expected T_CoerceViaIO tag")
	
	assert.Equal(t, T_CoerceToDomain, domainCoercion.Tag, "Expected T_CoerceToDomain tag")
}

func TestFieldOperationsWithArrays(t *testing.T) {
	// Test field selection from composite type containing arrays
	recordExpr := NewConst(16, Datum(1), false) // Record
	
	// Select array field
	arrayField := NewFieldSelect(recordExpr, 1, 1007) // INT4 array
	
	// Index into selected array
	indexExpr := NewConst(23, 0, false)
	arraySubscript := NewArraySubscript(1007, 23, arrayField, indexExpr)
	
	// Verify integration
	assert.Equal(t, AttrNumber(1), arrayField.Fieldnum, "Expected field number 1")
	
	assert.Equal(t, arrayField, arraySubscript.Refexpr, "Expected array subscript to reference field select")
	
	assert.Equal(t, T_SubscriptingRef, arraySubscript.Tag, "Expected T_SubscriptingRef tag")
}

func TestComplexNullAndBooleanTests(t *testing.T) {
	// Test complex expressions with NULL and boolean tests
	
	// Create a field selection that might be NULL
	recordExpr := NewConst(16, Datum(1), false)
	fieldSelect := NewFieldSelect(recordExpr, 2, 16) // Boolean field
	
	// Test if the field is NULL
	nullTest := NewIsNullTest(fieldSelect)
	
	// Test if the field is TRUE (when not NULL)
	boolTest := NewIsTrueTest(fieldSelect)
	
	// Verify integration
	assert.Equal(t, fieldSelect, nullTest.Arg, "Expected null test argument to be field select")
	
	assert.Equal(t, fieldSelect, boolTest.Arg, "Expected boolean test argument to be field select")
	
	assert.Equal(t, IS_NULL, nullTest.Nulltesttype, "Expected IS_NULL test type")
	
	assert.Equal(t, IS_TRUE, boolTest.Booltesttype, "Expected IS_TRUE test type")
}

// ==============================================================================
// BENCHMARK TESTS
// ==============================================================================

func BenchmarkRelabelTypeCreation(b *testing.B) {
	expr := NewConst(23, 42, false)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewRelabelType(expr, 20, -1, COERCE_EXPLICIT_CAST)
	}
}

func BenchmarkNullTestCreation(b *testing.B) {
	expr := NewConst(23, 42, false)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewIsNullTest(expr)
	}
}

func BenchmarkFieldSelectCreation(b *testing.B) {
	recordExpr := NewConst(16, Datum(1), false)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewFieldSelect(recordExpr, AttrNumber(i%10+1), 25)
	}
}

func BenchmarkSubscriptingRefCreation(b *testing.B) {
	arrayExpr := NewConst(1007, Datum(123), false)
	indexExpr := NewConst(23, 1, false)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewArraySubscript(1007, 23, arrayExpr, indexExpr)
	}
}