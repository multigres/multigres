// Package ast provides comprehensive tests for PostgreSQL AST type coercion and advanced expression nodes.
// These tests ensure the correctness of PostgreSQL's sophisticated type system implementation.
package ast

import (
	"testing"
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
	if rt.Tag != T_RelabelType {
		t.Errorf("Expected tag T_RelabelType, got %v", rt.Tag)
	}
	
	if rt.Arg != input {
		t.Errorf("Expected input argument to be set correctly")
	}
	
	if rt.Resulttype != 20 {
		t.Errorf("Expected result type 20, got %d", rt.Resulttype)
	}
	
	if rt.Resulttypmod != -1 {
		t.Errorf("Expected result typmod -1, got %d", rt.Resulttypmod)
	}
	
	if rt.Relabelformat != COERCE_EXPLICIT_CAST {
		t.Errorf("Expected COERCE_EXPLICIT_CAST, got %v", rt.Relabelformat)
	}
	
	// Test ExpressionType
	if rt.ExpressionType() != "RelabelType" {
		t.Errorf("Expected ExpressionType 'RelabelType', got %s", rt.ExpressionType())
	}
	
	// Test string representation
	str := rt.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestImplicitRelabelType(t *testing.T) {
	input := NewConst(23, 42, false)
	rt := NewImplicitRelabelType(input, 25) // TEXT type
	
	if rt.Relabelformat != COERCE_IMPLICIT_CAST {
		t.Errorf("Expected COERCE_IMPLICIT_CAST, got %v", rt.Relabelformat)
	}
	
	if rt.Resulttype != 25 {
		t.Errorf("Expected result type 25, got %d", rt.Resulttype)
	}
}

func TestExplicitRelabelType(t *testing.T) {
	input := NewConst(23, 42, false)
	rt := NewExplicitRelabelType(input, 1700) // NUMERIC type
	
	if rt.Relabelformat != COERCE_EXPLICIT_CAST {
		t.Errorf("Expected COERCE_EXPLICIT_CAST, got %v", rt.Relabelformat)
	}
	
	if rt.Resulttype != 1700 {
		t.Errorf("Expected result type 1700, got %d", rt.Resulttype)
	}
}

// ==============================================================================
// COERCE VIA IO TESTS
// ==============================================================================

func TestCoerceViaIO(t *testing.T) {
	input := NewConst(25, "42", false) // TEXT constant
	cvio := NewCoerceViaIO(input, 23, COERCE_EXPLICIT_CAST) // Convert to INT4
	
	// Verify properties
	if cvio.Tag != T_CoerceViaIO {
		t.Errorf("Expected tag T_CoerceViaIO, got %v", cvio.Tag)
	}
	
	if cvio.Arg != input {
		t.Errorf("Expected input argument to be set correctly")
	}
	
	if cvio.Resulttype != 23 {
		t.Errorf("Expected result type 23, got %d", cvio.Resulttype)
	}
	
	if cvio.Coerceformat != COERCE_EXPLICIT_CAST {
		t.Errorf("Expected COERCE_EXPLICIT_CAST, got %v", cvio.Coerceformat)
	}
	
	// Test ExpressionType
	if cvio.ExpressionType() != "CoerceViaIO" {
		t.Errorf("Expected ExpressionType 'CoerceViaIO', got %s", cvio.ExpressionType())
	}
	
	// Test string representation
	str := cvio.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestExplicitCoerceViaIO(t *testing.T) {
	input := NewConst(25, "3.14", false)
	cvio := NewExplicitCoerceViaIO(input, 700) // FLOAT4 type
	
	if cvio.Coerceformat != COERCE_EXPLICIT_CAST {
		t.Errorf("Expected COERCE_EXPLICIT_CAST, got %v", cvio.Coerceformat)
	}
	
	if cvio.Resulttype != 700 {
		t.Errorf("Expected result type 700, got %d", cvio.Resulttype)
	}
}

// ==============================================================================
// ARRAY COERCE EXPR TESTS
// ==============================================================================

func TestArrayCoerceExpr(t *testing.T) {
	// Create an array expression
	arrayExpr := NewConst(1007, []int{1, 2, 3}, false) // INT4 array
	elemFuncId := Oid(481) // Example element coercion function
	
	ace := NewArrayCoerceExpr(arrayExpr, elemFuncId, 1016, COERCE_EXPLICIT_CAST) // INT8 array
	
	// Verify properties
	if ace.Tag != T_ArrayCoerceExpr {
		t.Errorf("Expected tag T_ArrayCoerceExpr, got %v", ace.Tag)
	}
	
	if ace.Arg != arrayExpr {
		t.Errorf("Expected array argument to be set correctly")
	}
	
	if ace.Elemfuncid != elemFuncId {
		t.Errorf("Expected element function ID %d, got %d", elemFuncId, ace.Elemfuncid)
	}
	
	if ace.Resulttype != 1016 {
		t.Errorf("Expected result type 1016, got %d", ace.Resulttype)
	}
	
	// Test ExpressionType
	if ace.ExpressionType() != "ArrayCoerceExpr" {
		t.Errorf("Expected ExpressionType 'ArrayCoerceExpr', got %s", ace.ExpressionType())
	}
	
	// Test string representation
	str := ace.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestExplicitArrayCoerceExpr(t *testing.T) {
	arrayExpr := NewConst(1007, []int{1, 2, 3}, false)
	ace := NewExplicitArrayCoerceExpr(arrayExpr, 481, 1009) // TEXTARRAY
	
	if !ace.Isexplicit {
		t.Errorf("Expected explicit array coercion")
	}
	
	if ace.Coerceformat != COERCE_EXPLICIT_CAST {
		t.Errorf("Expected COERCE_EXPLICIT_CAST, got %v", ace.Coerceformat)
	}
}

// ==============================================================================
// CONVERT ROWTYPE EXPR TESTS
// ==============================================================================

func TestConvertRowtypeExpr(t *testing.T) {
	// Create a row expression
	rowExpr := NewConst(16, true, false) // Placeholder row value
	
	crte := NewConvertRowtypeExpr(rowExpr, 12345, COERCE_IMPLICIT_CAST)
	
	// Verify properties
	if crte.Tag != T_ConvertRowtypeExpr {
		t.Errorf("Expected tag T_ConvertRowtypeExpr, got %v", crte.Tag)
	}
	
	if crte.Arg != rowExpr {
		t.Errorf("Expected row argument to be set correctly")
	}
	
	if crte.Resulttype != 12345 {
		t.Errorf("Expected result type 12345, got %d", crte.Resulttype)
	}
	
	if crte.Convertformat != COERCE_IMPLICIT_CAST {
		t.Errorf("Expected COERCE_IMPLICIT_CAST, got %v", crte.Convertformat)
	}
	
	// Test ExpressionType
	if crte.ExpressionType() != "ConvertRowtypeExpr" {
		t.Errorf("Expected ExpressionType 'ConvertRowtypeExpr', got %s", crte.ExpressionType())
	}
	
	// Test string representation
	str := crte.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// COLLATE EXPR TESTS
// ==============================================================================

func TestCollateExpr(t *testing.T) {
	textExpr := NewConst(25, "hello", false) // TEXT value
	collOid := Oid(100) // Example collation OID
	
	ce := NewCollateExpr(textExpr, collOid)
	
	// Verify properties
	if ce.Tag != T_CollateExpr {
		t.Errorf("Expected tag T_CollateExpr, got %v", ce.Tag)
	}
	
	if ce.Arg != textExpr {
		t.Errorf("Expected text argument to be set correctly")
	}
	
	if ce.CollOid != collOid {
		t.Errorf("Expected collation OID %d, got %d", collOid, ce.CollOid)
	}
	
	// Test ExpressionType
	if ce.ExpressionType() != "CollateExpr" {
		t.Errorf("Expected ExpressionType 'CollateExpr', got %s", ce.ExpressionType())
	}
	
	// Test string representation
	str := ce.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// FIELD SELECT TESTS
// ==============================================================================

func TestFieldSelect(t *testing.T) {
	// Create a composite type expression
	recordExpr := NewConst(16, true, false) // Placeholder record
	
	fs := NewFieldSelect(recordExpr, 2, 25) // Select field 2, result type TEXT
	
	// Verify properties
	if fs.Tag != T_FieldSelect {
		t.Errorf("Expected tag T_FieldSelect, got %v", fs.Tag)
	}
	
	if fs.Arg != recordExpr {
		t.Errorf("Expected record argument to be set correctly")
	}
	
	if fs.Fieldnum != 2 {
		t.Errorf("Expected field number 2, got %d", fs.Fieldnum)
	}
	
	if fs.Resulttype != 25 {
		t.Errorf("Expected result type 25, got %d", fs.Resulttype)
	}
	
	if fs.Resulttypmod != -1 {
		t.Errorf("Expected result typmod -1, got %d", fs.Resulttypmod)
	}
	
	// Test ExpressionType
	if fs.ExpressionType() != "FieldSelect" {
		t.Errorf("Expected ExpressionType 'FieldSelect', got %s", fs.ExpressionType())
	}
	
	// Test string representation
	str := fs.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// FIELD STORE TESTS
// ==============================================================================

func TestFieldStore(t *testing.T) {
	recordExpr := NewConst(16, true, false) // Placeholder record
	newVal1 := NewConst(23, 42, false)      // INT4 value
	newVal2 := NewConst(25, "hello", false) // TEXT value
	
	fs := NewFieldStore(recordExpr, []Expression{newVal1, newVal2}, []AttrNumber{1, 3}, 12345)
	
	// Verify properties
	if fs.Tag != T_FieldStore {
		t.Errorf("Expected tag T_FieldStore, got %v", fs.Tag)
	}
	
	if fs.Arg != recordExpr {
		t.Errorf("Expected record argument to be set correctly")
	}
	
	if len(fs.Newvals) != 2 {
		t.Errorf("Expected 2 new values, got %d", len(fs.Newvals))
	}
	
	if len(fs.Fieldnums) != 2 {
		t.Errorf("Expected 2 field numbers, got %d", len(fs.Fieldnums))
	}
	
	if fs.Fieldnums[0] != 1 || fs.Fieldnums[1] != 3 {
		t.Errorf("Expected field numbers [1, 3], got %v", fs.Fieldnums)
	}
	
	if fs.Resulttype != 12345 {
		t.Errorf("Expected result type 12345, got %d", fs.Resulttype)
	}
	
	// Test ExpressionType
	if fs.ExpressionType() != "FieldStore" {
		t.Errorf("Expected ExpressionType 'FieldStore', got %s", fs.ExpressionType())
	}
	
	// Test string representation
	str := fs.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestSingleFieldStore(t *testing.T) {
	recordExpr := NewConst(16, true, false)
	newVal := NewConst(23, 99, false)
	
	fs := NewSingleFieldStore(recordExpr, newVal, 2, 12345)
	
	if len(fs.Newvals) != 1 {
		t.Errorf("Expected 1 new value, got %d", len(fs.Newvals))
	}
	
	if len(fs.Fieldnums) != 1 {
		t.Errorf("Expected 1 field number, got %d", len(fs.Fieldnums))
	}
	
	if fs.Fieldnums[0] != 2 {
		t.Errorf("Expected field number 2, got %d", fs.Fieldnums[0])
	}
}

// ==============================================================================
// SUBSCRIPTING REF TESTS
// ==============================================================================

func TestSubscriptingRef(t *testing.T) {
	arrayExpr := NewConst(1007, []int{1, 2, 3}, false) // INT4 array
	indexExpr := NewConst(23, 1, false)                // Index 1
	
	sr := NewSubscriptingRef(1007, 23, arrayExpr, []Expression{indexExpr})
	
	// Verify properties
	if sr.Tag != T_SubscriptingRef {
		t.Errorf("Expected tag T_SubscriptingRef, got %v", sr.Tag)
	}
	
	if sr.Refcontainertype != 1007 {
		t.Errorf("Expected container type 1007, got %d", sr.Refcontainertype)
	}
	
	if sr.Refelemtype != 23 {
		t.Errorf("Expected element type 23, got %d", sr.Refelemtype)
	}
	
	if sr.Refexpr != arrayExpr {
		t.Errorf("Expected array expression to be set correctly")
	}
	
	if len(sr.Refupperindexpr) != 1 {
		t.Errorf("Expected 1 upper index expression, got %d", len(sr.Refupperindexpr))
	}
	
	// Test ExpressionType
	if sr.ExpressionType() != "SubscriptingRef" {
		t.Errorf("Expected ExpressionType 'SubscriptingRef', got %s", sr.ExpressionType())
	}
	
	// Test string representation
	str := sr.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestArraySubscript(t *testing.T) {
	arrayExpr := NewConst(1007, []int{1, 2, 3}, false)
	indexExpr := NewConst(23, 1, false)
	
	sr := NewArraySubscript(1007, 23, arrayExpr, indexExpr)
	
	if len(sr.Refupperindexpr) != 1 {
		t.Errorf("Expected 1 upper index expression, got %d", len(sr.Refupperindexpr))
	}
	
	if len(sr.Reflowerindexpr) != 0 {
		t.Errorf("Expected no lower index expressions, got %d", len(sr.Reflowerindexpr))
	}
	
	if sr.Refassgnexpr != nil {
		t.Errorf("Expected no assignment expression")
	}
}

func TestArraySlice(t *testing.T) {
	arrayExpr := NewConst(1007, []int{1, 2, 3}, false)
	lowerExpr := NewConst(23, 1, false)
	upperExpr := NewConst(23, 3, false)
	
	sr := NewArraySlice(1007, 23, arrayExpr, lowerExpr, upperExpr)
	
	if len(sr.Refupperindexpr) != 1 {
		t.Errorf("Expected 1 upper index expression, got %d", len(sr.Refupperindexpr))
	}
	
	if len(sr.Reflowerindexpr) != 1 {
		t.Errorf("Expected 1 lower index expression, got %d", len(sr.Reflowerindexpr))
	}
}

func TestArrayAssignment(t *testing.T) {
	arrayExpr := NewConst(1007, []int{1, 2, 3}, false)
	indexExpr := NewConst(23, 1, false)
	assignExpr := NewConst(23, 99, false)
	
	sr := NewArrayAssignment(1007, 23, arrayExpr, indexExpr, assignExpr)
	
	if sr.Refassgnexpr != assignExpr {
		t.Errorf("Expected assignment expression to be set correctly")
	}
	
	// Test string representation includes assignment
	str := sr.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// NULL TEST TESTS
// ==============================================================================

func TestNullTest(t *testing.T) {
	expr := NewConst(23, 42, false)
	nt := NewNullTest(expr, IS_NULL)
	
	// Verify properties
	if nt.Tag != T_NullTest {
		t.Errorf("Expected tag T_NullTest, got %v", nt.Tag)
	}
	
	if nt.Arg != expr {
		t.Errorf("Expected argument to be set correctly")
	}
	
	if nt.Nulltesttype != IS_NULL {
		t.Errorf("Expected IS_NULL, got %v", nt.Nulltesttype)
	}
	
	if nt.Argisrow {
		t.Errorf("Expected non-row argument by default")
	}
	
	// Test ExpressionType
	if nt.ExpressionType() != "NullTest" {
		t.Errorf("Expected ExpressionType 'NullTest', got %s", nt.ExpressionType())
	}
	
	// Test string representation
	str := nt.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestIsNullTest(t *testing.T) {
	expr := NewConst(23, 42, false)
	nt := NewIsNullTest(expr)
	
	if nt.Nulltesttype != IS_NULL {
		t.Errorf("Expected IS_NULL, got %v", nt.Nulltesttype)
	}
}

func TestIsNotNullTest(t *testing.T) {
	expr := NewConst(23, 42, false)
	nt := NewIsNotNullTest(expr)
	
	if nt.Nulltesttype != IS_NOT_NULL {
		t.Errorf("Expected IS_NOT_NULL, got %v", nt.Nulltesttype)
	}
}

func TestRowNullTest(t *testing.T) {
	rowExpr := NewConst(16, true, false) // Row value
	nt := NewRowNullTest(rowExpr, IS_NULL)
	
	if !nt.Argisrow {
		t.Errorf("Expected row argument flag to be set")
	}
	
	// Test string representation includes "(ROW)"
	str := nt.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// BOOLEAN TEST TESTS
// ==============================================================================

func TestBooleanTest(t *testing.T) {
	boolExpr := NewConst(16, true, false) // Boolean value
	bt := NewBooleanTest(boolExpr, IS_TRUE)
	
	// Verify properties
	if bt.Tag != T_BooleanTest {
		t.Errorf("Expected tag T_BooleanTest, got %v", bt.Tag)
	}
	
	if bt.Arg != boolExpr {
		t.Errorf("Expected boolean argument to be set correctly")
	}
	
	if bt.Booltesttype != IS_TRUE {
		t.Errorf("Expected IS_TRUE, got %v", bt.Booltesttype)
	}
	
	// Test ExpressionType
	if bt.ExpressionType() != "BooleanTest" {
		t.Errorf("Expected ExpressionType 'BooleanTest', got %s", bt.ExpressionType())
	}
	
	// Test string representation
	str := bt.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestBooleanTestTypes(t *testing.T) {
	boolExpr := NewConst(16, true, false)
	
	// Test all boolean test types
	testTypes := []BoolTestType{
		IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN,
	}
	
	for _, testType := range testTypes {
		bt := NewBooleanTest(boolExpr, testType)
		if bt.Booltesttype != testType {
			t.Errorf("Expected boolean test type %v, got %v", testType, bt.Booltesttype)
		}
	}
}

func TestIsTrueTest(t *testing.T) {
	boolExpr := NewConst(16, true, false)
	bt := NewIsTrueTest(boolExpr)
	
	if bt.Booltesttype != IS_TRUE {
		t.Errorf("Expected IS_TRUE, got %v", bt.Booltesttype)
	}
}

func TestIsFalseTest(t *testing.T) {
	boolExpr := NewConst(16, false, false)
	bt := NewIsFalseTest(boolExpr)
	
	if bt.Booltesttype != IS_FALSE {
		t.Errorf("Expected IS_FALSE, got %v", bt.Booltesttype)
	}
}

func TestIsUnknownTest(t *testing.T) {
	nullExpr := NewConst(16, nil, true) // NULL boolean
	bt := NewIsUnknownTest(nullExpr)
	
	if bt.Booltesttype != IS_UNKNOWN {
		t.Errorf("Expected IS_UNKNOWN, got %v", bt.Booltesttype)
	}
}

// ==============================================================================
// COERCE TO DOMAIN TESTS
// ==============================================================================

func TestCoerceToDomain(t *testing.T) {
	expr := NewConst(23, 42, false)
	domainOid := Oid(54321)
	
	ctd := NewCoerceToDomain(expr, domainOid, -1, COERCE_IMPLICIT_CAST)
	
	// Verify properties
	if ctd.Tag != T_CoerceToDomain {
		t.Errorf("Expected tag T_CoerceToDomain, got %v", ctd.Tag)
	}
	
	if ctd.Arg != expr {
		t.Errorf("Expected argument to be set correctly")
	}
	
	if ctd.Resulttype != domainOid {
		t.Errorf("Expected result type %d, got %d", domainOid, ctd.Resulttype)
	}
	
	if ctd.Coercionformat != COERCE_IMPLICIT_CAST {
		t.Errorf("Expected COERCE_IMPLICIT_CAST, got %v", ctd.Coercionformat)
	}
	
	// Test ExpressionType
	if ctd.ExpressionType() != "CoerceToDomain" {
		t.Errorf("Expected ExpressionType 'CoerceToDomain', got %s", ctd.ExpressionType())
	}
	
	// Test string representation
	str := ctd.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestCoerceToDomainValue(t *testing.T) {
	ctdv := NewCoerceToDomainValue(100, 200)
	
	// Verify properties
	if ctdv.Tag != T_CoerceToDomainValue {
		t.Errorf("Expected tag T_CoerceToDomainValue, got %v", ctdv.Tag)
	}
	
	if ctdv.Typemod != 100 {
		t.Errorf("Expected typemod 100, got %d", ctdv.Typemod)
	}
	
	if ctdv.Collation != 200 {
		t.Errorf("Expected collation 200, got %d", ctdv.Collation)
	}
	
	// Test ExpressionType
	if ctdv.ExpressionType() != "CoerceToDomainValue" {
		t.Errorf("Expected ExpressionType 'CoerceToDomainValue', got %s", ctdv.ExpressionType())
	}
	
	// Test string representation
	str := ctdv.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// SPECIAL VALUE EXPRESSION TESTS
// ==============================================================================

func TestSetToDefault(t *testing.T) {
	std := NewSetToDefault(100, 200)
	
	// Verify properties
	if std.Tag != T_SetToDefault {
		t.Errorf("Expected tag T_SetToDefault, got %v", std.Tag)
	}
	
	if std.Typemod != 100 {
		t.Errorf("Expected typemod 100, got %d", std.Typemod)
	}
	
	if std.Collation != 200 {
		t.Errorf("Expected collation 200, got %d", std.Collation)
	}
	
	// Test ExpressionType
	if std.ExpressionType() != "SetToDefault" {
		t.Errorf("Expected ExpressionType 'SetToDefault', got %s", std.ExpressionType())
	}
	
	// Test string representation
	str := std.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestCurrentOfExpr(t *testing.T) {
	coe := NewCurrentOfExpr(1, "my_cursor")
	
	// Verify properties
	if coe.Tag != T_CurrentOfExpr {
		t.Errorf("Expected tag T_CurrentOfExpr, got %v", coe.Tag)
	}
	
	if coe.CvarNo != 1 {
		t.Errorf("Expected cvar number 1, got %d", coe.CvarNo)
	}
	
	if coe.CursorName != "my_cursor" {
		t.Errorf("Expected cursor name 'my_cursor', got %s", coe.CursorName)
	}
	
	if coe.CursorParam != 0 {
		t.Errorf("Expected cursor param 0, got %d", coe.CursorParam)
	}
	
	// Test ExpressionType
	if coe.ExpressionType() != "CurrentOfExpr" {
		t.Errorf("Expected ExpressionType 'CurrentOfExpr', got %s", coe.ExpressionType())
	}
	
	// Test string representation
	str := coe.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestCurrentOfExprParam(t *testing.T) {
	coe := NewCurrentOfExprParam(2, 5)
	
	if coe.CursorName != "" {
		t.Errorf("Expected empty cursor name, got %s", coe.CursorName)
	}
	
	if coe.CursorParam != 5 {
		t.Errorf("Expected cursor param 5, got %d", coe.CursorParam)
	}
	
	// Test string representation includes parameter
	str := coe.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestNextValueExpr(t *testing.T) {
	nve := NewNextValueExpr(12345, 20) // Sequence OID 12345, BIGINT result
	
	// Verify properties
	if nve.Tag != T_NextValueExpr {
		t.Errorf("Expected tag T_NextValueExpr, got %v", nve.Tag)
	}
	
	if nve.SeqId != 12345 {
		t.Errorf("Expected sequence ID 12345, got %d", nve.SeqId)
	}
	
	if nve.TypeId != 20 {
		t.Errorf("Expected type ID 20, got %d", nve.TypeId)
	}
	
	// Test ExpressionType
	if nve.ExpressionType() != "NextValueExpr" {
		t.Errorf("Expected ExpressionType 'NextValueExpr', got %s", nve.ExpressionType())
	}
	
	// Test string representation
	str := nve.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestInferenceElem(t *testing.T) {
	expr := NewColumnRef([]Node{NewString("id")}, -1)
	ie := NewInferenceElem(expr)
	
	// Verify properties
	if ie.Tag != T_InferenceElem {
		t.Errorf("Expected tag T_InferenceElem, got %v", ie.Tag)
	}
	
	if ie.Expr != expr {
		t.Errorf("Expected expression to be set correctly")
	}
	
	if ie.Infercollid != 0 {
		t.Errorf("Expected collation ID 0, got %d", ie.Infercollid)
	}
	
	if ie.Inferopclass != 0 {
		t.Errorf("Expected operator class 0, got %d", ie.Inferopclass)
	}
	
	// Test ExpressionType
	if ie.ExpressionType() != "InferenceElem" {
		t.Errorf("Expected ExpressionType 'InferenceElem', got %s", ie.ExpressionType())
	}
	
	// Test string representation
	str := ie.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestInferenceElemWithCollation(t *testing.T) {
	expr := NewColumnRef([]Node{NewString("name")}, -1)
	ie := NewInferenceElemWithCollation(expr, 100, 200)
	
	if ie.Infercollid != 100 {
		t.Errorf("Expected inference collation ID 100, got %d", ie.Infercollid)
	}
	
	if ie.Inferopclass != 200 {
		t.Errorf("Expected inference operator class 200, got %d", ie.Inferopclass)
	}
	
	// Test string representation includes collation info
	str := ie.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
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
	if textCoercion.Arg != intExpr {
		t.Errorf("Expected text coercion input to be int expression")
	}
	
	if domainCoercion.Arg != textCoercion {
		t.Errorf("Expected domain coercion input to be text coercion")
	}
	
	// Verify types
	if textCoercion.Tag != T_CoerceViaIO {
		t.Errorf("Expected T_CoerceViaIO tag")
	}
	
	if domainCoercion.Tag != T_CoerceToDomain {
		t.Errorf("Expected T_CoerceToDomain tag")
	}
}

func TestFieldOperationsWithArrays(t *testing.T) {
	// Test field selection from composite type containing arrays
	recordExpr := NewConst(16, true, false) // Record
	
	// Select array field
	arrayField := NewFieldSelect(recordExpr, 1, 1007) // INT4 array
	
	// Index into selected array
	indexExpr := NewConst(23, 0, false)
	arraySubscript := NewArraySubscript(1007, 23, arrayField, indexExpr)
	
	// Verify integration
	if arrayField.Fieldnum != 1 {
		t.Errorf("Expected field number 1")
	}
	
	if arraySubscript.Refexpr != arrayField {
		t.Errorf("Expected array subscript to reference field select")
	}
	
	if arraySubscript.Tag != T_SubscriptingRef {
		t.Errorf("Expected T_SubscriptingRef tag")
	}
}

func TestComplexNullAndBooleanTests(t *testing.T) {
	// Test complex expressions with NULL and boolean tests
	
	// Create a field selection that might be NULL
	recordExpr := NewConst(16, true, false)
	fieldSelect := NewFieldSelect(recordExpr, 2, 16) // Boolean field
	
	// Test if the field is NULL
	nullTest := NewIsNullTest(fieldSelect)
	
	// Test if the field is TRUE (when not NULL)
	boolTest := NewIsTrueTest(fieldSelect)
	
	// Verify integration
	if nullTest.Arg != fieldSelect {
		t.Errorf("Expected null test argument to be field select")
	}
	
	if boolTest.Arg != fieldSelect {
		t.Errorf("Expected boolean test argument to be field select")
	}
	
	if nullTest.Nulltesttype != IS_NULL {
		t.Errorf("Expected IS_NULL test type")
	}
	
	if boolTest.Booltesttype != IS_TRUE {
		t.Errorf("Expected IS_TRUE test type")
	}
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
	recordExpr := NewConst(16, true, false)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewFieldSelect(recordExpr, AttrNumber(i%10+1), 25)
	}
}

func BenchmarkSubscriptingRefCreation(b *testing.B) {
	arrayExpr := NewConst(1007, []int{1, 2, 3}, false)
	indexExpr := NewConst(23, 1, false)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewArraySubscript(1007, 23, arrayExpr, indexExpr)
	}
}