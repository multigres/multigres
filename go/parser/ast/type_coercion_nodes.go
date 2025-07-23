// Package ast provides PostgreSQL AST type coercion and advanced expression node definitions.
// These nodes handle PostgreSQL's sophisticated type system including type casting,
// field access, array operations, and various test expressions.
// Ported from postgres/src/include/nodes/primnodes.h
package ast

import (
	"fmt"
)

// ==============================================================================
// TYPE COERCION AND ADVANCED EXPRESSION NODES - PostgreSQL Type System
// ==============================================================================

// RelabelType represents type casting/relabeling operations.
// This is the most common type coercion mechanism in PostgreSQL, used when
// the representation doesn't change but the type label does.
// Ported from postgres/src/include/nodes/primnodes.h:1181
type RelabelType struct {
	BaseExpr
	Arg          Expression    // Input expression - primnodes.h:1184
	Resulttype   Oid          // Output type OID - primnodes.h:1185
	Resulttypmod int32        // Output typmod (usually -1) - primnodes.h:1187
	Resultcollid Oid          // OID of collation, or InvalidOid if none - primnodes.h:1189
	Relabelformat CoercionForm // How to display this node - primnodes.h:1191
}

// NewRelabelType creates a new RelabelType node.
func NewRelabelType(arg Expression, resulttype Oid, resulttypmod int32, relabelformat CoercionForm) *RelabelType {
	return &RelabelType{
		BaseExpr:      BaseExpr{BaseNode: BaseNode{Tag: T_RelabelType}},
		Arg:           arg,
		Resulttype:    resulttype,
		Resulttypmod:  resulttypmod,
		Relabelformat: relabelformat,
	}
}

// NewImplicitRelabelType creates a new RelabelType for implicit casts.
func NewImplicitRelabelType(arg Expression, resulttype Oid) *RelabelType {
	return &RelabelType{
		BaseExpr:      BaseExpr{BaseNode: BaseNode{Tag: T_RelabelType}},
		Arg:           arg,
		Resulttype:    resulttype,
		Resulttypmod:  -1,
		Relabelformat: COERCE_IMPLICIT_CAST,
	}
}

// NewExplicitRelabelType creates a new RelabelType for explicit casts.
func NewExplicitRelabelType(arg Expression, resulttype Oid) *RelabelType {
	return &RelabelType{
		BaseExpr:      BaseExpr{BaseNode: BaseNode{Tag: T_RelabelType}},
		Arg:           arg,
		Resulttype:    resulttype,
		Resulttypmod:  -1,
		Relabelformat: COERCE_EXPLICIT_CAST,
	}
}

func (rt *RelabelType) ExpressionType() string {
	return "RelabelType"
}

func (rt *RelabelType) String() string {
	formatStrs := map[CoercionForm]string{
		COERCE_EXPLICIT_CALL: "CALL", COERCE_EXPLICIT_CAST: "CAST",
		COERCE_IMPLICIT_CAST: "IMPLICIT", COERCE_SQL_SYNTAX: "SQL",
	}
	formatStr := formatStrs[rt.Relabelformat]
	if formatStr == "" {
		formatStr = fmt.Sprintf("FORMAT_%d", int(rt.Relabelformat))
	}
	
	return fmt.Sprintf("RelabelType(%s as %d, %s)", rt.Arg, rt.Resulttype, formatStr)
}

// CoerceViaIO represents type coercion through I/O functions.
// This is used when types need to be converted by invoking their I/O functions
// (output function of source type, input function of target type).
// Ported from postgres/src/include/nodes/primnodes.h:1204
type CoerceViaIO struct {
	BaseExpr
	Arg          Expression    // Input expression - primnodes.h:1669
	Resulttype   Oid          // Output type OID - primnodes.h:1670
	Resulttypmod int32        // Output typmod (usually -1) - primnodes.h:1671
	Resultcollid Oid          // OID of collation, or InvalidOid if none - primnodes.h:1672
	Coerceformat CoercionForm // How to display this coercion - primnodes.h:1673
}

// NewCoerceViaIO creates a new CoerceViaIO node.
func NewCoerceViaIO(arg Expression, resulttype Oid, coerceformat CoercionForm) *CoerceViaIO {
	return &CoerceViaIO{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_CoerceViaIO}},
		Arg:          arg,
		Resulttype:   resulttype,
		Resulttypmod: -1,
		Coerceformat: coerceformat,
	}
}

// NewExplicitCoerceViaIO creates a new CoerceViaIO for explicit coercion.
func NewExplicitCoerceViaIO(arg Expression, resulttype Oid) *CoerceViaIO {
	return &CoerceViaIO{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_CoerceViaIO}},
		Arg:          arg,
		Resulttype:   resulttype,
		Resulttypmod: -1,
		Coerceformat: COERCE_EXPLICIT_CAST,
	}
}

func (cvio *CoerceViaIO) ExpressionType() string {
	return "CoerceViaIO"
}

func (cvio *CoerceViaIO) String() string {
	formatStrs := map[CoercionForm]string{
		COERCE_EXPLICIT_CALL: "CALL", COERCE_EXPLICIT_CAST: "CAST",
		COERCE_IMPLICIT_CAST: "IMPLICIT", COERCE_SQL_SYNTAX: "SQL",
	}
	formatStr := formatStrs[cvio.Coerceformat]
	if formatStr == "" {
		formatStr = fmt.Sprintf("FORMAT_%d", int(cvio.Coerceformat))
	}
	
	return fmt.Sprintf("CoerceViaIO(%s as %d, %s)", cvio.Arg, cvio.Resulttype, formatStr)
}

// ArrayCoerceExpr represents array type coercion.
// This handles coercion of array types, including element-wise coercion.
// Ported from postgres/src/include/nodes/primnodes.h:1230
type ArrayCoerceExpr struct {
	BaseExpr
	Arg            Expression    // Input array expression - primnodes.h:1682
	Elemfuncid     Oid          // OID of element coercion function, or 0 - primnodes.h:1683
	Resulttype     Oid          // Output type OID (array type) - primnodes.h:1684
	Resulttypmod   int32        // Output typmod (usually -1) - primnodes.h:1685
	Resultcollid   Oid          // OID of collation, or InvalidOid if none - primnodes.h:1686
	Coerceformat   CoercionForm // How to display this coercion - primnodes.h:1687
	Isexplicit     bool         // Conversion semantics flag to pass to func - primnodes.h:1688
}

// NewArrayCoerceExpr creates a new ArrayCoerceExpr node.
func NewArrayCoerceExpr(arg Expression, elemfuncid, resulttype Oid, coerceformat CoercionForm) *ArrayCoerceExpr {
	return &ArrayCoerceExpr{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_ArrayCoerceExpr}},
		Arg:          arg,
		Elemfuncid:   elemfuncid,
		Resulttype:   resulttype,
		Resulttypmod: -1,
		Coerceformat: coerceformat,
	}
}

// NewExplicitArrayCoerceExpr creates a new explicit ArrayCoerceExpr.
func NewExplicitArrayCoerceExpr(arg Expression, elemfuncid, resulttype Oid) *ArrayCoerceExpr {
	return &ArrayCoerceExpr{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_ArrayCoerceExpr}},
		Arg:          arg,
		Elemfuncid:   elemfuncid,
		Resulttype:   resulttype,
		Resulttypmod: -1,
		Coerceformat: COERCE_EXPLICIT_CAST,
		Isexplicit:   true,
	}
}

func (ace *ArrayCoerceExpr) ExpressionType() string {
	return "ArrayCoerceExpr"
}

func (ace *ArrayCoerceExpr) String() string {
	explicit := ""
	if ace.Isexplicit {
		explicit = " EXPLICIT"
	}
	return fmt.Sprintf("ArrayCoerceExpr(%s as %d%s)", ace.Arg, ace.Resulttype, explicit)
}

// ConvertRowtypeExpr represents row type conversion.
// This converts a whole-row value from one composite type to another.
// Ported from postgres/src/include/nodes/primnodes.h:1258
type ConvertRowtypeExpr struct {
	BaseExpr
	Arg           Expression    // Input expression - primnodes.h:1706
	Resulttype    Oid          // Output type (always a composite type) - primnodes.h:1707
	Convertformat CoercionForm // How to display this node - primnodes.h:1708
}

// NewConvertRowtypeExpr creates a new ConvertRowtypeExpr node.
func NewConvertRowtypeExpr(arg Expression, resulttype Oid, convertformat CoercionForm) *ConvertRowtypeExpr {
	return &ConvertRowtypeExpr{
		BaseExpr:      BaseExpr{BaseNode: BaseNode{Tag: T_ConvertRowtypeExpr}},
		Arg:           arg,
		Resulttype:    resulttype,
		Convertformat: convertformat,
	}
}

func (crte *ConvertRowtypeExpr) ExpressionType() string {
	return "ConvertRowtypeExpr"
}

func (crte *ConvertRowtypeExpr) String() string {
	return fmt.Sprintf("ConvertRowtypeExpr(%s as %d)", crte.Arg, crte.Resulttype)
}

// CollateExpr represents a COLLATE expression.
// This specifies a collation to be used for a particular expression.
// Ported from postgres/src/include/nodes/primnodes.h:1276
type CollateExpr struct {
	BaseExpr
	Arg    Expression // Input expression - primnodes.h:1649
	CollOid Oid       // Collation OID - primnodes.h:1650
}

// NewCollateExpr creates a new CollateExpr node.
func NewCollateExpr(arg Expression, collOid Oid) *CollateExpr {
	return &CollateExpr{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_CollateExpr}},
		Arg:      arg,
		CollOid:  collOid,
	}
}

func (ce *CollateExpr) ExpressionType() string {
	return "CollateExpr"
}

func (ce *CollateExpr) String() string {
	return fmt.Sprintf("CollateExpr(%s COLLATE %d)", ce.Arg, ce.CollOid)
}

// ==============================================================================
// FIELD AND RECORD OPERATIONS
// ==============================================================================

// FieldSelect represents field selection from a composite value (record.field).
// This extracts a single field from a composite type value.
// Ported from postgres/src/include/nodes/primnodes.h:1125
type FieldSelect struct {
	BaseExpr
	Arg          Expression // Input expression (composite type) - primnodes.h:1409
	Fieldnum     AttrNumber // Attribute number of field to extract - primnodes.h:1410
	Resulttype   Oid        // Type OID of the field - primnodes.h:1411
	Resulttypmod int32      // Output typmod (usually -1) - primnodes.h:1412
}

// NewFieldSelect creates a new FieldSelect node.
func NewFieldSelect(arg Expression, fieldnum AttrNumber, resulttype Oid) *FieldSelect {
	return &FieldSelect{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_FieldSelect}},
		Arg:          arg,
		Fieldnum:     fieldnum,
		Resulttype:   resulttype,
		Resulttypmod: -1,
	}
}

func (fs *FieldSelect) ExpressionType() string {
	return "FieldSelect"
}

func (fs *FieldSelect) String() string {
	return fmt.Sprintf("FieldSelect(%s.%d)", fs.Arg, fs.Fieldnum)
}

// FieldStore represents field assignment to a composite value.
// This is used for UPDATE operations on composite type columns.
// Ported from postgres/src/include/nodes/primnodes.h:1156
type FieldStore struct {
	BaseExpr
	Arg        Expression   // Input expression (composite type) - primnodes.h:1429
	Newvals    []Expression // New value(s) for field(s) - primnodes.h:1430
	Fieldnums  []AttrNumber // Field number(s) to be updated - primnodes.h:1431
	Resulttype Oid          // Type OID of result (same as input type) - primnodes.h:1432
}

// NewFieldStore creates a new FieldStore node.
func NewFieldStore(arg Expression, newvals []Expression, fieldnums []AttrNumber, resulttype Oid) *FieldStore {
	return &FieldStore{
		BaseExpr:   BaseExpr{BaseNode: BaseNode{Tag: T_FieldStore}},
		Arg:        arg,
		Newvals:    newvals,
		Fieldnums:  fieldnums,
		Resulttype: resulttype,
	}
}

// NewSingleFieldStore creates a FieldStore for updating a single field.
func NewSingleFieldStore(arg Expression, newval Expression, fieldnum AttrNumber, resulttype Oid) *FieldStore {
	return &FieldStore{
		BaseExpr:   BaseExpr{BaseNode: BaseNode{Tag: T_FieldStore}},
		Arg:        arg,
		Newvals:    []Expression{newval},
		Fieldnums:  []AttrNumber{fieldnum},
		Resulttype: resulttype,
	}
}

func (fs *FieldStore) ExpressionType() string {
	return "FieldStore"
}

func (fs *FieldStore) String() string {
	return fmt.Sprintf("FieldStore(%s, fields=%d)", fs.Arg, len(fs.Fieldnums))
}

// SubscriptingRef represents array/JSON subscripting operations.
// This handles both array indexing (arr[1]) and JSON key access (json['key']).
// Ported from postgres/src/include/nodes/primnodes.h:679
type SubscriptingRef struct {
	BaseExpr
	Refcontainertype Oid          // Type OID of container (array or jsonb) - primnodes.h:598
	Refelemtype      Oid          // Type OID of contained elements - primnodes.h:599
	Reftypmod        int32        // Typmod of container type - primnodes.h:600
	Refcollid        Oid          // OID of collation, or InvalidOid if none - primnodes.h:601
	Refupperindexpr  []Expression // Expressions for upper index bounds - primnodes.h:602
	Reflowerindexpr  []Expression // Expressions for lower index bounds - primnodes.h:603
	Refexpr          Expression   // Expression for the container value - primnodes.h:604
	Refassgnexpr     Expression   // Expression for new value in assignment - primnodes.h:605
}

// NewSubscriptingRef creates a new SubscriptingRef node.
func NewSubscriptingRef(containertype, elemtype Oid, refexpr Expression, upperindex []Expression) *SubscriptingRef {
	return &SubscriptingRef{
		BaseExpr:         BaseExpr{BaseNode: BaseNode{Tag: T_SubscriptingRef}},
		Refcontainertype: containertype,
		Refelemtype:      elemtype,
		Reftypmod:        -1,
		Refupperindexpr:  upperindex,
		Refexpr:          refexpr,
	}
}

// NewArraySubscript creates a SubscriptingRef for array indexing (arr[index]).
func NewArraySubscript(arraytype, elemtype Oid, arrayexpr, indexexpr Expression) *SubscriptingRef {
	return &SubscriptingRef{
		BaseExpr:         BaseExpr{BaseNode: BaseNode{Tag: T_SubscriptingRef}},
		Refcontainertype: arraytype,
		Refelemtype:      elemtype,
		Reftypmod:        -1,
		Refupperindexpr:  []Expression{indexexpr},
		Refexpr:          arrayexpr,
	}
}

// NewArraySlice creates a SubscriptingRef for array slicing (arr[lower:upper]).
func NewArraySlice(arraytype, elemtype Oid, arrayexpr, lowerexpr, upperexpr Expression) *SubscriptingRef {
	return &SubscriptingRef{
		BaseExpr:         BaseExpr{BaseNode: BaseNode{Tag: T_SubscriptingRef}},
		Refcontainertype: arraytype,
		Refelemtype:      elemtype,
		Reftypmod:        -1,
		Refupperindexpr:  []Expression{upperexpr},
		Reflowerindexpr:  []Expression{lowerexpr},
		Refexpr:          arrayexpr,
	}
}

// NewArrayAssignment creates a SubscriptingRef for array assignment (arr[index] = value).
func NewArrayAssignment(arraytype, elemtype Oid, arrayexpr, indexexpr, assignexpr Expression) *SubscriptingRef {
	return &SubscriptingRef{
		BaseExpr:         BaseExpr{BaseNode: BaseNode{Tag: T_SubscriptingRef}},
		Refcontainertype: arraytype,
		Refelemtype:      elemtype,
		Reftypmod:        -1,
		Refupperindexpr:  []Expression{indexexpr},
		Refexpr:          arrayexpr,
		Refassgnexpr:     assignexpr,
	}
}

func (sr *SubscriptingRef) ExpressionType() string {
	return "SubscriptingRef"
}

func (sr *SubscriptingRef) String() string {
	if sr.Refassgnexpr != nil {
		return fmt.Sprintf("SubscriptingRef(%s[...] = %s)", sr.Refexpr, sr.Refassgnexpr)
	}
	if len(sr.Reflowerindexpr) > 0 {
		return fmt.Sprintf("SubscriptingRef(%s[%d:%d])", sr.Refexpr, len(sr.Reflowerindexpr), len(sr.Refupperindexpr))
	}
	return fmt.Sprintf("SubscriptingRef(%s[%d])", sr.Refexpr, len(sr.Refupperindexpr))
}

// ==============================================================================
// TEST EXPRESSIONS - NULL, BOOLEAN, AND DOMAIN TESTS
// ==============================================================================

// NullTestType represents the type of NULL test.
// Ported from postgres/src/include/nodes/primnodes.h:1950
type NullTestType int

const (
	IS_NULL     NullTestType = iota // IS NULL - primnodes.h:1952
	IS_NOT_NULL                     // IS NOT NULL - primnodes.h:1952
)

// NullTest represents IS NULL and IS NOT NULL tests.
// This is one of the most fundamental SQL test expressions.
// Ported from postgres/src/include/nodes/primnodes.h:1955
type NullTest struct {
	BaseExpr
	Arg         Expression   // Input expression - primnodes.h:1958
	Nulltesttype NullTestType // IS NULL or IS NOT NULL - primnodes.h:1959
	Argisrow     bool         // True if input is known to be a row value - primnodes.h:1961
}

// NewNullTest creates a new NullTest node.
func NewNullTest(arg Expression, nulltesttype NullTestType) *NullTest {
	return &NullTest{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_NullTest}},
		Arg:          arg,
		Nulltesttype: nulltesttype,
	}
}

// NewIsNullTest creates a new IS NULL test.
func NewIsNullTest(arg Expression) *NullTest {
	return &NullTest{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_NullTest}},
		Arg:          arg,
		Nulltesttype: IS_NULL,
	}
}

// NewIsNotNullTest creates a new IS NOT NULL test.
func NewIsNotNullTest(arg Expression) *NullTest {
	return &NullTest{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_NullTest}},
		Arg:          arg,
		Nulltesttype: IS_NOT_NULL,
	}
}

// NewRowNullTest creates a NullTest for row values.
func NewRowNullTest(arg Expression, nulltesttype NullTestType) *NullTest {
	return &NullTest{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_NullTest}},
		Arg:          arg,
		Nulltesttype: nulltesttype,
		Argisrow:     true,
	}
}

func (nt *NullTest) ExpressionType() string {
	return "NullTest"
}

func (nt *NullTest) String() string {
	testStrs := map[NullTestType]string{
		IS_NULL: "IS NULL", IS_NOT_NULL: "IS NOT NULL",
	}
	testStr := testStrs[nt.Nulltesttype]
	if testStr == "" {
		testStr = fmt.Sprintf("NULLTEST_%d", int(nt.Nulltesttype))
	}
	
	row := ""
	if nt.Argisrow {
		row = " (ROW)"
	}
	
	return fmt.Sprintf("NullTest(%s %s%s)", nt.Arg, testStr, row)
}

// BoolTestType represents the type of boolean test.
// Ported from postgres/src/include/nodes/primnodes.h:1974
type BoolTestType int

const (
	IS_TRUE     BoolTestType = iota // IS TRUE - primnodes.h:1800
	IS_NOT_TRUE                     // IS NOT TRUE - primnodes.h:1801
	IS_FALSE                        // IS FALSE - primnodes.h:1802
	IS_NOT_FALSE                    // IS NOT FALSE - primnodes.h:1803
	IS_UNKNOWN                      // IS UNKNOWN - primnodes.h:1804
	IS_NOT_UNKNOWN                  // IS NOT UNKNOWN - primnodes.h:1805
)

// BooleanTest represents boolean test expressions (IS TRUE, IS FALSE, etc.).
// These tests handle three-valued boolean logic (TRUE/FALSE/UNKNOWN).
// Ported from postgres/src/include/nodes/primnodes.h:1979
type BooleanTest struct {
	BaseExpr
	Arg         Expression   // Input expression - primnodes.h:1806
	Booltesttype BoolTestType // Kind of test - primnodes.h:1807
}

// NewBooleanTest creates a new BooleanTest node.
func NewBooleanTest(arg Expression, booltesttype BoolTestType) *BooleanTest {
	return &BooleanTest{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_BooleanTest}},
		Arg:          arg,
		Booltesttype: booltesttype,
	}
}

// NewIsTrueTest creates a new IS TRUE test.
func NewIsTrueTest(arg Expression) *BooleanTest {
	return &BooleanTest{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_BooleanTest}},
		Arg:          arg,
		Booltesttype: IS_TRUE,
	}
}

// NewIsFalseTest creates a new IS FALSE test.
func NewIsFalseTest(arg Expression) *BooleanTest {
	return &BooleanTest{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_BooleanTest}},
		Arg:          arg,
		Booltesttype: IS_FALSE,
	}
}

// NewIsUnknownTest creates a new IS UNKNOWN test.
func NewIsUnknownTest(arg Expression) *BooleanTest {
	return &BooleanTest{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_BooleanTest}},
		Arg:          arg,
		Booltesttype: IS_UNKNOWN,
	}
}

func (bt *BooleanTest) ExpressionType() string {
	return "BooleanTest"
}

func (bt *BooleanTest) String() string {
	testStrs := map[BoolTestType]string{
		IS_TRUE: "IS TRUE", IS_NOT_TRUE: "IS NOT TRUE",
		IS_FALSE: "IS FALSE", IS_NOT_FALSE: "IS NOT FALSE",
		IS_UNKNOWN: "IS UNKNOWN", IS_NOT_UNKNOWN: "IS NOT UNKNOWN",
	}
	testStr := testStrs[bt.Booltesttype]
	if testStr == "" {
		testStr = fmt.Sprintf("BOOLTEST_%d", int(bt.Booltesttype))
	}
	
	return fmt.Sprintf("BooleanTest(%s %s)", bt.Arg, testStr)
}

// CoerceToDomain represents coercion to a domain type.
// Domain types are user-defined types with constraints.
// Ported from postgres/src/include/nodes/primnodes.h:2025
type CoerceToDomain struct {
	BaseExpr
	Arg            Expression    // Input expression - primnodes.h:1716
	Resulttype     Oid          // Domain type OID - primnodes.h:1717
	Resulttypmod   int32        // Output typmod (usually -1) - primnodes.h:1718
	Resultcollid   Oid          // OID of collation, or InvalidOid if none - primnodes.h:1719
	Coercionformat CoercionForm // How to display this coercion - primnodes.h:1720
}

// NewCoerceToDomain creates a new CoerceToDomain node.
func NewCoerceToDomain(arg Expression, resulttype Oid, resulttypmod int32, coercionformat CoercionForm) *CoerceToDomain {
	return &CoerceToDomain{
		BaseExpr:       BaseExpr{BaseNode: BaseNode{Tag: T_CoerceToDomain}},
		Arg:            arg,
		Resulttype:     resulttype,
		Resulttypmod:   resulttypmod,
		Coercionformat: coercionformat,
	}
}

func (ctd *CoerceToDomain) ExpressionType() string {
	return "CoerceToDomain"
}

func (ctd *CoerceToDomain) String() string {
	return fmt.Sprintf("CoerceToDomain(%s as domain %d)", ctd.Arg, ctd.Resulttype)
}

// CoerceToDomainValue represents a value being coerced to a domain type.
// This is used in domain constraint checking.
// Ported from postgres/src/include/nodes/primnodes.h:2048
type CoerceToDomainValue struct {
	BaseExpr
	Typemod   int32 // Typmod of domain - primnodes.h:1733
	Collation Oid   // Collation of domain - primnodes.h:1734
}

// NewCoerceToDomainValue creates a new CoerceToDomainValue node.
func NewCoerceToDomainValue(typemod int32, collation Oid) *CoerceToDomainValue {
	return &CoerceToDomainValue{
		BaseExpr:  BaseExpr{BaseNode: BaseNode{Tag: T_CoerceToDomainValue}},
		Typemod:   typemod,
		Collation: collation,
	}
}

func (ctdv *CoerceToDomainValue) ExpressionType() string {
	return "CoerceToDomainValue"
}

func (ctdv *CoerceToDomainValue) String() string {
	return fmt.Sprintf("CoerceToDomainValue(typmod=%d, collation=%d)", ctdv.Typemod, ctdv.Collation)
}

// ==============================================================================
// SPECIAL VALUE EXPRESSIONS
// ==============================================================================

// SetToDefault represents a DEFAULT expression.
// This is used in INSERT and UPDATE statements to indicate use of the default value.
// Ported from postgres/src/include/nodes/primnodes.h:2068
type SetToDefault struct {
	BaseExpr
	Typemod   int32 // Typmod of destination column - primnodes.h:1744
	Collation Oid   // Collation of destination column - primnodes.h:1745
}

// NewSetToDefault creates a new SetToDefault node.
func NewSetToDefault(typemod int32, collation Oid) *SetToDefault {
	return &SetToDefault{
		BaseExpr:  BaseExpr{BaseNode: BaseNode{Tag: T_SetToDefault}},
		Typemod:   typemod,
		Collation: collation,
	}
}

func (std *SetToDefault) ExpressionType() string {
	return "SetToDefault"
}

func (std *SetToDefault) String() string {
	return "SetToDefault(DEFAULT)"
}

// CurrentOfExpr represents CURRENT OF cursor_name expressions.
// This is used in UPDATE and DELETE statements to refer to the current row of a cursor.
// Ported from postgres/src/include/nodes/primnodes.h:2094
type CurrentOfExpr struct {
	BaseExpr
	CvarNo     Index  // RT index of target relation - primnodes.h:1756
	CursorName string // Name of referenced cursor, or NULL - primnodes.h:1757
	CursorParam int   // Refno of param for cursor name, or 0 - primnodes.h:1758
}

// NewCurrentOfExpr creates a new CurrentOfExpr node.
func NewCurrentOfExpr(cvarNo Index, cursorName string) *CurrentOfExpr {
	return &CurrentOfExpr{
		BaseExpr:   BaseExpr{BaseNode: BaseNode{Tag: T_CurrentOfExpr}},
		CvarNo:     cvarNo,
		CursorName: cursorName,
	}
}

// NewCurrentOfExprParam creates a new CurrentOfExpr with parameter reference.
func NewCurrentOfExprParam(cvarNo Index, cursorParam int) *CurrentOfExpr {
	return &CurrentOfExpr{
		BaseExpr:    BaseExpr{BaseNode: BaseNode{Tag: T_CurrentOfExpr}},
		CvarNo:      cvarNo,
		CursorParam: cursorParam,
	}
}

func (coe *CurrentOfExpr) ExpressionType() string {
	return "CurrentOfExpr"
}

func (coe *CurrentOfExpr) String() string {
	if coe.CursorName != "" {
		return fmt.Sprintf("CurrentOfExpr(CURRENT OF %s)", coe.CursorName)
	}
	return fmt.Sprintf("CurrentOfExpr(CURRENT OF $%d)", coe.CursorParam)
}

// NextValueExpr represents nextval() and currval() sequence operations.
// This handles sequence value generation and retrieval.
// Ported from postgres/src/include/nodes/primnodes.h:2109
type NextValueExpr struct {
	BaseExpr
	SeqId   Oid // OID of sequence relation - primnodes.h:1769
	TypeId  Oid // Type OID of result - primnodes.h:1770
}

// NewNextValueExpr creates a new NextValueExpr node.
func NewNextValueExpr(seqId, typeId Oid) *NextValueExpr {
	return &NextValueExpr{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_NextValueExpr}},
		SeqId:    seqId,
		TypeId:   typeId,
	}
}

func (nve *NextValueExpr) ExpressionType() string {
	return "NextValueExpr"
}

func (nve *NextValueExpr) String() string {
	return fmt.Sprintf("NextValueExpr(seq=%d, type=%d)", nve.SeqId, nve.TypeId)
}

// InferenceElem represents an inference element for ON CONFLICT clauses.
// This is used to specify which unique index to use for conflict detection.
// Ported from postgres/src/include/nodes/primnodes.h:2123
type InferenceElem struct {
	BaseExpr
	Expr      Expression // Expression to infer from - primnodes.h:2014
	Infercollid Oid      // OID of collation, or InvalidOid - primnodes.h:2015
	Inferopclass Oid     // OID of operator class, or InvalidOid - primnodes.h:2016
}

// NewInferenceElem creates a new InferenceElem node.
func NewInferenceElem(expr Expression) *InferenceElem {
	return &InferenceElem{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_InferenceElem}},
		Expr:     expr,
	}
}

// NewInferenceElemWithCollation creates a new InferenceElem with collation.
func NewInferenceElemWithCollation(expr Expression, infercollid, inferopclass Oid) *InferenceElem {
	return &InferenceElem{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_InferenceElem}},
		Expr:         expr,
		Infercollid:  infercollid,
		Inferopclass: inferopclass,
	}
}

func (ie *InferenceElem) ExpressionType() string {
	return "InferenceElem"
}

func (ie *InferenceElem) String() string {
	if ie.Infercollid != 0 || ie.Inferopclass != 0 {
		return fmt.Sprintf("InferenceElem(%s, coll=%d, opclass=%d)", ie.Expr, ie.Infercollid, ie.Inferopclass)
	}
	return fmt.Sprintf("InferenceElem(%s)", ie.Expr)
}