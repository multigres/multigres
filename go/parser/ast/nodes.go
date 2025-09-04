// Package ast provides PostgreSQL AST node definitions and interfaces.
// Ported from postgres/src/include/nodes/nodes.h and related header files.
package ast

import (
	"fmt"
	"strings"
)

// NodeTag represents the type of an AST node.
// Ported from postgres/src/include/nodes/nodes.h:26-31 (NodeTag enum)
type NodeTag int

// NodeTag constants - ported from postgres/src/include/nodes/nodes.h:28-30
// These represent the fundamental node types in the PostgreSQL AST
const (
	T_Invalid NodeTag = iota // Ported from postgres/src/include/nodes/nodes.h:28

	// Basic node types - will be expanded in full implementation
	T_Node
	T_Query
	T_SelectStmt
	T_InsertStmt
	T_UpdateStmt
	T_DeleteStmt
	T_MergeStmt
	T_CreateStmt
	T_DropStmt
	T_AlterStmt
	T_AlterTableStmt
	T_AlterTableMoveAllStmt
	T_AlterTableCmd
	T_ReplicaIdentityStmt
	T_AlterDomainStmt
	T_IndexStmt
	T_IndexElem
	T_DefElem
	T_Constraint
	T_ViewStmt
	T_CreateSchemaStmt
	T_CreateSeqStmt
	T_AlterSeqStmt
	T_CreateTableAsStmt
	T_CreateAssertionStmt
	T_RefreshMatViewStmt
	T_CreateExtensionStmt
	T_AlterExtensionStmt
	T_AlterExtensionContentsStmt
	T_CreateFdwStmt
	T_AlterFdwStmt
	T_AlterForeignServerStmt
	T_AlterUserMappingStmt
	T_DropUserMappingStmt
	T_CreateEventTrigStmt
	T_AlterEventTrigStmt
	T_CreatedbStmt
	T_DropdbStmt
	T_DropTableSpaceStmt
	T_DropOwnedStmt
	T_ReassignOwnedStmt
	T_ImportForeignSchemaStmt
	T_CreateTableSpaceStmt
	T_AlterTableSpaceStmt
	T_CreateAmStmt
	T_CreateStatsStmt
	T_CreateCastStmt
	T_AlterStatsStmt
	T_AlterFunctionStmt
	T_AlterTypeStmt
	T_CreatePublicationStmt
	T_AlterPublicationStmt
	T_PublicationTable
	T_PublicationObjSpec
	T_CreateSubscriptionStmt
	T_AlterSubscriptionStmt
	T_AlterOpFamilyStmt
	T_CreateOpClassItem
	T_CreateOpClassStmt
	T_CreateOpFamilyStmt
	T_CreateDomainStmt
	T_DefineStmt
	T_CreateEnumStmt
	T_CreateRangeStmt
	T_AlterEnumStmt
	T_CompositeTypeStmt
	T_CreateFunctionStmt
	T_FunctionParameter
	T_CreateConversionStmt
	T_CreateTransformStmt
	T_CreatePLangStmt
	T_RoleSpec
	T_TypeName
	T_CollateClause

	// Utility statement nodes
	T_TransactionStmt
	T_GrantStmt
	T_GrantRoleStmt
	T_AlterDefaultPrivilegesStmt
	T_AccessPriv
	T_CreateRoleStmt
	T_AlterRoleStmt
	T_AlterRoleSetStmt
	T_DropRoleStmt
	T_VariableSetStmt
	T_VariableShowStmt
	T_AlterSystemStmt
	T_ExplainStmt
	T_PrepareStmt
	T_ExecuteStmt
	T_DeallocateStmt
	T_DeclareCursorStmt
	T_FetchStmt
	T_ClosePortalStmt
	T_CopyStmt
	T_VacuumStmt
	T_VacuumRelation
	T_ReindexStmt
	T_ClusterStmt
	T_CheckPointStmt
	T_DiscardStmt
	T_LoadStmt
	T_NotifyStmt
	T_ListenStmt
	T_UnlistenStmt
	T_ConstraintsSetStmt

	// Expression nodes
	T_Expr
	T_Var
	T_Const
	T_Param
	T_Aggref
	T_WindowFunc
	T_FuncExpr
	T_OpExpr
	T_BoolExpr
	T_CaseExpr
	T_ArrayExpr
	T_RowExpr
	T_CoalesceExpr
	T_ScalarArrayOpExpr
	T_SubLink

	// Query execution nodes (essential for SQL functionality)
	T_TargetEntry
	T_FromExpr
	T_JoinExpr
	T_SubPlan
	T_AlternativeSubPlan
	T_CommonTableExpr
	T_WindowClause
	T_SortGroupClause
	T_RowMarkClause
	T_OnConflictExpr

	// Type coercion and advanced expression nodes (essential for PostgreSQL type system)
	T_RelabelType
	T_CoerceViaIO
	T_ArrayCoerceExpr
	T_ConvertRowtypeExpr
	T_CollateExpr
	T_FieldSelect
	T_FieldStore
	T_SubscriptingRef
	T_NullTest
	T_BooleanTest
	T_CoerceToDomain
	T_CoerceToDomainValue
	T_SetToDefault
	T_CurrentOfExpr
	T_NextValueExpr
	T_InferenceElem

	// Administrative and advanced DDL nodes (comprehensive PostgreSQL DDL support)
	T_TableLikeClause
	T_PartitionSpec
	T_PartitionBoundSpec
	T_PartitionRangeDatum
	T_StatsElem
	T_CreateForeignServerStmt
	T_CreateForeignTableStmt
	T_CreateUserMappingStmt
	T_CreateTriggerStmt
	T_CreatePolicyStmt
	T_AlterPolicyStmt
	T_TriggerTransition

	// List and utility nodes
	T_List
	T_ResTarget
	T_RangeVar
	T_ColumnRef
	T_AConst

	// Core parse infrastructure nodes - Stage 1A (25 nodes)
	T_RawStmt
	T_A_Expr
	T_A_Const
	T_ParamRef
	T_TypeCast
	T_ParenExpr
	T_FuncCall
	T_A_Star
	T_A_Indices
	T_A_Indirection
	T_A_ArrayExpr
	T_ColumnDef
	T_WithClause
	T_CTESearchClause
	T_CTECycleClause
	T_MultiAssignRef
	T_WindowDef
	T_SortBy
	T_GroupingSet
	T_LockingClause
	T_XmlSerialize
	T_PartitionElem
	T_TableSampleClause
	T_ObjectWithArgs
	T_SinglePartitionSpec
	T_PartitionCmd

	// Advanced statement nodes - Phase 1B (12 nodes)
	T_SetOperationStmt
	T_ReturnStmt
	T_PLAssignStmt
	T_OnConflictClause
	T_InferClause
	T_WithCheckOption
	T_MergeWhenClause
	T_TruncateStmt
	T_CommentStmt
	T_SecLabelStmt
	T_DoStmt
	T_CallStmt
	T_RenameStmt
	T_AlterOwnerStmt
	T_RuleStmt
	T_LockStmt
	T_AlterObjectSchemaStmt
	T_AlterOperatorStmt
	T_AlterObjectDependsStmt
	T_AlterCollationStmt
	T_AlterDatabaseStmt
	T_AlterDatabaseSetStmt
	T_AlterDatabaseRefreshCollStmt
	T_AlterCompositeTypeStmt
	T_AlterTSConfigurationStmt
	T_AlterTSDictionaryStmt

	// Range table and FROM clause nodes - Phase 1D (9 nodes)
	T_RangeTblEntry
	T_RangeSubselect
	T_RangeFunction
	T_RangeTableFunc
	T_RangeTableFuncCol
	T_RangeTableSample
	T_RangeTblFunction
	T_RTEPermissionInfo
	T_RangeTblRef

	// JSON parse tree nodes - Phase 1E (16 main nodes + 4 supporting types)
	T_JsonFormat
	T_JsonReturning
	T_JsonValueExpr
	T_JsonBehavior
	T_JsonOutput
	T_JsonArgument
	T_JsonFuncExpr
	T_JsonTablePathSpec
	T_JsonTable
	T_JsonTableColumn
	T_JsonKeyValue
	T_JsonParseExpr
	T_JsonScalarExpr
	T_JsonSerializeExpr
	T_JsonObjectConstructor
	T_JsonArrayConstructor
	T_JsonArrayQueryConstructor
	T_JsonAggConstructor
	T_JsonObjectAgg
	T_JsonArrayAgg

	// Phase 1G: JSON Primitive Expressions - primnodes.h JSON expression nodes
	T_JsonConstructorExpr  // JSON constructor expression - primnodes.h:1703
	T_JsonIsPredicate      // JSON IS predicate - primnodes.h:1732
	T_JsonExpr             // JSON expression - primnodes.h:1813
	T_JsonTablePath        // JSON table path - primnodes.h:1867
	T_JsonTablePlan        // JSON table plan - primnodes.h:1882
	T_JsonTablePathScan    // JSON table path scan - primnodes.h:1893
	T_JsonTableSiblingJoin // JSON table sibling join - primnodes.h:1923

	// Phase 1F: Primitive Expression Completion Part 1 - primnodes.h nodes
	T_GroupingFunc           // GROUPING function - primnodes.h:537
	T_WindowFuncRunCondition // Window function run condition - primnodes.h:596
	T_MergeSupportFunc       // Merge support function - primnodes.h:628
	T_NamedArgExpr           // Named argument expression - primnodes.h:787
	T_CaseTestExpr           // CASE test expression - primnodes.h:1352
	T_MinMaxExpr             // MIN/MAX expression - primnodes.h:1506
	T_RowCompareExpr         // Row comparison - primnodes.h:1463
	T_SQLValueFunction       // SQL value function - primnodes.h:1553
	T_XmlExpr                // XML expression - primnodes.h:1596
	T_MergeAction            // MERGE action - primnodes.h:2003
	T_TableFunc              // Table function - primnodes.h:109
	T_IntoClause             // INTO clause - primnodes.h:158

	// Value nodes - ported from postgres/src/include/nodes/value.h
	T_Integer   // Integer literal - postgres/src/include/nodes/value.h:28-34
	T_Float     // Float literal - postgres/src/include/nodes/value.h:47-53
	T_Boolean   // Boolean literal - postgres/src/include/nodes/value.h:55-61
	T_String    // String literal - postgres/src/include/nodes/value.h:63-69
	T_BitString // Bit string literal - postgres/src/include/nodes/value.h:71-77
	T_Null      // NULL literal
)

// String returns the string representation of a NodeTag.
// Used for debugging and error reporting.
func (nt NodeTag) String() string {
	switch nt {
	case T_Invalid:
		return "T_Invalid"
	case T_Node:
		return "T_Node"
	case T_Query:
		return "T_Query"
	case T_SelectStmt:
		return "T_SelectStmt"
	case T_InsertStmt:
		return "T_InsertStmt"
	case T_UpdateStmt:
		return "T_UpdateStmt"
	case T_DeleteStmt:
		return "T_DeleteStmt"
	case T_MergeStmt:
		return "T_MergeStmt"
	case T_CreateStmt:
		return "T_CreateStmt"
	case T_DropStmt:
		return "T_DropStmt"
	case T_AlterStmt:
		return "T_AlterStmt"
	case T_AlterTableStmt:
		return "T_AlterTableStmt"
	case T_AlterTableMoveAllStmt:
		return "T_AlterTableMoveAllStmt"
	case T_AlterTableCmd:
		return "T_AlterTableCmd"
	case T_ReplicaIdentityStmt:
		return "T_ReplicaIdentityStmt"
	case T_AlterDomainStmt:
		return "T_AlterDomainStmt"
	case T_IndexStmt:
		return "T_IndexStmt"
	case T_IndexElem:
		return "T_IndexElem"
	case T_DefElem:
		return "T_DefElem"
	case T_Constraint:
		return "T_Constraint"
	case T_ViewStmt:
		return "T_ViewStmt"
	case T_CreateSchemaStmt:
		return "T_CreateSchemaStmt"
	case T_CreateSeqStmt:
		return "T_CreateSeqStmt"
	case T_AlterSeqStmt:
		return "T_AlterSeqStmt"
	case T_CreateTableAsStmt:
		return "T_CreateTableAsStmt"
	case T_CreateAssertionStmt:
		return "T_CreateAssertionStmt"
	case T_RefreshMatViewStmt:
		return "T_RefreshMatViewStmt"
	case T_CreateExtensionStmt:
		return "T_CreateExtensionStmt"
	case T_AlterExtensionStmt:
		return "T_AlterExtensionStmt"
	case T_AlterExtensionContentsStmt:
		return "T_AlterExtensionContentsStmt"
	case T_CreateFdwStmt:
		return "T_CreateFdwStmt"
	case T_AlterFdwStmt:
		return "T_AlterFdwStmt"
	case T_AlterForeignServerStmt:
		return "T_AlterForeignServerStmt"
	case T_AlterUserMappingStmt:
		return "T_AlterUserMappingStmt"
	case T_CreateEventTrigStmt:
		return "T_CreateEventTrigStmt"
	case T_AlterEventTrigStmt:
		return "T_AlterEventTrigStmt"
	case T_CreatedbStmt:
		return "T_CreatedbStmt"
	case T_DropdbStmt:
		return "T_DropdbStmt"
	case T_DropTableSpaceStmt:
		return "T_DropTableSpaceStmt"
	case T_DropOwnedStmt:
		return "T_DropOwnedStmt"
	case T_ReassignOwnedStmt:
		return "T_ReassignOwnedStmt"
	case T_ImportForeignSchemaStmt:
		return "T_ImportForeignSchemaStmt"
	case T_CreateTableSpaceStmt:
		return "T_CreateTableSpaceStmt"
	case T_AlterTableSpaceStmt:
		return "T_AlterTableSpaceStmt"
	case T_CreateAmStmt:
		return "T_CreateAmStmt"
	case T_CreateStatsStmt:
		return "T_CreateStatsStmt"
	case T_CreateCastStmt:
		return "T_CreateCastStmt"
	case T_AlterStatsStmt:
		return "T_AlterStatsStmt"
	case T_AlterFunctionStmt:
		return "T_AlterFunctionStmt"
	case T_AlterTypeStmt:
		return "T_AlterTypeStmt"
	case T_CreatePublicationStmt:
		return "T_CreatePublicationStmt"
	case T_AlterPublicationStmt:
		return "T_AlterPublicationStmt"
	case T_PublicationTable:
		return "T_PublicationTable"
	case T_PublicationObjSpec:
		return "T_PublicationObjSpec"
	case T_CreateSubscriptionStmt:
		return "T_CreateSubscriptionStmt"
	case T_AlterSubscriptionStmt:
		return "T_AlterSubscriptionStmt"
	case T_AlterOpFamilyStmt:
		return "T_AlterOpFamilyStmt"
	case T_CreateOpClassItem:
		return "T_CreateOpClassItem"
	case T_CreateOpClassStmt:
		return "T_CreateOpClassStmt"
	case T_CreateOpFamilyStmt:
		return "T_CreateOpFamilyStmt"
	case T_DropUserMappingStmt:
		return "T_DropUserMappingStmt"
	case T_CreateDomainStmt:
		return "T_CreateDomainStmt"
	case T_DefineStmt:
		return "T_DefineStmt"
	case T_CreateEnumStmt:
		return "T_CreateEnumStmt"
	case T_CreateRangeStmt:
		return "T_CreateRangeStmt"
	case T_AlterEnumStmt:
		return "T_AlterEnumStmt"
	case T_CompositeTypeStmt:
		return "T_CompositeTypeStmt"
	case T_CreateFunctionStmt:
		return "T_CreateFunctionStmt"
	case T_FunctionParameter:
		return "T_FunctionParameter"
	case T_CreateConversionStmt:
		return "T_CreateConversionStmt"
	case T_CreateTransformStmt:
		return "T_CreateTransformStmt"
	case T_CreatePLangStmt:
		return "T_CreatePLangStmt"
	case T_RoleSpec:
		return "T_RoleSpec"
	case T_TypeName:
		return "T_TypeName"
	case T_CollateClause:
		return "T_CollateClause"
	case T_TransactionStmt:
		return "T_TransactionStmt"
	case T_GrantStmt:
		return "T_GrantStmt"
	case T_GrantRoleStmt:
		return "T_GrantRoleStmt"
	case T_AlterDefaultPrivilegesStmt:
		return "T_AlterDefaultPrivilegesStmt"
	case T_AccessPriv:
		return "T_AccessPriv"
	case T_CreateRoleStmt:
		return "T_CreateRoleStmt"
	case T_AlterRoleStmt:
		return "T_AlterRoleStmt"
	case T_AlterRoleSetStmt:
		return "T_AlterRoleSetStmt"
	case T_DropRoleStmt:
		return "T_DropRoleStmt"
	case T_VariableSetStmt:
		return "T_VariableSetStmt"
	case T_VariableShowStmt:
		return "T_VariableShowStmt"
	case T_ExplainStmt:
		return "T_ExplainStmt"
	case T_PrepareStmt:
		return "T_PrepareStmt"
	case T_ExecuteStmt:
		return "T_ExecuteStmt"
	case T_DeallocateStmt:
		return "T_DeallocateStmt"
	case T_DeclareCursorStmt:
		return "T_DeclareCursorStmt"
	case T_FetchStmt:
		return "T_FetchStmt"
	case T_ClosePortalStmt:
		return "T_ClosePortalStmt"
	case T_CopyStmt:
		return "T_CopyStmt"
	case T_VacuumStmt:
		return "T_VacuumStmt"
	case T_VacuumRelation:
		return "T_VacuumRelation"
	case T_ReindexStmt:
		return "T_ReindexStmt"
	case T_ClusterStmt:
		return "T_ClusterStmt"
	case T_CheckPointStmt:
		return "T_CheckPointStmt"
	case T_DiscardStmt:
		return "T_DiscardStmt"
	case T_LoadStmt:
		return "T_LoadStmt"
	case T_NotifyStmt:
		return "T_NotifyStmt"
	case T_ListenStmt:
		return "T_ListenStmt"
	case T_UnlistenStmt:
		return "T_UnlistenStmt"
	case T_ConstraintsSetStmt:
		return "T_ConstraintsSetStmt"
	case T_Expr:
		return "T_Expr"
	case T_Var:
		return "T_Var"
	case T_Const:
		return "T_Const"
	case T_Param:
		return "T_Param"
	case T_Aggref:
		return "T_Aggref"
	case T_WindowFunc:
		return "T_WindowFunc"
	case T_FuncExpr:
		return "T_FuncExpr"
	case T_OpExpr:
		return "T_OpExpr"
	case T_BoolExpr:
		return "T_BoolExpr"
	case T_CaseExpr:
		return "T_CaseExpr"
	case T_ArrayExpr:
		return "T_ArrayExpr"
	case T_RowExpr:
		return "T_RowExpr"
	case T_CoalesceExpr:
		return "T_CoalesceExpr"
	case T_ScalarArrayOpExpr:
		return "T_ScalarArrayOpExpr"
	case T_SubLink:
		return "T_SubLink"
	case T_TargetEntry:
		return "T_TargetEntry"
	case T_FromExpr:
		return "T_FromExpr"
	case T_JoinExpr:
		return "T_JoinExpr"
	case T_SubPlan:
		return "T_SubPlan"
	case T_AlternativeSubPlan:
		return "T_AlternativeSubPlan"
	case T_CommonTableExpr:
		return "T_CommonTableExpr"
	case T_WindowClause:
		return "T_WindowClause"
	case T_SortGroupClause:
		return "T_SortGroupClause"
	case T_RowMarkClause:
		return "T_RowMarkClause"
	case T_OnConflictExpr:
		return "T_OnConflictExpr"
	case T_RelabelType:
		return "T_RelabelType"
	case T_CoerceViaIO:
		return "T_CoerceViaIO"
	case T_ArrayCoerceExpr:
		return "T_ArrayCoerceExpr"
	case T_ConvertRowtypeExpr:
		return "T_ConvertRowtypeExpr"
	case T_CollateExpr:
		return "T_CollateExpr"
	case T_FieldSelect:
		return "T_FieldSelect"
	case T_FieldStore:
		return "T_FieldStore"
	case T_SubscriptingRef:
		return "T_SubscriptingRef"
	case T_NullTest:
		return "T_NullTest"
	case T_BooleanTest:
		return "T_BooleanTest"
	case T_CoerceToDomain:
		return "T_CoerceToDomain"
	case T_CoerceToDomainValue:
		return "T_CoerceToDomainValue"
	case T_SetToDefault:
		return "T_SetToDefault"
	case T_CurrentOfExpr:
		return "T_CurrentOfExpr"
	case T_NextValueExpr:
		return "T_NextValueExpr"
	case T_InferenceElem:
		return "T_InferenceElem"
	case T_TableLikeClause:
		return "T_TableLikeClause"
	case T_PartitionSpec:
		return "T_PartitionSpec"
	case T_PartitionBoundSpec:
		return "T_PartitionBoundSpec"
	case T_PartitionRangeDatum:
		return "T_PartitionRangeDatum"
	case T_StatsElem:
		return "T_StatsElem"
	case T_CreateForeignServerStmt:
		return "T_CreateForeignServerStmt"
	case T_CreateForeignTableStmt:
		return "T_CreateForeignTableStmt"
	case T_CreateUserMappingStmt:
		return "T_CreateUserMappingStmt"
	case T_CreateTriggerStmt:
		return "T_CreateTriggerStmt"
	case T_CreatePolicyStmt:
		return "T_CreatePolicyStmt"
	case T_AlterPolicyStmt:
		return "T_AlterPolicyStmt"
	case T_TriggerTransition:
		return "T_TriggerTransition"
	case T_List:
		return "T_List"
	case T_ResTarget:
		return "T_ResTarget"
	case T_RangeVar:
		return "T_RangeVar"
	case T_ColumnRef:
		return "T_ColumnRef"
	case T_AConst:
		return "T_AConst"
	case T_Integer:
		return "T_Integer"
	case T_Float:
		return "T_Float"
	case T_Boolean:
		return "T_Boolean"
	case T_String:
		return "T_String"
	case T_BitString:
		return "T_BitString"
	case T_Null:
		return "T_Null"
	// Core parse infrastructure nodes - Stage 1A
	case T_RawStmt:
		return "T_RawStmt"
	case T_A_Expr:
		return "T_A_Expr"
	case T_A_Const:
		return "T_A_Const"
	case T_ParamRef:
		return "T_ParamRef"
	case T_TypeCast:
		return "T_TypeCast"
	case T_ParenExpr:
		return "T_ParenExpr"
	case T_FuncCall:
		return "T_FuncCall"
	case T_A_Star:
		return "T_A_Star"
	case T_A_Indices:
		return "T_A_Indices"
	case T_A_Indirection:
		return "T_A_Indirection"
	case T_A_ArrayExpr:
		return "T_A_ArrayExpr"
	case T_ColumnDef:
		return "T_ColumnDef"
	case T_WithClause:
		return "T_WithClause"
	case T_CTESearchClause:
		return "T_CTESearchClause"
	case T_CTECycleClause:
		return "T_CTECycleClause"
	case T_MultiAssignRef:
		return "T_MultiAssignRef"
	case T_WindowDef:
		return "T_WindowDef"
	case T_SortBy:
		return "T_SortBy"
	case T_GroupingSet:
		return "T_GroupingSet"
	case T_LockingClause:
		return "T_LockingClause"
	case T_XmlSerialize:
		return "T_XmlSerialize"
	case T_PartitionElem:
		return "T_PartitionElem"
	case T_TableSampleClause:
		return "T_TableSampleClause"
	case T_ObjectWithArgs:
		return "T_ObjectWithArgs"
	case T_SinglePartitionSpec:
		return "T_SinglePartitionSpec"
	case T_PartitionCmd:
		return "T_PartitionCmd"
	case T_SetOperationStmt:
		return "T_SetOperationStmt"
	case T_ReturnStmt:
		return "T_ReturnStmt"
	case T_PLAssignStmt:
		return "T_PLAssignStmt"
	case T_OnConflictClause:
		return "T_OnConflictClause"
	case T_InferClause:
		return "T_InferClause"
	case T_WithCheckOption:
		return "T_WithCheckOption"
	case T_MergeWhenClause:
		return "T_MergeWhenClause"
	case T_TruncateStmt:
		return "T_TruncateStmt"
	case T_CommentStmt:
		return "T_CommentStmt"
	case T_SecLabelStmt:
		return "T_SecLabelStmt"
	case T_DoStmt:
		return "T_DoStmt"
	case T_CallStmt:
		return "T_CallStmt"
	case T_RenameStmt:
		return "T_RenameStmt"
	case T_AlterOwnerStmt:
		return "T_AlterOwnerStmt"
	case T_RuleStmt:
		return "T_RuleStmt"
	case T_LockStmt:
		return "T_LockStmt"
	case T_AlterObjectSchemaStmt:
		return "T_AlterObjectSchemaStmt"
	case T_AlterOperatorStmt:
		return "T_AlterOperatorStmt"
	case T_AlterObjectDependsStmt:
		return "T_AlterObjectDependsStmt"
	case T_AlterCollationStmt:
		return "T_AlterCollationStmt"
	case T_AlterDatabaseStmt:
		return "T_AlterDatabaseStmt"
	case T_AlterDatabaseSetStmt:
		return "T_AlterDatabaseSetStmt"
	case T_AlterDatabaseRefreshCollStmt:
		return "T_AlterDatabaseRefreshCollStmt"
	case T_AlterCompositeTypeStmt:
		return "T_AlterCompositeTypeStmt"
	case T_AlterTSConfigurationStmt:
		return "T_AlterTSConfigurationStmt"
	case T_AlterTSDictionaryStmt:
		return "T_AlterTSDictionaryStmt"
	case T_RangeTblEntry:
		return "T_RangeTblEntry"
	case T_RangeSubselect:
		return "T_RangeSubselect"
	case T_RangeFunction:
		return "T_RangeFunction"
	case T_RangeTableFunc:
		return "T_RangeTableFunc"
	case T_RangeTableFuncCol:
		return "T_RangeTableFuncCol"
	case T_RangeTableSample:
		return "T_RangeTableSample"
	case T_RangeTblFunction:
		return "T_RangeTblFunction"
	case T_RTEPermissionInfo:
		return "T_RTEPermissionInfo"
	case T_RangeTblRef:
		return "T_RangeTblRef"
	case T_JsonFormat:
		return "T_JsonFormat"
	case T_JsonReturning:
		return "T_JsonReturning"
	case T_JsonValueExpr:
		return "T_JsonValueExpr"
	case T_JsonBehavior:
		return "T_JsonBehavior"
	case T_JsonOutput:
		return "T_JsonOutput"
	case T_JsonArgument:
		return "T_JsonArgument"
	case T_JsonFuncExpr:
		return "T_JsonFuncExpr"
	case T_JsonTablePathSpec:
		return "T_JsonTablePathSpec"
	case T_JsonTable:
		return "T_JsonTable"
	case T_JsonTableColumn:
		return "T_JsonTableColumn"
	case T_JsonKeyValue:
		return "T_JsonKeyValue"
	case T_JsonParseExpr:
		return "T_JsonParseExpr"
	case T_JsonScalarExpr:
		return "T_JsonScalarExpr"
	case T_JsonSerializeExpr:
		return "T_JsonSerializeExpr"
	case T_JsonObjectConstructor:
		return "T_JsonObjectConstructor"
	case T_JsonArrayConstructor:
		return "T_JsonArrayConstructor"
	case T_JsonArrayQueryConstructor:
		return "T_JsonArrayQueryConstructor"
	case T_JsonAggConstructor:
		return "T_JsonAggConstructor"
	case T_JsonObjectAgg:
		return "T_JsonObjectAgg"
	case T_JsonArrayAgg:
		return "T_JsonArrayAgg"
	case T_JsonConstructorExpr:
		return "T_JsonConstructorExpr"
	case T_JsonIsPredicate:
		return "T_JsonIsPredicate"
	case T_JsonExpr:
		return "T_JsonExpr"
	case T_JsonTablePath:
		return "T_JsonTablePath"
	case T_JsonTablePlan:
		return "T_JsonTablePlan"
	case T_JsonTablePathScan:
		return "T_JsonTablePathScan"
	case T_JsonTableSiblingJoin:
		return "T_JsonTableSiblingJoin"
	case T_GroupingFunc:
		return "T_GroupingFunc"
	case T_WindowFuncRunCondition:
		return "T_WindowFuncRunCondition"
	case T_MergeSupportFunc:
		return "T_MergeSupportFunc"
	case T_NamedArgExpr:
		return "T_NamedArgExpr"
	case T_CaseTestExpr:
		return "T_CaseTestExpr"
	case T_MinMaxExpr:
		return "T_MinMaxExpr"
	case T_RowCompareExpr:
		return "T_RowCompareExpr"
	case T_SQLValueFunction:
		return "T_SQLValueFunction"
	case T_XmlExpr:
		return "T_XmlExpr"
	case T_MergeAction:
		return "T_MergeAction"
	case T_TableFunc:
		return "T_TableFunc"
	case T_IntoClause:
		return "T_IntoClause"
	default:
		return fmt.Sprintf("NodeTag(%d)", int(nt))
	}
}

// Node is the base interface for all PostgreSQL AST nodes.
// Every node in the parse tree implements this interface.
// Ported from postgres/src/include/nodes/nodes.h:128 (base node concept)
type Node interface {
	// NodeTag returns the type tag for this node
	NodeTag() NodeTag

	// Location returns the byte offset in the source string where this node begins.
	// Returns -1 if location is not available.
	// Ported from postgres/src/include/nodes/parsenodes.h:6-12 location concept
	Location() int

	// String returns a string representation of the node (for debugging)
	String() string

	// SqlString returns the SQL representation of this node for deparsing
	// This enables round-trip parsing: SQL -> AST -> SQL
	SqlString() string
}

// BaseNode provides a basic implementation of the Node interface.
// Other node types should embed this to get default implementations.
// Ported from postgres base node structure concept
type BaseNode struct {
	Tag NodeTag // Node type tag - ported from postgres/src/include/nodes/nodes.h:130
	Loc int     // Source location in bytes - ported from postgres/src/include/nodes/parsenodes.h:6-12
}

// NodeTag returns the node's type tag.
func (n *BaseNode) NodeTag() NodeTag {
	return n.Tag
}

// Location returns the node's source location.
func (n *BaseNode) Location() int {
	return n.Loc
}

// String returns a basic string representation.
func (n *BaseNode) String() string {
	return fmt.Sprintf("%s@%d", n.Tag, n.Loc)
}

// SqlString provides a default implementation that panics with helpful message.
// Specific node types should override this method to provide actual SQL deparsing.
func (n *BaseNode) SqlString() string {
	panic(fmt.Sprintf("SqlString() not implemented for node type %s (tag: %d). "+
		"Please implement SqlString() method for this node type to enable SQL deparsing.",
		n.Tag, int(n.Tag)))
}

// SetLocation sets the source location for this node.
// Used during parsing to track where nodes came from in the source.
func (n *BaseNode) SetLocation(location int) {
	n.Loc = location
}

// NodeList represents a list of nodes.
// This is a fundamental PostgreSQL concept used throughout the AST.
// Ported from postgres List structure concept
type NodeList struct {
	BaseNode
	Items []Node // List of nodes
}

// NewNodeList creates a new node list.
func NewNodeList(items ...Node) *NodeList {
	return &NodeList{
		BaseNode: BaseNode{Tag: T_List},
		Items:    items,
	}
}

// Append adds a node to the list.
func (l *NodeList) Append(node Node) {
	l.Items = append(l.Items, node)
}

// Len returns the number of items in the list.
func (l *NodeList) Len() int {
	return len(l.Items)
}

// String returns a string representation of the list.
func (l *NodeList) String() string {
	return fmt.Sprintf("List[%d items]@%d", len(l.Items), l.Location())
}

// SqlString returns the SQL representation of NodeList
func (n *NodeList) SqlString() string {
	if len(n.Items) == 0 {
		return ""
	}

	var parts []string
	for _, item := range n.Items {
		if item != nil {
			parts = append(parts, item.SqlString())
		}
	}

	return strings.Join(parts, ", ")
}

// Stmt represents the base interface for all SQL statements.
// All top-level SQL constructs implement this interface.
// Ported from postgres statement node concept
type Stmt interface {
	Node
	StatementType() string
}

// Expression represents the base interface for all SQL expressions.
// All expressions in WHERE clauses, SELECT lists, etc. implement this.
// Ported from postgres expression node concept
type Expression interface {
	Node
	ExpressionType() string
}

// Identifier represents a simple identifier (table name, column name, etc.).
// Ported from basic identifier concept used throughout postgres AST
type Identifier struct {
	BaseNode
	Name string // The identifier name
}

// NewIdentifier creates a new identifier node.
func NewIdentifier(name string) *Identifier {
	return &Identifier{
		BaseNode: BaseNode{Tag: T_String}, // Use T_String for simple identifiers
		Name:     name,
	}
}

// String returns the identifier name.
func (i *Identifier) String() string {
	return fmt.Sprintf("Identifier(%s)@%d", i.Name, i.Location())
}

// ExpressionType returns the expression type for Expression interface.
func (i *Identifier) ExpressionType() string {
	return "Identifier"
}

// ==============================================================================
// VALUE NODE SYSTEM - Complete PostgreSQL value.h implementation
// Ported from postgres/src/include/nodes/value.h
// ==============================================================================

// Integer represents an integer literal value node.
// Ported from postgres/src/include/nodes/value.h:28-34
type Integer struct {
	BaseNode
	IVal int // Integer value - postgres/src/include/nodes/value.h:33
}

// NewInteger creates a new integer literal node.
// Ported from postgres/src/include/nodes/value.h:84 (makeInteger)
func NewInteger(value int) *Integer {
	return &Integer{
		BaseNode: BaseNode{Tag: T_Integer},
		IVal:     value,
	}
}

func (i *Integer) String() string {
	return fmt.Sprintf("Integer(%d)@%d", i.IVal, i.Location())
}

// SqlString returns the SQL representation of the Integer
func (i *Integer) SqlString() string {
	return fmt.Sprintf("%d", i.IVal)
}

func (i *Integer) ExpressionType() string {
	return "Integer"
}

// IntVal extracts integer value - ported from postgres/src/include/nodes/value.h:79
func IntVal(node Node) int {
	if i, ok := node.(*Integer); ok {
		return i.IVal
	}
	return 0
}

// Float represents a floating-point literal value node.
// Stored as string to preserve precision, like PostgreSQL.
// Ported from postgres/src/include/nodes/value.h:47-53
type Float struct {
	BaseNode
	FVal string // Float value as string - postgres/src/include/nodes/value.h:52
}

// NewFloat creates a new float literal node.
// Ported from postgres/src/include/nodes/value.h:85 (makeFloat)
func NewFloat(value string) *Float {
	return &Float{
		BaseNode: BaseNode{Tag: T_Float},
		FVal:     value,
	}
}

func (f *Float) String() string {
	return fmt.Sprintf("Float(%s)@%d", f.FVal, f.Location())
}

// SqlString returns the SQL representation of the Float
func (f *Float) SqlString() string {
	return f.FVal
}

func (f *Float) ExpressionType() string {
	return "Float"
}

// FloatVal extracts float value - ported from postgres/src/include/nodes/value.h:80
func FloatVal(node Node) float64 {
	if f, ok := node.(*Float); ok {
		var result float64
		if _, err := fmt.Sscanf(f.FVal, "%f", &result); err == nil {
			return result
		}
	}
	return 0.0
}

// Boolean represents a boolean literal value node.
// Ported from postgres/src/include/nodes/value.h:55-61
type Boolean struct {
	BaseNode
	BoolVal bool // Boolean value - postgres/src/include/nodes/value.h:60
}

// NewBoolean creates a new boolean literal node.
// Ported from postgres/src/include/nodes/value.h:86 (makeBoolean)
func NewBoolean(value bool) *Boolean {
	return &Boolean{
		BaseNode: BaseNode{Tag: T_Boolean},
		BoolVal:  value,
	}
}

func (b *Boolean) String() string {
	return fmt.Sprintf("Boolean(%t)@%d", b.BoolVal, b.Location())
}

// SqlString returns the SQL representation of the Boolean
func (b *Boolean) SqlString() string {
	if b.BoolVal {
		return "TRUE"
	}
	return "FALSE"
}

func (b *Boolean) ExpressionType() string {
	return "Boolean"
}

// BoolVal extracts boolean value - ported from postgres/src/include/nodes/value.h:81
func BoolVal(node Node) bool {
	if b, ok := node.(*Boolean); ok {
		return b.BoolVal
	}
	return false
}

// String represents a string literal value node.
// Ported from postgres/src/include/nodes/value.h:63-69
type String struct {
	BaseNode
	SVal string // String value - postgres/src/include/nodes/value.h:68
}

// NewString creates a new string literal node.
// Ported from postgres/src/include/nodes/value.h:87 (makeString)
func NewString(value string) *String {
	return &String{
		BaseNode: BaseNode{Tag: T_String},
		SVal:     value,
	}
}

func (s *String) String() string {
	return fmt.Sprintf("String(%q)@%d", s.SVal, s.Location())
}

// SqlString returns the SQL representation of the String (properly quoted)
func (s *String) SqlString() string {
	return QuoteStringLiteral(s.SVal)
}

func (s *String) ExpressionType() string {
	return "String"
}

// StrVal extracts string value - ported from postgres/src/include/nodes/value.h:82
func StrVal(node Node) string {
	if s, ok := node.(*String); ok {
		return s.SVal
	}
	return ""
}

// BitString represents a bit string literal value node.
// Ported from postgres/src/include/nodes/value.h:71-77
type BitString struct {
	BaseNode
	BSVal string // Bit string value - postgres/src/include/nodes/value.h:76
}

// NewBitString creates a new bit string literal node.
// Ported from postgres/src/include/nodes/value.h:88 (makeBitString)
func NewBitString(value string) *BitString {
	return &BitString{
		BaseNode: BaseNode{Tag: T_BitString},
		BSVal:    value,
	}
}

func (bs *BitString) String() string {
	return fmt.Sprintf("BitString(%q)@%d", bs.BSVal, bs.Location())
}

func (bs *BitString) ExpressionType() string {
	return "BitString"
}

// Null represents a NULL literal value node.
type Null struct {
	BaseNode
}

// NewNull creates a new NULL literal node.
func NewNull() *Null {
	return &Null{
		BaseNode: BaseNode{Tag: T_Null},
	}
}

func (n *Null) String() string {
	return fmt.Sprintf("NULL@%d", n.Location())
}

// SqlString returns the SQL representation of the Null
func (n *Null) SqlString() string {
	return "NULL"
}

func (n *Null) ExpressionType() string {
	return "Null"
}

// Value is a generic interface for all value types.
// This provides a common interface for all literal values.
type Value interface {
	Expression
	IsValue() bool
}

// Implement Value interface for all value types
func (i *Integer) IsValue() bool    { return true }
func (f *Float) IsValue() bool      { return true }
func (b *Boolean) IsValue() bool    { return true }
func (s *String) IsValue() bool     { return true }
func (bs *BitString) IsValue() bool { return true }
func (n *Null) IsValue() bool       { return true }

// NewValue creates a properly typed value node based on the Go type.
// This is a convenience function that delegates to the specific typed constructors.
func NewValue(val interface{}) Node {
	switch v := val.(type) {
	case string:
		return NewString(v)
	case int:
		return NewInteger(v)
	case int32:
		return NewInteger(int(v))
	case int64:
		return NewInteger(int(v))
	case float32:
		return NewFloat(fmt.Sprintf("%g", v))
	case float64:
		return NewFloat(fmt.Sprintf("%g", v))
	case bool:
		return NewBoolean(v)
	case nil:
		return NewNull()
	default:
		// For unknown types, create a string representation
		return NewString(fmt.Sprintf("%v", v))
	}
}

// Helper functions for node creation and manipulation

// IsNode checks if a value implements the Node interface.
func IsNode(v interface{}) bool {
	_, ok := v.(Node)
	return ok
}

// NodeTagOf returns the NodeTag of a node, or T_Invalid if not a node.
func NodeTagOf(v interface{}) NodeTag {
	if node, ok := v.(Node); ok {
		return node.NodeTag()
	}
	return T_Invalid
}

// LocationOf returns the location of a node, or -1 if not a node or no location.
func LocationOf(v interface{}) int {
	if node, ok := v.(Node); ok {
		return node.Location()
	}
	return -1
}

// CastNode safely casts a value to a Node, returning nil if not a node.
func CastNode(v interface{}) Node {
	if node, ok := v.(Node); ok {
		return node
	}
	return nil
}

// NewQualifiedName creates a qualified name node (schema.name)
func NewQualifiedName(schema, name string) Node {
	return &RangeVar{
		BaseNode:   BaseNode{Tag: T_RangeVar},
		SchemaName: schema,
		RelName:    name,
	}
}

// NodeWalker is a function type for walking the AST.
// It receives a node and returns whether to continue walking.
type NodeWalker func(Node) bool

// WalkNodes recursively walks all nodes in an AST, calling the walker function.
// This is useful for analysis, transformation, and debugging.
func WalkNodes(node Node, walker NodeWalker) {
	if node == nil || !walker(node) {
		return
	}

	// Handle specific node types that contain other nodes
	switch n := node.(type) {
	case *NodeList:
		for _, item := range n.Items {
			if item != nil {
				WalkNodes(item, walker)
			}
		}
	// Value nodes are leaf nodes - no traversal needed
	case *Integer, *Float, *Boolean, *String, *BitString, *Null:
		// Leaf nodes - no children to traverse
		return
	// Additional node types will be handled as they're implemented
	default:
		// For now, we don't traverse into other node types
		// This will be expanded as we implement more complex AST structures
	}
}

// FindNodes finds all nodes of a specific type in an AST.
func FindNodes(root Node, targetTag NodeTag) []Node {
	var found []Node
	WalkNodes(root, func(node Node) bool {
		if node.NodeTag() == targetTag {
			found = append(found, node)
		}
		return true
	})
	return found
}

// PrintAST prints a simple representation of an AST for debugging.
func PrintAST(node Node, indent int) {
	if node == nil {
		return
	}

	prefix := ""
	for i := 0; i < indent; i++ {
		prefix += "  "
	}

	fmt.Printf("%s%s\n", prefix, node.String())

	// Print children for specific node types
	switch n := node.(type) {
	case *NodeList:
		for _, item := range n.Items {
			PrintAST(item, indent+1)
		}
	}
}
