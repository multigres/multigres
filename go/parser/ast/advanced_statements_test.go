// Package ast provides PostgreSQL AST advanced statement node tests.
package ast

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==============================================================================
// MERGE Statement Tests
// ==============================================================================

func TestMergeMatchKind(t *testing.T) {
	tests := []struct {
		kind     MergeMatchKind
		expected string
	}{
		{MERGE_WHEN_MATCHED, "WHEN MATCHED"},
		{MERGE_WHEN_NOT_MATCHED_BY_SOURCE, "WHEN NOT MATCHED BY SOURCE"},
		{MERGE_WHEN_NOT_MATCHED_BY_TARGET, "WHEN NOT MATCHED BY TARGET"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.kind.String())
		})
	}
}

func TestOverridingKind(t *testing.T) {
	tests := []struct {
		kind     OverridingKind
		expected string
	}{
		{OVERRIDING_NOT_SET, ""},
		{OVERRIDING_USER_VALUE, "OVERRIDING USER VALUE"},
		{OVERRIDING_SYSTEM_VALUE, "OVERRIDING SYSTEM VALUE"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.kind.String())
		})
	}
}

func TestMergeStmt(t *testing.T) {
	t.Run("NewMergeStmt", func(t *testing.T) {
		relation := &RangeVar{RelName: "target_table"}
		sourceRelation := &RangeVar{RelName: "source_table"}
		joinCondition := NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{&String{SVal: "="}}}, nil, nil, 0)

		stmt := NewMergeStmt(relation, sourceRelation, joinCondition)

		require.NotNil(t, stmt)
		assert.Equal(t, T_MergeStmt, stmt.Tag)
		assert.Equal(t, relation, stmt.Relation)
		assert.Equal(t, sourceRelation, stmt.SourceRelation)
		assert.Equal(t, joinCondition, stmt.JoinCondition)
		assert.Contains(t, stmt.String(), "MERGE INTO")
		assert.Contains(t, stmt.String(), "USING")
		assert.Contains(t, stmt.String(), "ON")
	})

	t.Run("MergeStmtWithWhenClauses", func(t *testing.T) {
		relation := &RangeVar{RelName: "target_table"}
		sourceRelation := &RangeVar{RelName: "source_table"}
		joinCondition := NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{&String{SVal: "="}}}, nil, nil, 0)

		stmt := NewMergeStmt(relation, sourceRelation, joinCondition)
		whenClause := NewMergeWhenClause(MERGE_WHEN_MATCHED, CMD_UPDATE)
		stmt.MergeWhenClauses = NewNodeList(whenClause)

		assert.NotNil(t, stmt.MergeWhenClauses)
		assert.Equal(t, 1, stmt.MergeWhenClauses.Len())
		assert.Contains(t, stmt.String(), "WHEN MATCHED")
	})

	t.Run("MergeStmtWithReturning", func(t *testing.T) {
		relation := &RangeVar{RelName: "target_table"}
		sourceRelation := &RangeVar{RelName: "source_table"}
		joinCondition := NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{&String{SVal: "="}}}, nil, nil, 0)

		stmt := NewMergeStmt(relation, sourceRelation, joinCondition)
		stmt.ReturningList = NewNodeList(&ColumnRef{Fields: NewNodeList(&String{SVal: "id"})})

		assert.NotEmpty(t, stmt.ReturningList)
		assert.Contains(t, stmt.String(), "RETURNING")
	})
}

func TestMergeWhenClause(t *testing.T) {
	t.Run("NewMergeWhenClause", func(t *testing.T) {
		clause := NewMergeWhenClause(MERGE_WHEN_MATCHED, CMD_UPDATE)

		require.NotNil(t, clause)
		assert.Equal(t, T_MergeWhenClause, clause.Tag)
		assert.Equal(t, MERGE_WHEN_MATCHED, clause.MatchKind)
		assert.Equal(t, CMD_UPDATE, clause.CommandType)
		assert.Contains(t, clause.String(), "WHEN MATCHED")
		assert.Contains(t, clause.String(), "UPDATE SET")
	})

	t.Run("MergeWhenClauseInsert", func(t *testing.T) {
		clause := NewMergeWhenClause(MERGE_WHEN_NOT_MATCHED_BY_TARGET, CMD_INSERT)
		clause.Override = OVERRIDING_USER_VALUE
		clause.Values = NewNodeList(&A_Const{Val: &Integer{IVal: 1}})

		assert.Equal(t, CMD_INSERT, clause.CommandType)
		assert.Equal(t, OVERRIDING_USER_VALUE, clause.Override)
		assert.Contains(t, clause.String(), "INSERT")
		assert.Contains(t, clause.String(), "OVERRIDING USER VALUE")
		assert.Contains(t, clause.String(), "VALUES")
	})

	t.Run("MergeWhenClauseDelete", func(t *testing.T) {
		clause := NewMergeWhenClause(MERGE_WHEN_MATCHED, CMD_DELETE)

		assert.Equal(t, CMD_DELETE, clause.CommandType)
		assert.Contains(t, clause.String(), "DELETE")
	})

	t.Run("MergeWhenClauseDoNothing", func(t *testing.T) {
		clause := NewMergeWhenClause(MERGE_WHEN_MATCHED, CMD_NOTHING)

		assert.Equal(t, CMD_NOTHING, clause.CommandType)
		assert.Contains(t, clause.String(), "DO NOTHING")
	})
}

// ==============================================================================
// SET Operations Tests
// ==============================================================================

func TestSetOperation(t *testing.T) {
	tests := []struct {
		op       SetOperation
		expected string
	}{
		{SETOP_NONE, ""},
		{SETOP_UNION, "UNION"},
		{SETOP_INTERSECT, "INTERSECT"},
		{SETOP_EXCEPT, "EXCEPT"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.op.String())
		})
	}
}

func TestSetOperationStmt(t *testing.T) {
	t.Run("NewSetOperationStmt", func(t *testing.T) {
		larg := &SelectStmt{
			TargetList: NewNodeList(NewResTarget("", &ColumnRef{Fields: NewNodeList(&String{SVal: "id"})})),
		}
		rarg := &SelectStmt{
			TargetList: NewNodeList(NewResTarget("", &ColumnRef{Fields: NewNodeList(&String{SVal: "name"})})),
		}

		stmt := NewSetOperationStmt(SETOP_UNION, false, larg, rarg)

		require.NotNil(t, stmt)
		assert.Equal(t, T_SetOperationStmt, stmt.Tag)
		assert.Equal(t, SETOP_UNION, stmt.Op)
		assert.False(t, stmt.All)
		assert.Equal(t, larg, stmt.Larg)
		assert.Equal(t, rarg, stmt.Rarg)
		assert.Contains(t, stmt.String(), "UNION")
	})

	t.Run("SetOperationStmtWithAll", func(t *testing.T) {
		larg := &SelectStmt{}
		rarg := &SelectStmt{}

		stmt := NewSetOperationStmt(SETOP_UNION, true, larg, rarg)

		assert.True(t, stmt.All)
		assert.Contains(t, stmt.String(), "UNION ALL")
	})

	t.Run("SetOperationStmtIntersect", func(t *testing.T) {
		larg := &SelectStmt{}
		rarg := &SelectStmt{}

		stmt := NewSetOperationStmt(SETOP_INTERSECT, false, larg, rarg)

		assert.Equal(t, SETOP_INTERSECT, stmt.Op)
		assert.Contains(t, stmt.String(), "INTERSECT")
	})

	t.Run("SetOperationStmtExcept", func(t *testing.T) {
		larg := &SelectStmt{}
		rarg := &SelectStmt{}

		stmt := NewSetOperationStmt(SETOP_EXCEPT, false, larg, rarg)

		assert.Equal(t, SETOP_EXCEPT, stmt.Op)
		assert.Contains(t, stmt.String(), "EXCEPT")
	})
}

// ==============================================================================
// PL/pgSQL Statement Tests
// ==============================================================================

func TestReturnStmt(t *testing.T) {
	t.Run("NewReturnStmt", func(t *testing.T) {
		returnVal := &A_Const{Val: &Integer{IVal: 42}}
		stmt := NewReturnStmt(returnVal)

		require.NotNil(t, stmt)
		assert.Equal(t, T_ReturnStmt, stmt.Tag)
		assert.Equal(t, returnVal, stmt.ReturnVal)
		assert.Contains(t, stmt.String(), "RETURN")
	})

	t.Run("ReturnStmtWithoutValue", func(t *testing.T) {
		stmt := NewReturnStmt(nil)

		assert.Nil(t, stmt.ReturnVal)
		assert.Equal(t, "RETURN", stmt.String())
	})

	t.Run("ReturnStmtWithExpression", func(t *testing.T) {
		returnVal := &FuncCall{Funcname: &NodeList{Items: []Node{&String{SVal: "now"}}}}
		stmt := NewReturnStmt(returnVal)

		assert.Equal(t, returnVal, stmt.ReturnVal)
		assert.Contains(t, stmt.String(), "RETURN")
	})
}

func TestPLAssignStmt(t *testing.T) {
	t.Run("NewPLAssignStmt", func(t *testing.T) {
		val := &SelectStmt{
			TargetList: NewNodeList(NewResTarget("", &A_Const{Val: &Integer{IVal: 42}})),
		}
		stmt := NewPLAssignStmt("myvar", val)

		require.NotNil(t, stmt)
		assert.Equal(t, T_PLAssignStmt, stmt.Tag)
		assert.Equal(t, "myvar", stmt.Name)
		assert.Equal(t, val, stmt.Val)
		assert.Equal(t, -1, int(stmt.Location))
		assert.Contains(t, stmt.String(), "myvar")
		assert.Contains(t, stmt.String(), ":=")
	})

	t.Run("PLAssignStmtWithIndirection", func(t *testing.T) {
		val := &SelectStmt{}
		stmt := NewPLAssignStmt("myarray", val)
		stmt.Indirection = NewNodeList(&A_Indices{})

		assert.NotEmpty(t, stmt.Indirection)
		assert.Contains(t, stmt.String(), "[")
	})

	t.Run("PLAssignStmtWithNnames", func(t *testing.T) {
		val := &SelectStmt{}
		stmt := NewPLAssignStmt("qualified.name", val)
		stmt.Nnames = 2

		assert.Equal(t, 2, stmt.Nnames)
	})
}

// ==============================================================================
// INSERT ON CONFLICT Tests
// ==============================================================================

func TestOnConflictAction(t *testing.T) {
	tests := []struct {
		action   OnConflictAction
		expected string
	}{
		{ONCONFLICT_NONE, ""},
		{ONCONFLICT_NOTHING, "DO NOTHING"},
		{ONCONFLICT_UPDATE, "DO UPDATE"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.action.String())
		})
	}
}

func TestOnConflictClause(t *testing.T) {
	t.Run("NewOnConflictClause", func(t *testing.T) {
		clause := NewOnConflictClause(ONCONFLICT_NOTHING)

		require.NotNil(t, clause)
		assert.Equal(t, T_OnConflictClause, clause.Tag)
		assert.Equal(t, ONCONFLICT_NOTHING, clause.Action)
		assert.Equal(t, 0, clause.Location())
		assert.Contains(t, clause.String(), "ON CONFLICT")
		assert.Contains(t, clause.String(), "DO NOTHING")
	})

	t.Run("OnConflictClauseWithInfer", func(t *testing.T) {
		clause := NewOnConflictClause(ONCONFLICT_NOTHING)
		clause.Infer = NewInferClause()

		assert.NotNil(t, clause.Infer)
		str := clause.String()
		assert.Contains(t, str, "ON CONFLICT")
	})

	t.Run("OnConflictClauseUpdate", func(t *testing.T) {
		clause := NewOnConflictClause(ONCONFLICT_UPDATE)
		clause.TargetList = NewNodeList(&ResTarget{
			Name: "column1",
			Val:  &A_Const{Val: &String{SVal: "value1"}},
		})

		assert.Equal(t, ONCONFLICT_UPDATE, clause.Action)
		assert.NotNil(t, clause.TargetList)
		assert.Equal(t, 1, clause.TargetList.Len())
		assert.Contains(t, clause.String(), "DO UPDATE")
		assert.Contains(t, clause.String(), "SET")
	})

	t.Run("OnConflictClauseWithWhere", func(t *testing.T) {
		clause := NewOnConflictClause(ONCONFLICT_UPDATE)
		clause.WhereClause = NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{&String{SVal: ">"}}}, nil, nil, 0)

		assert.NotNil(t, clause.WhereClause)
		assert.Contains(t, clause.String(), "WHERE")
	})
}

func TestInferClause(t *testing.T) {
	t.Run("NewInferClause", func(t *testing.T) {
		clause := NewInferClause()

		require.NotNil(t, clause)
		assert.Equal(t, T_InferClause, clause.Tag)
		assert.Equal(t, 0, clause.Location())
	})

	t.Run("InferClauseWithIndexElems", func(t *testing.T) {
		clause := NewInferClause()
		clause.IndexElems = NewNodeList(&IndexElem{
			Name: "column1",
		})

		assert.NotNil(t, clause.IndexElems)
		assert.Equal(t, 1, clause.IndexElems.Len())
		assert.Contains(t, clause.String(), "(")
		assert.Contains(t, clause.String(), ")")
	})

	t.Run("InferClauseWithWhere", func(t *testing.T) {
		clause := NewInferClause()
		clause.WhereClause = NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{&String{SVal: "IS NOT NULL"}}}, nil, nil, 0)

		assert.NotNil(t, clause.WhereClause)
		assert.Contains(t, clause.String(), "WHERE")
	})

	t.Run("InferClauseWithConstraintName", func(t *testing.T) {
		clause := NewInferClause()
		clause.Conname = "unique_constraint"

		assert.Equal(t, "unique_constraint", clause.Conname)
		assert.Contains(t, clause.String(), "ON CONSTRAINT")
		assert.Contains(t, clause.String(), "unique_constraint")
	})
}

// ==============================================================================
// WITH CHECK OPTION Tests
// ==============================================================================

func TestWCOKind(t *testing.T) {
	tests := []struct {
		kind     WCOKind
		expected string
	}{
		{WCO_VIEW_CHECK, "VIEW CHECK"},
		{WCO_RLS_INSERT_CHECK, "RLS INSERT CHECK"},
		{WCO_RLS_UPDATE_CHECK, "RLS UPDATE CHECK"},
		{WCO_RLS_CONFLICT_CHECK, "RLS CONFLICT CHECK"},
		{WCO_RLS_MERGE_UPDATE_CHECK, "RLS MERGE UPDATE CHECK"},
		{WCO_RLS_MERGE_DELETE_CHECK, "RLS MERGE DELETE CHECK"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.kind.String())
		})
	}
}

func TestWithCheckOption(t *testing.T) {
	t.Run("NewWithCheckOption", func(t *testing.T) {
		wco := NewWithCheckOption(WCO_VIEW_CHECK, true)

		require.NotNil(t, wco)
		assert.Equal(t, T_WithCheckOption, wco.NodeTag())
		assert.Equal(t, WCO_VIEW_CHECK, wco.Kind)
		assert.True(t, wco.Cascaded)
		assert.Contains(t, wco.String(), "WITH")
		assert.Contains(t, wco.String(), "CASCADED")
		assert.Contains(t, wco.String(), "CHECK OPTION")
	})

	t.Run("WithCheckOptionLocal", func(t *testing.T) {
		wco := NewWithCheckOption(WCO_VIEW_CHECK, false)

		assert.False(t, wco.Cascaded)
		assert.Contains(t, wco.String(), "LOCAL")
		assert.NotContains(t, wco.String(), "CASCADED")
	})

	t.Run("WithCheckOptionRLS", func(t *testing.T) {
		wco := NewWithCheckOption(WCO_RLS_INSERT_CHECK, false)
		wco.Relname = "test_table"
		wco.Polname = "test_policy"

		assert.Equal(t, WCO_RLS_INSERT_CHECK, wco.Kind)
		assert.Equal(t, "test_table", wco.Relname)
		assert.Equal(t, "test_policy", wco.Polname)
	})
}

// ==============================================================================
// Additional Statement Tests
// ==============================================================================

func TestTruncateStmt(t *testing.T) {
	t.Run("NewTruncateStmt", func(t *testing.T) {
		relations := []*RangeVar{{RelName: "test_table"}}
		stmt := NewTruncateStmt(relations)

		require.NotNil(t, stmt)
		assert.Equal(t, T_TruncateStmt, stmt.Tag)
		assert.Equal(t, relations, stmt.Relations)
		assert.Equal(t, DropRestrict, stmt.Behavior)
		assert.Contains(t, stmt.String(), "TRUNCATE TABLE")
		assert.Contains(t, stmt.String(), "test_table")
	})

	t.Run("TruncateStmtRestartSeqs", func(t *testing.T) {
		relations := []*RangeVar{{RelName: "test_table"}}
		stmt := NewTruncateStmt(relations)
		stmt.RestartSeqs = true

		assert.True(t, stmt.RestartSeqs)
		assert.Contains(t, stmt.String(), "RESTART IDENTITY")
	})

	t.Run("TruncateStmtContinueIdentity", func(t *testing.T) {
		relations := []*RangeVar{{RelName: "test_table"}}
		stmt := NewTruncateStmt(relations)
		stmt.RestartSeqs = false

		assert.False(t, stmt.RestartSeqs)
		assert.Contains(t, stmt.String(), "CONTINUE IDENTITY")
	})

	t.Run("TruncateStmtCascade", func(t *testing.T) {
		relations := []*RangeVar{{RelName: "test_table"}}
		stmt := NewTruncateStmt(relations)
		stmt.Behavior = DropCascade

		assert.Equal(t, DropCascade, stmt.Behavior)
		assert.Contains(t, stmt.String(), "CASCADE")
	})
}

func TestCommentStmt(t *testing.T) {
	t.Run("NewCommentStmt", func(t *testing.T) {
		object := &RangeVar{RelName: "test_table"}
		stmt := NewCommentStmt(OBJECT_TABLE, object, "Test comment")

		require.NotNil(t, stmt)
		assert.Equal(t, T_CommentStmt, stmt.Tag)
		assert.Equal(t, OBJECT_TABLE, stmt.Objtype)
		assert.Equal(t, object, stmt.Object)
		assert.Equal(t, "Test comment", stmt.Comment)
		assert.Contains(t, stmt.String(), "COMMENT ON")
		assert.Contains(t, stmt.String(), "IS")
		assert.Contains(t, stmt.String(), "'Test comment'")
	})

	t.Run("CommentStmtRemoveComment", func(t *testing.T) {
		object := &RangeVar{RelName: "test_table"}
		stmt := NewCommentStmt(OBJECT_TABLE, object, "")

		assert.Equal(t, "", stmt.Comment)
		assert.Contains(t, stmt.String(), "NULL")
	})
}

func TestRenameStmt(t *testing.T) {
	t.Run("NewRenameStmt", func(t *testing.T) {
		stmt := NewRenameStmt(OBJECT_TABLE, "new_name")

		require.NotNil(t, stmt)
		assert.Equal(t, T_RenameStmt, stmt.Tag)
		assert.Equal(t, OBJECT_TABLE, stmt.RenameType)
		assert.Equal(t, "new_name", stmt.Newname)
		assert.Equal(t, DropRestrict, stmt.Behavior)
		assert.Contains(t, stmt.String(), "ALTER")
		assert.Contains(t, stmt.String(), "RENAME TO")
		assert.Contains(t, stmt.String(), "new_name")
	})

	t.Run("RenameStmtWithRelation", func(t *testing.T) {
		stmt := NewRenameStmt(OBJECT_TABLE, "new_name")
		stmt.Relation = &RangeVar{RelName: "old_table"}

		assert.NotNil(t, stmt.Relation)
		assert.Contains(t, stmt.String(), "old_table")
	})

	t.Run("RenameStmtWithSubname", func(t *testing.T) {
		stmt := NewRenameStmt(OBJECT_COLUMN, "new_column")
		stmt.Subname = "old_column"
		stmt.Relation = &RangeVar{RelName: "test_table"}

		assert.Equal(t, "old_column", stmt.Subname)
		assert.Contains(t, stmt.String(), "RENAME old_column TO new_column")
	})

	t.Run("RenameStmtMissingOk", func(t *testing.T) {
		stmt := NewRenameStmt(OBJECT_TABLE, "new_name")
		stmt.MissingOk = true

		assert.True(t, stmt.MissingOk)
	})
}

func TestAlterOwnerStmt(t *testing.T) {
	t.Run("NewAlterOwnerStmt", func(t *testing.T) {
		newowner := &RoleSpec{Roletype: ROLESPEC_CSTRING, Rolename: "new_owner"}
		stmt := NewAlterOwnerStmt(OBJECT_TABLE, newowner)

		require.NotNil(t, stmt)
		assert.Equal(t, T_AlterOwnerStmt, stmt.Tag)
		assert.Equal(t, OBJECT_TABLE, stmt.ObjectType)
		assert.Equal(t, newowner, stmt.Newowner)
		assert.Contains(t, stmt.String(), "ALTER")
		assert.Contains(t, stmt.String(), "OWNER TO")
	})

	t.Run("AlterOwnerStmtWithRelation", func(t *testing.T) {
		newowner := &RoleSpec{Roletype: ROLESPEC_CSTRING, Rolename: "new_owner"}
		stmt := NewAlterOwnerStmt(OBJECT_TABLE, newowner)
		stmt.Relation = &RangeVar{RelName: "test_table"}

		assert.NotNil(t, stmt.Relation)
		assert.Contains(t, stmt.String(), "test_table")
	})

	t.Run("AlterOwnerStmtWithObject", func(t *testing.T) {
		newowner := &RoleSpec{Roletype: ROLESPEC_CSTRING, Rolename: "new_owner"}
		stmt := NewAlterOwnerStmt(OBJECT_FUNCTION, newowner)
		stmt.Object = &ObjectWithArgs{Objname: &NodeList{Items: []Node{&String{SVal: "my_function"}}}}

		assert.NotNil(t, stmt.Object)
		assert.Equal(t, OBJECT_FUNCTION, stmt.ObjectType)
	})
}

func TestRuleStmt(t *testing.T) {
	t.Run("NewRuleStmt", func(t *testing.T) {
		relation := &RangeVar{RelName: "test_table"}
		stmt := NewRuleStmt(relation, "test_rule", CMD_SELECT)

		require.NotNil(t, stmt)
		assert.Equal(t, T_RuleStmt, stmt.Tag)
		assert.Equal(t, relation, stmt.Relation)
		assert.Equal(t, "test_rule", stmt.Rulename)
		assert.Equal(t, CMD_SELECT, stmt.Event)
		assert.Contains(t, stmt.String(), "CREATE RULE")
		assert.Contains(t, stmt.String(), "test_rule")
		assert.Contains(t, stmt.String(), "ON SELECT")
		assert.Contains(t, stmt.String(), "test_table")
	})

	t.Run("RuleStmtReplace", func(t *testing.T) {
		relation := &RangeVar{RelName: "test_table"}
		stmt := NewRuleStmt(relation, "test_rule", CMD_INSERT)
		stmt.Replace = true

		assert.True(t, stmt.Replace)
		assert.Contains(t, stmt.String(), "CREATE OR REPLACE RULE")
	})

	t.Run("RuleStmtWithWhere", func(t *testing.T) {
		relation := &RangeVar{RelName: "test_table"}
		stmt := NewRuleStmt(relation, "test_rule", CMD_UPDATE)
		stmt.WhereClause = NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{&String{SVal: "="}}}, nil, nil, 0)

		assert.NotNil(t, stmt.WhereClause)
		assert.Contains(t, stmt.String(), "WHERE")
	})

	t.Run("RuleStmtInstead", func(t *testing.T) {
		relation := &RangeVar{RelName: "test_table"}
		stmt := NewRuleStmt(relation, "test_rule", CMD_DELETE)
		stmt.Instead = true

		assert.True(t, stmt.Instead)
		assert.Contains(t, stmt.String(), "DO INSTEAD")
	})

	t.Run("RuleStmtNoActions", func(t *testing.T) {
		relation := &RangeVar{RelName: "test_table"}
		stmt := NewRuleStmt(relation, "test_rule", CMD_DELETE)

		assert.Empty(t, stmt.Actions)
		assert.Contains(t, stmt.String(), "NOTHING")
	})

	t.Run("RuleStmtSingleAction", func(t *testing.T) {
		relation := &RangeVar{RelName: "test_table"}
		stmt := NewRuleStmt(relation, "test_rule", CMD_INSERT)
		stmt.Actions = NewNodeList(&SelectStmt{})

		assert.Equal(t, 1, stmt.Actions.Len())
		assert.NotContains(t, stmt.String(), "(")
	})

	t.Run("RuleStmtMultipleActions", func(t *testing.T) {
		relation := &RangeVar{RelName: "test_table"}
		stmt := NewRuleStmt(relation, "test_rule", CMD_UPDATE)
		stmt.Actions = NewNodeList(&SelectStmt{}, &UpdateStmt{})

		assert.Equal(t, 2, stmt.Actions.Len())
		assert.Contains(t, stmt.String(), "(")
		assert.Contains(t, stmt.String(), ";")
	})
}

func TestLockStmt(t *testing.T) {
	t.Run("NewLockStmt", func(t *testing.T) {
		relations := []*RangeVar{{RelName: "test_table"}}
		stmt := NewLockStmt(relations, 1)

		require.NotNil(t, stmt)
		assert.Equal(t, T_LockStmt, stmt.Tag)
		assert.Equal(t, relations, stmt.Relations)
		assert.Equal(t, 1, stmt.Mode)
		assert.Contains(t, stmt.String(), "LOCK TABLE")
		assert.Contains(t, stmt.String(), "test_table")
		assert.Contains(t, stmt.String(), "IN MODE")
	})

	t.Run("LockStmtMultipleTables", func(t *testing.T) {
		relations := []*RangeVar{
			{RelName: "table1"},
			{RelName: "table2"},
		}
		stmt := NewLockStmt(relations, 2)

		assert.Len(t, stmt.Relations, 2)
		assert.Contains(t, stmt.String(), "table1")
		assert.Contains(t, stmt.String(), "table2")
		assert.Contains(t, stmt.String(), ",")
	})

	t.Run("LockStmtNowait", func(t *testing.T) {
		relations := []*RangeVar{{RelName: "test_table"}}
		stmt := NewLockStmt(relations, 3)
		stmt.Nowait = true

		assert.True(t, stmt.Nowait)
		assert.Contains(t, stmt.String(), "NOWAIT")
	})

	t.Run("LockStmtDifferentModes", func(t *testing.T) {
		relations := []*RangeVar{{RelName: "test_table"}}
		
		for mode := 1; mode <= 8; mode++ {
			stmt := NewLockStmt(relations, mode)
			assert.Equal(t, mode, stmt.Mode)
			assert.Contains(t, stmt.String(), fmt.Sprintf("IN MODE %d", mode))
		}
	})
}

// ==============================================================================
// Integration Tests
// ==============================================================================

func TestAdvancedStatementsIntegration(t *testing.T) {
	t.Run("ComplexMergeStatement", func(t *testing.T) {
		// Build a complex MERGE statement with multiple WHEN clauses
		mergeStmt := NewMergeStmt(
			&RangeVar{RelName: "target"},
			&RangeVar{RelName: "source"},
			NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{&String{SVal: "="}}}, nil, nil, 0),
		)

		whenMatched := NewMergeWhenClause(MERGE_WHEN_MATCHED, CMD_UPDATE)
		whenMatched.TargetList = []*ResTarget{NewResTarget("updated_at", &FuncCall{Funcname: &NodeList{Items: []Node{&String{SVal: "now"}}}})}

		whenNotMatched := NewMergeWhenClause(MERGE_WHEN_NOT_MATCHED_BY_TARGET, CMD_INSERT)
		whenNotMatched.Values = NewNodeList(&A_Const{Val: &String{SVal: "new_value"}})

		mergeStmt.MergeWhenClauses = NewNodeList(whenMatched, whenNotMatched)
		mergeStmt.ReturningList = NewNodeList(&ColumnRef{Fields: NewNodeList(&String{SVal: "*"})})

		assert.Contains(t, mergeStmt.String(), "MERGE INTO target USING source")
		assert.Contains(t, mergeStmt.String(), "WHEN MATCHED")
		assert.Contains(t, mergeStmt.String(), "WHEN NOT MATCHED BY TARGET")
		assert.Contains(t, mergeStmt.String(), "RETURNING")
	})

	t.Run("ComplexSetOperation", func(t *testing.T) {
		// Build nested set operations
		leftSelect := &SelectStmt{
			TargetList: NewNodeList(NewResTarget("", &ColumnRef{Fields: NewNodeList(&String{SVal: "id"})})),
		}
		rightSelect := &SelectStmt{
			TargetList: NewNodeList(NewResTarget("", &ColumnRef{Fields: NewNodeList(&String{SVal: "name"})})),
		}

		union := NewSetOperationStmt(SETOP_UNION, true, leftSelect, rightSelect)
		
		// Nest this in another set operation
		finalSelect := &SelectStmt{
			TargetList: NewNodeList(NewResTarget("", &ColumnRef{Fields: NewNodeList(&String{SVal: "value"})})),
		}
		
		except := NewSetOperationStmt(SETOP_EXCEPT, false, union, finalSelect)

		assert.Contains(t, except.String(), "UNION ALL")
		assert.Contains(t, except.String(), "EXCEPT")
	})

	t.Run("OnConflictWithComplexInference", func(t *testing.T) {
		// Build complex ON CONFLICT clause
		infer := NewInferClause()
		infer.IndexElems = NewNodeList(
			&IndexElem{Name: "col1"},
			&IndexElem{Name: "col2"},
		)
		infer.WhereClause = NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{&String{SVal: "IS NOT NULL"}}}, nil, nil, 0)

		onConflict := NewOnConflictClause(ONCONFLICT_UPDATE)
		onConflict.Infer = infer
		onConflict.TargetList = NewNodeList(
			&ResTarget{Name: "updated_count", Val: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{&String{SVal: "+"}}}, nil, nil, 0)},
		)
		onConflict.WhereClause = NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{&String{SVal: ">"}}}, nil, nil, 0)

		str := onConflict.String()
		assert.Contains(t, str, "ON CONFLICT")
		assert.Contains(t, str, "(")
		assert.Contains(t, str, ")")
		assert.Contains(t, str, "WHERE")
		assert.Contains(t, str, "DO UPDATE")
		assert.Contains(t, str, "SET")
	})
}

// TestRenameStmtSqlString tests the SqlString method for RenameStmt
func TestRenameStmtSqlString(t *testing.T) {
	t.Run("RenameTable", func(t *testing.T) {
		stmt := &RenameStmt{
			BaseNode:     BaseNode{Tag: T_RenameStmt},
			RenameType:   OBJECT_TABLE,
			RelationType: OBJECT_TABLE,
			Relation:     NewRangeVar("old_table", "", ""),
			Newname:      "new_table",
			MissingOk:    false,
		}
		
		expected := "ALTER TABLE old_table RENAME TO new_table"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("RenameTableIfExists", func(t *testing.T) {
		stmt := &RenameStmt{
			BaseNode:     BaseNode{Tag: T_RenameStmt},
			RenameType:   OBJECT_TABLE,
			RelationType: OBJECT_TABLE,
			Relation:     NewRangeVar("old_table", "", ""),
			Newname:      "new_table",
			MissingOk:    true,
		}
		
		expected := "ALTER TABLE IF EXISTS old_table RENAME TO new_table"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("RenameColumn", func(t *testing.T) {
		stmt := &RenameStmt{
			BaseNode:     BaseNode{Tag: T_RenameStmt},
			RenameType:   OBJECT_COLUMN,
			RelationType: OBJECT_TABLE,
			Relation:     NewRangeVar("users", "", ""),
			Subname:      "old_column",
			Newname:      "new_column",
			MissingOk:    false,
		}
		
		expected := "ALTER TABLE users RENAME COLUMN old_column TO new_column"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("RenameColumnIfExists", func(t *testing.T) {
		stmt := &RenameStmt{
			BaseNode:     BaseNode{Tag: T_RenameStmt},
			RenameType:   OBJECT_COLUMN,
			RelationType: OBJECT_TABLE,
			Relation:     NewRangeVar("users", "", ""),
			Subname:      "old_column", 
			Newname:      "new_column",
			MissingOk:    true,
		}
		
		expected := "ALTER TABLE IF EXISTS users RENAME COLUMN old_column TO new_column"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("RenameConstraint", func(t *testing.T) {
		stmt := &RenameStmt{
			BaseNode:     BaseNode{Tag: T_RenameStmt},
			RenameType:   OBJECT_TABCONSTRAINT,
			RelationType: OBJECT_TABLE,
			Relation:     NewRangeVar("users", "", ""),
			Subname:      "old_constraint",
			Newname:      "new_constraint",
			MissingOk:    false,
		}
		
		expected := "ALTER TABLE users RENAME CONSTRAINT old_constraint TO new_constraint"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("RenameIndex", func(t *testing.T) {
		stmt := &RenameStmt{
			BaseNode:     BaseNode{Tag: T_RenameStmt},
			RenameType:   OBJECT_INDEX,
			RelationType: OBJECT_INDEX,
			Relation:     NewRangeVar("old_index", "", ""),
			Newname:      "new_index",
			MissingOk:    false,
		}
		
		expected := "ALTER INDEX old_index RENAME TO new_index"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("RenameView", func(t *testing.T) {
		stmt := &RenameStmt{
			BaseNode:     BaseNode{Tag: T_RenameStmt},
			RenameType:   OBJECT_VIEW,
			RelationType: OBJECT_VIEW,
			Relation:     NewRangeVar("old_view", "", ""),
			Newname:      "new_view",
			MissingOk:    false,
		}
		
		expected := "ALTER VIEW old_view RENAME TO new_view"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("RenameMaterializedView", func(t *testing.T) {
		stmt := &RenameStmt{
			BaseNode:     BaseNode{Tag: T_RenameStmt},
			RenameType:   OBJECT_MATVIEW,
			RelationType: OBJECT_MATVIEW,
			Relation:     NewRangeVar("old_matview", "", ""),
			Newname:      "new_matview",
			MissingOk:    false,
		}
		
		expected := "ALTER MATERIALIZED VIEW old_matview RENAME TO new_matview"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("RenameColumnInView", func(t *testing.T) {
		stmt := &RenameStmt{
			BaseNode:     BaseNode{Tag: T_RenameStmt},
			RenameType:   OBJECT_COLUMN,
			RelationType: OBJECT_VIEW,
			Relation:     NewRangeVar("user_view", "", ""),
			Subname:      "old_col",
			Newname:      "new_col", 
			MissingOk:    false,
		}
		
		expected := "ALTER VIEW user_view RENAME COLUMN old_col TO new_col"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("RenameColumnInForeignTable", func(t *testing.T) {
		stmt := &RenameStmt{
			BaseNode:     BaseNode{Tag: T_RenameStmt},
			RenameType:   OBJECT_COLUMN,
			RelationType: OBJECT_FOREIGN_TABLE,
			Relation:     NewRangeVar("foreign_users", "", ""),
			Subname:      "old_field",
			Newname:      "new_field",
			MissingOk:    false,
		}
		
		expected := "ALTER FOREIGN TABLE foreign_users RENAME COLUMN old_field TO new_field"
		assert.Equal(t, expected, stmt.SqlString())
	})
}
