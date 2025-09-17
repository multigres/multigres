// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRTEKind tests the RTEKind enum and its String method
func TestRTEKind(t *testing.T) {
	tests := []struct {
		kind     RTEKind
		expected string
	}{
		{RTE_RELATION, "RELATION"},
		{RTE_SUBQUERY, "SUBQUERY"},
		{RTE_JOIN, "JOIN"},
		{RTE_FUNCTION, "FUNCTION"},
		{RTE_TABLEFUNC, "TABLEFUNC"},
		{RTE_VALUES, "VALUES"},
		{RTE_CTE, "CTE"},
		{RTE_NAMEDTUPLESTORE, "NAMEDTUPLESTORE"},
		{RTE_RESULT, "RESULT"},
		{RTEKind(999), "UNKNOWN"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			assert.Equal(t, test.expected, test.kind.String())
		})
	}
}

// TestRangeTblEntry tests the RangeTblEntry node creation and functionality
func TestRangeTblEntry(t *testing.T) {
	t.Run("NewRangeTblEntry", func(t *testing.T) {
		alias := NewAlias("table_alias", NewNodeList(NewString("col1"), NewString("col2")))
		rte := NewRangeTblEntry(RTE_RELATION, alias)

		require.NotNil(t, rte)
		assert.Equal(t, T_RangeTblEntry, rte.Tag)
		assert.Equal(t, RTE_RELATION, rte.RteKind)
		assert.Equal(t, alias, rte.Alias)
		assert.Equal(t, "RANGE_TBL_ENTRY", rte.StatementType())
	})

	t.Run("RelationRTE", func(t *testing.T) {
		rte := NewRangeTblEntry(RTE_RELATION, nil)
		rte.Relid = 12345
		rte.Inh = true
		rte.RelKind = 'r'
		rte.RelLockMode = 1
		rte.PermInfoIndex = 1

		assert.Equal(t, Oid(12345), rte.Relid)
		assert.True(t, rte.Inh)
		assert.Equal(t, byte('r'), rte.RelKind)
		assert.Equal(t, 1, rte.RelLockMode)
		assert.Equal(t, 1, rte.PermInfoIndex)
	})

	t.Run("SubqueryRTE", func(t *testing.T) {
		rte := NewRangeTblEntry(RTE_SUBQUERY, nil)
		subquery := NewQuery(CMD_SELECT)
		rte.Subquery = subquery
		rte.SecurityBarrier = true

		assert.Equal(t, subquery, rte.Subquery)
		assert.True(t, rte.SecurityBarrier)
	})

	t.Run("FunctionRTE", func(t *testing.T) {
		rte := NewRangeTblEntry(RTE_FUNCTION, nil)
		funcNode := NewRangeTblFunction(NewFuncCall(&NodeList{Items: []Node{NewString("myfunction")}}, nil, 0), 3)
		rte.Functions = []*RangeTblFunction{funcNode}
		rte.FuncOrdinality = true

		assert.Len(t, rte.Functions, 1)
		assert.True(t, rte.FuncOrdinality)
	})

	t.Run("String", func(t *testing.T) {
		rte := NewRangeTblEntry(RTE_RELATION, nil)
		str := rte.String()

		assert.Contains(t, str, "RangeTblEntry")
		assert.Contains(t, str, "RELATION")
	})
}

// TestRangeSubselect tests the RangeSubselect node
func TestRangeSubselect(t *testing.T) {
	t.Run("NewRangeSubselect", func(t *testing.T) {
		subquery := NewSelectStmt()
		alias := NewAlias("sub", NewNodeList(NewString("a"), NewString("b")))
		rs := NewRangeSubselect(true, subquery, alias)

		require.NotNil(t, rs)
		assert.Equal(t, T_RangeSubselect, rs.Tag)
		assert.True(t, rs.Lateral)
		assert.Equal(t, subquery, rs.Subquery)
		assert.Equal(t, alias, rs.Alias)
		assert.Equal(t, "RANGE_SUBSELECT", rs.StatementType())
	})

	t.Run("String", func(t *testing.T) {
		rs := NewRangeSubselect(true, NewSelectStmt(), nil)
		str := rs.String()

		assert.Contains(t, str, "RangeSubselect")
		assert.Contains(t, str, "LATERAL")
	})

	t.Run("String_NoLateral", func(t *testing.T) {
		rs := NewRangeSubselect(false, NewSelectStmt(), nil)
		str := rs.String()

		assert.Contains(t, str, "RangeSubselect")
		assert.NotContains(t, str, "LATERAL")
	})
}

// TestRangeFunction tests the RangeFunction node
func TestRangeFunction(t *testing.T) {
	t.Run("NewRangeFunction", func(t *testing.T) {
		// Create a NodeList containing a NodeList with a function
		funcList := NewNodeList(NewFuncCall(&NodeList{Items: []Node{NewString("func1")}}, nil, 0))
		functions := NewNodeList(funcList)
		alias := NewAlias("f", nil)
		colDefs := NewNodeList(NewColumnDef("col1", NewTypeName([]string{"int4"}), 0))

		rf := NewRangeFunction(true, true, true, functions, alias, colDefs)

		require.NotNil(t, rf)
		assert.Equal(t, T_RangeFunction, rf.Tag)
		assert.True(t, rf.Lateral)
		assert.True(t, rf.Ordinality)
		assert.True(t, rf.IsRowsFrom)
		assert.Equal(t, functions, rf.Functions)
		assert.Equal(t, alias, rf.Alias)
		assert.Equal(t, colDefs, rf.ColDefList)
		assert.Equal(t, "RANGE_FUNCTION", rf.StatementType())
	})

	t.Run("String", func(t *testing.T) {
		rf := NewRangeFunction(true, true, true, nil, nil, nil)
		str := rf.String()

		assert.Contains(t, str, "LATERAL")
		assert.Contains(t, str, "RangeFunction")
		assert.Contains(t, str, "ROWS_FROM")
		assert.Contains(t, str, "WITH_ORDINALITY")
	})
}

// TestRangeTableFunc tests the RangeTableFunc node
func TestRangeTableFunc(t *testing.T) {
	t.Run("NewRangeTableFunc", func(t *testing.T) {
		docExpr := NewA_Const(NewString("document"), -1)
		rowExpr := NewA_Const(NewString("/row"), -1)
		namespaces := NewNodeList()
		namespaces.Append(NewResTarget("namespace", NewA_Const(NewString("ns"), -1)))
		columns := NewNodeList()
		columns.Append(NewRangeTableFuncCol("col1", NewTypeName([]string{"text"}), false, false, nil, nil, 100))
		alias := NewAlias("xmltab", nil)

		rtf := NewRangeTableFunc(true, docExpr, rowExpr, namespaces, columns, alias, 100)

		require.NotNil(t, rtf)
		assert.Equal(t, T_RangeTableFunc, rtf.Tag)
		assert.True(t, rtf.Lateral)
		assert.Equal(t, docExpr, rtf.DocExpr)
		assert.Equal(t, rowExpr, rtf.RowExpr)
		assert.Equal(t, namespaces, rtf.Namespaces)
		assert.Equal(t, columns, rtf.Columns)
		assert.Equal(t, alias, rtf.Alias)
		assert.Equal(t, 100, rtf.Location())
		assert.Equal(t, "RANGE_TABLE_FUNC", rtf.StatementType())
	})

	t.Run("String", func(t *testing.T) {
		columns := NewNodeList()
		columns.Append(NewRangeTableFuncCol("col1", nil, false, false, nil, nil, 0))
		columns.Append(NewRangeTableFuncCol("col2", nil, false, false, nil, nil, 0))
		rtf := NewRangeTableFunc(true, nil, nil, nil, columns, nil, 50)
		str := rtf.String()

		assert.Contains(t, str, "RangeTableFunc")
		assert.Contains(t, str, "LATERAL")
		assert.Contains(t, str, "2 columns")
	})
}

// TestRangeTableFuncCol tests the RangeTableFuncCol node
func TestRangeTableFuncCol(t *testing.T) {
	t.Run("NewRangeTableFuncCol", func(t *testing.T) {
		typeName := NewTypeName([]string{"text"})
		colExpr := NewA_Const(NewString("path"), -1)
		defExpr := NewA_Const(NewString("default"), -1)

		rtfc := NewRangeTableFuncCol("mycolumn", typeName, false, true, colExpr, defExpr, 200)

		require.NotNil(t, rtfc)
		assert.Equal(t, T_RangeTableFuncCol, rtfc.Tag)
		assert.Equal(t, "mycolumn", rtfc.ColName)
		assert.Equal(t, typeName, rtfc.TypeName)
		assert.False(t, rtfc.ForOrdinality)
		assert.True(t, rtfc.IsNotNull)
		assert.Equal(t, colExpr, rtfc.ColExpr)
		assert.Equal(t, defExpr, rtfc.ColDefExpr)
		assert.Equal(t, 200, rtfc.Location())
		assert.Equal(t, "RANGE_TABLE_FUNC_COL", rtfc.StatementType())
	})

	t.Run("ForOrdinality", func(t *testing.T) {
		rtfc := NewRangeTableFuncCol("ord", nil, true, false, nil, nil, 0)
		str := rtfc.String()

		assert.Contains(t, str, "RangeTableFuncCol")
		assert.Contains(t, str, "ord")
		assert.Contains(t, str, "FOR_ORDINALITY")
	})

	t.Run("NotNull", func(t *testing.T) {
		rtfc := NewRangeTableFuncCol("col", nil, false, true, nil, nil, 0)
		str := rtfc.String()

		assert.Contains(t, str, "NOT_NULL")
	})
}

// TestRangeTableSample tests the RangeTableSample node
func TestRangeTableSample(t *testing.T) {
	t.Run("NewRangeTableSample", func(t *testing.T) {
		relation := NewRangeVar("mytable", "public", "")
		method := NewNodeList(NewString("bernoulli"))
		args := NewNodeList(NewA_Const(NewFloat("0.1"), -1))
		repeatable := NewA_Const(NewInteger(42), -1)

		rts := NewRangeTableSample(relation, method, args, repeatable, 300)

		require.NotNil(t, rts)
		assert.Equal(t, T_RangeTableSample, rts.Tag)
		assert.Equal(t, relation, rts.Relation)
		assert.Equal(t, method, rts.Method)
		assert.Equal(t, args, rts.Args)
		assert.Equal(t, repeatable, rts.Repeatable)
		assert.Equal(t, 300, rts.Location())
		assert.Equal(t, "RANGE_TABLE_SAMPLE", rts.StatementType())
	})

	t.Run("String_WithRepeatable", func(t *testing.T) {
		rts := NewRangeTableSample(nil, nil, NewNodeList(NewString("arg")), NewInteger(1), 0)
		str := rts.String()

		assert.Contains(t, str, "RangeTableSample")
		assert.Contains(t, str, "1 args")
		assert.Contains(t, str, "REPEATABLE")
	})

	t.Run("String_NoRepeatable", func(t *testing.T) {
		rts := NewRangeTableSample(nil, nil, NewNodeList(NewString("arg")), nil, 0)
		str := rts.String()

		assert.Contains(t, str, "RangeTableSample")
		assert.NotContains(t, str, "REPEATABLE")
	})
}

// TestRangeTblFunction tests the RangeTblFunction node
func TestRangeTblFunction(t *testing.T) {
	t.Run("NewRangeTblFunction", func(t *testing.T) {
		funcExpr := NewFuncCall(&NodeList{Items: []Node{NewString("generate_series")}}, NewNodeList(NewInteger(1), NewInteger(10)), 0)
		rtf := NewRangeTblFunction(funcExpr, 1)

		require.NotNil(t, rtf)
		assert.Equal(t, T_RangeTblFunction, rtf.Tag)
		assert.Equal(t, funcExpr, rtf.FuncExpr)
		assert.Equal(t, 1, rtf.FuncColCount)
		assert.Equal(t, "RANGE_TBL_FUNCTION", rtf.StatementType())
	})

	t.Run("WithColumnDefinitions", func(t *testing.T) {
		funcExpr := NewFuncCall(&NodeList{Items: []Node{NewString("myfunction")}}, nil, 0)
		rtf := NewRangeTblFunction(funcExpr, 3)
		rtf.FuncColNames = []string{"col1", "col2", "col3"}
		rtf.FuncColTypes = []Oid{23, 25, 16} // int4, text, bool
		rtf.FuncColTypMods = []int{-1, -1, -1}
		rtf.FuncColCollations = []Oid{0, 100, 0}

		assert.Equal(t, []string{"col1", "col2", "col3"}, rtf.FuncColNames)
		assert.Equal(t, []Oid{23, 25, 16}, rtf.FuncColTypes)
		assert.Equal(t, []int{-1, -1, -1}, rtf.FuncColTypMods)
		assert.Equal(t, []Oid{0, 100, 0}, rtf.FuncColCollations)
	})

	t.Run("String", func(t *testing.T) {
		rtf := NewRangeTblFunction(nil, 5)
		str := rtf.String()

		assert.Contains(t, str, "RangeTblFunction")
		assert.Contains(t, str, "5 cols")
	})
}

// TestRTEPermissionInfo tests the RTEPermissionInfo node
func TestRTEPermissionInfo(t *testing.T) {
	t.Run("NewRTEPermissionInfo", func(t *testing.T) {
		relid := Oid(12345)
		requiredPerms := AclMode(2) // ACL_SELECT
		checkAsUser := Oid(100)

		rpi := NewRTEPermissionInfo(relid, true, requiredPerms, checkAsUser)

		require.NotNil(t, rpi)
		assert.Equal(t, T_RTEPermissionInfo, rpi.Tag)
		assert.Equal(t, relid, rpi.Relid)
		assert.True(t, rpi.Inh)
		assert.Equal(t, requiredPerms, rpi.RequiredPerms)
		assert.Equal(t, checkAsUser, rpi.CheckAsUser)
		assert.Equal(t, "RTE_PERMISSION_INFO", rpi.StatementType())
	})

	t.Run("WithColumnPermissions", func(t *testing.T) {
		rpi := NewRTEPermissionInfo(123, false, 2, 0)
		rpi.SelectedCols = []int{1, 2, 3}
		rpi.InsertedCols = []int{2, 3}
		rpi.UpdatedCols = []int{3}

		assert.Equal(t, []int{1, 2, 3}, rpi.SelectedCols)
		assert.Equal(t, []int{2, 3}, rpi.InsertedCols)
		assert.Equal(t, []int{3}, rpi.UpdatedCols)
	})

	t.Run("String", func(t *testing.T) {
		rpi := NewRTEPermissionInfo(54321, false, 6, 200) // ACL_INSERT | ACL_UPDATE
		str := rpi.String()

		assert.Contains(t, str, "RTEPermissionInfo")
		assert.Contains(t, str, "relid=54321")
		assert.Contains(t, str, "perms=0x6")
	})
}

// TestRangeTblRef tests the RangeTblRef node
func TestRangeTblRef(t *testing.T) {
	t.Run("NewRangeTblRef", func(t *testing.T) {
		rtref := NewRangeTblRef(3)

		require.NotNil(t, rtref)
		assert.Equal(t, T_RangeTblRef, rtref.Tag)
		assert.Equal(t, 3, rtref.RtIndex)
		assert.Equal(t, "RANGE_TBL_REF", rtref.ExpressionType())
	})

	t.Run("String", func(t *testing.T) {
		rtref := NewRangeTblRef(7)
		str := rtref.String()

		assert.Contains(t, str, "RangeTblRef")
		assert.Contains(t, str, "index=7")
	})
}

// TestRangeTableNodeInterfaces tests that all range table nodes implement required interfaces
func TestRangeTableNodeInterfaces(t *testing.T) {
	tests := []struct {
		name string
		node Node
	}{
		{"RangeTblEntry", NewRangeTblEntry(RTE_RELATION, nil)},
		{"RangeSubselect", NewRangeSubselect(false, nil, nil)},
		{"RangeFunction", NewRangeFunction(false, false, false, nil, nil, nil)},
		{"RangeTableFunc", NewRangeTableFunc(false, nil, nil, nil, nil, nil, 0)},
		{"RangeTableFuncCol", NewRangeTableFuncCol("col", nil, false, false, nil, nil, 0)},
		{"RangeTableSample", NewRangeTableSample(nil, nil, nil, nil, 0)},
		{"RangeTblFunction", NewRangeTblFunction(nil, 0)},
		{"RTEPermissionInfo", NewRTEPermissionInfo(0, false, 0, 0)},
		{"RangeTblRef", NewRangeTblRef(0)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Test Node interface
			assert.Implements(t, (*Node)(nil), test.node)
			assert.NotEqual(t, T_Invalid, test.node.NodeTag())
			assert.GreaterOrEqual(t, test.node.Location(), 0)

			// Test String method
			str := test.node.String()
			assert.NotEmpty(t, str)
		})
	}
}

// TestRangeTableNodeTags tests that all range table nodes have correct tags
func TestRangeTableNodeTags(t *testing.T) {
	tests := []struct {
		name     string
		node     Node
		expected NodeTag
	}{
		{"RangeTblEntry", NewRangeTblEntry(RTE_RELATION, nil), T_RangeTblEntry},
		{"RangeSubselect", NewRangeSubselect(false, nil, nil), T_RangeSubselect},
		{"RangeFunction", NewRangeFunction(false, false, false, nil, nil, nil), T_RangeFunction},
		{"RangeTableFunc", NewRangeTableFunc(false, nil, nil, nil, nil, nil, 0), T_RangeTableFunc},
		{"RangeTableFuncCol", NewRangeTableFuncCol("col", nil, false, false, nil, nil, 0), T_RangeTableFuncCol},
		{"RangeTableSample", NewRangeTableSample(nil, nil, nil, nil, 0), T_RangeTableSample},
		{"RangeTblFunction", NewRangeTblFunction(nil, 0), T_RangeTblFunction},
		{"RTEPermissionInfo", NewRTEPermissionInfo(0, false, 0, 0), T_RTEPermissionInfo},
		{"RangeTblRef", NewRangeTblRef(0), T_RangeTblRef},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.node.NodeTag())
		})
	}
}
