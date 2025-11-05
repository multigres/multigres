// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

func TestSubstituteParameters(t *testing.T) {
	t.Run("no parameters", func(t *testing.T) {
		// Create a simple SELECT statement with no parameters
		stmt := &SelectStmt{
			BaseNode: BaseNode{Tag: T_SelectStmt},
		}

		result, err := SubstituteParameters(stmt, nil, nil, nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("text parameter - integer", func(t *testing.T) {
		// Create a SELECT with a parameter reference: SELECT $1
		paramRef := NewParamRef(1, 0)
		target := &ResTarget{
			BaseNode: BaseNode{Tag: T_ResTarget},
			Val:      paramRef,
		}

		stmt := &SelectStmt{
			BaseNode:    BaseNode{Tag: T_SelectStmt},
			TargetList:  NewNodeList(target),
			FromClause:  nil,
			WhereClause: nil,
		}

		// Parameter: $1 = "42" (text format, int4 type)
		params := [][]byte{[]byte("42")}
		formats := []int16{TextFormat}
		types := []uint32{uint32(INT4OID)}

		result, err := SubstituteParameters(stmt, params, formats, types)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify the parameter was substituted
		selectStmt := result.(*SelectStmt)
		assert.NotNil(t, selectStmt.TargetList)
		assert.Equal(t, 1, selectStmt.TargetList.Len())

		resTarget := selectStmt.TargetList.Items[0].(*ResTarget)
		aConst, ok := resTarget.Val.(*A_Const)
		require.True(t, ok, "Expected A_Const, got %T", resTarget.Val)
		assert.False(t, aConst.Isnull)

		intVal, ok := aConst.Val.(*Integer)
		require.True(t, ok, "Expected Integer value")
		assert.Equal(t, 42, intVal.IVal)
	})

	t.Run("text parameter - string", func(t *testing.T) {
		// Create a WHERE clause with parameter: WHERE name = $1
		paramRef := NewParamRef(1, 0)

		stmt := &SelectStmt{
			BaseNode:    BaseNode{Tag: T_SelectStmt},
			WhereClause: paramRef,
		}

		// Parameter: $1 = "hello" (text format, text type)
		params := [][]byte{[]byte("hello")}
		formats := []int16{TextFormat}
		types := []uint32{uint32(TEXTOID)}

		result, err := SubstituteParameters(stmt, params, formats, types)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify the parameter was substituted
		selectStmt := result.(*SelectStmt)
		aConst, ok := selectStmt.WhereClause.(*A_Const)
		require.True(t, ok, "Expected A_Const, got %T", selectStmt.WhereClause)
		assert.False(t, aConst.Isnull)

		strVal, ok := aConst.Val.(*String)
		require.True(t, ok, "Expected String value")
		assert.Equal(t, "hello", strVal.SVal)
	})

	t.Run("null parameter", func(t *testing.T) {
		paramRef := NewParamRef(1, 0)

		stmt := &SelectStmt{
			BaseNode:    BaseNode{Tag: T_SelectStmt},
			WhereClause: paramRef,
		}

		// Parameter: $1 = NULL
		params := [][]byte{nil}
		formats := []int16{TextFormat}
		types := []uint32{uint32(TEXTOID)}

		result, err := SubstituteParameters(stmt, params, formats, types)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify the parameter was substituted with NULL
		selectStmt := result.(*SelectStmt)
		aConst, ok := selectStmt.WhereClause.(*A_Const)
		require.True(t, ok, "Expected A_Const, got %T", selectStmt.WhereClause)
		assert.True(t, aConst.Isnull)
	})

	t.Run("multiple parameters", func(t *testing.T) {
		// Create: SELECT $1, $2
		param1 := NewParamRef(1, 0)
		param2 := NewParamRef(2, 0)

		target1 := &ResTarget{
			BaseNode: BaseNode{Tag: T_ResTarget},
			Val:      param1,
		}
		target2 := &ResTarget{
			BaseNode: BaseNode{Tag: T_ResTarget},
			Val:      param2,
		}

		stmt := &SelectStmt{
			BaseNode:   BaseNode{Tag: T_SelectStmt},
			TargetList: NewNodeList(target1, target2),
		}

		// Parameters: $1 = "100", $2 = "hello"
		params := [][]byte{[]byte("100"), []byte("hello")}
		formats := []int16{TextFormat, TextFormat}
		types := []uint32{uint32(INT4OID), uint32(TEXTOID)}

		result, err := SubstituteParameters(stmt, params, formats, types)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify both parameters were substituted
		selectStmt := result.(*SelectStmt)
		assert.Equal(t, 2, selectStmt.TargetList.Len())

		// Check first parameter (integer)
		resTarget1 := selectStmt.TargetList.Items[0].(*ResTarget)
		aConst1, ok := resTarget1.Val.(*A_Const)
		require.True(t, ok)
		intVal, ok := aConst1.Val.(*Integer)
		require.True(t, ok)
		assert.Equal(t, 100, intVal.IVal)

		// Check second parameter (string)
		resTarget2 := selectStmt.TargetList.Items[1].(*ResTarget)
		aConst2, ok := resTarget2.Val.(*A_Const)
		require.True(t, ok)
		strVal, ok := aConst2.Val.(*String)
		require.True(t, ok)
		assert.Equal(t, "hello", strVal.SVal)
	})

	t.Run("boolean parameter", func(t *testing.T) {
		paramRef := NewParamRef(1, 0)

		stmt := &SelectStmt{
			BaseNode:    BaseNode{Tag: T_SelectStmt},
			WhereClause: paramRef,
		}

		// Parameter: $1 = "true" (text format, bool type)
		params := [][]byte{[]byte("true")}
		formats := []int16{TextFormat}
		types := []uint32{uint32(BOOLOID)}

		result, err := SubstituteParameters(stmt, params, formats, types)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify the parameter was substituted
		selectStmt := result.(*SelectStmt)
		aConst, ok := selectStmt.WhereClause.(*A_Const)
		require.True(t, ok)

		boolVal, ok := aConst.Val.(*Boolean)
		require.True(t, ok)
		assert.True(t, boolVal.BoolVal)
	})
}

func TestCreateParameterValues(t *testing.T) {
	t.Run("text integer", func(t *testing.T) {
		params := [][]byte{[]byte("42")}
		formats := []int16{TextFormat}
		types := []uint32{uint32(INT4OID)}

		values, err := createParameterValues(params, formats, types)
		require.NoError(t, err)
		require.Len(t, values, 1)

		aConst := values[0]
		assert.False(t, aConst.Isnull)
		intVal, ok := aConst.Val.(*Integer)
		require.True(t, ok)
		assert.Equal(t, 42, intVal.IVal)
	})

	t.Run("text string", func(t *testing.T) {
		params := [][]byte{[]byte("hello world")}
		formats := []int16{TextFormat}
		types := []uint32{uint32(TEXTOID)}

		values, err := createParameterValues(params, formats, types)
		require.NoError(t, err)
		require.Len(t, values, 1)

		aConst := values[0]
		assert.False(t, aConst.Isnull)
		strVal, ok := aConst.Val.(*String)
		require.True(t, ok)
		assert.Equal(t, "hello world", strVal.SVal)
	})

	t.Run("null value", func(t *testing.T) {
		params := [][]byte{nil}
		formats := []int16{TextFormat}
		types := []uint32{uint32(TEXTOID)}

		values, err := createParameterValues(params, formats, types)
		require.NoError(t, err)
		require.Len(t, values, 1)

		aConst := values[0]
		assert.True(t, aConst.Isnull)
	})

	t.Run("invalid integer", func(t *testing.T) {
		params := [][]byte{[]byte("not a number")}
		formats := []int16{TextFormat}
		types := []uint32{uint32(INT4OID)}

		_, err := createParameterValues(params, formats, types)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid integer")
	})

	t.Run("invalid boolean", func(t *testing.T) {
		params := [][]byte{[]byte("maybe")}
		formats := []int16{TextFormat}
		types := []uint32{uint32(BOOLOID)}

		_, err := createParameterValues(params, formats, types)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid boolean")
	})

	t.Run("format code applies to all", func(t *testing.T) {
		// Single format code should apply to all parameters
		params := [][]byte{[]byte("42"), []byte("100")}
		formats := []int16{TextFormat} // Single format for all
		types := []uint32{uint32(INT4OID), uint32(INT4OID)}

		values, err := createParameterValues(params, formats, types)
		require.NoError(t, err)
		require.Len(t, values, 2)

		// Both should be parsed as integers
		for i, val := range values {
			assert.False(t, val.Isnull, "param %d", i)
			_, ok := val.Val.(*Integer)
			assert.True(t, ok, "param %d should be Integer", i)
		}
	})
}
