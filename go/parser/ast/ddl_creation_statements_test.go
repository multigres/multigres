// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2025, Supabase, Inc
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
package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==============================================================================
// DDL CREATION STATEMENTS TESTS - Comprehensive test coverage for DDL creation nodes
// ==============================================================================

func TestCoercionContext(t *testing.T) {
	tests := []struct {
		name     string
		context  CoercionContext
		expected string
	}{
		{"implicit", COERCION_IMPLICIT, "IMPLICIT"},
		{"assignment", COERCION_ASSIGNMENT, "ASSIGNMENT"},
		{"plpgsql", COERCION_PLPGSQL, "PLPGSQL"},
		{"explicit", COERCION_EXPLICIT, "EXPLICIT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.context.String())
		})
	}
}

func TestFunctionParameterMode(t *testing.T) {
	tests := []struct {
		name     string
		mode     FunctionParameterMode
		expected string
	}{
		{"in", FUNC_PARAM_IN, "IN"},
		{"out", FUNC_PARAM_OUT, "OUT"},
		{"inout", FUNC_PARAM_INOUT, "INOUT"},
		{"variadic", FUNC_PARAM_VARIADIC, "VARIADIC"},
		{"table", FUNC_PARAM_TABLE, "TABLE"},
		{"default", FUNC_PARAM_DEFAULT, "DEFAULT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mode.String())
		})
	}
}

func TestFetchDirection(t *testing.T) {
	tests := []struct {
		name      string
		direction FetchDirection
		expected  string
	}{
		{"forward", FETCH_FORWARD, "FORWARD"},
		{"backward", FETCH_BACKWARD, "BACKWARD"},
		{"absolute", FETCH_ABSOLUTE, "ABSOLUTE"},
		{"relative", FETCH_RELATIVE, "RELATIVE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.direction.String())
		})
	}
}

func TestFunctionParameter(t *testing.T) {
	t.Run("basic function parameter", func(t *testing.T) {
		name := "param1"
		argType := NewTypeName([]string{"int4"})
		param := NewFunctionParameter(name, argType, FUNC_PARAM_IN, nil)

		assert.NotNil(t, param)
		assert.Equal(t, name, param.Name)
		assert.Equal(t, argType, param.ArgType)
		assert.Equal(t, FUNC_PARAM_IN, param.Mode)
		assert.Nil(t, param.DefExpr)

		// Test node interface - now implemented via BaseNode
		assert.Equal(t, T_FunctionParameter, param.Tag)

		// Test string representation
		str := param.String()
		assert.Contains(t, str, "param1")
		assert.Contains(t, str, "int4")
	})

	t.Run("parameter with default", func(t *testing.T) {
		name := "param2"
		argType := NewTypeName([]string{"text"})
		defaultExpr := &A_Const{Val: NewString("default_value")}
		param := NewFunctionParameter(name, argType, FUNC_PARAM_IN, defaultExpr)

		assert.NotNil(t, param)
		assert.Equal(t, name, param.Name)
		assert.Equal(t, argType, param.ArgType)
		assert.Equal(t, FUNC_PARAM_IN, param.Mode)
		assert.Equal(t, defaultExpr, param.DefExpr)

		str := param.String()
		assert.Contains(t, str, "param2")
		assert.Contains(t, str, "text")
		assert.Contains(t, str, "DEFAULT")
	})

	t.Run("out parameter", func(t *testing.T) {
		name := "result"
		argType := NewTypeName([]string{"int4"})
		param := NewFunctionParameter(name, argType, FUNC_PARAM_OUT, nil)

		str := param.String()
		assert.Contains(t, str, "OUT")
		assert.Contains(t, str, "result")
		assert.Contains(t, str, "int4")
	})
}

func TestCreateFunctionStmt(t *testing.T) {
	t.Run("basic function", func(t *testing.T) {
		funcName := &NodeList{Items: []Node{NewString("public"), NewString("test_func")}}
		returnType := NewTypeName([]string{"int4"})
		param1 := NewFunctionParameter("", NewTypeName([]string{"text"}), FUNC_PARAM_IN, nil)
		parameters := &NodeList{Items: []Node{param1}}

		stmt := NewCreateFunctionStmt(false, false, funcName, parameters, returnType, nil, nil)

		assert.NotNil(t, stmt)
		assert.False(t, stmt.IsProcedure)
		assert.False(t, stmt.Replace)
		assert.Equal(t, funcName, stmt.FuncName)
		assert.Equal(t, 1, stmt.Parameters.Len())
		assert.Equal(t, returnType, stmt.ReturnType)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE FUNCTION")
		assert.Contains(t, str, "public.test_func")
		assert.Contains(t, str, "RETURNS int4")
	})

	t.Run("replace procedure", func(t *testing.T) {
		funcName := &NodeList{Items: []Node{NewString("test_proc")}}
		stmt := NewCreateFunctionStmt(true, true, funcName, nil, nil, nil, nil)

		assert.True(t, stmt.IsProcedure)
		assert.True(t, stmt.Replace)
		assert.Equal(t, funcName, stmt.FuncName)

		str := stmt.String()
		assert.Contains(t, str, "CREATE OR REPLACE PROCEDURE")
		assert.Contains(t, str, "test_proc")
		assert.NotContains(t, str, "RETURNS")
	})
}

func TestCreateSeqStmt(t *testing.T) {
	t.Run("basic sequence", func(t *testing.T) {
		sequence := &RangeVar{SchemaName: "public", RelName: "test_seq"}
		stmt := NewCreateSeqStmt(sequence, nil, 0, false, false)

		assert.NotNil(t, stmt)
		assert.Equal(t, sequence, stmt.Sequence)
		assert.False(t, stmt.ForIdentity)
		assert.False(t, stmt.IfNotExists)
		assert.Equal(t, Oid(0), stmt.OwnerID)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE SEQUENCE")
		assert.Contains(t, str, "public.test_seq")
	})

	t.Run("if not exists sequence", func(t *testing.T) {
		sequence := &RangeVar{RelName: "test_seq"}
		stmt := NewCreateSeqStmt(sequence, nil, 0, true, true)

		assert.True(t, stmt.ForIdentity)
		assert.True(t, stmt.IfNotExists)

		str := stmt.String()
		assert.Contains(t, str, "CREATE SEQUENCE IF NOT EXISTS")
	})
}

func TestCreateOpClassItem(t *testing.T) {
	t.Run("operator item", func(t *testing.T) {
		name := &ObjectWithArgs{Objname: &NodeList{Items: []Node{NewString("<")}}}
		item := NewCreateOpClassItem(OPCLASS_ITEM_OPERATOR, name, 1, nil, nil, nil)

		assert.NotNil(t, item)
		assert.Equal(t, OPCLASS_ITEM_OPERATOR, item.ItemType)
		assert.Equal(t, name, item.Name)
		assert.Equal(t, 1, item.Number)

		// CreateOpClassItem doesn't implement node interface directly

		str := item.String()
		assert.Contains(t, str, "OPERATOR")
		assert.Contains(t, str, "1")
	})

	t.Run("function item", func(t *testing.T) {
		name := &ObjectWithArgs{Objname: &NodeList{Items: []Node{NewString("btint4cmp")}}}
		item := NewCreateOpClassItem(OPCLASS_ITEM_FUNCTION, name, 1, nil, nil, nil)

		str := item.String()
		assert.Contains(t, str, "FUNCTION")
	})

	t.Run("storage item", func(t *testing.T) {
		storedType := NewTypeName([]string{"int4"})
		item := NewCreateOpClassItem(OPCLASS_ITEM_STORAGETYPE, nil, 0, nil, nil, storedType)

		str := item.String()
		assert.Contains(t, str, "STORAGE")
		assert.Contains(t, str, "int4")
	})
}

func TestCreateOpClassStmt(t *testing.T) {
	t.Run("basic operator class", func(t *testing.T) {
		opClassName := &NodeList{Items: []Node{NewString("int4_ops")}}
		dataType := NewTypeName([]string{"int4"})
		stmt := NewCreateOpClassStmt(opClassName, nil, "btree", dataType, nil, false)

		assert.NotNil(t, stmt)
		assert.Equal(t, opClassName, stmt.OpClassName)
		assert.Equal(t, "btree", stmt.AmName)
		assert.Equal(t, dataType, stmt.DataType)
		assert.False(t, stmt.IsDefault)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE OPERATOR CLASS")
		assert.Contains(t, str, "int4_ops")
		assert.Contains(t, str, "FOR TYPE int4")
		assert.Contains(t, str, "USING btree")
	})

	t.Run("default operator class with family", func(t *testing.T) {
		opClassName := &NodeList{Items: []Node{NewString("int4_ops")}}
		opFamilyName := &NodeList{Items: []Node{NewString("integer_ops")}}
		dataType := NewTypeName([]string{"int4"})
		stmt := NewCreateOpClassStmt(opClassName, opFamilyName, "btree", dataType, nil, true)

		assert.True(t, stmt.IsDefault)
		assert.Equal(t, opFamilyName, stmt.OpFamilyName)

		str := stmt.String()
		assert.Contains(t, str, "DEFAULT")
		assert.Contains(t, str, "FAMILY integer_ops")
	})
}

func TestCreateEnumStmt(t *testing.T) {
	t.Run("basic enum", func(t *testing.T) {
		typeName := NewNodeList(NewString("mood"))
		vals := NewNodeList(NewString("sad"))
		vals.Append(NewString("ok"))
		vals.Append(NewString("happy"))
		stmt := NewCreateEnumStmt(typeName, vals)

		assert.NotNil(t, stmt)
		assert.Equal(t, typeName, stmt.TypeName)
		assert.Equal(t, vals, stmt.Vals)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE TYPE mood AS ENUM")
		assert.Contains(t, str, "'sad', 'ok', 'happy'")
	})

	t.Run("empty enum", func(t *testing.T) {
		typeName := NewNodeList(NewString("empty_enum"))
		stmt := NewCreateEnumStmt(typeName, nil)

		str := stmt.String()
		assert.Contains(t, str, "CREATE TYPE empty_enum AS ENUM ()")
	})
}

func TestCreateRangeStmt(t *testing.T) {
	t.Run("basic range type", func(t *testing.T) {
		typeName := NewNodeList(NewString("int4range"))
		params := NewNodeList(&DefElem{Defname: "subtype", Arg: NewString("int4")})
		stmt := NewCreateRangeStmt(typeName, params)

		assert.NotNil(t, stmt)
		assert.Equal(t, typeName, stmt.TypeName)
		assert.Equal(t, params, stmt.Params)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE TYPE int4range AS RANGE")
		assert.Contains(t, str, "(...)")
	})

	t.Run("range type without params", func(t *testing.T) {
		typeName := NewNodeList(NewString("simplerange"))
		stmt := NewCreateRangeStmt(typeName, nil)

		str := stmt.String()
		assert.Contains(t, str, "CREATE TYPE simplerange AS RANGE")
		assert.NotContains(t, str, "(...)")
	})
}

func TestCreateStatsStmt(t *testing.T) {
	t.Run("basic statistics", func(t *testing.T) {
		defNames := &NodeList{Items: []Node{NewString("my_stats")}}
		statTypes := &NodeList{Items: []Node{NewString("ndistinct"), NewString("dependencies")}}
		relations := &NodeList{Items: []Node{&RangeVar{RelName: "users"}}}
		stmt := NewCreateStatsStmt(defNames, statTypes, nil, relations, "", false, false)

		assert.NotNil(t, stmt)
		assert.Equal(t, defNames, stmt.DefNames)
		assert.Equal(t, statTypes, stmt.StatTypes)
		assert.Equal(t, relations, stmt.Relations)
		assert.False(t, stmt.Transformed)
		assert.False(t, stmt.IfNotExists)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE STATISTICS my_stats")
		assert.Contains(t, str, "(ndistinct, dependencies)")
		assert.Contains(t, str, "ON")
		assert.Contains(t, str, "users")
	})

	t.Run("if not exists statistics", func(t *testing.T) {
		defNames := &NodeList{Items: []Node{NewString("my_stats")}}
		relations := &NodeList{Items: []Node{&RangeVar{RelName: "users"}}}
		stmt := NewCreateStatsStmt(defNames, nil, nil, relations, "", false, true)

		assert.True(t, stmt.IfNotExists)
		str := stmt.String()
		assert.Contains(t, str, "CREATE STATISTICS IF NOT EXISTS")
	})

	t.Run("statistics with comment", func(t *testing.T) {
		defNames := &NodeList{Items: []Node{NewString("my_stats")}}
		relations := &NodeList{Items: []Node{&RangeVar{RelName: "users"}}}
		comment := "Statistics for user table"
		stmt := NewCreateStatsStmt(defNames, nil, nil, relations, comment, true, false)

		assert.True(t, stmt.Transformed)
		assert.Equal(t, comment, stmt.StxComment)
	})
}

func TestCreatePLangStmt(t *testing.T) {
	t.Run("basic language", func(t *testing.T) {
		plHandler := NewNodeList(NewString("plperl_call_handler"))
		stmt := NewCreatePLangStmt(false, "plperl", plHandler, nil, nil, false)

		assert.NotNil(t, stmt)
		assert.False(t, stmt.Replace)
		assert.Equal(t, "plperl", stmt.PLName)
		assert.Equal(t, plHandler, stmt.PLHandler)
		assert.False(t, stmt.PLTrusted)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE LANGUAGE plperl")
		assert.Contains(t, str, "HANDLER plperl_call_handler")
	})

	t.Run("trusted language with replace", func(t *testing.T) {
		plHandler := NewNodeList(NewString("plpgsql_call_handler"))
		plInline := NewNodeList(NewString("plpgsql_inline_handler"))
		plValidator := NewNodeList(NewString("plpgsql_validator"))
		stmt := NewCreatePLangStmt(true, "plpgsql", plHandler, plInline, plValidator, true)

		assert.True(t, stmt.Replace)
		assert.True(t, stmt.PLTrusted)
		assert.Equal(t, plInline, stmt.PLInline)
		assert.Equal(t, plValidator, stmt.PLValidator)

		str := stmt.String()
		assert.Contains(t, str, "CREATE OR REPLACE TRUSTED LANGUAGE plpgsql")
		assert.Contains(t, str, "HANDLER plpgsql_call_handler")
		assert.Contains(t, str, "INLINE plpgsql_inline_handler")
		assert.Contains(t, str, "VALIDATOR plpgsql_validator")
	})

	t.Run("minimal language", func(t *testing.T) {
		stmt := NewCreatePLangStmt(false, "c", nil, nil, nil, true)

		assert.Equal(t, "c", stmt.PLName)
		assert.True(t, stmt.PLTrusted)
		assert.Nil(t, stmt.PLHandler)

		str := stmt.String()
		assert.Contains(t, str, "CREATE TRUSTED LANGUAGE c")
		assert.NotContains(t, str, "HANDLER")
	})
}

func TestCreateOpFamilyStmt(t *testing.T) {
	t.Run("basic operator family", func(t *testing.T) {
		opFamilyName := &NodeList{Items: []Node{NewString("integer_ops")}}
		stmt := NewCreateOpFamilyStmt(opFamilyName, "btree")

		assert.NotNil(t, stmt)
		assert.Equal(t, opFamilyName, stmt.OpFamilyName)
		assert.Equal(t, "btree", stmt.AmName)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE OPERATOR FAMILY")
		assert.Contains(t, str, "integer_ops")
		assert.Contains(t, str, "USING btree")
	})
}

func TestCreateCastStmt(t *testing.T) {
	t.Run("cast with function", func(t *testing.T) {
		sourceType := NewTypeName([]string{"int4"})
		targetType := NewTypeName([]string{"text"})
		function := &ObjectWithArgs{Objname: &NodeList{Items: []Node{NewString("int4out")}}}
		stmt := NewCreateCastStmt(sourceType, targetType, function, COERCION_EXPLICIT, false)

		assert.NotNil(t, stmt)
		assert.Equal(t, sourceType, stmt.SourceType)
		assert.Equal(t, targetType, stmt.TargetType)
		assert.Equal(t, function, stmt.Func)
		assert.Equal(t, COERCION_EXPLICIT, stmt.Context)
		assert.False(t, stmt.Inout)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE CAST")
		assert.Contains(t, str, "(int4 AS text)")
		assert.Contains(t, str, "WITH FUNCTION int4out")
	})

	t.Run("cast without function", func(t *testing.T) {
		sourceType := NewTypeName([]string{"int4"})
		targetType := NewTypeName([]string{"int8"})
		stmt := NewCreateCastStmt(sourceType, targetType, nil, COERCION_IMPLICIT, false)

		str := stmt.String()
		assert.Contains(t, str, "WITHOUT FUNCTION")
	})

	t.Run("inout cast", func(t *testing.T) {
		sourceType := NewTypeName([]string{"int4"})
		targetType := NewTypeName([]string{"text"})
		stmt := NewCreateCastStmt(sourceType, targetType, nil, COERCION_EXPLICIT, true)

		assert.True(t, stmt.Inout)
		str := stmt.String()
		assert.Contains(t, str, "WITH INOUT")
	})
}

func TestCreateConversionStmt(t *testing.T) {
	t.Run("basic conversion", func(t *testing.T) {
		conversionName := &NodeList{Items: []Node{NewString("utf8_to_latin1")}}
		funcName := &NodeList{Items: []Node{NewString("utf8_to_iso8859_1")}}
		stmt := NewCreateConversionStmt(conversionName, "UTF8", "LATIN1", funcName, false)

		assert.NotNil(t, stmt)
		assert.Equal(t, conversionName, stmt.ConversionName)
		assert.Equal(t, "UTF8", stmt.ForEncodingName)
		assert.Equal(t, "LATIN1", stmt.ToEncodingName)
		assert.Equal(t, funcName, stmt.FuncName)
		assert.False(t, stmt.Def)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE CONVERSION")
		assert.Contains(t, str, "utf8_to_latin1")
		assert.Contains(t, str, "FOR 'UTF8' TO 'LATIN1'")
		assert.Contains(t, str, "FROM utf8_to_iso8859_1")
	})

	t.Run("default conversion", func(t *testing.T) {
		conversionName := &NodeList{Items: []Node{NewString("utf8_to_latin1")}}
		funcName := &NodeList{Items: []Node{NewString("utf8_to_iso8859_1")}}
		stmt := NewCreateConversionStmt(conversionName, "UTF8", "LATIN1", funcName, true)

		assert.True(t, stmt.Def)
		str := stmt.String()
		assert.Contains(t, str, "CREATE DEFAULT CONVERSION")
	})
}

func TestCreateTransformStmt(t *testing.T) {
	t.Run("basic transform", func(t *testing.T) {
		typeName := NewTypeName([]string{"hstore"})
		fromSql := &ObjectWithArgs{Objname: &NodeList{Items: []Node{NewString("hstore_to_plperl")}}}
		toSql := &ObjectWithArgs{Objname: &NodeList{Items: []Node{NewString("plperl_to_hstore")}}}
		stmt := NewCreateTransformStmt(false, typeName, "plperl", fromSql, toSql)

		assert.NotNil(t, stmt)
		assert.False(t, stmt.Replace)
		assert.Equal(t, typeName, stmt.TypeName)
		assert.Equal(t, "plperl", stmt.Lang)
		assert.Equal(t, fromSql, stmt.FromSql)
		assert.Equal(t, toSql, stmt.ToSql)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE TRANSFORM FOR")
		assert.Contains(t, str, "hstore")
		assert.Contains(t, str, "LANGUAGE plperl")
	})

	t.Run("replace transform", func(t *testing.T) {
		typeName := NewTypeName([]string{"hstore"})
		stmt := NewCreateTransformStmt(true, typeName, "plperl", nil, nil)

		assert.True(t, stmt.Replace)
		str := stmt.String()
		assert.Contains(t, str, "CREATE OR REPLACE TRANSFORM FOR")
	})
}

func TestDefineStmt(t *testing.T) {
	t.Run("create aggregate", func(t *testing.T) {
		defNames := NewNodeList(NewString("my_avg"))
		args := NewNodeList(NewTypeName([]string{"int4"}))
		stmt := NewDefineStmt(OBJECT_AGGREGATE, false, defNames, args, nil, false, false)

		assert.NotNil(t, stmt)
		assert.Equal(t, OBJECT_AGGREGATE, stmt.Kind)
		assert.False(t, stmt.OldStyle)
		assert.Equal(t, defNames, stmt.DefNames)
		assert.Equal(t, args, stmt.Args)
		assert.False(t, stmt.IfNotExists)
		assert.False(t, stmt.Replace)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.String()
		assert.Contains(t, str, "CREATE AGGREGATE")
		assert.Contains(t, str, "my_avg")
	})

	t.Run("create operator with replace", func(t *testing.T) {
		defNames := NewNodeList(NewString("@"))
		stmt := NewDefineStmt(OBJECT_OPERATOR, false, defNames, nil, nil, false, true)

		assert.True(t, stmt.Replace)
		str := stmt.String()
		assert.Contains(t, str, "CREATE OR REPLACE OPERATOR")
	})

	t.Run("create type if not exists", func(t *testing.T) {
		defNames := NewNodeList(NewString("my_type"))
		stmt := NewDefineStmt(OBJECT_TYPE, false, defNames, nil, nil, true, false)

		assert.True(t, stmt.IfNotExists)
		str := stmt.String()
		assert.Contains(t, str, "CREATE TYPE IF NOT EXISTS")
	})
}

func TestDeclareCursorStmt(t *testing.T) {
	t.Run("basic cursor", func(t *testing.T) {
		portalName := "test_cursor"
		query := &SelectStmt{
			TargetList: NewNodeList(&ResTarget{Val: &ColumnRef{Fields: NewNodeList(NewString("*"))}}),
		}
		stmt := NewDeclareCursorStmt(portalName, 0, query)

		assert.NotNil(t, stmt)
		assert.Equal(t, portalName, stmt.PortalName)
		assert.Equal(t, 0, stmt.Options)
		assert.Equal(t, query, stmt.Query)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.SqlString()
		assert.Contains(t, str, "DECLARE test_cursor CURSOR FOR")
	})
}

func TestFetchStmt(t *testing.T) {
	t.Run("fetch forward", func(t *testing.T) {
		stmt := NewFetchStmt(FETCH_FORWARD, 10, "test_cursor", false)

		assert.NotNil(t, stmt)
		assert.Equal(t, FETCH_FORWARD, stmt.Direction)
		assert.Equal(t, int64(10), stmt.HowMany)
		assert.Equal(t, "test_cursor", stmt.PortalName)
		assert.False(t, stmt.IsMove)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.SqlString()
		assert.Contains(t, str, "FETCH 10")
		assert.Contains(t, str, "FROM test_cursor")
	})

	t.Run("fetch all", func(t *testing.T) {
		stmt := NewFetchStmt(FETCH_FORWARD, 9223372036854775807, "test_cursor", false) // FETCH_ALL

		str := stmt.SqlString()
		assert.Contains(t, str, "FETCH ALL")
	})

	t.Run("fetch backward", func(t *testing.T) {
		stmt := NewFetchStmt(FETCH_BACKWARD, 5, "test_cursor", false)

		str := stmt.SqlString()
		assert.Contains(t, str, "FETCH BACKWARD 5")
	})

	t.Run("fetch absolute", func(t *testing.T) {
		stmt := NewFetchStmt(FETCH_ABSOLUTE, 100, "test_cursor", false)

		str := stmt.SqlString()
		assert.Contains(t, str, "FETCH ABSOLUTE 100")
	})

	t.Run("fetch relative", func(t *testing.T) {
		stmt := NewFetchStmt(FETCH_RELATIVE, -10, "test_cursor", false)

		str := stmt.SqlString()
		assert.Contains(t, str, "FETCH RELATIVE -10")
	})

	t.Run("move statement", func(t *testing.T) {
		stmt := NewFetchStmt(FETCH_FORWARD, 12, "test_cursor", true)

		assert.True(t, stmt.IsMove)
		str := stmt.SqlString()
		assert.Contains(t, str, "MOVE 12")
	})
}

func TestClosePortalStmt(t *testing.T) {
	t.Run("close specific cursor", func(t *testing.T) {
		portalName := "test_cursor"
		stmt := NewClosePortalStmt(portalName)

		assert.NotNil(t, stmt)
		assert.NotNil(t, stmt.PortalName)
		assert.Equal(t, portalName, stmt.PortalName)

		// CreateOpClassStmt implements StatementType and NodeTag methods

		str := stmt.SqlString()
		assert.Equal(t, "CLOSE test_cursor", str)
	})

	t.Run("close all cursors", func(t *testing.T) {
		stmt := NewClosePortalStmt("")

		assert.Nil(t, stmt.PortalName)
		str := stmt.SqlString()
		assert.Equal(t, "CLOSE ALL", str)
	})
}

// Integration tests for complex scenarios
func TestDDLCreationStmtsIntegration(t *testing.T) {
	t.Run("complex function with multiple parameters", func(t *testing.T) {
		// Create function parameters
		param1 := NewFunctionParameter(
			"input_text",
			NewTypeName([]string{"text"}),
			FUNC_PARAM_IN,
			nil,
		)
		param2 := NewFunctionParameter(
			"max_length",
			NewTypeName([]string{"int4"}),
			FUNC_PARAM_IN,
			&A_Const{Val: NewInteger(100)},
		)
		param3 := NewFunctionParameter(
			"result_length",
			NewTypeName([]string{"int4"}),
			FUNC_PARAM_OUT,
			nil,
		)

		// Create function statement
		funcName := &NodeList{Items: []Node{NewString("public"), NewString("process_text")}}
		parameters := &NodeList{Items: []Node{param1, param2, param3}}
		stmt := NewCreateFunctionStmt(
			false, // not a procedure
			true,  // replace if exists
			funcName,
			parameters,
			NewTypeName([]string{"text"}),
			nil,
			nil,
		)

		require.NotNil(t, stmt)
		assert.Equal(t, 3, stmt.Parameters.Len())
		assert.True(t, stmt.Replace)
		assert.False(t, stmt.IsProcedure)

		str := stmt.String()
		assert.Contains(t, str, "CREATE OR REPLACE FUNCTION")
		assert.Contains(t, str, "public.process_text")
		assert.Contains(t, str, "RETURNS text")
	})

	t.Run("operator class with multiple items", func(t *testing.T) {
		// Create operator class items
		item1 := NewCreateOpClassItem(
			1, // OPCLASS_ITEM_OPERATOR
			&ObjectWithArgs{Objname: &NodeList{Items: []Node{NewString("<")}}},
			1,
			nil,
			nil,
			nil,
		)
		item2 := NewCreateOpClassItem(
			2, // OPCLASS_ITEM_FUNCTION
			&ObjectWithArgs{Objname: &NodeList{Items: []Node{NewString("btint4cmp")}}},
			1,
			nil,
			nil,
			nil,
		)

		// Create operator class statement
		stmt := NewCreateOpClassStmt(
			&NodeList{Items: []Node{NewString("my_int4_ops")}},
			&NodeList{Items: []Node{NewString("integer_ops")}},
			"btree",
			NewTypeName([]string{"int4"}),
			&NodeList{Items: []Node{item1, item2}},
			true,
		)

		require.NotNil(t, stmt)
		assert.Len(t, stmt.Items.Items, 2)
		assert.True(t, stmt.IsDefault)

		str := stmt.String()
		assert.Contains(t, str, "CREATE OPERATOR CLASS my_int4_ops DEFAULT")
		assert.Contains(t, str, "FAMILY integer_ops")
	})
}
