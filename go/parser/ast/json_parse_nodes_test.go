package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJsonEncoding tests the JsonEncoding enum
func TestJsonEncoding(t *testing.T) {
	tests := []struct {
		encoding JsonEncoding
		name     string
	}{
		{JS_ENC_DEFAULT, "JS_ENC_DEFAULT"},
		{JS_ENC_UTF8, "JS_ENC_UTF8"},
		{JS_ENC_UTF16, "JS_ENC_UTF16"},
		{JS_ENC_UTF32, "JS_ENC_UTF32"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.encoding, test.encoding)
		})
	}
}

// TestJsonFormatType tests the JsonFormatType enum
func TestJsonFormatType(t *testing.T) {
	tests := []struct {
		formatType JsonFormatType
		name       string
	}{
		{JS_FORMAT_DEFAULT, "JS_FORMAT_DEFAULT"},
		{JS_FORMAT_JSON, "JS_FORMAT_JSON"},
		{JS_FORMAT_JSONB, "JS_FORMAT_JSONB"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.formatType, test.formatType)
		})
	}
}

// TestJsonBehaviorType tests the JsonBehaviorType enum
func TestJsonBehaviorType(t *testing.T) {
	tests := []struct {
		behaviorType JsonBehaviorType
		expected     string
	}{
		{JSON_BEHAVIOR_NULL, "NULL"},
		{JSON_BEHAVIOR_ERROR, "ERROR"},
		{JSON_BEHAVIOR_EMPTY, "EMPTY"},
		{JSON_BEHAVIOR_TRUE, "TRUE"},
		{JSON_BEHAVIOR_FALSE, "FALSE"},
		{JSON_BEHAVIOR_UNKNOWN, "UNKNOWN"},
		{JSON_BEHAVIOR_EMPTY_ARRAY, "EMPTY_ARRAY"},
		{JSON_BEHAVIOR_EMPTY_OBJECT, "EMPTY_OBJECT"},
		{JSON_BEHAVIOR_DEFAULT, "DEFAULT"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			behavior := NewJsonBehavior(test.behaviorType, nil, -1)
			assert.Equal(t, test.expected, behavior.String())
		})
	}
}

// TestJsonWrapper tests the JsonWrapper enum
func TestJsonWrapper(t *testing.T) {
	tests := []struct {
		wrapper JsonWrapper
		name    string
	}{
		{JSW_UNSPEC, "JSW_UNSPEC"},
		{JSW_NONE, "JSW_NONE"},
		{JSW_CONDITIONAL, "JSW_CONDITIONAL"},
		{JSW_UNCONDITIONAL, "JSW_UNCONDITIONAL"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.wrapper, test.wrapper)
		})
	}
}

// TestJsonValueType tests the JsonValueType enum
func TestJsonValueType(t *testing.T) {
	tests := []struct {
		valueType JsonValueType
		name      string
	}{
		{JS_TYPE_ANY, "JS_TYPE_ANY"},
		{JS_TYPE_OBJECT, "JS_TYPE_OBJECT"},
		{JS_TYPE_ARRAY, "JS_TYPE_ARRAY"},
		{JS_TYPE_SCALAR, "JS_TYPE_SCALAR"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.valueType, test.valueType)
		})
	}
}

// TestJsonExprOp tests the JsonExprOp enum
func TestJsonExprOp(t *testing.T) {
	tests := []struct {
		op       JsonExprOp
		expected string
	}{
		{JSON_EXISTS_OP, "JSON_EXISTS"},
		{JSON_QUERY_OP, "JSON_QUERY"},
		{JSON_VALUE_OP, "JSON_VALUE"},
		{JSON_TABLE_OP, "JSON_TABLE"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			funcExpr := NewJsonFuncExpr(test.op, nil, nil)
			assert.Equal(t, test.expected, funcExpr.String())
		})
	}
}

// TestJsonQuotes tests the JsonQuotes enum
func TestJsonQuotes(t *testing.T) {
	tests := []struct {
		quotes JsonQuotes
		name   string
	}{
		{JS_QUOTES_UNSPEC, "JS_QUOTES_UNSPEC"},
		{JS_QUOTES_KEEP, "JS_QUOTES_KEEP"},
		{JS_QUOTES_OMIT, "JS_QUOTES_OMIT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.quotes, test.quotes)
		})
	}
}

// TestJsonTableColumnType tests the JsonTableColumnType enum
func TestJsonTableColumnType(t *testing.T) {
	tests := []struct {
		colType JsonTableColumnType
		name    string
	}{
		{JTC_FOR_ORDINALITY, "JTC_FOR_ORDINALITY"},
		{JTC_REGULAR, "JTC_REGULAR"},
		{JTC_EXISTS, "JTC_EXISTS"},
		{JTC_FORMATTED, "JTC_FORMATTED"},
		{JTC_NESTED, "JTC_NESTED"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.colType, test.colType)
		})
	}
}

// TestJsonFormat tests the JsonFormat node
func TestJsonFormat(t *testing.T) {
	t.Run("NewJsonFormat", func(t *testing.T) {
		format := NewJsonFormat(JS_FORMAT_JSON, JS_ENC_UTF8, 100)

		require.NotNil(t, format)
		assert.Equal(t, T_JsonFormat, format.Tag)
		assert.Equal(t, JS_FORMAT_JSON, format.FormatType)
		assert.Equal(t, JS_ENC_UTF8, format.Encoding)
		assert.Equal(t, 100, format.Location())
		assert.Equal(t, "JSON", format.String())
	})

	t.Run("DefaultFormat", func(t *testing.T) {
		format := NewJsonFormat(JS_FORMAT_DEFAULT, JS_ENC_DEFAULT, -1)
		assert.Equal(t, "DEFAULT", format.String())
	})

	t.Run("JSONBFormat", func(t *testing.T) {
		format := NewJsonFormat(JS_FORMAT_JSONB, JS_ENC_DEFAULT, -1)
		assert.Equal(t, "JSONB", format.String())
	})
}

// TestJsonReturning tests the JsonReturning node
func TestJsonReturning(t *testing.T) {
	t.Run("NewJsonReturning", func(t *testing.T) {
		format := NewJsonFormat(JS_FORMAT_JSON, JS_ENC_UTF8, 100)
		returning := NewJsonReturning(format, 25, 256)

		require.NotNil(t, returning)
		assert.Equal(t, T_JsonReturning, returning.Tag)
		assert.Equal(t, format, returning.Format)
		assert.Equal(t, Oid(25), returning.Typid)
		assert.Equal(t, int32(256), returning.Typmod)
		assert.Equal(t, "JsonReturning", returning.String())
	})
}

// TestJsonValueExpr tests the JsonValueExpr node
func TestJsonValueExpr(t *testing.T) {
	t.Run("NewJsonValueExpr", func(t *testing.T) {
		rawExpr := NewConst(25, Datum(0), false) // TEXT constant
		format := NewJsonFormat(JS_FORMAT_JSON, JS_ENC_UTF8, 100)
		valueExpr := NewJsonValueExpr(rawExpr, format)

		require.NotNil(t, valueExpr)
		assert.Equal(t, T_JsonValueExpr, valueExpr.Tag)
		assert.Equal(t, rawExpr, valueExpr.RawExpr)
		assert.Equal(t, format, valueExpr.Format)
		assert.True(t, valueExpr.IsExpr())
		assert.Equal(t, "JsonValueExpr", valueExpr.ExpressionType())
		assert.Equal(t, "JsonValueExpr", valueExpr.String())
	})
}

// TestJsonBehavior tests the JsonBehavior node
func TestJsonBehavior(t *testing.T) {
	t.Run("NewJsonBehavior", func(t *testing.T) {
		expr := NewConst(25, Datum(0), false) // TEXT constant
		behavior := NewJsonBehavior(JSON_BEHAVIOR_DEFAULT, expr, 200)

		require.NotNil(t, behavior)
		assert.Equal(t, T_JsonBehavior, behavior.Tag)
		assert.Equal(t, JSON_BEHAVIOR_DEFAULT, behavior.Btype)
		assert.Equal(t, expr, behavior.Expr)
		assert.Equal(t, 200, behavior.Location())
		assert.Equal(t, "DEFAULT", behavior.String())
	})
}

// TestJsonOutput tests the JsonOutput node
func TestJsonOutput(t *testing.T) {
	t.Run("NewJsonOutput", func(t *testing.T) {
		typeName := NewTypeName([]string{"text"})
		format := NewJsonFormat(JS_FORMAT_JSON, JS_ENC_UTF8, 100)
		returning := NewJsonReturning(format, 25, -1)
		output := NewJsonOutput(typeName, returning)

		require.NotNil(t, output)
		assert.Equal(t, T_JsonOutput, output.Tag)
		assert.Equal(t, typeName, output.TypeName)
		assert.Equal(t, returning, output.Returning)
		assert.Equal(t, "JsonOutput", output.String())
	})
}

// TestJsonArgument tests the JsonArgument node
func TestJsonArgument(t *testing.T) {
	t.Run("NewJsonArgument", func(t *testing.T) {
		valueExpr := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		arg := NewJsonArgument(valueExpr, "arg_name")

		require.NotNil(t, arg)
		assert.Equal(t, T_JsonArgument, arg.Tag)
		assert.Equal(t, valueExpr, arg.Val)
		assert.Equal(t, "arg_name", arg.Name)
		assert.Equal(t, "JsonArgument", arg.String())
	})
}

// TestJsonFuncExpr tests the JsonFuncExpr node
func TestJsonFuncExpr(t *testing.T) {
	t.Run("NewJsonFuncExpr", func(t *testing.T) {
		contextItem := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		pathspec := NewConst(25, Datum(0), false) // TEXT constant for JSON path
		funcExpr := NewJsonFuncExpr(JSON_EXISTS_OP, contextItem, pathspec)

		require.NotNil(t, funcExpr)
		assert.Equal(t, T_JsonFuncExpr, funcExpr.Tag)
		assert.Equal(t, JSON_EXISTS_OP, funcExpr.Op)
		assert.Equal(t, contextItem, funcExpr.ContextItem)
		assert.Equal(t, pathspec, funcExpr.Pathspec)
		assert.True(t, funcExpr.IsExpr())
		assert.Equal(t, "JsonFuncExpr", funcExpr.ExpressionType())
		assert.Equal(t, "JSON_EXISTS", funcExpr.String())
	})

	t.Run("CompleteJsonFuncExpr", func(t *testing.T) {
		contextItem := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		pathspec := NewConst(25, Datum(0), false)
		funcExpr := NewJsonFuncExpr(JSON_QUERY_OP, contextItem, pathspec)

		funcExpr.ColumnName = "json_col"
		funcExpr.Wrapper = JSW_CONDITIONAL
		funcExpr.Quotes = JS_QUOTES_KEEP
		funcExpr.SetLocation(300)

		assert.Equal(t, "json_col", funcExpr.ColumnName)
		assert.Equal(t, JSW_CONDITIONAL, funcExpr.Wrapper)
		assert.Equal(t, JS_QUOTES_KEEP, funcExpr.Quotes)
		assert.Equal(t, 300, funcExpr.Location())
		assert.Equal(t, "JSON_QUERY", funcExpr.String())
	})
}

// TestJsonTablePathSpec tests the JsonTablePathSpec node
func TestJsonTablePathSpec(t *testing.T) {
	t.Run("NewJsonTablePathSpec", func(t *testing.T) {
		pathStr := NewConst(25, Datum(0), false) // TEXT constant for JSON path
		pathSpec := NewJsonTablePathSpec(pathStr, "item_path", 400)

		require.NotNil(t, pathSpec)
		assert.Equal(t, T_JsonTablePathSpec, pathSpec.Tag)
		assert.Equal(t, pathStr, pathSpec.StringExpr)
		assert.Equal(t, "item_path", pathSpec.Name)
		assert.Equal(t, 400, pathSpec.Location())
		assert.Equal(t, "JsonTablePathSpec", pathSpec.String())
	})
}

// TestJsonTable tests the JsonTable node
func TestJsonTable(t *testing.T) {
	t.Run("NewJsonTable", func(t *testing.T) {
		contextItem := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		pathStr := NewConst(25, Datum(0), false)
		pathSpec := NewJsonTablePathSpec(pathStr, "items", 400)
		jsonTable := NewJsonTable(contextItem, pathSpec)

		require.NotNil(t, jsonTable)
		assert.Equal(t, T_JsonTable, jsonTable.Tag)
		assert.Equal(t, contextItem, jsonTable.ContextItem)
		assert.Equal(t, pathSpec, jsonTable.Pathspec)
		assert.Equal(t, "JsonTable", jsonTable.String())
	})

	t.Run("CompleteJsonTable", func(t *testing.T) {
		contextItem := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		pathSpec := NewJsonTablePathSpec(NewConst(25, Datum(0), false), "items", 400)
		jsonTable := NewJsonTable(contextItem, pathSpec)

		alias := NewAlias("jt", NewNodeList(NewConst(25, Datum(0), false)))
		onError := NewJsonBehavior(JSON_BEHAVIOR_ERROR, nil, -1)
		jsonTable.Alias = alias
		jsonTable.OnError = onError
		jsonTable.Lateral = true
		jsonTable.SetLocation(500)

		assert.Equal(t, alias, jsonTable.Alias)
		assert.Equal(t, onError, jsonTable.OnError)
		assert.True(t, jsonTable.Lateral)
		assert.Equal(t, 500, jsonTable.Location())
	})
}

// TestJsonTableColumn tests the JsonTableColumn node
func TestJsonTableColumn(t *testing.T) {
	t.Run("NewJsonTableColumn", func(t *testing.T) {
		column := NewJsonTableColumn(JTC_REGULAR, "item_id")

		require.NotNil(t, column)
		assert.Equal(t, T_JsonTableColumn, column.Tag)
		assert.Equal(t, JTC_REGULAR, column.Coltype)
		assert.Equal(t, "item_id", column.Name)
		assert.Equal(t, "JsonTableColumn", column.String())
	})

	t.Run("CompleteJsonTableColumn", func(t *testing.T) {
		column := NewJsonTableColumn(JTC_FORMATTED, "formatted_col")

		typeName := NewTypeName([]string{"text"})
		pathSpec := NewJsonTablePathSpec(NewConst(25, Datum(0), false), "name_path", 600)
		format := NewJsonFormat(JS_FORMAT_JSON, JS_ENC_UTF8, 650)
		onEmpty := NewJsonBehavior(JSON_BEHAVIOR_NULL, nil, -1)
		onError := NewJsonBehavior(JSON_BEHAVIOR_ERROR, nil, -1)

		column.TypeName = typeName
		column.Pathspec = pathSpec
		column.Format = format
		column.Wrapper = JSW_NONE
		column.Quotes = JS_QUOTES_OMIT
		column.OnEmpty = onEmpty
		column.OnError = onError
		column.SetLocation(700)

		assert.Equal(t, typeName, column.TypeName)
		assert.Equal(t, pathSpec, column.Pathspec)
		assert.Equal(t, format, column.Format)
		assert.Equal(t, JSW_NONE, column.Wrapper)
		assert.Equal(t, JS_QUOTES_OMIT, column.Quotes)
		assert.Equal(t, onEmpty, column.OnEmpty)
		assert.Equal(t, onError, column.OnError)
		assert.Equal(t, 700, column.Location())
	})
}

// TestJsonKeyValue tests the JsonKeyValue node
func TestJsonKeyValue(t *testing.T) {
	t.Run("NewJsonKeyValue", func(t *testing.T) {
		key := NewConst(25, Datum(0), false) // TEXT constant
		value := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		keyValue := NewJsonKeyValue(key, value)

		require.NotNil(t, keyValue)
		assert.Equal(t, T_JsonKeyValue, keyValue.Tag)
		assert.Equal(t, key, keyValue.Key)
		assert.Equal(t, value, keyValue.Value)
		assert.Equal(t, "JsonKeyValue", keyValue.String())
	})
}

// TestJsonParseExpr tests the JsonParseExpr node
func TestJsonParseExpr(t *testing.T) {
	t.Run("NewJsonParseExpr", func(t *testing.T) {
		expr := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		parseExpr := NewJsonParseExpr(expr, true)

		require.NotNil(t, parseExpr)
		assert.Equal(t, T_JsonParseExpr, parseExpr.Tag)
		assert.Equal(t, expr, parseExpr.Expr)
		assert.True(t, parseExpr.UniqueKeys)
		assert.True(t, parseExpr.IsExpr())
		assert.Equal(t, "JsonParseExpr", parseExpr.ExpressionType())
		assert.Equal(t, "JsonParseExpr", parseExpr.String())
	})
}

// TestJsonScalarExpr tests the JsonScalarExpr node
func TestJsonScalarExpr(t *testing.T) {
	t.Run("NewJsonScalarExpr", func(t *testing.T) {
		expr := NewConst(25, Datum(0), false) // TEXT constant
		scalarExpr := NewJsonScalarExpr(expr)

		require.NotNil(t, scalarExpr)
		assert.Equal(t, T_JsonScalarExpr, scalarExpr.Tag)
		assert.Equal(t, expr, scalarExpr.Expr)
		assert.True(t, scalarExpr.IsExpr())
		assert.Equal(t, "JsonScalarExpr", scalarExpr.ExpressionType())
		assert.Equal(t, "JsonScalarExpr", scalarExpr.String())
	})
}

// TestJsonSerializeExpr tests the JsonSerializeExpr node
func TestJsonSerializeExpr(t *testing.T) {
	t.Run("NewJsonSerializeExpr", func(t *testing.T) {
		expr := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		serializeExpr := NewJsonSerializeExpr(expr)

		require.NotNil(t, serializeExpr)
		assert.Equal(t, T_JsonSerializeExpr, serializeExpr.Tag)
		assert.Equal(t, expr, serializeExpr.Expr)
		assert.True(t, serializeExpr.IsExpr())
		assert.Equal(t, "JsonSerializeExpr", serializeExpr.ExpressionType())
		assert.Equal(t, "JsonSerializeExpr", serializeExpr.String())
	})
}

// TestJsonObjectConstructor tests the JsonObjectConstructor node
func TestJsonObjectConstructor(t *testing.T) {
	t.Run("NewJsonObjectConstructor", func(t *testing.T) {
		kv1 := NewJsonKeyValue(NewConst(25, Datum(0), false), NewJsonValueExpr(NewConst(25, Datum(0), false), nil))
		kv2 := NewJsonKeyValue(NewConst(25, Datum(0), false), NewJsonValueExpr(NewConst(25, Datum(0), false), nil))
		exprs := NewNodeList(kv1, kv2)

		objConstructor := NewJsonObjectConstructor(exprs, true, false)

		require.NotNil(t, objConstructor)
		assert.Equal(t, T_JsonObjectConstructor, objConstructor.Tag)
		assert.Equal(t, exprs, objConstructor.Exprs)
		assert.True(t, objConstructor.AbsentOnNull)
		assert.False(t, objConstructor.Unique)
		assert.True(t, objConstructor.IsExpr())
		assert.Equal(t, "JsonObjectConstructor", objConstructor.ExpressionType())
		assert.Equal(t, "JsonObjectConstructor", objConstructor.String())
	})
}

// TestJsonArrayConstructor tests the JsonArrayConstructor node
func TestJsonArrayConstructor(t *testing.T) {
	t.Run("NewJsonArrayConstructor", func(t *testing.T) {
		val1 := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		val2 := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		exprs := NewNodeList(val1, val2)

		arrayConstructor := NewJsonArrayConstructor(exprs, false)

		require.NotNil(t, arrayConstructor)
		assert.Equal(t, T_JsonArrayConstructor, arrayConstructor.Tag)
		assert.Equal(t, exprs, arrayConstructor.Exprs)
		assert.False(t, arrayConstructor.AbsentOnNull)
		assert.True(t, arrayConstructor.IsExpr())
		assert.Equal(t, "JsonArrayConstructor", arrayConstructor.ExpressionType())
		assert.Equal(t, "JsonArrayConstructor", arrayConstructor.String())
	})
}

// TestJsonArrayQueryConstructor tests the JsonArrayQueryConstructor node
func TestJsonArrayQueryConstructor(t *testing.T) {
	t.Run("NewJsonArrayQueryConstructor", func(t *testing.T) {
		query := NewSelectStmt()
		queryConstructor := NewJsonArrayQueryConstructor(query, true)

		require.NotNil(t, queryConstructor)
		assert.Equal(t, T_JsonArrayQueryConstructor, queryConstructor.Tag)
		assert.Equal(t, query, queryConstructor.Query)
		assert.True(t, queryConstructor.AbsentOnNull)
		assert.True(t, queryConstructor.IsExpr())
		assert.Equal(t, "JsonArrayQueryConstructor", queryConstructor.ExpressionType())
		assert.Equal(t, "JsonArrayQueryConstructor", queryConstructor.String())
	})
}

// TestJsonAggConstructor tests the JsonAggConstructor node
func TestJsonAggConstructor(t *testing.T) {
	t.Run("NewJsonAggConstructor", func(t *testing.T) {
		output := NewJsonOutput(nil, nil)
		aggConstructor := NewJsonAggConstructor(output)

		require.NotNil(t, aggConstructor)
		assert.Equal(t, T_JsonAggConstructor, aggConstructor.Tag)
		assert.Equal(t, output, aggConstructor.Output)
		assert.Equal(t, "JsonAggConstructor", aggConstructor.String())
	})

	t.Run("CompleteJsonAggConstructor", func(t *testing.T) {
		output := NewJsonOutput(nil, nil)
		aggConstructor := NewJsonAggConstructor(output)

		filter := NewConst(25, Datum(0), false) // TEXT constant
		orderBy := NewNodeList(NewConst(25, Datum(0), false))
		windowDef := NewWindowDef("win", -1)

		aggConstructor.AggFilter = filter
		aggConstructor.AggOrder = orderBy
		aggConstructor.Over = windowDef
		aggConstructor.SetLocation(800)

		assert.Equal(t, filter, aggConstructor.AggFilter)
		assert.Equal(t, orderBy, aggConstructor.AggOrder)
		assert.Equal(t, windowDef, aggConstructor.Over)
		assert.Equal(t, 800, aggConstructor.Location())
	})
}

// TestJsonObjectAgg tests the JsonObjectAgg node
func TestJsonObjectAgg(t *testing.T) {
	t.Run("NewJsonObjectAgg", func(t *testing.T) {
		constructor := NewJsonAggConstructor(nil)
		keyValue := NewJsonKeyValue(NewConst(25, Datum(0), false), NewJsonValueExpr(NewConst(25, Datum(0), false), nil))
		objectAgg := NewJsonObjectAgg(constructor, keyValue, true, false)

		require.NotNil(t, objectAgg)
		assert.Equal(t, T_JsonObjectAgg, objectAgg.Tag)
		assert.Equal(t, constructor, objectAgg.Constructor)
		assert.Equal(t, keyValue, objectAgg.Arg)
		assert.True(t, objectAgg.AbsentOnNull)
		assert.False(t, objectAgg.Unique)
		assert.True(t, objectAgg.IsExpr())
		assert.Equal(t, "JsonObjectAgg", objectAgg.ExpressionType())
		assert.Equal(t, "JsonObjectAgg", objectAgg.String())
	})
}

// TestJsonArrayAgg tests the JsonArrayAgg node
func TestJsonArrayAgg(t *testing.T) {
	t.Run("NewJsonArrayAgg", func(t *testing.T) {
		constructor := NewJsonAggConstructor(nil)
		arg := NewJsonValueExpr(NewConst(25, Datum(0), false), nil)
		arrayAgg := NewJsonArrayAgg(constructor, arg, false)

		require.NotNil(t, arrayAgg)
		assert.Equal(t, T_JsonArrayAgg, arrayAgg.Tag)
		assert.Equal(t, constructor, arrayAgg.Constructor)
		assert.Equal(t, arg, arrayAgg.Arg)
		assert.False(t, arrayAgg.AbsentOnNull)
		assert.True(t, arrayAgg.IsExpr())
		assert.Equal(t, "JsonArrayAgg", arrayAgg.ExpressionType())
		assert.Equal(t, "JsonArrayAgg", arrayAgg.String())
	})
}

// TestJsonNodeTags tests that all JSON nodes have correct tags
func TestJsonNodeTags(t *testing.T) {
	tests := []struct {
		node     Node
		expected NodeTag
	}{
		{NewJsonFormat(JS_FORMAT_DEFAULT, JS_ENC_DEFAULT, -1), T_JsonFormat},
		{NewJsonReturning(nil, 0, 0), T_JsonReturning},
		{NewJsonValueExpr(nil, nil), T_JsonValueExpr},
		{NewJsonBehavior(JSON_BEHAVIOR_NULL, nil, -1), T_JsonBehavior},
		{NewJsonOutput(nil, nil), T_JsonOutput},
		{NewJsonArgument(nil, ""), T_JsonArgument},
		{NewJsonFuncExpr(JSON_EXISTS_OP, nil, nil), T_JsonFuncExpr},
		{NewJsonTablePathSpec(nil, "", -1), T_JsonTablePathSpec},
		{NewJsonTable(nil, nil), T_JsonTable},
		{NewJsonTableColumn(JTC_REGULAR, ""), T_JsonTableColumn},
		{NewJsonKeyValue(nil, nil), T_JsonKeyValue},
		{NewJsonParseExpr(nil, false), T_JsonParseExpr},
		{NewJsonScalarExpr(nil), T_JsonScalarExpr},
		{NewJsonSerializeExpr(nil), T_JsonSerializeExpr},
		{NewJsonObjectConstructor(nil, false, false), T_JsonObjectConstructor},
		{NewJsonArrayConstructor(nil, false), T_JsonArrayConstructor},
		{NewJsonArrayQueryConstructor(nil, false), T_JsonArrayQueryConstructor},
		{NewJsonAggConstructor(nil), T_JsonAggConstructor},
		{NewJsonObjectAgg(nil, nil, false, false), T_JsonObjectAgg},
		{NewJsonArrayAgg(nil, nil, false), T_JsonArrayAgg},
	}

	for _, test := range tests {
		t.Run(test.expected.String(), func(t *testing.T) {
			assert.Equal(t, test.expected, test.node.NodeTag())
		})
	}
}

// TestJsonNodeInterfaces tests that expression nodes implement required interfaces
func TestJsonNodeInterfaces(t *testing.T) {
	expressionNodes := []Expr{
		NewJsonValueExpr(nil, nil),
		NewJsonFuncExpr(JSON_EXISTS_OP, nil, nil),
		NewJsonParseExpr(nil, false),
		NewJsonScalarExpr(nil),
		NewJsonSerializeExpr(nil),
		NewJsonObjectConstructor(nil, false, false),
		NewJsonArrayConstructor(nil, false),
		NewJsonArrayQueryConstructor(nil, false),
		NewJsonObjectAgg(nil, nil, false, false),
		NewJsonArrayAgg(nil, nil, false),
	}

	for _, expr := range expressionNodes {
		t.Run(expr.ExpressionType(), func(t *testing.T) {
			assert.True(t, expr.IsExpr())
			assert.NotEmpty(t, expr.ExpressionType())
		})
	}
}

// ==============================================================================
// PHASE 1G TESTS: JSON PRIMITIVE EXPRESSIONS
// ==============================================================================

// TestJsonConstructorType tests the JsonConstructorType enum
func TestJsonConstructorType(t *testing.T) {
	tests := []struct {
		constructorType JsonConstructorType
		expectedValue   int
	}{
		{JSCTOR_JSON_OBJECT, 1},
		{JSCTOR_JSON_ARRAY, 2},
		{JSCTOR_JSON_OBJECTAGG, 3},
		{JSCTOR_JSON_ARRAYAGG, 4},
		{JSCTOR_JSON_PARSE, 5},
		{JSCTOR_JSON_SCALAR, 6},
		{JSCTOR_JSON_SERIALIZE, 7},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expectedValue, int(tt.constructorType))
	}
}

// TestJsonConstructorExpr tests the JsonConstructorExpr node
func TestJsonConstructorExpr(t *testing.T) {
	t.Run("NewJsonConstructorExpr", func(t *testing.T) {
		args := NewNodeList(NewConst(23, Datum(42), false))
		funcExpr := NewConst(25, Datum(100), false)
		coercionExpr := NewConst(26, Datum(200), false)
		returning := NewJsonReturning(nil, 0, 0)

		constructorExpr := NewJsonConstructorExpr(JSCTOR_JSON_OBJECT, args, funcExpr, coercionExpr, returning, true, false, 500)

		require.NotNil(t, constructorExpr)
		assert.Equal(t, T_JsonConstructorExpr, constructorExpr.NodeTag())
		assert.Equal(t, "JsonConstructorExpr", constructorExpr.ExpressionType())
		assert.True(t, constructorExpr.IsExpr())
		assert.Equal(t, JSCTOR_JSON_OBJECT, constructorExpr.Type)
		assert.Equal(t, args, constructorExpr.Args)
		assert.Equal(t, funcExpr, constructorExpr.Func)
		assert.Equal(t, coercionExpr, constructorExpr.Coercion)
		assert.Equal(t, returning, constructorExpr.Returning)
		assert.True(t, constructorExpr.AbsentOnNull)
		assert.False(t, constructorExpr.Unique)
		assert.Equal(t, 500, constructorExpr.Location())
		assert.Contains(t, constructorExpr.String(), "JsonConstructorExpr")
	})

	t.Run("DifferentConstructorTypes", func(t *testing.T) {
		expr1 := NewJsonConstructorExpr(JSCTOR_JSON_ARRAY, nil, nil, nil, nil, false, true, 100)
		expr2 := NewJsonConstructorExpr(JSCTOR_JSON_PARSE, nil, nil, nil, nil, true, false, 200)

		assert.Equal(t, JSCTOR_JSON_ARRAY, expr1.Type)
		assert.Equal(t, JSCTOR_JSON_PARSE, expr2.Type)
		assert.False(t, expr1.AbsentOnNull)
		assert.True(t, expr1.Unique)
		assert.True(t, expr2.AbsentOnNull)
		assert.False(t, expr2.Unique)
	})
}

// TestJsonIsPredicate tests the JsonIsPredicate node
func TestJsonIsPredicate(t *testing.T) {
	t.Run("NewJsonIsPredicate", func(t *testing.T) {
		expr := NewConst(23, Datum(42), false)
		format := NewJsonFormat(JS_FORMAT_JSON, JS_ENC_UTF8, 100)

		predicate := NewJsonIsPredicate(expr, format, JS_TYPE_OBJECT, true, 600)

		require.NotNil(t, predicate)
		assert.Equal(t, T_JsonIsPredicate, predicate.NodeTag())
		assert.Equal(t, expr, predicate.Expr)
		assert.Equal(t, format, predicate.Format)
		assert.Equal(t, JS_TYPE_OBJECT, predicate.ItemType)
		assert.True(t, predicate.UniqueKeys)
		assert.Equal(t, 600, predicate.Location())
		assert.Contains(t, predicate.String(), "JsonIsPredicate")
	})

	t.Run("DifferentItemTypes", func(t *testing.T) {
		pred1 := NewJsonIsPredicate(nil, nil, JS_TYPE_ANY, false, 100)
		pred2 := NewJsonIsPredicate(nil, nil, JS_TYPE_ARRAY, true, 200)
		pred3 := NewJsonIsPredicate(nil, nil, JS_TYPE_SCALAR, false, 300)

		assert.Equal(t, JS_TYPE_ANY, pred1.ItemType)
		assert.Equal(t, JS_TYPE_ARRAY, pred2.ItemType)
		assert.Equal(t, JS_TYPE_SCALAR, pred3.ItemType)
		assert.False(t, pred1.UniqueKeys)
		assert.True(t, pred2.UniqueKeys)
		assert.False(t, pred3.UniqueKeys)
	})
}

// TestJsonExpr tests the JsonExpr node
func TestJsonExpr(t *testing.T) {
	t.Run("NewJsonExpr", func(t *testing.T) {
		formattedExpr := NewConst(23, Datum(42), false)
		format := NewJsonFormat(JS_FORMAT_JSON, JS_ENC_UTF8, 100)
		pathSpec := NewConst(25, Datum(100), false)
		returning := NewJsonReturning(format, 0, 0)
		passingNames := []string{"arg1", "arg2"}
		passingValues := NewNodeList(NewConst(26, Datum(200), false), NewConst(27, Datum(300), false))
		onEmpty := NewJsonBehavior(JSON_BEHAVIOR_NULL, nil, 150)
		onError := NewJsonBehavior(JSON_BEHAVIOR_ERROR, nil, 160)

		jsonExpr := NewJsonExpr(JSON_EXISTS_OP, "test_column", formattedExpr, format, pathSpec, returning, passingNames, passingValues, onEmpty, onError, true, false, JSW_CONDITIONAL, true, 12345, 700)

		require.NotNil(t, jsonExpr)
		assert.Equal(t, T_JsonExpr, jsonExpr.NodeTag())
		assert.Equal(t, "JsonExpr", jsonExpr.ExpressionType())
		assert.True(t, jsonExpr.IsExpr())
		assert.Equal(t, JSON_EXISTS_OP, jsonExpr.Op)
		assert.Equal(t, "test_column", jsonExpr.ColumnName)
		assert.Equal(t, formattedExpr, jsonExpr.FormattedExpr)
		assert.Equal(t, format, jsonExpr.Format)
		assert.Equal(t, pathSpec, jsonExpr.PathSpec)
		assert.Equal(t, returning, jsonExpr.Returning)
		assert.Equal(t, passingNames, jsonExpr.PassingNames)
		assert.Equal(t, passingValues, jsonExpr.PassingValues)
		assert.Equal(t, onEmpty, jsonExpr.OnEmpty)
		assert.Equal(t, onError, jsonExpr.OnError)
		assert.True(t, jsonExpr.UseIOCoercion)
		assert.False(t, jsonExpr.UseJsonCoercion)
		assert.Equal(t, JSW_CONDITIONAL, jsonExpr.Wrapper)
		assert.True(t, jsonExpr.OmitQuotes)
		assert.Equal(t, Oid(12345), jsonExpr.Collation)
		assert.Equal(t, 700, jsonExpr.Location())
		assert.Contains(t, jsonExpr.String(), "JsonExpr")
		assert.Contains(t, jsonExpr.String(), "test_column")
	})

	t.Run("DifferentOperations", func(t *testing.T) {
		expr1 := NewJsonExpr(JSON_QUERY_OP, "", nil, nil, nil, nil, nil, nil, nil, nil, false, true, JSW_NONE, false, 0, 100)
		expr2 := NewJsonExpr(JSON_VALUE_OP, "col2", nil, nil, nil, nil, nil, nil, nil, nil, true, false, JSW_UNCONDITIONAL, true, 54321, 200)

		assert.Equal(t, JSON_QUERY_OP, expr1.Op)
		assert.Equal(t, JSON_VALUE_OP, expr2.Op)
		assert.Empty(t, expr1.ColumnName)
		assert.Equal(t, "col2", expr2.ColumnName)
		assert.False(t, expr1.UseIOCoercion)
		assert.True(t, expr1.UseJsonCoercion)
		assert.True(t, expr2.UseIOCoercion)
		assert.False(t, expr2.UseJsonCoercion)
	})
}

// TestJsonTablePath tests the JsonTablePath node
func TestJsonTablePath(t *testing.T) {
	t.Run("NewJsonTablePath", func(t *testing.T) {
		value := NewConst(23, Datum(42), false)

		tablePath := NewJsonTablePath(value, "test_path", 800)

		require.NotNil(t, tablePath)
		assert.Equal(t, T_JsonTablePath, tablePath.NodeTag())
		assert.Equal(t, value, tablePath.Value)
		assert.Equal(t, "test_path", tablePath.Name)
		assert.Equal(t, 800, tablePath.Location())
		assert.Contains(t, tablePath.String(), "JsonTablePath")
		assert.Contains(t, tablePath.String(), "test_path")
	})

	t.Run("DifferentNames", func(t *testing.T) {
		path1 := NewJsonTablePath(nil, "path1", 100)
		path2 := NewJsonTablePath(nil, "another_path", 200)

		assert.Equal(t, "path1", path1.Name)
		assert.Equal(t, "another_path", path2.Name)
	})
}

// TestJsonTablePlan tests the JsonTablePlan node
func TestJsonTablePlan(t *testing.T) {
	t.Run("NewJsonTablePlan", func(t *testing.T) {
		tablePlan := NewJsonTablePlan(900)

		require.NotNil(t, tablePlan)
		assert.Equal(t, T_JsonTablePlan, tablePlan.NodeTag())
		assert.Equal(t, 900, tablePlan.Location())
		assert.Contains(t, tablePlan.String(), "JsonTablePlan")
	})
}

// TestJsonTablePathScan tests the JsonTablePathScan node
func TestJsonTablePathScan(t *testing.T) {
	t.Run("NewJsonTablePathScan", func(t *testing.T) {
		path := NewJsonTablePath(NewConst(23, Datum(42), false), "scan_path", 100)
		child := NewJsonTablePlan(200)

		pathScan := NewJsonTablePathScan(path, true, child, 5, 10, 1000)

		require.NotNil(t, pathScan)
		assert.Equal(t, T_JsonTablePathScan, pathScan.NodeTag())
		assert.Equal(t, path, pathScan.Path)
		assert.True(t, pathScan.ErrorOnError)
		assert.Equal(t, child, pathScan.Child)
		assert.Equal(t, 5, pathScan.ColMin)
		assert.Equal(t, 10, pathScan.ColMax)
		assert.Equal(t, 1000, pathScan.Location())
		assert.Contains(t, pathScan.String(), "JsonTablePathScan")
		assert.Contains(t, pathScan.String(), "5-10")
	})

	t.Run("DifferentColumnRanges", func(t *testing.T) {
		scan1 := NewJsonTablePathScan(nil, false, nil, 0, 5, 100)
		scan2 := NewJsonTablePathScan(nil, true, nil, 10, 20, 200)

		assert.Equal(t, 0, scan1.ColMin)
		assert.Equal(t, 5, scan1.ColMax)
		assert.False(t, scan1.ErrorOnError)
		assert.Equal(t, 10, scan2.ColMin)
		assert.Equal(t, 20, scan2.ColMax)
		assert.True(t, scan2.ErrorOnError)
	})
}

// TestJsonTableSiblingJoin tests the JsonTableSiblingJoin node
func TestJsonTableSiblingJoin(t *testing.T) {
	t.Run("NewJsonTableSiblingJoin", func(t *testing.T) {
		lplan := NewJsonTablePlan(100)
		rplan := NewJsonTablePlan(200)

		siblingJoin := NewJsonTableSiblingJoin(lplan, rplan, 1100)

		require.NotNil(t, siblingJoin)
		assert.Equal(t, T_JsonTableSiblingJoin, siblingJoin.NodeTag())
		assert.Equal(t, lplan, siblingJoin.Lplan)
		assert.Equal(t, rplan, siblingJoin.Rplan)
		assert.Equal(t, 1100, siblingJoin.Location())
		assert.Contains(t, siblingJoin.String(), "JsonTableSiblingJoin")
	})
}

// TestPhase1GNodeTags tests that all Phase 1G nodes have correct tags
func TestPhase1GNodeTags(t *testing.T) {
	testCases := []struct {
		node     Node
		expected NodeTag
	}{
		{NewJsonConstructorExpr(JSCTOR_JSON_OBJECT, nil, nil, nil, nil, false, false, -1), T_JsonConstructorExpr},
		{NewJsonIsPredicate(nil, nil, JS_TYPE_ANY, false, -1), T_JsonIsPredicate},
		{NewJsonExpr(JSON_EXISTS_OP, "", nil, nil, nil, nil, nil, nil, nil, nil, false, false, JSW_UNSPEC, false, 0, -1), T_JsonExpr},
		{NewJsonTablePath(nil, "", -1), T_JsonTablePath},
		{NewJsonTablePlan(-1), T_JsonTablePlan},
		{NewJsonTablePathScan(nil, false, nil, 0, 0, -1), T_JsonTablePathScan},
		{NewJsonTableSiblingJoin(nil, nil, -1), T_JsonTableSiblingJoin},
	}

	for _, tc := range testCases {
		t.Run(tc.expected.String(), func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.node.NodeTag())
		})
	}
}

// TestPhase1GExpressionNodes tests that expression nodes implement Expr interface correctly
func TestPhase1GExpressionNodes(t *testing.T) {
	expressions := []Expr{
		NewJsonConstructorExpr(JSCTOR_JSON_OBJECT, nil, nil, nil, nil, false, false, -1),
		NewJsonExpr(JSON_EXISTS_OP, "", nil, nil, nil, nil, nil, nil, nil, nil, false, false, JSW_UNSPEC, false, 0, -1),
	}

	for _, expr := range expressions {
		t.Run(expr.ExpressionType(), func(t *testing.T) {
			assert.True(t, expr.IsExpr())
			assert.NotEmpty(t, expr.ExpressionType())
		})
	}
}
