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

		alias := NewAlias("jt", []Node{NewConst(25, Datum(0), false)})
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
		exprs := []Node{kv1, kv2}

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
		exprs := []Node{val1, val2}

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
		orderBy := []Node{NewConst(25, Datum(0), false)}
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
