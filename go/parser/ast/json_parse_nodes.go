package ast

import (
	"fmt"
	"strings"
)

// JSON Parse Tree Support Nodes - Phase 1E
// This file contains all JSON-related parse tree nodes from PostgreSQL
// Based on postgres/src/include/nodes/parsenodes.h and primnodes.h

// JsonEncoding represents the JSON ENCODING clause
// Ported from postgres/src/include/nodes/primnodes.h:1636-1643
type JsonEncoding int

const (
	JS_ENC_DEFAULT JsonEncoding = iota // unspecified
	JS_ENC_UTF8
	JS_ENC_UTF16
	JS_ENC_UTF32
)

// JsonFormatType represents the JSON format types
// Ported from postgres/src/include/nodes/primnodes.h:1644-1651
type JsonFormatType int

const (
	JS_FORMAT_DEFAULT JsonFormatType = iota // unspecified
	JS_FORMAT_JSON                          // FORMAT JSON [ENCODING ...]
	JS_FORMAT_JSONB                         // implicit internal format for RETURNING jsonb
)

// JsonFormat represents a JSON FORMAT clause
// Ported from postgres/src/include/nodes/primnodes.h:1648-1655
type JsonFormat struct {
	BaseNode
	FormatType JsonFormatType // format type
	Encoding   JsonEncoding   // JSON encoding
}

// JsonReturning represents a transformed JSON RETURNING clause
// Ported from postgres/src/include/nodes/primnodes.h:1660-1668
type JsonReturning struct {
	BaseNode
	Format *JsonFormat // output JSON format
	Typid  Oid         // target type Oid
	Typmod int32       // target type modifier
}

// JsonValueExpr represents a JSON value expression (expr [FORMAT JsonFormat])
// Ported from postgres/src/include/nodes/primnodes.h:1680-1691
type JsonValueExpr struct {
	BaseNode
	RawExpr       Node        // user-specified expression
	FormattedExpr Expr        // coerced formatted expression
	Format        *JsonFormat // FORMAT clause, if specified
}

// JsonValueType represents JSON item types in IS JSON predicate
// Ported from postgres/src/include/nodes/primnodes.h:1723-1730
type JsonValueType int

const (
	JS_TYPE_ANY    JsonValueType = iota // IS JSON [VALUE]
	JS_TYPE_OBJECT                      // IS JSON OBJECT
	JS_TYPE_ARRAY                       // IS JSON ARRAY
	JS_TYPE_SCALAR                      // IS JSON SCALAR
)

// JsonWrapper represents WRAPPER clause for JSON_QUERY()
// Ported from postgres/src/include/nodes/primnodes.h:1761-1768
type JsonWrapper int

const (
	JSW_UNSPEC JsonWrapper = iota
	JSW_NONE
	JSW_CONDITIONAL
	JSW_UNCONDITIONAL
)

// JsonBehaviorType represents behavior types used in SQL/JSON ON ERROR/EMPTY clauses
// Ported from postgres/src/include/nodes/primnodes.h:1771-1783
type JsonBehaviorType int

const (
	JSON_BEHAVIOR_NULL JsonBehaviorType = iota
	JSON_BEHAVIOR_ERROR
	JSON_BEHAVIOR_EMPTY
	JSON_BEHAVIOR_TRUE
	JSON_BEHAVIOR_FALSE
	JSON_BEHAVIOR_UNKNOWN
	JSON_BEHAVIOR_EMPTY_ARRAY
	JSON_BEHAVIOR_EMPTY_OBJECT
	JSON_BEHAVIOR_DEFAULT
)

// JsonBehavior represents specifications for ON ERROR / ON EMPTY behaviors
// Ported from postgres/src/include/nodes/primnodes.h:1786-1797
type JsonBehavior struct {
	BaseNode
	Btype  JsonBehaviorType
	Expr   Node
	Coerce bool
}

// JsonExprOp represents enumeration of SQL/JSON query function types
// Ported from postgres/src/include/nodes/primnodes.h:1802-1808
type JsonExprOp int

const (
	JSON_EXISTS_OP JsonExprOp = iota // JSON_EXISTS()
	JSON_QUERY_OP                    // JSON_QUERY()
	JSON_VALUE_OP                    // JSON_VALUE()
	JSON_TABLE_OP                    // JSON_TABLE()
)

// JsonQuotes represents [KEEP|OMIT] QUOTES clause for JSON_QUERY()
// Ported from postgres/src/include/nodes/parsenodes.h:1773-1778
type JsonQuotes int

const (
	JS_QUOTES_UNSPEC JsonQuotes = iota // unspecified
	JS_QUOTES_KEEP                     // KEEP QUOTES
	JS_QUOTES_OMIT                     // OMIT QUOTES
)

// JsonOutput represents a JSON output specification
// Ported from postgres/src/include/nodes/parsenodes.h:1751-1756
type JsonOutput struct {
	BaseNode
	TypeName  *TypeName      // RETURNING type name, if specified
	Returning *JsonReturning // RETURNING FORMAT clause and type Oids
}

// JsonArgument represents an argument from JSON PASSING clause
// Ported from postgres/src/include/nodes/parsenodes.h:1762-1767
type JsonArgument struct {
	BaseNode
	Val  *JsonValueExpr // argument value expression
	Name string         // argument name
}

// JsonFuncExpr represents untransformed function expressions for SQL/JSON query functions
// Ported from postgres/src/include/nodes/parsenodes.h:1785-1800
type JsonFuncExpr struct {
	BaseNode
	Op          JsonExprOp     // expression type
	ColumnName  string         // JSON_TABLE() column name or NULL
	ContextItem *JsonValueExpr // context item expression
	Pathspec    Node           // JSON path specification expression
	Passing     *NodeList      // list of PASSING clause arguments, if any
	Output      *JsonOutput    // output clause, if specified
	OnEmpty     *JsonBehavior  // ON EMPTY behavior
	OnError     *JsonBehavior  // ON ERROR behavior
	Wrapper     JsonWrapper    // array wrapper behavior (JSON_QUERY only)
	Quotes      JsonQuotes     // omit or keep quotes? (JSON_QUERY only)
}

// JsonTablePathSpec represents untransformed specification of JSON path expression with an optional name
// Ported from postgres/src/include/nodes/parsenodes.h:1807-1815
type JsonTablePathSpec struct {
	BaseNode
	StringExpr   Node   // JSON path string expression
	Name         string // optional path name
	NameLocation int    // location of 'name'
}

// JsonTableColumnType represents enumeration of JSON_TABLE column types
// Ported from postgres/src/include/nodes/parsenodes.h:1838-1845
type JsonTableColumnType int

const (
	JTC_FOR_ORDINALITY JsonTableColumnType = iota
	JTC_REGULAR
	JTC_EXISTS
	JTC_FORMATTED
	JTC_NESTED
)

// JsonTable represents untransformed JSON_TABLE
// Ported from postgres/src/include/nodes/parsenodes.h:1821-1832
type JsonTable struct {
	BaseNode
	ContextItem *JsonValueExpr     // context item expression
	Pathspec    *JsonTablePathSpec // JSON path specification
	Passing     *NodeList          // list of PASSING clause arguments, if any
	Columns     *NodeList          // list of JsonTableColumn
	OnError     *JsonBehavior      // ON ERROR behavior
	Alias       *Alias             // table alias in FROM clause
	Lateral     bool               // does it have LATERAL prefix?
}

// JsonTableColumn represents untransformed JSON_TABLE column
// Ported from postgres/src/include/nodes/parsenodes.h:1851-1865
type JsonTableColumn struct {
	BaseNode
	Coltype  JsonTableColumnType // column type
	Name     string              // column name
	TypeName *TypeName           // column type name
	Pathspec *JsonTablePathSpec  // JSON path specification
	Format   *JsonFormat         // JSON format clause, if specified
	Wrapper  JsonWrapper         // WRAPPER behavior for formatted columns
	Quotes   JsonQuotes          // omit or keep quotes on scalar strings?
	Columns  *NodeList           // nested columns
	OnEmpty  *JsonBehavior       // ON EMPTY behavior
	OnError  *JsonBehavior       // ON ERROR behavior
}

// JsonKeyValue represents untransformed JSON object key-value pair
// Ported from postgres/src/include/nodes/parsenodes.h:1872-1877
type JsonKeyValue struct {
	BaseNode
	Key   Expr           // key expression
	Value *JsonValueExpr // JSON value expression
}

// JsonParseExpr represents untransformed JSON()
// Ported from postgres/src/include/nodes/parsenodes.h:1883-1890
type JsonParseExpr struct {
	BaseNode
	Expr       *JsonValueExpr // string expression
	Output     *JsonOutput    // RETURNING clause, if specified
	UniqueKeys bool           // WITH UNIQUE KEYS?
}

// JsonScalarExpr represents untransformed JSON_SCALAR()
// Ported from postgres/src/include/nodes/parsenodes.h:1896-1902
type JsonScalarExpr struct {
	BaseNode
	Expr   Expr        // scalar expression
	Output *JsonOutput // RETURNING clause, if specified
}

// JsonSerializeExpr represents untransformed JSON_SERIALIZE() function
// Ported from postgres/src/include/nodes/parsenodes.h:1908-1914
type JsonSerializeExpr struct {
	BaseNode
	Expr   *JsonValueExpr // json value expression
	Output *JsonOutput    // RETURNING clause, if specified
}

// JsonObjectConstructor represents untransformed JSON_OBJECT() constructor
// Ported from postgres/src/include/nodes/parsenodes.h:1920-1928
type JsonObjectConstructor struct {
	BaseNode
	Exprs        *NodeList   // list of JsonKeyValue pairs
	Output       *JsonOutput // RETURNING clause, if specified
	AbsentOnNull bool        // skip NULL values?
	Unique       bool        // check key uniqueness?
}

// JsonArrayConstructor represents untransformed JSON_ARRAY(element,...) constructor
// Ported from postgres/src/include/nodes/parsenodes.h:1934-1941
type JsonArrayConstructor struct {
	BaseNode
	Exprs        *NodeList   // list of JsonValueExpr elements
	Output       *JsonOutput // RETURNING clause, if specified
	AbsentOnNull bool        // skip NULL elements?
}

// JsonArrayQueryConstructor represents untransformed JSON_ARRAY(subquery) constructor
// Ported from postgres/src/include/nodes/parsenodes.h:1947-1955
type JsonArrayQueryConstructor struct {
	BaseNode
	Query        Node        // subquery
	Output       *JsonOutput // RETURNING clause, if specified
	Format       *JsonFormat // FORMAT clause for subquery, if specified
	AbsentOnNull bool        // skip NULL elements?
}

// JsonAggConstructor represents common fields of JSON_ARRAYAGG() and JSON_OBJECTAGG()
// Ported from postgres/src/include/nodes/parsenodes.h:1962-1970
type JsonAggConstructor struct {
	BaseNode
	Output    *JsonOutput // RETURNING clause, if any
	AggFilter Node        // FILTER clause, if any
	AggOrder  *NodeList   // ORDER BY clause, if any
	Over      *WindowDef  // OVER clause, if any
}

// JsonObjectAgg represents untransformed JSON_OBJECTAGG()
// Ported from postgres/src/include/nodes/parsenodes.h:1976-1983
type JsonObjectAgg struct {
	BaseNode
	Constructor  *JsonAggConstructor // common fields
	Arg          *JsonKeyValue       // object key-value pair
	AbsentOnNull bool                // skip NULL values?
	Unique       bool                // check key uniqueness?
}

// JsonArrayAgg represents untransformed JSON_ARRAYAGG()
// Ported from postgres/src/include/nodes/parsenodes.h:1989-1995
type JsonArrayAgg struct {
	BaseNode
	Constructor  *JsonAggConstructor // common fields
	Arg          *JsonValueExpr      // array element expression
	AbsentOnNull bool                // skip NULL elements?
}

// Implement Node interface for all JSON types

func (n *JsonFormat) node() {}
func (n *JsonFormat) String() string {
	formatStr := "DEFAULT"
	switch n.FormatType {
	case JS_FORMAT_JSON:
		formatStr = "JSON"
	case JS_FORMAT_JSONB:
		formatStr = "JSONB"
	}
	return formatStr
}

func (n *JsonReturning) node() {}
func (n *JsonReturning) String() string {
	return "JsonReturning"
}

func (n *JsonValueExpr) node() {}
func (n *JsonValueExpr) String() string {
	return "JsonValueExpr"
}

// SqlString returns the SQL representation of the JsonValueExpr.
func (n *JsonValueExpr) SqlString() string {
	var result strings.Builder

	if n.RawExpr != nil {
		result.WriteString(n.RawExpr.SqlString())
	}

	if n.Format != nil {
		result.WriteString(" FORMAT ")
		result.WriteString(n.Format.SqlString())
	}

	return result.String()
}

func (n *JsonValueExpr) IsExpr() bool           { return true }
func (n *JsonValueExpr) ExpressionType() string { return "JsonValueExpr" }

func (n *JsonBehavior) node() {}
func (n *JsonBehavior) String() string {
	behaviorStr := "NULL"
	switch n.Btype {
	case JSON_BEHAVIOR_ERROR:
		behaviorStr = "ERROR"
	case JSON_BEHAVIOR_EMPTY:
		behaviorStr = "EMPTY"
	case JSON_BEHAVIOR_TRUE:
		behaviorStr = "TRUE"
	case JSON_BEHAVIOR_FALSE:
		behaviorStr = "FALSE"
	case JSON_BEHAVIOR_UNKNOWN:
		behaviorStr = "UNKNOWN"
	case JSON_BEHAVIOR_EMPTY_ARRAY:
		behaviorStr = "EMPTY_ARRAY"
	case JSON_BEHAVIOR_EMPTY_OBJECT:
		behaviorStr = "EMPTY_OBJECT"
	case JSON_BEHAVIOR_DEFAULT:
		behaviorStr = "DEFAULT"
	}
	return behaviorStr
}

func (n *JsonOutput) node() {}
func (n *JsonOutput) String() string {
	return "JsonOutput"
}

func (n *JsonArgument) node() {}
func (n *JsonArgument) String() string {
	return "JsonArgument"
}

func (n *JsonFuncExpr) node() {}
func (n *JsonFuncExpr) String() string {
	opStr := "JSON_EXISTS"
	switch n.Op {
	case JSON_QUERY_OP:
		opStr = "JSON_QUERY"
	case JSON_VALUE_OP:
		opStr = "JSON_VALUE"
	case JSON_TABLE_OP:
		opStr = "JSON_TABLE"
	}
	return opStr
}
func (n *JsonFuncExpr) IsExpr() bool           { return true }
func (n *JsonFuncExpr) ExpressionType() string { return "JsonFuncExpr" }

func (n *JsonTablePathSpec) node() {}
func (n *JsonTablePathSpec) String() string {
	return "JsonTablePathSpec"
}

// SqlString returns the SQL representation of the JsonTablePathSpec.
func (n *JsonTablePathSpec) SqlString() string {
	if n.StringExpr != nil {
		return n.StringExpr.SqlString()
	}
	return ""
}

func (n *JsonTable) node() {}
func (n *JsonTable) String() string {
	return "JsonTable"
}

// SqlString returns the SQL representation of the JsonTable.
func (n *JsonTable) SqlString() string {
	var result strings.Builder

	if n.Lateral {
		result.WriteString("LATERAL ")
	}

	result.WriteString("JSON_TABLE(")

	// Context item expression (usually a JSON document)
	if n.ContextItem != nil {
		result.WriteString(n.ContextItem.SqlString())
	}

	// Path specification 
	if n.Pathspec != nil {
		result.WriteString(", ")
		result.WriteString(n.Pathspec.SqlString())
	}

	// PASSING clause
	if n.Passing != nil && len(n.Passing.Items) > 0 {
		result.WriteString(" PASSING ")
		for i, item := range n.Passing.Items {
			if i > 0 {
				result.WriteString(", ")
			}
			result.WriteString(item.SqlString())
		}
	}

	// COLUMNS clause
	if n.Columns != nil && len(n.Columns.Items) > 0 {
		result.WriteString(" COLUMNS (")
		for i, item := range n.Columns.Items {
			if i > 0 {
				result.WriteString(", ")
			}
			if col, ok := item.(*JsonTableColumn); ok {
				result.WriteString(col.SqlString())
			}
		}
		result.WriteString(")")
	}

	// ON ERROR clause
	if n.OnError != nil {
		result.WriteString(" ")
		result.WriteString(n.OnError.SqlString())
		result.WriteString(" ON ERROR")
	}

	result.WriteString(")")

	if n.Alias != nil {
		result.WriteString(" ")
		result.WriteString(n.Alias.SqlString())
	}

	return result.String()
}

func (n *JsonTableColumn) node() {}
func (n *JsonTableColumn) String() string {
	return "JsonTableColumn"
}

// SqlString returns the SQL representation of the JsonTableColumn.
func (n *JsonTableColumn) SqlString() string {
	var result strings.Builder

	// Column name (only for non-NESTED columns)
	if n.Coltype != JTC_NESTED {
		result.WriteString(n.Name)
	}

	switch n.Coltype {
	case JTC_FOR_ORDINALITY:
		result.WriteString(" FOR ORDINALITY")

	case JTC_REGULAR:
		if n.TypeName != nil {
			result.WriteString(" ")
			result.WriteString(n.TypeName.SqlString())
		}
		// Add PATH clause if specified
		if n.Pathspec != nil {
			result.WriteString(" PATH ")
			result.WriteString(n.Pathspec.SqlString())
		}

	case JTC_EXISTS:
		if n.TypeName != nil {
			result.WriteString(" ")
			result.WriteString(n.TypeName.SqlString())
		}
		result.WriteString(" EXISTS")
		// Add PATH clause if specified
		if n.Pathspec != nil {
			result.WriteString(" PATH ")
			result.WriteString(n.Pathspec.SqlString())
		}

	case JTC_FORMATTED:
		if n.TypeName != nil {
			result.WriteString(" ")
			result.WriteString(n.TypeName.SqlString())
		}
		// Add FORMAT clause if specified
		if n.Format != nil {
			result.WriteString(" FORMAT ")
			result.WriteString(n.Format.SqlString())
		}
		// Add PATH clause if specified
		if n.Pathspec != nil {
			result.WriteString(" PATH ")
			result.WriteString(n.Pathspec.SqlString())
		}

	case JTC_NESTED:
		result.WriteString("NESTED PATH ")
		if n.Pathspec != nil {
			result.WriteString(n.Pathspec.SqlString())
		}
		if n.Columns != nil && len(n.Columns.Items) > 0 {
			result.WriteString(" COLUMNS (")
			for i, item := range n.Columns.Items {
				if i > 0 {
					result.WriteString(", ")
				}
				if col, ok := item.(*JsonTableColumn); ok {
					result.WriteString(col.SqlString())
				}
			}
			result.WriteString(")")
		}
	}

	// Add ON EMPTY clause if specified
	if n.OnEmpty != nil {
		result.WriteString(" ")
		result.WriteString(n.OnEmpty.SqlString())
		result.WriteString(" ON EMPTY")
	}

	// Add ON ERROR clause if specified
	if n.OnError != nil {
		result.WriteString(" ")
		result.WriteString(n.OnError.SqlString())
		result.WriteString(" ON ERROR")
	}

	return result.String()
}

func (n *JsonKeyValue) node() {}
func (n *JsonKeyValue) String() string {
	return "JsonKeyValue"
}

func (n *JsonParseExpr) node() {}
func (n *JsonParseExpr) String() string {
	return "JsonParseExpr"
}
func (n *JsonParseExpr) IsExpr() bool           { return true }
func (n *JsonParseExpr) ExpressionType() string { return "JsonParseExpr" }

func (n *JsonScalarExpr) node() {}
func (n *JsonScalarExpr) String() string {
	return "JsonScalarExpr"
}
func (n *JsonScalarExpr) IsExpr() bool           { return true }
func (n *JsonScalarExpr) ExpressionType() string { return "JsonScalarExpr" }

func (n *JsonSerializeExpr) node() {}
func (n *JsonSerializeExpr) String() string {
	return "JsonSerializeExpr"
}
func (n *JsonSerializeExpr) IsExpr() bool           { return true }
func (n *JsonSerializeExpr) ExpressionType() string { return "JsonSerializeExpr" }

func (n *JsonObjectConstructor) node() {}
func (n *JsonObjectConstructor) String() string {
	return "JsonObjectConstructor"
}
func (n *JsonObjectConstructor) IsExpr() bool           { return true }
func (n *JsonObjectConstructor) ExpressionType() string { return "JsonObjectConstructor" }

func (n *JsonArrayConstructor) node() {}
func (n *JsonArrayConstructor) String() string {
	return "JsonArrayConstructor"
}
func (n *JsonArrayConstructor) IsExpr() bool           { return true }
func (n *JsonArrayConstructor) ExpressionType() string { return "JsonArrayConstructor" }

func (n *JsonArrayQueryConstructor) node() {}
func (n *JsonArrayQueryConstructor) String() string {
	return "JsonArrayQueryConstructor"
}
func (n *JsonArrayQueryConstructor) IsExpr() bool           { return true }
func (n *JsonArrayQueryConstructor) ExpressionType() string { return "JsonArrayQueryConstructor" }

func (n *JsonAggConstructor) node() {}
func (n *JsonAggConstructor) String() string {
	return "JsonAggConstructor"
}

func (n *JsonObjectAgg) node() {}
func (n *JsonObjectAgg) String() string {
	return "JsonObjectAgg"
}
func (n *JsonObjectAgg) IsExpr() bool           { return true }
func (n *JsonObjectAgg) ExpressionType() string { return "JsonObjectAgg" }

func (n *JsonArrayAgg) node() {}
func (n *JsonArrayAgg) String() string {
	return "JsonArrayAgg"
}
func (n *JsonArrayAgg) IsExpr() bool           { return true }
func (n *JsonArrayAgg) ExpressionType() string { return "JsonArrayAgg" }

// Constructor functions

// NewJsonFormat creates a new JsonFormat node
func NewJsonFormat(formatType JsonFormatType, encoding JsonEncoding, location int) *JsonFormat {
	node := &JsonFormat{
		BaseNode:   BaseNode{Tag: T_JsonFormat},
		FormatType: formatType,
		Encoding:   encoding,
	}
	node.SetLocation(location)
	return node
}

// NewJsonReturning creates a new JsonReturning node
func NewJsonReturning(format *JsonFormat, typid Oid, typmod int32) *JsonReturning {
	return &JsonReturning{
		BaseNode: BaseNode{Tag: T_JsonReturning},
		Format:   format,
		Typid:    typid,
		Typmod:   typmod,
	}
}

// NewJsonValueExpr creates a new JsonValueExpr node
func NewJsonValueExpr(rawExpr Node, format *JsonFormat) *JsonValueExpr {
	return &JsonValueExpr{
		BaseNode: BaseNode{Tag: T_JsonValueExpr},
		RawExpr:  rawExpr,
		Format:   format,
	}
}

// NewJsonBehavior creates a new JsonBehavior node
func NewJsonBehavior(btype JsonBehaviorType, expr Node, location int) *JsonBehavior {
	node := &JsonBehavior{
		BaseNode: BaseNode{Tag: T_JsonBehavior},
		Btype:    btype,
		Expr:     expr,
	}
	node.SetLocation(location)
	return node
}

// NewJsonOutput creates a new JsonOutput node
func NewJsonOutput(typeName *TypeName, returning *JsonReturning) *JsonOutput {
	return &JsonOutput{
		BaseNode:  BaseNode{Tag: T_JsonOutput},
		TypeName:  typeName,
		Returning: returning,
	}
}

// NewJsonArgument creates a new JsonArgument node
func NewJsonArgument(val *JsonValueExpr, name string) *JsonArgument {
	return &JsonArgument{
		BaseNode: BaseNode{Tag: T_JsonArgument},
		Val:      val,
		Name:     name,
	}
}

// NewJsonFuncExpr creates a new JsonFuncExpr node
func NewJsonFuncExpr(op JsonExprOp, contextItem *JsonValueExpr, pathspec Node) *JsonFuncExpr {
	return &JsonFuncExpr{
		BaseNode:    BaseNode{Tag: T_JsonFuncExpr},
		Op:          op,
		ContextItem: contextItem,
		Pathspec:    pathspec,
	}
}

// NewJsonTablePathSpec creates a new JsonTablePathSpec node
func NewJsonTablePathSpec(str Node, name string, location int) *JsonTablePathSpec {
	node := &JsonTablePathSpec{
		BaseNode:   BaseNode{Tag: T_JsonTablePathSpec},
		StringExpr: str,
		Name:       name,
	}
	node.SetLocation(location)
	return node
}

// NewJsonTable creates a new JsonTable node
func NewJsonTable(contextItem *JsonValueExpr, pathspec *JsonTablePathSpec) *JsonTable {
	return &JsonTable{
		BaseNode:    BaseNode{Tag: T_JsonTable},
		ContextItem: contextItem,
		Pathspec:    pathspec,
	}
}

// NewJsonTableColumn creates a new JsonTableColumn node
func NewJsonTableColumn(coltype JsonTableColumnType, name string) *JsonTableColumn {
	return &JsonTableColumn{
		BaseNode: BaseNode{Tag: T_JsonTableColumn},
		Coltype:  coltype,
		Name:     name,
	}
}

// NewJsonKeyValue creates a new JsonKeyValue node
func NewJsonKeyValue(key Expr, value *JsonValueExpr) *JsonKeyValue {
	return &JsonKeyValue{
		BaseNode: BaseNode{Tag: T_JsonKeyValue},
		Key:      key,
		Value:    value,
	}
}

// NewJsonParseExpr creates a new JsonParseExpr node
func NewJsonParseExpr(expr *JsonValueExpr, uniqueKeys bool) *JsonParseExpr {
	return &JsonParseExpr{
		BaseNode:   BaseNode{Tag: T_JsonParseExpr},
		Expr:       expr,
		UniqueKeys: uniqueKeys,
	}
}

// NewJsonScalarExpr creates a new JsonScalarExpr node
func NewJsonScalarExpr(expr Expr) *JsonScalarExpr {
	return &JsonScalarExpr{
		BaseNode: BaseNode{Tag: T_JsonScalarExpr},
		Expr:     expr,
	}
}

// NewJsonSerializeExpr creates a new JsonSerializeExpr node
func NewJsonSerializeExpr(expr *JsonValueExpr) *JsonSerializeExpr {
	return &JsonSerializeExpr{
		BaseNode: BaseNode{Tag: T_JsonSerializeExpr},
		Expr:     expr,
	}
}

// NewJsonObjectConstructor creates a new JsonObjectConstructor node
func NewJsonObjectConstructor(exprs *NodeList, absentOnNull bool, unique bool) *JsonObjectConstructor {
	return &JsonObjectConstructor{
		BaseNode:     BaseNode{Tag: T_JsonObjectConstructor},
		Exprs:        exprs,
		AbsentOnNull: absentOnNull,
		Unique:       unique,
	}
}

// NewJsonArrayConstructor creates a new JsonArrayConstructor node
func NewJsonArrayConstructor(exprs *NodeList, absentOnNull bool) *JsonArrayConstructor {
	return &JsonArrayConstructor{
		BaseNode:     BaseNode{Tag: T_JsonArrayConstructor},
		Exprs:        exprs,
		AbsentOnNull: absentOnNull,
	}
}

// NewJsonArrayQueryConstructor creates a new JsonArrayQueryConstructor node
func NewJsonArrayQueryConstructor(query Node, absentOnNull bool) *JsonArrayQueryConstructor {
	return &JsonArrayQueryConstructor{
		BaseNode:     BaseNode{Tag: T_JsonArrayQueryConstructor},
		Query:        query,
		AbsentOnNull: absentOnNull,
	}
}

// NewJsonAggConstructor creates a new JsonAggConstructor node
func NewJsonAggConstructor(output *JsonOutput) *JsonAggConstructor {
	return &JsonAggConstructor{
		BaseNode: BaseNode{Tag: T_JsonAggConstructor},
		Output:   output,
	}
}

// NewJsonObjectAgg creates a new JsonObjectAgg node
func NewJsonObjectAgg(constructor *JsonAggConstructor, arg *JsonKeyValue, absentOnNull bool, unique bool) *JsonObjectAgg {
	return &JsonObjectAgg{
		BaseNode:     BaseNode{Tag: T_JsonObjectAgg},
		Constructor:  constructor,
		Arg:          arg,
		AbsentOnNull: absentOnNull,
		Unique:       unique,
	}
}

// NewJsonArrayAgg creates a new JsonArrayAgg node
func NewJsonArrayAgg(constructor *JsonAggConstructor, arg *JsonValueExpr, absentOnNull bool) *JsonArrayAgg {
	return &JsonArrayAgg{
		BaseNode:     BaseNode{Tag: T_JsonArrayAgg},
		Constructor:  constructor,
		Arg:          arg,
		AbsentOnNull: absentOnNull,
	}
}

// ==============================================================================
// PHASE 1G: JSON PRIMITIVE EXPRESSIONS - Missing JSON expression nodes
// Ported from postgres/src/include/nodes/primnodes.h
// ==============================================================================

// JsonConstructorType represents JSON constructor types
// Ported from postgres/src/include/nodes/primnodes.h:1688-1697
type JsonConstructorType int

const (
	JSCTOR_JSON_OBJECT JsonConstructorType = iota + 1 // JSON_OBJECT constructor
	JSCTOR_JSON_ARRAY                                 // JSON_ARRAY constructor  
	JSCTOR_JSON_OBJECTAGG                             // JSON_OBJECTAGG constructor
	JSCTOR_JSON_ARRAYAGG                              // JSON_ARRAYAGG constructor
	JSCTOR_JSON_PARSE                                 // JSON_PARSE constructor
	JSCTOR_JSON_SCALAR                                // JSON_SCALAR constructor
	JSCTOR_JSON_SERIALIZE                             // JSON_SERIALIZE constructor
)

// JsonConstructorExpr represents a wrapper over FuncExpr/Aggref/WindowFunc for SQL/JSON constructors
// Ported from postgres/src/include/nodes/primnodes.h:1703-1714
type JsonConstructorExpr struct {
	BaseExpr
	Type         JsonConstructorType // constructor type
	Args         *NodeList           // arguments list
	Func         Expr                // underlying json[b]_xxx() function call
	Coercion     Expr                // coercion to RETURNING type
	Returning    *JsonReturning      // RETURNING clause
	AbsentOnNull bool                // ABSENT ON NULL?
	Unique       bool                // WITH UNIQUE KEYS? (JSON_OBJECT[AGG] only)
}

func (j *JsonConstructorExpr) ExpressionType() string {
	return "JsonConstructorExpr"
}

func (j *JsonConstructorExpr) String() string {
	return fmt.Sprintf("JsonConstructorExpr{type=%d}@%d", j.Type, j.Location())
}

// NewJsonConstructorExpr creates a new JsonConstructorExpr node
func NewJsonConstructorExpr(constructorType JsonConstructorType, args *NodeList, function, coercion Expr, returning *JsonReturning, absentOnNull, unique bool, location int) *JsonConstructorExpr {
	return &JsonConstructorExpr{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_JsonConstructorExpr, Loc: location}},
		Type:         constructorType,
		Args:         args,
		Func:         function,
		Coercion:     coercion,
		Returning:    returning,
		AbsentOnNull: absentOnNull,
		Unique:       unique,
	}
}

// JsonIsPredicate represents an IS JSON predicate
// Ported from postgres/src/include/nodes/primnodes.h:1732-1740
type JsonIsPredicate struct {
	BaseNode
	Expr       Node           // subject expression
	Format     *JsonFormat    // FORMAT clause, if specified
	ItemType   JsonValueType  // JSON item type
	UniqueKeys bool           // check key uniqueness?
}

func (j *JsonIsPredicate) String() string {
	return fmt.Sprintf("JsonIsPredicate{type=%d}@%d", j.ItemType, j.Location())
}

// NewJsonIsPredicate creates a new JsonIsPredicate node
func NewJsonIsPredicate(expr Node, format *JsonFormat, itemType JsonValueType, uniqueKeys bool, location int) *JsonIsPredicate {
	return &JsonIsPredicate{
		BaseNode:   BaseNode{Tag: T_JsonIsPredicate, Loc: location},
		Expr:       expr,
		Format:     format,
		ItemType:   itemType,
		UniqueKeys: uniqueKeys,
	}
}

// JsonExpr represents transformed representation of JSON_VALUE(), JSON_QUERY(), and JSON_EXISTS()
// Ported from postgres/src/include/nodes/primnodes.h:1813-1860
type JsonExpr struct {
	BaseExpr
	Op                JsonExprOp     // JSON expression operation type
	ColumnName        string         // JSON_TABLE() column name or empty if not for JSON_TABLE()
	FormattedExpr     Node           // jsonb-valued expression to query
	Format            *JsonFormat    // Format of the above expression needed by ruleutils.c
	PathSpec          Node           // jsonpath-valued expression containing the query pattern
	Returning         *JsonReturning // Expected type/format of the output
	PassingNames      []string       // PASSING argument names
	PassingValues     *NodeList      // PASSING argument values
	OnEmpty           *JsonBehavior  // User-specified or default ON EMPTY behavior
	OnError           *JsonBehavior  // User-specified or default ON ERROR behavior
	UseIOCoercion     bool           // Information about converting the result to RETURNING type
	UseJsonCoercion   bool           // Additional conversion information
	Wrapper           JsonWrapper    // WRAPPER specification for JSON_QUERY
	OmitQuotes        bool           // KEEP or OMIT QUOTES for singleton scalars returned by JSON_QUERY()
	Collation         Oid            // JsonExpr's collation
}

func (j *JsonExpr) ExpressionType() string {
	return "JsonExpr"
}

func (j *JsonExpr) String() string {
	return fmt.Sprintf("JsonExpr{op=%d, col=%s}@%d", j.Op, j.ColumnName, j.Location())
}

// NewJsonExpr creates a new JsonExpr node
func NewJsonExpr(op JsonExprOp, columnName string, formattedExpr Node, format *JsonFormat, pathSpec Node, returning *JsonReturning, passingNames []string, passingValues *NodeList, onEmpty, onError *JsonBehavior, useIOCoercion, useJsonCoercion bool, wrapper JsonWrapper, omitQuotes bool, collation Oid, location int) *JsonExpr {
	return &JsonExpr{
		BaseExpr:        BaseExpr{BaseNode: BaseNode{Tag: T_JsonExpr, Loc: location}},
		Op:              op,
		ColumnName:      columnName,
		FormattedExpr:   formattedExpr,
		Format:          format,
		PathSpec:        pathSpec,
		Returning:       returning,
		PassingNames:    passingNames,
		PassingValues:   passingValues,
		OnEmpty:         onEmpty,
		OnError:         onError,
		UseIOCoercion:   useIOCoercion,
		UseJsonCoercion: useJsonCoercion,
		Wrapper:         wrapper,
		OmitQuotes:      omitQuotes,
		Collation:       collation,
	}
}

// JsonTablePath represents a JSON path expression to be computed as part of evaluating a JSON_TABLE plan node
// Ported from postgres/src/include/nodes/primnodes.h:1867-1873
type JsonTablePath struct {
	BaseNode
	Value *Const // path value
	Name  string // path name
}

func (j *JsonTablePath) String() string {
	return fmt.Sprintf("JsonTablePath{name=%s}@%d", j.Name, j.Location())
}

// NewJsonTablePath creates a new JsonTablePath node
func NewJsonTablePath(value *Const, name string, location int) *JsonTablePath {
	return &JsonTablePath{
		BaseNode: BaseNode{Tag: T_JsonTablePath, Loc: location},
		Value:    value,
		Name:     name,
	}
}

// JsonTablePlan represents abstract base type for different types of JSON_TABLE "plans"
// Ported from postgres/src/include/nodes/primnodes.h:1882-1887
type JsonTablePlan struct {
	BaseNode
}

func (j *JsonTablePlan) String() string {
	return fmt.Sprintf("JsonTablePlan@%d", j.Location())
}

// NewJsonTablePlan creates a new JsonTablePlan node
func NewJsonTablePlan(location int) *JsonTablePlan {
	return &JsonTablePlan{
		BaseNode: BaseNode{Tag: T_JsonTablePlan, Loc: location},
	}
}

// JsonTablePathScan represents a JSON_TABLE plan to evaluate a JSON path expression and NESTED paths
// Ported from postgres/src/include/nodes/primnodes.h:1893-1916
type JsonTablePathScan struct {
	BaseNode
	Path        *JsonTablePath // JSON path to evaluate
	ErrorOnError bool          // ERROR/EMPTY ON ERROR behavior; only significant in the plan for the top-level path
	Child       *JsonTablePlan // Plan(s) for nested columns, if any
	ColMin      int            // 0-based index in TableFunc.colvalexprs of the 1st column covered by this plan
	ColMax      int            // 0-based index in TableFunc.colvalexprs of the last column covered by this plan
}

func (j *JsonTablePathScan) String() string {
	return fmt.Sprintf("JsonTablePathScan{cols=%d-%d}@%d", j.ColMin, j.ColMax, j.Location())
}

// NewJsonTablePathScan creates a new JsonTablePathScan node
func NewJsonTablePathScan(path *JsonTablePath, errorOnError bool, child *JsonTablePlan, colMin, colMax, location int) *JsonTablePathScan {
	return &JsonTablePathScan{
		BaseNode:     BaseNode{Tag: T_JsonTablePathScan, Loc: location},
		Path:         path,
		ErrorOnError: errorOnError,
		Child:        child,
		ColMin:       colMin,
		ColMax:       colMax,
	}
}

// JsonTableSiblingJoin represents a plan to join rows of sibling NESTED COLUMNS clauses in the same parent COLUMNS clause
// Ported from postgres/src/include/nodes/primnodes.h:1923-1929
type JsonTableSiblingJoin struct {
	BaseNode
	Lplan *JsonTablePlan // left plan
	Rplan *JsonTablePlan // right plan
}

func (j *JsonTableSiblingJoin) String() string {
	return fmt.Sprintf("JsonTableSiblingJoin@%d", j.Location())
}

// NewJsonTableSiblingJoin creates a new JsonTableSiblingJoin node
func NewJsonTableSiblingJoin(lplan, rplan *JsonTablePlan, location int) *JsonTableSiblingJoin {
	return &JsonTableSiblingJoin{
		BaseNode: BaseNode{Tag: T_JsonTableSiblingJoin, Loc: location},
		Lplan:    lplan,
		Rplan:    rplan,
	}
}
