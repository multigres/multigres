// Package ast provides PostgreSQL DDL creation statement node definitions.
// Ported from postgres/src/include/nodes/parsenodes.h
package ast

import (
	"fmt"
	"strings"
)

// ==============================================================================
// DDL CREATION STATEMENTS - PostgreSQL parsenodes.h DDL creation implementation
// Ported from postgres/src/include/nodes/parsenodes.h
// ==============================================================================

// CoercionContext represents the context for type coercion operations
// Ported from postgres/src/include/nodes/primnodes.h:712-718
type CoercionContext int

const (
	COERCION_IMPLICIT   CoercionContext = iota // coercion in context of expression
	COERCION_ASSIGNMENT                        // coercion in context of assignment
	COERCION_PLPGSQL                           // if no assignment cast, use CoerceViaIO
	COERCION_EXPLICIT                          // explicit cast operation
)

// String returns string representation of CoercionContext
func (cc CoercionContext) String() string {
	switch cc {
	case COERCION_IMPLICIT:
		return "IMPLICIT"
	case COERCION_ASSIGNMENT:
		return "ASSIGNMENT"
	case COERCION_PLPGSQL:
		return "PLPGSQL"
	case COERCION_EXPLICIT:
		return "EXPLICIT"
	default:
		return fmt.Sprintf("CoercionContext(%d)", int(cc))
	}
}

// FunctionParameterMode represents parameter passing modes for function parameters
// Ported from postgres/src/include/nodes/parsenodes.h:3439-3449
type FunctionParameterMode int

const (
	FUNC_PARAM_IN       FunctionParameterMode = iota // input only
	FUNC_PARAM_OUT                                   // output only
	FUNC_PARAM_INOUT                                 // both
	FUNC_PARAM_VARIADIC                              // variadic (always input)
	FUNC_PARAM_TABLE                                 // table function output column
	FUNC_PARAM_DEFAULT                               // default; effectively same as IN
)

// String returns string representation of FunctionParameterMode
func (fpm FunctionParameterMode) String() string {
	switch fpm {
	case FUNC_PARAM_IN:
		return "IN"
	case FUNC_PARAM_OUT:
		return "OUT"
	case FUNC_PARAM_INOUT:
		return "INOUT"
	case FUNC_PARAM_VARIADIC:
		return "VARIADIC"
	case FUNC_PARAM_TABLE:
		return "TABLE"
	case FUNC_PARAM_DEFAULT:
		return "DEFAULT"
	default:
		return fmt.Sprintf("FunctionParameterMode(%d)", int(fpm))
	}
}

// FetchDirection represents the direction for cursor fetch operations
// Ported from postgres/src/include/nodes/parsenodes.h:3316-3324
type FetchDirection int

const (
	FETCH_FORWARD  FetchDirection = iota // forward direction, howMany is row count
	FETCH_BACKWARD                       // backward direction, howMany is row count
	FETCH_ABSOLUTE                       // absolute position, howMany is position
	FETCH_RELATIVE                       // relative position, howMany is offset
)

// String returns string representation of FetchDirection
func (fd FetchDirection) String() string {
	switch fd {
	case FETCH_FORWARD:
		return "FORWARD"
	case FETCH_BACKWARD:
		return "BACKWARD"
	case FETCH_ABSOLUTE:
		return "ABSOLUTE"
	case FETCH_RELATIVE:
		return "RELATIVE"
	default:
		return fmt.Sprintf("FetchDirection(%d)", int(fd))
	}
}

// FunctionParameter represents a function parameter specification
// Ported from postgres/src/include/nodes/parsenodes.h:3451-3458
type FunctionParameter struct {
	BaseNode
	Name    string                // parameter name, or empty string if not given
	ArgType *TypeName             // type name for parameter type
	Mode    FunctionParameterMode // IN/OUT/INOUT/VARIADIC/TABLE/DEFAULT
	DefExpr Node                  // raw default expr, or nil if not given
}

// String returns string representation of FunctionParameter
func (fp *FunctionParameter) String() string {
	var parts []string

	if fp.Mode != FUNC_PARAM_IN && fp.Mode != FUNC_PARAM_DEFAULT {
		parts = append(parts, fp.Mode.String())
	}

	if fp.Name != "" {
		parts = append(parts, fp.Name)
	}

	if fp.ArgType != nil {
		parts = append(parts, fp.ArgType.String())
	}

	if fp.DefExpr != nil {
		parts = append(parts, "DEFAULT", fp.DefExpr.String())
	}

	return strings.Join(parts, " ")
}

// SqlString returns the SQL representation of FunctionParameter
func (fp *FunctionParameter) SqlString() string {
	var parts []string

	// Parameter mode
	switch fp.Mode {
	case FUNC_PARAM_IN:
		// IN is default, don't need to specify
	case FUNC_PARAM_OUT:
		parts = append(parts, "OUT")
	case FUNC_PARAM_INOUT:
		parts = append(parts, "INOUT")
	case FUNC_PARAM_VARIADIC:
		parts = append(parts, "VARIADIC")
	}

	// Parameter name
	if fp.Name != "" {
		parts = append(parts, fp.Name)
	}

	// Parameter type
	if fp.ArgType != nil {
		parts = append(parts, fp.ArgType.SqlString())
	}

	// Default value
	if fp.DefExpr != nil {
		parts = append(parts, "DEFAULT", fp.DefExpr.SqlString())
	}

	return strings.Join(parts, " ")
}

// NewFunctionParameter creates a new FunctionParameter node
func NewFunctionParameter(name string, argType *TypeName, mode FunctionParameterMode, defExpr Node) *FunctionParameter {
	return &FunctionParameter{
		BaseNode: BaseNode{Tag: T_FunctionParameter},
		Name:     name,
		ArgType:  argType,
		Mode:     mode,
		DefExpr:  defExpr,
	}
}

// CreateFunctionStmt represents a CREATE FUNCTION statement
// Ported from postgres/src/include/nodes/parsenodes.h:3427-3437
type CreateFunctionStmt struct {
	IsProcedure bool      // true for CREATE PROCEDURE
	Replace     bool      // true for CREATE OR REPLACE
	FuncName    *NodeList // qualified name of function to create
	Parameters  *NodeList // list of function parameters
	ReturnType  *TypeName // the return type
	Options     *NodeList // list of definition elements
	SQLBody     Node      // SQL body for SQL functions
}

// node implements the Node interface
func (cfs *CreateFunctionStmt) node() {}

// stmt implements the Stmt interface
func (cfs *CreateFunctionStmt) stmt() {}

// Location returns the statement's source location (dummy implementation)
func (cfs *CreateFunctionStmt) Location() int {
	return 0 // TODO: Implement proper location tracking
}

// NodeTag returns the node's type tag
func (cfs *CreateFunctionStmt) NodeTag() NodeTag {
	return T_CreateFunctionStmt
}

// StatementType returns the statement type for this node
func (cfs *CreateFunctionStmt) StatementType() string {
	if cfs.IsProcedure {
		return "CREATE PROCEDURE"
	}
	return "CREATE FUNCTION"
}

// SqlString returns SQL representation of the CREATE FUNCTION statement
func (cfs *CreateFunctionStmt) SqlString() string {
	var parts []string

	// CREATE [OR REPLACE]
	parts = append(parts, "CREATE")
	if cfs.Replace {
		parts = append(parts, "OR REPLACE")
	}

	// FUNCTION or PROCEDURE
	if cfs.IsProcedure {
		parts = append(parts, "PROCEDURE")
	} else {
		parts = append(parts, "FUNCTION")
	}

	// Function name
	if cfs.FuncName != nil && cfs.FuncName.Len() > 0 {
		var nameParts []string
		for _, item := range cfs.FuncName.Items {
			if name, ok := item.(*String); ok {
				nameParts = append(nameParts, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameParts, "."))
	}

	// Parameters
	var paramStrs []string
	if cfs.Parameters != nil {
		for _, item := range cfs.Parameters.Items {
			if param, ok := item.(*FunctionParameter); ok {
				paramStrs = append(paramStrs, param.SqlString())
			}
		}
	}
	paramClause := "(" + strings.Join(paramStrs, ", ") + ")"
	parts = append(parts, paramClause)

	// RETURNS type (for functions, not procedures)
	if !cfs.IsProcedure && cfs.ReturnType != nil {
		parts = append(parts, "RETURNS", cfs.ReturnType.SqlString())
	}

	// Function options
	var asClause string
	if cfs.Options != nil {
		for _, item := range cfs.Options.Items {
			if option, ok := item.(*DefElem); ok {
				if option.Defname == "language" {
					if str, ok := option.Arg.(*String); ok {
						parts = append(parts, "LANGUAGE", str.SVal)
					}
				} else if option.Defname == "as" {
					if str, ok := option.Arg.(*String); ok {
						// Wrap the AS clause content in $$ delimiters for proper SQL formatting
						asClause = "$$" + str.SVal + "$$"
					} else if list, ok := option.Arg.(*NodeList); ok {
						// The AS clause might be in a NodeList
						if len(list.Items) > 0 {
							if str, ok := list.Items[0].(*String); ok {
								// Wrap the AS clause content in $$ delimiters for proper SQL formatting
								asClause = "$$" + str.SVal + "$$"
							}
						}
					}
				}
				// Add other options as needed
			}
		}
	}

	// Add AS clause if we found it
	if asClause != "" {
		parts = append(parts, "AS", asClause)
	}

	// SQL body
	if cfs.SQLBody != nil {
		parts = append(parts, "AS", cfs.SQLBody.SqlString())
	}

	return strings.Join(parts, " ")
}

// String returns string representation of CreateFunctionStmt
func (cfs *CreateFunctionStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE")
	if cfs.Replace {
		parts = append(parts, "OR REPLACE")
	}

	if cfs.IsProcedure {
		parts = append(parts, "PROCEDURE")
	} else {
		parts = append(parts, "FUNCTION")
	}

	if cfs.FuncName != nil && cfs.FuncName.Len() > 0 {
		var nameStrs []string
		for _, item := range cfs.FuncName.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	// Parameters
	if cfs.Parameters != nil && cfs.Parameters.Len() > 0 {
		var paramStrs []string
		for _, item := range cfs.Parameters.Items {
			if param, ok := item.(*FunctionParameter); ok {
				paramStrs = append(paramStrs, param.String())
			}
		}
		parts = append(parts, "("+strings.Join(paramStrs, ", ")+")")
	} else {
		parts = append(parts, "()")
	}

	// Return type (only for functions, not procedures)
	if !cfs.IsProcedure && cfs.ReturnType != nil {
		if cfs.ReturnType.Names != nil && cfs.ReturnType.Names.Len() > 0 {
			lastItem := cfs.ReturnType.Names.Items[cfs.ReturnType.Names.Len()-1]
			if str, ok := lastItem.(*String); ok {
				parts = append(parts, "RETURNS", str.SVal)
			}
		}
	}

	return strings.Join(parts, " ")
}

// NewCreateFunctionStmt creates a new CreateFunctionStmt node
func NewCreateFunctionStmt(isProcedure, replace bool, funcName *NodeList, parameters *NodeList, returnType *TypeName, options *NodeList, sqlBody Node) *CreateFunctionStmt {
	return &CreateFunctionStmt{
		IsProcedure: isProcedure,
		Replace:     replace,
		FuncName:    funcName,
		Parameters:  parameters,
		ReturnType:  returnType,
		Options:     options,
		SQLBody:     sqlBody,
	}
}

// CreateSeqStmt represents a CREATE SEQUENCE statement
// Ported from postgres/src/include/nodes/parsenodes.h:3117-3125
type CreateSeqStmt struct {
	Sequence    *RangeVar  // the sequence to create
	Options     []*DefElem // list of options
	OwnerID     Oid        // ID of owner, or 0 for default (InvalidOid)
	ForIdentity bool       // true if for IDENTITY column
	IfNotExists bool       // true for IF NOT EXISTS
}

// node implements the Node interface
func (css *CreateSeqStmt) node() {}

// stmt implements the Stmt interface
func (css *CreateSeqStmt) stmt() {}

// String returns string representation of CreateSeqStmt
func (css *CreateSeqStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE SEQUENCE")

	if css.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	if css.Sequence != nil {
		parts = append(parts, css.Sequence.String())
	}

	return strings.Join(parts, " ")
}

// NewCreateSeqStmt creates a new CreateSeqStmt node
func NewCreateSeqStmt(sequence *RangeVar, options []*DefElem, ownerID Oid, forIdentity, ifNotExists bool) *CreateSeqStmt {
	return &CreateSeqStmt{
		Sequence:    sequence,
		Options:     options,
		OwnerID:     ownerID,
		ForIdentity: forIdentity,
		IfNotExists: ifNotExists,
	}
}

// CreateOpClassItem represents an item in a CREATE OPERATOR CLASS statement
// Ported from postgres/src/include/nodes/parsenodes.h:3184-3195
type CreateOpClassItem struct {
	ItemType    int             // OPCLASS_ITEM_OPERATOR, OPCLASS_ITEM_FUNCTION, or OPCLASS_ITEM_STORAGETYPE
	Name        *ObjectWithArgs // operator or function name and args
	Number      int             // strategy num or support proc num
	OrderFamily []*String       // only used for ordering operators
	ClassArgs   []*TypeName     // amproclefttype/amprocrighttype or amoplefttype/amoprighttype
	StoredType  *TypeName       // datatype stored in index (for storage type items)
}

// node implements the Node interface
func (oci *CreateOpClassItem) node() {}

// String returns string representation of CreateOpClassItem
func (oci *CreateOpClassItem) String() string {
	var parts []string

	switch oci.ItemType {
	case 1: // OPCLASS_ITEM_OPERATOR
		parts = append(parts, "OPERATOR")
	case 2: // OPCLASS_ITEM_FUNCTION
		parts = append(parts, "FUNCTION")
	case 3: // OPCLASS_ITEM_STORAGETYPE
		parts = append(parts, "STORAGE")
	}

	if oci.Number > 0 {
		parts = append(parts, fmt.Sprintf("%d", oci.Number))
	}

	if oci.Name != nil {
		parts = append(parts, oci.Name.String())
	}

	if oci.StoredType != nil {
		parts = append(parts, oci.StoredType.String())
	}

	return strings.Join(parts, " ")
}

// NewCreateOpClassItem creates a new CreateOpClassItem node
func NewCreateOpClassItem(itemType int, name *ObjectWithArgs, number int, orderFamily []*String, classArgs []*TypeName, storedType *TypeName) *CreateOpClassItem {
	return &CreateOpClassItem{
		ItemType:    itemType,
		Name:        name,
		Number:      number,
		OrderFamily: orderFamily,
		ClassArgs:   classArgs,
		StoredType:  storedType,
	}
}

// CreateOpClassStmt represents a CREATE OPERATOR CLASS statement
// Ported from postgres/src/include/nodes/parsenodes.h:3169-3178
type CreateOpClassStmt struct {
	OpClassName  []*String            // qualified name (list of String)
	OpFamilyName []*String            // qualified name (list of String); nil if omitted
	AmName       string               // name of index AM opclass is for
	DataType     *TypeName            // datatype of indexed column
	Items        []*CreateOpClassItem // list of CreateOpClassItem nodes
	IsDefault    bool                 // should be marked as default for type?
}

// node implements the Node interface
func (cocs *CreateOpClassStmt) node() {}

// stmt implements the Stmt interface
func (cocs *CreateOpClassStmt) stmt() {}

// String returns string representation of CreateOpClassStmt
func (cocs *CreateOpClassStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE OPERATOR CLASS")

	if len(cocs.OpClassName) > 0 {
		var nameStrs []string
		for _, name := range cocs.OpClassName {
			nameStrs = append(nameStrs, name.SVal)
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	if cocs.IsDefault {
		parts = append(parts, "DEFAULT")
	}

	parts = append(parts, "FOR TYPE")
	if cocs.DataType != nil && cocs.DataType.Names != nil && cocs.DataType.Names.Len() > 0 {
		lastItem := cocs.DataType.Names.Items[cocs.DataType.Names.Len()-1]
		if str, ok := lastItem.(*String); ok {
			parts = append(parts, str.SVal)
		}
	}

	parts = append(parts, "USING", cocs.AmName)

	if len(cocs.OpFamilyName) > 0 {
		var familyStrs []string
		for _, name := range cocs.OpFamilyName {
			familyStrs = append(familyStrs, name.SVal)
		}
		parts = append(parts, "FAMILY", strings.Join(familyStrs, "."))
	}

	return strings.Join(parts, " ")
}

// NewCreateOpClassStmt creates a new CreateOpClassStmt node
func NewCreateOpClassStmt(opClassName, opFamilyName []*String, amName string, dataType *TypeName, items []*CreateOpClassItem, isDefault bool) *CreateOpClassStmt {
	return &CreateOpClassStmt{
		OpClassName:  opClassName,
		OpFamilyName: opFamilyName,
		AmName:       amName,
		DataType:     dataType,
		Items:        items,
		IsDefault:    isDefault,
	}
}

// CreateOpFamilyStmt represents a CREATE OPERATOR FAMILY statement
// Ported from postgres/src/include/nodes/parsenodes.h:3201-3206
type CreateOpFamilyStmt struct {
	OpFamilyName []*String // qualified name (list of String)
	AmName       string    // name of index AM opfamily is for
}

// node implements the Node interface
func (cofs *CreateOpFamilyStmt) node() {}

// stmt implements the Stmt interface
func (cofs *CreateOpFamilyStmt) stmt() {}

// String returns string representation of CreateOpFamilyStmt
func (cofs *CreateOpFamilyStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE OPERATOR FAMILY")

	if len(cofs.OpFamilyName) > 0 {
		var nameStrs []string
		for _, name := range cofs.OpFamilyName {
			nameStrs = append(nameStrs, name.SVal)
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	parts = append(parts, "USING", cofs.AmName)

	return strings.Join(parts, " ")
}

// NewCreateOpFamilyStmt creates a new CreateOpFamilyStmt node
func NewCreateOpFamilyStmt(opFamilyName []*String, amName string) *CreateOpFamilyStmt {
	return &CreateOpFamilyStmt{
		OpFamilyName: opFamilyName,
		AmName:       amName,
	}
}

// CreateCastStmt represents a CREATE CAST statement
// Ported from postgres/src/include/nodes/parsenodes.h:4002-4010
type CreateCastStmt struct {
	SourceType *TypeName       // source data type
	TargetType *TypeName       // target data type
	Func       *ObjectWithArgs // conversion function, or nil
	Context    CoercionContext // coercion context
	Inout      bool            // true for INOUT cast
}

// node implements the Node interface
func (ccs *CreateCastStmt) node() {}

// stmt implements the Stmt interface
func (ccs *CreateCastStmt) stmt() {}

// String returns string representation of CreateCastStmt
func (ccs *CreateCastStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE CAST")

	if ccs.SourceType != nil && ccs.TargetType != nil {
		sourceTypeName := ""
		if ccs.SourceType.Names != nil && ccs.SourceType.Names.Len() > 0 {
			lastItem := ccs.SourceType.Names.Items[ccs.SourceType.Names.Len()-1]
			if str, ok := lastItem.(*String); ok {
				sourceTypeName = str.SVal
			}
		}
		targetTypeName := ""
		if ccs.TargetType.Names != nil && ccs.TargetType.Names.Len() > 0 {
			lastItem := ccs.TargetType.Names.Items[ccs.TargetType.Names.Len()-1]
			if str, ok := lastItem.(*String); ok {
				targetTypeName = str.SVal
			}
		}
		parts = append(parts, "("+sourceTypeName+" AS "+targetTypeName+")")
	}

	if ccs.Func != nil {
		funcName := ""
		if ccs.Func.Objname != nil && len(ccs.Func.Objname.Items) > 0 {
			if str, ok := ccs.Func.Objname.Items[len(ccs.Func.Objname.Items)-1].(*String); ok {
				funcName = str.SVal
			}
		}
		parts = append(parts, "WITH FUNCTION", funcName)
	} else if ccs.Inout {
		parts = append(parts, "WITH INOUT")
	} else {
		parts = append(parts, "WITHOUT FUNCTION")
	}

	return strings.Join(parts, " ")
}

// NewCreateCastStmt creates a new CreateCastStmt node
func NewCreateCastStmt(sourceType, targetType *TypeName, function *ObjectWithArgs, context CoercionContext, inout bool) *CreateCastStmt {
	return &CreateCastStmt{
		SourceType: sourceType,
		TargetType: targetType,
		Func:       function,
		Context:    context,
		Inout:      inout,
	}
}

// CreateConversionStmt represents a CREATE CONVERSION statement
// Ported from postgres/src/include/nodes/parsenodes.h:3988-3996
type CreateConversionStmt struct {
	ConversionName  []*String // name of the conversion
	ForEncodingName string    // source encoding name
	ToEncodingName  string    // destination encoding name
	FuncName        []*String // qualified conversion function name
	Def             bool      // true if this is a default conversion
}

// node implements the Node interface
func (ccs *CreateConversionStmt) node() {}

// stmt implements the Stmt interface
func (ccs *CreateConversionStmt) stmt() {}

// String returns string representation of CreateConversionStmt
func (ccs *CreateConversionStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE")
	if ccs.Def {
		parts = append(parts, "DEFAULT")
	}
	parts = append(parts, "CONVERSION")

	if len(ccs.ConversionName) > 0 {
		var nameStrs []string
		for _, name := range ccs.ConversionName {
			nameStrs = append(nameStrs, name.SVal)
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	parts = append(parts, "FOR", ccs.ForEncodingName, "TO", ccs.ToEncodingName)

	if len(ccs.FuncName) > 0 {
		var funcStrs []string
		for _, name := range ccs.FuncName {
			funcStrs = append(funcStrs, name.SVal)
		}
		parts = append(parts, "FROM", strings.Join(funcStrs, "."))
	}

	return strings.Join(parts, " ")
}

// NewCreateConversionStmt creates a new CreateConversionStmt node
func NewCreateConversionStmt(conversionName []*String, forEncodingName, toEncodingName string, funcName []*String, def bool) *CreateConversionStmt {
	return &CreateConversionStmt{
		ConversionName:  conversionName,
		ForEncodingName: forEncodingName,
		ToEncodingName:  toEncodingName,
		FuncName:        funcName,
		Def:             def,
	}
}

// CreateTransformStmt represents a CREATE TRANSFORM statement
// Ported from postgres/src/include/nodes/parsenodes.h:4016-4024
type CreateTransformStmt struct {
	Replace  bool            // true for CREATE OR REPLACE
	TypeName *TypeName       // type name
	Lang     string          // language name
	FromSql  *ObjectWithArgs // FROM SQL function
	ToSql    *ObjectWithArgs // TO SQL function
}

// node implements the Node interface
func (cts *CreateTransformStmt) node() {}

// stmt implements the Stmt interface
func (cts *CreateTransformStmt) stmt() {}

// String returns string representation of CreateTransformStmt
func (cts *CreateTransformStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE")
	if cts.Replace {
		parts = append(parts, "OR REPLACE")
	}
	parts = append(parts, "TRANSFORM FOR")

	if cts.TypeName != nil {
		parts = append(parts, cts.TypeName.String())
	}

	parts = append(parts, "LANGUAGE", cts.Lang)

	return strings.Join(parts, " ")
}

// NewCreateTransformStmt creates a new CreateTransformStmt node
func NewCreateTransformStmt(replace bool, typeName *TypeName, lang string, fromSql, toSql *ObjectWithArgs) *CreateTransformStmt {
	return &CreateTransformStmt{
		Replace:  replace,
		TypeName: typeName,
		Lang:     lang,
		FromSql:  fromSql,
		ToSql:    toSql,
	}
}

// DefineStmt represents a CREATE {AGGREGATE|OPERATOR|TYPE} statement
// Ported from postgres/src/include/nodes/parsenodes.h:3140-3150
type DefineStmt struct {
	BaseNode
	Kind        ObjectType // aggregate, operator, type
	OldStyle    bool       // hack to signal old CREATE AGG syntax
	DefNames    *NodeList  // qualified name (list of String)
	Args        *NodeList  // list of TypeName (if needed)
	Definition  *NodeList  // list of DefElem
	IfNotExists bool       // true for IF NOT EXISTS
	Replace     bool       // true for CREATE OR REPLACE
}

// node implements the Node interface
func (ds *DefineStmt) node() {}

// stmt implements the Stmt interface
func (ds *DefineStmt) stmt() {}

// StatementType returns the statement type
func (ds *DefineStmt) StatementType() string {
	return "DefineStmt"
}

// String returns string representation of DefineStmt
func (ds *DefineStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE")
	if ds.Replace {
		parts = append(parts, "OR REPLACE")
	}

	switch ds.Kind {
	case OBJECT_AGGREGATE:
		parts = append(parts, "AGGREGATE")
	case OBJECT_OPERATOR:
		parts = append(parts, "OPERATOR")
	case OBJECT_TYPE:
		parts = append(parts, "TYPE")
	case OBJECT_TSPARSER:
		parts = append(parts, "TEXT SEARCH PARSER")
	case OBJECT_TSDICTIONARY:
		parts = append(parts, "TEXT SEARCH DICTIONARY")
	case OBJECT_TSTEMPLATE:
		parts = append(parts, "TEXT SEARCH TEMPLATE")
	case OBJECT_TSCONFIGURATION:
		parts = append(parts, "TEXT SEARCH CONFIGURATION")
	case OBJECT_COLLATION:
		parts = append(parts, "COLLATION")
	default:
		parts = append(parts, "OBJECT")
	}

	if ds.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	if ds.DefNames != nil && len(ds.DefNames.Items) > 0 {
		var nameStrs []string
		for _, item := range ds.DefNames.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	// Add arguments for aggregates and operators
	if ds.Args != nil && len(ds.Args.Items) > 0 {
		var argStrs []string
		hasStarArg := false
		for _, item := range ds.Args.Items {
			// Check for nil which represents * in aggregates like COUNT(*)
			if item == nil {
				hasStarArg = true
				continue
			}
			switch arg := item.(type) {
			case *TypeName:
				argStrs = append(argStrs, arg.SqlString())
			case *FunctionParameter:
				if arg.ArgType != nil {
					argStrs = append(argStrs, arg.ArgType.SqlString())
				}
			}
		}
		if hasStarArg && len(argStrs) == 0 {
			parts = append(parts, "(*)")
		} else if len(argStrs) > 0 {
			parts = append(parts, "("+strings.Join(argStrs, ", ")+")")
		}
	}

	// Add definition
	if ds.Definition != nil && len(ds.Definition.Items) > 0 {
		var defStrs []string
		for _, item := range ds.Definition.Items {
			if defElem, ok := item.(*DefElem); ok {
				defStrs = append(defStrs, defElem.String())
			}
		}
		if len(defStrs) > 0 {
			parts = append(parts, "("+strings.Join(defStrs, ", ")+")")
		}
	}

	return strings.Join(parts, " ")
}

// NewDefineStmt creates a new DefineStmt node
func NewDefineStmt(kind ObjectType, oldStyle bool, defNames *NodeList, args *NodeList, definition *NodeList, ifNotExists, replace bool) *DefineStmt {
	return &DefineStmt{
		BaseNode:    BaseNode{Tag: T_DefineStmt},
		Kind:        kind,
		OldStyle:    oldStyle,
		DefNames:    defNames,
		Args:        args,
		Definition:  definition,
		IfNotExists: ifNotExists,
		Replace:     replace,
	}
}

// SqlString returns the SQL representation of DefineStmt
func (ds *DefineStmt) SqlString() string {
	var parts []string
	
	parts = append(parts, "CREATE")
	
	if ds.Replace {
		parts = append(parts, "OR REPLACE")
	}
	
	// Add object type
	switch ds.Kind {
	case OBJECT_TYPE:
		parts = append(parts, "TYPE")
	case OBJECT_AGGREGATE:
		parts = append(parts, "AGGREGATE")
	case OBJECT_OPERATOR:
		parts = append(parts, "OPERATOR")
	case OBJECT_TSPARSER:
		parts = append(parts, "TEXT SEARCH PARSER")
	case OBJECT_TSDICTIONARY:
		parts = append(parts, "TEXT SEARCH DICTIONARY")
	case OBJECT_TSTEMPLATE:
		parts = append(parts, "TEXT SEARCH TEMPLATE")
	case OBJECT_TSCONFIGURATION:
		parts = append(parts, "TEXT SEARCH CONFIGURATION")
	case OBJECT_COLLATION:
		parts = append(parts, "COLLATION")
	}
	
	// Add IF NOT EXISTS if present
	if ds.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}
	
	// Add name
	if ds.DefNames != nil && len(ds.DefNames.Items) > 0 {
		var nameStrs []string
		for _, item := range ds.DefNames.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	// Add arguments for aggregates and operators
	if ds.Args != nil && len(ds.Args.Items) > 0 {
		var argStrs []string
		hasStarArg := false
		for _, item := range ds.Args.Items {
			// Check for nil which represents * in aggregates like COUNT(*)
			if item == nil {
				hasStarArg = true
				continue
			}
			switch arg := item.(type) {
			case *TypeName:
				argStrs = append(argStrs, arg.SqlString())
			case *FunctionParameter:
				if arg.ArgType != nil {
					argStrs = append(argStrs, arg.ArgType.SqlString())
				}
			}
		}
		if hasStarArg && len(argStrs) == 0 {
			parts = append(parts, "(*)")
		} else if len(argStrs) > 0 {
			parts = append(parts, "("+strings.Join(argStrs, ", ")+")")
		}
	}
	
	// Add definition
	if ds.Definition != nil && len(ds.Definition.Items) > 0 {
		// Check if this is a FROM clause (for COLLATION FROM syntax)
		if ds.Kind == OBJECT_COLLATION && len(ds.Definition.Items) == 1 {
			// Check if the first item is a NodeList (which indicates FROM syntax)
			if firstItem := ds.Definition.Items[0]; firstItem != nil {
				if nodeList, ok := firstItem.(*NodeList); ok {
					// This is FROM syntax - handle qualified names in FROM clause
					var nameStrs []string
					for _, item := range nodeList.Items {
						if name, ok := item.(*String); ok {
							nameStrs = append(nameStrs, name.SVal)
						}
					}
					if len(nameStrs) > 0 {
						parts = append(parts, "FROM \""+strings.Join(nameStrs, ".")+"\"")
						return strings.Join(parts, " ")
					}
				}
			}
		}
		
		// Normal definition with DefElem items
		defParts := []string{}
		for _, item := range ds.Definition.Items {
			if def, ok := item.(*DefElem); ok {
				defParts = append(defParts, def.SqlString())
			}
		}
		if len(defParts) > 0 {
			parts = append(parts, "("+strings.Join(defParts, ", ")+")")
		}
	}
	
	return strings.Join(parts, " ")
}

// DeclareCursorStmt represents a DECLARE cursor statement
// Ported from postgres/src/include/nodes/parsenodes.h:3293-3299
type DeclareCursorStmt struct {
	PortalName string // name of the portal (cursor)
	Options    int    // bitmask of options
	Query      Node   // the query (raw parse tree)
}

// node implements the Node interface
func (dcs *DeclareCursorStmt) node() {}

// stmt implements the Stmt interface
func (dcs *DeclareCursorStmt) stmt() {}

// String returns string representation of DeclareCursorStmt
func (dcs *DeclareCursorStmt) String() string {
	var parts []string

	parts = append(parts, "DECLARE", dcs.PortalName, "CURSOR FOR")

	if dcs.Query != nil {
		parts = append(parts, dcs.Query.String())
	}

	return strings.Join(parts, " ")
}

// NewDeclareCursorStmt creates a new DeclareCursorStmt node
func NewDeclareCursorStmt(portalName string, options int, query Node) *DeclareCursorStmt {
	return &DeclareCursorStmt{
		PortalName: portalName,
		Options:    options,
		Query:      query,
	}
}

// FetchStmt represents a FETCH statement (also MOVE)
// Ported from postgres/src/include/nodes/parsenodes.h:3328-3335
type FetchStmt struct {
	Direction  FetchDirection // see FetchDirection enum
	HowMany    int64          // number of rows, or position argument
	PortalName string         // name of portal (cursor)
	IsMove     bool           // true if MOVE
}

// node implements the Node interface
func (fs *FetchStmt) node() {}

// stmt implements the Stmt interface
func (fs *FetchStmt) stmt() {}

// String returns string representation of FetchStmt
func (fs *FetchStmt) String() string {
	var parts []string

	if fs.IsMove {
		parts = append(parts, "MOVE")
	} else {
		parts = append(parts, "FETCH")
	}

	switch fs.Direction {
	case FETCH_FORWARD:
		if fs.HowMany == 9223372036854775807 { // FETCH_ALL = LONG_MAX
			parts = append(parts, "ALL")
		} else {
			parts = append(parts, fmt.Sprintf("%d", fs.HowMany))
		}
	case FETCH_BACKWARD:
		if fs.HowMany == 9223372036854775807 { // FETCH_ALL = LONG_MAX
			parts = append(parts, "BACKWARD ALL")
		} else {
			parts = append(parts, fmt.Sprintf("BACKWARD %d", fs.HowMany))
		}
	case FETCH_ABSOLUTE:
		parts = append(parts, fmt.Sprintf("ABSOLUTE %d", fs.HowMany))
	case FETCH_RELATIVE:
		parts = append(parts, fmt.Sprintf("RELATIVE %d", fs.HowMany))
	}

	if fs.PortalName != "" {
		parts = append(parts, "FROM", fs.PortalName)
	}

	return strings.Join(parts, " ")
}

// NewFetchStmt creates a new FetchStmt node
func NewFetchStmt(direction FetchDirection, howMany int64, portalName string, isMove bool) *FetchStmt {
	return &FetchStmt{
		Direction:  direction,
		HowMany:    howMany,
		PortalName: portalName,
		IsMove:     isMove,
	}
}

// ClosePortalStmt represents a CLOSE statement
// Ported from postgres/src/include/nodes/parsenodes.h:3305-3310
type ClosePortalStmt struct {
	PortalName *string // name of the portal (cursor); nil means CLOSE ALL
}

// node implements the Node interface
func (cps *ClosePortalStmt) node() {}

// stmt implements the Stmt interface
func (cps *ClosePortalStmt) stmt() {}

// String returns string representation of ClosePortalStmt
func (cps *ClosePortalStmt) String() string {
	if cps.PortalName == nil {
		return "CLOSE ALL"
	}
	return "CLOSE " + *cps.PortalName
}

// NewClosePortalStmt creates a new ClosePortalStmt node
func NewClosePortalStmt(portalName *string) *ClosePortalStmt {
	return &ClosePortalStmt{
		PortalName: portalName,
	}
}

// CreateEnumStmt represents a CREATE TYPE ... AS ENUM statement
// Ported from postgres/src/include/nodes/parsenodes.h:3696-3701
type CreateEnumStmt struct {
	BaseNode
	TypeName *NodeList // qualified name (list of String)
	Vals     *NodeList // enum values (list of String)
}

// node implements the Node interface
func (ces *CreateEnumStmt) node() {}

// stmt implements the Stmt interface
func (ces *CreateEnumStmt) stmt() {}

// StatementType returns the statement type
func (ces *CreateEnumStmt) StatementType() string {
	return "CreateEnumStmt"
}

// String returns string representation of CreateEnumStmt
func (ces *CreateEnumStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE TYPE")

	if ces.TypeName != nil && len(ces.TypeName.Items) > 0 {
		var nameStrs []string
		for _, item := range ces.TypeName.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	parts = append(parts, "AS ENUM")

	if ces.Vals != nil && len(ces.Vals.Items) > 0 {
		var valStrs []string
		for _, item := range ces.Vals.Items {
			if val, ok := item.(*String); ok {
				valStrs = append(valStrs, "'"+val.SVal+"'")
			}
		}
		parts = append(parts, "("+strings.Join(valStrs, ", ")+")")
	} else {
		parts = append(parts, "()")
	}

	return strings.Join(parts, " ")
}

// NewCreateEnumStmt creates a new CreateEnumStmt node
func NewCreateEnumStmt(typeName *NodeList, vals *NodeList) *CreateEnumStmt {
	return &CreateEnumStmt{
		BaseNode: BaseNode{Tag: T_CreateEnumStmt},
		TypeName: typeName,
		Vals:     vals,
	}
}

// SqlString returns the SQL representation of CreateEnumStmt
func (ces *CreateEnumStmt) SqlString() string {
	var parts []string
	
	parts = append(parts, "CREATE TYPE")
	
	// Add type name
	if ces.TypeName != nil && len(ces.TypeName.Items) > 0 {
		var nameStrs []string
		for _, item := range ces.TypeName.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	parts = append(parts, "AS ENUM")
	
	// Add enum values
	if ces.Vals != nil && len(ces.Vals.Items) > 0 {
		quotedVals := []string{}
		for _, item := range ces.Vals.Items {
			if val, ok := item.(*String); ok {
				quotedVals = append(quotedVals, "'"+val.SVal+"'")
			}
		}
		parts = append(parts, "("+strings.Join(quotedVals, ", ")+")")
	} else {
		parts = append(parts, "()")
	}
	
	return strings.Join(parts, " ")
}

// CompositeTypeStmt represents a CREATE TYPE ... AS (...) statement
// Ported from postgres/src/include/nodes/parsenodes.h:3684
type CompositeTypeStmt struct {
	BaseNode
	Typevar     *RangeVar   // the composite type name
	Coldeflist  *NodeList   // list of ColumnDef nodes
}

// node implements the Node interface
func (cts *CompositeTypeStmt) node() {}

// stmt implements the Stmt interface
func (cts *CompositeTypeStmt) stmt() {}

// StatementType returns the statement type
func (cts *CompositeTypeStmt) StatementType() string {
	return "CompositeTypeStmt"
}

// String returns string representation of CompositeTypeStmt
func (cts *CompositeTypeStmt) String() string {
	var parts []string
	
	parts = append(parts, "CREATE TYPE")
	
	if cts.Typevar != nil {
		parts = append(parts, cts.Typevar.String())
	}
	
	parts = append(parts, "AS (...)") // Simplified representation
	
	return strings.Join(parts, " ")
}

// NewCompositeTypeStmt creates a new CompositeTypeStmt node
func NewCompositeTypeStmt(typevar *RangeVar, coldeflist *NodeList) *CompositeTypeStmt {
	return &CompositeTypeStmt{
		BaseNode:   BaseNode{Tag: T_CompositeTypeStmt},
		Typevar:    typevar,
		Coldeflist: coldeflist,
	}
}

// SqlString returns the SQL representation of CompositeTypeStmt
func (cts *CompositeTypeStmt) SqlString() string {
	var parts []string
	
	parts = append(parts, "CREATE TYPE")
	
	// Add type name
	if cts.Typevar != nil {
		parts = append(parts, cts.Typevar.SqlString())
	}
	
	parts = append(parts, "AS")
	
	// Add column definitions
	if cts.Coldeflist != nil && len(cts.Coldeflist.Items) > 0 {
		colDefs := []string{}
		for _, item := range cts.Coldeflist.Items {
			if colDef, ok := item.(interface{ SqlString() string }); ok {
				colDefs = append(colDefs, colDef.SqlString())
			}
		}
		parts = append(parts, "("+strings.Join(colDefs, ", ")+")")
	} else {
		parts = append(parts, "()")
	}
	
	return strings.Join(parts, " ")
}

// AlterEnumStmt represents an ALTER TYPE ... ADD VALUE statement
// Ported from postgres/src/include/nodes/parsenodes.h:3693
type AlterEnumStmt struct {
	BaseNode
	TypeName            *NodeList // qualified name (list of String)
	OldVal              string    // old enum value's name, if renaming
	NewVal              string    // new enum value's name
	NewValNeighbor      string    // neighboring enum value, if specified
	NewValIsAfter       bool      // place new val after neighbor?
	SkipIfNewValExists  bool      // ignore statement if new already exists
}

// node implements the Node interface
func (aes *AlterEnumStmt) node() {}

// stmt implements the Stmt interface
func (aes *AlterEnumStmt) stmt() {}

// StatementType returns the statement type
func (aes *AlterEnumStmt) StatementType() string {
	return "AlterEnumStmt"
}

// String returns string representation of AlterEnumStmt
func (aes *AlterEnumStmt) String() string {
	var parts []string
	
	parts = append(parts, "ALTER TYPE")
	
	if aes.TypeName != nil && len(aes.TypeName.Items) > 0 {
		var nameStrs []string
		for _, item := range aes.TypeName.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	if aes.OldVal != "" {
		parts = append(parts, "RENAME VALUE", aes.OldVal, "TO", aes.NewVal)
	} else {
		parts = append(parts, "ADD VALUE", aes.NewVal)
	}
	
	return strings.Join(parts, " ")
}

// NewAlterEnumStmt creates a new AlterEnumStmt node
func NewAlterEnumStmt(typeName *NodeList) *AlterEnumStmt {
	return &AlterEnumStmt{
		BaseNode: BaseNode{Tag: T_AlterEnumStmt},
		TypeName: typeName,
	}
}

// SqlString returns the SQL representation of AlterEnumStmt
func (aes *AlterEnumStmt) SqlString() string {
	var parts []string
	
	parts = append(parts, "ALTER TYPE")
	
	// Add type name
	if aes.TypeName != nil && len(aes.TypeName.Items) > 0 {
		var nameStrs []string
		for _, item := range aes.TypeName.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	if aes.OldVal != "" {
		// RENAME VALUE
		parts = append(parts, "RENAME VALUE", "'"+aes.OldVal+"'", "TO", "'"+aes.NewVal+"'")
	} else {
		// ADD VALUE
		parts = append(parts, "ADD VALUE")
		if aes.SkipIfNewValExists {
			parts = append(parts, "IF NOT EXISTS")
		}
		parts = append(parts, "'"+aes.NewVal+"'")
		
		if aes.NewValNeighbor != "" {
			if aes.NewValIsAfter {
				parts = append(parts, "AFTER")
			} else {
				parts = append(parts, "BEFORE")
			}
			parts = append(parts, "'"+aes.NewValNeighbor+"'")
		}
	}
	
	return strings.Join(parts, " ")
}

// CreateRangeStmt represents a CREATE TYPE ... AS RANGE statement
// Ported from postgres/src/include/nodes/parsenodes.h:3707-3712
type CreateRangeStmt struct {
	BaseNode
	TypeName *NodeList // qualified name (list of String)
	Params   *NodeList // range parameters (list of DefElem)
}

// node implements the Node interface
func (crs *CreateRangeStmt) node() {}

// stmt implements the Stmt interface
func (crs *CreateRangeStmt) stmt() {}

// StatementType returns the statement type
func (crs *CreateRangeStmt) StatementType() string {
	return "CreateRangeStmt"
}

// String returns string representation of CreateRangeStmt
func (crs *CreateRangeStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE TYPE")

	if crs.TypeName != nil && len(crs.TypeName.Items) > 0 {
		var nameStrs []string
		for _, item := range crs.TypeName.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	parts = append(parts, "AS RANGE")

	if crs.Params != nil && len(crs.Params.Items) > 0 {
		parts = append(parts, "(...)") // Simplified representation
	}

	return strings.Join(parts, " ")
}

// NewCreateRangeStmt creates a new CreateRangeStmt node
func NewCreateRangeStmt(typeName *NodeList, params *NodeList) *CreateRangeStmt {
	return &CreateRangeStmt{
		BaseNode: BaseNode{Tag: T_CreateRangeStmt},
		TypeName: typeName,
		Params:   params,
	}
}

// SqlString returns the SQL representation of CreateRangeStmt
func (crs *CreateRangeStmt) SqlString() string {
	var parts []string
	
	parts = append(parts, "CREATE TYPE")
	
	// Add type name
	if crs.TypeName != nil && len(crs.TypeName.Items) > 0 {
		var nameStrs []string
		for _, item := range crs.TypeName.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	parts = append(parts, "AS RANGE")
	
	// Add parameters
	if crs.Params != nil && len(crs.Params.Items) > 0 {
		paramParts := []string{}
		for _, item := range crs.Params.Items {
			if param, ok := item.(*DefElem); ok {
				paramParts = append(paramParts, param.SqlString())
			}
		}
		parts = append(parts, "("+strings.Join(paramParts, ", ")+")")
	}
	
	return strings.Join(parts, " ")
}

// CreateStatsStmt represents a CREATE STATISTICS statement
// Ported from postgres/src/include/nodes/parsenodes.h:3384-3394
type CreateStatsStmt struct {
	DefNames    []*String   // qualified name (list of String)
	StatTypes   []*String   // stat types (list of String)
	Exprs       *NodeList   // expressions to build statistics on
	Relations   []*RangeVar // rels to build stats on (list of RangeVar)
	StxComment  *string     // comment to apply to stats, or nil
	Transformed bool        // true when transformStatsStmt is finished
	IfNotExists bool        // do nothing if stats name already exists
}

// node implements the Node interface
func (css *CreateStatsStmt) node() {}

// stmt implements the Stmt interface
func (css *CreateStatsStmt) stmt() {}

// String returns string representation of CreateStatsStmt
func (css *CreateStatsStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE STATISTICS")

	if css.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	if len(css.DefNames) > 0 {
		var nameStrs []string
		for _, name := range css.DefNames {
			nameStrs = append(nameStrs, name.SVal)
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	if len(css.StatTypes) > 0 {
		var typeStrs []string
		for _, statType := range css.StatTypes {
			typeStrs = append(typeStrs, statType.SVal)
		}
		parts = append(parts, "("+strings.Join(typeStrs, ", ")+")")
	}

	if len(css.Relations) > 0 {
		parts = append(parts, "ON")
		var relStrs []string
		for _, rel := range css.Relations {
			relStrs = append(relStrs, rel.String())
		}
		parts = append(parts, strings.Join(relStrs, ", "))
	}

	return strings.Join(parts, " ")
}

// NewCreateStatsStmt creates a new CreateStatsStmt node
func NewCreateStatsStmt(defNames []*String, statTypes []*String, exprs *NodeList, relations []*RangeVar, stxComment *string, transformed, ifNotExists bool) *CreateStatsStmt {
	return &CreateStatsStmt{
		DefNames:    defNames,
		StatTypes:   statTypes,
		Exprs:       exprs,
		Relations:   relations,
		StxComment:  stxComment,
		Transformed: transformed,
		IfNotExists: ifNotExists,
	}
}

// CreatePLangStmt represents a CREATE LANGUAGE statement
// Ported from postgres/src/include/nodes/parsenodes.h:3054-3063
type CreatePLangStmt struct {
	Replace     bool      // true => replace if already exists
	PLName      string    // PL name
	PLHandler   []*String // PL call handler function (qualified name)
	PLInline    []*String // optional inline function (qualified name)
	PLValidator []*String // optional validator function (qualified name)
	PLTrusted   bool      // PL is trusted
}

// node implements the Node interface
func (cpls *CreatePLangStmt) node() {}

// stmt implements the Stmt interface
func (cpls *CreatePLangStmt) stmt() {}

// String returns string representation of CreatePLangStmt
func (cpls *CreatePLangStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE")
	if cpls.Replace {
		parts = append(parts, "OR REPLACE")
	}
	if cpls.PLTrusted {
		parts = append(parts, "TRUSTED")
	}
	parts = append(parts, "LANGUAGE", cpls.PLName)

	if len(cpls.PLHandler) > 0 {
		var handlerStrs []string
		for _, handler := range cpls.PLHandler {
			handlerStrs = append(handlerStrs, handler.SVal)
		}
		parts = append(parts, "HANDLER", strings.Join(handlerStrs, "."))
	}

	if len(cpls.PLInline) > 0 {
		var inlineStrs []string
		for _, inline := range cpls.PLInline {
			inlineStrs = append(inlineStrs, inline.SVal)
		}
		parts = append(parts, "INLINE", strings.Join(inlineStrs, "."))
	}

	if len(cpls.PLValidator) > 0 {
		var validatorStrs []string
		for _, validator := range cpls.PLValidator {
			validatorStrs = append(validatorStrs, validator.SVal)
		}
		parts = append(parts, "VALIDATOR", strings.Join(validatorStrs, "."))
	}

	return strings.Join(parts, " ")
}

// NewCreatePLangStmt creates a new CreatePLangStmt node
func NewCreatePLangStmt(replace bool, plName string, plHandler, plInline, plValidator []*String, plTrusted bool) *CreatePLangStmt {
	return &CreatePLangStmt{
		Replace:     replace,
		PLName:      plName,
		PLHandler:   plHandler,
		PLInline:    plInline,
		PLValidator: plValidator,
		PLTrusted:   plTrusted,
	}
}

// CreateTableAsStmt represents CREATE TABLE AS and CREATE MATERIALIZED VIEW statements
// Ported from postgres/src/include/nodes/parsenodes.h:3607-3615
type CreateTableAsStmt struct {
	BaseNode
	Query        Node        // the query (generally a SelectStmt)
	Into         *IntoClause // target relation
	ObjType      ObjectType  // table or materialized view
	IsSelectInto bool        // it's a SELECT INTO, not CREATE TABLE AS
	IfNotExists  bool        // IF NOT EXISTS was specified
}

// Location returns the statement's source location (dummy implementation)
func (ctas *CreateTableAsStmt) Location() int {
	return 0 // TODO: Implement proper location tracking
}

// NodeTag returns the node's type tag
func (ctas *CreateTableAsStmt) NodeTag() NodeTag {
	return T_CreateTableAsStmt
}

// StatementType returns the statement type for this node
func (ctas *CreateTableAsStmt) StatementType() string {
	switch ctas.ObjType {
	case OBJECT_MATVIEW:
		return "CREATE MATERIALIZED VIEW"
	default:
		return "CREATE TABLE AS"
	}
}

// SqlString returns SQL representation of the CREATE MATERIALIZED VIEW statement
func (ctas *CreateTableAsStmt) SqlString() string {
	var parts []string

	// CREATE [MATERIALIZED]
	parts = append(parts, "CREATE")
	switch ctas.ObjType {
	case OBJECT_MATVIEW:
		// Check if UNLOGGED
		if ctas.Into != nil && ctas.Into.Rel != nil && ctas.Into.Rel.RelPersistence == 'u' {
			parts = append(parts, "UNLOGGED")
		}
		parts = append(parts, "MATERIALIZED VIEW")
		if ctas.IfNotExists {
			parts = append(parts, "IF NOT EXISTS")
		}
	default:
		parts = append(parts, "TABLE")
		if ctas.IfNotExists {
			parts = append(parts, "IF NOT EXISTS")
		}
	}

	// Target relation name and column list
	if ctas.Into != nil {
		targetStr := ctas.Into.TargetString()
		if targetStr != "" {
			parts = append(parts, targetStr)
		}
	}

	// AS query
	if ctas.Query != nil {
		parts = append(parts, "AS", ctas.Query.SqlString())
	}

	// WITH [NO] DATA (for materialized views only)
	if ctas.ObjType == OBJECT_MATVIEW && ctas.Into != nil {
		if ctas.Into.SkipData {
			parts = append(parts, "WITH NO DATA")
		} else {
			parts = append(parts, "WITH DATA")
		}
	}

	return strings.Join(parts, " ")
}

// String returns string representation of CreateTableAsStmt
func (ctas *CreateTableAsStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE")
	switch ctas.ObjType {
	case OBJECT_MATVIEW:
		parts = append(parts, "MATERIALIZED VIEW")
	default:
		parts = append(parts, "TABLE")
	}

	if ctas.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	if ctas.Into != nil && ctas.Into.Rel != nil {
		parts = append(parts, ctas.Into.Rel.String())
	}

	parts = append(parts, "AS")
	if ctas.Query != nil {
		parts = append(parts, ctas.Query.String())
	}

	return strings.Join(parts, " ")
}

// NewCreateTableAsStmt creates a new CreateTableAsStmt node
func NewCreateTableAsStmt(query Node, into *IntoClause, objType ObjectType, isSelectInto, ifNotExists bool) *CreateTableAsStmt {
	return &CreateTableAsStmt{
		BaseNode:     BaseNode{Tag: T_CreateTableAsStmt},
		Query:        query,
		Into:         into,
		ObjType:      objType,
		IsSelectInto: isSelectInto,
		IfNotExists:  ifNotExists,
	}
}

// RefreshMatViewStmt represents a REFRESH MATERIALIZED VIEW statement
// Ported from postgres/src/include/nodes/parsenodes.h:3617-3622
type RefreshMatViewStmt struct {
	BaseNode
	Concurrent bool      // allow concurrent access?
	SkipData   bool      // true for WITH NO DATA
	Relation   *RangeVar // relation to refresh
}

// Location returns the statement's source location (dummy implementation)
func (rmvs *RefreshMatViewStmt) Location() int {
	return 0 // TODO: Implement proper location tracking
}

// NodeTag returns the node's type tag
func (rmvs *RefreshMatViewStmt) NodeTag() NodeTag {
	return T_RefreshMatViewStmt
}

// StatementType returns the statement type for this node
func (rmvs *RefreshMatViewStmt) StatementType() string {
	return "REFRESH MATERIALIZED VIEW"
}

// SqlString returns SQL representation of the REFRESH MATERIALIZED VIEW statement
func (rmvs *RefreshMatViewStmt) SqlString() string {
	var parts []string

	parts = append(parts, "REFRESH MATERIALIZED VIEW")

	if rmvs.Concurrent {
		parts = append(parts, "CONCURRENTLY")
	}

	if rmvs.Relation != nil {
		parts = append(parts, rmvs.Relation.SqlString())
	}

	if rmvs.SkipData {
		parts = append(parts, "WITH NO DATA")
	}

	return strings.Join(parts, " ")
}

// String returns string representation of RefreshMatViewStmt
func (rmvs *RefreshMatViewStmt) String() string {
	var parts []string

	parts = append(parts, "REFRESH MATERIALIZED VIEW")

	if rmvs.Concurrent {
		parts = append(parts, "CONCURRENTLY")
	}

	if rmvs.Relation != nil {
		parts = append(parts, rmvs.Relation.String())
	}

	if rmvs.SkipData {
		parts = append(parts, "WITH NO DATA")
	}

	return strings.Join(parts, " ")
}

// NewRefreshMatViewStmt creates a new RefreshMatViewStmt node
func NewRefreshMatViewStmt(concurrent, skipData bool, relation *RangeVar) *RefreshMatViewStmt {
	return &RefreshMatViewStmt{
		BaseNode:   BaseNode{Tag: T_RefreshMatViewStmt},
		Concurrent: concurrent,
		SkipData:   skipData,
		Relation:   relation,
	}
}
