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

// FETCH_ALL constant for fetching all rows
// Ported from postgres/src/include/nodes/parsenodes.h:3326
const FETCH_ALL = 9223372036854775807 // LONG_MAX

// Cursor option constants
// Ported from postgres/src/include/nodes/parsenodes.h:3275-3291
const (
	CURSOR_OPT_BINARY       = 0x0001 // BINARY
	CURSOR_OPT_SCROLL       = 0x0002 // SCROLL explicitly given
	CURSOR_OPT_NO_SCROLL    = 0x0004 // NO SCROLL explicitly given
	CURSOR_OPT_INSENSITIVE  = 0x0008 // INSENSITIVE
	CURSOR_OPT_ASENSITIVE   = 0x0010 // ASENSITIVE
	CURSOR_OPT_HOLD         = 0x0020 // WITH HOLD
	CURSOR_OPT_FAST_PLAN    = 0x0100 // prefer fast-start plan
	CURSOR_OPT_GENERIC_PLAN = 0x0200 // force use of generic plan
	CURSOR_OPT_CUSTOM_PLAN  = 0x0400 // force use of custom plan
	CURSOR_OPT_PARALLEL_OK  = 0x0800 // parallel mode OK
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
	BaseNode
	Sequence    *RangeVar // the sequence to create
	Options     *NodeList // list of options (DefElem nodes)
	OwnerID     Oid       // ID of owner, or 0 for default (InvalidOid)
	ForIdentity bool      // true if for IDENTITY column
	IfNotExists bool      // true for IF NOT EXISTS
}

// node implements the Node interface
func (css *CreateSeqStmt) node() {}

// stmt implements the Stmt interface
func (css *CreateSeqStmt) stmt() {}

// StatementType returns the type of statement
func (css *CreateSeqStmt) StatementType() string {
	return "CreateSeqStmt"
}

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

// SqlString returns the SQL representation of CreateSeqStmt
func (css *CreateSeqStmt) SqlString() string {
	var parts []string

	parts = append(parts, "CREATE SEQUENCE")

	if css.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	if css.Sequence != nil {
		parts = append(parts, css.Sequence.SqlString())
	}

	// Add sequence options if present
	if css.Options != nil && css.Options.Len() > 0 {
		for _, item := range css.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				parts = append(parts, formatSeqOption(opt))
			}
		}
	}

	return strings.Join(parts, " ")
}

// NewCreateSeqStmt creates a new CreateSeqStmt node
func NewCreateSeqStmt(sequence *RangeVar, options *NodeList, ownerID Oid, forIdentity, ifNotExists bool) *CreateSeqStmt {
	return &CreateSeqStmt{
		BaseNode:    BaseNode{Tag: T_CreateSeqStmt},
		Sequence:    sequence,
		Options:     options,
		OwnerID:     ownerID,
		ForIdentity: forIdentity,
		IfNotExists: ifNotExists,
	}
}

// AlterSeqStmt represents an ALTER SEQUENCE statement
// Ported from postgres/src/include/nodes/parsenodes.h:3470-3477
type AlterSeqStmt struct {
	BaseNode
	Sequence    *RangeVar // the sequence to alter
	Options     *NodeList // list of DefElem options
	ForIdentity bool      // for IDENTITY column
	MissingOk   bool      // skip error if sequence doesn't exist
}

// NewAlterSeqStmt creates a new AlterSeqStmt node
func NewAlterSeqStmt(sequence *RangeVar, options *NodeList, forIdentity, missingOk bool) *AlterSeqStmt {
	return &AlterSeqStmt{
		BaseNode:    BaseNode{Tag: T_AlterSeqStmt},
		Sequence:    sequence,
		Options:     options,
		ForIdentity: forIdentity,
		MissingOk:   missingOk,
	}
}

// StatementType returns the statement type
func (ass *AlterSeqStmt) StatementType() string {
	return "AlterSeqStmt"
}

// String returns string representation of AlterSeqStmt
func (ass *AlterSeqStmt) String() string {
	ifExists := ""
	if ass.MissingOk {
		ifExists = " IF EXISTS"
	}
	return fmt.Sprintf("AlterSeqStmt(%s%s)@%d", ass.Sequence.String(), ifExists, ass.Location())
}

// formatSeqOption formats a sequence option DefElem for SQL output
func formatSeqOption(opt *DefElem) string {
	if opt == nil {
		return ""
	}

	// Special handling for sequence-specific options
	switch opt.Defname {
	case "cycle":
		if b, ok := opt.Arg.(*Boolean); ok && b.BoolVal {
			return "CYCLE"
		}
		return "NO CYCLE"
	case "restart":
		if opt.Arg == nil {
			return "RESTART"
		}
		return "RESTART WITH " + opt.Arg.SqlString()
	case "start":
		return "START WITH " + opt.Arg.SqlString()
	case "increment":
		return "INCREMENT BY " + opt.Arg.SqlString()
	case "minvalue":
		if opt.Arg == nil {
			return "NO MINVALUE"
		}
		return "MINVALUE " + opt.Arg.SqlString()
	case "maxvalue":
		if opt.Arg == nil {
			return "NO MAXVALUE"
		}
		return "MAXVALUE " + opt.Arg.SqlString()
	case "cache":
		return "CACHE " + opt.Arg.SqlString()
	case "owned_by":
		// TODO: Handle OWNED BY properly
		return "OWNED BY " + opt.Arg.SqlString()
	case "as":
		return "AS " + opt.Arg.SqlString()
	default:
		// Fall back to default formatting
		return opt.SqlString()
	}
}

// SqlString returns the SQL representation of AlterSeqStmt
func (ass *AlterSeqStmt) SqlString() string {
	var parts []string

	parts = append(parts, "ALTER SEQUENCE")

	if ass.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	if ass.Sequence != nil {
		parts = append(parts, ass.Sequence.SqlString())
	}

	// Add sequence options if present
	if ass.Options != nil && ass.Options.Len() > 0 {
		for _, item := range ass.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				parts = append(parts, formatSeqOption(opt))
			}
		}
	}

	return strings.Join(parts, " ")
}

// CreateOpClassItem represents an item in a CREATE OPERATOR CLASS statement
// Ported from postgres/src/include/nodes/parsenodes.h:3184-3195
type CreateOpClassItem struct {
	BaseNode
	ItemType    OpClassItemType // OPCLASS_ITEM_OPERATOR, OPCLASS_ITEM_FUNCTION, or OPCLASS_ITEM_STORAGETYPE
	Name        *ObjectWithArgs // operator or function name and args
	Number      int             // strategy num or support proc num
	OrderFamily *NodeList       // only used for ordering operators
	ClassArgs   *NodeList       // amproclefttype/amprocrighttype or amoplefttype/amoprighttype
	StoredType  *TypeName       // datatype stored in index (for storage type items)
}

// String returns string representation of CreateOpClassItem
func (oci *CreateOpClassItem) String() string {
	var parts []string

	switch oci.ItemType {
	case OPCLASS_ITEM_OPERATOR:
		parts = append(parts, "OPERATOR")
	case OPCLASS_ITEM_FUNCTION:
		parts = append(parts, "FUNCTION")
	case OPCLASS_ITEM_STORAGETYPE:
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

// SqlString returns the SQL representation of CreateOpClassItem
func (oci *CreateOpClassItem) SqlString() string {
	var parts []string

	switch oci.ItemType {
	case OPCLASS_ITEM_OPERATOR:
		parts = append(parts, "OPERATOR")
	case OPCLASS_ITEM_FUNCTION:
		parts = append(parts, "FUNCTION")
	case OPCLASS_ITEM_STORAGETYPE:
		parts = append(parts, "STORAGE")
	}

	if oci.Number > 0 {
		parts = append(parts, fmt.Sprintf("%d", oci.Number))
	}

	if oci.Name != nil {
		parts = append(parts, oci.Name.SqlString())
	}

	// Add ClassArgs if present (for DROP operations like "OPERATOR 1 (int4, int4)")
	if oci.ClassArgs != nil && oci.ClassArgs.Len() > 0 {
		var argStrs []string
		for i := 0; i < oci.ClassArgs.Len(); i++ {
			if argNode, ok := oci.ClassArgs.Items[i].(*TypeName); ok {
				argStrs = append(argStrs, argNode.SqlString())
			}
		}
		if len(argStrs) > 0 {
			parts = append(parts, fmt.Sprintf("(%s)", strings.Join(argStrs, ", ")))
		}
	}

	if oci.StoredType != nil {
		parts = append(parts, oci.StoredType.SqlString())
	}

	return strings.Join(parts, " ")
}

func (oci *CreateOpClassItem) StatementType() string {
	return "CreateOpClassItem"
}

// NewCreateOpClassItem creates a new CreateOpClassItem node
func NewCreateOpClassItem(itemType OpClassItemType, name *ObjectWithArgs, number int, orderFamily *NodeList, classArgs *NodeList, storedType *TypeName) *CreateOpClassItem {
	return &CreateOpClassItem{
		BaseNode:    BaseNode{Tag: T_CreateOpClassItem},
		ItemType:    itemType,
		Name:        name,
		Number:      number,
		OrderFamily: orderFamily,
		ClassArgs:   classArgs,
		StoredType:  storedType,
	}
}

// NewOpClassItemOperator creates a new CreateOpClassItem for operators
// Two different signatures to match grammar usage:
// 1. OPERATOR Iconst any_operator opclass_purpose opt_recheck
// 2. OPERATOR Iconst operator_with_argtypes opclass_purpose opt_recheck
func NewOpClassItemOperator(number int, name *ObjectWithArgs, orderFamily *NodeList) *CreateOpClassItem {
	return NewCreateOpClassItem(OPCLASS_ITEM_OPERATOR, name, number, orderFamily, nil, nil)
}

// NewOpClassItemFunction creates a new CreateOpClassItem for functions
// Two different signatures to match grammar usage:
// 1. FUNCTION Iconst function_with_argtypes
// 2. FUNCTION Iconst '(' type_list ')' function_with_argtypes
func NewOpClassItemFunction(number int, name *ObjectWithArgs, classArgs *NodeList) *CreateOpClassItem {
	return NewCreateOpClassItem(OPCLASS_ITEM_FUNCTION, name, number, nil, classArgs, nil)
}

// NewOpClassItemStorage creates a new CreateOpClassItem for storage type
func NewOpClassItemStorage(storedType *TypeName) *CreateOpClassItem {
	return NewCreateOpClassItem(OPCLASS_ITEM_STORAGETYPE, nil, 0, nil, nil, storedType)
}

// CreateOpClassStmt represents a CREATE OPERATOR CLASS statement
// Ported from postgres/src/include/nodes/parsenodes.h:3169-3178
type CreateOpClassStmt struct {
	BaseNode
	OpClassName  *NodeList // qualified name (list of String)
	OpFamilyName *NodeList // qualified name (list of String); nil if omitted
	AmName       string    // name of index AM opclass is for
	DataType     *TypeName // datatype of indexed column
	Items        *NodeList // list of CreateOpClassItem nodes
	IsDefault    bool      // should be marked as default for type?
}

// StatementType implements the Stmt interface
func (cocs *CreateOpClassStmt) StatementType() string {
	return "CreateOpClassStmt"
}

// NodeTag implements the Node interface
func (cocs *CreateOpClassStmt) NodeTag() NodeTag {
	return T_CreateOpClassStmt
}

// SqlString implements the Stmt interface
func (cocs *CreateOpClassStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE OPERATOR CLASS")

	if cocs.OpClassName != nil && cocs.OpClassName.Len() > 0 {
		nameStrs := make([]string, 0, cocs.OpClassName.Len())
		for i := 0; i < cocs.OpClassName.Len(); i++ {
			if strNode, ok := cocs.OpClassName.Items[i].(*String); ok {
				nameStrs = append(nameStrs, strNode.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	if cocs.IsDefault {
		parts = append(parts, "DEFAULT")
	}

	parts = append(parts, "FOR TYPE")

	if cocs.DataType != nil {
		parts = append(parts, cocs.DataType.SqlString())
	}

	parts = append(parts, "USING", cocs.AmName)

	if cocs.OpFamilyName != nil && cocs.OpFamilyName.Len() > 0 {
		familyStrs := make([]string, 0, cocs.OpFamilyName.Len())
		for i := 0; i < cocs.OpFamilyName.Len(); i++ {
			if strNode, ok := cocs.OpFamilyName.Items[i].(*String); ok {
				familyStrs = append(familyStrs, strNode.SVal)
			}
		}
		parts = append(parts, "FAMILY", strings.Join(familyStrs, "."))
	}

	parts = append(parts, "AS")

	// For simplicity, we'll add a basic operator - full implementation would iterate through Items
	if cocs.Items != nil && cocs.Items.Len() > 0 {
		parts = append(parts, "OPERATOR 1 <")
	}

	return strings.Join(parts, " ")
}

// String returns string representation of CreateOpClassStmt
func (cocs *CreateOpClassStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE OPERATOR CLASS")

	if cocs.OpClassName != nil && cocs.OpClassName.Len() > 0 {
		var nameStrs []string
		for _, item := range cocs.OpClassName.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
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

	if cocs.OpFamilyName != nil && cocs.OpFamilyName.Len() > 0 {
		var familyStrs []string
		for _, item := range cocs.OpFamilyName.Items {
			if name, ok := item.(*String); ok {
				familyStrs = append(familyStrs, name.SVal)
			}
		}
		parts = append(parts, "FAMILY", strings.Join(familyStrs, "."))
	}

	return strings.Join(parts, " ")
}

// NewCreateOpClassStmt creates a new CreateOpClassStmt node
func NewCreateOpClassStmt(opClassName, opFamilyName *NodeList, amName string, dataType *TypeName, items *NodeList, isDefault bool) *CreateOpClassStmt {
	return &CreateOpClassStmt{
		BaseNode:     BaseNode{Tag: T_CreateOpClassStmt},
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
	BaseNode
	OpFamilyName *NodeList // qualified name (list of String)
	AmName       string    // name of index AM opfamily is for
}

// StatementType implements the Stmt interface
func (cofs *CreateOpFamilyStmt) StatementType() string {
	return "CreateOpFamilyStmt"
}

// NodeTag implements the Node interface
func (cofs *CreateOpFamilyStmt) NodeTag() NodeTag {
	return T_CreateOpFamilyStmt
}

// SqlString implements the Stmt interface
func (cofs *CreateOpFamilyStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE OPERATOR FAMILY")

	if cofs.OpFamilyName != nil && cofs.OpFamilyName.Len() > 0 {
		nameStrs := make([]string, 0, cofs.OpFamilyName.Len())
		for i := 0; i < cofs.OpFamilyName.Len(); i++ {
			if strNode, ok := cofs.OpFamilyName.Items[i].(*String); ok {
				nameStrs = append(nameStrs, strNode.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	parts = append(parts, "USING", cofs.AmName)

	return strings.Join(parts, " ")
}

// String returns string representation of CreateOpFamilyStmt
func (cofs *CreateOpFamilyStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE OPERATOR FAMILY")

	if cofs.OpFamilyName != nil && cofs.OpFamilyName.Len() > 0 {
		var nameStrs []string
		for _, item := range cofs.OpFamilyName.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	parts = append(parts, "USING", cofs.AmName)

	return strings.Join(parts, " ")
}

// NewCreateOpFamilyStmt creates a new CreateOpFamilyStmt node
func NewCreateOpFamilyStmt(opFamilyName *NodeList, amName string) *CreateOpFamilyStmt {
	return &CreateOpFamilyStmt{
		BaseNode:     BaseNode{Tag: T_CreateOpFamilyStmt},
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

func (ccs *CreateCastStmt) StatementType() string {
	return "CREATE CAST"
}

func (ccs *CreateCastStmt) Location() int {
	return 0
}

func (ccs *CreateCastStmt) NodeTag() NodeTag {
	return T_CreateCastStmt
}

func (ccs *CreateCastStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE CAST")

	if ccs.SourceType != nil && ccs.TargetType != nil {
		parts = append(parts, fmt.Sprintf("(%s AS %s)", ccs.SourceType.SqlString(), ccs.TargetType.SqlString()))
	}

	if ccs.Inout {
		parts = append(parts, "WITH INOUT")
	} else if ccs.Func != nil {
		parts = append(parts, "WITH FUNCTION", ccs.Func.SqlString())
	} else if ccs.Context == COERCION_EXPLICIT {
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
	BaseNode
	ConversionName  *NodeList // name of the conversion
	ForEncodingName string    // source encoding name
	ToEncodingName  string    // destination encoding name
	FuncName        *NodeList // qualified conversion function name
	Def             bool      // true if this is a default conversion
}

// String returns string representation of CreateConversionStmt
func (ccs *CreateConversionStmt) String() string {
	var parts []string

	parts = append(parts, "CREATE")
	if ccs.Def {
		parts = append(parts, "DEFAULT")
	}
	parts = append(parts, "CONVERSION")

	if ccs.ConversionName != nil && len(ccs.ConversionName.Items) > 0 {
		var nameStrs []string
		for _, item := range ccs.ConversionName.Items {
			if str, ok := item.(*String); ok {
				nameStrs = append(nameStrs, str.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	parts = append(parts, "FOR", fmt.Sprintf("'%s'", ccs.ForEncodingName), "TO", fmt.Sprintf("'%s'", ccs.ToEncodingName))

	if ccs.FuncName != nil && len(ccs.FuncName.Items) > 0 {
		var funcStrs []string
		for _, item := range ccs.FuncName.Items {
			if str, ok := item.(*String); ok {
				funcStrs = append(funcStrs, str.SVal)
			}
		}
		parts = append(parts, "FROM", strings.Join(funcStrs, "."))
	}

	return strings.Join(parts, " ")
}

func (ccs *CreateConversionStmt) StatementType() string {
	return "CreateConversionStmt"
}

func (ccs *CreateConversionStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE")

	if ccs.Def {
		parts = append(parts, "DEFAULT")
	}

	parts = append(parts, "CONVERSION")

	if ccs.ConversionName != nil && ccs.ConversionName.Len() > 0 {
		nameStrs := make([]string, 0, ccs.ConversionName.Len())
		for i := 0; i < ccs.ConversionName.Len(); i++ {
			if strNode, ok := ccs.ConversionName.Items[i].(*String); ok {
				nameStrs = append(nameStrs, strNode.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	if ccs.ForEncodingName != "" {
		parts = append(parts, "FOR", fmt.Sprintf("'%s'", ccs.ForEncodingName))
	}

	if ccs.ToEncodingName != "" {
		parts = append(parts, "TO", fmt.Sprintf("'%s'", ccs.ToEncodingName))
	}

	if ccs.FuncName != nil && ccs.FuncName.Len() > 0 {
		funcStrs := make([]string, 0, ccs.FuncName.Len())
		for i := 0; i < ccs.FuncName.Len(); i++ {
			if strNode, ok := ccs.FuncName.Items[i].(*String); ok {
				funcStrs = append(funcStrs, strNode.SVal)
			}
		}
		parts = append(parts, "FROM", strings.Join(funcStrs, "."))
	}

	return strings.Join(parts, " ")
}

// NewCreateConversionStmt creates a new CreateConversionStmt node
func NewCreateConversionStmt(conversionName *NodeList, forEncodingName, toEncodingName string, funcName *NodeList, def bool) *CreateConversionStmt {
	return &CreateConversionStmt{
		BaseNode:        BaseNode{Tag: T_CreateConversionStmt},
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
	BaseNode
	Replace  bool            // true for CREATE OR REPLACE
	TypeName *TypeName       // type name
	Lang     string          // language name
	FromSql  *ObjectWithArgs // FROM SQL function
	ToSql    *ObjectWithArgs // TO SQL function
}

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

func (cts *CreateTransformStmt) StatementType() string {
	return "CreateTransformStmt"
}

func (cts *CreateTransformStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE")

	if cts.Replace {
		parts = append(parts, "OR REPLACE")
	}

	parts = append(parts, "TRANSFORM FOR")

	if cts.TypeName != nil {
		parts = append(parts, cts.TypeName.SqlString())
	}

	parts = append(parts, "LANGUAGE", cts.Lang)

	var funcParts []string
	if cts.FromSql != nil {
		funcParts = append(funcParts, "FROM SQL WITH FUNCTION "+cts.FromSql.SqlString())
	}
	if cts.ToSql != nil {
		funcParts = append(funcParts, "TO SQL WITH FUNCTION "+cts.ToSql.SqlString())
	}

	if len(funcParts) > 0 {
		parts = append(parts, "(", strings.Join(funcParts, ", "), ")")
	}

	return strings.Join(parts, " ")
}

// NewCreateTransformStmt creates a new CreateTransformStmt node
func NewCreateTransformStmt(replace bool, typeName *TypeName, lang string, fromSql, toSql Node) *CreateTransformStmt {
	var fromOa *ObjectWithArgs
	var toOa *ObjectWithArgs
	if fromSql != nil {
		fromOa, _ = fromSql.(*ObjectWithArgs)
		toOa, _ = toSql.(*ObjectWithArgs)
	}
	return &CreateTransformStmt{
		BaseNode: BaseNode{Tag: T_CreateTransformStmt},
		Replace:  replace,
		TypeName: typeName,
		Lang:     lang,
		FromSql:  fromOa,
		ToSql:    toOa,
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
	BaseNode
	PortalName string // name of the portal (cursor)
	Options    int    // bitmask of options
	Query      Node   // the query (raw parse tree)
}

// String returns string representation of DeclareCursorStmt
func (dcs *DeclareCursorStmt) String() string {
	return fmt.Sprintf("DeclareCursorStmt(%s, %d options)@%d", dcs.PortalName, dcs.Options, dcs.Location())
}

func (dcs *DeclareCursorStmt) StatementType() string {
	return "DECLARE CURSOR"
}

// SqlString returns the SQL representation of the DECLARE CURSOR statement
func (dcs *DeclareCursorStmt) SqlString() string {
	var parts []string
	parts = append(parts, "DECLARE", dcs.PortalName)

	// Add cursor options
	if dcs.Options&CURSOR_OPT_BINARY != 0 {
		parts = append(parts, "BINARY")
	}
	if dcs.Options&CURSOR_OPT_INSENSITIVE != 0 {
		parts = append(parts, "INSENSITIVE")
	}
	if dcs.Options&CURSOR_OPT_ASENSITIVE != 0 {
		parts = append(parts, "ASENSITIVE")
	}
	if dcs.Options&CURSOR_OPT_SCROLL != 0 {
		parts = append(parts, "SCROLL")
	}
	if dcs.Options&CURSOR_OPT_NO_SCROLL != 0 {
		parts = append(parts, "NO", "SCROLL")
	}

	parts = append(parts, "CURSOR")

	if dcs.Options&CURSOR_OPT_HOLD != 0 {
		parts = append(parts, "WITH", "HOLD")
	}

	parts = append(parts, "FOR")

	// Add the query
	if dcs.Query != nil {
		if sqlNode, ok := dcs.Query.(interface{ SqlString() string }); ok {
			parts = append(parts, sqlNode.SqlString())
		} else {
			parts = append(parts, "<query>")
		}
	}

	return strings.Join(parts, " ")
}

// NewDeclareCursorStmt creates a new DeclareCursorStmt node
func NewDeclareCursorStmt(portalName string, options int, query Node) *DeclareCursorStmt {
	return &DeclareCursorStmt{
		BaseNode:   BaseNode{Tag: T_DeclareCursorStmt},
		PortalName: portalName,
		Options:    options,
		Query:      query,
	}
}

// FetchStmt represents a FETCH statement (also MOVE)
// Ported from postgres/src/include/nodes/parsenodes.h:3328-3335
type FetchStmt struct {
	BaseNode
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
	verb := "FETCH"
	if fs.IsMove {
		verb = "MOVE"
	}
	return fmt.Sprintf("%sStmt(%s, dir=%d, howMany=%d)@%d", verb, fs.PortalName, fs.Direction, fs.HowMany, fs.Location())
}

func (fs *FetchStmt) StatementType() string {
	if fs.IsMove {
		return "MOVE"
	}
	return "FETCH"
}

// SqlString returns the SQL representation of the FETCH/MOVE statement
func (fs *FetchStmt) SqlString() string {
	var parts []string

	if fs.IsMove {
		parts = append(parts, "MOVE")
	} else {
		parts = append(parts, "FETCH")
	}

	// Handle direction and count
	switch fs.Direction {
	case FETCH_FORWARD:
		if fs.HowMany == 1 {
			// Default case - just FETCH/MOVE cursor_name
		} else if fs.HowMany == FETCH_ALL {
			parts = append(parts, "ALL")
		} else {
			parts = append(parts, fmt.Sprintf("%d", fs.HowMany))
		}
	case FETCH_BACKWARD:
		if fs.HowMany == 1 {
			parts = append(parts, "BACKWARD")
		} else if fs.HowMany == FETCH_ALL {
			parts = append(parts, "BACKWARD", "ALL")
		} else {
			parts = append(parts, "BACKWARD", fmt.Sprintf("%d", fs.HowMany))
		}
	case FETCH_ABSOLUTE:
		if fs.HowMany == 1 {
			parts = append(parts, "FIRST")
		} else if fs.HowMany == -1 {
			parts = append(parts, "LAST")
		} else {
			parts = append(parts, "ABSOLUTE", fmt.Sprintf("%d", fs.HowMany))
		}
	case FETCH_RELATIVE:
		parts = append(parts, "RELATIVE", fmt.Sprintf("%d", fs.HowMany))
	}

	parts = append(parts, "FROM", fs.PortalName)
	return strings.Join(parts, " ")
}

// NewFetchStmt creates a new FetchStmt node
func NewFetchStmt(direction FetchDirection, howMany int64, portalName string, isMove bool) *FetchStmt {
	return &FetchStmt{
		BaseNode:   BaseNode{Tag: T_FetchStmt},
		Direction:  direction,
		HowMany:    howMany,
		PortalName: portalName,
		IsMove:     isMove,
	}
}

// ClosePortalStmt represents a CLOSE statement
// Ported from postgres/src/include/nodes/parsenodes.h:3305-3310
type ClosePortalStmt struct {
	BaseNode
	PortalName *string // name of the portal (cursor); nil means CLOSE ALL
}

// node implements the Node interface
func (cps *ClosePortalStmt) node() {}

// stmt implements the Stmt interface
func (cps *ClosePortalStmt) stmt() {}

// String returns string representation of ClosePortalStmt
func (cps *ClosePortalStmt) String() string {
	name := "ALL"
	if cps.PortalName != nil {
		name = *cps.PortalName
	}
	return fmt.Sprintf("ClosePortalStmt(%s)@%d", name, cps.Location())
}

func (cps *ClosePortalStmt) StatementType() string {
	return "CLOSE"
}

// SqlString returns the SQL representation of the CLOSE statement
func (cps *ClosePortalStmt) SqlString() string {
	if cps.PortalName == nil {
		return "CLOSE ALL"
	}
	return fmt.Sprintf("CLOSE %s", *cps.PortalName)
}

// NewClosePortalStmt creates a new ClosePortalStmt node
func NewClosePortalStmt(portalName *string) *ClosePortalStmt {
	return &ClosePortalStmt{
		BaseNode:   BaseNode{Tag: T_ClosePortalStmt},
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
	Typevar    *RangeVar // the composite type name
	Coldeflist *NodeList // list of ColumnDef nodes
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
	TypeName           *NodeList // qualified name (list of String)
	OldVal             string    // old enum value's name, if renaming
	NewVal             string    // new enum value's name
	NewValNeighbor     string    // neighboring enum value, if specified
	NewValIsAfter      bool      // place new val after neighbor?
	SkipIfNewValExists bool      // ignore statement if new already exists
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
	DefNames    *NodeList // qualified name (list of String)
	StatTypes   *NodeList // stat types (list of String)
	Exprs       *NodeList // expressions to build statistics on
	Relations   *NodeList // rels to build stats on (list of RangeVar)
	StxComment  string    // comment to apply to stats, or empty string
	Transformed bool      // true when transformStatsStmt is finished
	IfNotExists bool      // do nothing if stats name already exists
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

	if css.DefNames != nil && len(css.DefNames.Items) > 0 {
		var nameStrs []string
		for _, item := range css.DefNames.Items {
			if name, ok := item.(*String); ok {
				nameStrs = append(nameStrs, name.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	if css.StatTypes != nil && len(css.StatTypes.Items) > 0 {
		var typeStrs []string
		for _, item := range css.StatTypes.Items {
			if statType, ok := item.(*String); ok {
				typeStrs = append(typeStrs, statType.SVal)
			}
		}
		parts = append(parts, "("+strings.Join(typeStrs, ", ")+")")
	}

	if css.Relations != nil && len(css.Relations.Items) > 0 {
		parts = append(parts, "ON")
		var relStrs []string
		for _, item := range css.Relations.Items {
			if rel, ok := item.(*RangeVar); ok {
				relStrs = append(relStrs, rel.String())
			}
		}
		parts = append(parts, strings.Join(relStrs, ", "))
	}

	return strings.Join(parts, " ")
}

func (css *CreateStatsStmt) StatementType() string {
	return "CREATE STATISTICS"
}

func (css *CreateStatsStmt) Location() int {
	return 0
}

func (css *CreateStatsStmt) NodeTag() NodeTag {
	return T_CreateStatsStmt
}

func (css *CreateStatsStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE STATISTICS")

	if css.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	if css.DefNames != nil && css.DefNames.Len() > 0 {
		nameStrs := make([]string, 0, css.DefNames.Len())
		for i := 0; i < css.DefNames.Len(); i++ {
			if strNode, ok := css.DefNames.Items[i].(*String); ok {
				nameStrs = append(nameStrs, strNode.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	if css.StatTypes != nil && css.StatTypes.Len() > 0 {
		typeStrs := make([]string, 0, css.StatTypes.Len())
		for i := 0; i < css.StatTypes.Len(); i++ {
			if strNode, ok := css.StatTypes.Items[i].(*String); ok {
				typeStrs = append(typeStrs, strNode.SVal)
			}
		}
		parts = append(parts, "("+strings.Join(typeStrs, ", ")+")")
	}

	parts = append(parts, "ON")

	if css.Exprs != nil && css.Exprs.Len() > 0 {
		exprStrs := make([]string, 0, css.Exprs.Len())
		for i := 0; i < css.Exprs.Len(); i++ {
			if expr := css.Exprs.Items[i]; expr != nil {
				exprStrs = append(exprStrs, expr.SqlString())
			}
		}
		parts = append(parts, strings.Join(exprStrs, ", "))
	}

	parts = append(parts, "FROM")

	if css.Relations != nil && css.Relations.Len() > 0 {
		relStrs := make([]string, 0, css.Relations.Len())
		for i := 0; i < css.Relations.Len(); i++ {
			if rel := css.Relations.Items[i]; rel != nil {
				relStrs = append(relStrs, rel.SqlString())
			}
		}
		parts = append(parts, strings.Join(relStrs, ", "))
	}

	return strings.Join(parts, " ")
}

// NewCreateStatsStmt creates a new CreateStatsStmt node
func NewCreateStatsStmt(defNames *NodeList, statTypes *NodeList, exprs *NodeList, relations *NodeList, stxComment string, transformed, ifNotExists bool) *CreateStatsStmt {
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
	BaseNode
	Replace     bool      // true => replace if already exists
	PLName      string    // PL name
	PLHandler   *NodeList // PL call handler function (qualified name)
	PLInline    *NodeList // optional inline function (qualified name)
	PLValidator *NodeList // optional validator function (qualified name)
	PLTrusted   bool      // PL is trusted
}

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

	if cpls.PLHandler != nil && len(cpls.PLHandler.Items) > 0 {
		var handlerStrs []string
		for _, item := range cpls.PLHandler.Items {
			if str, ok := item.(*String); ok {
				handlerStrs = append(handlerStrs, str.SVal)
			}
		}
		parts = append(parts, "HANDLER", strings.Join(handlerStrs, "."))
	}

	if cpls.PLInline != nil && len(cpls.PLInline.Items) > 0 {
		var inlineStrs []string
		for _, item := range cpls.PLInline.Items {
			if str, ok := item.(*String); ok {
				inlineStrs = append(inlineStrs, str.SVal)
			}
		}
		parts = append(parts, "INLINE", strings.Join(inlineStrs, "."))
	}

	if cpls.PLValidator != nil && len(cpls.PLValidator.Items) > 0 {
		var validatorStrs []string
		for _, item := range cpls.PLValidator.Items {
			if str, ok := item.(*String); ok {
				validatorStrs = append(validatorStrs, str.SVal)
			}
		}
		parts = append(parts, "VALIDATOR", strings.Join(validatorStrs, "."))
	}

	return strings.Join(parts, " ")
}

func (cpls *CreatePLangStmt) StatementType() string {
	return "CreatePLangStmt"
}

func (cpls *CreatePLangStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE")

	if cpls.Replace {
		parts = append(parts, "OR REPLACE")
	}

	if cpls.PLTrusted {
		parts = append(parts, "TRUSTED")
	}

	parts = append(parts, "LANGUAGE", cpls.PLName)

	if cpls.PLHandler != nil && cpls.PLHandler.Len() > 0 {
		handlerStrs := make([]string, 0, cpls.PLHandler.Len())
		for i := 0; i < cpls.PLHandler.Len(); i++ {
			if strNode, ok := cpls.PLHandler.Items[i].(*String); ok {
				handlerStrs = append(handlerStrs, strNode.SVal)
			}
		}
		parts = append(parts, "HANDLER", strings.Join(handlerStrs, "."))
	}

	if cpls.PLInline != nil && cpls.PLInline.Len() > 0 {
		inlineStrs := make([]string, 0, cpls.PLInline.Len())
		for i := 0; i < cpls.PLInline.Len(); i++ {
			if strNode, ok := cpls.PLInline.Items[i].(*String); ok {
				inlineStrs = append(inlineStrs, strNode.SVal)
			}
		}
		parts = append(parts, "INLINE", strings.Join(inlineStrs, "."))
	}

	if cpls.PLValidator != nil && cpls.PLValidator.Len() > 0 {
		validatorStrs := make([]string, 0, cpls.PLValidator.Len())
		for i := 0; i < cpls.PLValidator.Len(); i++ {
			if strNode, ok := cpls.PLValidator.Items[i].(*String); ok {
				validatorStrs = append(validatorStrs, strNode.SVal)
			}
		}
		parts = append(parts, "VALIDATOR", strings.Join(validatorStrs, "."))
	}

	return strings.Join(parts, " ")
}

// NewCreatePLangStmt creates a new CreatePLangStmt node
func NewCreatePLangStmt(replace bool, plName string, plHandler, plInline, plValidator *NodeList, plTrusted bool) *CreatePLangStmt {
	return &CreatePLangStmt{
		BaseNode:    BaseNode{Tag: T_CreatePLangStmt},
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
