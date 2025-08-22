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
	COERCION_IMPLICIT    CoercionContext = iota // coercion in context of expression
	COERCION_ASSIGNMENT                         // coercion in context of assignment
	COERCION_PLPGSQL                            // if no assignment cast, use CoerceViaIO
	COERCION_EXPLICIT                           // explicit cast operation
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
	FUNC_PARAM_OUT                                    // output only
	FUNC_PARAM_INOUT                                  // both
	FUNC_PARAM_VARIADIC                               // variadic (always input)
	FUNC_PARAM_TABLE                                  // table function output column
	FUNC_PARAM_DEFAULT                                // default; effectively same as IN
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
	Name     *string                // parameter name, or nil if not given
	ArgType  *TypeName              // type name for parameter type
	Mode     FunctionParameterMode  // IN/OUT/INOUT/VARIADIC/TABLE/DEFAULT
	DefExpr  Node                   // raw default expr, or nil if not given
}

// String returns string representation of FunctionParameter
func (fp *FunctionParameter) String() string {
	var parts []string
	
	if fp.Mode != FUNC_PARAM_IN && fp.Mode != FUNC_PARAM_DEFAULT {
		parts = append(parts, fp.Mode.String())
	}
	
	if fp.Name != nil {
		parts = append(parts, *fp.Name)
	}
	
	if fp.ArgType != nil {
		parts = append(parts, fp.ArgType.String())
	}
	
	if fp.DefExpr != nil {
		parts = append(parts, "DEFAULT", fp.DefExpr.String())
	}
	
	return strings.Join(parts, " ")
}

// NewFunctionParameter creates a new FunctionParameter node
func NewFunctionParameter(name *string, argType *TypeName, mode FunctionParameterMode, defExpr Node) *FunctionParameter {
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
	IsProcedure bool                 // true for CREATE PROCEDURE
	Replace     bool                 // true for CREATE OR REPLACE
	FuncName    []*String            // qualified name of function to create
	Parameters  []*FunctionParameter // list of function parameters
	ReturnType  *TypeName            // the return type
	Options     []*DefElem           // list of definition elements
	SQLBody     Node                 // SQL body for SQL functions
}

// node implements the Node interface
func (cfs *CreateFunctionStmt) node() {}

// stmt implements the Stmt interface
func (cfs *CreateFunctionStmt) stmt() {}

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
	
	if len(cfs.FuncName) > 0 {
		var nameStrs []string
		for _, name := range cfs.FuncName {
			nameStrs = append(nameStrs, name.SVal)
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	// Parameters
	if len(cfs.Parameters) > 0 {
		var paramStrs []string
		for _, param := range cfs.Parameters {
			paramStrs = append(paramStrs, param.String())
		}
		parts = append(parts, "("+strings.Join(paramStrs, ", ")+")")
	} else {
		parts = append(parts, "()")
	}
	
	// Return type (only for functions, not procedures)
	if !cfs.IsProcedure && cfs.ReturnType != nil {
		if len(cfs.ReturnType.Names) > 0 {
			typeName := cfs.ReturnType.Names[len(cfs.ReturnType.Names)-1]
			parts = append(parts, "RETURNS", typeName)
		}
	}
	
	return strings.Join(parts, " ")
}

// NewCreateFunctionStmt creates a new CreateFunctionStmt node
func NewCreateFunctionStmt(isProcedure, replace bool, funcName []*String, parameters []*FunctionParameter, returnType *TypeName, options []*DefElem, sqlBody Node) *CreateFunctionStmt {
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
	Sequence     *RangeVar   // the sequence to create
	Options      []*DefElem  // list of options
	OwnerID      Oid         // ID of owner, or 0 for default (InvalidOid)
	ForIdentity  bool        // true if for IDENTITY column
	IfNotExists  bool        // true for IF NOT EXISTS
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
	ItemType     int              // OPCLASS_ITEM_OPERATOR, OPCLASS_ITEM_FUNCTION, or OPCLASS_ITEM_STORAGETYPE
	Name         *ObjectWithArgs  // operator or function name and args
	Number       int              // strategy num or support proc num
	OrderFamily  []*String        // only used for ordering operators
	ClassArgs    []*TypeName      // amproclefttype/amprocrighttype or amoplefttype/amoprighttype
	StoredType   *TypeName        // datatype stored in index (for storage type items)
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
	OpClassName   []*String            // qualified name (list of String)
	OpFamilyName  []*String            // qualified name (list of String); nil if omitted
	AmName        string               // name of index AM opclass is for
	DataType      *TypeName            // datatype of indexed column
	Items         []*CreateOpClassItem // list of CreateOpClassItem nodes
	IsDefault     bool                 // should be marked as default for type?
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
	if cocs.DataType != nil && len(cocs.DataType.Names) > 0 {
		typeName := cocs.DataType.Names[len(cocs.DataType.Names)-1]
		parts = append(parts, typeName)
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
	SourceType *TypeName        // source data type
	TargetType *TypeName        // target data type
	Func       *ObjectWithArgs  // conversion function, or nil
	Context    CoercionContext  // coercion context
	Inout      bool             // true for INOUT cast
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
		if len(ccs.SourceType.Names) > 0 {
			sourceTypeName = ccs.SourceType.Names[len(ccs.SourceType.Names)-1]
		}
		targetTypeName := ""
		if len(ccs.TargetType.Names) > 0 {
			targetTypeName = ccs.TargetType.Names[len(ccs.TargetType.Names)-1]
		}
		parts = append(parts, "("+sourceTypeName+" AS "+targetTypeName+")")
	}
	
	if ccs.Func != nil {
		funcName := ""
		if len(ccs.Func.Objname) > 0 && ccs.Func.Objname[len(ccs.Func.Objname)-1] != nil {
			funcName = ccs.Func.Objname[len(ccs.Func.Objname)-1].SVal
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
	ConversionName    []*String // name of the conversion
	ForEncodingName   string    // source encoding name
	ToEncodingName    string    // destination encoding name
	FuncName          []*String // qualified conversion function name
	Def               bool      // true if this is a default conversion
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
	Replace   bool             // true for CREATE OR REPLACE
	TypeName  *TypeName        // type name
	Lang      string           // language name
	FromSql   *ObjectWithArgs  // FROM SQL function
	ToSql     *ObjectWithArgs  // TO SQL function
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
	Kind        ObjectType   // aggregate, operator, type
	OldStyle    bool         // hack to signal old CREATE AGG syntax
	DefNames    []*String    // qualified name (list of String)
	Args        []*TypeName  // list of TypeName (if needed)
	Definition  []*DefElem   // list of DefElem
	IfNotExists bool         // true for IF NOT EXISTS
	Replace     bool         // true for CREATE OR REPLACE
}

// node implements the Node interface
func (ds *DefineStmt) node() {}

// stmt implements the Stmt interface
func (ds *DefineStmt) stmt() {}

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
	default:
		parts = append(parts, "OBJECT")
	}
	
	if ds.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}
	
	if len(ds.DefNames) > 0 {
		var nameStrs []string
		for _, name := range ds.DefNames {
			nameStrs = append(nameStrs, name.SVal)
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	return strings.Join(parts, " ")
}

// NewDefineStmt creates a new DefineStmt node
func NewDefineStmt(kind ObjectType, oldStyle bool, defNames []*String, args []*TypeName, definition []*DefElem, ifNotExists, replace bool) *DefineStmt {
	return &DefineStmt{
		Kind:        kind,
		OldStyle:    oldStyle,
		DefNames:    defNames,
		Args:        args,
		Definition:  definition,
		IfNotExists: ifNotExists,
		Replace:     replace,
	}
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
	TypeName []*String // qualified name (list of String)
	Vals     []*String // enum values (list of String)
}

// node implements the Node interface
func (ces *CreateEnumStmt) node() {}

// stmt implements the Stmt interface
func (ces *CreateEnumStmt) stmt() {}

// String returns string representation of CreateEnumStmt
func (ces *CreateEnumStmt) String() string {
	var parts []string
	
	parts = append(parts, "CREATE TYPE")
	
	if len(ces.TypeName) > 0 {
		var nameStrs []string
		for _, name := range ces.TypeName {
			nameStrs = append(nameStrs, name.SVal)
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	parts = append(parts, "AS ENUM")
	
	if len(ces.Vals) > 0 {
		var valStrs []string
		for _, val := range ces.Vals {
			valStrs = append(valStrs, "'"+val.SVal+"'")
		}
		parts = append(parts, "("+strings.Join(valStrs, ", ")+")")
	} else {
		parts = append(parts, "()")
	}
	
	return strings.Join(parts, " ")
}

// NewCreateEnumStmt creates a new CreateEnumStmt node
func NewCreateEnumStmt(typeName []*String, vals []*String) *CreateEnumStmt {
	return &CreateEnumStmt{
		TypeName: typeName,
		Vals:     vals,
	}
}

// CreateRangeStmt represents a CREATE TYPE ... AS RANGE statement
// Ported from postgres/src/include/nodes/parsenodes.h:3707-3712
type CreateRangeStmt struct {
	TypeName []*String   // qualified name (list of String)
	Params   []*DefElem  // range parameters (list of DefElem)
}

// node implements the Node interface
func (crs *CreateRangeStmt) node() {}

// stmt implements the Stmt interface
func (crs *CreateRangeStmt) stmt() {}

// String returns string representation of CreateRangeStmt
func (crs *CreateRangeStmt) String() string {
	var parts []string
	
	parts = append(parts, "CREATE TYPE")
	
	if len(crs.TypeName) > 0 {
		var nameStrs []string
		for _, name := range crs.TypeName {
			nameStrs = append(nameStrs, name.SVal)
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	parts = append(parts, "AS RANGE")
	
	if len(crs.Params) > 0 {
		parts = append(parts, "(...)")  // Simplified representation
	}
	
	return strings.Join(parts, " ")
}

// NewCreateRangeStmt creates a new CreateRangeStmt node
func NewCreateRangeStmt(typeName []*String, params []*DefElem) *CreateRangeStmt {
	return &CreateRangeStmt{
		TypeName: typeName,
		Params:   params,
	}
}

// CreateStatsStmt represents a CREATE STATISTICS statement
// Ported from postgres/src/include/nodes/parsenodes.h:3384-3394
type CreateStatsStmt struct {
	DefNames     []*String    // qualified name (list of String)
	StatTypes    []*String    // stat types (list of String)
	Exprs        *NodeList    // expressions to build statistics on
	Relations    []*RangeVar  // rels to build stats on (list of RangeVar)
	StxComment   *string      // comment to apply to stats, or nil
	Transformed  bool         // true when transformStatsStmt is finished
	IfNotExists  bool         // do nothing if stats name already exists
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
	Replace     bool        // true => replace if already exists
	PLName      string      // PL name
	PLHandler   []*String   // PL call handler function (qualified name)
	PLInline    []*String   // optional inline function (qualified name)
	PLValidator []*String   // optional validator function (qualified name)
	PLTrusted   bool        // PL is trusted
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