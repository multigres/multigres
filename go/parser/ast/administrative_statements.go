// Package ast provides PostgreSQL AST administrative and advanced DDL statement definitions.
// These nodes handle advanced table operations, partitioning, foreign data wrappers,
// triggers, policies, and other administrative PostgreSQL features.
// Ported from postgres/src/include/nodes/parsenodes.h
package ast

import (
	"fmt"
	"strings"
)

// ==============================================================================
// ADVANCED ALTER TABLE OPERATIONS
// ==============================================================================

// Using existing AlterTableType and AlterTableCmd from ddl_statements.go
// Enhanced with additional constructor functions for advanced ALTER TABLE operations

// ==============================================================================
// TABLE DEFINITION NODES
// ==============================================================================

// ColumnDef is already defined as placeholder in statements.go
// Will be enhanced separately to replace the placeholder

// TableLikeClause represents LIKE clauses in CREATE TABLE.
// This allows table inheritance and copying of structure.
// Ported from postgres/src/include/nodes/parsenodes.h:751
type TableLikeClause struct {
	BaseNode
	Relation *RangeVar       // Table to copy from - parsenodes.h:752
	Options  TableLikeOption // OR of TableLikeOption flags - parsenodes.h:753
}

// TableLikeOption represents options for LIKE clauses.
// Ported from postgres/src/include/nodes/parsenodes.h:755-763
type TableLikeOption int

const (
	CREATE_TABLE_LIKE_COMMENTS    TableLikeOption = 1 << 0 // INCLUDING COMMENTS - parsenodes.h:756
	CREATE_TABLE_LIKE_COMPRESSION TableLikeOption = 1 << 1 // INCLUDING COMPRESSION - parsenodes.h:757
	CREATE_TABLE_LIKE_CONSTRAINTS TableLikeOption = 1 << 2 // INCLUDING CONSTRAINTS - parsenodes.h:758
	CREATE_TABLE_LIKE_DEFAULTS    TableLikeOption = 1 << 3 // INCLUDING DEFAULTS - parsenodes.h:759
	CREATE_TABLE_LIKE_GENERATED   TableLikeOption = 1 << 4 // INCLUDING GENERATED - parsenodes.h:760
	CREATE_TABLE_LIKE_IDENTITY    TableLikeOption = 1 << 5 // INCLUDING IDENTITY - parsenodes.h:761
	CREATE_TABLE_LIKE_INDEXES     TableLikeOption = 1 << 6 // INCLUDING INDEXES - parsenodes.h:762
	CREATE_TABLE_LIKE_STATISTICS  TableLikeOption = 1 << 7 // INCLUDING STATISTICS - parsenodes.h:763
	CREATE_TABLE_LIKE_STORAGE     TableLikeOption = 1 << 8 // INCLUDING STORAGE - parsenodes.h:764
	CREATE_TABLE_LIKE_ALL         TableLikeOption = 1 << 9 // INCLUDING ALL - parsenodes.h:765
)

// NewTableLikeClause creates a new TableLikeClause node.
func NewTableLikeClause(relation *RangeVar, options TableLikeOption) *TableLikeClause {
	return &TableLikeClause{
		BaseNode: BaseNode{Tag: T_TableLikeClause},
		Relation: relation,
		Options:  options,
	}
}

// NewTableLikeAll creates a LIKE clause including all options.
func NewTableLikeAll(relation *RangeVar) *TableLikeClause {
	return &TableLikeClause{
		BaseNode: BaseNode{Tag: T_TableLikeClause},
		Relation: relation,
		Options:  CREATE_TABLE_LIKE_ALL,
	}
}

func (tlc *TableLikeClause) String() string {
	optionStrs := []string{}
	if tlc.Options&CREATE_TABLE_LIKE_COMMENTS != 0 {
		optionStrs = append(optionStrs, "COMMENTS")
	}
	if tlc.Options&CREATE_TABLE_LIKE_CONSTRAINTS != 0 {
		optionStrs = append(optionStrs, "CONSTRAINTS")
	}
	if tlc.Options&CREATE_TABLE_LIKE_DEFAULTS != 0 {
		optionStrs = append(optionStrs, "DEFAULTS")
	}
	if tlc.Options&CREATE_TABLE_LIKE_INDEXES != 0 {
		optionStrs = append(optionStrs, "INDEXES")
	}

	options := ""
	if len(optionStrs) > 0 {
		options = fmt.Sprintf(" INCLUDING %v", optionStrs)
	}

	return fmt.Sprintf("TableLikeClause(LIKE %s%s)", tlc.Relation, options)
}

// ==============================================================================
// PARTITIONING SUPPORT
// ==============================================================================

// PartitionStrategy represents partitioning strategies.
// Ported from postgres/src/include/nodes/parsenodes.h:865-869
type PartitionStrategy string

const (
	PARTITION_STRATEGY_LIST  PartitionStrategy = "list"  // LIST partitioning - parsenodes.h:866
	PARTITION_STRATEGY_RANGE PartitionStrategy = "range" // RANGE partitioning - parsenodes.h:867
	PARTITION_STRATEGY_HASH  PartitionStrategy = "hash"  // HASH partitioning - parsenodes.h:868
)

// PartitionSpec represents table partitioning specifications.
// Modern PostgreSQL feature for table partitioning.
// Ported from postgres/src/include/nodes/parsenodes.h:882
type PartitionSpec struct {
	BaseNode
	Strategy   PartitionStrategy // Partitioning strategy - parsenodes.h:884
	PartParams *NodeList         // List of PartitionElem nodes - parsenodes.h:885
	// Location is provided by BaseNode.Location() method
}

// NewPartitionSpec creates a new PartitionSpec node.
func NewPartitionSpec(strategy PartitionStrategy, partParams *NodeList) *PartitionSpec {
	return &PartitionSpec{
		BaseNode:   BaseNode{Tag: T_PartitionSpec},
		Strategy:   strategy,
		PartParams: partParams,
	}
}

// NewListPartitionSpec creates a new LIST partition specification.
func NewListPartitionSpec(partParams *NodeList) *PartitionSpec {
	return &PartitionSpec{
		BaseNode:   BaseNode{Tag: T_PartitionSpec},
		Strategy:   PARTITION_STRATEGY_LIST,
		PartParams: partParams,
	}
}

// NewRangePartitionSpec creates a new RANGE partition specification.
func NewRangePartitionSpec(partParams *NodeList) *PartitionSpec {
	return &PartitionSpec{
		BaseNode:   BaseNode{Tag: T_PartitionSpec},
		Strategy:   PARTITION_STRATEGY_RANGE,
		PartParams: partParams,
	}
}

// NewHashPartitionSpec creates a new HASH partition specification.
func NewHashPartitionSpec(partParams *NodeList) *PartitionSpec {
	return &PartitionSpec{
		BaseNode:   BaseNode{Tag: T_PartitionSpec},
		Strategy:   PARTITION_STRATEGY_HASH,
		PartParams: partParams,
	}
}

func (ps *PartitionSpec) String() string {
	count := 0
	if ps.PartParams != nil {
		count = len(ps.PartParams.Items)
	}
	return fmt.Sprintf("PartitionSpec(PARTITION BY %s, %d params)", ps.Strategy, count)
}

// PartitionBoundSpec represents partition boundary specifications.
// This defines the actual bounds for individual partitions.
// Ported from postgres/src/include/nodes/parsenodes.h:896
type PartitionBoundSpec struct {
	BaseNode
	Strategy   PartitionStrategy // Partitioning strategy - parsenodes.h:898
	IsDefault  bool              // Is this a default partition? - parsenodes.h:899
	Modulus    int               // Hash partition modulus - parsenodes.h:900
	Remainder  int               // Hash partition remainder - parsenodes.h:901
	ListDatums *NodeList         // List of list datums per column - parsenodes.h:902
	LowDatums  *NodeList         // List of lower datums for range bounds - parsenodes.h:903
	HighDatums *NodeList         // List of upper datums for range bounds - parsenodes.h:904
	// Location is provided by BaseNode.Location() method
}

// NewPartitionBoundSpec creates a new PartitionBoundSpec node.
func NewPartitionBoundSpec(strategy PartitionStrategy) *PartitionBoundSpec {
	return &PartitionBoundSpec{
		BaseNode: BaseNode{Tag: T_PartitionBoundSpec},
		Strategy: strategy,
	}
}

// NewDefaultPartitionBound creates a new default partition bound.
func NewDefaultPartitionBound() *PartitionBoundSpec {
	return &PartitionBoundSpec{
		BaseNode:  BaseNode{Tag: T_PartitionBoundSpec},
		IsDefault: true,
	}
}

// NewHashPartitionBound creates a new hash partition bound.
func NewHashPartitionBound(modulus, remainder int) *PartitionBoundSpec {
	return &PartitionBoundSpec{
		BaseNode:  BaseNode{Tag: T_PartitionBoundSpec},
		Strategy:  PARTITION_STRATEGY_HASH,
		Modulus:   modulus,
		Remainder: remainder,
	}
}

// NewListPartitionBound creates a new list partition bound.
func NewListPartitionBound(listDatums *NodeList) *PartitionBoundSpec {
	return &PartitionBoundSpec{
		BaseNode:   BaseNode{Tag: T_PartitionBoundSpec},
		Strategy:   PARTITION_STRATEGY_LIST,
		ListDatums: listDatums,
	}
}

// NewRangePartitionBound creates a new range partition bound.
func NewRangePartitionBound(lowDatums, highDatums *NodeList) *PartitionBoundSpec {
	return &PartitionBoundSpec{
		BaseNode:   BaseNode{Tag: T_PartitionBoundSpec},
		Strategy:   PARTITION_STRATEGY_RANGE,
		LowDatums:  lowDatums,
		HighDatums: highDatums,
	}
}

func (pbs *PartitionBoundSpec) String() string {
	if pbs.IsDefault {
		return "PartitionBoundSpec(DEFAULT)"
	}

	switch pbs.Strategy {
	case PARTITION_STRATEGY_HASH:
		return fmt.Sprintf("PartitionBoundSpec(HASH modulus=%d remainder=%d)", pbs.Modulus, pbs.Remainder)
	case PARTITION_STRATEGY_LIST:
		count := 0
		if pbs.ListDatums != nil {
			count = pbs.ListDatums.Len()
		}
		return fmt.Sprintf("PartitionBoundSpec(LIST %d datums)", count)
	case PARTITION_STRATEGY_RANGE:
		lowCount := 0
		highCount := 0
		if pbs.LowDatums != nil {
			lowCount = len(pbs.LowDatums.Items)
		}
		if pbs.HighDatums != nil {
			highCount = len(pbs.HighDatums.Items)
		}
		return fmt.Sprintf("PartitionBoundSpec(RANGE low=%d high=%d)", lowCount, highCount)
	default:
		return fmt.Sprintf("PartitionBoundSpec(%s)", pbs.Strategy)
	}
}

func (pbs *PartitionBoundSpec) SqlString() string {
	if pbs.IsDefault {
		return "DEFAULT"
	}

	parts := []string{"FOR VALUES"}
	
	switch pbs.Strategy {
	case PARTITION_STRATEGY_HASH:
		parts = append(parts, fmt.Sprintf("WITH (modulus %d, remainder %d)", pbs.Modulus, pbs.Remainder))
		
	case PARTITION_STRATEGY_LIST:
		if pbs.ListDatums != nil && pbs.ListDatums.Len() > 0 {
			datumStrings := []string{}
			for _, item := range pbs.ListDatums.Items {
				if datumList, ok := item.(*NodeList); ok && datumList != nil && len(datumList.Items) > 0 {
					for _, datum := range datumList.Items {
						datumStrings = append(datumStrings, datum.SqlString())
					}
				} else if datum := item; datum != nil {
					// Handle single datum (not nested in another NodeList)
					datumStrings = append(datumStrings, datum.SqlString())
				}
			}
			parts = append(parts, "IN ("+strings.Join(datumStrings, ", ")+")")
		}
		
	case PARTITION_STRATEGY_RANGE:
		var rangeParts []string
		if pbs.LowDatums != nil && len(pbs.LowDatums.Items) > 0 {
			lowStrings := []string{}
			for _, datum := range pbs.LowDatums.Items {
				lowStrings = append(lowStrings, datum.SqlString())
			}
			rangeParts = append(rangeParts, "FROM ("+strings.Join(lowStrings, ", ")+")")
		}
		if pbs.HighDatums != nil && len(pbs.HighDatums.Items) > 0 {
			highStrings := []string{}
			for _, datum := range pbs.HighDatums.Items {
				highStrings = append(highStrings, datum.SqlString())
			}
			rangeParts = append(rangeParts, "TO ("+strings.Join(highStrings, ", ")+")")
		}
		parts = append(parts, strings.Join(rangeParts, " "))
		
	default:
		parts = append(parts, fmt.Sprintf("/* Unknown strategy %s */", pbs.Strategy))
	}
	
	return strings.Join(parts, " ")
}

// PartitionRangeDatum represents partition range datum values.
// Ported from postgres/src/include/nodes/parsenodes.h:929
type PartitionRangeDatum struct {
	BaseNode
	Kind     PartitionRangeDatumKind // What kind of datum this is - parsenodes.h:931
	Value    Node                    // The actual datum value - parsenodes.h:932
	// Location is provided by BaseNode.Location() method
}

// PartitionRangeDatumKind represents types of range datums.
// Ported from postgres/src/include/nodes/parsenodes.h:935-939
type PartitionRangeDatumKind int

const (
	PARTITION_RANGE_DATUM_MINVALUE PartitionRangeDatumKind = iota // MINVALUE - parsenodes.h:936
	PARTITION_RANGE_DATUM_VALUE                                   // Specific value - parsenodes.h:937
	PARTITION_RANGE_DATUM_MAXVALUE                                // MAXVALUE - parsenodes.h:938
)

// NewPartitionRangeDatum creates a new PartitionRangeDatum node.
func NewPartitionRangeDatum(kind PartitionRangeDatumKind, value Node) *PartitionRangeDatum {
	return &PartitionRangeDatum{
		BaseNode: BaseNode{Tag: T_PartitionRangeDatum},
		Kind:     kind,
		Value:    value,
	}
}

// NewMinValueDatum creates a MINVALUE range datum.
func NewMinValueDatum() *PartitionRangeDatum {
	return &PartitionRangeDatum{
		BaseNode: BaseNode{Tag: T_PartitionRangeDatum},
		Kind:     PARTITION_RANGE_DATUM_MINVALUE,
	}
}

// NewMaxValueDatum creates a MAXVALUE range datum.
func NewMaxValueDatum() *PartitionRangeDatum {
	return &PartitionRangeDatum{
		BaseNode: BaseNode{Tag: T_PartitionRangeDatum},
		Kind:     PARTITION_RANGE_DATUM_MAXVALUE,
	}
}

// NewValueDatum creates a specific value range datum.
func NewValueDatum(value Node) *PartitionRangeDatum {
	return &PartitionRangeDatum{
		BaseNode: BaseNode{Tag: T_PartitionRangeDatum},
		Kind:     PARTITION_RANGE_DATUM_VALUE,
		Value:    value,
	}
}

func (prd *PartitionRangeDatum) String() string {
	switch prd.Kind {
	case PARTITION_RANGE_DATUM_MINVALUE:
		return "PartitionRangeDatum(MINVALUE)"
	case PARTITION_RANGE_DATUM_MAXVALUE:
		return "PartitionRangeDatum(MAXVALUE)"
	case PARTITION_RANGE_DATUM_VALUE:
		return fmt.Sprintf("PartitionRangeDatum(VALUE %s)", prd.Value)
	default:
		return fmt.Sprintf("PartitionRangeDatum(KIND_%d)", int(prd.Kind))
	}
}

// ==============================================================================
// STATISTICS SUPPORT
// ==============================================================================

// StatsElem represents statistics element specifications.
// This is used for extended statistics in PostgreSQL.
// Ported from postgres/src/include/nodes/parsenodes.h:3403
type StatsElem struct {
	BaseNode
	Name string // Name of attribute to compute stats for - parsenodes.h:3405
	Expr Node   // Or expression to compute stats for - parsenodes.h:3406
}

// NewStatsElem creates a new StatsElem node.
func NewStatsElem(name string) *StatsElem {
	return &StatsElem{
		BaseNode: BaseNode{Tag: T_StatsElem},
		Name:     name,
	}
}

// NewStatsElemExpr creates a new StatsElem with expression.
func NewStatsElemExpr(expr Node) *StatsElem {
	return &StatsElem{
		BaseNode: BaseNode{Tag: T_StatsElem},
		Expr:     expr,
	}
}

func (se *StatsElem) String() string {
	if se.Name != "" {
		return fmt.Sprintf("StatsElem(%s)", se.Name)
	}
	return fmt.Sprintf("StatsElem(expr: %s)", se.Expr)
}

// ==============================================================================
// FOREIGN DATA WRAPPER STATEMENTS
// ==============================================================================

// CreateForeignServerStmt represents CREATE FOREIGN SERVER statements.
// This supports PostgreSQL's foreign data wrapper functionality.
// Ported from postgres/src/include/nodes/parsenodes.h:2870
type CreateForeignServerStmt struct {
	BaseNode
	Servername  string // Server name - parsenodes.h:2872
	Servertype  string // Optional server type - parsenodes.h:2873
	Version     string // Optional server version - parsenodes.h:2874
	Fdwname     string // FDW name - parsenodes.h:2875
	IfNotExists bool   // IF NOT EXISTS clause - parsenodes.h:2876
	Options     *NodeList // Generic options to FDW - parsenodes.h:2877
}

// NewCreateForeignServerStmt creates a new CreateForeignServerStmt node.
// NewCreateForeignServerStmt creates a new CreateForeignServerStmt node.
func NewCreateForeignServerStmt(servername, servertype, version, fdwname string, options *NodeList, ifNotExists bool) *CreateForeignServerStmt {
	return &CreateForeignServerStmt{
		BaseNode:    BaseNode{Tag: T_CreateForeignServerStmt},
		Servername:  servername,
		Servertype:  servertype,
		Version:     version,
		Fdwname:     fdwname,
		Options:     options,
		IfNotExists: ifNotExists,
	}
}

func (cfss *CreateForeignServerStmt) StatementType() string {
	return "CreateForeignServerStmt"
}

func (cfss *CreateForeignServerStmt) String() string {
	ifNotExists := ""
	if cfss.IfNotExists {
		ifNotExists = " IF NOT EXISTS"
	}
	return fmt.Sprintf("CreateForeignServerStmt(CREATE SERVER%s %s FOREIGN DATA WRAPPER %s)", ifNotExists, cfss.Servername, cfss.Fdwname)
}

// SqlString returns the SQL representation of the CREATE FOREIGN SERVER statement
func (cfss *CreateForeignServerStmt) SqlString() string {
	var parts []string
	
	// CREATE SERVER [IF NOT EXISTS]
	parts = append(parts, "CREATE SERVER")
	if cfss.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}
	
	// server name
	parts = append(parts, cfss.Servername)
	
	// TYPE clause
	if cfss.Servertype != "" {
		parts = append(parts, "TYPE", "'"+cfss.Servertype+"'")
	}
	
	// VERSION clause  
	if cfss.Version != "" {
		parts = append(parts, "VERSION", "'"+cfss.Version+"'")
	}
	
	// FOREIGN DATA WRAPPER
	parts = append(parts, "FOREIGN DATA WRAPPER", cfss.Fdwname)
	
	// OPTIONS clause
	if cfss.Options != nil && cfss.Options.Len() > 0 {
		var optParts []string
		for _, item := range cfss.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				// For generic options, use PostgreSQL format: key 'value' (no =)
				if opt.Arg != nil {
					optParts = append(optParts, opt.Defname+" "+opt.Arg.SqlString())
				} else {
					optParts = append(optParts, opt.Defname)
				}
			}
		}
		parts = append(parts, "OPTIONS", "("+strings.Join(optParts, ", ")+")")
	}
	
	return strings.Join(parts, " ")
}

// CreateForeignTableStmt represents CREATE FOREIGN TABLE statements.
// This creates tables that reference external data sources.
// Ported from postgres/src/include/nodes/parsenodes.h:2895
type CreateForeignTableStmt struct {
	BaseNode
	Base       *CreateStmt // Base CREATE TABLE statement - parsenodes.h:2897
	Servername string      // Foreign server name - parsenodes.h:2898
	Options    *NodeList   // OPTIONS clause - parsenodes.h:2899
}

// NewCreateForeignTableStmt creates a new CreateForeignTableStmt node.
// NewCreateForeignTableStmt creates a CreateForeignTableStmt from grammar components.
func NewCreateForeignTableStmt(relation *RangeVar, tableElts, inhRelations *NodeList, servername string, options *NodeList, partBound *PartitionBoundSpec, ifNotExists bool) *CreateForeignTableStmt {
	// Create the base CreateStmt
	base := &CreateStmt{
		BaseNode:     BaseNode{Tag: T_CreateStmt},
		Relation:     relation,
		TableElts:    tableElts,
		InhRelations: inhRelations,
		PartBound:    partBound,
		IfNotExists:  ifNotExists,
		OnCommit:     ONCOMMIT_NOOP,
	}
	
	return &CreateForeignTableStmt{
		BaseNode:   BaseNode{Tag: T_CreateForeignTableStmt},
		Base:       base,
		Servername: servername,
		Options:    options,
	}
}

func (cfts *CreateForeignTableStmt) StatementType() string {
	return "CreateForeignTableStmt"
}

func (cfts *CreateForeignTableStmt) String() string {
	return fmt.Sprintf("CreateForeignTableStmt(CREATE FOREIGN TABLE %s SERVER %s)", cfts.Base.Relation.RelName, cfts.Servername)
}

// SqlString returns the SQL representation of the CREATE FOREIGN TABLE statement
func (cfts *CreateForeignTableStmt) SqlString() string {
	var parts []string
	
	// CREATE FOREIGN TABLE [IF NOT EXISTS]
	parts = append(parts, "CREATE FOREIGN TABLE")
	if cfts.Base.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}
	
	// table name
	parts = append(parts, cfts.Base.Relation.SqlString())
	
	// column definitions
	if cfts.Base.TableElts != nil && cfts.Base.TableElts.Len() > 0 {
		parts = append(parts, "("+cfts.Base.TableElts.SqlString()+")")
	}
	
	// INHERITS clause
	if cfts.Base.InhRelations != nil && cfts.Base.InhRelations.Len() > 0 {
		parts = append(parts, "INHERITS", "("+cfts.Base.InhRelations.SqlString()+")")
	}
	
	// SERVER clause
	parts = append(parts, "SERVER", cfts.Servername)
	
	// OPTIONS clause
	if cfts.Options != nil && cfts.Options.Len() > 0 {
		var optParts []string
		for _, item := range cfts.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				// For generic options, use PostgreSQL format: key 'value' (no =)
				if opt.Arg != nil {
					optParts = append(optParts, opt.Defname+" "+opt.Arg.SqlString())
				} else {
					optParts = append(optParts, opt.Defname)
				}
			}
		}
		parts = append(parts, "OPTIONS", "("+strings.Join(optParts, ", ")+")")
	}
	
	return strings.Join(parts, " ")
}

// CreateUserMappingStmt represents CREATE USER MAPPING statements.
// This maps database users to foreign server users.
// Ported from postgres/src/include/nodes/parsenodes.h:2907
type CreateUserMappingStmt struct {
	BaseNode
	User        *RoleSpec // User role - parsenodes.h:2909
	Servername  string    // Foreign server name - parsenodes.h:2910
	IfNotExists bool      // IF NOT EXISTS clause - parsenodes.h:2911
	Options     *NodeList // Generic options to FDW - parsenodes.h:2912
}

// NewCreateUserMappingStmt creates a new CreateUserMappingStmt node.
// NewCreateUserMappingStmt creates a new CreateUserMappingStmt node.
func NewCreateUserMappingStmt(user *RoleSpec, servername string, options *NodeList, ifNotExists bool) *CreateUserMappingStmt {
	return &CreateUserMappingStmt{
		BaseNode:    BaseNode{Tag: T_CreateUserMappingStmt},
		User:        user,
		Servername:  servername,
		Options:     options,
		IfNotExists: ifNotExists,
	}
}

func (cums *CreateUserMappingStmt) StatementType() string {
	return "CreateUserMappingStmt"
}

func (cums *CreateUserMappingStmt) String() string {
	ifNotExists := ""
	if cums.IfNotExists {
		ifNotExists = " IF NOT EXISTS"
	}
	return fmt.Sprintf("CreateUserMappingStmt(CREATE USER MAPPING%s FOR %s SERVER %s)", ifNotExists, cums.User, cums.Servername)
}

// SqlString returns the SQL representation of the CREATE USER MAPPING statement
func (cums *CreateUserMappingStmt) SqlString() string {
	var parts []string
	
	// CREATE USER MAPPING [IF NOT EXISTS]
	parts = append(parts, "CREATE USER MAPPING")
	if cums.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}
	
	// FOR user
	parts = append(parts, "FOR")
	if cums.User != nil {
		parts = append(parts, cums.User.SqlString())
	}
	
	// SERVER name
	parts = append(parts, "SERVER", cums.Servername)
	
	// OPTIONS clause
	if cums.Options != nil && cums.Options.Len() > 0 {
		var optParts []string
		for _, item := range cums.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				// For generic options, use PostgreSQL format: key 'value' (no =)
				if opt.Arg != nil {
					optParts = append(optParts, opt.Defname+" "+opt.Arg.SqlString())
				} else {
					optParts = append(optParts, opt.Defname)
				}
			}
		}
		parts = append(parts, "OPTIONS", "("+strings.Join(optParts, ", ")+")")
	}
	
	return strings.Join(parts, " ")
}

// ==============================================================================
// TRIGGER STATEMENTS
// ==============================================================================

// TriggerTransition represents trigger transition tables.
// Ported from postgres/src/include/nodes/parsenodes.h:1737
type TriggerTransition struct {
	BaseNode
	Name    string // Transition table name - parsenodes.h:1739
	IsNew   bool   // Is this NEW table? (or OLD table?) - parsenodes.h:1740
	IsTable bool   // Is this a table? (or row?) - parsenodes.h:1741
}

// CreateTriggerStmt represents CREATE TRIGGER statements.
// Triggers are essential for PostgreSQL's event system.
// Ported from postgres/src/include/nodes/parsenodes.h:3001
type CreateTriggerStmt struct {
	BaseNode
	Replace      bool                 // Replace existing trigger? - parsenodes.h:3003
	IsConstraint bool                 // Is this a constraint trigger? - parsenodes.h:3004
	Trigname     string               // Trigger name - parsenodes.h:3005
	Relation     *RangeVar            // Relation trigger is on - parsenodes.h:3006
	Funcname     *NodeList            // Qual. name of function to call - parsenodes.h:3007
	Args         *NodeList            // List of (T_String) Values or NIL - parsenodes.h:3008
	Row          bool                 // ROW/STATEMENT - parsenodes.h:3009
	Timing       int16                // BEFORE, AFTER, or INSTEAD - parsenodes.h:3010
	Events       int16                // "OR" of INSERT/UPDATE/DELETE/TRUNCATE - parsenodes.h:3011
	Columns      *NodeList            // Column names, or NIL for all columns - parsenodes.h:3012
	WhenClause   Node                 // WHEN clause - parsenodes.h:3013
	Constrrel    *RangeVar            // Opposite relation, if RI trigger - parsenodes.h:3014
	Deferrable   bool                 // DEFERRABLE - parsenodes.h:3015
	Initdeferred bool                 // INITIALLY DEFERRED - parsenodes.h:3016
	Transitions  *NodeList           // Transition table clauses - parsenodes.h:3017
}

// TriggerType represents trigger event types.
const (
	TRIGGER_TYPE_INSERT   = 1 << 0 // INSERT trigger
	TRIGGER_TYPE_UPDATE   = 1 << 1 // UPDATE trigger
	TRIGGER_TYPE_DELETE   = 1 << 2 // DELETE trigger
	TRIGGER_TYPE_TRUNCATE = 1 << 3 // TRUNCATE trigger
)

// TriggerTiming represents trigger timing.
const (
	TRIGGER_TIMING_BEFORE  = 1 // BEFORE trigger
	TRIGGER_TIMING_AFTER   = 2 // AFTER trigger
	TRIGGER_TIMING_INSTEAD = 3 // INSTEAD OF trigger
)

// NewCreateTriggerStmt creates a new CreateTriggerStmt node.
func NewCreateTriggerStmt(trigname string, relation *RangeVar, funcname *NodeList, timing int16, events int16) *CreateTriggerStmt {
	return &CreateTriggerStmt{
		BaseNode: BaseNode{Tag: T_CreateTriggerStmt},
		Trigname: trigname,
		Relation: relation,
		Funcname: funcname,
		Timing:   timing,
		Events:   events,
		Row:      true, // Default to ROW trigger
	}
}

// NewBeforeInsertTrigger creates a BEFORE INSERT trigger.
func NewBeforeInsertTrigger(trigname string, relation *RangeVar, funcname *NodeList) *CreateTriggerStmt {
	return &CreateTriggerStmt{
		BaseNode: BaseNode{Tag: T_CreateTriggerStmt},
		Trigname: trigname,
		Relation: relation,
		Funcname: funcname,
		Timing:   TRIGGER_TIMING_BEFORE,
		Events:   TRIGGER_TYPE_INSERT,
		Row:      true,
	}
}

// NewAfterUpdateTrigger creates an AFTER UPDATE trigger.
func NewAfterUpdateTrigger(trigname string, relation *RangeVar, funcname *NodeList) *CreateTriggerStmt {
	return &CreateTriggerStmt{
		BaseNode: BaseNode{Tag: T_CreateTriggerStmt},
		Trigname: trigname,
		Relation: relation,
		Funcname: funcname,
		Timing:   TRIGGER_TIMING_AFTER,
		Events:   TRIGGER_TYPE_UPDATE,
		Row:      true,
	}
}

// NewConstraintTrigger creates a constraint trigger.
func NewConstraintTrigger(trigname string, relation *RangeVar, funcname *NodeList, timing int16, events int16, constrrel *RangeVar) *CreateTriggerStmt {
	return &CreateTriggerStmt{
		BaseNode:     BaseNode{Tag: T_CreateTriggerStmt},
		IsConstraint: true,
		Trigname:     trigname,
		Relation:     relation,
		Funcname:     funcname,
		Timing:       timing,
		Events:       events,
		Row:          true,
		Constrrel:    constrrel,
		Deferrable:   false, // Default to NOT DEFERRABLE
		Initdeferred: false, // Default to INITIALLY IMMEDIATE
	}
}

// NewDeferrableConstraintTrigger creates a deferrable constraint trigger.
func NewDeferrableConstraintTrigger(trigname string, relation *RangeVar, funcname *NodeList, timing int16, events int16, constrrel *RangeVar, initdeferred bool) *CreateTriggerStmt {
	return &CreateTriggerStmt{
		BaseNode:     BaseNode{Tag: T_CreateTriggerStmt},
		IsConstraint: true,
		Trigname:     trigname,
		Relation:     relation,
		Funcname:     funcname,
		Timing:       timing,
		Events:       events,
		Row:          true,
		Constrrel:    constrrel,
		Deferrable:   true,
		Initdeferred: initdeferred,
	}
}

func (cts *CreateTriggerStmt) String() string {
	timingStrs := map[int16]string{
		TRIGGER_TIMING_BEFORE: "BEFORE", TRIGGER_TIMING_AFTER: "AFTER", TRIGGER_TIMING_INSTEAD: "INSTEAD OF",
	}
	timingStr := timingStrs[cts.Timing]

	eventStrs := []string{}
	if cts.Events&TRIGGER_TYPE_INSERT != 0 {
		eventStrs = append(eventStrs, "INSERT")
	}
	if cts.Events&TRIGGER_TYPE_UPDATE != 0 {
		eventStrs = append(eventStrs, "UPDATE")
	}
	if cts.Events&TRIGGER_TYPE_DELETE != 0 {
		eventStrs = append(eventStrs, "DELETE")
	}
	if cts.Events&TRIGGER_TYPE_TRUNCATE != 0 {
		eventStrs = append(eventStrs, "TRUNCATE")
	}

	rowStmt := "STATEMENT"
	if cts.Row {
		rowStmt = "ROW"
	}

	// Build the base string
	result := fmt.Sprintf("CreateTriggerStmt(CREATE TRIGGER %s %s %v FOR EACH %s)", cts.Trigname, timingStr, eventStrs, rowStmt)

	// Add constraint trigger information
	if cts.IsConstraint {
		result += " CONSTRAINT"
		if cts.Constrrel != nil {
			result += fmt.Sprintf(" FROM %s", cts.Constrrel.RelName)
		}
		if cts.Deferrable {
			result += " DEFERRABLE"
			if cts.Initdeferred {
				result += " INITIALLY DEFERRED"
			} else {
				result += " INITIALLY IMMEDIATE"
			}
		} else {
			result += " NOT DEFERRABLE"
		}
	}

	// Add transition information
	if cts.Transitions != nil && cts.Transitions.Len() > 0 {
		result += fmt.Sprintf(" (%d transitions)", cts.Transitions.Len())
	}

	return result
}

// Location returns the location for this node
func (cts *CreateTriggerStmt) Location() int {
	return 0 // TODO: Implement proper location tracking
}

// NodeTag returns the node's type tag
func (cts *CreateTriggerStmt) NodeTag() NodeTag {
	return T_CreateTriggerStmt
}

// StatementType returns the statement type for this node
func (cts *CreateTriggerStmt) StatementType() string {
	if cts.IsConstraint {
		return "CREATE CONSTRAINT TRIGGER"
	}
	return "CREATE TRIGGER"
}

// SqlString returns SQL representation of the CREATE TRIGGER statement
func (cts *CreateTriggerStmt) SqlString() string {
	var parts []string
	
	// CREATE [OR REPLACE] [CONSTRAINT] TRIGGER
	parts = append(parts, "CREATE")
	if cts.Replace {
		parts = append(parts, "OR REPLACE")
	}
	if cts.IsConstraint {
		parts = append(parts, "CONSTRAINT")
	}
	parts = append(parts, "TRIGGER", cts.Trigname)
	
	// Timing (BEFORE, AFTER, INSTEAD OF)
	switch cts.Timing {
	case TRIGGER_TIMING_BEFORE:
		parts = append(parts, "BEFORE")
	case TRIGGER_TIMING_AFTER:
		parts = append(parts, "AFTER")
	case TRIGGER_TIMING_INSTEAD:
		parts = append(parts, "INSTEAD OF")
	}
	
	// Events (INSERT, UPDATE, DELETE, TRUNCATE)
	var events []string
	if cts.Events&TRIGGER_TYPE_INSERT != 0 {
		events = append(events, "INSERT")
	}
	if cts.Events&TRIGGER_TYPE_UPDATE != 0 {
		updateEvent := "UPDATE"
		// Add column list for UPDATE OF if present
		if cts.Columns != nil && len(cts.Columns.Items) > 0 {
			var colNames []string
			for _, col := range cts.Columns.Items {
				if str, ok := col.(*String); ok {
					colNames = append(colNames, str.SVal)
				}
			}
			if len(colNames) > 0 {
				updateEvent = fmt.Sprintf("UPDATE OF %s", strings.Join(colNames, ", "))
			}
		}
		events = append(events, updateEvent)
	}
	if cts.Events&TRIGGER_TYPE_DELETE != 0 {
		events = append(events, "DELETE")
	}
	if cts.Events&TRIGGER_TYPE_TRUNCATE != 0 {
		events = append(events, "TRUNCATE")
	}
	parts = append(parts, strings.Join(events, " OR "))
	
	// ON table
	parts = append(parts, "ON")
	if cts.Relation != nil {
		parts = append(parts, cts.Relation.SqlString())
	}
	
	// FROM constraint_table (for constraint triggers)
	if cts.IsConstraint && cts.Constrrel != nil {
		parts = append(parts, "FROM", cts.Constrrel.SqlString())
	}
	
	// Deferrable options (for constraint triggers)
	if cts.IsConstraint {
		if cts.Deferrable {
			parts = append(parts, "DEFERRABLE")
			if cts.Initdeferred {
				parts = append(parts, "INITIALLY DEFERRED")
			} else {
				parts = append(parts, "INITIALLY IMMEDIATE")
			}
		} else {
			parts = append(parts, "NOT DEFERRABLE")
		}
	}
	
	// REFERENCING transitions
	if cts.Transitions != nil && cts.Transitions.Len() > 0 {
		parts = append(parts, "REFERENCING")
		for i := 0; i < cts.Transitions.Len(); i++ {
			trans := cts.Transitions.Items[i].(*TriggerTransition)
			if trans.IsNew {
				parts = append(parts, "NEW")
			} else {
				parts = append(parts, "OLD")
			}
			if trans.IsTable {
				parts = append(parts, "TABLE")
			} else {
				parts = append(parts, "ROW")
			}
			parts = append(parts, "AS", trans.Name)
		}
	}
	
	// FOR EACH ROW/STATEMENT (only output if ROW is true, STATEMENT is default)
	if cts.Row {
		parts = append(parts, "FOR EACH ROW")
	}
	
	// WHEN clause
	if cts.WhenClause != nil {
		parts = append(parts, "WHEN", fmt.Sprintf("(%s)", cts.WhenClause.SqlString()))
	}
	
	// EXECUTE FUNCTION
	parts = append(parts, "EXECUTE FUNCTION")
	
	// Function name
	if cts.Funcname != nil {
		var funcNames []string
		for _, item := range cts.Funcname.Items {
			if str, ok := item.(*String); ok {
				funcNames = append(funcNames, str.SVal)
			}
		}
		parts = append(parts, strings.Join(funcNames, "."))
	}
	
	// Function arguments
	var argStrs []string
	if cts.Args != nil && len(cts.Args.Items) > 0 {
		for _, arg := range cts.Args.Items {
			if str, ok := arg.(*String); ok {
				// Check if it's a numeric string or needs quotes
				if _, err := fmt.Sscanf(str.SVal, "%d", new(int)); err != nil {
					// Not a number, add quotes
					argStrs = append(argStrs, fmt.Sprintf("'%s'", str.SVal))
				} else {
					// It's a number
					argStrs = append(argStrs, str.SVal)
				}
			}
		}
	}
	argsClause := "(" + strings.Join(argStrs, ", ") + ")"
	parts = append(parts, argsClause)
	
	return strings.Join(parts, " ")
}

// ==============================================================================
// POLICY STATEMENTS (ROW LEVEL SECURITY)
// ==============================================================================

// CreatePolicyStmt represents CREATE POLICY statements.
// This implements PostgreSQL's row-level security policies.
// Ported from postgres/src/include/nodes/parsenodes.h:2959
type CreatePolicyStmt struct {
	BaseNode
	PolicyName string    // Policy name - parsenodes.h:2961
	Table      *RangeVar // Table the policy applies to - parsenodes.h:2962
	CmdName    string    // Command name (SELECT, INSERT, etc.) - parsenodes.h:2963
	Permissive bool      // Is this a permissive policy? - parsenodes.h:2964
	Roles      *NodeList // Roles policy applies to - parsenodes.h:2965
	Qual       Node      // USING clause - parsenodes.h:2966
	WithCheck  Node      // WITH CHECK clause - parsenodes.h:2967
}

// NewCreatePolicyStmt creates a new CreatePolicyStmt node.
func NewCreatePolicyStmt(policyName string, table *RangeVar, cmdName string) *CreatePolicyStmt {
	return &CreatePolicyStmt{
		BaseNode:   BaseNode{Tag: T_CreatePolicyStmt},
		PolicyName: policyName,
		Table:      table,
		CmdName:    cmdName,
		Permissive: true, // Default to permissive
	}
}

// NewRestrictivePolicyStmt creates a new restrictive policy.
func NewRestrictivePolicyStmt(policyName string, table *RangeVar, cmdName string) *CreatePolicyStmt {
	return &CreatePolicyStmt{
		BaseNode:   BaseNode{Tag: T_CreatePolicyStmt},
		PolicyName: policyName,
		Table:      table,
		CmdName:    cmdName,
		Permissive: false,
	}
}

func (cps *CreatePolicyStmt) String() string {
	policyType := "PERMISSIVE"
	if !cps.Permissive {
		policyType = "RESTRICTIVE"
	}

	return fmt.Sprintf("CreatePolicyStmt(CREATE %s POLICY %s ON %s FOR %s)", policyType, cps.PolicyName, cps.Table.RelName, cps.CmdName)
}

// AlterPolicyStmt represents ALTER POLICY statements.
// Ported from postgres/src/include/nodes/parsenodes.h:2975
type AlterPolicyStmt struct {
	BaseNode
	PolicyName string    // Policy name - parsenodes.h:2977
	Table      *RangeVar // Table the policy applies to - parsenodes.h:2978
	Roles      *NodeList // Roles policy applies to - parsenodes.h:2979
	Qual       Node      // USING clause - parsenodes.h:2980
	WithCheck  Node      // WITH CHECK clause - parsenodes.h:2981
}

// NewAlterPolicyStmt creates a new AlterPolicyStmt node.
func NewAlterPolicyStmt(policyName string, table *RangeVar) *AlterPolicyStmt {
	return &AlterPolicyStmt{
		BaseNode:   BaseNode{Tag: T_AlterPolicyStmt},
		PolicyName: policyName,
		Table:      table,
	}
}

func (aps *AlterPolicyStmt) String() string {
	return fmt.Sprintf("AlterPolicyStmt(ALTER POLICY %s ON %s)", aps.PolicyName, aps.Table.RelName)
}
