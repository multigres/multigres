// Package ast provides PostgreSQL AST administrative and advanced DDL statement definitions.
// These nodes handle advanced table operations, partitioning, foreign data wrappers,
// triggers, policies, and other administrative PostgreSQL features.
// Ported from postgres/src/include/nodes/parsenodes.h
package ast

import (
	"fmt"
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
// Ported from postgres/src/include/nodes/parsenodes.h:831-841
type TableLikeClause struct {
	BaseNode
	Relation *RangeVar        // Table to copy from - parsenodes.h:832
	Options  TableLikeOption  // OR of TableLikeOption flags - parsenodes.h:833
}

// TableLikeOption represents options for LIKE clauses.
// Ported from postgres/src/include/nodes/parsenodes.h:843-851
type TableLikeOption int

const (
	CREATE_TABLE_LIKE_COMMENTS    TableLikeOption = 1 << 0 // INCLUDING COMMENTS - parsenodes.h:844
	CREATE_TABLE_LIKE_COMPRESSION TableLikeOption = 1 << 1 // INCLUDING COMPRESSION - parsenodes.h:845
	CREATE_TABLE_LIKE_CONSTRAINTS TableLikeOption = 1 << 2 // INCLUDING CONSTRAINTS - parsenodes.h:846
	CREATE_TABLE_LIKE_DEFAULTS    TableLikeOption = 1 << 3 // INCLUDING DEFAULTS - parsenodes.h:847
	CREATE_TABLE_LIKE_GENERATED   TableLikeOption = 1 << 4 // INCLUDING GENERATED - parsenodes.h:848
	CREATE_TABLE_LIKE_IDENTITY    TableLikeOption = 1 << 5 // INCLUDING IDENTITY - parsenodes.h:849
	CREATE_TABLE_LIKE_INDEXES     TableLikeOption = 1 << 6 // INCLUDING INDEXES - parsenodes.h:850
	CREATE_TABLE_LIKE_STATISTICS  TableLikeOption = 1 << 7 // INCLUDING STATISTICS - parsenodes.h:851
	CREATE_TABLE_LIKE_STORAGE     TableLikeOption = 1 << 8 // INCLUDING STORAGE - parsenodes.h:852
	CREATE_TABLE_LIKE_ALL         TableLikeOption = 1 << 9 // INCLUDING ALL - parsenodes.h:853
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
// Ported from postgres/src/include/nodes/parsenodes.h:936-940
type PartitionStrategy string

const (
	PARTITION_STRATEGY_LIST  PartitionStrategy = "list"  // LIST partitioning - parsenodes.h:937
	PARTITION_STRATEGY_RANGE PartitionStrategy = "range" // RANGE partitioning - parsenodes.h:938
	PARTITION_STRATEGY_HASH  PartitionStrategy = "hash"  // HASH partitioning - parsenodes.h:939
)

// PartitionSpec represents table partitioning specifications.
// Modern PostgreSQL feature for table partitioning.
// Ported from postgres/src/include/nodes/parsenodes.h:942-950
type PartitionSpec struct {
	BaseNode
	Strategy   PartitionStrategy // Partitioning strategy - parsenodes.h:943
	PartParams []Node            // List of PartitionElem nodes - parsenodes.h:944
	Location   int               // Parse location, or -1 if none/unknown - parsenodes.h:945
}

// NewPartitionSpec creates a new PartitionSpec node.
func NewPartitionSpec(strategy PartitionStrategy, partParams []Node) *PartitionSpec {
	return &PartitionSpec{
		BaseNode:   BaseNode{Tag: T_PartitionSpec},
		Strategy:   strategy,
		PartParams: partParams,
		Location:   -1,
	}
}

// NewListPartitionSpec creates a new LIST partition specification.
func NewListPartitionSpec(partParams []Node) *PartitionSpec {
	return &PartitionSpec{
		BaseNode:   BaseNode{Tag: T_PartitionSpec},
		Strategy:   PARTITION_STRATEGY_LIST,
		PartParams: partParams,
		Location:   -1,
	}
}

// NewRangePartitionSpec creates a new RANGE partition specification.
func NewRangePartitionSpec(partParams []Node) *PartitionSpec {
	return &PartitionSpec{
		BaseNode:   BaseNode{Tag: T_PartitionSpec},
		Strategy:   PARTITION_STRATEGY_RANGE,
		PartParams: partParams,
		Location:   -1,
	}
}

// NewHashPartitionSpec creates a new HASH partition specification.
func NewHashPartitionSpec(partParams []Node) *PartitionSpec {
	return &PartitionSpec{
		BaseNode:   BaseNode{Tag: T_PartitionSpec},
		Strategy:   PARTITION_STRATEGY_HASH,
		PartParams: partParams,
		Location:   -1,
	}
}

func (ps *PartitionSpec) String() string {
	return fmt.Sprintf("PartitionSpec(PARTITION BY %s, %d params)", ps.Strategy, len(ps.PartParams))
}

// PartitionBoundSpec represents partition boundary specifications.
// This defines the actual bounds for individual partitions.
// Ported from postgres/src/include/nodes/parsenodes.h:952-966
type PartitionBoundSpec struct {
	BaseNode
	Strategy     PartitionStrategy // Partitioning strategy - parsenodes.h:953
	IsDefault    bool              // Is this a default partition? - parsenodes.h:954
	Modulus      int               // Hash partition modulus - parsenodes.h:955
	Remainder    int               // Hash partition remainder - parsenodes.h:956
	ListDatums   [][]Node          // List of list datums per column - parsenodes.h:957
	LowDatums    []Node            // List of lower datums for range bounds - parsenodes.h:958
	HighDatums   []Node            // List of upper datums for range bounds - parsenodes.h:959
	Location     int               // Parse location, or -1 if none/unknown - parsenodes.h:960
}

// NewPartitionBoundSpec creates a new PartitionBoundSpec node.
func NewPartitionBoundSpec(strategy PartitionStrategy) *PartitionBoundSpec {
	return &PartitionBoundSpec{
		BaseNode: BaseNode{Tag: T_PartitionBoundSpec},
		Strategy: strategy,
		Location: -1,
	}
}

// NewDefaultPartitionBound creates a new default partition bound.
func NewDefaultPartitionBound() *PartitionBoundSpec {
	return &PartitionBoundSpec{
		BaseNode:  BaseNode{Tag: T_PartitionBoundSpec},
		IsDefault: true,
		Location:  -1,
	}
}

// NewHashPartitionBound creates a new hash partition bound.
func NewHashPartitionBound(modulus, remainder int) *PartitionBoundSpec {
	return &PartitionBoundSpec{
		BaseNode:  BaseNode{Tag: T_PartitionBoundSpec},
		Strategy:  PARTITION_STRATEGY_HASH,
		Modulus:   modulus,
		Remainder: remainder,
		Location:  -1,
	}
}

// NewListPartitionBound creates a new list partition bound.
func NewListPartitionBound(listDatums [][]Node) *PartitionBoundSpec {
	return &PartitionBoundSpec{
		BaseNode:   BaseNode{Tag: T_PartitionBoundSpec},
		Strategy:   PARTITION_STRATEGY_LIST,
		ListDatums: listDatums,
		Location:   -1,
	}
}

// NewRangePartitionBound creates a new range partition bound.
func NewRangePartitionBound(lowDatums, highDatums []Node) *PartitionBoundSpec {
	return &PartitionBoundSpec{
		BaseNode:   BaseNode{Tag: T_PartitionBoundSpec},
		Strategy:   PARTITION_STRATEGY_RANGE,
		LowDatums:  lowDatums,
		HighDatums: highDatums,
		Location:   -1,
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
		return fmt.Sprintf("PartitionBoundSpec(LIST %d datums)", len(pbs.ListDatums))
	case PARTITION_STRATEGY_RANGE:
		return fmt.Sprintf("PartitionBoundSpec(RANGE low=%d high=%d)", len(pbs.LowDatums), len(pbs.HighDatums))
	default:
		return fmt.Sprintf("PartitionBoundSpec(%s)", pbs.Strategy)
	}
}

// PartitionRangeDatum represents partition range datum values.
// Ported from postgres/src/include/nodes/parsenodes.h:968-976
type PartitionRangeDatum struct {
	BaseNode
	Kind     PartitionRangeDatumKind // What kind of datum this is - parsenodes.h:969
	Value    Node                    // The actual datum value - parsenodes.h:970
	Location int                     // Parse location, or -1 if none/unknown - parsenodes.h:971
}

// PartitionRangeDatumKind represents types of range datums.
// Ported from postgres/src/include/nodes/parsenodes.h:978-982
type PartitionRangeDatumKind int

const (
	PARTITION_RANGE_DATUM_MINVALUE PartitionRangeDatumKind = iota // MINVALUE - parsenodes.h:979
	PARTITION_RANGE_DATUM_VALUE                                   // Specific value - parsenodes.h:980
	PARTITION_RANGE_DATUM_MAXVALUE                                // MAXVALUE - parsenodes.h:981
)

// NewPartitionRangeDatum creates a new PartitionRangeDatum node.
func NewPartitionRangeDatum(kind PartitionRangeDatumKind, value Node) *PartitionRangeDatum {
	return &PartitionRangeDatum{
		BaseNode: BaseNode{Tag: T_PartitionRangeDatum},
		Kind:     kind,
		Value:    value,
		Location: -1,
	}
}

// NewMinValueDatum creates a MINVALUE range datum.
func NewMinValueDatum() *PartitionRangeDatum {
	return &PartitionRangeDatum{
		BaseNode: BaseNode{Tag: T_PartitionRangeDatum},
		Kind:     PARTITION_RANGE_DATUM_MINVALUE,
		Location: -1,
	}
}

// NewMaxValueDatum creates a MAXVALUE range datum.
func NewMaxValueDatum() *PartitionRangeDatum {
	return &PartitionRangeDatum{
		BaseNode: BaseNode{Tag: T_PartitionRangeDatum},
		Kind:     PARTITION_RANGE_DATUM_MAXVALUE,
		Location: -1,
	}
}

// NewValueDatum creates a specific value range datum.
func NewValueDatum(value Node) *PartitionRangeDatum {
	return &PartitionRangeDatum{
		BaseNode: BaseNode{Tag: T_PartitionRangeDatum},
		Kind:     PARTITION_RANGE_DATUM_VALUE,
		Value:    value,
		Location: -1,
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
// Ported from postgres/src/include/nodes/parsenodes.h:3176-3186
type StatsElem struct {
	BaseNode
	Name string // Name of attribute to compute stats for - parsenodes.h:3177
	Expr Node   // Or expression to compute stats for - parsenodes.h:3178
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
// Ported from postgres/src/include/nodes/parsenodes.h:2820-2829
type CreateForeignServerStmt struct {
	BaseNode
	Servername string   // Server name - parsenodes.h:2821
	Servertype string   // Optional server type - parsenodes.h:2822
	Version    string   // Optional server version - parsenodes.h:2823
	Fdwname    string   // FDW name - parsenodes.h:2824
	IfNotExists bool    // IF NOT EXISTS clause - parsenodes.h:2825
	Options    []Node   // Generic options to FDW - parsenodes.h:2826
}

// NewCreateForeignServerStmt creates a new CreateForeignServerStmt node.
func NewCreateForeignServerStmt(servername, fdwname string) *CreateForeignServerStmt {
	return &CreateForeignServerStmt{
		BaseNode:   BaseNode{Tag: T_CreateForeignServerStmt},
		Servername: servername,
		Fdwname:    fdwname,
	}
}

// NewCreateForeignServerIfNotExistsStmt creates a CREATE FOREIGN SERVER IF NOT EXISTS statement.
func NewCreateForeignServerIfNotExistsStmt(servername, fdwname string) *CreateForeignServerStmt {
	return &CreateForeignServerStmt{
		BaseNode:    BaseNode{Tag: T_CreateForeignServerStmt},
		Servername:  servername,
		Fdwname:     fdwname,
		IfNotExists: true,
	}
}

func (cfss *CreateForeignServerStmt) String() string {
	ifNotExists := ""
	if cfss.IfNotExists {
		ifNotExists = " IF NOT EXISTS"
	}
	return fmt.Sprintf("CreateForeignServerStmt(CREATE SERVER%s %s FOREIGN DATA WRAPPER %s)", ifNotExists, cfss.Servername, cfss.Fdwname)
}

// CreateForeignTableStmt represents CREATE FOREIGN TABLE statements.
// This creates tables that reference external data sources.
// Ported from postgres/src/include/nodes/parsenodes.h:2690-2701
type CreateForeignTableStmt struct {
	BaseNode
	Base        *CreateStmt // Base CREATE TABLE statement - parsenodes.h:2691
	Servername  string      // Foreign server name - parsenodes.h:2692
	Options     []Node      // OPTIONS clause - parsenodes.h:2693
}

// NewCreateForeignTableStmt creates a new CreateForeignTableStmt node.
func NewCreateForeignTableStmt(base *CreateStmt, servername string) *CreateForeignTableStmt {
	return &CreateForeignTableStmt{
		BaseNode:   BaseNode{Tag: T_CreateForeignTableStmt},
		Base:       base,
		Servername: servername,
	}
}

func (cfts *CreateForeignTableStmt) String() string {
	return fmt.Sprintf("CreateForeignTableStmt(CREATE FOREIGN TABLE %s SERVER %s)", cfts.Base.Relation.RelName, cfts.Servername)
}

// CreateUserMappingStmt represents CREATE USER MAPPING statements.
// This maps database users to foreign server users.
// Ported from postgres/src/include/nodes/parsenodes.h:2831-2839
type CreateUserMappingStmt struct {
	BaseNode
	User        *RoleSpec // User role - parsenodes.h:2832
	Servername  string    // Foreign server name - parsenodes.h:2833
	IfNotExists bool      // IF NOT EXISTS clause - parsenodes.h:2834
	Options     []Node    // Generic options to FDW - parsenodes.h:2835
}

// NewCreateUserMappingStmt creates a new CreateUserMappingStmt node.
func NewCreateUserMappingStmt(user *RoleSpec, servername string) *CreateUserMappingStmt {
	return &CreateUserMappingStmt{
		BaseNode:   BaseNode{Tag: T_CreateUserMappingStmt},
		User:       user,
		Servername: servername,
	}
}

func (cums *CreateUserMappingStmt) String() string {
	ifNotExists := ""
	if cums.IfNotExists {
		ifNotExists = " IF NOT EXISTS"
	}
	return fmt.Sprintf("CreateUserMappingStmt(CREATE USER MAPPING%s FOR %s SERVER %s)", ifNotExists, cums.User, cums.Servername)
}

// ==============================================================================
// TRIGGER STATEMENTS
// ==============================================================================

// TriggerTransition represents trigger transition tables.
// Ported from postgres/src/include/nodes/parsenodes.h:3015-3023
type TriggerTransition struct {
	BaseNode
	Name  string // Transition table name - parsenodes.h:3016
	IsNew bool   // Is this NEW table? (or OLD table?) - parsenodes.h:3017
	IsTable bool // Is this a table? (or row?) - parsenodes.h:3018
}

// CreateTriggerStmt represents CREATE TRIGGER statements.
// Triggers are essential for PostgreSQL's event system.
// Ported from postgres/src/include/nodes/parsenodes.h:2973-3013
type CreateTriggerStmt struct {
	BaseNode
	Replace       bool                // Replace existing trigger? - parsenodes.h:2974
	IsConstraint  bool                // Is this a constraint trigger? - parsenodes.h:2975
	Trigname      string              // Trigger name - parsenodes.h:2976
	Relation      *RangeVar           // Relation trigger is on - parsenodes.h:2977
	Funcname      []Node              // Qual. name of function to call - parsenodes.h:2978
	Args          []Node              // List of (T_String) Values or NIL - parsenodes.h:2979
	Row           bool                // ROW/STATEMENT - parsenodes.h:2980
	Timing        int16               // BEFORE, AFTER, or INSTEAD - parsenodes.h:2981
	Events        int16               // "OR" of INSERT/UPDATE/DELETE/TRUNCATE - parsenodes.h:2982
	Columns       []Node              // Column names, or NIL for all columns - parsenodes.h:2983
	WhenClause    Node                // WHEN clause - parsenodes.h:2984
	Constrrel     *RangeVar           // Opposite relation, if RI trigger - parsenodes.h:2985
	Deferrable    bool                // DEFERRABLE - parsenodes.h:2986
	Initdeferred  bool                // INITIALLY DEFERRED - parsenodes.h:2987
	Transitions   []*TriggerTransition // Transition table clauses - parsenodes.h:2988
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
func NewCreateTriggerStmt(trigname string, relation *RangeVar, funcname []Node, timing int16, events int16) *CreateTriggerStmt {
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
func NewBeforeInsertTrigger(trigname string, relation *RangeVar, funcname []Node) *CreateTriggerStmt {
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
func NewAfterUpdateTrigger(trigname string, relation *RangeVar, funcname []Node) *CreateTriggerStmt {
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
	
	return fmt.Sprintf("CreateTriggerStmt(CREATE TRIGGER %s %s %v FOR EACH %s)", cts.Trigname, timingStr, eventStrs, rowStmt)
}

// ==============================================================================
// POLICY STATEMENTS (ROW LEVEL SECURITY)
// ==============================================================================

// CreatePolicyStmt represents CREATE POLICY statements.
// This implements PostgreSQL's row-level security policies.
// Ported from postgres/src/include/nodes/parsenodes.h:3049-3064
type CreatePolicyStmt struct {
	BaseNode
	PolicyName string    // Policy name - parsenodes.h:3050
	Table      *RangeVar // Table the policy applies to - parsenodes.h:3051
	CmdName    string    // Command name (SELECT, INSERT, etc.) - parsenodes.h:3052
	Permissive bool      // Is this a permissive policy? - parsenodes.h:3053
	Roles      []Node    // Roles policy applies to - parsenodes.h:3054
	Qual       Node      // USING clause - parsenodes.h:3055
	WithCheck  Node      // WITH CHECK clause - parsenodes.h:3056
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
// Ported from postgres/src/include/nodes/parsenodes.h:3066-3076
type AlterPolicyStmt struct {
	BaseNode
	PolicyName string    // Policy name - parsenodes.h:3067
	Table      *RangeVar // Table the policy applies to - parsenodes.h:3068
	Roles      []Node    // Roles policy applies to - parsenodes.h:3069
	Qual       Node      // USING clause - parsenodes.h:3070
	WithCheck  Node      // WITH CHECK clause - parsenodes.h:3071
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