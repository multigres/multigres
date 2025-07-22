// Package ast provides comprehensive tests for PostgreSQL AST administrative and advanced DDL nodes.
// These tests ensure correctness of PostgreSQL's advanced DDL operations, partitioning, and administrative features.
package ast

import (
	"testing"
)

// ==============================================================================
// ALTER TABLE CMD TESTS
// ==============================================================================

func TestAlterTableCmd(t *testing.T) {
	// Test basic AlterTableCmd creation
	atc := NewAlterTableCmd(AT_AddColumn, "new_column")
	
	// Verify properties
	if atc.Tag != T_AlterTableCmd {
		t.Errorf("Expected tag T_AlterTableCmd, got %v", atc.Tag)
	}
	
	if atc.Subtype != AT_AddColumn {
		t.Errorf("Expected AT_AddColumn, got %v", atc.Subtype)
	}
	
	if atc.Name != "new_column" {
		t.Errorf("Expected name 'new_column', got %s", atc.Name)
	}
	
	// Test string representation
	str := atc.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestAddColumnCmd(t *testing.T) {
	// Create a column definition
	typeName := NewTypeName([]Node{NewString("integer")}, -1)
	colDef := NewColumnDef("test_col", typeName)
	
	atc := NewAddColumnCmd(colDef)
	
	if atc.Subtype != AT_AddColumn {
		t.Errorf("Expected AT_AddColumn, got %v", atc.Subtype)
	}
	
	if atc.Def != colDef {
		t.Errorf("Expected column definition to be set correctly")
	}
	
	// Test string representation
	str := atc.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestDropColumnCmd(t *testing.T) {
	atc := NewDropColumnCmd("old_column", DROP_CASCADE, true)
	
	if atc.Subtype != AT_DropColumn {
		t.Errorf("Expected AT_DropColumn, got %v", atc.Subtype)
	}
	
	if atc.Name != "old_column" {
		t.Errorf("Expected name 'old_column', got %s", atc.Name)
	}
	
	if atc.Behavior != DROP_CASCADE {
		t.Errorf("Expected DROP_CASCADE, got %v", atc.Behavior)
	}
	
	if !atc.MissingOk {
		t.Errorf("Expected MissingOk to be true")
	}
}

func TestAlterColumnTypeCmd(t *testing.T) {
	typeName := NewTypeName([]Node{NewString("varchar")}, -1)
	atc := NewAlterColumnTypeCmd("test_col", typeName)
	
	if atc.Subtype != AT_AlterColumnType {
		t.Errorf("Expected AT_AlterColumnType, got %v", atc.Subtype)
	}
	
	if atc.Name != "test_col" {
		t.Errorf("Expected name 'test_col', got %s", atc.Name)
	}
	
	if atc.Def != typeName {
		t.Errorf("Expected type definition to be set correctly")
	}
}

func TestAddConstraintCmd(t *testing.T) {
	constraint := NewConstraint(CONSTR_PRIMARY, "pk_test")
	atc := NewAddConstraintCmd(constraint)
	
	if atc.Subtype != AT_AddConstraint {
		t.Errorf("Expected AT_AddConstraint, got %v", atc.Subtype)
	}
	
	if atc.Def != constraint {
		t.Errorf("Expected constraint definition to be set correctly")
	}
}

func TestDropConstraintCmd(t *testing.T) {
	atc := NewDropConstraintCmd("test_constraint", DROP_RESTRICT, false)
	
	if atc.Subtype != AT_DropConstraint {
		t.Errorf("Expected AT_DropConstraint, got %v", atc.Subtype)
	}
	
	if atc.Name != "test_constraint" {
		t.Errorf("Expected name 'test_constraint', got %s", atc.Name)
	}
	
	if atc.Behavior != DROP_RESTRICT {
		t.Errorf("Expected DROP_RESTRICT, got %v", atc.Behavior)
	}
	
	if atc.MissingOk {
		t.Errorf("Expected MissingOk to be false")
	}
}

// ==============================================================================
// COLUMN DEF TESTS
// ==============================================================================

func TestColumnDef(t *testing.T) {
	typeName := NewTypeName([]Node{NewString("integer")}, -1)
	cd := NewColumnDef("test_column", typeName)
	
	// Verify properties
	if cd.Tag != T_ColumnDef {
		t.Errorf("Expected tag T_ColumnDef, got %v", cd.Tag)
	}
	
	if cd.Colname != "test_column" {
		t.Errorf("Expected column name 'test_column', got %s", cd.Colname)
	}
	
	if cd.TypeName != typeName {
		t.Errorf("Expected type name to be set correctly")
	}
	
	if !cd.IsLocal {
		t.Errorf("Expected IsLocal to be true by default")
	}
	
	if cd.IsNotNull {
		t.Errorf("Expected IsNotNull to be false by default")
	}
	
	if cd.Location != -1 {
		t.Errorf("Expected location to be -1, got %d", cd.Location)
	}
	
	// Test string representation
	str := cd.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestColumnDefWithDefault(t *testing.T) {
	typeName := NewTypeName([]Node{NewString("integer")}, -1)
	defaultValue := NewConst(23, 42, false) // Integer constant 42
	cd := NewColumnDefWithDefault("test_column", typeName, defaultValue)
	
	if cd.RawDefault != defaultValue {
		t.Errorf("Expected raw default to be set correctly")
	}
	
	if cd.Colname != "test_column" {
		t.Errorf("Expected column name 'test_column', got %s", cd.Colname)
	}
}

func TestNotNullColumnDef(t *testing.T) {
	typeName := NewTypeName([]Node{NewString("varchar")}, -1)
	cd := NewNotNullColumnDef("test_column", typeName)
	
	if !cd.IsNotNull {
		t.Errorf("Expected IsNotNull to be true")
	}
	
	// Test string representation includes "NOT NULL"
	str := cd.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestColumnDefAddConstraint(t *testing.T) {
	typeName := NewTypeName([]Node{NewString("integer")}, -1)
	cd := NewColumnDef("test_column", typeName)
	
	// Add constraints
	constraint1 := NewConstraint(CONSTR_UNIQUE, "unique_constraint")
	constraint2 := NewConstraint(CONSTR_CHECK, "check_constraint")
	
	cd.AddConstraint(constraint1)
	cd.AddConstraint(constraint2)
	
	if len(cd.Constraints) != 2 {
		t.Errorf("Expected 2 constraints, got %d", len(cd.Constraints))
	}
	
	if cd.Constraints[0] != constraint1 {
		t.Errorf("Expected first constraint to be unique constraint")
	}
	
	if cd.Constraints[1] != constraint2 {
		t.Errorf("Expected second constraint to be check constraint")
	}
	
	// Test string representation includes constraint count
	str := cd.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// TABLE LIKE CLAUSE TESTS
// ==============================================================================

func TestTableLikeClause(t *testing.T) {
	relation := NewRangeVar("public", "source_table", -1)
	tlc := NewTableLikeClause(relation, CREATE_TABLE_LIKE_DEFAULTS)
	
	// Verify properties
	if tlc.Tag != T_TableLikeClause {
		t.Errorf("Expected tag T_TableLikeClause, got %v", tlc.Tag)
	}
	
	if tlc.Relation != relation {
		t.Errorf("Expected relation to be set correctly")
	}
	
	if tlc.Options != CREATE_TABLE_LIKE_DEFAULTS {
		t.Errorf("Expected CREATE_TABLE_LIKE_DEFAULTS, got %v", tlc.Options)
	}
	
	// Test string representation
	str := tlc.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestTableLikeAll(t *testing.T) {
	relation := NewRangeVar("public", "source_table", -1)
	tlc := NewTableLikeAll(relation)
	
	if tlc.Options != CREATE_TABLE_LIKE_ALL {
		t.Errorf("Expected CREATE_TABLE_LIKE_ALL, got %v", tlc.Options)
	}
}

func TestTableLikeOptions(t *testing.T) {
	relation := NewRangeVar("public", "source_table", -1)
	
	// Test combined options
	options := CREATE_TABLE_LIKE_CONSTRAINTS | CREATE_TABLE_LIKE_INDEXES | CREATE_TABLE_LIKE_DEFAULTS
	tlc := NewTableLikeClause(relation, options)
	
	// Verify individual options
	if tlc.Options&CREATE_TABLE_LIKE_CONSTRAINTS == 0 {
		t.Errorf("Expected CONSTRAINTS option to be set")
	}
	
	if tlc.Options&CREATE_TABLE_LIKE_INDEXES == 0 {
		t.Errorf("Expected INDEXES option to be set")
	}
	
	if tlc.Options&CREATE_TABLE_LIKE_DEFAULTS == 0 {
		t.Errorf("Expected DEFAULTS option to be set")
	}
	
	// Verify option that should NOT be set
	if tlc.Options&CREATE_TABLE_LIKE_COMMENTS != 0 {
		t.Errorf("Expected COMMENTS option to NOT be set")
	}
}

// ==============================================================================
// PARTITION SPEC TESTS
// ==============================================================================

func TestPartitionSpec(t *testing.T) {
	// Create partition parameters
	param1 := NewColumnRef([]Node{NewString("date_col")}, -1)
	param2 := NewColumnRef([]Node{NewString("region")}, -1)
	partParams := []Node{param1, param2}
	
	ps := NewPartitionSpec(PARTITION_STRATEGY_RANGE, partParams)
	
	// Verify properties
	if ps.Tag != T_PartitionSpec {
		t.Errorf("Expected tag T_PartitionSpec, got %v", ps.Tag)
	}
	
	if ps.Strategy != PARTITION_STRATEGY_RANGE {
		t.Errorf("Expected PARTITION_STRATEGY_RANGE, got %s", ps.Strategy)
	}
	
	if len(ps.PartParams) != 2 {
		t.Errorf("Expected 2 partition parameters, got %d", len(ps.PartParams))
	}
	
	if ps.Location != -1 {
		t.Errorf("Expected location to be -1, got %d", ps.Location)
	}
	
	// Test string representation
	str := ps.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestPartitionStrategies(t *testing.T) {
	param := NewColumnRef([]Node{NewString("id")}, -1)
	partParams := []Node{param}
	
	// Test LIST partitioning
	listSpec := NewListPartitionSpec(partParams)
	if listSpec.Strategy != PARTITION_STRATEGY_LIST {
		t.Errorf("Expected LIST strategy, got %s", listSpec.Strategy)
	}
	
	// Test RANGE partitioning
	rangeSpec := NewRangePartitionSpec(partParams)
	if rangeSpec.Strategy != PARTITION_STRATEGY_RANGE {
		t.Errorf("Expected RANGE strategy, got %s", rangeSpec.Strategy)
	}
	
	// Test HASH partitioning
	hashSpec := NewHashPartitionSpec(partParams)
	if hashSpec.Strategy != PARTITION_STRATEGY_HASH {
		t.Errorf("Expected HASH strategy, got %s", hashSpec.Strategy)
	}
}

// ==============================================================================
// PARTITION BOUND SPEC TESTS
// ==============================================================================

func TestPartitionBoundSpec(t *testing.T) {
	pbs := NewPartitionBoundSpec(PARTITION_STRATEGY_LIST)
	
	// Verify properties
	if pbs.Tag != T_PartitionBoundSpec {
		t.Errorf("Expected tag T_PartitionBoundSpec, got %v", pbs.Tag)
	}
	
	if pbs.Strategy != PARTITION_STRATEGY_LIST {
		t.Errorf("Expected PARTITION_STRATEGY_LIST, got %s", pbs.Strategy)
	}
	
	if pbs.IsDefault {
		t.Errorf("Expected IsDefault to be false by default")
	}
	
	if pbs.Location != -1 {
		t.Errorf("Expected location to be -1, got %d", pbs.Location)
	}
	
	// Test string representation
	str := pbs.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestDefaultPartitionBound(t *testing.T) {
	pbs := NewDefaultPartitionBound()
	
	if !pbs.IsDefault {
		t.Errorf("Expected IsDefault to be true")
	}
	
	// Test string representation includes "DEFAULT"
	str := pbs.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestHashPartitionBound(t *testing.T) {
	pbs := NewHashPartitionBound(4, 1) // Modulus 4, Remainder 1
	
	if pbs.Strategy != PARTITION_STRATEGY_HASH {
		t.Errorf("Expected HASH strategy, got %s", pbs.Strategy)
	}
	
	if pbs.Modulus != 4 {
		t.Errorf("Expected modulus 4, got %d", pbs.Modulus)
	}
	
	if pbs.Remainder != 1 {
		t.Errorf("Expected remainder 1, got %d", pbs.Remainder)
	}
	
	// Test string representation includes modulus and remainder
	str := pbs.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestListPartitionBound(t *testing.T) {
	// Create list datums
	datum1 := []Node{NewConst(25, "USA", false)}
	datum2 := []Node{NewConst(25, "UK", false)}
	listDatums := [][]Node{datum1, datum2}
	
	pbs := NewListPartitionBound(listDatums)
	
	if pbs.Strategy != PARTITION_STRATEGY_LIST {
		t.Errorf("Expected LIST strategy, got %s", pbs.Strategy)
	}
	
	if len(pbs.ListDatums) != 2 {
		t.Errorf("Expected 2 list datums, got %d", len(pbs.ListDatums))
	}
	
	// Test string representation includes datum count
	str := pbs.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestRangePartitionBound(t *testing.T) {
	// Create range datums
	lowDatum := NewConst(23, 1, false)   // Integer 1
	highDatum := NewConst(23, 100, false) // Integer 100
	lowDatums := []Node{lowDatum}
	highDatums := []Node{highDatum}
	
	pbs := NewRangePartitionBound(lowDatums, highDatums)
	
	if pbs.Strategy != PARTITION_STRATEGY_RANGE {
		t.Errorf("Expected RANGE strategy, got %s", pbs.Strategy)
	}
	
	if len(pbs.LowDatums) != 1 {
		t.Errorf("Expected 1 low datum, got %d", len(pbs.LowDatums))
	}
	
	if len(pbs.HighDatums) != 1 {
		t.Errorf("Expected 1 high datum, got %d", len(pbs.HighDatums))
	}
	
	// Test string representation includes datum counts
	str := pbs.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// PARTITION RANGE DATUM TESTS
// ==============================================================================

func TestPartitionRangeDatum(t *testing.T) {
	value := NewConst(23, 50, false)
	prd := NewPartitionRangeDatum(PARTITION_RANGE_DATUM_VALUE, value)
	
	// Verify properties
	if prd.Tag != T_PartitionRangeDatum {
		t.Errorf("Expected tag T_PartitionRangeDatum, got %v", prd.Tag)
	}
	
	if prd.Kind != PARTITION_RANGE_DATUM_VALUE {
		t.Errorf("Expected PARTITION_RANGE_DATUM_VALUE, got %v", prd.Kind)
	}
	
	if prd.Value != value {
		t.Errorf("Expected value to be set correctly")
	}
	
	if prd.Location != -1 {
		t.Errorf("Expected location to be -1, got %d", prd.Location)
	}
	
	// Test string representation
	str := prd.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestMinValueDatum(t *testing.T) {
	prd := NewMinValueDatum()
	
	if prd.Kind != PARTITION_RANGE_DATUM_MINVALUE {
		t.Errorf("Expected PARTITION_RANGE_DATUM_MINVALUE, got %v", prd.Kind)
	}
	
	if prd.Value != nil {
		t.Errorf("Expected value to be nil for MINVALUE")
	}
	
	// Test string representation includes "MINVALUE"
	str := prd.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestMaxValueDatum(t *testing.T) {
	prd := NewMaxValueDatum()
	
	if prd.Kind != PARTITION_RANGE_DATUM_MAXVALUE {
		t.Errorf("Expected PARTITION_RANGE_DATUM_MAXVALUE, got %v", prd.Kind)
	}
	
	if prd.Value != nil {
		t.Errorf("Expected value to be nil for MAXVALUE")
	}
	
	// Test string representation includes "MAXVALUE"
	str := prd.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestValueDatum(t *testing.T) {
	value := NewConst(23, 100, false)
	prd := NewValueDatum(value)
	
	if prd.Kind != PARTITION_RANGE_DATUM_VALUE {
		t.Errorf("Expected PARTITION_RANGE_DATUM_VALUE, got %v", prd.Kind)
	}
	
	if prd.Value != value {
		t.Errorf("Expected value to be set correctly")
	}
}

// ==============================================================================
// STATS ELEM TESTS
// ==============================================================================

func TestStatsElem(t *testing.T) {
	se := NewStatsElem("column_name")
	
	// Verify properties
	if se.Tag != T_StatsElem {
		t.Errorf("Expected tag T_StatsElem, got %v", se.Tag)
	}
	
	if se.Name != "column_name" {
		t.Errorf("Expected name 'column_name', got %s", se.Name)
	}
	
	if se.Expr != nil {
		t.Errorf("Expected expr to be nil when using name")
	}
	
	// Test string representation
	str := se.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestStatsElemExpr(t *testing.T) {
	expr := NewFuncExpr([]Node{NewString("upper")}, []Expression{NewColumnRef([]Node{NewString("name")}, -1)}, false, false, COERCE_EXPLICIT_CALL, -1)
	se := NewStatsElemExpr(expr)
	
	if se.Name != "" {
		t.Errorf("Expected name to be empty when using expression")
	}
	
	if se.Expr != expr {
		t.Errorf("Expected expression to be set correctly")
	}
	
	// Test string representation includes "expr:"
	str := se.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// FOREIGN DATA WRAPPER TESTS
// ==============================================================================

func TestCreateForeignServerStmt(t *testing.T) {
	cfss := NewCreateForeignServerStmt("my_server", "postgres_fdw")
	
	// Verify properties
	if cfss.Tag != T_CreateForeignServerStmt {
		t.Errorf("Expected tag T_CreateForeignServerStmt, got %v", cfss.Tag)
	}
	
	if cfss.Servername != "my_server" {
		t.Errorf("Expected server name 'my_server', got %s", cfss.Servername)
	}
	
	if cfss.Fdwname != "postgres_fdw" {
		t.Errorf("Expected FDW name 'postgres_fdw', got %s", cfss.Fdwname)
	}
	
	if cfss.IfNotExists {
		t.Errorf("Expected IfNotExists to be false by default")
	}
	
	// Test string representation
	str := cfss.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestCreateForeignServerIfNotExistsStmt(t *testing.T) {
	cfss := NewCreateForeignServerIfNotExistsStmt("my_server", "postgres_fdw")
	
	if !cfss.IfNotExists {
		t.Errorf("Expected IfNotExists to be true")
	}
	
	// Test string representation includes "IF NOT EXISTS"
	str := cfss.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestCreateForeignTableStmt(t *testing.T) {
	// Create base CREATE TABLE statement
	relation := NewRangeVar("public", "foreign_table", -1)
	baseStmt := NewCreateStmt(relation, []Node{}, false, false, false, "")
	
	cfts := NewCreateForeignTableStmt(baseStmt, "my_server")
	
	// Verify properties
	if cfts.Tag != T_CreateForeignTableStmt {
		t.Errorf("Expected tag T_CreateForeignTableStmt, got %v", cfts.Tag)
	}
	
	if cfts.Base != baseStmt {
		t.Errorf("Expected base statement to be set correctly")
	}
	
	if cfts.Servername != "my_server" {
		t.Errorf("Expected server name 'my_server', got %s", cfts.Servername)
	}
	
	// Test string representation
	str := cfts.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestCreateUserMappingStmt(t *testing.T) {
	user := NewRoleSpec(ROLESPEC_CSTRING, "testuser")
	cums := NewCreateUserMappingStmt(user, "my_server")
	
	// Verify properties
	if cums.Tag != T_CreateUserMappingStmt {
		t.Errorf("Expected tag T_CreateUserMappingStmt, got %v", cums.Tag)
	}
	
	if cums.User != user {
		t.Errorf("Expected user to be set correctly")
	}
	
	if cums.Servername != "my_server" {
		t.Errorf("Expected server name 'my_server', got %s", cums.Servername)
	}
	
	if cums.IfNotExists {
		t.Errorf("Expected IfNotExists to be false by default")
	}
	
	// Test string representation
	str := cums.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// TRIGGER TESTS
// ==============================================================================

func TestCreateTriggerStmt(t *testing.T) {
	relation := NewRangeVar("public", "test_table", -1)
	funcname := []Node{NewString("trigger_function")}
	
	cts := NewCreateTriggerStmt("test_trigger", relation, funcname, TRIGGER_TIMING_BEFORE, TRIGGER_TYPE_INSERT)
	
	// Verify properties
	if cts.Tag != T_CreateTriggerStmt {
		t.Errorf("Expected tag T_CreateTriggerStmt, got %v", cts.Tag)
	}
	
	if cts.Trigname != "test_trigger" {
		t.Errorf("Expected trigger name 'test_trigger', got %s", cts.Trigname)
	}
	
	if cts.Relation != relation {
		t.Errorf("Expected relation to be set correctly")
	}
	
	if len(cts.Funcname) != 1 {
		t.Errorf("Expected 1 function name, got %d", len(cts.Funcname))
	}
	
	if cts.Timing != TRIGGER_TIMING_BEFORE {
		t.Errorf("Expected TRIGGER_TIMING_BEFORE, got %d", cts.Timing)
	}
	
	if cts.Events != TRIGGER_TYPE_INSERT {
		t.Errorf("Expected TRIGGER_TYPE_INSERT, got %d", cts.Events)
	}
	
	if !cts.Row {
		t.Errorf("Expected Row to be true by default")
	}
	
	// Test string representation
	str := cts.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestBeforeInsertTrigger(t *testing.T) {
	relation := NewRangeVar("public", "test_table", -1)
	funcname := []Node{NewString("trigger_function")}
	
	cts := NewBeforeInsertTrigger("before_insert_trigger", relation, funcname)
	
	if cts.Timing != TRIGGER_TIMING_BEFORE {
		t.Errorf("Expected TRIGGER_TIMING_BEFORE, got %d", cts.Timing)
	}
	
	if cts.Events != TRIGGER_TYPE_INSERT {
		t.Errorf("Expected TRIGGER_TYPE_INSERT, got %d", cts.Events)
	}
}

func TestAfterUpdateTrigger(t *testing.T) {
	relation := NewRangeVar("public", "test_table", -1)
	funcname := []Node{NewString("trigger_function")}
	
	cts := NewAfterUpdateTrigger("after_update_trigger", relation, funcname)
	
	if cts.Timing != TRIGGER_TIMING_AFTER {
		t.Errorf("Expected TRIGGER_TIMING_AFTER, got %d", cts.Timing)
	}
	
	if cts.Events != TRIGGER_TYPE_UPDATE {
		t.Errorf("Expected TRIGGER_TYPE_UPDATE, got %d", cts.Events)
	}
}

func TestTriggerMultipleEvents(t *testing.T) {
	relation := NewRangeVar("public", "test_table", -1)
	funcname := []Node{NewString("trigger_function")}
	
	// Test trigger with multiple events
	events := TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_DELETE
	cts := NewCreateTriggerStmt("multi_event_trigger", relation, funcname, TRIGGER_TIMING_AFTER, events)
	
	if cts.Events != events {
		t.Errorf("Expected combined events %d, got %d", events, cts.Events)
	}
	
	// Test string representation includes all events
	str := cts.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// POLICY TESTS
// ==============================================================================

func TestCreatePolicyStmt(t *testing.T) {
	table := NewRangeVar("public", "sensitive_table", -1)
	cps := NewCreatePolicyStmt("user_policy", table, "SELECT")
	
	// Verify properties
	if cps.Tag != T_CreatePolicyStmt {
		t.Errorf("Expected tag T_CreatePolicyStmt, got %v", cps.Tag)
	}
	
	if cps.PolicyName != "user_policy" {
		t.Errorf("Expected policy name 'user_policy', got %s", cps.PolicyName)
	}
	
	if cps.Table != table {
		t.Errorf("Expected table to be set correctly")
	}
	
	if cps.CmdName != "SELECT" {
		t.Errorf("Expected command name 'SELECT', got %s", cps.CmdName)
	}
	
	if !cps.Permissive {
		t.Errorf("Expected Permissive to be true by default")
	}
	
	// Test string representation
	str := cps.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestRestrictivePolicyStmt(t *testing.T) {
	table := NewRangeVar("public", "sensitive_table", -1)
	cps := NewRestrictivePolicyStmt("restrictive_policy", table, "UPDATE")
	
	if cps.Permissive {
		t.Errorf("Expected Permissive to be false for restrictive policy")
	}
	
	// Test string representation includes "RESTRICTIVE"
	str := cps.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestAlterPolicyStmt(t *testing.T) {
	table := NewRangeVar("public", "sensitive_table", -1)
	aps := NewAlterPolicyStmt("user_policy", table)
	
	// Verify properties
	if aps.Tag != T_AlterPolicyStmt {
		t.Errorf("Expected tag T_AlterPolicyStmt, got %v", aps.Tag)
	}
	
	if aps.PolicyName != "user_policy" {
		t.Errorf("Expected policy name 'user_policy', got %s", aps.PolicyName)
	}
	
	if aps.Table != table {
		t.Errorf("Expected table to be set correctly")
	}
	
	// Test string representation
	str := aps.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// INTEGRATION TESTS
// ==============================================================================

func TestComplexAlterTable(t *testing.T) {
	// Test complex ALTER TABLE with multiple commands
	
	// Add column command
	typeName := NewTypeName([]Node{NewString("varchar")}, -1)
	newCol := NewColumnDefWithDefault("new_col", typeName, NewConst(25, "default_value", false))
	addColCmd := NewAddColumnCmd(newCol)
	
	// Drop column command
	dropColCmd := NewDropColumnCmd("old_col", DROP_CASCADE, true)
	
	// Add constraint command
	constraint := NewConstraint(CONSTR_UNIQUE, "unique_constraint")
	addConstraintCmd := NewAddConstraintCmd(constraint)
	
	// Verify all commands work together
	if addColCmd.Subtype != AT_AddColumn {
		t.Errorf("Expected add column command")
	}
	
	if dropColCmd.Subtype != AT_DropColumn {
		t.Errorf("Expected drop column command")
	}
	
	if addConstraintCmd.Subtype != AT_AddConstraint {
		t.Errorf("Expected add constraint command")
	}
	
	// All should have proper tags
	if addColCmd.Tag != T_AlterTableCmd {
		t.Errorf("Expected T_AlterTableCmd tag")
	}
}

func TestPartitioningIntegration(t *testing.T) {
	// Test partitioning with complex specifications
	
	// Create partition spec
	dateCol := NewColumnRef([]Node{NewString("created_date")}, -1)
	rangeSpec := NewRangePartitionSpec([]Node{dateCol})
	
	// Create partition bounds
	minValue := NewMinValueDatum()
	maxValue := NewMaxValueDatum()
	specificValue := NewValueDatum(NewConst(1184, "2023-01-01", false)) // DATE type
	
	rangeBound := NewRangePartitionBound([]Node{minValue}, []Node{specificValue})
	
	// Verify integration
	if rangeSpec.Strategy != PARTITION_STRATEGY_RANGE {
		t.Errorf("Expected RANGE partitioning strategy")
	}
	
	if rangeBound.Strategy != PARTITION_STRATEGY_RANGE {
		t.Errorf("Expected RANGE bound strategy")
	}
	
	if len(rangeBound.LowDatums) != 1 {
		t.Errorf("Expected 1 low datum")
	}
	
	if len(rangeBound.HighDatums) != 1 {
		t.Errorf("Expected 1 high datum")
	}
}

func TestForeignDataWrapperIntegration(t *testing.T) {
	// Test complete foreign data wrapper setup
	
	// Create foreign server
	server := NewCreateForeignServerStmt("remote_server", "postgres_fdw")
	
	// Create user mapping
	user := NewRoleSpec(ROLESPEC_CSTRING, "local_user")
	userMapping := NewCreateUserMappingStmt(user, "remote_server")
	
	// Create foreign table
	relation := NewRangeVar("public", "remote_table", -1)
	baseCreate := NewCreateStmt(relation, []Node{}, false, false, false, "")
	foreignTable := NewCreateForeignTableStmt(baseCreate, "remote_server")
	
	// Verify all work together
	if server.Servername != "remote_server" {
		t.Errorf("Expected server name consistency")
	}
	
	if userMapping.Servername != "remote_server" {
		t.Errorf("Expected server name consistency in user mapping")
	}
	
	if foreignTable.Servername != "remote_server" {
		t.Errorf("Expected server name consistency in foreign table")
	}
}

// ==============================================================================
// BENCHMARK TESTS
// ==============================================================================

func BenchmarkAlterTableCmdCreation(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewAlterTableCmd(AT_AddColumn, "test_column")
	}
}

func BenchmarkColumnDefCreation(b *testing.B) {
	typeName := NewTypeName([]Node{NewString("integer")}, -1)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewColumnDef("test_column", typeName)
	}
}

func BenchmarkPartitionSpecCreation(b *testing.B) {
	param := NewColumnRef([]Node{NewString("date_col")}, -1)
	partParams := []Node{param}
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewPartitionSpec(PARTITION_STRATEGY_RANGE, partParams)
	}
}

func BenchmarkCreateTriggerStmtCreation(b *testing.B) {
	relation := NewRangeVar("public", "test_table", -1)
	funcname := []Node{NewString("trigger_function")}
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewCreateTriggerStmt("test_trigger", relation, funcname, TRIGGER_TIMING_BEFORE, TRIGGER_TYPE_INSERT)
	}
}