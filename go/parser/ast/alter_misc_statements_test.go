package ast

import (
	"testing"
)

func TestAlterObjectSchemaStmt(t *testing.T) {
	stmt := NewAlterObjectSchemaStmt(OBJECT_TABLE, "public")
	
	// Test basic properties
	if stmt.Tag != T_AlterObjectSchemaStmt {
		t.Errorf("Expected Tag %v, got %v", T_AlterObjectSchemaStmt, stmt.Tag)
	}
	
	if stmt.ObjectType != OBJECT_TABLE {
		t.Errorf("Expected ObjectType %v, got %v", OBJECT_TABLE, stmt.ObjectType)
	}
	
	if stmt.NewSchema != "public" {
		t.Errorf("Expected NewSchema 'public', got %s", stmt.NewSchema)
	}
	
	// Test SQL string generation
	stmt.Relation = &RangeVar{RelName: "mytable"}
	sql := stmt.SqlString()
	expected := "ALTER TABLE ONLY mytable SET SCHEMA public"
	if sql != expected {
		t.Errorf("Expected SQL: %s, got: %s", expected, sql)
	}
}

func TestAlterOperatorStmt(t *testing.T) {
	opName := &ObjectWithArgs{
		Objname: &NodeList{Items: []Node{&String{SVal: "+"}}},
	}
	
	options := &NodeList{Items: []Node{
		&DefElem{Defname: "RESTRICT", Arg: &String{SVal: "numeric_eq"}},
	}}
	
	stmt := NewAlterOperatorStmt(opName, options)
	
	// Test basic properties
	if stmt.Tag != T_AlterOperatorStmt {
		t.Errorf("Expected Tag %v, got %v", T_AlterOperatorStmt, stmt.Tag)
	}
	
	if stmt.Opername != opName {
		t.Errorf("Expected Opername %v, got %v", opName, stmt.Opername)
	}
}

func TestAlterCollationStmt(t *testing.T) {
	collname := &NodeList{Items: []Node{
		&String{SVal: "mycollation"},
	}}
	
	stmt := NewAlterCollationStmt(collname)
	
	// Test basic properties
	if stmt.Tag != T_AlterCollationStmt {
		t.Errorf("Expected Tag %v, got %v", T_AlterCollationStmt, stmt.Tag)
	}
	
	// Test SQL string generation
	sql := stmt.SqlString()
	expected := "ALTER COLLATION mycollation REFRESH VERSION"
	if sql != expected {
		t.Errorf("Expected SQL: %s, got: %s", expected, sql)
	}
}

func TestAlterDatabaseStmt(t *testing.T) {
	options := &NodeList{Items: []Node{
		&DefElem{Defname: "CONNECTION LIMIT", Arg: &Integer{IVal: 100}},
	}}
	
	stmt := NewAlterDatabaseStmt("mydb", options)
	
	// Test basic properties  
	if stmt.Tag != T_AlterDatabaseStmt {
		t.Errorf("Expected Tag %v, got %v", T_AlterDatabaseStmt, stmt.Tag)
	}
	
	if stmt.Dbname != "mydb" {
		t.Errorf("Expected Dbname 'mydb', got %s", stmt.Dbname)
	}
}

func TestAlterDatabaseSetStmt(t *testing.T) {
	setstmt := &VariableSetStmt{
		Kind: VAR_SET_VALUE,
		Name: "timezone",
		Args: &NodeList{Items: []Node{&String{SVal: "UTC"}}},
	}
	
	stmt := NewAlterDatabaseSetStmt("mydb", setstmt)
	
	// Test basic properties
	if stmt.Tag != T_AlterDatabaseSetStmt {
		t.Errorf("Expected Tag %v, got %v", T_AlterDatabaseSetStmt, stmt.Tag)
	}
	
	if stmt.Dbname != "mydb" {
		t.Errorf("Expected Dbname 'mydb', got %s", stmt.Dbname)
	}
}

func TestAlterCompositeTypeStmt(t *testing.T) {
	typeName := &NodeList{Items: []Node{
		&String{SVal: "mytype"},
	}}
	
	cmds := &NodeList{Items: []Node{
		&AlterTableCmd{Subtype: AT_AddColumn, Name: "newfield"},
	}}
	
	stmt := NewAlterCompositeTypeStmt(typeName, cmds)
	
	// Test basic properties
	if stmt.Tag != T_AlterCompositeTypeStmt {
		t.Errorf("Expected Tag %v, got %v", T_AlterCompositeTypeStmt, stmt.Tag)
	}
}

func TestAlterTSConfigurationStmt(t *testing.T) {
	cfgname := &NodeList{Items: []Node{
		&String{SVal: "english"},
	}}
	
	stmt := NewAlterTSConfigurationStmt(ALTER_TSCONFIG_ADD_MAPPING, cfgname)
	
	// Test basic properties
	if stmt.Tag != T_AlterTSConfigurationStmt {
		t.Errorf("Expected Tag %v, got %v", T_AlterTSConfigurationStmt, stmt.Tag)
	}
	
	if stmt.Kind != ALTER_TSCONFIG_ADD_MAPPING {
		t.Errorf("Expected Kind %v, got %v", ALTER_TSCONFIG_ADD_MAPPING, stmt.Kind)
	}
}

func TestAlterTSDictionaryStmt(t *testing.T) {
	dictname := &NodeList{Items: []Node{
		&String{SVal: "english_stem"},
	}}
	
	options := &NodeList{Items: []Node{
		&DefElem{Defname: "StopWords", Arg: &String{SVal: "english"}},
	}}
	
	stmt := NewAlterTSDictionaryStmt(dictname, options)
	
	// Test basic properties
	if stmt.Tag != T_AlterTSDictionaryStmt {
		t.Errorf("Expected Tag %v, got %v", T_AlterTSDictionaryStmt, stmt.Tag)
	}
}

func TestAlterObjectDependsStmt(t *testing.T) {
	extname := &String{SVal: "pg_stat_statements"}
	
	stmt := NewAlterObjectDependsStmt(OBJECT_FUNCTION, extname, false)
	
	// Test basic properties
	if stmt.Tag != T_AlterObjectDependsStmt {
		t.Errorf("Expected Tag %v, got %v", T_AlterObjectDependsStmt, stmt.Tag)
	}
	
	if stmt.ObjectType != OBJECT_FUNCTION {
		t.Errorf("Expected ObjectType %v, got %v", OBJECT_FUNCTION, stmt.ObjectType)
	}
	
	if stmt.Remove != false {
		t.Errorf("Expected Remove false, got %v", stmt.Remove)
	}
}

func TestAlterTSConfigType(t *testing.T) {
	tests := []struct {
		configType AlterTSConfigType
		expected   string
	}{
		{ALTER_TSCONFIG_ADD_MAPPING, "ADD MAPPING"},
		{ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN, "ALTER MAPPING FOR"},
		{ALTER_TSCONFIG_REPLACE_DICT, "REPLACE"},
		{ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN, "REPLACE FOR"},
		{ALTER_TSCONFIG_DROP_MAPPING, "DROP MAPPING"},
	}
	
	for _, test := range tests {
		if test.configType.String() != test.expected {
			t.Errorf("Expected %s, got %s", test.expected, test.configType.String())
		}
	}
}

func TestNodeListToQualifiedName(t *testing.T) {
	// Test with schema.table
	nodeList := &NodeList{Items: []Node{
		&String{SVal: "myschema"},
		&String{SVal: "mytable"},
	}}
	
	result := nodeListToQualifiedName(nodeList)
	expected := "myschema.mytable"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
	
	// Test with nil
	result = nodeListToQualifiedName(nil)
	if result != "" {
		t.Errorf("Expected empty string, got %s", result)
	}
	
	// Test with empty list
	emptyList := &NodeList{Items: []Node{}}
	result = nodeListToQualifiedName(emptyList)
	if result != "" {
		t.Errorf("Expected empty string, got %s", result)
	}
}