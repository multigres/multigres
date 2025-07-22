// Package ast provides comprehensive tests for PostgreSQL AST administrative and advanced DDL nodes.
// These tests ensure correctness of PostgreSQL's advanced DDL operations, partitioning, and administrative features.
package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ==============================================================================
// ALTER TABLE CMD TESTS
// ==============================================================================

func TestAlterTableCmd(t *testing.T) {
	// Test basic AlterTableCmd creation
	atc := NewAlterTableCmd(AT_AddColumn, "new_column", nil)
	
	// Verify properties
	assert.Equal(t, T_AlterTableCmd, atc.Tag, "Expected tag T_AlterTableCmd")
	assert.Equal(t, AT_AddColumn, atc.Subtype, "Expected AT_AddColumn")
	assert.Equal(t, "new_column", atc.Name, "Expected name 'new_column'")
	
	// Test string representation
	str := atc.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestAddColumnCmd(t *testing.T) {
	// Create a column definition
	typeName := NewTypeName([]string{"integer"})
	
	atc := NewAddColumnCmd("test_col", typeName)
	
	assert.Equal(t, AT_AddColumn, atc.Subtype, "Expected AT_AddColumn")
	assert.Equal(t, "test_col", atc.Name, "Expected column name to be set correctly")
	
	// Test string representation
	str := atc.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestDropColumnCmd(t *testing.T) {
	atc := NewDropColumnCmd("old_column", DROP_CASCADE)
	
	assert.Equal(t, AT_DropColumn, atc.Subtype, "Expected AT_DropColumn")
	assert.Equal(t, "old_column", atc.Name, "Expected name 'old_column'")
	assert.Equal(t, DROP_CASCADE, atc.Behavior, "Expected DROP_CASCADE")
}

func TestAddConstraintCmd(t *testing.T) {
	constraint := NewConstraint(CONSTR_PRIMARY)
	atc := NewAddConstraintCmd(constraint)
	
	assert.Equal(t, AT_AddConstraint, atc.Subtype, "Expected AT_AddConstraint")
	assert.Equal(t, constraint, atc.Def, "Expected constraint definition to be set correctly")
}

func TestDropConstraintCmd(t *testing.T) {
	atc := NewDropConstraintCmd("test_constraint", DROP_RESTRICT)
	
	assert.Equal(t, AT_DropConstraint, atc.Subtype, "Expected AT_DropConstraint")
	assert.Equal(t, "test_constraint", atc.Name, "Expected name 'test_constraint'")
	assert.Equal(t, DROP_RESTRICT, atc.Behavior, "Expected DROP_RESTRICT")
}

// ==============================================================================
// BENCHMARK TESTS
// ==============================================================================

func BenchmarkAlterTableCmdCreation(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewAlterTableCmd(AT_AddColumn, "test_column", nil)
	}
}