// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2025, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
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
	atc := NewDropColumnCmd("old_column", DropCascade)

	assert.Equal(t, AT_DropColumn, atc.Subtype, "Expected AT_DropColumn")
	assert.Equal(t, "old_column", atc.Name, "Expected name 'old_column'")
	assert.Equal(t, DropCascade, atc.Behavior, "Expected DropCascade")
}

func TestAddConstraintCmd(t *testing.T) {
	constraint := NewConstraint(CONSTR_PRIMARY)
	atc := NewAddConstraintCmd(constraint)

	assert.Equal(t, AT_AddConstraint, atc.Subtype, "Expected AT_AddConstraint")
	assert.Equal(t, constraint, atc.Def, "Expected constraint definition to be set correctly")
}

func TestDropConstraintCmd(t *testing.T) {
	atc := NewDropConstraintCmd("test_constraint", DropRestrict)

	assert.Equal(t, AT_DropConstraint, atc.Subtype, "Expected AT_DropConstraint")
	assert.Equal(t, "test_constraint", atc.Name, "Expected name 'test_constraint'")
	assert.Equal(t, DropRestrict, atc.Behavior, "Expected DropRestrict")
}

// ==============================================================================
// TRIGGER TESTS
// ==============================================================================

func TestCreateTriggerStmt(t *testing.T) {
	// Create basic components
	relation := NewRangeVar("test_table", "", "")
	funcname := NewNodeList(NewString("trigger_function"))

	// Test basic trigger creation
	trigger := NewCreateTriggerStmt("test_trigger", relation, funcname, TRIGGER_TIMING_BEFORE, TRIGGER_TYPE_INSERT)

	assert.Equal(t, T_CreateTriggerStmt, trigger.Tag, "Expected tag T_CreateTriggerStmt")
	assert.Equal(t, "test_trigger", trigger.Trigname, "Expected trigger name")
	assert.Equal(t, relation, trigger.Relation, "Expected relation to be set")
	assert.Equal(t, funcname, trigger.Funcname, "Expected function name to be set")
	assert.Equal(t, int16(TRIGGER_TIMING_BEFORE), trigger.Timing, "Expected BEFORE timing")
	assert.Equal(t, int16(TRIGGER_TYPE_INSERT), trigger.Events, "Expected INSERT event")
	assert.True(t, trigger.Row, "Expected ROW trigger by default")
	assert.False(t, trigger.IsConstraint, "Expected non-constraint trigger")
	assert.False(t, trigger.Deferrable, "Expected non-deferrable by default")
	assert.False(t, trigger.Initdeferred, "Expected initially immediate by default")

	// Test string representation
	str := trigger.String()
	assert.Contains(t, str, "test_trigger", "String should contain trigger name")
	assert.Contains(t, str, "BEFORE", "String should contain timing")
	assert.Contains(t, str, "INSERT", "String should contain event")
}

func TestBeforeInsertTrigger(t *testing.T) {
	relation := NewRangeVar("users", "", "")
	funcname := NewNodeList(NewString("audit_function"))

	trigger := NewBeforeInsertTrigger("audit_trigger", relation, funcname)

	assert.Equal(t, "audit_trigger", trigger.Trigname, "Expected trigger name")
	assert.Equal(t, int16(TRIGGER_TIMING_BEFORE), trigger.Timing, "Expected BEFORE timing")
	assert.Equal(t, int16(TRIGGER_TYPE_INSERT), trigger.Events, "Expected INSERT event")
	assert.True(t, trigger.Row, "Expected ROW trigger")
}

func TestAfterUpdateTrigger(t *testing.T) {
	relation := NewRangeVar("products", "", "")
	funcname := NewNodeList(NewString("update_timestamp"))

	trigger := NewAfterUpdateTrigger("timestamp_trigger", relation, funcname)

	assert.Equal(t, "timestamp_trigger", trigger.Trigname, "Expected trigger name")
	assert.Equal(t, int16(TRIGGER_TIMING_AFTER), trigger.Timing, "Expected AFTER timing")
	assert.Equal(t, int16(TRIGGER_TYPE_UPDATE), trigger.Events, "Expected UPDATE event")
	assert.True(t, trigger.Row, "Expected ROW trigger")
}

func TestConstraintTrigger(t *testing.T) {
	relation := NewRangeVar("orders", "", "")
	constrrel := NewRangeVar("customers", "", "")
	funcname := NewNodeList(NewString("fk_constraint_function"))

	trigger := NewConstraintTrigger("fk_trigger", relation, funcname, TRIGGER_TIMING_AFTER, TRIGGER_TYPE_INSERT, constrrel)

	assert.Equal(t, "fk_trigger", trigger.Trigname, "Expected trigger name")
	assert.True(t, trigger.IsConstraint, "Expected constraint trigger")
	assert.Equal(t, constrrel, trigger.Constrrel, "Expected constraint relation")
	assert.False(t, trigger.Deferrable, "Expected non-deferrable by default")
	assert.False(t, trigger.Initdeferred, "Expected initially immediate by default")

	// Test string representation includes constraint info
	str := trigger.String()
	assert.Contains(t, str, "CONSTRAINT", "String should contain CONSTRAINT")
	assert.Contains(t, str, "customers", "String should contain constraint relation")
	assert.Contains(t, str, "NOT DEFERRABLE", "String should contain deferability info")
}

func TestDeferrableConstraintTrigger(t *testing.T) {
	relation := NewRangeVar("order_items", "", "")
	constrrel := NewRangeVar("products", "", "")
	funcname := NewNodeList(NewString("check_stock_function"))

	// Test initially deferred
	trigger := NewDeferrableConstraintTrigger("stock_check", relation, funcname, TRIGGER_TIMING_AFTER, TRIGGER_TYPE_INSERT, constrrel, true)

	assert.Equal(t, "stock_check", trigger.Trigname, "Expected trigger name")
	assert.True(t, trigger.IsConstraint, "Expected constraint trigger")
	assert.True(t, trigger.Deferrable, "Expected deferrable trigger")
	assert.True(t, trigger.Initdeferred, "Expected initially deferred")

	// Test string representation
	str := trigger.String()
	assert.Contains(t, str, "DEFERRABLE", "String should contain DEFERRABLE")
	assert.Contains(t, str, "INITIALLY DEFERRED", "String should contain INITIALLY DEFERRED")

	// Test initially immediate
	trigger2 := NewDeferrableConstraintTrigger("stock_check2", relation, funcname, TRIGGER_TIMING_AFTER, TRIGGER_TYPE_INSERT, constrrel, false)
	assert.True(t, trigger2.Deferrable, "Expected deferrable trigger")
	assert.False(t, trigger2.Initdeferred, "Expected initially immediate")

	str2 := trigger2.String()
	assert.Contains(t, str2, "INITIALLY IMMEDIATE", "String should contain INITIALLY IMMEDIATE")
}

func TestTriggerWithTransitions(t *testing.T) {
	relation := NewRangeVar("audit_table", "", "")
	funcname := NewNodeList(NewString("audit_function"))

	// Create trigger with transition tables
	trigger := NewCreateTriggerStmt("audit_trigger", relation, funcname, TRIGGER_TIMING_AFTER, TRIGGER_TYPE_UPDATE)

	// Add transition tables
	oldTransition := &TriggerTransition{
		BaseNode: BaseNode{Tag: T_TriggerTransition},
		Name:     "old_table",
		IsNew:    false,
		IsTable:  true,
	}
	newTransition := &TriggerTransition{
		BaseNode: BaseNode{Tag: T_TriggerTransition},
		Name:     "new_table",
		IsNew:    true,
		IsTable:  true,
	}

	trigger.Transitions = NewNodeList()
	trigger.Transitions.Append(oldTransition)
	trigger.Transitions.Append(newTransition)

	assert.Equal(t, 2, trigger.Transitions.Len(), "Expected 2 transition tables")

	// Test string representation includes transition info
	str := trigger.String()
	assert.Contains(t, str, "(2 transitions)", "String should contain transition count")
}

func TestTriggerEventCombinations(t *testing.T) {
	relation := NewRangeVar("test_table", "", "")
	funcname := NewNodeList(NewString("multi_event_function"))

	// Test multiple events combined
	combinedEvents := int16(TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_DELETE)
	trigger := NewCreateTriggerStmt("multi_trigger", relation, funcname, TRIGGER_TIMING_BEFORE, combinedEvents)

	assert.Equal(t, combinedEvents, trigger.Events, "Expected combined events")

	// Test string representation includes all events
	str := trigger.String()
	assert.Contains(t, str, "INSERT", "String should contain INSERT")
	assert.Contains(t, str, "UPDATE", "String should contain UPDATE")
	assert.Contains(t, str, "DELETE", "String should contain DELETE")
}

// ==============================================================================
// BENCHMARK TESTS
// ==============================================================================

func BenchmarkAlterTableCmdCreation(b *testing.B) {
	for b.Loop() {
		_ = NewAlterTableCmd(AT_AddColumn, "test_column", nil)
	}
}

func BenchmarkCreateTriggerStmt(b *testing.B) {
	relation := NewRangeVar("test_table", "", "")
	funcname := NewNodeList(NewString("trigger_function"))

	for b.Loop() {
		_ = NewCreateTriggerStmt("test_trigger", relation, funcname, TRIGGER_TIMING_BEFORE, TRIGGER_TYPE_INSERT)
	}
}

func BenchmarkConstraintTriggerCreation(b *testing.B) {
	relation := NewRangeVar("orders", "", "")
	constrrel := NewRangeVar("customers", "", "")
	funcname := NewNodeList(NewString("fk_function"))

	for b.Loop() {
		_ = NewConstraintTrigger("fk_trigger", relation, funcname, TRIGGER_TIMING_AFTER, TRIGGER_TYPE_INSERT, constrrel)
	}
}
