// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionStateHash(t *testing.T) {
	// Same settings should produce same hash
	state1 := NewConnectionState(map[string]string{
		"timezone":          "UTC",
		"search_path":       "public",
		"statement_timeout": "30s",
	})
	state2 := NewConnectionState(map[string]string{
		"timezone":          "UTC",
		"search_path":       "public",
		"statement_timeout": "30s",
	})

	assert.Equal(t, state1.Hash(), state2.Hash(), "identical states should have same hash")

	// Different settings should produce different hash (usually)
	state3 := NewConnectionState(map[string]string{
		"timezone": "America/New_York",
	})
	assert.NotEqual(t, state1.Hash(), state3.Hash(), "different states should have different hash")
}

func TestConnectionStateEquals(t *testing.T) {
	state1 := NewConnectionState(map[string]string{
		"timezone":    "UTC",
		"search_path": "public",
	})

	state2 := NewConnectionState(map[string]string{
		"timezone":    "UTC",
		"search_path": "public",
	})

	assert.True(t, state1.Equals(state2), "identical states should be equal")
	assert.True(t, state2.Equals(state1), "equality should be symmetric")

	// Test inequality
	state3 := NewConnectionState(map[string]string{
		"timezone": "America/New_York",
	})

	assert.False(t, state1.Equals(state3), "different states should not be equal")
}

func TestConnectionStateNilEquals(t *testing.T) {
	var nilState *ConnectionState
	state := NewConnectionState(map[string]string{"timezone": "UTC"})

	assert.True(t, nilState.Equals(nilState), "nil should equal nil")
	assert.False(t, nilState.Equals(state), "nil should not equal non-nil")
	assert.False(t, state.Equals(nilState), "non-nil should not equal nil")
}

func TestConnectionStateIsClean(t *testing.T) {
	// Nil state is clean
	var nilState *ConnectionState
	assert.True(t, nilState.IsClean(), "nil state should be clean")

	// Empty state is clean
	emptyState := NewConnectionState(map[string]string{})
	assert.True(t, emptyState.IsClean(), "empty state should be clean")

	// State with settings is not clean
	stateWithSettings := NewConnectionState(map[string]string{"timezone": "UTC"})
	assert.False(t, stateWithSettings.IsClean(), "state with settings should not be clean")
}

func TestConnectionStateClone(t *testing.T) {
	original := NewConnectionState(map[string]string{
		"timezone":    "UTC",
		"search_path": "public",
	})

	clone := original.Clone()
	require.NotNil(t, clone)

	// Clone should be equal but not same pointer
	assert.True(t, original.Equals(clone), "clone should be equal to original")
	assert.NotSame(t, original, clone, "clone should be different pointer")

	// Modifying clone should not affect original
	clone.Settings["timezone"] = "America/New_York"
	assert.NotEqual(t, original.Settings["timezone"], clone.Settings["timezone"],
		"modifying clone should not affect original")
}

func TestConnectionStateApplySetting(t *testing.T) {
	state := NewConnectionState(map[string]string{
		"timezone": "UTC",
	})

	oldHash := state.Hash()

	// Apply new setting
	state.ApplySetting("search_path", "public")
	assert.Equal(t, "public", state.Settings["search_path"],
		"should add new setting")
	assert.Equal(t, "UTC", state.Settings["timezone"],
		"should keep existing setting")

	// Hash should be invalidated and different after modification
	assert.NotEqual(t, oldHash, state.Hash(),
		"hash should change after modifying state")

	// Override existing setting
	state.ApplySetting("timezone", "America/New_York")
	assert.Equal(t, "America/New_York", state.Settings["timezone"],
		"should override existing setting")
}

func TestConnectionStateRemoveSetting(t *testing.T) {
	state := NewConnectionState(map[string]string{
		"timezone":    "UTC",
		"search_path": "public",
	})

	oldHash := state.Hash()

	// Remove a setting
	state.RemoveSetting("timezone")
	assert.NotContains(t, state.Settings, "timezone",
		"removed setting should not be present")
	assert.Contains(t, state.Settings, "search_path",
		"other settings should remain")

	// Hash should be invalidated and different after modification
	assert.NotEqual(t, oldHash, state.Hash(),
		"hash should change after removing setting")

	// Removing non-existent setting should not panic
	state.RemoveSetting("non_existent")
}

func TestConnectionStateGenerateApplySQL(t *testing.T) {
	tests := []struct {
		name     string
		state    *ConnectionState
		expected []string
	}{
		{
			name:     "nil state",
			state:    nil,
			expected: nil,
		},
		{
			name:     "clean state",
			state:    NewConnectionState(map[string]string{}),
			expected: nil,
		},
		{
			name: "single setting",
			state: NewConnectionState(map[string]string{
				"timezone": "UTC",
			}),
			expected: []string{
				"SET SESSION timezone = 'UTC'",
			},
		},
		{
			name: "multiple settings",
			state: NewConnectionState(map[string]string{
				"timezone":    "UTC",
				"search_path": "public,pg_catalog",
			}),
			expected: []string{
				"SET SESSION search_path = 'public,pg_catalog'",
				"SET SESSION timezone = 'UTC'",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := tt.state.GenerateApplySQL()
			assert.Equal(t, tt.expected, sql)
		})
	}
}

func TestConnectionStateGenerateResetSQL(t *testing.T) {
	tests := []struct {
		name     string
		state    *ConnectionState
		expected []string
	}{
		{
			name:     "nil state",
			state:    nil,
			expected: nil,
		},
		{
			name:     "clean state",
			state:    NewConnectionState(map[string]string{}),
			expected: nil,
		},
		{
			name: "state with settings",
			state: NewConnectionState(map[string]string{
				"timezone":    "UTC",
				"search_path": "public",
			}),
			expected: []string{
				"RESET ALL",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := tt.state.GenerateResetSQL()
			assert.Equal(t, tt.expected, sql)
		})
	}
}

func TestConnectionStateOrderIndependence(t *testing.T) {
	// Settings in different orders should produce same hash
	state1 := NewConnectionState(map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	})

	state2 := NewConnectionState(map[string]string{
		"c": "3",
		"a": "1",
		"b": "2",
	})

	state3 := NewConnectionState(map[string]string{
		"b": "2",
		"c": "3",
		"a": "1",
	})

	// All should have same hash
	assert.Equal(t, state1.Hash(), state2.Hash(), "hash should be order-independent")
	assert.Equal(t, state1.Hash(), state3.Hash(), "hash should be order-independent")

	// And they should be equal
	assert.True(t, state1.Equals(state2), "states with same settings should be equal")
	assert.True(t, state1.Equals(state3), "states with same settings should be equal")
}

func TestConnectionStatePreparedStatements(t *testing.T) {
	state := NewEmptyConnectionState()

	// Test storing prepared statement
	stmt := NewPreparedStatement("test_stmt", nil, []uint32{23, 23})
	state.StorePreparedStatement(stmt)

	// Test retrieving prepared statement
	retrieved := state.GetPreparedStatement("test_stmt")
	require.NotNil(t, retrieved, "should retrieve stored statement")
	assert.Equal(t, "test_stmt", retrieved.Name)
	assert.Equal(t, []uint32{23, 23}, retrieved.ParamTypes)

	// Test retrieving non-existent statement
	notFound := state.GetPreparedStatement("non_existent")
	assert.Nil(t, notFound, "should return nil for non-existent statement")

	// Test deleting prepared statement
	state.DeletePreparedStatement("test_stmt")
	deleted := state.GetPreparedStatement("test_stmt")
	assert.Nil(t, deleted, "should return nil after deletion")

	// Test deleting non-existent statement (should not panic)
	state.DeletePreparedStatement("non_existent")
}

func TestConnectionStatePortals(t *testing.T) {
	state := NewEmptyConnectionState()

	// Create a prepared statement first
	stmt := NewPreparedStatement("test_stmt", nil, []uint32{23})

	// Test storing portal
	portal := NewPortal("test_portal", stmt, [][]byte{[]byte("value")}, []int16{0}, []int16{0})
	state.StorePortal(portal)

	// Test retrieving portal
	retrieved := state.GetPortal("test_portal")
	require.NotNil(t, retrieved, "should retrieve stored portal")
	assert.Equal(t, "test_portal", retrieved.Name)
	assert.Equal(t, stmt, retrieved.Statement)

	// Test retrieving non-existent portal
	notFound := state.GetPortal("non_existent")
	assert.Nil(t, notFound, "should return nil for non-existent portal")

	// Test deleting portal
	state.DeletePortal("test_portal")
	deleted := state.GetPortal("test_portal")
	assert.Nil(t, deleted, "should return nil after deletion")

	// Test deleting non-existent portal (should not panic)
	state.DeletePortal("non_existent")
}

func TestConnectionStateIsCleanWithPreparedStatements(t *testing.T) {
	state := NewEmptyConnectionState()
	assert.True(t, state.IsClean(), "new state should be clean")

	// Add prepared statement
	stmt := NewPreparedStatement("test", nil, nil)
	state.StorePreparedStatement(stmt)
	assert.False(t, state.IsClean(), "state with prepared statement should not be clean")
}

func TestConnectionStateIsCleanWithPortals(t *testing.T) {
	state := NewEmptyConnectionState()
	assert.True(t, state.IsClean(), "new state should be clean")

	// Add portal
	stmt := NewPreparedStatement("test", nil, nil)
	portal := NewPortal("test_portal", stmt, nil, nil, nil)
	state.StorePortal(portal)
	assert.False(t, state.IsClean(), "state with portal should not be clean")
}

func TestConnectionStateResetSQLWithPreparedStatements(t *testing.T) {
	state := NewEmptyConnectionState()

	// Add settings and prepared statements
	state.Settings["timezone"] = "UTC"
	stmt := NewPreparedStatement("test", nil, nil)
	state.StorePreparedStatement(stmt)
	portal := NewPortal("test_portal", stmt, nil, nil, nil)
	state.StorePortal(portal)

	sql := state.GenerateResetSQL()
	require.NotNil(t, sql)
	assert.Contains(t, sql, "RESET ALL", "should include RESET ALL for settings")
	assert.Contains(t, sql, "DEALLOCATE ALL", "should include DEALLOCATE ALL for prepared statements")
	assert.Contains(t, sql, "CLOSE ALL", "should include CLOSE ALL for portals")
}

func TestConnectionStateClose(t *testing.T) {
	state := NewEmptyConnectionState()
	state.Settings["timezone"] = "UTC"
	stmt := NewPreparedStatement("test", nil, nil)
	state.StorePreparedStatement(stmt)

	state.Close()

	// After close, all maps should be nil
	assert.Nil(t, state.Settings)
	assert.Nil(t, state.PreparedStatements)
	assert.Nil(t, state.Portals)
}

func TestConnectionStateCloneWithPreparedStatements(t *testing.T) {
	state := NewEmptyConnectionState()
	state.Settings["timezone"] = "UTC"
	stmt := NewPreparedStatement("test", nil, []uint32{23})
	state.StorePreparedStatement(stmt)

	clone := state.Clone()
	require.NotNil(t, clone)

	// Clone should have same prepared statements
	clonedStmt := clone.GetPreparedStatement("test")
	require.NotNil(t, clonedStmt)
	assert.Equal(t, "test", clonedStmt.Name)

	// Modifying clone should not affect original
	clone.DeletePreparedStatement("test")
	assert.Nil(t, clone.GetPreparedStatement("test"), "clone should not have statement after deletion")
	assert.NotNil(t, state.GetPreparedStatement("test"), "original should still have statement")
}
