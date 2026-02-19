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

package preparedstatement

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	querypb "github.com/multigres/multigres/go/pb/query"
)

func TestConsolidator_AddAndGetPreparedStatement(t *testing.T) {
	consolidator := NewConsolidator()
	connID := uint32(1)

	psi, err := consolidator.AddPreparedStatement(connID, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)
	require.NotNil(t, psi)
	require.Equal(t, "SELECT 1", psi.Query)
	require.Equal(t, "stmt0", psi.Name) // Internal name should be generated

	psi2 := consolidator.GetPreparedStatementInfo(connID, "stmt1")
	require.NotNil(t, psi2)
	require.Equal(t, psi, psi2)
}

func TestConsolidator_ConsolidatesDuplicateQueries(t *testing.T) {
	consolidator := NewConsolidator()
	connID1 := uint32(1)
	connID2 := uint32(2)

	// Add the same query with different names on different connections
	psi1, err := consolidator.AddPreparedStatement(connID1, "stmt_a", "SELECT * FROM users", nil)
	require.NoError(t, err)

	psi2, err := consolidator.AddPreparedStatement(connID2, "stmt_b", "SELECT * FROM users", nil)
	require.NoError(t, err)

	// Both should point to the same underlying prepared statement
	require.NotNil(t, psi1)
	require.NotNil(t, psi2)
	require.Equal(t, psi1.Name, psi2.Name) // Should have the same internal name
	require.Equal(t, "SELECT * FROM users", psi1.Query)
	require.Equal(t, "SELECT * FROM users", psi2.Query)

	// Usage count should be 2
	require.Equal(t, 2, consolidator.usageCount[psi1])
}

func TestConsolidator_DuplicateNameOnSameConnectionReturnsError(t *testing.T) {
	consolidator := NewConsolidator()
	connID := uint32(1)

	_, err := consolidator.AddPreparedStatement(connID, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)

	// Try to add another statement with the same name on the same connection
	_, err = consolidator.AddPreparedStatement(connID, "stmt1", "SELECT 2", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Prepared statement with this name exists")
}

func TestConsolidator_EmptyNameAllowsDuplicates(t *testing.T) {
	consolidator := NewConsolidator()
	connID := uint32(1)

	// Empty names should be allowed multiple times
	_, err := consolidator.AddPreparedStatement(connID, "", "SELECT 1", nil)
	require.NoError(t, err)

	_, err = consolidator.AddPreparedStatement(connID, "", "SELECT 2", nil)
	require.NoError(t, err)
}

func TestConsolidator_RemovePreparedStatement(t *testing.T) {
	consolidator := NewConsolidator()
	connID := uint32(1)

	psi, err := consolidator.AddPreparedStatement(connID, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)
	require.NotNil(t, psi)

	consolidator.RemovePreparedStatement(connID, "stmt1")

	psi = consolidator.GetPreparedStatementInfo(connID, "stmt1")
	require.Nil(t, psi)
}

func TestConsolidator_RemoveDecrementsUsageCount(t *testing.T) {
	consolidator := NewConsolidator()
	connID1 := uint32(1)
	connID2 := uint32(2)
	query := "SELECT * FROM users"

	// Add same query on two connections
	psi, err := consolidator.AddPreparedStatement(connID1, "stmt1", query, nil)
	require.NoError(t, err)

	_, err = consolidator.AddPreparedStatement(connID2, "stmt2", query, nil)
	require.NoError(t, err)

	require.Equal(t, 2, consolidator.usageCount[psi])

	// Remove from first connection
	consolidator.RemovePreparedStatement(connID1, "stmt1")
	require.Equal(t, 1, consolidator.usageCount[psi])

	// The query should still exist in stmts
	_, exists := consolidator.stmts[query]
	require.True(t, exists)

	// Remove from second connection
	consolidator.RemovePreparedStatement(connID2, "stmt2")

	// Now the query should be removed from stmts since usage count is 0
	_, exists = consolidator.stmts[query]
	require.False(t, exists)
}

func TestConsolidator_RemoveNonExistentStatement(t *testing.T) {
	consolidator := NewConsolidator()
	connID := uint32(1)

	// Removing a non-existent statement should not panic or error
	consolidator.RemovePreparedStatement(connID, "nonexistent")

	// Should be safe to call multiple times
	consolidator.RemovePreparedStatement(connID, "nonexistent")
}

func TestConsolidator_InvalidSQL(t *testing.T) {
	consolidator := NewConsolidator()
	connID := uint32(1)

	// Test with invalid SQL that the parser cannot handle
	_, err := consolidator.AddPreparedStatement(connID, "stmt1", "THIS IS NOT VALID SQL @@##", nil)
	require.Error(t, err)
}

func TestConsolidator_MultipleStatementsInQuery(t *testing.T) {
	consolidator := NewConsolidator()
	connID := uint32(1)

	// Prepared statements should only contain a single query
	_, err := consolidator.AddPreparedStatement(connID, "stmt1", "SELECT 1; SELECT 2", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "more than 1 query")
}

func TestConsolidator_ConcurrentAccess(t *testing.T) {
	consolidator := NewConsolidator()
	var wg sync.WaitGroup
	numGoroutines := 10

	// Concurrently add prepared statements
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			connID := uint32(id)
			psi, err := consolidator.AddPreparedStatement(connID, "stmt1", "SELECT 1", nil)
			require.NoError(t, err)
			require.NotNil(t, psi)

			consolidator.RemovePreparedStatement(connID, "stmt1")
		}(i)
	}

	wg.Wait()

	// After all removals, the consolidator should be empty
	require.Empty(t, consolidator.stmts)
	require.Empty(t, consolidator.usageCount)
}

func TestConsolidator_GetNonExistentStatement(t *testing.T) {
	consolidator := NewConsolidator()
	connID := uint32(1)

	psi := consolidator.GetPreparedStatementInfo(connID, "nonexistent")
	require.Nil(t, psi)
}

func TestConsolidator_DifferentConnectionsIndependentNamespaces(t *testing.T) {
	consolidator := NewConsolidator()
	connID1 := uint32(1)
	connID2 := uint32(2)

	// Same name on different connections should work
	psi1, err := consolidator.AddPreparedStatement(connID1, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)

	psi2, err := consolidator.AddPreparedStatement(connID2, "stmt1", "SELECT 2", nil)
	require.NoError(t, err)

	require.NotNil(t, psi1)
	require.NotNil(t, psi2)
	require.Equal(t, "SELECT 1", psi1.Query)
	require.Equal(t, "SELECT 2", psi2.Query)
}

func TestNewPortalInfo(t *testing.T) {
	consolidator := NewConsolidator()
	connID := uint32(1)

	psi, err := consolidator.AddPreparedStatement(connID, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)
	require.NotNil(t, psi)

	portal := &querypb.Portal{
		Name:                  "portal1",
		PreparedStatementName: "stmt1",
	}

	portalInfo := NewPortalInfo(psi, portal)
	require.NotNil(t, portalInfo)
	require.Equal(t, "portal1", portalInfo.Name)
	require.Equal(t, psi, portalInfo.PreparedStatementInfo)
}

func TestConsolidator_RemoveConnection(t *testing.T) {
	consolidator := NewConsolidator()
	connID := uint32(1)
	otherConnID := uint32(2)

	// Add statements on two connections (some sharing the same query)
	_, err := consolidator.AddPreparedStatement(connID, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)
	_, err = consolidator.AddPreparedStatement(connID, "stmt2", "SELECT 2", nil)
	require.NoError(t, err)
	_, err = consolidator.AddPreparedStatement(otherConnID, "stmt3", "SELECT 1", nil) // same query as stmt1
	require.NoError(t, err)

	// Verify initial state
	require.NotNil(t, consolidator.GetPreparedStatementInfo(connID, "stmt1"))
	require.NotNil(t, consolidator.GetPreparedStatementInfo(connID, "stmt2"))

	// Remove all statements for connID
	consolidator.RemoveConnection(connID)

	// connID statements should be gone
	require.Nil(t, consolidator.GetPreparedStatementInfo(connID, "stmt1"))
	require.Nil(t, consolidator.GetPreparedStatementInfo(connID, "stmt2"))

	// otherConnID should still work (shared query "SELECT 1" still alive)
	require.NotNil(t, consolidator.GetPreparedStatementInfo(otherConnID, "stmt3"))

	// "SELECT 2" should be fully removed (only connID used it)
	_, exists := consolidator.stmts["SELECT 2"]
	require.False(t, exists)

	// "SELECT 1" should still exist (otherConnID still references it)
	_, exists = consolidator.stmts["SELECT 1"]
	require.True(t, exists)
}

func TestConsolidator_RemoveConnection_NonExistent(t *testing.T) {
	consolidator := NewConsolidator()

	// Removing a non-existent connection should not panic
	consolidator.RemoveConnection(999)
}

func TestConsolidator_Stats(t *testing.T) {
	consolidator := NewConsolidator()

	// Test empty consolidator
	stats := consolidator.Stats()
	require.Equal(t, 0, stats.UniqueStatements)
	require.Equal(t, 0, stats.TotalReferences)
	require.Equal(t, 0, stats.ConnectionCount)
	require.Empty(t, stats.Statements)

	// Add statements from multiple connections
	connID1 := uint32(1)
	connID2 := uint32(2)
	connID3 := uint32(3)

	// Add first statement on connection 1
	_, err := consolidator.AddPreparedStatement(connID1, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)

	// Add same query on connection 2 (should be consolidated)
	_, err = consolidator.AddPreparedStatement(connID2, "stmt2", "SELECT 1", nil)
	require.NoError(t, err)

	// Add different query on connection 3
	_, err = consolidator.AddPreparedStatement(connID3, "stmt3", "SELECT 2", nil)
	require.NoError(t, err)

	stats = consolidator.Stats()
	require.Equal(t, 2, stats.UniqueStatements)
	require.Equal(t, 3, stats.TotalReferences)
	require.Equal(t, 3, stats.ConnectionCount)
	require.Len(t, stats.Statements, 2)

	// Verify statement details
	stmtMap := make(map[string]StatementStats)
	for _, s := range stats.Statements {
		stmtMap[s.Query] = s
	}

	require.Contains(t, stmtMap, "SELECT 1")
	require.Equal(t, 2, stmtMap["SELECT 1"].UsageCount)

	require.Contains(t, stmtMap, "SELECT 2")
	require.Equal(t, 1, stmtMap["SELECT 2"].UsageCount)
}
