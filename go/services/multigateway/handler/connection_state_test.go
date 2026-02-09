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

package handler

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

func TestNewMultiGatewayConnectionState(t *testing.T) {
	state := NewMultiGatewayConnectionState()

	require.NotNil(t, state)
	require.NotNil(t, state.Portals)
	require.Empty(t, state.Portals)
}

func TestMultiGatewayConnectionState_GetPortalInfoNonExistent(t *testing.T) {
	state := NewMultiGatewayConnectionState()

	portalInfo := state.GetPortalInfo("nonexistent")
	require.Nil(t, portalInfo)
}

func TestMultiGatewayConnectionState_StoreAndGetPortalInfo(t *testing.T) {
	state := NewMultiGatewayConnectionState()

	// Create a portal
	ps := protoutil.NewPreparedStatement("stmt1", "SELECT 1", nil)
	psi, err := preparedstatement.NewPreparedStatementInfo(ps)
	require.NoError(t, err)
	portal := protoutil.NewPortal("portal1", "stmt1", nil, nil, nil)

	// Store it
	state.StorePortalInfo(portal, psi)

	// Verify it exists
	retrieved := state.GetPortalInfo("portal1")
	require.NotNil(t, retrieved)
	require.Equal(t, "portal1", retrieved.Name)
	require.Equal(t, "SELECT 1", retrieved.Query)
}

func TestMultiGatewayConnectionState_DeletePortalInfo(t *testing.T) {
	state := NewMultiGatewayConnectionState()

	// Store a portal
	ps := protoutil.NewPreparedStatement("stmt1", "SELECT 1", nil)
	psi, err := preparedstatement.NewPreparedStatementInfo(ps)
	require.NoError(t, err)
	portal := protoutil.NewPortal("portal1", "stmt1", nil, nil, nil)

	state.StorePortalInfo(portal, psi)

	// Verify it exists
	retrieved := state.GetPortalInfo("portal1")
	require.NotNil(t, retrieved)

	// Delete it
	state.DeletePortalInfo("portal1")

	// Verify it's gone
	retrieved = state.GetPortalInfo("portal1")
	require.Nil(t, retrieved)
}

func TestMultiGatewayConnectionState_DeleteNonExistentPortal(t *testing.T) {
	state := NewMultiGatewayConnectionState()

	// Deleting a non-existent portal should not panic
	state.DeletePortalInfo("nonexistent")

	// Should be safe to call multiple times
	state.DeletePortalInfo("nonexistent")
}

func TestMultiGatewayConnectionState_ConcurrentAccess(t *testing.T) {
	state := NewMultiGatewayConnectionState()
	var wg sync.WaitGroup
	numGoroutines := 10

	// Create prepared statement info for testing
	ps := protoutil.NewPreparedStatement("stmt1", "SELECT 1", nil)
	psi, err := preparedstatement.NewPreparedStatementInfo(ps)
	require.NoError(t, err)

	// Concurrently access the connection state
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Create portal with unique name
			portalName := "portal" + string(rune(id))
			portal := protoutil.NewPortal(portalName, "stmt1", nil, nil, nil)

			// Store portal info
			state.StorePortalInfo(portal, psi)

			// Get it to verify it was stored
			retrieved := state.GetPortalInfo(portalName)
			require.NotNil(t, retrieved)

			// Delete it
			state.DeletePortalInfo(portalName)
		}(i)
	}

	wg.Wait()

	// After all operations, portals should be empty
	require.Empty(t, state.Portals)
}

func TestMultiGatewayConnectionState_MultiplePortals(t *testing.T) {
	state := NewMultiGatewayConnectionState()

	// Create multiple portals
	ps1 := protoutil.NewPreparedStatement("stmt1", "SELECT 1", nil)
	psi1, err := preparedstatement.NewPreparedStatementInfo(ps1)
	require.NoError(t, err)
	portal1 := protoutil.NewPortal("portal1", "stmt1", nil, nil, nil)

	ps2 := protoutil.NewPreparedStatement("stmt2", "SELECT 2", nil)
	psi2, err := preparedstatement.NewPreparedStatementInfo(ps2)
	require.NoError(t, err)
	portal2 := protoutil.NewPortal("portal2", "stmt2", nil, nil, nil)

	// Store them
	state.StorePortalInfo(portal1, psi1)
	state.StorePortalInfo(portal2, psi2)

	// Verify both exist
	require.NotNil(t, state.GetPortalInfo("portal1"))
	require.NotNil(t, state.GetPortalInfo("portal2"))

	// Delete one
	state.DeletePortalInfo("portal1")

	// Verify only one remains
	require.Nil(t, state.GetPortalInfo("portal1"))
	require.NotNil(t, state.GetPortalInfo("portal2"))

	// Delete the other
	state.DeletePortalInfo("portal2")

	// Verify both are gone
	require.Nil(t, state.GetPortalInfo("portal1"))
	require.Nil(t, state.GetPortalInfo("portal2"))
}

// newTestTarget creates a test Target for the given tableGroup.
func newTestTarget(tableGroup string) *query.Target {
	return &query.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
}

func TestTransactionState_InitialState(t *testing.T) {
	state := NewMultiGatewayConnectionState()

	require.Equal(t, TxStateIdle, state.GetTransactionState())
	require.False(t, state.IsInTransaction())
}

func TestTransactionState_Transitions(t *testing.T) {
	tests := []struct {
		name     string
		from     TransactionState
		to       TransactionState
		expected TransactionState
	}{
		{"Idle to InTransaction", TxStateIdle, TxStateInTransaction, TxStateInTransaction},
		{"InTransaction to Idle", TxStateInTransaction, TxStateIdle, TxStateIdle},
		{"InTransaction to Aborted", TxStateInTransaction, TxStateAborted, TxStateAborted},
		{"Aborted to Idle", TxStateAborted, TxStateIdle, TxStateIdle},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := NewMultiGatewayConnectionState()
			state.SetTransactionState(tt.from)
			state.SetTransactionState(tt.to)
			require.Equal(t, tt.expected, state.GetTransactionState())
		})
	}
}

func TestTransactionState_IsInTransaction(t *testing.T) {
	tests := []struct {
		name     string
		txState  TransactionState
		expected bool
	}{
		{"Idle", TxStateIdle, false},
		{"InTransaction", TxStateInTransaction, true},
		{"Aborted", TxStateAborted, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := NewMultiGatewayConnectionState()
			state.SetTransactionState(tt.txState)
			require.Equal(t, tt.expected, state.IsInTransaction())
		})
	}
}

func TestTransactionState_ShardStateOperations(t *testing.T) {
	state := NewMultiGatewayConnectionState()
	target := newTestTarget("tg1")

	// Initially no shard state
	ss := state.GetMatchingShardState(target)
	require.Nil(t, ss)

	// Store a reserved connection
	rs := queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
	}
	state.StoreReservedConnection(target, rs)

	// Verify it's retrievable
	ss = state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, int64(42), ss.ReservedConnectionId)
	require.Equal(t, "cell1", ss.PoolerID.Cell)

	// Update the same target's reserved connection
	rs2 := queryservice.ReservedState{
		ReservedConnectionId: 99,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell2", Name: "pooler2"},
	}
	state.StoreReservedConnection(target, rs2)

	ss = state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, int64(99), ss.ReservedConnectionId)
	require.Equal(t, "cell2", ss.PoolerID.Cell)

	// Different target should not match
	otherTarget := newTestTarget("tg2")
	require.Nil(t, state.GetMatchingShardState(otherTarget))

	// Clear the reserved connection
	state.ClearReservedConnection(target)
	require.Nil(t, state.GetMatchingShardState(target))
	require.Empty(t, state.ShardStates)
}

func TestTransactionState_ConcurrentTransactionStateAccess(t *testing.T) {
	state := NewMultiGatewayConnectionState()
	var wg sync.WaitGroup
	numGoroutines := 20

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if id%2 == 0 {
				state.SetTransactionState(TxStateInTransaction)
				_ = state.IsInTransaction()
				state.SetTransactionState(TxStateIdle)
			} else {
				_ = state.GetTransactionState()
				_ = state.IsInTransaction()
			}
		}(i)
	}

	wg.Wait()
	// No race panic means success; final state is indeterminate but valid
}

func TestMultiGatewayConnectionState_PortalInfoIntegrity(t *testing.T) {
	state := NewMultiGatewayConnectionState()

	// Create portal with specific data
	paramTypes := []uint32{23, 25} // int4, text
	ps := protoutil.NewPreparedStatement("stmt1", "SELECT $1, $2", paramTypes)
	psi, err := preparedstatement.NewPreparedStatementInfo(ps)
	require.NoError(t, err)

	params := [][]byte{[]byte("123"), []byte("hello")}
	paramFormats := []int16{0, 0}
	resultFormats := []int16{0}
	portal := protoutil.NewPortal("portal1", "stmt1", params, paramFormats, resultFormats)

	// Store it
	state.StorePortalInfo(portal, psi)

	// Retrieve and verify data integrity
	retrieved := state.GetPortalInfo("portal1")
	require.NotNil(t, retrieved)
	require.Equal(t, "portal1", retrieved.Name)
	require.Equal(t, "stmt1", retrieved.PreparedStatementName)
	require.Equal(t, "SELECT $1, $2", retrieved.Query)
	require.Equal(t, paramTypes, retrieved.ParamTypes)
	// Reconstruct params from the proto encoding
	retrievedParams := sqltypes.ParamsFromProto(retrieved.ParamLengths, retrieved.ParamValues)
	require.Equal(t, params, retrievedParams)
}
