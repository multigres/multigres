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
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
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

func TestIsInTransaction(t *testing.T) {
	tests := []struct {
		name      string
		txnStatus protocol.TransactionStatus
		expected  bool
	}{
		{"Idle", protocol.TxnStatusIdle, false},
		{"InTransaction", protocol.TxnStatusInBlock, true},
		{"Failed", protocol.TxnStatusFailed, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := server.NewTestConn(&bytes.Buffer{})
			tc.Conn.SetTxnStatus(tt.txnStatus)
			require.Equal(t, tt.expected, tc.Conn.IsInTransaction())
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
	rs := &query.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	}
	state.SetReservedConnection(target, rs)

	// Verify it's retrievable
	ss = state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(42), ss.ReservedState.GetReservedConnectionId())
	require.Equal(t, "cell1", ss.ReservedState.GetPoolerId().GetCell())
	require.Equal(t, protoutil.ReasonTransaction, ss.ReservedState.GetReservationReasons())

	// Update the same target's reserved connection (reasons should be replaced, not OR'd)
	rs2 := &query.ReservedState{
		ReservedConnectionId: 99,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell2", Name: "pooler2"},
		ReservationReasons:   protoutil.ReasonTempTable,
	}
	state.SetReservedConnection(target, rs2)

	ss = state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(99), ss.ReservedState.GetReservedConnectionId())
	require.Equal(t, "cell2", ss.ReservedState.GetPoolerId().GetCell())
	// Reasons should be replaced (set), not OR'd
	require.Equal(t, protoutil.ReasonTempTable, ss.ReservedState.GetReservationReasons())

	// Different target should not match
	otherTarget := newTestTarget("tg2")
	require.Nil(t, state.GetMatchingShardState(otherTarget))

	// Clear the reserved connection
	state.ClearReservedConnection(target)
	require.Nil(t, state.GetMatchingShardState(target))
	require.Empty(t, state.ShardStates)
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

func TestCommitPendingListens(t *testing.T) {
	tests := []struct {
		name           string
		activeChannels []string // channels active before the transaction
		actions        func(state *MultiGatewayConnectionState)
		wantSubs       []string
		wantUnsubs     []string
		wantAll        bool
	}{
		{
			name: "listen_then_unlisten_same_channel",
			actions: func(s *MultiGatewayConnectionState) {
				s.AddPendingListen("x")
				s.AddPendingUnlisten("x")
			},
		},
		{
			name:           "unlisten_then_listen_same_channel",
			activeChannels: []string{"x"},
			actions: func(s *MultiGatewayConnectionState) {
				s.AddPendingUnlisten("x")
				s.AddPendingListen("x")
			},
		},
		{
			name: "listen_new_channel",
			actions: func(s *MultiGatewayConnectionState) {
				s.AddPendingListen("x")
			},
			wantSubs: []string{"x"},
		},
		{
			name:           "unlisten_active_channel",
			activeChannels: []string{"x"},
			actions: func(s *MultiGatewayConnectionState) {
				s.AddPendingUnlisten("x")
			},
			wantUnsubs: []string{"x"},
		},
		{
			name:           "unlisten_all_then_listen",
			activeChannels: []string{"x"},
			actions: func(s *MultiGatewayConnectionState) {
				s.AddPendingUnlistenAll()
				s.AddPendingListen("y")
			},
			wantSubs: []string{"y"},
			wantAll:  true,
		},
		{
			name:           "listen_then_unlisten_all",
			activeChannels: []string{"x"},
			actions: func(s *MultiGatewayConnectionState) {
				s.AddPendingListen("y")
				s.AddPendingUnlistenAll()
			},
			wantAll: true,
		},
		{
			name: "duplicate_listen",
			actions: func(s *MultiGatewayConnectionState) {
				s.AddPendingListen("x")
				s.AddPendingListen("x")
			},
			wantSubs: []string{"x"},
		},
		{
			name:           "mixed_unlisten_all",
			activeChannels: []string{"z"},
			actions: func(s *MultiGatewayConnectionState) {
				s.AddPendingListen("x")
				s.AddPendingUnlistenAll()
				s.AddPendingListen("y")
			},
			wantSubs: []string{"y"},
			wantAll:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := NewMultiGatewayConnectionState()
			for _, ch := range tt.activeChannels {
				state.AddListenChannel(ch)
			}

			tt.actions(state)
			require.True(t, state.HasPendingListens())

			subs, unsubs, all := state.CommitPendingListens()

			require.ElementsMatch(t, tt.wantSubs, subs, "subscribes mismatch")
			require.ElementsMatch(t, tt.wantUnsubs, unsubs, "unsubscribes mismatch")
			require.Equal(t, tt.wantAll, all, "unsubscribeAll mismatch")
			require.False(t, state.HasPendingListens(), "pending actions should be cleared")
		})
	}
}

func TestDiscardPendingListens(t *testing.T) {
	state := NewMultiGatewayConnectionState()
	state.AddListenChannel("x")
	state.AddPendingListen("y")
	state.AddPendingUnlisten("x")

	require.True(t, state.HasPendingListens())
	state.DiscardPendingListens()
	require.False(t, state.HasPendingListens())

	// ListenChannels should be unchanged after discard.
	require.True(t, state.IsListening("x"))
	require.False(t, state.IsListening("y"))
}
