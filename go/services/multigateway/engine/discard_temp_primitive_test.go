// Copyright 2026 Supabase, Inc.
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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// TestDiscardTemp_KeepsDeferredBegin: the transaction a DISCARD TEMP runs in has not
// started on any backend yet, so its BEGIN stays pending for the first statement that
// does, and the transaction starts with the options the client gave it.
func TestDiscardTemp_KeepsDeferredBegin(t *testing.T) {
	state := handler.NewMultigatewayConnectionState()
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.PendingBeginQuery = "BEGIN ISOLATION LEVEL SERIALIZABLE"
	state.SetReservedConnection(protoutil.NewTarget("", "tg1", "", query.Mode_MODE_WRITABLE), &query.ReservedState{
		ReservedConnectionId: 100,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTempTable,
	})

	prim := NewDiscardTempPrimitive("DISCARD TEMP", "tg1")
	err := prim.StreamExecute(context.Background(), &txMockIExecute{}, conn, state, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.Equal(t, "BEGIN ISOLATION LEVEL SERIALIZABLE", state.PendingBeginQuery)
}
