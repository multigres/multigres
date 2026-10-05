// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scatterconn

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/readonly"
)

func TestScatterConn_ReadOnlyOverlaysDefaultTransactionReadOnly(t *testing.T) {
	gw := &mockGateway{
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 77,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		},
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	modes := readonly.New()
	sc.SetReadOnlyModes(modes)
	state := handler.NewMultigatewayConnectionState()
	state.SessionSettings = map[string]string{"search_path": "public"}
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	exec := func() map[string]string {
		err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
			func(_ context.Context, _ *sqltypes.Result) error { return nil })
		require.NoError(t, err)
		return gw.streamExecuteOpts.GetSessionSettings()
	}

	require.NotContains(t, exec(), "default_transaction_read_only", "read-write: settings pass through untouched")

	modes.Set(conn.Database(), readonly.Mode{Enabled: true})
	settings := exec()
	require.Equal(t, "on", settings["default_transaction_read_only"])
	require.Equal(t, "public", settings["search_path"], "overlay keeps the tracked settings")
	require.NotContains(t, state.GetSessionSettings(), "default_transaction_read_only",
		"overlay must not leak into the tracked session state")

	modes.Set(conn.Database(), readonly.Mode{})
	require.NotContains(t, exec(), "default_transaction_read_only", "lifting the mode is immediate")
}
