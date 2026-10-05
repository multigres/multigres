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
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/readonly"
)

func TestScatterConn_ReadOnlyOverlaysDefaultTransactionReadOnly(t *testing.T) {
	// Unpinned autocommit statements: the overlay follows the live mode.
	gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
	sc := NewScatterConn(gw, slog.Default())
	modes := readonly.New()
	sc.SetReadOnlyModes(modes)
	state := handler.NewMultigatewayConnectionState()
	state.SessionSettings = map[string]string{"search_path": "public"}
	conn := newTestConn()

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

// reserveUnderMode drives one statement through sc that creates a reservation
// while conn's database is in the given mode, mirroring a deferred BEGIN whose
// first statement checks out the backend.
func reserveUnderMode(t *testing.T, sc *ScatterConn, gw *mockGateway, modes *readonly.Modes, enabled bool) (*server.Conn, *handler.MultigatewayConnectionState) {
	t.Helper()
	modes.Set("", readonly.Mode{Enabled: enabled})
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.BeginTransaction()
	gw.streamExecuteReturnState = &querypb.ReservedState{
		ReservedConnectionId: 77,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	}
	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	require.True(t, state.HasAnyReservedConnection())
	require.Equal(t, enabled, state.ReadOnlyOverlay, "the record captures what the backend was checked out with")
	return conn, state
}

// The pooler applies settings only at checkout and relabels the backend on
// release with whatever map the gateway sends, so every label for a reserved
// backend must say what that backend really has, not what the live mode says.
func TestScatterConn_ReadOnlyOverlayFrozenWhileReserved(t *testing.T) {
	const key = "default_transaction_read_only"

	t.Run("enabled at checkout, lifted before rollback", func(t *testing.T) {
		gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
		sc := NewScatterConn(gw, slog.Default())
		modes := readonly.New()
		sc.SetReadOnlyModes(modes)
		conn, state := reserveUnderMode(t, sc, gw, modes, true)

		modes.Set("", readonly.Mode{})
		require.NoError(t, sc.ConcludeTransaction(context.Background(), conn, state,
			multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK, nil, false, false,
			func(_ context.Context, _ *sqltypes.Result) error { return nil }))
		require.Equal(t, "on", gw.concludeRollbackSessionSettings[key],
			"the backend still has the GUC: labelling it clean would hand a read-only backend to a read-write borrower")

		// Reservation gone: the next statement follows the live mode again.
		require.NotContains(t, sc.sessionSettings(conn, state), key)
	})

	t.Run("disabled at checkout, enabled before release", func(t *testing.T) {
		gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
		sc := NewScatterConn(gw, slog.Default())
		modes := readonly.New()
		sc.SetReadOnlyModes(modes)
		conn, state := reserveUnderMode(t, sc, gw, modes, false)

		modes.Set("", readonly.Mode{Enabled: true})
		require.NotContains(t, sc.sessionSettings(conn, state), key, "statements on the reservation keep the checkout-time map")
		require.NoError(t, sc.ReleaseAllReservedConnections(context.Background(), conn, state, false))
		require.NotContains(t, gw.releaseReservedConnectionSettings, key,
			"the backend never got the GUC: claiming it would make the pool skip the SET for the next read-only borrower")
	})
}
