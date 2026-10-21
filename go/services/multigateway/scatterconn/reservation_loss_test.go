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

package scatterconn

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

var lossTarget = protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)

// newLossSession returns a session that tracks a reservation on lossTarget
// with the given reasons, in a transaction block when the reasons say so.
func newLossSession(reasons uint32) (*server.Conn, *handler.MultigatewayConnectionState) {
	conn := newTestConn()
	if protoutil.HasTransactionReason(reasons) {
		conn.SetTxnStatus(protocol.TxnStatusInBlock)
	} else {
		conn.SetTxnStatus(protocol.TxnStatusIdle)
	}
	state := handler.NewMultigatewayConnectionState()
	state.SetReservedConnection(lossTarget, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   reasons,
	})
	return conn, state
}

// requireSessionEnded asserts err is the FATAL that ends the session, naming
// lost, and returns its detail.
func requireSessionEnded(t *testing.T, err error, lost string) string {
	t.Helper()
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	require.True(t, diag.IsFatal(), "got %v", err)
	require.Equal(t, mterrors.PgSSAdminShutdown, diag.Code)
	require.Contains(t, diag.Detail, lost)
	return diag.Detail
}

func requireNotFatal(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.False(t, mterrors.IsFatal(err), "got %v", err)
}

func TestLostSessionState(t *testing.T) {
	noHold := handler.NewMultigatewayConnectionState()
	withHold := handler.NewMultigatewayConnectionState()
	withHold.AddOpenHoldCursor("c")

	tests := []struct {
		name    string
		reasons uint32
		state   *handler.MultigatewayConnectionState
		want    uint32
	}{
		{"transaction only", protoutil.ReasonTransaction, noHold, 0},
		{"copy in a transaction", protoutil.ReasonTransaction | protoutil.ReasonCopy, noHold, 0},
		{"temp table", protoutil.ReasonTempTable, noHold, protoutil.ReasonTempTable},
		{"advisory lock in a transaction", protoutil.ReasonTransaction | protoutil.ReasonSessionAdvisoryLock, noHold, protoutil.ReasonSessionAdvisoryLock},
		{"set seed", protoutil.ReasonSetSeed, noHold, protoutil.ReasonSetSeed},
		{"unsafe connection", protoutil.ReasonUnsafeConnection, noHold, protoutil.ReasonUnsafeConnection},
		{"logical replication", protoutil.ReasonLogicalReplication, noHold, protoutil.ReasonLogicalReplication},
		{"portal outside a transaction", protoutil.ReasonPortal, noHold, protoutil.ReasonPortal},
		{"portal owned by a transaction", protoutil.ReasonTransaction | protoutil.ReasonPortal, noHold, 0},
		{"portal in a transaction with hold cursors", protoutil.ReasonTransaction | protoutil.ReasonPortal, withHold, protoutil.ReasonPortal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, lostSessionState(tt.reasons, tt.state))
		})
	}
}

// A statement on a reservation that comes back with an error and no
// reservation lost it. Session state ends the session; a transaction alone
// keeps the pooler's error and fails the transaction.
func TestScatterConn_StreamExecute_ReservationLost(t *testing.T) {
	tests := []struct {
		name    string
		reasons uint32
		lost    string // empty: the session survives
	}{
		{"transaction only", protoutil.ReasonTransaction, ""},
		{"transaction with its own portal", protoutil.ReasonTransaction | protoutil.ReasonPortal, ""},
		{"temp table", protoutil.ReasonTempTable, "temp_table"},
		{"advisory lock", protoutil.ReasonSessionAdvisoryLock, "session_advisory_lock"},
		{"set seed", protoutil.ReasonSetSeed, "set_seed"},
		{"temp table in a transaction", protoutil.ReasonTransaction | protoutil.ReasonTempTable, "temp_table"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &mockGateway{streamExecuteErr: mterrors.NewReservedConnectionTerminated(42)}
			sc := NewScatterConn(gw, slog.Default())
			conn, state := newLossSession(tt.reasons)

			err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
				func(context.Context, *sqltypes.Result) error { return nil })

			if tt.lost != "" {
				requireSessionEnded(t, err, tt.lost)
			} else {
				requireNotFatal(t, err)
				require.Equal(t, mterrors.PgSSSerializationFailure, mterrors.ExtractSQLSTATE(err))
			}
			require.Nil(t, state.GetMatchingShardState(lossTarget))
			if protoutil.HasTransactionReason(tt.reasons) {
				require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
			}
		})
	}
}

// A reservation the pooler releases without an error was released on purpose
// (e.g. pg_advisory_unlock dropped the last lock) and loses nothing.
func TestScatterConn_StreamExecute_CleanReleaseKeepsSession(t *testing.T) {
	gw := &mockGateway{} // nil reserved state, no error
	sc := NewScatterConn(gw, slog.Default())
	conn, state := newLossSession(protoutil.ReasonSessionAdvisoryLock)

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT pg_advisory_unlock(1)", nil, state,
		engine.PlanExecInfo{RecheckAdvisoryLocks: true}, false,
		func(context.Context, *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.Nil(t, state.GetMatchingShardState(lossTarget))
}

// A failed statement the pooler still vouches for leaves the session alone.
func TestScatterConn_StreamExecute_SurvivingReservationKeepsSession(t *testing.T) {
	gw := &mockGateway{
		streamExecuteErr: mterrors.NewPgError("ERROR", "22012", "division by zero", ""),
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	conn, state := newLossSession(protoutil.ReasonTempTable)

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1/0", nil, state, engine.PlanExecInfo{}, false,
		func(context.Context, *sqltypes.Result) error { return nil })

	requireNotFatal(t, err)
	require.NotNil(t, state.GetMatchingShardState(lossTarget))
}

func TestScatterConn_PortalStreamExecute_ReservationLost(t *testing.T) {
	gw := &mockGateway{portalErr: mterrors.NewReservedConnectionTerminated(42)}
	sc := NewScatterConn(gw, slog.Default())
	conn, state := newLossSession(protoutil.ReasonSessionAdvisoryLock)

	err := sc.PortalStreamExecute(context.Background(), "tg1", "", conn, state, testPortalInfo(), 0, false,
		engine.PlanExecInfo{}, false, func(context.Context, *sqltypes.Result) error { return nil })

	requireSessionEnded(t, err, "session_advisory_lock")
	require.Nil(t, state.GetMatchingShardState(lossTarget))
}

// Even a plain ROLLBACK, which otherwise recovers from a lost connection,
// ends the session when the connection also held session state.
func TestScatterConn_ConcludeTransaction_ReservationLost(t *testing.T) {
	for _, conclusion := range []multipoolerpb.TransactionConclusion{
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK,
	} {
		t.Run(conclusion.String(), func(t *testing.T) {
			gw := &mockGateway{concludeTransactionErr: mterrors.NewReservedConnectionTerminated(42)}
			sc := NewScatterConn(gw, slog.Default())
			conn, state := newLossSession(protoutil.ReasonTransaction | protoutil.ReasonTempTable)

			callbackCalled := false
			err := sc.ConcludeTransaction(context.Background(), conn, state, conclusion, nil, false, false,
				func(context.Context, *sqltypes.Result) error {
					callbackCalled = true
					return nil
				})

			requireSessionEnded(t, err, "temp_table")
			require.False(t, callbackCalled)
			require.Nil(t, state.GetMatchingShardState(lossTarget))
		})
	}
}

func TestScatterConn_ConcludeTransaction_PoolerLookupFailedLosesState(t *testing.T) {
	gw := &mockGateway{queryServiceByIDErr: mterrors.NewReservedConnectionTerminated(42)}
	sc := NewScatterConn(gw, slog.Default())
	conn, state := newLossSession(protoutil.ReasonTransaction | protoutil.ReasonSetSeed)

	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, nil, false, false,
		func(context.Context, *sqltypes.Result) error { return nil })

	requireSessionEnded(t, err, "set_seed")
}

// DISCARD TEMP losing its connection loses the temp tables it was dropping
// anyway; only other session state ends the session.
func TestScatterConn_DiscardTempTables_ReservationLost(t *testing.T) {
	t.Run("temp tables only", func(t *testing.T) {
		gw := &mockGateway{discardTempTablesErr: mterrors.NewReservedConnectionTerminated(42)}
		sc := NewScatterConn(gw, slog.Default())
		conn, state := newLossSession(protoutil.ReasonTempTable)

		err := sc.DiscardTempTables(context.Background(), conn, state,
			func(context.Context, *sqltypes.Result) error { return nil })

		requireNotFatal(t, err)
		require.Nil(t, state.GetMatchingShardState(lossTarget))
	})
	t.Run("temp tables and an advisory lock", func(t *testing.T) {
		gw := &mockGateway{discardTempTablesErr: mterrors.NewReservedConnectionTerminated(42)}
		sc := NewScatterConn(gw, slog.Default())
		conn, state := newLossSession(protoutil.ReasonTempTable | protoutil.ReasonSessionAdvisoryLock)

		err := sc.DiscardTempTables(context.Background(), conn, state,
			func(context.Context, *sqltypes.Result) error { return nil })

		detail := requireSessionEnded(t, err, "session_advisory_lock")
		require.NotContains(t, detail, "temp_table")
	})
}

// DISCARD ALL keeps the sticky reasons; losing them ends the session. A
// disconnecting client releases everything and has nobody to tell.
func TestScatterConn_ReleaseAllReservedConnections_ReservationLost(t *testing.T) {
	t.Run("discard all", func(t *testing.T) {
		gw := &mockGateway{releaseReservedConnectionErr: mterrors.NewReservedConnectionTerminated(42)}
		sc := NewScatterConn(gw, slog.Default())
		conn, state := newLossSession(protoutil.ReasonSetSeed | protoutil.ReasonTempTable)

		err := sc.ReleaseAllReservedConnections(context.Background(), conn, state, true)

		detail := requireSessionEnded(t, err, "set_seed")
		require.NotContains(t, detail, "temp_table")
		require.Nil(t, state.GetMatchingShardState(lossTarget))
	})
	t.Run("disconnect", func(t *testing.T) {
		gw := &mockGateway{releaseReservedConnectionErr: mterrors.NewReservedConnectionTerminated(42)}
		sc := NewScatterConn(gw, slog.Default())
		conn, state := newLossSession(protoutil.ReasonSetSeed)

		err := sc.ReleaseAllReservedConnections(context.Background(), conn, state, false)

		requireNotFatal(t, err)
	})
}

func TestScatterConn_CopyAbort_ReservationLost(t *testing.T) {
	gw := &mockGateway{copyAbortErr: mterrors.NewReservedConnectionTerminated(42)}
	sc := NewScatterConn(gw, slog.Default())
	conn, state := newLossSession(protoutil.ReasonCopy | protoutil.ReasonTempTable)

	err := sc.CopyAbort(context.Background(), conn, "tg1", "", state)

	requireSessionEnded(t, err, "temp_table")
	require.Nil(t, state.GetMatchingShardState(lossTarget))
}

func TestScatterConn_CopyFinalize_ReservationLost(t *testing.T) {
	gw := &mockGateway{copyFinalizeErr: mterrors.NewReservedConnectionTerminated(42)}
	sc := NewScatterConn(gw, slog.Default())
	conn, state := newLossSession(protoutil.ReasonCopy | protoutil.ReasonSessionAdvisoryLock)

	err := sc.CopyFinalize(context.Background(), conn, "tg1", "", state, nil,
		func(context.Context, *sqltypes.Result) error { return nil })

	requireSessionEnded(t, err, "session_advisory_lock")
}
