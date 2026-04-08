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

package executor

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

// mockReservedConn is a hand-rolled stub satisfying reservedConnAPI for unit tests.
// It records what the executor calls and lets tests inject errors.
type mockReservedConn struct {
	connID           int64
	inTxn            bool
	txnStatus        protocol.TransactionStatus
	remainingReasons uint32

	beginCalls      []string
	addedReasons    uint32
	removedReasons  uint32
	streamingCalled bool
	streamingSQL    string

	beginErr     error
	streamingErr error
}

func (m *mockReservedConn) ConnID() int64                         { return m.connID }
func (m *mockReservedConn) RemainingReasons() uint32              { return m.remainingReasons }
func (m *mockReservedConn) IsInTransaction() bool                 { return m.inTxn }
func (m *mockReservedConn) TxnStatus() protocol.TransactionStatus { return m.txnStatus }

func (m *mockReservedConn) BeginWithQuery(_ context.Context, q string) error {
	m.beginCalls = append(m.beginCalls, q)
	if m.beginErr != nil {
		return m.beginErr
	}
	m.inTxn = true
	m.remainingReasons |= protoutil.ReasonTransaction
	return nil
}

func (m *mockReservedConn) AddReservationReason(reason uint32) {
	m.addedReasons |= reason
	m.remainingReasons |= reason
}

func (m *mockReservedConn) RemoveReservationReason(reason uint32) bool {
	m.removedReasons |= reason
	m.remainingReasons &^= reason
	return m.remainingReasons == 0
}

func (m *mockReservedConn) QueryStreaming(_ context.Context, sql string, _ func(context.Context, *sqltypes.Result) error) error {
	m.streamingCalled = true
	m.streamingSQL = sql
	return m.streamingErr
}

// Compile-time check.
var _ reservedConnAPI = (*mockReservedConn)(nil)

// newTestExecutor returns an Executor that has just enough wiring to exercise
// streamExecuteOnReservedConn. The pool manager is left nil because the helper
// never touches it.
func newTestExecutor() *Executor {
	return &Executor{
		logger:   slog.Default(),
		poolerID: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
	}
}

func noopCallback(_ context.Context, _ *sqltypes.Result) error { return nil }

// TestStreamExecuteOnReservedConn_AddsTransactionViaBegin covers the new code
// path the reviewer flagged: an existing reserved connection (e.g. from a temp
// table) gets a transaction added on top via ReservationOptions, which should
// trigger a BEGIN with the requested begin_query before running the query.
func TestStreamExecuteOnReservedConn_AddsTransactionViaBegin(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		txnStatus:        protocol.TxnStatusInBlock,
		remainingReasons: protoutil.ReasonTempTable,
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "INSERT INTO t VALUES (1)",
		&query.ReservationOptions{
			Reasons:    protoutil.ReasonTransaction,
			BeginQuery: "BEGIN ISOLATION LEVEL SERIALIZABLE",
		},
		noopCallback,
	)

	require.NoError(t, err)
	require.Equal(t, []string{"BEGIN ISOLATION LEVEL SERIALIZABLE"}, rc.beginCalls,
		"should issue BEGIN with the caller-supplied query")
	require.True(t, rc.streamingCalled, "should stream the user query after BEGIN")
	require.Equal(t, "INSERT INTO t VALUES (1)", rc.streamingSQL)
	require.Equal(t, uint64(42), state.GetReservedConnectionId())
	// Both the original temp_table reason and the newly-added transaction reason
	// should be reflected in the returned state.
	require.Equal(t,
		protoutil.ReasonTransaction|protoutil.ReasonTempTable,
		state.GetReservationReasons(),
		"returned state should carry both pre-existing and newly-added reasons")
}

// TestStreamExecuteOnReservedConn_SkipsBeginIfAlreadyInTxn covers the guard
// that prevents a duplicate BEGIN when the connection is already in a
// transaction (e.g., the gateway re-sent ReasonTransaction redundantly).
func TestStreamExecuteOnReservedConn_SkipsBeginIfAlreadyInTxn(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		txnStatus:        protocol.TxnStatusInBlock,
		remainingReasons: protoutil.ReasonTransaction,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{Reasons: protoutil.ReasonTransaction},
		noopCallback,
	)

	require.NoError(t, err)
	require.Empty(t, rc.beginCalls, "should not BEGIN again when already in a transaction")
	require.True(t, rc.streamingCalled)
}

// TestStreamExecuteOnReservedConn_AddsTempTableReasonOnly covers the
// non-transaction reason branch: passing only ReasonTempTable should bypass
// BEGIN entirely and just record the reason on the connection.
func TestStreamExecuteOnReservedConn_AddsTempTableReasonOnly(t *testing.T) {
	rc := &mockReservedConn{
		connID:    42,
		txnStatus: protocol.TxnStatusIdle,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "CREATE TEMP TABLE t (id int)",
		&query.ReservationOptions{Reasons: protoutil.ReasonTempTable},
		noopCallback,
	)

	require.NoError(t, err)
	require.Empty(t, rc.beginCalls, "non-transaction reasons must not trigger BEGIN")
	require.Equal(t, protoutil.ReasonTempTable, rc.addedReasons,
		"temp_table reason should be added to the reservation")
	require.True(t, rc.streamingCalled)
}

// TestStreamExecuteOnReservedConn_BeginErrorPropagates covers the failure path
// when BEGIN itself fails: the error is returned wrapped, and the user query is
// never run.
func TestStreamExecuteOnReservedConn_BeginErrorPropagates(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		txnStatus:        protocol.TxnStatusIdle,
		remainingReasons: protoutil.ReasonTempTable,
		beginErr:         errors.New("boom"),
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{Reasons: protoutil.ReasonTransaction},
		noopCallback,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to begin transaction")
	require.False(t, rc.streamingCalled, "must not run the query when BEGIN fails")
	require.NotNil(t, state, "should still return current ReservedState on BEGIN failure")
	require.Equal(t, uint64(42), state.GetReservedConnectionId())
}

// TestStreamExecuteOnReservedConn_DefaultBeginQueryWhenEmpty covers the
// fallback to plain "BEGIN" when ReservationOptions.BeginQuery is empty.
func TestStreamExecuteOnReservedConn_DefaultBeginQueryWhenEmpty(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		txnStatus:        protocol.TxnStatusInBlock,
		remainingReasons: protoutil.ReasonTempTable,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{Reasons: protoutil.ReasonTransaction}, // BeginQuery left empty
		noopCallback,
	)

	require.NoError(t, err)
	require.Equal(t, []string{"BEGIN"}, rc.beginCalls,
		"empty BeginQuery should default to plain BEGIN")
}

// TestStreamExecuteOnReservedConn_NoReservationOptions covers the case where
// the caller passes a nil ReservationOptions: the helper should run the query
// directly without touching reservation reasons.
func TestStreamExecuteOnReservedConn_NoReservationOptions(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		txnStatus:        protocol.TxnStatusInBlock,
		remainingReasons: protoutil.ReasonTransaction,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1", nil, noopCallback,
	)

	require.NoError(t, err)
	require.Empty(t, rc.beginCalls)
	require.Equal(t, uint32(0), rc.addedReasons)
	require.True(t, rc.streamingCalled)
}

// TestStreamExecuteOnReservedConn_ReconcilesInlineCommit covers the
// after-the-fact reconciliation: if the SQL itself contained a COMMIT (e.g.
// "COMMIT" sent as a raw query), PG returns to TxnStatusIdle and the helper
// should drop the transaction reason from in-memory tracking.
func TestStreamExecuteOnReservedConn_ReconcilesInlineCommit(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		txnStatus:        protocol.TxnStatusIdle, // PG is no longer in a transaction
		remainingReasons: protoutil.ReasonTransaction,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "COMMIT", nil, noopCallback,
	)

	require.NoError(t, err)
	require.Equal(t, protoutil.ReasonTransaction, rc.removedReasons,
		"helper should drop the transaction reason when PG reports idle")
}

// TestStreamExecuteOnReservedConn_ReconcilesInlineBegin covers the symmetric
// case: SQL like "BEGIN; SELECT 1;" leaves PG in TxnStatusInBlock without
// having gone through ReservationOptions, so the helper must add the reason
// back so DiscardTempTables / ConcludeTransaction can find it.
func TestStreamExecuteOnReservedConn_ReconcilesInlineBegin(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            false,
		txnStatus:        protocol.TxnStatusInBlock, // PG entered a transaction mid-payload
		remainingReasons: protoutil.ReasonTempTable,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "BEGIN; SELECT 1;", nil, noopCallback,
	)

	require.NoError(t, err)
	require.Equal(t, protoutil.ReasonTransaction, rc.addedReasons,
		"helper should add the transaction reason when PG reports in-block")
}
