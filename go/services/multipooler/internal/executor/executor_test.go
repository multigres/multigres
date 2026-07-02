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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/admin"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
)

// mockReservedConn is a hand-rolled stub satisfying reservedConnAPI for unit tests.
// It records what the executor calls and lets tests inject errors.
type mockReservedConn struct {
	connID           int64
	inTxn            bool
	remainingReasons uint32

	beginCalls      []string
	addedReasons    uint32
	removedReasons  uint32
	streamingCalled bool
	streamingSQL    string

	beginErr     error
	streamingErr error

	queryCalls   []string
	queryResults []*sqltypes.Result
	queryErr     error

	pinnedPortals   []string
	releasedPortals []string
	releaseCalls    []reserved.ReleaseReason
	markedUntrusted bool
	openHoldCursors map[string]bool
}

func (m *mockReservedConn) ConnID() int64            { return m.connID }
func (m *mockReservedConn) ProcessID() uint32        { return 0 }
func (m *mockReservedConn) RemainingReasons() uint32 { return m.remainingReasons }
func (m *mockReservedConn) IsInTransaction() bool    { return m.inTxn }

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

func (m *mockReservedConn) ReserveForPortal(portalName string) {
	if m.openHoldCursors == nil {
		m.openHoldCursors = make(map[string]bool)
	}
	m.openHoldCursors[portalName] = true
	m.pinnedPortals = append(m.pinnedPortals, portalName)
	m.remainingReasons |= protoutil.ReasonPortal
	m.addedReasons |= protoutil.ReasonPortal
}

func (m *mockReservedConn) ReleasePortal(portalName string) bool {
	if _, ok := m.openHoldCursors[portalName]; !ok {
		return false
	}
	delete(m.openHoldCursors, portalName)
	m.releasedPortals = append(m.releasedPortals, portalName)
	if len(m.openHoldCursors) == 0 {
		m.remainingReasons &^= protoutil.ReasonPortal
		m.removedReasons |= protoutil.ReasonPortal
		return m.remainingReasons == 0
	}
	return false
}

func (m *mockReservedConn) Query(_ context.Context, sql string) ([]*sqltypes.Result, error) {
	m.queryCalls = append(m.queryCalls, sql)
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return m.queryResults, nil
}

func (m *mockReservedConn) Release(reason reserved.ReleaseReason, _ map[string]string) {
	m.releaseCalls = append(m.releaseCalls, reason)
}

func (m *mockReservedConn) MarkSessionStateUntrusted() {
	m.markedUntrusted = true
}

// Compile-time check.
var _ reservedConnAPI = (*mockReservedConn)(nil)

type stubPoolManager struct {
	reservedConn    *reserved.Conn
	reservedConnOK  bool
	regularConn     regular.PooledConn
	regularErr      error
	newReservedConn *reserved.Conn
	newReservedPool *reserved.Pool
	newReservedErr  error
}

func (m *stubPoolManager) Open(context.Context, *connpoolmanager.ConnectionConfig) {}
func (m *stubPoolManager) Close()                                                  {}
func (m *stubPoolManager) CloseForReopen()                                         {}
func (m *stubPoolManager) PgUser() string                                          { return "postgres" }
func (m *stubPoolManager) PgPassword() (string, bool)                              { return "", true }
func (m *stubPoolManager) GetAdminConn(context.Context) (admin.PooledConn, error)  { return nil, nil }
func (m *stubPoolManager) GetRegularConn(context.Context, string, []byte, []byte) (regular.PooledConn, error) {
	return nil, nil
}

func (m *stubPoolManager) GetRegularConnWithSettings(context.Context, map[string]string, string, []byte, []byte) (regular.PooledConn, error) {
	if m.regularErr != nil {
		return nil, m.regularErr
	}
	return m.regularConn, nil
}

func (m *stubPoolManager) NewReservedConn(ctx context.Context, _ map[string]string, _ string, _, _ []byte, opts ...reserved.ReservedConnOption) (*reserved.Conn, error) {
	if m.newReservedErr != nil {
		return nil, m.newReservedErr
	}
	if m.newReservedConn != nil {
		return m.newReservedConn, nil
	}
	if m.newReservedPool == nil {
		return nil, errors.New("not implemented in test stub")
	}
	return m.newReservedPool.NewConn(ctx, nil, opts...)
}

func (m *stubPoolManager) NewLogicalReplicationConn(context.Context, string, []byte, []byte) (*reserved.Conn, error) {
	return nil, errors.New("not implemented in test stub")
}

func (m *stubPoolManager) GetReservedConn(int64, string) (*reserved.Conn, bool) {
	return m.reservedConn, m.reservedConnOK
}

func (m *stubPoolManager) ApplySettingsToConn(context.Context, *regular.Conn, map[string]string) error {
	return nil
}
func (m *stubPoolManager) WaitForDrain(context.Context) error           { return nil }
func (m *stubPoolManager) WaitForReservedDrain(context.Context) error   { return nil }
func (m *stubPoolManager) CloseReservedConnections(context.Context) int { return 0 }
func (m *stubPoolManager) Stats() connpoolmanager.ManagerStats          { return connpoolmanager.ManagerStats{} }
func (m *stubPoolManager) CredentialQueryRecorder() connpoolmanager.CredentialQueryRecorder {
	return nil
}

var _ connpoolmanager.PoolManager = (*stubPoolManager)(nil)

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

// boolResult builds a single-row, single-column result holding a PostgreSQL
// boolean ("t"/"f"), as the pg_locks advisory probe returns.
func boolResult(b bool) []*sqltypes.Result {
	v := "f"
	if b {
		v = "t"
	}
	return []*sqltypes.Result{makeResult(makeRow(v))}
}

// TestReserveAndStreamExecute_BeginRetriesIdleSessionTimeout verifies the
// dashboard-refocus failure mode: the first write on a newly reserved backend is
// BEGIN, and PostgreSQL may have killed the pooled socket while it sat idle
// after a client SET idle_session_timeout. BEGIN must run inside the reserved
// pool's validation hook so acquireValidated can discard the stale connection
// and retry on a fresh one before surfacing an error to the client.
func TestReserveAndStreamExecute_BeginRetriesIdleSessionTimeout(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.OrderMatters()

	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT warmup",
		QueryResult: fakepgserver.MakeResult([]string{"?column?"}, [][]any{{1}}),
	})
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query: "BEGIN",
		Error: mterrors.NewIdleSessionTimeout(),
	})
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "BEGIN",
		QueryResult: &sqltypes.Result{CommandTag: "BEGIN"},
	})
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT 1",
		QueryResult: fakepgserver.MakeResult([]string{"?column?"}, [][]any{{1}}),
	})

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	defer pool.Close()

	// Put a backend through a successful borrow/recycle cycle so the BEGIN below
	// exercises a pooled idle socket rather than a brand-new connection.
	warm, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)
	_, err = warm.Query(context.Background(), "SELECT warmup")
	require.NoError(t, err)
	warm.Release(reserved.ReleaseCommit, nil)

	e := newTestExecutor()
	e.metrics = newQueryStats()
	e.poolManager = &stubPoolManager{newReservedPool: pool}

	var results []*sqltypes.Result
	state, err := e.reserveAndStreamExecute(
		context.Background(),
		"SELECT 1",
		&query.ExecuteOptions{User: "dashboard"},
		&query.ReservationOptions{Reasons: protoutil.ReasonTransaction},
		func(_ context.Context, result *sqltypes.Result) error {
			results = append(results, result)
			return nil
		},
	)

	require.NoError(t, err)
	require.NotNil(t, state)
	assert.NotZero(t, state.GetReservedConnectionId())
	assert.Equal(t, protoutil.ReasonTransaction, state.GetReservationReasons())
	require.Len(t, results, 1)
	assert.Equal(t, "SELECT 1", results[0].CommandTag)
	server.VerifyAllExecutedOrFail()
}

// TestStreamExecuteOnReservedConn_AdvisoryLockStillHeld verifies that after a
// statement on an advisory-lock-reserved connection, if PostgreSQL still
// reports an advisory lock the connection stays reserved.
func TestStreamExecuteOnReservedConn_AdvisoryLockStillHeld(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		remainingReasons: protoutil.ReasonSessionAdvisoryLock,
		queryResults:     boolResult(true),
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{RecheckAdvisoryLocks: true},
		nil,
		noopCallback,
	)

	require.NoError(t, err)
	require.Equal(t, []string{constants.PgLocksAdvisoryProbeSQL}, rc.queryCalls, "should probe pg_locks once")
	require.Empty(t, rc.releaseCalls, "connection must stay reserved while a lock is held")
	require.NotNil(t, state)
	require.Equal(t, protoutil.ReasonSessionAdvisoryLock, state.GetReservationReasons())
}

// TestStreamExecuteOnReservedConn_AdvisoryLockReleased verifies that when the
// probe reports no advisory locks remain, the reason is cleared and the
// connection is released back to the pool.
func TestStreamExecuteOnReservedConn_AdvisoryLockReleased(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		remainingReasons: protoutil.ReasonSessionAdvisoryLock,
		queryResults:     boolResult(false),
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT pg_advisory_unlock(101)",
		&query.ReservationOptions{RecheckAdvisoryLocks: true},
		nil,
		noopCallback,
	)

	require.NoError(t, err)
	require.Equal(t, []string{constants.PgLocksAdvisoryProbeSQL}, rc.queryCalls)
	require.Equal(t, protoutil.ReasonSessionAdvisoryLock, rc.removedReasons,
		"advisory-lock reason must be cleared when no locks remain")
	require.Equal(t, []reserved.ReleaseReason{reserved.ReleaseAdvisoryUnlock}, rc.releaseCalls,
		"connection must be released once the last advisory lock is gone")
	require.Nil(t, state, "released connection should report a nil (zero) reservation state")
}

// TestStreamExecuteOnReservedConn_AdvisoryLockSkippedInTxn verifies that the
// probe is skipped while a transaction is open — ReasonTransaction keeps the
// connection pinned and transaction-level advisory locks would pollute the
// probe.
func TestStreamExecuteOnReservedConn_AdvisoryLockSkippedInTxn(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		remainingReasons: protoutil.ReasonSessionAdvisoryLock | protoutil.ReasonTransaction,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{RecheckAdvisoryLocks: true},
		nil,
		noopCallback,
	)

	require.NoError(t, err)
	require.Empty(t, rc.queryCalls, "must not probe pg_locks inside a transaction")
	require.Empty(t, rc.releaseCalls)
}

// TestStreamExecuteOnReservedConn_AdvisoryProbeErrorKeepsPinned verifies that a
// failed probe leaves the connection pinned rather than risking a lock leak.
func TestStreamExecuteOnReservedConn_AdvisoryProbeErrorKeepsPinned(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		remainingReasons: protoutil.ReasonSessionAdvisoryLock,
		queryErr:         errors.New("probe boom"),
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{RecheckAdvisoryLocks: true},
		nil,
		noopCallback,
	)

	require.NoError(t, err)
	require.Empty(t, rc.releaseCalls, "probe failure must not release the connection")
	require.NotNil(t, state)
}

// TestStreamExecuteOnReservedConn_AdvisoryEmptyProbeKeepsPinned verifies that an
// unexpected empty probe result (no rows) is treated like a probe failure: the
// connection stays pinned rather than being released with held defaulting to
// false, which would risk leaking the client's locks.
func TestStreamExecuteOnReservedConn_AdvisoryEmptyProbeKeepsPinned(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		remainingReasons: protoutil.ReasonSessionAdvisoryLock,
		queryResults:     nil, // probe returned no rows
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT pg_advisory_unlock(101)",
		&query.ReservationOptions{RecheckAdvisoryLocks: true},
		nil,
		noopCallback,
	)

	require.NoError(t, err)
	require.Equal(t, []string{constants.PgLocksAdvisoryProbeSQL}, rc.queryCalls, "should still probe")
	require.Empty(t, rc.releaseCalls, "empty probe result must not release the connection")
	require.Empty(t, rc.removedReasons, "advisory reason must be kept on an empty probe result")
	require.NotNil(t, state)
}

// TestStreamExecuteOnReservedConn_AdvisoryNoRecheckNoProbe verifies the gating:
// an ordinary statement on an advisory-pinned connection (recheck flag NOT set)
// must not probe pg_locks at all, keeping the probe off the per-statement hot
// path. The gateway only sets the recheck flag for statements that touch
// advisory locks.
func TestStreamExecuteOnReservedConn_AdvisoryNoRecheckNoProbe(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		remainingReasons: protoutil.ReasonSessionAdvisoryLock,
		queryResults:     boolResult(false), // would unpin IF the probe ran
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{}, // no RecheckAdvisoryLocks
		nil,
		noopCallback,
	)

	require.NoError(t, err)
	require.Empty(t, rc.queryCalls, "must not probe pg_locks without the recheck signal")
	require.Empty(t, rc.releaseCalls, "must stay reserved without a recheck")
	require.NotNil(t, state)
	require.Equal(t, protoutil.ReasonSessionAdvisoryLock, state.GetReservationReasons())
}

// TestStreamExecuteOnReservedConn_AddsTransactionViaBegin covers the new code
// path the reviewer flagged: an existing reserved connection (e.g. from a temp
// table) gets a transaction added on top via ReservationOptions, which should
// trigger a BEGIN with the requested begin_query before running the query.
func TestStreamExecuteOnReservedConn_AddsTransactionViaBegin(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		remainingReasons: protoutil.ReasonTempTable,
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "INSERT INTO t VALUES (1)",
		&query.ReservationOptions{
			Reasons:    protoutil.ReasonTransaction,
			BeginQuery: "BEGIN ISOLATION LEVEL SERIALIZABLE",
		},
		nil,
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
		remainingReasons: protoutil.ReasonTransaction,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{Reasons: protoutil.ReasonTransaction},
		nil,
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
		connID: 42,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "CREATE TEMP TABLE t (id int)",
		&query.ReservationOptions{Reasons: protoutil.ReasonTempTable},
		nil,
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
		remainingReasons: protoutil.ReasonTempTable,
		beginErr:         errors.New("boom"),
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{Reasons: protoutil.ReasonTransaction},
		nil,
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
		remainingReasons: protoutil.ReasonTempTable,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{Reasons: protoutil.ReasonTransaction}, // BeginQuery left empty
		nil,
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
		remainingReasons: protoutil.ReasonTransaction,
	}
	e := newTestExecutor()

	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1", nil, nil, noopCallback,
	)

	require.NoError(t, err)
	require.Empty(t, rc.beginCalls)
	require.Equal(t, uint32(0), rc.addedReasons)
	require.True(t, rc.streamingCalled)
}

// TestStreamExecuteOnReservedConn_PinPortalSuccess covers the WITH HOLD pin
// path: PinPortalNames arrives in ReservationOptions, ReserveForPortal is
// called BEFORE the query, and the cursor stays pinned after a successful
// DECLARE.
func TestStreamExecuteOnReservedConn_PinPortalSuccess(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		remainingReasons: protoutil.ReasonTransaction,
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc,
		"DECLARE c1 CURSOR WITH HOLD FOR SELECT 1",
		&query.ReservationOptions{PinPortalNames: []string{"c1"}},
		nil,
		noopCallback,
	)

	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, rc.pinnedPortals,
		"pin should be registered for the WITH HOLD cursor")
	require.Empty(t, rc.releasedPortals, "no release on success")
	require.Empty(t, rc.releaseCalls, "connection should not be released on a successful pin")
	require.True(t, rc.streamingCalled)
	require.Equal(t, uint64(42), state.GetReservedConnectionId())
	require.Equal(t,
		protoutil.ReasonTransaction|protoutil.ReasonPortal,
		state.GetReservationReasons(),
		"returned state should carry the new portal pin alongside the transaction reason")
}

// TestStreamExecuteOnReservedConn_PinPortalFailureRollsBack verifies the
// MUL-389 review-fix B2 invariant: if DECLARE fails on the backend, every
// pin we just registered is rolled back. If the rollback drains the last
// reservation reason, the connection is released and a nil ReservedState is
// returned so the gateway clears its tracking.
func TestStreamExecuteOnReservedConn_PinPortalFailureRollsBack(t *testing.T) {
	rc := &mockReservedConn{
		connID:       42,
		streamingErr: errors.New("DECLARE CURSOR WITH HOLD cannot be used outside of a transaction"),
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc,
		"DECLARE c1 CURSOR WITH HOLD FOR SELECT 1",
		&query.ReservationOptions{PinPortalNames: []string{"c1"}},
		nil,
		noopCallback,
	)

	require.Error(t, err)
	require.Equal(t, []string{"c1"}, rc.pinnedPortals,
		"pin should be registered before the failing DECLARE")
	require.Equal(t, []string{"c1"}, rc.releasedPortals,
		"failed DECLARE must roll back every pin it added")
	require.Equal(t, uint32(0), rc.remainingReasons,
		"no reasons should remain after rollback")
	require.Equal(t, []reserved.ReleaseReason{reserved.ReleaseError}, rc.releaseCalls,
		"connection should be released when the rollback drains the last reason")
	require.Nil(t, state, "released conn must surface as zero ReservedState")
}

// TestStreamExecuteOnReservedConn_PinPortalFailureKeepsOtherReasons covers
// the case where pin rollback drains ReasonPortal but other reasons (e.g.,
// ReasonTransaction) remain — the connection must stay reserved and the
// returned state should reflect the surviving bitmask.
func TestStreamExecuteOnReservedConn_PinPortalFailureKeepsOtherReasons(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		remainingReasons: protoutil.ReasonTransaction,
		streamingErr:     errors.New("syntax error"),
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc,
		"DECLARE bad CURSOR WITH HOLD FOR SELECT garbage",
		&query.ReservationOptions{PinPortalNames: []string{"bad"}},
		nil,
		noopCallback,
	)

	require.Error(t, err)
	require.Equal(t, []string{"bad"}, rc.releasedPortals,
		"failed DECLARE must roll back the pin")
	require.Empty(t, rc.releaseCalls,
		"connection must stay reserved while the transaction reason persists")
	require.Equal(t, protoutil.ReasonTransaction, rc.remainingReasons,
		"transaction reason must survive pin rollback")
	require.NotNil(t, state, "non-released conn must surface its remaining reasons")
	require.Equal(t, uint64(42), state.GetReservedConnectionId())
}

// TestStreamExecuteOnReservedConn_ReleasePortalDrainsConnection verifies
// CLOSE / DISCARD ALL semantics: ReleasePortalNames drains the matching
// pins, and when the last reason clears, the connection is released with
// a zero ReservedState.
func TestStreamExecuteOnReservedConn_ReleasePortalDrainsConnection(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		remainingReasons: protoutil.ReasonPortal,
		openHoldCursors:  map[string]bool{"c1": true},
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "CLOSE c1",
		&query.ReservationOptions{ReleasePortalNames: []string{"c1"}},
		nil,
		noopCallback,
	)

	require.NoError(t, err)
	require.True(t, rc.streamingCalled, "CLOSE must reach the backend before the pin is dropped")
	require.Equal(t, []string{"c1"}, rc.releasedPortals)
	require.Equal(t, []reserved.ReleaseReason{reserved.ReleasePortalComplete}, rc.releaseCalls,
		"draining the final ReasonPortal must release the backend")
	require.Nil(t, state, "released conn must surface as zero ReservedState")
}

// TestStreamExecuteOnReservedConn_ReleasePortalKeepsOtherReasons covers
// CLOSE on a HOLD cursor while a transaction is still active: the pin
// drops but the transaction reason keeps the conn reserved.
func TestStreamExecuteOnReservedConn_ReleasePortalKeepsOtherReasons(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		remainingReasons: protoutil.ReasonTransaction | protoutil.ReasonPortal,
		openHoldCursors:  map[string]bool{"c1": true},
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "CLOSE c1",
		&query.ReservationOptions{ReleasePortalNames: []string{"c1"}},
		nil,
		noopCallback,
	)

	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, rc.releasedPortals)
	require.Empty(t, rc.releaseCalls,
		"conn must stay reserved while ReasonTransaction is set")
	require.Equal(t, protoutil.ReasonTransaction, rc.remainingReasons)
	require.Equal(t, uint64(42), state.GetReservedConnectionId())
}

// TestStreamExecuteOnReservedConn_MarkSessionStateUntrusted verifies that a
// statement carrying ReservationOptions.MarkSessionStateUntrusted (e.g. a
// ROLLBACK TO SAVEPOINT) marks the reserved connection's session state
// untrusted so the next reconciliation is forced.
func TestStreamExecuteOnReservedConn_MarkSessionStateUntrusted(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		remainingReasons: protoutil.ReasonTransaction,
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "ROLLBACK TO SAVEPOINT sp",
		&query.ReservationOptions{MarkSessionStateUntrusted: true},
		nil,
		noopCallback,
	)

	require.NoError(t, err)
	require.True(t, rc.markedUntrusted,
		"ROLLBACK TO SAVEPOINT must mark the reserved connection untrusted")
	require.Empty(t, rc.releaseCalls)
	require.Equal(t, uint64(42), state.GetReservedConnectionId())
}

func TestScramKeysFromOptions(t *testing.T) {
	ck := []byte{1, 2, 3}
	sk := []byte{4, 5, 6}

	tests := []struct {
		name    string
		options *query.ExecuteOptions
		wantCK  []byte
		wantSK  []byte
	}{
		{
			name:    "nil options",
			options: nil,
		},
		{
			name:    "options without user_auth",
			options: &query.ExecuteOptions{User: "alice"},
		},
		{
			name:    "options with populated user_auth",
			options: &query.ExecuteOptions{User: "alice", UserAuth: &query.UserAuth{ClientKey: ck, ServerKey: sk}},
			wantCK:  ck,
			wantSK:  sk,
		},
		{
			name:    "options with empty user_auth",
			options: &query.ExecuteOptions{User: "alice", UserAuth: &query.UserAuth{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCK, gotSK := scramKeysFromOptions(tt.options)
			require.Equal(t, tt.wantCK, gotCK)
			require.Equal(t, tt.wantSK, gotSK)
		})
	}
}

// --- sessionSettingsForPool tests ---

func TestSessionSettingsForPool_DisabledPassthrough(t *testing.T) {
	e := &Executor{vpidStampEnabled: false}

	t.Run("nil settings", func(t *testing.T) {
		require.Nil(t, e.sessionSettingsForPool(nil))
	})

	t.Run("application_name preserved", func(t *testing.T) {
		in := map[string]string{"application_name": "client-app", "search_path": "public"}
		got := e.sessionSettingsForPool(in)
		require.Equal(t, in, got)
	})
}

func TestSessionSettingsForPool_EnabledFiltersAppName(t *testing.T) {
	e := &Executor{vpidStampEnabled: true}

	t.Run("nil settings stays nil", func(t *testing.T) {
		require.Nil(t, e.sessionSettingsForPool(nil))
	})

	t.Run("only application_name collapses to nil", func(t *testing.T) {
		require.Nil(t, e.sessionSettingsForPool(map[string]string{"application_name": "x"}))
	})

	t.Run("mixed settings drops application_name only", func(t *testing.T) {
		got := e.sessionSettingsForPool(map[string]string{
			"application_name":  "client-app",
			"search_path":       "public",
			"statement_timeout": "1000",
		})
		require.Equal(t, map[string]string{
			"search_path":       "public",
			"statement_timeout": "1000",
		}, got)
	})

	t.Run("case-insensitive match on application_name", func(t *testing.T) {
		got := e.sessionSettingsForPool(map[string]string{
			"Application_Name": "client-app",
			"APPLICATION_NAME": "other",
			"search_path":      "public",
		})
		require.Equal(t, map[string]string{"search_path": "public"}, got)
	})

	t.Run("no application_name returns equivalent map", func(t *testing.T) {
		in := map[string]string{"search_path": "public"}
		got := e.sessionSettingsForPool(in)
		require.Equal(t, in, got)
	})
}

// --- sessionSettingsFromOptions tests ---

func TestSessionSettingsFromOptions_NilOptions(t *testing.T) {
	e := &Executor{vpidStampEnabled: false}
	require.Nil(t, e.sessionSettingsFromOptions(nil))
}

// --- stampVpid* early-return tests ---
//
// The happy-path SET application_name issue is covered by integration tests
// (it requires a real pool connection). Here we lock in the guard semantics:
// the helpers must be safe no-ops when stamping is disabled, options is nil,
// or ClientConnectionId is zero. A nil conn is intentionally passed to prove
// the helpers return before touching it.

func TestStampVpidOnReserved_NoOpGuards(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		enabled bool
		options *query.ExecuteOptions
	}{
		{"disabled with options", false, &query.ExecuteOptions{ClientConnectionId: 5}},
		{"enabled with nil options", true, nil},
		{"enabled with zero id", true, &query.ExecuteOptions{ClientConnectionId: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &Executor{vpidStampEnabled: tc.enabled}
			// nil conn would panic on SetApplicationName — guard must short-circuit first.
			e.stampVpidOnReserved(ctx, nil, tc.options)
		})
	}
}

func TestStampVpidOnRegular_NoOpGuards(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		enabled bool
		options *query.ExecuteOptions
	}{
		{"disabled with options", false, &query.ExecuteOptions{ClientConnectionId: 5}},
		{"enabled with nil options", true, nil},
		{"enabled with zero id", true, &query.ExecuteOptions{ClientConnectionId: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &Executor{vpidStampEnabled: tc.enabled}
			e.stampVpidOnRegular(ctx, nil, tc.options)
		})
	}
}

// --- stampVpid* happy-path tests ---
//
// These wire a real *regular.Conn / *reserved.Conn against a fakepgserver and
// verify that the helper issues the expected SET application_name when
// stamping is enabled and ClientConnectionId is non-zero. This is the only
// behaviour the early-return tests above don't cover.

func TestStampVpidOnRegular_HappyPath(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	ctx := context.Background()
	clientConn, err := client.Connect(ctx, ctx, server.ClientConfig())
	require.NoError(t, err)
	conn := regular.NewConn(clientConn, nil)
	defer conn.Close()

	e := &Executor{vpidStampEnabled: true}
	server.ResetQueryLog()
	e.stampVpidOnRegular(ctx, conn, &query.ExecuteOptions{ClientConnectionId: 99})

	assert.Equal(t, "set application_name = 'multigres_vpid:99'", server.QueryLog())
}

func TestStampVpidOnReserved_HappyPath(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()
	rconn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	defer rconn.Release(reserved.ReleaseCommit, nil)

	e := &Executor{vpidStampEnabled: true}
	server.ResetQueryLog()
	e.stampVpidOnReserved(ctx, rconn, &query.ExecuteOptions{ClientConnectionId: 123})

	assert.Equal(t, "set application_name = 'multigres_vpid:123'", server.QueryLog())
}

// TestReleaseReservedConnection_UntrustedSyncsConnstateFromGateway is a
// regression test for the cross-client GUC leak where a sticky
// ROLLBACK-TO-SAVEPOINT "untrusted" flag survived to session teardown under a
// surviving session reason (e.g. a session-level advisory lock that outlives
// COMMIT). ReleaseReservedConnection must forward the gateway's authoritative
// session settings to the release boundary so connstate is synced to the truth,
// not wrongly cleared — clearing it would leak the backend's real session GUCs
// to the next client that reuses this pooled backend.
func TestReleaseReservedConnection_UntrustedSyncsConnstateFromGateway(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	cache := connstate.NewSettingsCache(16)
	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		SettingsCache:     cache,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()

	// Simulate the post-ROLLBACK-TO-SAVEPOINT, post-COMMIT state: connstate is
	// stale (holds the pre-rollback value), the connection is marked untrusted,
	// and it is no longer in a transaction (a surviving session reason kept it
	// reserved, so the teardown's rollback step is skipped and the untrusted flag
	// stays sticky).
	stale := cache.GetOrCreate(map[string]string{"search_path": "myschema", "work_mem": "256MB"})
	rconn, err := pool.NewConn(ctx, stale)
	require.NoError(t, err)
	rconn.MarkSessionStateUntrusted()
	require.False(t, rconn.IsInTransaction())

	e := &Executor{
		logger:      slog.Default(),
		poolerID:    &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		poolManager: &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
	}

	// Gateway's authoritative settings after the savepoint rollback: work_mem
	// reverted, the pre-savepoint search_path retained.
	gatewaySettings := map[string]string{"search_path": "myschema"}
	server.ResetQueryLog()

	err = e.ReleaseReservedConnection(ctx, nil, &query.ExecuteOptions{
		ReservedConnectionId: uint64(rconn.ConnID()),
		SessionSettings:      gatewaySettings,
	})
	require.NoError(t, err)

	// The connstate sync is in-memory only — no backend SQL.
	assert.NotContains(t, server.QueryLog(), "reset all")
	assert.NotContains(t, server.QueryLog(), "set_config")

	// connstate must equal the gateway truth: NOT cleared to nil (the bug) and
	// NOT left at the stale pre-rollback value.
	expected := cache.GetOrCreate(gatewaySettings)
	assert.Equal(t, expected, rconn.Conn().Settings(),
		"untrusted teardown must sync connstate to gateway settings, not clear or leave it stale")
	assert.False(t, rconn.SessionStateUntrusted(), "successful sync must clear the untrusted flag")
}

func TestMaterializeExecuteSQLPreparedStatementUsesPoolerConsolidation(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	ctx := context.Background()
	clientConn, err := client.Connect(ctx, ctx, server.ClientConfig())
	require.NoError(t, err)
	conn := regular.NewConn(clientConn, nil)
	defer conn.Close()

	e := NewExecutor(slog.Default(), nil, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	first := &query.ExecuteSqlPreparedStatement{
		PreparedStatement: &query.PreparedStatement{Name: "stmt0", Query: "SELECT $1", ParamTypes: []uint32{23}},
		SqlPrefix:         "EXECUTE ",
		SqlSuffix:         " ( 1 )",
	}
	second := &query.ExecuteSqlPreparedStatement{
		PreparedStatement: &query.PreparedStatement{Name: "stmt99", Query: "SELECT $1", ParamTypes: []uint32{23}},
		SqlPrefix:         "EXPLAIN EXECUTE ",
		SqlSuffix:         " ( 2 )",
	}

	sql1, err := e.materializeExecuteSQLPreparedStatement(ctx, conn, first)
	require.NoError(t, err)
	sql2, err := e.materializeExecuteSQLPreparedStatement(ctx, conn, second)
	require.NoError(t, err)

	assert.Equal(t, "EXECUTE ppstmt0 ( 1 )", sql1)
	assert.Equal(t, "EXPLAIN EXECUTE ppstmt0 ( 2 )", sql2)
	assert.NotNil(t, conn.State().GetPreparedStatement("ppstmt0"))
	assert.Nil(t, conn.State().GetPreparedStatement("stmt0"))
	assert.Nil(t, conn.State().GetPreparedStatement("stmt99"))
}

func TestMaterializeExecuteSQLPreparedStatementValidation(t *testing.T) {
	e := NewExecutor(slog.Default(), nil, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	_, err := e.materializeExecuteSQLPreparedStatement(context.Background(), nil, nil)
	require.ErrorContains(t, err, "SQL EXECUTE prepared statement is required")

	_, err = e.materializeExecuteSQLPreparedStatement(context.Background(), nil, &query.ExecuteSqlPreparedStatement{})
	require.ErrorContains(t, err, "SQL EXECUTE prepared statement metadata is required")
}

func TestStreamExecuteMaterializesExecuteSQLOnRegularConnection(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	ctx := context.Background()
	clientConn, err := client.Connect(ctx, ctx, server.ClientConfig())
	require.NoError(t, err)

	pm := &stubPoolManager{
		regularConn: &connpool.Pooled[*regular.Conn]{Conn: regular.NewConn(clientConn, nil)},
	}
	e := NewExecutor(slog.Default(), pm, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	_, err = e.StreamExecute(ctx, &query.Target{}, "EXECUTE gateway_stmt ( 1 )", &query.ExecuteOptions{
		User: "postgres",
		ExecuteSqlPreparedStatement: &query.ExecuteSqlPreparedStatement{
			PreparedStatement: &query.PreparedStatement{Name: "stmt0", Query: "SELECT $1", ParamTypes: []uint32{23}},
			SqlPrefix:         "EXECUTE ",
			SqlSuffix:         " ( 1 )",
		},
	}, nil, noopCallback)
	require.NoError(t, err)

	assert.Equal(t, "execute ppstmt0 ( 1 )", server.QueryLog())
}

func TestStreamExecuteMaterializesExecuteSQLOnExistingReservedConnection(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()
	rconn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	defer rconn.Release(reserved.ReleaseCommit, nil)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true}, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	state, err := e.StreamExecute(ctx, &query.Target{}, "EXPLAIN EXECUTE gateway_stmt", &query.ExecuteOptions{
		User:                 "postgres",
		ReservedConnectionId: uint64(rconn.ConnID()),
		ExecuteSqlPreparedStatement: &query.ExecuteSqlPreparedStatement{
			PreparedStatement: &query.PreparedStatement{Name: "stmt0", Query: "SELECT 1"},
			SqlPrefix:         "EXPLAIN EXECUTE ",
		},
	}, nil, noopCallback)
	require.NoError(t, err)
	require.NotNil(t, state)

	assert.Equal(t, "explain execute ppstmt0", server.QueryLog())
}

func TestStreamExecuteMaterializesExecuteSQLOnNewReservedConnection(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()
	e := NewExecutor(slog.Default(), &stubPoolManager{newReservedPool: pool}, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	state, err := e.StreamExecute(ctx, &query.Target{}, "CREATE TEMP TABLE t AS EXECUTE gateway_stmt", &query.ExecuteOptions{
		User: "postgres",
		ExecuteSqlPreparedStatement: &query.ExecuteSqlPreparedStatement{
			PreparedStatement: &query.PreparedStatement{Name: "stmt0", Query: "SELECT 1"},
			SqlPrefix:         "CREATE TEMP TABLE t AS EXECUTE ",
		},
	}, &query.ReservationOptions{Reasons: protoutil.ReasonTempTable}, noopCallback)
	require.NoError(t, err)
	require.NotNil(t, state)

	assert.Equal(t, protoutil.ReasonTempTable, state.GetReservationReasons())
	assert.Equal(t, "create temp table t as execute ppstmt0", server.QueryLog())
}

func TestStreamExecuteRollsBackNewReservedTransactionOnMaterializationError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()
	rconn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	e := NewExecutor(slog.Default(), &stubPoolManager{newReservedConn: rconn}, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	_, err = e.StreamExecute(ctx, &query.Target{}, "EXECUTE gateway_stmt", &query.ExecuteOptions{
		User: "postgres",
		ExecuteSqlPreparedStatement: &query.ExecuteSqlPreparedStatement{
			SqlPrefix: "EXECUTE ",
		},
	}, &query.ReservationOptions{Reasons: protoutil.ReasonTransaction}, noopCallback)
	require.ErrorContains(t, err, "failed to materialize SQL EXECUTE prepared statement")

	assert.Equal(t, "rollback", server.QueryLog())
}

// --- NewExecutor smoke test ---

func TestNewExecutor(t *testing.T) {
	logger := slog.Default()
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}

	t.Run("stamp enabled", func(t *testing.T) {
		e := NewExecutor(logger, nil, poolerID, true)
		require.NotNil(t, e)
		assert.True(t, e.vpidStampEnabled)
		assert.Equal(t, poolerID, e.poolerID)
		assert.NotNil(t, e.poolerConsolidator, "constructor must initialise the consolidator")
	})

	t.Run("stamp disabled", func(t *testing.T) {
		e := NewExecutor(logger, nil, poolerID, false)
		require.NotNil(t, e)
		assert.False(t, e.vpidStampEnabled)
	})
}

func TestCopyOutReady_ReservedConnectionNotFound(t *testing.T) {
	e := newTestExecutor()
	e.poolManager = &stubPoolManager{}

	_, _, _, _, err := e.CopyOutReady(
		context.Background(),
		protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		"COPY t TO STDOUT",
		&query.ExecuteOptions{User: "alice", ReservedConnectionId: 42},
		nil,
	)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSSerializationFailure), "expected 40001, got: %v", err)
	require.Contains(t, err.Error(), "terminated during a planned failover")
}

// TestConcludeTransaction_ReservedConnTerminated covers the failover-leak fix:
// when a COMMIT/ROLLBACK arrives for a reserved connection that was already
// force-closed (e.g. the planned-failover drain exceeded its grace period while
// the client sat idle-in-transaction), the executor must return an honest 40001
// (transaction aborted) rather than a bare error or the misleading MTF01 — so
// the client retries the whole transaction.
func TestConcludeTransaction_ReservedConnTerminated(t *testing.T) {
	e := newTestExecutor()
	e.poolManager = &stubPoolManager{reservedConnOK: false}

	_, _, err := e.ConcludeTransaction(
		context.Background(),
		protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		&query.ExecuteOptions{User: "alice", ReservedConnectionId: 42},
		0, // TRANSACTION_CONCLUSION_UNSPECIFIED — unused on the not-found path
		nil,
		false,
		false,
	)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSSerializationFailure), "expected 40001, got: %v", err)
	assert.False(t, mterrors.IsErrorCode(err, mterrors.MTF01.ID), "must not surface MTF01: %v", err)
	require.Contains(t, err.Error(), "terminated during a planned failover")
}

func TestCopyOutStream_ValidationAndNotFound(t *testing.T) {
	e := newTestExecutor()

	t.Run("missing reserved connection id", func(t *testing.T) {
		_, _, err := e.CopyOutStream(
			context.Background(),
			protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
			&query.ExecuteOptions{},
			func(client.CopyOutMessage) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "options.ReservedConnectionId is required for CopyOutStream")
	})

	t.Run("reserved connection not found", func(t *testing.T) {
		e.poolManager = &stubPoolManager{}
		_, _, err := e.CopyOutStream(
			context.Background(),
			protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
			&query.ExecuteOptions{User: "alice", ReservedConnectionId: 99},
			func(client.CopyOutMessage) error { return nil },
		)
		require.Error(t, err)
		assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSSerializationFailure), "expected 40001, got: %v", err)
		require.Contains(t, err.Error(), "terminated during a planned failover")
	})
}

func TestCopyAbort_NilOptionsAndNoCopyReason(t *testing.T) {
	e := newTestExecutor()

	t.Run("nil options is best-effort no-op", func(t *testing.T) {
		state, err := e.CopyAbort(context.Background(), protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED), "abort", nil)
		require.NoError(t, err)
		require.Nil(t, state)
	})

	t.Run("missing reserved conn is best-effort no-op", func(t *testing.T) {
		e.poolManager = &stubPoolManager{}

		state, err := e.CopyAbort(
			context.Background(),
			protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
			"abort",
			&query.ExecuteOptions{User: "postgres", ReservedConnectionId: 777},
		)
		require.NoError(t, err)
		require.Nil(t, state)
	})
}

// newDeadReservedConnTestExecutor spins up a reserved connection backed by a
// fake PostgreSQL server and returns the executor, the pool, and the conn.
// Callers force-close the connection's raw socket to simulate a silently dead
// backend (the same failure mode as a killed/crashed PostgreSQL process),
// then exercise Describe against it.
func newDeadReservedConnTestExecutor(t *testing.T) (*Executor, *reserved.Pool, *reserved.Conn) {
	t.Helper()

	server := fakepgserver.New(t)
	t.Cleanup(server.Close)
	server.SetNeverFail(true)

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	t.Cleanup(pool.Close)

	rconn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	return e, pool, rconn
}

// applySettingsPoolManager forwards ApplySettingsToConn to the real
// regular.Conn.ApplySettings so tests can exercise session-settings-apply failures
// against a force-closed socket. stubPoolManager's own ApplySettingsToConn is a
// permanent no-op success and can never surface a failure.
type applySettingsPoolManager struct {
	stubPoolManager
}

func (m *applySettingsPoolManager) ApplySettingsToConn(ctx context.Context, conn *regular.Conn, settings map[string]string) error {
	cache := connstate.NewSettingsCache(16)
	return conn.ApplySettings(ctx, cache.GetOrCreate(settings))
}

// newDeadReservedConnTestExecutorApplySettings is newDeadReservedConnTestExecutor but
// wired with applySettingsPoolManager so ApplySettingsToConn performs a real write.
func newDeadReservedConnTestExecutorApplySettings(t *testing.T) (*Executor, *reserved.Pool, *reserved.Conn) {
	t.Helper()

	server := fakepgserver.New(t)
	t.Cleanup(server.Close)
	server.SetNeverFail(true)

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	t.Cleanup(pool.Close)

	rconn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	e := NewExecutor(slog.Default(), &applySettingsPoolManager{stubPoolManager{reservedConn: rconn, reservedConnOK: true}},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	return e, pool, rconn
}

// TestDescribeReservedConnDeadSocket_EnsurePreparedError is the regression for
// MTD06 "describe failed ... broken pipe": when the reserved backend socket
// is already dead and the statement has never been prepared on it,
// ensurePrepared's Parse write fails first. Describe must release the
// reservation and return a clean, retryable "reserved connection terminated"
// error instead of wrapping the raw connection error.
func TestDescribeReservedConnDeadSocket_EnsurePreparedError(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutor(t)
	connID := rconn.ConnID()

	// Simulate the backend socket having silently died: force-close without a
	// graceful Terminate, so the next write fails like a real broken pipe.
	rconn.Conn().RawConn().ForceClose()

	desc, err := e.Describe(context.Background(), &query.Target{},
		&query.PreparedStatement{Name: "s1", Query: "SELECT 1"}, nil,
		&query.ExecuteOptions{ReservedConnectionId: uint64(connID)})

	require.Nil(t, desc)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "failed to ensure prepared statement",
		"must not leak the raw wrap/connection error")
	assert.Equal(t, mterrors.NewReservedConnectionTerminated(uint64(connID)), err)

	_, stillActive := pool.Get(connID)
	assert.False(t, stillActive, "dead reserved connection must be released, not left dangling")
}

// TestDescribeReservedConnDeadSocket_DescribePreparedError covers the case
// where the statement is already prepared on the reserved connection (so
// ensurePrepared is a no-op) and the backend dies before a subsequent
// Describe. The DescribePrepared write must fail cleanly.
func TestDescribeReservedConnDeadSocket_DescribePreparedError(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutor(t)
	connID := rconn.ConnID()
	options := &query.ExecuteOptions{ReservedConnectionId: uint64(connID)}
	stmt := &query.PreparedStatement{Name: "s1", Query: "SELECT 1"}

	// Prepare the statement while the backend is still alive.
	_, err := e.Describe(context.Background(), &query.Target{}, stmt, nil, options)
	require.NoError(t, err)

	// The backend socket dies silently; the reserved conn stays held (no
	// background health check), same as the real MTD06 scenario.
	rconn.Conn().RawConn().ForceClose()

	desc, err := e.Describe(context.Background(), &query.Target{}, stmt, nil, options)
	require.Nil(t, desc)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "failed to describe prepared statement",
		"must not leak the raw wrap/connection error")
	assert.Equal(t, mterrors.NewReservedConnectionTerminated(uint64(connID)), err)

	_, stillActive := pool.Get(connID)
	assert.False(t, stillActive, "dead reserved connection must be released, not left dangling")
}

// TestDescribeReservedConnDeadSocket_BindAndDescribeError covers the portal
// describe path (Describe called with a bound portal rather than just a
// prepared statement name).
func TestDescribeReservedConnDeadSocket_BindAndDescribeError(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutor(t)
	connID := rconn.ConnID()
	options := &query.ExecuteOptions{ReservedConnectionId: uint64(connID)}
	stmt := &query.PreparedStatement{Name: "s1", Query: "SELECT 1"}

	// Prepare the statement while the backend is still alive.
	_, err := e.Describe(context.Background(), &query.Target{}, stmt, nil, options)
	require.NoError(t, err)

	rconn.Conn().RawConn().ForceClose()

	desc, err := e.Describe(context.Background(), &query.Target{}, stmt, &query.Portal{}, options)
	require.Nil(t, desc)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "failed to describe portal",
		"must not leak the raw wrap/connection error")
	assert.Equal(t, mterrors.NewReservedConnectionTerminated(uint64(connID)), err)

	_, stillActive := pool.Get(connID)
	assert.False(t, stillActive, "dead reserved connection must be released, not left dangling")
}

// TestReservedConnError_NonConnectionErrorIsWrappedNotReleased verifies
// that reservedConnError only treats connection-level failures as a
// signal to release the reservation. An ordinary (non-connection) error, such
// as a syntax error, must be wrapped with the given context and must leave
// the reservation intact for the client to keep using.
func TestReservedConnError_NonConnectionErrorIsWrappedNotReleased(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutor(t)
	connID := rconn.ConnID()

	err := e.reservedConnError(rconn, "failed to ensure prepared statement", errors.New("syntax error"))

	require.EqualError(t, err, "failed to ensure prepared statement: syntax error")

	_, stillActive := pool.Get(connID)
	assert.True(t, stillActive, "a non-connection error must not release the reservation")
}

// TestExecuteQueryReservedConnDeadSocket_SettingsApplyError covers the gap where
// applyReservedSessionSettingsIfNeeded's failure was never checked for
// IsConnectionError anywhere in the file: a dead backend socket was wrapped into an
// opaque error while the reservation was reported as still alive.
func TestExecuteQueryReservedConnDeadSocket_SettingsApplyError(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutorApplySettings(t)
	connID := rconn.ConnID()

	rconn.Conn().RawConn().ForceClose()

	options := &query.ExecuteOptions{
		ReservedConnectionId: uint64(connID),
		SessionSettings:      map[string]string{"search_path": "foo"},
	}

	result, state, err := e.ExecuteQuery(context.Background(), &query.Target{}, "SELECT 1", options)

	require.Nil(t, result)
	require.Nil(t, state)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "failed to apply session settings",
		"must not leak the raw wrap/connection error")
	assert.Equal(t, mterrors.NewReservedConnectionTerminated(uint64(connID)), err)

	_, stillActive := pool.Get(connID)
	assert.False(t, stillActive, "dead reserved connection must be released, not left dangling")
}

// TestExecuteQueryReservedConnDeadSocket_QueryError covers reservedConn.Query's error
// path, which previously never checked IsConnectionError and never released — a dead
// socket was reported back to the gateway as a live connection.
func TestExecuteQueryReservedConnDeadSocket_QueryError(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutor(t)
	connID := rconn.ConnID()
	options := &query.ExecuteOptions{ReservedConnectionId: uint64(connID)}

	rconn.Conn().RawConn().ForceClose()

	result, state, err := e.ExecuteQuery(context.Background(), &query.Target{}, "SELECT 1", options)

	require.Nil(t, result)
	require.Nil(t, state)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "query execution failed",
		"must not leak the raw wrap/connection error")
	assert.Equal(t, mterrors.NewReservedConnectionTerminated(uint64(connID)), err)

	_, stillActive := pool.Get(connID)
	assert.False(t, stillActive, "dead reserved connection must be released, not left dangling")
}

// TestStreamExecuteReservedConnDeadSocket_SettingsApplyError mirrors
// TestExecuteQueryReservedConnDeadSocket_SettingsApplyError for the StreamExecute path.
func TestStreamExecuteReservedConnDeadSocket_SettingsApplyError(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutorApplySettings(t)
	connID := rconn.ConnID()

	rconn.Conn().RawConn().ForceClose()

	options := &query.ExecuteOptions{
		ReservedConnectionId: uint64(connID),
		SessionSettings:      map[string]string{"search_path": "foo"},
	}

	state, err := e.StreamExecute(context.Background(), &query.Target{}, "SELECT 1", options, nil, noopCallback)

	require.Nil(t, state)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "failed to prepare reserved connection",
		"must not leak the raw wrap/connection error")
	assert.Equal(t, mterrors.NewReservedConnectionTerminated(uint64(connID)), err)

	_, stillActive := pool.Get(connID)
	assert.False(t, stillActive, "dead reserved connection must be released, not left dangling")
}

// TestStreamExecuteReservedConnDeadSocket_MaterializeError covers the SQL EXECUTE
// prepared-statement materialization path, which internally issues a Parse (via
// ensurePrepared) — the first write on a dead socket.
func TestStreamExecuteReservedConnDeadSocket_MaterializeError(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutor(t)
	connID := rconn.ConnID()

	rconn.Conn().RawConn().ForceClose()

	options := &query.ExecuteOptions{
		ReservedConnectionId: uint64(connID),
		ExecuteSqlPreparedStatement: &query.ExecuteSqlPreparedStatement{
			PreparedStatement: &query.PreparedStatement{Name: "stmt0", Query: "SELECT $1", ParamTypes: []uint32{23}},
			SqlPrefix:         "EXECUTE ",
			SqlSuffix:         " ( 1 )",
		},
	}

	state, err := e.StreamExecute(context.Background(), &query.Target{}, "", options, nil, noopCallback)

	require.Nil(t, state)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "failed to materialize SQL EXECUTE prepared statement on reserved connection",
		"must not leak the raw wrap/connection error")
	assert.Equal(t, mterrors.NewReservedConnectionTerminated(uint64(connID)), err)

	_, stillActive := pool.Get(connID)
	assert.False(t, stillActive, "dead reserved connection must be released, not left dangling")
}

// TestStreamExecuteReservedConnDeadSocket_QueryStreamingError covers
// streamExecuteOnReservedConn's rc.QueryStreaming error path, which previously released
// only when portal-pin rollback happened to drain the last reservation reason on the
// connection — a dead socket with no pinned portals fell through to "still alive".
func TestStreamExecuteReservedConnDeadSocket_QueryStreamingError(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutor(t)
	connID := rconn.ConnID()
	options := &query.ExecuteOptions{ReservedConnectionId: uint64(connID)}

	rconn.Conn().RawConn().ForceClose()

	state, err := e.StreamExecute(context.Background(), &query.Target{}, "SELECT 1", options, nil, noopCallback)

	require.Nil(t, state)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "query execution failed",
		"must not leak the raw wrap/connection error")
	assert.Equal(t, mterrors.NewReservedConnectionTerminated(uint64(connID)), err)

	_, stillActive := pool.Get(connID)
	assert.False(t, stillActive, "dead reserved connection must be released, not left dangling")
}

// TestPortalStreamExecute_ExistingReservationStatementErrorKeepsConnection is
// the regression test for the reserved connection being destroyed on a plain
// SQL error (e.g. division_by_zero, an RLS WITH CHECK denial). Such an error
// only aborts the transaction — PostgreSQL keeps the backend alive — so a
// session-owned reservation must survive the failed portal and stay usable for
// ROLLBACK [TO SAVEPOINT].
func TestPortalStreamExecute_ExistingReservationStatementErrorKeepsConnection(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("select 1/0", mterrors.NewPgError("ERROR", "22012", "division by zero", ""))

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()
	rconn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true}, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	state, err := e.PortalStreamExecute(ctx, &query.Target{},
		&query.PreparedStatement{Name: "stmt0", Query: "SELECT 1/0"},
		&query.Portal{Name: "p0"},
		&query.ExecuteOptions{User: "postgres", ReservedConnectionId: uint64(rconn.ConnID())},
		nil, nil, noopCallback)

	require.Error(t, err)
	require.NotNil(t, state, "gateway must keep tracking the session-owned reservation")
	assert.Equal(t, uint64(rconn.ConnID()), state.GetReservedConnectionId())
	assert.False(t, rconn.IsReleased(), "a plain statement error must not destroy the reserved connection")
}

// TestPortalStreamExecute_ExistingReservationConnectionErrorReleases verifies
// that a genuine connection failure (unlike a plain statement error) still
// destroys the reserved connection, since the backend is actually gone.
func TestPortalStreamExecute_ExistingReservationConnectionErrorReleases(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("select 1", mterrors.NewPgError("FATAL", "57P01", "terminating connection due to administrator command", ""))

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()
	rconn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true}, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	state, err := e.PortalStreamExecute(ctx, &query.Target{},
		&query.PreparedStatement{Name: "stmt0", Query: "SELECT 1"},
		&query.Portal{Name: "p0"},
		&query.ExecuteOptions{User: "postgres", ReservedConnectionId: uint64(rconn.ConnID())},
		nil, nil, noopCallback)

	require.Error(t, err)
	require.Nil(t, state)
	assert.True(t, rconn.IsReleased(), "a genuine connection failure must still destroy the reserved connection")
}
