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
	"fmt"
	"io"
	"log/slog"
	"sync"
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
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/admin"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
)

func TestPreExecutionUnavailableError(t *testing.T) {
	t.Run("connection failure becomes retryable pre-execution error", func(t *testing.T) {
		err := preExecutionUnavailableError(fmt.Errorf("acquire backend: %w", io.EOF))
		assert.True(t, mterrors.IsPreExecutionUnavailable(err))

		var diagnostic *mterrors.PgDiagnostic
		require.ErrorAs(t, err, &diagnostic)
		assert.Equal(t, mterrors.PgSSCannotConnectNow, diagnostic.Code)
	})

	t.Run("ordinary acquisition error is unchanged", func(t *testing.T) {
		original := errors.New("pool exhausted")
		assert.Same(t, original, preExecutionUnavailableError(original))
	})
}

func TestCopyReadyAcquisitionErrorsArePreExecutionUnavailable(t *testing.T) {
	tests := []struct {
		name string
		call func(*Executor) error
	}{
		{
			name: "COPY FROM",
			call: func(e *Executor) error {
				_, _, _, err := e.CopyReady(t.Context(), &query.Target{}, "COPY t FROM STDIN", nil, nil)
				return err
			},
		},
		{
			name: "COPY TO",
			call: func(e *Executor) error {
				_, _, _, _, err := e.CopyOutReady(t.Context(), &query.Target{}, "COPY t TO STDOUT", nil, nil)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewExecutor(slog.Default(), &stubPoolManager{newReservedErr: io.EOF}, &clustermetadatapb.ID{}, false)
			err := tt.call(e)
			assert.True(t, mterrors.IsPreExecutionUnavailable(err))

			var diagnostic *mterrors.PgDiagnostic
			require.ErrorAs(t, err, &diagnostic)
			assert.Equal(t, mterrors.PgSSCannotConnectNow, diagnostic.Code)
		})
	}
}

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
	tempTainted     bool
	openHoldCursors map[string]bool

	resetCalls int
	resetErr   error
}

func (m *mockReservedConn) ConnID() int64            { return m.connID }
func (m *mockReservedConn) ProcessID() uint32        { return 0 }
func (m *mockReservedConn) RemainingReasons() uint32 { return m.remainingReasons }
func (m *mockReservedConn) IsInTransaction() bool    { return m.inTxn }
func (m *mockReservedConn) Conn() *regular.Conn      { return nil }

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

func (m *mockReservedConn) ResetAllSettings(_ context.Context) error {
	m.resetCalls++
	return m.resetErr
}

func (m *mockReservedConn) MarkTempTainted() {
	m.tempTainted = true
}

// Compile-time check.
var _ reservedConnAPI = (*mockReservedConn)(nil)

type stubPoolManager struct {
	reservedConn     *reserved.Conn
	reservedConnOK   bool
	regularConn      regular.PooledConn
	regularErr       error
	newReservedConn  *reserved.Conn
	newReservedPool  *reserved.Pool
	newReservedErr   error
	reservedPool     *reserved.Pool
	settingsCache    *connstate.SettingsCache
	adminConnFactory func(context.Context) (admin.PooledConn, error)
	adminErr         error
}

func (m *stubPoolManager) Open(context.Context, *connpoolmanager.ConnectionConfig) {}
func (m *stubPoolManager) Close()                                                  {}
func (m *stubPoolManager) CloseForReopen()                                         {}
func (m *stubPoolManager) PgUser() string                                          { return "postgres" }
func (m *stubPoolManager) PgDatabase() string                                      { return "postgres" }
func (m *stubPoolManager) PgPassword() (string, bool)                              { return "", true }
func (m *stubPoolManager) GetAdminConn(ctx context.Context) (admin.PooledConn, error) {
	if m.adminErr != nil {
		return nil, m.adminErr
	}
	if m.adminConnFactory != nil {
		return m.adminConnFactory(ctx)
	}
	return nil, nil
}

func (m *stubPoolManager) GetRegularConn(context.Context, string, []byte, []byte) (regular.PooledConn, error) {
	return nil, nil
}

func (m *stubPoolManager) GetRegularConnWithSettings(context.Context, map[string]string, string, []byte, []byte) (regular.PooledConn, error) {
	if m.regularErr != nil {
		return nil, m.regularErr
	}
	return m.regularConn, nil
}

func (m *stubPoolManager) NewReservedConn(ctx context.Context, settings map[string]string, _ string, _, _ []byte, opts ...reserved.ReservedConnOption) (*reserved.Conn, error) {
	if m.newReservedErr != nil {
		return nil, m.newReservedErr
	}
	if m.newReservedConn != nil {
		return m.newReservedConn, nil
	}
	pool := m.newReservedPool
	if pool == nil {
		pool = m.reservedPool
	}
	if pool == nil {
		return nil, errors.New("not implemented in test stub")
	}
	var cached *connstate.Settings
	if len(settings) > 0 {
		if m.settingsCache == nil {
			m.settingsCache = connstate.NewSettingsCache(16)
		}
		cached = m.settingsCache.GetOrCreate(settings)
	}
	return pool.NewConn(ctx, cached, opts...)
}

func (m *stubPoolManager) NewLogicalReplicationConn(context.Context, string, []byte, []byte) (*reserved.Conn, error) {
	return nil, errors.New("not implemented in test stub")
}

func (m *stubPoolManager) GetReservedConn(int64, string) (*reserved.Conn, bool) {
	return m.reservedConn, m.reservedConnOK
}

func (m *stubPoolManager) WaitForDrain(context.Context) error           { return nil }
func (m *stubPoolManager) WaitForReservedDrain(context.Context) error   { return nil }
func (m *stubPoolManager) CloseReservedConnections(context.Context) int { return 0 }
func (m *stubPoolManager) Stats() connpoolmanager.ManagerStats          { return connpoolmanager.ManagerStats{} }
func (m *stubPoolManager) CredentialQueryRecorder() connpoolmanager.CredentialQueryRecorder {
	return nil
}

var _ connpoolmanager.PoolManager = (*stubPoolManager)(nil)

func newAdminConnFactory(t *testing.T, server *fakepgserver.Server) func(context.Context) (admin.PooledConn, error) {
	t.Helper()
	return func(ctx context.Context) (admin.PooledConn, error) {
		clientConn, err := client.Connect(ctx, ctx, server.ClientConfig())
		if err != nil {
			return nil, err
		}
		return &connpool.Pooled[*admin.Conn]{Conn: admin.NewConn(clientConn)}, nil
	}
}

func newVpidTrackingExecutor(t *testing.T, server *fakepgserver.Server) *Executor {
	e := &Executor{
		logger:                     slog.Default(),
		poolManager:                &stubPoolManager{adminConnFactory: newAdminConnFactory(t, server)},
		backendVpidTrackingEnabled: true,
	}
	e.SetBackendVpidTrackingWritable(true)
	return e
}

// newTestExecutor returns an Executor that has just enough wiring to exercise
// reserved-connection execution helpers.
func newTestExecutor() *Executor {
	return &Executor{
		logger:   slog.Default(),
		poolerID: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		metrics:  newQueryStats(),
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

func TestReserveAndStreamExecute_TempReservationRetriesStaleSocket(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.OrderMatters()

	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT warmup",
		QueryResult: fakepgserver.MakeResult([]string{"?column?"}, [][]any{{1}}),
	})
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query: "SELECT 1",
		Error: mterrors.NewIdleSessionTimeout(),
	})
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT 1",
		QueryResult: fakepgserver.MakeResult([]string{"?column?"}, [][]any{{1}}),
	})
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "CREATE TEMP TABLE t(i int)",
		QueryResult: &sqltypes.Result{CommandTag: "CREATE TABLE"},
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

	warm, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)
	_, err = warm.Query(context.Background(), "SELECT warmup")
	require.NoError(t, err)
	warm.Release(reserved.ReleaseCommit, nil)

	e := newTestExecutor()
	e.metrics = newQueryStats()
	e.poolManager = &stubPoolManager{newReservedPool: pool}

	state, err := e.reserveAndStreamExecute(
		context.Background(),
		"CREATE TEMP TABLE t(i int)",
		&query.ExecuteOptions{User: "postgres"},
		&query.ReservationOptions{Reasons: protoutil.ReasonTempTable},
		noopCallback,
	)

	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, protoutil.ReasonTempTable, state.GetReservationReasons())
	server.VerifyAllExecutedOrFail()
}

// TestStreamExecuteOnReservedConn_TempTaintOnlyOnSuccess pins the taint
// semantics: a temp-reason statement taints the backend only when PostgreSQL
// accepts it. A rejected statement is unwound (reason removed, no taint) so
// the backend stays reusable — it created nothing.
func TestStreamExecuteOnReservedConn_TempTaintOnlyOnSuccess(t *testing.T) {
	e := newTestExecutor()
	tempOpts := &query.ReservationOptions{Reasons: protoutil.ReasonTempTable}

	// Success with temp reason: tainted.
	rc := &mockReservedConn{connID: 1, inTxn: true}
	_, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "CREATE TEMP TABLE t (i int)", tempOpts, nil, noopCallback)
	require.NoError(t, err)
	assert.True(t, rc.tempTainted, "successful temp statement must taint the backend")

	// PostgreSQL-level failure with temp reason: reason unwound, no taint.
	rc = &mockReservedConn{
		connID: 2, inTxn: true, remainingReasons: protoutil.ReasonTransaction,
		streamingErr: errors.New(`ERROR: syntax error`),
	}
	_, err = e.streamExecuteOnReservedConn(
		context.Background(), rc, "CREATE TEMP TABLE t (i int", tempOpts, nil, noopCallback)
	require.Error(t, err)
	assert.False(t, rc.tempTainted, "rejected temp statement must not taint the backend")
	assert.NotZero(t, rc.removedReasons&protoutil.ReasonTempTable, "statement-local temp reason must be unwound")

	// Success without temp reason: untouched.
	rc = &mockReservedConn{connID: 3, inTxn: true, remainingReasons: protoutil.ReasonTransaction}
	_, err = e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1", &query.ReservationOptions{}, nil, noopCallback)
	require.NoError(t, err)
	assert.False(t, rc.tempTainted)
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
	require.Zero(t, rc.resetCalls,
		"an existing reservation being unpinned is released with its truthful settings, not reset")
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

func TestStreamExecuteOnReservedConn_FailedTempTablePromotionRollsBackNewReason(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		remainingReasons: protoutil.ReasonTransaction,
		streamingErr:     errors.New("backend rejected CREATE TEMP TABLE"),
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "CREATE TEMP TABLE bad (id missing_type)",
		&query.ReservationOptions{Reasons: protoutil.ReasonTempTable},
		nil,
		noopCallback,
	)

	require.Error(t, err)
	require.Equal(t, protoutil.ReasonTempTable, rc.addedReasons,
		"temp-table reason is installed before the query so the bitmask is consistent while it runs")
	require.Equal(t, protoutil.ReasonTempTable, rc.removedReasons,
		"failed statement must unwind the temp-table reason it just added")
	require.Equal(t, protoutil.ReasonTransaction, rc.remainingReasons,
		"surviving transaction reservation must be preserved after a PostgreSQL statement error")
	require.Empty(t, rc.releaseCalls, "connection must stay reserved while the transaction reason persists")
	require.NotNil(t, state)
	require.Equal(t, protoutil.ReasonTransaction, state.GetReservationReasons())
}

func TestStreamExecuteOnReservedConn_FailedTempTablePromotionPreservesExistingReason(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		remainingReasons: protoutil.ReasonTransaction | protoutil.ReasonTempTable,
		streamingErr:     errors.New("backend rejected CREATE TEMP TABLE"),
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "CREATE TEMP TABLE bad (id missing_type)",
		&query.ReservationOptions{Reasons: protoutil.ReasonTempTable},
		nil,
		noopCallback,
	)

	require.Error(t, err)
	require.Equal(t, protoutil.ReasonTempTable, rc.addedReasons)
	require.Zero(t, rc.removedReasons,
		"failed statement must not remove a temp-table reason that existed before this query")
	require.Equal(t, protoutil.ReasonTransaction|protoutil.ReasonTempTable, rc.remainingReasons)
	require.Empty(t, rc.releaseCalls)
	require.NotNil(t, state)
	require.Equal(t, protoutil.ReasonTransaction|protoutil.ReasonTempTable, state.GetReservationReasons())
}

func TestStreamExecuteOnReservedConn_ConnectionErrorReleasesReservedConn(t *testing.T) {
	rc := &mockReservedConn{
		connID:           42,
		inTxn:            true,
		remainingReasons: protoutil.ReasonTransaction,
		streamingErr:     io.EOF,
	}
	e := newTestExecutor()

	state, err := e.streamExecuteOnReservedConn(
		context.Background(), rc, "SELECT 1",
		&query.ReservationOptions{},
		nil,
		noopCallback,
	)

	require.Error(t, err)
	require.Nil(t, state)
	require.Equal(t, []reserved.ReleaseReason{reserved.ReleaseError}, rc.releaseCalls,
		"connection-level errors must taint/release the reserved backend")
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
func TestReserveAndStreamExecute_FirstStatementErrorUnwindsStatementLocalReasons(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	const badDeclare = "DECLARE bad CURSOR WITH HOLD FOR SELECT * FROM missing_table"
	server.AddRejectedQuery(badDeclare, errors.New("ERROR: relation \"missing_table\" does not exist"))

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

	e := newTestExecutor()
	e.poolManager = &stubPoolManager{reservedPool: pool}

	state, err := e.reserveAndStreamExecute(
		context.Background(),
		badDeclare,
		&query.ExecuteOptions{User: "postgres"},
		&query.ReservationOptions{
			Reasons:        protoutil.ReasonTransaction | protoutil.StatementLocalReasons,
			BeginQuery:     "BEGIN",
			PinPortalNames: []string{"bad"},
		},
		noopCallback,
	)

	require.Error(t, err)
	require.NotNil(t, state, "failed first statement should preserve the transaction reservation")
	assert.Equal(t, protoutil.ReasonTransaction, state.GetReservationReasons(),
		"every statement-local reason must be unwound before returning surviving state")

	rconn, ok := pool.Get(int64(state.GetReservedConnectionId()))
	require.True(t, ok, "surviving transaction should still be in the reserved pool")
	assert.Equal(t, protoutil.ReasonTransaction, rconn.RemainingReasons())
	assert.False(t, rconn.HasPortal("bad"), "failed DECLARE must not leave a phantom portal pin")
}

// TestPortalReservedError_UnwindsAddedStatementLocalReasons verifies the
// portal-path counterpart of the statement-local unwind: a clean SQL error on
// Bind/Execute removes the statement-local reasons the portal newly added
// (the statement aborted atomically, so they never materialized), leaving
// only the surviving transaction reservation. Before this, the portal path
// unwound nothing, so a reason added by the failed Bind/Execute outlived it
// and pinned an otherwise healthy backend until the inactivity timeout.
func TestPortalReservedError_UnwindsAddedStatementLocalReasons(t *testing.T) {
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

	e := newTestExecutor()

	t.Run("transaction survives, added reasons unwind", func(t *testing.T) {
		rconn, err := pool.NewConn(context.Background(), nil)
		require.NoError(t, err)
		rconn.AddReservationReason(protoutil.ReasonTransaction | protoutil.ReasonTempTable)

		state, rerr := e.portalReservedError(rconn, "p1", &query.ExecuteOptions{}, false,
			protoutil.ReasonTempTable,
			errors.New("ERROR: division by zero"))
		require.Error(t, rerr)
		require.NotNil(t, state, "transaction reservation must survive a clean SQL error")
		assert.Equal(t, protoutil.ReasonTransaction, rconn.RemainingReasons(),
			"portal-added statement-local reasons must be unwound")
		rconn.Release(reserved.ReleaseRollback, nil)
	})

	t.Run("sole added statement-local reason drains and releases", func(t *testing.T) {
		rconn, err := pool.NewConn(context.Background(), nil)
		require.NoError(t, err)
		rconn.AddReservationReason(protoutil.ReasonTempTable)

		state, rerr := e.portalReservedError(rconn, "p1", &query.ExecuteOptions{}, true,
			protoutil.ReasonTempTable,
			errors.New("ERROR: invalid value for parameter"))
		require.Error(t, rerr)
		assert.Nil(t, state, "a drained reservation must release the backend and return a zero state")
	})
}

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

// --- sessionSettingsFromOptions tests ---

func TestSessionSettingsFromOptions_NilOptions(t *testing.T) {
	e := &Executor{}
	require.Nil(t, e.sessionSettingsFromOptions(nil))
}

// --- trackVpid* early-return tests ---
//
// The happy-path upsert is covered below with a fakepgserver. Here we lock in
// the guard semantics: the helpers must be safe no-ops when tracking is
// disabled, options is nil, or ClientConnectionId is zero. A nil conn is
// intentionally passed to prove the helpers return before touching it.

func TestTrackVpidOnReserved_NoOpGuards(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		options *query.ExecuteOptions
		enabled bool
	}{
		{"tracking disabled", &query.ExecuteOptions{ClientConnectionId: 1}, false},
		{"nil options", nil, true},
		{"zero id", &query.ExecuteOptions{ClientConnectionId: 0}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &Executor{backendVpidTrackingEnabled: tc.enabled}
			// nil conn would panic on Query — guard must short-circuit first.
			e.trackVpidOnReserved(ctx, nil, tc.options)
		})
	}
}

func TestTrackVpidOnRegular_NoOpGuards(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		options *query.ExecuteOptions
		enabled bool
	}{
		{"tracking disabled", &query.ExecuteOptions{ClientConnectionId: 1}, false},
		{"nil options", nil, true},
		{"zero id", &query.ExecuteOptions{ClientConnectionId: 0}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &Executor{backendVpidTrackingEnabled: tc.enabled}
			e.trackVpidOnRegular(ctx, nil, tc.options)
		})
	}
}

func TestReservedConnOptionsGatesVpidCleanup(t *testing.T) {
	validate := reserved.WithValidate(func(context.Context, *regular.Conn) error { return nil })

	disabled := &Executor{}
	assert.Empty(t, disabled.reservedConnOptions())
	assert.Len(t, disabled.reservedConnOptions(validate), 1)

	enabled := &Executor{backendVpidTrackingEnabled: true}
	assert.Len(t, enabled.reservedConnOptions(), 1)
	assert.Len(t, enabled.reservedConnOptions(validate), 2)
}

// --- trackVpid* happy-path tests ---
//
// These wire a real *regular.Conn / *reserved.Conn against a fakepgserver and
// verify that the helper upserts the (backend_pid → vpid) row,
// skips the upsert when the connection already tracks the same vpid, and
// clears the row at recycle/release.

func TestTrackVpidOnRegular_HappyPath(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	ctx := context.Background()
	clientConn, err := client.Connect(ctx, ctx, server.ClientConfig())
	require.NoError(t, err)
	conn := regular.NewConn(clientConn, nil)
	defer conn.Close()

	e := newVpidTrackingExecutor(t, server)
	server.ResetQueryLog()
	e.trackVpidOnRegular(ctx, conn, &query.ExecuteOptions{ClientConnectionId: 99})

	log := server.QueryLog()
	assert.NotContains(t, log, "create unlogged table", "tracking must not run DDL on the query path")
	assert.NotContains(t, log, "pg_backend_pid()", "tracking writes must not require client-side DML")
	assert.Contains(t, log, "values ($1::int4, $2::int8)")

	// Same vpid again: the per-conn cache skips the redundant upsert.
	server.ResetQueryLog()
	e.trackVpidOnRegular(ctx, conn, &query.ExecuteOptions{ClientConnectionId: 99})
	assert.Empty(t, server.QueryLog(), "re-tracking the same vpid must be a no-op")

	// Cleanup deletes this backend's row and resets the per-conn cache so a
	// later hand-off to the same vpid records a fresh association.
	server.ResetQueryLog()
	require.True(t, e.clearVpidOnRegular(ctx, conn))
	assert.Contains(t, server.QueryLog(), "delete from multigres.backend_vpid where backend_pid = $1::int4")
	assert.Zero(t, conn.State().TrackedVpid())

	server.ResetQueryLog()
	e.trackVpidOnRegular(ctx, conn, &query.ExecuteOptions{ClientConnectionId: 99})
	assert.Contains(t, server.QueryLog(), "values ($1::int4, $2::int8)", "same vpid after cleanup must upsert again")

	// A different vpid re-upserts.
	server.ResetQueryLog()
	e.trackVpidOnRegular(ctx, conn, &query.ExecuteOptions{ClientConnectionId: 100})
	log = server.QueryLog()
	assert.NotContains(t, log, "create unlogged table")
	assert.NotContains(t, log, "pg_backend_pid()")
	assert.Contains(t, log, "values ($1::int4, $2::int8)")
}

func TestTrackVpidOnRegular_UsesAdminPool(t *testing.T) {
	targetServer := fakepgserver.New(t)
	defer targetServer.Close()
	adminServer := fakepgserver.New(t)
	defer adminServer.Close()
	adminServer.SetNeverFail(true)

	ctx := context.Background()
	clientConn, err := client.Connect(ctx, ctx, targetServer.ClientConfig())
	require.NoError(t, err)
	conn := regular.NewConn(clientConn, nil)
	defer conn.Close()

	e := newVpidTrackingExecutor(t, adminServer)
	targetServer.ResetQueryLog()
	adminServer.ResetQueryLog()

	e.trackVpidOnRegular(ctx, conn, &query.ExecuteOptions{ClientConnectionId: 99})
	require.True(t, e.clearVpidOnRegular(ctx, conn))

	assert.Empty(t, targetServer.QueryLog(), "vpid tracking must not issue DML on the borrowed client backend")
	adminLog := adminServer.QueryLog()
	assert.Contains(t, adminLog, "insert into multigres.backend_vpid")
	assert.Contains(t, adminLog, "delete from multigres.backend_vpid")
}

func TestTrackVpidOnRegular_SkipsWhenPostgresNotWritable(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	ctx := context.Background()
	clientConn, err := client.Connect(ctx, ctx, server.ClientConfig())
	require.NoError(t, err)
	conn := regular.NewConn(clientConn, nil)
	defer conn.Close()

	e := &Executor{
		logger:                     slog.Default(),
		poolManager:                &stubPoolManager{adminErr: errors.New("admin pool should not be used")},
		backendVpidTrackingEnabled: true,
	}
	server.ResetQueryLog()

	e.trackVpidOnRegular(ctx, conn, &query.ExecuteOptions{ClientConnectionId: 99})
	assert.Empty(t, server.QueryLog(), "read replicas should skip backend_vpid upserts")
	assert.Zero(t, conn.State().TrackedVpid())

	conn.State().SetTrackedVpid(99)
	assert.False(t, e.clearVpidOnRegular(ctx, conn), "tracked backends should be closed if cleanup cannot run on a read replica")
	assert.Empty(t, server.QueryLog(), "read replicas should skip backend_vpid cleanup writes")
	assert.Zero(t, conn.State().TrackedVpid())
}

func TestTrackVpidOnReserved_HappyPath(t *testing.T) {
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
	e := newVpidTrackingExecutor(t, server)
	rconn, err := pool.NewConn(ctx, nil, reserved.WithReleaseCleanup(e.vpidReleaseCleanup()))
	require.NoError(t, err)
	defer rconn.Release(reserved.ReleaseCommit, nil)

	server.ResetQueryLog()
	e.trackVpidOnReserved(ctx, rconn, &query.ExecuteOptions{ClientConnectionId: 123})

	log := server.QueryLog()
	assert.NotContains(t, log, "create unlogged table", "tracking must not run DDL on the query path")
	assert.NotContains(t, log, "pg_backend_pid()", "tracking writes must not require client-side DML")
	assert.Contains(t, log, "values ($1::int4, $2::int8)")

	server.ResetQueryLog()
	rconn.Release(reserved.ReleaseCommit, nil)
	assert.Contains(t, server.QueryLog(), "delete from multigres.backend_vpid where backend_pid = $1::int4")
}

func TestReservedConnOptionsAttachVpidCleanup(t *testing.T) {
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
	e := newVpidTrackingExecutor(t, server)
	rconn, err := pool.NewConn(ctx, nil, e.reservedConnOptions()...)
	require.NoError(t, err)

	server.ResetQueryLog()
	e.trackVpidOnReserved(ctx, rconn, &query.ExecuteOptions{ClientConnectionId: 321})
	assert.Contains(t, server.QueryLog(), "values ($1::int4, $2::int8)")

	server.ResetQueryLog()
	rconn.Release(reserved.ReleaseCommit, nil)
	assert.Contains(t, server.QueryLog(), "delete from multigres.backend_vpid where backend_pid = $1::int4")
}

// TestTrackVpidOnRegular_BestEffortOnError verifies the failure path: when
// the upsert errors, the helper never surfaces an error to the query path and
// does not mark the connection as tracked.
func TestTrackVpidOnRegular_BestEffortOnError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	// neverFail not set: unmatched queries return errors.

	ctx := context.Background()
	clientConn, err := client.Connect(ctx, ctx, server.ClientConfig())
	require.NoError(t, err)
	conn := regular.NewConn(clientConn, nil)
	defer conn.Close()

	e := newVpidTrackingExecutor(t, server)
	server.ResetQueryLog()
	// Must not panic or block the caller even though every statement fails.
	e.trackVpidOnRegular(ctx, conn, &query.ExecuteOptions{ClientConnectionId: 7})

	log := server.QueryLog()
	assert.NotContains(t, log, "create unlogged table", "upsert failure must not trigger hot-path DDL")
	assert.NotContains(t, log, "pg_backend_pid()")
	assert.Contains(t, log, "values ($1::int4, $2::int8)")
	assert.Zero(t, conn.State().TrackedVpid())
}

// TestReleaseReservedConnection_KeepStickyReservations_SetSeedStaysReserved
// verifies that when only a sticky reason (ReasonSetSeed) remains,
// keepStickyReservations=true (the DISCARD ALL path) leaves the connection
// reserved instead of returning it to the pool. DISCARD ALL does not reset a
// seeded backend's PRNG (verified against a real backend: the random()
// sequence continues uninterrupted across it), so releasing the connection
// here would let another session inherit this one's seed while this session's
// own later random() calls could land on a fresh, unseeded backend.
func TestReleaseReservedConnection_KeepStickyReservations_SetSeedStaysReserved(t *testing.T) {
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

	rconn, err := pool.NewConn(ctx, cache.GetOrCreate(nil))
	require.NoError(t, err)
	rconn.AddReservationReason(protoutil.ReasonSetSeed)

	e := &Executor{
		logger:      slog.Default(),
		poolerID:    &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		poolManager: &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
	}

	reservedState, err := e.ReleaseReservedConnection(ctx, nil, &query.ExecuteOptions{
		ReservedConnectionId: uint64(rconn.ConnID()),
	}, true)
	require.NoError(t, err)

	require.NotNil(t, reservedState, "connection must stay reserved while a sticky reason remains")
	assert.Equal(t, uint64(rconn.ConnID()), reservedState.GetReservedConnectionId())
	assert.False(t, rconn.IsReleased(), "sticky reservation must not be returned to the pool")
	assert.True(t, protoutil.HasSetSeedReason(rconn.RemainingReasons()))
}

// TestReleaseReservedConnection_KeepStickyReservations_TempTableReasonClearedAfterDiscardTemp
// is a regression test (found in review of #1324): a connection reserved for
// both ReasonTempTable and ReasonSetSeed must have ReasonTempTable actually
// cleared once DISCARD TEMP runs, not just physically dropped on the backend.
// Before this fix, Step 3 ran DISCARD TEMP but never called
// RemoveReservationReason(ReasonTempTable) — harmless when every release
// unconditionally returned the connection to the pool, but once a sticky
// reason (ReasonSetSeed) can keep the connection reserved, the stale bit
// would incorrectly survive into the returned ReservedState, telling the
// gateway a DISCARD ALL'd connection still holds temp tables it no longer has.
func TestReleaseReservedConnection_KeepStickyReservations_TempTableReasonClearedAfterDiscardTemp(t *testing.T) {
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

	rconn, err := pool.NewConn(ctx, cache.GetOrCreate(nil))
	require.NoError(t, err)
	rconn.AddReservationReason(protoutil.ReasonTempTable)
	rconn.AddReservationReason(protoutil.ReasonSetSeed)

	e := &Executor{
		logger:      slog.Default(),
		poolerID:    &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		poolManager: &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
	}

	server.ResetQueryLog()
	reservedState, err := e.ReleaseReservedConnection(ctx, nil, &query.ExecuteOptions{
		ReservedConnectionId: uint64(rconn.ConnID()),
	}, true)
	require.NoError(t, err)

	require.NotNil(t, reservedState, "connection must stay reserved while ReasonSetSeed remains")
	assert.False(t, rconn.IsReleased())
	assert.Contains(t, server.QueryLog(), "discard temp", "DISCARD TEMP must actually run on the backend")
	assert.True(t, protoutil.HasSetSeedReason(reservedState.GetReservationReasons()))
	assert.False(t, protoutil.HasTempTableReason(reservedState.GetReservationReasons()),
		"ReasonTempTable must be cleared once DISCARD TEMP has run, not left stale on the surviving reservation")
}

// TestReleaseReservedConnection_RealDisconnectReleasesSetSeed verifies that
// keepStickyReservations=false (the real client-disconnect path) fully
// releases a connection even if a sticky reason (ReasonSetSeed) remains —
// sticky reasons only protect against DISCARD ALL, never against real
// teardown.
func TestReleaseReservedConnection_RealDisconnectReleasesSetSeed(t *testing.T) {
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

	rconn, err := pool.NewConn(ctx, cache.GetOrCreate(nil))
	require.NoError(t, err)
	rconn.AddReservationReason(protoutil.ReasonSetSeed)

	e := &Executor{
		logger:      slog.Default(),
		poolerID:    &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		poolManager: &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
	}

	reservedState, err := e.ReleaseReservedConnection(ctx, nil, &query.ExecuteOptions{
		ReservedConnectionId: uint64(rconn.ConnID()),
	}, false)
	require.NoError(t, err)

	assert.Nil(t, reservedState, "real disconnect must fully release regardless of sticky reasons")
	assert.True(t, rconn.IsReleased())
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

func TestStreamExecuteEagerParseRequiresReservation(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	clientConn, err := client.Connect(context.Background(), context.Background(), server.ClientConfig())
	require.NoError(t, err)
	e := NewExecutor(slog.Default(), &stubPoolManager{
		regularConn: &connpool.Pooled[*regular.Conn]{Conn: regular.NewConn(clientConn, nil)},
	}, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	_, err = e.StreamExecute(context.Background(), &query.Target{}, "", &query.ExecuteOptions{
		User: "postgres",
		ExecuteSqlPreparedStatement: &query.ExecuteSqlPreparedStatement{
			PreparedStatement: &query.PreparedStatement{Query: "SELECT 1"},
			ForceUnnamedParse: true,
		},
	}, nil, noopCallback)
	require.ErrorContains(t, err, "requires a reserved transaction")
}

func TestStreamExecuteEagerParseOnExistingReservation(t *testing.T) {
	e, _, rconn := newDeadReservedConnTestExecutor(t)
	state, err := e.StreamExecute(context.Background(), &query.Target{}, "", &query.ExecuteOptions{
		User:                 "postgres",
		ReservedConnectionId: uint64(rconn.ConnID()),
		ExecuteSqlPreparedStatement: &query.ExecuteSqlPreparedStatement{
			PreparedStatement: &query.PreparedStatement{Query: "SELECT $1", ParamTypes: []uint32{23}},
			ForceUnnamedParse: true,
		},
	}, &query.ReservationOptions{
		Reasons:    protoutil.ReasonTransaction,
		BeginQuery: "BEGIN ISOLATION LEVEL SERIALIZABLE",
	}, noopCallback)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.True(t, rconn.IsInTransaction())
	assert.Equal(t, protoutil.ReasonTransaction, state.GetReservationReasons())
}

func TestStreamExecuteEagerParseErrors(t *testing.T) {
	t.Run("begin", func(t *testing.T) {
		server := fakepgserver.New(t)
		defer server.Close()
		server.SetNeverFail(true)
		server.AddRejectedQuery("BEGIN", errors.New("begin failed"))
		pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
			InactivityTimeout: 5 * time.Second,
			RegularPoolConfig: &regular.PoolConfig{
				ClientConfig:   server.ClientConfig(),
				ConnPoolConfig: &connpool.Config{Capacity: 1, MaxIdleCount: 1},
			},
		})
		defer pool.Close()
		rconn, err := pool.NewConn(context.Background(), nil)
		require.NoError(t, err)
		e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true}, &clustermetadatapb.ID{}, false)

		state, err := e.StreamExecute(context.Background(), &query.Target{}, "", &query.ExecuteOptions{
			ReservedConnectionId: uint64(rconn.ConnID()),
			ExecuteSqlPreparedStatement: &query.ExecuteSqlPreparedStatement{
				PreparedStatement: &query.PreparedStatement{Query: "SELECT 1"},
				ForceUnnamedParse: true,
			},
		}, &query.ReservationOptions{Reasons: protoutil.ReasonTransaction}, noopCallback)
		require.ErrorContains(t, err, "failed to begin transaction")
		require.NotNil(t, state)
	})

	t.Run("parse connection", func(t *testing.T) {
		e, pool, rconn := newDeadReservedConnTestExecutor(t)
		connID := rconn.ConnID()
		rconn.Conn().RawConn().ForceClose()

		state, err := e.StreamExecute(context.Background(), &query.Target{}, "", &query.ExecuteOptions{
			ReservedConnectionId: uint64(connID),
			ExecuteSqlPreparedStatement: &query.ExecuteSqlPreparedStatement{
				PreparedStatement: &query.PreparedStatement{Query: "SELECT 1"},
				ForceUnnamedParse: true,
			},
		}, nil, noopCallback)
		require.Error(t, err)
		require.Nil(t, state)
		_, ok := pool.Get(connID)
		assert.False(t, ok)
	})

	t.Run("new reservation fatal parse", func(t *testing.T) {
		server := fakepgserver.New(t)
		defer server.Close()
		server.SetNeverFail(true)
		server.SetParseError(&mterrors.PgDiagnostic{Severity: "FATAL", Code: "XX000", Message: "fatal parse"})
		pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
			InactivityTimeout: 5 * time.Second,
			RegularPoolConfig: &regular.PoolConfig{
				ClientConfig:   server.ClientConfig(),
				ConnPoolConfig: &connpool.Config{Capacity: 1, MaxIdleCount: 1},
			},
		})
		defer pool.Close()
		rconn, err := pool.NewConn(context.Background(), nil)
		require.NoError(t, err)
		connID := rconn.ConnID()
		e := NewExecutor(slog.Default(), &stubPoolManager{newReservedConn: rconn}, &clustermetadatapb.ID{}, false)

		state, err := e.StreamExecute(context.Background(), &query.Target{}, "", &query.ExecuteOptions{
			ExecuteSqlPreparedStatement: &query.ExecuteSqlPreparedStatement{
				PreparedStatement: &query.PreparedStatement{Query: "SELECT 1"},
				ForceUnnamedParse: true,
			},
		}, &query.ReservationOptions{Reasons: protoutil.ReasonTransaction}, noopCallback)
		require.Error(t, err)
		require.Nil(t, state)
		_, ok := pool.Get(connID)
		assert.False(t, ok, "FATAL parse must release the dead reservation")
	})

	e := NewExecutor(slog.Default(), nil, &clustermetadatapb.ID{}, false)
	require.ErrorContains(t, e.forceUnnamedParse(context.Background(), nil, nil), "prepared statement is required")
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

	e := NewExecutor(logger, nil, poolerID, true)
	require.NotNil(t, e)
	assert.Equal(t, poolerID, e.poolerID)
	assert.NotNil(t, e.poolerConsolidator, "constructor must initialise the consolidator")
	assert.True(t, e.backendVpidTrackingEnabled)
	assert.False(t, e.backendVpidTrackingWritable.Load(), "writability is supplied by pooler state transitions")
	e.SetBackendVpidTrackingWritable(true)
	assert.True(t, e.backendVpidTrackingWritable.Load())
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
	require.Contains(t, err.Error(), "reserved connection terminated; please retry")
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
		nil,
	)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSSerializationFailure), "expected 40001, got: %v", err)
	assert.False(t, mterrors.IsErrorCode(err, mterrors.MTF01.ID), "must not surface MTF01: %v", err)
	require.Contains(t, err.Error(), "reserved connection terminated; please retry")
}

// TestConcludeTransaction_CommitFailsCleanlyKeepsSurvivingReason verifies that
// a COMMIT which fails with a clean PostgreSQL SQL-level error (e.g. a
// deferred constraint violation) removes only the transaction reason and
// does not release/taint the connection when another reason (e.g. a
// temp table) still holds it — regression test for the temp table being
// silently orphaned on a different backend after a failed COMMIT.
func TestConcludeTransaction_CommitFailsCleanlyKeepsSurvivingReason(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("COMMIT", mterrors.NewPgError("ERROR", "23503",
		`update or delete on table "p" violates foreign key constraint`, ""))

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

	require.NoError(t, rconn.Begin(ctx))
	rconn.AddReservationReason(protoutil.ReasonTempTable)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	_, state, err := e.ConcludeTransaction(
		ctx,
		protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		&query.ExecuteOptions{User: "postgres", ReservedConnectionId: uint64(rconn.ConnID())},
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil, false, false, nil,
	)

	require.Error(t, err)
	assert.False(t, mterrors.IsConnectionDead(err), "a deferred constraint violation is a clean SQL error, not a dead connection")
	require.NotNil(t, state, "the temp-table reservation must survive a failed COMMIT")
	assert.Equal(t, uint64(rconn.ConnID()), state.GetReservedConnectionId())
	assert.Equal(t, protoutil.ReasonTempTable, state.GetReservationReasons(), "only the transaction reason should be cleared")
	assert.False(t, rconn.IsReleased(), "a clean COMMIT failure must not release/taint a healthy connection")
}

// TestConcludeTransaction_CommitConnectionDeathReleases verifies that a
// genuine connection failure during COMMIT (unlike a clean SQL error) still
// releases/taints the connection, even when other reasons were set.
func TestConcludeTransaction_CommitConnectionDeathReleases(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("COMMIT", mterrors.NewPgError("FATAL", "57P01",
		"terminating connection due to administrator command", ""))

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

	require.NoError(t, rconn.Begin(ctx))
	rconn.AddReservationReason(protoutil.ReasonTempTable)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	_, state, err := e.ConcludeTransaction(
		ctx,
		protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		&query.ExecuteOptions{User: "postgres", ReservedConnectionId: uint64(rconn.ConnID())},
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil, false, false, nil,
	)

	require.Error(t, err)
	require.Nil(t, state)
	assert.True(t, rconn.IsReleased(), "a genuine connection failure must still destroy the reserved connection")
}

// TestConcludeTransaction_CommitFailsCleanlyReleasesWhenNoOtherReason verifies
// that a COMMIT which fails with a clean SQL-level error still releases the
// connection when no other reservation reason (e.g. temp table) remains —
// the surviving-reservation handling in concludeTransactionError must not
// leak a connection that has nothing left holding it reserved.
func TestConcludeTransaction_CommitFailsCleanlyReleasesWhenNoOtherReason(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("COMMIT", mterrors.NewPgError("ERROR", "23503",
		`update or delete on table "p" violates foreign key constraint`, ""))

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

	require.NoError(t, rconn.Begin(ctx))
	// No other reservation reason (e.g. temp table) is added, so once the
	// transaction reason clears, RemainingReasons() should be 0.

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	_, state, err := e.ConcludeTransaction(
		ctx,
		protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		&query.ExecuteOptions{User: "postgres", ReservedConnectionId: uint64(rconn.ConnID())},
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil, false, false, nil,
	)

	require.Error(t, err)
	assert.False(t, mterrors.IsConnectionDead(err), "a deferred constraint violation is a clean SQL error, not a dead connection")
	require.Nil(t, state, "no reason remains, so the connection should be released rather than reported as reserved")
	assert.True(t, rconn.IsReleased(), "with no surviving reason, the connection must be released even on a clean COMMIT failure")
}

// TestConcludeTransaction_CommitAndChainFailsCleanlyKeepsSurvivingReason is
// the chain=true counterpart of the plain-COMMIT regression test — it
// exercises CommitAndChainResult's error-handling path (rather than
// CommitResult's), which mirrors the same bookkeeping.
func TestConcludeTransaction_CommitAndChainFailsCleanlyKeepsSurvivingReason(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("COMMIT AND CHAIN", mterrors.NewPgError("ERROR", "23503",
		`update or delete on table "p" violates foreign key constraint`, ""))

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

	require.NoError(t, rconn.Begin(ctx))
	rconn.AddReservationReason(protoutil.ReasonTempTable)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	_, state, err := e.ConcludeTransaction(
		ctx,
		protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		&query.ExecuteOptions{User: "postgres", ReservedConnectionId: uint64(rconn.ConnID())},
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil, false, true, nil,
	)

	require.Error(t, err)
	assert.False(t, mterrors.IsConnectionDead(err), "a deferred constraint violation is a clean SQL error, not a dead connection")
	require.NotNil(t, state, "the temp-table reservation must survive a failed COMMIT AND CHAIN")
	assert.Equal(t, uint64(rconn.ConnID()), state.GetReservedConnectionId())
	assert.Equal(t, protoutil.ReasonTempTable, state.GetReservationReasons(), "only the transaction reason should be cleared")
	assert.False(t, rconn.IsReleased(), "a clean COMMIT AND CHAIN failure must not release/taint a healthy connection")
}

// TestConcludeTransaction_CommitContextCancelReleasesEvenWithSurvivingReason
// is a regression test for the indeterminate-error taint fix: a COMMIT cut
// off by context cancellation carries no proof PostgreSQL ever replied, so
// concludeTransactionError must release/taint the connection exactly like a
// confirmed-dead one — even when another reason (e.g. a temp table) is also
// set. Before the fix, only mterrors.IsConnectionDead(err) triggered
// release; a context-cancelled COMMIT is not "dead" by that check, so with
// a surviving reason present the connection would have been wrongly
// reported as still safely reserved.
func TestConcludeTransaction_CommitContextCancelReleasesEvenWithSurvivingReason(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	release := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	server.AddQueryPatternWithCallback(`^COMMIT$`, &sqltypes.Result{CommandTag: "COMMIT"},
		func(string) {
			startOnce.Do(func() { close(started) })
			<-release
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
	// Registered last so it runs first on unwind (LIFO): release the blocked
	// server callback before pool.Close()/server.Close() try to tear down
	// the connection, or those would deadlock waiting on it.
	defer close(release)

	ctx := context.Background()
	rconn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, rconn.Begin(ctx))
	rconn.AddReservationReason(protoutil.ReasonTempTable)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	commitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	type concludeResult struct {
		state *query.ReservedState
		err   error
	}
	done := make(chan concludeResult, 1)
	go func() {
		_, state, err := e.ConcludeTransaction(
			commitCtx,
			protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
			&query.ExecuteOptions{User: "postgres", ReservedConnectionId: uint64(rconn.ConnID())},
			multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
			nil, false, false, nil,
		)
		done <- concludeResult{state, err}
	}()

	<-started

	var result concludeResult
	select {
	case result = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConcludeTransaction did not return after the context deadline")
	}

	require.Error(t, result.err)
	assert.ErrorIs(t, result.err, context.DeadlineExceeded)
	require.Nil(t, result.state,
		"a context-cancelled COMMIT carries no proof PG replied — it must be treated like a dead connection, not a surviving reservation")
	assert.True(t, rconn.IsReleased(),
		"the indeterminate case must taint/release the connection even though a temp-table reason was also present")
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
		require.Contains(t, err.Error(), "reserved connection terminated; please retry")
	})
}

// TestStreamReplication_Unimplemented verifies that the executor's
// StreamReplication stub always returns UNIMPLEMENTED: replication is served
// by the pooler's dedicated gRPC service, never through this query path.
func TestStreamReplication_Unimplemented(t *testing.T) {
	e := newTestExecutor()

	stream, err := e.StreamReplication(context.Background(), &multipoolerpb.StreamReplicationInit{})

	require.Error(t, err)
	assert.Nil(t, stream)
	assert.Equal(t, mtrpcpb.Code_UNIMPLEMENTED, mterrors.Code(err))
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

	state, err := e.reservedConnError(rconn, "failed to ensure prepared statement", errors.New("syntax error"))

	require.EqualError(t, err, "failed to ensure prepared statement: syntax error")
	assert.NotNil(t, state, "a non-connection error must return the live reservation state")
	assert.Equal(t, uint64(connID), state.GetReservedConnectionId())

	_, stillActive := pool.Get(connID)
	assert.True(t, stillActive, "a non-connection error must not release the reservation")
}

func TestReservedConnError_NonRetryableFatalReturnsDiagnosticAndReleases(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutor(t)
	connID := rconn.ConnID()
	diag := &mterrors.PgDiagnostic{MessageType: 'E', Severity: "FATAL", Code: "53300", Message: "sorry, too many clients already"}

	state, err := e.reservedConnError(rconn, "query execution failed", diag)

	require.Nil(t, state)
	require.ErrorIs(t, err, diag)
	assert.False(t, mterrors.IsConnectionError(err))
	assert.True(t, mterrors.IsConnectionDead(err))

	_, stillActive := pool.Get(connID)
	assert.False(t, stillActive, "a FATAL diagnostic must release the reservation even when it is not retryable")
}

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

func TestCopySendDataReservedConnDeadSocket(t *testing.T) {
	e, pool, rconn := newDeadReservedConnTestExecutor(t)
	connID := rconn.ConnID()
	options := &query.ExecuteOptions{ReservedConnectionId: uint64(connID)}

	rconn.Conn().RawConn().ForceClose()

	err := e.CopySendData(context.Background(), &query.Target{}, []byte("1\t2\n"), options)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "failed to write COPY data",
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

// newConcludeStampFixture builds a reserved connection (in a transaction) on a
// fake server whose pool has a SettingsCache, so Release's label stamp is
// active and the stamped settings can be inspected after conclusion.
func newConcludeStampFixture(t *testing.T, rejectCommitWith error) (*Executor, *reserved.Conn) {
	t.Helper()
	server := fakepgserver.New(t)
	t.Cleanup(server.Close)
	server.SetNeverFail(true)
	if rejectCommitWith != nil {
		server.AddRejectedQuery("COMMIT", rejectCommitWith)
	}

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		SettingsCache:     connstate.NewSettingsCache(16),
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
	require.NoError(t, rconn.Begin(context.Background()))

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)
	return e, rconn
}

func concludeWithMaps(t *testing.T, e *Executor, rconn *reserved.Conn, conclusion multipoolerpb.TransactionConclusion, inTxn, rollback map[string]string) error {
	t.Helper()
	_, state, err := e.ConcludeTransaction(
		context.Background(),
		protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		&query.ExecuteOptions{User: "postgres", ReservedConnectionId: uint64(rconn.ConnID()), SessionSettings: inTxn},
		conclusion,
		nil, false, false,
		rollback,
	)
	require.Nil(t, state, "no reason should survive; the connection must be released")
	require.True(t, rconn.IsReleased())
	return err
}

// TestConcludeTransaction_CommitSuccessStampsInTxnSettings pins the
// outcome-conditional label: a successful COMMIT keeps the in-transaction
// settings, so the released backend is labelled with options.SessionSettings.
func TestConcludeTransaction_CommitSuccessStampsInTxnSettings(t *testing.T) {
	e, rconn := newConcludeStampFixture(t, nil)
	inTxn := map[string]string{"work_mem": "64MB"}
	rollback := map[string]string{"search_path": "public"}

	err := concludeWithMaps(t, e, rconn, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, inTxn, rollback)
	require.NoError(t, err)

	label := rconn.Conn().State().GetSettings()
	require.NotNil(t, label)
	assert.Equal(t, "64MB", label.Vars["work_mem"], "COMMIT success keeps the in-transaction map")
	_, hasRollbackOnly := label.Vars["search_path"]
	assert.False(t, hasRollbackOnly)
}

// TestConcludeTransaction_RollbackStampsRollbackSettings pins that a ROLLBACK
// conclusion labels the released backend with the pre-BEGIN snapshot the
// gateway sent, not the in-transaction map — PostgreSQL reverted the backend's
// session state, so the in-transaction values no longer exist there.
func TestConcludeTransaction_RollbackStampsRollbackSettings(t *testing.T) {
	e, rconn := newConcludeStampFixture(t, nil)
	inTxn := map[string]string{"work_mem": "64MB"}
	rollback := map[string]string{"search_path": "public"}

	err := concludeWithMaps(t, e, rconn, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK, inTxn, rollback)
	require.NoError(t, err)

	label := rconn.Conn().State().GetSettings()
	require.NotNil(t, label)
	assert.Equal(t, "public", label.Vars["search_path"])
	_, hasInTxn := label.Vars["work_mem"]
	assert.False(t, hasInTxn, "an aborted transaction's settings must not be stamped onto the released backend")
}

// TestConcludeTransaction_FailedCommitStampsRollbackSettings covers the
// commit-time-failure variant: PostgreSQL treats a COMMIT that fails cleanly
// (e.g. a deferred constraint violation) as a rollback, so the released
// backend must be labelled with the rollback snapshot even though the request
// asked for COMMIT and its options carry the in-transaction map.
func TestConcludeTransaction_FailedCommitStampsRollbackSettings(t *testing.T) {
	e, rconn := newConcludeStampFixture(t, mterrors.NewPgError("ERROR", "23503",
		`update or delete on table "p" violates foreign key constraint`, ""))
	inTxn := map[string]string{"work_mem": "64MB"}
	rollback := map[string]string{"search_path": "public"}

	err := concludeWithMaps(t, e, rconn, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, inTxn, rollback)
	require.Error(t, err, "the client must still see the COMMIT failure")

	label := rconn.Conn().State().GetSettings()
	require.NotNil(t, label)
	assert.Equal(t, "public", label.Vars["search_path"])
	_, hasInTxn := label.Vars["work_mem"]
	assert.False(t, hasInTxn, "a COMMIT concluded as rollback must not stamp the abandoned in-transaction settings")
}

// TestConcludeTransaction_RollbackWithoutRollbackSettingsTaints pins the
// strict contract: the gateway always sends rollback_session_settings, so its
// absence on a rollback outcome is an invariant violation — stamping the
// in-transaction map would label the backend with settings PostgreSQL just
// reverted. Fail closed: no stamp, connection replaced.
func TestConcludeTransaction_RollbackWithoutRollbackSettingsTaints(t *testing.T) {
	e, rconn := newConcludeStampFixture(t, nil)
	inTxn := map[string]string{"work_mem": "64MB"}

	err := concludeWithMaps(t, e, rconn, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK, inTxn, nil)
	require.NoError(t, err, "the client's ROLLBACK still succeeded")

	assert.Nil(t, rconn.Conn().State().GetSettings(),
		"no label may be stamped when the rollback map is missing")
}

// TestConcludeTransaction_FailedCommitWithoutRollbackSettingsTaints covers the
// same violation on the commit-failure-as-rollback path.
func TestConcludeTransaction_FailedCommitWithoutRollbackSettingsTaints(t *testing.T) {
	e, rconn := newConcludeStampFixture(t, mterrors.NewPgError("ERROR", "23503",
		`update or delete on table "p" violates foreign key constraint`, ""))
	inTxn := map[string]string{"work_mem": "64MB"}

	err := concludeWithMaps(t, e, rconn, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, inTxn, nil)
	require.Error(t, err, "the client must still see the COMMIT failure")

	assert.Nil(t, rconn.Conn().State().GetSettings(),
		"no label may be stamped when the rollback map is missing")
}

// TestStreamExecuteOnReservedConn_CleanStatementErrorKeepsBackend pins the
// no-reconnect-per-failing-statement contract: a statement that reserves only a
// statement-local reason (a temp table here) and then fails with a clean
// PostgreSQL error must release its backend for reuse — the failed statement
// aborted atomically, so the backend is unchanged since acquisition. Only
// indeterminate failures (cancellation, deadline, dead socket) may close it.
func TestStreamExecuteOnReservedConn_CleanStatementErrorKeepsBackend(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("CREATE TEMP TABLE t (id int)", mterrors.NewPgError("ERROR", "42P07",
		`relation "t" already exists`, ""))

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		SettingsCache:     connstate.NewSettingsCache(16),
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig:   server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{Capacity: 2, MaxIdleCount: 2},
		},
	})
	defer pool.Close()

	rconn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	// The reason arrives with the statement (statement-local).
	state, err := e.streamExecuteOnReservedConn(context.Background(), rconn,
		"CREATE TEMP TABLE t (id int)",
		&query.ReservationOptions{Reasons: protoutil.ReasonTempTable},
		map[string]string{"search_path": "public"},
		func(context.Context, *sqltypes.Result) error { return nil })

	require.Error(t, err, "the client must see the PostgreSQL error")
	assert.Nil(t, state, "the sole statement-local reason drained, so the connection was released")
	assert.False(t, rconn.Conn().IsClosed(),
		"a clean statement error must not close a healthy backend")
}

// TestReserveAndStreamExecute_CleanStatementErrorKeepsBackend covers the same
// contract on the reserve-and-run path, whose non-transaction failure branch
// is separate: a statement that reserved solely a statement-local reason and
// then failed cleanly must release its backend for reuse rather than closing
// it.
func TestReserveAndStreamExecute_CleanStatementErrorKeepsBackend(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("CREATE TEMP TABLE t (id int)", mterrors.NewPgError("ERROR", "42P07",
		`relation "t" already exists`, ""))

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		SettingsCache:     connstate.NewSettingsCache(16),
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig:   server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{Capacity: 2, MaxIdleCount: 2},
		},
	})
	defer pool.Close()

	rconn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true, newReservedConn: rconn},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	_, err = e.reserveAndStreamExecute(context.Background(),
		"CREATE TEMP TABLE t (id int)",
		&query.ExecuteOptions{User: "postgres"},
		&query.ReservationOptions{Reasons: protoutil.ReasonTempTable},
		func(context.Context, *sqltypes.Result) error { return nil })

	require.Error(t, err, "the client must see the PostgreSQL error")
	assert.False(t, rconn.Conn().IsClosed(),
		"a clean statement error must not close a healthy backend on the reserve-and-run path")
}

// TestReserveAndStreamExecute_CleanErrorKeepsNonTransactionalReasons pins the
// deliberate asymmetry in the clean-failure unwind: transactional reasons
// (a temp table here) unwind because the abort provably rolled their side
// effects back, while non-transactional reasons (session advisory locks)
// survive — SELECT pg_advisory_lock(1), 1/0 leaves the lock held on real
// PostgreSQL, so dropping the reason or closing the backend would destroy
// client-visible state. The reservation must be returned, not released.
func TestReserveAndStreamExecute_CleanErrorKeepsNonTransactionalReasons(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("SELECT pg_advisory_lock(1) INTO TEMP t",
		mterrors.NewPgError("ERROR", "42P07", `relation "t" already exists`, ""))

	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		SettingsCache:     connstate.NewSettingsCache(16),
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig:   server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{Capacity: 2, MaxIdleCount: 2},
		},
	})
	defer pool.Close()

	rconn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true, newReservedConn: rconn},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	state, err := e.reserveAndStreamExecute(context.Background(),
		"SELECT pg_advisory_lock(1) INTO TEMP t",
		&query.ExecuteOptions{User: "postgres"},
		&query.ReservationOptions{Reasons: protoutil.ReasonSessionAdvisoryLock | protoutil.ReasonTempTable},
		func(context.Context, *sqltypes.Result) error { return nil })

	require.Error(t, err, "the client must see the PostgreSQL error")
	require.NotNil(t, state, "the reservation must survive so the gateway records the pin")
	assert.Equal(t, protoutil.ReasonSessionAdvisoryLock, rconn.RemainingReasons(),
		"the transactional temp-table reason unwinds; the possibly-held advisory lock keeps the pin")
	assert.False(t, rconn.Conn().IsClosed(),
		"closing the backend would destroy a lock the client may legitimately hold")
}

// TestReleaseReservedConnection_StampsOptionsMapVerbatim pins the pooler side
// of the disconnect-release contract: this path rolls an open transaction
// back and then labels the released backend with EXACTLY the map the
// gateway's options carry — it has no rollback snapshot of its own. The
// gateway is therefore responsible for sending the post-rollback truth
// (the pre-BEGIN map) on a mid-transaction disconnect; a gateway that sent
// the in-transaction map here would produce a label describing state
// PostgreSQL just discarded.
func TestReleaseReservedConnection_StampsOptionsMapVerbatim(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	cache := connstate.NewSettingsCache(16)
	pool := reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		SettingsCache:     cache,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig:   server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{Capacity: 2, MaxIdleCount: 2},
		},
	})
	defer pool.Close()

	rconn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, rconn.BeginWithQuery(context.Background(), "BEGIN"))
	require.True(t, rconn.IsInTransaction())

	e := NewExecutor(slog.Default(), &stubPoolManager{reservedConn: rconn, reservedConnOK: true},
		&clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)

	preBegin := map[string]string{"work_mem": "7MB"}
	underlying := rconn.Conn()
	state, err := e.ReleaseReservedConnection(context.Background(), nil,
		&query.ExecuteOptions{User: "postgres", ReservedConnectionId: 1, SessionSettings: preBegin},
		false)
	require.NoError(t, err)
	require.Nil(t, state, "the reservation must be fully released")

	assert.False(t, underlying.IsClosed(), "clean disconnect release must recycle, not close")
	label := underlying.State().GetSettings()
	require.NotNil(t, label, "release must stamp the gateway's map as the label")
	assert.Same(t, cache.GetOrCreate(preBegin), label,
		"the label is the options map verbatim, interned for pointer-equality bucket hits")
}
