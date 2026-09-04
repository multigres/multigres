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

package executor

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/plancache"
	"github.com/multigres/multigres/go/services/multigateway/planner"
)

// mockExec is a minimal IExecute mock that records calls for verification.
type mockExec struct {
	streamExecuteCalls              atomic.Int32
	portalStreamExecuteCalls        atomic.Int32
	lastStreamExecuteSQL            atomic.Value // string
	lastExecuteSQLPreparedStatement atomic.Pointer[querypb.ExecuteSqlPreparedStatement]
	lastPortalStreamExecuteQS       atomic.Value // string

	// StreamReplication tracking
	streamReplicationTableGroup atomic.Value // string
	streamReplicationShard      atomic.Value // string
	streamReplicationInit       atomic.Pointer[multipoolerpb.StreamReplicationInit]
	streamReplicationStream     multipoolerpb.MultipoolerService_StreamReplicationClient
	streamReplicationErr        error
}

func (m *mockExec) StreamExecute(
	_ context.Context, _ *server.Conn, _, _ string, sql string,
	preparedStatement *querypb.ExecuteSqlPreparedStatement,
	_ *handler.MultigatewayConnectionState,
	_ engine.PlanExecInfo,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	m.streamExecuteCalls.Add(1)
	m.lastStreamExecuteSQL.Store(sql)
	m.lastExecuteSQLPreparedStatement.Store(preparedStatement)
	return callback(context.Background(), &sqltypes.Result{})
}

func (m *mockExec) PortalStreamExecute(
	_ context.Context, _, _ string, _ *server.Conn,
	_ *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo, _ int32, _ bool,
	_ engine.PlanExecInfo,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	m.portalStreamExecuteCalls.Add(1)
	m.lastPortalStreamExecuteQS.Store(portalInfo.PreparedStatementInfo.Query)
	return callback(context.Background(), &sqltypes.Result{})
}

func (m *mockExec) Describe(context.Context, string, string, *server.Conn, *handler.MultigatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*querypb.StatementDescription, error) {
	return nil, nil
}

func (m *mockExec) ConcludeTransaction(context.Context, *server.Conn, *handler.MultigatewayConnectionState, multipoolerpb.TransactionConclusion, []string, bool, bool, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *mockExec) DiscardTempTables(context.Context, *server.Conn, *handler.MultigatewayConnectionState, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *mockExec) ReleaseAllReservedConnections(context.Context, *server.Conn, *handler.MultigatewayConnectionState, bool) error {
	return nil
}

func (m *mockExec) CopyInitiate(context.Context, *server.Conn, string, string, string, *handler.MultigatewayConnectionState, func(context.Context, *sqltypes.Result) error) (int16, []int16, error) {
	return 0, nil, nil
}

func (m *mockExec) CopySendData(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, []byte) error {
	return nil
}

func (m *mockExec) CopyFinalize(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, []byte, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *mockExec) CopyAbort(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState) error {
	return nil
}

func (m *mockExec) CopyOutInitiate(context.Context, *server.Conn, string, string, string, *handler.MultigatewayConnectionState) (int16, []int16, []*mterrors.PgDiagnostic, error) {
	return 0, nil, nil, nil
}

func (m *mockExec) CopyOutStream(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, func(pgClient.CopyOutMessage) error) (*sqltypes.Result, error) {
	return nil, nil
}

func (m *mockExec) StreamReplication(_ context.Context, _ *server.Conn, tableGroup, shard string, _ *handler.MultigatewayConnectionState, init *multipoolerpb.StreamReplicationInit) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
	m.streamReplicationTableGroup.Store(tableGroup)
	m.streamReplicationShard.Store(shard)
	m.streamReplicationInit.Store(init)
	return m.streamReplicationStream, m.streamReplicationErr
}

func newTestExecutor(mock *mockExec) *Executor {
	logger := slog.Default()
	txnMetrics, _ := engine.NewTransactionMetrics()
	return &Executor{
		planner:   planner.NewPlanner(DefaultTableGroup, logger, txnMetrics),
		exec:      mock,
		logger:    logger,
		planCache: plancache.NewForTest(1024 * 1024), // 1MB, doorkeeper disabled
	}
}

func testConn() *server.Conn {
	return server.NewTestConn(&bytes.Buffer{}).Conn
}

func makeTestPlan() *engine.Plan {
	return engine.NewPlan("SELECT 1", engine.NewRoute(DefaultTableGroup, "0", "SELECT 1", nil))
}

func testConnWithDB(database string) *server.Conn {
	return server.NewTestConn(&bytes.Buffer{}, server.WithTestDatabase(database)).Conn
}

func noopCallback(_ context.Context, _ *sqltypes.Result) error {
	return nil
}

func parseOne(t *testing.T, sql string) ast.Stmt {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	return stmts[0]
}

func makePortalInfo(t *testing.T, sql string) *preparedstatement.PortalInfo {
	t.Helper()
	psi, err := preparedstatement.NewPreparedStatementInfo(&querypb.PreparedStatement{
		Name:  "",
		Query: sql,
	})
	require.NoError(t, err)
	return preparedstatement.NewPortalInfo(psi, &querypb.Portal{Name: ""})
}

func TestEagerParseInTransaction(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()

	require.NoError(t, exec.EagerParseInTransaction(context.Background(), testConn(), handler.NewMultigatewayConnectionState(), "SELECT $1", []uint32{23}))
	assert.Equal(t, int32(1), mock.streamExecuteCalls.Load())
	assert.Empty(t, mock.lastStreamExecuteSQL.Load())
	assert.True(t, mock.lastExecuteSQLPreparedStatement.Load().GetForceUnnamedParse())
}

// ---------- StreamExecute plan cache tests ----------

func TestStreamExecute_CacheHitOnRepeatedQuery(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	// First execution — cache miss
	res1, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM users WHERE id = 42", parseOne(t, "SELECT * FROM users WHERE id = 42"), noopCallback)
	require.NoError(t, err)
	assert.False(t, res1.CacheHit)

	// theine processes writes asynchronously
	time.Sleep(50 * time.Millisecond)

	// Second execution with different literal — same shape, should hit cache
	res2, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM users WHERE id = 99", parseOne(t, "SELECT * FROM users WHERE id = 99"), noopCallback)
	require.NoError(t, err)
	assert.True(t, res2.CacheHit, "second query with same shape should hit cache")

	// Both should have executed against the backend
	assert.Equal(t, int32(2), mock.streamExecuteCalls.Load())
}

// TestSetSlotBasedReplicationEnabled_ReloadInvalidatesStaleAdmission proves
// the exact hazard the review comment on planner.go:201 described, and that
// InvalidatePlanCache (wired to config reload in Multigateway.CobraPreRunE)
// closes it: a failover-slot-creation statement admitted while the flag is
// on is cached; without invalidation, it keeps being served from cache after
// the flag flips off (asserted here as the "before" state); after
// InvalidatePlanCache, the identical statement is re-planned under the
// current flag value and correctly rejected.
// TestSetSlotBasedReplicationEnabled_TransitionInvalidatesOnNextRequest
// proves invalidateOnSlotBasedReplicationTransition's guarantee: the very
// next request after the flag flips gets the cache invalidated automatically
// — no explicit InvalidatePlanCache call needed, and no waiting on
// CobraPreRunE's asynchronous config-reload handler to get scheduled. A
// stale plan admitted under the old value is never observable by any
// request that itself sees the new value.
func TestSetSlotBasedReplicationEnabled_TransitionInvalidatesOnNextRequest(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := t.Context()
	conn := testConn()

	enabled := true
	exec.SetSlotBasedReplicationEnabled(func() bool { return enabled })

	const sql = "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', false, false, true)"
	stmt := parseOne(t, sql)

	// Admitted while the flag is on, and cached.
	res1, err := exec.StreamExecute(ctx, conn, nil, sql, stmt, noopCallback)
	require.NoError(t, err)
	assert.False(t, res1.CacheHit)
	time.Sleep(50 * time.Millisecond) // theine processes writes asynchronously

	enabled = false

	// No explicit InvalidatePlanCache call here — the transition is caught
	// synchronously on this very request, before its cache lookup, so it
	// must be re-planned under the current (disabled) flag and rejected.
	_, err = exec.StreamExecute(ctx, conn, nil, sql, stmt, noopCallback)
	require.Error(t, err, "the first request to observe the flip must never see the stale cached admission")
	assert.Contains(t, err.Error(), "requires temporary=true")
}

// TestInvalidateOnSlotBasedReplicationTransition covers the transition
// detector in isolation: it must invalidate exactly on a change (either
// direction), never on a steady value, and never panic on a nil getter.
func TestInvalidateOnSlotBasedReplicationTransition(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()

	t.Run("nil getter is a no-op", func(t *testing.T) {
		exec.SetSlotBasedReplicationEnabled(nil)
		exec.planCache.Put("q", makeTestPlan())
		time.Sleep(50 * time.Millisecond)
		exec.invalidateOnSlotBasedReplicationTransition()
		_, ok := exec.planCache.Get(t.Context(), "q")
		assert.True(t, ok, "no getter means nothing to transition, cache must be untouched")
	})

	t.Run("steady value never invalidates", func(t *testing.T) {
		exec.SetSlotBasedReplicationEnabled(func() bool { return true })
		exec.planCache.Put("q", makeTestPlan())
		time.Sleep(50 * time.Millisecond)
		exec.invalidateOnSlotBasedReplicationTransition() // establishes baseline
		exec.invalidateOnSlotBasedReplicationTransition() // same value again
		_, ok := exec.planCache.Get(t.Context(), "q")
		assert.True(t, ok, "an unchanged value must never invalidate")
	})

	t.Run("a transition in either direction invalidates", func(t *testing.T) {
		enabled := true
		exec.SetSlotBasedReplicationEnabled(func() bool { return enabled })
		exec.planCache.Put("q", makeTestPlan())
		time.Sleep(50 * time.Millisecond)

		enabled = false
		exec.invalidateOnSlotBasedReplicationTransition()
		_, ok := exec.planCache.Get(t.Context(), "q")
		assert.False(t, ok, "true->false must invalidate")

		exec.planCache.Put("q2", makeTestPlan())
		time.Sleep(50 * time.Millisecond)
		enabled = true
		exec.invalidateOnSlotBasedReplicationTransition()
		_, ok = exec.planCache.Get(t.Context(), "q2")
		assert.False(t, ok, "false->true must invalidate too")
	})
}

// TestInvalidateOnSlotBasedReplicationTransition_ConcurrentRequestsNeverObserveStaleCache
// stress-tests concurrent callers around a flag flip. The check, the flip,
// and the invalidation share one critical section: a request that finds the
// flag already up to date (because a sibling request already recorded the
// transition) is guaranteed that sibling's invalidation has already
// completed. Each iteration plants a canary entry, flips the flag, then races
// several goroutines through invalidateOnSlotBasedReplicationTransition
// followed immediately by a cache lookup — every one of them observes the
// post-flip flag value, so none of them may ever see the canary still cached.
func TestInvalidateOnSlotBasedReplicationTransition_ConcurrentRequestsNeverObserveStaleCache(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()

	var enabled atomic.Bool
	enabled.Store(true)
	exec.SetSlotBasedReplicationEnabled(enabled.Load)

	const iterations = 200
	const racers = 8
	var staleHits atomic.Int32

	for range iterations {
		exec.planCache.Put("canary", makeTestPlan())
		enabled.Store(!enabled.Load())

		var wg sync.WaitGroup
		wg.Add(racers)
		for range racers {
			go func() {
				defer wg.Done()
				exec.invalidateOnSlotBasedReplicationTransition()
				if _, ok := exec.planCache.Get(t.Context(), "canary"); ok {
					staleHits.Add(1)
				}
			}()
		}
		wg.Wait()
	}

	assert.Equal(t, int32(0), staleHits.Load(),
		"a request that ran invalidateOnSlotBasedReplicationTransition after the flag flipped must never see the pre-flip cache entry")
}

// TestInvalidatePlanCache_ForcesReplan confirms InvalidatePlanCache discards
// previously cached plans: it exists so a caller (CobraPreRunE's config-reload
// handler) can force every query to be re-analyzed after a dynamic,
// plan-affecting flag changes value — otherwise a plan admitted under the old
// value would keep being served from the cache after the flag flips (see
// SetSlotBasedReplicationEnabled).
func TestInvalidatePlanCache_ForcesReplan(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := t.Context()
	conn := testConn()

	res1, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM users WHERE id = 42", parseOne(t, "SELECT * FROM users WHERE id = 42"), noopCallback)
	require.NoError(t, err)
	assert.False(t, res1.CacheHit)

	// theine processes writes asynchronously
	time.Sleep(50 * time.Millisecond)

	res2, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM users WHERE id = 99", parseOne(t, "SELECT * FROM users WHERE id = 99"), noopCallback)
	require.NoError(t, err)
	assert.True(t, res2.CacheHit, "same shape should hit cache before invalidation")

	exec.InvalidatePlanCache()

	res3, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM users WHERE id = 7", parseOne(t, "SELECT * FROM users WHERE id = 7"), noopCallback)
	require.NoError(t, err)
	assert.False(t, res3.CacheHit, "same shape must miss and be re-planned after invalidation")
}

func TestStreamExecute_DifferentShapesAreSeparateCacheEntries(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	res1, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM users WHERE id = 1", parseOne(t, "SELECT * FROM users WHERE id = 1"), noopCallback)
	require.NoError(t, err)
	assert.False(t, res1.CacheHit)

	time.Sleep(50 * time.Millisecond)

	// Different table — different cache entry
	res2, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM orders WHERE id = 1", parseOne(t, "SELECT * FROM orders WHERE id = 1"), noopCallback)
	require.NoError(t, err)
	assert.False(t, res2.CacheHit, "different query shape should miss cache")
}

func TestStreamExecute_NoLiteralsCachedBySQL(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	// SELECT with no literals — still cached by its SQL string.
	sql := "SELECT * FROM users"
	res, err := exec.StreamExecute(ctx, conn, nil, sql, parseOne(t, sql), noopCallback)
	require.NoError(t, err)
	assert.False(t, res.CacheHit)

	time.Sleep(50 * time.Millisecond)

	res2, err := exec.StreamExecute(ctx, conn, nil, sql, parseOne(t, sql), noopCallback)
	require.NoError(t, err)
	assert.True(t, res2.CacheHit, "repeated query with no literals should hit cache")
}

// ---------- PortalStreamExecute plan cache tests ----------

func TestPortalStreamExecute_CacheHitOnRepeatedPortal(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	portal := makePortalInfo(t, "SELECT * FROM users WHERE id = $1")

	// First portal execution — cache miss
	res1, err := exec.PortalStreamExecute(ctx, conn, nil, portal, 0, false, noopCallback)
	require.NoError(t, err)
	assert.False(t, res1.CacheHit)

	time.Sleep(50 * time.Millisecond)

	// Second portal execution — cache hit (same query string)
	res2, err := exec.PortalStreamExecute(ctx, conn, nil, portal, 0, false, noopCallback)
	require.NoError(t, err)
	assert.True(t, res2.CacheHit, "repeated portal should hit cache")

	assert.Equal(t, int32(2), mock.portalStreamExecuteCalls.Load())
}

// TestPortalStreamExecute_UnsafeConnectionNotCached is the regression guard for
// the cross-protocol plan-cache poisoning vector. A unsafe connection's plan is
// built with the unsafe-statement rejections suppressed, so it must never enter
// the shared, database-wide plan cache: otherwise a normal connection could
// receive it as a cache hit and run a blocklisted call the planner would reject
// (SELECT pg_read_file(...) — an LFI/SSRF bypass). The extended-protocol
// resolvePortalPlan must exclude unsafe connections just as resolvePlan does.
//
// Uses the doorkeeper-disabled test cache (newTestExecutor) so admission is
// deterministic — the same reason this cannot be verified reliably end-to-end.
func TestPortalStreamExecute_UnsafeConnectionNotCached(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()

	// A blocklisted call: rejected on an enforcing connection, accepted on a
	// direct one. Cacheable (a plain SELECT), so absent the guard its accepted
	// plan would be cached.
	const sql = "SELECT pg_read_file('/etc/passwd')"

	// Sanity: an enforcing connection is rejected outright, so nothing is cached.
	_, err := exec.PortalStreamExecute(ctx, testConn(), nil, makePortalInfo(t, sql), 0, false, noopCallback)
	require.Error(t, err, "blocklisted call must be rejected on an enforcing connection")

	// A unsafe connection accepts and executes it. Absent the guard, its plan is
	// put into the shared cache here.
	direct := server.NewTestConn(&bytes.Buffer{}, server.WithTestUnsafeConnection()).Conn
	res, err := exec.PortalStreamExecute(ctx, direct, nil, makePortalInfo(t, sql), 0, false, noopCallback)
	require.NoError(t, err, "unsafe connection must accept the blocklisted call")
	assert.False(t, res.CacheHit, "an unsafe connection must never serve from or populate the shared cache")

	// theine processes writes asynchronously; give any (erroneous) write time to land.
	time.Sleep(50 * time.Millisecond)

	// The crux: a normal connection running the same statement must STILL be
	// rejected — the unsafe connection's accepted plan must not have poisoned the
	// shared cache.
	_, err = exec.PortalStreamExecute(ctx, testConn(), nil, makePortalInfo(t, sql), 0, false, noopCallback)
	require.Error(t, err, "unsafe-connection plan must not be cached for a normal connection")
	assert.Contains(t, err.Error(), "pg_read_file is not supported")
}

// ---------- Cross-protocol plan cache tests ----------

func TestCrossProtocol_SimpleProtocolCachesForPortal(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	// Simple protocol: "SELECT * FROM users WHERE id = 42"
	// Normalizes to "SELECT * FROM users WHERE id = $1" and caches the plan.
	res1, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM users WHERE id = 42", parseOne(t, "SELECT * FROM users WHERE id = 42"), noopCallback)
	require.NoError(t, err)
	assert.False(t, res1.CacheHit)

	time.Sleep(50 * time.Millisecond)

	// Extended protocol: query is already "SELECT * FROM users WHERE id = $1"
	// This should hit the cache populated by the simple protocol execution.
	portal := makePortalInfo(t, "SELECT * FROM users WHERE id = $1")
	res2, err := exec.PortalStreamExecute(ctx, conn, nil, portal, 0, false, noopCallback)
	require.NoError(t, err)
	assert.True(t, res2.CacheHit, "portal should hit plan cached by simple protocol")
}

func TestCrossProtocol_PortalCachesForSimpleProtocol(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	// Extended protocol first — caches plan for "SELECT * FROM orders WHERE id = $1"
	portal := makePortalInfo(t, "SELECT * FROM orders WHERE id = $1")
	res1, err := exec.PortalStreamExecute(ctx, conn, nil, portal, 0, false, noopCallback)
	require.NoError(t, err)
	assert.False(t, res1.CacheHit)

	time.Sleep(50 * time.Millisecond)

	// Simple protocol: "SELECT * FROM orders WHERE id = 7"
	// Normalizes to "SELECT * FROM orders WHERE id = $1" — should hit.
	res2, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM orders WHERE id = 7", parseOne(t, "SELECT * FROM orders WHERE id = 7"), noopCallback)
	require.NoError(t, err)
	assert.True(t, res2.CacheHit, "simple protocol should hit plan cached by portal")
}

func TestCrossProtocol_PortalCachedPlanReconstructsSQL(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	// Extended protocol first — caches plan for "SELECT * FROM orders WHERE id = $1"
	portal := makePortalInfo(t, "SELECT * FROM orders WHERE id = $1")
	_, err := exec.PortalStreamExecute(ctx, conn, nil, portal, 0, false, noopCallback)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// Simple protocol with literal value — should hit the portal-cached plan
	// and reconstruct SQL with the bind value substituted back in.
	res, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM orders WHERE id = 7", parseOne(t, "SELECT * FROM orders WHERE id = 7"), noopCallback)
	require.NoError(t, err)
	assert.True(t, res.CacheHit)

	// The SQL sent to the backend must have the literal value, not $1.
	backendSQL, ok := mock.lastStreamExecuteSQL.Load().(string)
	require.True(t, ok)
	assert.Equal(t, "SELECT * FROM orders WHERE id = 7", backendSQL,
		"cross-protocol cache hit must reconstruct SQL with bind values")
}

func TestCrossProtocol_SimpleCachedPlanReconstructsSQL(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	// Simple protocol first — normalizes and caches.
	_, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM users WHERE name = 'alice'", parseOne(t, "SELECT * FROM users WHERE name = 'alice'"), noopCallback)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// Same shape, different value — cache hit, must reconstruct.
	res, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM users WHERE name = 'bob'", parseOne(t, "SELECT * FROM users WHERE name = 'bob'"), noopCallback)
	require.NoError(t, err)
	assert.True(t, res.CacheHit)

	backendSQL, ok := mock.lastStreamExecuteSQL.Load().(string)
	require.True(t, ok)
	assert.Equal(t, "SELECT * FROM users WHERE name = 'bob'", backendSQL,
		"cache hit must reconstruct SQL with current bind values")
}

func TestCrossProtocol_MixedWorkload(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	// 1. Simple protocol miss
	res, err := exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM t WHERE x = 1", parseOne(t, "SELECT * FROM t WHERE x = 1"), noopCallback)
	require.NoError(t, err)
	assert.False(t, res.CacheHit)
	time.Sleep(50 * time.Millisecond)

	// 2. Simple protocol hit (different value, same shape)
	res, err = exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM t WHERE x = 2", parseOne(t, "SELECT * FROM t WHERE x = 2"), noopCallback)
	require.NoError(t, err)
	assert.True(t, res.CacheHit)

	// 3. Portal hit (same normalized form)
	portal := makePortalInfo(t, "SELECT * FROM t WHERE x = $1")
	res, err = exec.PortalStreamExecute(ctx, conn, nil, portal, 0, false, noopCallback)
	require.NoError(t, err)
	assert.True(t, res.CacheHit)

	// 4. Different query — portal miss
	portal2 := makePortalInfo(t, "SELECT * FROM t WHERE y = $1")
	res, err = exec.PortalStreamExecute(ctx, conn, nil, portal2, 0, false, noopCallback)
	require.NoError(t, err)
	assert.False(t, res.CacheHit)
	time.Sleep(50 * time.Millisecond)

	// 5. Simple protocol hits the plan cached by portal in step 4
	res, err = exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM t WHERE y = 99", parseOne(t, "SELECT * FROM t WHERE y = 99"), noopCallback)
	require.NoError(t, err)
	assert.True(t, res.CacheHit)
}

// ---------- Cache key isolation tests ----------

func TestCacheKey_DifferentDatabasesAreSeparate(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()

	connDB1 := testConnWithDB("db1")
	connDB2 := testConnWithDB("db2")

	sql := "SELECT * FROM users WHERE id = 1"
	astStmt := parseOne(t, sql)

	// Cache plan in db1
	res1, err := exec.StreamExecute(ctx, connDB1, nil, sql, astStmt, noopCallback)
	require.NoError(t, err)
	assert.False(t, res1.CacheHit)
	time.Sleep(50 * time.Millisecond)

	// Same query on db2 — must miss (different database)
	res2, err := exec.StreamExecute(ctx, connDB2, nil, sql, parseOne(t, sql), noopCallback)
	require.NoError(t, err)
	assert.False(t, res2.CacheHit, "different databases must not share cached plans")

	time.Sleep(50 * time.Millisecond)

	// Same query on db1 again — should hit
	res3, err := exec.StreamExecute(ctx, connDB1, nil, sql, parseOne(t, sql), noopCallback)
	require.NoError(t, err)
	assert.True(t, res3.CacheHit, "same database should hit cached plan")
}

func TestCacheKey_PortalDifferentDatabasesAreSeparate(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()

	connDB1 := testConnWithDB("db1")
	connDB2 := testConnWithDB("db2")

	portal := makePortalInfo(t, "SELECT * FROM orders WHERE id = $1")

	// Cache plan in db1
	res1, err := exec.PortalStreamExecute(ctx, connDB1, nil, portal, 0, false, noopCallback)
	require.NoError(t, err)
	assert.False(t, res1.CacheHit)
	time.Sleep(50 * time.Millisecond)

	// Same portal on db2 — must miss
	res2, err := exec.PortalStreamExecute(ctx, connDB2, nil, portal, 0, false, noopCallback)
	require.NoError(t, err)
	assert.False(t, res2.CacheHit, "different databases must not share cached plans via portal")
}

// TestPortalStreamExecute_RunsCacheableSequencePlan verifies that the
// cacheable extended-protocol path actually runs the planned primitive —
// not just its routing — so a Sequence built for SELECT set_config(..., false)
// has both effects: the Route forwards the portal, and the silent
// ApplySessionState updates the gateway tracker after backend success. Earlier
// the executor short-circuited to extractRouting + exec.PortalStreamExecute
// directly, dropping silent tracking entirely; the redesign delegates to
// plan.PortalStreamExecute so each primitive owns its portal-mode behavior.
func TestPortalStreamExecute_RunsCacheableSequencePlan(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()
	state := handler.NewMultigatewayConnectionState()

	portal := makePortalInfo(t, "SELECT set_config('work_mem', '256MB', false)")

	_, err := exec.PortalStreamExecute(ctx, conn, state, portal, 0, false, noopCallback)
	require.NoError(t, err)

	// Silent tracking must have written the tracker.
	got, ok := state.GetSessionVariable("work_mem")
	require.True(t, ok, "silent ApplySessionState should have updated SessionSettings")
	assert.Equal(t, "256MB", got)

	// On this unpinned session the SessionStateBranch reissues the portal with
	// the set_config rewritten to is_local := true, so nothing persists on the
	// pooled backend; the value lives only in the gateway map (asserted above)
	// and is replayed at the next checkout.
	assert.Equal(t, int32(1), mock.portalStreamExecuteCalls.Load(),
		"the portal must be forwarded to the backend before silent tracking")
}

// TestStreamExecute_SetConfigWithSiblingLiteral covers the simple-protocol
// shape `SELECT set_config(literal, literal, false), <other-literal>`. The
// normalizer skips the set_config subtree but still parameterizes the
// sibling literal; the planner emits a Sequence whose Route holds the
// normalized SQL + NormalizedAST. If Sequence.StreamExecute drops
// bindVars on its way to children, Route can't reconstruct and the
// `$N` placeholder reaches PG unbound — which surfaces as
// `there is no parameter $1`. Verify the backend receives the literal,
// not the placeholder.
func TestStreamExecute_SetConfigWithSiblingLiteral(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()
	state := handler.NewMultigatewayConnectionState()

	sql := "SELECT set_config('work_mem', '256MB', false), 42 AS num"
	_, err := exec.StreamExecute(ctx, conn, state, sql, parseOne(t, sql), noopCallback)
	require.NoError(t, err)

	backendSQL, _ := mock.lastStreamExecuteSQL.Load().(string)
	assert.Contains(t, backendSQL, "42",
		"sibling literal must be substituted back; backend SQL was %q", backendSQL)
	assert.NotContains(t, backendSQL, "$1",
		"normalized placeholder must not reach the backend; backend SQL was %q", backendSQL)

	// Silent ApplySessionState must still have updated the tracker.
	got, ok := state.GetSessionVariable("work_mem")
	require.True(t, ok)
	assert.Equal(t, "256MB", got)
}

// TestStreamExecute_SetConfigGMVLocalPlanCacheReuse is the regression for
// the simple-protocol plan-cache flow of a gateway-managed
// set_config(..., true). The normalizer parameterizes the value (collapsing
// different literals into one cached plan), so the ApplySessionState carries a
// ValueParam BindRef that StreamExecute must resolve from the
// normalizer-extracted bindVars on EVERY execution. Before that fix,
// StreamExecute ignored bindVars entirely and applied the synthetic
// `__bind_$1__` placeholder to gateway state.
//
// The gateway-managed set_config is rewritten out of the backend query
// (GatewayManagedValueRoute): the real set_config never reaches PG — it is replaced
// by a constant of its value — so no GUC is persisted on the pooled backend.
func TestStreamExecute_SetConfigGMVLocalPlanCacheReuse(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state := handler.NewMultigatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)

	// Cache miss: plan is minted from the normalized AST, so the value slot
	// is a ParamRef and must be resolved from this execution's bindVars.
	sql1 := "SELECT set_config('statement_timeout', '1s', true)"
	res1, err := exec.StreamExecute(ctx, conn, state, sql1, parseOne(t, sql1), noopCallback)
	require.NoError(t, err)
	assert.False(t, res1.CacheHit)
	assert.Equal(t, time.Second, state.GetStatementTimeout(),
		"first execution must apply its own literal, not a __bind placeholder")

	time.Sleep(50 * time.Millisecond) // plan cache Put is async

	// Cache hit with a different literal: the same cached plan must apply
	// THIS execution's value to gateway state.
	sql2 := "SELECT set_config('statement_timeout', '2s', true)"
	res2, err := exec.StreamExecute(ctx, conn, state, sql2, parseOne(t, sql2), noopCallback)
	require.NoError(t, err)
	assert.True(t, res2.CacheHit, "same normalized shape must hit the plan cache")
	assert.Equal(t, 2*time.Second, state.GetStatementTimeout(),
		"cache hit must apply this execution's value, not the first-seen literal or a placeholder")

	// The query that reaches the backend has the gateway-managed set_config
	// rewritten out: no statement_timeout / set_config call, just a constant of the
	// (canonical) value from this execution.
	backendSQL, _ := mock.lastStreamExecuteSQL.Load().(string)
	assert.NotContains(t, backendSQL, "statement_timeout",
		"the gateway-managed set_config must be rewritten out of the backend query")
	assert.NotContains(t, backendSQL, "set_config(",
		"no set_config call may reach the backend for a gateway-managed variable (the AS set_config alias is fine)")
	assert.Contains(t, backendSQL, "2s",
		"the rewritten query projects this execution's canonical value")
}

func TestCrossProtocol_CasingNormalization(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()

	// 1. Simple protocol with lowercase keywords — cache miss, populates cache.
	res, err := exec.StreamExecute(ctx, conn, nil,
		"select * from users where id = 1", parseOne(t, "select * from users where id = 1"), noopCallback)
	require.NoError(t, err)
	assert.False(t, res.CacheHit)
	time.Sleep(50 * time.Millisecond)

	// 2. Simple protocol with UPPERCASE keywords, different value — same AST
	//    shape, SqlString() produces the same canonical form, so cache hit.
	res, err = exec.StreamExecute(ctx, conn, nil,
		"SELECT * FROM users WHERE id = 99", parseOne(t, "SELECT * FROM users WHERE id = 99"), noopCallback)
	require.NoError(t, err)
	assert.True(t, res.CacheHit, "different keyword casing should share cached plan")

	// Verify the backend got the correct literal value, not $1.
	backendSQL, _ := mock.lastStreamExecuteSQL.Load().(string)
	assert.Contains(t, backendSQL, "99", "cache hit must reconstruct SQL with current bind values")

	// 3. Simple protocol with mixed casing and extra whitespace.
	res, err = exec.StreamExecute(ctx, conn, nil,
		"Select  *  From  users  Where  id = 7", parseOne(t, "Select  *  From  users  Where  id = 7"), noopCallback)
	require.NoError(t, err)
	assert.True(t, res.CacheHit, "mixed casing and extra whitespace should share cached plan")

	backendSQL, _ = mock.lastStreamExecuteSQL.Load().(string)
	assert.Contains(t, backendSQL, "7")

	// 4. Portal with uppercase keywords — should hit the plan cached
	//    in step 1, since SqlString() produces the same canonical form.
	portal := makePortalInfo(t, "SELECT * FROM users WHERE id = $1")
	res, err = exec.PortalStreamExecute(ctx, conn, nil, portal, 0, false, noopCallback)
	require.NoError(t, err)
	assert.True(t, res.CacheHit, "portal should share cache with simple protocol regardless of original casing")

	// 5. Portal with lowercase keywords — same canonical form, cache hit.
	portalLower := makePortalInfo(t, "select * from users where id = $1")
	res, err = exec.PortalStreamExecute(ctx, conn, nil, portalLower, 0, false, noopCallback)
	require.NoError(t, err)
	assert.True(t, res.CacheHit, "portal with different casing should share cached plan")
}

// TestStreamReplication_RoutesToDefaultTableGroupAndShard verifies that
// Executor.StreamReplication bypasses query planning and forwards directly to
// the execution backend with the default tablegroup/shard, passing state and
// init through unchanged.
func TestStreamReplication_RoutesToDefaultTableGroupAndShard(t *testing.T) {
	wantStream := multipoolerpb.MultipoolerService_StreamReplicationClient(nil)
	mock := &mockExec{streamReplicationStream: wantStream}
	exec := newTestExecutor(mock)
	conn := testConn()
	state := handler.NewMultigatewayConnectionState()
	init := &multipoolerpb.StreamReplicationInit{User: "repluser"}

	stream, err := exec.StreamReplication(context.Background(), conn, state, init)

	require.NoError(t, err)
	assert.Equal(t, wantStream, stream)
	assert.Equal(t, DefaultTableGroup, mock.streamReplicationTableGroup.Load())
	assert.Equal(t, constants.DefaultShard, mock.streamReplicationShard.Load())
	assert.Same(t, init, mock.streamReplicationInit.Load(), "the same init must be forwarded, not a copy")
}

// TestStreamReplication_PropagatesError verifies that a backend error (e.g.
// no leader available) is returned to the caller unchanged.
func TestStreamReplication_PropagatesError(t *testing.T) {
	wantErr := errors.New("no leader available")
	mock := &mockExec{streamReplicationErr: wantErr}
	exec := newTestExecutor(mock)
	conn := testConn()
	state := handler.NewMultigatewayConnectionState()

	stream, err := exec.StreamReplication(context.Background(), conn, state, &multipoolerpb.StreamReplicationInit{})

	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, stream)
}
