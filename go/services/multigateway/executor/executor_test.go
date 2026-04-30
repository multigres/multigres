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
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
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
	streamExecuteCalls        atomic.Int32
	portalStreamExecuteCalls  atomic.Int32
	lastStreamExecuteSQL      atomic.Value // string
	lastPortalStreamExecuteQS atomic.Value // string
}

func (m *mockExec) StreamExecute(
	_ context.Context, _ *server.Conn, _, _ string, sql string,
	_ *querypb.PreparedStatement,
	_ *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	m.streamExecuteCalls.Add(1)
	m.lastStreamExecuteSQL.Store(sql)
	return callback(context.Background(), &sqltypes.Result{})
}

func (m *mockExec) PortalStreamExecute(
	_ context.Context, _, _ string, _ *server.Conn,
	_ *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo, _ int32, _ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	m.portalStreamExecuteCalls.Add(1)
	m.lastPortalStreamExecuteQS.Store(portalInfo.PreparedStatementInfo.Query)
	return callback(context.Background(), &sqltypes.Result{})
}

func (m *mockExec) Describe(context.Context, string, string, *server.Conn, *handler.MultiGatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*querypb.StatementDescription, error) {
	return nil, nil
}

func (m *mockExec) ConcludeTransaction(context.Context, *server.Conn, *handler.MultiGatewayConnectionState, multipoolerpb.TransactionConclusion, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *mockExec) DiscardTempTables(context.Context, *server.Conn, *handler.MultiGatewayConnectionState, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *mockExec) ReleaseAllReservedConnections(context.Context, *server.Conn, *handler.MultiGatewayConnectionState) error {
	return nil
}

func (m *mockExec) CopyInitiate(context.Context, *server.Conn, string, string, string, *handler.MultiGatewayConnectionState, func(context.Context, *sqltypes.Result) error) (int16, []int16, error) {
	return 0, nil, nil
}

func (m *mockExec) CopySendData(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState, []byte) error {
	return nil
}

func (m *mockExec) CopyFinalize(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState, []byte, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *mockExec) CopyAbort(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState) error {
	return nil
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
// has both effects: the silent ApplySessionState updates the gateway
// tracker, and the trailing Route still forwards the portal. Earlier
// the executor short-circuited to extractRouting + exec.PortalStreamExecute
// directly, dropping the silent prefix entirely; the redesign delegates
// to plan.PortalStreamExecute so each primitive owns its portal-mode
// behavior.
func TestPortalStreamExecute_RunsCacheableSequencePlan(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	ctx := context.Background()
	conn := testConn()
	state := handler.NewMultiGatewayConnectionState()

	portal := makePortalInfo(t, "SELECT set_config('work_mem', '256MB', false)")

	_, err := exec.PortalStreamExecute(ctx, conn, state, portal, 0, false, noopCallback)
	require.NoError(t, err)

	// Silent prefix must have written the tracker.
	got, ok := state.GetSessionVariable("work_mem")
	require.True(t, ok, "silent ApplySessionState prefix should have updated SessionSettings")
	assert.Equal(t, "256MB", got)

	// And the portal forward to the backend must still have happened.
	assert.Equal(t, int32(1), mock.portalStreamExecuteCalls.Load(),
		"portal must still be forwarded to the backend after silent prefix")
}

// TestStreamExecute_SetConfigWithSiblingLiteral covers the simple-protocol
// shape `SELECT set_config(literal, literal, false), <other-literal>`. The
// normalizer skips the set_config subtree but still parameterizes the
// sibling literal; the planner emits a Sequence whose trailing Route holds
// the normalized SQL + NormalizedAST. If Sequence.StreamExecute drops
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
	state := handler.NewMultiGatewayConnectionState()

	sql := "SELECT set_config('work_mem', '256MB', false), 42 AS num"
	_, err := exec.StreamExecute(ctx, conn, state, sql, parseOne(t, sql), noopCallback)
	require.NoError(t, err)

	backendSQL, _ := mock.lastStreamExecuteSQL.Load().(string)
	assert.Contains(t, backendSQL, "42",
		"sibling literal must be substituted back; backend SQL was %q", backendSQL)
	assert.NotContains(t, backendSQL, "$1",
		"normalized placeholder must not reach the backend; backend SQL was %q", backendSQL)

	// Silent ApplySessionState prefix must still have updated the tracker.
	got, ok := state.GetSessionVariable("work_mem")
	require.True(t, ok)
	assert.Equal(t, "256MB", got)
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
