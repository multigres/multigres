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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
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
	lastEagerParsePreparedStatement atomic.Pointer[querypb.PreparedStatement]
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
	preparedStatement *querypb.PreparedStatement,
	_ *handler.MultigatewayConnectionState,
	_ engine.PlanExecInfo,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	m.streamExecuteCalls.Add(1)
	m.lastStreamExecuteSQL.Store(sql)
	m.lastEagerParsePreparedStatement.Store(preparedStatement)
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

func (m *mockExec) StreamReplication(_ context.Context, _ *server.Conn, tableGroup, shard string, _ *handler.MultigatewayConnectionState, init *multipoolerpb.StreamReplicationInit) (queryservice.ReplicationStream, error) {
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

// newMigrationConfiguredExecutor is newTestExecutor with a migration backend
// wired to a spyMigratorClient, so a migration/connection DDL statement can
// actually execute (not just plan) instead of failing with an unrelated
// "not configured" error.
func newMigrationConfiguredExecutor(mock *mockExec) *Executor {
	logger := slog.Default()
	txnMetrics, _ := engine.NewTransactionMetrics()
	p := planner.NewPlanner(DefaultTableGroup, logger, txnMetrics)
	p.SetMigrationBackend(&engine.MigrationBackend{Client: func() migratorpb.MigratorClient { return &spyMigratorClient{} }})
	return &Executor{
		planner:   p,
		exec:      mock,
		logger:    logger,
		planCache: plancache.NewForTest(1024 * 1024),
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
	return makePortalInfoWithResultFormats(t, sql, nil)
}

func makePortalInfoWithResultFormats(t *testing.T, sql string, resultFormats []int32) *preparedstatement.PortalInfo {
	t.Helper()
	psi, err := preparedstatement.NewPreparedStatementInfo(&querypb.PreparedStatement{
		Name:  "",
		Query: sql,
	})
	require.NoError(t, err)
	return preparedstatement.NewPortalInfo(psi, &querypb.Portal{Name: "", ResultFormats: resultFormats})
}

func TestPrepareInTransaction(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()

	require.NoError(t, exec.PrepareInTransaction(context.Background(), testConn(), handler.NewMultigatewayConnectionState(), "SELECT $1", []uint32{23}))
	assert.Equal(t, int32(1), mock.streamExecuteCalls.Load())
	assert.Empty(t, mock.lastStreamExecuteSQL.Load())
	ps := mock.lastEagerParsePreparedStatement.Load()
	require.NotNil(t, ps)
	assert.Equal(t, "SELECT $1", ps.GetQuery())
	assert.Equal(t, []uint32{23}, ps.GetParamTypes())
	assert.True(t, ps.GetForceReparse())
}

// TestIsCacheable_ExcludesStatMigrationSelect is the regression test for the
// finding this guards against: a recognized multigres.stat_migration query
// (e.g. "... WHERE migration_name = 'foo-move'") was cacheable, so the
// gateway's literal-to-$1 normalization ran on it before
// engine.IsStatMigrationSelect ever saw the AST — the recognizer requires an
// *ast.A_Const filter value and treats a bind parameter as unrecognized,
// silently falling back to ordinary (gated) routing and defeating the whole
// point of the bypass (checking migration progress while NOT_SERVING).
func TestIsCacheable_ExcludesStatMigrationSelect(t *testing.T) {
	t.Run("filtered stat_migration select is not cacheable", func(t *testing.T) {
		stmt := parseOne(t, "SELECT * FROM multigres.stat_migration WHERE migration_name = 'foo-move'")
		assert.False(t, isCacheable(stmt))
	})
	t.Run("unfiltered stat_migration select is not cacheable", func(t *testing.T) {
		stmt := parseOne(t, "SELECT * FROM multigres.stat_migration")
		assert.False(t, isCacheable(stmt))
	})
	t.Run("an ordinary select stays cacheable", func(t *testing.T) {
		stmt := parseOne(t, "SELECT * FROM t WHERE id = 'x'")
		assert.True(t, isCacheable(stmt))
	})
}

// spyMigratorClient tracks whether any migrator RPC was invoked, standing in
// for a real migrator connection in TestStatMigrationSelect_SupportedVsNearbyUnsupported.
// Embeds the nil interface so an unexpected method call panics rather than
// silently succeeding.
type spyMigratorClient struct {
	migratorpb.MigratorClient
	called atomic.Bool
}

func (s *spyMigratorClient) GetMigration(context.Context, *migratorpb.GetMigrationRequest, ...grpc.CallOption) (*migratorpb.GetMigrationResponse, error) {
	s.called.Store(true)
	return &migratorpb.GetMigrationResponse{}, nil
}

func (s *spyMigratorClient) ListMigrations(context.Context, *migratorpb.ListMigrationsRequest, ...grpc.CallOption) (*migratorpb.ListMigrationsResponse, error) {
	s.called.Store(true)
	return &migratorpb.ListMigrationsResponse{}, nil
}

// CreateMigration is a bare no-op implementation, only present so
// newMigrationConfiguredExecutor's spy can actually execute a mutating
// migration DDL statement end to end in tests that don't care about its
// result, just that it doesn't panic on the embedded nil interface.
func (s *spyMigratorClient) CreateMigration(context.Context, *migratorpb.CreateMigrationRequest, ...grpc.CallOption) (*migratorpb.CreateMigrationResponse, error) {
	return &migratorpb.CreateMigrationResponse{}, nil
}

// migrationPrivilegedTestConn returns a *server.Conn whose cached credentials
// satisfy checkMigrationDDLPrivilege, so a recognized stat_migration query
// reaches the migrator RPC instead of being rejected before it gets there.
func migrationPrivilegedTestConn() *server.Conn {
	return server.NewTestConn(&bytes.Buffer{},
		server.WithTestCredentials(&server.Credentials{CanCreateMigration: true})).Conn
}

// TestStatMigrationSelect_SupportedVsNearbyUnsupported draws the exact
// boundary of the multigres.stat_migration SELECT bypass end to end, through
// the real StreamExecute -> resolvePlan -> Plan() pipeline — not Plan() called
// directly (TestPlan_StatMigrationSelectRouting in the planner package), which
// never exercises ast.Normalize and so would miss the normalization bug
// isCacheable's stat_migration exclusion guards against. Each "supported"
// query must reach the migrator RPC and never touch the ordinary backend;
// each "not supported" query is a near miss of a different kind (wrong
// operator, wrong column, an extra clause, a genuine bind parameter, ...) and
// must fall through to ordinary, serving-gated routing against the real view
// instead, never reaching the migrator RPC.
func TestStatMigrationSelect_SupportedVsNearbyUnsupported(t *testing.T) {
	runCase := func(t *testing.T, sql string) (migratorRPCCalled bool, ordinaryRoutingCalls int32) {
		t.Helper()
		spy := &spyMigratorClient{}
		p := planner.NewPlanner(DefaultTableGroup, slog.Default(), nil)
		p.SetMigrationBackend(&engine.MigrationBackend{Client: func() migratorpb.MigratorClient { return spy }})
		mock := &mockExec{}
		exec := &Executor{planner: p, exec: mock, logger: slog.Default(), planCache: plancache.NewForTest(1024 * 1024)}
		defer exec.planCache.Close()

		stmt := parseOne(t, sql)
		_, err := exec.StreamExecute(context.Background(), migrationPrivilegedTestConn(),
			handler.NewMultigatewayConnectionState(), sql, stmt, noopCallback)
		require.NoError(t, err)
		return spy.called.Load(), mock.streamExecuteCalls.Load()
	}

	supported := []string{
		// Bare, unfiltered.
		"SELECT * FROM multigres.stat_migration",
		// A specific column list, still unfiltered.
		"SELECT migration_name, lag_bytes FROM multigres.stat_migration",
		// Filtered by a literal — the exact case normalization used to break.
		"SELECT * FROM multigres.stat_migration WHERE migration_name = 'foo-move'",
		// Filtered by migration_id, an integer literal.
		"SELECT * FROM multigres.stat_migration WHERE migration_id = 5",
		// Operand order reversed: constant on the left.
		"SELECT * FROM multigres.stat_migration WHERE 'foo-move' = migration_name",
	}
	for _, sql := range supported {
		t.Run("supported: "+sql, func(t *testing.T) {
			rpcCalled, ordinaryCalls := runCase(t, sql)
			assert.True(t, rpcCalled, "expected the migrator RPC to be called")
			assert.Zero(t, ordinaryCalls, "ordinary routing must not be used")
		})
	}

	notSupported := []struct {
		name, sql string
	}{
		{
			"a join against another table",
			"SELECT a.migration_name FROM multigres.stat_migration a JOIN other b ON a.migration_id = b.id",
		},
		{
			"a genuinely parameterized filter (a real client Bind, not gateway normalization)",
			"SELECT * FROM multigres.stat_migration WHERE migration_name = $1",
		},
		{
			"a comparison operator other than =",
			"SELECT * FROM multigres.stat_migration WHERE migration_name <> 'foo-move'",
		},
		{
			"a filter column other than migration_name/migration_id",
			"SELECT * FROM multigres.stat_migration WHERE lag_bytes = 100",
		},
		{
			"two ANDed conditions, not a single comparison",
			"SELECT * FROM multigres.stat_migration WHERE migration_name = 'a' AND migration_id = 1",
		},
		{
			"an aliased FROM clause",
			"SELECT * FROM multigres.stat_migration AS sm",
		},
		{
			"an aggregate target list",
			"SELECT count(*) FROM multigres.stat_migration",
		},
		{
			"a LIMIT clause",
			"SELECT * FROM multigres.stat_migration LIMIT 1",
		},
	}
	for _, tc := range notSupported {
		t.Run("not supported: "+tc.name, func(t *testing.T) {
			rpcCalled, ordinaryCalls := runCase(t, tc.sql)
			assert.False(t, rpcCalled, "the migrator RPC must not be reached")
			assert.Equal(t, int32(1), ordinaryCalls, "expected ordinary routing to the backend")
		})
	}
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
// the staleness hazard a dynamic, plan-affecting flag creates, and that
// InvalidatePlanCache (driven by config-reload notifications in Init) closes
// it: a failover-slot-creation statement admitted while the flag is on gets
// cached, keeps being served from that cache after the flag flips off (the
// "before" state asserted here, which is why the invalidation has to exist at
// all), and is re-planned under the current value and correctly rejected once
// the reload handler invalidates.
func TestSetSlotBasedReplicationEnabled_ReloadInvalidatesStaleAdmission(t *testing.T) {
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

	// The cached plan carries the admission decision, so it survives the flip
	// until something invalidates it. This is the behavior InvalidatePlanCache
	// exists to correct, not a bug being asserted as correct.
	res2, err := exec.StreamExecute(ctx, conn, nil, sql, stmt, noopCallback)
	require.NoError(t, err)
	assert.True(t, res2.CacheHit, "the admitting plan is still cached before invalidation")

	// What the config-reload consumer in Init does on every live config change.
	exec.InvalidatePlanCache()

	_, err = exec.StreamExecute(ctx, conn, nil, sql, stmt, noopCallback)
	require.Error(t, err, "after invalidation the statement must be re-planned under the current flag value")
	assert.Contains(t, err.Error(), "requires temporary=true")
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

// Tablegroup DDL is not PostgreSQL syntax: the backend must never be asked to
// prepare or describe it.
func TestTableGroupDDL_NeverReachesTheBackendForParseOrDescribe(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	state := handler.NewMultigatewayConnectionState()

	for _, sql := range []string{"CREATE TABLEGROUP commerce SHARDS ('0-80', '80-inf')", "drop tablegroup if exists commerce"} {
		require.NoError(t, exec.PrepareInTransaction(context.Background(), testConn(), state, sql, nil))
		desc, err := exec.Describe(context.Background(), testConn(), state, makePortalInfo(t, sql), nil)
		require.NoError(t, err)
		require.NotNil(t, desc, "answered locally")
		assert.Empty(t, desc.GetFields(), "no rows")
		assert.Empty(t, desc.GetParameters(), "no parameters")
	}
	assert.Zero(t, mock.streamExecuteCalls.Load())

	// Anything else still goes to the backend, including SQL that only
	// mentions the word.
	require.NoError(t, exec.PrepareInTransaction(context.Background(), testConn(), state, "SELECT tablegroup FROM t", nil))
	assert.Equal(t, int32(1), mock.streamExecuteCalls.Load())
	desc, err := exec.Describe(context.Background(), testConn(), state, makePortalInfo(t, "SELECT 1"), nil)
	require.NoError(t, err)
	assert.Nil(t, desc, "forwarded to the backend mock")
}

// Migration/connection DDL is not PostgreSQL syntax either: the backend must
// never be asked to prepare or describe it. This is the regression test for
// the finding this guards against: a client driving the extended query
// protocol (Parse, then Describe separately from Execute — the default for
// most real client libraries) used to fail at Parse with a backend syntax
// error, since Postgres has never heard of CREATE MIGRATION.
func TestMigrationDDL_NeverReachesTheBackendForParseOrDescribe(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	state := handler.NewMultigatewayConnectionState()

	for _, sql := range []string{
		"CREATE MIGRATION m CONNECTION c FOR ALL TABLES",
		"ALTER MIGRATION m PHASE IMPORT",
		"DROP MIGRATION IF EXISTS m",
		"CREATE CONNECTION c OPTIONS (host 'h')",
		"DROP CONNECTION IF EXISTS c",
	} {
		require.NoError(t, exec.PrepareInTransaction(context.Background(), testConn(), state, sql, nil))
		desc, err := exec.Describe(context.Background(), testConn(), state, makePortalInfo(t, sql), nil)
		require.NoError(t, err)
		require.NotNil(t, desc, "answered locally: %s", sql)
		assert.Empty(t, desc.GetFields(), "no rows: %s", sql)
		assert.Empty(t, desc.GetParameters(), "no parameters: %s", sql)
	}
	assert.Zero(t, mock.streamExecuteCalls.Load())

	// SHOW CONNECTION name (explicit) returns rows, so it gets its fixed column
	// list instead of an empty description, but still never reaches the backend.
	sql := "SHOW CONNECTION c"
	require.NoError(t, exec.PrepareInTransaction(context.Background(), testConn(), state, sql, nil))
	desc, err := exec.Describe(context.Background(), testConn(), state, makePortalInfo(t, sql), nil)
	require.NoError(t, err)
	require.NotNil(t, desc)
	require.Len(t, desc.GetFields(), 6)
	assert.Equal(t, "name", desc.GetFields()[0].GetName())
	assert.Zero(t, mock.streamExecuteCalls.Load())

	// The bare plural SHOW CONNECTIONS has no dedicated grammar production (it
	// parses as an ordinary SHOW of a GUC that doesn't exist — see
	// engine.IsShowConnections), so Parse is not skipped for it, but Describe
	// must still answer it the same way as the explicit form.
	sql = "SHOW CONNECTIONS"
	desc, err = exec.Describe(context.Background(), testConn(), state, makePortalInfo(t, sql), nil)
	require.NoError(t, err)
	require.NotNil(t, desc)
	require.Len(t, desc.GetFields(), 6)
	assert.Equal(t, "name", desc.GetFields()[0].GetName())

	// A recognized multigres.stat_migration query answers with its exact
	// projected columns, also without reaching the backend.
	sql = "SELECT migration_id, lag_bytes FROM multigres.stat_migration"
	desc, err = exec.Describe(context.Background(), testConn(), state, makePortalInfo(t, sql), nil)
	require.NoError(t, err)
	require.NotNil(t, desc)
	require.Len(t, desc.GetFields(), 2)
	assert.Equal(t, "migration_id", desc.GetFields()[0].GetName())
	assert.Equal(t, "lag_bytes", desc.GetFields()[1].GetName())

	// Anything else still goes to the backend, including SQL that only
	// mentions the words "migration"/"connection", and an ordinary query
	// against multigres.stat_migration outside the recognized shape (the view
	// is real, so the backend can describe it).
	require.NoError(t, exec.PrepareInTransaction(context.Background(), testConn(), state, "SELECT migration FROM t", nil))
	assert.Equal(t, int32(1), mock.streamExecuteCalls.Load())
	desc, err = exec.Describe(context.Background(), testConn(), state, makePortalInfo(t, "SELECT * FROM multigres.stat_migration JOIN t ON true"), nil)
	require.NoError(t, err)
	assert.Nil(t, desc, "forwarded to the backend mock")
}

// TestMigrationDDL_RejectsBinaryResultFormat is the regression test for the
// finding this guards against: MigrationDDL always synthesizes its rows as
// text, so silently answering Describe/Execute as if a requested binary
// format were honored would send text bytes tagged as binary. A portal
// Describe or Execute that requested a non-text format for a row-returning
// statement (SHOW CONNECTION[S], a recognized multigres.stat_migration query)
// must be rejected instead; a statement Describe (no Bind yet) and a
// non-row-returning statement (no columns to misdescribe) are unaffected.
func TestMigrationDDL_RejectsBinaryResultFormat(t *testing.T) {
	ctx := context.Background()
	state := handler.NewMultigatewayConnectionState()

	rowReturningSQL := []string{
		"SHOW CONNECTIONS",
		"SELECT migration_id FROM multigres.stat_migration",
	}

	for _, sql := range rowReturningSQL {
		t.Run("Describe rejects a binary portal request: "+sql, func(t *testing.T) {
			mock := &mockExec{}
			exec := newTestExecutor(mock)
			defer exec.planCache.Close()

			_, err := exec.Describe(ctx, testConn(), state, makePortalInfoWithResultFormats(t, sql, []int32{1}), nil)
			require.Error(t, err)
			assert.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err))
		})
		t.Run("Describe allows an explicit text portal request: "+sql, func(t *testing.T) {
			mock := &mockExec{}
			exec := newTestExecutor(mock)
			defer exec.planCache.Close()

			desc, err := exec.Describe(ctx, testConn(), state, makePortalInfoWithResultFormats(t, sql, []int32{0}), nil)
			require.NoError(t, err)
			assert.NotNil(t, desc)
		})
		t.Run("statement Describe (no Bind yet) is unaffected: "+sql, func(t *testing.T) {
			mock := &mockExec{}
			exec := newTestExecutor(mock)
			defer exec.planCache.Close()

			psi, err := preparedstatement.NewPreparedStatementInfo(&querypb.PreparedStatement{Query: sql})
			require.NoError(t, err)
			desc, err := exec.Describe(ctx, testConn(), state, nil, psi)
			require.NoError(t, err)
			assert.NotNil(t, desc)
		})
		t.Run("PortalStreamExecute rejects a binary portal request: "+sql, func(t *testing.T) {
			mock := &mockExec{}
			// A migration backend must be configured, or the stat_migration
			// SELECT case never routes to MigrationDDL in the first place
			// (that bypass itself is gated on a configured backend) — this
			// would otherwise pass vacuously via ordinary routing instead of
			// actually exercising the rejection.
			exec := newMigrationConfiguredExecutor(mock)
			defer exec.planCache.Close()

			_, err := exec.PortalStreamExecute(ctx, migrationPrivilegedTestConn(), state,
				makePortalInfoWithResultFormats(t, sql, []int32{1}), 0, false, noopCallback)
			require.Error(t, err)
			assert.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err))
			assert.Zero(t, mock.streamExecuteCalls.Load())
		})
	}

	// A non-row-returning statement has no columns to misdescribe, so a
	// binary request is moot and must not be rejected.
	t.Run("a non-row-returning statement ignores the requested format", func(t *testing.T) {
		mock := &mockExec{}
		exec := newMigrationConfiguredExecutor(mock)
		defer exec.planCache.Close()
		sql := "CREATE MIGRATION m CONNECTION c FOR ALL TABLES"

		desc, err := exec.Describe(ctx, testConn(), state, makePortalInfoWithResultFormats(t, sql, []int32{1}), nil)
		require.NoError(t, err)
		assert.NotNil(t, desc)

		_, err = exec.PortalStreamExecute(ctx, migrationPrivilegedTestConn(), state,
			makePortalInfoWithResultFormats(t, sql, []int32{1}), 0, false, noopCallback)
		require.NoError(t, err)
	})
}
