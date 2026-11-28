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

package manager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/heartbeat"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
	testutils "github.com/multigres/multigres/go/test/utils"
)

// switchableConsensus is a StateManager consensus injector whose committed
// rule names testPoolerID as the leader at a term the test can change.
type switchableConsensus struct {
	term atomic.Int64
}

func (c *switchableConsensus) status() *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: testPoolerID,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: c.term.Load()},
				LeaderId:   testPoolerID,
			}},
		},
	}
}

// hookRecorder is a beforeServingPrimary hook that records its calls and
// fails while err is set.
type hookRecorder struct {
	mu      sync.Mutex
	targets []servingstate.State
	err     error
}

func (h *hookRecorder) hook(_ context.Context, target servingstate.State) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.targets = append(h.targets, target)
	return h.err
}

func (h *hookRecorder) calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.targets)
}

func (h *hookRecorder) setErr(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.err = err
}

func (h *hookRecorder) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.targets = nil
}

func mutateTo(mode pgmode.Mode, status clustermetadatapb.PoolerServingStatus) func(*servingStateMutation) {
	return func(s *servingStateMutation) {
		s.PostgresMode = mode
		s.ServingStatus = status
	}
}

// TestBeforeServingPrimary_RunsOnlyOnTransitionToServingPrimary pins when the
// hook runs: on a transition whose target is PRIMARY and SERVING from a state
// that was not both, and on nothing else.
func TestBeforeServingPrimary_RunsOnlyOnTransitionToServingPrimary(t *testing.T) {
	const (
		serving  = clustermetadatapb.PoolerServingStatus_SERVING
		draining = clustermetadatapb.PoolerServingStatus_DRAINING
		disabled = clustermetadatapb.PoolerServingStatus_DISABLED
	)
	tests := []struct {
		name string
		// setup runs before the transition under test; nil leaves nothing
		// fanned out yet.
		setup    func(*servingStateMutation)
		bumpTerm bool
		target   func(*servingStateMutation)
		wantRun  bool
	}{
		{name: "from nothing to primary serving", target: mutateTo(pgmode.Primary, serving), wantRun: true},
		{name: "from replica to primary serving", setup: mutateTo(pgmode.InRecovery, serving), target: mutateTo(pgmode.Primary, serving), wantRun: true},
		{name: "from primary draining to primary serving", setup: mutateTo(pgmode.Primary, draining), target: mutateTo(pgmode.Primary, serving), wantRun: true},
		{name: "from primary disabled to primary serving", setup: mutateTo(pgmode.Primary, disabled), target: mutateTo(pgmode.Primary, serving), wantRun: true},
		{name: "primary serving with a new rule", setup: mutateTo(pgmode.Primary, serving), bumpTerm: true, target: mutateTo(pgmode.Primary, serving)},
		{name: "to replica serving", target: mutateTo(pgmode.InRecovery, serving)},
		{name: "to primary draining", target: mutateTo(pgmode.Primary, draining)},
		{name: "to primary disabled", target: mutateTo(pgmode.Primary, disabled)},
		{name: "from primary serving to draining", setup: mutateTo(pgmode.Primary, serving), target: mutateTo(pgmode.Primary, draining)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := &switchableConsensus{}
			cs.term.Store(1)
			comp := &testComponent{}
			ssm := NewStateManager(newTestLogger(), newTestRecord(clustermetadatapb.PoolerType_REPLICA, disabled), cs.status, comp)
			h := &hookRecorder{}
			ssm.SetBeforeServingPrimary(h.hook)

			ctx := newActionLockedCtx(t)
			if tt.setup != nil {
				require.NoError(t, ssm.Mutate(ctx, tt.setup))
			}
			h.reset()
			fanOuts := comp.callCount
			if tt.bumpTerm {
				cs.term.Store(2)
			}
			require.NoError(t, ssm.Mutate(ctx, tt.target))

			if !tt.wantRun {
				assert.Zero(t, h.calls())
				if tt.bumpTerm {
					assert.Equal(t, fanOuts+1, comp.callCount, "a rule-only bump still fans out")
				}
				return
			}
			require.Equal(t, 1, h.calls())
			got := h.targets[0]
			assert.Equal(t, servingstate.RoutingRolePrimary, got.Routing.Role)
			assert.Equal(t, serving, got.ServingStatus)
			assert.Equal(t, int64(1), got.Routing.Rule.GetCoordinatorTerm(), "the hook receives the target's rule")
			assert.Equal(t, fanOuts+1, comp.callCount, "the transition fans out after the hook succeeds")
		})
	}
}

// TestBeforeServingPrimary_FailureBlocksUntilFixDriftSucceeds checks that a
// failing hook changes nothing, that the monitor's drift check still sees the
// drift so it retries, and that a later fixDrift completes the transition once
// the hook succeeds.
func TestBeforeServingPrimary_FailureBlocksUntilFixDriftSucceeds(t *testing.T) {
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING)
	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus, comp)
	h := &hookRecorder{}
	ssm.SetBeforeServingPrimary(h.hook)
	ctx := newActionLockedCtx(t)

	// Serving as a replica first, as a pooler that opened in recovery.
	require.NoError(t, ssm.Mutate(ctx, mutateTo(pgmode.InRecovery, clustermetadatapb.PoolerServingStatus_SERVING)))
	require.Equal(t, 1, comp.callCount)
	require.False(t, ssm.hasDrift(pgmode.InRecovery, false, false))

	// Promotion: postgres is out of recovery, the hook fails.
	h.setErr(errors.New("catalog lock timeout"))
	err := ssm.Mutate(ctx, mutateTo(pgmode.Primary, clustermetadatapb.PoolerServingStatus_SERVING))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not serving as primary: catalog lock timeout")

	// Nothing moved: no fan-out, the replica state is still the last one
	// delivered, the record and the cached recovery mode are unchanged.
	assert.Equal(t, 1, comp.callCount)
	assert.Equal(t, servingstate.RoutingRoleReplica, ssm.lastFannedOut.Routing.Role)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, r.Type())
	assert.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA, ssm.RoutingRole())
	// The monitor still sees the drift, which is what makes it retry.
	assert.True(t, ssm.hasDrift(pgmode.Primary, false, false))

	// The monitor retries every tick, and each retry fails the same way.
	for range 3 {
		require.Error(t, ssm.fixDrift(ctx, pgmode.Primary, false, false))
	}
	assert.Equal(t, 1, comp.callCount)
	assert.Equal(t, 4, h.calls())

	// The hook succeeds: the next drift fix completes the transition.
	h.setErr(nil)
	require.True(t, ssm.hasDrift(pgmode.Primary, false, false))
	require.NoError(t, ssm.fixDrift(ctx, pgmode.Primary, false, false))
	assert.Equal(t, 2, comp.callCount)
	assert.Equal(t, servingstate.RoutingRolePrimary, comp.lastRole)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, comp.lastStatus)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.False(t, ssm.hasDrift(pgmode.Primary, false, false))
	assert.Equal(t, 5, h.calls())
}

// TestBeforeServingPrimary_HoldReleaseRetries covers the release of a serving
// hold: PRIMARY + DRAINING back to SERVING runs the hook, and a failure leaves
// the pooler draining with the drift still reported.
func TestBeforeServingPrimary_HoldReleaseRetries(t *testing.T) {
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus, comp)
	h := &hookRecorder{}
	ssm.SetBeforeServingPrimary(h.hook)
	ctx := newActionLockedCtx(t)

	require.NoError(t, ssm.Mutate(ctx, mutateTo(pgmode.Primary, clustermetadatapb.PoolerServingStatus_SERVING)))
	require.NoError(t, ssm.ForceMigrationHold(ctx))
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DRAINING, r.ServingStatus())

	h.setErr(errors.New("boom"))
	require.Error(t, ssm.ReconcileMigrationHold(ctx, false))
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DRAINING, r.ServingStatus())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DRAINING, comp.lastStatus)
	assert.True(t, ssm.hasDrift(pgmode.Primary, false, false))

	h.setErr(nil)
	require.NoError(t, ssm.fixDrift(ctx, pgmode.Primary, false, false))
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
	assert.Equal(t, 3, h.calls())
}

// TestBeforeServingPrimary_RunsWithoutTheStateManagerLock checks that
// readers of the routing state, such as Status, do not wait for the hook: it
// runs before Mutate takes the StateManager's lock.
func TestBeforeServingPrimary_RunsWithoutTheStateManagerLock(t *testing.T) {
	ssm := NewStateManager(newTestLogger(),
		newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING),
		selfLeaderConsensusStatus, &testComponent{})
	entered, release := make(chan struct{}), make(chan struct{})
	ssm.SetBeforeServingPrimary(func(context.Context, servingstate.State) error {
		close(entered)
		<-release
		return nil
	})

	ctx := newActionLockedCtx(t)
	done := make(chan error, 1)
	go func() {
		done <- ssm.Mutate(ctx, mutateTo(pgmode.Primary, clustermetadatapb.PoolerServingStatus_SERVING))
	}()
	<-entered

	read := make(chan clustermetadatapb.RoutingRole, 1)
	go func() { read <- ssm.RoutingRole() }()
	select {
	case role := <-read:
		assert.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA, role, "readers see the state before the transition")
	case <-time.After(5 * time.Second):
		t.Fatal("RoutingRole waited for the hook")
	}

	close(release)
	require.NoError(t, <-done)
	assert.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, ssm.RoutingRole())
}

// TestBeforeServingPrimary_RoutingChangeDuringHookRetries checks that a
// transition fails, changing nothing, when the consensus snapshot moves while
// the hook runs, so the pooler never serves under a rule the hook did not
// prepare for; the next fixDrift prepares for the new rule and completes it.
func TestBeforeServingPrimary_RoutingChangeDuringHookRetries(t *testing.T) {
	cs := &switchableConsensus{}
	cs.term.Store(1)
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING)
	ssm := NewStateManager(newTestLogger(), r, cs.status, comp)
	var rules []int64
	ssm.SetBeforeServingPrimary(func(_ context.Context, target servingstate.State) error {
		rules = append(rules, target.Routing.Rule.GetCoordinatorTerm())
		if len(rules) == 1 {
			cs.term.Store(2) // a new committed rule lands while the hook runs
		}
		return nil
	})
	ctx := newActionLockedCtx(t)
	require.NoError(t, ssm.Mutate(ctx, mutateTo(pgmode.InRecovery, clustermetadatapb.PoolerServingStatus_SERVING)))

	err := ssm.Mutate(ctx, mutateTo(pgmode.Primary, clustermetadatapb.PoolerServingStatus_SERVING))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "routing state changed while preparing to serve")
	assert.Equal(t, 1, comp.callCount, "nothing fans out")
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, r.Type())
	assert.True(t, ssm.hasDrift(pgmode.Primary, false, false))

	require.NoError(t, ssm.fixDrift(ctx, pgmode.Primary, false, false))
	assert.Equal(t, []int64{1, 2}, rules, "the retry prepares for the new rule")
	assert.Equal(t, servingstate.RoutingRolePrimary, comp.lastRole)
	assert.Equal(t, int64(2), r.RoutingState().GetRule().GetCoordinatorTerm())
}

// addCatalogUpgradeFromFormat1 registers a catalog shard whose catalog is at
// format 1 with nothing else in place, so the hook's ensure runs everything,
// including a migration that advances routing_version (and so publishes under
// the rule). Every other statement succeeds and is recorded.
func addCatalogUpgradeFromFormat1(m *mock.QueryService, record func(string)) {
	addShardRefEnsure(m, record)
	m.AddQueryPatternWithCallback(catalogDefinitionReadPattern,
		mock.MakeQueryResult([]string{"format_version", "definition_hash"}, [][]any{{1, nil}}), record)
	m.AddQueryPatternWithCallback(catalogStatePresentPattern,
		mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}), record)
	m.AddQueryPatternWithCallback("^SELECT format_version FROM multigres.catalog_state FOR UPDATE$",
		mock.MakeQueryResult([]string{"format_version"}, [][]any{{1}}), record)
	m.AddQueryPatternWithCallback("SET routing_version",
		mock.MakeQueryResult([]string{"routing_version", "plan_epoch"}, [][]any{{int64(5), int64(0)}}), record)
	m.AddQueryPatternWithCallback(connectionsProbePattern,
		mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}), record)
	m.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), record)
}

// TestBeforeServingPrimary_RealEnsureThroughMutate runs the real hook through
// a real StateManager's Mutate, over a full catalog upgrade, and checks it
// receives the target's rule and completes the transition. The timeout turns a
// hang into a failure.
func TestBeforeServingPrimary_RealEnsureThroughMutate(t *testing.T) {
	pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	var mu sync.Mutex
	var queries []string
	addCatalogUpgradeFromFormat1(m, func(q string) {
		mu.Lock()
		queries = append(queries, q)
		mu.Unlock()
	})

	comp := &testComponent{}
	pm.stateManager = NewStateManager(newTestLogger(),
		newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING),
		selfLeaderConsensusStatus, comp)
	var gotRule *clustermetadatapb.RuleNumber
	pm.stateManager.SetBeforeServingPrimary(func(ctx context.Context, target servingstate.State) error {
		gotRule = target.Routing.Rule
		return pm.beforeServingPrimary(ctx, target)
	})

	ctx := newActionLockedCtx(t)
	done := make(chan error, 1)
	go func() {
		done <- pm.stateManager.Mutate(ctx, mutateTo(pgmode.Primary, clustermetadatapb.PoolerServingStatus_SERVING))
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Mutate did not return")
	}

	assert.Equal(t, int64(1), gotRule.GetCoordinatorTerm())
	assert.Equal(t, servingstate.RoutingRolePrimary, comp.lastRole)
	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, queries, catalogMigrations[1].postStatements[0], "the full ensure ran")
	assert.Equal(t, recordCatalogDefinitionSQL, queries[len(queries)-1], "the hash is recorded last")
}

// TestBeforeServingPrimary_UnmanagedSkips checks that an unmanaged pooler,
// whose postgres is external, runs no sidecar ensure.
func TestBeforeServingPrimary_UnmanagedSkips(t *testing.T) {
	pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	pm.record = newRecordFromProto(&clustermetadatapb.Multipooler{
		Id:             testPoolerID,
		LeadershipMode: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED,
		ShardRef: &clustermetadatapb.ShardRef{
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	})
	var ran bool
	m.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), func(string) { ran = true })

	require.NoError(t, pm.beforeServingPrimary(t.Context(), servingstate.State{
		Routing:       servingstate.RoutingState{Role: servingstate.RoutingRolePrimary},
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}))
	assert.False(t, ran, "an unmanaged pooler must not touch the external postgres")
}

// TestEnsureSidecarSchemas_FastPath checks which catalog states skip the full
// ensure. A mock without a pattern for a query fails it, so where only the
// reads are registered, a passing ensure proves it wrote nothing.
func TestEnsureSidecarSchemas_FastPath(t *testing.T) {
	definition := func(format int, hash any) *sqltypes.Result {
		return mock.MakeQueryResult([]string{"format_version", "definition_hash"}, [][]any{{format, hash}})
	}

	t.Run("matching format and hash only reads", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		addSidecarsInPlace(m)
		require.NoError(t, pm.ensureSidecarSchemas(t.Context(), nil))
	})

	t.Run("newer format is left untouched", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnce(catalogDefinitionReadPattern, definition(latestCatalogFormat()+1, "a newer binary's hash"))
		addSidecarsInPlace(m)
		require.NoError(t, pm.ensureSidecarSchemas(t.Context(), nil))
		assert.NoError(t, m.ExpectationsWereMet())
	})

	for _, tt := range []struct {
		name string
		read func(m *mock.QueryService)
	}{
		{name: "different hash", read: func(m *mock.QueryService) {
			m.AddQueryPatternOnce(catalogDefinitionReadPattern, definition(latestCatalogFormat(), "another binary's hash"))
		}},
		{name: "no hash yet", read: func(m *mock.QueryService) {
			m.AddQueryPatternOnce(catalogDefinitionReadPattern, definition(latestCatalogFormat(), nil))
		}},
		{name: "older format", read: func(m *mock.QueryService) {
			m.AddQueryPatternOnce(catalogDefinitionReadPattern, definition(latestCatalogFormat()-1, catalogDefinitionHash))
		}},
		{name: "no catalog_state", read: addCatalogDefinitionMissing},
	} {
		t.Run(tt.name+" runs the full ensure", func(t *testing.T) {
			pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
			tt.read(m)
			var queries []string
			addSidecarsInPlace(m)
			m.AddQueryPatternWithCallback(catalogStatePresentPattern,
				mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}), nil)
			m.AddQueryPatternWithCallback("^SELECT format_version FROM multigres.catalog_state FOR UPDATE$",
				mock.MakeQueryResult([]string{"format_version"}, [][]any{{latestCatalogFormat()}}), nil)
			m.AddQueryPatternWithCallback(connectionsProbePattern,
				mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}), nil)
			m.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), func(q string) { queries = append(queries, q) })

			require.NoError(t, pm.ensureSidecarSchemas(t.Context(), nil))
			require.NoError(t, m.ExpectationsWereMet())
			assert.Contains(t, queries, "SELECT pg_advisory_xact_lock($1)")
			assert.Contains(t, queries, catalogTables[0], "catalogTables ran")
			assert.Equal(t, recordCatalogDefinitionSQL, queries[len(queries)-1], "the hash is recorded last")
		})
	}

	t.Run("a failed full ensure records no hash", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		addCatalogDefinitionMissing(m)
		addSidecarsInPlace(m)
		m.AddQueryPatternOnceWithError("pg_advisory_xact_lock", errors.New("lock timeout"))
		var wrote bool
		m.AddQueryPatternWithCallback(catalogDefinitionWritePattern, mock.MakeQueryResult(nil, nil), func(string) { wrote = true })

		require.Error(t, pm.ensureSidecarSchemas(t.Context(), nil))
		assert.False(t, wrote)
	})
}

// TestCatalogDefinitionHash_CoversTheEnsure checks that the hash changes with
// each list it covers, so a binary that applies something new never mistakes
// a catalog for current.
func TestCatalogDefinitionHash_CoversTheEnsure(t *testing.T) {
	assert.Regexp(t, regexp.MustCompile(`^[0-9a-f]{64}$`), catalogDefinitionHash)
	for _, list := range []*[]string{&catalogTables, &catalogSeed} {
		saved := *list
		*list = append(slices.Clone(saved), "SELECT 1")
		changed := computeCatalogDefinitionHash()
		*list = saved
		assert.NotEqual(t, catalogDefinitionHash, changed)
	}
	assert.Equal(t, catalogDefinitionHash, computeCatalogDefinitionHash())
}

// ----------------------------------------------------------------------------
// Real postgres: the up-to-date ensure assigns no transaction id
// ----------------------------------------------------------------------------

// startTestPostgres initdbs and starts a postgres reachable only over a unix
// socket, stopped at cleanup, and returns a connection factory.
func startTestPostgres(t *testing.T) func() *client.Conn {
	t.Helper()
	if !testutils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL binaries not found")
	}
	// /tmp keeps the socket path under the macOS limit.
	dir, err := os.MkdirTemp("/tmp", "sidecar_ensure_test-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	data, sockets := filepath.Join(dir, "data"), filepath.Join(dir, "sockets")
	require.NoError(t, os.MkdirAll(sockets, 0o700))
	env := append(os.Environ(), "LC_ALL=C")

	initdb := exec.Command("initdb", "-D", data, "-U", "postgres", "--no-instructions", "-A", "trust")
	initdb.Env = env
	out, err := initdb.CombinedOutput()
	require.NoError(t, err, "initdb: %s", out)

	start := exec.Command("pg_ctl", "start", "-D", data, "-l", filepath.Join(dir, "postgres.log"), "-w", "-t", "30",
		"-o", "-c listen_addresses= -c unix_socket_directories="+sockets)
	start.Env = env
	out, err = start.CombinedOutput()
	require.NoError(t, err, "pg_ctl start: %s", out)
	t.Cleanup(func() {
		stop := exec.Command("pg_ctl", "stop", "-D", data, "-m", "fast")
		stop.Env = env
		_ = stop.Run()
	})

	return func() *client.Conn {
		conn, err := client.Connect(t.Context(), t.Context(), &client.Config{
			SocketFile: filepath.Join(sockets, ".s.PGSQL.5432"),
			User:       "postgres",
			Database:   "postgres",
		})
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		return conn
	}
}

// sessionQueryService runs every query of the InternalQueryService on one
// connection. With observed set, the test has opened a transaction on it, and
// a transaction the code under test begins is a savepoint inside it, so
// everything the code does happens in the test's transaction, where
// pg_current_xact_id_if_assigned shows whether any of it assigned a
// transaction id.
type sessionQueryService struct {
	conn     *client.Conn
	observed bool
	next     int
}

var _ executor.InternalQueryService = (*sessionQueryService)(nil)

func firstResult(results []*sqltypes.Result, err error) (*sqltypes.Result, error) {
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return &sqltypes.Result{}, nil
	}
	return results[0], nil
}

func (s *sessionQueryService) Query(ctx context.Context, q string) (*sqltypes.Result, error) {
	return firstResult(s.conn.Query(ctx, q))
}

func (s *sessionQueryService) QueryArgs(ctx context.Context, q string, args ...any) (*sqltypes.Result, error) {
	return firstResult(s.conn.QueryArgs(ctx, q, args...))
}

func (s *sessionQueryService) QueryMultiStatement(ctx context.Context, q string) error {
	_, err := s.conn.Query(ctx, q)
	return err
}

func (s *sessionQueryService) QueryAdmin(ctx context.Context, q string) (*sqltypes.Result, error) {
	return s.Query(ctx, q)
}

func (s *sessionQueryService) QueryAdminArgs(ctx context.Context, q string, args ...any) (*sqltypes.Result, error) {
	return s.QueryArgs(ctx, q, args...)
}

func (s *sessionQueryService) QueryAdminMultiStatement(ctx context.Context, q string) error {
	return s.QueryMultiStatement(ctx, q)
}

func (s *sessionQueryService) QueryAdminNoQueryText(ctx context.Context, q string) (*sqltypes.Result, error) {
	return s.Query(ctx, q)
}

func (s *sessionQueryService) Begin(ctx context.Context) (executor.InternalTx, error) {
	tx := &sessionTx{conn: s.conn, begin: "BEGIN", commit: "COMMIT", rollback: "ROLLBACK"}
	if s.observed {
		s.next++
		name := fmt.Sprintf("s%d", s.next)
		tx.begin = "SAVEPOINT " + name
		tx.commit = "RELEASE SAVEPOINT " + name
		tx.rollback = "ROLLBACK TO SAVEPOINT " + name + "; RELEASE SAVEPOINT " + name
	}
	if _, err := s.conn.Query(ctx, tx.begin); err != nil {
		return nil, err
	}
	return tx, nil
}

func (s *sessionQueryService) BeginAdmin(ctx context.Context) (executor.InternalTx, error) {
	return s.Begin(ctx)
}

type sessionTx struct {
	conn                    *client.Conn
	begin, commit, rollback string
	finished                bool
}

func (tx *sessionTx) Query(ctx context.Context, q string) (*sqltypes.Result, error) {
	return firstResult(tx.conn.QueryArgs(ctx, q))
}

func (tx *sessionTx) QueryArgs(ctx context.Context, q string, args ...any) (*sqltypes.Result, error) {
	return firstResult(tx.conn.QueryArgs(ctx, q, args...))
}

func (tx *sessionTx) Commit(ctx context.Context) error {
	tx.finished = true
	_, err := tx.conn.Query(ctx, tx.commit)
	return err
}

func (tx *sessionTx) Rollback(ctx context.Context) error {
	if tx.finished {
		return nil
	}
	tx.finished = true
	_, err := tx.conn.Query(ctx, tx.rollback)
	return err
}

// sessionController hands the manager a sessionQueryService.
type sessionController struct {
	mockPoolerController
	qs executor.InternalQueryService
}

func (c *sessionController) InternalQueryService() executor.InternalQueryService { return c.qs }

// TestEnsureSidecarSchemas_UpToDateAssignsNoTransactionID proves, against a
// real postgres, that the ensure a transition to serving primary runs writes
// nothing on a shard that is already up to date: run inside the test's own
// transaction, it leaves pg_current_xact_id_if_assigned() NULL. A transaction
// id is what any write (a row, a row lock, catalog DDL, a GRANT that changes
// nothing) assigns, and what makes a commit wait for a synchronous standby, so
// this checks the whole ensure at once rather than statement by statement. The
// same measurement after clearing the recorded hash shows an id, so it is not
// vacuous.
func TestEnsureSidecarSchemas_UpToDateAssignsNoTransactionID(t *testing.T) {
	connect := startTestPostgres(t)
	ctx := t.Context()

	// Bootstrap what createSidecarSchema creates before the ensure, plus the
	// probe role, so every step of the ensure has something to check.
	setup := &sessionQueryService{conn: connect()}
	for _, stmt := range []string{
		"CREATE SCHEMA multigres",
		"GRANT USAGE ON SCHEMA multigres TO PUBLIC",
		heartbeat.CreateTableSQL,
		"CREATE ROLE " + heartbeat.ProbeRole,
	} {
		_, err := setup.Query(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	for _, tt := range []struct {
		name       string
		tableGroup string
	}{
		{name: "catalog shard", tableGroup: constants.DefaultTableGroup},
		{name: "other shard", tableGroup: "commerce"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pm, _ := newTestManagerWithMock(t, tt.tableGroup, constants.DefaultShard)

			// The first ensure creates everything, for real.
			pm.qsc = &sessionController{qs: setup}
			require.NoError(t, pm.ensureSidecarSchemas(ctx, nil))

			observe := func() (assigned bool) {
				t.Helper()
				conn := connect()
				_, err := conn.Query(ctx, "BEGIN")
				require.NoError(t, err)
				pm.qsc = &sessionController{qs: &sessionQueryService{conn: conn, observed: true}}
				require.NoError(t, pm.ensureSidecarSchemas(ctx, nil))
				res, err := conn.Query(ctx, "SELECT pg_current_xact_id_if_assigned() IS NOT NULL")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "ROLLBACK")
				require.NoError(t, err)
				require.Len(t, res, 1)
				require.Len(t, res[0].Rows, 1)
				return string(res[0].Rows[0].Values[0]) == "t"
			}

			for range 3 {
				assert.False(t, observe(), "an up-to-date ensure must not assign a transaction id")
			}

			// Control: the same measurement sees the write of a full ensure.
			// Off the catalog shard, an out-of-place backend_vpid is the write.
			if tt.tableGroup == constants.DefaultTableGroup {
				_, err := setup.Query(ctx, "UPDATE multigres.catalog_state SET definition_hash = NULL")
				require.NoError(t, err)
			} else {
				_, err := setup.Query(ctx, "ALTER TABLE multigres.backend_vpid RESET (autovacuum_vacuum_threshold)")
				require.NoError(t, err)
			}
			assert.True(t, observe(), "the control ensure must assign a transaction id")
			// It ran in a rolled-back transaction, so the shard is still out of
			// date until a real ensure repairs it.
			pm.qsc = &sessionController{qs: setup}
			require.NoError(t, pm.ensureSidecarSchemas(ctx, nil))
			assert.False(t, observe())
		})
	}
}
