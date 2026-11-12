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

package manager

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

const (
	testTableGroupDB = "testdb"
	testOpID         = "6b9c6d1e-9c4e-4f2d-8c0a-1d2e3f4a5b6c"
	testTableGroupID = int64(7)
	testCreatedAt    = int64(1793268000000000) // 2026-10-29 10:00:00 UTC, in microseconds since the epoch
)

// tableGroupTestEnv is a manager on the catalog shard with a mock query
// service and an in-memory topology, plus the SQL it ran in order.
type tableGroupTestEnv struct {
	pm      *MultipoolerManager
	qs      *mock.QueryService
	ts      topoclient.Store
	topo    *memorytopo.Factory // SetError makes every topology read fail
	queries []string
}

// newTableGroupTestEnv builds a ready manager serving the given shard of the
// given tablegroup in testdb, as its elected primary: the state manager sees
// postgres out of recovery and this pooler as the active consensus leader.
func newTableGroupTestEnv(t *testing.T, tableGroup, shard string) *tableGroupTestEnv {
	t.Helper()
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "cell1", "cell2")
	env := &tableGroupTestEnv{qs: mock.NewQueryService(), ts: ts, topo: factory}
	logger := slog.New(slog.DiscardHandler)
	record := newRecordFromProto(&clustermetadatapb.Multipooler{
		ShardRef: &clustermetadatapb.ShardRef{Database: testTableGroupDB, TableGroup: tableGroup, Shard: shard},
	})
	env.pm = &MultipoolerManager{
		logger:       logger,
		serviceID:    testPoolerID,
		qsc:          &mockPoolerController{queryService: env.qs},
		topoClient:   env.ts,
		config:       &Config{},
		state:        ManagerStateReady,
		record:       record,
		stateManager: NewStateManager(logger, record, selfLeaderConsensusStatus),
	}
	env.pm.stateManager.pgMode = pgmode.Primary
	return env
}

func newCatalogTestEnv(t *testing.T) *tableGroupTestEnv {
	t.Helper()
	return newTableGroupTestEnv(t, constants.DefaultTableGroup, constants.DefaultShard)
}

// expect registers a pattern whose match is appended to env.queries. Patterns
// are not consumed: a query is answered by the first registered pattern it
// matches, for as long as the environment lives.
func (env *tableGroupTestEnv) expect(pattern string, columns []string, rows [][]any) {
	env.qs.AddQueryPatternWithCallback(pattern, mock.MakeQueryResult(columns, rows), func(q string) {
		env.queries = append(env.queries, q)
	})
}

// nameTaken registers the claim of a name that already has a row: the insert
// conflicts and returns nothing, and the lookup returns the existing row.
func (env *tableGroupTestEnv) nameTaken(id int64, state string) {
	env.expect("ON CONFLICT \\(name\\) DO NOTHING", []string{"tablegroup_id"}, nil)
	env.expect("FROM multigres.tablegroup WHERE name = \\$1 FOR UPDATE", []string{"tablegroup_id", "state"}, [][]any{{id, state}})
}

// expectLock registers what writeCatalog does before its writer runs: the
// catalog primary check and the catalog_state lock.
func (env *tableGroupTestEnv) expectLock() {
	env.expectPrimary()
	env.expect("FROM multigres.catalog_state FOR UPDATE", []string{"routing_version", "plan_epoch"}, [][]any{{int64(4), int64(2)}})
}

// expectAdvance registers the routing_version increment, returning version.
func (env *tableGroupTestEnv) expectAdvance(version int64) {
	env.expect("UPDATE multigres.catalog_state SET routing_version", []string{"routing_version", "plan_epoch"}, [][]any{{version, int64(0)}})
}

// expectCommit registers COMMIT, so it shows in env.queries.
func (env *tableGroupTestEnv) expectCommit() {
	env.expect("^COMMIT$", nil, nil)
}

// queriesMatching returns the recorded queries that contain substr.
func (env *tableGroupTestEnv) queriesMatching(substr string) []string {
	var out []string
	for _, q := range env.queries {
		if strings.Contains(q, substr) {
			out = append(out, q)
		}
	}
	return out
}

func (env *tableGroupTestEnv) expectPrimary() {
	env.expect("SELECT pg_is_in_recovery", []string{"pg_is_in_recovery"}, [][]any{{"f"}})
}

// addPooler registers a pooler record in topology.
func (env *tableGroupTestEnv) addPooler(t *testing.T, cell, name, tableGroup, shard string, tableGroupID int64, role clustermetadatapb.RoutingRole, serving clustermetadatapb.PoolerServingStatus, lifecycle clustermetadatapb.PoolerLifecycleStatus) {
	t.Helper()
	mp := &clustermetadatapb.Multipooler{
		Id:              &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: cell, Name: name},
		ShardRef:        &clustermetadatapb.ShardRef{Database: testTableGroupDB, TableGroup: tableGroup, Shard: shard},
		TablegroupId:    tableGroupID,
		ServingStatus:   serving,
		LifecycleStatus: &clustermetadatapb.PoolerLifecycle{Status: lifecycle},
	}
	if role != clustermetadatapb.RoutingRole_ROUTING_ROLE_UNKNOWN {
		mp.RoutingState = &clustermetadatapb.RoutingState{Role: role}
	}
	require.NoError(t, env.ts.CreateMultipooler(t.Context(), mp))
}

// addServingPrimary registers a serving primary for one shard of a tablegroup.
func (env *tableGroupTestEnv) addServingPrimary(t *testing.T, cell, tableGroup, shard string, tableGroupID int64) {
	t.Helper()
	env.addPooler(t, cell, tableGroup+"-"+shard, tableGroup, shard, tableGroupID,
		clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)
}

func operationColumnsList() []string {
	return []string{"operation_id", "kind", "state", "request", "cancel_requested", "last_error", "created_at", "updated_at"}
}

func creatingOperationRow(createdAt int64, cancelRequested bool) []any {
	return []any{
		testOpID, operationKindCreateTableGroup, operationStateCreating,
		`{"tablegroup_id":7,"name":"commerce","shards":["0-80","80-inf"]}`, cancelRequested, "", createdAt, createdAt,
	}
}

func TestValidateTableGroupName(t *testing.T) {
	for _, name := range []string{"a", "commerce", "shop-2", "a1", "abcdefghijklmnopqrstuvwxy"} {
		assert.NoError(t, validateTableGroupName(name), name)
	}
	for _, name := range []string{"", "Commerce", "commerce_1", "-shop", "shop-", "abcdefghijklmnopqrstuvwxyz", "a b", "défaut"} {
		err := validateTableGroupName(name)
		require.Error(t, err, name)
		assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err), name)
	}
}

func TestCreateTableGroup_RecordsIntent(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expectLock()
	env.expect("INSERT INTO multigres.tablegroup", []string{"tablegroup_id"}, [][]any{{testTableGroupID}})
	env.expect("INSERT INTO multigres.shard", nil, nil)
	env.expect("INSERT INTO multigres.operation", nil, nil)
	env.expectCatalogRows()
	env.expectAdvance(5)
	env.expectCommit()

	// Shards are given out of order; the catalog gets them in canonical order.
	resp, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{
		Name: "commerce", Shards: []string{"80-inf", "0-80"},
	})
	require.NoError(t, err)
	assert.Equal(t, testTableGroupID, resp.TablegroupId)
	assert.False(t, resp.AlreadyExists)
	_, err = uuid.Parse(resp.OperationId)
	require.NoError(t, err, "operation id is a UUID")

	// The RPC checks for the catalog primary and writeCatalog checks again;
	// the catalog_state lock comes before any other catalog row, the snapshot
	// the write would commit is checked, and the version advances once,
	// inside the transaction.
	want := []string{
		"SELECT pg_is_in_recovery()",
		"SELECT pg_is_in_recovery()",
		"SELECT routing_version, plan_epoch FROM multigres.catalog_state FOR UPDATE",
		"INSERT INTO multigres.tablegroup (name, state) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING RETURNING tablegroup_id",
		"INSERT INTO multigres.shard (tablegroup_id, shard_name, range_start, range_end, state) VALUES ($1, $2, decode($3, 'hex'), decode($4, 'hex'), $5)",
		"INSERT INTO multigres.shard (tablegroup_id, shard_name, range_start, range_end, state) VALUES ($1, $2, decode($3, 'hex'), decode($4, 'hex'), $5)",
		"INSERT INTO multigres.operation (operation_id, kind, state, request) VALUES ($1, $2, $3, $4::jsonb)",
		snapshotTableGroupsQuery, snapshotShardsQuery, snapshotTablesQuery,
		snapshotShardIndicesQuery, snapshotShardKeysQuery, snapshotShardKeyColumnsQuery, selectRoutingRulesSQL,
		"UPDATE multigres.catalog_state SET routing_version = routing_version + 1 RETURNING routing_version, plan_epoch",
		"COMMIT",
	}
	assert.Equal(t, want, env.queries)
}

func TestCreateTableGroup_DefaultsToSingleShard(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expectLock()
	env.expect("INSERT INTO multigres.tablegroup", []string{"tablegroup_id"}, [][]any{{testTableGroupID}})
	env.expect("INSERT INTO multigres.shard", nil, nil)
	env.expect("INSERT INTO multigres.operation", nil, nil)
	env.expectCatalogRows()
	env.expectAdvance(5)

	_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
	require.NoError(t, err)
	shardInserts := 0
	for _, q := range env.queries {
		if strings.HasPrefix(q, "INSERT INTO multigres.shard") {
			shardInserts++
		}
	}
	assert.Equal(t, 1, shardInserts)
}

func TestCreateTableGroup_ExistingActive(t *testing.T) {
	t.Run("without IF NOT EXISTS is an error", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expectLock()
		env.nameTaken(testTableGroupID, tableGroupStateActive)

		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_ALREADY_EXISTS, mterrors.Code(err))
	})
	t.Run("with IF NOT EXISTS is a no-op", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expectLock()
		env.nameTaken(testTableGroupID, tableGroupStateActive)

		resp, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce", IfNotExists: true})
		require.NoError(t, err)
		assert.True(t, resp.AlreadyExists)
		assert.Equal(t, testTableGroupID, resp.TablegroupId)
		assert.Empty(t, resp.OperationId)
		assert.Len(t, env.queries, 5, "nothing written after the claim conflicts")
		assert.Empty(t, env.queriesMatching("routing_version + 1"), "an unchanged catalog keeps its version")
	})
	t.Run("the default tablegroup is always active", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expectLock()
		env.nameTaken(int64(1), tableGroupStateActive)

		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "default"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_ALREADY_EXISTS, mterrors.Code(err))
	})
}

func TestCreateTableGroup_ExistingCreating(t *testing.T) {
	t.Run("same layout joins the in-flight operation", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expectLock()
		env.nameTaken(testTableGroupID, tableGroupStateCreating)
		env.expect("SELECT shard_name FROM multigres.shard", []string{"shard_name"}, [][]any{{"0-80"}, {"80-inf"}})
		env.expect("SELECT operation_id FROM multigres.operation", []string{"operation_id"}, [][]any{{testOpID}})

		resp, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{
			Name: "commerce", Shards: []string{"80-inf", "0-80"},
		})
		require.NoError(t, err)
		assert.Equal(t, testOpID, resp.OperationId)
		assert.Equal(t, testTableGroupID, resp.TablegroupId)
		assert.Empty(t, env.queriesMatching("routing_version + 1"), "joining a creation writes nothing")
	})
	t.Run("different layout is a conflict", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expectLock()
		env.nameTaken(testTableGroupID, tableGroupStateCreating)
		env.expect("SELECT shard_name FROM multigres.shard", []string{"shard_name"}, [][]any{{"0-80"}, {"80-inf"}})

		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{
			Name: "commerce", Shards: []string{"0-inf"},
		})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
		assert.Contains(t, err.Error(), "already being created with shards [0-80 80-inf]")
	})
	t.Run("IF NOT EXISTS joins whatever the layout", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expectLock()
		env.nameTaken(testTableGroupID, tableGroupStateCreating)
		env.expect("SELECT operation_id FROM multigres.operation", []string{"operation_id"}, [][]any{{testOpID}})

		resp, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{
			Name: "commerce", Shards: []string{"0-inf"}, IfNotExists: true,
		})
		require.NoError(t, err)
		assert.Equal(t, testOpID, resp.OperationId)
		assert.False(t, resp.AlreadyExists)
	})
	t.Run("a creating tablegroup with more than one pending operation is internal", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expectLock()
		env.nameTaken(testTableGroupID, tableGroupStateCreating)
		env.expect("SELECT shard_name FROM multigres.shard", []string{"shard_name"}, [][]any{{"0-inf"}})
		env.expect("SELECT operation_id FROM multigres.operation", []string{"operation_id"}, [][]any{{testOpID}, {uuid.NewString()}})

		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(err))
		assert.Contains(t, err.Error(), "with 2 pending operations, want 1")
	})
	t.Run("a conflicting row deleted before the lookup asks for a retry", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expectLock()
		env.expect("ON CONFLICT \\(name\\) DO NOTHING", []string{"tablegroup_id"}, nil)
		env.expect("FROM multigres.tablegroup WHERE name = \\$1 FOR UPDATE", []string{"tablegroup_id", "state"}, nil)

		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
	})
	t.Run("dropping is a conflict", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expectLock()
		env.nameTaken(testTableGroupID, tableGroupStateDropping)

		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})
}

func TestCreateTableGroup_Rejects(t *testing.T) {
	t.Run("invalid name before any catalog access", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "Commerce"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
		assert.Equal(t, []string{"SELECT pg_is_in_recovery()"}, env.queries)
	})
	t.Run("invalid layout before any catalog access", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce", Shards: []string{"0-80"}})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
		assert.Contains(t, err.Error(), "end of the keyspace")
		assert.Equal(t, []string{"SELECT pg_is_in_recovery()"}, env.queries)
	})
	t.Run("not the catalog shard", func(t *testing.T) {
		env := newTableGroupTestEnv(t, "commerce", "0-80")
		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "other"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
		assert.Empty(t, env.queries)
	})
	t.Run("standby", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expect("SELECT pg_is_in_recovery", []string{"pg_is_in_recovery"}, [][]any{{"t"}})
		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})
	t.Run("not ready", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.pm.state = ManagerStateStarting
		_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
		require.Error(t, err)
	})
}

// TestTableGroupOps_RequireTheElectedPrimary covers a pooler whose postgres is
// out of recovery but which is not the active consensus leader: a standby
// promoted before its rule is committed, or a former primary not yet demoted.
// Every catalog RPC refuses it, ListTableGroups included.
func TestTableGroupOps_RequireTheElectedPrimary(t *testing.T) {
	calls := map[string]func(pm *MultipoolerManager) error{
		"CreateTableGroup": func(pm *MultipoolerManager) error {
			_, err := pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
			return err
		},
		"GetOperation": func(pm *MultipoolerManager) error {
			_, err := pm.GetOperation(t.Context(), testOpID)
			return err
		},
		"ListTableGroups": func(pm *MultipoolerManager) error {
			_, err := pm.ListTableGroups(t.Context())
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			env := newCatalogTestEnv(t)
			env.pm.stateManager = NewStateManager(env.pm.logger, env.pm.record, nilConsensusStatus)
			env.pm.stateManager.pgMode = pgmode.Primary
			env.expectPrimary()
			err := call(env.pm)
			require.Error(t, err)
			assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
			assert.Contains(t, err.Error(), "not the elected primary of the catalog shard")
			assert.Equal(t, []string{"SELECT pg_is_in_recovery()"}, env.queries, "nothing reads the catalog")
		})
	}
}

func TestGetOperation(t *testing.T) {
	t.Run("creating reports progress from topology", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expect("FROM multigres.operation WHERE operation_id", operationColumnsList(), [][]any{creatingOperationRow(testCreatedAt, false)})
		env.addServingPrimary(t, "cell1", "commerce", "0-80", testTableGroupID)
		// The other shard has only a replica so far.
		env.addPooler(t, "cell2", "commerce-80-inf", "commerce", "80-inf", testTableGroupID,
			clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)

		op, err := env.pm.GetOperation(t.Context(), testOpID)
		require.NoError(t, err)
		assert.Equal(t, testOpID, op.OperationId)
		assert.Equal(t, multipoolermanagerdatapb.OperationKind_OPERATION_KIND_CREATE_TABLEGROUP, op.Kind)
		assert.Equal(t, multipoolermanagerdatapb.OperationState_OPERATION_STATE_CREATING, op.State)
		assert.Equal(t, testTableGroupID, op.TablegroupId)
		assert.Equal(t, "commerce", op.TablegroupName)
		assert.Equal(t, int32(2), op.ShardsDesired)
		assert.Equal(t, int32(1), op.ShardsServing)
		assert.False(t, op.CancelRequested)
		assert.Equal(t, time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC), op.CreatedAt.AsTime())
	})
	t.Run("done reports every shard serving", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		row := creatingOperationRow(testCreatedAt, false)
		row[2] = operationStateDone
		env.expect("FROM multigres.operation WHERE operation_id", operationColumnsList(), [][]any{row})

		op, err := env.pm.GetOperation(t.Context(), testOpID)
		require.NoError(t, err)
		assert.Equal(t, multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE, op.State)
		assert.Equal(t, int32(2), op.ShardsServing)
	})
	t.Run("failed keeps the reason", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		row := creatingOperationRow(testCreatedAt, true)
		row[2] = operationStateFailed
		row[5] = "cancelled"
		env.expect("FROM multigres.operation WHERE operation_id", operationColumnsList(), [][]any{row})

		op, err := env.pm.GetOperation(t.Context(), testOpID)
		require.NoError(t, err)
		assert.Equal(t, multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED, op.State)
		assert.Equal(t, "cancelled", op.LastError)
		assert.True(t, op.CancelRequested)
		assert.Equal(t, int32(0), op.ShardsServing)
	})
	t.Run("not found", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expect("FROM multigres.operation WHERE operation_id", operationColumnsList(), nil)
		_, err := env.pm.GetOperation(t.Context(), testOpID)
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_NOT_FOUND, mterrors.Code(err))
	})
	t.Run("a replica refuses", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expect("SELECT pg_is_in_recovery", []string{"pg_is_in_recovery"}, [][]any{{"t"}})
		_, err := env.pm.GetOperation(t.Context(), testOpID)
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})
	t.Run("not a uuid", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		_, err := env.pm.GetOperation(t.Context(), "nope")
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
		assert.Empty(t, env.queries)
	})
}

func TestListTableGroups(t *testing.T) {
	t.Run("groups shards under their tablegroup, dropping ones included", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expect("FROM multigres.tablegroup t JOIN multigres.shard s", []string{"tablegroup_id", "name", "state", "shard_name"},
			[][]any{
				{int64(7), "commerce", tableGroupStateActive, "0-80"},
				{int64(7), "commerce", tableGroupStateActive, "80-inf"},
				{int64(9), "analytics", tableGroupStateCreating, "0-inf"},
				{int64(10), "old", tableGroupStateDropping, "0-inf"},
			})

		groups, err := env.pm.ListTableGroups(t.Context())
		require.NoError(t, err)
		require.Len(t, groups, 3)
		assert.Equal(t, int64(10), groups[2].TablegroupId)
		assert.Equal(t, shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_DROPPING, groups[2].State, "a dropping tablegroup is the removal signal")
		assert.Equal(t, []string{"0-inf"}, groups[2].Shards)
		assert.Equal(t, int64(7), groups[0].TablegroupId)
		assert.Equal(t, "commerce", groups[0].Name)
		assert.Equal(t, shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE, groups[0].State)
		assert.Equal(t, []string{"0-80", "80-inf"}, groups[0].Shards)
		assert.Equal(t, int64(9), groups[1].TablegroupId)
		assert.Equal(t, "analytics", groups[1].Name)
		assert.Equal(t, shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_CREATING, groups[1].State)
		assert.Equal(t, []string{"0-inf"}, groups[1].Shards)
		require.Len(t, env.queries, 2)
		assert.Contains(t, env.queries[1], "WHERE t.name <> $1 ORDER BY", "every state is listed")
		for _, state := range []string{tableGroupStateCreating, tableGroupStateActive, tableGroupStateDropping} {
			assert.NotEqual(t, shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_UNSPECIFIED, tableGroupStates[state], state)
		}
	})
	t.Run("empty catalog is an empty list, not nil", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expect("FROM multigres.tablegroup t JOIN multigres.shard s", []string{"tablegroup_id", "name", "state", "shard_name"}, nil)
		groups, err := env.pm.ListTableGroups(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, groups)
		assert.Empty(t, groups)
	})
	t.Run("a replica refuses", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expect("SELECT pg_is_in_recovery", []string{"pg_is_in_recovery"}, [][]any{{"t"}})
		_, err := env.pm.ListTableGroups(t.Context())
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})
	t.Run("not the catalog shard", func(t *testing.T) {
		env := newTableGroupTestEnv(t, "commerce", "0-inf")
		_, err := env.pm.ListTableGroups(t.Context())
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})
}

func TestPoolerIsServingPrimaryOf(t *testing.T) {
	base := func() *clustermetadatapb.Multipooler {
		return &clustermetadatapb.Multipooler{
			TablegroupId:    testTableGroupID,
			RoutingState:    &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
			ServingStatus:   clustermetadatapb.PoolerServingStatus_SERVING,
			LifecycleStatus: &clustermetadatapb.PoolerLifecycle{Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE},
		}
	}
	assert.True(t, poolerIsServingPrimaryOf(base(), testTableGroupID))

	older := base()
	older.LifecycleStatus = nil
	assert.True(t, poolerIsServingPrimaryOf(older, testTableGroupID), "a record without a lifecycle is accepted")

	stale := base()
	stale.TablegroupId = 3
	assert.False(t, poolerIsServingPrimaryOf(stale, testTableGroupID), "a pooler of another incarnation")

	unknownID := base()
	unknownID.TablegroupId = 0
	assert.False(t, poolerIsServingPrimaryOf(unknownID, testTableGroupID), "a pooler started without the id")

	replica := base()
	replica.RoutingState.Role = clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA
	assert.False(t, poolerIsServingPrimaryOf(replica, testTableGroupID))

	noRole := base()
	noRole.RoutingState = nil
	assert.False(t, poolerIsServingPrimaryOf(noRole, testTableGroupID))

	disabled := base()
	disabled.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	assert.False(t, poolerIsServingPrimaryOf(disabled, testTableGroupID))

	stopping := base()
	stopping.LifecycleStatus.Status = clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STOPPING
	assert.False(t, poolerIsServingPrimaryOf(stopping, testTableGroupID))

	starting := base()
	starting.LifecycleStatus.Status = clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING
	assert.False(t, poolerIsServingPrimaryOf(starting, testTableGroupID))
}
