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
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

// expectSnapshotRead registers the snapshot transaction's isolation and the
// catalog_state read.
func (env *tableGroupTestEnv) expectSnapshotRead(routingVersion, planEpoch int64) {
	env.expect("^SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY$", nil, nil)
	env.expect("^SELECT routing_version, plan_epoch FROM multigres.catalog_state$",
		[]string{"routing_version", "plan_epoch"}, [][]any{{routingVersion, planEpoch}})
}

// expectCatalogRows registers every entity read with a catalog holding the
// default tablegroup, a sharded commerce tablegroup, a table with a primary
// shard key, and rows in states the snapshot does not know.
func (env *tableGroupTestEnv) expectCatalogRows() {
	env.expect("FROM multigres.tablegroup ORDER BY", []string{"tablegroup_id", "name", "state"}, [][]any{
		{int64(1), "default", "active"},
		{int64(7), "commerce", "active"},
		{int64(8), "future", "hibernating"},
	})
	env.expect("FROM multigres.shard ORDER BY", []string{"shard_id", "tablegroup_id", "shard_name", "range_start", "range_end", "state"}, [][]any{
		{int64(1), int64(1), "0-inf", "", "", "active"},
		{int64(2), int64(7), "0-80", "", "80", "active"},
		{int64(3), int64(7), "80-inf", "80", "", "active"},
		{int64(4), int64(7), "0-inf", "", "", "retired"},
	})
	env.expect("FROM multigres.tablegroup_table ORDER BY", []string{"table_id", "schema_name", "table_name", "tablegroup_id"}, [][]any{
		{int64(10), "public", "orders", int64(7)},
	})
	env.expect("FROM multigres.shard_index ORDER BY", []string{"shard_index_id", "name", "type", "params", "state"}, [][]any{
		{int64(20), "hash_xx", "hash", `{"function": "xxhash64"}`, "active"},
	})
	env.expect("FROM multigres.shard_key ORDER BY", []string{"shard_key_id", "table_id", "shard_index_id", "name", "role", "state"}, [][]any{
		{int64(30), int64(10), int64(20), "by_customer", "primary", "active"},
		{int64(31), int64(10), int64(20), "by_id", "secondary", "preparing"},
	})
	env.expect("FROM multigres.shard_key_column ORDER BY", []string{"shard_key_id", "column_name"}, [][]any{
		{int64(30), "customer_id"},
		{int64(30), "region"},
		{int64(31), "id"},
	})
}

func sinceVersion(v int64) *multipoolermanagerdatapb.GetRoutingSnapshotRequest {
	return &multipoolermanagerdatapb.GetRoutingSnapshotRequest{SinceVersion: &v}
}

func TestGetRoutingSnapshot_Full(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expectSnapshotRead(9, 2)
	env.expectCatalogRows()

	resp, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(8))
	require.NoError(t, err)
	assert.False(t, resp.GetUnchanged())
	assert.Equal(t, int64(9), resp.GetRoutingVersion())
	assert.Equal(t, int64(2), resp.GetPlanEpoch())

	want := &shardingcatalogpb.Snapshot{
		RoutingVersion: 9,
		PlanEpoch:      2,
		Tablegroups: []*shardingcatalogpb.TableGroup{
			{TablegroupId: 1, Name: "default", State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE},
			{TablegroupId: 7, Name: "commerce", State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE},
			// A state this binary does not know reads as UNSPECIFIED.
			{TablegroupId: 8, Name: "future", State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_UNSPECIFIED},
		},
		Shards: []*shardingcatalogpb.Shard{
			{ShardId: 1, TablegroupId: 1, Name: "0-inf", KeyRange: &clustermetadatapb.KeyRange{Start: []byte{}, End: []byte{}}, State: shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE},
			{ShardId: 2, TablegroupId: 7, Name: "0-80", KeyRange: &clustermetadatapb.KeyRange{Start: []byte{}, End: []byte{0x80}}, State: shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE},
			{ShardId: 3, TablegroupId: 7, Name: "80-inf", KeyRange: &clustermetadatapb.KeyRange{Start: []byte{0x80}, End: []byte{}}, State: shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE},
			{ShardId: 4, TablegroupId: 7, Name: "0-inf", KeyRange: &clustermetadatapb.KeyRange{Start: []byte{}, End: []byte{}}, State: shardingcatalogpb.ShardState_SHARD_STATE_RETIRED},
		},
		Tables: []*shardingcatalogpb.Table{
			{TableId: 10, SchemaName: "public", TableName: "orders", TablegroupId: 7},
		},
		ShardIndices: []*shardingcatalogpb.ShardIndex{
			{ShardIndexId: 20, Name: "hash_xx", Type: "hash", Params: `{"function": "xxhash64"}`, State: shardingcatalogpb.ShardIndexState_SHARD_INDEX_STATE_ACTIVE},
		},
		ShardKeys: []*shardingcatalogpb.ShardKey{
			{ShardKeyId: 30, TableId: 10, ShardIndexId: 20, Name: "by_customer", Role: shardingcatalogpb.ShardKeyRole_SHARD_KEY_ROLE_PRIMARY, State: shardingcatalogpb.ShardKeyState_SHARD_KEY_STATE_ACTIVE, Columns: []string{"customer_id", "region"}},
			{ShardKeyId: 31, TableId: 10, ShardIndexId: 20, Name: "by_id", Role: shardingcatalogpb.ShardKeyRole_SHARD_KEY_ROLE_SECONDARY, State: shardingcatalogpb.ShardKeyState_SHARD_KEY_STATE_PREPARING, Columns: []string{"id"}},
		},
	}
	assert.True(t, proto.Equal(want, resp.GetSnapshot()), "got %v", resp.GetSnapshot())

	// What the pooler serves is what a gateway accepts.
	s, err := shardingcatalog.New(resp.GetSnapshot())
	require.NoError(t, err)
	commerce, ok := s.TableGroupByName("commerce")
	require.True(t, ok)
	assert.True(t, commerce.IsSharded())

	assert.Equal(t, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", env.queries[1],
		"the isolation is set before anything is read")
	assert.Empty(t, env.queriesMatching("FOR UPDATE"), "a snapshot read takes no locks")
}

func TestGetRoutingSnapshot_Unchanged(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expectSnapshotRead(9, 2)

	resp, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(9))
	require.NoError(t, err)
	assert.True(t, resp.GetUnchanged())
	assert.Nil(t, resp.GetSnapshot())
	assert.Equal(t, int64(9), resp.GetRoutingVersion())
	assert.Equal(t, int64(2), resp.GetPlanEpoch())
	assert.Len(t, env.queries, 3, "no entity is read")
}

// A caller with no snapshot gets one even from a fresh catalog at version 0.
func TestGetRoutingSnapshot_NoSinceVersion(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expectSnapshotRead(0, 0)
	env.expectCatalogRows()

	resp, err := env.pm.GetRoutingSnapshot(t.Context(), &multipoolermanagerdatapb.GetRoutingSnapshotRequest{})
	require.NoError(t, err)
	assert.False(t, resp.GetUnchanged())
	require.NotNil(t, resp.GetSnapshot())
	assert.Len(t, resp.GetSnapshot().GetTablegroups(), 3)
}

// A catalog older than since_version (the gateway saw a newer primary) is
// still served in full; the gateway decides not to install it.
func TestGetRoutingSnapshot_OlderThanSince(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expectSnapshotRead(3, 0)
	env.expectCatalogRows()

	resp, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(5))
	require.NoError(t, err)
	assert.False(t, resp.GetUnchanged())
	assert.Equal(t, int64(3), resp.GetSnapshot().GetRoutingVersion())
}

func TestGetRoutingSnapshot_ReadError(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expectSnapshotRead(9, 2)
	env.qs.AddQueryPatternWithError("FROM multigres.tablegroup ORDER BY", errors.New("boom"))

	_, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read tablegroups")
}

func TestGetRoutingSnapshot_Rejects(t *testing.T) {
	t.Run("not the catalog shard", func(t *testing.T) {
		env := newTableGroupTestEnv(t, "commerce", "0-80")
		_, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(1))
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
		assert.Empty(t, env.queries)
	})
	t.Run("standby", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expect("SELECT pg_is_in_recovery", []string{"pg_is_in_recovery"}, [][]any{{"t"}})
		_, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(1))
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
		assert.Empty(t, env.queriesMatching("catalog_state"))
	})
	t.Run("out of recovery but not the elected primary", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.pm.stateManager = NewStateManager(env.pm.logger, env.pm.record, nilConsensusStatus)
		env.pm.stateManager.pgMode = pgmode.Primary
		env.expectPrimary()
		_, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(1))
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
		assert.Contains(t, err.Error(), "not the elected primary of the catalog shard")
		assert.Equal(t, []string{"SELECT pg_is_in_recovery()"}, env.queries)
	})
	t.Run("not ready", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.pm.state = ManagerStateStarting
		_, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(1))
		require.Error(t, err)
	})
}

// withHealthStreamer gives the env a health streamer whose routing state is
// the state manager's, as OnStateChange would set it.
func (env *tableGroupTestEnv) withHealthStreamer(t *testing.T, role clustermetadatapb.RoutingRole) *healthStreamer {
	t.Helper()
	hs := newHealthStreamer(slog.New(slog.DiscardHandler), testPoolerID, "default", "0-inf")
	hs.routingState = &clustermetadatapb.RoutingState{Role: role}
	env.pm.healthStreamer = hs
	return hs
}

func TestCreateTableGroup_PublishesTheAdvancedVersion(t *testing.T) {
	env := newCatalogTestEnv(t)
	hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	_, updates := hs.subscribe()
	env.expectPrimary()
	env.expectLock()
	env.expect("INSERT INTO multigres.tablegroup", []string{"tablegroup_id"}, [][]any{{testTableGroupID}})
	env.expect("INSERT INTO multigres.shard", nil, nil)
	env.expect("INSERT INTO multigres.operation", nil, nil)
	env.expectAdvance(5)

	_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
	require.NoError(t, err)
	assert.Len(t, env.queriesMatching("SET routing_version = routing_version + 1"), 1, "advanced exactly once")
	select {
	case got := <-updates:
		assert.Equal(t, int64(5), got.RoutingVersion)
	default:
		t.Fatal("the committed version was not broadcast")
	}
}

func TestCreateTableGroup_FailedCommitPublishesNothing(t *testing.T) {
	env := newCatalogTestEnv(t)
	hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	env.expectPrimary()
	env.expectLock()
	env.expect("INSERT INTO multigres.tablegroup", []string{"tablegroup_id"}, [][]any{{testTableGroupID}})
	env.expect("INSERT INTO multigres.shard", nil, nil)
	env.expect("INSERT INTO multigres.operation", nil, nil)
	env.expectAdvance(5)
	env.qs.AddQueryPatternWithError("^COMMIT$", errors.New("connection lost"))

	_, err := env.pm.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
	require.Error(t, err)
	assert.Zero(t, hs.getState().RoutingVersion)
}

func TestHealthStreamer_CatalogVersions(t *testing.T) {
	env := newCatalogTestEnv(t)
	hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	_, updates := hs.subscribe()
	drain := func() int {
		n := 0
		for {
			select {
			case <-updates:
				n++
			default:
				return n
			}
		}
	}

	hs.observeCatalogVersions(catalogVersions{routingVersion: 3, planEpoch: 1})
	assert.Equal(t, 1, drain())
	state := hs.getState()
	assert.Equal(t, int64(3), state.RoutingVersion)
	assert.Equal(t, int64(1), state.PlanEpoch)
	assert.Equal(t, int64(3), state.ToProto().GetRoutingVersion())

	hs.observeCatalogVersions(catalogVersions{routingVersion: 3, planEpoch: 1})
	assert.Zero(t, drain(), "an unchanged read does not broadcast")
	hs.observeCatalogVersions(catalogVersions{routingVersion: 2, planEpoch: 1})
	assert.Zero(t, drain(), "an older read is ignored")
	assert.Equal(t, int64(3), hs.getState().RoutingVersion)

	// A demoted pooler stops reporting the versions with its role.
	hs.mu.Lock()
	hs.routingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA}
	hs.mu.Unlock()
	assert.Zero(t, hs.getState().RoutingVersion)
	assert.Zero(t, hs.getState().PlanEpoch)

	hs.clearCatalogVersions()
	hs.observeCatalogVersions(catalogVersions{routingVersion: 1})
	assert.Equal(t, int64(1), hs.catalog.routingVersion, "after a clear, a new term starts from its own read")
}

func TestRefreshCatalogVersions(t *testing.T) {
	t.Run("the catalog primary reads and publishes", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
		env.expect("^SELECT routing_version, plan_epoch FROM multigres.catalog_state$",
			[]string{"routing_version", "plan_epoch"}, [][]any{{int64(11), int64(4)}})

		env.pm.refreshCatalogVersions(t.Context())
		state := hs.getState()
		assert.Equal(t, int64(11), state.RoutingVersion)
		assert.Equal(t, int64(4), state.PlanEpoch)
	})
	t.Run("a read error keeps the last value", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
		hs.observeCatalogVersions(catalogVersions{routingVersion: 6})
		env.qs.AddQueryPatternWithError("FROM multigres.catalog_state", errors.New("boom"))

		env.pm.refreshCatalogVersions(t.Context())
		assert.Equal(t, int64(6), hs.getState().RoutingVersion)
	})
	t.Run("a pooler that is not the routing primary clears without reading", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.pm.stateManager = NewStateManager(env.pm.logger, env.pm.record, nilConsensusStatus)
		hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
		hs.observeCatalogVersions(catalogVersions{routingVersion: 6})

		env.pm.refreshCatalogVersions(t.Context())
		assert.Zero(t, hs.catalog.routingVersion)
		assert.Empty(t, env.queries)
	})
}

func TestWriteCatalog(t *testing.T) {
	writeRow := func(ctx context.Context, tx executor.InternalTx) error {
		_, err := tx.Query(ctx, "INSERT INTO multigres.tablegroup_table VALUES (1)")
		return err
	}

	t.Run("a routing change locks first, advances once, commits and publishes", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
		env.expectLock()
		env.expect("INSERT INTO multigres.tablegroup_table", nil, nil)
		env.expectAdvance(9)
		env.expectCommit()

		v, err := env.pm.writeCatalog(t.Context(), func(ctx context.Context, tx executor.InternalTx) (catalogChange, error) {
			return catalogRoutingChange, writeRow(ctx, tx)
		})
		require.NoError(t, err)
		assert.Equal(t, int64(9), v.routingVersion)
		assert.Equal(t, []string{
			"SELECT routing_version FROM multigres.catalog_state FOR UPDATE",
			"INSERT INTO multigres.tablegroup_table VALUES (1)",
			"UPDATE multigres.catalog_state SET routing_version = routing_version + 1 RETURNING routing_version, plan_epoch",
			"COMMIT",
		}, env.queries)
		assert.Equal(t, int64(9), hs.getState().RoutingVersion)
	})
	t.Run("a change that invalidates plans advances plan_epoch too", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
		env.expectLock()
		env.expect("INSERT INTO multigres.tablegroup_table", nil, nil)
		env.expect("SET routing_version = routing_version \\+ 1, plan_epoch = plan_epoch \\+ 1",
			[]string{"routing_version", "plan_epoch"}, [][]any{{int64(9), int64(3)}})
		env.expectCommit()

		v, err := env.pm.writeCatalog(t.Context(), func(ctx context.Context, tx executor.InternalTx) (catalogChange, error) {
			return catalogInvalidatesPlans, writeRow(ctx, tx)
		})
		require.NoError(t, err)
		assert.Equal(t, catalogVersions{routingVersion: 9, planEpoch: 3}, v)
		assert.Len(t, env.queriesMatching("UPDATE multigres.catalog_state"), 1, "both advance in one statement")
		assert.Equal(t, "COMMIT", env.queries[len(env.queries)-1])
		assert.Equal(t, int64(3), hs.getState().PlanEpoch)
	})
	t.Run("a routing change leaves plan_epoch alone", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.expectAdvance(9)
		_, err := env.pm.writeCatalog(t.Context(), func(context.Context, executor.InternalTx) (catalogChange, error) {
			return catalogRoutingChange, nil
		})
		require.NoError(t, err)
		assert.Empty(t, env.queriesMatching("plan_epoch = plan_epoch"))
	})
	t.Run("an unchanged catalog rolls back and keeps its version", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.expectCommit()

		v, err := env.pm.writeCatalog(t.Context(), func(context.Context, executor.InternalTx) (catalogChange, error) {
			return catalogUnchanged, nil
		})
		require.NoError(t, err)
		assert.Zero(t, v)
		assert.Equal(t, []string{"SELECT routing_version FROM multigres.catalog_state FOR UPDATE"}, env.queries)
	})
	t.Run("a writer error rolls back without advancing", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.expectCommit()

		_, err := env.pm.writeCatalog(t.Context(), func(context.Context, executor.InternalTx) (catalogChange, error) {
			return catalogRoutingChange, errors.New("boom")
		})
		require.ErrorContains(t, err, "boom")
		assert.Empty(t, env.queriesMatching("routing_version + 1"))
		assert.Empty(t, env.queriesMatching("COMMIT"))
	})
	t.Run("an unknown change is refused", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.expectCommit()

		_, err := env.pm.writeCatalog(t.Context(), func(context.Context, executor.InternalTx) (catalogChange, error) {
			return catalogChange(99), nil
		})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(err))
		assert.Empty(t, env.queriesMatching("COMMIT"))
	})
	t.Run("a failed lock runs nothing", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.qs.AddQueryPatternWithError("FROM multigres.catalog_state FOR UPDATE", errors.New("lock timeout"))
		ran := false
		_, err := env.pm.writeCatalog(t.Context(), func(context.Context, executor.InternalTx) (catalogChange, error) {
			ran = true
			return catalogRoutingChange, nil
		})
		require.ErrorContains(t, err, "failed to lock catalog state")
		assert.False(t, ran)
	})
}
