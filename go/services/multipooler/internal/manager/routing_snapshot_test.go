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
	"sync/atomic"
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
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
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
// shard key, rows in states the snapshot does not know, and an import's
// routing rules: a shard rule from default and a table rule.
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
	env.expect("FROM multigres.routing_rule ORDER BY", []string{
		"rule_id", "operation_id", "scope", "from_tablegroup_id", "from_table_id",
		"to_tablegroup_id", "to_shard_id", "to_shard_key_id", "created_version",
	}, [][]any{
		{int64(40), importOp, "tablegroup", int64(1), nil, int64(7), int64(2), nil, int64(8)},
		{int64(41), importOp, "table", int64(7), int64(10), int64(1), nil, int64(30), int64(9)},
	})
}

// testLeaderRule is the rule selfLeaderConsensusStatus makes the test pooler
// the primary under.
var testLeaderRule = &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}

// sinceVersion asks for changes since version v of a snapshot served under
// testLeaderRule.
func sinceVersion(v int64) *multipoolermanagerdatapb.GetRoutingSnapshotRequest {
	return &multipoolermanagerdatapb.GetRoutingSnapshotRequest{SinceVersion: &v, SinceConsensusRule: testLeaderRule}
}

// leaderAtTerm is a consensus snapshot naming the test pooler the active
// leader under the given coordinator term.
func leaderAtTerm(term int64) *clustermetadatapb.ConsensusStatus {
	cs := selfLeaderConsensusStatus()
	cs.CurrentPosition.Position.Decision.RuleNumber = &clustermetadatapb.RuleNumber{CoordinatorTerm: term}
	return cs
}

func TestGetRoutingSnapshot_Full(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expectPrimary() // checked again after the read
	env.expectSnapshotRead(9, 2)
	env.expectCatalogRows()

	resp, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(8))
	require.NoError(t, err)
	assert.False(t, resp.GetUnchanged())
	assert.Equal(t, int64(9), resp.GetRoutingVersion())
	assert.Equal(t, int64(2), resp.GetPlanEpoch())
	assert.True(t, proto.Equal(testLeaderRule, resp.GetConsensusRule()), "stamped with the rule it served under")

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
		RoutingRules: []*shardingcatalogpb.RoutingRule{
			{
				RuleId: 40, OperationId: importOp, Scope: shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP,
				FromTablegroupId: 1, ToTablegroupId: 7, ToShardId: proto.Int64(2), CreatedVersion: 8,
			},
			{
				RuleId: 41, OperationId: importOp, Scope: shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLE,
				FromTablegroupId: 7, FromTableId: proto.Int64(10), ToTablegroupId: 1, ToShardKeyId: proto.Int64(30), CreatedVersion: 9,
			},
		},
	}
	assert.True(t, proto.Equal(want, resp.GetSnapshot()), "got %v", resp.GetSnapshot())

	// What the pooler serves is what a gateway accepts.
	s, err := shardingcatalog.New(resp.GetSnapshot())
	require.NoError(t, err)
	commerce, ok := s.TableGroupByName("commerce")
	require.True(t, ok)
	assert.True(t, commerce.IsSharded())
	def, ok := s.TableGroupByName("default")
	require.True(t, ok)
	require.Len(t, def.ShardRules(), 1)
	assert.Same(t, commerce, def.ShardRules()[0].ToTableGroup())
	orders, ok := s.Table("public", "orders")
	require.True(t, ok)
	require.NotNil(t, orders.RoutingRule())
	assert.Equal(t, int64(41), orders.RoutingRule().ID())

	assert.Equal(t, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", env.queries[1],
		"the isolation is set before anything is read")
	assert.Empty(t, env.queriesMatching("FOR UPDATE"), "a snapshot read takes no locks")
}

func TestGetRoutingSnapshot_Unchanged(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expectPrimary() // checked again after the read
	env.expectSnapshotRead(9, 2)

	resp, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(9))
	require.NoError(t, err)
	assert.True(t, resp.GetUnchanged())
	assert.Nil(t, resp.GetSnapshot())
	assert.Equal(t, int64(9), resp.GetRoutingVersion())
	assert.Equal(t, int64(2), resp.GetPlanEpoch())
	assert.Equal(t, []string{
		"SELECT pg_is_in_recovery()",
		"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
		"SELECT routing_version, plan_epoch FROM multigres.catalog_state",
		"SELECT pg_is_in_recovery()",
	}, env.queries, "no entity is read, and the primary is checked again after the read")
	assert.True(t, proto.Equal(testLeaderRule, resp.GetConsensusRule()))
}

// The same version under another rule may be different contents (a version
// lost in a failover and reused), so it is served in full.
func TestGetRoutingSnapshot_SameVersionUnderAnotherRule(t *testing.T) {
	for name, rule := range map[string]*clustermetadatapb.RuleNumber{
		"an older rule": nil,
		"a newer rule":  {CoordinatorTerm: 2},
	} {
		t.Run(name, func(t *testing.T) {
			env := newCatalogTestEnv(t)
			env.expectPrimary()
			env.expectPrimary() // checked again after the read
			env.expectSnapshotRead(9, 2)
			env.expectCatalogRows()

			v := int64(9)
			resp, err := env.pm.GetRoutingSnapshot(t.Context(), &multipoolermanagerdatapb.GetRoutingSnapshotRequest{SinceVersion: &v, SinceConsensusRule: rule})
			require.NoError(t, err)
			assert.False(t, resp.GetUnchanged())
			require.NotNil(t, resp.GetSnapshot())
		})
	}
}

// A pooler whose rule changes while it reads must not label what it read
// with either rule.
func TestGetRoutingSnapshot_RuleChangesDuringRead(t *testing.T) {
	env := newCatalogTestEnv(t)
	var term atomic.Int64
	term.Store(1)
	env.pm.stateManager = NewStateManager(env.pm.logger, env.pm.record, func() *clustermetadatapb.ConsensusStatus {
		return leaderAtTerm(term.Load())
	})
	env.pm.stateManager.pgMode = pgmode.Primary
	env.expectPrimary()
	env.expectPrimary() // checked again after the read
	env.expect("^SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY$", nil, nil)
	env.qs.AddQueryPatternWithCallback("^SELECT routing_version, plan_epoch FROM multigres.catalog_state$",
		mock.MakeQueryResult([]string{"routing_version", "plan_epoch"}, [][]any{{int64(9), int64(2)}}),
		func(string) { term.Store(2) }) // a new rule commits mid-read
	env.expectCatalogRows()

	_, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(8))
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
	assert.Contains(t, err.Error(), "consensus rule changed from 1.0 to 2.0 during the snapshot read")
}

// A pooler demoted while it reads fails as a non-primary would, rather than
// answer with what it read.
func TestGetRoutingSnapshot_DemotedDuringRead(t *testing.T) {
	env := newCatalogTestEnv(t)
	var demoted atomic.Bool
	env.pm.stateManager = NewStateManager(env.pm.logger, env.pm.record, func() *clustermetadatapb.ConsensusStatus {
		if demoted.Load() {
			return nil
		}
		return leaderAtTerm(1)
	})
	env.pm.stateManager.pgMode = pgmode.Primary
	env.expectPrimary()
	env.expectPrimary()
	env.expect("^SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY$", nil, nil)
	env.qs.AddQueryPatternWithCallback("^SELECT routing_version, plan_epoch FROM multigres.catalog_state$",
		mock.MakeQueryResult([]string{"routing_version", "plan_epoch"}, [][]any{{int64(9), int64(2)}}),
		func(string) { demoted.Store(true) })
	env.expectCatalogRows()

	_, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(8))
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "not the elected primary of the catalog shard")
}

// A caller with no snapshot gets one even from a fresh catalog at version 0.
func TestGetRoutingSnapshot_NoSinceVersion(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expectPrimary() // checked again after the read
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
	env.expectPrimary() // checked again after the read
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
	env.expectPrimary() // checked again after the read
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
// the state manager's, as OnStateChange would set it: role, under
// testLeaderRule.
func (env *tableGroupTestEnv) withHealthStreamer(t *testing.T, role clustermetadatapb.RoutingRole) *healthStreamer {
	t.Helper()
	hs := newHealthStreamer(slog.New(slog.DiscardHandler), testPoolerID, "default", "0-inf")
	hs.routingState = &clustermetadatapb.RoutingState{Role: role, Rule: testLeaderRule}
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
	env.expectCatalogRows()
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
	env.expectCatalogRows()
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
	setRouting := func(role clustermetadatapb.RoutingRole, rule *clustermetadatapb.RuleNumber) {
		hs.mu.Lock()
		hs.routingState = &clustermetadatapb.RoutingState{Role: role, Rule: rule}
		hs.mu.Unlock()
	}
	primary := clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY

	hs.observeCatalogVersions(testLeaderRule, catalogVersions{routingVersion: 10, planEpoch: 1})
	assert.Equal(t, 1, drain())
	state := hs.getState()
	assert.Equal(t, int64(10), state.RoutingVersion)
	assert.Equal(t, int64(1), state.PlanEpoch)
	assert.Equal(t, int64(10), state.ToProto().GetRoutingVersion())

	hs.observeCatalogVersions(testLeaderRule, catalogVersions{routingVersion: 10, planEpoch: 1})
	assert.Zero(t, drain(), "an unchanged read does not broadcast")
	hs.observeCatalogVersions(testLeaderRule, catalogVersions{routingVersion: 9, planEpoch: 1})
	assert.Zero(t, drain(), "an older read under the same rule is ignored")
	assert.Equal(t, int64(10), hs.getState().RoutingVersion)

	// A demoted pooler stops reporting the versions with its role.
	setRouting(clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA, nil)
	assert.Zero(t, hs.getState().RoutingVersion)
	assert.Zero(t, hs.getState().PlanEpoch)

	// Promoted again under rule 3, before any refresh, with its data rewound
	// to version 9: it reports nothing from the earlier term...
	rule3 := &clustermetadatapb.RuleNumber{CoordinatorTerm: 3}
	setRouting(primary, rule3)
	assert.Zero(t, hs.getState().RoutingVersion, "the version read under rule 1 is not published under rule 3")
	// ...a late reading taken under the old rule is dropped...
	hs.observeCatalogVersions(testLeaderRule, catalogVersions{routingVersion: 11})
	assert.Zero(t, hs.getState().RoutingVersion, "a reading from an earlier term is dropped")
	// ...and its first reading under the new rule is published, though lower.
	hs.observeCatalogVersions(rule3, catalogVersions{routingVersion: 9})
	assert.Equal(t, int64(9), hs.getState().RoutingVersion)
	assert.Equal(t, int64(9), hs.getState().ToProto().GetRoutingVersion())
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
		hs.observeCatalogVersions(testLeaderRule, catalogVersions{routingVersion: 6})
		env.qs.AddQueryPatternWithError("FROM multigres.catalog_state", errors.New("boom"))

		env.pm.refreshCatalogVersions(t.Context())
		assert.Equal(t, int64(6), hs.getState().RoutingVersion)
	})
	t.Run("a pooler that is not the routing primary reads nothing", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.pm.stateManager = NewStateManager(env.pm.logger, env.pm.record, nilConsensusStatus)
		env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)

		env.pm.refreshCatalogVersions(t.Context())
		assert.Empty(t, env.queries)
	})
	t.Run("a reading is tagged with the rule it was taken under", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.pm.stateManager = NewStateManager(env.pm.logger, env.pm.record, func() *clustermetadatapb.ConsensusStatus { return leaderAtTerm(3) })
		env.pm.stateManager.pgMode = pgmode.Primary
		hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
		hs.routingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3}}
		hs.observeCatalogVersions(hs.routingState.Rule, catalogVersions{routingVersion: 10})
		env.expect("^SELECT routing_version, plan_epoch FROM multigres.catalog_state$",
			[]string{"routing_version", "plan_epoch"}, [][]any{{int64(12), int64(0)}})

		env.pm.refreshCatalogVersions(t.Context())
		assert.Equal(t, int64(12), hs.getState().RoutingVersion)
		assert.Equal(t, int64(3), hs.catalogRule.GetCoordinatorTerm())
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
		env.expectCatalogRows()
		env.expectAdvance(9)
		env.expectCommit()

		v, err := env.pm.writeCatalog(t.Context(), func(ctx context.Context, tx executor.InternalTx, _ catalogVersions) (catalogChange, error) {
			return catalogRoutingChange, writeRow(ctx, tx)
		})
		require.NoError(t, err)
		assert.Equal(t, int64(9), v.routingVersion)
		assert.Equal(t, []string{
			"SELECT routing_version, plan_epoch FROM multigres.catalog_state FOR UPDATE",
			"INSERT INTO multigres.tablegroup_table VALUES (1)",
			snapshotTableGroupsQuery, snapshotShardsQuery, snapshotTablesQuery,
			snapshotShardIndicesQuery, snapshotShardKeysQuery, snapshotShardKeyColumnsQuery, selectRoutingRulesSQL,
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
		env.expectCatalogRows()
		env.expect("SET routing_version = routing_version \\+ 1, plan_epoch = plan_epoch \\+ 1",
			[]string{"routing_version", "plan_epoch"}, [][]any{{int64(9), int64(3)}})
		env.expectCommit()

		v, err := env.pm.writeCatalog(t.Context(), func(ctx context.Context, tx executor.InternalTx, _ catalogVersions) (catalogChange, error) {
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
		env.expectCatalogRows()
		env.expectAdvance(9)
		_, err := env.pm.writeCatalog(t.Context(), func(context.Context, executor.InternalTx, catalogVersions) (catalogChange, error) {
			return catalogRoutingChange, nil
		})
		require.NoError(t, err)
		assert.Empty(t, env.queriesMatching("plan_epoch = plan_epoch"))
	})
	t.Run("a write outside the snapshot commits without advancing", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
		_, updates := hs.subscribe()
		env.expectLock()
		env.expect("UPDATE multigres.operation", nil, nil)
		env.expectCommit()

		var got catalogVersions
		v, err := env.pm.writeCatalog(t.Context(), func(ctx context.Context, tx executor.InternalTx, current catalogVersions) (catalogChange, error) {
			got = current
			_, err := tx.Query(ctx, "UPDATE multigres.operation SET last_error = 'x'")
			return catalogNoSnapshotChange, err
		})
		require.NoError(t, err)
		assert.Equal(t, catalogVersions{routingVersion: 4, planEpoch: 2}, got, "the writer sees the locked versions")
		assert.Equal(t, got, v, "a write outside the snapshot returns the versions it found")
		assert.Equal(t, []string{
			"SELECT routing_version, plan_epoch FROM multigres.catalog_state FOR UPDATE",
			"UPDATE multigres.operation SET last_error = 'x'",
			"COMMIT",
		}, env.queries, "the write is kept, and no version advances")
		assert.Empty(t, updates, "nothing is published")
	})
	t.Run("a write that would leave an invalid snapshot rolls back", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
		env.expectLock()
		env.expect("INSERT INTO multigres.shard", nil, nil)
		// The write left commerce active with only the shard 0-80, which does
		// not reach the end of the keyspace.
		env.expect("FROM multigres.tablegroup ORDER BY", []string{"tablegroup_id", "name", "state"}, [][]any{
			{int64(1), "default", "active"},
			{int64(7), "commerce", "active"},
		})
		env.expect("FROM multigres.shard ORDER BY", []string{"shard_id", "tablegroup_id", "shard_name", "range_start", "range_end", "state"}, [][]any{
			{int64(1), int64(1), "0-inf", "", "", "active"},
			{int64(2), int64(7), "0-80", "", "80", "active"},
		})
		for _, table := range []string{"tablegroup_table", "shard_index", "shard_key", "shard_key_column", "routing_rule"} {
			env.expect("FROM multigres."+table+" ORDER BY", nil, nil)
		}
		env.expectCommit()

		_, err := env.pm.writeCatalog(t.Context(), func(ctx context.Context, tx executor.InternalTx, _ catalogVersions) (catalogChange, error) {
			_, err := tx.Query(ctx, "INSERT INTO multigres.shard VALUES (2)")
			return catalogRoutingChange, err
		})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(err), "the writer is at fault, not its caller")
		assert.Contains(t, err.Error(), "catalog write would leave an invalid routing snapshot")
		assert.Contains(t, err.Error(), "does not cover the end of the keyspace")
		assert.Empty(t, env.queriesMatching("routing_version + 1"))
		assert.Empty(t, env.queriesMatching("COMMIT"))
		assert.Zero(t, hs.getState().RoutingVersion)
	})
	t.Run("a writer error rolls back without advancing", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.expectCommit()

		_, err := env.pm.writeCatalog(t.Context(), func(context.Context, executor.InternalTx, catalogVersions) (catalogChange, error) {
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

		_, err := env.pm.writeCatalog(t.Context(), func(context.Context, executor.InternalTx, catalogVersions) (catalogChange, error) {
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
		_, err := env.pm.writeCatalog(t.Context(), func(context.Context, executor.InternalTx, catalogVersions) (catalogChange, error) {
			ran = true
			return catalogRoutingChange, nil
		})
		require.ErrorContains(t, err, "failed to lock catalog state")
		assert.False(t, ran)
	})
}
