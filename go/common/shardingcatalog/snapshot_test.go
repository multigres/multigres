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

package shardingcatalog

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pb "github.com/multigres/multigres/go/pb/shardingcatalog"
)

const (
	tgActive   = pb.TableGroupState_TABLE_GROUP_STATE_ACTIVE
	tgCreating = pb.TableGroupState_TABLE_GROUP_STATE_CREATING
	tgDropping = pb.TableGroupState_TABLE_GROUP_STATE_DROPPING

	shardActive    = pb.ShardState_SHARD_STATE_ACTIVE
	shardPreparing = pb.ShardState_SHARD_STATE_PREPARING
	shardRetired   = pb.ShardState_SHARD_STATE_RETIRED

	keyPrimary   = pb.ShardKeyRole_SHARD_KEY_ROLE_PRIMARY
	keySecondary = pb.ShardKeyRole_SHARD_KEY_ROLE_SECONDARY
	keyActive    = pb.ShardKeyState_SHARD_KEY_STATE_ACTIVE
	keyRetired   = pb.ShardKeyState_SHARD_KEY_STATE_RETIRED
)

func shard(t *testing.T, id, tableGroupID int64, name string, state pb.ShardState) *pb.Shard {
	t.Helper()
	kr, err := keyrange.ParseShardName(name)
	require.NoError(t, err)
	return &pb.Shard{ShardId: id, TablegroupId: tableGroupID, Name: name, KeyRange: kr, State: state}
}

// validSnapshot is a catalog with the default tablegroup, an active sharded
// tablegroup commerce (0-80, 80-inf, plus a retired 0-inf), one being
// created, and a table in commerce with an active primary key, a retired
// primary key, and a secondary key.
func validSnapshot(t *testing.T) *pb.Snapshot {
	t.Helper()
	return &pb.Snapshot{
		RoutingVersion: 12,
		PlanEpoch:      3,
		Tablegroups: []*pb.TableGroup{
			{TablegroupId: 1, Name: "default", State: tgActive},
			{TablegroupId: 2, Name: "commerce", State: tgActive},
			{TablegroupId: 3, Name: "analytics", State: tgCreating},
		},
		Shards: []*pb.Shard{
			shard(t, 1, 1, "0-inf", shardActive),
			// Listed out of range order: the snapshot sorts them.
			shard(t, 3, 2, "80-inf", shardActive),
			shard(t, 2, 2, "0-80", shardActive),
			shard(t, 4, 2, "0-inf", shardRetired),
			shard(t, 5, 3, "0-40", shardPreparing),
			shard(t, 6, 3, "40-inf", shardPreparing),
		},
		Tables: []*pb.Table{
			{TableId: 10, SchemaName: "public", TableName: "orders", TablegroupId: 2},
			{TableId: 11, SchemaName: "public", TableName: "settings", TablegroupId: 1},
		},
		ShardIndices: []*pb.ShardIndex{
			{ShardIndexId: 20, Name: "hash_xx", Type: "hash", Params: `{"function":"xxhash64"}`, State: pb.ShardIndexState_SHARD_INDEX_STATE_ACTIVE},
		},
		ShardKeys: []*pb.ShardKey{
			{ShardKeyId: 30, TableId: 10, ShardIndexId: 20, Name: "old", Role: keyPrimary, State: keyRetired, Columns: []string{"id"}},
			{ShardKeyId: 31, TableId: 10, ShardIndexId: 20, Name: "by_customer", Role: keyPrimary, State: keyActive, Columns: []string{"customer_id", "region"}},
			{ShardKeyId: 32, TableId: 10, ShardIndexId: 20, Name: "by_order", Role: keySecondary, State: keyActive, Columns: []string{"id"}},
		},
	}
}

func shardNames(shards []*Shard) []string {
	names := make([]string, len(shards))
	for i, sh := range shards {
		names[i] = sh.Name()
	}
	return names
}

func TestNew_Valid(t *testing.T) {
	s, err := New(validSnapshot(t))
	require.NoError(t, err)

	assert.Equal(t, int64(12), s.RoutingVersion())
	assert.Equal(t, int64(3), s.PlanEpoch())

	var names []string
	for _, g := range s.TableGroups() {
		names = append(names, g.Name())
	}
	assert.Equal(t, []string{"default", "commerce", "analytics"}, names, "ordered by id")

	commerce, ok := s.TableGroupByName("commerce")
	require.True(t, ok)
	assert.Equal(t, int64(2), commerce.ID())
	assert.Equal(t, tgActive, commerce.State())
	byID, ok := s.TableGroupByID(2)
	require.True(t, ok)
	assert.Same(t, commerce, byID)
	assert.True(t, commerce.IsSharded())
	assert.Equal(t, []string{"0-80", "80-inf"}, shardNames(commerce.ActiveShards()), "range order")
	assert.Equal(t, []string{"0-80", "80-inf", "0-inf"}, shardNames(commerce.Shards()), "every state, by id")
	assert.Same(t, commerce, commerce.ActiveShards()[0].TableGroup())
	assert.Equal(t, int64(2), commerce.ActiveShards()[0].ID())
	assert.Equal(t, shardActive, commerce.ActiveShards()[0].State())
	assert.Equal(t, []byte{0x80}, commerce.ActiveShards()[0].KeyRange().GetEnd())

	def, ok := s.TableGroupByName("default")
	require.True(t, ok)
	assert.False(t, def.IsSharded())
	assert.Equal(t, []string{"0-inf"}, shardNames(def.ActiveShards()))

	analytics, ok := s.TableGroupByName("analytics")
	require.True(t, ok, "tablegroups in every state are included")
	assert.Equal(t, tgCreating, analytics.State())
	assert.Empty(t, analytics.ActiveShards())
	assert.False(t, analytics.IsSharded(), "no active shards yet")
	assert.Len(t, analytics.Shards(), 2)

	_, ok = s.TableGroupByName("missing")
	assert.False(t, ok)
	_, ok = s.TableGroupByID(99)
	assert.False(t, ok)
}

func TestNew_TableLookup(t *testing.T) {
	s, err := New(validSnapshot(t))
	require.NoError(t, err)

	orders, ok := s.Table("public", "orders")
	require.True(t, ok)
	assert.Equal(t, int64(10), orders.ID())
	assert.Equal(t, "public", orders.Schema())
	assert.Equal(t, "orders", orders.Name())
	assert.Equal(t, "commerce", orders.TableGroup().Name())

	key := orders.PrimaryShardKey()
	require.NotNil(t, key, "the active primary key, not the retired one or the secondary")
	assert.Equal(t, int64(31), key.ID())
	assert.Equal(t, "by_customer", key.Name())
	assert.Equal(t, keyPrimary, key.Role())
	assert.Equal(t, keyActive, key.State())
	assert.Equal(t, []string{"customer_id", "region"}, key.Columns(), "in key order")
	idx := key.ShardIndex()
	assert.Equal(t, int64(20), idx.ID())
	assert.Equal(t, "hash_xx", idx.Name())
	assert.Equal(t, "hash", idx.Type())
	assert.JSONEq(t, `{"function":"xxhash64"}`, idx.Params())
	assert.Equal(t, pb.ShardIndexState_SHARD_INDEX_STATE_ACTIVE, idx.State())

	settings, ok := s.Table("public", "settings")
	require.True(t, ok)
	assert.Equal(t, "default", settings.TableGroup().Name())
	assert.Nil(t, settings.PrimaryShardKey())

	_, ok = s.Table("other", "orders")
	assert.False(t, ok)
	_, ok = s.Table("public", "Orders")
	assert.False(t, ok, "names match exactly")
}

func TestShardFor(t *testing.T) {
	p := validSnapshot(t)
	// A four-way layout with a two-byte bound, to cover multi-byte compares.
	p.Tablegroups = append(p.Tablegroups, &pb.TableGroup{TablegroupId: 4, Name: "events", State: tgActive})
	p.Shards = append(p.Shards,
		shard(t, 7, 4, "c0-inf", shardActive),
		shard(t, 8, 4, "0-40", shardActive),
		shard(t, 9, 4, "40-8040", shardActive),
		shard(t, 10, 4, "8040-c0", shardActive),
		// A preparing shard overlapping the active ones is ignored.
		shard(t, 11, 4, "0-80", shardPreparing),
	)
	s, err := New(p)
	require.NoError(t, err)
	events, ok := s.TableGroupByName("events")
	require.True(t, ok)

	for _, tc := range []struct {
		id   []byte
		want string
	}{
		{nil, "0-40"},
		{[]byte{}, "0-40"},
		{[]byte{0x00}, "0-40"},
		{[]byte{0x3f, 0xff, 0xff}, "0-40"},
		{[]byte{0x40}, "40-8040"}, // a start is inclusive
		{[]byte{0x40, 0x00}, "40-8040"},
		{[]byte{0x80}, "40-8040"}, // 0x80 sorts before 0x8040
		{[]byte{0x80, 0x3f, 0xff}, "40-8040"},
		{[]byte{0x80, 0x40}, "8040-c0"}, // an end is exclusive
		{[]byte{0xbf, 0xff}, "8040-c0"},
		{[]byte{0xc0}, "c0-inf"},
		{[]byte{0xff, 0xff, 0xff, 0xff}, "c0-inf"},
	} {
		got, ok := events.ShardFor(tc.id)
		require.True(t, ok, "%x", tc.id)
		assert.Equal(t, tc.want, got.Name(), "%x", tc.id)
	}

	def, _ := s.TableGroupByName("default")
	for _, id := range [][]byte{nil, {0x00}, {0x80}, {0xff}} {
		got, ok := def.ShardFor(id)
		require.True(t, ok)
		assert.Equal(t, "0-inf", got.Name())
	}

	analytics, _ := s.TableGroupByName("analytics")
	_, ok = analytics.ShardFor([]byte{0x10})
	assert.False(t, ok, "no active shards")
}

func TestNew_DroppingTableGroupKeepsItsLayout(t *testing.T) {
	p := validSnapshot(t)
	p.Tablegroups[1].State = tgDropping
	s, err := New(p)
	require.NoError(t, err)
	commerce, _ := s.TableGroupByName("commerce")
	assert.Equal(t, tgDropping, commerce.State())
	assert.Len(t, commerce.ActiveShards(), 2)
}

func TestNew_Rejects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, p *pb.Snapshot)
		want   string
	}{
		{"nil", nil, "snapshot is nil"},
		{"repeated tablegroup id", func(t *testing.T, p *pb.Snapshot) {
			p.Tablegroups = append(p.Tablegroups, &pb.TableGroup{TablegroupId: 2, Name: "other", State: tgCreating})
		}, "tablegroup id 2 is repeated"},
		{"repeated tablegroup name", func(t *testing.T, p *pb.Snapshot) {
			p.Tablegroups = append(p.Tablegroups, &pb.TableGroup{TablegroupId: 9, Name: "commerce", State: tgCreating})
		}, `tablegroup name "commerce" is repeated`},
		{"shard with a dangling tablegroup", func(t *testing.T, p *pb.Snapshot) {
			p.Shards = append(p.Shards, shard(t, 50, 99, "0-inf", shardPreparing))
		}, "shard 50 refers to tablegroup 99"},
		{"repeated shard id", func(t *testing.T, p *pb.Snapshot) {
			p.Shards = append(p.Shards, shard(t, 6, 3, "0-inf", shardPreparing))
		}, "shard id 6 is repeated"},
		{"repeated shard name", func(t *testing.T, p *pb.Snapshot) {
			p.Shards = append(p.Shards, shard(t, 50, 3, "0-40", shardRetired))
		}, `tablegroup "analytics" has two shards named "0-40"`},
		{"name does not match the range", func(t *testing.T, p *pb.Snapshot) {
			p.Shards[1].Name = "80-c0"
		}, `is named "80-c0", but its key range is "80-inf"`},
		{"non-canonical range", func(t *testing.T, p *pb.Snapshot) {
			p.Shards[1].KeyRange.Start = []byte{0x80, 0x00}
			p.Shards[1].Name = "8000-inf"
		}, "has an invalid key range"},
		{"gap in active shards", func(t *testing.T, p *pb.Snapshot) {
			p.Shards[1] = shard(t, 3, 2, "c0-inf", shardActive)
		}, `gap in shard layout between "0-80" and "c0-inf"`},
		{"overlapping active shards", func(t *testing.T, p *pb.Snapshot) {
			p.Shards[3].State = shardActive
		}, `active shards of tablegroup "commerce"`},
		{"active tablegroup without active shards", func(t *testing.T, p *pb.Snapshot) {
			p.Tablegroups[2].State = tgActive
		}, `active tablegroup "analytics" has no active shards`},
		{"partial layout on a creating tablegroup", func(t *testing.T, p *pb.Snapshot) {
			p.Shards[4].State = shardActive
		}, `active shards of tablegroup "analytics"`},
		{"missing default", func(t *testing.T, p *pb.Snapshot) {
			p.Tablegroups[0].Name = "base"
		}, "the default tablegroup is missing"},
		{"default not active", func(t *testing.T, p *pb.Snapshot) {
			p.Tablegroups[0].State = tgDropping
		}, "the default tablegroup is TABLE_GROUP_STATE_DROPPING, not active"},
		{"default split", func(t *testing.T, p *pb.Snapshot) {
			p.Shards[0] = shard(t, 1, 1, "0-80", shardActive)
			p.Shards = append(p.Shards, shard(t, 50, 1, "80-inf", shardActive))
		}, "the default tablegroup must have the single active shard 0-inf"},
		{"default without active shards", func(t *testing.T, p *pb.Snapshot) {
			p.Shards[0].State = shardRetired
		}, `active tablegroup "default" has no active shards`},
		{"table with a dangling tablegroup", func(t *testing.T, p *pb.Snapshot) {
			p.Tables[0].TablegroupId = 99
		}, "table 10 refers to tablegroup 99"},
		{"repeated table id", func(t *testing.T, p *pb.Snapshot) {
			p.Tables[1].TableId = 10
		}, "table id 10 is repeated"},
		{"repeated table name", func(t *testing.T, p *pb.Snapshot) {
			p.Tables[1].TableName = "orders"
		}, `table "public"."orders" is repeated`},
		{"repeated shard index id", func(t *testing.T, p *pb.Snapshot) {
			p.ShardIndices = append(p.ShardIndices, &pb.ShardIndex{ShardIndexId: 20, Name: "other"})
		}, "shard index id 20 is repeated"},
		{"repeated shard index name", func(t *testing.T, p *pb.Snapshot) {
			p.ShardIndices = append(p.ShardIndices, &pb.ShardIndex{ShardIndexId: 21, Name: "hash_xx"})
		}, `shard index name "hash_xx" is repeated`},
		{"shard key with a dangling table", func(t *testing.T, p *pb.Snapshot) {
			p.ShardKeys[0].TableId = 99
		}, "shard key 30 refers to table 99"},
		{"shard key with a dangling shard index", func(t *testing.T, p *pb.Snapshot) {
			p.ShardKeys[0].ShardIndexId = 99
		}, "shard key 30 refers to shard index 99"},
		{"repeated shard key id", func(t *testing.T, p *pb.Snapshot) {
			p.ShardKeys[1].ShardKeyId = 30
		}, "shard key id 30 is repeated"},
		{"two active primary keys", func(t *testing.T, p *pb.Snapshot) {
			p.ShardKeys[0].State = keyActive
		}, `table "public"."orders" has two active primary shard keys, 30 and 31`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var p *pb.Snapshot
			if tc.mutate != nil {
				p = validSnapshot(t)
				tc.mutate(t, p)
			}
			_, err := New(p)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
		})
	}
}

func TestNew_DoesNotRetainTheProto(t *testing.T) {
	p := validSnapshot(t)
	s, err := New(p)
	require.NoError(t, err)
	p.Tablegroups[1].Name = "renamed"
	p.ShardKeys[1].Columns[0] = "changed"
	_, ok := s.TableGroupByName("commerce")
	assert.True(t, ok)
	orders, _ := s.Table("public", "orders")
	assert.Equal(t, []string{"customer_id", "region"}, orders.PrimaryShardKey().Columns())
}

func TestSnapshot_ConcurrentReaders(t *testing.T) {
	s, err := New(validSnapshot(t))
	require.NoError(t, err)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				g, _ := s.TableGroupByName("commerce")
				sh, _ := g.ShardFor([]byte{0x90})
				assert.Equal(t, "80-inf", sh.Name())
				orders, _ := s.Table("public", "orders")
				assert.Equal(t, "by_customer", orders.PrimaryShardKey().Name())
			}
		})
	}
	wg.Wait()
}
