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

package shardindex

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
)

// testSnapshot is a catalog with the default tablegroup, the tablegroup
// "two" (0-80, 80-inf, and a retired 0-inf), "four" (0-40, 40-80, 80-c0,
// c0-inf) and "empty", which is being created and has no active shards.
func testSnapshot(t *testing.T) *shardingcatalog.Snapshot {
	t.Helper()
	shard := func(id, tableGroupID int64, name string, state shardingcatalogpb.ShardState) *shardingcatalogpb.Shard {
		kr, err := keyrange.ParseShardName(name)
		require.NoError(t, err)
		return &shardingcatalogpb.Shard{ShardId: id, TablegroupId: tableGroupID, Name: name, KeyRange: kr, State: state}
	}
	const (
		active   = shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE
		retired  = shardingcatalogpb.ShardState_SHARD_STATE_RETIRED
		tgActive = shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE
	)
	snapshot, err := shardingcatalog.New(&shardingcatalogpb.Snapshot{
		Tablegroups: []*shardingcatalogpb.TableGroup{
			{TablegroupId: 1, Name: "default", State: tgActive},
			{TablegroupId: 2, Name: "two", State: tgActive},
			{TablegroupId: 3, Name: "four", State: tgActive},
			{TablegroupId: 4, Name: "empty", State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_CREATING},
		},
		Shards: []*shardingcatalogpb.Shard{
			shard(1, 1, "0-inf", active),
			shard(2, 2, "0-80", active),
			shard(3, 2, "80-inf", active),
			shard(4, 2, "0-inf", retired),
			shard(5, 3, "0-40", active),
			shard(6, 3, "40-80", active),
			shard(7, 3, "80-c0", active),
			shard(8, 3, "c0-inf", active),
		},
	})
	require.NoError(t, err)
	return snapshot
}

func tableGroup(t *testing.T, name string) *shardingcatalog.TableGroup {
	t.Helper()
	tg, ok := testSnapshot(t).TableGroupByName(name)
	require.True(t, ok)
	return tg
}

// resolve returns the names of the shards d resolves to in tg, in the order
// Resolve adds them.
func resolve(t *testing.T, d Destination, tg *shardingcatalog.TableGroup) []string {
	t.Helper()
	names := []string{}
	require.NoError(t, d.Resolve(tg, func(sh *shardingcatalog.Shard) error {
		names = append(names, sh.Name())
		return nil
	}))
	return names
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func keyRange(t *testing.T, name string) *clustermetadatapb.KeyRange {
	t.Helper()
	kr, err := keyrange.ParseShardName(name)
	require.NoError(t, err)
	return kr
}

// Values route to the shard owning their hash, on both sides of the 0x80
// boundary of a tablegroup split into 0-80 and 80-inf.
func TestHash_Routing(t *testing.T) {
	t.Parallel()
	two := tableGroup(t, "two")
	h := newHash(t)

	// Every golden vector lands on the shard its first byte says.
	golden := loadHashGolden(t)
	values := make([]Value, len(golden))
	for i, g := range golden {
		values[i] = intValue(t, g.Value, g.Width)
	}
	destinations, err := h.Map(context.Background(), nil, values)
	require.NoError(t, err)
	seen := map[string]int{}
	for i, d := range destinations {
		want := "0-80"
		if unhex(t, golden[i].RoutingID)[0] >= 0x80 {
			want = "80-inf"
		}
		assert.Equal(t, []string{want}, resolve(t, d, two), "value %d, routing identifier %s", golden[i].Value, golden[i].RoutingID)
		seen[want]++
	}
	assert.NotZero(t, seen["0-80"])
	assert.NotZero(t, seen["80-inf"])

	// These two integers hash just below and just at the boundary.
	for _, tc := range []struct {
		value     int64
		routingID string
		shard     string
	}{
		{732307, "7ffff63c407ddbd7", "0-80"},
		{1744007, "8000200780ae011a", "80-inf"},
	} {
		got, err := h.Map(context.Background(), nil, []Value{intValue(t, tc.value, 4), intValue(t, tc.value, 8)})
		require.NoError(t, err)
		for _, d := range got {
			assert.Equal(t, tc.routingID, hex.EncodeToString(d.RoutingID()))
			assert.Equal(t, []string{tc.shard}, resolve(t, d, two))
		}
	}

	// The exact edges of the hash space: the upper bound is exclusive.
	for id, want := range map[string]string{
		"0000000000000000": "0-80",
		"7fffffffffffffff": "0-80",
		"8000000000000000": "80-inf",
		"ffffffffffffffff": "80-inf",
	} {
		assert.Equal(t, []string{want}, resolve(t, DestinationRoutingID(unhex(t, id)), two), id)
	}
}

func TestDestination_Resolve(t *testing.T) {
	t.Parallel()
	four := tableGroup(t, "four")
	all := []string{"0-40", "40-80", "80-c0", "c0-inf"}

	for name, tc := range map[string]struct {
		d      Destination
		want   []string
		string string
	}{
		"none":       {DestinationNone(), []string{}, "None()"},
		"zero value": {Destination{}, []string{}, "None()"},
		"all":        {DestinationAllShards(), all, "AllShards()"},
		"id":         {DestinationRoutingID(unhex(t, "40")), []string{"40-80"}, "RoutingID(40)"},
		"empty id":   {DestinationRoutingID(nil), []string{"0-40"}, "RoutingID()"},
		"long id":    {DestinationRoutingID(unhex(t, "bfffffffffffffffffff")), []string{"80-c0"}, "RoutingID(bfffffffffffffffffff)"},
		// One shard per identifier, in order, repeats included.
		"ids": {
			DestinationRoutingIDs([][]byte{unhex(t, "c0"), unhex(t, "3f"), unhex(t, "ff")}),
			[]string{"c0-inf", "0-40", "c0-inf"},
			"RoutingIDs(c0,3f,ff)",
		},
		"no ids":            {DestinationRoutingIDs(nil), []string{}, "RoutingIDs()"},
		"full range":        {DestinationKeyRange(nil), all, "KeyRange(0-inf)"},
		"one shard's range": {DestinationKeyRange(keyRange(t, "40-80")), []string{"40-80"}, "KeyRange(40-80)"},
		// Ranges that only touch a shard's bound do not overlap it.
		"straddling range": {DestinationKeyRange(keyRange(t, "60-a0")), []string{"40-80", "80-c0"}, "KeyRange(60-a0)"},
		"open start":       {DestinationKeyRange(keyRange(t, "0-41")), []string{"0-40", "40-80"}, "KeyRange(0-41)"},
		"open end":         {DestinationKeyRange(keyRange(t, "c0-inf")), []string{"c0-inf"}, "KeyRange(c0-inf)"},
		"narrow range":     {DestinationKeyRange(keyRange(t, "8010-8020")), []string{"80-c0"}, "KeyRange(8010-8020)"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, resolve(t, tc.d, four))
			assert.Equal(t, tc.string, tc.d.String())
		})
	}

	// An unsharded tablegroup takes everything on its one shard.
	def := tableGroup(t, "default")
	for _, d := range []Destination{
		DestinationAllShards(), DestinationRoutingID(unhex(t, "ff")), DestinationKeyRange(keyRange(t, "40-80")),
	} {
		assert.Equal(t, []string{"0-inf"}, resolve(t, d, def), d.String())
	}
	assert.Empty(t, resolve(t, DestinationNone(), def))

	// Retired shards are never resolved to.
	assert.Equal(t, []string{"0-80", "80-inf"}, resolve(t, DestinationAllShards(), tableGroup(t, "two")))
}

func TestDestination_ResolveStopsAtTheFirstError(t *testing.T) {
	t.Parallel()
	four := tableGroup(t, "four")
	stop := errors.New("stop")
	for _, d := range []Destination{
		DestinationAllShards(),
		DestinationKeyRange(nil),
		DestinationRoutingIDs([][]byte{{0x10}, {0x50}, {0x90}}),
		DestinationRoutingID([]byte{0x10}),
	} {
		calls := 0
		err := d.Resolve(four, func(*shardingcatalog.Shard) error {
			calls++
			return stop
		})
		require.ErrorIs(t, err, stop, d.String())
		assert.Equal(t, 1, calls, d.String())
	}
}

func TestDestination_ResolveWithoutActiveShards(t *testing.T) {
	t.Parallel()
	empty := tableGroup(t, "empty")
	add := func(*shardingcatalog.Shard) error {
		t.Fatal("no shard to add")
		return nil
	}
	for _, d := range []Destination{
		DestinationAllShards(),
		DestinationKeyRange(nil),
		DestinationRoutingIDs([][]byte{{0x10}}),
		DestinationRoutingID([]byte{0x10}),
	} {
		require.ErrorContains(t, d.Resolve(empty, add), `tablegroup "empty" has no active shards`, d.String())
	}
	require.NoError(t, DestinationNone().Resolve(empty, add))
}

func TestDestination_Accessors(t *testing.T) {
	t.Parallel()
	id := []byte{0x01}
	d := DestinationRoutingID(id)
	assert.Equal(t, KindRoutingID, d.Kind())
	assert.Equal(t, id, d.RoutingID())
	assert.Nil(t, d.RoutingIDs())
	assert.Nil(t, d.KeyRange())

	d = DestinationRoutingIDs([][]byte{id})
	assert.Equal(t, KindRoutingIDs, d.Kind())
	assert.Equal(t, [][]byte{id}, d.RoutingIDs())
	assert.Nil(t, d.RoutingID())

	kr := keyRange(t, "40-80")
	d = DestinationKeyRange(kr)
	assert.Equal(t, KindKeyRange, d.Kind())
	assert.Same(t, kr, d.KeyRange())

	assert.Equal(t, KindAllShards, DestinationAllShards().Kind())
	assert.Equal(t, KindNone, DestinationNone().Kind())
	assert.Equal(t, "Unknown()", Destination{kind: 99}.String())
	require.Error(t, Destination{kind: 99}.Resolve(tableGroup(t, "two"), nil))
}
