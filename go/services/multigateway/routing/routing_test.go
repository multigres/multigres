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

package routing

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	querypb "github.com/multigres/multigres/go/pb/query"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
)

// fakeCatalog answers Await from a snapshot or an error per database. A
// database it does not know has no catalog.
type fakeCatalog struct {
	snapshots map[string]*shardingcatalog.Snapshot
	errs      map[string]error
	awaited   []string
}

func (f *fakeCatalog) Await(ctx context.Context, database string) (*shardingcatalog.Snapshot, error) {
	f.awaited = append(f.awaited, database)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := f.errs[database]; err != nil {
		return nil, err
	}
	return f.snapshots[database], nil
}

// catalogBuilder builds a valid sharding catalog snapshot: the default
// tablegroup with its 0-inf shard, and whatever is added.
type catalogBuilder struct {
	pb     *shardingcatalogpb.Snapshot
	tgIDs  map[string]int64
	shards map[string]int64
}

func newCatalog() *catalogBuilder {
	b := &catalogBuilder{pb: &shardingcatalogpb.Snapshot{RoutingVersion: 1}, tgIDs: map[string]int64{}, shards: map[string]int64{}}
	b.tablegroup("default", "0-inf")
	return b
}

// tablegroup adds an active tablegroup with active shards; a shard named
// with a "+" suffix is added preparing instead (the suffix is dropped).
func (b *catalogBuilder) tablegroup(name string, shards ...string) *catalogBuilder {
	id := int64(len(b.pb.Tablegroups) + 1)
	b.tgIDs[name] = id
	b.pb.Tablegroups = append(b.pb.Tablegroups, &shardingcatalogpb.TableGroup{
		TablegroupId: id, Name: name, State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE,
	})
	for _, sh := range shards {
		state := shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE
		if sh[len(sh)-1] == '+' {
			sh, state = sh[:len(sh)-1], shardingcatalogpb.ShardState_SHARD_STATE_PREPARING
		}
		kr, err := keyrange.ParseShardName(sh)
		if err != nil {
			panic(err)
		}
		shardID := int64(len(b.pb.Shards) + 1)
		b.shards[name+"/"+sh] = shardID
		b.pb.Shards = append(b.pb.Shards, &shardingcatalogpb.Shard{
			ShardId: shardID, TablegroupId: id, Name: sh, State: state,
			KeyRange: &clustermetadatapb.KeyRange{Start: kr.GetStart(), End: kr.GetEnd()},
		})
	}
	return b
}

// rule adds a shard rule from tablegroup from to shard of tablegroup to.
func (b *catalogBuilder) rule(from, to, shard string) *catalogBuilder {
	b.pb.RoutingRules = append(b.pb.RoutingRules, &shardingcatalogpb.RoutingRule{
		RuleId: int64(len(b.pb.RoutingRules) + 1), OperationId: "op",
		Scope:            shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP,
		FromTablegroupId: b.tgIDs[from], ToTablegroupId: b.tgIDs[to], ToShardId: new(b.shards[to+"/"+shard]),
	})
	return b
}

func (b *catalogBuilder) build(t *testing.T) *shardingcatalog.Snapshot {
	t.Helper()
	s, err := shardingcatalog.New(b.pb)
	require.NoError(t, err)
	return s
}

// importCatalog routes the default tablegroup to srctg, as an import does.
func importCatalog(t *testing.T) *shardingcatalog.Snapshot {
	return newCatalog().tablegroup("srctg", "0-inf").rule("default", "srctg", "0-inf").build(t)
}

func target(database, tablegroup, shard string) *querypb.Target {
	return &querypb.Target{
		ShardRef: &clustermetadatapb.ShardRef{Database: database, TableGroup: tablegroup, Shard: shard},
		Mode:     querypb.Mode_MODE_WRITABLE,
	}
}

func key(t *querypb.Target) string {
	ref := t.GetShardRef()
	return ref.GetDatabase() + "/" + ref.GetTableGroup() + "/" + ref.GetShard()
}

func TestResolve_NoRulesIsTheSameTarget(t *testing.T) {
	r := NewRouter(&fakeCatalog{snapshots: map[string]*shardingcatalog.Snapshot{"db1": newCatalog().build(t)}})
	in := target("db1", "default", "0-inf")
	out, err := r.Resolve(context.Background(), in)
	require.NoError(t, err)
	assert.Same(t, in, out, "no rule: the planned target itself, no copy")
}

func TestResolve_RuleRewritesOnlyItsDatabase(t *testing.T) {
	r := NewRouter(&fakeCatalog{snapshots: map[string]*shardingcatalog.Snapshot{
		"db1": importCatalog(t),
		"db2": newCatalog().build(t),
	}})
	ctx := context.Background()

	in := target("db1", "default", "0-inf")
	in.Mode = querypb.Mode_MODE_INCONSISTENT
	out, err := r.Resolve(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, "db1/srctg/0-inf", key(out))
	assert.Equal(t, querypb.Mode_MODE_INCONSISTENT, out.GetMode(), "the mode is kept")
	assert.Equal(t, "db1/default/0-inf", key(in), "the planned target is not modified")

	out, err = r.Resolve(ctx, target("db2", "default", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db2/default/0-inf", key(out), "another database keeps its placement")

	out, err = r.Resolve(ctx, target("db1", "other", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/other/0-inf", key(out), "a tablegroup the catalog does not know keeps its placement")
}

// Without a catalog, or for a database whose catalog primary predates the
// snapshot, there are no rules.
func TestResolve_NoCatalogMeansNoRules(t *testing.T) {
	in := target("db1", "default", "0-inf")
	for name, r := range map[string]*Router{
		"no catalog cache":         NewRouter(nil),
		"nil router":               nil,
		"catalog primary predates": NewRouter(&fakeCatalog{}),
	} {
		t.Run(name, func(t *testing.T) {
			out, err := r.Resolve(context.Background(), in)
			require.NoError(t, err)
			assert.Same(t, in, out)
		})
	}
}

func TestResolve_UnknownRoutingFailsClosed(t *testing.T) {
	catalog := &fakeCatalog{errs: map[string]error{"db1": errors.New("no writable pooler")}}
	r := NewRouter(catalog)

	_, err := r.Resolve(context.Background(), target("db1", "default", "0-inf"))
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSCannotConnectNow), "57P03, retryable: %v", err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))

	// The next statement asks again, and succeeds once the catalog answers.
	catalog.errs = nil
	catalog.snapshots = map[string]*shardingcatalog.Snapshot{"db1": importCatalog(t)}
	out, err := r.Resolve(context.Background(), target("db1", "default", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/srctg/0-inf", key(out))
}

func TestResolve_CancelledWhileWaitingFailsClosed(t *testing.T) {
	r := NewRouter(&fakeCatalog{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := r.Resolve(ctx, target("db1", "default", "0-inf"))
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSCannotConnectNow))
}

func TestResolve_ShardRules(t *testing.T) {
	snap := newCatalog().
		// commerce's 0-80 is split into 0-40 and 40-80.
		tablegroup("commerce", "0-80", "80-inf", "0-40+", "40-80+").
		rule("commerce", "commerce", "0-40").
		rule("commerce", "commerce", "40-80").
		// orders moves whole to orders2.
		tablegroup("orders", "0-inf").
		tablegroup("orders2", "0-inf").
		rule("orders", "orders2", "0-inf").
		tablegroup("users", "0-inf").
		build(t)
	r := NewRouter(&fakeCatalog{snapshots: map[string]*shardingcatalog.Snapshot{"db1": snap}})
	ctx := context.Background()

	out, err := r.Resolve(ctx, target("db1", "commerce", "80-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/commerce/80-inf", key(out), "a shard no rule covers keeps its placement")

	_, err = r.Resolve(ctx, target("db1", "commerce", "0-80"))
	require.Error(t, err, "a split shard needs a routing identifier")
	assert.Equal(t, mtrpcpb.Code_UNIMPLEMENTED, mterrors.Code(err))

	out, err = r.Resolve(ctx, target("db1", "commerce", "0-40"))
	require.NoError(t, err)
	assert.Equal(t, "db1/commerce/0-40", key(out), "a shard inside one rule's range goes to that rule's shard")

	out, err = r.Resolve(ctx, target("db1", "orders", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/orders2/0-inf", key(out))

	out, err = r.Resolve(ctx, target("db1", "users", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/users/0-inf", key(out))
}
