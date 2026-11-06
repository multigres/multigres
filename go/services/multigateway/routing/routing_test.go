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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	mdpb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// fakeSource serves canned snapshots per database and counts reads.
type fakeSource struct {
	mu    sync.Mutex
	snaps map[string]*mdpb.GetRoutingSnapshotResponse
	errs  map[string]error
	reads map[string]int
	// waited records, per database, the waitForFailover of each read.
	waited map[string][]bool
	// gate, when set, blocks every read until it is closed.
	gate chan struct{}
}

func newFakeSource() *fakeSource {
	return &fakeSource{snaps: map[string]*mdpb.GetRoutingSnapshotResponse{}, errs: map[string]error{}, reads: map[string]int{}, waited: map[string][]bool{}}
}

func (f *fakeSource) GetRoutingSnapshot(ctx context.Context, database string, waitForFailover bool) (*mdpb.GetRoutingSnapshotResponse, error) {
	f.mu.Lock()
	f.reads[database]++
	f.waited[database] = append(f.waited[database], waitForFailover)
	gate := f.gate
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs[database]; err != nil {
		return nil, err
	}
	if snap := f.snaps[database]; snap != nil {
		return snap, nil
	}
	return &mdpb.GetRoutingSnapshotResponse{Full: true}, nil
}

func (f *fakeSource) set(database string, snap *mdpb.GetRoutingSnapshotResponse, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snaps[database], f.errs[database] = snap, err
}

func (f *fakeSource) readCount(database string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads[database]
}

// importSnapshot routes the default tablegroup to srctg's 0-inf shard.
func importSnapshot(version int64) *mdpb.GetRoutingSnapshotResponse {
	return &mdpb.GetRoutingSnapshotResponse{RoutingVersion: version, Full: true, Rules: []*mdpb.RoutingRule{{
		Scope:          mdpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP,
		FromTablegroup: "default",
		ToTablegroup:   "srctg",
		ToShard:        "0-inf",
		ToKeyRange:     &clustermetadatapb.KeyRange{},
	}}}
}

var testConfig = Config{PollInterval: time.Hour, FetchTimeout: time.Second, FirstFetchTimeout: 5 * time.Second, IdleTimeout: time.Minute}

func newTestCache(src SnapshotSource) *Cache {
	return NewCache(context.Background(), src, testConfig, slog.New(slog.DiscardHandler))
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
	c := newTestCache(newFakeSource())
	in := target("db1", "default", "0-inf")
	out, err := c.Resolve(context.Background(), in)
	require.NoError(t, err)
	assert.Same(t, in, out, "no rule: the planned target itself, no copy")
}

func TestResolve_RuleRewritesOnlyItsDatabase(t *testing.T) {
	src := newFakeSource()
	src.set("db1", importSnapshot(1), nil)
	c := newTestCache(src)
	ctx := context.Background()

	in := target("db1", "default", "0-inf")
	in.Mode = querypb.Mode_MODE_INCONSISTENT
	out, err := c.Resolve(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, "db1/srctg/0-inf", key(out))
	assert.Equal(t, querypb.Mode_MODE_INCONSISTENT, out.GetMode(), "the mode is kept")
	assert.Equal(t, "db1/default/0-inf", key(in), "the planned target is not modified")

	out, err = c.Resolve(ctx, target("db2", "default", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db2/default/0-inf", key(out), "another database keeps its placement")

	out, err = c.Resolve(ctx, target("db1", "other", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/other/0-inf", key(out), "another tablegroup of the database keeps its placement")
}

func TestResolve_ControlTargetIsNeverRewritten(t *testing.T) {
	src := newFakeSource()
	src.set("db1", importSnapshot(1), nil)
	src.set("db2", nil, errors.New("catalog primary unreachable"))
	c := newTestCache(src)

	for _, db := range []string{"db1", "db2"} {
		in := target(db, "default", "0-inf")
		in.Control = true
		out, err := c.Resolve(context.Background(), in)
		require.NoError(t, err, "control traffic needs no routing")
		assert.Same(t, in, out)
	}
	assert.Zero(t, src.readCount("db1")+src.readCount("db2"), "control traffic does not fetch routing")
}

func TestResolve_UnknownRoutingFailsClosed(t *testing.T) {
	src := newFakeSource()
	src.set("db1", nil, status.Error(codes.Unavailable, "no primary"))
	c := newTestCache(src)

	_, err := c.Resolve(context.Background(), target("db1", "default", "0-inf"))
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSCannotConnectNow), "57P03, retryable: %v", err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))

	// The next statement tries again, and succeeds once the catalog answers.
	src.set("db1", importSnapshot(1), nil)
	out, err := c.Resolve(context.Background(), target("db1", "default", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/srctg/0-inf", key(out))
}

func TestResolve_CancelledWhileWaitingFailsClosed(t *testing.T) {
	src := newFakeSource()
	src.gate = make(chan struct{})
	defer close(src.gate)
	c := newTestCache(src)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Resolve(ctx, target("db1", "default", "0-inf"))
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSCannotConnectNow))
}

// The first statements of a database wait for one snapshot read, whatever
// their number.
func TestResolve_FirstUseIsSingleFlight(t *testing.T) {
	src := newFakeSource()
	src.set("db1", importSnapshot(1), nil)
	src.gate = make(chan struct{})
	c := newTestCache(src)

	const n = 20
	var wg sync.WaitGroup
	var ok atomic.Int32
	for range n {
		wg.Go(func() {
			out, err := c.Resolve(context.Background(), target("db1", "default", "0-inf"))
			if err == nil && key(out) == "db1/srctg/0-inf" {
				ok.Add(1)
			}
		})
	}
	require.Eventually(t, func() bool { return src.readCount("db1") == 1 }, time.Second, time.Millisecond)
	close(src.gate)
	wg.Wait()
	assert.Equal(t, int32(n), ok.Load())
	assert.Equal(t, 1, src.readCount("db1"))
}

func TestPoll_InstallsOnlyNewerVersionsAndNotifies(t *testing.T) {
	src := newFakeSource()
	src.set("db1", importSnapshot(3), nil)
	c := newTestCache(src)
	var changes []string
	c.OnChange(func(db string) { changes = append(changes, db) })
	ctx := context.Background()

	_, err := c.Resolve(ctx, target("db1", "default", "0-inf"))
	require.NoError(t, err)
	assert.Empty(t, changes, "learning the routing is not a change")

	poll := func() {
		c.pollOnce()
		require.Eventually(t, func() bool {
			d := c.database("db1")
			d.mu.Lock()
			defer d.mu.Unlock()
			return d.fetch == nil
		}, time.Second, time.Millisecond)
	}

	// An older snapshot (a lagging read) is ignored.
	src.set("db1", &mdpb.GetRoutingSnapshotResponse{RoutingVersion: 2, Full: true}, nil)
	poll()
	out, _ := c.Resolve(ctx, target("db1", "default", "0-inf"))
	assert.Equal(t, "db1/srctg/0-inf", key(out))
	assert.Empty(t, changes)

	// A failed read keeps the last snapshot.
	src.set("db1", nil, errors.New("catalog primary unreachable"))
	poll()
	out, err = c.Resolve(ctx, target("db1", "default", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/srctg/0-inf", key(out))

	// A newer one is installed and reported.
	src.set("db1", &mdpb.GetRoutingSnapshotResponse{RoutingVersion: 4, Full: true}, nil)
	poll()
	out, _ = c.Resolve(ctx, target("db1", "default", "0-inf"))
	assert.Equal(t, "db1/default/0-inf", key(out), "the rule was deleted")
	assert.Equal(t, []string{"db1"}, changes)
}

func TestPoll_ForgetsIdleDatabases(t *testing.T) {
	src := newFakeSource()
	c := newTestCache(src)
	now := time.Now()
	c.now = func() time.Time { return now }

	_, err := c.Resolve(context.Background(), target("db1", "default", "0-inf"))
	require.NoError(t, err)

	now = now.Add(30 * time.Second)
	c.pollOnce()
	require.Eventually(t, func() bool { return src.readCount("db1") == 2 }, time.Second, time.Millisecond, "a database in use is polled")

	now = now.Add(2 * time.Minute)
	c.pollOnce()
	c.mu.Lock()
	_, kept := c.databases["db1"]
	c.mu.Unlock()
	assert.False(t, kept, "a database idle for longer than IdleTimeout is forgotten")
}

// A catalog primary that predates routing rules serves no snapshot: the
// database has no rules. The gateway says so once per database.
func TestResolve_UnimplementedMeansNoRules(t *testing.T) {
	src := newFakeSource()
	src.set("db1", nil, status.Error(codes.Unimplemented, "unknown method GetRoutingSnapshot"))
	src.set("db2", nil, status.Error(codes.Unimplemented, "unknown method GetRoutingSnapshot"))
	var logs bytes.Buffer
	c := NewCache(context.Background(), src, testConfig,
		slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))

	for _, db := range []string{"db1", "db2"} {
		out, err := c.Resolve(context.Background(), target(db, "default", "0-inf"))
		require.NoError(t, err)
		assert.Equal(t, db+"/default/0-inf", key(out))
	}
	c.pollOnce()
	require.Eventually(t, func() bool { return src.readCount("db1") == 2 && src.readCount("db2") == 2 }, time.Second, time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, 1, strings.Count(logs.String(), "database=db1"), "warned once for db1: %s", logs.String())
	assert.Equal(t, 1, strings.Count(logs.String(), "database=db2"), "warned once for db2: %s", logs.String())
}

func TestResolve_ShardRules(t *testing.T) {
	rule := func(from, to, shard string, start, end []byte) *mdpb.RoutingRule {
		return &mdpb.RoutingRule{
			Scope: mdpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP, FromTablegroup: from, ToTablegroup: to, ToShard: shard,
			ToKeyRange: &clustermetadatapb.KeyRange{Start: start, End: end},
		}
	}
	src := newFakeSource()
	src.set("db1", &mdpb.GetRoutingSnapshotResponse{RoutingVersion: 1, Rules: []*mdpb.RoutingRule{
		// commerce's 0-80 is split into 0-40 and 40-80.
		rule("commerce", "commerce", "0-40", nil, []byte{0x40}),
		rule("commerce", "commerce", "40-80", []byte{0x40}, []byte{0x80}),
		// orders moves whole to orders2.
		rule("orders", "orders2", "0-inf", nil, nil),
		// A table rule applies at plan time and is ignored here.
		{Scope: mdpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLE, FromTablegroup: "users", ToTablegroup: "users2"},
	}}, nil)
	c := newTestCache(src)
	ctx := context.Background()

	out, err := c.Resolve(ctx, target("db1", "commerce", "80-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/commerce/80-inf", key(out), "a shard no rule covers keeps its placement")

	_, err = c.Resolve(ctx, target("db1", "commerce", "0-80"))
	require.Error(t, err, "a split shard needs a routing identifier")
	assert.Equal(t, mtrpcpb.Code_UNIMPLEMENTED, mterrors.Code(err))

	out, err = c.Resolve(ctx, target("db1", "orders", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/orders2/0-inf", key(out))

	out, err = c.Resolve(ctx, target("db1", "users", "0-inf"))
	require.NoError(t, err)
	assert.Equal(t, "db1/users/0-inf", key(out))
}

func TestCache_StartAndClose(t *testing.T) {
	src := newFakeSource()
	c := NewCache(context.Background(), src, Config{PollInterval: 5 * time.Millisecond, FetchTimeout: time.Second, FirstFetchTimeout: 5 * time.Second, IdleTimeout: time.Minute}, slog.New(slog.DiscardHandler))
	_, err := c.Resolve(context.Background(), target("db1", "default", "0-inf"))
	require.NoError(t, err)
	c.Start()
	require.Eventually(t, func() bool { return src.readCount("db1") >= 3 }, time.Second, time.Millisecond, "the poll reads again")
	c.Close()
	n := src.readCount("db1")
	time.Sleep(20 * time.Millisecond)
	assert.LessOrEqual(t, src.readCount("db1"), n+1, "no polling after Close")

	// Closing a cache that never started returns.
	newTestCache(src).Close()
}

// The read the first statement of a database waits for waits out a failover
// of the catalog primary, however long the background poll's timeout is; the
// statement's own context still bounds its wait. The poll reads without
// waiting, and only databases whose routing the gateway has.
func TestResolve_FirstReadWaitsOutAFailover(t *testing.T) {
	src := newFakeSource()
	src.set("db1", importSnapshot(1), nil)
	src.gate = make(chan struct{})
	c := NewCache(context.Background(), src,
		Config{PollInterval: time.Hour, FetchTimeout: 10 * time.Millisecond, FirstFetchTimeout: 5 * time.Second, IdleTimeout: time.Minute},
		slog.New(slog.DiscardHandler))

	// A statement whose context ends first fails closed.
	shortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := c.Resolve(shortCtx, target("db1", "default", "0-inf"))
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSCannotConnectNow))

	// A database with no routing yet is not polled.
	c.pollOnce()

	// Another statement waits longer than the poll timeout and gets the
	// routing once the failover ends.
	done := make(chan *querypb.Target, 1)
	go func() {
		out, err := c.Resolve(context.Background(), target("db1", "default", "0-inf"))
		if err == nil {
			done <- out
		}
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	close(src.gate)
	out := <-done
	require.NotNil(t, out, "the statement should succeed once the read returns")
	assert.Equal(t, "db1/srctg/0-inf", key(out))

	src.mu.Lock()
	assert.Equal(t, []bool{true}, src.waited["db1"], "one read, the first-use read that waits out a failover")
	src.mu.Unlock()

	c.pollOnce()
	require.Eventually(t, func() bool { return src.readCount("db1") == 2 }, time.Second, time.Millisecond)
	src.mu.Lock()
	assert.Equal(t, []bool{true, false}, src.waited["db1"], "the poll reads without waiting")
	src.mu.Unlock()
}
