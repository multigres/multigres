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

package shardingcatalogcache

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
)

const (
	testDB      = "postgres"
	waitFor     = 5 * time.Second
	waitTick    = 5 * time.Millisecond
	noPoll      = time.Duration(0)
	quietPeriod = 50 * time.Millisecond
)

// snapshotAt is a valid snapshot at routingVersion with the default
// tablegroup and extra tablegroups named in tableGroups, each one shard.
func snapshotAt(routingVersion int64, tableGroups ...string) *shardingcatalogpb.Snapshot {
	s := &shardingcatalogpb.Snapshot{
		RoutingVersion: routingVersion,
		Tablegroups:    []*shardingcatalogpb.TableGroup{{TablegroupId: 1, Name: "default", State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE}},
		Shards: []*shardingcatalogpb.Shard{{
			ShardId: 1, TablegroupId: 1, Name: "0-inf", KeyRange: &clustermetadatapb.KeyRange{}, State: shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE,
		}},
	}
	for i, name := range tableGroups {
		id := int64(i + 2)
		s.Tablegroups = append(s.Tablegroups, &shardingcatalogpb.TableGroup{TablegroupId: id, Name: name, State: shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_CREATING})
		s.Shards = append(s.Shards, &shardingcatalogpb.Shard{
			ShardId: id, TablegroupId: id, Name: "0-inf", KeyRange: &clustermetadatapb.KeyRange{}, State: shardingcatalogpb.ShardState_SHARD_STATE_PREPARING,
		})
	}
	return s
}

// fakePrimary answers fetches from what its catalog currently holds, serving
// under rule, and records the since of every fetch.
type fakePrimary struct {
	mu      sync.Mutex
	catalog *shardingcatalogpb.Snapshot
	rule    *clustermetadatapb.RuleNumber
	err     error
	calls   []*Position
	// gate, when set, blocks each fetch until it is closed or ctx ends.
	gate chan struct{}
}

func (f *fakePrimary) set(s *shardingcatalogpb.Snapshot, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.catalog, f.err = s, err
}

// setRule makes the fake serve under a new rule, as after a failover.
func (f *fakePrimary) setRule(term int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rule = rule(term)
}

func (f *fakePrimary) fetch(ctx context.Context, database string, since *Position) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error) {
	f.mu.Lock()
	f.calls = append(f.calls, since)
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
	if f.err != nil {
		return nil, f.err
	}
	v := f.catalog.GetRoutingVersion()
	if since != nil && since.Compare(Position{Rule: f.rule, RoutingVersion: v}) == 0 {
		return &multipoolermanagerdatapb.GetRoutingSnapshotResponse{Unchanged: true, RoutingVersion: v, ConsensusRule: f.rule}, nil
	}
	return &multipoolermanagerdatapb.GetRoutingSnapshotResponse{RoutingVersion: v, Snapshot: f.catalog, ConsensusRule: f.rule}, nil
}

func (f *fakePrimary) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakePrimary) lastSince() *Position {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[len(f.calls)-1]
}

func rule(term int64) *clustermetadatapb.RuleNumber {
	return &clustermetadatapb.RuleNumber{CoordinatorTerm: term}
}

func newTestCache(t *testing.T, f *fakePrimary, poll time.Duration) *Cache {
	t.Helper()
	return New(t.Context(), slog.New(slog.DiscardHandler), f.fetch, poll)
}

func waitForVersion(t *testing.T, c *Cache, want int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		s := c.Snapshot(testDB)
		return s != nil && s.RoutingVersion() == want
	}, waitFor, waitTick, "snapshot never reached routing_version %d", want)
}

func waitForCalls(t *testing.T, f *fakePrimary, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return f.callCount() >= want }, waitFor, waitTick)
}

func TestCache_FirstSightFetches(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(0)}
	c := newTestCache(t, f, noPoll)
	assert.Nil(t, c.Snapshot(testDB), "nothing before the database is seen")

	// The first sight fetches even from a replica's report.
	c.ObserveCatalogShard(testDB, false, nil, 0)
	waitForVersion(t, c, 0)
	assert.Nil(t, f.lastSince(), "no since without a cached snapshot")
	g, ok := c.Snapshot(testDB).TableGroupByName("default")
	require.True(t, ok)
	assert.Equal(t, "0-inf", g.ActiveShards()[0].Name())
	assert.Equal(t, []string{testDB}, c.Databases())
	assert.Nil(t, c.Snapshot("other"))
}

func TestCache_VersionBumpFetches(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(3)}
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, true, nil, 3)
	waitForVersion(t, c, 3)
	calls := f.callCount()

	// A report at or below the cached version does not fetch.
	c.ObserveCatalogShard(testDB, true, nil, 3)
	c.ObserveCatalogShard(testDB, true, nil, 2)
	// A replica's report never does, whatever it says.
	c.ObserveCatalogShard(testDB, false, nil, 9)
	time.Sleep(quietPeriod)
	assert.Equal(t, calls, f.callCount())

	f.set(snapshotAt(4, "commerce"), nil)
	c.ObserveCatalogShard(testDB, true, nil, 4)
	waitForVersion(t, c, 4)
	require.NotNil(t, f.lastSince())
	assert.Equal(t, int64(3), f.lastSince().RoutingVersion, "since is the cached position")
	_, ok := c.Snapshot(testDB).TableGroupByName("commerce")
	assert.True(t, ok)
}

func TestCache_PollBackstop(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(1)}
	c := newTestCache(t, f, 20*time.Millisecond)
	c.ObserveCatalogShard(testDB, true, nil, 1)
	waitForVersion(t, c, 1)

	// No health report of the change: the poll finds it.
	f.set(snapshotAt(2, "commerce"), nil)
	waitForVersion(t, c, 2)
}

func TestCache_NeverGoesBackwards(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(5, "commerce")}
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, true, nil, 5)
	waitForVersion(t, c, 5)
	installed := c.Snapshot(testDB)

	// A lagging primary serves an older catalog.
	f.set(snapshotAt(4), nil)
	c.ObserveCatalogShard(testDB, true, nil, 6)
	waitForCalls(t, f, 2)
	time.Sleep(quietPeriod)
	assert.Same(t, installed, c.Snapshot(testDB))

	// The same version again is not reinstalled either.
	f.set(snapshotAt(5), nil)
	c.ObserveCatalogShard(testDB, true, nil, 6)
	waitForCalls(t, f, 3)
	time.Sleep(quietPeriod)
	assert.Same(t, installed, c.Snapshot(testDB))
}

func TestCache_ConcurrentTriggersCoalesce(t *testing.T) {
	gate := make(chan struct{})
	f := &fakePrimary{catalog: snapshotAt(1), gate: gate}
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, true, nil, 1)
	waitForCalls(t, f, 1) // the first fetch is in flight, blocked on the gate

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() { c.ObserveCatalogShard(testDB, true, nil, 7) })
	}
	wg.Wait()
	time.Sleep(quietPeriod)
	assert.Equal(t, 1, f.callCount(), "no second fetch while one is in flight")

	f.set(snapshotAt(7), nil)
	close(gate)
	waitForVersion(t, c, 7)
	time.Sleep(quietPeriod)
	// The first fetch installed version 7 already, and the 50 triggers
	// queued one more, which found it unchanged.
	assert.Equal(t, 2, f.callCount())
}

func TestCache_FailedFetchKeepsTheSnapshot(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(2)}
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, true, nil, 2)
	waitForVersion(t, c, 2)
	installed := c.Snapshot(testDB)

	f.set(nil, errors.New("no writable pooler"))
	c.ObserveCatalogShard(testDB, true, nil, 3)
	waitForCalls(t, f, 2)
	time.Sleep(quietPeriod)
	assert.Same(t, installed, c.Snapshot(testDB))

	// The next trigger retries.
	f.set(snapshotAt(3), nil)
	c.ObserveCatalogShard(testDB, true, nil, 3)
	waitForVersion(t, c, 3)
}

func TestCache_InvalidSnapshotKeepsTheSnapshot(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(2)}
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, true, nil, 2)
	waitForVersion(t, c, 2)
	installed := c.Snapshot(testDB)

	bad := snapshotAt(3)
	bad.Tablegroups = bad.Tablegroups[1:] // no default tablegroup
	bad.Shards = nil
	f.set(bad, nil)
	c.ObserveCatalogShard(testDB, true, nil, 3)
	waitForCalls(t, f, 2)
	time.Sleep(quietPeriod)
	assert.Same(t, installed, c.Snapshot(testDB))
}

func TestCache_NoSnapshotYetRetriesOnAPrimaryReport(t *testing.T) {
	f := &fakePrimary{}
	f.set(nil, errors.New("no writable pooler"))
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, false, nil, 0)
	waitForCalls(t, f, 1)
	assert.Nil(t, c.Snapshot(testDB))

	// The primary shows up, reporting the fresh catalog's version 0.
	f.set(snapshotAt(0), nil)
	c.ObserveCatalogShard(testDB, true, nil, 0)
	waitForVersion(t, c, 0)
}

func TestCache_StopsWithItsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	f := &fakePrimary{catalog: snapshotAt(1)}
	c := New(ctx, slog.New(slog.DiscardHandler), f.fetch, 10*time.Millisecond)
	c.ObserveCatalogShard(testDB, true, nil, 1)
	waitForVersion(t, c, 1)
	cancel()
	time.Sleep(quietPeriod)
	calls := f.callCount()
	time.Sleep(quietPeriod)
	assert.Equal(t, calls, f.callCount(), "no polls after the context ends")
}

func TestCache_RuleOrdering(t *testing.T) {
	t.Run("a higher rule wins even at a lower version", func(t *testing.T) {
		// The old primary's version 5 was lost in the failover; the new one
		// serves version 4 under a higher rule.
		f := &fakePrimary{catalog: snapshotAt(5, "commerce"), rule: rule(1)}
		c := newTestCache(t, f, noPoll)
		c.ObserveCatalogShard(testDB, true, rule(1), 5)
		waitForVersion(t, c, 5)

		f.set(snapshotAt(4), nil)
		f.setRule(2)
		c.ObserveCatalogShard(testDB, true, rule(2), 4)
		waitForVersion(t, c, 4)
		_, pos := c.Installed(testDB)
		assert.Equal(t, int64(2), pos.Rule.GetCoordinatorTerm())
	})
	t.Run("the same version under a new rule is fetched again", func(t *testing.T) {
		// A lost version reused by the new primary with different contents.
		f := &fakePrimary{catalog: snapshotAt(5), rule: rule(1)}
		c := newTestCache(t, f, noPoll)
		c.ObserveCatalogShard(testDB, true, rule(1), 5)
		waitForVersion(t, c, 5)

		f.set(snapshotAt(5, "commerce"), nil)
		f.setRule(2)
		c.ObserveCatalogShard(testDB, true, rule(2), 5)
		require.Eventually(t, func() bool {
			_, ok := c.Snapshot(testDB).TableGroupByName("commerce")
			return ok
		}, waitFor, waitTick, "the reused version was never refetched")
	})
	t.Run("a lower rule never wins, even at a higher version", func(t *testing.T) {
		f := &fakePrimary{catalog: snapshotAt(5), rule: rule(2)}
		c := newTestCache(t, f, noPoll)
		c.ObserveCatalogShard(testDB, true, rule(2), 5)
		waitForVersion(t, c, 5)
		installed := c.Snapshot(testDB)

		// A former primary that has not learned it was superseded.
		f.set(snapshotAt(9, "commerce"), nil)
		f.setRule(1)
		c.ObserveCatalogShard(testDB, true, rule(1), 9)
		time.Sleep(quietPeriod)
		assert.Equal(t, 1, f.callCount(), "a report from a lower rule does not even fetch")

		// Even if a poll reaches it, its snapshot is refused.
		c.ObserveCatalogShard(testDB, true, rule(3), 0)
		waitForCalls(t, f, 2)
		time.Sleep(quietPeriod)
		assert.Same(t, installed, c.Snapshot(testDB))
	})
}

// waitResult starts WaitForRoutingVersion in the background and returns the
// channel its result arrives on.
func waitResult(ctx context.Context, c *Cache, name string, after int64) <-chan error {
	done := make(chan error, 1)
	go func() { done <- c.WaitForRoutingVersion(ctx, name, after) }()
	return done
}

// requireBlocked checks, after a quiet period, that no wait has returned.
func requireBlocked(t *testing.T, dones ...<-chan error) {
	t.Helper()
	time.Sleep(quietPeriod)
	for _, done := range dones {
		select {
		case err := <-done:
			require.FailNow(t, "WaitForRoutingVersion returned early", "err: %v", err)
		default:
		}
	}
}

// settle lets the fetches the first observation triggered finish, so a later
// fetch count is only what the test causes.
func settle(t *testing.T, f *fakePrimary) int {
	t.Helper()
	time.Sleep(quietPeriod)
	return f.callCount()
}

func requireReturned(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(waitFor):
		require.FailNow(t, "WaitForRoutingVersion never returned")
		return nil
	}
}

// The wait returns only once a snapshot past after is installed: not for the
// cached one, nor an install that only reaches after.
func TestWaitForRoutingVersion_ReturnsOnlyPastAfter(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(3)}
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, true, nil, 3)
	waitForVersion(t, c, 3)

	done := waitResult(t.Context(), c, testDB, 4)
	requireBlocked(t, done)

	f.set(snapshotAt(4), nil)
	c.ObserveCatalogShard(testDB, true, nil, 4)
	waitForVersion(t, c, 4)
	requireBlocked(t, done)

	f.set(snapshotAt(5), nil)
	c.ObserveCatalogShard(testDB, true, nil, 5)
	require.NoError(t, requireReturned(t, done))
	assert.Equal(t, int64(5), c.Snapshot(testDB).RoutingVersion())
	assert.Equal(t, int64(5), c.RoutingVersion(testDB))
	assert.Zero(t, c.RoutingVersion("other"), "0 without a snapshot")
}

// The wait fetches at once instead of sitting until a health report or poll,
// neither of which comes here.
func TestWaitForRoutingVersion_TriggersAFetch(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(3)}
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, true, nil, 3)
	waitForVersion(t, c, 3)
	calls := settle(t, f)

	f.set(snapshotAt(4), nil)
	require.NoError(t, requireReturned(t, waitResult(t.Context(), c, testDB, 3)))
	assert.Equal(t, int64(4), c.Snapshot(testDB).RoutingVersion())
	assert.Equal(t, calls+1, f.callCount())
}

// A snapshot already past after returns at once, without a fetch.
func TestWaitForRoutingVersion_AlreadyPast(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(6)}
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, true, nil, 6)
	waitForVersion(t, c, 6)
	calls := settle(t, f)

	require.NoError(t, c.WaitForRoutingVersion(t.Context(), testDB, 5))
	time.Sleep(quietPeriod)
	assert.Equal(t, calls, f.callCount())
}

// A database the cache has never seen starts its fetch loop, and the wait
// ends with its first install past after.
func TestWaitForRoutingVersion_UnseenDatabase(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(2)}
	c := newTestCache(t, f, noPoll)
	require.NoError(t, requireReturned(t, waitResult(t.Context(), c, testDB, 1)))
	assert.Equal(t, []string{testDB}, c.Databases())
	assert.Equal(t, int64(2), c.Snapshot(testDB).RoutingVersion())
}

func TestWaitForRoutingVersion_HonoursItsContext(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(3)}
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, true, nil, 3)
	waitForVersion(t, c, 3)

	ctx, cancel := context.WithCancel(t.Context())
	done := waitResult(ctx, c, testDB, 3)
	requireBlocked(t, done)
	cancel()
	assert.ErrorIs(t, requireReturned(t, done), context.Canceled)

	ctx, cancel = context.WithTimeout(t.Context(), quietPeriod)
	defer cancel()
	assert.ErrorIs(t, c.WaitForRoutingVersion(ctx, testDB, 3), context.DeadlineExceeded)
}

// Many waiters on one database are each released by the first install past
// their own after.
func TestWaitForRoutingVersion_ManyWaiters(t *testing.T) {
	f := &fakePrimary{catalog: snapshotAt(3)}
	c := newTestCache(t, f, noPoll)
	c.ObserveCatalogShard(testDB, true, nil, 3)
	waitForVersion(t, c, 3)

	const n = 50
	var early, late []<-chan error
	for range n {
		early = append(early, waitResult(t.Context(), c, testDB, 3))
		late = append(late, waitResult(t.Context(), c, testDB, 4))
	}
	requireBlocked(t, append(early, late...)...)

	f.set(snapshotAt(4), nil)
	c.ObserveCatalogShard(testDB, true, nil, 4)
	for _, done := range early {
		require.NoError(t, requireReturned(t, done))
	}
	requireBlocked(t, late...)

	f.set(snapshotAt(5), nil)
	c.ObserveCatalogShard(testDB, true, nil, 5)
	for _, done := range late {
		require.NoError(t, requireReturned(t, done))
	}
}
