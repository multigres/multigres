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

package topoclient_test

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func discardCacheLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// waitForCount polls until Count() reaches n or the timeout elapses.
func waitForCacheCount(t *testing.T, cache *topoclient.PoolerCache, n int) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cache.Count() == n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func newTestPooler(cell, name, database, tableGroup, shard string, typ clustermetadatapb.PoolerType) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id:         &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: cell, Name: name},
		Database:   database,
		TableGroup: tableGroup,
		Shard:      shard,
		Type:       typ,
		Hostname:   name + ".host",
	}
}

// TestPoolerCache_InitialDiscovery verifies that poolers already in etcd when Start is
// called are loaded and visible via Get/All/Count.
func TestPoolerCache_InitialDiscovery(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY)))
	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p2", "db", "tg", "0", clustermetadatapb.PoolerType_REPLICA)))

	cache := topoclient.NewPoolerCache(ctx, ts, discardCacheLogger())
	cache.Start()
	defer cache.Stop()

	require.True(t, waitForCacheCount(t, cache, 2))

	p, ok := cache.Get(topoclient.MultiPoolerIDString(&clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p1",
	}))
	require.True(t, ok)
	assert.Equal(t, "p1.host", p.Hostname)

	assert.Equal(t, 2, cache.Count())
	assert.Len(t, cache.All(), 2)
	assert.Len(t, cache.AllForCell("zone1"), 2)
}

// TestPoolerCache_UpsertAfterStart verifies that a pooler added to etcd after Start is
// reflected in the cache.
func TestPoolerCache_UpsertAfterStart(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cache := topoclient.NewPoolerCache(ctx, ts, discardCacheLogger())
	cache.Start()
	defer cache.Stop()

	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY)))

	require.True(t, waitForCacheCount(t, cache, 1))
	assert.Len(t, cache.AllForCell("zone1"), 1)
}

// TestPoolerCache_DeletionEventHandled verifies that a pooler deleted from etcd is removed
// from the cache.
func TestPoolerCache_DeletionEventHandled(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY)))

	cache := topoclient.NewPoolerCache(ctx, ts, discardCacheLogger())
	cache.Start()
	defer cache.Stop()

	require.True(t, waitForCacheCount(t, cache, 1))

	require.NoError(t, ts.UnregisterMultiPooler(ctx, &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p1",
	}))

	require.True(t, waitForCacheCount(t, cache, 0))
	assert.Empty(t, cache.AllForCell("zone1"))
}

// TestPoolerCache_SubscribeReceivesReplay verifies that a subscriber registered after some
// poolers are already in the cache receives replay notifications for them.
func TestPoolerCache_SubscribeReceivesReplay(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY)))

	cache := topoclient.NewPoolerCache(ctx, ts, discardCacheLogger())
	cache.Start()
	defer cache.Stop()

	require.True(t, waitForCacheCount(t, cache, 1))

	// Subscribe after pooler is in cache — should receive replay.
	var mu sync.Mutex
	var seen []string
	unsub := cache.Subscribe(func(p *clustermetadatapb.MultiPooler, removed bool) {
		if !removed {
			mu.Lock()
			seen = append(seen, p.Id.Name)
			mu.Unlock()
		}
	})
	defer unsub()

	// Wait for the replay to be delivered asynchronously.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, seen, "p1")
}

// TestPoolerCache_SubscribeReceivesLiveEvents verifies that a subscriber receives
// notifications for poolers added after subscription.
func TestPoolerCache_SubscribeReceivesLiveEvents(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cache := topoclient.NewPoolerCache(ctx, ts, discardCacheLogger())
	cache.Start()
	defer cache.Stop()

	added := make(chan string, 10)
	unsub := cache.Subscribe(func(p *clustermetadatapb.MultiPooler, removed bool) {
		if !removed {
			added <- p.Id.Name
		}
	})
	defer unsub()

	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY)))

	select {
	case name := <-added:
		assert.Equal(t, "p1", name)
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber did not receive live event within 5s")
	}
}

// TestPoolerCache_SubscribeUnsubscribe verifies that unsubscribing stops future deliveries.
func TestPoolerCache_SubscribeUnsubscribe(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cache := topoclient.NewPoolerCache(ctx, ts, discardCacheLogger())
	cache.Start()
	defer cache.Stop()

	var mu sync.Mutex
	var count int
	unsub := cache.Subscribe(func(_ *clustermetadatapb.MultiPooler, _ bool) {
		mu.Lock()
		count++
		mu.Unlock()
	})

	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY)))
	require.True(t, waitForCacheCount(t, cache, 1))

	// Unsubscribe and ensure no further notifications.
	unsub()

	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p2", "db", "tg", "0", clustermetadatapb.PoolerType_REPLICA)))
	require.True(t, waitForCacheCount(t, cache, 2))

	mu.Lock()
	countAfterUnsub := count
	mu.Unlock()

	// Give delivery goroutine time to settle; count should not increase.
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	assert.Equal(t, countAfterUnsub, count, "no notifications should arrive after unsubscribe")
	mu.Unlock()
}

// TestPoolerCache_CellStatuses verifies that CellStatuses returns per-cell information
// including LastActivity.
func TestPoolerCache_CellStatuses(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY)))

	before := time.Now()
	cache := topoclient.NewPoolerCache(ctx, ts, discardCacheLogger())
	cache.Start()
	defer cache.Stop()

	require.True(t, waitForCacheCount(t, cache, 1))

	statuses := cache.CellStatuses()
	require.Len(t, statuses, 1)
	assert.Equal(t, "zone1", statuses[0].Cell)
	assert.Len(t, statuses[0].Poolers, 1)
	assert.True(t, statuses[0].LastActivity.After(before), "LastActivity should be set after pooler discovery")
}

// TestPoolerCache_MultipleSubscribers verifies that multiple subscribers all receive events.
func TestPoolerCache_MultipleSubscribers(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cache := topoclient.NewPoolerCache(ctx, ts, discardCacheLogger())
	cache.Start()
	defer cache.Stop()

	const numSubs = 3
	chs := make([]chan string, numSubs)
	unsubs := make([]func(), numSubs)
	for i := range numSubs {
		ch := make(chan string, 10)
		chs[i] = ch
		unsubs[i] = cache.Subscribe(func(p *clustermetadatapb.MultiPooler, removed bool) {
			if !removed {
				ch <- p.Id.Name
			}
		})
	}
	defer func() {
		for _, u := range unsubs {
			u()
		}
	}()

	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY)))

	for i, ch := range chs {
		select {
		case name := <-ch:
			assert.Equal(t, "p1", name, "subscriber %d", i)
		case <-time.After(5 * time.Second):
			t.Fatalf("subscriber %d did not receive event within 5s", i)
		}
	}
}

// TestPoolerCache_NoSpuriousNotificationsOnReconnect verifies that when the watch
// reconnects and delivers the same pooler data, no change notification is emitted.
func TestPoolerCache_NoSpuriousNotificationsOnReconnect(t *testing.T) {
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	require.NoError(t, ts.CreateMultiPooler(ctx, newTestPooler("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY)))

	cache := topoclient.NewPoolerCache(ctx, ts, discardCacheLogger())
	cache.Start()
	defer cache.Stop()

	require.True(t, waitForCacheCount(t, cache, 1))

	// Count notifications received after initial discovery.
	var mu sync.Mutex
	notifCount := 0
	unsub := cache.Subscribe(func(_ *clustermetadatapb.MultiPooler, _ bool) {
		mu.Lock()
		notifCount++
		mu.Unlock()
	})
	defer unsub()

	// Wait for replay to be delivered.
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	countAfterSubscribe := notifCount // should be 1 (replay of p1)
	mu.Unlock()

	// Force a reconnect by triggering a watch error.
	factory.SetError(errors.New("simulated connection failure"))
	time.Sleep(20 * time.Millisecond)
	factory.SetError(nil)

	// Give time for reconnect and onInitialCell to run.
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	finalCount := notifCount
	mu.Unlock()

	// Only the replay notification (1) should have been delivered; no spurious
	// re-notification after reconnect since the pooler data hasn't changed.
	assert.Equal(t, countAfterSubscribe, finalCount, "no spurious notifications on reconnect")
}
