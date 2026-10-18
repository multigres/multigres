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

package poolerwatch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// fakeRecord is a pooler record that can be followed, as the multipooler's is.
type fakeRecord struct {
	record   *clustermetadatapb.Multipooler
	watchers map[int]func(*clustermetadatapb.Multipooler)
	nextID   int
	stops    int
}

func (f *fakeRecord) watch(fn func(*clustermetadatapb.Multipooler)) func() {
	if f.watchers == nil {
		f.watchers = make(map[int]func(*clustermetadatapb.Multipooler))
	}
	id := f.nextID
	f.nextID++
	f.watchers[id] = fn
	fn(f.record)
	return func() {
		f.stops++
		delete(f.watchers, id)
	}
}

func (f *fakeRecord) set(record *clustermetadatapb.Multipooler) {
	f.record = record
	for _, fn := range f.watchers {
		fn(record)
	}
}

func newFeedCache(t *testing.T, record *fakeRecord) (*PoolerCache[*testRider], *hookRecorder) {
	t.Helper()
	rec := &hookRecorder{}
	cache := New(t.Context(), Config[*testRider]{
		Feed:   RecordFeed(record.watch),
		Logger: silentLogger(),
	})
	cache.Start(Hooks[*testRider]{
		OnLive: func(p *clustermetadatapb.Multipooler, _ *testRider) *testRider {
			rec.record("live:" + p.Id.Name)
			return newRider()
		},
		OnUpdate: func(_, curr *clustermetadatapb.Multipooler, _ *testRider) {
			rec.record("update:" + curr.Id.Name)
		},
		OnGone: func(p *clustermetadatapb.Multipooler, _ *testRider, r GoneReason) {
			rec.record("gone-" + r.String() + ":" + p.Id.Name)
		},
	})
	return cache, rec
}

func TestFeed_DeliversRecordWithoutTopology(t *testing.T) {
	active := clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE
	record := &fakeRecord{record: pool("zone1", "p1", "db", "tg", "0", active)}
	cache, rec := newFeedCache(t, record)

	assert.Equal(t, []string{"live:p1"}, rec.snapshot(), "the current record is known at Start")
	assert.Equal(t, 1, cache.Len())

	changed := pool("zone1", "p1", "db", "tg", "0", active)
	changed.Hostname = "moved.local"
	record.set(changed)
	assert.Equal(t, []string{"live:p1", "update:p1"}, rec.snapshot(), "a change is an upsert")

	cache.Shutdown()
	assert.Equal(t, 1, record.stops, "Shutdown stops the feed")
	assert.Empty(t, record.watchers)
}

func TestFeed_StartAfterShutdownStopsFeed(t *testing.T) {
	record := &fakeRecord{record: pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)}
	cache := New(t.Context(), Config[*testRider]{Feed: RecordFeed(record.watch), Logger: silentLogger()})
	cache.Shutdown()

	cache.Start(Hooks[*testRider]{})

	require.Equal(t, 1, record.stops, "a feed started after Shutdown is stopped at once")
	assert.Empty(t, record.watchers)
	assert.Equal(t, 0, cache.Len())
}
