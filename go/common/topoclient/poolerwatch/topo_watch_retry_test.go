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

package poolerwatch

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestExtractPoolerIDFromPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected topoclient.ComponentID
	}{
		{"relative path", "poolers/multipooler-cell1-pooler1/Pooler", "multipooler-cell1-pooler1"},
		{"simple id", "poolers/some-complex-id/Pooler", "some-complex-id"},
		{"short id", "poolers/id/Pooler", "id"},
		{"absolute etcd path", "/multigres/zone1/poolers/multipooler-zone1-p1/Pooler", "multipooler-zone1-p1"},
		{"missing id segment", "poolers/Pooler", ""},
		{"wrong directory", "other/path/Pooler", ""},
		{"wrong filename", "poolers/id/other", ""},
		{"empty", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, extractPoolerIDFromPath(tc.path))
		})
	}
}

// watchEventRecorder collects callbacks fired by watchAllPoolersWithRetry so
// tests can assert on the observed event stream. All access is serialized
// through mu so tests can read snapshots safely from another goroutine.
type watchEventRecorder struct {
	mu          sync.Mutex
	snapshots   map[string][][]topoclient.ComponentID // cell -> sequence of snapshot IDs
	upserts     []topoclient.ComponentID
	deletes     []topoclient.ComponentID
	cellRemoved []string
}

func newWatchEventRecorder() *watchEventRecorder {
	return &watchEventRecorder{snapshots: make(map[string][][]topoclient.ComponentID)}
}

func (r *watchEventRecorder) onInitial(cell string, poolers []*clustermetadatapb.MultiPooler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]topoclient.ComponentID, 0, len(poolers))
	for _, p := range poolers {
		ids = append(ids, topoclient.ComponentIDString(p.Id))
	}
	slices.Sort(ids)
	r.snapshots[cell] = append(r.snapshots[cell], ids)
}

func (r *watchEventRecorder) onUpserted(p *clustermetadatapb.MultiPooler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upserts = append(r.upserts, topoclient.ComponentIDString(p.Id))
}

func (r *watchEventRecorder) onDeleted(id topoclient.ComponentID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deletes = append(r.deletes, id)
}

func (r *watchEventRecorder) onCellRemoved(cell string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cellRemoved = append(r.cellRemoved, cell)
}

func (r *watchEventRecorder) snapshotsForCell(cell string) [][]topoclient.ComponentID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]topoclient.ComponentID, len(r.snapshots[cell]))
	for i, s := range r.snapshots[cell] {
		dup := make([]topoclient.ComponentID, len(s))
		copy(dup, s)
		out[i] = dup
	}
	return out
}

func (r *watchEventRecorder) upsertedIDs() []topoclient.ComponentID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]topoclient.ComponentID, len(r.upserts))
	copy(out, r.upserts)
	return out
}

func (r *watchEventRecorder) deletedIDs() []topoclient.ComponentID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]topoclient.ComponentID, len(r.deletes))
	copy(out, r.deletes)
	return out
}

func (r *watchEventRecorder) cellsRemoved() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.cellRemoved))
	copy(out, r.cellRemoved)
	return out
}

func newPoolerProto(cell, name, tableGroup, shard string) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      name,
		},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "db",
			TableGroup: tableGroup,
			Shard:      shard,
		},
		LifecycleStatus: &clustermetadatapb.PoolerLifecycle{
			Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		},
		Hostname: name + ".local",
	}
}

// TestWatchAllPoolers_CellAddRemoveFlow verifies, end-to-end against
// memorytopo, that:
//   - the initial cell list produces a per-cell OnSnapshot,
//   - upserts/deletes on poolers within a cell route to OnUpsert/OnDelete,
//   - adding a new cell spins up a watcher and emits OnSnapshot,
//   - removing a cell fires OnCellRemoved.
func TestWatchAllPoolers_CellAddRemoveFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	rec := newWatchEventRecorder()
	broadcaster := newCellSyncBroadcaster()
	logger := slog.New(slog.DiscardHandler)

	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		watchAllPoolersWithRetry(ctx, ts, logger, broadcaster,
			rec.onInitial,
			rec.onUpserted,
			rec.onDeleted,
			rec.onCellRemoved,
		)
	}()

	// Initial snapshot for zone1 should arrive (empty).
	require.Eventually(t, func() bool {
		return len(rec.snapshotsForCell("zone1")) >= 1
	}, 2*time.Second, 5*time.Millisecond, "expected initial OnSnapshot for zone1")
	require.Empty(t, rec.snapshotsForCell("zone1")[0], "empty cell should yield empty snapshot")

	// Add a pooler to zone1 and observe OnUpsert.
	p1 := newPoolerProto("zone1", "p1", "tg", "0")
	require.NoError(t, ts.CreateMultiPooler(ctx, p1))
	require.Eventually(t, func() bool {
		return slices.Contains(rec.upsertedIDs(), topoclient.ComponentIDString(p1.Id))
	}, 2*time.Second, 5*time.Millisecond, "expected OnUpsert for p1")

	// Add a new cell. A per-cell watcher should start and emit OnSnapshot.
	require.NoError(t, factory.AddCell(ctx, ts, "zone2"))
	require.Eventually(t, func() bool {
		return len(rec.snapshotsForCell("zone2")) >= 1
	}, 2*time.Second, 5*time.Millisecond, "expected OnSnapshot for zone2 after AddCell")

	// Delete the pooler — OnDelete must fire.
	require.NoError(t, ts.UnregisterMultiPooler(ctx, p1.Id))
	require.Eventually(t, func() bool {
		return slices.Contains(rec.deletedIDs(), topoclient.ComponentIDString(p1.Id))
	}, 2*time.Second, 5*time.Millisecond, "expected OnDelete for p1")

	// Remove zone2 — OnCellRemoved must fire for it.
	require.NoError(t, ts.DeleteCell(ctx, "zone2", true /*force*/))
	require.Eventually(t, func() bool {
		return slices.Contains(rec.cellsRemoved(), "zone2")
	}, 2*time.Second, 5*time.Millisecond, "expected OnCellRemoved for zone2")

	cancel()
	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("watchAllPoolersWithRetry did not return after ctx cancel")
	}
}

// TestCellSyncBroadcaster_SyncAllEmpty verifies syncAll on an empty
// broadcaster returns nil immediately.
func TestCellSyncBroadcaster_SyncAllEmpty(t *testing.T) {
	b := newCellSyncBroadcaster()
	require.NoError(t, b.syncAll(t.Context()))
}

// TestCellSyncBroadcaster_SyncAllCtxCancel verifies syncAll returns
// ctx.Err when its context is cancelled while watchers are still
// holding events. Two cells are registered but never consume from their
// sync channel — the first send lands in the cell's 1-buffered channel
// but the second blocks. Cancelling unblocks it.
func TestCellSyncBroadcaster_SyncAllCtxCancel(t *testing.T) {
	b := newCellSyncBroadcaster()
	_ = b.register("zone1")
	_ = b.register("zone2")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.syncAll(ctx) }()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("syncAll did not return after ctx cancel")
	}
}

// TestCellSyncBroadcaster_SyncAllDispatchesAllEvents verifies that, in a
// full watchAllPoolersWithRetry deployment, syncAll waits for every
// per-cell watcher to drain its in-flight events. We create several
// poolers across two cells; on each Create, memorytopo posts an event
// onto the watcher's changes channel. Calling syncAll must guarantee
// that by the time it returns, OnUpsert has fired for every event
// already in flight.
func TestCellSyncBroadcaster_SyncAllDispatchesAllEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
	defer ts.Close()

	rec := newWatchEventRecorder()
	broadcaster := newCellSyncBroadcaster()
	logger := slog.New(slog.DiscardHandler)

	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		watchAllPoolersWithRetry(ctx, ts, logger, broadcaster,
			rec.onInitial,
			rec.onUpserted,
			rec.onDeleted,
			rec.onCellRemoved,
		)
	}()

	// Wait for both cells' initial snapshots to land so syncReq channels
	// are registered by the time we call syncAll.
	require.Eventually(t, func() bool {
		return len(rec.snapshotsForCell("zone1")) >= 1 && len(rec.snapshotsForCell("zone2")) >= 1
	}, 2*time.Second, 5*time.Millisecond)

	// Create poolers across both cells.
	poolers := []*clustermetadatapb.MultiPooler{
		newPoolerProto("zone1", "p1a", "tg", "0"),
		newPoolerProto("zone1", "p1b", "tg", "0"),
		newPoolerProto("zone2", "p2a", "tg", "0"),
		newPoolerProto("zone2", "p2b", "tg", "0"),
	}
	for _, p := range poolers {
		require.NoError(t, ts.CreateMultiPooler(ctx, p))
	}

	// Sync barrier: every per-cell watcher must have drained the events
	// queued before this call.
	syncCtx, syncCancel := context.WithTimeout(ctx, 3*time.Second)
	defer syncCancel()
	require.NoError(t, broadcaster.syncAll(syncCtx))

	got := rec.upsertedIDs()
	for _, p := range poolers {
		assert.Contains(t, got, topoclient.ComponentIDString(p.Id),
			"syncAll must drain in-flight events before returning")
	}

	cancel()
	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("watchAllPoolersWithRetry did not return after ctx cancel")
	}
}
