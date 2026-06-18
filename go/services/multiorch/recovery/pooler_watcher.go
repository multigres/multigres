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

package recovery

import (
	"context"
	"log/slog"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// PoolerWatcher keeps the multiorch pooler store in sync with the topology by
// subscribing to a shared topoclient.PoolerCache. The cache handles cell
// discovery, per-cell watches, reconnect backoff, and proto.Equal dedup; this
// type contributes the multiorch-specific behavior:
//
//   - WatchTarget filtering (drop poolers outside the configured shards).
//   - Lifecycle-transition awareness (SHUTDOWN, restart, NoNode) with distinct
//     callbacks for caller-owned per-pooler resources (e.g. health streams).
//   - Health-check field preservation via PoolerStore.DoUpdate when refreshing
//     the cached MultiPooler proto.
type PoolerWatcher struct {
	cache           *topoclient.PoolerCache
	targets         func() []config.WatchTarget // live accessor, same as Engine.shardWatchTargets
	store           *store.PoolerStore
	onNewPooler     func(id *clustermetadatapb.ID) // called when a new pooler is first discovered (or restarted from SHUTDOWN)
	onPoolerStopped func(id *clustermetadatapb.ID) // called on lifecycle transition into LIFECYCLE_SHUTDOWN
	onPoolerDeleted func(id *clustermetadatapb.ID) // called when a tracked pooler's topology entry is removed (NoNode); reserved for future use
	logger          *slog.Logger

	unsub func()
}

// NewPoolerWatcher creates a new PoolerWatcher.
// targets is a function that returns the current WatchTargets (consulted on every event).
// onNewPooler is called when a new pooler is discovered, or when one restarts
// from LIFECYCLE_SHUTDOWN; the pooler is already present in the store when the
// callback fires.
// onPoolerStopped is called when a tracked pooler's lifecycle transitions to
// LIFECYCLE_SHUTDOWN; the store entry is retained so failover analyzers can
// still observe it.
// onPoolerDeleted is called when a tracked pooler's topology entry is removed
// (NoNode). Reserved for future use — pass nil to leave NoNode events as a
// log-only no-op that does not evict the cache or stop per-pooler resources.
func NewPoolerWatcher(
	ctx context.Context,
	topoStore topoclient.Store,
	targets func() []config.WatchTarget,
	poolerStore *store.PoolerStore,
	onNewPooler func(id *clustermetadatapb.ID),
	onPoolerStopped func(id *clustermetadatapb.ID),
	onPoolerDeleted func(id *clustermetadatapb.ID),
	logger *slog.Logger,
) *PoolerWatcher {
	return &PoolerWatcher{
		cache:           topoclient.NewPoolerCache(ctx, topoStore, logger),
		targets:         targets,
		store:           poolerStore,
		onNewPooler:     onNewPooler,
		onPoolerStopped: onPoolerStopped,
		onPoolerDeleted: onPoolerDeleted,
		logger:          logger,
	}
}

// Start subscribes to the cache and begins watching for topology changes.
// Existing poolers in the cache are replayed synchronously to the subscriber
// before Start returns.
func (pw *PoolerWatcher) Start() {
	pw.logger.Info("starting pooler watcher")
	pw.unsub = pw.cache.Subscribe(pw.onCacheChange)
	pw.cache.Start()
}

// Stop unsubscribes and waits for the underlying cache to finish.
func (pw *PoolerWatcher) Stop() {
	if pw.unsub != nil {
		pw.unsub()
	}
	pw.cache.Stop()
	pw.logger.Info("pooler watcher stopped")
}

// Sync blocks until all events enqueued by the underlying PoolerCache at the
// time of the call have been dispatched to this watcher. See PoolerCache.Sync
// for caveats around watch→cache enqueue timing.
func (pw *PoolerWatcher) Sync(ctx context.Context) error {
	return pw.cache.Sync(ctx)
}

// onCacheChange handles a (prev, curr) transition from the topology cache.
func (pw *PoolerWatcher) onCacheChange(prev, curr *clustermetadatapb.MultiPooler) {
	// Deletion (NoNode in topology). The happy-path graceful-shutdown flow
	// uses LIFECYCLE_SHUTDOWN, not deletion — so a NoNode here is unexpected
	// (operator typo, automation bug, manual etcd delete). Leave the store
	// entry and any per-pooler resources intact, but fire the reserved hook
	// so callers can choose to react if they want.
	if curr == nil {
		if !pw.matchesAnyTarget(prev) {
			return
		}
		poolerID := topoclient.ComponentIDString(prev.Id)
		if _, ok := pw.store.Get(poolerID); ok {
			pw.logger.Warn("pooler topology entry removed; cache and stream left intact", "pooler_id", poolerID)
			if pw.onPoolerDeleted != nil {
				pw.onPoolerDeleted(prev.Id)
			}
		}
		return
	}

	if !pw.matchesAnyTarget(curr) {
		return
	}

	poolerID := topoclient.ComponentIDString(curr.Id)
	newLifecycle := curr.GetLifecycleStatus().GetStatus()

	// Atomic read-modify-write of the store: capture whether an entry already
	// existed and what its previous lifecycle was, then refresh MultiPooler in
	// place so health-check fields written concurrently by the stream worker
	// are preserved.
	var (
		existing      bool
		prevLifecycle clustermetadatapb.PoolerLifecycleStatus
	)
	pw.store.DoUpdate(poolerID, func(state *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
		existing = true
		prevLifecycle = state.MultiPooler.GetLifecycleStatus().GetStatus()
		state.MultiPooler = curr
		return state
	})

	switch {
	case !existing && newLifecycle == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN:
		// Cold-start discovery of an already-SHUTDOWN pooler. Cache so a later
		// restart within the 4 h bookkeeping window will be detected, but don't
		// open a health stream now — nothing live to monitor.
		pw.store.Set(poolerID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: curr,
			IsUpToDate:  false,
		})
		pw.logger.Debug("cached already-SHUTDOWN pooler without opening stream", "pooler_id", poolerID)

	case existing && newLifecycle == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN && prevLifecycle != clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN:
		// Transition into SHUTDOWN: tear down per-pooler resources, retain
		// the store entry so analyzers (e.g. LeaderIsDeadAnalyzer) can still
		// observe it. Bookkeeping handles eventual eviction.
		if pw.onPoolerStopped != nil {
			pw.onPoolerStopped(curr.Id)
		}
		pw.logger.Info("pooler entered SHUTDOWN lifecycle", "pooler_id", poolerID)

	case existing && prevLifecycle == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN && newLifecycle != clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN:
		// Restart from SHUTDOWN: re-fire onNewPooler so per-pooler resources
		// get re-established.
		pw.onNewPooler(curr.Id)
		pw.logger.Info("pooler restarted from SHUTDOWN lifecycle",
			"pooler_id", poolerID,
			"new_lifecycle", newLifecycle.String(),
		)

	case existing:
		pw.logger.Debug("pooler metadata updated from topology", "pooler_id", poolerID)

	default:
		// New pooler — add to store and open a stream.
		pw.store.Set(poolerID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: curr,
			IsUpToDate:  false,
		})
		pw.onNewPooler(curr.Id)
		pw.logger.Info("new pooler discovered via watcher",
			"pooler_id", poolerID,
			"database", curr.GetShardKey().GetDatabase(),
			"tablegroup", curr.GetShardKey().GetTableGroup(),
			"shard", curr.GetShardKey().GetShard(),
			"leader", curr.GetSelfLeadership().GetLeaderId() != nil,
		)
	}
}

// matchesAnyTarget returns true if the pooler matches at least one of the
// configured WatchTargets.
func (pw *PoolerWatcher) matchesAnyTarget(pooler *clustermetadatapb.MultiPooler) bool {
	for _, target := range pw.targets() {
		if target.MatchesShard(pooler.GetShardKey().GetDatabase(), pooler.GetShardKey().GetTableGroup(), pooler.GetShardKey().GetShard()) {
			return true
		}
	}
	return false
}
