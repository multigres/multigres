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
	"time"

	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/multigres/multigres/go/tools/retry"
)

// Topo sync state values for topoSyncState atomic.Int32.
const (
	// topoSyncIdle means topo is in sync with local state.
	topoSyncIdle int32 = 0
	// topoSyncNeeded means a sync failed and retry is needed, but no goroutine is running.
	topoSyncNeeded int32 = 1
	// topoSyncSyncing means a goroutine is actively retrying the topo write.
	topoSyncSyncing int32 = 2
)

// scheduleTopoSync signals that local state has diverged from topo and a background
// retry is needed. It spawns a goroutine if none is already running.
//
// State transitions:
//   - idle    → syncing: spawn goroutine
//   - syncing → needed:  tell the running goroutine there's new work
func (pm *MultiPoolerManager) scheduleTopoSync() {
	for {
		state := pm.topoSyncState.Load()
		switch state {
		case topoSyncIdle:
			if pm.topoSyncState.CompareAndSwap(topoSyncIdle, topoSyncSyncing) {
				go pm.runTopoSyncRetry()
				return
			}
		case topoSyncSyncing:
			if pm.topoSyncState.CompareAndSwap(topoSyncSyncing, topoSyncNeeded) {
				return
			}
		default:
			return
		}
	}
}

// runTopoSyncRetry is the background goroutine that retries writing local state to topo.
// It always reads the current pm.multipooler under pm.mu at retry time so the latest
// state is written.
func (pm *MultiPoolerManager) runTopoSyncRetry() {
	r := retry.New(1*time.Second, 30*time.Second, retry.WithInitialDelay())
	for _, err := range r.Attempts(pm.ctx) {
		if err != nil {
			// Context cancelled — manager is shutting down.
			pm.topoSyncState.Store(topoSyncIdle)
			return
		}

		// If a direct topo write succeeded and cleared the flag, exit.
		if pm.topoSyncState.Load() == topoSyncIdle {
			return
		}

		// Pick up the "needed" flag — we're about to sync.
		pm.topoSyncState.CompareAndSwap(topoSyncNeeded, topoSyncSyncing)

		// Clone the current multipooler state under lock.
		pm.mu.Lock()
		clone := proto.Clone(pm.multipooler).(*clustermetadatapb.MultiPooler)
		pm.mu.Unlock()

		// Attempt to write to topo.
		syncCtx, cancel := context.WithTimeout(pm.ctx, 5*time.Second)
		syncErr := pm.topoClient.RegisterMultiPooler(syncCtx, clone, true)
		cancel()

		if syncErr != nil {
			pm.logger.Warn("Background topo sync failed, will retry", "error", syncErr)
			continue
		}

		// Success — check if new work arrived while we were syncing.
		if pm.topoSyncState.CompareAndSwap(topoSyncSyncing, topoSyncIdle) {
			return // done, no new work
		}
		// State is "needed" — new work arrived. Reset backoff and loop again.
		r.Reset()
	}
}
