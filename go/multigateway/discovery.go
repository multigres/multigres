// Copyright 2025 Supabase, Inc.
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

package multigateway

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/tools/retry"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"google.golang.org/protobuf/proto"
)

// PoolerDiscovery is a discovery service that watches for multipoolers
// in the topology using topology watches and maintains a list of available poolers.
type PoolerDiscovery struct {
	// Configuration
	topoStore topo.Store
	cell      string
	logger    *slog.Logger

	// Control
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// State
	mu          sync.RWMutex
	poolers     map[string]*topo.MultiPoolerInfo // pooler ID -> pooler info
	lastRefresh time.Time
}

// NewPoolerDiscovery creates a new pooler discovery service.
func NewPoolerDiscovery(ctx context.Context, topoStore topo.Store, cell string, logger *slog.Logger) *PoolerDiscovery {
	discoveryCtx, cancel := context.WithCancel(ctx)

	return &PoolerDiscovery{
		topoStore:  topoStore,
		cell:       cell,
		logger:     logger,
		ctx:        discoveryCtx,
		cancelFunc: cancel,
		poolers:    make(map[string]*topo.MultiPoolerInfo),
	}
}

// Start begins the discovery process using topology watch.
func (pd *PoolerDiscovery) Start() {
	pd.wg.Go(func() {
		pd.logger.Info("Starting pooler discovery with topology watch", "cell", pd.cell)

		r := retry.New(100*time.Millisecond, 30*time.Second)
		for attempt, err := range r.Attempts(pd.ctx) {
			if err != nil {
				// Context cancelled
				pd.logger.Info("Pooler discovery shutting down")
				return
			}

			if attempt > 0 {
				pd.logger.Info("Restarting pooler discovery with topology watch", "cell", pd.cell)
			}

			// Establish watch and process changes
			func() {
				// Get connection for the cell
				conn, err := pd.topoStore.ConnForCell(pd.ctx, pd.cell)
				if err != nil {
					pd.logger.Error("Failed to get connection for cell", "cell", pd.cell, "error", err)
					return
				}

				// Start watching the poolers directory
				poolersPath := "poolers" // This matches the PoolersPath constant from store.go
				initial, changes, err := conn.WatchRecursive(pd.ctx, poolersPath)
				if err != nil {
					pd.logger.Error("Failed to start recursive watch on poolers", "path", poolersPath, "error", err)
					return
				}

				// Process initial values
				pd.processInitialPoolers(initial)

				// Reset backoff after watch has been stable for 30s
				resetTimer := time.AfterFunc(30*time.Second, func() {
					r.Reset()
				})
				defer resetTimer.Stop()

				// Process changes as they come in
				for {
					select {
					case <-pd.ctx.Done():
						return
					case watchData, ok := <-changes:
						if !ok {
							pd.logger.Info("Watch channel closed, will reconnect")
							return
						}

						if watchData.Err != nil {
							pd.logger.Error("Watch error received", "error", watchData.Err)
							// Continue watching despite the error
							continue
						}

						pd.processPoolerChange(watchData)
					}
				}
			}()
		}
	})
}

// Stop stops the discovery service.
func (pd *PoolerDiscovery) Stop() {
	pd.cancelFunc()
	pd.wg.Wait()
}

// processInitialPoolers processes the initial set of poolers from the watch
func (pd *PoolerDiscovery) processInitialPoolers(initial []*topo.WatchDataRecursive) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Clear existing poolers
	pd.poolers = make(map[string]*topo.MultiPoolerInfo)

	// Process initial pooler data
	for _, watchData := range initial {
		if watchData.Err != nil {
			pd.logger.Warn("Error in initial watch data", "path", watchData.Path, "error", watchData.Err)
			continue
		}

		// Parse the pooler from watch data
		pooler, err := pd.parsePoolerFromWatchData(watchData)
		if err != nil {
			pd.logger.Warn("Failed to parse pooler from initial data", "path", watchData.Path, "error", err)
			continue
		}

		if pooler != nil {
			poolerID := topo.MultiPoolerIDString(pooler.Id)
			pd.poolers[poolerID] = pooler
			pd.logger.Info("Initial pooler discovered",
				"id", poolerID,
				"hostname", pooler.Hostname,
				"addr", pooler.Addr(),
				"database", pooler.Database,
				"shard", pooler.Shard,
				"type", pooler.Type.String())
		}
	}

	pd.lastRefresh = time.Now()
	pd.logger.Info("Initial pooler discovery completed",
		"cell", pd.cell,
		"pooler_count", len(pd.poolers))
}

// processPoolerChange processes a single pooler change from the watch
func (pd *PoolerDiscovery) processPoolerChange(watchData *topo.WatchDataRecursive) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Parse the pooler from watch data
	pooler, err := pd.parsePoolerFromWatchData(watchData)
	if err != nil {
		pd.logger.Warn("Failed to parse pooler from change data", "path", watchData.Path, "error", err)
		return
	}

	if pooler == nil {
		// This might be a deletion or non-pooler file
		pd.logger.Debug("Skipping non-pooler file or deletion", "path", watchData.Path)
		return
	}

	// Add or update the pooler
	poolerID := topo.MultiPoolerIDString(pooler.Id)

	// Check if this is a new pooler
	_, existed := pd.poolers[poolerID]
	pd.poolers[poolerID] = pooler
	pd.lastRefresh = time.Now()

	if !existed {
		pd.logger.Info("New pooler discovered",
			"id", poolerID,
			"hostname", pooler.Hostname,
			"addr", pooler.Addr(),
			"database", pooler.Database,
			"shard", pooler.Shard,
			"type", pooler.Type.String())
	} else {
		pd.logger.Info("Pooler updated",
			"id", poolerID,
			"hostname", pooler.Hostname,
			"addr", pooler.Addr(),
			"database", pooler.Database,
			"shard", pooler.Shard,
			"type", pooler.Type.String())
	}
}

// GetPoolersName returns a list of all discovered pooler names.
func (pd *PoolerDiscovery) GetPoolers() []*clustermetadatapb.MultiPooler {
	pd.mu.RLock()
	defer pd.mu.RUnlock()

	poolers := make([]*clustermetadatapb.MultiPooler, 0, len(pd.poolers))
	for _, pooler := range pd.poolers {
		poolers = append(poolers, proto.Clone(pooler).(*clustermetadatapb.MultiPooler))
	}
	return poolers
}

// LastRefresh returns the timestamp of the last successful refresh.
func (pd *PoolerDiscovery) LastRefresh() time.Time {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	return pd.lastRefresh
}

// PoolerCount returns the current number of discovered poolers.
func (pd *PoolerDiscovery) PoolerCount() int {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	return len(pd.poolers)
}

// parsePoolerFromWatchData parses a MultiPooler from watch data
func (pd *PoolerDiscovery) parsePoolerFromWatchData(watchData *topo.WatchDataRecursive) (*topo.MultiPoolerInfo, error) {
	// Only process files that end with "Pooler" (the actual pooler data files)
	if !strings.HasSuffix(watchData.Path, "/Pooler") {
		return nil, nil // Not a pooler file, skip
	}

	// If Contents is nil, this might be a deletion
	if watchData.Contents == nil {
		return nil, nil
	}

	// Parse the protobuf data
	pooler := &clustermetadatapb.MultiPooler{}
	if err := proto.Unmarshal(watchData.Contents, pooler); err != nil {
		return nil, err
	}

	return &topo.MultiPoolerInfo{
		MultiPooler: pooler,
	}, nil
}
