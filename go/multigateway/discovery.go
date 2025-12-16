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

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/tools/retry"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"google.golang.org/protobuf/proto"
)

// PoolerDiscovery is a discovery service that watches for multipoolers
// in the topology using topology watches and maintains a list of available poolers.
type PoolerDiscovery struct {
	// Configuration
	topoStore topoclient.Store
	cell      string
	logger    *slog.Logger

	// Control
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// State
	mu          sync.Mutex
	poolers     map[string]*topoclient.MultiPoolerInfo // pooler ID -> pooler info
	lastRefresh time.Time

	// Cross-zone PRIMARY tracking (updated from watching all cells)
	primaryPooler *topoclient.MultiPoolerInfo
}

// NewPoolerDiscovery creates a new pooler discovery service.
func NewPoolerDiscovery(ctx context.Context, topoStore topoclient.Store, cell string, logger *slog.Logger) *PoolerDiscovery {
	discoveryCtx, cancel := context.WithCancel(ctx)

	return &PoolerDiscovery{
		topoStore:  topoStore,
		cell:       cell,
		logger:     logger,
		ctx:        discoveryCtx,
		cancelFunc: cancel,
		poolers:    make(map[string]*topoclient.MultiPoolerInfo),
	}
}

// Start begins the discovery process using topology watch.
// It watches the local cell for all poolers, and remote cells for PRIMARY poolers only.
func (pd *PoolerDiscovery) Start() {
	// Discover all cells
	allCells, err := pd.topoStore.GetCellNames(pd.ctx)
	if err != nil {
		pd.logger.Warn("Failed to get all cells, watching local cell only", "error", err)
		allCells = []string{pd.cell}
	}

	pd.logger.Info("Starting pooler discovery with multi-cell support",
		"local_cell", pd.cell,
		"all_cells", allCells)

	// Start a watch goroutine for each cell
	for _, cell := range allCells {
		pd.wg.Go(func() {
			pd.watchCell(cell)
		})
	}
}

// watchCell watches a single cell for pooler changes.
// For the local cell, it tracks all poolers. For remote cells, only PRIMARY.
func (pd *PoolerDiscovery) watchCell(cell string) {
	isLocalCell := cell == pd.cell

	pd.logger.Info("Starting pooler watch for cell", "cell", cell, "is_local", isLocalCell)

	r := retry.New(100*time.Millisecond, 30*time.Second)
	for attempt, err := range r.Attempts(pd.ctx) {
		if err != nil {
			// Context cancelled
			pd.logger.Info("Pooler discovery shutting down for cell", "cell", cell)
			return
		}

		if attempt > 0 {
			pd.logger.Info("Restarting pooler watch for cell", "cell", cell)
		}

		// Establish watch and process changes
		func() {
			// Get connection for the cell
			conn, err := pd.topoStore.ConnForCell(pd.ctx, cell)
			if err != nil {
				pd.logger.Error("Failed to get connection for cell", "cell", cell, "error", err)
				return
			}

			// Start watching the poolers directory
			poolersPath := "poolers" // This matches the PoolersPath constant from store.go
			initial, changes, err := conn.WatchRecursive(pd.ctx, poolersPath)
			if err != nil {
				pd.logger.Error("Failed to start recursive watch on poolers", "cell", cell, "path", poolersPath, "error", err)
				return
			}

			// Process initial values
			pd.processInitialPoolers(cell, initial)

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
						pd.logger.Info("Watch channel closed, will reconnect", "cell", cell)
						return
					}

					if watchData.Err != nil {
						pd.logger.Error("Watch error received", "cell", cell, "error", watchData.Err)
						// Continue watching despite the error
						continue
					}

					pd.processPoolerChange(cell, watchData)
				}
			}
		}()
	}
}

// Stop stops the discovery service.
func (pd *PoolerDiscovery) Stop() {
	pd.cancelFunc()
	pd.wg.Wait()
}

// processInitialPoolers processes the initial set of poolers from the watch.
// For local cell: stores all poolers. For remote cells: only tracks PRIMARY.
func (pd *PoolerDiscovery) processInitialPoolers(cell string, initial []*topoclient.WatchDataRecursive) {
	isLocalCell := cell == pd.cell
	var primaryCandidates []*topoclient.MultiPoolerInfo

	// Process pooler data under lock
	func() {
		pd.mu.Lock()
		defer pd.mu.Unlock()

		// Only clear local poolers map for local cell
		if isLocalCell {
			pd.poolers = make(map[string]*topoclient.MultiPoolerInfo)
		}

		// Process initial pooler data
		for _, watchData := range initial {
			if watchData.Err != nil {
				pd.logger.Warn("Error in initial watch data", "cell", cell, "path", watchData.Path, "error", watchData.Err)
				continue
			}

			// Parse the pooler from watch data
			pooler, err := pd.parsePoolerFromWatchData(watchData)
			if err != nil {
				pd.logger.Warn("Failed to parse pooler from initial data", "cell", cell, "path", watchData.Path, "error", err)
				continue
			}

			if pooler != nil {
				poolerID := topoclient.MultiPoolerIDString(pooler.Id)

				// Local cell: store all poolers
				if isLocalCell {
					pd.poolers[poolerID] = pooler
					pd.logger.Info("Initial pooler discovered",
						"cell", cell,
						"id", poolerID,
						"hostname", pooler.Hostname,
						"addr", pooler.Addr(),
						"database", pooler.Database,
						"shard", pooler.Shard,
						"type", pooler.Type.String())
				}

				// Collect PRIMARY candidates from any cell
				if pooler.Type == clustermetadatapb.PoolerType_PRIMARY {
					primaryCandidates = append(primaryCandidates, pooler)
				}
			}
		}

		pd.lastRefresh = time.Now()
		if isLocalCell {
			pd.logger.Info("Initial pooler discovery completed",
				"cell", cell,
				"pooler_count", len(pd.poolers))
		}
	}()

	// Evaluate PRIMARY candidates after releasing lock.
	// Called serially so primaryPooler is set before this function returns.
	for _, candidate := range primaryCandidates {
		pd.considerPrimaryCandidate(cell, candidate)
	}
}

// processPoolerChange processes a single pooler change from the watch.
// For local cell: stores all poolers. For remote cells: only tracks PRIMARY.
func (pd *PoolerDiscovery) processPoolerChange(cell string, watchData *topoclient.WatchDataRecursive) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	isLocalCell := cell == pd.cell

	// Parse the pooler from watch data
	pooler, err := pd.parsePoolerFromWatchData(watchData)
	if err != nil {
		pd.logger.Warn("Failed to parse pooler from change data", "cell", cell, "path", watchData.Path, "error", err)
		return
	}

	if pooler == nil {
		// This might be a deletion or non-pooler file
		pd.logger.Debug("Skipping non-pooler file or deletion", "cell", cell, "path", watchData.Path)
		return
	}

	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	// Local cell: store all poolers
	if isLocalCell {
		_, existed := pd.poolers[poolerID]
		pd.poolers[poolerID] = pooler
		pd.lastRefresh = time.Now()

		if !existed {
			pd.logger.Info("New pooler discovered",
				"cell", cell,
				"id", poolerID,
				"hostname", pooler.Hostname,
				"addr", pooler.Addr(),
				"tableGroup", pooler.TableGroup,
				"database", pooler.Database,
				"shard", pooler.Shard,
				"type", pooler.Type.String())
		} else {
			pd.logger.Info("Pooler updated",
				"cell", cell,
				"id", poolerID,
				"hostname", pooler.Hostname,
				"addr", pooler.Addr(),
				"tableGroup", pooler.TableGroup,
				"database", pooler.Database,
				"shard", pooler.Shard,
				"type", pooler.Type.String())
		}
	}

	// If this pooler claims to be PRIMARY, consider it as a candidate
	// Run in goroutine so verification doesn't block watch processing
	if pooler.Type == clustermetadatapb.PoolerType_PRIMARY {
		go pd.considerPrimaryCandidate(cell, pooler)
	}
}

// considerPrimaryCandidate evaluates a pooler that claims to be PRIMARY.
// Currently trusts etcd - if etcd says it's PRIMARY, accept it.
func (pd *PoolerDiscovery) considerPrimaryCandidate(cell string, candidate *topoclient.MultiPoolerInfo) {
	// TODO: Add verification logic.
	// 1. Verify pooler is reachable (gRPC dial with timeout)
	// 2. Call ConsensusStatus to confirm it agrees it's PRIMARY
	// 3. Check term number is >= current primary's term
	// 4. Only then update primaryPooler

	// Trust etcd - if etcd says it's PRIMARY, accept it
	pd.mu.Lock()
	pd.primaryPooler = candidate
	pd.lastRefresh = time.Now()
	pd.mu.Unlock()

	poolerID := topoclient.MultiPoolerIDString(candidate.Id)
	pd.logger.Info("PRIMARY pooler updated",
		"cell", cell,
		"id", poolerID,
		"hostname", candidate.Hostname,
		"addr", candidate.Addr())
}

// GetPoolers returns a list of all discovered poolers.
func (pd *PoolerDiscovery) GetPoolers() []*clustermetadatapb.MultiPooler {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	poolers := make([]*clustermetadatapb.MultiPooler, 0, len(pd.poolers))
	for _, pooler := range pd.poolers {
		poolers = append(poolers, proto.Clone(pooler.MultiPooler).(*clustermetadatapb.MultiPooler))
	}
	return poolers
}

// GetPooler returns a pooler matching the target specification.
// Target specifies the tablegroup, shard, and pooler type to route to.
// Returns nil if no matching pooler is found.
//
// Routing strategy:
// - PRIMARY: Returns the cross-zone primaryPooler (can be in any cell)
// - REPLICA: Returns a pooler from the local cell only
//
// Filtering logic:
// - TableGroup: Required, must match exactly
// - PoolerType: If not specified (UNKNOWN), defaults to PRIMARY
// - Shard: If empty, matches any shard; otherwise must match exactly
func (pd *PoolerDiscovery) GetPooler(target *query.Target) *clustermetadatapb.MultiPooler {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Default to PRIMARY if not specified
	targetType := target.PoolerType
	if targetType == clustermetadatapb.PoolerType_UNKNOWN {
		targetType = clustermetadatapb.PoolerType_PRIMARY
	}

	// Debug: Log all discovered poolers
	pd.logger.Debug("GetPooler called - listing all discovered poolers",
		"target_tablegroup", target.TableGroup,
		"target_shard", target.Shard,
		"target_pooler_type", targetType.String(),
		"total_poolers", len(pd.poolers))
	for i, pooler := range pd.poolers {
		pd.logger.Debug("discovered pooler",
			"index", i,
			"pooler_id", topoclient.MultiPoolerIDString(pooler.Id),
			"tablegroup", pooler.TableGroup,
			"shard", pooler.Shard,
			"type", pooler.Type.String())
	}

	// PRIMARY: Use cross-zone primaryPooler
	if targetType == clustermetadatapb.PoolerType_PRIMARY {
		if pd.primaryPooler != nil {
			pooler := pd.primaryPooler
			// Verify it matches the target
			if pooler.TableGroup == target.TableGroup &&
				(target.Shard == "" || pooler.Shard == target.Shard) {
				pd.logger.Debug("selected PRIMARY pooler (cross-zone)",
					"pooler_id", topoclient.MultiPoolerIDString(pooler.Id),
					"tablegroup", pooler.TableGroup,
					"shard", pooler.Shard)
				return proto.Clone(pooler.MultiPooler).(*clustermetadatapb.MultiPooler)
			}
		}
		pd.logger.Warn("no PRIMARY pooler found",
			"tablegroup", target.TableGroup,
			"shard", target.Shard)
		return nil
	}

	// REPLICA/other: Search local cell poolers only
	for _, pooler := range pd.poolers {
		// TableGroup must match
		if pooler.TableGroup != target.TableGroup {
			continue
		}

		// PoolerType must match
		if pooler.Type != targetType {
			continue
		}

		// Shard must match if specified
		if target.Shard != "" && pooler.Shard != target.Shard {
			continue
		}

		// Found a match!
		pd.logger.Debug("selected pooler for target",
			"pooler_id", topoclient.MultiPoolerIDString(pooler.Id),
			"pooler_type", pooler.Type.String(),
			"tablegroup", pooler.TableGroup,
			"shard", pooler.Shard)
		return proto.Clone(pooler.MultiPooler).(*clustermetadatapb.MultiPooler)
	}

	pd.logger.Warn("no matching pooler found",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", targetType.String())
	return nil
}

// LastRefresh returns the timestamp of the last successful refresh.
func (pd *PoolerDiscovery) LastRefresh() time.Time {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	return pd.lastRefresh
}

// PoolerCount returns the current number of discovered poolers.
func (pd *PoolerDiscovery) PoolerCount() int {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	return len(pd.poolers)
}

// parsePoolerFromWatchData parses a MultiPooler from watch data
func (pd *PoolerDiscovery) parsePoolerFromWatchData(watchData *topoclient.WatchDataRecursive) (*topoclient.MultiPoolerInfo, error) {
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

	return &topoclient.MultiPoolerInfo{
		MultiPooler: pooler,
	}, nil
}
