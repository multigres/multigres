// Copyright 2025 Supabase, Inc.
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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multiorch/store"
)

// runIfNotRunning executes fn in a goroutine only if inProgress flag is false.
// If the operation is already in progress, it logs a debug message and returns immediately.
// This prevents pile-up of concurrent operations that may be slow.
func runIfNotRunning(logger *slog.Logger, inProgress *atomic.Bool, taskName string, fn func()) {
	if !inProgress.CompareAndSwap(false, true) {
		logger.Debug("skipping task, previous run still in progress", "task", taskName)
		return
	}
	go func() {
		defer inProgress.Store(false)
		fn()
	}()
}

// Engine orchestrates health checking and automated recovery for Multigres poolers.
//
// The Engine provides high availability for Multigres
// by continuously monitoring pooler health and automatically
// recovering from failures.
//
// # Architecture
//
// The Engine runs three main loops operating at different intervals:
//
//	┌──────────────────────────────────────────────────────────────────┐
//	│                        Engine                            │
//	├──────────────────────────────────────────────────────────────────┤
//	│                                                                  │
//	│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
//	│  │ Healthcheck Loop│  │  Recovery Loop  │  │ Maintenance Loop│   │
//	│  │  (5s)           │  │   (1s)          │  │   (1s)          │   │
//	│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘   │
//	│           │                    │                    │            │
//	│           └────────────────────┼────────────────────┘            │
//	│                                ▼                                 │
//	│                        (TODO: State Store)                       │
//	└──────────────────────────────────────────────────────────────────┘
//
// # Loop Details
//
// Maintenance Loop:
//
//	Keeps the engine's view of the cluster up-to-date and performs general maintance tasks.
//	Runs two types of operations at configurable intervals:
//
//	Cluster Metadata Refresh:
//	- Queries all cells in the topology to find multipoolers that are part of the shards_to_watch
//	- Reads database information for the shards being watched (this will contain durability policy information)
//
//	Bookkeeping Tasks:
//	- Forget unseen poolers (remove stale entries)
//	- Clean up stale data from the in memory state store
//
// Healthcheck Loop:
//
//	Continuously monitors the health of all poolers by polling their status.
//	This loop maintains an up-to-date health snapshot in the state store.
//
//	- Poll each pooler for health status
//	- Update in-memory state store with current status
//
// Recovery Loop:
//
//	Detects problems and executes automated recovery actions.
//	This is where the actual failover logic lives.
//
//	- Analyze pooler state for problems
//	- Execute recovery actions for detected issues
//	- Coordinate failovers via consensus protocol
//
// # Configuration
//
// The Engine requires:
//   - watch-targets: List of database/tablegroup/shard targets to monitor
//   - bookkeeping-interval: How often to run cleanup tasks (default: 1m)
//   - cluster-metadata-refresh-interval: How often to refresh from topology (default: 15s)
//   - cluster-metadata-refresh-timeout: Timeout for metadata refresh operation (default: 30s)
//
// Example:
//
//	engine := NewRecoveryEngine(
//	    "zone1",                          // cell
//	    topoStore,                        // topology service
//	    logger,                           // structured logger
//	    []string{"postgres"},             // watch entire database
//	    1*time.Minute,                    // bookkeeping interval
//	    15*time.Second,                   // metadata refresh interval
//	)
//	engine.Start()
type Engine struct {
	cell   string
	ts     topo.Store
	logger *slog.Logger

	// In-memory state store
	poolerStore *store.Store[string, *store.PoolerInfo]

	// Current configuration values
	mu                             sync.Mutex // protects shardWatchTargets
	shardWatchTargets              []WatchTarget
	bookkeepingInterval            time.Duration
	clusterMetadataRefreshInterval time.Duration
	clusterMetadataRefreshTimeout  time.Duration

	// Config reloader for dynamic updates (only shardWatchTargets is dynamic)
	reloadConfig func() []string

	// Goroutine management - prevent pile-up of concurrent operations
	metadataRefreshInProgress atomic.Bool
	bookkeepingInProgress     atomic.Bool

	// Metrics
	poolerStoreSize metric.Int64ObservableGauge
	refreshLatency  metric.Float64Histogram

	// Context for shutting down loops
	ctx    context.Context
	cancel context.CancelFunc
}

// NewRecoveryEngine creates a new RecoveryEngine instance.
func NewRecoveryEngine(
	cell string,
	ts topo.Store,
	logger *slog.Logger,
	shardWatchTargets []WatchTarget,
	bookkeepingInterval time.Duration,
	clusterMetadataRefreshInterval time.Duration,
	clusterMetadataRefreshTimeout time.Duration,
) *Engine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &Engine{
		cell:                           cell,
		ts:                             ts,
		logger:                         logger,
		poolerStore:                    store.NewStore[string, *store.PoolerInfo](),
		shardWatchTargets:              shardWatchTargets,
		bookkeepingInterval:            bookkeepingInterval,
		clusterMetadataRefreshInterval: clusterMetadataRefreshInterval,
		clusterMetadataRefreshTimeout:  clusterMetadataRefreshTimeout,
		ctx:                            ctx,
		cancel:                         cancel,
	}

	// Initialize metrics
	engine.initMetrics()

	return engine
}

// initMetrics initializes OpenTelemetry metrics for the recovery engine.
func (re *Engine) initMetrics() {
	meter := otel.Meter("github.com/multigres/multigres/go/multiorch/recovery")

	// Gauge for current pooler store size
	var err error
	re.poolerStoreSize, err = meter.Int64ObservableGauge(
		"multiorch.recovery.pooler_store_size",
		metric.WithDescription("Current number of poolers tracked in the recovery engine store"),
		metric.WithUnit("{poolers}"),
	)
	if err != nil {
		re.logger.Error("failed to create pooler_store_size gauge", "error", err)
	}

	// Register callback to update the gauge
	_, err = meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			observer.ObserveInt64(re.poolerStoreSize, int64(re.poolerStore.Len()))
			return nil
		},
		re.poolerStoreSize,
	)
	if err != nil {
		re.logger.Error("failed to register pooler_store_size callback", "error", err)
	}

	// Histogram for cluster metadata refresh latency
	re.refreshLatency, err = meter.Float64Histogram(
		"multiorch.recovery.refresh_latency",
		metric.WithDescription("Latency of cluster metadata refresh operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		re.logger.Error("failed to create refresh_latency histogram", "error", err)
	}
}

// SetConfigReloader sets the function to reload configuration dynamically.
// The reloader function should return raw string targets (e.g., from viper).
// Only shardWatchTargets can be reloaded; intervals require a restart.
func (re *Engine) SetConfigReloader(reloader func() []string) {
	re.reloadConfig = reloader
}

// Start initializes and starts the RecoveryEngine loops.
func (re *Engine) Start() error {
	re.logger.Info("starting recovery engine",
		"cell", re.cell,
		"watch_targets", re.shardWatchTargets,
		"bookkeeping_interval", re.bookkeepingInterval,
		"cluster_metadata_refresh_interval", re.clusterMetadataRefreshInterval,
		"cluster_metadata_refresh_timeout", re.clusterMetadataRefreshTimeout,
	)

	// Start maintenance loop (cluster metadata refresh + bookkeeping)
	go re.runMaintenanceLoop()

	re.logger.Info("recovery engine started successfully")
	return nil
}

// Stop gracefully shuts down the RecoveryEngine.
func (re *Engine) Stop() {
	re.logger.Info("stopping recovery engine")
	re.cancel()
}

// runMaintenanceLoop runs the cluster metadata refresh and bookkeeping tasks.
// Supports dynamic reloading of shardWatchTargets via SetConfigReloader.
func (re *Engine) runMaintenanceLoop() {
	bookkeepingTicker := time.NewTicker(re.bookkeepingInterval)
	defer bookkeepingTicker.Stop()

	metadataTicker := time.NewTicker(re.clusterMetadataRefreshInterval)
	defer metadataTicker.Stop()

	re.logger.Info("maintenance loop started")

	// Do initial metadata refresh
	re.refreshClusterMetadata()

	for {
		select {
		case <-re.ctx.Done():
			re.logger.Info("maintenance loop stopped")
			return

		case <-metadataTicker.C:
			runIfNotRunning(re.logger, &re.metadataRefreshInProgress, "cluster_metadata_refresh", re.refreshClusterMetadata)

		case <-bookkeepingTicker.C:
			runIfNotRunning(re.logger, &re.bookkeepingInProgress, "bookkeeping", re.runBookkeeping)
		}
	}
}

// reloadConfigs checks for configuration changes and reloads if necessary.
// Only shardWatchTargets can be reloaded; intervals require a restart.
func (re *Engine) reloadConfigs() {
	if re.reloadConfig == nil {
		return
	}

	// Get raw target strings from viper (or other config source)
	rawTargets := re.reloadConfig()

	// Handle empty targets - keep current configuration
	if len(rawTargets) == 0 {
		re.logger.Warn("ignoring empty watch-targets during reload, keeping current targets")
		return
	}

	// Parse the raw strings into ShardWatchTarget structs
	newTargets, err := ParseShardWatchTargets(rawTargets)
	if err != nil {
		re.logger.Error("failed to parse watch-targets during reload", "error", err)
		return
	}

	// Acquire lock and update if changed
	re.mu.Lock()
	defer re.mu.Unlock()

	if !shardWatchTargetsEqual(re.shardWatchTargets, newTargets) {
		re.logger.Info("reloading shard watch targets",
			"old", shardWatchTargetsToStrings(re.shardWatchTargets),
			"new", shardWatchTargetsToStrings(newTargets),
		)
		re.shardWatchTargets = newTargets
	}
}

// shardWatchTargetsEqual compares two ShardWatchTarget slices for equality.
func shardWatchTargetsEqual(a, b []WatchTarget) bool {
	return slices.Equal(a, b)
}

// shardWatchTargetsToStrings converts ShardWatchTargets to their string representations.
func shardWatchTargetsToStrings(targets []WatchTarget) []string {
	result := make([]string, len(targets))
	for i, t := range targets {
		result[i] = t.String()
	}
	return result
}
