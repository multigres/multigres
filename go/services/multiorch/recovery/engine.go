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
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/coordinator"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
	"github.com/multigres/multigres/go/tools/timer"
)

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
//	│                        Engine                                    │
//	├──────────────────────────────────────────────────────────────────┤
//	│                                                                  │
//	│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
//	│  │ Healthcheck Loop│  │  Recovery Loop  │  │ Maintenance Loop│   │
//	│  │  (5s)           │  │   (1s)          │  │                 │   │
//	│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘   │
//	│           │                    │                    │            │
//	│           └────────────────────┼────────────────────┘            │
//	│                                ▼                                 │
//	│                        (State Store)                             │
//	└──────────────────────────────────────────────────────────────────┘
//
// # Loop Details
//
// Maintenance Loop:
//
//	Keeps the engine's view of the cluster up-to-date and performs general maintenance tasks.
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
//	The health check system uses a queue-based architecture with worker pools:
//
//	1. Discovery Phase (runs during cluster metadata refresh):
//	   - When refreshClusterMetadata() discovers poolers from topology
//	   - Newly discovered poolers (never health checked) are immediately queued
//	   - This ensures fast initial health check after discovery
//
//	2. Worker Pool (always running):
//	   - N concurrent worker goroutines (configurable via health-check-workers)
//	   - Workers consume from the health check queue (blocking)
//	   - Each worker calls pollPooler() to perform the actual health check
//	   - Updates LastCheckAttempted, LastSeen, IsUpToDate in the store
//
//	3. Health Check Ticker (periodic re-queueing):
//	   - Runs every pooler-health-check-interval (e.g., 5s)
//	   - Scans all poolers in the store
//	   - Queues poolers with outdated checks (LastCheckAttempted older than interval)
//	   - Ensures continuous health monitoring even if checks fail
//
//	The queue deduplicates entries, so a pooler can only be queued once at a time.
//
//	Health Check Queue Flow:
//
//	  ┌──────────────────────┐                  ┌──────────────────────┐
//	  │  Metadata Refresh    │──new poolers  ─> │                      │
//	  │  (periodic: 30s)     │                  │   Health Check       │
//	  └──────────────────────┘                  │   Queue              │
//	                                            │   (deduplicates)     │
//	  ┌──────────────────────┐                  │                      │
//	  │  Health Check Ticker │──stale checks -> │                      │
//	  │  (periodic: 5s)      │                  └──────────┬───────────┘
//	  └──────────────────────┘                             │
//	                                                       │ consume
//	                                                       ▼
//	                                            ┌─────────────────────┐
//	                                            │   Worker Pool       │
//	                                            │   (N workers)       │
//	                                            └──────────┬──────────┘
//	                                                       │
//	                                                       │ updates
//	                                                       ▼
//	                                            ┌──────────────────────┐
//	                                            │   Pooler Store       │
//	                                            └──────────────────────┘
//
// Recovery Loop:
//
//	Detects problems and executes automated recovery actions (runs every 1s).
//	This is where the actual failover logic lives.
//
//	Recovery Loop Flow:
//
//	  ┌──────────────────────┐
//	  │   Pooler Store       │──────────┐
//	  │  (Health Snapshots)  │          │ read
//	  └──────────────────────┘          │
//	                                    ▼
//	                         ┌──────────────────────┐
//	                         │  Analysis Generator  │
//	                         │  (builds analyses)   │
//	                         └──────────┬───────────┘
//	                                    │ per-pooler analysis
//	                                    ▼
//	                         ┌──────────────────────┐
//	                         │   Analyzers          │
//	                         │  (detect problems)   │
//	                         └──────────┬───────────┘
//	                                    │ problems
//	                                    ▼
//	                         ┌──────────────────────┐
//	                         │  Deduplication       │
//	                         │  & Prioritization    │
//	                         └──────────┬───────────┘
//	                                    │ filtered problems
//	                                    ▼
//	                         ┌──────────────────────┐
//	                         │  Validation          │
//	                         │  (force re-poll)     │
//	                         └──────────┬───────────┘
//	                                    │ validated problems
//	                                    ▼
//	                         ┌──────────────────────┐
//	                         │  Recovery Actions    │
//	                         │  (automated repairs) │
//	                         └──────────────────────┘
//
//	Deduplication Strategy:
//
//	The recovery loop applies intelligent deduplication to avoid redundant
//	recovery attempts and ensure safe, efficient problem resolution:
//
//	Sort by Priority:
//	  - Higher priority problems are processed first
//	  - Ensures critical issues (e.g., primary failure) take precedence
//
//	Smart Filtering by Scope:
//	  - If ANY shard-wide problem exists:
//	    * Return ONLY the highest priority shard-wide problem
//	    * Rationale: Shard-wide recoveries (e.g., failover) fix multiple
//	      pooler-level issues, so addressing them first is more efficient
//	  - Otherwise (only single-pooler problems):
//	    * Deduplicate by pooler ID
//	    * Keep highest priority problem per pooler
//	    * Rationale: One pooler can have multiple issues; fixing the
//	      highest priority issue often resolves downstream problems
//
//	Validation Before Recovery:
//	  - Force re-poll affected poolers to get fresh state
//	  - Re-run analyzers to confirm problem still exists
//	  - Prevents acting on stale/transient issues
//
//	Post-Recovery Refresh:
//	  - After shard-wide recoveries, force refresh all shard poolers
//	  - Ensures accurate state before next recovery cycle
//	  - Prevents re-queueing problems that were just fixed
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
//	engine := Engine(
//	    "zone1",                          // cell
//	    topoStore,                        // topology service
//	    logger,                           // structured logger
//	    []string{"postgres"},             // watch entire database
//	    1*time.Minute,                    // bookkeeping interval
//	    15*time.Second,                   // metadata refresh interval
//	)
//	engine.Start()
type Engine struct {
	ts        topoclient.Store
	logger    *slog.Logger
	config    *config.Config
	rpcClient rpcclient.MultiPoolerClient

	// In-memory state store
	poolerStore *store.PoolerHealthStore

	// Health check queue for concurrent pooler polling
	healthCheckQueue *Queue

	// Current configuration values
	mu                sync.Mutex // protects shardWatchTargets
	shardWatchTargets []config.WatchTarget

	// Config reloader for dynamic updates (only shardWatchTargets is dynamic)
	reloadConfig func() []string

	// Periodic runners for background tasks
	bookkeepingRunner      *timer.PeriodicRunner
	recoveryRunner         *timer.PeriodicRunner
	metadataRefreshRunner  *timer.PeriodicRunner
	healthCheckQueueRunner *timer.PeriodicRunner

	// Cache for deduplication (prevents redundant health checks)
	recentPollCache   map[string]time.Time
	recentPollCacheMu sync.Mutex

	// Detected problems tracking for metrics (replaced each cycle)
	detectedProblemsMu sync.Mutex
	detectedProblems   []DetectedProblemData

	// Metrics
	metrics *Metrics

	// Action factory for creating recovery actions
	actionFactory *analysis.RecoveryActionFactory

	// recoveryGracePeriodTracker tracker for grace periods before recovery actions
	recoveryGracePeriodTracker *RecoveryGracePeriodTracker

	// Context for shutting down loops
	shutdownCtx context.Context
	cancel      context.CancelFunc

	// WaitGroup to track goroutines for graceful shutdown
	wg sync.WaitGroup
}

// NewEngine creates a new RecoveryEngine instance.
func NewEngine(
	ts topoclient.Store,
	logger *slog.Logger,
	config *config.Config,
	shardWatchTargets []config.WatchTarget,
	rpcClient rpcclient.MultiPoolerClient,
	coordinator *coordinator.Coordinator,
) *Engine {
	ctx, cancel := context.WithCancel(context.TODO())

	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	engine := &Engine{
		ts:                     ts,
		logger:                 logger,
		config:                 config,
		rpcClient:              rpcClient,
		poolerStore:            poolerStore,
		healthCheckQueue:       NewQueue(logger, config),
		shardWatchTargets:      shardWatchTargets,
		recentPollCache:        make(map[string]time.Time),
		detectedProblems:       nil,
		shutdownCtx:            ctx,
		cancel:                 cancel,
		bookkeepingRunner:      timer.NewPeriodicRunner(ctx, config.GetBookkeepingInterval()),
		recoveryRunner:         timer.NewPeriodicRunner(ctx, config.GetRecoveryCycleInterval()),
		metadataRefreshRunner:  timer.NewPeriodicRunner(ctx, config.GetClusterMetadataRefreshInterval()),
		healthCheckQueueRunner: timer.NewPeriodicRunner(ctx, config.GetPoolerHealthCheckInterval()),
	}

	// Initialize metrics
	var err error
	engine.metrics, err = NewMetrics()
	if err != nil {
		logger.Error("failed to initialize recovery metrics", "error", err)
	}

	// Register callback for pooler store size observable gauge
	err = engine.metrics.RegisterPoolerStoreSizeCallback(func() int {
		return poolerStore.Len()
	})
	if err != nil {
		logger.Error("failed to monitor pooler store size", "error", err)
	}

	// Register callback for detected problems gauge
	err = engine.metrics.RegisterDetectedProblemsCallback(func() []DetectedProblemData {
		return engine.collectDetectedProblemsData()
	})
	if err != nil {
		logger.Error("failed to register detected problems callback", "error", err)
	}

	// Create action factory for recovery actions
	engine.actionFactory = analysis.NewRecoveryActionFactory(
		config,
		poolerStore,
		rpcClient,
		ts,
		coordinator,
		logger,
	)

	// Create deadline tracker for grace periods
	engine.recoveryGracePeriodTracker = NewRecoveryGracePeriodTracker(engine.shutdownCtx, config,
		WithLogger(logger))

	return engine
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
		"cell", re.config.GetCell(),
		"watch_targets", re.shardWatchTargets,
		"bookkeeping_interval", re.config.GetBookkeepingInterval(),
		"cluster_metadata_refresh_interval", re.config.GetClusterMetadataRefreshInterval(),
		"cluster_metadata_refresh_timeout", re.config.GetClusterMetadataRefreshTimeout(),
		"pooler_health_check_interval", re.config.GetPoolerHealthCheckInterval(),
		"health_check_workers", re.config.GetHealthCheckWorkers(),
	)

	// Start health check worker pool
	re.startHealthCheckWorkers()

	// Start bookkeeping runner
	re.bookkeepingRunner.Start(func(ctx context.Context) {
		re.runBookkeeping()
	}, nil)

	// Start recovery runner with dynamic interval support
	re.recoveryRunner.Start(func(ctx context.Context) {
		// Check if interval changed (dynamic config)
		newInterval := re.config.GetRecoveryCycleInterval()
		if re.recoveryRunner.UpdateInterval(newInterval) {
			re.logger.InfoContext(re.shutdownCtx, "recovery cycle interval changed", "interval", newInterval)
		}
		re.performRecoveryCycle()
	}, nil)

	// Start metadata refresh runner
	re.metadataRefreshRunner.Start(func(ctx context.Context) {
		// This interval is static today, but UpdateInterval keeps behavior consistent across runners.
		newInterval := re.config.GetClusterMetadataRefreshInterval()
		if re.metadataRefreshRunner.UpdateInterval(newInterval) {
			re.logger.InfoContext(re.shutdownCtx, "cluster metadata refresh interval changed", "interval", newInterval)
		}
		re.refreshClusterMetadata()
	}, func() { // OnStart callback
		re.logger.Info("maintenance loop started")
		re.refreshClusterMetadata()
	})

	// Start health check ticker runner
	re.healthCheckQueueRunner.Start(func(ctx context.Context) {
		newInterval := re.config.GetPoolerHealthCheckInterval()
		if re.healthCheckQueueRunner.UpdateInterval(newInterval) {
			re.logger.InfoContext(re.shutdownCtx, "pooler health check interval changed", "interval", newInterval)
		}
		re.queuePoolersHealthCheck()
	}, func() { // OnStart callback
		re.logger.Info("health check ticker loop started")
	})

	re.logger.Info("recovery engine started successfully")
	return nil
}

// Stop gracefully shuts down the RecoveryEngine.
// It cancels the context and waits for all goroutines to finish.
func (re *Engine) Stop() {
	re.logger.Info("stopping recovery engine")
	re.cancel()
	re.bookkeepingRunner.Stop()
	re.recoveryRunner.Stop()
	re.metadataRefreshRunner.Stop()
	re.healthCheckQueueRunner.Stop()
	re.wg.Wait()
	re.logger.Info("recovery engine stopped")
}

// startHealthCheckWorkers starts the worker pool that consumes from the health check queue.
// Each worker runs in its own goroutine and processes health checks concurrently.
func (re *Engine) startHealthCheckWorkers() {
	numWorkers := re.config.GetHealthCheckWorkers()
	for range numWorkers {
		re.wg.Go(func() {
			re.handlePoolerHealthChecks()
		})
	}
	re.logger.Info("health check worker pool started", "workers", numWorkers)
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
	newTargets, err := config.ParseShardWatchTargets(rawTargets)
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
func shardWatchTargetsEqual(a, b []config.WatchTarget) bool {
	return slices.Equal(a, b)
}

// shardWatchTargetsToStrings converts ShardWatchTargets to their string representations.
func shardWatchTargetsToStrings(targets []config.WatchTarget) []string {
	result := make([]string, len(targets))
	for i, t := range targets {
		result[i] = t.String()
	}
	return result
}

// collectDetectedProblemsData returns the current detected problems for metrics.
// This is called by the observable gauge callback.
func (re *Engine) collectDetectedProblemsData() []DetectedProblemData {
	re.detectedProblemsMu.Lock()
	defer re.detectedProblemsMu.Unlock()
	return re.detectedProblems
}

// updateDetectedProblems replaces the detected problems slice with current problems.
// Called each recovery cycle - the slice is replaced entirely rather than incrementally updated
// for simplicity.
func (re *Engine) updateDetectedProblems(problems []types.Problem) {
	data := make([]DetectedProblemData, 0, len(problems))
	for _, p := range problems {
		data = append(data, DetectedProblemData{
			AnalysisType: string(p.CheckName),
			DBNamespace:  p.ShardKey.Database,
			Shard:        p.ShardKey.Shard,
			PoolerID:     topoclient.MultiPoolerIDString(p.PoolerID),
		})
	}

	re.detectedProblemsMu.Lock()
	re.detectedProblems = data
	re.detectedProblemsMu.Unlock()
}
