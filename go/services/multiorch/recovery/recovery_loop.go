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
	"fmt"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// runRecoveryLoop is the main recovery loop that detects and fixes problems.
func (re *Engine) runRecoveryLoop() {
	interval := re.config.GetRecoveryCycleInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	re.logger.InfoContext(re.shutdownCtx, "recovery loop started", "interval", interval)

	for {
		select {
		case <-re.shutdownCtx.Done():
			re.logger.InfoContext(re.shutdownCtx, "recovery loop stopped")
			return

		case <-ticker.C:
			// Check if interval changed (dynamic config)
			newInterval := re.config.GetRecoveryCycleInterval()
			if newInterval != interval {
				re.logger.InfoContext(re.shutdownCtx, "recovery cycle interval changed", "old", interval, "new", newInterval)
				interval = newInterval
				ticker.Reset(interval)
			}
			runIfNotRunning(re.logger, &re.recoveryLoopInProgress, "recovery_loop", re.performRecoveryCycle)
		}
	}
}

// performRecoveryCycle runs one cycle of problem detection and recovery.
func (re *Engine) performRecoveryCycle() {
	ctx, span := telemetry.Tracer().Start(re.shutdownCtx, "recovery/cycle")
	defer span.End()

	// Create generator - this builds the poolersByTG map once
	generator := analysis.NewAnalysisGenerator(re.poolerStore)
	analyses := generator.GenerateAnalyses()

	// Run all analyzers to detect problems
	var problems []types.Problem
	analyzers := analysis.DefaultAnalyzers(re.actionFactory)

	// Track which problem codes have been detected
	detectedProblems := make(map[types.ProblemCode]bool)

	for _, poolerAnalysis := range analyses {
		for _, analyzer := range analyzers {
			problem, err := analyzer.Analyze(poolerAnalysis)
			if err != nil {
				re.logger.ErrorContext(ctx, "analyzer error",
					"analyzer", string(analyzer.Name()),
					"pooler_id", topoclient.MultiPoolerIDString(poolerAnalysis.PoolerID),
					"error", err,
				)
				re.metrics.errorsTotal.Add(ctx, "analyzer",
					attribute.String("analyzer", string(analyzer.Name())),
				)
				continue
			}

			// Only append to problems list if detected
			if problem != nil {
				problems = append(problems, *problem)
				// Mark this problem code as detected
				detectedProblems[problem.Code] = true
			}
		}
	}

	// Observe health for each analyzer (once per cycle, not once per pooler)
	// This updates grace period tracking based on whether problems were detected
	for _, analyzer := range analyzers {
		isHealthy := !detectedProblems[analyzer.ProblemCode()]
		re.deadlineTracker.Observe(analyzer.ProblemCode(), analyzer.RecoveryAction(), isHealthy)
	}

	// Update detected problems metric
	re.updateDetectedProblems(problems)

	if len(problems) == 0 {
		return // no problems detected
	}

	span.SetAttributes(attribute.Int("problems.count", len(problems)))
	re.logger.InfoContext(ctx, "problems detected", "count", len(problems))

	// Group problems by shard
	problemsByShard := re.groupProblemsByShard(problems)

	// Process each shard independently in parallel
	var wg sync.WaitGroup
	for shardKey, shardProblems := range problemsByShard {
		wg.Add(1)
		go func(key commontypes.ShardKey, problems []types.Problem) {
			defer wg.Done()
			re.processShardProblems(ctx, key, problems)
		}(shardKey, shardProblems)
	}
	wg.Wait()
}

// groupProblemsByShard groups problems by their shard.
func (re *Engine) groupProblemsByShard(problems []types.Problem) map[commontypes.ShardKey][]types.Problem {
	grouped := make(map[commontypes.ShardKey][]types.Problem)

	for _, problem := range problems {
		grouped[problem.ShardKey] = append(grouped[problem.ShardKey], problem)
	}

	return grouped
}

// processShardProblems handles all problems for a single shard.
func (re *Engine) processShardProblems(ctx context.Context, shardKey commontypes.ShardKey, problems []types.Problem) {
	re.logger.DebugContext(ctx, "processing shard problems",
		"database", shardKey.Database,
		"tablegroup", shardKey.TableGroup,
		"shard", shardKey.Shard,
		"problem_count", len(problems),
	)

	// Sort by priority and apply filtering logic
	filteredProblems := re.filterAndPrioritize(problems)

	// Check if there's a primary problem in this shard
	hasPrimaryProblem := re.hasPrimaryProblem(filteredProblems)

	// Attempt recoveries in priority order
	for _, problem := range filteredProblems {
		// Skip replica recoveries if primary is unhealthy and action requires healthy primary
		if problem.RecoveryAction.RequiresHealthyPrimary() && hasPrimaryProblem {
			re.logger.InfoContext(ctx, "skipping recovery - requires healthy primary but primary is unhealthy",
				"problem_code", problem.Code,
				"pooler_id", topoclient.MultiPoolerIDString(problem.PoolerID),
			)
			continue
		}

		re.attemptRecovery(ctx, problem)
	}
}

// hasPrimaryProblem checks if any of the problems indicate an unhealthy primary.
// Shard-wide problems (e.g., PrimaryDead) imply an unhealthy primary.
func (re *Engine) hasPrimaryProblem(problems []types.Problem) bool {
	for _, problem := range problems {
		if problem.Scope == types.ScopeShard {
			return true
		}
	}
	return false
}

// filterAndPrioritize sorts problems by priority and applies filtering:
// - Sorts by priority (highest first)
// - If there's a shard-wide problem, return only the highest priority shard-wide problem
// - Otherwise, return all problems sorted by priority
func (re *Engine) filterAndPrioritize(problems []types.Problem) []types.Problem {
	if len(problems) == 0 {
		return problems
	}

	// Sort by priority (highest priority first)
	sort.SliceStable(problems, func(i, j int) bool {
		return problems[i].Priority > problems[j].Priority
	})

	// Check if there are any shard-wide problems
	var shardWideProblems []types.Problem
	for _, problem := range problems {
		if problem.Scope == types.ScopeShard {
			shardWideProblems = append(shardWideProblems, problem)
		}
	}

	// If we have shard-wide problems, return only the highest priority one
	// (since problems are now sorted by priority, the first one is highest)
	if len(shardWideProblems) > 0 {
		re.logger.DebugContext(re.shutdownCtx, "shard-wide problem detected, focusing on single recovery",
			"problem_code", shardWideProblems[0].Code,
			"priority", shardWideProblems[0].Priority,
			"total_shard_wide", len(shardWideProblems),
			"total_problems", len(problems),
		)
		return []types.Problem{shardWideProblems[0]}
	}

	// No shard-wide problems, return all sorted by priority.
	return problems
}

// attemptRecovery attempts to recover from a single problem.
// IMPORTANT: Before attempting recovery, force re-poll the affected pooler
// to ensure the problem still exists.
func (re *Engine) attemptRecovery(ctx context.Context, problem types.Problem) {
	poolerIDStr := topoclient.MultiPoolerIDString(problem.PoolerID)
	actionName := problem.RecoveryAction.Metadata().Name

	ctx, span := telemetry.Tracer().Start(ctx, "recovery/attempt",
		trace.WithAttributes(
			attribute.String("shard.database", problem.ShardKey.Database),
			attribute.String("shard.tablegroup", problem.ShardKey.TableGroup),
			attribute.String("shard.id", problem.ShardKey.Shard),
			attribute.String("problem.code", string(problem.Code)),
			attribute.String("pooler.id", poolerIDStr),
			attribute.String("action.name", actionName),
			attribute.Int("problem.priority", int(problem.Priority)),
		))
	defer span.End()

	re.logger.DebugContext(ctx, "attempting recovery",
		"problem_code", problem.Code,
		"pooler_id", poolerIDStr,
		"priority", problem.Priority,
		"description", problem.Description,
	)

	// Check if deadline has expired (noop for problems without deadline tracking)
	if !re.deadlineTracker.ShouldExecute(problem) {
		return
	}

	// Force re-poll to validate the problem still exists
	stillExists, err := re.recheckProblem(ctx, problem)
	if err != nil {
		span.SetAttributes(attribute.String("result", "recheck_failed"))
		re.logger.WarnContext(ctx, "failed to validate problem, skipping recovery",
			"problem_code", problem.Code,
			"pooler_id", poolerIDStr,
			"error", err,
		)
		return
	}
	if !stillExists {
		span.SetAttributes(attribute.String("result", "problem_resolved"))
		re.logger.DebugContext(ctx, "problem no longer exists after re-poll, skipping recovery",
			"problem_code", problem.Code,
			"pooler_id", poolerIDStr,
		)
		// Problem resolved - Observe() in next cycle will reset deadline
		return
	}

	// Execute recovery action
	ctx, cancel := context.WithTimeout(ctx, problem.RecoveryAction.Metadata().Timeout)
	defer cancel()

	startTime := time.Now()

	err = problem.RecoveryAction.Execute(ctx, problem)
	durationMs := float64(time.Since(startTime).Milliseconds())

	if err != nil {
		span.SetAttributes(attribute.String("result", "action_failed"))
		span.RecordError(err)
		re.logger.ErrorContext(ctx, "recovery action failed",
			"problem_code", problem.Code,
			"pooler_id", poolerIDStr,
			"error", err,
		)
		re.metrics.recoveryActionDuration.Record(ctx, durationMs, actionName, string(problem.Code), RecoveryActionStatusFailure, problem.ShardKey.Database, problem.ShardKey.Shard)
		return
	}

	span.SetAttributes(attribute.String("result", "success"))
	re.logger.InfoContext(ctx, "recovery action successful",
		"problem_code", problem.Code,
		"pooler_id", poolerIDStr,
	)
	re.metrics.recoveryActionDuration.Record(ctx, durationMs, actionName, string(problem.Code), RecoveryActionStatusSuccess, problem.ShardKey.Database, problem.ShardKey.Shard)

	// After successful recovery, Observe() in next cycle will reset deadline

	// Post-recovery refresh
	// If we ran a shard-wide recovery, force health check all poolers in the shard
	// to ensure they have up-to-date state. Health check returns PoolerType from
	// pg_is_in_recovery which is authoritative (topology type is only a fallback).
	if problem.Scope == types.ScopeShard {
		re.logger.InfoContext(ctx, "forcing refresh of all poolers post recovery",
			"shard_key", problem.ShardKey.String(),
		)
		re.forceHealthCheckShardPoolers(ctx, problem.ShardKey, nil /* poolersToIgnore */)
	}
}

// recheckProblem force re-polls the pooler and re-runs analysis
// to check if the problem still exists.
//
// The validation strategy depends on the problem scope:
// - ShardWide: Refresh shard metadata + force poll all poolers in shard (except dead ones)
// - SinglePooler: Only refresh the affected pooler + primary pooler
//
// Returns (stillExists bool, error).
func (re *Engine) recheckProblem(ctx context.Context, problem types.Problem) (bool, error) {
	poolerIDStr := topoclient.MultiPoolerIDString(problem.PoolerID)
	isShardWide := problem.Scope == types.ScopeShard

	re.logger.DebugContext(ctx, "validating problem still exists",
		"pooler_id", poolerIDStr,
		"problem_code", problem.Code,
		"scope", problem.Scope,
	)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Refresh metadata for the shard
	if err := re.refreshShardMetadata(ctx, problem.ShardKey, nil); err != nil {
		return false, fmt.Errorf("failed to refresh shard metadata: %w", err)
	}

	// Force re-poll poolers based on scope
	if isShardWide {
		// Shard-wide: refresh all poolers in shard except the dead one
		var poolersToIgnore []string
		if problem.Code == types.ProblemPrimaryIsDead {
			poolersToIgnore = []string{poolerIDStr}
		}
		re.forceHealthCheckShardPoolers(ctx, problem.ShardKey, poolersToIgnore)
	} else {
		// Single-pooler: only refresh this pooler + primary
		re.logger.DebugContext(ctx, "refreshing single pooler and primary")

		// Refresh the affected pooler
		if ph, ok := re.poolerStore.Get(poolerIDStr); ok {
			re.pollPooler(ctx, ph.MultiPooler.Id, ph, true /* forceDiscovery */)
		}

		// Find and refresh primary if different
		primaryID, err := re.findPrimaryInShard(problem.ShardKey)
		if err == nil && primaryID != poolerIDStr {
			if ph, ok := re.poolerStore.Get(primaryID); ok {
				re.pollPooler(ctx, ph.MultiPooler.Id, ph, true /* forceDiscovery */)
			}
		}
	}

	// Re-generate analysis for this specific pooler using updated store data.
	// A new generator is created to capture the updated store state from the re-poll above.
	generator := analysis.NewAnalysisGenerator(re.poolerStore)
	poolerAnalysis, err := generator.GenerateAnalysisForPooler(poolerIDStr)
	if err != nil {
		return false, fmt.Errorf("failed to generate analysis after re-poll: %w", err)
	}

	// Re-run the analyzer that originally detected this problem
	analyzers := analysis.DefaultAnalyzers(re.actionFactory)
	for _, analyzer := range analyzers {
		if analyzer.Name() == problem.CheckName {
			redetectedProblem, err := analyzer.Analyze(poolerAnalysis)
			if err != nil {
				re.metrics.errorsTotal.Add(ctx, "analyzer",
					attribute.String("analyzer", string(analyzer.Name())),
				)
				return false, fmt.Errorf("analyzer %s failed during recheck: %w", analyzer.Name(), err)
			}

			// Check if the same problem code is still detected
			if redetectedProblem != nil && redetectedProblem.Code == problem.Code {
				re.logger.DebugContext(ctx, "problem still exists after re-poll",
					"pooler_id", poolerIDStr,
					"problem_code", problem.Code,
				)
				return true, nil
			}

			// Problem was not re-detected
			re.logger.DebugContext(ctx, "problem no longer exists after re-poll",
				"pooler_id", poolerIDStr,
				"problem_code", problem.Code,
			)
			return false, nil
		}
	}

	return false, fmt.Errorf("analyzer %s not found", problem.CheckName)
}

// findPrimaryInShard finds the primary pooler ID for a given shard.
func (re *Engine) findPrimaryInShard(shardKey commontypes.ShardKey) (string, error) {
	var primaryID string
	var found bool

	re.poolerStore.Range(func(poolerID string, poolerHealth *multiorchdatapb.PoolerHealthState) bool {
		if poolerHealth == nil || poolerHealth.MultiPooler == nil || poolerHealth.MultiPooler.Id == nil {
			return true
		}

		if poolerHealth.MultiPooler.Database == shardKey.Database &&
			poolerHealth.MultiPooler.TableGroup == shardKey.TableGroup &&
			poolerHealth.MultiPooler.Shard == shardKey.Shard &&
			poolerHealth.MultiPooler.Type == clustermetadatapb.PoolerType_PRIMARY {
			primaryID = poolerID
			found = true
			return false // stop iteration
		}
		return true
	})

	if !found {
		return "", fmt.Errorf("no primary found for shard %s", shardKey)
	}

	return primaryID, nil
}
