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
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// performRecoveryCycle runs one cycle of problem detection and recovery.
func (re *Engine) performRecoveryCycle(ctx context.Context) {
	ctx, span := telemetry.Tracer().Start(ctx, "recovery/cycle")
	defer span.End()

	// Create generator - this builds the poolersByTG map once
	generator := analysis.NewAnalysisGenerator(re.poolerStore, re.makePolicyLookup(ctx))
	shardAnalyses := generator.GenerateShardAnalyses()

	// Run all analyzers to detect problems
	var problems []types.Problem
	analyzers := analysis.DefaultAnalyzers(re.actionFactory)

	for _, shardAnalysis := range shardAnalyses {
		for _, analyzer := range analyzers {
			detectedProblems, err := analyzer.Analyze(shardAnalysis)
			if err != nil {
				re.logger.ErrorContext(ctx, "analyzer error",
					"analyzer", analyzer.Name(),
					"shard", shardAnalysis.ShardKey,
					"error", err,
				)
				re.metrics.errorsTotal.Add(ctx, "analyzer",
					attribute.String("analyzer", string(analyzer.Name())),
				)
			}

			// Observe health per-pooler: a pooler is unhealthy if it appears in pooler-scoped problems.
			problematicPoolerIDs := make(map[string]bool, len(detectedProblems))
			for _, p := range detectedProblems {
				if !p.IsShardWide() && p.PoolerID != nil {
					problematicPoolerIDs[topoclient.MultiPoolerIDString(p.PoolerID)] = true
				}
			}
			for _, pa := range shardAnalysis.Analyses {
				poolerID := topoclient.MultiPoolerIDString(pa.PoolerID)
				isHealthy := !problematicPoolerIDs[poolerID]
				re.recoveryGracePeriodTracker.Observe(analyzer.ProblemCode(), poolerID, analyzer.RecoveryAction(), isHealthy)
			}

			// Observe shard-level health. The entity key is the shard key string.
			shardHasProblem := false
			for _, p := range detectedProblems {
				if p.IsShardWide() {
					shardHasProblem = true
					break
				}
			}
			re.recoveryGracePeriodTracker.Observe(analyzer.ProblemCode(), shardAnalysis.ShardKey.String(), analyzer.RecoveryAction(), !shardHasProblem)

			problems = append(problems, detectedProblems...)
		}
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

	// Check for dynamic interval changes
	newInterval := re.config.GetRecoveryCycleInterval()
	re.recoveryRunner.UpdateInterval(newInterval)
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
	filteredProblems := re.filterAndPrioritize(ctx, problems)

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
		if problem.IsShardWide() {
			return true
		}
	}
	return false
}

// filterAndPrioritize sorts problems by priority and applies filtering:
// - Sorts by priority (highest first)
// - If there's a shard-wide problem, return only the highest priority shard-wide problem
// - Otherwise, return all problems sorted by priority
func (re *Engine) filterAndPrioritize(ctx context.Context, problems []types.Problem) []types.Problem {
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
		if problem.IsShardWide() {
			shardWideProblems = append(shardWideProblems, problem)
		}
	}

	// If we have shard-wide problems, return only the highest priority one
	// (since problems are now sorted by priority, the first one is highest)
	if len(shardWideProblems) > 0 {
		re.logger.DebugContext(ctx, "shard-wide problem detected, focusing on single recovery",
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
	entityID := problem.EntityID()
	actionName := problem.RecoveryAction.Metadata().Name

	ctx, span := telemetry.Tracer().Start(ctx, "recovery/attempt",
		trace.WithAttributes(
			attribute.String("shard.database", problem.ShardKey.Database),
			attribute.String("shard.tablegroup", problem.ShardKey.TableGroup),
			attribute.String("shard.id", problem.ShardKey.Shard),
			attribute.String("problem.code", string(problem.Code)),
			attribute.String("entity.id", entityID),
			attribute.String("action.name", actionName),
			attribute.Int("problem.priority", int(problem.Priority)),
		))
	defer span.End()

	re.logger.DebugContext(ctx, "attempting recovery",
		"problem_code", problem.Code,
		"entity_id", entityID,
		"priority", problem.Priority,
		"description", problem.Description,
	)

	// Check if deadline has expired (noop for problems without deadline tracking)
	if !re.recoveryGracePeriodTracker.ShouldExecute(problem) {
		return
	}

	// Force re-poll to validate the problem still exists
	stillExists, err := re.recheckProblem(ctx, problem)
	if err != nil {
		span.SetAttributes(attribute.String("result", "recheck_failed"))
		re.logger.WarnContext(ctx, "failed to validate problem, skipping recovery",
			"problem_code", problem.Code,
			"entity_id", entityID,
			"error", err,
		)
		return
	}
	if !stillExists {
		span.SetAttributes(attribute.String("result", "problem_resolved"))
		re.logger.DebugContext(ctx, "problem no longer exists after re-poll, skipping recovery",
			"problem_code", problem.Code,
			"entity_id", entityID,
		)
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
			"entity_id", entityID,
			"error", err,
		)
		re.metrics.recoveryActionDuration.Record(ctx, durationMs, actionName, string(problem.Code), RecoveryActionStatusFailure, problem.ShardKey.Database, problem.ShardKey.Shard)
		return
	}

	span.SetAttributes(attribute.String("result", "success"))
	re.logger.InfoContext(ctx, "recovery action successful",
		"problem_code", problem.Code,
		"entity_id", entityID,
	)
	re.metrics.recoveryActionDuration.Record(ctx, durationMs, actionName, string(problem.Code), RecoveryActionStatusSuccess, problem.ShardKey.Database, problem.ShardKey.Shard)
}

// recheckProblem re-runs analysis on the current store state to confirm the
// problem still exists before executing a recovery action.
//
// Under streaming, the store is continuously updated by ManagerHealthStream
// streams, so no explicit force-poll is needed. We simply re-generate the
// shard analysis from the current store and re-run the analyzer.
//
// Returns (stillExists bool, error).
func (re *Engine) recheckProblem(ctx context.Context, problem types.Problem) (bool, error) {
	entityID := problem.EntityID()

	re.logger.DebugContext(ctx, "validating problem still exists",
		"entity_id", entityID,
		"problem_code", problem.Code,
		"scope", problem.Scope,
	)

	// Re-generate analysis for this shard using current store data.
	// Note: we analyze the full shard (all poolers) rather than a single pooler; for
	// single-pooler problems the extra poolers are harmless since analyzePooler filters by role.
	generator := analysis.NewAnalysisGenerator(re.poolerStore, re.makePolicyLookup(ctx))
	shardAnalysis, err := generator.GenerateShardAnalysis(problem.ShardKey)
	if err != nil {
		return false, fmt.Errorf("failed to generate analysis after re-poll: %w", err)
	}

	// Re-run the analyzer that originally detected this problem
	analyzers := analysis.DefaultAnalyzers(re.actionFactory)
	for _, analyzer := range analyzers {
		if analyzer.Name() == problem.CheckName {
			redetectedProblems, err := analyzer.Analyze(shardAnalysis)
			if err != nil {
				re.metrics.errorsTotal.Add(ctx, "analyzer",
					attribute.String("analyzer", string(analyzer.Name())),
				)
				return false, fmt.Errorf("analyzer %s failed during recheck: %w", analyzer.Name(), err)
			}

			// Check if the same problem is still detected.
			// For shard-wide problems, any re-detection counts (primary may have changed).
			// For pooler-scoped problems, only the same pooler counts.
			for _, p := range redetectedProblems {
				if p.Code != problem.Code {
					continue
				}
				if problem.IsShardWide() || topoclient.MultiPoolerIDString(p.PoolerID) == topoclient.MultiPoolerIDString(problem.PoolerID) {
					re.logger.DebugContext(ctx, "problem still exists after re-poll",
						"entity_id", entityID,
						"problem_code", problem.Code,
					)
					return true, nil
				}
			}

			// Problem was not re-detected
			re.logger.DebugContext(ctx, "problem no longer exists after re-poll",
				"entity_id", entityID,
				"problem_code", problem.Code,
			)
			return false, nil
		}
	}

	return false, fmt.Errorf("analyzer %s not found", problem.CheckName)
}

// makePolicyLookup returns a closure that fetches the bootstrap durability policy
// for a given database. The lookup uses a short per-call timeout so a slow etcd
// read doesn't stall a full recovery cycle.
//
// A nil return value is not a correctness issue: analyzers that require a policy
// (e.g. ShardNeedsInitialization) refuse to fire when policy is nil, so a transient
// failure simply delays bootstrap until the next cycle. GetBootstrapPolicy caches
// successful results in a sync.Map, so a healthy cluster never hits the error path.
func (re *Engine) makePolicyLookup(ctx context.Context) func(string) *clustermetadatapb.DurabilityPolicy {
	return func(database string) *clustermetadatapb.DurabilityPolicy {
		lookupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		policy, err := re.coordinator.GetBootstrapPolicy(lookupCtx, database)
		if err != nil {
			re.logger.WarnContext(ctx, "failed to load bootstrap policy; bootstrap will be skipped this cycle",
				"database", database,
				"error", err)
		}
		return policy
	}
}
