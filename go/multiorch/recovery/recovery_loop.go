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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// runRecoveryLoop is the main recovery loop that detects and fixes problems.
func (re *Engine) runRecoveryLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	re.logger.InfoContext(re.ctx, "recovery loop started")

	for {
		select {
		case <-re.ctx.Done():
			re.logger.InfoContext(re.ctx, "recovery loop stopped")
			return

		case <-ticker.C:
			runIfNotRunning(re.logger, &re.recoveryLoopInProgress, "recovery_loop", re.performRecoveryCycle)
		}
	}
}

// performRecoveryCycle runs one cycle of problem detection and recovery.
func (re *Engine) performRecoveryCycle() {
	// Create generator - this builds the poolersByTG map once
	generator := analysis.NewAnalysisGenerator(re.poolerStore)
	analyses := generator.GenerateAnalyses()

	// Run all analyzers to detect problems
	var problems []analysis.Problem
	analyzers := analysis.DefaultAnalyzers()

	for _, poolerAnalysis := range analyses {
		for _, analyzer := range analyzers {
			detectedProblems := analyzer.Analyze(poolerAnalysis)
			problems = append(problems, detectedProblems...)
		}
	}

	if len(problems) == 0 {
		return // no problems detected
	}

	re.logger.InfoContext(re.ctx, "problems detected", "count", len(problems))

	// Group problems by shard
	problemsByShard := re.groupProblemsByShard(problems)

	// Process each shard independently in parallel
	var wg sync.WaitGroup
	for shardKey, shardProblems := range problemsByShard {
		wg.Add(1)
		go func(key analysis.ShardKey, problems []analysis.Problem) {
			defer wg.Done()
			// Create a new generator for this goroutine to avoid contention
			shardGenerator := analysis.NewAnalysisGenerator(re.poolerStore)
			re.processShardProblems(key, problems, shardGenerator)
		}(shardKey, shardProblems)
	}
	wg.Wait()
}

// groupProblemsByShard groups problems by their shard.
func (re *Engine) groupProblemsByShard(problems []analysis.Problem) map[analysis.ShardKey][]analysis.Problem {
	grouped := make(map[analysis.ShardKey][]analysis.Problem)

	for _, problem := range problems {
		key := analysis.ShardKey{
			Database:   problem.Database,
			TableGroup: problem.TableGroup,
			Shard:      problem.Shard,
		}
		grouped[key] = append(grouped[key], problem)
	}

	return grouped
}

// processShardProblems handles all problems for a single shard.
func (re *Engine) processShardProblems(shardKey analysis.ShardKey, problems []analysis.Problem, generator *analysis.AnalysisGenerator) {
	re.logger.DebugContext(re.ctx, "processing shard problems",
		"database", shardKey.Database,
		"tablegroup", shardKey.TableGroup,
		"shard", shardKey.Shard,
		"problem_count", len(problems),
	)

	// Sort by priority (highest priority first)
	sort.SliceStable(problems, func(i, j int) bool {
		return problems[i].Priority > problems[j].Priority
	})

	// Apply smart deduplication logic
	filteredProblems := re.smartFilterProblems(problems)

	// Check if there's a primary problem in this shard
	hasPrimaryProblem := re.hasPrimaryProblem(filteredProblems)

	// Attempt recoveries in priority order
	for _, problem := range filteredProblems {
		// Skip replica recoveries if primary is unhealthy and action requires healthy primary
		if problem.RecoveryAction.RequiresHealthyPrimary() && hasPrimaryProblem {
			re.logger.InfoContext(re.ctx, "skipping recovery - requires healthy primary but primary is unhealthy",
				"problem_code", problem.Code,
				"pooler_id", topo.MultiPoolerIDString(problem.PoolerID),
			)
			continue
		}

		re.attemptRecovery(problem)
	}
}

// hasPrimaryProblem checks if any of the problems indicate an unhealthy primary.
// Shard-wide problems (e.g., PrimaryDead) imply an unhealthy primary.
func (re *Engine) hasPrimaryProblem(problems []analysis.Problem) bool {
	for _, problem := range problems {
		if problem.Scope == analysis.ScopeShard {
			return true
		}
	}
	return false
}

// smartFilterProblems applies intelligent filtering to the problem list:
// - If there's a shard-wide problem, return only the highest priority shard-wide problem
// - Otherwise, deduplicate by pooler ID, keeping only the highest priority problem per pooler
//
// IMPORTANT: Input must already be sorted by priority (highest first).
func (re *Engine) smartFilterProblems(problems []analysis.Problem) []analysis.Problem {
	if len(problems) == 0 {
		return problems
	}

	// Check if there are any shard-wide problems
	var shardWideProblems []analysis.Problem
	for _, problem := range problems {
		if problem.Scope == analysis.ScopeShard {
			shardWideProblems = append(shardWideProblems, problem)
		}
	}

	// If we have shard-wide problems, return only the highest priority one
	// (since input is already sorted by priority, the first one is highest)
	if len(shardWideProblems) > 0 {
		re.logger.DebugContext(re.ctx, "shard-wide problem detected, focusing on single recovery",
			"problem_code", shardWideProblems[0].Code,
			"priority", shardWideProblems[0].Priority,
			"total_shard_wide", len(shardWideProblems),
			"total_problems", len(problems),
		)
		return []analysis.Problem{shardWideProblems[0]}
	}

	// No shard-wide problems, keep them all.
	return problems
}

// attemptRecovery attempts to recover from a single problem.
// IMPORTANT: Before attempting recovery, force re-poll the affected pooler
// to ensure the problem still exists.
func (re *Engine) attemptRecovery(problem analysis.Problem) {
	poolerIDStr := topo.MultiPoolerIDString(problem.PoolerID)

	re.logger.DebugContext(re.ctx, "attempting recovery",
		"problem_code", problem.Code,
		"pooler_id", poolerIDStr,
		"priority", problem.Priority,
		"description", problem.Description,
	)

	// Force re-poll to validate the problem still exists
	stillExists, err := re.validateProblemStillExists(problem)
	if err != nil {
		re.logger.WarnContext(re.ctx, "failed to validate problem, skipping recovery",
			"problem_code", problem.Code,
			"pooler_id", poolerIDStr,
			"error", err,
		)
		return
	}
	if !stillExists {
		re.logger.DebugContext(re.ctx, "problem no longer exists after re-poll, skipping recovery",
			"problem_code", problem.Code,
			"pooler_id", poolerIDStr,
		)
		return
	}

	// Acquire lock if needed
	if problem.RecoveryAction.RequiresLock() {
		// TODO: Implement shard locking
		re.logger.DebugContext(re.ctx, "recovery action requires lock", "problem_code", problem.Code)
	}

	// Execute recovery action
	ctx, cancel := context.WithTimeout(re.ctx, problem.RecoveryAction.Metadata().Timeout)
	defer cancel()

	err = problem.RecoveryAction.Execute(ctx, problem)
	if err != nil {
		re.logger.ErrorContext(re.ctx, "recovery action failed",
			"problem_code", problem.Code,
			"pooler_id", poolerIDStr,
			"error", err,
		)
		// TODO: Record failure in metrics
		return
	}

	re.logger.InfoContext(re.ctx, "recovery action successful",
		"problem_code", problem.Code,
		"pooler_id", poolerIDStr,
	)
	// TODO: Record success in metrics

	// Post-recovery refresh
	// If we ran a shard-wide recovery, force refresh all poolers in the shard
	// to ensure they have up-to-date state and prevent re-queueing the same problem.
	if problem.Scope == analysis.ScopeShard {
		re.logger.InfoContext(re.ctx, "forcing refresh of all poolers post recovery",
			"database", problem.Database,
			"tablegroup", problem.TableGroup,
			"shard", problem.Shard,
		)
		re.forceHealthCheckShardPoolers(context.Background(), problem.Database, problem.TableGroup, problem.Shard, nil /* poolersToIgnore */)
	}
}

// validateProblemStillExists force re-polls the pooler and re-runs analysis
// to check if the problem still exists.
//
// The validation strategy depends on the problem scope:
// - ShardWide: Refresh shard metadata + force poll all poolers in shard (except dead ones)
// - SinglePooler: Only refresh the affected pooler + primary pooler
//
// Returns (stillExists bool, error).
func (re *Engine) validateProblemStillExists(problem analysis.Problem) (bool, error) {
	poolerIDStr := topo.MultiPoolerIDString(problem.PoolerID)
	isShardWide := problem.Scope == analysis.ScopeShard

	re.logger.DebugContext(re.ctx, "validating problem still exists",
		"pooler_id", poolerIDStr,
		"problem_code", problem.Code,
		"scope", problem.Scope,
	)

	ctx, cancel := context.WithTimeout(re.ctx, 30*time.Second)
	defer cancel()

	// Refresh metadata for the shard
	if err := re.refreshShardMetadata(ctx, problem.Database, problem.TableGroup, problem.Shard, nil); err != nil {
		return false, fmt.Errorf("failed to refresh shard metadata: %w", err)
	}

	// Force re-poll poolers based on scope
	if isShardWide {
		// Shard-wide: refresh all poolers in shard except the dead one
		var poolersToIgnore []string
		if problem.Code == analysis.ProblemPrimaryDead {
			poolersToIgnore = []string{poolerIDStr}
		}
		re.forceHealthCheckShardPoolers(ctx, problem.Database, problem.TableGroup, problem.Shard, poolersToIgnore)
	} else {
		// Single-pooler: only refresh this pooler + primary
		re.logger.DebugContext(re.ctx, "refreshing single pooler and primary")

		// Refresh the affected pooler
		if ph, ok := re.poolerStore.Get(poolerIDStr); ok {
			re.pollPooler(ctx, ph.ID, ph, true /* forceDiscovery */)
		}

		// Find and refresh primary if different
		primaryID, err := re.findPrimaryInShard(problem.Database, problem.TableGroup, problem.Shard)
		if err == nil && primaryID != poolerIDStr {
			if ph, ok := re.poolerStore.Get(primaryID); ok {
				re.pollPooler(ctx, ph.ID, ph, true /* forceDiscovery */)
			}
		}
	}

	// Re-generate analysis for this specific pooler using updated store data
	// Note: GenerateAnalysisForPooler rebuilds its internal map from the current store state,
	// so it will see the fresh data from the re-poll above.
	generator := analysis.NewAnalysisGenerator(re.poolerStore)
	poolerAnalysis, err := generator.GenerateAnalysisForPooler(poolerIDStr)
	if err != nil {
		return false, fmt.Errorf("failed to generate analysis after re-poll: %w", err)
	}

	// Re-run the analyzer that originally detected this problem
	analyzers := analysis.DefaultAnalyzers()
	for _, analyzer := range analyzers {
		if analyzer.Name() == problem.CheckName {
			redetectedProblems := analyzer.Analyze(poolerAnalysis)

			// Check if the same problem code is still detected
			for _, p := range redetectedProblems {
				if p.Code == problem.Code {
					re.logger.DebugContext(re.ctx, "problem still exists after re-poll",
						"pooler_id", poolerIDStr,
						"problem_code", problem.Code,
					)
					return true, nil
				}
			}
		}
	}

	// Problem was not re-detected
	re.logger.DebugContext(re.ctx, "problem no longer exists after re-poll",
		"pooler_id", poolerIDStr,
		"problem_code", problem.Code,
	)
	return false, nil
}

// findPrimaryInShard finds the primary pooler ID for a given shard.
func (re *Engine) findPrimaryInShard(database, tablegroup, shard string) (string, error) {
	var primaryID string
	var found bool

	re.poolerStore.Range(func(poolerID string, poolerHealth *store.PoolerHealth) bool {
		if poolerHealth == nil || poolerHealth.ID == nil {
			return true
		}

		if poolerHealth.Database == database &&
			poolerHealth.TableGroup == tablegroup &&
			poolerHealth.Shard == shard &&
			poolerHealth.TopoPoolerType == clustermetadatapb.PoolerType_PRIMARY {
			primaryID = poolerID
			found = true
			return false // stop iteration
		}
		return true
	})

	if !found {
		return "", fmt.Errorf("no primary found for shard %s/%s/%s", database, tablegroup, shard)
	}

	return primaryID, nil
}
