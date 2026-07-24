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

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// performRecoveryCycle runs one cycle of problem detection and recovery.
func (re *Engine) performRecoveryCycle(ctx context.Context) {
	ctx, span := telemetry.Tracer().Start(ctx, "recovery/cycle")
	defer span.End()

	// Create generator - this builds the poolersByTG map once
	generator := analysis.NewAnalysisGenerator(re.poolerCache, re.makePolicyLookup(ctx))
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

			problems = append(problems, detectedProblems...)
		}
	}

	// Reconcile grace-period deadlines against everything detected this cycle:
	// new problems start their countdown, still-present ones keep counting, and
	// problems that dropped out of the detected set are treated as resolved. This
	// must run once per cycle, after all analyzers, with the full detected set.
	//
	// Failover problems are included here (harmlessly) but their grace deadline is
	// never consulted — attemptRecovery gates failover on recruitment backoff, not
	// the grace tracker. Reconciling them keeps eviction bookkeeping uniform.
	re.recoveryGracePeriodTracker.Reconcile(problems)

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
	for _, shardProblems := range problemsByShard {
		wg.Add(1)
		go func(problems []types.Problem) {
			defer wg.Done()
			re.processShardProblems(ctx, problems[0].ShardKey, problems)
		}(shardProblems)
	}
	wg.Wait()

	// Check for dynamic interval changes
	newInterval := re.config.GetRecoveryCycleInterval()
	re.recoveryRunner.UpdateInterval(newInterval)
}

// groupProblemsByShard groups problems by their shard string key.
func (re *Engine) groupProblemsByShard(problems []types.Problem) map[string][]types.Problem {
	grouped := make(map[string][]types.Problem)

	for _, problem := range problems {
		key := string(commontypes.FormatShardKey(problem.ShardKey))
		grouped[key] = append(grouped[key], problem)
	}

	return grouped
}

// processShardProblems handles all problems for a single shard.
func (re *Engine) processShardProblems(ctx context.Context, shardKey *clustermetadatapb.ShardKey, problems []types.Problem) {
	re.logger.DebugContext(ctx, "processing shard problems",
		"database", shardKey.Database,
		"tablegroup", shardKey.TableGroup,
		"shard", shardKey.Shard,
		"problem_count", len(problems),
	)

	// Sort by priority and apply filtering logic
	filteredProblems := re.filterAndPrioritize(problems)

	// Check if there's a leader problem in this shard
	hasLeaderProblem := re.hasLeaderProblem(filteredProblems)

	// Attempt recoveries. Pooler-scoped problems run in parallel since each
	// targets a distinct node and can take up to its action timeout (e.g. 60s
	// for DemoteStaleLeader). Shard-wide problems are always returned one at
	// a time by filterAndPrioritize, so the WaitGroup has no practical effect
	// there, but the code path is unified for simplicity.
	var wg sync.WaitGroup
	for _, problem := range filteredProblems {
		// Skip follower recoveries if leader is unhealthy and action requires healthy leader
		if problem.RecoveryAction.RequiresHealthyLeader() && hasLeaderProblem {
			re.logger.InfoContext(ctx, "skipping recovery - requires healthy leader but leader is unhealthy",
				"problem_code", problem.Code,
				"pooler_id", topoclient.ComponentIDString(problem.PoolerID),
			)
			continue
		}

		wg.Add(1)
		go func(p types.Problem) {
			defer wg.Done()
			re.attemptRecovery(ctx, p)
		}(problem)
	}
	wg.Wait()
}

// hasLeaderProblem checks if any of the problems indicate an unhealthy leader.
// Shard-wide problems (e.g., LeaderIsDead) imply an unhealthy leader.
func (re *Engine) hasLeaderProblem(problems []types.Problem) bool {
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
// - Otherwise, return the highest-priority problem per pooler (different poolers run in parallel)
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
		if problem.IsShardWide() {
			shardWideProblems = append(shardWideProblems, problem)
		}
	}

	// If we have shard-wide problems, return only the highest priority one
	// (since problems are now sorted by priority, the first one is highest)
	//
	// TODO: this drops all pooler-scoped problems while any shard-wide problem
	// is active. That can deadlock: a failover (shard-wide) may require a pooler
	// to be fixed first (e.g. pg_rewind so it can participate as a standby), but
	// the pooler-scoped fix is never scheduled because the shard-wide problem
	// keeps preempting it.
	if len(shardWideProblems) > 0 {
		re.logger.DebugContext(re.shutdownCtx, "shard-wide problem detected, focusing on single recovery",
			"problem_code", shardWideProblems[0].Code,
			"priority", shardWideProblems[0].Priority,
			"total_shard_wide", len(shardWideProblems),
			"total_problems", len(problems),
		)
		return []types.Problem{shardWideProblems[0]}
	}

	// No shard-wide problems: keep only the highest-priority problem per pooler.
	// Problems are already sorted highest-first, so the first occurrence for each
	// pooler is the one to run. Different poolers execute in parallel.
	seen := make(map[topoclient.ComponentID]bool)
	var filtered []types.Problem
	for _, p := range problems {
		id := topoclient.ComponentIDString(p.PoolerID)
		if !seen[id] {
			seen[id] = true
			filtered = append(filtered, p)
		}
	}
	return filtered
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

	// Gate execution: a problem may only proceed once its timing gate permits it.
	if readyAt, ready := re.readyToExecute(problem); !ready {
		span.SetAttributes(attribute.String("result", "gated"))
		re.logger.DebugContext(ctx, "deferring recovery: gate not yet satisfied",
			"problem_code", problem.Code,
			"entity_id", entityID,
			"ready_at", readyAt,
		)
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
	generator := analysis.NewAnalysisGenerator(re.poolerCache, re.makePolicyLookup(ctx))
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
				if problem.IsShardWide() || proto.Equal(p.PoolerID, problem.PoolerID) {
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

// readyToExecute reports whether problem's timing gate permits acting now, and
// the earliest time it will (zero if immediate). It selects between the two
// gating mechanisms:
//
//   - Failover uses collective recruitment backoff: independent orchs each defer
//     to their own deterministic slot derived from the shard's observed
//     TermRevocation, and the interval escalates while recruits keep churning
//     against the same decided baseline. A shard with no observed revocation acts
//     immediately (aggressive-first) — the multi-signal failover trigger is
//     self-confirming, so no detection grace is needed.
//   - Every other action uses the local grace period.
//
// TODO: this failover-vs-everything-else split is a stopgap. The cleaner shape is
// for each recovery action to own its "may I act now?" gate (failover → backoff,
// others → grace) so this central selection dissolves.
func (re *Engine) readyToExecute(problem types.Problem) (readyAt time.Time, ready bool) {
	if isFailoverProblem(problem.Code) {
		return re.nextFailoverAttempt(problem.ShardKey)
	}
	return time.Time{}, re.recoveryGracePeriodTracker.ShouldExecute(problem)
}

// isFailoverProblem reports whether a problem is resolved by leader-replacement
// recruitment, and so is gated on collective recruitment backoff rather than the
// local grace period.
func isFailoverProblem(code types.ProblemCode) bool {
	return code == types.ProblemLeaderIsDead || code == types.ProblemLeaderResigned
}

// nextFailoverAttempt returns this orchestrator's earliest permitted failover
// recruitment time for the shard and whether that time has arrived. When no
// revocation has been observed it returns (zero, true) — act immediately
// (aggressive-first). Otherwise it derives the deterministic per-orch next-attempt
// time from the observed revocation's collective backoff, so independent orchs
// stagger and escalate their retries without coordinating.
func (re *Engine) nextFailoverAttempt(shardKey *clustermetadatapb.ShardKey) (readyAt time.Time, ready bool) {
	rev := latestObservedRevocation(re.poolerCache, shardKey)
	if rev == nil {
		return time.Time{}, true
	}
	readyAt = re.recruitmentBackoff.NextAttempt(rev, re.coordinator.GetCoordinatorID())
	return readyAt, !time.Now().Before(readyAt)
}

// latestObservedRevocation returns the most recent TermRevocation any pooler in
// the shard has accepted (highest revoked_below_term), as seen through the
// streamed health snapshots, or nil if none has been observed yet. This is how
// an orchestrator observes other orchestrators' in-flight recruitment for a
// shard without any orch-to-orch RPC.
//
// Note: an orchestrator does not observe its OWN just-written revocation until it
// streams back, so immediately after recruiting it may briefly still see the
// prior (or no) revocation and re-enter the gate. That window is bounded by
// recheckProblem, the term CAS on recruit (a stale re-attempt loses), and the
// next cycle observing the new revocation.
func latestObservedRevocation(cache *store.PoolerCache, shardKey *clustermetadatapb.ShardKey) *clustermetadatapb.TermRevocation {
	var latest *clustermetadatapb.TermRevocation
	for _, p := range store.FindPoolersInShard(cache, shardKey) {
		rev := p.Health().GetConsensusStatus().GetTermRevocation()
		if rev.GetRevokedBelowTerm() > 0 && (latest == nil || rev.GetRevokedBelowTerm() > latest.GetRevokedBelowTerm()) {
			latest = rev
		}
	}
	return latest
}
