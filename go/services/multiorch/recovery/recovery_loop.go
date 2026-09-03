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
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/actions"
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

	// Check if there's a leader problem in this shard.
	hasLeaderProblem := re.hasLeaderProblem(problems)

	// Sort by priority and apply filtering logic
	filteredProblems := re.filterAndPrioritize(ctx, problems)

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
// Shard-wide problems (e.g., LeaderUnreachable) imply an unhealthy leader.
func (re *Engine) hasLeaderProblem(problems []types.Problem) bool {
	for _, problem := range problems {
		if problem.IsShardWide() {
			return true
		}
	}
	return false
}

// filterAndPrioritize sorts problems by priority, resolves scope conflicts,
// and drops any that aren't ready to execute yet (logging why via
// recordGated) — attemptRecovery only ever runs on problems already known to
// be ready:
//   - Sorts by priority (highest first)
//   - Returns the highest-priority actionable shard-wide problem that's ready
//     now, if any (gated ones are skipped, not just the top one)
//   - If none are ready, don't let a gated shard-wide problem preempt
//     pooler-scoped fixes — fall through to per-pooler filtering
//   - Otherwise, return the highest-priority ready problem per pooler
//     (different poolers run in parallel)
func (re *Engine) filterAndPrioritize(ctx context.Context, problems []types.Problem) []types.Problem {
	if len(problems) == 0 {
		return problems
	}

	// Sort by priority (highest priority first)
	sort.SliceStable(problems, func(i, j int) bool {
		return problems[i].Priority > problems[j].Priority
	})

	// Separate alert-only shard-wide problems (no remediation, so nothing to
	// preempt for) from ones with a real recovery action.
	var shardWideProblems, alertOnlyShardWideProblems []types.Problem
	for _, problem := range problems {
		if !problem.IsShardWide() {
			continue
		}
		if _, ok := problem.RecoveryAction.(*actions.AlertOnlyAction); ok {
			alertOnlyShardWideProblems = append(alertOnlyShardWideProblems, problem)
		} else {
			shardWideProblems = append(shardWideProblems, problem)
		}
	}

	// TODO: a shard-wide problem that's always ready (e.g. a failover that
	// never manages to persist a revocation anywhere, so backoff never gates
	// it) still preempts pooler-scoped fixes every cycle, indefinitely — the
	// gated case below only narrows the original deadlock risk, doesn't
	// eliminate it.
	//
	// Record every shard-wide problem that doesn't run this cycle, not just
	// the highest-priority one: gated ones (not ready) and, among the ready
	// ones, any outranked by a higher-priority ready problem (recordPreempted)
	// — a lower-priority ready problem should still run if nothing
	// higher-priority is ready, so check readiness for all of them first.
	var readyShardWide []types.Problem
	for _, p := range shardWideProblems {
		if readyAt, ready := re.readyToExecute(p); !ready {
			re.recordGated(ctx, p, readyAt)
		} else {
			readyShardWide = append(readyShardWide, p)
		}
	}
	if len(readyShardWide) > 0 {
		picked := readyShardWide[0]
		for _, p := range readyShardWide[1:] {
			re.recordPreempted(ctx, p)
		}
		re.logger.DebugContext(ctx, "shard-wide problem detected, focusing on single recovery",
			"problem_code", picked.Code,
			"priority", picked.Priority,
			"total_shard_wide", len(shardWideProblems),
			"total_problems", len(problems),
		)
		return []types.Problem{picked}
	}

	// No shard-wide problem preempting: gather the highest-priority candidate
	// per pooler (problems are sorted highest-first, so the first occurrence
	// per pooler wins), then keep only the ones actually ready to run.
	seen := make(map[topoclient.ComponentID]bool)
	var candidates []types.Problem
	if len(alertOnlyShardWideProblems) > 0 {
		candidates = append(candidates, alertOnlyShardWideProblems[0])
	}
	for _, p := range problems {
		if p.IsShardWide() {
			continue
		}
		id := topoclient.ComponentIDString(p.PoolerID)
		if !seen[id] {
			seen[id] = true
			candidates = append(candidates, p)
		}
	}

	var filtered []types.Problem
	for _, p := range candidates {
		readyAt, ready := re.readyToExecute(p)
		if !ready {
			re.recordGated(ctx, p, readyAt)
			continue
		}
		filtered = append(filtered, p)
	}
	return filtered
}

// recoveryAttemptAttributes builds the OTel attributes shared by every
// recovery/attempt span, whether the attempt runs the full attemptRecovery
// path or filterAndPrioritize already found it gated.
func recoveryAttemptAttributes(problem types.Problem) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("shard.database", problem.ShardKey.GetDatabase()),
		attribute.String("shard.tablegroup", problem.ShardKey.GetTableGroup()),
		attribute.String("shard.id", problem.ShardKey.GetShard()),
		attribute.String("problem.code", string(problem.Code)),
		attribute.String("entity.id", problem.EntityID()),
		attribute.String("action.name", problem.RecoveryAction.Metadata().Name),
		attribute.Int("problem.priority", int(problem.Priority)),
	}
}

// recordGated emits the same span/log combination for a problem that isn't
// ready to execute yet, so gating is decided and recorded once regardless of
// whether filterAndPrioritize excludes it up front or attemptRecovery's own
// gate would have deferred it.
//
// TODO: this logs at Debug (a gated problem can repeat every cycle for
// minutes) and relies on the span for prod visibility. Consider an Info log
// once some anti-spam mechanism exists (e.g. log on state transition only).
func (re *Engine) recordGated(ctx context.Context, problem types.Problem, readyAt time.Time) {
	_, span := telemetry.Tracer().Start(ctx, "recovery/attempt",
		trace.WithAttributes(recoveryAttemptAttributes(problem)...))
	span.SetAttributes(attribute.String("result", "gated"))

	args := []any{"problem_code", problem.Code, "entity_id", problem.EntityID()}
	if !readyAt.IsZero() {
		// Zero here means the action needs no grace-period tracking at all
		// (readyToExecute wouldn't have gated it in that case, so this is
		// defensive rather than a real path today).
		span.SetAttributes(attribute.String("ready_at", readyAt.Format(time.RFC3339)))
		args = append(args, "ready_at", readyAt)
	}
	span.End()
	re.logger.DebugContext(ctx, "deferring recovery: gate not yet satisfied", args...)
}

// recordPreempted emits a recovery/attempt span (result=preempted) for a
// shard-wide problem that was ready to run but lost this cycle's single slot
// to a higher-priority shard-wide problem that was also ready — distinct
// from recordGated, since this one could have run.
func (re *Engine) recordPreempted(ctx context.Context, problem types.Problem) {
	_, span := telemetry.Tracer().Start(ctx, "recovery/attempt",
		trace.WithAttributes(recoveryAttemptAttributes(problem)...))
	span.SetAttributes(attribute.String("result", "preempted"))
	span.End()
	re.logger.DebugContext(ctx, "skipping recovery: preempted by a higher-priority shard-wide problem",
		"problem_code", problem.Code, "entity_id", problem.EntityID())
}

// attemptRecovery attempts to recover from a single problem. Callers must
// only pass problems filterAndPrioritize has already found ready to execute.
// IMPORTANT: Before attempting recovery, force re-poll the affected pooler
// to ensure the problem still exists.
func (re *Engine) attemptRecovery(ctx context.Context, problem types.Problem) {
	entityID := problem.EntityID()
	actionName := problem.RecoveryAction.Metadata().Name

	ctx, span := telemetry.Tracer().Start(ctx, "recovery/attempt",
		trace.WithAttributes(recoveryAttemptAttributes(problem)...))
	defer span.End()

	re.logger.DebugContext(ctx, "attempting recovery",
		"problem_code", problem.Code,
		"entity_id", entityID,
		"priority", problem.Priority,
		"description", problem.Description,
	)

	// Force re-poll to validate the problem still exists
	rechecked, err := re.recheckProblem(ctx, problem)
	if err != nil {
		span.SetAttributes(attribute.String("result", "recheck_failed"))
		re.logger.WarnContext(ctx, "failed to validate problem, skipping recovery",
			"problem_code", problem.Code,
			"entity_id", entityID,
			"error", err,
		)
		return
	}
	if rechecked == nil {
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

	err = problem.RecoveryAction.Execute(ctx, *rechecked)
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
// Returns nil (with a nil error) if the problem is no longer detected — the
// caller should skip recovery. Otherwise returns the redetected problem
// bundled with the shard's highest known consensus position as of this
// recheck (the same snapshot the analyzer just re-verified it against), so
// Execute anchors its CAS on exactly what was just judged safe rather than
// re-deriving the rule itself and risking a second, independently-read
// snapshot that could in principle disagree.
func (re *Engine) recheckProblem(ctx context.Context, problem types.Problem) (*types.RecheckedProblem, error) {
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
		return nil, fmt.Errorf("failed to generate analysis after re-poll: %w", err)
	}
	rule := shardAnalysis.HighestPosition

	// Re-run the analyzer that originally detected this problem
	analyzers := analysis.DefaultAnalyzers(re.actionFactory)
	for _, analyzer := range analyzers {
		if analyzer.Name() == problem.CheckName {
			redetectedProblems, err := analyzer.Analyze(shardAnalysis)
			if err != nil {
				re.metrics.errorsTotal.Add(ctx, "analyzer",
					attribute.String("analyzer", string(analyzer.Name())),
				)
				return nil, fmt.Errorf("analyzer %s failed during recheck: %w", analyzer.Name(), err)
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
					return &types.RecheckedProblem{Problem: p, HighestKnownRule: rule}, nil
				}
			}

			// Problem was not re-detected
			re.logger.DebugContext(ctx, "problem no longer exists after re-poll",
				"entity_id", entityID,
				"problem_code", problem.Code,
			)
			return nil, nil
		}
	}

	return nil, fmt.Errorf("analyzer %s not found", problem.CheckName)
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
// the earliest time it will (zero if immediate). Failover problems use
// collective recruitment backoff (independent orchs defer to a deterministic
// slot derived from the shard's observed TermRevocation, escalating while
// recruits churn against the same baseline; no observed revocation means act
// immediately). Every other action uses the local grace period.
//
// TODO: this failover-vs-everything-else split is a stopgap; each recovery
// action should own its "may I act now?" gate so this selection dissolves.
//
// TODO: consider throttling how often *successful* failovers happen per
// shard, as a backstop against a flapping health bug (each flap looks like a
// fresh, ungated problem here).
//
// TODO: collective backoff has no persist-across-ticks debounce like
// recoveryGracePeriodTracker — a first-ever failover acts on one detection.
// Fine for first-hand causes (LeaderResigned); less clearly so for
// observer-derived ones (LeaderUnreachableByCohort, LeaderUnhealthy), whose
// quorum-of-followers check is a different anti-false-positive mechanism.
// Revisit if this causes false-positive failovers.
func (re *Engine) readyToExecute(problem types.Problem) (readyAt time.Time, ready bool) {
	if problem.Code.IsFailoverProblem() {
		return re.nextFailoverAttempt(problem.ShardKey)
	}
	return re.recoveryGracePeriodTracker.ShouldExecute(problem)
}

// nextFailoverAttempt returns this orchestrator's earliest permitted failover
// recruitment time for the shard and whether that time has arrived — the
// decision logic lives in consensus.Coordinator.NextFailoverAttempt, which
// this just supplies with the shard's pooler health states (via streamed
// health snapshots, no orch-to-orch RPC).
//
// Note: an orchestrator doesn't see its own just-written revocation until it
// streams back, so it may briefly re-enter the failover gate right after
// recruiting. Bounded by recheckProblem, the term CAS (a stale re-attempt
// loses), and the next cycle observing the new revocation.
func (re *Engine) nextFailoverAttempt(shardKey *clustermetadatapb.ShardKey) (readyAt time.Time, ready bool) {
	poolers := store.FindPoolersInShard(re.poolerCache, shardKey)
	healthStates := make([]*multiorchdatapb.PoolerHealthState, len(poolers))
	for i, p := range poolers {
		healthStates[i] = p.Health()
	}
	return re.coordinator.NextFailoverAttempt(healthStates, re.recruitmentBackoff)
}
