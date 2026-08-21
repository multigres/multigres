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

// Package types contains shared types for the recovery system.
// This package exists to avoid circular dependencies between the actions and analysis packages.
package types

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// CheckName uniquely identifies a health check.
type CheckName string

// ProblemCode identifies the category of problem.
// Multiple checks can return the same ProblemCode.
// This enables many-to-one mapping: many checks → one recovery action.
type ProblemCode string

const (
	// Shard bootstrap problems (highest priority - shard cannot function at all).
	ProblemShardNeedsInitialization ProblemCode = "ShardNeedsInitialization"

	// Leader problems (catastrophic - block everything else). Each names a leader
	// that must be replaced; they differ in the evidence that convicted it, so
	// dashboards and failover reasons can distinguish the cause. They share one
	// recovery action and one per-shard failover throttle — the throttle keys on
	// the outgoing decision, not the cause.
	//
	// Predictors vs backstop: the property we actually care about is whether the
	// shard is making durable (quorum-commit) write progress. The eventual
	// LeaderStuck (see TODO below) measures that directly and is the backstop that
	// catches a stall from any cause. The codes here are faster, higher-confidence
	// *predictors* of (imminent) stuckness — they let us act before, or explain
	// why, progress stops — but they are not exhaustive.
	//
	// The dividing principle is first-hand vs observer-derived evidence:
	//   - LeaderUnspecified: the rule has a cohort but names no leader (e.g. a leader
	//     was removed and none recruited yet) — recruit one. There is no leader to
	//     reason about, so only the feasibility gate applies. (An *empty* cohort is
	//     the unbootstrapped case and belongs to ShardNeedsInitialization instead.)
	//   - LeaderResigned: the leader voluntarily signalled it should step down.
	//     First-hand; act immediately.
	//   - LeaderUnhealthy: the leader is observed live but reports its own postgres
	//     dead/unresponsive. First-hand about itself, so no quorum corroboration is
	//     required.
	//   - LeaderUnreachableByCohort: observer-derived — a durability-sufficient set
	//     of the cohort no longer reaches the leader, so it cannot maintain quorum.
	//     Quorum-gated precisely because we are inferring rather than being told.
	//
	// TODO(LeaderStuck): a further cause — leader reachable and claiming health but
	// the quorum-commit position is not advancing — is not yet split out. Detecting
	// it correctly needs a quorum-commit signal (per-replica replay lag is not
	// quorum-safe: standbys replay WAL ahead of the synchronous-quorum ack). That
	// waits on a quorum-commit watermark in the heartbeat row; see the failover
	// detection redesign note.
	ProblemLeaderUnspecified         ProblemCode = "LeaderUnspecified"
	ProblemLeaderUnreachableByCohort ProblemCode = "LeaderUnreachableByCohort"
	ProblemLeaderUnhealthy           ProblemCode = "LeaderUnhealthy"
	ProblemLeaderResigned            ProblemCode = "LeaderResigned"
)

// IsFailoverProblem reports whether this problem is resolved by
// leader-replacement recruitment — true for exactly the Leader* codes above,
// which share one recovery action and one per-shard failover throttle.
func (c ProblemCode) IsFailoverProblem() bool {
	return c == ProblemLeaderUnspecified ||
		c == ProblemLeaderUnreachableByCohort ||
		c == ProblemLeaderUnhealthy ||
		c == ProblemLeaderResigned
}

const (

	// Replica problems (require healthy leader).
	ProblemReplicaNotReplicating ProblemCode = "ReplicaNotReplicating"
	// ProblemReplicaRecruitAbandoned is a follower stranded by an abandoned
	// recruit: its TermRevocation revokes the leader's committed rule (a failover
	// was started at a higher term, reached this follower, then never committed),
	// so it rejects the leader's SetPrimary and cannot rejoin. The remedy is a
	// leader-led no-op rule advance (same leader/cohort/term, fresh subterm) that
	// moves the committed decision past the revocation's outgoing_rule, defeating
	// it via the runaway-recruit override in IsRuleRevoked.
	ProblemReplicaRecruitAbandoned ProblemCode = "ReplicaRecruitAbandoned"
	ProblemReplicaWrongPrimary     ProblemCode = "ReplicaWrongPrimary"
	ProblemReplicaLagging          ProblemCode = "ReplicaLagging"
	ProblemReplicaMisconfigured    ProblemCode = "ReplicaMisconfigured"
	ProblemReplicaIsWritable       ProblemCode = "ReplicaIsWritable"
	ProblemStaleLeader             ProblemCode = "StaleLeader"

	// Cohort drift problems (require healthy leader; not service-impacting on
	// their own, but durability degrades if left unaddressed).
	ProblemPoolerNotInCohort      ProblemCode = "PoolerNotInCohort"
	ProblemCohortMemberIneligible ProblemCode = "CohortMemberIneligible"

	// Shard-level assessments the leader-health analyzer emits about failover
	// feasibility and observability, rather than a specific leader fault. The
	// Shard* prefix marks these as not directly actionable by orch, in contrast to
	// the Leader* problems above (which the shard recovers from by failing over).
	// None implies data loss — committed transactions met quorum and remain durable;
	// only forward progress is blocked or (when health is unknown) unverifiable. All
	// are non-actionable alerts (no-op recovery action; the detected-problems metric
	// is the signal), distinguished by whether the shard is progressing, stuck, or
	// unobservable:
	//   - ShardAtRisk: the leader is healthy and progressing, but a loss of it could
	//     not be recovered from. A warning — the shard is up but fragile.
	//   - ShardStuck: the leader needs replacement AND no recruitment quorum is
	//     reachable, so progress is halted and cannot resume automatically. Critical
	//     — a human must intervene. (Stronger than LeaderStuck, which is recoverable.)
	//   - NoHealthyCohortMembers: orch has no fresh, valid health from any initialized
	//     pooler in the shard, so it is blind — it can determine the leader/rule only
	//     from stale observations. Rather than convict the leader on stale evidence
	//     (which would misreport ShardStuck), it surfaces the blind spot. Distinct
	//     from ShardStuck, a confident verdict backed by fresh health showing the
	//     leader failed with no reachable quorum. Often transient (an orch-side health
	//     gap) and clears once fresh health returns.
	//   - LeaderHealthUnknown: orch has a fresh, recoverable cohort but cannot conclude
	//     whether the leader is serving a quorum or cut off from it — it hasn't gathered
	//     conclusive evidence either way. A warning, not an emergency: the leader may be
	//     fine. Transient at cold start (clears as evidence arrives); persistent means
	//     orch has genuinely lost sight of the leader with an ambiguous cohort.
	ProblemShardAtRisk            ProblemCode = "ShardAtRisk"
	ProblemShardStuck             ProblemCode = "ShardStuck"
	ProblemNoHealthyCohortMembers ProblemCode = "NoHealthyCohortMembers"
	ProblemLeaderHealthUnknown    ProblemCode = "LeaderHealthUnknown"
)

// Category groups checks by what they monitor.
type Category string

const (
	CategoryLeader        Category = "Leader"
	CategoryReplica       Category = "Replica"
	CategoryConfiguration Category = "Configuration"
)

// Priority defines the priority of recovery actions.
// Higher values = higher priority (will be attempted first).
type Priority int

const (
	// PriorityShardBootstrap is for shard-wide bootstrap issues like no primary exists.
	// This is the highest priority - the shard cannot function at all.
	// Arbitrarily high priority, to leave room for other priorities.
	PriorityShardBootstrap Priority = 10000

	// PriorityEmergency is for catastrophic issues like dead leader.
	// These must be fixed before anything else can proceed.
	PriorityEmergency Priority = 1000

	// PriorityHigh is for serious issues that don't block everything.
	// Examples: replica not replicating, replica pointing to wrong primary.
	PriorityHigh Priority = 500

	// PriorityNormal is for configuration drift and minor issues.
	// Examples: leader not accepting writes, replica is writable.
	PriorityNormal Priority = 100
)

// ProblemScope indicates whether a problem affects the whole shard or just a single pooler.
type ProblemScope string

const (
	// ScopeShard indicates the problem affects the entire shard (e.g., leader dead).
	// Recovery requires refreshing all poolers in the shard and may involve shard-wide operations like failover.
	ScopeShard ProblemScope = "Shard"

	// ScopePooler indicates the problem affects only a specific pooler.
	// Recovery only requires refreshing the affected pooler and potentially the leader.
	ScopePooler ProblemScope = "Pooler"
)

// Problem represents a detected issue.
type Problem struct {
	Code           ProblemCode                 // Category of problem
	CheckName      CheckName                   // Which check detected it
	PoolerID       *clustermetadatapb.ID       // Affected pooler; can be nil for shard-scoped problems
	ShardKey       *clustermetadatapb.ShardKey // Identifies the affected shard
	Description    string                      // Human-readable description
	Priority       Priority                    // Priority of this problem
	Scope          ProblemScope                // Whether this affects the whole cluster or just one pooler
	DetectedAt     time.Time                   // When the problem was detected
	RecoveryAction RecoveryAction              // What to do about it
}

// IsShardWide reports whether this problem affects the entire shard.
func (p Problem) IsShardWide() bool {
	return p.Scope == ScopeShard
}

// EntityID returns a stable string identifying the affected entity.
// For pooler-scoped problems this is the pooler ID string; for shard-scoped
// problems it is the shard key string. Safe to call when PoolerID is nil.
func (p Problem) EntityID() string {
	if p.Scope == ScopePooler && p.PoolerID != nil {
		return string(topoclient.ComponentIDString(p.PoolerID))
	}
	return string(commontypes.FormatShardKey(p.ShardKey))
}

// GracePeriodConfig holds grace period settings for recovery actions.
type GracePeriodConfig struct {
	BaseDelay time.Duration
	MaxJitter time.Duration
}

// RecheckedProblem pairs a Problem with the exact consensus rule the engine's
// pre-execution recheck just re-verified it against (the same ShardAnalysis
// that redetected the problem — see recovery_loop.go's recheckProblem). The
// two are bundled deliberately: an action performing a CAS-anchored mutation
// must judge safety and anchor its CAS on the same rule, or the two could
// silently disagree. Passing them as independent parameters would let a
// caller pass a mismatched pair and still compile; this makes that
// structurally impossible — the only way to obtain one is from a successful
// recheck. HighestKnownRule is nil if no rule was known for the shard at
// recheck time; actions that don't mutate the consensus rule can ignore it.
type RecheckedProblem struct {
	Problem
	HighestKnownRule *clustermetadatapb.RulePosition
}

// RecoveryAction is a function that fixes a problem.
type RecoveryAction interface {
	// Execute performs the recovery against the problem and rule the
	// engine's pre-execution recheck just reconfirmed together.
	Execute(ctx context.Context, rechecked RecheckedProblem) error

	// Metadata returns info about this recovery.
	Metadata() RecoveryMetadata

	// RequiresHealthyLeader indicates if this recovery requires a healthy leader.
	// If true, the recovery will be skipped when the leader is unhealthy.
	// This provides an extra guardrail to avoid accidental operations on replicas
	// when the cluster is not healthy (e.g., can't fix replica replication if leader is dead).
	RequiresHealthyLeader() bool

	// GracePeriod returns the grace period configuration for this action.
	// Returns nil if no grace period is needed (action executes immediately).
	GracePeriod() *GracePeriodConfig
}

// RecoveryMetadata describes the recovery action.
type RecoveryMetadata struct {
	Name        string
	Description string
	Timeout     time.Duration
	// LockTimeout is the maximum time to wait for lock acquisition.
	// Should be shorter than Timeout to leave time for the actual operation.
	// Defaults to 15 seconds if zero.
	LockTimeout time.Duration
	Retryable   bool
}

// GetLockTimeout returns the lock timeout, defaulting to 15 seconds if not set.
func (m RecoveryMetadata) GetLockTimeout() time.Duration {
	if m.LockTimeout == 0 {
		return 15 * time.Second
	}
	return m.LockTimeout
}
