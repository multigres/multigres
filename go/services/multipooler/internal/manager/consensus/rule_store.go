// Copyright 2026 Supabase, Inc.
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

package consensus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// RuleStorer is the interface for reading and writing the current shard rule.
// *ruleStore implements this; tests use fakeRuleStore.
type RuleStorer interface {
	// ObservePosition reads the current rule and WAL LSN from postgres.
	// Always returns a non-nil position when err is nil (the initial row guarantees a row exists).
	ObservePosition(ctx context.Context) (*clustermetadatapb.PoolerPosition, error)
	UpdateRule(ctx context.Context, update *RuleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error)
	// CreateRuleTables creates multigres.current_rule and multigres.rule_history
	// if they do not already exist, and inserts the initial row for the default
	// shard, populated with the given durability policy. bootstrapID is recorded
	// as the initial row's coordinator_id, analogous to how a pooler is the
	// coordinator for leader-led rule changes. It is idempotent and safe to
	// call multiple times.
	CreateRuleTables(ctx context.Context, policy *clustermetadatapb.DurabilityPolicy, bootstrapID *clustermetadatapb.ID) error
	// CachedPosition returns the most recently observed or written PoolerPosition
	// from memory, without querying postgres. Returns nil if no position has been
	// cached yet (e.g. before the first ObservePosition or UpdateRule call).
	CachedPosition() *clustermetadatapb.PoolerPosition

	// HasInconsistentGUC returns true if the cached rule's policy would produce
	// different GUC strings than what postgres currently has. Safe to call
	// without the action lock.
	HasInconsistentGUC(ctx context.Context) bool

	// ReconcileGUC re-reads the current rule (under SELECT FOR UPDATE when
	// inRecovery is false) and re-applies the GUC if needed. Requires the
	// action lock.
	ReconcileGUC(ctx context.Context, inRecovery bool) error

	// ClearSyncStandby clears synchronous_standby_names via SyncStandbyManager so
	// the manager's cache stays coherent (it is the sole writer). Used when a node
	// is demoted to a read-only standby. Requires the action lock and that postgres
	// is already in recovery.
	ClearSyncStandby(ctx context.Context) error
}

// ruleStore manages the current shard rule in postgres.
//
// All DB operations that write or read the current rule go through ruleStore,
// ensuring consistent access to rule state.
type ruleStore struct {
	logger       *slog.Logger
	queryService executor.InternalQueryService
	syncStandby  SyncStandbyManager

	mu      sync.Mutex
	lastPos *clustermetadatapb.PoolerPosition // updated on every ObservePosition / UpdateRule
}

// NewRuleStore creates a ruleStore. ssm must not be nil; tests that do not
// need GUC verification should pass noopSyncStandbyManager{}.
func NewRuleStore(
	logger *slog.Logger,
	qs executor.InternalQueryService,
	ssm SyncStandbyManager,
) *ruleStore {
	return &ruleStore{
		logger:       logger,
		queryService: qs,
		syncStandby:  ssm,
	}
}

// cacheRuleObservation updates the in-memory position cache.
func (rs *ruleStore) cacheRuleObservation(pos *clustermetadatapb.PoolerPosition) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.lastPos != nil && pos != nil && consensus.CompareRuleNumbers(pos.GetRule().GetRuleNumber(), rs.lastPos.GetRule().GetRuleNumber()) < 0 {
		// This position observation is stale. Ignore it.
		return
	}
	rs.lastPos = pos
}

// CachedPosition returns the most recently observed or written PoolerPosition
// from memory. Returns nil if no position has been cached yet.
func (rs *ruleStore) CachedPosition() *clustermetadatapb.PoolerPosition {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.lastPos
}

// HasInconsistentGUC returns true if the cached rule's policy would produce
// different GUC strings than what postgres currently has. Safe to call
// without the action lock.
func (rs *ruleStore) HasInconsistentGUC(ctx context.Context) bool {
	pos := rs.CachedPosition()
	if pos.GetRule().GetDurabilityPolicy() == nil {
		return false
	}
	policy, err := consensus.NewPolicyFromProto(pos.GetRule().GetDurabilityPolicy())
	if err != nil {
		return false
	}
	needs, err := rs.syncStandby.NeedsApply(ctx, consensus.PolicyWithCohort{
		Policy: policy,
		Cohort: pos.GetRule().GetCohortMembers(),
	})
	if err != nil {
		return false
	}
	return needs
}

// ReconcileGUC re-reads the current rule under SELECT FOR UPDATE to drain prior
// writers, then re-applies the GUC if the cached values are stale. Requires the
// action lock.
func (rs *ruleStore) ReconcileGUC(ctx context.Context, inRecovery bool) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return fmt.Errorf("ReconcileGUC: %w", err)
	}
	pos, lockedCtx, err := rs.readCurrentRuleLocked(ctx, inRecovery)
	if err != nil {
		return fmt.Errorf("ReconcileGUC: %w", err)
	}
	if pos.GetRule().GetDurabilityPolicy() == nil {
		return nil
	}
	policy, err := consensus.NewPolicyFromProto(pos.GetRule().GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("ReconcileGUC: invalid durability policy: %w", err)
	}
	return rs.syncStandby.SetPolicy(lockedCtx, consensus.PolicyWithCohort{
		Policy: policy,
		Cohort: pos.GetRule().GetCohortMembers(),
	})
}

// ClearSyncStandby clears synchronous_standby_names via SyncStandbyManager so the
// manager's cache stays coherent. See SyncStandbyManager.Clear.
func (rs *ruleStore) ClearSyncStandby(ctx context.Context) error {
	return rs.syncStandby.Clear(ctx)
}

// ----------------------------------------------------------------------------
// Rule Update Builder
// ----------------------------------------------------------------------------

// ruleNumber identifies a specific rule version by coordinator term and subterm.
type ruleNumber struct {
	coordinatorTerm int64
	leaderSubterm   int64
}

// RuleUpdateBuilder constructs the parameters for UpdateRule.
// coordinatorID, eventType, reason, and createdAt are always required.
// Fields not set via builder methods retain their current value in current_rule.
type RuleUpdateBuilder struct {
	// required
	termNumber    int64
	coordinatorID *clustermetadatapb.ID
	eventType     string
	reason        string
	createdAt     time.Time

	// optional; nil means keep the existing value in current_rule
	leaderID         *clustermetadatapb.ID
	cohortMembers    []*clustermetadatapb.ID
	durabilityPolicy *clustermetadatapb.DurabilityPolicy

	// history-only optional fields
	walPosition     string
	operation       string
	acceptedMembers []*clustermetadatapb.ID

	force              bool
	skipOutgoingQuorum bool        // skip BuildPolicyTransition; apply incoming GUC directly
	previousRule       *ruleNumber // for compare-and-swap; nil means no check
	promotionHook      promotionFn // non-nil iff postgres is known to be in recovery
}

// promotionFn is called by UpdateRule after the pre-promote GUC is applied and
// before the rule history write. It must call pg_promote() and wait for promotion
// to complete. It is provided iff the caller has already verified that postgres
// is in recovery.
type promotionFn func(ctx context.Context) error

// Accessors for the builder's fields. Exposed so callers and tests in other
// packages can inspect a constructed update.
func (b *RuleUpdateBuilder) GetEventType() string                      { return b.eventType }
func (b *RuleUpdateBuilder) GetReason() string                         { return b.reason }
func (b *RuleUpdateBuilder) GetTermNumber() int64                      { return b.termNumber }
func (b *RuleUpdateBuilder) GetCoordinatorID() *clustermetadatapb.ID   { return b.coordinatorID }
func (b *RuleUpdateBuilder) GetLeaderID() *clustermetadatapb.ID        { return b.leaderID }
func (b *RuleUpdateBuilder) GetCohortMembers() []*clustermetadatapb.ID { return b.cohortMembers }
func (b *RuleUpdateBuilder) GetWALPosition() string                    { return b.walPosition }

func (b *RuleUpdateBuilder) GetDurabilityPolicy() *clustermetadatapb.DurabilityPolicy {
	return b.durabilityPolicy
}
func (b *RuleUpdateBuilder) GetAcceptedMembers() []*clustermetadatapb.ID { return b.acceptedMembers }

// GetPromotionHook returns the promotion hook set on this update, if any. Exposed for tests.
func (b *RuleUpdateBuilder) GetPromotionHook() promotionFn { return b.promotionHook }

func NewRuleUpdate(termNumber int64, coordinatorID *clustermetadatapb.ID, eventType, reason string, createdAt time.Time) *RuleUpdateBuilder {
	return &RuleUpdateBuilder{
		termNumber:    termNumber,
		coordinatorID: coordinatorID,
		eventType:     eventType,
		reason:        reason,
		createdAt:     createdAt,
	}
}

func (b *RuleUpdateBuilder) WithLeader(id *clustermetadatapb.ID) *RuleUpdateBuilder {
	b.leaderID = id
	return b
}

func (b *RuleUpdateBuilder) WithCohort(members []*clustermetadatapb.ID) *RuleUpdateBuilder {
	b.cohortMembers = members
	return b
}

func (b *RuleUpdateBuilder) WithWALPosition(pos string) *RuleUpdateBuilder {
	b.walPosition = pos
	return b
}

func (b *RuleUpdateBuilder) WithPromotionHook(fn promotionFn) *RuleUpdateBuilder {
	b.promotionHook = fn
	return b
}

func (b *RuleUpdateBuilder) WithOperation(op string) *RuleUpdateBuilder {
	b.operation = op
	return b
}

func (b *RuleUpdateBuilder) WithAcceptedMembers(members []*clustermetadatapb.ID) *RuleUpdateBuilder {
	b.acceptedMembers = members
	return b
}

func (b *RuleUpdateBuilder) WithDurabilityPolicy(policy *clustermetadatapb.DurabilityPolicy) *RuleUpdateBuilder {
	b.durabilityPolicy = policy
	return b
}

func (b *RuleUpdateBuilder) WithForce() *RuleUpdateBuilder {
	b.force = true
	return b
}

// WithSkipOutgoingQuorum instructs UpdateRule to skip BuildPolicyTransition and apply
// the incoming cohort GUC directly (Both = Incoming). Used for coordinator-directed
// changes where the outgoing cohort is empty (bootstrap) or the coordinator has already
// verified the transition is safe, so no dual-ack window is needed.
func (b *RuleUpdateBuilder) WithSkipOutgoingQuorum() *RuleUpdateBuilder {
	b.skipOutgoingQuorum = true
	return b
}

// WithPreviousRule adds a compare-and-swap check: the update only proceeds if the
// current rule matches the given coordinator term and subterm.
func (b *RuleUpdateBuilder) WithPreviousRule(coordinatorTerm, leaderSubterm int64) *RuleUpdateBuilder {
	b.previousRule = &ruleNumber{coordinatorTerm: coordinatorTerm, leaderSubterm: leaderSubterm}
	return b
}

// ----------------------------------------------------------------------------
// Schema Operations
// ----------------------------------------------------------------------------

// CreateRuleTables creates multigres.current_rule and multigres.rule_history if
// they do not already exist, then inserts the initial row for the default
// shard. It is idempotent and safe to call multiple times.
//
// current_rule holds a single row per shard representing the current cluster rule.
// It is used as a locking target (SELECT FOR UPDATE) to serialise concurrent
// writes; rule_history provides the append-only audit log.
//
// coordinator_term=0 in the initial row means no rule has been applied yet.
// policy is written into the initial row so all subsequent rule reads have a
// non-nil DurabilityPolicy; operations that do not change the policy (e.g.
// Promote) carry it forward via COALESCE in UpdateRule.
//
// bootstrapID becomes the initial row's coordinator_id. The pooler that
// initializes the schema acts as the coordinator for the initial row —
// analogous to how a pooler is the coordinator for leader-led rule changes.
func (rs *ruleStore) CreateRuleTables(ctx context.Context, policy *clustermetadatapb.DurabilityPolicy, bootstrapID *clustermetadatapb.ID) error {
	if policy == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "durability policy required to initialize rule tables")
	}
	if policy.QuorumType == clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN || policy.RequiredCount <= 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"invalid durability policy: quorum_type=%v required_count=%d", policy.QuorumType, policy.RequiredCount)
	}
	if bootstrapID == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "bootstrapID is required to initialize rule tables")
	}

	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	if _, err := rs.queryService.Query(execCtx, `CREATE TABLE multigres.current_rule (
		shard_id                  BYTEA PRIMARY KEY,
		coordinator_term          BIGINT NOT NULL,
		leader_subterm            BIGINT NOT NULL,
		leader_id                 TEXT,
		coordinator_id            TEXT NOT NULL,
		cohort_members            TEXT[] NOT NULL,
		durability_policy_name    TEXT NOT NULL,
		durability_quorum_type    TEXT NOT NULL,
		durability_required_count INT NOT NULL,
		created_at                TIMESTAMPTZ NOT NULL
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create current_rule table")
	}

	if _, err := rs.queryService.QueryArgs(execCtx, `
		INSERT INTO multigres.current_rule
		  (shard_id, coordinator_term, leader_subterm, coordinator_id, cohort_members,
		   durability_policy_name, durability_quorum_type, durability_required_count, created_at)
		VALUES ($1, 0, 0, $2, '{}', $3, $4, $5, now())`,
		[]byte("0"), topoclient.ClusterIDString(bootstrapID), policy.PolicyName, policy.QuorumType.String(), int64(policy.RequiredCount)); err != nil {
		return mterrors.Wrap(err, "failed to initialize current_rule")
	}

	// Each row records a cluster state change (promotion, cohort membership, durability policy).
	// The composite primary key (coordinator_term, leader_subterm) uniquely identifies each rule;
	// leader_subterm is assigned by the application as MAX(leader_subterm)+1 within a coordinator_term.
	if _, err := rs.queryService.Query(execCtx, `CREATE TABLE multigres.rule_history (
		coordinator_term          BIGINT NOT NULL,
		leader_subterm            BIGINT NOT NULL,
		event_type                TEXT NOT NULL,
		leader_id                 TEXT,
		coordinator_id            TEXT NOT NULL,
		wal_position              TEXT,
		accepted_members          TEXT[],
		reason                    TEXT NOT NULL,
		cohort_members            TEXT[] NOT NULL,
		durability_policy_name    TEXT NOT NULL,
		durability_quorum_type    TEXT NOT NULL,
		durability_required_count INT NOT NULL,
		operation                 TEXT,
		created_at                TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (coordinator_term, leader_subterm)
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create rule_history table")
	}

	return nil
}

// ----------------------------------------------------------------------------
// Read/Write Operations
// ----------------------------------------------------------------------------

// errRuleConflict is returned by UpdateRule when a compare-and-swap check fails:
// either WithPreviousRule's explicit version check did not match, or a concurrent
// write changed the rule between our read and our write.
var errRuleConflict = errors.New("rule conflict: current rule version changed since last read")

// ----------------------------------------------------------------------------
// Shared row reader
// ----------------------------------------------------------------------------

// readCurrentRule reads the current_rule row for the default shard. If forUpdate
// is true, appends FOR UPDATE NOWAIT to acquire a row-level lock; the NOWAIT
// clause causes an immediate error if the row is already locked rather than
// blocking, so callers never wait indefinitely. On a standby this must be false
// since the node is read-only. Returns an error when the sentinel row is missing
// (tables not initialized) or when postgres is unreachable.
//
// The caller is responsible for adding an appropriate context timeout.
func (rs *ruleStore) readCurrentRule(ctx context.Context, forUpdate bool) (*clustermetadatapb.PoolerPosition, error) {
	suffix := ""
	if forUpdate {
		suffix = " FOR UPDATE NOWAIT"
	}
	result, err := rs.queryService.QueryArgs(ctx, `
		SELECT coordinator_term, leader_subterm, leader_id, coordinator_id, cohort_members,
		       durability_policy_name, durability_quorum_type, durability_required_count,
		       created_at,
		       CASE
		         WHEN pg_is_in_recovery()
		           THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), '0/0'::pg_lsn)
		         ELSE pg_current_wal_lsn()
		       END::text AS current_lsn
		FROM multigres.current_rule
		WHERE shard_id = $1`+suffix, []byte("0"))
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read current_rule")
	}
	if len(result.Rows) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "current_rule initial row missing for shard 0: tables may not be initialized")
	}

	var coordinatorTerm, leaderSubterm int64
	var leaderIDStr, coordinatorIDStr *string
	var cohortNames []string
	var durabilityPolicyName, durabilityQuorumType string
	var durabilityRequiredCount int64
	var createdAt time.Time
	var lsn string
	if err := executor.ScanRow(result.Rows[0],
		&coordinatorTerm,
		&leaderSubterm,
		&leaderIDStr,
		&coordinatorIDStr,
		&cohortNames,
		&durabilityPolicyName,
		&durabilityQuorumType,
		&durabilityRequiredCount,
		&createdAt,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan current_rule")
	}

	var coordinatorIDStrVal string
	if coordinatorIDStr != nil {
		coordinatorIDStrVal = *coordinatorIDStr
	}
	pos, err := buildPoolerPosition(
		coordinatorTerm, leaderSubterm,
		leaderIDStr, coordinatorIDStrVal, cohortNames,
		durabilityPolicyName, durabilityQuorumType, durabilityRequiredCount,
		createdAt,
		lsn,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse current_rule")
	}
	return pos, nil
}

// ObservePosition reads the current rule and WAL LSN from postgres and returns
// the observed position. Always returns a non-nil position when err is nil.
//
// Returns an error if postgres is unreachable or if the current_rule sentinel
// row is missing (which indicates the tables are not initialized).
//
// TODO: a position observation is how a node first learns its committed rule was
// superseded by a higher one — a consensus change that flips the routing role
// with no revoke/promote to carry it. Ideally observing such a change would
// trigger a state recalc here too (like the revoke path does), so the routing
// role converges immediately instead of on the monitor's next drift tick. The
// obstacle is layering + locking: ObservePosition is a hot read path called
// without the action lock, and it lives below the manager's StateManager, so it
// cannot call Recalc directly. Figure out if/how to surface "the observed rule
// moved" to the StateManager (a lock-free recalc, or an observer the manager
// wires up) without coupling this layer to serving state.
func (rs *ruleStore) ObservePosition(ctx context.Context) (*clustermetadatapb.PoolerPosition, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeouts.RuleReadTimeout)
	defer cancel()
	pos, err := rs.readCurrentRule(queryCtx, false)
	if err != nil {
		return nil, err
	}
	rs.cacheRuleObservation(pos)
	return pos, nil
}

// readCurrentRuleLocked reads the current_rule row and returns a lockedCtx that
// carries proof that prior rule writes from any previous action lock holder have
// been drained (WithPriorRuleWritesDrained). The timeout is managed internally;
// lockedCtx is derived from ctx (not the internal timeout context) and remains
// valid for subsequent operations after the read completes.
//
// When inRecovery is false (primary path): uses FOR UPDATE NOWAIT, which
// succeeds immediately if no other transaction holds the row lock, or fails
// fast if the row is locked. Callers that receive an error should retry.
// When inRecovery is true (standby/promotion path): omits FOR UPDATE since the
// node is read-only and no concurrent writes to current_rule are possible.
func (rs *ruleStore) readCurrentRuleLocked(ctx context.Context, inRecovery bool) (*clustermetadatapb.PoolerPosition, context.Context, error) {
	readCtx, readCancel := context.WithTimeout(ctx, timeouts.RuleReadTimeout)
	defer readCancel()
	pos, err := rs.readCurrentRule(readCtx, !inRecovery)
	if err != nil {
		return nil, nil, err
	}
	lockedCtx := WithPriorRuleWritesDrained(ctx)
	return pos, lockedCtx, nil
}

// UpdateRule writes a new rule to current_rule and rule_history.
//
// The leader_subterm is assigned as:
//   - 0 if termNumber is greater than the current coordinator_term (new term)
//   - current leader_subterm + 1 if termNumber equals the current coordinator_term
//
// Fields not set via the builder (leaderID, cohortMembers, durabilityPolicy) retain
// their current values from current_rule.
//
// GUC transition: the outgoing ("both") policy is applied before the WAL write so that
// writes issued during the transition satisfy both the old and new replication requirements.
// The incoming (new) policy is applied after the write commits. On a promotion the outgoing
// GUC is applied while still a standby (before pg_promote); on a primary-side rule change
// it is applied immediately before the write CTE.
//
// Returns the node's position (rule + WAL LSN) at the time of the write,
// or nil if force mode skipped the write.
//
// This operation uses the remote-operation-timeout and will fail if it cannot
// complete within that time. A timeout typically indicates that synchronous
// replication is not functioning.
func (rs *ruleStore) UpdateRule(ctx context.Context, update *RuleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error) {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("UpdateRule: %w", err)
	}

	if update.force {
		// Force mode skips history recording entirely. Force operations are emergency
		// operations that must configure replication GUCs regardless. The write would
		// block on sync replication with unreachable standbys, consuming the parent
		// context's deadline and causing subsequent GUC changes to fail.
		rs.logger.InfoContext(ctx, "Skipping rule update in force mode",
			"coordinator_term", update.termNumber,
			"event_type", update.eventType)
		return nil, nil
	}

	// Identity and timing must be supplied by the caller. ClusterIDString(nil)
	// silently returns "" and the coordinator_id column is TEXT NOT NULL (not
	// rejected by postgres because "" != NULL), so without these checks a nil
	// coordinatorID would write a corrupt row instead of failing. createdAt
	// has the same property: a zero time.Time inserts as a zero timestamp.
	// Failing fast here also avoids leaving partial work in the caller, which
	// often touches postgres GUCs around this write.
	if update.coordinatorID == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"UpdateRule requires a non-nil coordinator_id")
	}
	if update.createdAt.IsZero() {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"UpdateRule requires a non-zero created_at")
	}

	isPromotion := update.promotionHook != nil

	// Read the current rule to establish the CAS baseline and drain any in-flight
	// rule writes from a previous action lock holder.
	current, lockedCtx, err := rs.readCurrentRuleLocked(ctx, isPromotion)
	if err != nil {
		return nil, err
	}

	currentRule := current.GetRule()
	currentTerm := currentRule.GetRuleNumber().GetCoordinatorTerm()
	currentSubterm := currentRule.GetRuleNumber().GetLeaderSubterm()

	// Optional explicit CAS: verify the caller's expected version matches what we read.
	if update.previousRule != nil {
		if currentTerm != update.previousRule.coordinatorTerm || currentSubterm != update.previousRule.leaderSubterm {
			return nil, errRuleConflict
		}
	}

	// Compute the next leader_subterm.
	var nextSubterm int64
	if update.termNumber > currentTerm {
		nextSubterm = 0
	} else if update.termNumber < currentTerm {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"rule update rejected for term %d: current rule is at term %d",
			update.termNumber, currentTerm)
	} else {
		nextSubterm = currentSubterm + 1
	}

	// Resolve values to write: caller-supplied values take priority; nil retains existing.
	newLeader := currentRule.GetLeaderId()
	if update.leaderID != nil {
		newLeader = update.leaderID
	}
	newCohort := currentRule.GetCohortMembers()
	if update.cohortMembers != nil {
		newCohort = update.cohortMembers
	}
	newDP := currentRule.GetDurabilityPolicy()
	if update.durabilityPolicy != nil {
		dp := update.durabilityPolicy
		if dp.QuorumType == clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN || dp.RequiredCount <= 0 {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
				"durability policy has missing or invalid fields: quorum_type=%v required_count=%d",
				dp.QuorumType, dp.RequiredCount)
		}
		newDP = dp
	}

	// Validate that the new cohort can satisfy the new durability policy.
	if len(newCohort) > 0 {
		policy, err := consensus.NewPolicyFromProto(newDP)
		if err != nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "invalid durability policy: %v", err)
		}
		if err := policy.CheckAchievable(newCohort); err != nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort cannot achieve durability policy: %v", err)
		}
	}

	// Compute the GUC transition. The Both policy satisfies the old and new durability
	// requirements simultaneously and is applied before the WAL write. The Incoming
	// (new) policy is applied after the write commits.
	incomingPWC, err := consensus.NewPolicyWithCohort(newCohort, newDP)
	if err != nil {
		return nil, err
	}
	var transition *consensus.PolicyTransition
	if update.skipOutgoingQuorum {
		// Skip BuildPolicyTransition and apply the incoming cohort directly.
		// Used when the outgoing cohort is empty (bootstrap) or the coordinator
		// has already verified the transition is safe.
		transition = &consensus.PolicyTransition{Both: incomingPWC, Incoming: incomingPWC}
	} else {
		outgoingPWC, err := consensus.NewPolicyWithCohort(currentRule.GetCohortMembers(), currentRule.GetDurabilityPolicy())
		if err != nil {
			return nil, err
		}
		transition, err = consensus.BuildPolicyTransition(outgoingPWC, incomingPWC)
		if err != nil {
			return nil, fmt.Errorf("compute GUC transition: %w", err)
		}
	}

	// Convert values to SQL parameters.
	var newLeaderStr string
	if newLeader != nil {
		pid, err := NewReplicaID(newLeader)
		if err != nil {
			return nil, mterrors.Wrap(err, "invalid leader ID")
		}
		newLeaderStr = pid.appName
	}

	cohortPIDs, err := ToReplicaIDs(newCohort)
	if err != nil {
		return nil, mterrors.Wrap(err, "invalid cohort member ID")
	}
	newCohortParam := ReplicaIDsToAppNames(cohortPIDs)
	if newCohortParam == nil {
		newCohortParam = []string{}
	}

	var acceptedParam []string
	if len(update.acceptedMembers) > 0 {
		pids, err := ToReplicaIDs(update.acceptedMembers)
		if err != nil {
			return nil, mterrors.Wrap(err, "invalid accepted member ID")
		}
		acceptedParam = ReplicaIDsToAppNames(pids)
	}

	coordinatorIDStr := topoclient.ClusterIDString(update.coordinatorID)
	// newDP is always non-nil: UpdateRule falls back to the current rule's policy when
	// the caller omits WithDurabilityPolicy(), so these values are always present.
	dpName := newDP.PolicyName
	dpQuorumType := newDP.QuorumType.String()
	dpRequiredCount := int64(newDP.RequiredCount)

	// Apply the transition GUC before writing the rule. The transition (Both) policy
	// satisfies both old and new durability requirements simultaneously.
	// Promotion path: set GUC while still a standby, then call pg_promote().
	// Primary path: set GUC immediately before the write CTE.
	if isPromotion {
		if err := rs.syncStandby.SetPolicy(lockedCtx, transition.Both); err != nil {
			return nil, fmt.Errorf("pre-promote GUC: %w", err)
		}
		if err := update.promotionHook(lockedCtx); err != nil {
			return nil, fmt.Errorf("promotion hook: %w", err)
		}
	} else {
		if err := rs.syncStandby.SetPolicy(lockedCtx, transition.Both); err != nil {
			return nil, fmt.Errorf("pre-write GUC: %w", err)
		}
	}

	// Write the rule. This write blocks until a sync-standby WAL ack arrives.
	// For promotions the ack only arrives after the full SetPrimary round-trip
	// (Recruit clears all replication; standbys reconnect only after SetPrimary,
	// including optional pg_rewind). For primary-side cohort changes standbys are
	// already streaming and the ack is nearly immediate. RuleWriteTimeout covers
	// both cases.
	execCtx, cancel := context.WithTimeout(ctx, timeouts.RuleWriteTimeout)
	defer cancel()

	if isPromotion {
		var ackSpan trace.Span
		execCtx, ackSpan = telemetry.Tracer().Start(execCtx, "consensus/standby-ack",
			trace.WithAttributes(attribute.Bool("is_promotion", true)))
		defer ackSpan.End()
	}

	result, err := rs.queryService.QueryArgs(execCtx, `
		WITH
		  params AS (
		    -- Name all query parameters once so the rest of the CTE references them by name.
		    SELECT $1::bytea        AS shard_id,
		           $2::bigint       AS cas_term,
		           $3::bigint       AS cas_subterm,
		           $4::bigint       AS new_term,
		           $5::bigint       AS new_subterm,
		           NULLIF($6, '')   AS new_leader_id,
		           $7::text         AS new_coordinator_id,
		           $8::text[]       AS new_cohort,
		           $9::text         AS dp_name,
		           $10::text        AS dp_quorum_type,
		           $11::bigint      AS dp_required_count,
		           $12::timestamptz AS created_at,
		           $13::text        AS event_type,
		           NULLIF($14, '')  AS wal_position,
		           NULLIF($15, '')  AS operation,
		           $16::text        AS reason,
		           $17::text[]      AS accepted_members
		  ),
		  locked AS (
		    -- NOWAIT returns an error immediately if another transaction holds the row lock
		    -- rather than blocking; callers that see an error should retry.
		    -- CAS: only proceed if the rule hasn't changed since we read it above.
		    SELECT current_rule.shard_id
		    FROM multigres.current_rule, params
		    WHERE current_rule.shard_id = params.shard_id
		      AND coordinator_term      = params.cas_term
		      AND leader_subterm        = params.cas_subterm
		    FOR UPDATE NOWAIT
		  ),
		  updated AS (
		    UPDATE multigres.current_rule
		    SET coordinator_term          = params.new_term,
		        leader_subterm            = params.new_subterm,
		        leader_id                 = params.new_leader_id,
		        coordinator_id            = params.new_coordinator_id,
		        cohort_members            = params.new_cohort,
		        durability_policy_name    = params.dp_name,
		        durability_quorum_type    = params.dp_quorum_type,
		        durability_required_count = params.dp_required_count,
		        created_at                = params.created_at
		    FROM locked, params
		    WHERE current_rule.shard_id = params.shard_id
		    RETURNING coordinator_term, leader_subterm, leader_id, coordinator_id, cohort_members,
		              durability_policy_name, durability_quorum_type, durability_required_count,
		              params.created_at
		  ),
		  inserted AS (
		    INSERT INTO multigres.rule_history
		      (coordinator_term, leader_subterm, event_type, leader_id, coordinator_id,
		       wal_position, operation, reason, cohort_members, accepted_members,
		       durability_policy_name, durability_quorum_type, durability_required_count, created_at)
		    SELECT updated.coordinator_term, updated.leader_subterm,
		           params.event_type, updated.leader_id, updated.coordinator_id,
		           params.wal_position, params.operation, params.reason,
		           updated.cohort_members, params.accepted_members,
		           updated.durability_policy_name, updated.durability_quorum_type,
		           updated.durability_required_count, params.created_at
		    FROM updated, params
		    RETURNING coordinator_term
		  )
		-- Cross-joining inserted ensures a zero-row history insert (a bug) also returns zero
		-- rows here, causing the caller to surface an error rather than silently succeeding.
		SELECT updated.coordinator_term, updated.leader_subterm,
		       updated.leader_id, updated.coordinator_id, updated.cohort_members,
		       updated.durability_policy_name, updated.durability_quorum_type,
		       updated.durability_required_count,
		       updated.created_at,
		       CASE
		         WHEN pg_is_in_recovery()
		           THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), '0/0'::pg_lsn)
		         ELSE pg_current_wal_lsn()
		       END::text AS current_lsn
		FROM updated, inserted`,
		[]byte("0"),        // shard_id
		currentTerm,        // cas_term
		currentSubterm,     // cas_subterm
		update.termNumber,  // new_term
		nextSubterm,        // new_subterm
		newLeaderStr,       // new_leader_id (NULLIF: leader absent on sentinel row)
		coordinatorIDStr,   // new_coordinator_id
		newCohortParam,     // new_cohort
		dpName,             // dp_name
		dpQuorumType,       // dp_quorum_type
		dpRequiredCount,    // dp_required_count
		update.createdAt,   // created_at
		update.eventType,   // event_type
		update.walPosition, // wal_position (NULLIF: optional)
		update.operation,   // operation    (NULLIF: optional)
		update.reason,      // reason
		acceptedParam,      // accepted_members
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to write rule history record")
	}

	// Zero rows means either the CAS check failed (concurrent write between our read
	// and write) or the shard row is missing (should never happen after initialisation).
	if len(result.Rows) == 0 {
		return nil, errRuleConflict
	}

	var coordinatorTerm, leaderSubterm int64
	var leaderIDStr *string
	var coordinatorIDStrResult string
	var cohortNames []string
	var durabilityPolicyName, durabilityQuorumType string
	var durabilityRequiredCount int64
	var createdAt time.Time
	var lsn string
	if err := executor.ScanSingleRow(result,
		&coordinatorTerm,
		&leaderSubterm,
		&leaderIDStr,
		&coordinatorIDStrResult,
		&cohortNames,
		&durabilityPolicyName,
		&durabilityQuorumType,
		&durabilityRequiredCount,
		&createdAt,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan written rule position")
	}

	pos, err := buildPoolerPosition(
		coordinatorTerm, leaderSubterm,
		leaderIDStr, coordinatorIDStrResult, cohortNames,
		durabilityPolicyName, durabilityQuorumType, durabilityRequiredCount,
		createdAt,
		lsn,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse written rule position")
	}

	// Apply the incoming (new) GUC after the write commits.
	if err := rs.syncStandby.SetPolicy(lockedCtx, transition.Incoming); err != nil {
		return nil, fmt.Errorf("post-write GUC: %w", err)
	}

	rs.cacheRuleObservation(pos)
	return pos, nil
}

// queryRuleHistory returns the most recent rule history records in descending
// order by (coordinator_term, leader_subterm). Returns at most limit records.
func (rs *ruleStore) queryRuleHistory(ctx context.Context, limit int) ([]ruleHistoryRecord, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeouts.RuleReadTimeout)
	defer cancel()

	result, err := rs.queryService.QueryArgs(queryCtx, `
		SELECT coordinator_term, leader_subterm, event_type, leader_id, coordinator_id,
		       wal_position, operation, reason, cohort_members, accepted_members,
		       durability_policy_name, durability_quorum_type, durability_required_count,
		       created_at
		FROM multigres.rule_history
		ORDER BY coordinator_term DESC, leader_subterm DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query rule_history")
	}

	records := make([]ruleHistoryRecord, 0, len(result.Rows))
	for _, row := range result.Rows {
		var rec ruleHistoryRecord
		var leaderIDStr *string
		var cohortNames, acceptedNames []string
		var durabilityRequiredCount int64
		if err := executor.ScanRow(row,
			&rec.CoordinatorTerm,
			&rec.LeaderSubterm,
			&rec.EventType,
			&leaderIDStr,
			&rec.CoordinatorID,
			&rec.WALPosition,
			&rec.Operation,
			&rec.Reason,
			&cohortNames,
			&acceptedNames,
			&rec.DurabilityPolicyName,
			&rec.DurabilityQuorumType,
			&durabilityRequiredCount,
			&rec.CreatedAt,
		); err != nil {
			return nil, mterrors.Wrap(err, "failed to parse rule_history row")
		}
		rec.DurabilityRequiredCount = int32(durabilityRequiredCount)
		if err := scanRuleHistoryRow(&rec, leaderIDStr, cohortNames, acceptedNames); err != nil {
			return nil, mterrors.Wrap(err, "failed to parse rule_history row")
		}
		records = append(records, rec)
	}
	return records, nil
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

// buildPoolerPosition constructs a *clustermetadatapb.PoolerPosition from raw DB column values.
// leaderIDStr and coordinatorIDStr are app-name formatted strings (e.g. "zone1_pooler-name").
// Durability fields are NOT NULL in the DB and are always populated in the returned position.
// createdAt is the coordinator-supplied CreationTime persisted with the rule.
func buildPoolerPosition(
	coordinatorTerm, leaderSubterm int64,
	leaderIDStr *string,
	coordinatorIDStr string,
	cohortNames []string,
	durabilityPolicyName, durabilityQuorumType string,
	durabilityRequiredCount int64,
	createdAt time.Time,
	lsn string,
) (*clustermetadatapb.PoolerPosition, error) {
	rule := &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{
			CoordinatorTerm: coordinatorTerm,
			LeaderSubterm:   leaderSubterm,
		},
	}

	if leaderIDStr != nil {
		id, err := ParseApplicationName(*leaderIDStr)
		if err != nil {
			return nil, mterrors.Wrapf(err, "failed to parse leader_id %q", *leaderIDStr)
		}
		rule.LeaderId = id
	}

	if coordinatorIDStr != "" {
		// Coordinator IDs are multiorch, not multipooler — ParseApplicationName
		// is pooler-specific, so decode the cell_name encoding directly.
		cell, name, err := topoclient.SplitClusterID(coordinatorIDStr)
		if err != nil {
			return nil, mterrors.Wrapf(err, "failed to parse coordinator_id %q", coordinatorIDStr)
		}
		rule.CoordinatorId = &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      cell,
			Name:      name,
		}
	}

	cohortIDs, err := appNamesToIDs(cohortNames)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse cohort_members")
	}
	rule.CohortMembers = cohortIDs

	v, ok := clustermetadatapb.QuorumType_value[durabilityQuorumType]
	if !ok {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "unknown quorum_type %q", durabilityQuorumType)
	}
	rule.DurabilityPolicy = &clustermetadatapb.DurabilityPolicy{
		PolicyName:    durabilityPolicyName,
		QuorumType:    clustermetadatapb.QuorumType(v),
		RequiredCount: int32(durabilityRequiredCount),
	}
	if !createdAt.IsZero() {
		rule.CreationTime = timestamppb.New(createdAt)
	}

	return &clustermetadatapb.PoolerPosition{
		Rule: rule,
		Lsn:  lsn,
	}, nil
}

// appNamesToIDs converts a slice of app-name formatted strings to proto IDs.
func appNamesToIDs(names []string) ([]*clustermetadatapb.ID, error) {
	ids := make([]*clustermetadatapb.ID, 0, len(names))
	for _, name := range names {
		id, err := ParseApplicationName(name)
		if err != nil {
			return nil, mterrors.Wrapf(err, "invalid ID %q", name)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ruleHistoryRecord represents a row from multigres.rule_history or multigres.current_rule.
type ruleHistoryRecord struct {
	CoordinatorTerm         int64
	LeaderSubterm           int64
	EventType               string
	LeaderID                *ReplicaID // nil if not set
	CoordinatorID           *string    // informational only; component type is not stored
	WALPosition             *string
	Operation               *string
	Reason                  string
	CohortMembers           []ReplicaID
	AcceptedMembers         []ReplicaID
	DurabilityPolicyName    string
	DurabilityQuorumType    string
	DurabilityRequiredCount int32
	CreatedAt               time.Time
}

// scanRuleHistoryRow scans string-typed DB columns into a ruleHistoryRecord,
// parsing leader_id, cohort_members, and accepted_members into poolerIDs.
// leaderIDStr, cohortNames, and acceptedNames are intermediary scan targets.
func scanRuleHistoryRow(rec *ruleHistoryRecord, leaderIDStr *string, cohortNames, acceptedNames []string) error {
	if leaderIDStr != nil {
		id, err := ParseApplicationName(*leaderIDStr)
		if err != nil {
			return mterrors.Wrapf(err, "failed to parse leader_id %q", *leaderIDStr)
		}
		p := ReplicaID{id: id, appName: *leaderIDStr}
		rec.LeaderID = &p
	}
	cohort, err := ParseReplicaIDStrings(cohortNames)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse cohort_members")
	}
	rec.CohortMembers = cohort

	accepted, err := ParseReplicaIDStrings(acceptedNames)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse accepted_members")
	}
	rec.AcceptedMembers = accepted
	return nil
}

// priorRuleWritesDrainedKey is a context key proving that any in-flight rule
// writes from a previous action lock holder have been resolved before
// SyncStandbyManager.SetPolicy is called. This is established in one of two ways:
//
//   - Primary path: a SELECT FOR UPDATE on current_rule blocks until any
//     in-progress transaction from the prior holder commits or rolls back, after
//     which our row lock prevents new writers from interposing.
//   - Recovery path (standby before pg_promote): the node is read-only, so no
//     concurrent writes to current_rule are possible.
//
// The action lock (checked separately via actionlock.AssertActionLockHeld) ensures no
// concurrent goroutine in this process can also hold this proof.
type priorRuleWritesDrainedKey struct{}

// WithPriorRuleWritesDrained returns a derived context carrying proof that any
// in-flight rule writes from the previous action lock holder have been resolved.
// Called by readCurrentRuleLocked; callers must not stamp the context themselves.
func WithPriorRuleWritesDrained(ctx context.Context) context.Context {
	return context.WithValue(ctx, priorRuleWritesDrainedKey{}, struct{}{})
}

// AssertPriorRuleWritesDrained returns an error if the context does not carry
// proof that prior rule writes have been drained. Called automatically via
// readCurrentRuleLocked; callers must not stamp the context themselves.
func AssertPriorRuleWritesDrained(ctx context.Context) error {
	if _, ok := ctx.Value(priorRuleWritesDrainedKey{}).(struct{}); !ok {
		return errors.New("SetPolicy requires prior rule writes to be drained (call readCurrentRuleLocked first)")
	}
	return nil
}
