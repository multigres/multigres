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

package manager

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// buildAvailabilityStatus returns the current AvailabilityStatus for this node.
// Leaders that have resigned publish a LeadershipStatus. Every pooler publishes
// its cohort eligibility, so the result is non-nil.
func (pm *MultipoolerManager) buildAvailabilityStatus() *clustermetadatapb.AvailabilityStatus {
	return &clustermetadatapb.AvailabilityStatus{
		LeadershipStatus:        pm.consensusMgr.LeadershipStatus(),
		CohortEligibilityStatus: pm.buildCohortEligibilityStatus(),
		SuspectedDivergence:     pm.consensusMgr.SuspectedDivergence(),
	}
}

// buildCohortEligibilityStatus returns the pooler's self-reported willingness
// to be a cohort member. Defaults to ELIGIBLE; downgraded to INELIGIBLE when
// the WAL receiver was manually stopped (StopReplication cleared
// primary_conninfo), so the coordinator does not try to re-include this node
// while the admin signal is in effect. ConsensusManager.SetCohortEligibility
// sets the base value the dynamic downgrade applies on top of.
func (pm *MultipoolerManager) buildCohortEligibilityStatus() *clustermetadatapb.CohortEligibilityStatus {
	if pm.walReceiverManuallyStopped.Load() {
		return &clustermetadatapb.CohortEligibilityStatus{
			Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
		}
	}
	return &clustermetadatapb.CohortEligibilityStatus{Signal: pm.consensusMgr.CohortEligibility()}
}

// markPoolerActive performs the STARTING → ACTIVE transition once postgres
// has been observed running and responsive. The pgMonitor callback fires
// every 5 s; the early-return below is the only thing preventing that tick
// from issuing a topology publish every cycle. Removing it would turn
// lifecycle into a per-tick heartbeat at the cost of N publishes per 5 s
// across the cluster and would silently change the meaning of
// LifecycleStatus.Updated from "first time ACTIVE" to "last seen ACTIVE".
// Keep the guard.
//
// pm.multipooler is the single in-memory source of truth: the guard reads it,
// the mutation writes it, and topoPublisher.Notify hands the same pointer to
// the publisher goroutine which clones-then-writes. The action lock is held
// across the mutate-then-Notify sequence so lifecycle writes serialise
// against the consensus state machine (Promote/Demote/BeginTerm) the same
// way every other Notify caller does.
func (pm *MultipoolerManager) markPoolerActive(ctx context.Context) {
	// Cheap pre-check before acquiring the action lock: if the record
	// already reads ACTIVE, skip the lock acquisition entirely. The guard
	// inside record.Mutate's callback is the authoritative one.
	if pm.record.Snapshot().GetLifecycleStatus().GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE {
		return
	}

	lockCtx, err := pm.actionLock.Acquire(ctx, "markPoolerActive")
	if err != nil {
		pm.logger.WarnContext(ctx, "failed to acquire action lock for lifecycle ACTIVE; will retry next tick",
			"error", err)
		return
	}
	defer pm.actionLock.Release(lockCtx)

	if err := pm.record.Mutate(lockCtx, func(s *MutablePoolerRecordState) {
		if s.LifecycleStatus.GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE {
			return
		}
		s.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
			Status:  clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
			Reason:  "pooler active",
			Updated: timestamppb.Now(),
		}
	}); err != nil {
		pm.logger.WarnContext(lockCtx, "record.Mutate for ACTIVE lifecycle failed",
			"error", err)
	}
}

// Recruit handles a coordinator's request to stop replication participation and
// record a TermRevocation, returning the node's stable position afterward.
//
// Order of operations:
//  1. Sanity-check the current rule position against the revocation term.
//  2. Stop replication participation (primary: full demote + restart as standby;
//     standby: clear primary_conninfo + drain replay).
//  3. Read the stable position and re-check against the revocation term to catch
//     the rare race where a WAL rule entry arrived after the sanity check.
//     On failure: primary re-promotes; standby restores primary_conninfo.
//  4. Persist the TermRevocation only if the position is consistent.
//  5. Return ConsensusStatus with the stable post-revoke position.
func (pm *MultipoolerManager) Recruit(ctx context.Context, req *consensusdatapb.RecruitRequest) (*consensusdatapb.RecruitResponse, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "consensus/recruit")
	defer span.End()

	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "Recruit")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	revocation := req.GetTermRevocation()
	if revocation == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "term_revocation is required")
	}
	revokedBelowTerm := revocation.GetRevokedBelowTerm()
	coordinatorID := revocation.GetAcceptedCoordinatorId()

	pm.logger.InfoContext(ctx, "Recruit received",
		"revoked_below_term", revokedBelowTerm,
		"coordinator_id", coordinatorID.GetName())

	// State check — reject immediately if the node's committed WAL rule,
	// stored revocation, or recruit position floor already conflicts with
	// this request (the floor check matters because pg_rewind deletes back
	// to the last common checkpoint, potentially discarding acknowledged,
	// durably-stored transactions — see ConsensusStatus.recruit_blocked_until).
	// Fails open on I/O error: a nil status passes ValidateRevocation safely.
	preStatus, _ := pm.consensusMgr.ConsensusStatus(ctx)
	if err := commonconsensus.ValidateRevocation(preStatus, revocation); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, err.Error())
	}

	pgMode, err := pm.postgresMode(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to determine role for recruit")
	}

	termEvent := eventlog.ConsensusRecruit{
		Rule: commonconsensus.FormatRuleNumber(revocation.GetOutgoingRule()),
	}
	eventlog.Emit(ctx, pm.logger, eventlog.Started, termEvent)

	savedConnInfo, err := pm.stopReplicationForRecruit(ctx, pgMode, revokedBelowTerm)
	if err != nil {
		eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", err)
		return nil, mterrors.Wrap(err, "failed to stop replication during recruit")
	}

	if !pgMode.OutOfRecovery() {
		_, err = pm.waitForReplayComplete(ctx)
		if err != nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", err)
			return nil, mterrors.Wrap(err, "failed waiting for replay to complete during recruit")
		}
	}

	// Re-check against the stable position and persist atomically.
	// AcceptRevocation combines the observed WAL position with the locked stored
	// revocation so ValidateRevocation sees authoritative state for both checks.
	stableStatus, err := pm.consensusMgr.ConsensusStatus(ctx)
	if err != nil {
		eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", err)
		return nil, mterrors.Wrap(err, "failed to read stable status after stopping replication")
	}

	if err := pm.consensusMgr.Promises().AcceptRevocation(ctx, stableStatus, revocation); err != nil {
		raceErr := mterrors.Wrap(err, "failed to persist term revocation")
		eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", raceErr)
		pm.restoreReplicationAfterRecruitRace(ctx, pgMode, savedConnInfo)
		return nil, raceErr
	}

	eventlog.Emit(ctx, pm.logger, eventlog.Success, termEvent)
	pm.logger.InfoContext(ctx, "Recruit complete", "revoked_below_term", revokedBelowTerm)

	// The revocation persisted: this pooler's term is now revoked, so it is no
	// longer the active leader even if postgres has not yet left recovery. Recalc
	// re-derives the routing role (PRIMARY -> REPLICA) and re-fans it, clearing the
	// writable signal and self-leadership advertisement immediately rather than
	// waiting for the monitor's next drift tick. Kept next to the revoke that
	// causes it. Lock-safe: the action lock is held (acquired at the top of
	// Recruit) and AcceptRevocation has released the consensus lock, so reading the
	// consensus snapshot inside Recalc cannot deadlock.
	if err := pm.stateManager.Recalc(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Recruit: failed to recalc serving state after revocation", "error", err)
	}

	// Step 5: Return ConsensusStatus with the stable post-revoke position.
	// Uses the cached position warmed by the getConsensusStatus call in step 3.
	return &consensusdatapb.RecruitResponse{ConsensusStatus: pm.consensusMgr.CachedConsensusStatus()}, nil
}

// recruitDrainTimeout is the drain window when recruiting a primary.
const recruitDrainTimeout = 5 * time.Second

// stopReplicationForRecruit stops this pooler's replication participation as
// part of Recruit: a primary is demoted and restarted as standby; a standby
// pauses its receiver and confirms restore_command is fully stopped. Returns
// the primary_conninfo saved before stopping, for
// restoreReplicationAfterRecruitRace to use if the recruit loses a race.
func (pm *MultipoolerManager) stopReplicationForRecruit(ctx context.Context, pgMode pgmode.Mode, revokedBelowTerm int64) (savedConnInfo string, err error) {
	stopCtx, stopSpan := telemetry.Tracer().Start(ctx, "consensus/stop-replication")
	defer stopSpan.End()

	if pgMode.OutOfRecovery() {
		pm.logger.InfoContext(stopCtx, "Recruiting primary: demoting and restarting as standby",
			"revoked_below_term", revokedBelowTerm)
		return "", pm.demoteToStandbyLocked(stopCtx, revokedBelowTerm, recruitDrainTimeout)
	}

	// Save primary_conninfo so we can restore it if the position check fails.
	if savedConnInfo, err = pm.readPrimaryConnInfo(stopCtx); err != nil {
		pm.logger.WarnContext(stopCtx, "Failed to save primary_conninfo before recruit; recovery from race condition will not be possible", "error", err)
	}
	pm.logger.InfoContext(stopCtx, "Recruiting standby: pausing replication",
		"revoked_below_term", revokedBelowTerm)
	if _, err := pm.pauseReplication(stopCtx,
		multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
		true /* wait */); err != nil {
		return savedConnInfo, err
	}

	// This pooler is becoming a cohort member: from here on it must only ever
	// advance via streaming from the current leader, never the archive (an
	// observer catching up may have had it enabled). Clear restore_command
	// and confirm any in-flight invocation has actually stopped before the
	// caller's waitForReplayComplete is allowed to declare replay complete —
	// postgres cannot cancel an in-flight invocation on its own, so clearing
	// the config alone only stops future fetches.
	//
	// A failure here is a hard error, not a warning: consensus correctness
	// depends on a cohort member never trusting archive-sourced WAL, so
	// proceeding without confirming this would put that at risk.
	if err := pm.resetRestoreCommand(stopCtx); err != nil {
		return savedConnInfo, err
	}
	return savedConnInfo, pm.stopRestoreCommand(stopCtx)
}

// restoreReplicationAfterRecruitRace attempts to restore this pooler to its
// prior replication role after AcceptRevocation loses a race (another
// coordinator's revocation was already persisted). Best-effort: logs and
// continues on error rather than compounding the original failure.
func (pm *MultipoolerManager) restoreReplicationAfterRecruitRace(ctx context.Context, pgMode pgmode.Mode, savedConnInfo string) {
	if pgMode.OutOfRecovery() {
		// TODO: In theory it should be safe to re-promote the primary if this happens, but to keep things
		// simpler for now we just keep publishing the signal that this pooler resigned from its term as
		// leader to allow orch to do a failover.
		return
	}
	if savedConnInfo == "" {
		return
	}
	if err := pm.setPrimaryConnInfoAndReload(ctx, savedConnInfo); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to restore primary_conninfo after recruit failure", "error", err)
	}
}

// setPrimaryConnInfoAndReload sets primary_conninfo and reloads postgres config so the
// WAL receiver reconnects. Used to restore a standby's replication after a recruit failure.
func (pm *MultipoolerManager) setPrimaryConnInfoAndReload(ctx context.Context, connInfo string) error {
	if err := pm.setPrimaryConnInfo(ctx, connInfo); err != nil {
		return err
	}
	return pm.reloadPostgresConfig(ctx)
}

// Promote handles a coordinator's proposal for a new shard rule. The pooler
// either promotes its postgres to primary (if designated leader) or configures
// replication toward the new primary (if replica).
//
// Promote requires prior recruitment: the stored term_revocation must match the
// proposal's term exactly. There is no implicit recruitment on Promote.
//
// Order of operations:
//  1. Validate fields and check that the stored revocation matches the proposal term.
//  2. Determine role by comparing this pooler's ID to proposal_leader.id.
//     3a. Leader: promote postgres, write the rule to the rule store, enable query service.
//     3b. Replica: configure primary_conninfo toward the new leader's postgres.
//  4. Return ConsensusStatus with the post-promote position.
func (pm *MultipoolerManager) Promote(ctx context.Context, req *consensusdatapb.PromoteRequest) (*consensusdatapb.PromoteResponse, error) {
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "Promote")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	proposal := req.GetProposal()
	if proposal == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "proposal is required")
	}
	revocation := proposal.GetTermRevocation()
	if revocation == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "proposal.term_revocation is required")
	}
	proposalLeader := proposal.GetProposalLeader()
	if proposalLeader == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "proposal.proposal_leader is required")
	}
	if proposalLeader.GetId() == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "proposal.proposal_leader.id is required")
	}
	if proposalLeader.GetPostgresPort() == 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "proposal.proposal_leader.postgres_port is required")
	}
	proposedRule := proposal.GetProposedTransition().GetProposal()
	if proposedRule == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "proposal.proposed_rule is required")
	}
	// Identity and timing for the installed rule come from the proposed
	// rule itself, not the revocation. The revocation's
	// accepted_coordinator_id identifies who ran the recruit round; the
	// rule's coordinator_id identifies the coordinator-of-record for this
	// rule change. They are usually the same orch but the proposal is the
	// authoritative source — falling back to time.Now() or the revocation
	// would silently rewrite the caller's intent.
	if proposedRule.GetCoordinatorId() == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"proposal.proposed_rule.coordinator_id is required")
	}
	if proposedRule.GetCreationTime() == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"proposal.proposed_rule.creation_time is required")
	}
	// Promote is only valid for the designated leader. Non-leaders should
	// receive the leader's identity via SetPrimary, which handles
	// replication setup without requiring a prior Recruit.
	if !proto.Equal(pm.serviceID, proposalLeader.GetId()) {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"Promote received on %s but proposal_leader is %s; non-leaders should be told via SetPrimary",
			pm.serviceID.GetName(), proposalLeader.GetId().GetName())
	}

	promoteEvent := eventlog.ConsensusPromote{
		Rule: commonconsensus.FormatRuleNumber(proposedRule.GetRuleNumber()),
	}
	eventlog.Emit(ctx, pm.logger, eventlog.Started, promoteEvent)

	resp, err := pm.promoteLocked(ctx, req)
	if err != nil {
		eventlog.Emit(ctx, pm.logger, eventlog.Failed, promoteEvent, "error", err)
	} else {
		eventlog.Emit(ctx, pm.logger, eventlog.Success, promoteEvent)
	}
	return resp, err
}

func (pm *MultipoolerManager) promoteLocked(ctx context.Context, req *consensusdatapb.PromoteRequest) (*consensusdatapb.PromoteResponse, error) {
	proposal := req.GetProposal()
	revocation := proposal.GetTermRevocation()
	proposalLeader := proposal.GetProposalLeader()
	proposedRule := proposal.GetProposedTransition().GetProposal()

	revokedBelowTerm := revocation.GetRevokedBelowTerm()
	coordinatorID := revocation.GetAcceptedCoordinatorId()

	pm.logger.InfoContext(ctx, "Promote received",
		"rule", commonconsensus.FormatRuleNumber(proposedRule.GetRuleNumber()),
		"revoked_below_term", revokedBelowTerm,
		"coordinator_id", coordinatorID.GetName(),
		"leader_id", proposalLeader.GetId().GetName())

	// Step 1: Validate the term revocation.
	// ValidateRevocation ensures the WAL position is safe and the coordinator is consistent.
	// Fails open on I/O error (nil status passes safely).
	beforeStatus, err := pm.consensusMgr.ConsensusStatus(ctx)
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, err.Error())
	}
	if err := commonconsensus.ValidateRevocation(beforeStatus, revocation); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, err.Error())
	}

	// Require an explicit Recruit() for this exact term before accepting a
	// Promote. Implicit recruitment (accepting the term here without a prior
	// Recruit call) could in principle be made safe, but it would need to
	// reproduce everything Recruit does: pausing replication on replicas and
	// restarting primaries in standby mode. We keep things simple for now by
	// requiring the two-phase protocol. ValidateRevocation already ensures
	// storedTerm <= revokedBelowTerm, so a mismatch here always means Recruit
	// was never called for this term.
	storedTerm := beforeStatus.GetTermRevocation().GetRevokedBelowTerm()
	if storedTerm != revokedBelowTerm {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"must Recruit before Promote: stored term %d != proposal term %d", storedTerm, revokedBelowTerm)
	}

	// The revocation must revoke ALL terms below the rule being established, so the
	// promoted rule is unambiguously the highest non-revoked committed leader.
	// ValidateRevocation only checks the revocation against the outgoing/recorded
	// rule; it never sees the proposed rule. A proposal that pairs a rule at
	// coordinator term T with a revocation from an older recruitment
	// (revoked_below_term M < T) would leave terms [M, T) non-revoked and able to
	// compete, so require revoked_below_term to equal the new rule's term exactly.
	ruleTerm := proposedRule.GetRuleNumber().GetCoordinatorTerm()
	if revokedBelowTerm != ruleTerm {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"revocation revoked_below_term %d must equal the promoted rule's coordinator term %d: "+
				"the revocation must revoke all rules below the new term", revokedBelowTerm, ruleTerm)
	}

	// Verify postgres is in the expected standby state: in recovery with no
	// primary_conninfo set. Together these prove that Recruit ran (which clears
	// primary_conninfo and goes into recovery mode) and that no prior Promote on
	// this node succeeded.
	pgMode, err := pm.postgresMode(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to verify standby state before promote")
	}
	if pgMode.OutOfRecovery() {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"postgres is not in standby mode; call Recruit before Promote")
	}
	connInfo, err := pm.readPrimaryConnInfo(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to verify primary_conninfo before promote")
	}
	if connInfo != "" {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"primary_conninfo is set (%q); call Recruit before Promote to stop replication", connInfo)
	}

	// Leader path: promote postgres, write rule, enable query service.
	state, err := pm.checkPromotionState(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: If this proposal already exists, we're being asked to propagate
	// rather than make a new entry. We can make the rule store understand
	// propagation for that case.
	reason := req.GetReason()
	if reason == "" {
		reason = "promote"
	}
	ruleUpdate := consensus.NewRuleUpdate(
		revokedBelowTerm,
		proposedRule.GetCoordinatorId(),
		"promotion",
		reason,
		proposedRule.GetCreationTime().AsTime()).
		WithLeader(pm.serviceID).
		WithCohort(proposedRule.GetCohortMembers()).
		WithDurabilityPolicy(proposedRule.GetDurabilityPolicy()).
		WithAcceptedMembers(req.GetAcceptedNodeIds()).
		WithWALPosition(beforeStatus.GetCurrentPosition().GetLsn()).
		WithPromotionHook(func(hookCtx context.Context) error {
			if err := pm.consensusMgr.ClearResignedLeaderAtTerm(ctx); err != nil {
				return mterrors.Wrap(err, "failed to clear resigned primary term")
			}
			return pm.promoteStandbyToPrimary(hookCtx, state, proposal.GetProposedTransition())
		})
	if req.GetProposal().GetSkipOutgoingQuorum() {
		ruleUpdate.WithSkipOutgoingQuorum()
	}
	if _, err = pm.DoUpdateRule(ctx, ruleUpdate); err != nil {
		return nil, mterrors.Wrap(err, "promote failed: could not write rule")
	}
	// Advertise PRIMARY + SERVING now that the rule has committed — this opens the
	// gateway to write traffic, so it must run only after UpdateRule succeeds
	// (UpdateRule is the durability gate: it waits for sync-standby acknowledgment).
	//
	// Promotion has already waited for postgres to leave recovery AND for the new
	// rule to commit, so the consensus snapshot now names this pooler the active
	// committed leader: poking PostgresMode here derives routing role PRIMARY
	// and its leadership observation, starting the heartbeat writer / LISTEN and
	// opening the gateway immediately rather than waiting for the next monitor tick.
	//
	// The serving transition must run even when topology already reports PRIMARY:
	// on re-promotion of the same pooler at a higher term, demoteToStandbyLocked
	// left Type=PRIMARY but serving=DRAINING, and skipping this would strand the
	// pooler at PRIMARY/DRAINING and prevent the gateway buffer from draining.
	if err := pm.stateManager.Mutate(ctx, func(s *servingStateMutation) {
		s.PostgresMode = pgmode.Primary
		if s.ServingStatus == clustermetadatapb.PoolerServingStatus_DRAINING {
			s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
		}
	}); err != nil {
		pm.logger.WarnContext(ctx, "Failed to update serving state after promote", "error", err)
	}

	// Record the (rule, primary) — this pooler IS now the primary. Stamping
	// the published ReplicationPrimary lets the health stream advertise the
	// new leadership immediately.
	if err := pm.consensusMgr.RecordTermPrimary(ctx, &clustermetadatapb.ReplicationPrimary{
		// DoUpdateRule above already committed this rule, so it is now a
		// settled decision.
		Position: &clustermetadatapb.RulePosition{Decision: proposedRule},
		Primary:  proposalLeader,
	}); err != nil {
		pm.logger.ErrorContext(ctx, "failed to record replication primary after promote", "error", err)
	}

	pm.logger.InfoContext(ctx, "Promote complete",
		"rule", commonconsensus.FormatRuleNumber(proposedRule.GetRuleNumber()),
		"revoked_below_term", revokedBelowTerm)

	// Step 4: Return ConsensusStatus. The cache was warmed by getConsensusStatus in step 1
	// and updated by UpdateRule (leader path); the replica position is unchanged.
	return &consensusdatapb.PromoteResponse{ConsensusStatus: pm.consensusMgr.CachedConsensusStatus()}, nil
}

// SetPrimary updates this pooler's replication settings to point at the supplied
// primary, but only if the supplied position is strictly higher than the
// pooler's own current position. If the supplied position is equal or behind,
// SetPrimary is a successful no-op — this makes it safe under retries and under
// out-of-order delivery from stale recovery rounds.
//
// When the receiver is a standby, SetPrimary rewrites primary_conninfo.
// When the receiver is currently acting as primary, the caller knows about a
// more recent rule with a different leader, so this node is a stale primary
// and gets demoted via pg_rewind.
//
// SetPrimary does not perform term validation — the rule comparison is the gate.
//
// TODO: when the rule comparison no-ops but WAL replay is paused
// (pg_is_wal_replay_paused), the caller's intent ("ensure this replica is
// pointed at the right primary") would be better served by also resuming
// replay. We don't do that today because StopReplication() is an explicit
// admin/test signal — auto-resuming would silently override it. Implement
// once StopReplication() can leave behind a "do not auto-resume" marker that
// SetPrimary can check.
func (pm *MultipoolerManager) SetPrimary(ctx context.Context, req *consensusdatapb.SetPrimaryRequest) (*consensusdatapb.SetPrimaryResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	rp := req.GetReplicationPrimary()
	leader := rp.GetPrimary()
	undecidedRule := commonconsensus.PossiblyUndecidedRule(rp.GetPosition())
	if leader == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "replication_primary.primary is required")
	}
	if undecidedRule == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "replication_primary.position is required")
	}
	// The rule's leader_id is authoritative; the leader field carries contact
	// info for that ID. A mismatch is a caller bug — we'd otherwise route
	// replication at an identity that doesn't match the consensus-elected one.
	ruleLeaderID := undecidedRule.GetLeaderId()
	if ruleLeaderID == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "rule.leader_id is required")
	}
	leaderID := leader.GetId()
	if leaderID == nil ||
		leaderID.GetCell() != ruleLeaderID.GetCell() ||
		leaderID.GetName() != ruleLeaderID.GetName() {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"leader.id %q does not match rule.leader_id %q",
			leaderID.GetName(), ruleLeaderID.GetName())
	}
	if leader.GetHost() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "leader host is required")
	}
	port := leader.GetPostgresPort()
	if port == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"leader %s has no postgres port configured", leaderID.GetName())
	}
	// SetPrimary is the follower-side RPC. If the coordinator is telling this
	// pooler that it is the new primary, that's a routing mistake — the leader
	// path goes through Promote (which carries the full CoordinatorProposal and
	// the Recruit-established term revocation needed to safely promote).
	//
	// TODO: maybe treat "leader is self" as a no-op rather than an error?
	if proto.Equal(pm.serviceID, leaderID) {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"SetPrimary received on %s but leader is self; designated leaders are appointed via Promote",
			pm.serviceID.GetName())
	}

	ctx, err := pm.actionLock.Acquire(ctx, "SetPrimary")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Honor the revocation promise we made via Recruit. If the
	// incoming rule is revoked, ignore it: SetPrimary is a best-effort FYI and
	// the cohort will reconverge as it makes progress. Returning the cached
	// status keeps the response shape consistent with the "incoming rule
	// not higher" no-op below.
	revocation, err := pm.consensusMgr.Promises().GetRevocation(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read revocation while validating SetPrimary")
	}
	if commonconsensus.IsRuleRevoked(rp.GetPosition(), revocation) {
		pm.logger.InfoContext(ctx, "SetPrimary: rule revoked, ignoring",
			"incoming_rule", undecidedRule.GetRuleNumber(),
			"revoked_below_term", revocation.GetRevokedBelowTerm(),
			"outgoing_rule", revocation.GetOutgoingRule())
		return &consensusdatapb.SetPrimaryResponse{ConsensusStatus: pm.consensusMgr.CachedConsensusStatus()}, nil
	}

	// Record what we've been told, even if we don't end up applying the change.
	// Two consumers:
	//   - Health stream / multiorch: reads highest_known_rule to skip redundant
	//     SetPrimary RPCs during the window after an apply but before streaming
	//     replication has caught up enough for current_position to advance.
	//   - Pooler-side reconciliation: reads last-known-primary to retry
	//     ALTER SYSTEM SET primary_conninfo if this SetPrimary arrived while
	//     postgres was unavailable.
	if err := pm.consensusMgr.RecordTermPrimary(ctx, rp); err != nil {
		pm.logger.ErrorContext(ctx, "failed to record replication primary in SetPrimary", "error", err)
	}

	// Observe the freshest view of our rule. SetPrimary is the staleness gate,
	// so we want authoritative state — not the cached snapshot.
	selfPos, err := pm.consensusMgr.Rules().ObservePosition(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to observe local position")
	}

	// Compare by rule position only — LSN is intentionally not part of the
	// gate. See SetPrimaryRequest's proto comment for the reasoning.
	if commonconsensus.CompareRulePosition(rp.GetPosition(), selfPos.GetPosition()) <= 0 {
		pm.logger.InfoContext(ctx, "SetPrimary: incoming position not higher, no-op",
			"incoming_position", commonconsensus.FormatRulePosition(rp.GetPosition()),
			"self_position", commonconsensus.FormatRulePosition(selfPos.GetPosition()))
		// The position itself is a no-op, but primary_conninfo may have
		// drifted from what we're recorded as following (e.g. an operator or
		// test manually cleared it without changing consensus state). We
		// already hold the action lock and have everything needed to check —
		// fix it now rather than leaving it to MonitorPostgres's next
		// periodic tick, up to monitorRetryInterval away.
		//
		// Read the target back from the just-updated record rather than
		// trusting this call's own leader/port: RecordTermPrimary has its own
		// staleness comparison against whatever was already recorded, so this
		// request may not be what actually got persisted.
		if pgMode, err := pm.postgresMode(ctx); err != nil {
			pm.logger.WarnContext(ctx, "SetPrimary: failed to check recovery status before drift check; skipping", "error", err)
		} else if !pgMode.OutOfRecovery() && pm.primaryConnInfoDiffersFromRecorded(ctx) {
			pm.reconcilePrimaryConnInfoToRecorded(ctx, "SetPrimary")
		}
		return &consensusdatapb.SetPrimaryResponse{ConsensusStatus: pm.consensusMgr.CachedConsensusStatus()}, nil
	}

	// Both no-op gates passed — this call will actually reconfigure replication.
	setPrimaryEvent := eventlog.ConsensusSetPrimary{
		Rule: commonconsensus.FormatRuleNumber(undecidedRule.GetRuleNumber()),
	}
	eventlog.Emit(ctx, pm.logger, eventlog.Started, setPrimaryEvent)

	resp, err := pm.setPrimaryLocked(ctx, req, selfPos)
	if err != nil {
		eventlog.Emit(ctx, pm.logger, eventlog.Failed, setPrimaryEvent, "error", err)
	} else {
		eventlog.Emit(ctx, pm.logger, eventlog.Success, setPrimaryEvent)
	}
	return resp, err
}

func (pm *MultipoolerManager) setPrimaryLocked(ctx context.Context, req *consensusdatapb.SetPrimaryRequest, selfPos *clustermetadatapb.PoolerPosition) (*consensusdatapb.SetPrimaryResponse, error) {
	rp := req.GetReplicationPrimary()
	leader := rp.GetPrimary()
	incomingPosition := commonconsensus.FormatRulePosition(rp.GetPosition())
	port := leader.GetPostgresPort()

	// Decide between "standby update" and "stale-primary demote" based on
	// actual postgres recovery state rather than topology — a node mid-promote
	// or mid-demote may have a topology label that lags reality.
	pgMode, err := pm.postgresMode(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to check recovery status")
	}

	walRule := commonconsensus.PossiblyUndecidedRule(selfPos.GetPosition())
	if pgMode.OutOfRecovery() || commonconsensus.RuleNamesLeader(walRule, pm.serviceID) {
		if _, err := pm.consensusMgr.SetSuspectedDivergence(ctx, true); err != nil {
			pm.logger.ErrorContext(ctx, "failed to set suspected divergence in SetPrimary", "error", err)
		}
	}

	// If this pooler is being asked to serve a cohort member, it should not be accepting any WAL
	// from the restore_command / pgbackrest WAL archive, only from the indicated primary.
	if pm.consensusMgr.IsPotentialCohortMember(pm.serviceID) {
		if err := pm.resetRestoreCommand(ctx); err != nil {
			pm.logger.WarnContext(ctx, "SetPrimary: failed to clear restore_command for cohort member", "error", err)
		}
		if err := pm.stopRestoreCommand(ctx); err != nil {
			pm.logger.WarnContext(ctx, "SetPrimary: failed to confirm restore_command stopped for cohort member", "error", err)
		}
	}

	if pm.consensusMgr.SuspectedDivergence() {
		// Demoting a (likely diverged) stale primary restarts it as a standby of the
		// new leader, which requires a pg_rewind. Defer that until the leader is
		// rewind-ready — it has checkpointed onto its current timeline, relayed here
		// as replication_primary.rewind_ready. Restarting before then would FATAL on
		// this node's own un-replicated WAL and leave it stuck. Deferring leaves the
		// node running as-is (queryable, no downtime); the record was already updated
		// by RecordTermPrimary above, so orch's retry and the monitor's demote path
		// both re-attempt once the leader advertises rewind_ready.
		if !rp.GetRewindReady() {
			pm.logger.InfoContext(ctx, "SetPrimary: leader not yet rewind-ready; deferring stale-primary demote",
				"new_leader", leader.GetId().GetName(), "incoming_position", incomingPosition)
			return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
				"leader %s not yet rewind-ready; deferring demote of stale primary", leader.GetId().GetName())
		}
		pm.logger.InfoContext(ctx, "SetPrimary: stale primary, restarting as standby",
			"new_leader", leader.GetId().GetName(),
			"incoming_position", incomingPosition,
			"postgres_mode", pgMode)
		// restartAsStandbyLocked sets primary_conninfo to leader on success,
		// so we don't need a separate setPrimaryConnInfoLocked call here.
		if _, err := pm.restartAsStandbyLocked(ctx, leader.GetHost(), port); err != nil {
			return nil, err
		}
		// Clear synchronous_standby_names now that this node is a read-only standby.
		// Route it through the rule store's SyncStandbyManager (not a direct
		// ALTER SYSTEM) so the manager stays the sole writer and its cache stays
		// coherent.
		if err := pm.consensusMgr.Rules().ClearSyncStandby(ctx); err != nil {
			pm.logger.WarnContext(ctx, "Failed to clear synchronous replication after demotion", "error", err)
		}
	} else {
		pm.logger.InfoContext(ctx, "SetPrimary: updating standby primary_conninfo",
			"new_leader", leader.GetId().GetName(),
			"incoming_position", incomingPosition)
		// Already a standby with an active stream; pause replay, swap
		// conninfo, resume on the new primary.
		if err := pm.setPrimaryConnInfoLocked(ctx, leader.GetHost(), port,
			true /* stopReplicationBefore */, true /* startReplicationAfter */); err != nil {
			return nil, err
		}
	}

	// Ensure topology reflects REPLICA. This matters when postgres has
	// already been demoted (e.g. by a prior Recruit on this node or an
	// external pg_promote-then-restart) but the pooler's topology entry still
	// reads PRIMARY. Without this, the stale PRIMARY label causes the
	// stale-leader analyzer to keep firing forever. Promote has the same
	// step on its replica branch for the same reason.
	// A REPLICA pooler record carries no self leadership observation.
	// Sync physical primary-ness: we just restarted as a standby, so postgres is
	// no longer primary. That derives routing role REPLICA, clearing any stale
	// PRIMARY label/self-leadership (so the stale-leader analyzer stops firing) and
	// the published writable signal immediately rather than waiting a monitor
	// cycle. Serving status is owned by the lifecycle and the monitor's reconcile,
	// not by "here is your primary" bookkeeping.
	if err := pm.stateManager.Mutate(ctx, func(s *servingStateMutation) {
		s.PostgresMode = pgmode.InRecovery
	}); err != nil {
		pm.logger.WarnContext(ctx, "Failed to update postgres mode to InRecovery after SetPrimary", "error", err)
	}

	if err := pm.consensusMgr.ClearResignedLeaderAtTerm(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to clear resigned leader term after promote", "error", err)
	}

	pm.broadcastHealth()

	cs, err := pm.consensusMgr.ConsensusStatus(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to build consensus status after SetPrimary")
	}
	return &consensusdatapb.SetPrimaryResponse{ConsensusStatus: cs}, nil
}
