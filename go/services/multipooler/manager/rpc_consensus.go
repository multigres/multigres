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
	"fmt"
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
	"github.com/multigres/multigres/go/services/multipooler/poolerserver"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// buildConsensusStatus constructs a ConsensusStatus from a pre-resolved revocation,
// position, and the highest-known RPC-told (rule, primary). Any argument may be
// nil; the corresponding field is left unset. Never performs I/O.
//
// The published HighestKnownRule reflects best knowledge from any source:
//   - rule: max of the observed position's rule and the rule from the most
//     recent SetTermPrimary/Propose RPC.
//   - primary: the contact info from the most recent SetTermPrimary/Propose, since
//     observePosition cannot carry it.
//
// Result is left nil only when neither source has any information.
func buildConsensusStatus(id *clustermetadatapb.ID, revocation *clustermetadatapb.TermRevocation, pos *clustermetadatapb.PoolerPosition, replicationPrimary *clustermetadatapb.ReplicationPrimary) *clustermetadatapb.ConsensusStatus {
	status := &clustermetadatapb.ConsensusStatus{Id: id}
	if revocation != nil {
		status.TermRevocation = revocation
	}
	if pos != nil {
		status.CurrentPosition = pos
	}
	if highest := buildStatusReplicationPrimary(pos, replicationPrimary); highest != nil {
		status.ReplicationPrimary = highest
	}
	return status
}

// buildStatusReplicationPrimary returns the HighestKnownRule to publish given the most
// recent observed position and the most recent rule+primary heard via RPC.
// See buildConsensusStatus for the merge semantics.
func buildStatusReplicationPrimary(pos *clustermetadatapb.PoolerPosition, replicationPrimary *clustermetadatapb.ReplicationPrimary) *clustermetadatapb.ReplicationPrimary {
	observedRule := pos.GetRule()
	rpcRule := replicationPrimary.GetRule()
	rpcPrimary := replicationPrimary.GetPrimary()
	if observedRule == nil && rpcRule == nil && rpcPrimary == nil {
		return nil
	}
	rule := observedRule
	if commonconsensus.CompareRuleNumbers(rpcRule.GetRuleNumber(), observedRule.GetRuleNumber()) > 0 {
		rule = rpcRule
	}
	return &clustermetadatapb.ReplicationPrimary{
		Rule:    rule,
		Primary: rpcPrimary,
	}
}

// getConsensusStatus builds a ConsensusStatus snapshot while holding the action lock.
// Callers must already hold the action lock (i.e. this is called from Recruit or
// executeRevoke). Uses a consistent disk read for the term and a fresh postgres query
// for the current position.
//
// Returns an error if postgres is unreachable, since a partial status (term revocation
// without current_position) could mislead callers about this pooler's rule position.
func (pm *MultiPoolerManager) getConsensusStatus(ctx context.Context) (*clustermetadatapb.ConsensusStatus, error) {
	revocation, err := pm.consensusState.GetRevocation(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read consensus term: %w", err)
	}

	pos, err := pm.rules.observePosition(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read current rule position: %w", err)
	}
	return buildConsensusStatus(pm.serviceID, revocation, pos, pm.consensusState.GetReplicationPrimary()), nil
}

// getCachedConsensusStatus builds a ConsensusStatus using the in-memory term cache and
// the ruleStore's cached position. Never queries postgres or disk.
//
// The action lock must be held by the caller, which prevents concurrent term updates.
// Returns nil if no position has been cached yet (i.e. observePosition or updateRule
// has never been called).
func (pm *MultiPoolerManager) getCachedConsensusStatus() (*clustermetadatapb.ConsensusStatus, error) {
	revocation, err := pm.consensusState.GetInconsistentRevocation()
	if err != nil {
		return nil, err
	}

	pos := pm.rules.cachedPosition()
	if pos == nil {
		return nil, nil
	}
	return buildConsensusStatus(pm.serviceID, revocation, pos, pm.consensusState.GetReplicationPrimary()), nil
}

// getInconsistentConsensusStatus builds a ConsensusStatus without holding the action lock.
// Like GetInconsistentTerm, it may observe a partially-updated state during a concurrent
// Recruit, so it is suitable for observability (StatusResponse, health monitors) but not
// for decisions that require a consistent view.
//
// Falls back to the ruleStore's cached position when postgres is unreachable, so
// that callers can still derive the last-known primary term (e.g. for stale-
// primary detection) after postgres has crashed.
func (pm *MultiPoolerManager) getInconsistentConsensusStatus(ctx context.Context) (*clustermetadatapb.ConsensusStatus, error) {
	revocation, err := pm.consensusState.GetInconsistentRevocation()
	if err != nil {
		return nil, err
	}

	pos, err := pm.rules.observePosition(ctx)
	if err != nil {
		// Postgres is unreachable — fall back to the last observed position
		// cached in memory. May be stale, but preserves visibility into the
		// most recent rule across postgres restarts and crashes.
		pm.logger.DebugContext(ctx, "observePosition failed; falling back to cached rule position", "error", err)
		pos = pm.rules.cachedPosition()
	}
	return buildConsensusStatus(pm.serviceID, revocation, pos, pm.consensusState.GetReplicationPrimary()), nil
}

// buildAvailabilityStatus returns the current AvailabilityStatus for this node.
// Leaders that have resigned publish a LeadershipStatus. Every pooler publishes
// its cohort eligibility, so the result is non-nil.
func (pm *MultiPoolerManager) buildAvailabilityStatus() *clustermetadatapb.AvailabilityStatus {
	return &clustermetadatapb.AvailabilityStatus{
		LeadershipStatus:        pm.buildLeadershipStatus(),
		CohortEligibilityStatus: pm.buildCohortEligibilityStatus(),
	}
}

// buildCohortEligibilityStatus returns the pooler's self-reported willingness
// to be a cohort member. Defaults to ELIGIBLE; downgraded to INELIGIBLE when
// the WAL receiver was manually stopped (StopReplication cleared
// primary_conninfo), so the coordinator does not try to re-include this node
// while the admin signal is in effect. setCohortEligibility (currently
// test-only) sets the base value the dynamic downgrade applies on top of.
func (pm *MultiPoolerManager) buildCohortEligibilityStatus() *clustermetadatapb.CohortEligibilityStatus {
	if pm.walReceiverManuallyStopped.Load() {
		return &clustermetadatapb.CohortEligibilityStatus{
			Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
		}
	}
	pm.mu.Lock()
	signal := pm.cohortEligibility
	pm.mu.Unlock()
	return &clustermetadatapb.CohortEligibilityStatus{Signal: signal}
}

// setCohortEligibility records this pooler's cohort eligibility. If the
// signal actually changed, an immediate health broadcast is pushed so the
// coordinator sees the new value without waiting for the next heartbeat —
// otherwise a transition INELIGIBLE → cohort removal could be delayed by up
// to a heartbeat interval. Currently test-only — there is no operator/admin
// RPC to flip it yet.
func (pm *MultiPoolerManager) setCohortEligibility(signal clustermetadatapb.CohortEligibilitySignal) {
	pm.mu.Lock()
	changed := pm.cohortEligibility != signal
	pm.cohortEligibility = signal
	pm.mu.Unlock()
	if changed {
		pm.broadcastHealth()
	}
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
func (pm *MultiPoolerManager) markPoolerActive(ctx context.Context) {
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

// buildLeadershipStatus returns the LeadershipStatus for this node. Non-nil only
// when resignedLeaderAtTerm is set (i.e. after a Recruit-driven emergency
// demotion or graceful shutdown of a leader). Nil means this node has not
// recently held or resigned from primary leadership.
func (pm *MultiPoolerManager) buildLeadershipStatus() *clustermetadatapb.LeadershipStatus {
	pm.mu.Lock()
	resignedTerm := pm.resignedLeaderAtTerm
	pm.mu.Unlock()

	if resignedTerm == 0 {
		return nil
	}

	return &clustermetadatapb.LeadershipStatus{
		LeaderTerm: resignedTerm,
		Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
	}
}

// setResignedLeaderAtTerm records that this node is requesting demotion as primary
// for the given term. The signal is included in subsequent StatusResponses so the
// coordinator can trigger an immediate election. Broadcasts to health stream
// subscribers when the value actually changes, so the coordinator sees the new
// signal without waiting for the next periodic snapshot.
// Requires the action lock (ctx must be an action-lock context).
func (pm *MultiPoolerManager) setResignedLeaderAtTerm(ctx context.Context, term int64) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	pm.mu.Lock()
	changed := pm.resignedLeaderAtTerm != term
	pm.resignedLeaderAtTerm = term
	pm.mu.Unlock()
	if changed {
		pm.broadcastHealth()
	}
	return nil
}

// clearResignedLeaderAtTerm clears the leadership demotion request. Called when
// this node is appointed as primary at a new term.
// Requires the action lock (ctx must be an action-lock context).
func (pm *MultiPoolerManager) clearResignedLeaderAtTerm(ctx context.Context) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	pm.mu.Lock()
	pm.resignedLeaderAtTerm = 0
	pm.mu.Unlock()
	return nil
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
func (pm *MultiPoolerManager) Recruit(ctx context.Context, req *consensusdatapb.RecruitRequest) (*consensusdatapb.RecruitResponse, error) {
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

	// State check — reject immediately if the node's committed WAL
	// rule or stored revocation already conflicts with this request.
	// Fails open on I/O error: a nil status passes ValidateRevocation safely.
	preStatus, _ := pm.getConsensusStatus(ctx)
	if err := commonconsensus.ValidateRevocation(preStatus, revocation); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, err.Error())
	}

	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to determine role for recruit")
	}

	termEvent := eventlog.TermBegin{NewTerm: revokedBelowTerm}
	eventlog.Emit(ctx, pm.logger, eventlog.Started, termEvent)

	// Stop replication participation.
	var savedConnInfo string // non-empty if standby; used for recovery on race failure
	{
		stopCtx, stopSpan := telemetry.Tracer().Start(ctx, "consensus/stop-replication")
		if isPrimary {
			pm.logger.InfoContext(stopCtx, "Recruiting primary: demoting and restarting as standby",
				"revoked_below_term", revokedBelowTerm)
			err = pm.emergencyDemoteLocked(stopCtx, revokedBelowTerm, recruitDrainTimeout)
		} else {
			// Save primary_conninfo so we can restore it if the position check fails.
			if savedConnInfo, err = pm.readPrimaryConnInfo(stopCtx); err != nil {
				pm.logger.WarnContext(stopCtx, "Failed to save primary_conninfo before recruit; recovery from race condition will not be possible", "error", err)
			}
			pm.logger.InfoContext(stopCtx, "Recruiting standby: pausing replication",
				"revoked_below_term", revokedBelowTerm)
			_, err = pm.pauseReplication(stopCtx,
				multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
				true /* wait */)
		}
		stopSpan.End()
		if err != nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", err)
			return nil, mterrors.Wrap(err, "failed to stop replication during recruit")
		}
	}

	if !isPrimary {
		stabilizeCtx, stabilizeSpan := telemetry.Tracer().Start(ctx, "consensus/stabilize")
		_, err = pm.waitForReplayStabilize(stabilizeCtx)
		stabilizeSpan.End()
		if err != nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", err)
			return nil, mterrors.Wrap(err, "failed waiting for replay to stabilize during recruit")
		}
	}

	// Re-check against the stable position and persist atomically.
	// AcceptRevocation combines the observed WAL position with the locked stored
	// revocation so ValidateRevocation sees authoritative state for both checks.
	stableStatus, err := pm.getConsensusStatus(ctx)
	if err != nil {
		eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", err)
		return nil, mterrors.Wrap(err, "failed to read stable status after stopping replication")
	}
	{
		acceptCtx, acceptSpan := telemetry.Tracer().Start(ctx, "consensus/accept-revocation")
		err = pm.consensusState.AcceptRevocation(acceptCtx, stableStatus, revocation)
		acceptSpan.End()
	}
	if err != nil {
		raceErr := mterrors.Wrap(err, "failed to persist term revocation")
		eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", raceErr)
		// Attempt to restore the node to its prior replication role.
		if isPrimary {
			// TODO: In theory it should be safe to re-promote the primary if this happens, but to keep things
			// simpler for now we just keep publishing the signal that this pooler resigned from its term as
			// leader to allow orch to do a failover.
		} else if savedConnInfo != "" {
			if restoreErr := pm.setPrimaryConnInfoAndReload(ctx, savedConnInfo); restoreErr != nil {
				pm.logger.ErrorContext(ctx, "Failed to restore primary_conninfo after recruit failure", "error", restoreErr)
			}
		}
		return nil, raceErr
	}

	eventlog.Emit(ctx, pm.logger, eventlog.Success, termEvent)
	pm.logger.InfoContext(ctx, "Recruit complete", "revoked_below_term", revokedBelowTerm)

	// Step 5: Return ConsensusStatus with the stable post-revoke position.
	// Uses the cached position warmed by the getConsensusStatus call in step 3.
	cs, err := pm.getCachedConsensusStatus()
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to build consensus status")
	}
	return &consensusdatapb.RecruitResponse{ConsensusStatus: cs}, nil
}

// recruitDrainTimeout is the drain window when recruiting a primary.
const recruitDrainTimeout = 5 * time.Second

// setPrimaryConnInfoAndReload sets primary_conninfo and reloads postgres config so the
// WAL receiver reconnects. Used to restore a standby's replication after a recruit failure.
func (pm *MultiPoolerManager) setPrimaryConnInfoAndReload(ctx context.Context, connInfo string) error {
	if err := pm.setPrimaryConnInfo(ctx, connInfo); err != nil {
		return err
	}
	return pm.reloadPostgresConfig(ctx)
}

// Propose handles a coordinator's proposal for a new shard rule. The pooler
// either promotes its postgres to primary (if designated leader) or configures
// replication toward the new primary (if replica).
//
// Propose requires prior recruitment: the stored term_revocation must match the
// proposal's term exactly. There is no implicit recruitment on Propose.
//
// Order of operations:
//  1. Validate fields and check that the stored revocation matches the proposal term.
//  2. Determine role by comparing this pooler's ID to proposal_leader.id.
//     3a. Leader: promote postgres, write the rule to the rule store, enable query service.
//     3b. Replica: configure primary_conninfo toward the new leader's postgres.
//  4. Return ConsensusStatus with the post-propose position.
func (pm *MultiPoolerManager) Propose(ctx context.Context, req *consensusdatapb.ProposeRequest) (*consensusdatapb.ProposeResponse, error) {
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "Propose")
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
	proposedRule := proposal.GetProposedRule()
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

	revokedBelowTerm := revocation.GetRevokedBelowTerm()
	coordinatorID := revocation.GetAcceptedCoordinatorId()

	pm.logger.InfoContext(ctx, "Propose received",
		"revoked_below_term", revokedBelowTerm,
		"coordinator_id", coordinatorID.GetName(),
		"leader_id", proposalLeader.GetId().GetName())

	// Step 1: Validate the term revocation.
	// ValidateRevocation ensures the WAL position is safe and the coordinator is consistent.
	// Fails open on I/O error (nil status passes safely).
	beforeStatus, err := pm.getConsensusStatus(ctx)
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, err.Error())
	}
	if err := commonconsensus.ValidateRevocation(beforeStatus, revocation); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, err.Error())
	}

	// Require an explicit Recruit() for this exact term before accepting a
	// Propose. Implicit recruitment (accepting the term here without a prior
	// Recruit call) could in principle be made safe, but it would need to
	// reproduce everything Recruit does: pausing replication on replicas and
	// restarting primaries in standby mode. We keep things simple for now by
	// requiring the two-phase protocol. ValidateRevocation already ensures
	// storedTerm <= revokedBelowTerm, so a mismatch here always means Recruit
	// was never called for this term.
	storedTerm := beforeStatus.GetTermRevocation().GetRevokedBelowTerm()
	if storedTerm != revokedBelowTerm {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"must Recruit before Propose: stored term %d != proposal term %d", storedTerm, revokedBelowTerm)
	}

	// Verify postgres is in the expected standby state: in recovery with no
	// primary_conninfo set. Together these prove that Recruit ran (which clears
	// primary_conninfo and goes into recovery mode) and that no prior Propose on
	// this node succeeded.
	inRecovery, err := pm.isInRecovery(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to verify standby state before propose")
	}
	if !inRecovery {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"postgres is not in standby mode; call Recruit before Propose")
	}
	connInfo, err := pm.readPrimaryConnInfo(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to verify primary_conninfo before propose")
	}
	if connInfo != "" {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"primary_conninfo is set (%q); call Recruit before Propose to stop replication", connInfo)
	}

	// Propose is only valid for the designated leader. Non-leaders should
	// receive the leader's identity via SetTermPrimary, which handles
	// replication setup without requiring a prior Recruit.
	if !proto.Equal(pm.serviceID, proposalLeader.GetId()) {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"Propose received on %s but proposal_leader is %s; non-leaders should be told via SetTermPrimary",
			pm.serviceID.GetName(), proposalLeader.GetId().GetName())
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
		reason = "propose"
	}
	ruleUpdate := newRuleUpdate(
		revokedBelowTerm,
		proposedRule.GetCoordinatorId(),
		"promotion",
		reason,
		proposedRule.GetCreationTime().AsTime()).
		withLeader(pm.serviceID).
		withCohort(proposedRule.GetCohortMembers()).
		withDurabilityPolicy(proposedRule.GetDurabilityPolicy()).
		withAcceptedMembers(req.GetAcceptedNodeIds()).
		withWALPosition(beforeStatus.GetCurrentPosition().GetLsn()).
		withPromotionHook(func(hookCtx context.Context) error {
			if err := pm.clearResignedLeaderAtTerm(ctx); err != nil {
				return mterrors.Wrap(err, "failed to clear resigned primary term")
			}
			return pm.promoteStandbyToPrimary(hookCtx, state)
		})
	if req.GetProposal().GetSkipOutgoingQuorum() {
		ruleUpdate.withSkipOutgoingQuorum()
	}
	if _, err = pm.DoUpdateRule(ctx, ruleUpdate); err != nil {
		return nil, mterrors.Wrap(err, "propose failed: could not write rule")
	}
	pm.healthStreamer.UpdateLeaderObservation(&poolerserver.LeaderObservation{
		LeaderID:   pm.serviceID,
		LeaderTerm: revokedBelowTerm,
	})
	// IMPORTANT: updateTopologyAfterPromotion must only be called after updateRule
	// succeeds. It advertises PRIMARY + SERVING to the gateway, opening write traffic.
	// updateRule is the durability gate: it waits for sync-standby acknowledgment.
	if err := pm.updateTopologyAfterPromotion(ctx, state); err != nil {
		pm.logger.WarnContext(ctx, "Failed to update topology after propose", "error", err)
	}

	// Record the (rule, primary) — this pooler IS now the primary. Stamping
	// the published ReplicationPrimary lets the health stream advertise the
	// new leadership immediately.
	pm.consensusState.RecordTermPrimary(proposedRule, proposalLeader)

	pm.logger.InfoContext(ctx, "Propose complete",
		"revoked_below_term", revokedBelowTerm)

	// Step 4: Return ConsensusStatus. The cache was warmed by getConsensusStatus in step 1
	// and updated by updateRule (leader path); the replica position is unchanged.
	cs, err := pm.getCachedConsensusStatus()
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to build consensus status")
	}
	return &consensusdatapb.ProposeResponse{ConsensusStatus: cs}, nil
}

// SetTermPrimary updates this pooler's replication settings to point at the supplied
// primary, but only if the supplied position is strictly higher than the
// pooler's own current position. If the supplied position is equal or behind,
// SetTermPrimary is a successful no-op — this makes it safe under retries and under
// out-of-order delivery from stale recovery rounds.
//
// When the receiver is a standby, SetTermPrimary rewrites primary_conninfo.
// When the receiver is currently acting as primary, the caller knows about a
// more recent rule with a different leader, so this node is a stale primary
// and gets demoted via pg_rewind.
//
// SetTermPrimary does not perform term validation — the rule comparison is the gate.
//
// TODO: when the rule comparison no-ops but WAL replay is paused
// (pg_is_wal_replay_paused), the caller's intent ("ensure this replica is
// pointed at the right primary") would be better served by also resuming
// replay. We don't do that today because StopReplication() is an explicit
// admin/test signal — auto-resuming would silently override it. Implement
// once StopReplication() can leave behind a "do not auto-resume" marker that
// SetTermPrimary can check.
func (pm *MultiPoolerManager) SetTermPrimary(ctx context.Context, req *consensusdatapb.SetTermPrimaryRequest) (*consensusdatapb.SetTermPrimaryResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	leader := req.GetLeader()
	rule := req.GetRule()
	if leader == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "leader is required")
	}
	if rule == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "rule is required")
	}
	// The rule's leader_id is authoritative; the leader field carries contact
	// info for that ID. A mismatch is a caller bug — we'd otherwise route
	// replication at an identity that doesn't match the consensus-elected one.
	ruleLeaderID := rule.GetLeaderId()
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
	// SetTermPrimary is the follower-side RPC. If the coordinator is telling this
	// pooler that it is the new primary, that's a routing mistake — the leader
	// path goes through Propose (which carries the full CoordinatorProposal and
	// the Recruit-established term revocation needed to safely promote).
	if proto.Equal(pm.serviceID, leaderID) {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"SetTermPrimary received on %s but leader is self; designated leaders are appointed via Propose",
			pm.serviceID.GetName())
	}

	ctx, err := pm.actionLock.Acquire(ctx, "SetTermPrimary")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Honor the revocation promise we made via Recruit. If the
	// incoming rule is revoked, ignore it: SetTermPrimary is a best-effort FYI and
	// the cohort will reconverge as it makes progress. Returning the cached
	// status keeps the response shape consistent with the "incoming rule
	// not higher" no-op below.
	revocation, err := pm.consensusState.GetRevocation(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read revocation while validating SetTermPrimary")
	}
	if commonconsensus.IsRuleRevoked(rule, revocation) {
		pm.logger.InfoContext(ctx, "SetTermPrimary: rule revoked, ignoring",
			"incoming_rule", rule.GetRuleNumber(),
			"revoked_below_term", revocation.GetRevokedBelowTerm(),
			"outgoing_rule", revocation.GetOutgoingRule())
		cs, err := pm.getCachedConsensusStatus()
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to build consensus status")
		}
		return &consensusdatapb.SetTermPrimaryResponse{ConsensusStatus: cs}, nil
	}

	// Record what we've been told, even if we don't end up applying the change.
	// Two consumers:
	//   - Health stream / multiorch: reads highest_known_rule to skip redundant
	//     SetTermPrimary RPCs during the window after an apply but before streaming
	//     replication has caught up enough for current_position to advance.
	//   - Pooler-side reconciliation: reads last-known-primary to retry
	//     ALTER SYSTEM SET primary_conninfo if this SetTermPrimary arrived while
	//     postgres was unavailable.
	pm.consensusState.RecordTermPrimary(rule, leader)

	// Observe the freshest view of our rule. SetTermPrimary is the staleness gate,
	// so we want authoritative state — not the cached snapshot.
	selfPos, err := pm.rules.observePosition(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to observe local position")
	}

	// Compare by RuleNumber only — LSN is intentionally not part of the gate.
	// See SetTermPrimaryRequest's proto comment for the reasoning.
	if commonconsensus.CompareRuleNumbers(rule.GetRuleNumber(), selfPos.GetRule().GetRuleNumber()) <= 0 {
		pm.logger.InfoContext(ctx, "SetTermPrimary: incoming rule not higher, no-op",
			"incoming_rule", rule.GetRuleNumber(),
			"self_rule", selfPos.GetRule().GetRuleNumber())
		cs, err := pm.getCachedConsensusStatus()
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to build consensus status")
		}
		return &consensusdatapb.SetTermPrimaryResponse{ConsensusStatus: cs}, nil
	}

	// Decide between "standby update" and "stale-primary demote" based on
	// actual postgres recovery state rather than topology — a node mid-promote
	// or mid-demote may have a topology label that lags reality.
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to check recovery status")
	}

	// A standby with rewindPending=true was emergency-demoted earlier and still
	// has divergent WAL relative to the new primary. Routing through
	// demoteStalePrimaryLocked runs pg_rewind, which clears rewindPending and
	// makes the node recruitable again. Without this, the lightweight standby
	// branch sets primary_conninfo but leaves the WAL divergent, and the next
	// Recruit refuses with "rewind pending after emergency demotion".
	needsRewind := pm.rewindPending.Load()

	// Reported to the gateway as the new leader's term. Not term validation —
	// the rule compare above is the gate. SetTermPrimary does not bump the local
	// revocation: revocations are authored by coordinators via Recruit, and
	// an SetTermPrimary is a notification, not a revoke.
	consensusTerm := rule.GetRuleNumber().GetCoordinatorTerm()

	if isPrimary || needsRewind {
		pm.logger.InfoContext(ctx, "SetTermPrimary: demoting stale primary",
			"new_leader", leader.GetId().GetName(),
			"incoming_rule", rule.GetRuleNumber(),
			"is_primary", isPrimary,
			"rewind_pending", needsRewind)
		if _, _, err := pm.demoteStalePrimaryLocked(ctx, leader, rule); err != nil {
			return nil, err
		}
	} else {
		pm.logger.InfoContext(ctx, "SetTermPrimary: updating standby primary_conninfo",
			"new_leader", leader.GetId().GetName(),
			"incoming_rule", rule.GetRuleNumber())
		if err := pm.setPrimaryConnInfoLocked(ctx, leader.GetHost(), port,
			true /* stopReplicationBefore */, true /* startReplicationAfter */); err != nil {
			return nil, err
		}
		// Ensure topology reflects REPLICA. This matters when postgres has
		// already been demoted (e.g. by a prior Recruit on this node or an
		// external pg_promote-then-restart) but the pooler's topology entry still
		// reads PRIMARY. Without this, the stale PRIMARY label causes the
		// stale-leader analyzer to keep firing forever. Propose has the same
		// step on its replica branch for the same reason.
		if err := pm.changeTypeLocked(ctx, clustermetadatapb.PoolerType_REPLICA); err != nil {
			pm.logger.WarnContext(ctx, "Failed to update pooler type to REPLICA after SetTermPrimary", "error", err)
		}
	}

	// Advertise the new leader to the health stream so the gateway can route
	// reads/writes against it. The stale-primary branch gets this for free
	// via demoteStalePrimaryLocked; the standby branch must do it explicitly.
	// TODO: LeaderObservation is redundant with the (rule, primary) tuple
	// already recorded in consensusState.replicationPrimary. Plan to make
	// RecordTermPrimary (or its successor) drive the health-stream
	// observation directly, so callers don't have to remember to do both.
	pm.healthStreamer.UpdateLeaderObservation(&poolerserver.LeaderObservation{
		LeaderID:   leader.GetId(),
		LeaderTerm: consensusTerm,
	})

	if err := pm.clearResignedLeaderAtTerm(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to clear resigned leader term after propose", "error", err)
	}

	pm.broadcastHealth()

	cs, err := pm.getConsensusStatus(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to build consensus status after SetTermPrimary")
	}
	return &consensusdatapb.SetTermPrimaryResponse{ConsensusStatus: cs}, nil
}

// ConsensusStatus returns the current status of this node for consensus
func (pm *MultiPoolerManager) ConsensusStatus(ctx context.Context, req *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error) {
	consensusStatus, statusErr := pm.getInconsistentConsensusStatus(ctx)
	if statusErr != nil {
		pm.logger.WarnContext(ctx, "Failed to build consensus status for StatusResponse", "error", statusErr)
	}

	return &consensusdatapb.StatusResponse{
		Id:                 pm.serviceID,
		ConsensusStatus:    consensusStatus,
		AvailabilityStatus: pm.buildAvailabilityStatus(),
	}, nil
}
