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
	"log/slog"
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
)

// BeginTerm handles coordinator requests during leader appointments.
// It consists of two phases:
//
// 1. Term Acceptance: Accept the new term based on consensus rules
//   - Term must be >= current term
//   - Cannot accept different coordinator for same term
//   - Atomically update term and accept candidate
//
// 2. Action Execution: Execute the specified action after term acceptance
//   - NO_ACTION: Do nothing
//   - REVOKE: Demote primary or pause standby replication to revoke old term
func (pm *MultiPoolerManager) BeginTerm(ctx context.Context, req *consensusdatapb.BeginTermRequest) (_ *consensusdatapb.BeginTermResponse, retErr error) {
	// Acquire the action lock to ensure only one consensus operation runs at a time
	// This prevents split-brain acceptance and ensures term updates are serialized
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "BeginTerm")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Log the action type for observability
	pm.logger.InfoContext(ctx, "BeginTerm received",
		"term", req.Term,
		"candidate_id", req.CandidateId.GetName(),
		"action", req.Action.String(),
		"shard_id", req.ShardId)

	// Validate action
	switch req.Action {
	case consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE:
		// Valid action
	case consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION:
		// Valid action
	case consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_UNSPECIFIED:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"action must be specified (cannot be UNSPECIFIED)")
	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unknown BeginTerm action type: %v", req.Action)
	}

	// ========================================================================
	// Term Acceptance (Consensus Rules)
	// ========================================================================

	// Get current term for response
	currentTerm, err := pm.consensusState.GetCurrentTermNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current term: %w", err)
	}

	// Atomically update term and accept candidate
	// This handles all consensus rules: term validation, duplicate check, etc.
	err = pm.consensusState.UpdateTermAndAcceptCandidate(ctx, req.Term, req.CandidateId)
	if err != nil {
		// Term not accepted - return rejection with consensus status so the coordinator
		// learns this pooler's current state even from a rejection.
		pm.logger.InfoContext(ctx, "Term not accepted",
			"request_term", req.Term,
			"current_term", currentTerm,
			"error", err)
		resp := &consensusdatapb.BeginTermResponse{
			Term:     currentTerm,
			Accepted: false,
			PoolerId: pm.serviceID.GetName(),
		}
		if cs, statusErr := pm.getConsensusStatus(ctx); statusErr != nil {
			pm.logger.WarnContext(ctx, "Failed to build consensus status for rejection response", "error", statusErr)
		} else {
			resp.ConsensusStatus = cs
		}
		return resp, nil
	}

	pm.logger.InfoContext(ctx, "Term accepted",
		"term", req.Term,
		"coordinator", req.CandidateId.GetName())

	// Determine revoked role before executing any action (needed for event)
	revokedRole := ""
	if req.Action == consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE {
		if primary, err := pm.isPrimary(ctx); err == nil {
			if primary {
				revokedRole = "primary"
			} else {
				revokedRole = "standby"
			}
		}
	}

	termEvent := eventlog.TermBegin{
		NewTerm:      req.Term,
		PreviousTerm: currentTerm,
		RevokedRole:  revokedRole,
	}
	eventlog.Emit(ctx, pm.logger, eventlog.Started, termEvent)
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Success, termEvent)
		} else {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", retErr)
		}
	}()

	response := &consensusdatapb.BeginTermResponse{
		Term:     req.Term,
		Accepted: true,
		PoolerId: pm.serviceID.GetName(),
	}

	// ========================================================================
	// Action Execution
	// ========================================================================

	switch req.Action {
	case consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION:
		if cs, statusErr := pm.getConsensusStatus(ctx); statusErr != nil {
			pm.logger.WarnContext(ctx, "Failed to build consensus status for NO_ACTION response", "error", statusErr)
		} else {
			response.ConsensusStatus = cs
		}
		return response, nil

	case consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE:
		if err := pm.executeRevoke(ctx, req.Term, response); err != nil {
			// Term was already accepted and persisted above, so we must return
			// the response with accepted=true AND the error. This tells the coordinator:
			// 1. The term was accepted (response.Accepted = true)
			// 2. The revoke action failed (error != nil)
			pm.logger.ErrorContext(ctx, "Term accepted but revoke action failed",
				"term", req.Term,
				"error", err)
			return response, mterrors.Wrap(err, "term accepted but revoke action failed")
		}
		return response, nil

	default:
		// Should never reach here due to validation above
		return response, nil
	}
}

// executeRevoke executes the REVOKE action by demoting primary or pausing standby replication.
// This is called after the term has been accepted.
func (pm *MultiPoolerManager) executeRevoke(ctx context.Context, term int64, response *consensusdatapb.BeginTermResponse) error {
	// CRITICAL: Must be able to reach Postgres to execute revoke
	if _, err := pm.query(ctx, "SELECT 1"); err != nil {
		return mterrors.Wrap(err, "postgres unhealthy, cannot execute revoke")
	}

	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		return mterrors.Wrap(err, "failed to determine role for revoke")
	}

	response.WalPosition = &consensusdatapb.WALPosition{
		Timestamp: timestamppb.Now(),
	}

	if isPrimary {
		// Revoke primary: demote
		// TODO: Implement graceful (non-emergency) demote for planned failovers.
		// This emergency demote path will remain for BeginTerm REVOKE actions.
		pm.logger.InfoContext(ctx, "Revoking primary", "term", term)
		drainTimeout := 5 * time.Second
		demoteResp, err := pm.emergencyDemoteLocked(ctx, term, drainTimeout)
		if err != nil {
			return mterrors.Wrap(err, "failed to demote primary during revoke")
		}
		response.WalPosition.CurrentLsn = demoteResp.LsnPosition
		pm.logger.InfoContext(ctx, "Primary demoted", "lsn", demoteResp.LsnPosition, "term", term)
	} else {
		// Revoke standby: stop receiver and wait for replay to catch up
		pm.logger.InfoContext(ctx, "Revoking standby", "term", term)

		// Stop WAL receiver and wait for it to fully disconnect
		_, err := pm.pauseReplication(
			ctx,
			multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			true /* wait */)
		if err != nil {
			return mterrors.Wrap(err, "failed to pause replication during revoke")
		}

		// Wait for replay to finish processing all WAL that is on disk
		status, err := pm.waitForReplayStabilize(ctx)
		if err != nil {
			return mterrors.Wrap(err, "failed waiting for replay to stabilize during revoke")
		}

		response.WalPosition.LastReceiveLsn = status.LastReceiveLsn
		response.WalPosition.LastReplayLsn = status.LastReplayLsn
		pm.logger.InfoContext(ctx, "Standby revoke complete",
			"term", term,
			"last_receive_lsn", status.LastReceiveLsn,
			"last_replay_lsn", status.LastReplayLsn)
	}

	// Always capture timeline ID after WAL positions are frozen.
	// Retained for observability only; does not affect candidate selection.
	timelineID, err := pm.getTimelineID(ctx)
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to get timeline ID during revoke; observability data will be incomplete",
			"term", term, "error", err)
	} else {
		response.WalPosition.TimelineId = timelineID
		pm.logger.InfoContext(ctx, "Captured timeline ID for observability",
			"term", term, "timeline_id", timelineID)
	}

	// Capture the highest consensus term replicated to this node, plus the cohort
	// that was active at that point. The coordinator uses leadership_term as
	// the primary criterion: a node that has seen a higher term has applied more
	// of the agreed WAL history (the history write uses RemoteOperationTimeout,
	// so sync standbys are guaranteed to have acknowledged it).
	//
	// observePosition also warms the ruleStore cache, allowing getCachedConsensusStatus
	// below to read the position without an additional postgres round-trip.
	if nodePosition, err := pm.rules.observePosition(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to get rule history during revoke; candidate selection may be suboptimal",
			"term", term, "error", err)
	} else if nodePosition != nil {
		response.WalPosition.LeadershipTerm = nodePosition.GetRule().GetRuleNumber().GetCoordinatorTerm()
		pids, pidErr := toPoolerIDs(nodePosition.GetRule().GetCohortMembers())
		if pidErr != nil {
			pm.logger.WarnContext(ctx, "Some cohort member IDs have invalid format; using approximate names for candidate selection",
				"term", term, "error", pidErr)
		}
		response.WalPosition.CohortMembers = poolerIDsToAppNames(pids)
		pm.logger.InfoContext(ctx, "Captured coordinator term for candidate selection",
			"term", term, "coordinator_term", nodePosition.GetRule().GetRuleNumber().GetCoordinatorTerm())
	}

	// Capture consensus status after WAL positions are frozen (post-revoke snapshot).
	// Uses the cached position warmed by observePosition above — no extra DB round-trip.
	cs, err := pm.getCachedConsensusStatus()
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to build cached consensus status", "error", err)
	}
	response.ConsensusStatus = cs

	return nil
}

// buildConsensusStatus constructs a ConsensusStatus from a pre-resolved revocation and position.
// Both arguments may be nil; in that case the corresponding fields in the returned status
// are left unset. Never performs I/O.
func buildConsensusStatus(id *clustermetadatapb.ID, revocation *clustermetadatapb.TermRevocation, pos *clustermetadatapb.PoolerPosition) *clustermetadatapb.ConsensusStatus {
	status := &clustermetadatapb.ConsensusStatus{Id: id}
	if revocation != nil {
		status.TermRevocation = revocation
	}
	if pos != nil {
		status.CurrentPosition = pos
	}
	return status
}

// getConsensusStatus builds a ConsensusStatus snapshot while holding the action lock.
// Callers must already hold the action lock (i.e. this is called from BeginTerm or
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
	return buildConsensusStatus(pm.serviceID, revocation, pos), nil
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
	return buildConsensusStatus(pm.serviceID, revocation, pos), nil
}

// getInconsistentConsensusStatus builds a ConsensusStatus without holding the action lock.
// Like GetInconsistentTerm, it may observe a partially-updated state during a concurrent
// BeginTerm, so it is suitable for observability (StatusResponse, health monitors) but not
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
	return buildConsensusStatus(pm.serviceID, revocation, pos), nil
}

// buildAvailabilityStatus returns the current AvailabilityStatus for this node.
// Leaders always publish a LeadershipStatus. Returns nil if no signals are set
// and no leadership context exists.
func (pm *MultiPoolerManager) buildAvailabilityStatus() *clustermetadatapb.AvailabilityStatus {
	ls := pm.buildLeadershipStatus()
	if ls == nil {
		return nil
	}
	return &clustermetadatapb.AvailabilityStatus{LeadershipStatus: ls}
}

// buildLeadershipStatus returns the LeadershipStatus for this node. Non-nil only
// when resignedPrimaryAtTerm is set (i.e. after a BeginTerm REVOKE). Nil means
// this node has not recently held or resigned from primary leadership.
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
// coordinator can trigger an immediate election.
// Requires the action lock (ctx must be an action-lock context).
func (pm *MultiPoolerManager) setResignedLeaderAtTerm(ctx context.Context, term int64) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	pm.mu.Lock()
	pm.resignedLeaderAtTerm = term
	pm.mu.Unlock()
	return nil
}

// clearResignedLeaderAtTerm clears the leadership demotion request. Called by
// coordinator-driven promotion (Promote) when this node is explicitly
// re-appointed as primary at a new term.
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

	// Refuse recruitment if a rewind is still pending from a prior emergency
	// demotion. The node's WAL is in an indeterminate state until RewindToSource
	// completes; allowing it to be recruited could elect a leader with divergent
	// or missing WAL.
	if pm.rewindPending.Load() {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"rewind pending after emergency demotion; call RewindToSource before Recruit")
	}

	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to determine role for recruit")
	}

	termEvent := eventlog.TermBegin{NewTerm: revokedBelowTerm}
	eventlog.Emit(ctx, pm.logger, eventlog.Started, termEvent)

	// Stop replication participation.
	var savedConnInfo string // non-empty if standby; used for recovery on race failure
	if isPrimary {
		pm.logger.InfoContext(ctx, "Recruiting primary: demoting and restarting as standby",
			"revoked_below_term", revokedBelowTerm)
		if _, err := pm.emergencyDemoteLocked(ctx, revokedBelowTerm, recruitDrainTimeout); err != nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", err)
			return nil, mterrors.Wrap(err, "failed to demote primary during recruit")
		}
	} else {
		// Save primary_conninfo so we can restore it if the position check fails.
		if savedConnInfo, err = pm.readPrimaryConnInfo(ctx); err != nil {
			pm.logger.WarnContext(ctx, "Failed to save primary_conninfo before recruit; recovery from race condition will not be possible", "error", err)
		}
		pm.logger.InfoContext(ctx, "Recruiting standby: pausing replication",
			"revoked_below_term", revokedBelowTerm)
		if _, err := pm.pauseReplication(ctx,
			multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			true /* wait */); err != nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", err)
			return nil, mterrors.Wrap(err, "failed to pause replication during recruit")
		}
		if _, err := pm.waitForReplayStabilize(ctx); err != nil {
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
	if err := pm.consensusState.AcceptRevocation(ctx, stableStatus, revocation); err != nil {
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

	revokedBelowTerm := revocation.GetRevokedBelowTerm()
	coordinatorID := revocation.GetAcceptedCoordinatorId()

	pm.logger.InfoContext(ctx, "Propose received",
		"revoked_below_term", revokedBelowTerm,
		"coordinator_id", coordinatorID.GetName(),
		"leader_id", proposalLeader.GetId().GetName())

	// Step 1: Validate the term revocation.
	// ValidateRevocation ensures the WAL position is safe and the coordinator is consistent.
	// Fails open on I/O error (nil status passes safely).
	currentStatus, err := pm.getConsensusStatus(ctx)
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, err.Error())
	}
	if err := commonconsensus.ValidateRevocation(currentStatus, revocation); err != nil {
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
	storedTerm := currentStatus.GetTermRevocation().GetRevokedBelowTerm()
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

	// Step 2: Act based on role.
	isLeader := proto.Equal(pm.serviceID, proposalLeader.GetId())

	if isLeader {
		// Step 3a: Leader — promote postgres, write rule, enable query service.
		state, err := pm.checkPromotionState(ctx, nil)
		if err != nil {
			return nil, err
		}
		// Pre-configure synchronous_standby_names before pg_promote() so the primary
		// enforces sync replication from the moment it starts accepting connections.
		// Both failures are fatal: a misconfigured or partially applied GUC would let
		// the primary accept writes without the required sync acknowledgment, risking
		// data loss.
		// TODO: Eventually updateRule() should own this GUC update so that the ordering
		// of WAL writes and GUC changes is managed in one place, ensuring the durability
		// configuration is always consistent with what is recorded in the rule history.
		if proposedRule.GetDurabilityPolicy() != nil {
			syncCfg, err := syncConfigFromProposedRule(pm.logger, proposedRule, pm.serviceID)
			if err != nil {
				return nil, mterrors.Wrap(err, "cannot derive sync config from proposed rule")
			}
			if err := pm.applyGUCsForSyncReplication(ctx, syncCfg); err != nil {
				return nil, mterrors.Wrap(err, "failed to pre-configure sync replication before promote")
			}
		}
		// ---- BEGIN CRITICAL ORDERING SECTION ----------------------------------------
		// postgres becomes a writable primary after pg_promote(), but this pooler's
		// type remains REPLICA until updateTopologyAfterPromotion() below. While type
		// is REPLICA, the query server returns MTF01 for any PRIMARY (write) request,
		// causing the gateway to buffer — so no client write transactions can be served.
		//
		// DO NOT call updateTopologyAfterPromotion() before updateRule() succeeds.
		// The rule commit is the durability gate: it waits for sync-standby
		// acknowledgment, proving the new primary position is replicated. Only after
		// that gate closes is it safe to advertise PRIMARY + SERVING to the gateway.
		if err := pm.promoteStandbyToPrimary(ctx, state); err != nil {
			return nil, err
		}
		if err := pm.clearResignedLeaderAtTerm(ctx); err != nil {
			return nil, mterrors.Wrap(err, "failed to clear resigned primary term")
		}
		finalLSN, err := pm.getPrimaryLSN(ctx)
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
		if _, err = pm.rules.updateRule(ctx, newRuleUpdate(
			revokedBelowTerm,
			coordinatorID,
			"promotion",
			reason,
			time.Now()).
			withLeader(pm.serviceID).
			withCohort(proposedRule.GetCohortMembers()).
			withDurabilityPolicy(proposedRule.GetDurabilityPolicy()).
			withAcceptedMembers(req.GetAcceptedNodeIds()).
			withWALPosition(finalLSN)); err != nil {
			return nil, mterrors.Wrap(err, "propose failed: could not write rule")
		}
		// ---- END CRITICAL ORDERING SECTION ------------------------------------------
		// Rule committed and replicated. Safe to open write traffic.
		// updateTopologyAfterPromotion sets poolerType to PRIMARY + SERVING, which
		// ends the write-buffering window and lets the gateway route writes here.
		pm.healthStreamer.UpdateLeaderObservation(&poolerserver.LeaderObservation{
			LeaderID:   pm.serviceID,
			LeaderTerm: revokedBelowTerm,
		})
		if err := pm.updateTopologyAfterPromotion(ctx, state); err != nil {
			pm.logger.WarnContext(ctx, "Failed to update topology after propose", "error", err)
		}
	} else {
		// Step 3b: Replica — configure replication toward the new primary.
		// Recruit stopped only the WAL receiver (RECEIVER_ONLY), not replay, so
		// startAfter=false: replication restarts automatically when postgres
		// reloads the new primary_conninfo; no pg_wal_replay_resume() needed.
		host := proposalLeader.GetHost()
		port := proposalLeader.GetPostgresPort()
		pm.mu.Lock()
		pm.primaryPoolerID = proposalLeader.GetId()
		pm.primaryHost = host
		pm.primaryPort = port
		pm.mu.Unlock()

		if err := pm.setPrimaryConnInfoLocked(ctx, host, port, false /* stopBefore */, false /* startAfter */); err != nil {
			return nil, err
		}
		// Ensure health state reflects REPLICA, even if this node was recently
		// an emergency-demoted primary (which restarts postgres as standby but
		// does not update the pooler type until a Propose arrives).
		if err := pm.changeTypeLocked(ctx, clustermetadatapb.PoolerType_REPLICA); err != nil {
			pm.logger.WarnContext(ctx, "Failed to update pooler type to REPLICA after propose", "error", err)
		}
		if err := pm.clearResignedLeaderAtTerm(ctx); err != nil {
			pm.logger.WarnContext(ctx, "Failed to clear resigned leader term after propose", "error", err)
		}
		pm.broadcastHealth()
		if _, err := pm.rules.observePosition(ctx); err != nil {
			return nil, mterrors.Wrap(err, "propose failed: could not refresh rule position")
		}
	}

	pm.logger.InfoContext(ctx, "Propose complete",
		"revoked_below_term", revokedBelowTerm,
		"is_leader", isLeader)

	// Step 4: Return ConsensusStatus. The cache was warmed by getConsensusStatus in step 1
	// and updated by updateRule (leader path); the replica position is unchanged.
	cs, err := pm.getCachedConsensusStatus()
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to build consensus status")
	}
	return &consensusdatapb.ProposeResponse{ConsensusStatus: cs}, nil
}

// syncConfigFromProposedRule derives the synchronous replication config from a proposed rule
// by delegating to the durability policy.
func syncConfigFromProposedRule(
	logger *slog.Logger,
	rule *clustermetadatapb.ShardRule,
	leaderID *clustermetadatapb.ID,
) (*commonconsensus.LeaderDurabilityPostgresConfig, error) {
	policy, err := commonconsensus.NewPolicyFromProto(rule.GetDurabilityPolicy())
	if err != nil {
		return nil, fmt.Errorf("cannot derive sync config: %w", err)
	}
	return policy.BuildLeaderDurabilityPostgresConfig(logger, rule.GetCohortMembers(), leaderID)
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
