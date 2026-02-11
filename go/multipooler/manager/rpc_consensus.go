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
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/multipooler/executor"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
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
func (pm *MultiPoolerManager) BeginTerm(ctx context.Context, req *consensusdatapb.BeginTermRequest) (*consensusdatapb.BeginTermResponse, error) {
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

	pm.mu.Lock()
	cs := pm.consensusState
	pm.mu.Unlock()

	if cs == nil {
		return nil, errors.New("consensus state not initialized")
	}

	// Get current term for response
	currentTerm, err := cs.GetCurrentTermNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current term: %w", err)
	}

	// Atomically update term and accept candidate
	// This handles all consensus rules: term validation, duplicate check, etc.
	err = cs.UpdateTermAndAcceptCandidate(ctx, req.Term, req.CandidateId)
	if err != nil {
		// Term not accepted - return rejection
		pm.logger.InfoContext(ctx, "Term not accepted",
			"request_term", req.Term,
			"current_term", currentTerm,
			"error", err)
		return &consensusdatapb.BeginTermResponse{
			Term:     currentTerm,
			Accepted: false,
			PoolerId: pm.serviceID.GetName(),
		}, nil
	}

	pm.logger.InfoContext(ctx, "Term accepted",
		"term", req.Term,
		"coordinator", req.CandidateId.GetName())

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
			true)
		if err != nil {
			return mterrors.Wrap(err, "failed to pause replication during revoke")
		}

		// Wait for replay to finish processing all buffered WAL
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

	return nil
}

// ConsensusStatus returns the current status of this node for consensus
func (pm *MultiPoolerManager) ConsensusStatus(ctx context.Context, req *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error) {
	// Get consensus state
	pm.mu.Lock()
	cs := pm.consensusState
	pm.mu.Unlock()

	if cs == nil {
		return nil, errors.New("consensus state not initialized")
	}

	term, err := cs.GetInconsistentTerm()
	if err != nil {
		return nil, fmt.Errorf("failed to get consensus term: %w", err)
	}

	localCurrentTerm := int64(0)
	if term != nil {
		localCurrentTerm = term.GetTermNumber()
	}
	localPrimaryTerm := int64(0)
	if term != nil {
		localPrimaryTerm = term.GetPrimaryTerm()
	}

	// Check if database is healthy by attempting a simple query
	_, healthErr := pm.query(ctx, "SELECT 1")
	isHealthy := healthErr == nil

	// Get WAL position and determine role (primary/replica)
	walPosition := &consensusdatapb.WALPosition{
		Timestamp: timestamppb.New(time.Now()),
	}
	role := "unknown"

	if isHealthy {
		// Check role and get appropriate WAL position
		isPrimary, err := pm.isPrimary(ctx)
		if err == nil {
			if isPrimary {
				// On primary: get current write position
				role = "primary"
				currentLsn, err := pm.getPrimaryLSN(ctx)
				if err == nil {
					walPosition.CurrentLsn = currentLsn
				}
			} else {
				role = "replica"
				// On standby: get receive and replay positions
				status, err := pm.queryReplicationStatus(ctx)
				if err == nil {
					walPosition.LastReceiveLsn = status.LastReceiveLsn
					walPosition.LastReplayLsn = status.LastReplayLsn
				}
			}
		}
	}

	// Get timeline information for divergence detection
	var timelineInfo *consensusdatapb.TimelineInfo
	if isHealthy {
		timelineID, err := pm.getTimelineID(ctx)
		if err == nil {
			timelineInfo = &consensusdatapb.TimelineInfo{
				TimelineId: timelineID,
				// TODO: Populate history for primaries
			}
		}
	}

	return &consensusdatapb.StatusResponse{
		PoolerId:     pm.serviceID.GetName(),
		CurrentTerm:  localCurrentTerm,
		WalPosition:  walPosition,
		IsHealthy:    isHealthy,
		IsEligible:   true, // TODO: implement eligibility logic based on policy
		Cell:         pm.serviceID.GetCell(),
		Role:         role,
		TimelineInfo: timelineInfo,
		PrimaryTerm:  localPrimaryTerm,
	}, nil
}

// GetLeadershipView returns leadership information from the heartbeat table
func (pm *MultiPoolerManager) GetLeadershipView(ctx context.Context, req *consensusdatapb.LeadershipViewRequest) (*consensusdatapb.LeadershipViewResponse, error) {
	if pm.replTracker == nil {
		return nil, errors.New("replication tracker not initialized")
	}

	// Use the heartbeat reader to get leadership view
	reader := pm.replTracker.HeartbeatReader()
	view, err := reader.GetLeadershipView()
	if err != nil {
		return nil, fmt.Errorf("failed to get leadership view: %w", err)
	}

	return &consensusdatapb.LeadershipViewResponse{
		LeaderId:         view.LeaderID,
		LastHeartbeat:    timestamppb.New(view.LastHeartbeat),
		ReplicationLagNs: view.ReplicationLag.Nanoseconds(),
	}, nil
}

// CanReachPrimary checks if this node can reach the specified primary
// by querying the pg_stat_wal_receiver view to check the WAL receiver status
// and verifying it's connected to the expected primary host/port
func (pm *MultiPoolerManager) CanReachPrimary(ctx context.Context, req *consensusdatapb.CanReachPrimaryRequest) (*consensusdatapb.CanReachPrimaryResponse, error) {
	// Query pg_stat_wal_receiver to check if we can reach the primary
	result, err := pm.query(ctx, "SELECT status, conninfo FROM pg_stat_wal_receiver")
	if err != nil {
		//nolint:nilerr // Error is communicated via response struct, not error return
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: "database connection not available",
		}, nil
	}
	var status, conninfo string
	err = executor.ScanSingleRow(result, &status, &conninfo)
	if err != nil {
		// No rows returned means we're not receiving WAL (likely not a replica or not connected)
		//nolint:nilerr // Error is communicated via response struct, not error return
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: "no active WAL receiver",
		}, nil
	}

	// If status is "stopping", the connection is not healthy
	if status == "stopping" {
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: "WAL receiver is stopping",
		}, nil
	}

	// Parse conninfo to extract host and port
	parsedConnInfo, err := parseAndRedactPrimaryConnInfo(conninfo)
	if err != nil {
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: fmt.Sprintf("failed to parse conninfo: %v", err),
		}, nil
	}

	// Compare with requested primary host and port
	if parsedConnInfo.Host != req.PrimaryHost {
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: fmt.Sprintf("WAL receiver connected to different host: expected %s, got %s", req.PrimaryHost, parsedConnInfo.Host),
		}, nil
	}

	if parsedConnInfo.Port != req.PrimaryPort {
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: fmt.Sprintf("WAL receiver connected to different port: expected %d, got %d", req.PrimaryPort, parsedConnInfo.Port),
		}, nil
	}

	// WAL receiver is active and connected to the expected primary
	return &consensusdatapb.CanReachPrimaryResponse{
		Reachable:    true,
		ErrorMessage: "",
	}, nil
}
