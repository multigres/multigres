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

	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// BeginTerm handles coordinator requests during leader appointments.
// Accepting a new term means this node will not accept any new requests from
// the current term. This is the revocation step of consensus, which applies
// to both primaries and standbys:
//
//   - If this node is a primary, it must demote itself as part of revocation.
//   - If this node is a standby, it should break replication as part of
//     revocation. However, breaking replication on all standbys before the primary
//     is demoted will have the effect of blocking writes to the primary,
//     so this has to be done at the proper time (TODO: not implemented yet).
func (pm *MultiPoolerManager) BeginTerm(ctx context.Context, req *consensusdatapb.BeginTermRequest) (*consensusdatapb.BeginTermResponse, error) {
	// Acquire the action lock to ensure only one consensus operation runs at a time
	// This prevents split-brain acceptance and ensures term updates are serialized
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "BeginTerm")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// CRITICAL: Must be able to reach Postgres to participate in cohort
	if pm.db == nil {
		return nil, fmt.Errorf("postgres unreachable, cannot accept new term")
	}

	// Test database connectivity
	if err = pm.db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("postgres unhealthy, cannot accept new term: %w", err)
	}

	// Check if we're currently a primary (before any term changes)
	wasPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		// If we can't determine role, log warning but continue
		pm.logger.WarnContext(ctx, "Failed to determine if primary", "error", err)
		wasPrimary = false
	}

	// Get current consensus state
	pm.mu.Lock()
	cs := pm.consensusState
	pm.mu.Unlock()

	if cs == nil {
		return nil, fmt.Errorf("consensus state not initialized")
	}

	currentTerm, err := cs.GetCurrentTermNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current term: %w", err)
	}

	term, err := cs.GetTerm(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get term: %w", err)
	}

	response := &consensusdatapb.BeginTermResponse{
		Term:     currentTerm,
		Accepted: false,
		PoolerId: pm.serviceID.GetName(),
	}

	// Reject if term is outdated
	if req.Term < currentTerm {
		return response, nil
	}

	// Check if we've already accepted this term from a different coordinator
	if req.Term == currentTerm && term != nil && term.AcceptedTermFromCoordinatorId != nil {
		// Compare full ID (component, cell, name) not just name
		if !proto.Equal(term.AcceptedTermFromCoordinatorId, req.CandidateId) {
			return response, nil
		}
	}

	// If we're a primary accepting a higher term, we must demote FIRST.
	// Only accept the term if demotion succeeds. This prevents split-brain
	// by ensuring the old primary stops accepting writes before acknowledging
	// the new term.
	if wasPrimary && req.Term > currentTerm {
		pm.logger.InfoContext(ctx, "Primary receiving higher term, attempting demotion before acceptance",
			"current_term", currentTerm,
			"new_term", req.Term,
			"candidate_id", req.CandidateId.GetName())

		// Use a reasonable drain timeout for demotion
		drainTimeout := 5 * time.Second
		demoteLSN, demoteErr := pm.demoteLocked(ctx, req.Term, drainTimeout)
		if demoteErr != nil {
			// Demotion failed - do NOT accept the term
			// Return accepted=false so coordinator knows this node couldn't safely step down
			pm.logger.ErrorContext(ctx, "Demotion failed, rejecting term acceptance",
				"error", demoteErr,
				"term", req.Term)
			return response, nil
		}

		// Demotion succeeded - now update term and accept
		if err := pm.validateAndUpdateTerm(ctx, req.Term, false); err != nil {
			return nil, fmt.Errorf("failed to update term after demotion: %w", err)
		}
		response.Term = req.Term
		response.DemoteLsn = demoteLSN.LsnPosition

		pm.logger.InfoContext(ctx, "Demotion completed, accepting term",
			"demote_lsn", demoteLSN.LsnPosition,
			"term", req.Term)
	} else {
		// Not a primary or same/lower term - handle normally

		// Update term if needed (only if req.Term > currentTerm)
		if req.Term > currentTerm {
			if err := pm.validateAndUpdateTerm(ctx, req.Term, false); err != nil {
				return nil, fmt.Errorf("failed to update term: %w", err)
			}
			response.Term = req.Term
		}

		// Check if we're caught up with replication (within 30 seconds)
		// Only relevant for standbys - primaries don't have WAL receivers
		if !wasPrimary {
			var lastMsgReceiptTime *time.Time
			err = pm.db.QueryRowContext(ctx, "SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").Scan(&lastMsgReceiptTime)
			if err != nil {
				// No WAL receiver (could be disconnected standby)
				// Don't reject the acceptance - let it proceed
			} else if lastMsgReceiptTime != nil {
				timeSinceLastMessage := time.Since(*lastMsgReceiptTime)
				if timeSinceLastMessage > 30*time.Second {
					// We're too far behind in replication, don't accept
					return response, nil
				}
			}
		}
	}

	// Accept term from coordinator atomically (checks, saves to disk, updates memory)
	if err := cs.AcceptCandidateAndSave(ctx, req.CandidateId); err != nil {
		return nil, fmt.Errorf("failed to accept term from coordinator: %w", err)
	}

	response.Accepted = true

	return response, nil
}

// ConsensusStatus returns the current status of this node for consensus
func (pm *MultiPoolerManager) ConsensusStatus(ctx context.Context, req *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error) {
	// Get consensus state
	pm.mu.Lock()
	cs := pm.consensusState
	pm.mu.Unlock()

	if cs == nil {
		return nil, fmt.Errorf("consensus state not initialized")
	}

	// Get local term from consensus state
	localTerm, err := cs.GetInconsistentCurrentTermNumber()
	if err != nil {
		return nil, fmt.Errorf("failed to get current term: %w", err)
	}

	// Check if database is healthy
	isHealthy := pm.db != nil

	// Get WAL position and determine role (primary/replica)
	walPosition := &consensusdatapb.WALPosition{
		Timestamp: timestamppb.New(time.Now()),
	}
	role := "replica"

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
				// On standby: get receive and replay positions
				status, err := pm.queryReplicationStatus(ctx)
				if err == nil {
					walPosition.LastReceiveLsn = status.LastReceiveLsn
					walPosition.LastReplayLsn = status.LastReplayLsn
				}
			}
		}
	}

	return &consensusdatapb.StatusResponse{
		PoolerId:    pm.serviceID.GetName(),
		CurrentTerm: localTerm,
		WalPosition: walPosition,
		IsHealthy:   isHealthy,
		IsEligible:  true, // TODO: implement eligibility logic based on policy
		Cell:        pm.serviceID.GetCell(),
		Role:        role,
	}, nil
}

// GetLeadershipView returns leadership information from the heartbeat table
func (pm *MultiPoolerManager) GetLeadershipView(ctx context.Context, req *consensusdatapb.LeadershipViewRequest) (*consensusdatapb.LeadershipViewResponse, error) {
	if pm.replTracker == nil {
		return nil, fmt.Errorf("replication tracker not initialized")
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
	if pm.db == nil {
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: "database connection not available",
		}, nil
	}

	// Query pg_stat_wal_receiver to check if we can reach the primary
	var status, conninfo string
	err := pm.db.QueryRowContext(ctx, "SELECT status, conninfo FROM pg_stat_wal_receiver").Scan(&status, &conninfo)
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
