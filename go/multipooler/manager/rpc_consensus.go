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

// BeginTerm handles coordinator requests during leader appointments
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

	// Check if we've already accepted a different candidate in this term
	if req.Term == currentTerm && term != nil && term.AcceptedLeader != nil {
		// Compare full ID (component, cell, name) not just name
		if !proto.Equal(term.AcceptedLeader, req.CandidateId) {
			return response, nil
		}
	}

	// Update term if needed (only if req.Term > currentTerm)
	// This will reset AcceptedLeader to nil
	if req.Term > currentTerm {
		if err := pm.validateAndUpdateTerm(ctx, req.Term, false); err != nil {
			return nil, fmt.Errorf("failed to update term: %w", err)
		}
		response.Term = req.Term
	}

	// TODO: Use pooler serving state to decide whether to accept
	// Check if we're caught up with replication (within 30 seconds)
	var lastMsgReceiptTime *time.Time
	err = pm.db.QueryRowContext(ctx, "SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").Scan(&lastMsgReceiptTime)
	if err != nil {
		// No WAL receiver (could be primary or disconnected standby)
		// Don't reject the acceptance - let it proceed
	} else if lastMsgReceiptTime != nil {
		timeSinceLastMessage := time.Since(*lastMsgReceiptTime)
		if timeSinceLastMessage > 30*time.Second {
			// We're too far behind in replication, don't accept
			return response, nil
		}
	}

	// Accept candidate atomically (checks, saves to disk, updates memory)
	if err := cs.AcceptCandidateAndSave(ctx, req.CandidateId); err != nil {
		return nil, fmt.Errorf("failed to accept candidate: %w", err)
	}

	response.Accepted = true
	return response, nil
}

// ConsensusStatus returns the current status of this node for consensus
func (pm *MultiPoolerManager) ConsensusStatus(ctx context.Context, req *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error) {
	// Acquire the action lock to read consensus state
	ctx, err := pm.actionLock.Acquire(ctx, "ConsensusStatus")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Get consensus state
	pm.mu.Lock()
	cs := pm.consensusState
	pm.mu.Unlock()

	if cs == nil {
		return nil, fmt.Errorf("consensus state not initialized")
	}

	// Get local term from consensus state
	localTerm, err := cs.GetCurrentTermNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current term: %w", err)
	}

	// Get last successful leader term from Postgres (replicated data)
	var leaderTerm int64
	isHealthy := false

	if pm.db != nil {
		err := pm.db.QueryRowContext(ctx, `
			SELECT COALESCE(MAX(leader_term), 0)
			FROM multigres.heartbeat
			WHERE shard_id = $1
		`, []byte(req.ShardId)).Scan(&leaderTerm)

		if err != nil {
			pm.logger.WarnContext(ctx, "Failed to query postgres leader term", "error", err)
			leaderTerm = 0
		} else {
			isHealthy = true
		}
	}

	// Get WAL position and determine role (primary/replica)
	walPosition := &consensusdatapb.WALPosition{
		Timestamp: timestamppb.New(time.Now()),
	}
	role := "replica"

	if isHealthy {
		// Check if we're in recovery (standby)
		var inRecovery bool
		err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
		if err == nil {
			if inRecovery {
				// On standby: get receive and replay positions
				status, err := pm.queryReplicationStatus(ctx)
				if err == nil {
					walPosition.LastReceiveLsn = status.LastReceiveLsn
					walPosition.LastReplayLsn = status.LastReplayLsn
				}
			} else {
				// On primary: get current write position
				role = "primary"
				var currentLsn string
				err = pm.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()").Scan(&currentLsn)
				if err == nil {
					walPosition.CurrentLsn = currentLsn
				}
			}
		}
	}

	return &consensusdatapb.StatusResponse{
		PoolerId:    pm.serviceID.GetName(),
		CurrentTerm: localTerm,
		LeaderTerm:  leaderTerm,
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
		LeaderTerm:       view.LeaderTerm,
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
