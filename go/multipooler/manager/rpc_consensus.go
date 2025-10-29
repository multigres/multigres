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

	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// RequestVote handles vote requests during leader election (Raft-style)
func (pm *MultiPoolerManager) RequestVote(ctx context.Context, req *consensusdatapb.RequestVoteRequest) (*consensusdatapb.RequestVoteResponse, error) {
	// CRITICAL: Must be able to reach Postgres to participate in election
	if pm.db == nil {
		return nil, fmt.Errorf("postgres unreachable, cannot vote")
	}

	// Test database connectivity
	if err := pm.db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("postgres unhealthy, cannot vote: %w", err)
	}

	// Load consensus term from disk if not already loaded
	pm.mu.Lock()
	if pm.consensusTerm == nil {
		term, err := GetTerm(pm.config.PoolerDir)
		if err != nil {
			pm.mu.Unlock()
			return nil, fmt.Errorf("failed to load consensus state: %w", err)
		}
		pm.consensusTerm = term
	}

	currentTerm := pm.consensusTerm.GetCurrentTerm()
	votedFor := pm.consensusTerm.GetVotedFor()
	pm.mu.Unlock()

	response := &consensusdatapb.RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: false,
		NodeId:      pm.serviceID.GetName(),
	}

	// Reject if term is outdated
	if req.Term < currentTerm {
		return response, nil
	}

	// Check if we've already voted in this term
	if req.Term == currentTerm && votedFor != nil && votedFor.GetName() != req.CandidateId {
		return response, nil
	}

	// At this point, req.Term must be > currentTerm (since we've already handled < and == cases above)
	// Update our term and reset vote
	newTerm := &pgctldpb.ConsensusTerm{
		CurrentTerm: req.Term,
		VotedFor:    nil,
	}
	if err := SetTerm(pm.config.PoolerDir, newTerm); err != nil {
		return nil, fmt.Errorf("failed to update term: %w", err)
	}
	pm.mu.Lock()
	pm.consensusTerm = newTerm
	pm.mu.Unlock()

	currentTerm = req.Term
	votedFor = nil
	response.Term = currentTerm

	// TODO: Use pooler serving state to decide whether to vote
	// Check if we're caught up with replication (within 30 seconds)
	var lastMsgReceiptTime *time.Time
	err := pm.db.QueryRowContext(ctx, "SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").Scan(&lastMsgReceiptTime)
	if err != nil {
		// No WAL receiver (could be primary or disconnected standby)
		// Don't reject the vote - let it proceed
	} else if lastMsgReceiptTime != nil {
		timeSinceLastMessage := time.Since(*lastMsgReceiptTime)
		if timeSinceLastMessage > 30*time.Second {
			// We're too far behind in replication, don't vote
			return response, nil
		}
	}

	// Grant vote and persist decision to LOCAL FILE (not Postgres!)
	candidateID := &clustermetadatapb.ID{
		Cell: pm.serviceID.GetCell(),
		Name: req.CandidateId,
	}

	pm.mu.Lock()
	pm.consensusTerm.VotedFor = candidateID
	termToSave := pm.consensusTerm
	pm.mu.Unlock()

	if err := SetTerm(pm.config.PoolerDir, termToSave); err != nil {
		return nil, fmt.Errorf("failed to persist vote: %w", err)
	}

	response.VoteGranted = true
	return response, nil
}

// ConsensusStatus returns the current status of this node for consensus
func (pm *MultiPoolerManager) ConsensusStatus(ctx context.Context, req *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error) {
	// Load consensus term from disk if not already loaded
	pm.mu.Lock()
	if pm.consensusTerm == nil {
		term, err := GetTerm(pm.config.PoolerDir)
		if err != nil {
			pm.mu.Unlock()
			return nil, fmt.Errorf("failed to load consensus state: %w", err)
		}
		pm.consensusTerm = term
	}

	// Get local voting term from file
	localTerm := pm.consensusTerm.GetCurrentTerm()
	pm.mu.Unlock()

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
			pm.logger.Warn("Failed to query postgres leader term", "error", err)
			leaderTerm = 0
		} else {
			isHealthy = true
		}
	}

	// Get current WAL position
	walPosition := &consensusdatapb.WALPosition{}
	if isHealthy {
		var err error
		walPosition, err = pm.GetCurrentWALPosition(ctx)
		if err != nil {
			pm.logger.Warn("Failed to get WAL position", "error", err)
		}
	}

	// Determine role (primary/replica)
	role := "replica"
	if isHealthy {
		var inRecovery bool
		err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
		if err == nil && !inRecovery {
			role = "primary"
		}
	}

	return &consensusdatapb.StatusResponse{
		NodeId:      pm.serviceID.GetName(),
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
		LeaderId:          view.LeaderID,
		LeaderTerm:        view.LeaderTerm,
		LeaderWalPosition: view.LeaderWALPosition,
		LastHeartbeat:     timestamppb.New(view.LastHeartbeat),
		ReplicationLagNs:  view.ReplicationLag.Nanoseconds(),
	}, nil
}

// GetWALPosition returns the current WAL position
func (pm *MultiPoolerManager) GetWALPosition(ctx context.Context, req *consensusdatapb.GetWALPositionRequest) (*consensusdatapb.GetWALPositionResponse, error) {
	walPosition, err := pm.GetCurrentWALPosition(ctx)
	if err != nil {
		return nil, err
	}

	return &consensusdatapb.GetWALPositionResponse{
		WalPosition: walPosition,
	}, nil
}

// CanReachPrimary checks if this node can reach the specified primary
// by querying the pg_stat_wal_receiver view to check the WAL receiver status
func (pm *MultiPoolerManager) CanReachPrimary(ctx context.Context, req *consensusdatapb.CanReachPrimaryRequest) (*consensusdatapb.CanReachPrimaryResponse, error) {
	if pm.db == nil {
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: "database connection not available",
		}, nil
	}

	// Query pg_stat_wal_receiver to check if we can reach the primary
	var status string
	err := pm.db.QueryRowContext(ctx, "SELECT status FROM pg_stat_wal_receiver").Scan(&status)
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

	// WAL receiver is active and in a good state
	return &consensusdatapb.CanReachPrimaryResponse{
		Reachable:    true,
		ErrorMessage: "",
	}, nil
}
