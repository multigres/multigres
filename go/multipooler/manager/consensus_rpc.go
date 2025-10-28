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

	// If request term is newer, update our term and reset vote
	if req.Term > currentTerm {
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
	}

	// Reject if term is outdated
	if req.Term < currentTerm {
		return response, nil
	}

	// Check if we've already voted in this term
	if votedFor != nil && votedFor.GetName() != req.CandidateId {
		return response, nil
	}

	// Validate candidate's WAL position (query Postgres)
	ourWAL, err := pm.GetCurrentWALPosition(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get WAL position: %w", err)
	}

	if !IsWALPositionUpToDate(req.LastLogIndex, req.LastLogTerm, ourWAL) {
		return response, nil
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

// GetNodeStatus returns the current status of this node for consensus
func (pm *MultiPoolerManager) GetNodeStatus(ctx context.Context, req *consensusdatapb.NodeStatusRequest) (*consensusdatapb.NodeStatusResponse, error) {
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
	var postgresLeaderTerm int64
	isHealthy := false

	if pm.db != nil {
		err := pm.db.QueryRowContext(ctx, `
			SELECT COALESCE(MAX(leader_term), 0)
			FROM multigres.heartbeat
			WHERE shard_id = $1
		`, []byte(req.ShardId)).Scan(&postgresLeaderTerm)

		if err != nil {
			pm.logger.Warn("Failed to query postgres leader term", "error", err)
			postgresLeaderTerm = 0
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

	return &consensusdatapb.NodeStatusResponse{
		NodeId:             pm.serviceID.GetName(),
		CurrentTerm:        localTerm,
		PostgresLeaderTerm: postgresLeaderTerm,
		WalPosition:        walPosition,
		IsHealthy:          isHealthy,
		IsEligible:         true, // TODO: implement eligibility logic based on policy
		HealthScore:        100,  // TODO: implement health scoring
		Zone:               pm.serviceID.GetCell(),
		Role:               role,
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
		PoolerId:          view.PoolerID,
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
func (pm *MultiPoolerManager) CanReachPrimary(ctx context.Context, req *consensusdatapb.CanReachPrimaryRequest) (*consensusdatapb.CanReachPrimaryResponse, error) {
	// TODO: Implement actual connectivity check
	// For now, just return a placeholder
	return &consensusdatapb.CanReachPrimaryResponse{
		Reachable:    true,
		ErrorMessage: "",
	}, nil
}
