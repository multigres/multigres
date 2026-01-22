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

package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// BeginTerm implements stages 1-3 of the consensus protocol:
// 1. Select candidate based on WAL position
// 2. Send BeginTerm RPC to all nodes in parallel
// 3. Validate quorum using durability policy
//
// The proposedTerm parameter is the term number to use for this election.
// It should be computed by the caller as maxTerm + 1.
//
// Returns the candidate node, standbys, the proposed term, and any error.
func (c *Coordinator) BeginTerm(ctx context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, quorumRule *clustermetadatapb.QuorumRule, proposedTerm int64) (*multiorchdatapb.PoolerHealthState, []*multiorchdatapb.PoolerHealthState, int64, error) {
	c.logger.InfoContext(ctx, "Beginning term", "shard", shardID, "term", proposedTerm)

	// Stage 1: Select Candidate - Choose based on WAL position
	candidate, err := c.selectCandidate(ctx, cohort)
	if err != nil {
		return nil, nil, 0, mterrors.Wrap(err, "failed to select candidate")
	}

	c.logger.InfoContext(ctx, "Selected candidate", "shard", shardID, "candidate", candidate.MultiPooler.Id.Name)

	// Stage 2: Recruit Nodes - Send BeginTerm RPC to all nodes in parallel
	recruited, err := c.recruitNodes(ctx, cohort, proposedTerm, candidate)
	if err != nil {
		return nil, nil, 0, mterrors.Wrap(err, "failed to recruit nodes")
	}

	c.logger.InfoContext(ctx, "Recruited nodes", "shard", shardID, "count", len(recruited))

	// Stage 3: Validate Quorum using durability policy
	c.logger.InfoContext(ctx, "Validating quorum",
		"shard", shardID,
		"quorum_type", quorumRule.QuorumType,
		"required_count", quorumRule.RequiredCount)

	if err := c.ValidateQuorum(quorumRule, cohort, recruited); err != nil {
		return nil, nil, 0, mterrors.Wrapf(err, "quorum validation failed for shard %s", shardID)
	}

	// Separate candidate from standbys
	var standbys []*multiorchdatapb.PoolerHealthState
	for _, node := range recruited {
		if node.MultiPooler.Id.Name != candidate.MultiPooler.Id.Name {
			standbys = append(standbys, node)
		}
	}

	return candidate, standbys, proposedTerm, nil
}

// discoverMaxTerm finds the maximum consensus term from cached health state.
// This uses the ConsensusTerm data already populated by health checks, avoiding extra RPCs.
func (c *Coordinator) discoverMaxTerm(cohort []*multiorchdatapb.PoolerHealthState) (int64, error) {
	var maxTerm int64

	for _, pooler := range cohort {
		// Invariant: poolers in the cohort with successful health checks must have ConsensusTerm populated
		if pooler.IsLastCheckValid && pooler.ConsensusTerm == nil {
			return 0, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"healthy pooler %s in cohort missing consensus term data - health check invariant violated",
				pooler.MultiPooler.Id.Name)
		}

		if pooler.ConsensusTerm != nil && pooler.ConsensusTerm.TermNumber > maxTerm {
			maxTerm = pooler.ConsensusTerm.TermNumber
		}
	}

	// Invariant: at least one pooler in the cohort must have a term > 0
	if maxTerm == 0 {
		return 0, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no poolers in cohort have initialized consensus term - cannot discover max term")
	}

	return maxTerm, nil
}

// selectCandidate chooses the best candidate based on WAL position and health.
func (c *Coordinator) selectCandidate(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState) (*multiorchdatapb.PoolerHealthState, error) {
	type nodeStatus struct {
		node        *multiorchdatapb.PoolerHealthState
		walPosition string
		healthy     bool
		initialized bool
	}

	statuses := make([]nodeStatus, 0, len(cohort))

	// Query status from all nodes
	for _, pooler := range cohort {
		req := &consensusdatapb.StatusRequest{}
		resp, err := c.rpcClient.ConsensusStatus(ctx, pooler.MultiPooler, req)
		if err != nil {
			c.logger.WarnContext(ctx, "Failed to get status from node",
				"node", pooler.MultiPooler.Id.Name,
				"error", err)
			continue
		}

		status := nodeStatus{
			node:        pooler,
			healthy:     resp.IsHealthy,
			initialized: store.IsInitialized(pooler),
		}

		// Extract WAL position based on role
		if resp.WalPosition != nil {
			if resp.Role == "primary" {
				status.walPosition = resp.WalPosition.CurrentLsn
			} else {
				// For standbys, use receive position (includes unreplayed WAL)
				status.walPosition = resp.WalPosition.LastReceiveLsn
			}
		}

		statuses = append(statuses, status)
	}

	if len(statuses) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE,
			"no healthy nodes available for candidate selection")
	}

	// Prefer initialized nodes over uninitialized nodes
	// Within each group (initialized vs uninitialized), select by WAL position
	var bestCandidate *multiorchdatapb.PoolerHealthState
	var bestLSN pglogrepl.LSN
	var bestIsInitialized bool

	for _, status := range statuses {
		if !status.healthy {
			continue
		}

		// Skip nodes with empty WAL position (e.g., postgres is down but multipooler is still responding)
		if status.walPosition == "" {
			c.logger.InfoContext(ctx, "Skipping node with empty WAL position",
				"node", status.node.MultiPooler.Id.Name)
			continue
		}

		// Parse and validate LSN
		lsn, err := pglogrepl.ParseLSN(status.walPosition)
		if err != nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
				"invalid LSN format for node %s: %s (error: %v)",
				status.node.MultiPooler.Id.Name, status.walPosition, err)
		}

		// Selection criteria:
		// 1. Prefer initialized nodes over uninitialized nodes
		// 2. Within same initialization status, prefer highest LSN
		shouldSelect := false
		if bestCandidate == nil {
			shouldSelect = true
		} else if status.initialized && !bestIsInitialized {
			// Prefer initialized over uninitialized
			shouldSelect = true
			c.logger.InfoContext(ctx, "Preferring initialized node over uninitialized",
				"initialized_node", status.node.MultiPooler.Id.Name,
				"uninitialized_node", bestCandidate.MultiPooler.Id.Name)
		} else if status.initialized == bestIsInitialized && lsn > bestLSN {
			// Same initialization status, prefer higher LSN
			shouldSelect = true
		}

		if shouldSelect {
			bestCandidate = status.node
			bestLSN = lsn
			bestIsInitialized = status.initialized
		}
	}

	if bestCandidate == nil {
		// Fall back to first available node if no healthy nodes
		bestCandidate = statuses[0].node
		c.logger.WarnContext(ctx, "No healthy nodes, using first available",
			"node", bestCandidate.MultiPooler.Id.Name)
	}

	c.logger.InfoContext(ctx, "Selected candidate",
		"node", bestCandidate.MultiPooler.Id.Name,
		"initialized", bestIsInitialized,
		"lsn", bestLSN)

	return bestCandidate, nil
}

// recruitNodes sends BeginTerm RPC to all nodes in parallel and returns those that accepted.
func (c *Coordinator) recruitNodes(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState, term int64, candidate *multiorchdatapb.PoolerHealthState) ([]*multiorchdatapb.PoolerHealthState, error) {
	type result struct {
		node     *multiorchdatapb.PoolerHealthState
		accepted bool
		err      error
	}

	results := make(chan result, len(cohort))
	var wg sync.WaitGroup

	for _, pooler := range cohort {
		wg.Add(1)
		go func(n *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			req := &consensusdatapb.BeginTermRequest{
				Term:        term,
				CandidateId: c.coordinatorID,
			}
			resp, err := c.rpcClient.BeginTerm(ctx, n.MultiPooler, req)
			if err != nil {
				results <- result{node: n, err: err}
				return
			}
			results <- result{node: n, accepted: resp.Accepted}
		}(pooler)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect accepted nodes
	var recruited []*multiorchdatapb.PoolerHealthState
	for r := range results {
		if r.err != nil {
			c.logger.WarnContext(ctx, "BeginTerm failed for node",
				"node", r.node.MultiPooler.Id.Name,
				"error", r.err)
			continue
		}
		if r.accepted {
			recruited = append(recruited, r.node)
			c.logger.InfoContext(ctx, "Node accepted term",
				"node", r.node.MultiPooler.Id.Name,
				"term", term)
		} else {
			c.logger.WarnContext(ctx, "Node rejected term",
				"node", r.node.MultiPooler.Id.Name,
				"term", term)
		}
	}

	return recruited, nil
}

// Propagate implements stage 5 of the consensus protocol:
// - Configure standbys to replicate from the candidate (before promotion)
// - Promote the candidate to primary (includes sync replication configuration)
//
// Standbys are configured BEFORE promotion to avoid a deadlock: the Promote RPC
// configures sync replication and writes leadership history, which blocks until
// at least one standby acknowledges. If standbys aren't configured to replicate
// yet, the write blocks forever.
func (c *Coordinator) Propagate(ctx context.Context, candidate *multiorchdatapb.PoolerHealthState, standbys []*multiorchdatapb.PoolerHealthState, term int64, quorumRule *clustermetadatapb.QuorumRule, reason string, cohort []*multiorchdatapb.PoolerHealthState, recruited []*multiorchdatapb.PoolerHealthState) error {
	// Build synchronous replication configuration based on quorum policy
	syncConfig, err := BuildSyncReplicationConfig(c.logger, quorumRule, standbys, candidate)
	if err != nil {
		return mterrors.Wrap(err, "failed to build synchronous replication config")
	}

	// Promote candidate to primary
	c.logger.InfoContext(ctx, "Promoting candidate to primary",
		"node", candidate.MultiPooler.Id.Name,
		"term", term)

	// Get current WAL position before promotion (for validation)
	statusReq := &consensusdatapb.StatusRequest{}
	status, err := c.rpcClient.ConsensusStatus(ctx, candidate.MultiPooler, statusReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to get candidate status before promotion")
	}

	expectedLSN := ""
	if status.WalPosition != nil {
		if status.Role == "primary" {
			expectedLSN = status.WalPosition.CurrentLsn
		} else {
			// For standbys, use receive position (includes unreplayed WAL)
			expectedLSN = status.WalPosition.LastReceiveLsn

			// Wait for standby to replay all received WAL before promotion
			// This ensures validateExpectedLSN in Promote will pass
			if expectedLSN != "" {
				c.logger.InfoContext(ctx, "Waiting for candidate to replay all received WAL",
					"node", candidate.MultiPooler.Id.Name,
					"target_lsn", expectedLSN)

				waitReq := &multipoolermanagerdatapb.WaitForLSNRequest{
					TargetLsn: expectedLSN,
				}
				if _, err := c.rpcClient.WaitForLSN(ctx, candidate.MultiPooler, waitReq); err != nil {
					return mterrors.Wrapf(err, "candidate failed to replay WAL to %s", expectedLSN)
				}
			}
		}
	}

	// Build lists of cohort member names and accepted member names
	cohortMembers := make([]string, 0, len(cohort))
	for _, pooler := range cohort {
		if pooler.MultiPooler != nil && pooler.MultiPooler.Id != nil {
			cohortMembers = append(cohortMembers, pooler.MultiPooler.Id.Name)
		}
	}

	acceptedMembers := make([]string, 0, len(recruited))
	for _, pooler := range recruited {
		if pooler.MultiPooler != nil && pooler.MultiPooler.Id != nil {
			acceptedMembers = append(acceptedMembers, pooler.MultiPooler.Id.Name)
		}
	}

	// Configure standbys to replicate from the candidate BEFORE promoting.
	// This ensures standbys are ready to connect when sync replication is configured.
	// Without this, the Promote call can deadlock: it configures sync replication and
	// tries to write leadership history, but blocks waiting for standby acknowledgment.
	// The standbys can't acknowledge because they haven't been told to replicate yet.
	var wg sync.WaitGroup
	errChan := make(chan error, len(standbys))

	for _, standby := range standbys {
		wg.Add(1)
		go func(s *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			c.logger.InfoContext(ctx, "Configuring standby replication before promotion",
				"standby", s.MultiPooler.Id.Name,
				"primary", candidate.MultiPooler.Id.Name)

			setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
				Primary:               candidate.MultiPooler,
				CurrentTerm:           term,
				StopReplicationBefore: false,
				StartReplicationAfter: true, // Start streaming immediately so sync replication can proceed
				Force:                 false,
			}
			if _, err := c.rpcClient.SetPrimaryConnInfo(ctx, s.MultiPooler, setPrimaryReq); err != nil {
				errChan <- mterrors.Wrapf(err, "failed to configure standby %s", s.MultiPooler.Id.Name)
				return
			}

			c.logger.InfoContext(ctx, "Standby configured successfully", "standby", s.MultiPooler.Id.Name)
		}(standby)
	}

	wg.Wait()
	close(errChan)

	// Check for errors - log but don't fail, standbys can be fixed later
	var standbyErrs []error
	for err := range errChan {
		standbyErrs = append(standbyErrs, err)
	}
	for _, err := range standbyErrs {
		c.logger.WarnContext(ctx, "Standby configuration failed", "error", err)
	}

	// Get coordinator ID as a string
	coordinatorIDStr := topoclient.ClusterIDString(c.GetCoordinatorID())

	promoteReq := &multipoolermanagerdatapb.PromoteRequest{
		ConsensusTerm:         term,
		ExpectedLsn:           expectedLSN,
		SyncReplicationConfig: syncConfig,
		Force:                 false,
		Reason:                reason,
		CoordinatorId:         coordinatorIDStr,
		CohortMembers:         cohortMembers,
		AcceptedMembers:       acceptedMembers,
	}
	_, err = c.rpcClient.Promote(ctx, candidate.MultiPooler, promoteReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to promote candidate")
	}

	c.logger.InfoContext(ctx, "Candidate promoted successfully", "node", candidate.MultiPooler.Id.Name)

	return nil
}

// EstablishLeader implements stage 6 of the consensus protocol:
// - Start heartbeat writer on the primary
// - Enable serving (if needed)
func (c *Coordinator) EstablishLeader(ctx context.Context, candidate *multiorchdatapb.PoolerHealthState, term int64) error {
	// The Promote RPC already handles:
	// 1. Starting the heartbeat writer
	// 2. Enabling serving
	// 3. Updating the consensus term
	//
	// So this function is mostly a placeholder for any additional
	// finalization steps that might be needed in the future.

	c.logger.InfoContext(ctx, "Leader established",
		"node", candidate.MultiPooler.Id.Name,
		"term", term)

	// Verify the leader is actually serving
	stateReq := &multipoolermanagerdatapb.StateRequest{}
	state, err := c.rpcClient.State(ctx, candidate.MultiPooler, stateReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to verify leader status")
	}

	if state.State != "ready" {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"leader is not in ready state: %s", state.State)
	}

	return nil
}

// preVote performs a pre-election check to determine if an election is likely to succeed.
// This prevents disruptive elections that would fail due to:
// 1. Insufficient healthy poolers to form a quorum (based on durability policy)
// 2. Another coordinator recently started an election (within last 10 seconds)
//
// Returns (canProceed, reason) where canProceed indicates if election should proceed.
func (c *Coordinator) preVote(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState, quorumRule *clustermetadatapb.QuorumRule, proposedTerm int64) (bool, string) {
	now := time.Now()
	const recentAcceptanceWindow = 4 * time.Second

	// Check 1: Verify we have enough healthy initialized poolers with consensus term data
	// PreVote is conservative and doesn't handle bootstrap - if poolers lack consensus
	// term information, we can't make an informed decision about election safety.
	var healthyInitializedPoolers []*multiorchdatapb.PoolerHealthState
	for _, pooler := range cohort {
		if pooler.IsLastCheckValid && pooler.IsInitialized && pooler.ConsensusTerm != nil {
			healthyInitializedPoolers = append(healthyInitializedPoolers, pooler)
		}
	}

	c.logger.InfoContext(ctx, "pre-vote health check",
		"healthy_initialized_poolers", len(healthyInitializedPoolers),
		"total_poolers", len(cohort),
		"quorum_type", quorumRule.QuorumType,
		"required_count", quorumRule.RequiredCount,
		"proposed_term", proposedTerm)

	// Validate we have enough healthy initialized poolers to satisfy quorum
	if err := c.ValidateQuorum(quorumRule, cohort, healthyInitializedPoolers); err != nil {
		return false, fmt.Sprintf("insufficient healthy initialized poolers for quorum: %v", err)
	}

	// Check 2: Has another coordinator recently started an election?
	// If we detect a recent term acceptance (within the last 10 seconds), back off
	// to give the other coordinator a chance to complete their election.
	for _, pooler := range healthyInitializedPoolers {
		// Check if this pooler recently accepted a term from another coordinator
		if pooler.ConsensusTerm.LastAcceptanceTime != nil {
			lastAcceptanceTime := pooler.ConsensusTerm.LastAcceptanceTime.AsTime()
			timeSinceAcceptance := now.Sub(lastAcceptanceTime)

			// If the acceptance was recent (within our window), back off
			if timeSinceAcceptance < recentAcceptanceWindow && timeSinceAcceptance >= 0 {
				c.logger.InfoContext(ctx, "detected recent term acceptance, backing off to avoid disruption",
					"pooler", pooler.MultiPooler.Id.Name,
					"accepted_term", pooler.ConsensusTerm.TermNumber,
					"accepted_from", pooler.ConsensusTerm.AcceptedTermFromCoordinatorId,
					"time_since_acceptance", timeSinceAcceptance,
					"backoff_window", recentAcceptanceWindow)

				return false, fmt.Sprintf("another coordinator started election recently (%v ago), backing off to avoid disruption",
					timeSinceAcceptance.Round(time.Millisecond))
			}
		}
	}

	c.logger.InfoContext(ctx, "pre-vote check passed",
		"proposed_term", proposedTerm,
		"healthy_initialized_poolers", len(healthyInitializedPoolers))

	return true, ""
}
