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

	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multipooler/lsn"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// BeginTerm implements stages 1-5 of the consensus protocol:
// 1. Discover max term from all nodes
// 2. Increment to get new term
// 3. Select candidate based on WAL position
// 4. Send BeginTerm RPC to all nodes in parallel
// 5. Validate quorum using durability policy
//
// Returns the candidate node, standbys, the new term, and any error.
func (c *Coordinator) BeginTerm(ctx context.Context, shardID string, cohort []*Node, quorumRule *clustermetadatapb.QuorumRule) (*Node, []*Node, int64, error) {
	// Stage 1: Obtain Term Number - Query all nodes for max term
	c.logger.InfoContext(ctx, "Discovering max term", "shard", shardID, "cohort_size", len(cohort))
	maxTerm, err := c.discoverMaxTerm(ctx, cohort)
	if err != nil {
		return nil, nil, 0, mterrors.Wrap(err, "failed to discover max term")
	}

	// Stage 2: Increment term
	newTerm := maxTerm + 1
	c.logger.InfoContext(ctx, "New term", "shard", shardID, "term", newTerm)

	// Stage 3: Select Candidate - Choose based on WAL position
	candidate, err := c.selectCandidate(ctx, cohort)
	if err != nil {
		return nil, nil, 0, mterrors.Wrap(err, "failed to select candidate")
	}

	c.logger.InfoContext(ctx, "Selected candidate", "shard", shardID, "candidate", candidate.ID.Name)

	// Stage 4: Recruit Nodes - Send BeginTerm RPC to all nodes in parallel
	recruited, err := c.recruitNodes(ctx, cohort, newTerm, candidate)
	if err != nil {
		return nil, nil, 0, mterrors.Wrap(err, "failed to recruit nodes")
	}

	c.logger.InfoContext(ctx, "Recruited nodes", "shard", shardID, "count", len(recruited))

	// Stage 5: Validate Quorum using durability policy
	c.logger.InfoContext(ctx, "Validating quorum",
		"shard", shardID,
		"quorum_type", quorumRule.QuorumType,
		"required_count", quorumRule.RequiredCount)

	if err := c.ValidateQuorum(quorumRule, cohort, recruited); err != nil {
		return nil, nil, 0, mterrors.Wrapf(err, "quorum validation failed for shard %s", shardID)
	}

	// Separate candidate from standbys
	var standbys []*Node
	for _, node := range recruited {
		if node.ID.Name != candidate.ID.Name {
			standbys = append(standbys, node)
		}
	}

	return candidate, standbys, newTerm, nil
}

// discoverMaxTerm queries all nodes in parallel to find the maximum consensus term.
func (c *Coordinator) discoverMaxTerm(ctx context.Context, cohort []*Node) (int64, error) {
	type result struct {
		term int64
		err  error
	}

	results := make(chan result, len(cohort))
	var wg sync.WaitGroup

	for _, node := range cohort {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			req := &consensusdatapb.StatusRequest{}
			resp, err := n.RpcClient.ConsensusStatus(ctx, n.Pooler, req)
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{term: resp.CurrentTerm}
		}(node)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Find max term
	var maxTerm int64
	var errs []error
	for r := range results {
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		if r.term > maxTerm {
			maxTerm = r.term
		}
	}

	// If we got at least one successful response, use that
	if maxTerm > 0 || len(errs) < len(cohort) {
		return maxTerm, nil
	}

	// All nodes failed
	if len(errs) > 0 {
		return 0, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"failed to query term from any node: %v", errs[0])
	}

	return maxTerm, nil
}

// selectCandidate chooses the best candidate based on WAL position and health.
func (c *Coordinator) selectCandidate(ctx context.Context, cohort []*Node) (*Node, error) {
	type nodeStatus struct {
		node        *Node
		walPosition string
		healthy     bool
	}

	statuses := make([]nodeStatus, 0, len(cohort))

	// Query status from all nodes
	for _, node := range cohort {
		req := &consensusdatapb.StatusRequest{}
		resp, err := node.RpcClient.ConsensusStatus(ctx, node.Pooler, req)
		if err != nil {
			c.logger.WarnContext(ctx, "Failed to get status from node",
				"node", node.ID.Name,
				"error", err)
			continue
		}

		status := nodeStatus{
			node:    node,
			healthy: resp.IsHealthy,
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

	// Select node with most advanced WAL position using proper PostgreSQL LSN comparison
	var bestCandidate *Node
	var bestWAL string

	for _, status := range statuses {
		if !status.healthy {
			continue
		}

		// Validate LSN format first
		if _, err := lsn.Parse(status.walPosition); err != nil {
			c.logger.WarnContext(ctx, "Invalid LSN format, skipping node",
				"node", status.node.ID.Name,
				"lsn", status.walPosition,
				"error", err)
			continue
		}

		if bestCandidate == nil {
			bestCandidate = status.node
			bestWAL = status.walPosition
			continue
		}

		cmp, err := lsn.CompareStrings(status.walPosition, bestWAL)
		if err != nil {
			c.logger.WarnContext(ctx, "Failed to compare LSN, skipping node",
				"node", status.node.ID.Name,
				"lsn", status.walPosition,
				"error", err)
			continue
		}

		if cmp > 0 { // status.walPosition > bestWAL
			bestCandidate = status.node
			bestWAL = status.walPosition
		}
	}

	if bestCandidate == nil {
		// Fall back to first available node if no healthy nodes
		bestCandidate = statuses[0].node
		c.logger.WarnContext(ctx, "No healthy nodes, using first available",
			"node", bestCandidate.ID.Name)
	}

	return bestCandidate, nil
}

// recruitNodes sends BeginTerm RPC to all nodes in parallel and returns those that accepted.
func (c *Coordinator) recruitNodes(ctx context.Context, cohort []*Node, term int64, candidate *Node) ([]*Node, error) {
	type result struct {
		node     *Node
		accepted bool
		err      error
	}

	results := make(chan result, len(cohort))
	var wg sync.WaitGroup

	for _, node := range cohort {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			req := &consensusdatapb.BeginTermRequest{
				Term:        term,
				CandidateId: c.coordinatorID,
			}
			resp, err := n.RpcClient.BeginTerm(ctx, n.Pooler, req)
			if err != nil {
				results <- result{node: n, err: err}
				return
			}
			results <- result{node: n, accepted: resp.Accepted}
		}(node)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect accepted nodes
	var recruited []*Node
	for r := range results {
		if r.err != nil {
			c.logger.WarnContext(ctx, "BeginTerm failed for node",
				"node", r.node.ID.Name,
				"error", r.err)
			continue
		}
		if r.accepted {
			recruited = append(recruited, r.node)
			c.logger.InfoContext(ctx, "Node accepted term",
				"node", r.node.ID.Name,
				"term", term)
		} else {
			c.logger.WarnContext(ctx, "Node rejected term",
				"node", r.node.ID.Name,
				"term", term)
		}
	}

	return recruited, nil
}

// buildSyncReplicationConfig creates synchronous replication configuration based on the quorum policy.
// Returns nil if synchronous replication should not be configured (required_count=1 or no standbys).
// For MULTI_CELL_ANY_N policies, excludes standbys in the same cell as the candidate (primary).
func (c *Coordinator) buildSyncReplicationConfig(quorumRule *clustermetadatapb.QuorumRule, standbys []*Node, candidate *Node) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest, error) {
	requiredCount := int(quorumRule.RequiredCount)

	// Determine async fallback mode (default to REJECT if unset)
	asyncFallback := quorumRule.AsyncFallback
	if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_UNKNOWN {
		asyncFallback = clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT
	}

	// If required_count is 1, don't configure synchronous replication
	// With required_count=1, only the primary itself is needed for quorum, so async replication is sufficient
	if requiredCount == 1 {
		c.logger.Info("Skipping synchronous replication configuration",
			"required_count", requiredCount,
			"standbys_count", len(standbys),
			"reason", "async replication sufficient for quorum")
		return nil, nil
	}

	// If there are no standbys, check async fallback mode
	if len(standbys) == 0 {
		if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				fmt.Sprintf("cannot establish synchronous replication: no standbys available (required %d standbys, async_fallback=REJECT)",
					requiredCount-1))
		}
		c.logger.Info("Skipping synchronous replication configuration",
			"required_count", requiredCount,
			"standbys_count", 0,
			"reason", "async replication sufficient for quorum")
		return nil, nil
	}

	// For MULTI_CELL_ANY_N, filter out standbys in the same cell as the primary
	// This ensures that synchronous replication requires acknowledgment from different cells
	eligibleStandbys := standbys
	if quorumRule.QuorumType == clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N {
		eligibleStandbys = c.filterStandbysByCell(candidate, standbys)

		c.logger.Info("Filtered standbys for MULTI_CELL_ANY_N",
			"candidate_cell", candidate.ID.Cell,
			"total_standbys", len(standbys),
			"eligible_standbys", len(eligibleStandbys),
			"excluded_same_cell", len(standbys)-len(eligibleStandbys))

		// If no eligible standbys remain after filtering, check async fallback mode
		if len(eligibleStandbys) == 0 {
			if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT {
				return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
					fmt.Sprintf("cannot establish synchronous replication: no eligible standbys in different cells (candidate_cell=%s, async_fallback=REJECT)",
						candidate.ID.Cell))
			}
			c.logger.Warn("No eligible standbys in different cells, using async replication",
				"candidate_cell", candidate.ID.Cell,
				"total_standbys", len(standbys))
			return nil, nil
		}
	}

	// Calculate num_sync: required_count - 1 (since primary counts as 1)
	// This ensures that primary + num_sync standbys = required_count total nodes/cells
	requiredNumSync := requiredCount - 1

	// Check if we have enough standbys to meet the requirement
	if requiredNumSync > len(eligibleStandbys) {
		if asyncFallback == clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				fmt.Sprintf("cannot establish synchronous replication: insufficient standbys (required %d standbys, available %d, async_fallback=REJECT)",
					requiredNumSync, len(eligibleStandbys)))
		}
		c.logger.Warn("Not enough standbys for required sync count, using all available",
			"required_num_sync", requiredNumSync,
			"available_standbys", len(eligibleStandbys))
	}

	// Cap num_sync at the number of available eligible standbys
	numSync := int32(requiredNumSync)
	if int(numSync) > len(eligibleStandbys) {
		numSync = int32(len(eligibleStandbys))
	}

	// Convert standby nodes to IDs
	standbyIDs := make([]*clustermetadatapb.ID, len(eligibleStandbys))
	for i, standby := range eligibleStandbys {
		standbyIDs[i] = standby.ID
	}

	c.logger.Info("Configuring synchronous replication",
		"quorum_type", quorumRule.QuorumType,
		"required_count", requiredCount,
		"num_sync", numSync,
		"total_standbys", len(eligibleStandbys))

	return &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
		SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE,
		SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
		NumSync:           numSync,
		StandbyIds:        standbyIDs,
		ReloadConfig:      true,
	}, nil
}

// filterStandbysByCell returns standbys that are NOT in the same cell as the candidate.
// Used for MULTI_CELL_ANY_N to ensure synchronous replication spans multiple cells.
func (c *Coordinator) filterStandbysByCell(candidate *Node, standbys []*Node) []*Node {
	candidateCell := candidate.ID.Cell
	filtered := make([]*Node, 0, len(standbys))

	for _, standby := range standbys {
		if standby.ID.Cell != candidateCell {
			filtered = append(filtered, standby)
		}
	}

	return filtered
}

// Propagate implements stage 5 of the consensus protocol:
// - Promote the candidate to primary
// - Configure standbys to replicate from the new primary
// - Configure synchronous replication based on quorum policy
func (c *Coordinator) Propagate(ctx context.Context, candidate *Node, standbys []*Node, term int64, quorumRule *clustermetadatapb.QuorumRule) error {
	// Build synchronous replication configuration based on quorum policy
	syncConfig, err := c.buildSyncReplicationConfig(quorumRule, standbys, candidate)
	if err != nil {
		return mterrors.Wrap(err, "failed to build synchronous replication config")
	}

	// Promote candidate to primary
	c.logger.InfoContext(ctx, "Promoting candidate to primary",
		"node", candidate.ID.Name,
		"term", term)

	// Get current WAL position before promotion (for validation)
	statusReq := &consensusdatapb.StatusRequest{}
	status, err := candidate.RpcClient.ConsensusStatus(ctx, candidate.Pooler, statusReq)
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
					"node", candidate.ID.Name,
					"target_lsn", expectedLSN)

				waitReq := &multipoolermanagerdatapb.WaitForLSNRequest{
					TargetLsn: expectedLSN,
				}
				if _, err := candidate.RpcClient.WaitForLSN(ctx, candidate.Pooler, waitReq); err != nil {
					return mterrors.Wrapf(err, "candidate failed to replay WAL to %s", expectedLSN)
				}
			}
		}
	}

	promoteReq := &multipoolermanagerdatapb.PromoteRequest{
		ConsensusTerm:         term,
		ExpectedLsn:           expectedLSN,
		SyncReplicationConfig: syncConfig,
		Force:                 false,
	}
	_, err = candidate.RpcClient.Promote(ctx, candidate.Pooler, promoteReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to promote candidate")
	}

	c.logger.InfoContext(ctx, "Candidate promoted successfully", "node", candidate.ID.Name)

	// Configure standbys to replicate from the new primary
	var wg sync.WaitGroup
	errChan := make(chan error, len(standbys))

	for _, standby := range standbys {
		wg.Add(1)
		go func(s *Node) {
			defer wg.Done()
			c.logger.InfoContext(ctx, "Configuring standby replication",
				"standby", s.ID.Name,
				"primary", candidate.ID.Name)

			setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
				Host:                  candidate.Hostname,
				Port:                  candidate.Port,
				CurrentTerm:           term,
				StopReplicationBefore: false,
				StartReplicationAfter: false,
				Force:                 false,
			}
			if _, err := s.RpcClient.SetPrimaryConnInfo(ctx, s.Pooler, setPrimaryReq); err != nil {
				errChan <- mterrors.Wrapf(err, "failed to configure standby %s", s.ID.Name)
				return
			}

			c.logger.InfoContext(ctx, "Standby configured successfully", "standby", s.ID.Name)
		}(standby)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		// Log all errors but continue - partial success is acceptable
		for _, err := range errs {
			c.logger.WarnContext(ctx, "Standby configuration failed", "error", err)
		}
		// Don't fail the whole operation if some standbys failed
		// The primary is established, standbys can be fixed later
	}

	return nil
}

// EstablishLeader implements stage 6 of the consensus protocol:
// - Start heartbeat writer on the primary
// - Enable serving (if needed)
func (c *Coordinator) EstablishLeader(ctx context.Context, candidate *Node, term int64) error {
	// The Promote RPC already handles:
	// 1. Starting the heartbeat writer
	// 2. Enabling serving
	// 3. Updating the consensus term
	//
	// So this function is mostly a placeholder for any additional
	// finalization steps that might be needed in the future.

	c.logger.InfoContext(ctx, "Leader established",
		"node", candidate.ID.Name,
		"term", term)

	// Verify the leader is actually serving
	stateReq := &multipoolermanagerdatapb.StateRequest{}
	state, err := candidate.RpcClient.State(ctx, candidate.Pooler, stateReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to verify leader status")
	}

	if state.State != "ready" {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"leader is not in ready state: %s", state.State)
	}

	return nil
}
