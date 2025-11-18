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
	"sync"

	"github.com/multigres/multigres/go/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// BeginTerm implements stages 1-4 of the consensus protocol:
// 1. Discover max term from all nodes
// 2. Increment to get new term
// 3. Select candidate based on WAL position
// 4. Send BeginTerm RPC to all nodes in parallel
// 5. Validate quorum (majority acceptance)
//
// Returns the candidate node, standbys, the new term, and any error.
func (c *Coordinator) BeginTerm(ctx context.Context, shardID string, cohort []*Node) (*Node, []*Node, int64, error) {
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

	// Stage 5: Validate Quorum
	// TODO(durability-policy): Replace hardcoded majority with pluggable policy evaluation
	// Future: Load ruleset from Topo Server and validate against policy (e.g., multi-AZ)
	quorumSize := len(cohort)/2 + 1
	if len(recruited) < quorumSize {
		return nil, nil, 0, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"insufficient quorum in shard %s: got %d/%d nodes (need %d)",
			shardID, len(recruited), len(cohort), quorumSize)
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
			resp, err := n.ConsensusStatus(ctx)
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
		resp, err := node.ConsensusStatus(ctx)
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

	// Select node with most advanced WAL position
	// TODO: Implement proper LSN comparison (PostgreSQL format: X/XXXXXXXX)
	// For now, use lexicographic comparison as a placeholder
	var bestCandidate *Node
	var bestWAL string

	for _, status := range statuses {
		if !status.healthy {
			continue
		}

		if bestCandidate == nil || status.walPosition > bestWAL {
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
			resp, err := n.BeginTerm(ctx, term, c.coordinatorID)
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

// Propagate implements stage 5 of the consensus protocol:
// - Promote the candidate to primary
// - Configure standbys to replicate from the new primary
// - Wait for quorum to catch up (optional, based on sync replication config)
func (c *Coordinator) Propagate(ctx context.Context, candidate *Node, standbys []*Node, term int64) error {
	// Build synchronous replication configuration
	// For now, use FIRST method with all standbys
	syncConfig := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
		SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
		SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
		NumSync:           int32(len(standbys)),
		StandbyIds:        make([]*clustermetadatapb.ID, len(standbys)),
		ReloadConfig:      true,
	}

	// Convert standby IDs to the format expected by the RPC
	for i, standby := range standbys {
		syncConfig.StandbyIds[i] = standby.ID
	}

	// Promote candidate to primary
	c.logger.InfoContext(ctx, "Promoting candidate to primary",
		"node", candidate.ID.Name,
		"term", term)

	// Get current WAL position before promotion (for validation)
	status, err := candidate.ConsensusStatus(ctx)
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

				if err := candidate.WaitForLSN(ctx, expectedLSN); err != nil {
					return mterrors.Wrapf(err, "candidate failed to replay WAL to %s", expectedLSN)
				}
			}
		}
	}

	_, err = candidate.Promote(ctx, term, expectedLSN, syncConfig)
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

			err := s.SetPrimaryConnInfo(ctx, candidate.Hostname, candidate.Port, term)
			if err != nil {
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
	status, err := candidate.ManagerStatus(ctx)
	if err != nil {
		return mterrors.Wrap(err, "failed to verify leader status")
	}

	if status.State != "ready" {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"leader is not in ready state: %s", status.State)
	}

	return nil
}
