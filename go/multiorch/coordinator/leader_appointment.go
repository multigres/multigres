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

	"github.com/jackc/pglogrepl"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
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
func (c *Coordinator) BeginTerm(ctx context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, quorumRule *clustermetadatapb.QuorumRule) (*multiorchdatapb.PoolerHealthState, []*multiorchdatapb.PoolerHealthState, int64, error) {
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

	c.logger.InfoContext(ctx, "Selected candidate", "shard", shardID, "candidate", candidate.MultiPooler.Id.Name)

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
	var standbys []*multiorchdatapb.PoolerHealthState
	for _, node := range recruited {
		if node.MultiPooler.Id.Name != candidate.MultiPooler.Id.Name {
			standbys = append(standbys, node)
		}
	}

	return candidate, standbys, newTerm, nil
}

// discoverMaxTerm queries all nodes in parallel to find the maximum consensus term.
func (c *Coordinator) discoverMaxTerm(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState) (int64, error) {
	type result struct {
		term int64
		err  error
	}

	results := make(chan result, len(cohort))
	var wg sync.WaitGroup

	for _, pooler := range cohort {
		wg.Add(1)
		go func(n *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			req := &consensusdatapb.StatusRequest{}
			resp, err := c.rpcClient.ConsensusStatus(ctx, n.MultiPooler, req)
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{term: resp.CurrentTerm}
		}(pooler)
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
// - Promote the candidate to primary
// - Configure standbys to replicate from the new primary
// - Configure synchronous replication based on quorum policy
func (c *Coordinator) Propagate(ctx context.Context, candidate *multiorchdatapb.PoolerHealthState, standbys []*multiorchdatapb.PoolerHealthState, term int64, quorumRule *clustermetadatapb.QuorumRule) error {
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

	promoteReq := &multipoolermanagerdatapb.PromoteRequest{
		ConsensusTerm:         term,
		ExpectedLsn:           expectedLSN,
		SyncReplicationConfig: syncConfig,
		Force:                 false,
	}
	_, err = c.rpcClient.Promote(ctx, candidate.MultiPooler, promoteReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to promote candidate")
	}

	c.logger.InfoContext(ctx, "Candidate promoted successfully", "node", candidate.MultiPooler.Id.Name)

	// Configure standbys to replicate from the new primary
	var wg sync.WaitGroup
	errChan := make(chan error, len(standbys))

	for _, standby := range standbys {
		wg.Add(1)
		go func(s *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			c.logger.InfoContext(ctx, "Configuring standby replication",
				"standby", s.MultiPooler.Id.Name,
				"primary", candidate.MultiPooler.Id.Name)

			setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
				PrimaryPoolerId:       candidate.MultiPooler.Id.Name,
				Host:                  candidate.MultiPooler.Hostname,
				Port:                  candidate.MultiPooler.PortMap["postgres"],
				CurrentTerm:           term,
				StopReplicationBefore: false,
				StartReplicationAfter: false,
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
