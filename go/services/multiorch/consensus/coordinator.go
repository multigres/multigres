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

package consensus

import (
	"context"
	"log/slog"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// Coordinator orchestrates consensus-based leader election for shards.
// It implements the consensus protocol from multigres-consensus-design-v2.md.
type Coordinator struct {
	coordinatorID *clustermetadatapb.ID
	topoStore     topoclient.Store
	rpcClient     rpcclient.MultiPoolerClient
	logger        *slog.Logger
}

// NewCoordinator creates a new coordinator instance.
func NewCoordinator(coordinatorID *clustermetadatapb.ID, topoStore topoclient.Store, rpcClient rpcclient.MultiPoolerClient, logger *slog.Logger) *Coordinator {
	return &Coordinator{
		coordinatorID: coordinatorID,
		topoStore:     topoStore,
		rpcClient:     rpcClient,
		logger:        logger,
	}
}

// AppointLeader orchestrates the full consensus protocol to appoint a new leader
// for the given shard. It operates on a cohort of nodes (all nodes in the shard).
//
// The process achieves the following goals (from multigres-consensus-design-v2.md):
//
//  1. Obtaining a term number: Discover max term from cached health state and increment
//  2. Revocation, Candidacy, Discovery: BeginTerm recruits nodes under the new term,
//     achieving revocation (no old leader can complete requests), candidacy (recruited
//     nodes contain a suitable candidate), and discovery (identify most progressed node)
//  3. Propagation: Make the timeline durable under the new term by configuring replication
//     and promoting the candidate. Timeline becomes durable when quorum rules are satisfied.
//  4. Establishment: Finalize the leader by starting heartbeat and enabling serving
//
// Returns an error if any stage fails. The operation is idempotent and can be
// retried safely.
func (c *Coordinator) AppointLeader(ctx context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, database string, reason string) error {
	c.logger.InfoContext(ctx, "Starting leader appointment",
		"shard", shardID,
		"database", database,
		"cohort_size", len(cohort))

	if len(cohort) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort is empty for shard %s", shardID)
	}

	quorumRule, err := c.LoadQuorumRule(ctx, cohort, database)
	if err != nil {
		return mterrors.Wrap(err, "failed to load durability policy")
	}

	c.logger.InfoContext(ctx, "Loaded durability policy",
		"shard", shardID,
		"quorum_type", quorumRule.QuorumType,
		"required_count", quorumRule.RequiredCount,
		"description", quorumRule.Description)

	// Goal 1: Obtaining a term number
	// Discover max term from cached health state and increment to get proposed term
	maxTerm, err := c.discoverMaxTerm(cohort)
	if err != nil {
		return mterrors.Wrap(err, "failed to discover max term")
	}
	proposedTerm := maxTerm + 1

	// PreVote - validate that leadership change is likely to succeed
	canProceed, preVoteReason := c.preVote(ctx, cohort, quorumRule, proposedTerm)
	if !canProceed {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"pre-vote failed for shard %s: %s", shardID, preVoteReason)
	}

	// Goal 2: Revocation, Candidacy, Discovery
	// BeginTerm recruits nodes under the new term, which achieves:
	// - Revocation: recruited nodes accept new term, preventing old leader from completing requests
	// - Discovery: identify the most progressed node based on WAL position
	// - Candidacy: validate recruited nodes satisfy quorum rules for the candidate
	candidate, standbys, term, err := c.BeginTerm(ctx, shardID, cohort, quorumRule, proposedTerm)
	if err != nil {
		return mterrors.Wrap(err, "BeginTerm failed")
	}

	c.logger.InfoContext(ctx, "Recruitment succeeded",
		"shard", shardID,
		"term", term,
		"candidate", candidate.MultiPooler.Id.Name,
		"standbys", len(standbys))

	// Goal 3: Propagation
	// Make the timeline durable under the new term by configuring replication
	// and promoting the candidate. The timeline becomes durable when quorum rules
	// are satisfied (synchronous replication configured, promotion completes).
	c.logger.InfoContext(ctx, "Propagating leadership change", "shard", shardID)

	// Reconstruct the recruited list (nodes that accepted the term).
	// This is candidate + standbys.
	//
	// The recruited list may differ from the original cohort in these scenarios:
	// - Some nodes in the cohort were unreachable during BeginTerm
	// - Some nodes rejected the term (e.g., had a higher term already)
	// - Some nodes failed validation (e.g., insufficient LSN)
	recruited := make([]*multiorchdatapb.PoolerHealthState, 0, len(standbys)+1)
	recruited = append(recruited, candidate)
	recruited = append(recruited, standbys...)

	if err := c.Propagate(ctx, candidate, standbys, term, quorumRule, reason, cohort, recruited); err != nil {
		return mterrors.Wrap(err, "Propagate failed")
	}

	c.logger.InfoContext(ctx, "Propagation succeeded", "shard", shardID)

	// Goal 4: Establishment
	// Finalize the leader by verifying heartbeat is running and serving is enabled.
	// At this point, the candidate has become the new leader with the delegated term.
	c.logger.InfoContext(ctx, "Establishing leader", "shard", shardID)
	if err := c.EstablishLeader(ctx, candidate, term); err != nil {
		return mterrors.Wrap(err, "EstablishLeader failed")
	}

	c.logger.InfoContext(ctx, "Establishment succeeded", "shard", shardID)

	// Update topology to reflect new primary
	if err := c.updateTopology(ctx, candidate, standbys); err != nil {
		// Log but don't fail - the leader is established, topology is just metadata
		c.logger.WarnContext(ctx, "Failed to update topology",
			"shard", shardID,
			"error", err)
	}

	c.logger.InfoContext(ctx, "Leadership change complete",
		"shard", shardID,
		"leader", candidate.MultiPooler.Id.Name,
		"term", term)

	// Async: Repair excluded nodes in this shard
	// TODO: Implement RepairExcluded
	// go c.RepairExcluded(context.Background(), candidate, shardID)

	return nil
}

// updateTopology updates the topology store to reflect the new primary and standbys.
func (c *Coordinator) updateTopology(ctx context.Context, candidate *multiorchdatapb.PoolerHealthState, standbys []*multiorchdatapb.PoolerHealthState) error {
	// Update candidate to PRIMARY type
	_, err := c.topoStore.UpdateMultiPoolerFields(ctx, candidate.MultiPooler.Id, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_PRIMARY
		return nil
	})
	if err != nil {
		return mterrors.Wrap(err, "failed to update candidate to PRIMARY")
	}

	// Update standbys to REPLICA type
	for _, standby := range standbys {
		_, err := c.topoStore.UpdateMultiPoolerFields(ctx, standby.MultiPooler.Id, func(mp *clustermetadatapb.MultiPooler) error {
			mp.Type = clustermetadatapb.PoolerType_REPLICA
			return nil
		})
		if err != nil {
			// Log but continue with other standbys
			c.logger.WarnContext(ctx, "Failed to update standby type",
				"node", standby.MultiPooler.Id.Name,
				"error", err)
		}
	}

	return nil
}

// GetCoordinatorID returns the coordinator's ID.
func (c *Coordinator) GetCoordinatorID() *clustermetadatapb.ID {
	return c.coordinatorID
}

// GetShardNodes retrieves all multipooler nodes for a given shard from the topology.
func (c *Coordinator) GetShardNodes(ctx context.Context, cell string, database string, tablegroup string, shardID string) ([]*multiorchdatapb.PoolerHealthState, error) {
	// Get all multipoolers in the cell for this specific shard
	poolers, err := c.topoStore.GetMultiPoolersByCell(ctx, cell, &topoclient.GetMultiPoolersByCellOptions{
		DatabaseShard: &topoclient.DatabaseShard{
			Database:   database,
			TableGroup: tablegroup,
			Shard:      shardID,
		},
	})
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to get multipoolers from topology")
	}

	if len(poolers) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND,
			"no multipoolers found for shard %s in cell %s", shardID, cell)
	}

	// Convert topology poolers to PoolerHealthState instances
	poolerHealths := make([]*multiorchdatapb.PoolerHealthState, 0, len(poolers))
	for _, poolerInfo := range poolers {
		ph := &multiorchdatapb.PoolerHealthState{
			MultiPooler: poolerInfo.MultiPooler,
		}
		poolerHealths = append(poolerHealths, ph)
	}

	return poolerHealths, nil
}
