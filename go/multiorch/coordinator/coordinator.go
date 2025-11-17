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
	"log/slog"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// Coordinator orchestrates consensus-based leader election for shards.
// It implements the consensus protocol from multigres-consensus-design-v2.md.
type Coordinator struct {
	coordinatorID *clustermetadatapb.ID
	topoStore     topo.Store
	logger        *slog.Logger
}

// NewCoordinator creates a new coordinator instance.
func NewCoordinator(coordinatorID *clustermetadatapb.ID, topoStore topo.Store, logger *slog.Logger) *Coordinator {
	return &Coordinator{
		coordinatorID: coordinatorID,
		topoStore:     topoStore,
		logger:        logger,
	}
}

// AppointLeader orchestrates the full consensus protocol to appoint a new leader
// for the given shard. It operates on a cohort of nodes (all nodes in the shard).
//
// The process follows these stages:
// 0. Load durability policy (quorum rule)
// 1-5. BeginTerm: Recruit nodes, establish consensus on term and candidate, validate quorum
// 6. Propagate: Setup replication topology
// 7. Establish: Start heartbeat writer, enable serving
//
// Returns an error if any stage fails. The operation is idempotent and can be
// retried safely.
func (c *Coordinator) AppointLeader(ctx context.Context, shardID string, cohort []*Node, database string) error {
	c.logger.InfoContext(ctx, "Starting leader appointment",
		"shard", shardID,
		"database", database,
		"cohort_size", len(cohort))

	if len(cohort) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort is empty for shard %s", shardID)
	}

	// Stage 0: Load durability policy from any available node
	c.logger.InfoContext(ctx, "Loading durability policy", "shard", shardID)
	quorumRule, err := c.LoadQuorumRule(ctx, cohort, database)
	if err != nil {
		return mterrors.Wrap(err, "failed to load durability policy")
	}

	c.logger.InfoContext(ctx, "Loaded durability policy",
		"shard", shardID,
		"quorum_type", quorumRule.QuorumType,
		"required_count", quorumRule.RequiredCount,
		"description", quorumRule.Description)

	// Stage 1-5: BeginTerm (recruit nodes, get consensus, validate quorum)
	c.logger.InfoContext(ctx, "Stage 1-5: Beginning term", "shard", shardID)
	candidate, standbys, term, err := c.BeginTerm(ctx, shardID, cohort, quorumRule)
	if err != nil {
		return mterrors.Wrap(err, "BeginTerm failed")
	}

	c.logger.InfoContext(ctx, "BeginTerm succeeded",
		"shard", shardID,
		"term", term,
		"candidate", candidate.ID.Name,
		"standbys", len(standbys))

	// Stage 6: Propagate (setup replication within shard)
	c.logger.InfoContext(ctx, "Stage 6: Propagating replication", "shard", shardID)
	if err := c.Propagate(ctx, candidate, standbys, term); err != nil {
		return mterrors.Wrap(err, "Propagate failed")
	}

	c.logger.InfoContext(ctx, "Propagate succeeded", "shard", shardID)

	// Stage 7: Establish (start heartbeat, enable serving)
	c.logger.InfoContext(ctx, "Stage 7: Establishing leader", "shard", shardID)
	if err := c.EstablishLeader(ctx, candidate, term); err != nil {
		return mterrors.Wrap(err, "EstablishLeader failed")
	}

	c.logger.InfoContext(ctx, "EstablishLeader succeeded", "shard", shardID)

	// Update topology to reflect new primary
	if err := c.updateTopology(ctx, candidate, standbys); err != nil {
		// Log but don't fail - the leader is established, topology is just metadata
		c.logger.WarnContext(ctx, "Failed to update topology",
			"shard", shardID,
			"error", err)
	}

	c.logger.InfoContext(ctx, "Leader appointment complete",
		"shard", shardID,
		"leader", candidate.ID.Name)

	// Async: Repair excluded nodes in this shard
	// TODO: Implement RepairExcluded
	// go c.RepairExcluded(context.Background(), candidate, shardID)

	return nil
}

// updateTopology updates the topology store to reflect the new primary and standbys.
func (c *Coordinator) updateTopology(ctx context.Context, candidate *Node, standbys []*Node) error {
	// Update candidate to PRIMARY type
	_, err := c.topoStore.UpdateMultiPoolerFields(ctx, candidate.ID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_PRIMARY
		return nil
	})
	if err != nil {
		return mterrors.Wrap(err, "failed to update candidate to PRIMARY")
	}

	// Update standbys to REPLICA type
	for _, standby := range standbys {
		_, err := c.topoStore.UpdateMultiPoolerFields(ctx, standby.ID, func(mp *clustermetadatapb.MultiPooler) error {
			mp.Type = clustermetadatapb.PoolerType_REPLICA
			return nil
		})
		if err != nil {
			// Log but continue with other standbys
			c.logger.WarnContext(ctx, "Failed to update standby type",
				"node", standby.ID.Name,
				"error", err)
		}
	}

	return nil
}

// GetShardNodes retrieves all multipooler nodes for a given shard from the topology.
func (c *Coordinator) GetShardNodes(ctx context.Context, cell string, database string, tablegroup string, shardID string) ([]*Node, error) {
	// Get all multipoolers in the cell for this specific shard
	poolers, err := c.topoStore.GetMultiPoolersByCell(ctx, cell, &topo.GetMultiPoolersByCellOptions{
		DatabaseShard: &topo.DatabaseShard{
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

	// Create Node instances with gRPC clients
	nodes, err := CreateNodes(ctx, poolers)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to create node clients")
	}

	return nodes, nil
}
