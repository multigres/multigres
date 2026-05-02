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
	"sync"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// Coordinator orchestrates consensus-based leader election for shards.
//
// TODO: PoolerStore should be reorganized to be shard-centric rather than pooler-centric.
type Coordinator struct {
	coordinatorID *clustermetadatapb.ID
	topoStore     topoclient.Store
	rpcClient     rpcclient.MultiPoolerClient
	logger        *slog.Logger

	// TODO: policyCache will go away when we start reading the policy from nodes instead of etcd.
	// This cache is a temporary way to avoid making failover depend on etcd.
	policyCache sync.Map // database name → *clustermetadatapb.DurabilityPolicy
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
// The process achieves the following goals:
//
//  1. Obtaining a term number: Discover max term from cached health state and increment
//  2. Revocation, Candidacy, Discovery: BeginTerm recruits nodes under the new term,
//     achieving revocation (no old leader can complete requests), candidacy (recruited
//     nodes contain a suitable candidate), and discovery (identify most progressed node)
//  3. Propagation, Establishment: Make the timeline durable under the new term by configuring replication
//     and promoting the candidate. Timeline becomes durable when quorum rules are satisfied.
//
// Returns an error if any stage fails. The operation is idempotent and can be
// retried safely.
func (c *Coordinator) AppointLeader(ctx context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, database string, reason string) (retErr error) {
	c.logger.InfoContext(ctx, "Starting leader appointment",
		"shard", shardID,
		"database", database,
		"cohort_size", len(cohort))

	if len(cohort) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort is empty for shard %s", shardID)
	}

	// TODO: Apply the policy from the nodes themselves instead of assuming the bootstrap policy is
	// still the durable shard policy. To do this, add durability_policy to PoolerHealthState so
	// the health check loop populates it, then read from the cohort here for preVote and from the
	// BeginTerm response after revocation. Note: GetDurabilityPolicy and CreateDurabilityPolicy
	// RPCs in multipoolermanagerdata.proto are currently unimplemented stubs in the pooler.
	policy, err := c.GetBootstrapPolicy(ctx, database)
	if err != nil {
		return mterrors.Wrap(err, "failed to load durability policy")
	}

	c.logger.InfoContext(ctx, "Loaded durability policy",
		"shard", shardID,
		"quorum_type", policy.QuorumType,
		"required_count", policy.RequiredCount,
		"description", policy.Description)

	// Goal 1: Obtaining a term number
	// Discover max term from cached health state and increment to get proposed term
	maxTerm, err := c.discoverMaxTerm(cohort)
	if err != nil {
		return mterrors.Wrap(err, "failed to discover max term")
	}
	proposedTerm := maxTerm + 1

	return c.appointLeaderWithTerm(ctx, shardID, cohort, policy, proposedTerm, reason)
}

// appointLeaderWithTerm is the shared core of AppointLeader and AppointInitialLeader.
// Given a resolved policy and proposed term, it runs preVote, BeginTerm, and
// EstablishLeadership.
func (c *Coordinator) appointLeaderWithTerm(ctx context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, policy *clustermetadatapb.DurabilityPolicy, proposedTerm int64, reason string) (retErr error) {
	// Parse the proto policy once into the typed DurabilityPolicy interface so
	// preVote, BeginTerm, and EstablishLeadership can call its quorum,
	// recruitment, and leader-config methods directly.
	durabilityPolicy, err := commonconsensus.NewPolicyFromProto(policy)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse durability policy")
	}

	// PreVote — validate that leadership change is likely to succeed.
	canProceed, preVoteReason := c.preVote(ctx, cohort, durabilityPolicy, proposedTerm)
	if !canProceed {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"pre-vote failed for shard %s: %s", shardID, preVoteReason)
	}

	// Goal 2: Revocation, Candidacy, Discovery
	// BeginTerm recruits nodes under the new term, which achieves:
	// - Revocation: recruited nodes accept new term, preventing old leader from completing requests
	// - Discovery: identify the most progressed node based on WAL position
	// - Candidacy: validate recruited nodes satisfy quorum rules for the candidate
	candidate, standbys, term, err := c.BeginTerm(ctx, shardID, cohort, durabilityPolicy, proposedTerm)
	if err != nil {
		return mterrors.Wrap(err, "BeginTerm failed")
	}

	c.logger.InfoContext(ctx, "Recruitment succeeded",
		"shard", shardID,
		"term", term,
		"candidate", candidate.MultiPooler.Id.Name,
		"standbys", len(standbys))

	// We know the candidate now — emit Started before establishing leadership.
	eventlog.Emit(ctx, c.logger, eventlog.Started, eventlog.PrimaryPromotion{
		NewPrimary: candidate.MultiPooler.Id.Name,
	})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, c.logger, eventlog.Success, eventlog.PrimaryPromotion{
				NewPrimary: candidate.MultiPooler.Id.Name,
			})
		} else {
			eventlog.Emit(ctx, c.logger, eventlog.Failed, eventlog.PrimaryPromotion{
				NewPrimary: candidate.MultiPooler.Id.Name,
			}, "error", retErr)
		}
	}()

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

	// Propagation and Establishment
	if err := c.EstablishLeadership(ctx, candidate, standbys, term, durabilityPolicy, reason, cohort, recruited); err != nil {
		return mterrors.Wrap(err, "EstablishLeadership failed")
	}

	c.logger.InfoContext(ctx, "Leadership established", "shard", shardID)
	return nil
}

// AppointInitialLeader orchestrates consensus leader election for a freshly bootstrapped
// shard where all poolers start at term 0. It skips term discovery (which would
// return 0 for brand-new nodes) and calls BeginTerm directly with term=1.
//
// Uses GetBootstrapPolicy (not AppointLeader's LoadQuorumRule) because freshly restored
// standbys report UNKNOWN pooler type, which causes LoadQuorumRule to fall back
// to majority quorum instead of the configured durability policy.
func (c *Coordinator) AppointInitialLeader(ctx context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, database string) error {
	if len(cohort) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort is empty for shard %s", shardID)
	}

	policy, err := c.GetBootstrapPolicy(ctx, database)
	if err != nil {
		return mterrors.Wrap(err, "failed to load durability policy from topology")
	}

	// Freshly bootstrapped shards start at term 0; skip discoverMaxTerm (which
	// would return 0 for brand-new nodes) and use term 1 directly.
	return c.appointLeaderWithTerm(ctx, shardID, cohort, policy, 1 /* initialTerm */, "ShardInit")
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

// GetBootstrapPolicy returns the durability policy for the given database by reading
// bootstrap_durability_policy from the topology Database record. The result is cached
// in memory since the policy is assumed not to change for the lifetime of this process.
//
// TODO: Once pooler status updates carry policy information, this should be replaced
// with a live policy loaded from the shard's nodes rather than the bootstrap record.
func (c *Coordinator) GetBootstrapPolicy(ctx context.Context, database string) (*clustermetadatapb.DurabilityPolicy, error) {
	if cached, ok := c.policyCache.Load(database); ok {
		return cached.(*clustermetadatapb.DurabilityPolicy), nil
	}

	db, err := c.topoStore.GetDatabase(ctx, database)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to get database %s from topology", database)
	}

	if db.BootstrapDurabilityPolicy == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"database %s has no bootstrap_durability_policy configured", database)
	}

	c.policyCache.Store(database, db.BootstrapDurabilityPolicy)
	return db.BootstrapDurabilityPolicy, nil
}
