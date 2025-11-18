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

package actions

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multiorch/coordinator"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// InitializationScenario represents the different ways a shard can be initialized
type InitializationScenario int

const (
	// ScenarioUnknown indicates we couldn't determine the scenario
	ScenarioUnknown InitializationScenario = iota

	// ScenarioBootstrap: All nodes are empty (no data directories or no initialized schemas)
	// Action: Initialize one node as primary, then initialize others as standbys
	ScenarioBootstrap

	// ScenarioRepair: Some nodes have data but no current primary
	// Action: Use coordinator.AppointLeader to elect the most advanced node
	ScenarioRepair

	// ScenarioReelect: All nodes are initialized but no current primary
	// Action: Use coordinator.AppointLeader to re-elect a leader
	ScenarioReelect
)

// String returns the string representation of the scenario
func (s InitializationScenario) String() string {
	switch s {
	case ScenarioBootstrap:
		return "Bootstrap"
	case ScenarioRepair:
		return "Repair"
	case ScenarioReelect:
		return "Reelect"
	default:
		return "Unknown"
	}
}

// InitializeShardAction handles shard initialization for all three scenarios
type InitializeShardAction struct {
	coordinator *coordinator.Coordinator
	topoStore   topo.Store
	logger      *slog.Logger
}

// NewInitializeShardAction creates a new shard initialization action
func NewInitializeShardAction(coord *coordinator.Coordinator, topoStore topo.Store, logger *slog.Logger) *InitializeShardAction {
	return &InitializeShardAction{
		coordinator: coord,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute determines the initialization scenario and executes the appropriate workflow
func (a *InitializeShardAction) Execute(ctx context.Context, shardID string, database string, cohort []*coordinator.Node) error {
	a.logger.InfoContext(ctx, "Starting shard initialization",
		"shard", shardID,
		"database", database,
		"cohort_size", len(cohort))

	if len(cohort) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort is empty for shard %s", shardID)
	}

	// Step 1: Gather initialization status from all nodes
	statuses, err := a.gatherInitializationStatus(ctx, cohort)
	if err != nil {
		return mterrors.Wrap(err, "failed to gather initialization status")
	}

	// Step 2: Determine scenario
	scenario := a.determineScenario(statuses)
	a.logger.InfoContext(ctx, "Determined initialization scenario",
		"shard", shardID,
		"database", database,
		"scenario", scenario.String())

	// Step 3: Execute appropriate workflow based on scenario
	switch scenario {
	case ScenarioBootstrap:
		return a.executeBootstrap(ctx, shardID, database, cohort)
	case ScenarioRepair:
		return a.executeRepair(ctx, shardID, database, cohort)
	case ScenarioReelect:
		return a.executeReelect(ctx, shardID, database, cohort)
	default:
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"unable to determine initialization scenario for shard %s", shardID)
	}
}

// gatherInitializationStatus queries all nodes for their initialization status
func (a *InitializeShardAction) gatherInitializationStatus(ctx context.Context, cohort []*coordinator.Node) ([]*multipoolermanagerdatapb.InitializationStatusResponse, error) {
	statuses := make([]*multipoolermanagerdatapb.InitializationStatusResponse, len(cohort))

	for i, node := range cohort {
		req := &multipoolermanagerdatapb.InitializationStatusRequest{}
		status, err := node.RpcClient.InitializationStatus(ctx, node.Pooler, req)
		if err != nil {
			a.logger.WarnContext(ctx, "Failed to get initialization status from node",
				"node", node.ID.Name,
				"error", err)
			// Continue with nil status - we'll handle missing statuses in determineScenario
			statuses[i] = nil
			continue
		}
		statuses[i] = status
	}

	return statuses, nil
}

// determineScenario analyzes initialization statuses to determine the appropriate scenario
func (a *InitializeShardAction) determineScenario(statuses []*multipoolermanagerdatapb.InitializationStatusResponse) InitializationScenario {
	var initializedCount int
	var emptyCount int
	var unavailableCount int

	for _, status := range statuses {
		if status == nil {
			unavailableCount++
			continue
		}

		if status.IsInitialized {
			initializedCount++
		} else {
			emptyCount++
		}
	}

	a.logger.Debug("Analyzing initialization statuses",
		"initialized", initializedCount,
		"empty", emptyCount,
		"unavailable", unavailableCount)

	// All nodes are empty (or unavailable) → Bootstrap
	if initializedCount == 0 && emptyCount > 0 {
		return ScenarioBootstrap
	}

	// All reachable nodes are initialized → Reelect
	if initializedCount > 0 && emptyCount == 0 {
		return ScenarioReelect
	}

	// Mix of initialized and empty nodes → Repair
	if initializedCount > 0 && emptyCount > 0 {
		return ScenarioRepair
	}

	// All nodes unavailable or unknown state
	return ScenarioUnknown
}

// executeBootstrap handles the bootstrap scenario: initialize a new shard from scratch
func (a *InitializeShardAction) executeBootstrap(ctx context.Context, shardID string, database string, cohort []*coordinator.Node) error {
	a.logger.InfoContext(ctx, "Executing bootstrap initialization",
		"shard", shardID,
		"database", database,
		"cohort_size", len(cohort))

	// Step 1: Get the durability policy name from the Database proto in topology
	policyName, err := a.getDurabilityPolicyName(ctx, database)
	if err != nil {
		return mterrors.Wrap(err, "failed to get durability policy name from topology")
	}

	a.logger.InfoContext(ctx, "Using durability policy",
		"shard", shardID,
		"database", database,
		"policy_name", policyName)

	// Step 2: Select the first reachable node as the initial primary
	candidate, err := a.selectBootstrapCandidate(ctx, cohort)
	if err != nil {
		return mterrors.Wrap(err, "failed to select bootstrap candidate")
	}

	a.logger.InfoContext(ctx, "Selected bootstrap candidate",
		"shard", shardID,
		"database", database,
		"candidate", candidate.ID.Name)

	// Step 3: Initialize the candidate as an empty primary with term=1
	req := &multipoolermanagerdatapb.InitializeEmptyPrimaryRequest{
		ConsensusTerm: 1,
	}
	resp, err := candidate.RpcClient.InitializeEmptyPrimary(ctx, candidate.Pooler, req)
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize empty primary")
	}

	if !resp.Success {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"failed to initialize empty primary on node %s: %s",
			candidate.ID.Name, resp.ErrorMessage)
	}

	a.logger.InfoContext(ctx, "Successfully initialized primary",
		"shard", shardID,
		"database", database,
		"primary", candidate.ID.Name)

	// Step 4: Create the durability policy in the primary's database
	if err := a.createDurabilityPolicy(ctx, candidate, database, policyName); err != nil {
		return mterrors.Wrap(err, "failed to create durability policy in database")
	}

	a.logger.InfoContext(ctx, "Created durability policy in database",
		"shard", shardID,
		"database", database,
		"policy_name", policyName)

	// Step 5: Initialize remaining nodes as standbys
	standbys := make([]*coordinator.Node, 0, len(cohort)-1)
	for _, node := range cohort {
		if node.ID.Name != candidate.ID.Name {
			standbys = append(standbys, node)
		}
	}

	if err := a.initializeStandbys(ctx, shardID, candidate, standbys); err != nil {
		// Log but don't fail - we have a primary at least
		a.logger.WarnContext(ctx, "Failed to initialize some standbys",
			"shard", shardID,
			"database", database,
			"error", err)
	}

	a.logger.InfoContext(ctx, "Bootstrap initialization complete",
		"shard", shardID,
		"database", database,
		"primary", candidate.ID.Name,
		"standbys", len(standbys))

	return nil
}

// executeRepair handles the repair scenario: some nodes have data, elect the most advanced
func (a *InitializeShardAction) executeRepair(ctx context.Context, shardID string, database string, cohort []*coordinator.Node) error {
	a.logger.InfoContext(ctx, "Executing repair initialization",
		"shard", shardID,
		"database", database,
		"cohort_size", len(cohort))

	// Use the coordinator's AppointLeader to handle the election
	// It will select the most advanced node based on WAL position
	if err := a.coordinator.AppointLeader(ctx, shardID, cohort, database); err != nil {
		return mterrors.Wrap(err, "failed to appoint leader during repair")
	}

	a.logger.InfoContext(ctx, "Repair initialization complete",
		"shard", shardID,
		"database", database)
	return nil
}

// executeReelect handles the reelect scenario: all nodes initialized, re-elect leader
func (a *InitializeShardAction) executeReelect(ctx context.Context, shardID string, database string, cohort []*coordinator.Node) error {
	a.logger.InfoContext(ctx, "Executing reelect initialization",
		"shard", shardID,
		"database", database,
		"cohort_size", len(cohort))

	// Use the coordinator's AppointLeader to handle the re-election
	if err := a.coordinator.AppointLeader(ctx, shardID, cohort, database); err != nil {
		return mterrors.Wrap(err, "failed to appoint leader during reelect")
	}

	a.logger.InfoContext(ctx, "Reelect initialization complete",
		"shard", shardID,
		"database", database)
	return nil
}

// selectBootstrapCandidate selects the first healthy node as the bootstrap candidate
func (a *InitializeShardAction) selectBootstrapCandidate(ctx context.Context, cohort []*coordinator.Node) (*coordinator.Node, error) {
	// For bootstrap, we just pick the first reachable node
	// In a production system, you might want to consider factors like:
	// - Node with fastest storage
	// - Node in preferred availability zone
	// - Node with most resources available

	for _, node := range cohort {
		req := &multipoolermanagerdatapb.InitializationStatusRequest{}
		status, err := node.RpcClient.InitializationStatus(ctx, node.Pooler, req)
		if err != nil {
			a.logger.WarnContext(ctx, "Node unreachable during candidate selection",
				"node", node.ID.Name,
				"error", err)
			continue
		}

		if !status.IsInitialized {
			a.logger.InfoContext(ctx, "Selected node as bootstrap candidate",
				"node", node.ID.Name)
			return node, nil
		}
	}

	return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
		"no suitable candidate found for bootstrap")
}

// initializeStandbys initializes multiple nodes as standbys of the given primary
func (a *InitializeShardAction) initializeStandbys(ctx context.Context, shardID string, primary *coordinator.Node, standbys []*coordinator.Node) error {
	if len(standbys) == 0 {
		return nil
	}

	a.logger.InfoContext(ctx, "Initializing standbys",
		"shard", shardID,
		"primary", primary.ID.Name,
		"standby_count", len(standbys))

	// Initialize all standbys in parallel
	// Use a simple error aggregation approach
	type result struct {
		node *coordinator.Node
		err  error
	}

	results := make(chan result, len(standbys))

	for _, standby := range standbys {
		go func(node *coordinator.Node) {
			req := &multipoolermanagerdatapb.InitializeAsStandbyRequest{
				PrimaryHost:   primary.Hostname,
				PrimaryPort:   primary.Port,
				ConsensusTerm: 1,
				Force:         false,
			}
			resp, err := node.RpcClient.InitializeAsStandby(ctx, node.Pooler, req)
			if err != nil {
				results <- result{node: node, err: err}
				return
			}

			if !resp.Success {
				results <- result{
					node: node,
					err:  fmt.Errorf("initialization failed: %s", resp.ErrorMessage),
				}
				return
			}

			results <- result{node: node, err: nil}
		}(standby)
	}

	// Collect results
	var failedNodes []string
	for i := 0; i < len(standbys); i++ {
		res := <-results
		if res.err != nil {
			a.logger.WarnContext(ctx, "Failed to initialize standby",
				"shard", shardID,
				"node", res.node.ID.Name,
				"error", res.err)
			failedNodes = append(failedNodes, res.node.ID.Name)
		} else {
			a.logger.InfoContext(ctx, "Successfully initialized standby",
				"shard", shardID,
				"node", res.node.ID.Name)
		}
	}

	if len(failedNodes) > 0 {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"failed to initialize %d standbys: %v", len(failedNodes), failedNodes)
	}

	return nil
}

// getDurabilityPolicyName retrieves the durability policy name from the Database proto in topology
func (a *InitializeShardAction) getDurabilityPolicyName(ctx context.Context, database string) (string, error) {
	db, err := a.topoStore.GetDatabase(ctx, database)
	if err != nil {
		return "", mterrors.Wrapf(err, "failed to get database %s from topology", database)
	}

	if db.DurabilityPolicy == "" {
		return "", mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"database %s has no durability_policy configured", database)
	}

	// Validate that the policy name is supported
	switch db.DurabilityPolicy {
	case "ANY_2", "MULTI_CELL_ANY_2":
		return db.DurabilityPolicy, nil
	default:
		return "", mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported durability policy: %s (must be ANY_2 or MULTI_CELL_ANY_2)", db.DurabilityPolicy)
	}
}

// createDurabilityPolicy creates the durability policy entry in the primary's database
// This will be replicated to standbys automatically through PostgreSQL replication
func (a *InitializeShardAction) createDurabilityPolicy(ctx context.Context, primary *coordinator.Node, database string, policyName string) error {
	// Parse the policy name to get the quorum configuration
	quorumRule, err := a.parsePolicy(policyName)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse policy")
	}

	// Marshal the quorum rule to JSON
	quorumRuleJSON, err := protojson.Marshal(quorumRule)
	if err != nil {
		return mterrors.Wrap(err, "failed to marshal quorum rule")
	}

	// Connect to the primary's PostgreSQL database
	// Always use 'postgres' as the actual PostgreSQL database name
	dsn := fmt.Sprintf("host=%s port=%d dbname=postgres user=postgres sslmode=disable",
		primary.Hostname, primary.Port)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return mterrors.Wrapf(err, "failed to connect to primary %s", primary.ID.Name)
	}
	defer db.Close()

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		return mterrors.Wrapf(err, "failed to ping primary %s", primary.ID.Name)
	}

	// Insert the policy into the durability_policy table
	now := time.Now()
	query := `
		INSERT INTO multigres.durability_policy
			(policy_name, policy_version, quorum_rule, is_active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err = db.ExecContext(ctx, query,
		policyName,     // policy_name
		1,              // policy_version (initial version)
		quorumRuleJSON, // quorum_rule (JSONB)
		true,           // is_active
		now,            // created_at
		now,            // updated_at
	)
	if err != nil {
		return mterrors.Wrapf(err, "failed to insert durability policy into database")
	}

	a.logger.InfoContext(ctx, "Inserted durability policy into primary database",
		"primary", primary.ID.Name,
		"logical_database", database,
		"policy_name", policyName,
		"quorum_type", quorumRule.QuorumType,
		"required_count", quorumRule.RequiredCount)

	return nil
}

// parsePolicy converts a policy name into a QuorumRule
func (a *InitializeShardAction) parsePolicy(policyName string) (*clustermetadatapb.QuorumRule, error) {
	switch policyName {
	case "ANY_2":
		return &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Any 2 nodes must acknowledge",
		}, nil

	case "MULTI_CELL_ANY_2":
		return &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 2,
			Description:   "Any 2 nodes from different cells must acknowledge",
		}, nil

	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported policy name: %s", policyName)
	}
}
