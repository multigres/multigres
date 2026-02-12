// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consensus

import (
	"fmt"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// ValidateQuorum checks if the recruited nodes satisfy the quorum rule
func (c *Coordinator) ValidateQuorum(rule *clustermetadatapb.QuorumRule, cohort []*multiorchdatapb.PoolerHealthState, recruited []*multiorchdatapb.PoolerHealthState) error {
	switch rule.QuorumType {
	case clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N:
		return c.validateAnyNQuorum(rule, cohort, recruited)

	case clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N:
		return c.validateMultiCellQuorum(rule, recruited)

	default:
		return fmt.Errorf("unknown quorum type: %v", rule.QuorumType)
	}
}

// validateAnyNQuorum validates that we have at least N nodes recruited
func (c *Coordinator) validateAnyNQuorum(rule *clustermetadatapb.QuorumRule, cohort []*multiorchdatapb.PoolerHealthState, recruited []*multiorchdatapb.PoolerHealthState) error {
	required := int(rule.RequiredCount)
	recruitedCount := len(recruited)

	c.logger.Debug("validating ANY_N quorum",
		"required", required,
		"recruited", recruitedCount,
		"cohort_size", len(cohort))

	if recruitedCount < required {
		return fmt.Errorf("quorum not satisfied: recruited %d nodes, required %d (%s)",
			recruitedCount, required, rule.Description)
	}

	c.logger.Info("ANY_N quorum satisfied",
		"recruited", recruitedCount,
		"required", required)

	return nil
}

// validateMultiCellQuorum validates that we have at least one node from required_count distinct cells
func (c *Coordinator) validateMultiCellQuorum(rule *clustermetadatapb.QuorumRule, recruited []*multiorchdatapb.PoolerHealthState) error {
	// Group recruited nodes by cell
	nodesByCell := make(map[string][]*multiorchdatapb.PoolerHealthState)
	for _, node := range recruited {
		cell := node.MultiPooler.Id.Cell
		nodesByCell[cell] = append(nodesByCell[cell], node)
	}

	requiredCells := int(rule.RequiredCount)
	recruitedCells := len(nodesByCell)

	c.logger.Debug("validating MULTI_CELL_ANY_N quorum",
		"required_cells", requiredCells,
		"recruited_cells", recruitedCells,
		"cells", getCellNames(nodesByCell))

	if recruitedCells < requiredCells {
		return fmt.Errorf("quorum not satisfied: recruited nodes from %d cells, required %d cells (%s)",
			recruitedCells, requiredCells, rule.Description)
	}

	c.logger.Info("MULTI_CELL_ANY_N quorum satisfied",
		"recruited_cells", recruitedCells,
		"required_cells", requiredCells,
		"cells", getCellNames(nodesByCell))

	return nil
}

// getCellNames extracts cell names from the nodesByCell map for logging
func getCellNames(nodesByCell map[string][]*multiorchdatapb.PoolerHealthState) []string {
	cells := make([]string, 0, len(nodesByCell))
	for cell := range nodesByCell {
		cells = append(cells, cell)
	}
	return cells
}
