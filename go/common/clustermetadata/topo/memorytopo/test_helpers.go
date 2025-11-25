// Copyright 2025 The Multigres Authors.
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

package memorytopo

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// CreateTestGateway creates a test gateway in the specified cell for testing
func CreateTestGateway(ctx context.Context, ts topo.Store, cellName, gatewayName string) error {
	gateway := &clustermetadatapb.MultiGateway{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIGATEWAY,
			Cell:      cellName,
			Name:      gatewayName,
		},
		Hostname: fmt.Sprintf("gateway-%s.%s", gatewayName, cellName),
		PortMap:  map[string]int32{"grpc": 8080, "postgres": 5432},
	}
	return ts.CreateMultiGateway(ctx, gateway)
}

// CreateTestPooler creates a test pooler in the specified cell for testing
func CreateTestPooler(ctx context.Context, ts topo.Store, cellName, poolerName string, database string) error {
	pooler := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cellName,
			Name:      poolerName,
		},
		Hostname: fmt.Sprintf("pooler-%s.%s", poolerName, cellName),
		PortMap:  map[string]int32{"grpc": 8081, "postgres": 5433},
		Database: database,
	}
	return ts.CreateMultiPooler(ctx, pooler)
}

// CreateTestOrchestrator creates a test orchestrator in the specified cell for testing
func CreateTestOrchestrator(ctx context.Context, ts topo.Store, cellName, orchName string) error {
	orch := &clustermetadatapb.MultiOrch{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      cellName,
			Name:      orchName,
		},
		Hostname: fmt.Sprintf("orch-%s.%s", orchName, cellName),
		PortMap:  map[string]int32{"grpc": 8082},
	}
	return ts.CreateMultiOrch(ctx, orch)
}

// SetupMultiCellTestData creates test data across multiple cells for testing
func SetupMultiCellTestData(ctx context.Context, ts topo.Store) error {
	// Create test gateways in multiple cells
	if err := CreateTestGateway(ctx, ts, "cell1", "gw1"); err != nil {
		return fmt.Errorf("failed to create gateway in cell1: %w", err)
	}
	if err := CreateTestGateway(ctx, ts, "cell1", "gw2"); err != nil {
		return fmt.Errorf("failed to create second gateway in cell1: %w", err)
	}
	if err := CreateTestGateway(ctx, ts, "cell2", "gw3"); err != nil {
		return fmt.Errorf("failed to create gateway in cell2: %w", err)
	}

	// Create test poolers in multiple cells
	if err := CreateTestPooler(ctx, ts, "cell1", "pool1", "db1"); err != nil {
		return fmt.Errorf("failed to create pooler in cell1: %w", err)
	}
	if err := CreateTestPooler(ctx, ts, "cell2", "pool2", "db1"); err != nil {
		return fmt.Errorf("failed to create pooler in cell2: %w", err)
	}
	if err := CreateTestPooler(ctx, ts, "cell2", "pool3", "db2"); err != nil {
		return fmt.Errorf("failed to create second pooler in cell2: %w", err)
	}

	// Create test orchestrators in multiple cells
	if err := CreateTestOrchestrator(ctx, ts, "cell1", "orch1"); err != nil {
		return fmt.Errorf("failed to create orchestrator in cell1: %w", err)
	}
	if err := CreateTestOrchestrator(ctx, ts, "cell2", "orch2"); err != nil {
		return fmt.Errorf("failed to create orchestrator in cell2: %w", err)
	}

	return nil
}
