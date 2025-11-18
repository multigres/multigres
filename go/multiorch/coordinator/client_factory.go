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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
)

// CreateNode creates a Node with connected gRPC clients for both
// MultiPoolerManager and MultiPoolerConsensus services.
// The caller is responsible for closing the connections when done.
func CreateNode(ctx context.Context, poolerInfo *topo.MultiPoolerInfo) (*Node, error) {
	// Get the gRPC address (hostname:port)
	addr := poolerInfo.Addr()
	if addr == "" {
		return nil, fmt.Errorf("multipooler %s has no gRPC address", poolerInfo.IDString())
	}

	// Create gRPC connection
	// TODO: Add proper TLS configuration for production
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to multipooler at %s: %w", addr, err)
	}

	// Create clients for both services on the same connection
	managerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)
	consensusClient := consensuspb.NewMultiPoolerConsensusClient(conn)

	node := &Node{
		ID:              poolerInfo.Id,
		Hostname:        poolerInfo.Hostname,
		Port:            poolerInfo.PortMap["grpc"],
		ShardID:         poolerInfo.Shard,
		ManagerClient:   managerClient,
		ConsensusClient: consensusClient,
	}

	return node, nil
}

// CreateNodes creates Node instances for all multipoolers in the given list.
// Returns an error if any node fails to connect.
func CreateNodes(ctx context.Context, poolerInfos []*topo.MultiPoolerInfo) ([]*Node, error) {
	nodes := make([]*Node, 0, len(poolerInfos))

	for _, poolerInfo := range poolerInfos {
		node, err := CreateNode(ctx, poolerInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to create node for %s: %w", poolerInfo.IDString(), err)
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}
