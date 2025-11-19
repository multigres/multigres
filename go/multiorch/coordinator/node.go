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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multipooler/rpcclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Node represents a single multipooler node with a cached gRPC client for both
// manager and consensus services.
type Node struct {
	// ID is the cluster metadata ID for this multipooler
	ID *clustermetadatapb.ID

	// Hostname is the network address where this node is reachable
	Hostname string

	// Port is the gRPC port for this node
	Port int32

	// ShardID identifies which shard this node belongs to
	ShardID string

	// rpcClient is the shared cached RPC client for calling multipooler services
	rpcClient rpcclient.MultiPoolerClient

	// pooler is the full multipooler metadata needed for RPC calls
	pooler *clustermetadatapb.MultiPooler
}

// CreateNode creates a Node with a reference to the shared cached RPC client.
// The node stores the pooler metadata needed for making RPC calls.
func CreateNode(ctx context.Context, rpcClient rpcclient.MultiPoolerClient, poolerInfo *topo.MultiPoolerInfo) (*Node, error) {
	// Convert topo.MultiPoolerInfo to clustermetadatapb.MultiPooler
	pooler := poolerInfo.MultiPooler

	node := &Node{
		ID:        poolerInfo.Id,
		Hostname:  poolerInfo.Hostname,
		Port:      poolerInfo.PortMap["grpc"],
		ShardID:   poolerInfo.Shard,
		rpcClient: rpcClient,
		pooler:    pooler,
	}

	return node, nil
}

// CreateNodes creates Node instances for all multipoolers in the given list.
// All nodes share the same cached RPC client for connection pooling.
func CreateNodes(ctx context.Context, rpcClient rpcclient.MultiPoolerClient, poolerInfos []*topo.MultiPoolerInfo) ([]*Node, error) {
	nodes := make([]*Node, 0, len(poolerInfos))

	for _, poolerInfo := range poolerInfos {
		node, err := CreateNode(ctx, rpcClient, poolerInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to create node for %s: %w", poolerInfo.IDString(), err)
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// ConsensusStatus gets the consensus status of this node
func (n *Node) ConsensusStatus(ctx context.Context) (*consensusdatapb.StatusResponse, error) {
	req := &consensusdatapb.StatusRequest{}
	return n.rpcClient.ConsensusStatus(ctx, n.pooler, req)
}

// BeginTerm sends a BeginTerm request for leader appointment
func (n *Node) BeginTerm(ctx context.Context, term int64, candidateID *clustermetadatapb.ID) (*consensusdatapb.BeginTermResponse, error) {
	req := &consensusdatapb.BeginTermRequest{
		Term:        term,
		CandidateId: candidateID,
	}
	return n.rpcClient.BeginTerm(ctx, n.pooler, req)
}

// ManagerStatus gets the manager status of this node
func (n *Node) ManagerStatus(ctx context.Context) (*multipoolermanagerdatapb.StateResponse, error) {
	req := &multipoolermanagerdatapb.StateRequest{}
	return n.rpcClient.State(ctx, n.pooler, req)
}

// InitializeEmptyPrimary initializes this node as an empty primary
func (n *Node) InitializeEmptyPrimary(ctx context.Context, term int64) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error) {
	req := &multipoolermanagerdatapb.InitializeEmptyPrimaryRequest{
		ConsensusTerm: term,
	}
	return n.rpcClient.InitializeEmptyPrimary(ctx, n.pooler, req)
}

// InitializeAsStandby initializes this node as a standby from a primary
func (n *Node) InitializeAsStandby(ctx context.Context, primaryHost string, primaryPort int32, term int64, force bool) (*multipoolermanagerdatapb.InitializeAsStandbyResponse, error) {
	req := &multipoolermanagerdatapb.InitializeAsStandbyRequest{
		PrimaryHost:   primaryHost,
		PrimaryPort:   primaryPort,
		ConsensusTerm: term,
		Force:         force,
	}
	return n.rpcClient.InitializeAsStandby(ctx, n.pooler, req)
}

// SetPrimaryConnInfo configures this standby's connection to a primary
func (n *Node) SetPrimaryConnInfo(ctx context.Context, primaryHost string, primaryPort int32, term int64) error {
	req := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
		Host:                  primaryHost,
		Port:                  primaryPort,
		CurrentTerm:           term,
		StopReplicationBefore: false,
		StartReplicationAfter: false,
		Force:                 false,
	}
	_, err := n.rpcClient.SetPrimaryConnInfo(ctx, n.pooler, req)
	return err
}

// Promote promotes this node to primary
func (n *Node) Promote(ctx context.Context, term int64, expectedLSN string, syncConfig *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) (*multipoolermanagerdatapb.PromoteResponse, error) {
	req := &multipoolermanagerdatapb.PromoteRequest{
		ConsensusTerm:         term,
		ExpectedLsn:           expectedLSN,
		SyncReplicationConfig: syncConfig,
		Force:                 false,
	}
	return n.rpcClient.Promote(ctx, n.pooler, req)
}
