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
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Node represents a single multipooler node with gRPC clients for both
// manager and consensus services. It provides convenience methods for
// calling RPCs during leader appointment.
type Node struct {
	// ID is the cluster metadata ID for this multipooler
	ID *clustermetadatapb.ID

	// Hostname is the network address where this node is reachable
	Hostname string

	// Port is the gRPC port for this node
	Port int32

	// ShardID identifies which shard this node belongs to
	ShardID string

	// ManagerClient is the gRPC client for MultiPoolerManager service
	ManagerClient multipoolermanagerpb.MultiPoolerManagerClient

	// ConsensusClient is the gRPC client for MultiPoolerConsensus service
	ConsensusClient consensuspb.MultiPoolerConsensusClient
}

// InitializationStatus gets the initialization status of this node
func (n *Node) InitializationStatus(ctx context.Context) (*multipoolermanagerdatapb.InitializationStatusResponse, error) {
	req := &multipoolermanagerdatapb.InitializationStatusRequest{}
	return n.ManagerClient.InitializationStatus(ctx, req)
}

// WaitForLSN waits for this node to replay WAL up to the target LSN
func (n *Node) WaitForLSN(ctx context.Context, targetLsn string) error {
	req := &multipoolermanagerdatapb.WaitForLSNRequest{
		TargetLsn: targetLsn,
	}
	_, err := n.ManagerClient.WaitForLSN(ctx, req)
	return err
}

// BeginTerm sends a BeginTerm request to this node for leader appointment
func (n *Node) BeginTerm(ctx context.Context, term int64, candidateID *clustermetadatapb.ID) (*consensusdatapb.BeginTermResponse, error) {
	req := &consensusdatapb.BeginTermRequest{
		Term:        term,
		CandidateId: candidateID,
	}
	return n.ConsensusClient.BeginTerm(ctx, req)
}

// Promote promotes this node to primary
func (n *Node) Promote(ctx context.Context, term int64, expectedLSN string, syncConfig *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) (*multipoolermanagerdatapb.PromoteResponse, error) {
	req := &multipoolermanagerdatapb.PromoteRequest{
		ConsensusTerm:         term,
		ExpectedLsn:           expectedLSN,
		SyncReplicationConfig: syncConfig,
		Force:                 false,
	}
	return n.ManagerClient.Promote(ctx, req)
}

// Demote demotes this node from primary
func (n *Node) Demote(ctx context.Context, term int64, drainTimeout time.Duration) (*multipoolermanagerdatapb.DemoteResponse, error) {
	req := &multipoolermanagerdatapb.DemoteRequest{
		ConsensusTerm: term,
		DrainTimeout:  durationpb.New(drainTimeout),
		Force:         false,
	}
	return n.ManagerClient.Demote(ctx, req)
}

// ConsensusStatus gets the consensus status of this node
func (n *Node) ConsensusStatus(ctx context.Context) (*consensusdatapb.StatusResponse, error) {
	req := &consensusdatapb.StatusRequest{}
	return n.ConsensusClient.Status(ctx, req)
}

// ManagerStatus gets the manager status of this node
func (n *Node) ManagerStatus(ctx context.Context) (*multipoolermanagerdatapb.StatusResponse, error) {
	req := &multipoolermanagerdatapb.StatusRequest{}
	return n.ManagerClient.Status(ctx, req)
}

// InitializeEmptyPrimary initializes this node as an empty primary
func (n *Node) InitializeEmptyPrimary(ctx context.Context, term int64) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error) {
	req := &multipoolermanagerdatapb.InitializeEmptyPrimaryRequest{
		ConsensusTerm: term,
	}
	return n.ManagerClient.InitializeEmptyPrimary(ctx, req)
}

// InitializeAsStandby initializes this node as a standby from a primary
func (n *Node) InitializeAsStandby(ctx context.Context, primaryHost string, primaryPort int32, term int64, force bool) (*multipoolermanagerdatapb.InitializeAsStandbyResponse, error) {
	req := &multipoolermanagerdatapb.InitializeAsStandbyRequest{
		PrimaryHost:   primaryHost,
		PrimaryPort:   primaryPort,
		ConsensusTerm: term,
		Force:         force,
	}
	return n.ManagerClient.InitializeAsStandby(ctx, req)
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
	_, err := n.ManagerClient.SetPrimaryConnInfo(ctx, req)
	return err
}

// StopReplicationAndGetStatus stops replication on this standby and returns status
func (n *Node) StopReplicationAndGetStatus(ctx context.Context, mode multipoolermanagerdatapb.ReplicationPauseMode, wait bool) (*multipoolermanagerdatapb.ReplicationStatus, error) {
	req := &multipoolermanagerdatapb.StopReplicationAndGetStatusRequest{
		Mode: mode,
		Wait: wait,
	}
	resp, err := n.ManagerClient.StopReplicationAndGetStatus(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Status, nil
}
