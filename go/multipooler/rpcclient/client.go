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

// Package rpcclient provides a unified client interface for communicating with multipooler nodes.
//
// This package provides a single MultiPoolerClient interface that encompasses both
// consensus and manager RPC services, with support for persistent connections per pooler
// to optimize for the continuous monitoring use case in multiorch.
//
// # Architecture
//
// This implementation provides:
//  1. Unified Interface: Single MultiPoolerClient interface with both consensus and manager methods
//  2. Persistent Connections: One long-lived gRPC connection per multipooler address
//  3. Automatic Invalidation: Connections are automatically closed on errors and recreated on next use
//  4. Explicit Connection Management: CloseTablet() method to remove poolers from monitoring
//
// # Design Rationale
//
// The coordinator always needs both consensus and manager RPCs for the same pooler.
// Maintaining separate connection pools would be wasteful. A unified interface is simpler
// and more efficient, with both services sharing the same gRPC connection.
//
// Multiorch continuously monitors a relatively stable set of poolers. Health checks run
// every few seconds on the same poolers. Creating/destroying connections for each RPC is
// expensive. One persistent connection per pooler minimizes overhead.
//
// # Usage
//
// Basic usage:
//
//	// Create client (typically done once at startup)
//	client := rpcclient.NewMultiPoolerClient()
//	defer client.Close()
//
//	// Call consensus methods
//	resp, err := client.BeginTerm(ctx, tablet, &consensusdatapb.BeginTermRequest{
//	    Term: 5,
//	    CandidateId: coordinatorID,
//	})
//
//	// Call manager methods
//	status, err := client.Status(ctx, tablet, &multipoolermanagerdatapb.StatusRequest{})
//
// Connection management:
//
//	// Remove a pooler from monitoring (closes its connection)
//	client.CloseTablet(tablet)
//
//	// Connection will be automatically recreated on next RPC to this tablet
//
// Error handling - connections are automatically invalidated on errors:
//
//	resp, err := client.Status(ctx, tablet, req)
//	if err != nil {
//	    // Connection has been closed and removed from cache
//	    // Next call will create a new connection
//	    return err
//	}
//
// # Implementation Details
//
// The client maintains a map of address -> *persistentConn. Each connection holds
// clients for both consensus and manager services, created from the same grpc.ClientConn.
//
// All connection management operations are protected by a mutex, making the client
// safe for concurrent use by multiple goroutines.
//
// Connection lifecycle:
//  1. Creation: First RPC to an address creates a new persistent connection
//  2. Reuse: Subsequent RPCs reuse the same connection
//  3. Invalidation: On error, the invalidator function closes and removes the connection
//  4. Explicit Removal: CloseTablet() can explicitly remove a connection
//  5. Shutdown: Close() closes all connections
package rpcclient

import (
	"context"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// MultiPoolerClient defines the unified interface for communicating with a multipooler node.
// It provides methods for both consensus and manager services, maintaining persistent
// connections per pooler address for optimal performance in monitoring scenarios.
//
// Implementations should:
//   - Maintain one persistent connection per multipooler address
//   - Support connection invalidation when errors occur
//   - Be thread-safe for concurrent use
//
// All methods take a *clustermetadatapb.MultiPooler parameter to identify the target pooler.
type MultiPoolerClient interface {
	//
	// Consensus Service Methods (consensuspb.MultiPoolerConsensusClient)
	//

	// BeginTerm sends a BeginTerm request for leader appointment.
	// This is part of the consensus protocol for establishing a new term.
	BeginTerm(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.BeginTermRequest) (*consensusdatapb.BeginTermResponse, error)

	// ConsensusStatus gets the consensus status of the multipooler.
	// This may be called frequently for monitoring, so implementations use persistent connections.
	ConsensusStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error)

	// GetLeadershipView gets the leadership view from the multipooler's perspective.
	GetLeadershipView(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.LeadershipViewRequest) (*consensusdatapb.LeadershipViewResponse, error)

	// CanReachPrimary checks if the multipooler can reach the primary.
	CanReachPrimary(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.CanReachPrimaryRequest) (*consensusdatapb.CanReachPrimaryResponse, error)

	//
	// Manager Service Methods - Initialization
	//

	// InitializeEmptyPrimary initializes the multipooler as an empty primary.
	InitializeEmptyPrimary(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.InitializeEmptyPrimaryRequest) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error)

	// InitializeAsStandby initializes the multipooler as a standby from a primary.
	InitializeAsStandby(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.InitializeAsStandbyRequest) (*multipoolermanagerdatapb.InitializeAsStandbyResponse, error)

	// InitializationStatus gets the initialization status of the multipooler.
	// This is called frequently during discovery, so implementations use persistent connections.
	InitializationStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.InitializationStatusRequest) (*multipoolermanagerdatapb.InitializationStatusResponse, error)

	//
	// Manager Service Methods - Status and Monitoring
	//

	// Status gets the current status of the multipooler manager.
	// This is called very frequently by the recovery engine health checks,
	// so implementations use persistent connections.
	Status(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StatusRequest) (*multipoolermanagerdatapb.StatusResponse, error)

	//
	// Manager Service Methods - Replication
	//

	// WaitForLSN waits for the multipooler to replay WAL up to the target LSN.
	WaitForLSN(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.WaitForLSNRequest) (*multipoolermanagerdatapb.WaitForLSNResponse, error)

	// SetPrimaryConnInfo configures the standby's connection to a primary.
	SetPrimaryConnInfo(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.SetPrimaryConnInfoRequest) (*multipoolermanagerdatapb.SetPrimaryConnInfoResponse, error)

	// StartReplication starts WAL replay on standby.
	StartReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StartReplicationRequest) (*multipoolermanagerdatapb.StartReplicationResponse, error)

	// StopReplication stops replication based on the specified mode.
	StopReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StopReplicationRequest) (*multipoolermanagerdatapb.StopReplicationResponse, error)

	// ReplicationStatus gets the current replication status of the standby.
	ReplicationStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ReplicationStatusRequest) (*multipoolermanagerdatapb.ReplicationStatusResponse, error)

	// ResetReplication resets the standby's connection to its primary.
	ResetReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ResetReplicationRequest) (*multipoolermanagerdatapb.ResetReplicationResponse, error)

	// StopReplicationAndGetStatus stops replication and returns the current status.
	StopReplicationAndGetStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StopReplicationAndGetStatusRequest) (*multipoolermanagerdatapb.StopReplicationAndGetStatusResponse, error)

	//
	// Manager Service Methods - Synchronous Replication
	//

	// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings.
	ConfigureSynchronousReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse, error)

	// UpdateSynchronousStandbyList updates the synchronous standby list.
	UpdateSynchronousStandbyList(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error)

	//
	// Manager Service Methods - Primary Status
	//

	// PrimaryStatus gets the status of the primary server.
	PrimaryStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.PrimaryStatusRequest) (*multipoolermanagerdatapb.PrimaryStatusResponse, error)

	// PrimaryPosition gets the current LSN position of the primary.
	PrimaryPosition(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.PrimaryPositionRequest) (*multipoolermanagerdatapb.PrimaryPositionResponse, error)

	// GetFollowers gets the list of follower servers with detailed replication status.
	GetFollowers(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetFollowersRequest) (*multipoolermanagerdatapb.GetFollowersResponse, error)

	//
	// Manager Service Methods - Promotion and Demotion
	//

	// Promote promotes the multipooler to primary.
	Promote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.PromoteRequest) (*multipoolermanagerdatapb.PromoteResponse, error)

	// Demote demotes the multipooler from primary.
	Demote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.DemoteRequest) (*multipoolermanagerdatapb.DemoteResponse, error)

	// UndoDemote undoes a demotion.
	UndoDemote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.UndoDemoteRequest) (*multipoolermanagerdatapb.UndoDemoteResponse, error)

	//
	// Manager Service Methods - Type and Term Management
	//

	// ChangeType changes the pooler type (PRIMARY/REPLICA).
	ChangeType(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ChangeTypeRequest) (*multipoolermanagerdatapb.ChangeTypeResponse, error)

	// SetTerm sets the consensus term information.
	SetTerm(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.SetTermRequest) (*multipoolermanagerdatapb.SetTermResponse, error)

	//
	// Manager Service Methods - Backup and Restore
	//

	// Backup performs a backup.
	Backup(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.BackupRequest) (*multipoolermanagerdatapb.BackupResponse, error)

	// RestoreFromBackup restores from a backup.
	RestoreFromBackup(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.RestoreFromBackupRequest) (*multipoolermanagerdatapb.RestoreFromBackupResponse, error)

	// GetBackups retrieves backup information.
	GetBackups(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetBackupsRequest) (*multipoolermanagerdatapb.GetBackupsResponse, error)

	//
	// Connection Management Methods
	//

	// Close closes all persistent connections and frees resources.
	// After calling Close, this client should not be used anymore.
	Close()

	// CloseTablet closes the persistent connection to a specific tablet.
	// This should be called when a pooler is removed from monitoring or becomes unreachable.
	// Subsequent calls to the same tablet will create a new connection.
	CloseTablet(pooler *clustermetadatapb.MultiPooler)
}

// NewMultiPoolerClient creates a new MultiPoolerClient with persistent connections.
func NewMultiPoolerClient() MultiPoolerClient {
	return NewClient()
}
