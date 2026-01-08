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
// consensus and manager RPC services, with support for cached connections with LRU eviction
// to optimize for the continuous monitoring use case in multiorch.
//
// # Architecture
//
// This implementation provides:
//  1. Unified Interface: Single MultiPoolerClient interface with both consensus and manager methods
//  2. Connection Cache: Bounded cache with LRU eviction (default capacity: 100 connections)
//  3. Automatic Connection Management: Connections are reused when available, created on demand
//  4. Capacity-Based Eviction: When cache is full, least-recently-used unreferenced connections are evicted
//  5. Explicit Connection Removal: CloseTablet() method to immediately remove a connection from cache
//
// # Design Rationale
//
// The coordinator always needs both consensus and manager RPCs for the same pooler.
// Maintaining separate connection pools would be wasteful. A unified interface is simpler
// and more efficient, with both services sharing the same gRPC connection.
//
// Multiorch continuously monitors a relatively stable set of poolers. Health checks run
// every few seconds on the same poolers. Creating/destroying connections for each RPC is
// expensive. Connection caching minimizes overhead while preventing unbounded resource usage.
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
//	// Explicitly remove a pooler connection from cache
//	client.CloseTablet(tablet)
//
//	// Connection will be automatically recreated on next RPC to this tablet
//
// Connection lifecycle and caching:
//
//	// First call to a tablet - creates new connection and adds to cache
//	resp1, err := client.Status(ctx, tablet1, req)
//
//	// Second call to same tablet - reuses cached connection
//	resp2, err := client.Status(ctx, tablet1, req)
//
//	// When cache is full, least-recently-used unreferenced connections are evicted
//	// to make room for new connections
//
// # Implementation Details
//
// The client uses a connection cache (connCache) that maintains:
//   - A map of address -> *cachedConn for O(1) lookups
//   - An eviction queue sorted by reference count and last access time
//   - A semaphore to limit concurrent connection attempts to the cache capacity
//
// Each cached connection holds clients for both consensus and manager services,
// created from the same grpc.ClientConn.
//
// All cache operations are protected by a mutex, making the client safe for
// concurrent use by multiple goroutines.
//
// Connection lifecycle:
//  1. Cache Lookup: Check if connection exists in cache and is available
//  2. Fast Dial: If cache miss and capacity available, dial new connection immediately
//  3. Eviction: If at capacity, wait for evictable (unreferenced) connection, evict it, then dial
//  4. Reuse: Increment reference count while RPC is in flight
//  5. Release: Decrement reference count after RPC completes (via closer function)
//  6. Explicit Removal: CloseTablet() immediately closes and removes a connection
//  7. Shutdown: Close() closes all connections and clears the cache
//
// The three-path dial strategy (cache_fast, sema_fast, sema_poll) is borrowed from
// Vitess's cachedConnDialer and optimizes for high-throughput scenarios while
// preventing unbounded connection growth.
package rpcclient

import (
	"context"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// MultiPoolerClient defines the unified interface for communicating with a multipooler node.
// It provides methods for both consensus and manager services, maintaining a cache of
// connections with LRU eviction for optimal performance in monitoring scenarios.
//
// Implementations should:
//   - Cache connections up to a configurable capacity (default: 100)
//   - Evict least-recently-used unreferenced connections when cache is full
//   - Reuse existing connections when available
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
	// This may be called frequently for monitoring, so implementations cache connections.
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

	//
	// Manager Service Methods - Status and Monitoring
	//

	// State gets the current status of the multipooler manager.
	// This is called very frequently by the recovery engine health checks,
	// so implementations cache connections.
	State(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StateRequest) (*multipoolermanagerdatapb.StateResponse, error)

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

	// StandbyReplicationStatus gets the current replication status of the standby.
	StandbyReplicationStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StandbyReplicationStatusRequest) (*multipoolermanagerdatapb.StandbyReplicationStatusResponse, error)

	// Status gets unified status that works for both PRIMARY and REPLICA poolers.
	Status(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StatusRequest) (*multipoolermanagerdatapb.StatusResponse, error)

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
	// Manager Service Methods - Durability Policy
	//

	// GetDurabilityPolicy retrieves the active durability policy from the local database.
	GetDurabilityPolicy(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetDurabilityPolicyRequest) (*multipoolermanagerdatapb.GetDurabilityPolicyResponse, error)

	// CreateDurabilityPolicy creates a new durability policy in the local database.
	CreateDurabilityPolicy(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.CreateDurabilityPolicyRequest) (*multipoolermanagerdatapb.CreateDurabilityPolicyResponse, error)

	//
	// Manager Service Methods - Backup and Restore
	//

	// Backup performs a backup.
	Backup(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.BackupRequest) (*multipoolermanagerdatapb.BackupResponse, error)

	// RestoreFromBackup restores from a backup.
	RestoreFromBackup(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.RestoreFromBackupRequest) (*multipoolermanagerdatapb.RestoreFromBackupResponse, error)

	// GetBackups retrieves backup information.
	GetBackups(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetBackupsRequest) (*multipoolermanagerdatapb.GetBackupsResponse, error)

	// GetBackupByJobId queries a multipooler for a backup by its job_id annotation.
	GetBackupByJobId(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetBackupByJobIdRequest) (*multipoolermanagerdatapb.GetBackupByJobIdResponse, error)

	//
	// Manager Service Methods - Timeline Repair
	//

	// RewindToSource performs pg_rewind to synchronize a replica with its source.
	RewindToSource(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.RewindToSourceRequest) (*multipoolermanagerdatapb.RewindToSourceResponse, error)

	//
	// Manager Service Methods - PostgreSQL Monitoring Control
	//

	// EnableMonitor enables the PostgreSQL monitoring goroutine on a pooler.
	EnableMonitor(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.EnableMonitorRequest) (*multipoolermanagerdatapb.EnableMonitorResponse, error)

	// DisableMonitor disables the PostgreSQL monitoring goroutine on a pooler.
	DisableMonitor(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.DisableMonitorRequest) (*multipoolermanagerdatapb.DisableMonitorResponse, error)

	//
	// Connection Management Methods
	//

	// Close closes all cached connections and frees resources.
	// After calling Close, this client should not be used anymore.
	Close()

	// CloseTablet closes and removes the cached connection to a specific tablet.
	// This should be called when a pooler is removed from monitoring or becomes unreachable.
	// Subsequent calls to the same tablet will create a new connection.
	CloseTablet(pooler *clustermetadatapb.MultiPooler)
}

// NewMultiPoolerClient creates a new MultiPoolerClient with connection caching.
// The capacity parameter determines the maximum number of simultaneous connections
// to distinct multipoolers. Connections are cached with LRU eviction.
//
// For multiorch deployments monitoring many poolers, a capacity of 1000 is recommended.
// For smaller deployments or testing, 100 may be sufficient.
func NewMultiPoolerClient(capacity int) MultiPoolerClient {
	return NewClient(capacity)
}
