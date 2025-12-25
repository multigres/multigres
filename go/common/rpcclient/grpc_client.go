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

package rpcclient

import (
	"context"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/tools/netutil"
)

// Client implements MultiPoolerClient using gRPC with cached persistent connections.
// It maintains one persistent connection per pooler address, up to a maximum capacity.
// When capacity is reached, it uses LRU eviction to close least recently used connections.
//
// This follows Vitess's cached connection pattern where:
//   - Each multipooler gets its own persistent gRPC connection
//   - Connections are reused across multiple RPC calls
//   - A capacity limit prevents unbounded connection growth
//   - LRU eviction removes idle connections when capacity is reached
//   - Reference counting prevents eviction of in-use connections
type Client struct {
	cache *connCache
}

// NewClient creates a new gRPC-based MultiPoolerClient with specified capacity.
// The capacity parameter determines the maximum number of simultaneous connections
// to distinct multipoolers. When the cache is full, least-recently-used unreferenced
// connections are evicted to make room for new connections.
func NewClient(capacity int) *Client {
	return &Client{
		cache: newConnCacheWithCapacity(capacity),
	}
}

// dialPersistent gets or creates a cached connection to the pooler.
// It returns the connection and a closer function that must be called
// when the RPC is complete to decrement the reference count.
// The closer should be called even if the RPC fails.
func (c *Client) dialPersistent(ctx context.Context, pooler *clustermetadatapb.MultiPooler) (*cachedConn, closeFunc, error) {
	addr := getPoolerAddr(pooler)
	return c.cache.getOrDial(ctx, addr)
}

// getPoolerAddr returns the gRPC address for a pooler.
func getPoolerAddr(pooler *clustermetadatapb.MultiPooler) string {
	return netutil.JoinHostPort(pooler.Hostname, pooler.PortMap["grpc"])
}

//
// Consensus Service Methods
//

// BeginTerm sends a BeginTerm request for leader appointment.
func (c *Client) BeginTerm(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.BeginTermRequest) (*consensusdatapb.BeginTermResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.BeginTerm(ctx, request)
}

// ConsensusStatus gets the consensus status of the multipooler.
func (c *Client) ConsensusStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.Status(ctx, request)
}

// GetLeadershipView gets the leadership view from the multipooler's perspective.
func (c *Client) GetLeadershipView(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.LeadershipViewRequest) (*consensusdatapb.LeadershipViewResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.GetLeadershipView(ctx, request)
}

// CanReachPrimary checks if the multipooler can reach the primary.
func (c *Client) CanReachPrimary(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.CanReachPrimaryRequest) (*consensusdatapb.CanReachPrimaryResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.CanReachPrimary(ctx, request)
}

//
// Manager Service Methods - Initialization
//

// InitializeEmptyPrimary initializes the multipooler as an empty primary.
func (c *Client) InitializeEmptyPrimary(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.InitializeEmptyPrimaryRequest) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.InitializeEmptyPrimary(ctx, request)
}

// InitializeAsStandby initializes the multipooler as a standby from a primary.
func (c *Client) InitializeAsStandby(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.InitializeAsStandbyRequest) (*multipoolermanagerdatapb.InitializeAsStandbyResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.InitializeAsStandby(ctx, request)
}

//
// Manager Service Methods - Status and Monitoring
//

// State gets the current status of the multipooler manager.
// This is called very frequently by the recovery engine health checks.
func (c *Client) State(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StateRequest) (*multipoolermanagerdatapb.StateResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.State(ctx, request)
}

//
// Manager Service Methods - Replication
//

// WaitForLSN waits for the multipooler to replay WAL up to the target LSN.
func (c *Client) WaitForLSN(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.WaitForLSNRequest) (*multipoolermanagerdatapb.WaitForLSNResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.WaitForLSN(ctx, request)
}

// SetPrimaryConnInfo configures the standby's connection to a primary.
func (c *Client) SetPrimaryConnInfo(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.SetPrimaryConnInfoRequest) (*multipoolermanagerdatapb.SetPrimaryConnInfoResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.SetPrimaryConnInfo(ctx, request)
}

// StartReplication starts WAL replay on standby.
func (c *Client) StartReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StartReplicationRequest) (*multipoolermanagerdatapb.StartReplicationResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.StartReplication(ctx, request)
}

// StopReplication stops replication based on the specified mode.
func (c *Client) StopReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StopReplicationRequest) (*multipoolermanagerdatapb.StopReplicationResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.StopReplication(ctx, request)
}

// StandbyReplicationStatus gets the current replication status of the standby.
func (c *Client) StandbyReplicationStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StandbyReplicationStatusRequest) (*multipoolermanagerdatapb.StandbyReplicationStatusResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.StandbyReplicationStatus(ctx, request)
}

// Status gets unified status that works for both PRIMARY and REPLICA poolers.
func (c *Client) Status(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StatusRequest) (*multipoolermanagerdatapb.StatusResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.Status(ctx, request)
}

// ResetReplication resets the standby's connection to its primary.
func (c *Client) ResetReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ResetReplicationRequest) (*multipoolermanagerdatapb.ResetReplicationResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.ResetReplication(ctx, request)
}

// StopReplicationAndGetStatus stops replication and returns the current status.
func (c *Client) StopReplicationAndGetStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StopReplicationAndGetStatusRequest) (*multipoolermanagerdatapb.StopReplicationAndGetStatusResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.StopReplicationAndGetStatus(ctx, request)
}

//
// Manager Service Methods - Synchronous Replication
//

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings.
func (c *Client) ConfigureSynchronousReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.ConfigureSynchronousReplication(ctx, request)
}

// UpdateSynchronousStandbyList updates the synchronous standby list.
func (c *Client) UpdateSynchronousStandbyList(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.UpdateSynchronousStandbyList(ctx, request)
}

//
// Manager Service Methods - Primary Status
//

// PrimaryStatus gets the status of the primary server.
func (c *Client) PrimaryStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.PrimaryStatusRequest) (*multipoolermanagerdatapb.PrimaryStatusResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.PrimaryStatus(ctx, request)
}

// PrimaryPosition gets the current LSN position of the primary.
func (c *Client) PrimaryPosition(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.PrimaryPositionRequest) (*multipoolermanagerdatapb.PrimaryPositionResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.PrimaryPosition(ctx, request)
}

// GetFollowers gets the list of follower servers with detailed replication status.
func (c *Client) GetFollowers(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetFollowersRequest) (*multipoolermanagerdatapb.GetFollowersResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.GetFollowers(ctx, request)
}

//
// Manager Service Methods - Promotion and Demotion
//

// Promote promotes the multipooler to primary.
func (c *Client) Promote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.PromoteRequest) (*multipoolermanagerdatapb.PromoteResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.Promote(ctx, request)
}

// Demote demotes the multipooler from primary.
func (c *Client) Demote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.DemoteRequest) (*multipoolermanagerdatapb.DemoteResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.Demote(ctx, request)
}

// UndoDemote undoes a demotion.
func (c *Client) UndoDemote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.UndoDemoteRequest) (*multipoolermanagerdatapb.UndoDemoteResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.UndoDemote(ctx, request)
}

//
// Manager Service Methods - Type and Term Management
//

// ChangeType changes the pooler type (PRIMARY/REPLICA).
func (c *Client) ChangeType(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ChangeTypeRequest) (*multipoolermanagerdatapb.ChangeTypeResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.ChangeType(ctx, request)
}

// SetTerm sets the consensus term information.
func (c *Client) SetTerm(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.SetTermRequest) (*multipoolermanagerdatapb.SetTermResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.SetTerm(ctx, request)
}

//
// Manager Service Methods - Durability Policy
//

// GetDurabilityPolicy retrieves the active durability policy from the local database.
func (c *Client) GetDurabilityPolicy(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetDurabilityPolicyRequest) (*multipoolermanagerdatapb.GetDurabilityPolicyResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.GetDurabilityPolicy(ctx, request)
}

// CreateDurabilityPolicy creates a new durability policy in the local database.
func (c *Client) CreateDurabilityPolicy(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.CreateDurabilityPolicyRequest) (*multipoolermanagerdatapb.CreateDurabilityPolicyResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.CreateDurabilityPolicy(ctx, request)
}

//
// Manager Service Methods - Backup and Restore
//

// Backup performs a backup.
func (c *Client) Backup(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.BackupRequest) (*multipoolermanagerdatapb.BackupResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.Backup(ctx, request)
}

// RestoreFromBackup restores from a backup.
func (c *Client) RestoreFromBackup(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.RestoreFromBackupRequest) (*multipoolermanagerdatapb.RestoreFromBackupResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.RestoreFromBackup(ctx, request)
}

// GetBackups retrieves backup information.
func (c *Client) GetBackups(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetBackupsRequest) (*multipoolermanagerdatapb.GetBackupsResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.GetBackups(ctx, request)
}

// GetBackupByJobId queries a multipooler for a backup by its job_id annotation.
func (c *Client) GetBackupByJobId(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetBackupByJobIdRequest) (*multipoolermanagerdatapb.GetBackupByJobIdResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.GetBackupByJobId(ctx, request)
}

//
// Manager Service Methods - Timeline Repair
//

// RewindToSource performs pg_rewind to synchronize a replica with its source.
func (c *Client) RewindToSource(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.RewindToSourceRequest) (*multipoolermanagerdatapb.RewindToSourceResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.RewindToSource(ctx, request)
}

//
// Connection Management Methods
//

// Close closes all persistent connections and frees resources.
func (c *Client) Close() {
	c.cache.closeAll()
}

// CloseTablet closes the persistent connection to a specific pooler.
func (c *Client) CloseTablet(pooler *clustermetadatapb.MultiPooler) {
	addr := getPoolerAddr(pooler)
	c.cache.close(addr)
}
