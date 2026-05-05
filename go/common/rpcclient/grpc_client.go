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
	"sync"

	"google.golang.org/grpc"

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
// The transportCreds dial option configures TLS or insecure transport.
func NewClient(capacity int, transportCreds grpc.DialOption) *Client {
	return &Client{
		cache: newConnCacheWithCapacity(capacity, transportCreds),
	}
}

// dialPersistent gets or creates a cached connection to the pooler.
// It returns the connection and a closer function that must be called
// when the RPC is complete to decrement the reference count.
// The closer should be called even if the RPC fails.
func (c *Client) dialPersistent(ctx context.Context, pooler *clustermetadatapb.MultiPooler) (*cachedConn, closeFunc, error) {
	addr := getPoolerAddr(pooler)
	return c.cache.getOrDial(ctx, addr, pooler.Id)
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

// Recruit asks a pooler to stop replication participation and record a TermRevocation.
func (c *Client) Recruit(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.RecruitRequest) (*consensusdatapb.RecruitResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.Recruit(ctx, request)
}

// Propose sends a role assignment to a recruited pooler.
func (c *Client) Propose(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.ProposeRequest) (*consensusdatapb.ProposeResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.Propose(ctx, request)
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

// EmergencyDemote demotes the current leader server.
func (c *Client) EmergencyDemote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.EmergencyDemoteRequest) (*multipoolermanagerdatapb.EmergencyDemoteResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.EmergencyDemote(ctx, request)
}

// DemoteStalePrimary demotes a stale primary that came back after failover.
func (c *Client) DemoteStalePrimary(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.DemoteStalePrimaryRequest) (*multipoolermanagerdatapb.DemoteStalePrimaryResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.DemoteStalePrimary(ctx, request)
}

// Promote promotes the multipooler to primary.
func (c *Client) Promote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.PromoteRequest) (*multipoolermanagerdatapb.PromoteResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.Promote(ctx, request)
}

// UpdateConsensusRule updates the synchronous standby list (quorum membership).
func (c *Client) UpdateConsensusRule(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.UpdateConsensusRule(ctx, request)
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

	return conn.consensusClient.SetPrimaryConnInfo(ctx, request)
}

// RewindToSource performs pg_rewind to synchronize a replica with its source.
func (c *Client) RewindToSource(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.RewindToSourceRequest) (*multipoolermanagerdatapb.RewindToSourceResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.consensusClient.RewindToSource(ctx, request)
}

//
// Manager Service Methods - Status and Monitoring
//

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

// ExpireBackups removes old backups according to retention policy.
func (c *Client) ExpireBackups(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ExpireBackupsRequest) (*multipoolermanagerdatapb.ExpireBackupsResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.ExpireBackups(ctx, request)
}

//
// Manager Service Methods - PostgreSQL Restart Control
//

// SetPostgresRestartsEnabled enables or disables automatic PostgreSQL restarts on a pooler.
func (c *Client) SetPostgresRestartsEnabled(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest) (*multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = closer()
	}()

	return conn.managerClient.SetPostgresRestartsEnabled(ctx, request)
}

//
// Manager Service Methods - Health Streaming
//

// managerHealthStream wraps the gRPC bidirectional stream and releases the
// connection reference when the stream ends.
type managerHealthStream struct {
	stream grpc.BidiStreamingClient[multipoolermanagerdatapb.ManagerHealthStreamClientMessage, multipoolermanagerdatapb.ManagerHealthStreamResponse]
	closer closeFunc
	once   sync.Once
}

// ManagerHealthStream opens a bidirectional health stream to a pooler.
//
// The caller must send an init message via stream.Send before reading snapshots.
// The connection reference is held for the stream's lifetime and released
// automatically when Recv or Send returns a non-nil error.
func (c *Client) ManagerHealthStream(ctx context.Context, pooler *clustermetadatapb.MultiPooler) (ManagerHealthStream, error) {
	conn, closer, err := c.dialPersistent(ctx, pooler)
	if err != nil {
		return nil, err
	}

	stream, err := conn.managerClient.ManagerHealthStream(ctx)
	if err != nil {
		_ = closer()
		return nil, err
	}

	return &managerHealthStream{stream: stream, closer: closer}, nil
}

// Recv receives the next health snapshot from the stream.
// Returns a non-nil error on stream end or network failure, and releases the
// connection reference.
func (s *managerHealthStream) Recv() (*multipoolermanagerdatapb.ManagerHealthStreamResponse, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		s.once.Do(func() { _ = s.closer() })
	}
	return resp, err
}

// Send sends a message to the pooler (init or poll request).
// Returns a non-nil error on failure and releases the connection reference.
func (s *managerHealthStream) Send(msg *multipoolermanagerdatapb.ManagerHealthStreamClientMessage) error {
	err := s.stream.Send(msg)
	if err != nil {
		s.once.Do(func() { _ = s.closer() })
	}
	return err
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
