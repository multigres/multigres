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

package manager

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/servenv"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestBeginTerm_AlreadyVotedInOlderTerm tests the scenario where:
// 1. Node has voted for candidate A in term 5
// 2. Candidate B requests vote in term 10 (newer term)
// Expected: Vote should be accepted because the newer term should reset the vote
// Bug: Currently the code checks votedFor before updating the term, which would reject the vote
func TestBeginTerm_AlreadyVotedInOlderTerm(t *testing.T) {
	ctx := context.Background()
	pm, mock, tmpDir := setupManagerWithMockDB(t)

	// Step 1: Initialize term to 5 and vote for "candidate-A"
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		CurrentTerm: 5,
		VotedFor: &clustermetadatapb.ID{
			Cell: "zone1",
			Name: "candidate-A",
		},
	}
	err := SetTerm(tmpDir, initialTerm)
	require.NoError(t, err)

	// Reload the term in the manager
	pm.mu.Lock()
	pm.consensusTerm = initialTerm
	pm.mu.Unlock()

	// Step 2: Candidate B requests vote in term 10 (newer term)
	// Mock expectations for Ping call (health check)
	mock.ExpectPing()

	// Mock expectations for replication status check
	// Return recent timestamp to indicate we're caught up
	recentTime := time.Now().Add(-5 * time.Second)
	mock.ExpectQuery("SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").
		WillReturnRows(sqlmock.NewRows([]string{"last_msg_receipt_time"}).AddRow(recentTime))

	req := &consensusdatapb.BeginTermRequest{
		Term:        10, // Newer term
		CandidateId: "candidate-B",
		ShardId:     "shard-1",
	}

	resp, err := pm.BeginTerm(ctx, req)

	// Should succeed
	require.NoError(t, err)
	require.NotNil(t, resp)

	// BUG EXPOSED: The vote should be accepted because term 10 is newer than term 5
	// The code should:
	// 1. See that req.Term (10) > currentTerm (5)
	// 2. Update the term to 10 and reset votedFor to nil
	// 3. Grant the vote to candidate-B
	//
	// However, with the current bug, the code checks votedFor != nil && votedFor != "candidate-B"
	// BEFORE updating the term, so it incorrectly rejects the vote.
	assert.True(t, resp.Accepted, "Vote should be accepted when request term is newer than current term, even if already voted in older term")
	assert.Equal(t, int64(10), resp.Term)

	// Verify the vote was persisted
	loadedTerm, err := GetTerm(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, int64(10), loadedTerm.CurrentTerm)
	assert.Equal(t, "candidate-B", loadedTerm.VotedFor.GetName())

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestBeginTerm_AlreadyVotedInSameTerm tests the scenario where:
// 1. Node has voted for candidate A in term 5
// 2. Candidate B requests vote in term 5 (same term)
// Expected: Vote should be rejected
func TestBeginTerm_AlreadyVotedInSameTerm(t *testing.T) {
	ctx := context.Background()
	pm, mock, tmpDir := setupManagerWithMockDB(t)

	// Step 1: Initialize term to 5 and vote for "candidate-A"
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		CurrentTerm: 5,
		VotedFor: &clustermetadatapb.ID{
			Cell: "zone1",
			Name: "candidate-A",
		},
	}
	err := SetTerm(tmpDir, initialTerm)
	require.NoError(t, err)

	// Reload the term in the manager
	pm.mu.Lock()
	pm.consensusTerm = initialTerm
	pm.mu.Unlock()

	// Step 2: Candidate B requests vote in term 5 (same term)
	// Mock expectations for Ping call (health check) - will reject early, no WAL check needed
	mock.ExpectPing()

	req := &consensusdatapb.BeginTermRequest{
		Term:        5, // Same term
		CandidateId: "candidate-B",
		ShardId:     "shard-1",
	}

	resp, err := pm.BeginTerm(ctx, req)

	// Should succeed (no error)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Vote should be rejected because already voted for candidate-A in this term
	assert.False(t, resp.Accepted, "Vote should be rejected when already voted for different candidate in same term")
	assert.Equal(t, int64(5), resp.Term)

	// Verify the vote was NOT changed
	loadedTerm, err := GetTerm(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, int64(5), loadedTerm.CurrentTerm)
	assert.Equal(t, "candidate-A", loadedTerm.VotedFor.GetName(), "Vote should remain for candidate-A")

	// Verify no database calls were made (early rejection)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestBeginTerm_AlreadyVotedForSameCandidateInSameTerm tests the scenario where:
// 1. Node has voted for candidate A in term 5
// 2. Candidate A requests vote again in term 5 (same candidate, same term)
// Expected: Vote should be accepted (idempotent)
func TestBeginTerm_AlreadyVotedForSameCandidateInSameTerm(t *testing.T) {
	ctx := context.Background()
	pm, mock, tmpDir := setupManagerWithMockDB(t)

	// Step 1: Initialize term to 5 and vote for "candidate-A"
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		CurrentTerm: 5,
		VotedFor: &clustermetadatapb.ID{
			Cell: "zone1",
			Name: "candidate-A",
		},
	}
	err := SetTerm(tmpDir, initialTerm)
	require.NoError(t, err)

	// Reload the term in the manager
	pm.mu.Lock()
	pm.consensusTerm = initialTerm
	pm.mu.Unlock()

	// Step 2: Candidate A requests vote again in term 5 (same candidate, same term)
	// Mock expectations for Ping call (health check)
	mock.ExpectPing()

	// Mock expectations for replication status check
	// Return recent timestamp to indicate we're caught up
	recentTime := time.Now().Add(-5 * time.Second)
	mock.ExpectQuery("SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").
		WillReturnRows(sqlmock.NewRows([]string{"last_msg_receipt_time"}).AddRow(recentTime))

	req := &consensusdatapb.BeginTermRequest{
		Term:        5, // Same term
		CandidateId: "candidate-A",
		ShardId:     "shard-1",
	}

	resp, err := pm.BeginTerm(ctx, req)

	// Should succeed
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Vote should be accepted (idempotent - already voted for this candidate)
	assert.True(t, resp.Accepted, "Vote should be accepted when already voted for same candidate in same term (idempotent)")
	assert.Equal(t, int64(5), resp.Term)

	// Verify the vote remains the same
	loadedTerm, err := GetTerm(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, int64(5), loadedTerm.CurrentTerm)
	assert.Equal(t, "candidate-A", loadedTerm.VotedFor.GetName())

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Helper function to setup a manager with a mock database
func setupManagerWithMockDB(t *testing.T) (*MultiPoolerManager, sqlmock.Sqlmock, string) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })

	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-voter",
	}
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	tmpDir := t.TempDir()
	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PgctldAddr: pgctldAddr,
		PoolerDir:  tmpDir,
	}
	pm := NewMultiPoolerManager(logger, config)
	t.Cleanup(func() { pm.Close() })

	senv := servenv.NewServEnv()
	go pm.Start(senv)

	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	// Create mock database connection with ping monitoring enabled
	mockDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	t.Cleanup(func() { mockDB.Close() })

	pm.db = mockDB

	// Create the pg_data directory to simulate initialized data directory
	pgDataDir := tmpDir + "/pg_data"
	err = os.MkdirAll(pgDataDir, 0o755)
	require.NoError(t, err)
	// Create PG_VERSION file to mark it as initialized
	err = os.WriteFile(pgDataDir+"/PG_VERSION", []byte("18\n"), 0o644)
	require.NoError(t, err)

	return pm, mock, tmpDir
}

// ============================================================================
// CanReachPrimary Tests
// ============================================================================

func TestCanReachPrimary_Success_MatchingHostPort(t *testing.T) {
	ctx := context.Background()
	pm, mock, _ := setupManagerWithMockDB(t)

	// Mock expectations for pg_stat_wal_receiver query
	conninfo := "host=localhost port=5432 user=replicator application_name=test-cell_standby-1"
	mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
		WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
			AddRow("streaming", conninfo))

	req := &consensusdatapb.CanReachPrimaryRequest{
		PrimaryHost: "localhost",
		PrimaryPort: 5432,
	}

	resp, err := pm.CanReachPrimary(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Reachable, "Should be reachable when WAL receiver is active and connected to correct host/port")
	assert.Empty(t, resp.ErrorMessage)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCanReachPrimary_NoWALReceiver(t *testing.T) {
	ctx := context.Background()
	pm, mock, _ := setupManagerWithMockDB(t)

	// Mock expectations - query returns no rows (no WAL receiver)
	mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
		WillReturnError(sql.ErrNoRows)

	req := &consensusdatapb.CanReachPrimaryRequest{
		PrimaryHost: "localhost",
		PrimaryPort: 5432,
	}

	resp, err := pm.CanReachPrimary(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Reachable, "Should not be reachable when there is no WAL receiver")
	assert.Contains(t, resp.ErrorMessage, "no active WAL receiver")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCanReachPrimary_WALReceiverStopping(t *testing.T) {
	ctx := context.Background()
	pm, mock, _ := setupManagerWithMockDB(t)

	// Mock expectations - WAL receiver is stopping
	conninfo := "host=localhost port=5432 user=replicator"
	mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
		WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
			AddRow("stopping", conninfo))

	req := &consensusdatapb.CanReachPrimaryRequest{
		PrimaryHost: "localhost",
		PrimaryPort: 5432,
	}

	resp, err := pm.CanReachPrimary(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Reachable, "Should not be reachable when WAL receiver is stopping")
	assert.Contains(t, resp.ErrorMessage, "WAL receiver is stopping")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCanReachPrimary_HostMismatch(t *testing.T) {
	ctx := context.Background()
	pm, mock, _ := setupManagerWithMockDB(t)

	// Mock expectations - connected to different host
	conninfo := "host=other-host port=5432 user=replicator"
	mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
		WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
			AddRow("streaming", conninfo))

	req := &consensusdatapb.CanReachPrimaryRequest{
		PrimaryHost: "localhost",
		PrimaryPort: 5432,
	}

	resp, err := pm.CanReachPrimary(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Reachable, "Should not be reachable when connected to different host")
	assert.Contains(t, resp.ErrorMessage, "expected localhost, got other-host")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCanReachPrimary_PortMismatch(t *testing.T) {
	ctx := context.Background()
	pm, mock, _ := setupManagerWithMockDB(t)

	// Mock expectations - connected to different port
	conninfo := "host=localhost port=5433 user=replicator"
	mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
		WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
			AddRow("streaming", conninfo))

	req := &consensusdatapb.CanReachPrimaryRequest{
		PrimaryHost: "localhost",
		PrimaryPort: 5432,
	}

	resp, err := pm.CanReachPrimary(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Reachable, "Should not be reachable when connected to different port")
	assert.Contains(t, resp.ErrorMessage, "expected 5432, got 5433")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCanReachPrimary_NoDatabaseConnection(t *testing.T) {
	ctx := context.Background()
	pm, _, _ := setupManagerWithMockDB(t)

	// Set db to nil to simulate no connection
	pm.db = nil

	req := &consensusdatapb.CanReachPrimaryRequest{
		PrimaryHost: "localhost",
		PrimaryPort: 5432,
	}

	resp, err := pm.CanReachPrimary(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Reachable, "Should not be reachable when database connection is not available")
	assert.Contains(t, resp.ErrorMessage, "database connection not available")
}

func TestCanReachPrimary_InvalidConnInfo(t *testing.T) {
	ctx := context.Background()
	pm, mock, _ := setupManagerWithMockDB(t)

	// Mock expectations - invalid conninfo format
	conninfo := "invalid format without equals"
	mock.ExpectQuery("SELECT status, conninfo FROM pg_stat_wal_receiver").
		WillReturnRows(sqlmock.NewRows([]string{"status", "conninfo"}).
			AddRow("streaming", conninfo))

	req := &consensusdatapb.CanReachPrimaryRequest{
		PrimaryHost: "localhost",
		PrimaryPort: 5432,
	}

	resp, err := pm.CanReachPrimary(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Reachable, "Should not be reachable when conninfo parsing fails")
	assert.Contains(t, resp.ErrorMessage, "failed to parse conninfo")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ============================================================================
// GetWALPosition Tests
// ============================================================================

func TestGetWALPosition_Primary(t *testing.T) {
	ctx := context.Background()
	pm, mock, _ := setupManagerWithMockDB(t)

	// Mock expectations for primary (not in recovery)
	mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))

	// Mock pg_current_wal_lsn for primary
	mock.ExpectQuery("SELECT pg_current_wal_lsn\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/3000000"))

	req := &consensusdatapb.GetWALPositionRequest{}

	resp, err := pm.GetWALPosition(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.WalPosition)
	assert.Equal(t, "0/3000000", resp.WalPosition.CurrentLsn)
	assert.Empty(t, resp.WalPosition.LastReceiveLsn, "Primary should not have LastReceiveLsn")
	assert.Empty(t, resp.WalPosition.LastReplayLsn, "Primary should not have LastReplayLsn")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetWALPosition_Standby(t *testing.T) {
	ctx := context.Background()
	pm, mock, _ := setupManagerWithMockDB(t)

	// Mock expectations for standby (in recovery)
	mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))

	// Mock pg_last_wal_receive_lsn and pg_last_wal_replay_lsn for standby
	mock.ExpectQuery("SELECT pg_last_wal_receive_lsn\\(\\), pg_last_wal_replay_lsn\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_last_wal_receive_lsn", "pg_last_wal_replay_lsn"}).
			AddRow("0/3000000", "0/2FFFFFF"))

	req := &consensusdatapb.GetWALPositionRequest{}

	resp, err := pm.GetWALPosition(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.WalPosition)
	assert.Empty(t, resp.WalPosition.CurrentLsn, "Standby should not have CurrentLsn")
	assert.Equal(t, "0/3000000", resp.WalPosition.LastReceiveLsn)
	assert.Equal(t, "0/2FFFFFF", resp.WalPosition.LastReplayLsn)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetWALPosition_NoDatabaseConnection(t *testing.T) {
	ctx := context.Background()
	pm, _, _ := setupManagerWithMockDB(t)

	// Set db to nil to simulate no connection
	pm.db = nil

	req := &consensusdatapb.GetWALPositionRequest{}

	resp, err := pm.GetWALPosition(ctx, req)

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "database connection not available")
}

func TestGetWALPosition_QueryError(t *testing.T) {
	ctx := context.Background()
	pm, mock, _ := setupManagerWithMockDB(t)

	// Mock pg_is_in_recovery query failure
	mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
		WillReturnError(assert.AnError)

	req := &consensusdatapb.GetWALPositionRequest{}

	resp, err := pm.GetWALPosition(ctx, req)

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "failed to check recovery status")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ============================================================================
// ConsensusStatus Tests
// ============================================================================

func TestConsensusStatus_HealthyPrimary(t *testing.T) {
	ctx := context.Background()
	pm, mock, tmpDir := setupManagerWithMockDB(t)

	// Setup: Initialize term file
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		CurrentTerm: 5,
		VotedFor: &clustermetadatapb.ID{
			Cell: "zone1",
			Name: "leader-node",
		},
	}
	err := SetTerm(tmpDir, initialTerm)
	require.NoError(t, err)

	pm.mu.Lock()
	pm.consensusTerm = initialTerm
	pm.mu.Unlock()

	// Mock expectations for heartbeat query
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(leader_term\\), 0\\)").
		WillReturnRows(sqlmock.NewRows([]string{"leader_term"}).AddRow(5))

	// Mock expectations for WAL position (primary)
	mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
	mock.ExpectQuery("SELECT pg_current_wal_lsn\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/4000000"))

	// Mock expectations for role determination
	mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))

	req := &consensusdatapb.StatusRequest{
		ShardId: "test-shard",
	}

	resp, err := pm.ConsensusStatus(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test-voter", resp.PoolerId)
	assert.Equal(t, int64(5), resp.CurrentTerm)
	assert.Equal(t, int64(5), resp.LeaderTerm)
	assert.True(t, resp.IsHealthy)
	assert.True(t, resp.IsEligible)
	assert.Equal(t, "zone1", resp.Cell)
	assert.Equal(t, "primary", resp.Role)
	require.NotNil(t, resp.WalPosition)
	assert.Equal(t, "0/4000000", resp.WalPosition.CurrentLsn)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConsensusStatus_HealthyStandby(t *testing.T) {
	ctx := context.Background()
	pm, mock, tmpDir := setupManagerWithMockDB(t)

	// Setup: Initialize term file
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		CurrentTerm: 3,
		VotedFor:    nil,
	}
	err := SetTerm(tmpDir, initialTerm)
	require.NoError(t, err)

	pm.mu.Lock()
	pm.consensusTerm = initialTerm
	pm.mu.Unlock()

	// Mock expectations for heartbeat query
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(leader_term\\), 0\\)").
		WillReturnRows(sqlmock.NewRows([]string{"leader_term"}).AddRow(5))

	// Mock expectations for WAL position (standby)
	mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
	mock.ExpectQuery("SELECT pg_last_wal_receive_lsn\\(\\), pg_last_wal_replay_lsn\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_last_wal_receive_lsn", "pg_last_wal_replay_lsn"}).
			AddRow("0/5000000", "0/4FFFFFF"))

	// Mock expectations for role determination
	mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))

	req := &consensusdatapb.StatusRequest{
		ShardId: "test-shard",
	}

	resp, err := pm.ConsensusStatus(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test-voter", resp.PoolerId)
	assert.Equal(t, int64(3), resp.CurrentTerm)
	assert.Equal(t, int64(5), resp.LeaderTerm)
	assert.True(t, resp.IsHealthy)
	assert.True(t, resp.IsEligible)
	assert.Equal(t, "zone1", resp.Cell)
	assert.Equal(t, "replica", resp.Role)
	require.NotNil(t, resp.WalPosition)
	assert.Equal(t, "0/5000000", resp.WalPosition.LastReceiveLsn)
	assert.Equal(t, "0/4FFFFFF", resp.WalPosition.LastReplayLsn)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConsensusStatus_NoDatabaseConnection(t *testing.T) {
	ctx := context.Background()
	pm, _, tmpDir := setupManagerWithMockDB(t)

	// Setup: Initialize term file
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		CurrentTerm: 7,
		VotedFor:    nil,
	}
	err := SetTerm(tmpDir, initialTerm)
	require.NoError(t, err)

	pm.mu.Lock()
	pm.consensusTerm = initialTerm
	pm.mu.Unlock()

	// Set db to nil to simulate no connection
	pm.db = nil

	req := &consensusdatapb.StatusRequest{
		ShardId: "test-shard",
	}

	resp, err := pm.ConsensusStatus(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test-voter", resp.PoolerId)
	assert.Equal(t, int64(7), resp.CurrentTerm)
	assert.Equal(t, int64(0), resp.LeaderTerm, "LeaderTerm should be 0 when DB is unavailable")
	assert.False(t, resp.IsHealthy, "Should be unhealthy when DB is unavailable")
	assert.True(t, resp.IsEligible)
	assert.Equal(t, "zone1", resp.Cell)
	assert.Equal(t, "replica", resp.Role, "Role should default to replica when DB is unavailable")
	require.NotNil(t, resp.WalPosition)
	assert.Empty(t, resp.WalPosition.CurrentLsn)
}

func TestConsensusStatus_DatabaseQueryFailure(t *testing.T) {
	ctx := context.Background()
	pm, mock, tmpDir := setupManagerWithMockDB(t)

	// Setup: Initialize term file
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		CurrentTerm: 4,
		VotedFor:    nil,
	}
	err := SetTerm(tmpDir, initialTerm)
	require.NoError(t, err)

	pm.mu.Lock()
	pm.consensusTerm = initialTerm
	pm.mu.Unlock()

	// Mock expectations - heartbeat query fails
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(leader_term\\), 0\\)").
		WillReturnError(assert.AnError)

	req := &consensusdatapb.StatusRequest{
		ShardId: "test-shard",
	}

	resp, err := pm.ConsensusStatus(ctx, req)

	require.NoError(t, err, "ConsensusStatus should not return error even if DB query fails")
	require.NotNil(t, resp)
	assert.Equal(t, "test-voter", resp.PoolerId)
	assert.Equal(t, int64(4), resp.CurrentTerm)
	assert.Equal(t, int64(0), resp.LeaderTerm, "LeaderTerm should be 0 when query fails")
	assert.False(t, resp.IsHealthy, "Should be unhealthy when query fails")
	assert.True(t, resp.IsEligible)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConsensusStatus_TermNotLoadedYet(t *testing.T) {
	ctx := context.Background()
	pm, mock, tmpDir := setupManagerWithMockDB(t)

	// Setup: Initialize term file on disk but not in memory
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		CurrentTerm: 8,
		VotedFor:    nil,
	}
	err := SetTerm(tmpDir, initialTerm)
	require.NoError(t, err)

	// Set consensusTerm to nil to simulate it not being loaded yet
	pm.mu.Lock()
	pm.consensusTerm = nil
	pm.mu.Unlock()

	// Mock expectations for heartbeat query
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(leader_term\\), 0\\)").
		WillReturnRows(sqlmock.NewRows([]string{"leader_term"}).AddRow(8))

	// Mock expectations for WAL position (primary)
	mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
	mock.ExpectQuery("SELECT pg_current_wal_lsn\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/6000000"))

	// Mock expectations for role determination
	mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))

	req := &consensusdatapb.StatusRequest{
		ShardId: "test-shard",
	}

	resp, err := pm.ConsensusStatus(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(8), resp.CurrentTerm, "Should load term from disk if not in memory")
	assert.Equal(t, int64(8), resp.LeaderTerm)
	assert.True(t, resp.IsHealthy)

	// Verify term was loaded into memory
	pm.mu.Lock()
	assert.NotNil(t, pm.consensusTerm, "Term should be loaded into memory")
	assert.Equal(t, int64(8), pm.consensusTerm.CurrentTerm)
	pm.mu.Unlock()

	assert.NoError(t, mock.ExpectationsWereMet())
}
