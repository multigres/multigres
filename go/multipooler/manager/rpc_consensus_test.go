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
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// TestRequestVote_AlreadyVotedInOlderTerm tests the scenario where:
// 1. Node has voted for candidate A in term 5
// 2. Candidate B requests vote in term 10 (newer term)
// Expected: Vote should be granted because the newer term should reset the vote
// Bug: Currently the code checks votedFor before updating the term, which would reject the vote
func TestRequestVote_AlreadyVotedInOlderTerm(t *testing.T) {
	ctx := context.Background()
	pm, mock, tmpDir := setupManagerWithMockDB(t)

	// Step 1: Initialize term to 5 and vote for "candidate-A"
	initialTerm := &pgctldpb.ConsensusTerm{
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

	// Mock expectations for GetCurrentWALPosition call
	// Return a small LSN so candidate's LastLogIndex of 1000 is higher
	mock.ExpectQuery("SELECT pg_current_wal_lsn\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/100"))

	req := &consensusdatapb.RequestVoteRequest{
		Term:         10, // Newer term
		CandidateId:  "candidate-B",
		ShardId:      "shard-1",
		LastLogIndex: 1000, // Higher than LSN 0/100 (256)
		LastLogTerm:  10,   // Must be at least as high as the term being voted in
	}

	resp, err := pm.RequestVote(ctx, req)

	// Should succeed
	require.NoError(t, err)
	require.NotNil(t, resp)

	// BUG EXPOSED: The vote should be granted because term 10 is newer than term 5
	// The code should:
	// 1. See that req.Term (10) > currentTerm (5)
	// 2. Update the term to 10 and reset votedFor to nil
	// 3. Grant the vote to candidate-B
	//
	// However, with the current bug, the code checks votedFor != nil && votedFor != "candidate-B"
	// BEFORE updating the term, so it incorrectly rejects the vote.
	assert.True(t, resp.VoteGranted, "Vote should be granted when request term is newer than current term, even if already voted in older term")
	assert.Equal(t, int64(10), resp.Term)

	// Verify the vote was persisted
	loadedTerm, err := GetTerm(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, int64(10), loadedTerm.CurrentTerm)
	assert.Equal(t, "candidate-B", loadedTerm.VotedFor.GetName())

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRequestVote_AlreadyVotedInSameTerm tests the scenario where:
// 1. Node has voted for candidate A in term 5
// 2. Candidate B requests vote in term 5 (same term)
// Expected: Vote should be rejected
func TestRequestVote_AlreadyVotedInSameTerm(t *testing.T) {
	ctx := context.Background()
	pm, mock, tmpDir := setupManagerWithMockDB(t)

	// Step 1: Initialize term to 5 and vote for "candidate-A"
	initialTerm := &pgctldpb.ConsensusTerm{
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

	req := &consensusdatapb.RequestVoteRequest{
		Term:         5, // Same term
		CandidateId:  "candidate-B",
		ShardId:      "shard-1",
		LastLogIndex: 200,
		LastLogTerm:  4,
	}

	resp, err := pm.RequestVote(ctx, req)

	// Should succeed (no error)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Vote should be rejected because already voted for candidate-A in this term
	assert.False(t, resp.VoteGranted, "Vote should be rejected when already voted for different candidate in same term")
	assert.Equal(t, int64(5), resp.Term)

	// Verify the vote was NOT changed
	loadedTerm, err := GetTerm(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, int64(5), loadedTerm.CurrentTerm)
	assert.Equal(t, "candidate-A", loadedTerm.VotedFor.GetName(), "Vote should remain for candidate-A")

	// Verify no database calls were made (early rejection)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRequestVote_AlreadyVotedForSameCandidateInSameTerm tests the scenario where:
// 1. Node has voted for candidate A in term 5
// 2. Candidate A requests vote again in term 5 (same candidate, same term)
// Expected: Vote should be granted (idempotent)
func TestRequestVote_AlreadyVotedForSameCandidateInSameTerm(t *testing.T) {
	ctx := context.Background()
	pm, mock, tmpDir := setupManagerWithMockDB(t)

	// Step 1: Initialize term to 5 and vote for "candidate-A"
	initialTerm := &pgctldpb.ConsensusTerm{
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

	// Mock expectations for GetCurrentWALPosition call
	// Return a small LSN so candidate's LastLogIndex is higher
	mock.ExpectQuery("SELECT pg_current_wal_lsn\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/100"))

	req := &consensusdatapb.RequestVoteRequest{
		Term:         5, // Same term
		CandidateId:  "candidate-A",
		ShardId:      "shard-1",
		LastLogIndex: 1000, // Higher than LSN 0/100 (256)
		LastLogTerm:  5,    // Must match the term
	}

	resp, err := pm.RequestVote(ctx, req)

	// Should succeed
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Vote should be granted (idempotent - already voted for this candidate)
	assert.True(t, resp.VoteGranted, "Vote should be granted when already voted for same candidate in same term (idempotent)")
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
