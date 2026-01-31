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
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// createMockNode creates a mock node for testing using FakeClient
func createMockNode(fakeClient *rpcclient.FakeClient, name string, term int64, walPosition string, healthy bool, role string) *multiorchdatapb.PoolerHealthState {
	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      name,
	}

	pooler := &clustermetadatapb.MultiPooler{
		Id:       poolerID,
		Hostname: "localhost",
		PortMap: map[string]int32{
			"grpc": 9000,
		},
	}

	// Use topo helper to generate consistent key format
	poolerKey := topoclient.MultiPoolerIDString(poolerID)

	// Configure FakeClient responses for this pooler
	fakeClient.ConsensusStatusResponses[poolerKey] = &consensusdatapb.StatusResponse{
		CurrentTerm: term,
		IsHealthy:   healthy,
		Role:        role,
		WalPosition: &consensusdatapb.WALPosition{
			CurrentLsn:     walPosition,
			LastReceiveLsn: walPosition,
			LastReplayLsn:  walPosition,
		},
	}

	fakeClient.BeginTermResponses[poolerKey] = &consensusdatapb.BeginTermResponse{
		Accepted: true,
	}

	fakeClient.StateResponses[poolerKey] = &multipoolermanagerdatapb.StateResponse{
		State: "ready",
	}

	fakeClient.PromoteResponses[poolerKey] = &multipoolermanagerdatapb.PromoteResponse{}

	fakeClient.SetPrimaryConnInfoResponses[poolerKey] = &multipoolermanagerdatapb.SetPrimaryConnInfoResponse{}

	// Build ConsensusTerm if term > 0
	var consensusTerm *multipoolermanagerdatapb.ConsensusTerm
	if term > 0 {
		consensusTerm = &multipoolermanagerdatapb.ConsensusTerm{
			TermNumber: term,
		}
	}

	return &multiorchdatapb.PoolerHealthState{
		MultiPooler:      pooler,
		IsLastCheckValid: healthy,
		IsInitialized:    term > 0,
		ConsensusTerm:    consensusTerm,
	}
}

func TestDiscoverMaxTerm(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("success - finds max term from cohort", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/1000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 3, "0/1000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 7, "0/1000000", true, "standby"),
		}

		maxTerm, err := c.discoverMaxTerm(cohort)
		require.NoError(t, err)
		require.Equal(t, int64(7), maxTerm)
	})

	t.Run("error - returns error when all nodes have term 0", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 0, "0/1000000", false /* unhealthy */, "standby"),
			createMockNode(fakeClient, "mp2", 0, "0/1000000", false /* unhealthy */, "standby"),
		}

		_, err := c.discoverMaxTerm(cohort)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no poolers in cohort have initialized consensus term")
	})

	t.Run("success - ignores failed nodes", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}

		pooler2ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp2",
		}
		pooler2 := &clustermetadatapb.MultiPooler{
			Id:       pooler2ID,
			Hostname: "localhost",
			PortMap:  map[string]int32{"grpc": 9000},
		}
		fakeClient.Errors[topoclient.MultiPoolerIDString(pooler2ID)] = context.DeadlineExceeded

		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/1000000", true, "standby"),
			{MultiPooler: pooler2},
			createMockNode(fakeClient, "mp3", 3, "0/1000000", true, "standby"),
		}

		maxTerm, err := c.discoverMaxTerm(cohort)
		require.NoError(t, err)
		require.Equal(t, int64(5), maxTerm)
	})
}

func TestSelectCandidate(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("success - selects node with most advanced WAL", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/1000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 5, "0/3000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/2000000", true, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name)
	})

	t.Run("success - prefers healthy nodes", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", false, "standby"),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name)
	})

	t.Run("success - falls back to first node if none healthy", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/1000000", false, "standby"),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", false, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.NotNil(t, candidate)
		// Should select first available node
		require.Equal(t, "mp1", candidate.MultiPooler.Id.Name)
	})

	t.Run("error - no nodes available", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}

		poolerID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp1",
		}
		fakeClient.Errors[topoclient.MultiPoolerIDString(poolerID)] = context.DeadlineExceeded

		pooler := &clustermetadatapb.MultiPooler{
			Id:       poolerID,
			Hostname: "localhost",
			PortMap:  map[string]int32{"grpc": 9000},
		}

		cohort := []*multiorchdatapb.PoolerHealthState{
			{MultiPooler: pooler},
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.Error(t, err)
		require.Nil(t, candidate)
		require.Contains(t, err.Error(), "no healthy nodes available")
	})
}

func TestRecruitNodes(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("success - all nodes accept", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		candidate := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary")
		cohort := []*multiorchdatapb.PoolerHealthState{
			candidate,
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		recruited, err := c.recruitNodes(ctx, cohort, 6, candidate, consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE)
		require.NoError(t, err)
		require.Len(t, recruited, 3)
	})

	t.Run("success - some nodes reject", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		candidate := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary")

		cohort := []*multiorchdatapb.PoolerHealthState{
			candidate,
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/2000000", true, "standby"),
		}

		// mp3 will reject the term (override after creating the node)
		mp3ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp3",
		}
		fakeClient.BeginTermResponses[topoclient.MultiPoolerIDString(mp3ID)] = &consensusdatapb.BeginTermResponse{Accepted: false}

		recruited, err := c.recruitNodes(ctx, cohort, 6, candidate, consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE)
		require.NoError(t, err)
		require.Len(t, recruited, 2)
	})

	t.Run("success - excludes nodes with BeginTerm error", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		candidate := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary")

		cohort := []*multiorchdatapb.PoolerHealthState{
			candidate,
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		// mp3 returns an error even though it would accept the term
		mp3ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp3",
		}
		fakeClient.Errors[topoclient.MultiPoolerIDString(mp3ID)] = context.DeadlineExceeded

		recruited, err := c.recruitNodes(ctx, cohort, 6, candidate, consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE)
		require.NoError(t, err)
		require.Len(t, recruited, 2)
	})
}

func TestBeginTerm(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("success - achieves quorum", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		// Create default ANY_N quorum rule (majority: 2 of 3)
		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Test majority quorum",
		}

		proposedTerm := int64(6) // maxTerm (5) + 1
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, quorumRule, proposedTerm)
		require.NoError(t, err)
		require.NotNil(t, candidate)
		require.Equal(t, "mp1", candidate.MultiPooler.Id.Name) // Most advanced WAL
		require.Len(t, standbys, 2)
		require.Equal(t, int64(6), term)
	})

	t.Run("error - insufficient quorum", func(t *testing.T) {
		// Create cohort where only 1 out of 3 accepts (need 2 for quorum)
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		candidate := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "standby")

		cohort := []*multiorchdatapb.PoolerHealthState{
			candidate,
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		// Override responses after creating nodes
		// mp2 rejects the term
		mp2ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp2",
		}
		fakeClient.BeginTermResponses[topoclient.MultiPoolerIDString(mp2ID)] = &consensusdatapb.BeginTermResponse{Accepted: false}

		// mp3 returns an error
		mp3ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp3",
		}
		fakeClient.Errors[topoclient.MultiPoolerIDString(mp3ID)] = context.DeadlineExceeded

		// Create ANY_N quorum rule requiring 2 nodes
		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Test quorum requiring 2 nodes",
		}

		proposedTerm := int64(6) // maxTerm (5) + 1
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, quorumRule, proposedTerm)
		require.Error(t, err)
		require.Nil(t, candidate)
		require.Nil(t, standbys)
		require.Equal(t, int64(0), term)
		require.Contains(t, err.Error(), "quorum")
	})
}

func TestPropagate(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("success - promotes candidate and configures standbys", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		candidate := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary")
		standbys := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Test quorum",
		}

		// Build cohort and recruited lists for the test
		cohort := []*multiorchdatapb.PoolerHealthState{candidate}
		cohort = append(cohort, standbys...)
		recruited := []*multiorchdatapb.PoolerHealthState{candidate}
		recruited = append(recruited, standbys...)

		err := c.Propagate(ctx, candidate, standbys, 6, quorumRule, "test_election", cohort, recruited)
		require.NoError(t, err)

		// Verify the PromoteRequest contains the expected election metadata
		candidateKey := "multipooler-zone1-mp1"
		promoteReq, ok := fakeClient.PromoteRequests[candidateKey]
		require.True(t, ok, "PromoteRequest should be recorded for candidate")
		require.NotNil(t, promoteReq, "PromoteRequest should not be nil")

		// Verify election metadata fields
		require.Equal(t, "test_election", promoteReq.Reason, "Reason should match")
		require.Equal(t, "test-cell_test-coordinator", promoteReq.CoordinatorId, "CoordinatorId should match cell_name format")
		require.ElementsMatch(t, []string{"mp1", "mp2", "mp3"}, promoteReq.CohortMembers, "CohortMembers should match")
		require.ElementsMatch(t, []string{"mp1", "mp2", "mp3"}, promoteReq.AcceptedMembers, "AcceptedMembers should match")
		require.Equal(t, int64(6), promoteReq.ConsensusTerm, "ConsensusTerm should match")
	})

	t.Run("success - continues even if some standbys fail", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		candidate := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary")

		// mp3 will fail SetPrimaryConnInfo
		mp3ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp3",
		}
		fakeClient.SetPrimaryConnInfoResponses[topoclient.MultiPoolerIDString(mp3ID)] = nil
		fakeClient.Errors[topoclient.MultiPoolerIDString(mp3ID)] = context.DeadlineExceeded

		standbys := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Test quorum",
		}

		// Build cohort and recruited lists for the test
		cohort := []*multiorchdatapb.PoolerHealthState{candidate}
		cohort = append(cohort, standbys...)
		recruited := []*multiorchdatapb.PoolerHealthState{candidate}
		recruited = append(recruited, standbys...)

		err := c.Propagate(ctx, candidate, standbys, 6, quorumRule, "test_election", cohort, recruited)
		// Should succeed even though one standby failed
		require.NoError(t, err)

		// Verify the PromoteRequest contains the expected election metadata even when some standbys fail
		candidateKey := "multipooler-zone1-mp1"
		promoteReq, ok := fakeClient.PromoteRequests[candidateKey]
		require.True(t, ok, "PromoteRequest should be recorded for candidate")
		require.NotNil(t, promoteReq, "PromoteRequest should not be nil")

		// Verify election metadata fields
		require.Equal(t, "test_election", promoteReq.Reason, "Reason should match")
		require.Equal(t, "test-cell_test-coordinator", promoteReq.CoordinatorId, "CoordinatorId should match cell_name format")
		require.ElementsMatch(t, []string{"mp1", "mp2", "mp3"}, promoteReq.CohortMembers, "CohortMembers should include all cohort members")
		require.ElementsMatch(t, []string{"mp1", "mp2", "mp3"}, promoteReq.AcceptedMembers, "AcceptedMembers should include all recruited members")
	})
}

func TestEstablishLeader(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("success - leader is ready", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		candidate := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary")

		err := c.EstablishLeader(ctx, candidate, 6)
		require.NoError(t, err)
	})

	t.Run("error - leader not in ready state", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		candidate := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary")

		// Override the status response to indicate not ready
		mp1ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp1",
		}
		fakeClient.StateResponses[topoclient.MultiPoolerIDString(mp1ID)] = &multipoolermanagerdatapb.StateResponse{
			State: "initializing",
		}

		err := c.EstablishLeader(ctx, candidate, 6)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in ready state")
	})
}

func TestSelectCandidate_PrefersInitializedNodes(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("prefers initialized node over uninitialized with higher LSN", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}

		// Create cohort with mixed initialization:
		// - mp1: initialized replica with lower LSN (0/1000000)
		// - mp2: uninitialized node with higher LSN (0/2000000)
		node1 := createMockNode(fakeClient, "mp1", 5, "0/1000000", true, "standby")
		node1.IsLastCheckValid = true
		node1.IsInitialized = true // Use IsInitialized field directly
		node1.ReplicationStatus = &multipoolermanagerdatapb.StandbyReplicationStatus{
			LastReplayLsn:  "0/1000000",
			LastReceiveLsn: "0/1000000",
		}

		node2 := createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby")
		node2.IsLastCheckValid = true
		node2.IsInitialized = false // Explicitly uninitialized

		cohort := []*multiorchdatapb.PoolerHealthState{node1, node2}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp1", candidate.MultiPooler.Id.Name,
			"should prefer initialized node mp1 over uninitialized node mp2 despite lower LSN")
	})

	t.Run("prefers higher LSN among initialized nodes", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}

		// Both nodes initialized, mp2 has higher LSN
		node1 := createMockNode(fakeClient, "mp1", 5, "0/1000000", true, "standby")
		node1.IsLastCheckValid = true
		node1.IsInitialized = true
		node1.ReplicationStatus = &multipoolermanagerdatapb.StandbyReplicationStatus{
			LastReplayLsn:  "0/1000000",
			LastReceiveLsn: "0/1000000",
		}

		node2 := createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby")
		node2.IsLastCheckValid = true
		node2.IsInitialized = true
		node2.ReplicationStatus = &multipoolermanagerdatapb.StandbyReplicationStatus{
			LastReplayLsn:  "0/2000000",
			LastReceiveLsn: "0/2000000",
		}

		cohort := []*multiorchdatapb.PoolerHealthState{node1, node2}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"should prefer higher LSN among initialized nodes")
	})

	t.Run("prefers higher LSN among uninitialized nodes", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}

		// Both nodes uninitialized, mp2 has higher LSN
		node1 := createMockNode(fakeClient, "mp1", 5, "0/1000000", true, "standby")
		node1.IsLastCheckValid = true
		node1.IsInitialized = false

		node2 := createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby")
		node2.IsLastCheckValid = true
		node2.IsInitialized = false

		cohort := []*multiorchdatapb.PoolerHealthState{node1, node2}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"should prefer higher LSN among uninitialized nodes")
	})
}

func TestSelectCandidate_LSNComparison(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("selects node with highest LSN using numeric comparison", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/5000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 5, "0/9000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/3000000", true, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"should select node with highest LSN (0/9000000)")
	})

	t.Run("handles multi-digit segment numbers correctly", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		// Numeric: 10/1000000 > 9/9000000 (segment 10 > segment 9)
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "9/9000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 5, "10/1000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "8/5000000", true, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"should use numeric comparison: segment 10 > segment 9")
	})

	t.Run("returns error when LSN is invalid", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "invalid-lsn", true, "standby"),
			createMockNode(fakeClient, "mp2", 5, "0/5000000", true, "standby"),
		}

		_, err := c.selectCandidate(ctx, cohort)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid LSN format")
	})

	t.Run("returns error when any healthy node has invalid LSN", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/5000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 5, "not-an-lsn", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/3000000", true, "standby"),
		}

		_, err := c.selectCandidate(ctx, cohort)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid LSN format")
	})
}
