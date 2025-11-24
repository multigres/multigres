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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// createMockNode creates a mock node for testing using FakeClient
func createMockNode(fakeClient *rpcclient.FakeClient, name string, term int64, walPosition string, healthy bool, role string) *store.PoolerHealth {
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
	poolerKey := topo.MultiPoolerIDString(poolerID)

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

	return &store.PoolerHealth{
		ID:       pooler.Id,
		Hostname: pooler.Hostname,
		PortMap:  map[string]int32{"grpc": pooler.PortMap["grpc"]},
		Shard:    "shard0",
	}
}

func TestDiscoverMaxTerm(t *testing.T) {
	ctx := context.Background()
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
		cohort := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp1", 5, "0/1000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 3, "0/1000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 7, "0/1000000", true, "standby"),
		}

		maxTerm, err := c.discoverMaxTerm(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, int64(7), maxTerm)
	})

	t.Run("success - returns 0 when all nodes have term 0", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp1", 0, "0/1000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 0, "0/1000000", true, "standby"),
		}

		maxTerm, err := c.discoverMaxTerm(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, int64(0), maxTerm)
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
		fakeClient.Errors[topo.MultiPoolerIDString(pooler2ID)] = context.DeadlineExceeded

		cohort := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp1", 5, "0/1000000", true, "standby"),
			store.NewPoolerHealthFromMultiPooler(pooler2),
			createMockNode(fakeClient, "mp3", 3, "0/1000000", true, "standby"),
		}

		maxTerm, err := c.discoverMaxTerm(ctx, cohort)
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
		cohort := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp1", 5, "0/1000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 5, "0/3000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/2000000", true, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.ID.Name)
	})

	t.Run("success - prefers healthy nodes", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", false, "standby"),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.ID.Name)
	})

	t.Run("success - falls back to first node if none healthy", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp1", 5, "0/1000000", false, "standby"),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", false, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.NotNil(t, candidate)
		// Should select first available node
		require.Equal(t, "mp1", candidate.ID.Name)
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
		fakeClient.Errors[topo.MultiPoolerIDString(poolerID)] = context.DeadlineExceeded

		pooler := &clustermetadatapb.MultiPooler{
			Id:       poolerID,
			Hostname: "localhost",
			PortMap:  map[string]int32{"grpc": 9000},
		}

		cohort := []*store.PoolerHealth{
			store.NewPoolerHealthFromMultiPooler(pooler),
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
		cohort := []*store.PoolerHealth{
			candidate,
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		recruited, err := c.recruitNodes(ctx, cohort, 6, candidate)
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

		cohort := []*store.PoolerHealth{
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
		fakeClient.BeginTermResponses[topo.MultiPoolerIDString(mp3ID)] = &consensusdatapb.BeginTermResponse{Accepted: false}

		recruited, err := c.recruitNodes(ctx, cohort, 6, candidate)
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
		cohort := []*store.PoolerHealth{
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

		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, quorumRule)
		require.NoError(t, err)
		require.NotNil(t, candidate)
		require.Equal(t, "mp1", candidate.ID.Name) // Most advanced WAL
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

		cohort := []*store.PoolerHealth{
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
		fakeClient.BeginTermResponses[topo.MultiPoolerIDString(mp2ID)] = &consensusdatapb.BeginTermResponse{Accepted: false}

		// mp3 returns an error
		mp3ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp3",
		}
		fakeClient.Errors[topo.MultiPoolerIDString(mp3ID)] = context.DeadlineExceeded

		// Create ANY_N quorum rule requiring 2 nodes
		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Test quorum requiring 2 nodes",
		}

		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, quorumRule)
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
		standbys := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Test quorum",
		}

		err := c.Propagate(ctx, candidate, standbys, 6, quorumRule)
		require.NoError(t, err)
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
		fakeClient.SetPrimaryConnInfoResponses[topo.MultiPoolerIDString(mp3ID)] = nil
		fakeClient.Errors[topo.MultiPoolerIDString(mp3ID)] = context.DeadlineExceeded

		standbys := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Test quorum",
		}

		err := c.Propagate(ctx, candidate, standbys, 6, quorumRule)
		// Should succeed even though one standby failed
		require.NoError(t, err)
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
		fakeClient.StateResponses[topo.MultiPoolerIDString(mp1ID)] = &multipoolermanagerdatapb.StateResponse{
			State: "initializing",
		}

		err := c.EstablishLeader(ctx, candidate, 6)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in ready state")
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
		cohort := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp1", 5, "0/5000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 5, "0/9000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/3000000", true, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.ID.Name,
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
		cohort := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp1", 5, "9/9000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 5, "10/1000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "8/5000000", true, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.ID.Name,
			"should use numeric comparison: segment 10 > segment 9")
	})

	t.Run("returns error when LSN is invalid", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*store.PoolerHealth{
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
		cohort := []*store.PoolerHealth{
			createMockNode(fakeClient, "mp1", 5, "0/5000000", true, "standby"),
			createMockNode(fakeClient, "mp2", 5, "not-an-lsn", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/3000000", true, "standby"),
		}

		_, err := c.selectCandidate(ctx, cohort)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid LSN format")
	})
}
