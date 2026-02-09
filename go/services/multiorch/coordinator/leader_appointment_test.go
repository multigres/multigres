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
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
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
	// Build WAL position based on role (maintain invariant: primary XOR standby)
	var statusWalPos, beginTermWalPos *consensusdatapb.WALPosition
	if role == "primary" {
		statusWalPos = &consensusdatapb.WALPosition{
			CurrentLsn: walPosition,
		}
		beginTermWalPos = &consensusdatapb.WALPosition{
			CurrentLsn: walPosition,
		}
	} else {
		statusWalPos = &consensusdatapb.WALPosition{
			LastReceiveLsn: walPosition,
			LastReplayLsn:  walPosition,
		}
		beginTermWalPos = &consensusdatapb.WALPosition{
			LastReceiveLsn: walPosition,
			LastReplayLsn:  walPosition,
		}
	}

	fakeClient.ConsensusStatusResponses[poolerKey] = &consensusdatapb.StatusResponse{
		CurrentTerm: term,
		IsHealthy:   healthy,
		Role:        role,
		WalPosition: statusWalPos,
	}

	fakeClient.BeginTermResponses[poolerKey] = &consensusdatapb.BeginTermResponse{
		Accepted:    true,
		PoolerId:    name,
		WalPosition: beginTermWalPos,
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

	t.Run("selects node with highest LSN", func(t *testing.T) {
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		recruited := []recruitmentResult{
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp1"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/1000000"},
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp2"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/3000000"}, // Highest
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp3"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/2000000"},
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name)
	})

	t.Run("prefers CurrentLsn for primary over LastReceiveLsn", func(t *testing.T) {
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		recruited := []recruitmentResult{
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "primary"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "0/3000000"}, // Primary
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "standby"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/2000000"}, // Standby
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "primary", candidate.MultiPooler.Id.Name)
	})

	t.Run("skips nodes with nil WAL position", func(t *testing.T) {
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		recruited := []recruitmentResult{
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp1"},
					},
				},
				walPosition: nil, // Nil
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp2"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "0/2000000"}, // Valid
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name)
	})

	t.Run("skips nodes with empty WAL position", func(t *testing.T) {
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		recruited := []recruitmentResult{
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp1"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{}, // Empty
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp2"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/1000000"}, // Valid
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name)
	})

	t.Run("skips nodes with invalid LSN format", func(t *testing.T) {
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		recruited := []recruitmentResult{
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "invalid"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "not-an-lsn"},
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "valid"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "0/2000000"},
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "valid", candidate.MultiPooler.Id.Name)
	})

	t.Run("error - empty recruited list", func(t *testing.T) {
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		_, err := c.selectCandidate(ctx, []recruitmentResult{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no recruited poolers available")
	})

	t.Run("error - all nodes have invalid WAL positions", func(t *testing.T) {
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		recruited := []recruitmentResult{
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp1"},
					},
				},
				walPosition: nil,
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp2"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "invalid"},
			},
		}

		_, err := c.selectCandidate(ctx, recruited)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no valid candidate found")
	})

	t.Run("selects primary with highest CurrentLsn among multiple primaries", func(t *testing.T) {
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		recruited := []recruitmentResult{
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "primary1"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "0/5000000"},
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "primary2"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "0/8000000"}, // Highest
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "primary3"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "0/3000000"},
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "primary2", candidate.MultiPooler.Id.Name)
	})

	t.Run("mixed primaries and standbys - selects highest LSN overall", func(t *testing.T) {
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		recruited := []recruitmentResult{
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "primary1"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "0/5000000"},
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "standby1"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/9000000"}, // Highest
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "primary2"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "0/7000000"},
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "standby1", candidate.MultiPooler.Id.Name,
			"should select standby with highest LSN even though others are primaries")
	})

	t.Run("handles multi-segment LSN comparison correctly", func(t *testing.T) {
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		recruited := []recruitmentResult{
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "node1"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "9/FF000000"},
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "node2"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "A/10000000"}, // Higher segment
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "node2", candidate.MultiPooler.Id.Name,
			"should correctly compare multi-segment LSNs (segment A > segment 9)")
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
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary"),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		recruited, err := c.recruitNodes(ctx, cohort, 6, consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE)
		require.NoError(t, err)
		require.Len(t, recruited, 3)
		// Verify WAL positions are populated
		for _, r := range recruited {
			require.NotNil(t, r.walPosition, "walPosition should be populated")
		}
	})

	t.Run("success - some nodes reject", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary"),
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

		recruited, err := c.recruitNodes(ctx, cohort, 6, consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE)
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
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary"),
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

		recruited, err := c.recruitNodes(ctx, cohort, 6, consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE)
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

	t.Run("error - all nodes reject term cleanly (no errors)", func(t *testing.T) {
		// All nodes reject the term (Accepted: false) with no RPC errors
		// This could happen if nodes are in a higher term, or other consensus reasons
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

		// All nodes cleanly reject (Accepted: false) - no errors
		mp1ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp1",
		}
		mp2ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp2",
		}
		mp3ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp3",
		}
		fakeClient.BeginTermResponses[topoclient.MultiPoolerIDString(mp1ID)] = &consensusdatapb.BeginTermResponse{
			Accepted: false,
			Term:     10, // e.g., already in higher term
			PoolerId: "mp1",
		}
		fakeClient.BeginTermResponses[topoclient.MultiPoolerIDString(mp2ID)] = &consensusdatapb.BeginTermResponse{
			Accepted: false,
			Term:     10,
			PoolerId: "mp2",
		}
		fakeClient.BeginTermResponses[topoclient.MultiPoolerIDString(mp3ID)] = &consensusdatapb.BeginTermResponse{
			Accepted: false,
			Term:     10,
			PoolerId: "mp3",
		}

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Test quorum",
		}

		proposedTerm := int64(6)
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, quorumRule, proposedTerm)
		require.Error(t, err)
		require.Nil(t, candidate)
		require.Nil(t, standbys)
		require.Equal(t, int64(0), term)
		require.Contains(t, err.Error(), "no poolers accepted the term",
			"should fail with 'no poolers accepted' when all cleanly reject")
	})

	t.Run("error - insufficient quorum (mix of reject and error)", func(t *testing.T) {
		// Create cohort where only 1 out of 3 accepts (need 2 for quorum)
		// Mix of clean rejection and RPC error
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

		// mp2 cleanly rejects the term (Accepted: false, no error)
		mp2ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp2",
		}
		fakeClient.BeginTermResponses[topoclient.MultiPoolerIDString(mp2ID)] = &consensusdatapb.BeginTermResponse{
			Accepted: false,
			PoolerId: "mp2",
		}

		// mp3 returns an RPC error
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

	t.Run("success - selects node with highest LSN from recruited nodes", func(t *testing.T) {
		// Regression test: node with highest LSN (mp1) rejects term,
		// second-highest (mp2) accepts and should be selected as candidate
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/5000000", true, "standby"), // Highest LSN
			createMockNode(fakeClient, "mp2", 5, "0/4000000", true, "standby"), // Second highest
			createMockNode(fakeClient, "mp3", 5, "0/3000000", true, "standby"),
			createMockNode(fakeClient, "mp4", 5, "0/2000000", true, "standby"),
		}

		// mp1 (highest LSN) rejects the term
		mp1ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp1",
		}
		fakeClient.BeginTermResponses[topoclient.MultiPoolerIDString(mp1ID)] = &consensusdatapb.BeginTermResponse{
			Accepted: false,
			PoolerId: "mp1",
		}

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 3,
			Description:   "Test quorum",
		}

		proposedTerm := int64(6)
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, quorumRule, proposedTerm)
		require.NoError(t, err)
		require.NotNil(t, candidate)
		// CRITICAL: mp2 should be selected (highest LSN among recruited), NOT mp1
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"should select mp2 with second-highest LSN since mp1 rejected")
		require.Len(t, standbys, 2) // mp3, mp4
		require.Equal(t, int64(6), term)
	})

	t.Run("error - node accepts term but revoke fails, not recruited", func(t *testing.T) {
		// When a node accepts the term but revoke action fails,
		// the multipooler returns accepted=true AND an error.
		// The coordinator should exclude this node from recruited list.
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary"), // Highest LSN
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		// mp1 accepts term but revoke fails (simulates postgres crash during revoke)
		// The multipooler returns accepted=true AND an error
		mp1ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp1",
		}
		// Set error for mp1 to simulate revoke failure
		fakeClient.Errors[topoclient.MultiPoolerIDString(mp1ID)] = errors.New("term accepted but revoke action failed")

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Test quorum",
		}

		proposedTerm := int64(6)
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, quorumRule, proposedTerm)
		require.NoError(t, err)
		require.NotNil(t, candidate)
		// CRITICAL: mp2 should be selected (highest LSN among recruited)
		// mp1 is NOT recruited because revoke failed (error returned)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"should select mp2, not mp1 which failed revoke")
		require.Len(t, standbys, 1) // Only mp3
		require.Equal(t, int64(6), term)
	})

	t.Run("error - multiple nodes accept but revoke fails, insufficient quorum", func(t *testing.T) {
		// If multiple nodes accept but revoke fails, quorum might not be achieved
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "primary"),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "primary"),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby"),
		}

		// mp1 and mp2 accept term but revoke fails on both
		mp1ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp1",
		}
		mp2ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp2",
		}
		fakeClient.Errors[topoclient.MultiPoolerIDString(mp1ID)] = errors.New("term accepted but revoke action failed")
		fakeClient.Errors[topoclient.MultiPoolerIDString(mp2ID)] = errors.New("term accepted but revoke action failed")

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Test quorum requiring 2 nodes",
		}

		proposedTerm := int64(6)
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, quorumRule, proposedTerm)
		require.Error(t, err)
		require.Nil(t, candidate)
		require.Nil(t, standbys)
		require.Equal(t, int64(0), term)
		require.Contains(t, err.Error(), "quorum",
			"should fail quorum validation when only 1 node recruited but need 2")
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

func TestAppointLeader(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("promote request contains full cohort when some nodes reject term", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		c := NewCoordinator(coordID, ts, fakeClient, logger)

		// Create 3 nodes: mp1 (most advanced WAL), mp2, mp3
		mp1 := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, "standby")
		mp1.IsPostgresRunning = true

		mp2 := createMockNode(fakeClient, "mp2", 5, "0/2000000", true, "standby")
		mp2.IsPostgresRunning = true

		mp3 := createMockNode(fakeClient, "mp3", 5, "0/1000000", true, "standby")
		mp3.IsPostgresRunning = true

		// mp3 rejects the term during BeginTerm
		mp3Key := topoclient.MultiPoolerIDString(mp3.MultiPooler.Id)
		fakeClient.BeginTermResponses[mp3Key] = &consensusdatapb.BeginTermResponse{Accepted: false}

		// Register poolers in topo store so updateTopology doesn't panic
		require.NoError(t, ts.CreateMultiPooler(ctx, mp1.MultiPooler))
		require.NoError(t, ts.CreateMultiPooler(ctx, mp2.MultiPooler))
		require.NoError(t, ts.CreateMultiPooler(ctx, mp3.MultiPooler))

		cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2, mp3}

		err := c.AppointLeader(ctx, "shard0", cohort, "testdb", "test_primary_lost")
		require.NoError(t, err)

		// Verify the PromoteRequest on the candidate (mp1, highest WAL)
		candidateKey := topoclient.MultiPoolerIDString(mp1.MultiPooler.Id)
		promoteReq, ok := fakeClient.PromoteRequests[candidateKey]
		require.True(t, ok, "PromoteRequest should be recorded for candidate")
		require.NotNil(t, promoteReq)

		// CohortMembers must include ALL nodes in the original cohort,
		// including mp3 which rejected the term
		require.ElementsMatch(t, []string{"mp1", "mp2", "mp3"}, promoteReq.CohortMembers,
			"CohortMembers should include all nodes in the cohort, even those that rejected the term")

		// AcceptedMembers should only include nodes that accepted the term
		require.ElementsMatch(t, []string{"mp1", "mp2"}, promoteReq.AcceptedMembers,
			"AcceptedMembers should only include nodes that accepted the term")

		require.Equal(t, "test_primary_lost", promoteReq.Reason)
		require.Equal(t, "test-cell_test-coordinator", promoteReq.CoordinatorId)
		require.Equal(t, int64(6), promoteReq.ConsensusTerm)

		// Verify syncConfig includes the full cohort in the standby list,
		// including the leader and nodes that rejected the term
		require.NotNil(t, promoteReq.SyncReplicationConfig, "SyncReplicationConfig should be set")
		syncConfig := promoteReq.SyncReplicationConfig
		require.Equal(t, int32(1), syncConfig.NumSync)

		standbyNames := make([]string, len(syncConfig.StandbyIds))
		for i, id := range syncConfig.StandbyIds {
			standbyNames[i] = id.Name
		}
		require.ElementsMatch(t, []string{"mp1", "mp2", "mp3"}, standbyNames,
			"StandbyIds should include the full cohort: leader, accepted standbys, and rejected nodes")
	})
}
