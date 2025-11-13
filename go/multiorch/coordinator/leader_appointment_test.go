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
	"google.golang.org/grpc"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// mockConsensusClient implements a mock for testing
type mockConsensusClient struct {
	statusResp    *consensusdatapb.StatusResponse
	statusErr     error
	beginTermResp *consensusdatapb.BeginTermResponse
	beginTermErr  error
}

func (m *mockConsensusClient) Status(ctx context.Context, req *consensusdatapb.StatusRequest, opts ...grpc.CallOption) (*consensusdatapb.StatusResponse, error) {
	return m.statusResp, m.statusErr
}

func (m *mockConsensusClient) BeginTerm(ctx context.Context, req *consensusdatapb.BeginTermRequest, opts ...grpc.CallOption) (*consensusdatapb.BeginTermResponse, error) {
	return m.beginTermResp, m.beginTermErr
}

func (m *mockConsensusClient) CanReachPrimary(ctx context.Context, req *consensusdatapb.CanReachPrimaryRequest, opts ...grpc.CallOption) (*consensusdatapb.CanReachPrimaryResponse, error) {
	return &consensusdatapb.CanReachPrimaryResponse{}, nil
}

func (m *mockConsensusClient) GetLeadershipView(ctx context.Context, req *consensusdatapb.LeadershipViewRequest, opts ...grpc.CallOption) (*consensusdatapb.LeadershipViewResponse, error) {
	return &consensusdatapb.LeadershipViewResponse{}, nil
}

// mockManagerClient implements a mock for testing
type mockManagerClient struct {
	statusResp            *multipoolermanagerdatapb.StatusResponse
	statusErr             error
	promoteResp           *multipoolermanagerdatapb.PromoteResponse
	promoteErr            error
	setPrimaryConnInfoErr error
}

func (m *mockManagerClient) Status(ctx context.Context, req *multipoolermanagerdatapb.StatusRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.StatusResponse, error) {
	return m.statusResp, m.statusErr
}

func (m *mockManagerClient) Promote(ctx context.Context, req *multipoolermanagerdatapb.PromoteRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.PromoteResponse, error) {
	return m.promoteResp, m.promoteErr
}

func (m *mockManagerClient) SetPrimaryConnInfo(ctx context.Context, req *multipoolermanagerdatapb.SetPrimaryConnInfoRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.SetPrimaryConnInfoResponse, error) {
	return &multipoolermanagerdatapb.SetPrimaryConnInfoResponse{}, m.setPrimaryConnInfoErr
}

func (m *mockManagerClient) InitializeEmptyPrimary(ctx context.Context, req *multipoolermanagerdatapb.InitializeEmptyPrimaryRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error) {
	return &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{}, nil
}

func (m *mockManagerClient) InitializeAsStandby(ctx context.Context, req *multipoolermanagerdatapb.InitializeAsStandbyRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.InitializeAsStandbyResponse, error) {
	return &multipoolermanagerdatapb.InitializeAsStandbyResponse{}, nil
}

func (m *mockManagerClient) InitializationStatus(ctx context.Context, req *multipoolermanagerdatapb.InitializationStatusRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.InitializationStatusResponse, error) {
	return &multipoolermanagerdatapb.InitializationStatusResponse{}, nil
}

func (m *mockManagerClient) Demote(ctx context.Context, req *multipoolermanagerdatapb.DemoteRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.DemoteResponse, error) {
	return &multipoolermanagerdatapb.DemoteResponse{}, nil
}

func (m *mockManagerClient) StopReplicationAndGetStatus(ctx context.Context, req *multipoolermanagerdatapb.StopReplicationAndGetStatusRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.StopReplicationAndGetStatusResponse, error) {
	return &multipoolermanagerdatapb.StopReplicationAndGetStatusResponse{}, nil
}

func (m *mockManagerClient) ConfigureSynchronousReplication(ctx context.Context, req *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse, error) {
	return &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}, nil
}

func (m *mockManagerClient) ChangeType(ctx context.Context, req *multipoolermanagerdatapb.ChangeTypeRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.ChangeTypeResponse, error) {
	return &multipoolermanagerdatapb.ChangeTypeResponse{}, nil
}

func (m *mockManagerClient) GetFollowers(ctx context.Context, req *multipoolermanagerdatapb.GetFollowersRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.GetFollowersResponse, error) {
	return &multipoolermanagerdatapb.GetFollowersResponse{}, nil
}

func (m *mockManagerClient) PrimaryPosition(ctx context.Context, req *multipoolermanagerdatapb.PrimaryPositionRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.PrimaryPositionResponse, error) {
	return &multipoolermanagerdatapb.PrimaryPositionResponse{}, nil
}

func (m *mockManagerClient) PrimaryStatus(ctx context.Context, req *multipoolermanagerdatapb.PrimaryStatusRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.PrimaryStatusResponse, error) {
	return &multipoolermanagerdatapb.PrimaryStatusResponse{}, nil
}

func (m *mockManagerClient) WaitForLSN(ctx context.Context, req *multipoolermanagerdatapb.WaitForLSNRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.WaitForLSNResponse, error) {
	return &multipoolermanagerdatapb.WaitForLSNResponse{}, nil
}

func (m *mockManagerClient) StartReplication(ctx context.Context, req *multipoolermanagerdatapb.StartReplicationRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.StartReplicationResponse, error) {
	return &multipoolermanagerdatapb.StartReplicationResponse{}, nil
}

func (m *mockManagerClient) StopReplication(ctx context.Context, req *multipoolermanagerdatapb.StopReplicationRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.StopReplicationResponse, error) {
	return &multipoolermanagerdatapb.StopReplicationResponse{}, nil
}

func (m *mockManagerClient) ReplicationStatus(ctx context.Context, req *multipoolermanagerdatapb.ReplicationStatusRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.ReplicationStatusResponse, error) {
	return &multipoolermanagerdatapb.ReplicationStatusResponse{}, nil
}

func (m *mockManagerClient) ResetReplication(ctx context.Context, req *multipoolermanagerdatapb.ResetReplicationRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.ResetReplicationResponse, error) {
	return &multipoolermanagerdatapb.ResetReplicationResponse{}, nil
}

func (m *mockManagerClient) UpdateSynchronousStandbyList(ctx context.Context, req *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error) {
	return &multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{}, nil
}

func (m *mockManagerClient) UndoDemote(ctx context.Context, req *multipoolermanagerdatapb.UndoDemoteRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.UndoDemoteResponse, error) {
	return &multipoolermanagerdatapb.UndoDemoteResponse{}, nil
}

func (m *mockManagerClient) SetTerm(ctx context.Context, req *multipoolermanagerdatapb.SetTermRequest, opts ...grpc.CallOption) (*multipoolermanagerdatapb.SetTermResponse, error) {
	return &multipoolermanagerdatapb.SetTermResponse{}, nil
}

// createMockNode creates a mock node for testing
func createMockNode(name string, term int64, walPosition string, healthy bool, role string) *Node {
	return &Node{
		ID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		},
		Hostname: "localhost",
		Port:     9000,
		ShardID:  "shard0",
		ConsensusClient: &mockConsensusClient{
			statusResp: &consensusdatapb.StatusResponse{
				CurrentTerm: term,
				IsHealthy:   healthy,
				Role:        role,
				WalPosition: &consensusdatapb.WALPosition{
					CurrentLsn:     walPosition,
					LastReceiveLsn: walPosition,
					LastReplayLsn:  walPosition,
				},
			},
			beginTermResp: &consensusdatapb.BeginTermResponse{
				Accepted: true,
			},
		},
		ManagerClient: &mockManagerClient{
			statusResp: &multipoolermanagerdatapb.StatusResponse{
				State: "ready",
			},
			promoteResp: &multipoolermanagerdatapb.PromoteResponse{},
		},
	}
}

func TestDiscoverMaxTerm(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	c := &Coordinator{logger: logger}

	t.Run("success - finds max term from cohort", func(t *testing.T) {
		cohort := []*Node{
			createMockNode("mp1", 5, "0/1000000", true, "standby"),
			createMockNode("mp2", 3, "0/1000000", true, "standby"),
			createMockNode("mp3", 7, "0/1000000", true, "standby"),
		}

		maxTerm, err := c.discoverMaxTerm(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, int64(7), maxTerm)
	})

	t.Run("success - returns 0 when all nodes have term 0", func(t *testing.T) {
		cohort := []*Node{
			createMockNode("mp1", 0, "0/1000000", true, "standby"),
			createMockNode("mp2", 0, "0/1000000", true, "standby"),
		}

		maxTerm, err := c.discoverMaxTerm(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, int64(0), maxTerm)
	})

	t.Run("success - ignores failed nodes", func(t *testing.T) {
		cohort := []*Node{
			createMockNode("mp1", 5, "0/1000000", true, "standby"),
			{
				ID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "mp2",
				},
				ConsensusClient: &mockConsensusClient{
					statusErr: context.DeadlineExceeded,
				},
			},
			createMockNode("mp3", 3, "0/1000000", true, "standby"),
		}

		maxTerm, err := c.discoverMaxTerm(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, int64(5), maxTerm)
	})
}

func TestSelectCandidate(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	c := &Coordinator{logger: logger}

	t.Run("success - selects node with most advanced WAL", func(t *testing.T) {
		cohort := []*Node{
			createMockNode("mp1", 5, "0/1000000", true, "standby"),
			createMockNode("mp2", 5, "0/3000000", true, "standby"),
			createMockNode("mp3", 5, "0/2000000", true, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.ID.Name)
	})

	t.Run("success - prefers healthy nodes", func(t *testing.T) {
		cohort := []*Node{
			createMockNode("mp1", 5, "0/3000000", false, "standby"),
			createMockNode("mp2", 5, "0/2000000", true, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.ID.Name)
	})

	t.Run("success - falls back to first node if none healthy", func(t *testing.T) {
		cohort := []*Node{
			createMockNode("mp1", 5, "0/1000000", false, "standby"),
			createMockNode("mp2", 5, "0/2000000", false, "standby"),
		}

		candidate, err := c.selectCandidate(ctx, cohort)
		require.NoError(t, err)
		require.NotNil(t, candidate)
		// Should select first available node
		require.Equal(t, "mp1", candidate.ID.Name)
	})

	t.Run("error - no nodes available", func(t *testing.T) {
		cohort := []*Node{
			{
				ID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "mp1",
				},
				ConsensusClient: &mockConsensusClient{
					statusErr: context.DeadlineExceeded,
				},
			},
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
	c := &Coordinator{logger: logger}

	t.Run("success - all nodes accept", func(t *testing.T) {
		candidate := createMockNode("mp1", 5, "0/3000000", true, "primary")
		cohort := []*Node{
			candidate,
			createMockNode("mp2", 5, "0/2000000", true, "standby"),
			createMockNode("mp3", 5, "0/1000000", true, "standby"),
		}

		recruited, err := c.recruitNodes(ctx, cohort, 6, candidate)
		require.NoError(t, err)
		require.Len(t, recruited, 3)
	})

	t.Run("success - some nodes reject", func(t *testing.T) {
		candidate := createMockNode("mp1", 5, "0/3000000", true, "primary")
		cohort := []*Node{
			candidate,
			createMockNode("mp2", 5, "0/2000000", true, "standby"),
			{
				ID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "mp3",
				},
				ConsensusClient: &mockConsensusClient{
					beginTermResp: &consensusdatapb.BeginTermResponse{
						Accepted: false,
					},
				},
			},
		}

		recruited, err := c.recruitNodes(ctx, cohort, 6, candidate)
		require.NoError(t, err)
		require.Len(t, recruited, 2)
	})
}

func TestBeginTerm(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	c := &Coordinator{logger: logger}

	t.Run("success - achieves quorum", func(t *testing.T) {
		cohort := []*Node{
			createMockNode("mp1", 5, "0/3000000", true, "standby"),
			createMockNode("mp2", 5, "0/2000000", true, "standby"),
			createMockNode("mp3", 5, "0/1000000", true, "standby"),
		}

		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort)
		require.NoError(t, err)
		require.NotNil(t, candidate)
		require.Equal(t, "mp1", candidate.ID.Name) // Most advanced WAL
		require.Len(t, standbys, 2)
		require.Equal(t, int64(6), term)
	})

	t.Run("error - insufficient quorum", func(t *testing.T) {
		// Create cohort where only 1 out of 3 accepts (need 2 for quorum)
		candidate := createMockNode("mp1", 5, "0/3000000", true, "standby")
		cohort := []*Node{
			candidate,
			{
				ID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "mp2",
				},
				ConsensusClient: &mockConsensusClient{
					statusResp: &consensusdatapb.StatusResponse{
						CurrentTerm: 5,
						IsHealthy:   true,
						Role:        "standby",
						WalPosition: &consensusdatapb.WALPosition{
							CurrentLsn:    "0/2000000",
							LastReplayLsn: "0/2000000",
						},
					},
					beginTermResp: &consensusdatapb.BeginTermResponse{
						Accepted: false,
					},
				},
			},
			{
				ID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "mp3",
				},
				ConsensusClient: &mockConsensusClient{
					statusResp: &consensusdatapb.StatusResponse{
						CurrentTerm: 5,
						IsHealthy:   true,
						Role:        "standby",
						WalPosition: &consensusdatapb.WALPosition{
							CurrentLsn:    "0/1000000",
							LastReplayLsn: "0/1000000",
						},
					},
					beginTermErr: context.DeadlineExceeded,
				},
			},
		}

		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort)
		require.Error(t, err)
		require.Nil(t, candidate)
		require.Nil(t, standbys)
		require.Equal(t, int64(0), term)
		require.Contains(t, err.Error(), "insufficient quorum")
	})
}

func TestPropagate(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	c := &Coordinator{logger: logger}

	t.Run("success - promotes candidate and configures standbys", func(t *testing.T) {
		candidate := createMockNode("mp1", 5, "0/3000000", true, "primary")
		standbys := []*Node{
			createMockNode("mp2", 5, "0/2000000", true, "standby"),
			createMockNode("mp3", 5, "0/1000000", true, "standby"),
		}

		err := c.Propagate(ctx, candidate, standbys, 6)
		require.NoError(t, err)
	})

	t.Run("success - continues even if some standbys fail", func(t *testing.T) {
		candidate := createMockNode("mp1", 5, "0/3000000", true, "primary")
		standbys := []*Node{
			createMockNode("mp2", 5, "0/2000000", true, "standby"),
			{
				ID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "mp3",
				},
				ConsensusClient: &mockConsensusClient{
					statusResp: &consensusdatapb.StatusResponse{
						CurrentTerm: 5,
						IsHealthy:   true,
						Role:        "standby",
						WalPosition: &consensusdatapb.WALPosition{
							CurrentLsn:    "0/1000000",
							LastReplayLsn: "0/1000000",
						},
					},
				},
				ManagerClient: &mockManagerClient{
					setPrimaryConnInfoErr: context.DeadlineExceeded,
				},
			},
		}

		err := c.Propagate(ctx, candidate, standbys, 6)
		// Should succeed even though one standby failed
		require.NoError(t, err)
	})
}

func TestEstablishLeader(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	c := &Coordinator{logger: logger}

	t.Run("success - leader is ready", func(t *testing.T) {
		candidate := createMockNode("mp1", 5, "0/3000000", true, "primary")

		err := c.EstablishLeader(ctx, candidate, 6)
		require.NoError(t, err)
	})

	t.Run("error - leader not in ready state", func(t *testing.T) {
		candidate := createMockNode("mp1", 5, "0/3000000", true, "primary")
		candidate.ManagerClient = &mockManagerClient{
			statusResp: &multipoolermanagerdatapb.StatusResponse{
				State: "initializing",
			},
		}

		err := c.EstablishLeader(ctx, candidate, 6)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in ready state")
	})
}
