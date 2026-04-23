// Copyright 2026 Supabase, Inc.
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

package consensus

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
	"github.com/multigres/multigres/go/tools/prototest"
)

// primaryRule returns a ShardRule that designates the named node in zone1 as primary.
// Passing the same rule to all nodes in a test makes the incumbent primary explicit.
func primaryRule(name string) *clustermetadatapb.ShardRule {
	return &clustermetadatapb.ShardRule{
		PrimaryId: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		},
	}
}

// createMockNode creates a mock node for testing using FakeClient.
// rule is the current shard rule shared by all nodes in the cluster (nil if no primary exists).
func createMockNode(fakeClient *rpcclient.FakeClient, name string, term int64, walPosition string, healthy bool, rule *clustermetadatapb.ShardRule) *multiorchdatapb.PoolerHealthState {
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

	fakeClient.ConsensusStatusResponses[poolerKey] = &consensusdatapb.StatusResponse{
		Id: poolerID,
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: poolerID,
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm: term,
			},
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Lsn:  walPosition,
				Rule: rule,
			},
		},
	}

	fakeClient.BeginTermResponses[poolerKey] = &consensusdatapb.BeginTermResponse{
		Accepted: true,
		PoolerId: name,
		WalPosition: &consensusdatapb.WALPosition{
			LastReceiveLsn: walPosition,
			LastReplayLsn:  walPosition,
		},
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
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: term > 0,
			ConsensusTerm: consensusTerm,
		},
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
			createMockNode(fakeClient, "mp1", 5, "0/1000000", true, nil),
			createMockNode(fakeClient, "mp2", 3, "0/1000000", true, nil),
			createMockNode(fakeClient, "mp3", 7, "0/1000000", true, nil),
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
			createMockNode(fakeClient, "mp1", 0, "0/1000000", false /* unhealthy */, nil),
			createMockNode(fakeClient, "mp2", 0, "0/1000000", false /* unhealthy */, nil),
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
			createMockNode(fakeClient, "mp1", 5, "0/1000000", true, nil),
			{MultiPooler: pooler2},
			createMockNode(fakeClient, "mp3", 3, "0/1000000", true, nil),
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

	// -----------------------------------------------------------------------
	// Leadership-term-first selection tests
	//
	// These tests encode the failure mode where a standby stranded on an older
	// consensus term has a numerically higher LSN than standbys on the current
	// term. Promoting it would discard committed transactions and resurrect ones
	// clients never received acknowledgement for.
	//
	// Selection order: leadership_term → LSN.
	// -----------------------------------------------------------------------

	t.Run("leadership term takes precedence over LSN - naive LSN selection would be wrong", func(t *testing.T) {
		// Scenario: after two failovers the cluster has nodes on different terms.
		//
		//   mp1 (term=1, TLI=1, LSN=0/5000000): stranded on the original term after
		//       a large transaction was written just before the first crash. LSN is
		//       high but the term is stale — promoting mp1 would resurrect those
		//       transactions and discard everything committed on term 2.
		//
		//   mp2 (term=2, TLI=2, LSN=0/3000000): on the current term and timeline.
		//       Lower LSN but the correct choice.
		//
		//   mp3 (term=2, TLI=2, LSN=0/2500000): also on the current term.
		//
		// A naive highest-LSN selector would pick mp1. Term-first must pick mp2.
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
				// Old term, high LSN — the "naive" choice that would be WRONG
				walPosition: &consensusdatapb.WALPosition{
					LastReceiveLsn: "0/5000000",
					TimelineId:     1,
					LeadershipTerm: 1,
				},
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp2"},
					},
				},
				// Current term, lower LSN — the CORRECT choice
				walPosition: &consensusdatapb.WALPosition{
					LastReceiveLsn: "0/3000000",
					TimelineId:     2,
					LeadershipTerm: 2,
				},
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp3"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{
					LastReceiveLsn: "0/2500000",
					TimelineId:     2,
					LeadershipTerm: 2,
				},
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"must select the node on the highest leadership term (term 2), not the one with the highest LSN (term 1)")
	})

	t.Run("ties on leadership term resolved by LSN", func(t *testing.T) {
		// When multiple nodes share the highest term, LSN is the tiebreaker.
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
				walPosition: &consensusdatapb.WALPosition{
					LastReceiveLsn: "0/2000000",
					TimelineId:     3,
					LeadershipTerm: 3,
				},
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp2"},
					},
				},
				// Same term, highest LSN — should win
				walPosition: &consensusdatapb.WALPosition{
					LastReceiveLsn: "0/5000000",
					TimelineId:     3,
					LeadershipTerm: 3,
				},
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp3"},
					},
				},
				// Lower term entirely
				walPosition: &consensusdatapb.WALPosition{
					LastReceiveLsn: "0/9000000",
					TimelineId:     1,
					LeadershipTerm: 1,
				},
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"among nodes on the same (highest) term, must select the one with the highest LSN")
	})

	t.Run("nodes without leadership term fall back to LSN", func(t *testing.T) {
		// If leadership_term is 0 (empty rule_history, e.g. pre-bootstrap),
		// all nodes compare as equal on term, so LSN alone determines the winner.
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
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/1000000"}, // no term, no timeline
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp2"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/4000000"}, // highest LSN
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"without leadership term, highest LSN wins")
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

	// -----------------------------------------------------------------------
	// Resigned-primary avoidance tests
	//
	// A node that has voluntarily resigned via EmergencyDemote carries a
	// REQUESTING_DEMOTION LeadershipStatus signal in its PoolerHealthState.
	// The coordinator should prefer any non-resigned node over it, even if
	// the resigned node has a higher LSN (it was the most-recent primary and
	// therefore has the most WAL).
	// -----------------------------------------------------------------------

	t.Run("skips resigned primary in favour of non-resigned standby", func(t *testing.T) {
		// mp1 resigned as primary at term 4 — it has the highest LSN because it
		// was the most recent primary, but should not be re-elected.
		// mp2 and mp3 are non-resigned standbys with lower LSNs and must win.
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		resignedPooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Name: "mp1-resigned"},
			},
			ConsensusStatus: &consensusdatapb.StatusResponse{
				PrimaryTerm: 4,
				AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{
					LeadershipStatus: &clustermetadatapb.LeadershipStatus{
						PrimaryTerm: 4,
						Signal:      clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
					},
				},
			},
		}

		recruited := []recruitmentResult{
			{
				pooler:      resignedPooler,
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "0/9000000", LeadershipTerm: 4}, // highest LSN but resigned
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp2"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/5000000", LeadershipTerm: 4},
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp3"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/4000000", LeadershipTerm: 4},
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"must not re-elect the resigned primary; should pick the non-resigned node with highest LSN")
	})

	t.Run("returns error when all candidates have resigned", func(t *testing.T) {
		// The resignation signal is honored unconditionally: if every recruited
		// candidate has resigned, the election is deferred rather than re-electing
		// a node that explicitly requested demotion.
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		resignedPooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Name: "only-node"},
			},
			ConsensusStatus: &consensusdatapb.StatusResponse{
				PrimaryTerm: 3,
				AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{
					LeadershipStatus: &clustermetadatapb.LeadershipStatus{
						PrimaryTerm: 3,
						Signal:      clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
					},
				},
			},
		}

		recruited := []recruitmentResult{
			{
				pooler:      resignedPooler,
				walPosition: &consensusdatapb.WALPosition{CurrentLsn: "0/5000000"},
			},
		}

		_, err := c.selectCandidate(ctx, recruited)
		require.Error(t, err, "should return error when all candidates have resigned")
	})

	t.Run("stale resignation signal (different term) does not disqualify node", func(t *testing.T) {
		// A REQUESTING_DEMOTION signal from a previous election cycle (primary_term
		// in the signal does not match the node's current consensus primary_term)
		// is stale and should not disqualify the node.
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
		}

		nodeWithStaleSignal := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Name: "mp1-stale-signal"},
			},
			ConsensusStatus: &consensusdatapb.StatusResponse{
				PrimaryTerm: 5, // current term is 5
				AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{
					LeadershipStatus: &clustermetadatapb.LeadershipStatus{
						PrimaryTerm: 3, // signal from an old term — stale
						Signal:      clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
					},
				},
			},
		}

		recruited := []recruitmentResult{
			{
				pooler:      nodeWithStaleSignal,
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/7000000", LeadershipTerm: 5}, // highest LSN
			},
			{
				pooler: &multiorchdatapb.PoolerHealthState{
					MultiPooler: &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{Name: "mp2"},
					},
				},
				walPosition: &consensusdatapb.WALPosition{LastReceiveLsn: "0/4000000", LeadershipTerm: 5},
			},
		}

		candidate, err := c.selectCandidate(ctx, recruited)
		require.NoError(t, err)
		require.Equal(t, "mp1-stale-signal", candidate.MultiPooler.Id.Name,
			"a stale resignation signal must not disqualify the node; it should win on LSN")
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
		mp1Rule := primaryRule("mp1") // mp1 is the incumbent primary being revoked
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, mp1Rule),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, mp1Rule),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, mp1Rule),
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
		mp1Rule := primaryRule("mp1") // mp1 is the incumbent primary being revoked
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, mp1Rule),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, mp1Rule),
			createMockNode(fakeClient, "mp3", 5, "0/2000000", true, mp1Rule),
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
		mp1Rule := primaryRule("mp1") // mp1 is the incumbent primary being revoked
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, mp1Rule),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, mp1Rule),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, mp1Rule),
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
		mp1Rule := primaryRule("mp1") // mp1 is the incumbent primary being revoked
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, mp1Rule),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, mp1Rule),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, mp1Rule),
		}

		// Create default AT_LEAST_N quorum rule (majority: 2 of 3)
		quorumRule := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
			Description:   "Test majority quorum",
		}

		proposedTerm := int64(6) // maxTerm (5) + 1
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, mustPolicy(t, quorumRule), proposedTerm)
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
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, nil),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, nil),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, nil),
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

		quorumRule := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
			Description:   "Test quorum",
		}

		proposedTerm := int64(6)
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, mustPolicy(t, quorumRule), proposedTerm)
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
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, nil),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, nil),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, nil),
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

		// Create AT_LEAST_N quorum rule requiring 2 nodes
		quorumRule := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
			Description:   "Test quorum requiring 2 nodes",
		}

		proposedTerm := int64(6) // maxTerm (5) + 1
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, mustPolicy(t, quorumRule), proposedTerm)
		require.Error(t, err)
		require.Nil(t, candidate)
		require.Nil(t, standbys)
		require.Equal(t, int64(0), term)
		require.Contains(t, err.Error(), "recruitment validation failed")
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
			createMockNode(fakeClient, "mp1", 5, "0/5000000", true, nil), // Highest LSN
			createMockNode(fakeClient, "mp2", 5, "0/4000000", true, nil), // Second highest
			createMockNode(fakeClient, "mp3", 5, "0/3000000", true, nil),
			createMockNode(fakeClient, "mp4", 5, "0/2000000", true, nil),
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

		quorumRule := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 3,
			Description:   "Test quorum",
		}

		proposedTerm := int64(6)
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, mustPolicy(t, quorumRule), proposedTerm)
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
		mp1Rule := primaryRule("mp1") // mp1 is the incumbent primary; revoke crashes during demotion
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, mp1Rule), // Highest LSN
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, mp1Rule),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, mp1Rule),
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

		quorumRule := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
			Description:   "Test quorum",
		}

		proposedTerm := int64(6)
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, mustPolicy(t, quorumRule), proposedTerm)
		require.NoError(t, err)
		require.NotNil(t, candidate)
		// CRITICAL: mp2 should be selected (highest LSN among recruited)
		// mp1 is NOT recruited because revoke failed (error returned)
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"should select mp2, not mp1 which failed revoke")
		require.Len(t, standbys, 1) // Only mp3
		require.Equal(t, int64(6), term)
	})

	t.Run("selects candidate by leadership term first not LSN - end-to-end through BeginTerm", func(t *testing.T) {
		// End-to-end scenario proving the naive-LSN selection would produce the wrong
		// result and that leadership-term-first ordering corrects it.
		//
		// Cluster state after two failovers:
		//
		//   mp1 (term=1, TLI=1, LSN=0/5000000): stranded standby. Received a large
		//       batch of WAL on term 1 before the primary crashed. Never promoted.
		//       Its LSN is higher than any node on term 2, but its data diverges
		//       from the current consensus history at the term-1 branch point.
		//
		//   mp2 (term=2, TLI=2, LSN=0/3000000): promoted during the first failover.
		//       Has the correct term and timeline. Lower LSN but must win.
		//
		//   mp3 (term=2, TLI=2, LSN=0/2000000): also on the current term.
		//
		// Naive highest-LSN: mp1 (WRONG — would discard all term-2 commits)
		// Term-first:        mp2 (CORRECT)
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}

		// mp2 was promoted during the first failover (term=2, timeline=2) and is the current primary.
		// All nodes share the same rule reflecting mp2's leadership.
		mp2Rule := primaryRule("mp2")
		mp1 := createMockNode(fakeClient, "mp1", 5, "0/5000000", true, mp2Rule)
		mp2 := createMockNode(fakeClient, "mp2", 5, "0/3000000", true, mp2Rule)
		mp3 := createMockNode(fakeClient, "mp3", 5, "0/2000000", true, mp2Rule)
		cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2, mp3}

		// Inject leadership terms into the BeginTermResponses to simulate what the
		// multipooler returns after the executeRevoke change. Timeline IDs are set
		// for realism but do not affect selection.
		mp1Key := topoclient.MultiPoolerIDString(mp1.MultiPooler.Id)
		mp2Key := topoclient.MultiPoolerIDString(mp2.MultiPooler.Id)
		mp3Key := topoclient.MultiPoolerIDString(mp3.MultiPooler.Id)
		fakeClient.BeginTermResponses[mp1Key] = &consensusdatapb.BeginTermResponse{
			Accepted: true,
			PoolerId: "mp1",
			WalPosition: &consensusdatapb.WALPosition{
				LastReceiveLsn: "0/5000000",
				TimelineId:     1,
				LeadershipTerm: 1, // stranded on the old term
			},
		}
		fakeClient.BeginTermResponses[mp2Key] = &consensusdatapb.BeginTermResponse{
			Accepted: true,
			PoolerId: "mp2",
			WalPosition: &consensusdatapb.WALPosition{
				LastReceiveLsn: "0/3000000",
				TimelineId:     2,
				LeadershipTerm: 2, // current term
			},
		}
		fakeClient.BeginTermResponses[mp3Key] = &consensusdatapb.BeginTermResponse{
			Accepted: true,
			PoolerId: "mp3",
			WalPosition: &consensusdatapb.WALPosition{
				LastReceiveLsn: "0/2000000",
				TimelineId:     2,
				LeadershipTerm: 2, // current term
			},
		}

		quorumRule := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
		}

		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, mustPolicy(t, quorumRule), int64(6))
		require.NoError(t, err)
		require.NotNil(t, candidate)

		// The critical assertion: mp2 (term=2, lower LSN) must win over mp1 (term=1, higher LSN).
		require.Equal(t, "mp2", candidate.MultiPooler.Id.Name,
			"must promote the standby on the highest leadership term (term 2), not the one with the highest LSN (term 1)")
		require.Len(t, standbys, 2)
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
		mp1Rule := primaryRule("mp1") // mp1 is the incumbent primary being revoked
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, mp1Rule),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, mp1Rule),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, mp1Rule),
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

		quorumRule := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
			Description:   "Test quorum requiring 2 nodes",
		}

		proposedTerm := int64(6)
		candidate, standbys, term, err := c.BeginTerm(ctx, "shard0", cohort, mustPolicy(t, quorumRule), proposedTerm)
		require.Error(t, err)
		require.Nil(t, candidate)
		require.Nil(t, standbys)
		require.Equal(t, int64(0), term)
		require.Contains(t, err.Error(), "recruitment validation failed",
			"should fail recruitment validation when only 1 node recruited but need 2")
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
		candidate := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, nil)
		standbys := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, nil),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, nil),
		}

		quorumRule := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
			Description:   "Test quorum",
		}

		// Build cohort and recruited lists for the test
		cohort := []*multiorchdatapb.PoolerHealthState{candidate}
		cohort = append(cohort, standbys...)
		recruited := []*multiorchdatapb.PoolerHealthState{candidate}
		recruited = append(recruited, standbys...)

		err := c.EstablishLeadership(ctx, candidate, standbys, 6, quorumRule, "test_election", cohort, recruited)
		require.NoError(t, err)

		// Verify the PromoteRequest contains the expected election metadata
		candidateKey := "multipooler-zone1-mp1"
		promoteReq, ok := fakeClient.PromoteRequests[candidateKey]
		require.True(t, ok, "PromoteRequest should be recorded for candidate")
		require.NotNil(t, promoteReq, "PromoteRequest should not be nil")

		// Verify election metadata fields
		require.Equal(t, "test_election", promoteReq.Reason, "Reason should match")
		prototest.RequireEqual(t, coordID, promoteReq.CoordinatorId, "CoordinatorId should match coordinator ID")
		allCohortIDs := []*clustermetadatapb.ID{
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"},
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp2"},
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp3"},
		}
		prototest.RequireElementsMatch(t, allCohortIDs, promoteReq.CohortMembers, "CohortMembers should match")
		prototest.RequireElementsMatch(t, allCohortIDs, promoteReq.AcceptedMembers, "AcceptedMembers should match")
		require.Equal(t, int64(6), promoteReq.ConsensusTerm, "ConsensusTerm should match")
	})

	t.Run("success - continues even if some standbys fail", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		candidate := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, nil)

		// mp3 will fail SetPrimaryConnInfo
		mp3ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp3",
		}
		fakeClient.SetPrimaryConnInfoResponses[topoclient.MultiPoolerIDString(mp3ID)] = nil
		fakeClient.Errors[topoclient.MultiPoolerIDString(mp3ID)] = context.DeadlineExceeded

		standbys := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, nil),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, nil),
		}

		quorumRule := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
			Description:   "Test quorum",
		}

		// Build cohort and recruited lists for the test
		cohort := []*multiorchdatapb.PoolerHealthState{candidate}
		cohort = append(cohort, standbys...)
		recruited := []*multiorchdatapb.PoolerHealthState{candidate}
		recruited = append(recruited, standbys...)

		err := c.EstablishLeadership(ctx, candidate, standbys, 6, quorumRule, "test_election", cohort, recruited)
		// Should succeed even though one standby failed
		require.NoError(t, err)

		// Verify the PromoteRequest contains the expected election metadata even when some standbys fail
		candidateKey := "multipooler-zone1-mp1"
		promoteReq, ok := fakeClient.PromoteRequests[candidateKey]
		require.True(t, ok, "PromoteRequest should be recorded for candidate")
		require.NotNil(t, promoteReq, "PromoteRequest should not be nil")

		// Verify election metadata fields
		require.Equal(t, "test_election", promoteReq.Reason, "Reason should match")
		require.Equal(t, coordID, promoteReq.CoordinatorId, "CoordinatorId should match coordinator ID")
		allCohortIDs := []*clustermetadatapb.ID{
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"},
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp2"},
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp3"},
		}
		require.ElementsMatch(t, allCohortIDs, promoteReq.CohortMembers, "CohortMembers should include all cohort members")
		require.ElementsMatch(t, allCohortIDs, promoteReq.AcceptedMembers, "AcceptedMembers should include all recruited members")
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

		// Register database in topology so getBootstrapPolicy can find it
		require.NoError(t, ts.CreateDatabase(ctx, "testdb", &clustermetadatapb.Database{
			Name: "testdb",
			BootstrapDurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "AT_LEAST_2",
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 2,
			},
		}))

		c := NewCoordinator(coordID, ts, fakeClient, logger)

		// Create 3 nodes: mp1 (most advanced WAL), mp2, mp3
		mp1 := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, nil)
		mp1.Status.PostgresReady = true

		mp2 := createMockNode(fakeClient, "mp2", 5, "0/2000000", true, nil)
		mp2.Status.PostgresReady = true

		mp3 := createMockNode(fakeClient, "mp3", 5, "0/1000000", true, nil)
		mp3.Status.PostgresReady = true

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
		prototest.RequireElementsMatch(t, []*clustermetadatapb.ID{
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"},
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp2"},
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp3"},
		}, promoteReq.CohortMembers,
			"CohortMembers should include all nodes in the cohort, even those that rejected the term")

		// AcceptedMembers should only include nodes that accepted the term
		prototest.RequireElementsMatch(t, []*clustermetadatapb.ID{
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"},
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp2"},
		}, promoteReq.AcceptedMembers,
			"AcceptedMembers should only include nodes that accepted the term")

		require.Equal(t, "test_primary_lost", promoteReq.Reason)
		prototest.RequireEqual(t, coordID, promoteReq.CoordinatorId)
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

func TestAppointInitialLeader(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	setupDatabase := func(t *testing.T, ts topoclient.Store, policy *clustermetadatapb.DurabilityPolicy) {
		t.Helper()
		require.NoError(t, ts.CreateDatabase(ctx, "testdb", &clustermetadatapb.Database{
			Name:                      "testdb",
			BootstrapDurabilityPolicy: policy,
		}))
	}

	t.Run("success with term 1 and ShardInit reason", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		setupDatabase(t, ts, &clustermetadatapb.DurabilityPolicy{
			PolicyName:    "AT_LEAST_2",
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
		})

		c := NewCoordinator(coordID, ts, fakeClient, logger)

		// Fresh standbys at term 0 (brand new nodes, just restored from backup)
		mp1 := createMockNode(fakeClient, "mp1", 0, "0/2000000", true, nil)
		mp1.Status.IsInitialized = true
		mp1.Status.PostgresReady = true
		mp1.Status.ConsensusTerm = &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 0}

		mp2 := createMockNode(fakeClient, "mp2", 0, "0/1000000", true, nil)
		mp2.Status.IsInitialized = true
		mp2.Status.PostgresReady = true
		mp2.Status.ConsensusTerm = &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 0}

		require.NoError(t, ts.CreateMultiPooler(ctx, mp1.MultiPooler))
		require.NoError(t, ts.CreateMultiPooler(ctx, mp2.MultiPooler))

		cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}

		err := c.AppointInitialLeader(ctx, "shard0", cohort, "testdb")
		require.NoError(t, err)

		// Verify term=1 was used (not discovered from nodes)
		candidateKey := topoclient.MultiPoolerIDString(mp1.MultiPooler.Id)
		promoteReq, ok := fakeClient.PromoteRequests[candidateKey]
		require.True(t, ok, "candidate should receive PromoteRequest")
		require.Equal(t, int64(1), promoteReq.ConsensusTerm, "initial leader should use term 1")
		require.Equal(t, "ShardInit", promoteReq.Reason)
		prototest.RequireEqual(t, coordID, promoteReq.CoordinatorId)
	})

	t.Run("empty cohort returns error", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		c := NewCoordinator(coordID, ts, fakeClient, logger)

		err := c.AppointInitialLeader(ctx, "shard0", nil, "testdb")
		require.Error(t, err)
		require.Contains(t, err.Error(), "cohort is empty")
	})

	t.Run("missing bootstrap policy returns error", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Database without bootstrap policy
		require.NoError(t, ts.CreateDatabase(ctx, "testdb", &clustermetadatapb.Database{
			Name: "testdb",
		}))

		c := NewCoordinator(coordID, ts, fakeClient, logger)

		mp1 := createMockNode(fakeClient, "mp1", 0, "0/2000000", true, nil)
		cohort := []*multiorchdatapb.PoolerHealthState{mp1}

		err := c.AppointInitialLeader(ctx, "shard0", cohort, "testdb")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no bootstrap_durability_policy configured")
	})

	t.Run("pre-vote failure returns error", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Policy requires 3, but only 1 node — preVote will fail
		setupDatabase(t, ts, &clustermetadatapb.DurabilityPolicy{
			PolicyName:    "AT_LEAST_3",
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 3,
		})

		c := NewCoordinator(coordID, ts, fakeClient, logger)

		mp1 := createMockNode(fakeClient, "mp1", 0, "0/2000000", true, nil)
		cohort := []*multiorchdatapb.PoolerHealthState{mp1}

		err := c.AppointInitialLeader(ctx, "shard0", cohort, "testdb")
		require.Error(t, err)
		require.Contains(t, err.Error(), "pre-vote failed")
	})
}
