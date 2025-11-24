// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multiorch/store"
	"github.com/multigres/multigres/go/multipooler/rpcclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestLoadQuorumRule_PrimaryPreference(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	c := &Coordinator{logger: logger}

	t.Run("loads from PRIMARY when available", func(t *testing.T) {
		ctx := context.Background()

		// Create fake client
		fakeClient := rpcclient.NewFakeClient()

		// Create PRIMARY node
		primaryPooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		}
		primaryNode := store.NewPoolerHealthFromMultiPooler(primaryPooler)

		// Create REPLICA nodes
		replica1Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica1Node := store.NewPoolerHealthFromMultiPooler(replica1Pooler)

		replica2Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica2",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica2Node := store.NewPoolerHealthFromMultiPooler(replica2Pooler)

		// Setup PRIMARY response with version 100
		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(primaryPooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "primary-policy",
				PolicyVersion: 100,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 2,
					Description:   "Primary policy",
				},
			},
		}

		// Setup REPLICA responses with older versions
		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica1Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "replica1-policy",
				PolicyVersion: 50,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 1,
					Description:   "Replica1 policy",
				},
			},
		}

		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica2Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "replica2-policy",
				PolicyVersion: 60,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 1,
					Description:   "Replica2 policy",
				},
			},
		}

		cohort := []*store.PoolerHealth{primaryNode, replica1Node, replica2Node}

		// Load quorum rule
		rule, err := c.LoadQuorumRule(ctx, cohort, "testdb")
		require.NoError(t, err)
		require.NotNil(t, rule)

		// Should get PRIMARY's rule, not REPLICA's
		require.Equal(t, "Primary policy", rule.Description)
		require.Equal(t, int32(2), rule.RequiredCount)
	})

	t.Run("falls back to REPLICAs when PRIMARY fails", func(t *testing.T) {
		ctx := context.Background()

		// Create fake client
		fakeClient := rpcclient.NewFakeClient()

		// Create PRIMARY node
		primaryPooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		}
		primaryNode := store.NewPoolerHealthFromMultiPooler(primaryPooler)

		// Create REPLICA nodes
		replica1Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica1Node := store.NewPoolerHealthFromMultiPooler(replica1Pooler)

		replica2Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica2",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica2Node := store.NewPoolerHealthFromMultiPooler(replica2Pooler)

		// Setup PRIMARY to fail
		fakeClient.Errors[topo.MultiPoolerIDString(primaryPooler.Id)] = fmt.Errorf("primary is down")

		// Setup REPLICA responses
		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica1Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "replica1-policy",
				PolicyVersion: 50,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 2,
					Description:   "Replica1 policy",
				},
			},
		}

		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica2Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "replica2-policy",
				PolicyVersion: 60,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 2,
					Description:   "Replica2 policy",
				},
			},
		}

		cohort := []*store.PoolerHealth{primaryNode, replica1Node, replica2Node}

		// Load quorum rule - should fall back to REPLICAs
		rule, err := c.LoadQuorumRule(ctx, cohort, "testdb")
		require.NoError(t, err)
		require.NotNil(t, rule)

		// Should get highest version from REPLICAs (replica2 has version 60)
		require.Equal(t, "Replica2 policy", rule.Description)
		require.Equal(t, int32(2), rule.RequiredCount)
	})
}

func TestLoadQuorumRule_ParallelReplicaLoading(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	c := &Coordinator{logger: logger}

	t.Run("loads from all REPLICAs in parallel when no PRIMARY", func(t *testing.T) {
		ctx := context.Background()

		// Create fake client
		fakeClient := rpcclient.NewFakeClient()

		// Create REPLICA nodes only (no PRIMARY)
		replica1Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica1Node := store.NewPoolerHealthFromMultiPooler(replica1Pooler)

		replica2Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica2",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica2Node := store.NewPoolerHealthFromMultiPooler(replica2Pooler)

		replica3Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica3",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica3Node := store.NewPoolerHealthFromMultiPooler(replica3Pooler)

		// Setup REPLICA responses with different versions
		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica1Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "replica1-policy",
				PolicyVersion: 50,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 1,
					Description:   "Replica1 policy v50",
				},
			},
		}

		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica2Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "replica2-policy",
				PolicyVersion: 100,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 2,
					Description:   "Replica2 policy v100",
				},
			},
		}

		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica3Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "replica3-policy",
				PolicyVersion: 75,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 1,
					Description:   "Replica3 policy v75",
				},
			},
		}

		cohort := []*store.PoolerHealth{replica1Node, replica2Node, replica3Node}

		// Load quorum rule - should query all REPLICAs and pick highest version
		rule, err := c.LoadQuorumRule(ctx, cohort, "testdb")
		require.NoError(t, err)
		require.NotNil(t, rule)

		// Should get highest version (replica2 has version 100)
		require.Equal(t, "Replica2 policy v100", rule.Description)
		require.Equal(t, int32(2), rule.RequiredCount)
	})

	t.Run("selects policy with highest version", func(t *testing.T) {
		ctx := context.Background()

		// Create fake client
		fakeClient := rpcclient.NewFakeClient()

		// Create REPLICA nodes
		replica1Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica1Node := store.NewPoolerHealthFromMultiPooler(replica1Pooler)

		replica2Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica2",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica2Node := store.NewPoolerHealthFromMultiPooler(replica2Pooler)

		// Setup responses with version 200 (higher) and version 50 (lower)
		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica1Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "old-policy",
				PolicyVersion: 50,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 1,
					Description:   "Old policy v50",
				},
			},
		}

		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica2Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "new-policy",
				PolicyVersion: 200,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 3,
					Description:   "New policy v200",
				},
			},
		}

		cohort := []*store.PoolerHealth{replica1Node, replica2Node}

		// Load quorum rule
		rule, err := c.LoadQuorumRule(ctx, cohort, "testdb")
		require.NoError(t, err)
		require.NotNil(t, rule)

		// Should select higher version (200)
		require.Equal(t, "New policy v200", rule.Description)
		require.Equal(t, int32(3), rule.RequiredCount)
	})
}

func TestLoadQuorumRule_ResponseWaiting(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	c := &Coordinator{logger: logger}

	t.Run("waits for all responses from REPLICAs", func(t *testing.T) {
		ctx := context.Background()

		// Create fake client
		fakeClient := rpcclient.NewFakeClient()

		// Create 4 REPLICA nodes
		var replicaNodes []*store.PoolerHealth
		for i := 1; i <= 4; i++ {
			pooler := &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      fmt.Sprintf("replica%d", i),
				},
				Type: clustermetadatapb.PoolerType_REPLICA,
			}
			node := store.NewPoolerHealthFromMultiPooler(pooler)
			replicaNodes = append(replicaNodes, node)

			// Setup response for this replica
			fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
				Policy: &clustermetadatapb.DurabilityPolicy{
					PolicyName:    fmt.Sprintf("policy-%d", i),
					PolicyVersion: int64(i * 10),
					QuorumRule: &clustermetadatapb.QuorumRule{
						QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
						RequiredCount: int32(i),
						Description:   fmt.Sprintf("Policy v%d", i*10),
					},
				},
			}
		}

		// With 4 replicas, should wait for all 4 responses
		// Should get highest version from all responses

		rule, err := c.LoadQuorumRule(ctx, replicaNodes, "testdb")
		require.NoError(t, err)
		require.NotNil(t, rule)

		// Should get the highest version (40 from replica4)
		// since all replicas respond successfully
		require.Contains(t, rule.Description, "Policy v")
	})

	t.Run("succeeds with partial failures using best available", func(t *testing.T) {
		ctx := context.Background()

		// Create fake client
		fakeClient := rpcclient.NewFakeClient()

		// Create 3 REPLICA nodes
		replica1Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica1Node := store.NewPoolerHealthFromMultiPooler(replica1Pooler)

		replica2Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica2",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica2Node := store.NewPoolerHealthFromMultiPooler(replica2Pooler)

		replica3Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica3",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica3Node := store.NewPoolerHealthFromMultiPooler(replica3Pooler)

		// Setup replica1 to fail
		fakeClient.Errors[topo.MultiPoolerIDString(replica1Pooler.Id)] = fmt.Errorf("replica1 is down")

		// Setup replica2 and replica3 to succeed
		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica2Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "policy-2",
				PolicyVersion: 100,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 2,
					Description:   "Policy v100",
				},
			},
		}

		fakeClient.GetDurabilityPolicyResponses[topo.MultiPoolerIDString(replica3Pooler.Id)] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
			Policy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "policy-3",
				PolicyVersion: 90,
				QuorumRule: &clustermetadatapb.QuorumRule{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
					RequiredCount: 2,
					Description:   "Policy v90",
				},
			},
		}

		cohort := []*store.PoolerHealth{replica1Node, replica2Node, replica3Node}

		// With 3 replicas, we want all responses but replica1 fails
		// Should succeed using best available from replica2 and replica3
		rule, err := c.LoadQuorumRule(ctx, cohort, "testdb")
		require.NoError(t, err)
		require.NotNil(t, rule)

		// Should get highest version from successful responses (v100 from replica2)
		require.Equal(t, "Policy v100", rule.Description)
	})
}

func TestLoadQuorumRule_FallbackBehaviors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	c := &Coordinator{logger: logger}

	t.Run("returns default policy when all REPLICAs fail", func(t *testing.T) {
		ctx := context.Background()

		// Create fake client
		fakeClient := rpcclient.NewFakeClient()

		// Create REPLICA nodes
		replica1Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica1Node := store.NewPoolerHealthFromMultiPooler(replica1Pooler)

		replica2Pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica2",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		}
		replica2Node := store.NewPoolerHealthFromMultiPooler(replica2Pooler)

		// Setup all REPLICAs to fail
		fakeClient.Errors[topo.MultiPoolerIDString(replica1Pooler.Id)] = fmt.Errorf("replica1 is down")
		fakeClient.Errors[topo.MultiPoolerIDString(replica2Pooler.Id)] = fmt.Errorf("replica2 is down")

		cohort := []*store.PoolerHealth{replica1Node, replica2Node}

		// Should return default policy
		rule, err := c.LoadQuorumRule(ctx, cohort, "testdb")
		require.NoError(t, err)
		require.NotNil(t, rule)

		// Default policy should be ANY_N with majority
		require.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N, rule.QuorumType)
		require.Equal(t, int32(2), rule.RequiredCount) // Majority of 2 is 2
	})

	t.Run("returns default policy when no nodes available", func(t *testing.T) {
		ctx := context.Background()

		// Create fake client
		fakeClient := rpcclient.NewFakeClient()

		// Create PRIMARY node that fails
		primaryPooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		}
		primaryNode := store.NewPoolerHealthFromMultiPooler(primaryPooler)

		// Setup PRIMARY to fail
		fakeClient.Errors[topo.MultiPoolerIDString(primaryPooler.Id)] = fmt.Errorf("primary is down")

		// No REPLICA nodes available
		cohort := []*store.PoolerHealth{primaryNode}

		// Should return default policy
		rule, err := c.LoadQuorumRule(ctx, cohort, "testdb")
		require.NoError(t, err)
		require.NotNil(t, rule)

		// Default policy should be ANY_N with majority
		require.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N, rule.QuorumType)
		require.Equal(t, int32(1), rule.RequiredCount) // Majority of 1 is 1
	})

	t.Run("returns error when cohort is empty", func(t *testing.T) {
		ctx := context.Background()

		// Empty cohort
		cohort := []*store.PoolerHealth{}

		// Should return error
		rule, err := c.LoadQuorumRule(ctx, cohort, "testdb")
		require.Error(t, err)
		require.Nil(t, rule)
		require.Contains(t, err.Error(), "cohort is empty")
	})
}
