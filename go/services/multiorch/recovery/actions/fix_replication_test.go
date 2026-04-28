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

package actions

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestFixReplicationAction_Metadata(t *testing.T) {
	action := NewFixReplicationAction(nil, nil, nil, nil, slog.Default())

	metadata := action.Metadata()

	assert.Equal(t, "FixReplication", metadata.Name)
	assert.Equal(t, "Configure or repair replication on a replica", metadata.Description)
	assert.True(t, metadata.Retryable)
	assert.Equal(t, 45*time.Second, metadata.Timeout)
}

func TestFixReplicationAction_RequiresHealthyPrimary(t *testing.T) {
	action := NewFixReplicationAction(nil, nil, nil, nil, slog.Default())

	// FixReplication requires a healthy primary to configure replication
	assert.True(t, action.RequiresHealthyPrimary())
}

func TestFixReplicationAction_Priority(t *testing.T) {
	action := NewFixReplicationAction(nil, nil, nil, nil, slog.Default())

	assert.Equal(t, types.PriorityHigh, action.Priority())
}

func TestFixReplicationAction_ExecuteReplicaNotFound(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	action := NewFixReplicationAction(nil, fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "replica1",
		},
	}

	err := action.Execute(ctx, problem)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find affected replica")
}

func TestFixReplicationAction_ExecuteNoPrimary(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	// Add only replicas, no primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	poolerStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replicaID,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
	})

	action := NewFixReplicationAction(nil, fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: replicaID,
	}

	err := action.Execute(ctx, problem)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find primary")
}

func TestFixReplicationAction_ExecuteUnsupportedProblemCode(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			"multipooler-cell1-primary": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
					},
				},
			},
		},
		ConsensusStatusResponses: map[string]*consensusdatapb.StatusResponse{
			"multipooler-cell1-primary": {
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{
					TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
				},
			},
		},
	}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	// Add replica and primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	poolerStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replicaID,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
	})
	poolerStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
			Hostname:   "primary.example.com",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	})

	action := NewFixReplicationAction(nil, fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemReplicaLagging, // Not yet supported
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: replicaID,
	}

	err := action.Execute(ctx, problem)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported problem code")
}

func TestFixReplicationAction_ExecuteSuccessNotReplicating(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			"multipooler-cell1-primary": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
					},
				},
			},
			"multipooler-cell1-replica1": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							// No PrimaryConnInfo - replication not configured (triggers fix)
							WalReceiverStatus: "streaming", // Streaming after fix is applied
							LastReceiveLsn:    "0/1234",
						},
					},
				},
			},
		},
		ConsensusStatusResponses: map[string]*consensusdatapb.StatusResponse{
			"multipooler-cell1-primary": {
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{
					TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
				},
			},
		},
		SetPrimaryConnInfoResponses: map[string]*multipoolermanagerdatapb.SetPrimaryConnInfoResponse{
			"multipooler-cell1-replica1": {},
		},
		UpdateConsensusRuleResponses: map[string]*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{
			"multipooler-cell1-primary": {},
		},
	}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	// Add replica and primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	poolerStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replicaID,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
	})
	poolerStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
			Hostname:   "primary.example.com",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	})

	action := NewFixReplicationAction(nil, fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: replicaID,
	}

	err := action.Execute(ctx, problem)

	require.NoError(t, err)

	// Verify SetPrimaryConnInfo was called on the replica
	assert.Contains(t, fakeClient.CallLog, "SetPrimaryConnInfo(multipooler-cell1-replica1)")

	// Verify UpdateConsensusRule was called on the primary to add the replica
	assert.Contains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
}

func TestFixReplicationAction_ExecuteAlreadyConfigured(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			"multipooler-cell1-primary": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
					},
				},
			},
			"multipooler-cell1-replica1": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							// Already configured correctly
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
								Host: "primary.example.com",
								Port: 5432,
							},
							WalReceiverStatus: "streaming",
							LastReceiveLsn:    "0/1234",
							LastReplayLsn:     "0/1234",
							IsWalReplayPaused: false,
						},
					},
				},
			},
		},
	}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	// Add replica and primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	poolerStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replicaID,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
	})
	poolerStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
			Hostname:   "primary.example.com",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	})

	action := NewFixReplicationAction(nil, fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: replicaID,
	}

	err := action.Execute(ctx, problem)

	// Should succeed without calling SetPrimaryConnInfo (already configured)
	require.NoError(t, err)
	assert.NotContains(t, fakeClient.CallLog, "SetPrimaryConnInfo(multipooler-cell1-replica1)")
}

// delayedStreamingClient wraps FakeClient to simulate a WAL receiver that takes
// several polling cycles to start streaming, as happens under coverage builds.
type delayedStreamingClient struct {
	*rpcclient.FakeClient
	callCount        int
	streamAfterCalls int // Return "streaming" starting at this call number
}

func (c *delayedStreamingClient) Status(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
	request *multipoolermanagerdatapb.StatusRequest,
) (*multipoolermanagerdatapb.StatusResponse, error) {
	c.callCount++
	if c.callCount >= c.streamAfterCalls {
		return &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					WalReceiverStatus: "streaming",
					LastReceiveLsn:    "0/1234",
				},
			},
		}, nil
	}
	return &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				WalReceiverStatus: "startup",
			},
		},
	}, nil
}

func TestVerifyReplicationStarted_SlowWalReceiver(t *testing.T) {
	ctx := context.Background()

	// WAL receiver starts streaming halfway through the polling window.
	// This verifies the polling loop correctly waits for slow WAL receivers
	// (as seen in coverage builds).
	streamAfterCalls := DefaultVerifyMaxAttempts/2 + 1

	fakeClient := &delayedStreamingClient{
		FakeClient:       rpcclient.NewFakeClient(),
		streamAfterCalls: streamAfterCalls,
	}

	action := NewFixReplicationAction(nil, fakeClient, nil, nil, slog.Default())
	action.verifyPollInterval = 10 * time.Millisecond // Fast polling for tests

	replica := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
		},
	}

	err := action.verifyReplicationStarted(ctx, replica)
	require.NoError(t, err, "verifyReplicationStarted should succeed when WAL receiver starts streaming after several polling cycles")
	assert.Equal(t, streamAfterCalls, fakeClient.callCount, "should have polled exactly until streaming started")
}

// replicationStatusClient wraps FakeClient to return different Status responses for the replica
// based on call count, simulating the progression from "not configured" to a failure state.
// The walReceiverStatus field controls what subsequent calls return.
// Primary calls are delegated to the embedded FakeClient.
type replicationStatusClient struct {
	*rpcclient.FakeClient
	callCount         int
	walReceiverStatus string // WAL receiver status for calls after the first
}

func (c *replicationStatusClient) Status(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
	request *multipoolermanagerdatapb.StatusRequest,
) (*multipoolermanagerdatapb.StatusResponse, error) {
	// Delegate primary calls to embedded FakeClient
	if pooler == nil || pooler.Id == nil || pooler.Id.Name != "replica1" {
		return c.FakeClient.Status(ctx, pooler, request)
	}
	c.callCount++
	// First call is verifyProblemExists (no primary_conninfo - triggers the fix)
	// Subsequent calls are verifyReplicationStarted polling (never reaches streaming)
	if c.callCount == 1 {
		return &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					// No PrimaryConnInfo - triggers the fix
				},
			},
		}, nil
	}
	return &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				WalReceiverStatus: c.walReceiverStatus,
			},
		},
	}, nil
}

func TestFixReplicationAction_FailsWhenReplicationDoesNotStart(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	baseFakeClient := rpcclient.NewFakeClient()
	baseFakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: true,
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
		},
	})
	baseFakeClient.ConsensusStatusResponses = map[string]*consensusdatapb.StatusResponse{
		"multipooler-cell1-primary": {
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm: 1,
				},
			},
		},
		"multipooler-cell1-replica1": {
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm: 1,
				},
			},
		},
	}
	baseFakeClient.SetPrimaryConnInfoResponses = map[string]*multipoolermanagerdatapb.SetPrimaryConnInfoResponse{
		"multipooler-cell1-replica1": {},
	}
	baseFakeClient.UpdateConsensusRuleResponses = map[string]*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{
		"multipooler-cell1-primary": {},
	}
	// pg_rewind dry-run fails, so it marks the pooler as DRAINED
	baseFakeClient.RewindToSourceResponses = map[string]*multipoolermanagerdatapb.RewindToSourceResponse{
		"multipooler-cell1-replica1": {
			Success:      false,
			ErrorMessage: "pg_rewind not feasible: source timeline diverged before target's last checkpoint",
		},
	}

	fakeClient := &replicationStatusClient{FakeClient: baseFakeClient, walReceiverStatus: "stopping"}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	// Add replica and primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	replica := &clustermetadatapb.MultiPooler{
		Id:         replicaID,
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
		Type:       clustermetadatapb.PoolerType_REPLICA,
	}
	primary := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary",
		},
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
		Type:       clustermetadatapb.PoolerType_PRIMARY,
		Hostname:   "primary.example.com",
		PortMap:    map[string]int32{"postgres": 5432},
	}

	// Create in topology for markPoolerDrained to work
	require.NoError(t, ts.CreateMultiPooler(ctx, replica))
	require.NoError(t, ts.CreateMultiPooler(ctx, primary))

	poolerStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: replica,
	})
	poolerStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: primary,
		Status:      &multipoolermanagerdatapb.Status{PostgresReady: true},
	})

	action := NewFixReplicationAction(nil, fakeClient, poolerStore, ts, slog.Default())
	action.verifyPollInterval = 10 * time.Millisecond // Fast polling for tests

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: replicaID,
	}

	err := action.Execute(ctx, problem)

	// Should succeed: pg_rewind was not feasible so the pooler is marked DRAINED
	// and the action returns nil — the problem is resolved by draining the node.
	require.NoError(t, err)

	// Verify SetPrimaryConnInfo was called (configuration was attempted)
	assert.Contains(t, fakeClient.CallLog, "SetPrimaryConnInfo(multipooler-cell1-replica1)")
	// Verify pg_rewind was tried after replication failed to start
	assert.Contains(t, fakeClient.CallLog, "RewindToSource(multipooler-cell1-replica1)")

	// Verify the pooler was marked as DRAINED in topology when pg_rewind wasn't feasible
	updatedPooler, err := ts.GetMultiPooler(ctx, replicaID)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_DRAINED, updatedPooler.Type)
}
