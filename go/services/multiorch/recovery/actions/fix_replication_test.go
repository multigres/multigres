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
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestFixReplicationAction_Metadata(t *testing.T) {
	action := NewFixReplicationAction(config.NewTestConfig(), nil, nil, nil, slog.Default())

	metadata := action.Metadata()

	assert.Equal(t, "FixReplication", metadata.Name)
	assert.Equal(t, "Configure or repair replication on a replica", metadata.Description)
	assert.True(t, metadata.Retryable)
	assert.Equal(t, 45*time.Second, metadata.Timeout)
}

func TestFixReplicationAction_RequiresHealthyLeader(t *testing.T) {
	action := NewFixReplicationAction(config.NewTestConfig(), nil, nil, nil, slog.Default())

	// FixReplication requires a healthy primary to configure replication
	assert.True(t, action.RequiresHealthyLeader())
}

func TestFixReplicationAction_Priority(t *testing.T) {
	action := NewFixReplicationAction(config.NewTestConfig(), nil, nil, nil, slog.Default())

	assert.Equal(t, types.PriorityHigh, action.Priority())
}

func TestFixReplicationAction_ExecuteReplicaNotFound(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: &clustermetadatapb.ShardKey{
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
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	})

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: &clustermetadatapb.ShardKey{
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
					ConsensusStatus: &clustermetadatapb.ConsensusStatus{
						TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
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
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	})
	poolerStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type:     clustermetadatapb.PoolerType_PRIMARY,
			Hostname: "primary.example.com",
			PortMap:  map[string]int32{"postgres": 5432},
		},
	})

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemReplicaLagging, // Not yet supported
		ShardKey: &clustermetadatapb.ShardKey{
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

// TestFixReplicationAction_ExecuteSuccessNotReplicating asserts that
// fixNotReplicating routes through SetTermPrimary on the replica.
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
					ConsensusStatus: &clustermetadatapb.ConsensusStatus{
						TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
					},
				},
			},
			"multipooler-cell1-replica1": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							WalReceiverStatus: "streaming",
							LastReceiveLsn:    "0/1234",
						},
					},
				},
			},
		},
		SetTermPrimaryResponses: map[string]*consensusdatapb.SetTermPrimaryResponse{
			"multipooler-cell1-replica1": {},
		},
	}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	poolerStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	})
	primaryPosition := &clustermetadatapb.PoolerPosition{
		Rule: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
		},
		Lsn: "0/1234",
	}
	poolerStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type:     clustermetadatapb.PoolerType_PRIMARY,
			Hostname: "primary.example.com",
			PortMap:  map[string]int32{"postgres": 5432},
		},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
			CurrentPosition: primaryPosition,
		},
	})

	cfg := config.NewTestConfig()
	action := NewFixReplicationAction(cfg, fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: replicaID,
	}

	err := action.Execute(ctx, problem)
	require.NoError(t, err)

	// Verify SetTermPrimary was called on the replica, NOT SetPrimaryConnInfo.
	assert.Contains(t, fakeClient.CallLog, "SetTermPrimary(multipooler-cell1-replica1)")
	assert.NotContains(t, fakeClient.CallLog, "SetPrimaryConnInfo(multipooler-cell1-replica1)")

	// Verify the request carried the primary's contact info and known position.
	informReq := fakeClient.SetTermPrimaryRequests["multipooler-cell1-replica1"]
	require.NotNil(t, informReq)
	require.NotNil(t, informReq.Leader)
	assert.Equal(t, "primary", informReq.Leader.Id.Name)
	assert.Equal(t, "primary.example.com", informReq.Leader.GetHost())
	require.NotNil(t, informReq.Rule)
	assert.Equal(t, int64(1), informReq.Rule.GetRuleNumber().GetCoordinatorTerm())
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
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	})
	poolerStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type:     clustermetadatapb.PoolerType_PRIMARY,
			Hostname: "primary.example.com",
			PortMap:  map[string]int32{"postgres": 5432},
		},
	})

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: &clustermetadatapb.ShardKey{
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

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, nil, nil, slog.Default())
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

// streamingAfterRewindClient wraps FakeClient to simulate the full pg_rewind
// success path: the replica has no primary_conninfo initially, the WAL receiver
// stays idle through the first verifyReplicationStarted window (exhausting all
// attempts), then streams after RewindToSource restores primary_conninfo.
//
// Call sequence (with verifyMaxAttempts = N):
//  1. verifyProblemExists        → no PrimaryConnInfo (triggers fix)
//     2..N+1. verifyReplicationStarted after SetTermPrimary  → not streaming (all fail)
//     N+2+.  verifyReplicationStarted after RewindToSource → streaming
type streamingAfterRewindClient struct {
	*rpcclient.FakeClient
	replicaCallCount  int
	nonStreamingCalls int // number of non-streaming calls after call 1 before transitioning
}

func (c *streamingAfterRewindClient) Status(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
	request *multipoolermanagerdatapb.StatusRequest,
) (*multipoolermanagerdatapb.StatusResponse, error) {
	if pooler == nil || pooler.Id == nil || pooler.Id.Name != "replica1" {
		return c.FakeClient.Status(ctx, pooler, request)
	}
	c.replicaCallCount++
	switch {
	case c.replicaCallCount == 1:
		// verifyProblemExists: no primary_conninfo → triggers fix
		return &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{},
			},
		}, nil
	case c.replicaCallCount <= 1+c.nonStreamingCalls:
		// verifyReplicationStarted after SetTermPrimary: WAL receiver idle
		return &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					WalReceiverStatus: "",
				},
			},
		}, nil
	default:
		// verifyReplicationStarted after RewindToSource: streaming
		return &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					WalReceiverStatus: "streaming",
					LastReceiveLsn:    "0/5000100",
				},
			},
		}, nil
	}
}

// TestFixReplicationAction_SucceedsViaRewind is a regression test for the bug where
// RewindToSource did not restore primary_conninfo after pg_rewind, leaving the WAL
// receiver with no primary to connect to. The full path under test:
//
//  1. SetTermPrimary is called but the WAL receiver never starts streaming.
//  2. tryPgRewind → RewindToSource runs (which now restores primary_conninfo).
//  3. verifyReplicationStarted sees "streaming" and the action succeeds.
func TestFixReplicationAction_SucceedsViaRewind(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	const verifyAttempts = 3 // override default to keep test fast

	baseFakeClient := rpcclient.NewFakeClient()
	baseFakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: true,
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
		},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		},
	})
	baseFakeClient.SetStatusResponse("multipooler-cell1-replica1", &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		},
	})
	baseFakeClient.UpdateConsensusRuleResponses = map[string]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
		"multipooler-cell1-primary": {},
	}
	// RewindToSource succeeds, simulating pg_rewind running and primary_conninfo
	// being restored by the fix in rpc_manager.go.
	baseFakeClient.RewindToSourceResponses = map[string]*multipoolermanagerdatapb.RewindToSourceResponse{
		"multipooler-cell1-replica1": {
			Success:         true,
			RewindPerformed: true,
		},
	}

	fakeClient := &streamingAfterRewindClient{
		FakeClient:        baseFakeClient,
		nonStreamingCalls: verifyAttempts,
	}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	replica := &clustermetadatapb.MultiPooler{
		Id: replicaID,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		Type: clustermetadatapb.PoolerType_REPLICA,
	}
	primary := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary",
		},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		Type:     clustermetadatapb.PoolerType_PRIMARY,
		Hostname: "primary.example.com",
		PortMap:  map[string]int32{"postgres": 5432},
	}

	require.NoError(t, ts.CreateMultiPooler(ctx, replica))
	require.NoError(t, ts.CreateMultiPooler(ctx, primary))

	poolerStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: replica,
	})
	poolerStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: primary,
		Status:      &multipoolermanagerdatapb.Status{PostgresReady: true},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				},
			},
		},
	})

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, ts, slog.Default())
	action.verifyPollInterval = 10 * time.Millisecond
	action.verifyMaxAttempts = verifyAttempts

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: replicaID,
	}

	err := action.Execute(ctx, problem)
	require.NoError(t, err, "fixNotReplicating must succeed via pg_rewind path")

	// Verify the expected call sequence.
	assert.Contains(t, fakeClient.CallLog, "SetTermPrimary(multipooler-cell1-replica1)",
		"SetTermPrimary must be called first")
	assert.Contains(t, fakeClient.CallLog, "RewindToSource(multipooler-cell1-replica1)",
		"RewindToSource must be called when SetTermPrimary doesn't start streaming")

	// REGRESSION: the pooler must NOT be drained — RewindToSource succeeded and
	// streaming started. Before the fix, primary_conninfo was wiped by pg_rewind
	// so verifyReplicationStarted always failed, eventually routing to DRAINED.
	updatedPooler, err := ts.GetMultiPooler(ctx, replicaID)
	require.NoError(t, err)
	assert.NotEqual(t, clustermetadatapb.PoolerType_DRAINED, updatedPooler.Type,
		"pooler must not be drained when replication starts successfully after rewind")
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
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm: 1,
			},
		},
	})
	baseFakeClient.SetStatusResponse("multipooler-cell1-replica1", &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm: 1,
			},
		},
	})
	baseFakeClient.UpdateConsensusRuleResponses = map[string]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
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
		Id: replicaID,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		Type: clustermetadatapb.PoolerType_REPLICA,
	}
	primary := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary",
		},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		Type:     clustermetadatapb.PoolerType_PRIMARY,
		Hostname: "primary.example.com",
		PortMap:  map[string]int32{"postgres": 5432},
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

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, ts, slog.Default())
	action.verifyPollInterval = 10 * time.Millisecond // Fast polling for tests

	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: &clustermetadatapb.ShardKey{
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

	// Verify SetTermPrimary was called (configuration was attempted)
	assert.Contains(t, fakeClient.CallLog, "SetTermPrimary(multipooler-cell1-replica1)")
	// Verify pg_rewind was tried after replication failed to start
	assert.Contains(t, fakeClient.CallLog, "RewindToSource(multipooler-cell1-replica1)")

	// Verify the pooler was marked as DRAINED in topology when pg_rewind wasn't feasible
	updatedPooler, err := ts.GetMultiPooler(ctx, replicaID)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_DRAINED, updatedPooler.Type)
}
