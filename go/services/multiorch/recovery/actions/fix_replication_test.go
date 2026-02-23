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

	protoStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	fakeClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(protoStore, fakeClient, slog.Default())

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

	protoStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	fakeClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(protoStore, fakeClient, slog.Default())

	// Add only replicas, no primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	protoStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
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

	protoStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			"multipooler-cell1-primary": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{IsInitialized: true},
				},
			},
		},
		ConsensusStatusResponses: map[string]*consensusdatapb.StatusResponse{
			"multipooler-cell1-primary": {
				CurrentTerm: 1,
			},
		},
		StandbyReplicationStatusResponses: map[string]*multipoolermanagerdatapb.StandbyReplicationStatusResponse{
			"multipooler-cell1-replica1": {
				Status: &multipoolermanagerdatapb.StandbyReplicationStatus{},
			},
		},
	}
	poolerStore := store.NewPoolerStore(protoStore, fakeClient, slog.Default())

	// Add replica and primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	protoStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replicaID,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
	})
	protoStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
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

	protoStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			"multipooler-cell1-primary": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{IsInitialized: true},
				},
			},
		},
		ConsensusStatusResponses: map[string]*consensusdatapb.StatusResponse{
			"multipooler-cell1-primary": {
				CurrentTerm: 1,
			},
		},
		StandbyReplicationStatusResponses: map[string]*multipoolermanagerdatapb.StandbyReplicationStatusResponse{
			"multipooler-cell1-replica1": {
				Status: &multipoolermanagerdatapb.StandbyReplicationStatus{
					// No PrimaryConnInfo - replication not configured
					WalReceiverStatus: "streaming", // WAL receiver connected after fix
					LastReceiveLsn:    "0/1234",    // Set after fix
				},
			},
		},
		SetPrimaryConnInfoResponses: map[string]*multipoolermanagerdatapb.SetPrimaryConnInfoResponse{
			"multipooler-cell1-replica1": {},
		},
		UpdateSynchronousStandbyListResponses: map[string]*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{
			"multipooler-cell1-primary": {},
		},
	}
	poolerStore := store.NewPoolerStore(protoStore, fakeClient, slog.Default())

	// Add replica and primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	protoStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replicaID,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
	})
	protoStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
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

	// Verify UpdateSynchronousStandbyList was called on the primary to add the replica
	assert.Contains(t, fakeClient.CallLog, "UpdateSynchronousStandbyList(multipooler-cell1-primary)")
}

func TestFixReplicationAction_ExecuteAlreadyConfigured(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	protoStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			"multipooler-cell1-primary": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{IsInitialized: true},
				},
			},
		},
		StandbyReplicationStatusResponses: map[string]*multipoolermanagerdatapb.StandbyReplicationStatusResponse{
			"multipooler-cell1-replica1": {
				Status: &multipoolermanagerdatapb.StandbyReplicationStatus{
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
	}
	poolerStore := store.NewPoolerStore(protoStore, fakeClient, slog.Default())

	// Add replica and primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	protoStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replicaID,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
	})
	protoStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
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

func (c *delayedStreamingClient) StandbyReplicationStatus(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
	request *multipoolermanagerdatapb.StandbyReplicationStatusRequest,
) (*multipoolermanagerdatapb.StandbyReplicationStatusResponse, error) {
	c.callCount++
	if c.callCount >= c.streamAfterCalls {
		return &multipoolermanagerdatapb.StandbyReplicationStatusResponse{
			Status: &multipoolermanagerdatapb.StandbyReplicationStatus{
				WalReceiverStatus: "streaming",
				LastReceiveLsn:    "0/1234",
			},
		}, nil
	}
	return &multipoolermanagerdatapb.StandbyReplicationStatusResponse{
		Status: &multipoolermanagerdatapb.StandbyReplicationStatus{
			WalReceiverStatus: "startup",
		},
	}, nil
}

func TestVerifyReplicationStarted_SlowWalReceiver(t *testing.T) {
	ctx := context.Background()

	// WAL receiver starts streaming after more attempts than would fit in the
	// old 2.5s window (5 × 500ms), but within the current limit. This ensures
	// the polling window is wide enough for coverage builds where WAL receiver
	// takes several seconds to connect.
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

// replicationStatusClient wraps FakeClient to return different StandbyReplicationStatus responses
// based on call count, simulating the progression from "not configured" to various failure states.
// The walReceiverStatus field controls what subsequent calls return, allowing tests to simulate
// different failure modes (slow startup vs timeline divergence).
type replicationStatusClient struct {
	*rpcclient.FakeClient
	callCount         int
	walReceiverStatus string // WAL receiver status for calls after the first
}

func (c *replicationStatusClient) StandbyReplicationStatus(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
	request *multipoolermanagerdatapb.StandbyReplicationStatusRequest,
) (*multipoolermanagerdatapb.StandbyReplicationStatusResponse, error) {
	c.callCount++
	// First call is verifyProblemExists (no primary_conninfo - triggers the fix)
	// Subsequent calls are verifyReplicationStarted polling (never reaches streaming)
	if c.callCount == 1 {
		return &multipoolermanagerdatapb.StandbyReplicationStatusResponse{
			Status: &multipoolermanagerdatapb.StandbyReplicationStatus{
				// No PrimaryConnInfo - triggers the fix
			},
		}, nil
	}
	return &multipoolermanagerdatapb.StandbyReplicationStatusResponse{
		Status: &multipoolermanagerdatapb.StandbyReplicationStatus{
			WalReceiverStatus: c.walReceiverStatus,
		},
	}, nil
}

func TestFixReplicationAction_FailsWhenReplicationDoesNotStart(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	protoStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	baseFakeClient := rpcclient.NewFakeClient()
	baseFakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{IsInitialized: true},
	})
	baseFakeClient.ConsensusStatusResponses = map[string]*consensusdatapb.StatusResponse{
		"multipooler-cell1-primary": {
			CurrentTerm: 1,
			TimelineInfo: &consensusdatapb.TimelineInfo{
				TimelineId: 2, // Primary on timeline 2
			},
		},
		"multipooler-cell1-replica1": {
			CurrentTerm: 1,
			TimelineInfo: &consensusdatapb.TimelineInfo{
				TimelineId: 1, // Replica still on timeline 1 - DIVERGED!
			},
		},
	}
	baseFakeClient.SetPrimaryConnInfoResponses = map[string]*multipoolermanagerdatapb.SetPrimaryConnInfoResponse{
		"multipooler-cell1-replica1": {},
	}
	baseFakeClient.UpdateSynchronousStandbyListResponses = map[string]*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{
		"multipooler-cell1-primary": {},
	}
	// pg_rewind dry-run fails, so it marks the pooler as DRAINED
	baseFakeClient.RewindToSourceResponses = map[string]*multipoolermanagerdatapb.RewindToSourceResponse{
		"multipooler-cell1-replica1": {
			Success:      false,
			ErrorMessage: "pg_rewind not feasible: source timeline diverged before target's last checkpoint",
		},
	}

	// WAL receiver status "stopping" indicates the WAL receiver connected but was
	// terminated, which happens with timeline divergence.
	fakeClient := &replicationStatusClient{FakeClient: baseFakeClient, walReceiverStatus: "stopping"}
	poolerStore := store.NewPoolerStore(protoStore, fakeClient, slog.Default())

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

	protoStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: replica,
	})
	protoStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: primary,
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

	// Should fail: pg_rewind marked pooler as DRAINED (returned nil), then
	// re-verification fails because the replica is not actually replicating.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replication did not start after pg_rewind")

	// Verify SetPrimaryConnInfo was called (configuration was attempted)
	assert.Contains(t, fakeClient.CallLog, "SetPrimaryConnInfo(multipooler-cell1-replica1)")
	// Verify pg_rewind was tried because WAL receiver "stopping" indicates divergence
	assert.Contains(t, fakeClient.CallLog, "RewindToSource(multipooler-cell1-replica1)")

	// Verify the pooler was marked as DRAINED in topology when pg_rewind wasn't feasible
	updatedPooler, err := ts.GetMultiPooler(ctx, replicaID)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_DRAINED, updatedPooler.Type)
}

func TestFixNotReplicating_NoRewindOnSlowStartup(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	protoStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	baseFakeClient := rpcclient.NewFakeClient()
	baseFakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{IsInitialized: true},
	})
	baseFakeClient.ConsensusStatusResponses = map[string]*consensusdatapb.StatusResponse{
		"multipooler-cell1-primary": {
			CurrentTerm: 1,
		},
	}
	baseFakeClient.SetPrimaryConnInfoResponses = map[string]*multipoolermanagerdatapb.SetPrimaryConnInfoResponse{
		"multipooler-cell1-replica1": {},
	}

	// WAL receiver status "" (not running) indicates the WAL receiver hasn't
	// connected yet — just slow to start, not timeline divergence.
	fakeClient := &replicationStatusClient{FakeClient: baseFakeClient, walReceiverStatus: ""}
	poolerStore := store.NewPoolerStore(protoStore, fakeClient, slog.Default())

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

	require.NoError(t, ts.CreateMultiPooler(ctx, replica))
	require.NoError(t, ts.CreateMultiPooler(ctx, primary))

	protoStore.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: replica,
	})
	protoStore.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: primary,
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

	// Should fail because replication didn't start, but should NOT trigger pg_rewind
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no timeline divergence")

	// Verify SetPrimaryConnInfo was called
	assert.Contains(t, fakeClient.CallLog, "SetPrimaryConnInfo(multipooler-cell1-replica1)")
	// Verify pg_rewind was NOT called — no evidence of timeline divergence
	assert.NotContains(t, fakeClient.CallLog, "RewindToSource(multipooler-cell1-replica1)")

	// Verify the pooler was NOT marked as DRAINED
	updatedPooler, err := ts.GetMultiPooler(ctx, replicaID)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, updatedPooler.Type)
}
