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
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestFixReplicationAction_Metadata(t *testing.T) {
	action := NewFixReplicationAction(nil, nil, nil, slog.Default())

	metadata := action.Metadata()

	assert.Equal(t, "FixReplication", metadata.Name)
	assert.Equal(t, "Configure or repair replication on a replica", metadata.Description)
	assert.True(t, metadata.Retryable)
	assert.Equal(t, 45*time.Second, metadata.Timeout)
}

func TestFixReplicationAction_RequiresHealthyPrimary(t *testing.T) {
	action := NewFixReplicationAction(nil, nil, nil, slog.Default())

	// FixReplication requires a healthy primary to configure replication
	assert.True(t, action.RequiresHealthyPrimary())
}

func TestFixReplicationAction_Priority(t *testing.T) {
	action := NewFixReplicationAction(nil, nil, nil, slog.Default())

	assert.Equal(t, types.PriorityHigh, action.Priority())
}

func TestFixReplicationAction_ExecuteReplicaNotFound(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	protoStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	fakeClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(protoStore, fakeClient, slog.Default())

	action := NewFixReplicationAction(fakeClient, poolerStore, ts, slog.Default())

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

	action := NewFixReplicationAction(fakeClient, poolerStore, ts, slog.Default())

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

	action := NewFixReplicationAction(fakeClient, poolerStore, ts, slog.Default())

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
					LastReceiveLsn: "0/1234", // Set after fix
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

	action := NewFixReplicationAction(fakeClient, poolerStore, ts, slog.Default())

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

	action := NewFixReplicationAction(fakeClient, poolerStore, ts, slog.Default())

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

// replicationStatusClient wraps FakeClient to return different StandbyReplicationStatus responses
// based on call count, simulating the progression from "not configured" to "configured but not streaming".
type replicationStatusClient struct {
	*rpcclient.FakeClient
	callCount int
}

func (c *replicationStatusClient) StandbyReplicationStatus(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
	request *multipoolermanagerdatapb.StandbyReplicationStatusRequest,
) (*multipoolermanagerdatapb.StandbyReplicationStatusResponse, error) {
	c.callCount++
	// First call is verifyProblemExists (no primary_conninfo - triggers the fix)
	// Subsequent calls are verifyReplicationStarted polling (never returns LSN)
	if c.callCount == 1 {
		return &multipoolermanagerdatapb.StandbyReplicationStatusResponse{
			Status: &multipoolermanagerdatapb.StandbyReplicationStatus{
				// No PrimaryConnInfo - triggers the fix
			},
		}, nil
	}
	return &multipoolermanagerdatapb.StandbyReplicationStatusResponse{
		Status: &multipoolermanagerdatapb.StandbyReplicationStatus{
			LastReceiveLsn: "", // Still not streaming
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
		"multipooler-cell1-primary": {CurrentTerm: 1},
	}
	baseFakeClient.SetPrimaryConnInfoResponses = map[string]*multipoolermanagerdatapb.SetPrimaryConnInfoResponse{
		"multipooler-cell1-replica1": {},
	}
	baseFakeClient.UpdateSynchronousStandbyListResponses = map[string]*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{
		"multipooler-cell1-primary": {},
	}

	fakeClient := &replicationStatusClient{FakeClient: baseFakeClient}
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

	action := NewFixReplicationAction(fakeClient, poolerStore, ts, slog.Default())

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

	// Should fail because replication never started streaming
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replication configured but failed to verify streaming")

	// Verify SetPrimaryConnInfo was still called (configuration was attempted)
	assert.Contains(t, fakeClient.CallLog, "SetPrimaryConnInfo(multipooler-cell1-replica1)")
}
