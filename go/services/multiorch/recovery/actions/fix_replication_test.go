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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestFixReplicationAction_Metadata(t *testing.T) {
	action := NewFixReplicationAction(config.NewTestConfig(), nil, nil, slog.Default())

	metadata := action.Metadata()

	assert.Equal(t, "FixReplication", metadata.Name)
	assert.Equal(t, "Configure or repair replication on a replica", metadata.Description)
	assert.True(t, metadata.Retryable)
	assert.Equal(t, 45*time.Second, metadata.Timeout)
}

func TestFixReplicationAction_RequiresHealthyLeader(t *testing.T) {
	action := NewFixReplicationAction(config.NewTestConfig(), nil, nil, slog.Default())

	// FixReplication requires a healthy primary to configure replication
	assert.True(t, action.RequiresHealthyLeader())
}

func TestFixReplicationAction_ExecuteReplicaNotFound(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := &rpcclient.FakeClient{}
	poolerStore := store.NewTestCache(t)

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, slog.Default())

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
	poolerStore := store.NewTestCache(t)

	// Add only replicas, no primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	}, nil))

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, slog.Default())

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
	assert.Contains(t, err.Error(), "no consensus leader known")
}

func TestFixReplicationAction_ExecuteUnsupportedProblemCode(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			"multipooler-cell1-primary": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
					},
					ConsensusStatus: &clustermetadatapb.ConsensusStatus{
						Id:              fixReplPrimaryID,
						TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
						CurrentPosition: leaderCurrentPosition(1),
					},
				},
			},
		},
	}
	poolerStore := store.NewTestCache(t)

	// Add replica and primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	}, nil))
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: fixReplPrimaryID,
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
			Id:              fixReplPrimaryID,
			CurrentPosition: leaderCurrentPosition(1),
		},
	}, nil))

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, slog.Default())

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
// fixNotReplicating routes through SetPrimary on the replica.
func TestFixReplicationAction_ExecuteSuccessNotReplicating(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			"multipooler-cell1-primary": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
					},
					ConsensusStatus: &clustermetadatapb.ConsensusStatus{
						Id:              fixReplPrimaryID,
						TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
						CurrentPosition: leaderCurrentPosition(1),
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
		SetPrimaryResponses: map[topoclient.ComponentID]*consensusdatapb.SetPrimaryResponse{
			"multipooler-cell1-replica1": {},
		},
	}
	poolerStore := store.NewTestCache(t)

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	}, nil))
	primaryPosition := &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			LeaderId:   fixReplPrimaryID,
		}},
		Lsn: "0/1234",
	}
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: fixReplPrimaryID,
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
			Id:              fixReplPrimaryID,
			TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
			CurrentPosition: primaryPosition,
		},
	}, nil))

	cfg := config.NewTestConfig()
	action := NewFixReplicationAction(cfg, fakeClient, poolerStore, slog.Default())

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

	// Verify SetPrimary was called on the replica, NOT SetPrimaryConnInfo.
	assert.Contains(t, fakeClient.CallLog, "SetPrimary(multipooler-cell1-replica1)")
	assert.NotContains(t, fakeClient.CallLog, "SetPrimaryConnInfo(multipooler-cell1-replica1)")

	// Verify the request carried the primary's contact info and known position.
	setPrimaryReq := fakeClient.SetPrimaryRequests["multipooler-cell1-replica1"]
	require.NotNil(t, setPrimaryReq)
	require.NotNil(t, setPrimaryReq.GetReplicationPrimary().GetPrimary())
	assert.Equal(t, "primary", setPrimaryReq.GetReplicationPrimary().GetPrimary().GetId().GetName())
	assert.Equal(t, "primary.example.com", setPrimaryReq.GetReplicationPrimary().GetPrimary().GetHost())
	require.NotNil(t, setPrimaryReq.GetReplicationPrimary().GetPosition().GetDecision())
	assert.Equal(t, int64(1), setPrimaryReq.GetReplicationPrimary().GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
}

func TestFixReplicationAction_ExecuteAlreadyConfigured(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			"multipooler-cell1-primary": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
					},
					ConsensusStatus: &clustermetadatapb.ConsensusStatus{
						Id:              fixReplPrimaryID,
						CurrentPosition: leaderCurrentPosition(1),
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
	poolerStore := store.NewTestCache(t)

	// Add replica and primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	}, nil))
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: fixReplPrimaryID,
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
			Id:              fixReplPrimaryID,
			CurrentPosition: leaderCurrentPosition(1),
		},
	}, nil))

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, slog.Default())

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

// nullLsnThenStreamingClient wraps FakeClient to simulate the brief window
// during a WAL receiver reconnect where the first Status call for the replica
// returns "streaming" with a null LSN, and subsequent calls return "streaming"
// with a real LSN.
type nullLsnThenStreamingClient struct {
	*rpcclient.FakeClient
	replicaID    topoclient.ComponentID
	replicaCalls int
}

func (c *nullLsnThenStreamingClient) Status(
	ctx context.Context,
	pooler *clustermetadatapb.Multipooler,
	request *multipoolermanagerdatapb.StatusRequest,
) (*multipoolermanagerdatapb.StatusResponse, error) {
	id := topoclient.ComponentIDString(pooler.GetId())
	if id == c.replicaID {
		c.replicaCalls++
		lsn := ""
		if c.replicaCalls > 1 {
			lsn = "0/1234"
		}
		return &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: "primary.example.com",
						Port: 5432,
					},
					WalReceiverStatus: "streaming",
					LastReceiveLsn:    lsn,
				},
			},
		}, nil
	}
	return c.FakeClient.Status(ctx, pooler, request)
}

// TestFixReplicationAction_ExecuteStreamingNullLsn covers the brief window
// during a WAL receiver reconnect attempt where pg_stat_wal_receiver shows
// "streaming" but pg_last_wal_receive_lsn() is still NULL. Without the null-LSN
// guard, Execute would return early treating the replica as healthy, and never
// call SetPrimary.
func TestFixReplicationAction_ExecuteStreamingNullLsn(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}

	baseClient := &rpcclient.FakeClient{
		StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			"multipooler-cell1-primary": {
				Response: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
					},
					ConsensusStatus: &clustermetadatapb.ConsensusStatus{
						Id:              fixReplPrimaryID,
						CurrentPosition: leaderCurrentPosition(1),
					},
				},
			},
		},
	}
	client := &nullLsnThenStreamingClient{
		FakeClient: baseClient,
		replicaID:  "multipooler-cell1-replica1",
	}

	poolerStore := store.NewTestCache(t)
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
	}, nil))
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: fixReplPrimaryID,
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
			Id:              fixReplPrimaryID,
			CurrentPosition: leaderCurrentPosition(1),
		},
	}, nil))

	action := NewFixReplicationAction(config.NewTestConfig(), client, poolerStore, slog.Default())

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

	// The null-LSN guard treats the first poll as not-yet-replicating, so
	// Execute proceeds to call SetPrimary (fixNotReplicating path).
	require.NoError(t, err)
	require.NotNil(t, baseClient.SetPrimaryRequests["multipooler-cell1-replica1"],
		"SetPrimary must be called when streaming status has a null LSN")
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
	pooler *clustermetadatapb.Multipooler,
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

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, nil, slog.Default())
	action.verifyPollInterval = 10 * time.Millisecond // Fast polling for tests

	replica := store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
		},
	}, nil)

	err := action.verifyReplicationStarted(ctx, replica)
	require.NoError(t, err, "verifyReplicationStarted should succeed when WAL receiver starts streaming after several polling cycles")
	assert.Equal(t, streamAfterCalls, fakeClient.callCount, "should have polled exactly until streaming started")
}

// connInfoSetNotStreamingClient simulates a replica where primary_conninfo is already
// correctly configured (e.g. from a prior fix attempt or set before a timeline divergence)
// but the WAL receiver is not streaming. Subsequent calls, from verifyReplicationStarted
// after the fix re-runs SetPrimary, return "streaming".
type connInfoSetNotStreamingClient struct {
	*rpcclient.FakeClient
	callCount int
	host      string
	port      int32
}

func (c *connInfoSetNotStreamingClient) Status(
	ctx context.Context,
	pooler *clustermetadatapb.Multipooler,
	request *multipoolermanagerdatapb.StatusRequest,
) (*multipoolermanagerdatapb.StatusResponse, error) {
	if pooler == nil || pooler.Id == nil || pooler.Id.Name != "replica1" {
		return c.FakeClient.Status(ctx, pooler, request)
	}
	c.callCount++
	if c.callCount == 1 {
		// verifyProblemExists: primary_conninfo set but WAL receiver not streaming
		return &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: c.host,
						Port: c.port,
					},
					WalReceiverStatus: "none",
				},
			},
		}, nil
	}
	// verifyReplicationStarted: streaming after the fix is re-applied
	return &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				WalReceiverStatus: "streaming",
				LastReceiveLsn:    "0/5678",
			},
		},
	}, nil
}

// TestFixReplicationAction_ExecuteRetryWhenConnInfoSetButNotStreaming is a regression test
// for the bug where verifyProblemExists returned "no problem" when primary_conninfo was
// already set from a previous failed attempt, but the WAL receiver was not actually
// streaming. This caused orch to skip the fix and loop forever detecting the problem
// without resolving it.
func TestFixReplicationAction_ExecuteRetryWhenConnInfoSetButNotStreaming(t *testing.T) {
	ctx := context.Background()

	baseFakeClient := rpcclient.NewFakeClient()
	baseFakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: true,
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
		},
	})

	fakeClient := &connInfoSetNotStreamingClient{
		FakeClient: baseFakeClient,
		host:       "primary.example.com",
		port:       5432,
	}
	poolerStore := store.NewTestCache(t)

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	shardKey := &clustermetadatapb.ShardKey{
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
	}
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       replicaID,
			ShardKey: shardKey,
			Type:     clustermetadatapb.PoolerType_REPLICA,
		},
	}, nil))
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       fixReplPrimaryID,
			ShardKey: shardKey,
			Type:     clustermetadatapb.PoolerType_PRIMARY,
			Hostname: "primary.example.com",
			PortMap:  map[string]int32{"postgres": 5432},
		},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id:              fixReplPrimaryID,
			CurrentPosition: leaderCurrentPosition(1),
		},
	}, nil))

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, slog.Default())
	action.verifyPollInterval = 10 * time.Millisecond

	problem := types.Problem{
		Code:     types.ProblemReplicaNotReplicating,
		ShardKey: shardKey,
		PoolerID: replicaID,
	}

	err := action.Execute(ctx, problem)

	// Should succeed: the fix was re-run despite primary_conninfo being set
	require.NoError(t, err)

	// SetPrimary must have been called — confirms the fix ran again rather
	// than being skipped because primary_conninfo was already present.
	assert.Contains(t, fakeClient.CallLog, "SetPrimary(multipooler-cell1-replica1)")
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
	pooler *clustermetadatapb.Multipooler,
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
//     2..N+1. verifyReplicationStarted after SetPrimary  → not streaming (all fail)
//     N+2+.  verifyReplicationStarted after RewindToSource → streaming
type streamingAfterRewindClient struct {
	*rpcclient.FakeClient
	replicaCallCount  int
	nonStreamingCalls int // number of non-streaming calls after call 1 before transitioning
}

func (c *streamingAfterRewindClient) Status(
	ctx context.Context,
	pooler *clustermetadatapb.Multipooler,
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
		// verifyReplicationStarted after SetPrimary: WAL receiver idle
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
//  1. SetPrimary is called but the WAL receiver never starts streaming.
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
			Id:              fixReplPrimaryID,
			TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
			CurrentPosition: leaderCurrentPosition(1),
		},
	})
	baseFakeClient.SetStatusResponse("multipooler-cell1-replica1", &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		},
	})
	baseFakeClient.UpdateConsensusRuleResponses = map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
		"multipooler-cell1-primary": {},
	}
	// RewindToSource succeeds, simulating pg_rewind running and primary_conninfo
	// being restored by the fix in rpc_manager.go.
	baseFakeClient.RewindToSourceResponses = map[topoclient.ComponentID]*multipoolermanagerdatapb.RewindToSourceResponse{
		"multipooler-cell1-replica1": {
			Success:         true,
			RewindPerformed: true,
		},
	}

	fakeClient := &streamingAfterRewindClient{
		FakeClient:        baseFakeClient,
		nonStreamingCalls: verifyAttempts,
	}
	poolerStore := store.NewTestCache(t)

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	replica := &clustermetadatapb.Multipooler{
		Id: replicaID,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		Type: clustermetadatapb.PoolerType_REPLICA,
	}
	primary := &clustermetadatapb.Multipooler{
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

	require.NoError(t, ts.CreateMultipooler(ctx, replica))
	require.NoError(t, ts.CreateMultipooler(ctx, primary))

	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: replica,
	}, nil))
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: primary,
		Status:      &multipoolermanagerdatapb.Status{PostgresReady: true},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id:             fixReplPrimaryID,
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					LeaderId:   fixReplPrimaryID,
				}},
			},
			// Leader is rewind-ready, so fix_replication's tryPgRewind gate proceeds.
			ReplicationPrimary: rewindReadyPrimary(1),
		},
	}, nil))

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, slog.Default())
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
	assert.Contains(t, fakeClient.CallLog, "SetPrimary(multipooler-cell1-replica1)",
		"SetPrimary must be called first")
	assert.Contains(t, fakeClient.CallLog, "RewindToSource(multipooler-cell1-replica1)",
		"RewindToSource must be called when SetPrimary doesn't start streaming")

	// REGRESSION: the pooler must NOT be drained — RewindToSource succeeded and
	// streaming started. Before the fix, primary_conninfo was wiped by pg_rewind
	// so verifyReplicationStarted always failed, eventually routing to DRAINED.
	updatedPooler, err := ts.GetMultipooler(ctx, replicaID)
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
			Id:              fixReplPrimaryID,
			TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
			CurrentPosition: leaderCurrentPosition(1),
		},
	})
	baseFakeClient.SetStatusResponse("multipooler-cell1-replica1", &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm: 1,
			},
		},
	})
	baseFakeClient.UpdateConsensusRuleResponses = map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
		"multipooler-cell1-primary": {},
	}
	// pg_rewind dry-run fails, so it marks the pooler as DRAINED
	baseFakeClient.RewindToSourceResponses = map[topoclient.ComponentID]*multipoolermanagerdatapb.RewindToSourceResponse{
		"multipooler-cell1-replica1": {
			Success:      false,
			ErrorMessage: "pg_rewind not feasible: source timeline diverged before target's last checkpoint",
		},
	}

	fakeClient := &replicationStatusClient{FakeClient: baseFakeClient, walReceiverStatus: "stopping"}
	poolerStore := store.NewTestCache(t)

	// Add replica and primary
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1",
	}
	replica := &clustermetadatapb.Multipooler{
		Id: replicaID,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		Type: clustermetadatapb.PoolerType_REPLICA,
	}
	primary := &clustermetadatapb.Multipooler{
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
	require.NoError(t, ts.CreateMultipooler(ctx, replica))
	require.NoError(t, ts.CreateMultipooler(ctx, primary))

	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: replica,
	}, nil))
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: primary,
		Status:      &multipoolermanagerdatapb.Status{PostgresReady: true},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id:              fixReplPrimaryID,
			CurrentPosition: leaderCurrentPosition(1),
			// Leader is rewind-ready, so fix_replication's tryPgRewind gate proceeds.
			ReplicationPrimary: rewindReadyPrimary(1),
		},
	}, nil))

	action := NewFixReplicationAction(config.NewTestConfig(), fakeClient, poolerStore, slog.Default())
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

	// pg_rewind was not feasible, so the action fails with FAILED_PRECONDITION.
	// Orch no longer drains the pooler itself; surfacing the broken pooler to the
	// provisioner via its lifecycle stage is future work (see fix_replication.go).
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))

	// Verify SetPrimary was called (configuration was attempted)
	assert.Contains(t, fakeClient.CallLog, "SetPrimary(multipooler-cell1-replica1)")
	// Verify pg_rewind was tried after replication failed to start
	assert.Contains(t, fakeClient.CallLog, "RewindToSource(multipooler-cell1-replica1)")

	// The pooler's topology Type must be left untouched: orch no longer writes a
	// pooler's record to mark it broken. Surfacing the broken pooler durably to the
	// provisioner via its lifecycle stage is future work (see fix_replication.go).
	updatedPooler, err := ts.GetMultipooler(ctx, replicaID)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, updatedPooler.Type)
}

// fixReplPrimaryID is the consensus leader used across the fix-replication tests.
var fixReplPrimaryID = &clustermetadatapb.ID{
	Component: clustermetadatapb.ID_MULTIPOOLER,
	Cell:      "cell1",
	Name:      "primary",
}

// leaderCurrentPosition builds a current position whose rule names fixReplPrimaryID
// as the consensus leader at the given coordinator term.
func leaderCurrentPosition(term int64) *clustermetadatapb.PoolerPosition {
	return &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
			LeaderId:   fixReplPrimaryID,
		}},
	}
}

// rewindReadyPrimary builds a replication primary that names fixReplPrimaryID as
// the rewind-ready leader at the given coordinator term. The rule number must be
// non-zero so ReplicationPrimaryOrNil does not treat it as a phantom 0/0 entry,
// which would read rewind_ready as false and make fix_replication defer pg_rewind.
func rewindReadyPrimary(term int64) *clustermetadatapb.ReplicationPrimary {
	return &clustermetadatapb.ReplicationPrimary{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
			LeaderId:   fixReplPrimaryID,
		}},
		RewindReady: true,
	}
}
