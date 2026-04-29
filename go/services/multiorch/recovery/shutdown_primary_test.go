// Copyright 2026 Supabase, Inc.
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

package recovery

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// makePoolerID returns an ID for a multipooler in cell1.
func makePoolerID(name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      name,
	}
}

// makeMultiPooler creates a MultiPooler for the given shard and type.
func makeMultiPooler(name string, t clustermetadatapb.PoolerType) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id:         makePoolerID(name),
		Database:   "db1",
		TableGroup: "default",
		Shard:      "0",
		Type:       t,
		Hostname:   "host-" + name,
	}
}

// shutdownTestFixture sets up a test engine pre-populated with one primary and
// some standbys. It also configures the fake client so that:
//   - Status on the primary returns finalLSN so ShutdownPrimary can read it
//     before waiting for standbys to catch up.
//   - EmergencyDemote on the primary succeeds (called after standbys catch up).
//   - Status polls for standbys report REPLICA unless overridden.
//   - Status poll for standby1 reports PRIMARY after promotion so that
//     shutdownFindNewPrimary finds it.
func shutdownTestFixture(t *testing.T) (*Engine, *rpcclient.FakeClient, *clustermetadatapb.MultiPooler, []*clustermetadatapb.MultiPooler) {
	t.Helper()
	const finalLSN = "0/ABC123"

	fakeClient := rpcclient.NewFakeClient()

	primary := makeMultiPooler("primary", clustermetadatapb.PoolerType_PRIMARY)
	standby1 := makeMultiPooler("standby1", clustermetadatapb.PoolerType_REPLICA)
	standby2 := makeMultiPooler("standby2", clustermetadatapb.PoolerType_REPLICA)

	primaryKey := topoclient.MultiPoolerIDString(primary.Id)
	standby1Key := topoclient.MultiPoolerIDString(standby1.Id)
	standby2Key := topoclient.MultiPoolerIDString(standby2.Id)

	// Status on the primary returns the current LSN so ShutdownPrimary can wait
	// for standbys to catch up before stopping the primary.
	fakeClient.SetStatusResponse(primaryKey, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:      clustermetadatapb.PoolerType_PRIMARY,
			PostgresRunning: true,
			PostgresReady:   true,
			IsInitialized:   true,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   finalLSN,
				Ready: true,
			},
		},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		},
	})
	// EmergencyDemote is called after standbys have caught up.
	fakeClient.EmergencyDemoteResponses[primaryKey] = &multipoolermanagerdatapb.EmergencyDemoteResponse{}

	// WaitForLSN succeeds for both standbys.
	fakeClient.WaitForLSNResponses[standby1Key] = &multipoolermanagerdatapb.WaitForLSNResponse{}
	fakeClient.WaitForLSNResponses[standby2Key] = &multipoolermanagerdatapb.WaitForLSNResponse{}

	// AppointLeader requires a complete set of fake responses per standby:
	//
	//  1. Status — forceHealthCheckShardPoolers (step 3) calls this and populates
	//     IsLastCheckValid, IsInitialized, IsPostgresReady, ConsensusTerm in the store.
	//     Pre-vote checks all four fields. After promotion standby1 reports PRIMARY so
	//     shutdownFindNewPrimary can identify the new leader.
	//
	//  2. ConsensusStatus — used by the BeginTerm phase to read current term and WAL position.
	//
	//  3. BeginTermResponse — must have Accepted:true plus a WalPosition so the candidate
	//     selector can rank nodes by LSN.
	//
	//  4. PromoteResponse — called on the winning candidate.
	//
	//  5. SetPrimaryConnInfo — called on standbys after promotion to reconfigure replication.

	for _, key := range []string{standby1Key, standby2Key} {
		fakeClient.ConsensusStatusResponses[key] = &consensusdatapb.StatusResponse{
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Lsn: finalLSN,
				},
			},
		}
		fakeClient.BeginTermResponses[key] = &consensusdatapb.BeginTermResponse{
			Accepted: true,
			WalPosition: &consensusdatapb.WALPosition{
				LastReceiveLsn: finalLSN,
				LastReplayLsn:  finalLSN,
			},
		}
		fakeClient.PromoteResponses[key] = &multipoolermanagerdatapb.PromoteResponse{}
		fakeClient.SetPrimaryConnInfoResponses[key] = &multipoolermanagerdatapb.SetPrimaryConnInfoResponse{}
	}
	// Give standby1 a strictly higher LSN so it consistently wins the election.
	// This ensures EstablishLeadership calls WaitForLSN on standby1 (which succeeds),
	// not standby2 (which may have a per-test injected error).
	const standby1LSN = "0/FFFFFF"
	fakeClient.ConsensusStatusResponses[standby1Key] = &consensusdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Lsn: standby1LSN,
			},
		},
	}
	fakeClient.BeginTermResponses[standby1Key] = &consensusdatapb.BeginTermResponse{
		Accepted: true,
		WalPosition: &consensusdatapb.WALPosition{
			LastReceiveLsn: standby1LSN,
			LastReplayLsn:  standby1LSN,
		},
	}
	// Status RPC responses used by AppointLeader's pre-vote health checks.
	// standby1 reports PRIMARY because pollShardPoolers is fire-and-forget in
	// tests (no real stream), so we pre-populate its post-promotion state here.
	consensusStatus := &clustermetadatapb.ConsensusStatus{
		TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
	}
	fakeClient.SetStatusResponse(standby1Key, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:      clustermetadatapb.PoolerType_PRIMARY,
			PostgresRunning: true,
			PostgresReady:   true,
			IsInitialized:   true,
		},
		ConsensusStatus: consensusStatus,
	})
	fakeClient.SetStatusResponse(standby2Key, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:      clustermetadatapb.PoolerType_REPLICA,
			PostgresRunning: true,
			PostgresReady:   true,
			IsInitialized:   true,
		},
		ConsensusStatus: consensusStatus,
	})

	engine := newTestEngine(context.Background(), t, WithFakeClient(fakeClient))

	// AppointLeader loads the durability policy from topology; create the database first.
	err := engine.ts.CreateDatabase(context.Background(), "db1", &clustermetadatapb.Database{
		Name:                      "db1",
		BootstrapDurabilityPolicy: topoclient.AtLeastN(1),
	})
	require.NoError(t, err)

	// Pre-populate the store so AppointLeader and shutdownFindNewPrimary work without
	// a live health stream. IsLastCheckValid + ConsensusStatus satisfy discoverMaxTerm
	// and preVote. standby1 is set to PRIMARY (its post-promotion state) so that
	// shutdownFindNewPrimary can locate it.
	// The primary is stored as STOPPING: in real operation the multipooler sets
	// PoolerType_STOPPING on the health stream before ShutdownPrimary is called
	// (that STOPPING signal is what triggers ShutdownPrimary). isPrimaryCandidate
	// relies on this to exclude the old primary from the election cohort.
	engine.poolerStore.Set(primaryKey, &multiorchdatapb.PoolerHealthState{
		MultiPooler:      primary,
		IsLastCheckValid: true,
		ConsensusStatus:  consensusStatus,
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:      clustermetadatapb.PoolerType_STOPPING,
			PostgresRunning: true,
			PostgresReady:   true,
			IsInitialized:   true,
		},
	})
	// standby1 is pre-populated as PRIMARY (its post-promotion state) so that
	// shutdownFindNewPrimary can locate it without a real snapshot arriving.
	engine.poolerStore.Set(standby1Key, &multiorchdatapb.PoolerHealthState{
		MultiPooler:      standby1,
		IsLastCheckValid: true,
		ConsensusStatus:  consensusStatus,
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:      clustermetadatapb.PoolerType_PRIMARY,
			PostgresRunning: true,
			PostgresReady:   true,
			IsInitialized:   true,
		},
	})
	engine.poolerStore.Set(standby2Key, &multiorchdatapb.PoolerHealthState{
		MultiPooler:      standby2,
		IsLastCheckValid: true,
		ConsensusStatus:  consensusStatus,
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:      clustermetadatapb.PoolerType_REPLICA,
			PostgresRunning: true,
			PostgresReady:   true,
			IsInitialized:   true,
		},
	})

	return engine, fakeClient, primary, []*clustermetadatapb.MultiPooler{standby1, standby2}
}

// TestShutdownPrimary_HappyPath verifies the full switchover sequence:
// EmergencyDemote is called, standbys are waited on, and a new primary is
// returned in the response.
func TestShutdownPrimary_HappyPath(t *testing.T) {
	ctx := context.Background()
	engine, fakeClient, primary, standbys := shutdownTestFixture(t)

	req := &multiorchpb.ShutdownPrimaryRequest{
		Database:              primary.Database,
		TableGroup:            primary.TableGroup,
		Shard:                 primary.Shard,
		PrimaryId:             primary.Id,
		DrainTimeout:          durationpb.New(5e9),  // 5s
		StandbyCatchupTimeout: durationpb.New(30e9), // 30s
	}

	resp, err := engine.ShutdownPrimary(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.NewPrimaryId)

	// The new primary must be one of the original standbys.
	newPrimaryName := resp.NewPrimaryId.Name
	standbyNames := make(map[string]bool)
	for _, s := range standbys {
		standbyNames[s.Id.Name] = true
	}
	assert.True(t, standbyNames[newPrimaryName],
		"new primary %q should be one of the standbys", newPrimaryName)

	// Status must be called on the primary to read the LSN, then EmergencyDemote
	// to stop it after standbys have caught up.
	primaryKey := topoclient.MultiPoolerIDString(primary.Id)
	callLog := fakeClient.GetCallLog()
	assert.Contains(t, callLog, fmt.Sprintf("Status(%s)", primaryKey))
	assert.Contains(t, callLog, fmt.Sprintf("EmergencyDemote(%s)", primaryKey))
}

// TestShutdownPrimary_RejectsNonPrimary verifies that calling ShutdownPrimary
// with a pooler that is not PRIMARY returns an error without touching the cluster.
func TestShutdownPrimary_RejectsNonPrimary(t *testing.T) {
	ctx := context.Background()
	engine, fakeClient, _, standbys := shutdownTestFixture(t)

	// standbys[1] is standby2, which is stored as REPLICA in the fixture.
	replica := standbys[1]
	req := &multiorchpb.ShutdownPrimaryRequest{
		Database:              replica.Database,
		TableGroup:            replica.TableGroup,
		Shard:                 replica.Shard,
		PrimaryId:             replica.Id,
		DrainTimeout:          durationpb.New(5e9),
		StandbyCatchupTimeout: durationpb.New(30e9),
	}

	_, err := engine.ShutdownPrimary(ctx, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not PRIMARY")

	// No RPCs should have been issued.
	assert.Empty(t, fakeClient.GetCallLog())
}

// TestShutdownPrimary_OldPrimaryExcludedFromElection verifies that the pooler
// requesting shutdown is excluded from the AppointLeader cohort.
func TestShutdownPrimary_OldPrimaryExcludedFromElection(t *testing.T) {
	ctx := context.Background()

	// Use a tracking fake that records AppointLeader cohorts.
	engine, _, primary, _ := shutdownTestFixture(t)
	primaryIDStr := topoclient.MultiPoolerIDString(primary.Id)

	req := &multiorchpb.ShutdownPrimaryRequest{
		Database:              primary.Database,
		TableGroup:            primary.TableGroup,
		Shard:                 primary.Shard,
		PrimaryId:             primary.Id,
		DrainTimeout:          durationpb.New(5e9),
		StandbyCatchupTimeout: durationpb.New(30e9),
	}

	resp, err := engine.ShutdownPrimary(ctx, req)
	require.NoError(t, err)

	// Verify the old primary is not returned as the new primary.
	assert.NotEqual(t, primaryIDStr, topoclient.MultiPoolerIDString(resp.NewPrimaryId),
		"old primary should not be re-elected")
}

// TestShutdownPrimary_RecoveryUnaffectedOnError verifies that a fatal step
// (EmergencyDemote) failing does not stop the recovery loop. ShutdownPrimary
// no longer disables/re-enables the recovery runner — it stays running
// throughout, so a failure must leave it in whatever state it was in before.
func TestShutdownPrimary_RecoveryUnaffectedOnError(t *testing.T) {
	ctx := context.Background()
	engine, fakeClient, primary, _ := shutdownTestFixture(t)

	// Start recovery so we can verify it remains running after a failure.
	engine.EnableRecovery()
	t.Cleanup(func() { engine.DisableRecovery() })

	// Make EmergencyDemote fail.
	primaryKey := topoclient.MultiPoolerIDString(primary.Id)
	fakeClient.Errors[primaryKey] = errors.New("simulated RPC failure")

	req := &multiorchpb.ShutdownPrimaryRequest{
		Database:              primary.Database,
		TableGroup:            primary.TableGroup,
		Shard:                 primary.Shard,
		PrimaryId:             primary.Id,
		DrainTimeout:          durationpb.New(5e9),
		StandbyCatchupTimeout: durationpb.New(30e9),
	}

	_, err := engine.ShutdownPrimary(ctx, req)
	require.Error(t, err)

	// Recovery must still be running — ShutdownPrimary must not stop it.
	assert.True(t, engine.IsRecoveryEnabled(), "recovery must remain running after ShutdownPrimary failure")
}

// TestShutdownPrimary_StandbyTimeoutDoesNotAbort verifies that a WaitForLSN
// timeout on one standby does not abort the switchover — it is best-effort.
func TestShutdownPrimary_StandbyTimeoutDoesNotAbort(t *testing.T) {
	ctx := context.Background()
	engine, fakeClient, primary, standbys := shutdownTestFixture(t)

	// Make WaitForLSN fail for standby2 only (not BeginTerm).
	standby2Key := topoclient.MultiPoolerIDString(standbys[1].Id)
	fakeClient.WaitForLSNErrors[standby2Key] = errors.New("context deadline exceeded")

	req := &multiorchpb.ShutdownPrimaryRequest{
		Database:              primary.Database,
		TableGroup:            primary.TableGroup,
		Shard:                 primary.Shard,
		PrimaryId:             primary.Id,
		DrainTimeout:          durationpb.New(5e9),
		StandbyCatchupTimeout: durationpb.New(30e9),
	}

	// Should still succeed despite one standby timing out.
	resp, err := engine.ShutdownPrimary(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.NewPrimaryId)
}

// TestShutdownPrimary_UnknownPooler verifies that an unknown primary_id returns
// NOT_FOUND.
func TestShutdownPrimary_UnknownPooler(t *testing.T) {
	ctx := context.Background()
	engine, _, primary, _ := shutdownTestFixture(t)

	unknownID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "does-not-exist",
	}
	req := &multiorchpb.ShutdownPrimaryRequest{
		Database:              primary.Database,
		TableGroup:            primary.TableGroup,
		Shard:                 primary.Shard,
		PrimaryId:             unknownID,
		DrainTimeout:          durationpb.New(5e9),
		StandbyCatchupTimeout: durationpb.New(30e9),
	}

	_, err := engine.ShutdownPrimary(ctx, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
