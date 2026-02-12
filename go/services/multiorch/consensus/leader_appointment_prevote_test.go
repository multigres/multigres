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

package consensus

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// createPoolerForPreVote creates a pooler with specific health state and consensus term for testing
func createPoolerForPreVote(name string, isHealthy bool, termNumber int64, lastAcceptanceTime *time.Time, acceptedFrom *clustermetadatapb.ID) *multiorchdatapb.PoolerHealthState {
	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      name,
	}

	pooler := &clustermetadatapb.MultiPooler{
		Id:         poolerID,
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
		Hostname:   "localhost",
		PortMap: map[string]int32{
			"grpc": 9000,
		},
	}

	var consensusTerm *multipoolermanagerdatapb.ConsensusTerm
	isInitialized := false
	if termNumber > 0 {
		consensusTerm = &multipoolermanagerdatapb.ConsensusTerm{
			TermNumber:                    termNumber,
			AcceptedTermFromCoordinatorId: acceptedFrom,
		}
		if lastAcceptanceTime != nil {
			consensusTerm.LastAcceptanceTime = timestamppb.New(*lastAcceptanceTime)
		}
		isInitialized = true
	}

	return &multiorchdatapb.PoolerHealthState{
		MultiPooler:       pooler,
		IsLastCheckValid:  isHealthy,
		ConsensusTerm:     consensusTerm,
		IsInitialized:     isInitialized,
		IsPostgresRunning: isHealthy && isInitialized, // postgres is running if healthy and initialized
	}
}

// setupDurabilityPolicyForPreVote sets up durability policy response for FakeClient
func setupDurabilityPolicyForPreVote(fakeClient *rpcclient.FakeClient, poolerKey string, requiredCount int32) {
	fakeClient.GetDurabilityPolicyResponses[poolerKey] = &multipoolermanagerdatapb.GetDurabilityPolicyResponse{
		Policy: &clustermetadatapb.DurabilityPolicy{
			PolicyName: "ANY_2",
			QuorumRule: &clustermetadatapb.QuorumRule{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
				RequiredCount: requiredCount,
				Description:   "Any 2 poolers",
			},
		},
	}
}

func TestPreVote(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx := context.Background()

	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("passes when enough healthy poolers and no recent acceptances", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		coord := NewCoordinator(coordID, nil, fakeClient, logger)

		// Create 3 healthy poolers with no recent acceptances
		oldTime := time.Now().Add(-20 * time.Second)
		cohort := []*multiorchdatapb.PoolerHealthState{
			createPoolerForPreVote("mp1", true /* isHealthy */, 5 /* termNumber */, &oldTime, coordID),
			createPoolerForPreVote("mp2", true /* isHealthy */, 5 /* termNumber */, &oldTime, coordID),
			createPoolerForPreVote("mp3", true /* isHealthy */, 5 /* termNumber */, &oldTime, coordID),
		}

		// Setup durability policy
		poolerKey := topoclient.MultiPoolerIDString(cohort[0].MultiPooler.Id)
		setupDurabilityPolicyForPreVote(fakeClient, poolerKey, 2)

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
		}
		proposedTerm := int64(6)

		canProceed, reason := coord.preVote(ctx, cohort, quorumRule, proposedTerm)

		require.True(t, canProceed, "should allow election when no recent acceptances")
		require.Empty(t, reason)
	})

	t.Run("backs off when recent term acceptance detected", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		coord := NewCoordinator(coordID, nil, fakeClient, logger)

		// One pooler accepted a term very recently (2 seconds ago)
		recentTime := time.Now().Add(-2 * time.Second)
		otherCoordID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      "other-cell",
			Name:      "other-coordinator",
		}

		cohort := []*multiorchdatapb.PoolerHealthState{
			createPoolerForPreVote("mp1", true /* isHealthy */, 10 /* termNumber */, &recentTime, otherCoordID),
			createPoolerForPreVote("mp2", true /* isHealthy */, 8 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
			createPoolerForPreVote("mp3", true /* isHealthy */, 8 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
		}

		// Setup durability policy
		poolerKey := topoclient.MultiPoolerIDString(cohort[0].MultiPooler.Id)
		setupDurabilityPolicyForPreVote(fakeClient, poolerKey, 2)

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
		}
		proposedTerm := int64(11)

		canProceed, reason := coord.preVote(ctx, cohort, quorumRule, proposedTerm)

		require.False(t, canProceed, "should back off when recent acceptance detected")
		require.Contains(t, reason, "another coordinator started election recently")
	})

	t.Run("fails when insufficient healthy poolers for quorum", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		coord := NewCoordinator(coordID, nil, fakeClient, logger)

		// Only 1 healthy pooler out of 3 (need 2 for quorum)
		cohort := []*multiorchdatapb.PoolerHealthState{
			createPoolerForPreVote("mp1", true /* isHealthy */, 5 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
			createPoolerForPreVote("mp2", false /* isHealthy */, 5 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
			createPoolerForPreVote("mp3", false /* isHealthy */, 5 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
		}

		// Setup durability policy requiring 2 poolers
		poolerKey := topoclient.MultiPoolerIDString(cohort[0].MultiPooler.Id)
		setupDurabilityPolicyForPreVote(fakeClient, poolerKey, 2)

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
		}
		proposedTerm := int64(6)

		canProceed, reason := coord.preVote(ctx, cohort, quorumRule, proposedTerm)

		require.False(t, canProceed, "should fail when insufficient healthy poolers")
		require.Contains(t, reason, "insufficient healthy initialized poolers for quorum")
	})

	t.Run("fails when poolers have no consensus term info", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		coord := NewCoordinator(coordID, nil, fakeClient, logger)

		// Poolers with no consensus term (uninitialized)
		cohort := []*multiorchdatapb.PoolerHealthState{
			createPoolerForPreVote("mp1", true /* isHealthy */, 0 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
			createPoolerForPreVote("mp2", true /* isHealthy */, 0 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
			createPoolerForPreVote("mp3", true /* isHealthy */, 0 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
		}

		// Setup durability policy
		poolerKey := topoclient.MultiPoolerIDString(cohort[0].MultiPooler.Id)
		setupDurabilityPolicyForPreVote(fakeClient, poolerKey, 2)

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
		}
		proposedTerm := int64(1)

		canProceed, reason := coord.preVote(ctx, cohort, quorumRule, proposedTerm)

		require.False(t, canProceed, "should block election for uninitialized poolers")
		require.Contains(t, reason, "insufficient healthy initialized poolers for quorum")
	})

	t.Run("passes when only unhealthy poolers have recent acceptances", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		coord := NewCoordinator(coordID, nil, fakeClient, logger)

		// Unhealthy pooler has recent acceptance, but we ignore it
		recentTime := time.Now().Add(-2 * time.Second)
		otherCoordID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      "other-cell",
			Name:      "other-coordinator",
		}

		cohort := []*multiorchdatapb.PoolerHealthState{
			createPoolerForPreVote("mp1", true /* isHealthy */, 5 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
			createPoolerForPreVote("mp2", true /* isHealthy */, 5 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
			createPoolerForPreVote("mp3", false /* isHealthy */, 10 /* termNumber */, &recentTime, otherCoordID), // unhealthy with recent acceptance
		}

		// Setup durability policy
		poolerKey := topoclient.MultiPoolerIDString(cohort[0].MultiPooler.Id)
		setupDurabilityPolicyForPreVote(fakeClient, poolerKey, 2)

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
		}
		proposedTerm := int64(6)

		canProceed, reason := coord.preVote(ctx, cohort, quorumRule, proposedTerm)

		require.True(t, canProceed, "should ignore unhealthy poolers with recent acceptances")
		require.Empty(t, reason)
	})

	t.Run("passes with valid quorum rule", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		coord := NewCoordinator(coordID, nil, fakeClient, logger)

		cohort := []*multiorchdatapb.PoolerHealthState{
			createPoolerForPreVote("mp1", true /* isHealthy */, 5 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
			createPoolerForPreVote("mp2", true /* isHealthy */, 5 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */),
		}

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
		}
		proposedTerm := int64(6)

		canProceed, reason := coord.preVote(ctx, cohort, quorumRule, proposedTerm)

		require.True(t, canProceed, "should proceed with valid quorum rule")
		require.Empty(t, reason)
	})

	t.Run("fails when postgres is not running on poolers", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		coord := NewCoordinator(coordID, nil, fakeClient, logger)

		// Create 3 poolers: 2 healthy but with postgres not running, 1 healthy with postgres running
		pooler1 := createPoolerForPreVote("mp1", true /* isHealthy */, 5 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */)
		pooler1.IsPostgresRunning = false // postgres not running

		pooler2 := createPoolerForPreVote("mp2", true /* isHealthy */, 5 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */)
		pooler2.IsPostgresRunning = false // postgres not running

		pooler3 := createPoolerForPreVote("mp3", true /* isHealthy */, 5 /* termNumber */, nil /* lastAcceptanceTime */, nil /* acceptedFrom */)
		// pooler3 has postgres running (default from helper)

		cohort := []*multiorchdatapb.PoolerHealthState{pooler1, pooler2, pooler3}

		// Setup durability policy requiring 2 poolers
		poolerKey := topoclient.MultiPoolerIDString(cohort[0].MultiPooler.Id)
		setupDurabilityPolicyForPreVote(fakeClient, poolerKey, 2)

		quorumRule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
		}
		proposedTerm := int64(6)

		canProceed, reason := coord.preVote(ctx, cohort, quorumRule, proposedTerm)

		require.False(t, canProceed, "should fail when insufficient poolers have postgres running")
		require.Contains(t, reason, "insufficient healthy initialized poolers for quorum")
	})
}
