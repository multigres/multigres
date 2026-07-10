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

package consensus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

var promisesTestInitiatedAt = timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

// promisesTestPosition returns a PoolerPosition whose decision rule is at
// the given coordinator term, with no outstanding proposal.
func promisesTestPosition(coordinatorTerm int64, lsn string) *clustermetadatapb.PoolerPosition {
	return &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: coordinatorTerm},
		}},
		Lsn: lsn,
	}
}

func TestConsensusPromises_PersistsAcrossReload(t *testing.T) {
	dir := t.TempDir()
	ctx := withTestActionLock(t)

	first := NewConsensusPromises(dir, nil)
	_, err := first.Load()
	require.NoError(t, err)

	floor := &clustermetadatapb.LsnPosition{Lsn: "0/2000"}
	require.NoError(t, first.SetRecruitBlockedUntil(ctx, floor))

	revocation := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       5,
		AcceptedCoordinatorId:  &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator"},
		CoordinatorInitiatedAt: promisesTestInitiatedAt,
		OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
	}
	status := &clustermetadatapb.ConsensusStatus{
		TermRevocation:  first.GetInconsistentRevocation(),
		CurrentPosition: promisesTestPosition(0, "0/3000"),
	}
	require.NoError(t, first.AcceptRevocation(ctx, status, revocation))

	// Simulate a process restart: a fresh ConsensusPromises rooted at the
	// same directory must recover both promises from disk.
	second := NewConsensusPromises(dir, nil)
	term, err := second.Load()
	require.NoError(t, err)
	assert.Equal(t, int64(5), term)
	assert.Equal(t, "0/2000", second.GetRecruitBlockedUntil().GetLsn())

	reloaded, err := second.GetRevocation(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5), reloaded.GetRevokedBelowTerm())
}

func TestConsensusPromises_SetRecruitBlockedUntilDoesNotClobberRevocation(t *testing.T) {
	dir := t.TempDir()
	ctx := withTestActionLock(t)

	promises := NewConsensusPromises(dir, nil)
	_, err := promises.Load()
	require.NoError(t, err)

	revocation := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       3,
		AcceptedCoordinatorId:  &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator"},
		CoordinatorInitiatedAt: promisesTestInitiatedAt,
		OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
	}
	status := &clustermetadatapb.ConsensusStatus{
		TermRevocation:  promises.GetInconsistentRevocation(),
		CurrentPosition: promisesTestPosition(0, "0/3000"),
	}
	require.NoError(t, promises.AcceptRevocation(ctx, status, revocation))

	require.NoError(t, promises.SetRecruitBlockedUntil(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/1000"}))

	reloaded := NewConsensusPromises(dir, nil)
	term, err := reloaded.Load()
	require.NoError(t, err)
	assert.Equal(t, int64(3), term, "recording the floor must not clobber the already-persisted revocation")
	assert.Equal(t, "0/1000", reloaded.GetRecruitBlockedUntil().GetLsn())
}
