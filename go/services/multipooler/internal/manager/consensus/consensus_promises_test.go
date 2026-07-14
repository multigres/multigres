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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/constants"
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
		TermRevocation:  first.GetInconsistent().GetTermRevocation(),
		CurrentPosition: promisesTestPosition(0, "0/3000"),
	}
	require.NoError(t, first.AcceptRevocation(ctx, status, revocation))

	// Simulate a process restart: a fresh ConsensusPromises rooted at the
	// same directory must recover both promises from disk.
	second := NewConsensusPromises(dir, nil)
	term, err := second.Load()
	require.NoError(t, err)
	assert.Equal(t, int64(5), term)
	assert.Equal(t, "0/2000", second.GetInconsistent().GetRecruitBlockedUntil().GetLsn())

	reloadedPromises, err := second.GetConsistent(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5), reloadedPromises.GetTermRevocation().GetRevokedBelowTerm())
	assert.Equal(t, "0/3000", reloadedPromises.GetRecruitObservedLsn(),
		"AcceptRevocation must snapshot status.CurrentPosition.Lsn as the recruit-observed baseline")
}

// TestConsensusPromises_AcceptRevocationOverwritesObservedLsnOnRetry verifies
// that a retried Recruit (idempotent re-accept of the same revocation)
// re-snapshots the latest observed LSN rather than keeping the first one —
// the invariant checked by Promote/SetPrimary is "since the last accept, has
// the position moved further," not "since the very first accept."
func TestConsensusPromises_AcceptRevocationOverwritesObservedLsnOnRetry(t *testing.T) {
	dir := t.TempDir()
	ctx := withTestActionLock(t)

	promises := NewConsensusPromises(dir, nil)
	_, err := promises.Load()
	require.NoError(t, err)

	revocation := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       5,
		AcceptedCoordinatorId:  &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator"},
		CoordinatorInitiatedAt: promisesTestInitiatedAt,
		OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
	}
	firstStatus := &clustermetadatapb.ConsensusStatus{
		TermRevocation:  promises.GetInconsistent().GetTermRevocation(),
		CurrentPosition: promisesTestPosition(0, "0/1000"),
	}
	require.NoError(t, promises.AcceptRevocation(ctx, firstStatus, revocation))
	got, err := promises.GetConsistent(ctx)
	require.NoError(t, err)
	assert.Equal(t, "0/1000", got.GetRecruitObservedLsn())

	// Idempotent retry: same revocation, but replay had progressed further
	// before this attempt's pause.
	retryStatus := &clustermetadatapb.ConsensusStatus{
		TermRevocation:  promises.GetInconsistent().GetTermRevocation(),
		CurrentPosition: promisesTestPosition(0, "0/2000"),
	}
	require.NoError(t, promises.AcceptRevocation(ctx, retryStatus, revocation))
	got, err = promises.GetConsistent(ctx)
	require.NoError(t, err)
	assert.Equal(t, "0/2000", got.GetRecruitObservedLsn(), "retry must re-snapshot the latest observed LSN")
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
		TermRevocation:  promises.GetInconsistent().GetTermRevocation(),
		CurrentPosition: promisesTestPosition(0, "0/3000"),
	}
	require.NoError(t, promises.AcceptRevocation(ctx, status, revocation))

	require.NoError(t, promises.SetRecruitBlockedUntil(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/1000"}))

	reloaded := NewConsensusPromises(dir, nil)
	term, err := reloaded.Load()
	require.NoError(t, err)
	assert.Equal(t, int64(3), term, "recording the floor must not clobber the already-persisted revocation")
	assert.Equal(t, "0/1000", reloaded.GetInconsistent().GetRecruitBlockedUntil().GetLsn())
}

func TestConsensusPromises_LoadWithCorruptFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, constants.ConsensusPromisesFile), []byte("not json"), 0o644))

	promises := NewConsensusPromises(dir, nil)
	_, err := promises.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load consensus promises")
}

func TestConsensusPromises_SetRecruitBlockedUntilFilesystemReadOnly(t *testing.T) {
	dir := t.TempDir()
	ctx := withTestActionLock(t)

	promises := NewConsensusPromises(dir, nil)
	_, err := promises.Load()
	require.NoError(t, err)

	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	err = promises.SetRecruitBlockedUntil(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/1000"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save recruit position floor")
	assert.Nil(t, promises.GetInconsistent().GetRecruitBlockedUntil(), "a failed save must not update in-memory state")
}

func TestConsensusPromises_SetRecruitBlockedUntilRequiresActionLock(t *testing.T) {
	promises := NewConsensusPromises(t.TempDir(), nil)
	err := promises.SetRecruitBlockedUntil(t.Context(), &clustermetadatapb.LsnPosition{Lsn: "0/1000"})
	require.Error(t, err)
	assert.Nil(t, promises.GetInconsistent().GetRecruitBlockedUntil())
}
