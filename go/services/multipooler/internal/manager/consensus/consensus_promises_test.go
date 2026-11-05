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
	require.NoError(t, first.EnsureRecruitBlockedUntilAtLeast(ctx, floor))

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

func TestConsensusPromises_EnsureRecruitBlockedUntilAtLeastDoesNotClobberRevocation(t *testing.T) {
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

	require.NoError(t, promises.EnsureRecruitBlockedUntilAtLeast(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/1000"}))

	reloaded := NewConsensusPromises(dir, nil)
	term, err := reloaded.Load()
	require.NoError(t, err)
	assert.Equal(t, int64(3), term, "recording the floor must not clobber the already-persisted revocation")
	assert.Equal(t, "0/1000", reloaded.GetRecruitBlockedUntil().GetLsn())
}

func TestConsensusPromises_LoadWithCorruptFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, constants.ConsensusPromisesFile), []byte("not json"), 0o644))

	promises := NewConsensusPromises(dir, nil)
	_, err := promises.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load consensus promises")
}

func TestConsensusPromises_LoadIgnoresUnknownFields(t *testing.T) {
	dir := t.TempDir()
	data := `{"recruitBlockedUntil": {"lsn": "0/1000"}, "fieldFromNewerBinary": {"x": 1}}`
	require.NoError(t, os.WriteFile(filepath.Join(dir, constants.ConsensusPromisesFile), []byte(data), 0o644))

	promises := NewConsensusPromises(dir, nil)
	_, err := promises.Load()
	require.NoError(t, err)
	assert.Equal(t, "0/1000", promises.GetRecruitBlockedUntil().GetLsn())
}

func TestConsensusPromises_EnsureRecruitBlockedUntilAtLeastFilesystemReadOnly(t *testing.T) {
	dir := t.TempDir()
	ctx := withTestActionLock(t)

	promises := NewConsensusPromises(dir, nil)
	_, err := promises.Load()
	require.NoError(t, err)

	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	err = promises.EnsureRecruitBlockedUntilAtLeast(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/1000"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save recruit position floor")
	assert.Nil(t, promises.GetRecruitBlockedUntil(), "a failed save must not update in-memory state")
}

func TestConsensusPromises_EnsureRecruitBlockedUntilAtLeastRequiresActionLock(t *testing.T) {
	promises := NewConsensusPromises(t.TempDir(), nil)
	err := promises.EnsureRecruitBlockedUntilAtLeast(t.Context(), &clustermetadatapb.LsnPosition{Lsn: "0/1000"})
	require.Error(t, err)
	assert.Nil(t, promises.GetRecruitBlockedUntil())
}

func TestConsensusPromises_RecruitBlockedUntilNeverLowers(t *testing.T) {
	floorAt := func(term int64, lsn string) *clustermetadatapb.LsnPosition {
		return &clustermetadatapb.LsnPosition{
			Position: &clustermetadatapb.RuleNumberPosition{Decision: &clustermetadatapb.RuleNumber{CoordinatorTerm: term}},
			Lsn:      lsn,
		}
	}
	tests := []struct {
		name     string
		first    *clustermetadatapb.LsnPosition
		second   *clustermetadatapb.LsnPosition
		wantTerm int64
		wantLSN  string
	}{
		{name: "lower LSN, same rule", first: floorAt(3, "0/5000"), second: floorAt(3, "0/1000"), wantTerm: 3, wantLSN: "0/5000"},
		{name: "lower rule, higher LSN", first: floorAt(3, "0/5000"), second: floorAt(2, "0/9000"), wantTerm: 3, wantLSN: "0/5000"},
		{name: "higher LSN, same rule", first: floorAt(3, "0/5000"), second: floorAt(3, "0/9000"), wantTerm: 3, wantLSN: "0/9000"},
		{name: "higher rule, lower LSN", first: floorAt(3, "0/5000"), second: floorAt(4, "0/1000"), wantTerm: 4, wantLSN: "0/1000"},
		{name: "identical", first: floorAt(3, "0/5000"), second: floorAt(3, "0/5000"), wantTerm: 3, wantLSN: "0/5000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := withTestActionLock(t)
			promises := NewConsensusPromises(dir, nil)
			_, err := promises.Load()
			require.NoError(t, err)

			require.NoError(t, promises.EnsureRecruitBlockedUntilAtLeast(ctx, tt.first))
			require.NoError(t, promises.EnsureRecruitBlockedUntilAtLeast(ctx, tt.second))

			got := promises.GetRecruitBlockedUntil()
			assert.Equal(t, tt.wantTerm, got.GetPosition().GetDecision().GetCoordinatorTerm())
			assert.Equal(t, tt.wantLSN, got.GetLsn())

			// The kept floor must also be what a restart recovers.
			reloaded := NewConsensusPromises(dir, nil)
			_, err = reloaded.Load()
			require.NoError(t, err)
			assert.Equal(t, tt.wantTerm, reloaded.GetRecruitBlockedUntil().GetPosition().GetDecision().GetCoordinatorTerm())
			assert.Equal(t, tt.wantLSN, reloaded.GetRecruitBlockedUntil().GetLsn())
		})
	}
}

func TestConsensusPromises_RecruitBlockedUntilRejectsUnparsableLSNOverFloor(t *testing.T) {
	ctx := withTestActionLock(t)
	promises := NewConsensusPromises(t.TempDir(), nil)
	_, err := promises.Load()
	require.NoError(t, err)

	require.NoError(t, promises.EnsureRecruitBlockedUntilAtLeast(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/5000"}))
	err = promises.EnsureRecruitBlockedUntilAtLeast(ctx, &clustermetadatapb.LsnPosition{Lsn: "garbage"})

	require.ErrorContains(t, err, "failed to compare recruit position floors")
	assert.Equal(t, "0/5000", promises.GetRecruitBlockedUntil().GetLsn(), "a rejected update must keep the existing floor")
}
