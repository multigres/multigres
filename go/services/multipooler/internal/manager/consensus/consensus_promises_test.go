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
	"google.golang.org/protobuf/proto"
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
	floorAt := func(decision, proposal int64, lsn string) *clustermetadatapb.LsnPosition {
		pos := &clustermetadatapb.RuleNumberPosition{Decision: &clustermetadatapb.RuleNumber{CoordinatorTerm: decision}}
		if proposal != 0 {
			pos.Proposal = &clustermetadatapb.RuleNumber{CoordinatorTerm: proposal}
		}
		return &clustermetadatapb.LsnPosition{Position: pos, Lsn: lsn}
	}
	tests := []struct {
		name   string
		first  *clustermetadatapb.LsnPosition
		second *clustermetadatapb.LsnPosition
		want   *clustermetadatapb.LsnPosition
	}{
		{name: "lower LSN, same rule", first: floorAt(3, 0, "0/5000"), second: floorAt(3, 0, "0/1000"), want: floorAt(3, 0, "0/5000")},
		{name: "lower rule, higher LSN", first: floorAt(3, 0, "0/5000"), second: floorAt(2, 0, "0/9000"), want: floorAt(3, 0, "0/5000")},
		{name: "higher LSN, same rule", first: floorAt(3, 0, "0/5000"), second: floorAt(3, 0, "0/9000"), want: floorAt(3, 0, "0/9000")},
		{name: "higher rule, lower LSN", first: floorAt(3, 0, "0/5000"), second: floorAt(4, 0, "0/1000"), want: floorAt(4, 0, "0/1000")},
		{name: "identical", first: floorAt(3, 0, "0/5000"), second: floorAt(3, 0, "0/5000"), want: floorAt(3, 0, "0/5000")},
		{name: "same decision, lower proposal", first: floorAt(3, 5, "0/5000"), second: floorAt(3, 4, "0/9000"), want: floorAt(3, 5, "0/5000")},
		{name: "same decision, higher proposal", first: floorAt(3, 4, "0/5000"), second: floorAt(3, 5, "0/1000"), want: floorAt(3, 5, "0/1000")},
		// LSNs are compared numerically: as strings "0/9000" > "0/10000" and "0/FFFFFFFF" > "1/0".
		{name: "LSN of different width", first: floorAt(3, 0, "0/9000"), second: floorAt(3, 0, "0/10000"), want: floorAt(3, 0, "0/10000")},
		{name: "LSN across the high word", first: floorAt(3, 0, "0/FFFFFFFF"), second: floorAt(3, 0, "1/0"), want: floorAt(3, 0, "1/0")},
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
			assert.True(t, proto.Equal(tt.want, promises.GetRecruitBlockedUntil()), "got %v, want %v", promises.GetRecruitBlockedUntil(), tt.want)

			// The kept floor must also be what a restart recovers.
			reloaded := NewConsensusPromises(dir, nil)
			_, err = reloaded.Load()
			require.NoError(t, err)
			assert.True(t, proto.Equal(tt.want, reloaded.GetRecruitBlockedUntil()), "reloaded %v, want %v", reloaded.GetRecruitBlockedUntil(), tt.want)
		})
	}
}

func TestConsensusPromises_RecruitBlockedUntilRejectsInvalidFloorBeforeWriting(t *testing.T) {
	tests := []struct {
		name  string
		first *clustermetadatapb.LsnPosition
		pos   *clustermetadatapb.LsnPosition
	}{
		{name: "unparsable LSN over a floor", first: &clustermetadatapb.LsnPosition{Lsn: "0/5000"}, pos: &clustermetadatapb.LsnPosition{Lsn: "garbage"}},
		{name: "empty LSN over a floor", first: &clustermetadatapb.LsnPosition{Lsn: "0/5000"}, pos: &clustermetadatapb.LsnPosition{}},
		{name: "unparsable LSN with no floor", pos: &clustermetadatapb.LsnPosition{Lsn: "garbage"}},
		{name: "nil position with no floor"},
		{name: "nil position over a floor", first: &clustermetadatapb.LsnPosition{Lsn: "0/5000"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := withTestActionLock(t)
			promises := NewConsensusPromises(dir, nil)
			_, err := promises.Load()
			require.NoError(t, err)
			if tt.first != nil {
				require.NoError(t, promises.EnsureRecruitBlockedUntilAtLeast(ctx, tt.first))
			}

			require.ErrorContains(t, promises.EnsureRecruitBlockedUntilAtLeast(ctx, tt.pos), "invalid recruit position floor")

			assert.True(t, proto.Equal(tt.first, promises.GetRecruitBlockedUntil()), "a rejected update must keep the existing floor")
			reloaded := NewConsensusPromises(dir, nil)
			_, err = reloaded.Load()
			require.NoError(t, err)
			assert.True(t, proto.Equal(tt.first, reloaded.GetRecruitBlockedUntil()), "a rejected update must not reach disk")
		})
	}
}

func TestConsensusPromises_RecruitBlockedUntilKeepsUnparsableStoredFloor(t *testing.T) {
	ctx := withTestActionLock(t)
	promises := NewConsensusPromises(t.TempDir(), nil)
	_, err := promises.Load()
	require.NoError(t, err)
	// A mangled promise can't be fulfilled or compared, so it must not be replaced.
	stored := &clustermetadatapb.LsnPosition{Lsn: "garbage"}
	promises.recruitBlockedUntil = stored

	err = promises.EnsureRecruitBlockedUntilAtLeast(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/5000"})

	require.ErrorContains(t, err, "failed to compare recruit position floors")
	assert.True(t, proto.Equal(stored, promises.GetRecruitBlockedUntil()))
}

func TestConsensusPromises_RecruitBlockedUntilLowerFloorWritesNothing(t *testing.T) {
	dir := t.TempDir()
	ctx := withTestActionLock(t)
	promises := NewConsensusPromises(dir, nil)
	_, err := promises.Load()
	require.NoError(t, err)
	require.NoError(t, promises.EnsureRecruitBlockedUntilAtLeast(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/5000"}))

	// With the directory read-only, any write would fail.
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	require.NoError(t, promises.EnsureRecruitBlockedUntilAtLeast(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/1000"}))
	assert.Equal(t, "0/5000", promises.GetRecruitBlockedUntil().GetLsn())
}

func TestConsensusPromises_RecruitBlockedUntilFailedRaiseKeepsFloor(t *testing.T) {
	dir := t.TempDir()
	ctx := withTestActionLock(t)
	promises := NewConsensusPromises(dir, nil)
	_, err := promises.Load()
	require.NoError(t, err)
	require.NoError(t, promises.EnsureRecruitBlockedUntilAtLeast(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/5000"}))

	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	err = promises.EnsureRecruitBlockedUntilAtLeast(ctx, &clustermetadatapb.LsnPosition{Lsn: "0/9000"})
	require.ErrorContains(t, err, "failed to save recruit position floor")
	assert.Equal(t, "0/5000", promises.GetRecruitBlockedUntil().GetLsn(), "a failed save must not raise the in-memory floor")
}
