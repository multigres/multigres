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

package manager

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/executor/mock"
)

// newTestSSM creates a postgresqlSyncStandbyManager backed by the given mock query service.
func newTestSSM(mockQS *mock.QueryService) *postgresqlSyncStandbyManager {
	return newSyncStandbyManager(slog.New(slog.NewTextHandler(io.Discard, nil)), mockQS, nil)
}

// withTestActionLock returns a context that satisfies AssertActionLockHeld.
func withTestActionLock(t *testing.T) context.Context {
	t.Helper()
	lock := NewActionLock()
	ctx, err := lock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	t.Cleanup(func() { lock.Release(ctx) })
	return ctx
}

// ssmTestCohort returns a single standby ID for use in sync standby manager tests.
func ssmTestCohort() []*clustermetadatapb.ID {
	return []*clustermetadatapb.ID{{Cell: "zone1", Name: "standby-1"}}
}

// ssmTestPolicyWithCohort returns a PolicyWithCohort that produces:
//
//	synchronous_commit = 'on'
//	synchronous_standby_names = 'ANY 1 ("zone1_standby-1")'
func ssmTestPolicyWithCohort() commonconsensus.PolicyWithCohort {
	return commonconsensus.PolicyWithCohort{
		Policy: commonconsensus.AtLeastNPolicy{N: 2},
		Cohort: ssmTestCohort(),
	}
}

func addSetPolicyExpectations(m *mock.QueryService) {
	m.AddQueryPatternOnce("ALTER SYSTEM SET synchronous_commit", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("ALTER SYSTEM SET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
}

func TestSetPolicy_SkipsQueriesWhenCached(t *testing.T) {
	mockQS := mock.NewQueryService()
	ssm := newTestSSM(mockQS)

	// Pre-seed cache with the exact values ssmTestPolicy() + ssmTestCohort() would produce.
	ssm.lastSyncCommit = "on"
	ssm.lastStandbyNames = `ANY 1 ("zone1_standby-1")`

	ctx := withPriorRuleWritesDrained(withTestActionLock(t))
	err := ssm.SetPolicy(ctx, ssmTestPolicyWithCohort())
	require.NoError(t, err)
	// No queries should have been issued — cache already held the correct values.
	assert.NoError(t, mockQS.ExpectationsWereMet())
}

func TestSetPolicy_AppliesWhenCacheMisses(t *testing.T) {
	mockQS := mock.NewQueryService()
	ssm := newTestSSM(mockQS)
	addSetPolicyExpectations(mockQS)

	ctx := withPriorRuleWritesDrained(withTestActionLock(t))
	require.NoError(t, ssm.SetPolicy(ctx, ssmTestPolicyWithCohort()))
	require.NoError(t, mockQS.ExpectationsWereMet())

	assert.Equal(t, "on", ssm.lastSyncCommit)
	assert.Equal(t, `ANY 1 ("zone1_standby-1")`, ssm.lastStandbyNames)
}

func TestSetPolicy_SecondCallHitsCache(t *testing.T) {
	mockQS := mock.NewQueryService()
	ssm := newTestSSM(mockQS)

	// First call writes; second call hits cache and issues no queries.
	addSetPolicyExpectations(mockQS)
	ctx := withPriorRuleWritesDrained(withTestActionLock(t))
	require.NoError(t, ssm.SetPolicy(ctx, ssmTestPolicyWithCohort()))
	require.NoError(t, mockQS.ExpectationsWereMet())

	// Second call: no new expectations, no queries should be issued.
	require.NoError(t, ssm.SetPolicy(ctx, ssmTestPolicyWithCohort()))
	assert.NoError(t, mockQS.ExpectationsWereMet())
}

func TestSetPolicy_ErrorLeavesCache(t *testing.T) {
	mockQS := mock.NewQueryService()
	ssm := newTestSSM(mockQS)
	mockQS.AddQueryPatternOnceWithError("ALTER SYSTEM SET synchronous_commit", errors.New("postgres is down"))

	ctx := withPriorRuleWritesDrained(withTestActionLock(t))
	err := ssm.SetPolicy(ctx, ssmTestPolicyWithCohort())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres is down")
	require.NoError(t, mockQS.ExpectationsWereMet())

	// Cache must still be empty so the next call re-applies.
	assert.Empty(t, ssm.lastSyncCommit)
	assert.Empty(t, ssm.lastStandbyNames)
}

func TestClear_ResetsAndInvalidatesCache(t *testing.T) {
	mockQS := mock.NewQueryService()
	ssm := newTestSSM(mockQS)
	mockQS.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	mockQS.AddQueryPatternOnce("ALTER SYSTEM RESET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
	mockQS.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))

	// Pre-seed cache to verify it gets cleared.
	ssm.lastSyncCommit = "on"
	ssm.lastStandbyNames = `ANY 1 ("zone1_standby-1")`

	require.NoError(t, ssm.Clear(withTestActionLock(t)))
	require.NoError(t, mockQS.ExpectationsWereMet())

	assert.Empty(t, ssm.lastSyncCommit)
	assert.Empty(t, ssm.lastStandbyNames)
}

func TestClear_RefusedOnPrimary(t *testing.T) {
	mockQS := mock.NewQueryService()
	ssm := newTestSSM(mockQS)
	mockQS.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))

	err := ssm.Clear(withTestActionLock(t))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in recovery mode")
	require.NoError(t, mockQS.ExpectationsWereMet())
}

func TestClear_ResetFails(t *testing.T) {
	mockQS := mock.NewQueryService()
	ssm := newTestSSM(mockQS)
	mockQS.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	mockQS.AddQueryPatternOnceWithError("ALTER SYSTEM RESET synchronous_standby_names", errors.New("permission denied"))

	err := ssm.Clear(withTestActionLock(t))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")
	require.NoError(t, mockQS.ExpectationsWereMet())
}

func TestClear_ReloadFails(t *testing.T) {
	mockQS := mock.NewQueryService()
	ssm := newTestSSM(mockQS)
	mockQS.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	mockQS.AddQueryPatternOnce("ALTER SYSTEM RESET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
	mockQS.AddQueryPatternOnceWithError("SELECT pg_reload_conf", errors.New("reload failed"))

	err := ssm.Clear(withTestActionLock(t))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reload failed")
	require.NoError(t, mockQS.ExpectationsWereMet())
}

func TestSetPolicy_RequiresActionLock(t *testing.T) {
	ssm := newTestSSM(mock.NewQueryService())
	ctx := withPriorRuleWritesDrained(t.Context())
	err := ssm.SetPolicy(ctx, ssmTestPolicyWithCohort())
	assert.EqualError(t, err, "SetPolicy: context does not hold an action lock")
}

func TestSetPolicy_RequiresPriorRuleWritesDrained(t *testing.T) {
	ssm := newTestSSM(mock.NewQueryService())
	err := ssm.SetPolicy(withTestActionLock(t), ssmTestPolicyWithCohort())
	assert.EqualError(t, err, "SetPolicy: SetPolicy requires prior rule writes to be drained (call readCurrentRuleLocked first)")
}

func TestNeedsApply_TrueWhenCacheCold(t *testing.T) {
	ssm := newTestSSM(mock.NewQueryService())
	needs, err := ssm.NeedsApply(ssmTestPolicyWithCohort())
	require.NoError(t, err)
	assert.True(t, needs)
}

func TestNeedsApply_FalseWhenCacheWarm(t *testing.T) {
	ssm := newTestSSM(mock.NewQueryService())
	ssm.lastSyncCommit = "on"
	ssm.lastStandbyNames = `ANY 1 ("zone1_standby-1")`
	needs, err := ssm.NeedsApply(ssmTestPolicyWithCohort())
	require.NoError(t, err)
	assert.False(t, needs)
}

func TestSyncCommitString(t *testing.T) {
	tests := []struct {
		level multipoolermanagerdatapb.SynchronousCommitLevel
		want  string
	}{
		{multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF, "off"},
		{multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL, "local"},
		{multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE, "remote_write"},
		{multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON, "on"},
		{multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY, "remote_apply"},
	}
	for _, tt := range tests {
		got, err := syncCommitString(tt.level)
		require.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}

	_, err := syncCommitString(multipoolermanagerdatapb.SynchronousCommitLevel(999))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown synchronous_commit level")
}
