// Copyright 2026 Supabase, Inc.
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
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/heartbeat"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// newProofRecorder returns a rule-store hook that records every LSN it is given
// and writes it through a real heartbeat writer, as the multipooler wires it.
func newProofRecorder(t *testing.T, writer *heartbeat.Writer) (hook func(context.Context, pgutil.LSN, time.Time), lsns *[]string) {
	t.Helper()
	var got []string
	return func(ctx context.Context, lsn pgutil.LSN, startedAt time.Time) {
		got = append(got, lsn.String())
		require.NoError(t, writer.RecordProven(ctx, lsn, startedAt))
	}, &got
}

func newHeartbeatWriterForTest(ctx context.Context, t *testing.T) (*heartbeat.Writer, func(string) string) {
	t.Helper()
	conn, err := pgTestFixture.newClientConn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Mirrors the manager's createHeartbeatTable.
	_, err = conn.Query(ctx, `CREATE TABLE IF NOT EXISTS multigres.heartbeat (
		shard_id BYTEA PRIMARY KEY,
		leader_id TEXT NOT NULL,
		ts BIGINT NOT NULL,
		quorum_commit_lsn pg_lsn,
		quorum_commit_ts TIMESTAMPTZ
	)`)
	require.NoError(t, err)
	_, err = conn.Query(ctx, `DELETE FROM multigres.heartbeat`)
	require.NoError(t, err)

	writer := heartbeat.NewWriter(&connQueryService{conn: conn}, slog.New(slog.DiscardHandler), []byte("0"), "pooler-1", 1000)
	query := func(sql string) string {
		t.Helper()
		results, err := conn.QueryArgs(ctx, sql)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Len(t, results[0].Rows, 1)
		return string(results[0].Rows[0].Values[0])
	}
	return writer, query
}

// TestRuleStorePG_DecideRecordsProofInHeartbeat: deciding a rule records its
// quorum-acked LSN in the heartbeat row, naming the new leader, and leaves the
// connection's synchronous_commit unchanged.
func TestRuleStorePG_DecideRecordsProofInHeartbeat(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()
	writer, query := newHeartbeatWriterForTest(ctx, t)
	commitModeBefore := query(`SHOW synchronous_commit`)
	var lsns *[]string
	rs.onProposalAcked, lsns = newProofRecorder(t, writer)

	_, err := rs.UpdateRule(ctx, NewRuleUpdate(1, testPoolerID(t, "zone1", "coordinator-1"), "promotion", "initial primary", time.Now()))
	require.NoError(t, err)

	require.Len(t, *lsns, 1)
	assert.NotEmpty(t, (*lsns)[0])
	assert.Equal(t, "pooler-1", query(`SELECT leader_id FROM multigres.heartbeat`))
	assert.Equal(t, (*lsns)[0], query(`SELECT quorum_commit_lsn::text FROM multigres.heartbeat`),
		"the row carries the proposal's quorum-acked LSN")
	assert.Equal(t, "true", query(`SELECT (quorum_commit_ts IS NOT NULL)::text FROM multigres.heartbeat`))
	assert.Equal(t, commitModeBefore, query(`SHOW synchronous_commit`),
		"the forced write must not leak its commit mode into the connection")
}

// TestRuleStorePG_PropagationReportsConfirmedLSNToHook: when a promotion first
// finishes deciding a stuck proposal, both decisions report a real LSN.
func TestRuleStorePG_PropagationReportsConfirmedLSNToHook(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()
	writer, _ := newHeartbeatWriterForTest(ctx, t)
	var lsns *[]string
	rs.onProposalAcked, lsns = newProofRecorder(t, writer)

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	leaderID := testPoolerID(t, "zone1", "leader-1")
	cohort := []*clustermetadatapb.ID{testPoolerID(t, "zone1", "member-1"), testPoolerID(t, "zone1", "member-2")}
	_, err := rs.UpdateRule(ctx, NewRuleUpdate(3, coordinatorID, "promotion", "bootstrap", time.Now()).
		WithLeader(leaderID).WithCohort(cohort))
	require.NoError(t, err)
	seedStuckProposal(ctx, t, conn, 4, leaderID, cohort, testBootstrapPolicy())
	*lsns = nil // only the propagation below matters

	_, err = rs.UpdateRule(ctx, NewRuleUpdate(5, coordinatorID, "promotion", "auto-recover stuck proposal", time.Now()).
		WithLeader(testPoolerID(t, "zone1", "leader-3")).WithCohort(cohort).
		WithPromotionHook(func(context.Context) error { return nil }))
	require.NoError(t, err)

	require.Len(t, *lsns, 2, "one report for the finished stuck proposal, one for the new promotion")
	for _, lsn := range *lsns {
		assert.NotEmpty(t, lsn)
	}
}

// confirmWriteCounter counts, and can fail, the no-op writes that confirm a proposal's
// quorum (propagation's own, and the proof refresh).
type confirmWriteCounter struct {
	*connQueryService
	confirms int
	failWith error
}

func (c *confirmWriteCounter) QueryAdminArgs(ctx context.Context, query string, args ...any) (*sqltypes.Result, error) {
	if strings.Contains(query, "SET proposal_created_at = proposal_created_at") {
		c.confirms++
		if c.failWith != nil {
			return nil, c.failWith
		}
	}
	return c.connQueryService.QueryAdminArgs(ctx, query, args...)
}

// propagate runs an ordinary promotion over a stuck proposal, so it first finishes
// deciding the stuck one and then decides its own rule. It returns the counter's confirm
// count for just that promotion.
func propagate(ctx context.Context, t *testing.T, proofRefreshAfter time.Duration, confirmErr error) (confirms int, err error) {
	t.Helper()
	resetRuleStoreTables(ctx, t)
	conn, connErr := pgTestFixture.newClientConn(ctx)
	require.NoError(t, connErr)
	t.Cleanup(func() { _ = conn.Close() })
	counter := &confirmWriteCounter{connQueryService: &connQueryService{conn: conn}}
	rs := NewRuleStore(slog.New(slog.DiscardHandler), counter, noopSyncStandbyManager{})
	rs.proofRefreshAfter = proofRefreshAfter

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	leaderID := testPoolerID(t, "zone1", "leader-1")
	cohort := []*clustermetadatapb.ID{testPoolerID(t, "zone1", "member-1"), testPoolerID(t, "zone1", "member-2")}
	_, err = rs.UpdateRule(ctx, NewRuleUpdate(3, coordinatorID, "promotion", "bootstrap", time.Now()).
		WithLeader(leaderID).WithCohort(cohort))
	require.NoError(t, err)
	seedStuckProposal(ctx, t, conn, 4, leaderID, cohort, testBootstrapPolicy())
	counter.confirms, counter.failWith = 0, confirmErr

	_, err = rs.UpdateRule(ctx, NewRuleUpdate(5, coordinatorID, "promotion", "auto-recover stuck proposal", time.Now()).
		WithLeader(testPoolerID(t, "zone1", "leader-3")).WithCohort(cohort).
		WithPromotionHook(func(context.Context) error { return nil }))
	return counter.confirms, err
}

// TestRuleStorePG_PropagationRefreshesAnOldProofBeforeEachDecision: propagation always
// confirms the stuck proposal once; when proofs count as old, each of the two decisions
// (the stuck proposal's and the new rule's) is also preceded by a refresh write.
func TestRuleStorePG_PropagationRefreshesAnOldProofBeforeEachDecision(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)

	confirms, err := propagate(ctx, t, time.Hour, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, confirms, "recent proofs are not refreshed")

	confirms, err = propagate(ctx, t, time.Nanosecond, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, confirms, "the confirm, then one refresh before each decision")
}

// TestRuleStorePG_PropagationStopsWhenTheConfirmWriteFails: a stuck proposal whose quorum
// cannot be confirmed is not decided, and the promotion reports why.
func TestRuleStorePG_PropagationStopsWhenTheConfirmWriteFails(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)

	confirms, err := propagate(ctx, t, time.Hour, errors.New("standby gone"))
	require.ErrorContains(t, err, "failed to confirm stuck proposal quorum")
	require.ErrorContains(t, err, "standby gone")
	assert.Equal(t, 1, confirms)
}
