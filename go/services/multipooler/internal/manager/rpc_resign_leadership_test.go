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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

// The SQL-reset and pgctld-restart barriers make publication order observable
// without sleeps: an early resignation lets orch pause the standby's receiver
// before the outgoing primary has sent its shutdown-checkpoint WAL.
func TestResignLeadership_PublicationOrder(t *testing.T) {
	for _, failure := range []string{"none", "reset", "restart", "verify standby"} {
		t.Run(failure, func(t *testing.T) {
			self := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"}
			position := makeRulePosition(7)
			position.Position.Decision.LeaderId = self
			rules := &fakeRuleStore{pos: position}
			qs := mock.NewQueryService()
			// Guard, demotion-state probe and getWALPosition all see primary.
			for range 3 {
				qs.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))
			}
			qs.AddQueryPattern("SELECT pg_current_wal_lsn", mock.MakeQueryResult([]string{"lsn"}, [][]any{{"0/1000"}}))
			qs.AddQueryPattern("FROM pg_stat_activity", mock.MakeQueryResult([]string{"pid"}, nil))
			qs.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult([]string{"one"}, [][]any{{1}}))
			qs.AddQueryPattern("SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult([]string{"lsn"}, [][]any{{"0/2000"}}))
			pm, _ := setupManagerWithMockDB(t, qs, rules)

			lockCtx, err := pm.actionLock.Acquire(t.Context(), "seed-resign-test")
			require.NoError(t, err)
			require.NoError(t, pm.stateManager.Mutate(lockCtx, func(s *servingStateMutation) {
				s.PostgresMode = pgmode.Primary
				s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
			}))
			_, err = pm.consensusMgr.SetSuspectedDivergence(lockCtx, true)
			require.NoError(t, err)
			pm.actionLock.Release(lockCtx)

			resetEntered, allowReset := make(chan struct{}), make(chan struct{})
			releaseReset := sync.OnceFunc(func() { close(allowReset) })
			t.Cleanup(releaseReset)
			qs.AddQueryPatternWithContextCallback("ALTER SYSTEM RESET restore_command", mock.MakeQueryResult(nil, nil), func(ctx context.Context, _ string) {
				assert.NoError(t, actionlock.AssertActionLockHeld(ctx))
				close(resetEntered)
				<-allowReset
			})
			resetErr := errors.New("resign test reset failed")
			if failure == "reset" {
				expectReloadConfigFailure(qs, resetErr)
			} else {
				expectReloadConfig(qs)
			}
			if failure == "verify standby" {
				qs.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))
			} else {
				qs.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
			}

			restartEntered, allowRestart := make(chan struct{}), make(chan struct{})
			releaseRestart := sync.OnceFunc(func() { close(allowRestart) })
			t.Cleanup(releaseRestart)
			pg := &testutil.MockPgCtldService{}
			if failure == "restart" {
				pg.RestartError = errors.New("resign test restart failed")
			}
			pg.RestartFunc = func(req *pgctldpb.RestartRequest) {
				assert.Equal(t, "fast", req.Mode)
				assert.True(t, req.AsStandby)
				close(restartEntered)
				<-allowRestart
			}
			addr, cleanup := testutil.StartMockPgctldServer(t, pg)
			t.Cleanup(cleanup)
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			pm.pgctldClient = NewProtectedPgctldClient(pgctldpb.NewPgCtldClient(conn))
			// Release barriers before gRPC-server cleanup even if an assertion
			// aborts the test while the manager is blocked inside a mock.
			t.Cleanup(func() {
				releaseReset()
				releaseRestart()
			})

			type result struct {
				resp *multipoolermanagerdatapb.ResignLeadershipResponse
				err  error
			}
			done := make(chan result, 1)
			go func() {
				resp, err := pm.ResignLeadership(t.Context(), &multipoolermanagerdatapb.ResignLeadershipRequest{})
				done <- result{resp, err}
			}()
			select {
			case <-resetEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("ResignLeadership did not reach restore-command reset")
			}
			assert.Zero(t, pm.consensusMgr.ResignedLeaderAtTerm(), "no signal before reset completes")
			assert.Equal(t, clustermetadatapb.PoolerServingStatus_DRAINING, pm.record.ServingStatus())

			// A refreshed cached position must not change the outgoing term or
			// re-evaluate the original self-leader condition after the attempt.
			refreshPosition := func() {
				rules.mu.Lock()
				defer rules.mu.Unlock()
				rules.pos = makeRulePosition(9) // no longer names self as leader
			}
			if failure == "reset" {
				refreshPosition()
			}
			releaseReset()
			if failure != "reset" {
				select {
				case <-restartEntered:
				case <-time.After(5 * time.Second):
					t.Fatal("ResignLeadership did not reach restart")
				}
				assert.Zero(t, pm.consensusMgr.ResignedLeaderAtTerm(), "no signal while restart is blocked")
				assert.Equal(t, clustermetadatapb.PoolerServingStatus_DRAINING, pm.record.ServingStatus())
				refreshPosition()
				releaseRestart()
			}
			var got result
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("ResignLeadership did not return")
			}
			assert.Equal(t, int64(7), pm.consensusMgr.ResignedLeaderAtTerm(), "publish captured outgoing term, even on failure")
			if failure == "reset" {
				assert.ErrorIs(t, got.err, resetErr)
				assert.Empty(t, pg.RestartCalls, "failed reset must prevent restart")
			} else {
				assert.Len(t, pg.RestartCalls, 1)
			}
			pm.stateManager.mu.Lock()
			mode := pm.stateManager.pgMode
			pm.stateManager.mu.Unlock()
			if failure == "none" {
				require.NoError(t, got.err)
				require.NotNil(t, got.resp)
				assert.Equal(t, "0/2000", got.resp.FlushLsn)
				assert.False(t, pm.consensusMgr.SuspectedDivergence())
				assert.Equal(t, pgmode.InRecovery, mode)
				assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, pm.record.ServingStatus())
			} else {
				require.Error(t, got.err)
				assert.Nil(t, got.resp)
				switch failure {
				case "restart":
					assert.ErrorContains(t, got.err, "resign test restart failed")
				case "verify standby":
					assert.ErrorContains(t, got.err, "server not in recovery mode")
				}
				assert.True(t, pm.consensusMgr.SuspectedDivergence(), "failure must not clear divergence")
				assert.Equal(t, pgmode.Primary, mode, "failure must not claim verified standby mode")
				assert.Equal(t, clustermetadatapb.PoolerServingStatus_DRAINING, pm.record.ServingStatus())
			}
			// Publication requires the held action lock, and it must be released
			// on return so normal recovery can acquire it.
			lockCtx, err = pm.actionLock.Acquire(t.Context(), "after-resign-test")
			require.NoError(t, err)
			pm.actionLock.Release(lockCtx)
		})
	}
}
