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
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/tools/timer"
)

func newExternalMonitorManager(t *testing.T) (*MultipoolerManager, *mock.QueryService) {
	t.Helper()
	initial := newTestMultipooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	initial.ShardRef = &clustermetadatapb.ShardRef{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"}
	initial.ManagementMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	pm, err := NewMultipoolerManager(newTestLogger(), initial, &Config{
		ExternalBackend: &ExternalBackend{Host: "db.example.com", Port: 5432, Database: "db"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		pm.cancel()
		pm.shutdownCancel()
	})
	qs := mock.NewQueryService()
	pm.qsc = &mockPoolerController{queryService: qs}
	return pm, qs
}

func exactly(sql string) string { return "^" + regexp.QuoteMeta(sql) + "$" }

func TestExternalReadinessFailsClosedAndRecovers(t *testing.T) {
	pm, qs := newExternalMonitorManager(t)
	for _, step := range []struct {
		name    string
		value   any
		err     error
		serving bool
	}{
		{name: "writable", value: true, serving: true},
		{name: "read only", value: false},
		{name: "recovered", value: true, serving: true},
		{name: "disconnected", err: errors.New("connection lost")},
		{name: "malformed", value: "invalid"},
		{name: "reconnected", value: true, serving: true},
	} {
		t.Run(step.name, func(t *testing.T) {
			if step.err != nil {
				qs.AddQueryPatternOnceWithError(exactly(externalReadinessQuery), step.err)
			} else {
				qs.AddQueryPatternOnce(exactly(externalReadinessQuery), mock.MakeQueryResult([]string{"writable", "database"}, [][]any{{step.value, "db"}}))
			}
			pm.checkExternalReadiness(t.Context())
			require.Equal(t, step.serving, pm.record.ServingStatus() == clustermetadatapb.PoolerServingStatus_SERVING)
			require.Equal(t, step.serving, pm.record.RoutingState().GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
			require.Nil(t, pm.record.RoutingState().GetRule(), "an external backend never carries a consensus rule")
			require.NoError(t, qs.ExpectationsWereMet())
		})
	}
}

// TestExternalDatabaseMismatchWithdrawsReadiness verifies that an endpoint that
// answers in a database other than the one the connection names is never served
// from, and that readiness returns once it is the right one again.
func TestExternalDatabaseMismatchWithdrawsReadiness(t *testing.T) {
	pm, qs := newExternalMonitorManager(t)
	for _, tc := range []struct {
		database string
		serving  bool
	}{
		{"db", true},
		{"otherdb", false},
		{"db", true},
	} {
		qs.AddQueryPatternOnce(exactly(externalReadinessQuery), mock.MakeQueryResult([]string{"writable", "database"}, [][]any{{true, tc.database}}))
		pm.checkExternalReadiness(t.Context())
		require.Equal(t, tc.serving, pm.record.ServingStatus() == clustermetadatapb.PoolerServingStatus_SERVING, "%+v", tc)
	}
	require.NoError(t, qs.ExpectationsWereMet())
}

func healthyBackend(qs *mock.QueryService) {
	qs.AddQueryPatternOnce(exactly(externalReadinessQuery), mock.MakeQueryResult([]string{"writable", "database"}, [][]any{{true, "db"}}))
}

// TestReadinessIsNotAppliedWithoutTheActionLock: the transition to SERVING runs under the action
// lock, so while something else holds it a healthy backend does not open the pooler.
func TestReadinessIsNotAppliedWithoutTheActionLock(t *testing.T) {
	pm, qs := newExternalMonitorManager(t)
	healthyBackend(qs)
	// Hold the action lock so that the transition cannot run.
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	pm.checkExternalReadiness(ctx)
	pm.actionLock.Release(lockCtx)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, pm.record.ServingStatus())
}

func TestUnmanagedOpensClosed(t *testing.T) {
	pm, _ := newExternalMonitorManager(t)
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)
	// The monitor is what opens an unmanaged pooler; Open alone must not serve.
	pm.pgMonitor = noopRunner{}
	pm.Open(lockCtx)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, pm.record.ServingStatus())
}

type noopRunner struct{}

func (noopRunner) StartWithOptions(func(ctx context.Context), ...timer.StartOption) bool { return true }
func (noopRunner) Stop()                                                                 {}
