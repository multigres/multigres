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
	"cmp"
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func newExternalMonitorManager(t *testing.T) (*MultipoolerManager, *mock.QueryService) {
	t.Helper()
	initial := newTestMultipooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	initial.ShardRef = &clustermetadatapb.ShardRef{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"}
	initial.LeadershipMode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED
	initial.TablegroupId = 5
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

// probeRow is the answer to the readiness probe: whether the endpoint is writable, and the
// database it is in.
func probeRow(writable any, database string) *sqltypes.Result {
	return mock.MakeQueryResult([]string{"writable", "database"}, [][]any{{writable, database}})
}

func TestExternalReadinessFailsClosedAndRecovers(t *testing.T) {
	pm, qs := newExternalMonitorManager(t)
	for _, step := range []struct {
		name     string
		value    any
		database string // "db" when empty
		err      error
		serving  bool
	}{
		{name: "writable", value: true, serving: true},
		// Up, but not writable: a standby, or a backend that is in recovery or read only, never
		// serves, and it is still the shard's PRIMARY.
		{name: "up but in recovery or read only", value: false},
		{name: "recovered", value: true, serving: true},
		{name: "disconnected", err: errors.New("connection lost")},
		{name: "malformed", value: "invalid"},
		{name: "another database", value: true, database: "otherdb"},
		{name: "reconnected", value: true, serving: true},
	} {
		t.Run(step.name, func(t *testing.T) {
			if step.err != nil {
				qs.AddQueryPatternOnceWithError(exactly(externalReadinessQuery), step.err)
			} else {
				qs.AddQueryPatternOnce(exactly(externalReadinessQuery), probeRow(step.value, cmp.Or(step.database, "db")))
			}
			pm.checkExternalReadiness(t.Context())
			require.Equal(t, step.serving, pm.record.ServingStatus() == clustermetadatapb.PoolerServingStatus_SERVING)
			require.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, pm.record.RoutingState().GetRole(), "the role does not follow the backend")
			require.Nil(t, pm.record.RoutingState().GetRule(), "an external backend never carries a consensus rule")
			require.NoError(t, qs.ExpectationsWereMet())
		})
	}
}

// TestAnExternalPoolerIsActiveAndPublishedAsServingPrimaryWhenReady: once its backend is ready the
// pooler is published as PRIMARY, SERVING and ACTIVE, which is what a tablegroup's serving-shard
// count asks for. It stays ACTIVE when the backend is not ready.
func TestAnExternalPoolerIsActiveAndPublishedAsServingPrimaryWhenReady(t *testing.T) {
	pm, qs := newExternalMonitorManager(t)
	// As published: the record the topology holds, not only the one in memory.
	published := func() *clustermetadatapb.Multipooler { return routingStateForPublish(pm.record.Snapshot()) }
	lifecycle := func() clustermetadatapb.PoolerLifecycleStatus {
		return pm.record.Snapshot().GetLifecycleStatus().GetStatus()
	}

	require.NotEqual(t, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE, lifecycle(), "not before its backend is ready")
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, published().GetServingStatus())

	healthyBackend(qs)
	pm.checkExternalReadiness(t.Context())
	require.Equal(t, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE, lifecycle())
	require.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, published().GetRoutingState().GetRole())
	require.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, published().GetServingStatus())
	require.EqualValues(t, 5, published().GetTablegroupId())

	qs.AddQueryPatternOnceWithError(exactly(externalReadinessQuery), errors.New("connection lost"))
	pm.checkExternalReadiness(t.Context())
	require.Equal(t, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE, lifecycle())
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, published().GetServingStatus(), "not while its backend is not ready")
}

func healthyBackend(qs *mock.QueryService) {
	qs.AddQueryPatternOnce(exactly(externalReadinessQuery), probeRow(true, "db"))
}

// TestReadinessIsNotAppliedWithoutTheActionLock: the transition to SERVING runs under the action
// lock, so while something else holds it a healthy backend does not open the pooler.
func TestReadinessIsNotAppliedWithoutTheActionLock(t *testing.T) {
	pm, qs := newExternalMonitorManager(t)
	healthyBackend(qs)
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
	pm.pgMonitor = noopPeriodicRunner{}
	pm.Open(lockCtx)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, pm.record.ServingStatus())
}

// recordingComponent records the state every fan-out hands it.
type recordingComponent struct{ states []servingstate.State }

func (c *recordingComponent) OnStateChange(_ context.Context, state servingstate.State) error {
	c.states = append(c.states, state)
	return nil
}

// TestABackendThatIsDownLeavesTheRoutingRolePrimary: a backend that stops being ready makes the
// pooler DISABLED but not a non-primary, and its recovery is no role transition, so what keys on
// the role becoming PRIMARY (the fence gate) is not reset by it.
func TestABackendThatIsDownLeavesTheRoutingRolePrimary(t *testing.T) {
	pm, qs := newExternalMonitorManager(t)
	rec := &recordingComponent{}
	pm.stateManager.Register(rec)
	published := func() *clustermetadatapb.Multipooler { return routingStateForPublish(pm.record.Snapshot()) }

	for _, step := range []struct {
		name    string
		err     error
		serving clustermetadatapb.PoolerServingStatus
	}{
		{name: "ready", serving: clustermetadatapb.PoolerServingStatus_SERVING},
		{name: "down", err: errors.New("connection lost"), serving: clustermetadatapb.PoolerServingStatus_DISABLED},
		{name: "still down", err: errors.New("connection refused"), serving: clustermetadatapb.PoolerServingStatus_DISABLED},
		{name: "recovered", serving: clustermetadatapb.PoolerServingStatus_SERVING},
	} {
		if step.err != nil {
			qs.AddQueryPatternOnceWithError(exactly(externalReadinessQuery), step.err)
		} else {
			healthyBackend(qs)
		}
		pm.checkExternalReadiness(t.Context())
		require.Equal(t, step.serving, pm.record.ServingStatus(), step.name)
		// What the topology holds, and so what the coordinator that refreshes fences finds.
		require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, published().GetType(), step.name)
		require.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, published().GetRoutingState().GetRole(), step.name)
		require.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, pm.stateManager.RoutingRole(), step.name)
	}

	// Components saw the serving status move and the role never change.
	require.NotEmpty(t, rec.states)
	var serving []clustermetadatapb.PoolerServingStatus
	for _, s := range rec.states {
		require.Equal(t, servingstate.RoutingRolePrimary, s.Routing.Role)
		serving = append(serving, s.ServingStatus)
	}
	require.Equal(t, []clustermetadatapb.PoolerServingStatus{
		clustermetadatapb.PoolerServingStatus_SERVING, clustermetadatapb.PoolerServingStatus_DISABLED, clustermetadatapb.PoolerServingStatus_SERVING,
	}, serving, "one fan-out per serving change; a probe that changes nothing fans out nothing")
}
