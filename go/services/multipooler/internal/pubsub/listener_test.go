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

package pubsub

import (
	"context"
	"log/slog"
	"testing"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// running reports whether the listener's background event loop is active. Start
// sets cancel; Stop (which waits for the goroutine to exit) clears it.
func running(l *Listener) bool { return l.cancel != nil }

// newTestListener builds a Listener with no pool manager. The dedicated PG
// connection is acquired lazily on the first subscribe, so a listener that is
// only started/stopped (no subscribers) never touches the pool — which lets us
// exercise the OnStateChange gating without a real postgres.
func newTestListener(t *testing.T) *Listener {
	t.Helper()
	metrics, err := NewPubSubMetrics()
	require.NoError(t, err)
	l := NewListener(nil, slog.Default(), metrics)
	t.Cleanup(l.Stop)
	return l
}

// TestListenerOnStateChangeGating verifies the LISTEN/NOTIFY listener runs only
// when this pooler is the writable leader (RoutingRolePrimary) AND serving. The
// routing role folds in both the consensus-leader and out-of-recovery facts:
// LISTEN/NOTIFY only delivers on the actual postgres primary, so a pooler that is
// not the writable leader must not run the listener.
func TestListenerOnStateChangeGating(t *testing.T) {
	tests := []struct {
		name          string
		routingRole   servingstate.RoutingRole
		servingStatus clustermetadatapb.PoolerServingStatus
		wantRunning   bool
	}{
		{
			name:          "writable leader, serving -> listener runs",
			routingRole:   servingstate.RoutingRolePrimary,
			servingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			wantRunning:   true,
		},
		{
			name:          "not the writable leader, serving -> listener stays off",
			routingRole:   servingstate.RoutingRoleReplica,
			servingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			wantRunning:   false,
		},
		{
			name:          "writable leader but draining -> listener stays off",
			routingRole:   servingstate.RoutingRolePrimary,
			servingStatus: clustermetadatapb.PoolerServingStatus_DRAINING,
			wantRunning:   false,
		},
		{
			name:          "writable leader but disabled -> listener stays off",
			routingRole:   servingstate.RoutingRolePrimary,
			servingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
			wantRunning:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newTestListener(t)
			err := l.OnStateChange(context.Background(), servingstate.State{Routing: servingstate.RoutingState{Role: tt.routingRole}, ServingStatus: tt.servingStatus})
			require.NoError(t, err)
			assert.Equal(t, tt.wantRunning, running(l))
		})
	}
}

// TestListenerOnStateChangeStopsOnDemotion verifies that a running listener is
// stopped when the node loses writability (e.g. demoted from leader to standby),
// which is the failure this gating guards against.
func TestListenerOnStateChangeStopsOnDemotion(t *testing.T) {
	l := newTestListener(t)

	require.NoError(t, l.OnStateChange(context.Background(), servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	require.True(t, running(l), "listener should run while leader+writable+serving")

	// Postgres falls back into recovery (demoted to standby) while still nominally
	// the leader: the listener must stop.
	require.NoError(t, l.OnStateChange(context.Background(), servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	assert.False(t, running(l), "listener must stop once postgres is no longer writable")
}
